package com.ww.cache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP 缓存客户端：连接远程节点发送命令并获取结果
 */
public class CacheClient implements CacheRpcClient {

    private static final Logger log = LoggerFactory.getLogger(CacheClient.class);

    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 10000;

    private final String host;
    private final int port;
    private final String authToken;
    private final AtomicInteger requestIdGen = new AtomicInteger(0);
    private final Object lock = new Object();
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;

    public CacheClient(String host, int port) {
        this(host, port, null);
    }

    public CacheClient(String host, int port, String authToken) {
        this.host = host;
        this.port = port;
        this.authToken = authToken;
    }

    private void ensureConnected(String purpose) throws IOException {
        synchronized (lock) {
            if (socket == null || socket.isClosed()) {
                socket = new Socket();
                socket.connect(new java.net.InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                socket.setSoTimeout(READ_TIMEOUT_MS);
                out = new DataOutputStream(socket.getOutputStream());
                in = new DataInputStream(socket.getInputStream());
                log.info("已{}到远程缓存 {}:{}", purpose, host, port);

                // 认证握手
                if (authToken != null && !authToken.isEmpty()) {
                    byte[] authPayload = Codec.encodeValue(authToken);
                    byte[] authReq = CacheProtocol.encodeRequest(CacheProtocol.FLAG_NORMAL,
                            CacheProtocol.OP_AUTH, 0, authPayload);
                    out.write(authReq);
                    out.flush();
                    byte[] resp = readRawResponse();
                    // 检查认证响应状态
                    if (resp != null && resp.length >= CacheProtocol.HEADER_SIZE) {
                        byte status = resp[6]; // STATUS 字段偏移
                        if (status == CacheProtocol.STATUS_ERROR) {
                            close();
                            throw new IOException("远程缓存认证失败: " + host + ":" + port);
                        }
                    }
                }
            }
        }
    }

    // 修复 #17：原实现把 reqId (header[3..6]) 当作 payloadLen，
    // 导致 reqId=0 时 payloadLen 错读为 0、后续命令 readResponse 头部错位、连接立即失效。
    // 协议头布局：[version 1][flags 1][status 1][reqId 4][payloadLen 4] = 11 字节
    // payloadLen 正确偏移在 header[7..10]。
    private byte[] readRawResponse() throws IOException {
        int magic = in.readInt();
        if (magic != CacheProtocol.MAGIC) {
            throw new IOException("invalid response magic: " + Integer.toHexString(magic));
        }
        byte[] header = new byte[CacheProtocol.HEADER_SIZE - 4]; // 已读 magic
        in.readFully(header);
        int payloadLen = ((header[7] & 0xFF) << 24) | ((header[8] & 0xFF) << 16)
                | ((header[9] & 0xFF) << 8) | (header[10] & 0xFF);
        if (payloadLen < 0) {
            throw new IOException("invalid payloadLen: " + payloadLen);
        }
        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) in.readFully(payload);
        byte[] full = new byte[CacheProtocol.HEADER_SIZE + payloadLen];
        System.arraycopy(header, 0, full, 4, header.length);
        if (payloadLen > 0) System.arraycopy(payload, 0, full, CacheProtocol.HEADER_SIZE, payloadLen);
        return full;
    }

    @Override
    public void close() {
        synchronized (lock) {
            try { if (in != null) in.close(); } catch (IOException ignored) {}
            try { if (out != null) out.close(); } catch (IOException ignored) {}
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
            socket = null;
            in = null;
            out = null;
        }
    }

    /**
     * 发送命令并返回响应载荷
     */
    public byte[] send(byte opCode, byte[] argsPayload) throws IOException {
        return send(CacheProtocol.FLAG_NORMAL, opCode, argsPayload, "调用");
    }

    /**
     * 发送带标志的命令（如 FLAG_REPLICA）
     */
    public byte[] send(byte flags, byte opCode, byte[] argsPayload) throws IOException {
        return send(flags, opCode, argsPayload, "调用");
    }

    private static final int MAX_SEND_RETRIES = 2;

    /**
     * 发送带标志和上下文描述的命令。连接断开时自动重连并重试一次。
     */
    public byte[] send(byte flags, byte opCode, byte[] argsPayload, String purpose) throws IOException {
        IOException lastError = null;
        for (int attempt = 0; attempt <= MAX_SEND_RETRIES; attempt++) {
            try {
                ensureConnected(purpose);
                int reqId = requestIdGen.incrementAndGet();
                byte[] request = CacheProtocol.encodeRequest(flags, opCode, reqId, argsPayload);
                synchronized (lock) {
                    out.write(request);
                    out.flush();
                    return readResponse(reqId);
                }
            } catch (IOException e) {
                lastError = e;
                // 服务端业务错误（STATUS_ERROR → RemoteCacheException）不重试：
                // 重试只会得到相同结果，且会丢失原始 "remote error: ..." 消息。
                // 直接向上抛出，保留原始错误信息供调用方诊断。
                if (e instanceof RemoteCacheException) {
                    throw e;
                }
                // 网络错误：关闭旧连接，准备重连重试
                close();
                if (attempt < MAX_SEND_RETRIES) {
                    log.debug("发送失败 (尝试 {}/{}), 准备重连重试: {}", attempt + 1, MAX_SEND_RETRIES + 1, e.getMessage());
                }
            }
        }
        throw new IOException("发送命令到 " + host + ":" + port + " 连续 " + (MAX_SEND_RETRIES + 1) + " 次失败: " + purpose, lastError);
    }

    /**
     * 发送命令不等待响应（用于副本同步等 fire-and-forget 场景）。
     * <p>服务端对 FLAG_REPLICA 命令不回复响应，客户端也不读取响应，避免套接字缓冲区堆积。
     */
    public void sendAsync(byte flags, byte opCode, byte[] argsPayload) throws IOException {
        sendAsync(flags, opCode, argsPayload, "调用");
    }

    /**
     * 发送带上下文描述的命令，不等待响应（fire-and-forget）。
     */
    public void sendAsync(byte flags, byte opCode, byte[] argsPayload, String purpose) throws IOException {
        ensureConnected(purpose);
        int reqId = requestIdGen.incrementAndGet();
        byte[] request = CacheProtocol.encodeRequest(flags, opCode, reqId, argsPayload);
        synchronized (lock) {
            out.write(request);
            out.flush();
            // 不读取响应 — 服务端对 FLAG_REPLICA 不回复
        }
    }

    private byte[] readResponse(int expectedReqId) throws IOException {
        int magic = in.readInt();
        if (magic != CacheProtocol.MAGIC) {
            throw new IOException("invalid response magic: " + Integer.toHexString(magic));
        }
        in.readByte(); // version
        in.readByte(); // flags
        byte status = in.readByte();
        int reqId = in.readInt();
        int payloadLen = in.readInt();
        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) in.readFully(payload);

        if (reqId != expectedReqId) {
            log.warn("响应 requestId 不匹配: expected={}, got={}", expectedReqId, reqId);
        }
        if (status == CacheProtocol.STATUS_ERROR) {
            String errMsg = Codec.decodeValue(payload, String.class);
            // 使用 RemoteCacheException 以便 send() 重试逻辑识别并跳过重试：
            // 服务端业务错误是确定性的，重试只会得到相同结果且丢失原始错误消息。
            throw new RemoteCacheException("remote error: " + (errMsg != null ? errMsg : "unknown"));
        }
        return payload;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }

    // ==================== 便捷方法 ====================

    public void put(String key, Object value) throws IOException {
        send(CacheProtocol.OP_PUT, CacheProtocol.encodeArgs(CacheProtocol.OP_PUT, key, value));
    }

    public void put(String key, Object value, long ttlMillis) throws IOException {
        send(CacheProtocol.OP_PUT_EXPIRED, CacheProtocol.encodeArgs(CacheProtocol.OP_PUT_EXPIRED, key, value, ttlMillis));
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) throws IOException {
        byte[] resp = send(CacheProtocol.OP_GET, CacheProtocol.encodeArgs(CacheProtocol.OP_GET, key));
        return (T) Codec.decodeValue(resp);
    }

    public void remove(String key) throws IOException {
        send(CacheProtocol.OP_REMOVE, CacheProtocol.encodeArgs(CacheProtocol.OP_REMOVE, key));
    }

    public void clear() throws IOException {
        send(CacheProtocol.OP_CLEAR, new byte[]{CacheProtocol.OP_CLEAR});
    }

    // ==================== Stream 便捷方法 ====================

    public StreamId xadd(String key, StreamId idReq, java.util.Map<String, String> fields, XAddOptions options) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XADD,
                CacheProtocol.encodeXAddArgs(key, idReq, fields, options));
        return CacheProtocol.decodeXAddResponse(resp);
    }

    public long xlen(String key) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XLEN, CacheProtocol.encodeKeyOnlyArgs(CacheProtocol.OP_XLEN, key));
        Object v = Codec.decodeValue(resp);
        return v instanceof Number n ? n.longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    public java.util.List<StreamEntry> xrange(String key, StreamId min, StreamId max, long count) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XRANGE,
                CacheProtocol.encodeXRangeArgs(CacheProtocol.OP_XRANGE, key, min, max, count));
        return CacheProtocol.decodeXRangeResponse(resp);
    }

    @SuppressWarnings("unchecked")
    public java.util.List<StreamEntry> xrevrange(String key, StreamId max, StreamId min, long count) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XREVRANGE,
                CacheProtocol.encodeXRangeArgs(CacheProtocol.OP_XREVRANGE, key, min, max, count));
        return CacheProtocol.decodeXRangeResponse(resp);
    }

    public long xdel(String key, StreamId... ids) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XDEL, CacheProtocol.encodeXDelArgs(key, ids));
        Object v = Codec.decodeValue(resp);
        return v instanceof Number n ? n.longValue() : 0L;
    }

    public long xtrim(String key, long maxLen, boolean approximate) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XTRIM, CacheProtocol.encodeXTrimArgs(key, maxLen, approximate));
        Object v = Codec.decodeValue(resp);
        return v instanceof Number n ? n.longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    public java.util.List<StreamEntry> xread(String key, StreamId from, long count, long blockMs) throws IOException {
        // 注意：阻塞 XREAD 会让当前连接阻塞最多 blockMs（受服务端 MAX_BLOCK_MS=30s 钳制）
        // 客户端的 READ_TIMEOUT_MS=10s 可能不够，调用方需自行调大 socket soTimeout
        byte[] resp = send(CacheProtocol.OP_XREAD, CacheProtocol.encodeXReadArgs(key, from, count, blockMs));
        Object v = Codec.decodeValue(resp);
        if (v instanceof java.util.List<?> list) return (java.util.List<StreamEntry>) (java.util.List<?>) list;
        return new java.util.ArrayList<>();
    }

    public boolean xgroupCreate(String key, String group, StreamId startId, boolean mkstream) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XGROUP_CREATE,
                CacheProtocol.encodeXGroupCreateArgs(key, group, startId, mkstream));
        Object v = Codec.decodeValue(resp);
        return v instanceof Boolean b ? b : Boolean.TRUE.equals(v);
    }

    public boolean xgroupDestroy(String key, String group) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XGROUP_DESTROY,
                CacheProtocol.encodeXGroupDestroyArgs(key, group));
        Object v = Codec.decodeValue(resp);
        return v instanceof Boolean b ? b : Boolean.TRUE.equals(v);
    }

    public long xack(String key, String group, StreamId... ids) throws IOException {
        byte[] resp = send(CacheProtocol.OP_XACK, CacheProtocol.encodeXAckArgs(key, group, ids));
        Object v = Codec.decodeValue(resp);
        return v instanceof Number n ? n.longValue() : 0L;
    }

    // ==================== 集群管理便捷方法 ====================

    /** 发送 PING（gossip ping，携带本端 membership 快照），返回对端 PONG 快照 */
    @SuppressWarnings("unchecked")
    public java.util.List<java.util.List<Object>> ping(String senderNodeKey, long senderEpoch,
                                                       java.util.List<java.util.List<Object>> snapshot) throws IOException {
        byte[] resp = send(CacheProtocol.OP_PING,
                CacheProtocol.encodeGossipArgs(CacheProtocol.OP_PING, senderNodeKey, senderEpoch, snapshot));
        Object v = Codec.decodeValue(resp);
        if (v instanceof java.util.List<?> list) return (java.util.List<java.util.List<Object>>) (java.util.List<?>) list;
        return new java.util.ArrayList<>();
    }

    /** 发送 NODE_JOIN */
    public byte[] nodeJoin(String host, int port, double weight, long epoch) throws IOException {
        return send(CacheProtocol.OP_NODE_JOIN,
                CacheProtocol.encodeNodeMembershipArgs(CacheProtocol.OP_NODE_JOIN, host, port, weight, epoch));
    }

    /** 发送 NODE_LEAVE */
    public byte[] nodeLeave(String host, int port, double weight, long epoch) throws IOException {
        return send(CacheProtocol.OP_NODE_LEAVE,
                CacheProtocol.encodeNodeMembershipArgs(CacheProtocol.OP_NODE_LEAVE, host, port, weight, epoch));
    }

    /** 发送 PROMOTE（人工 failover 通知） */
    public byte[] promote(String failedNodeKey, String newPrimaryNodeKey, long epoch) throws IOException {
        return send(CacheProtocol.OP_PROMOTE,
                CacheProtocol.encodePromoteArgs(failedNodeKey, newPrimaryNodeKey, epoch));
    }

    /** 发送 CLUSTER_STATUS 查询，返回集群状态对象 */
    public Object clusterStatus() throws IOException {
        byte[] resp = send(CacheProtocol.OP_CLUSTER_STATUS, CacheProtocol.encodeClusterStatusArgs());
        return Codec.decodeValue(resp);
    }
}
