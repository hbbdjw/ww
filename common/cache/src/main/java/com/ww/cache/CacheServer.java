package com.ww.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP 缓存服务器：监听端口，接收远程节点的缓存命令并执行。
 */
public class CacheServer {

    private static final Logger log = LoggerFactory.getLogger(CacheServer.class);

    /** 有界工作线程池，避免慢客户端/恶意攻击打爆线程数 {@code 缺陷18修复} */
    private static final int MAX_WORKER_THREADS = 64;
    private static final int WORKER_QUEUE_SIZE = 256;

    private final int port;
    private final DistributedCache distributedCache;
    private final SimpleCache<String, Object> localCache;
    private final Cache<String, Object> cache;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService threadPool = new ThreadPoolExecutor(
            8, MAX_WORKER_THREADS,
            60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(WORKER_QUEUE_SIZE),
            r -> {
                Thread t = new Thread(r, "cache-handler");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());
    private final String authToken;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    /** 集群管理组件（可选；CacheConfig 注入） */
    private ClusterMembership membership;
    private FailoverManager failoverManager;
    private RebalanceManager rebalanceManager;
    /** gRPC 桥接器（可选；bindRpcService 后创建） */
    private CacheRpcBridge rpcBridge;

    public CacheServer(int port, Cache<String, Object> cache) {
        this(port, cache, null);
    }

    public CacheServer(int port, Cache<String, Object> cache, String authToken) {
        this.port = port;
        this.cache = cache;
        this.authToken = authToken;
        if (cache instanceof DistributedCache dc) {
            this.distributedCache = dc;
            this.localCache = dc.getLocalCache();
        } else {
            this.distributedCache = null;
            this.localCache = null;
        }
    }

    /** 注入集群成员管理（用于处理 PING/JOIN/LEAVE/GOSSIP/CLUSTER_STATUS 等） */
    public void setMembership(ClusterMembership membership) {
        this.membership = membership;
    }

    public void setFailoverManager(FailoverManager failoverManager) {
        this.failoverManager = failoverManager;
    }

    public void setRebalanceManager(RebalanceManager rebalanceManager) {
        this.rebalanceManager = rebalanceManager;
    }

    /**
     * 将本 CacheServer 的命令处理能力注册到 cluster-rpc 的 RpcService 上，
     * 以支持 cache.command / cache.replica 两个 gRPC 服务。
     * <p>注意：此方法仅注册 handler，不启动/关闭 RpcService（由外部管理生命周期）。
     *
     * @param rpcService cluster-rpc 服务实例
     * @return 桥接器对象（可用于获取 metrics）
     */
    public CacheRpcBridge bindRpcService(com.ww.rpc.core.RpcService rpcService) {
        if (rpcBridge != null) {
            log.warn("CacheServer 已绑定过 RpcService，旧 bridge 将被替换");
        }
        CacheRpcBridge bridge = new CacheRpcBridge(this, rpcService, authToken);
        bridge.register();
        this.rpcBridge = bridge;
        return bridge;
    }

    public CacheRpcBridge getRpcBridge() {
        return rpcBridge;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        try {
            serverSocket = new ServerSocket(port);
            log.info("Cache TCP 服务已启动，监听端口 {}", port);
        } catch (IOException e) {
            log.error("Cache TCP 服务启动失败，端口 {}", port, e);
            running.set(false);
            return;
        }
        acceptThread = new Thread(this::acceptLoop, "cache-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        log.info("正在停止 Cache TCP 服务...");
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            log.warn("关闭 ServerSocket 异常", e);
        }
        threadPool.shutdownNow();
        try {
            threadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) { }
        if (acceptThread != null) {
            try { acceptThread.join(5000); } catch (InterruptedException ignored) { }
        }
        log.info("Cache TCP 服务已停止");
    }

    public int getPort() { return port; }
    public boolean isRunning() { return running.get(); }

    /**
     * 命令分发结果：status + 序列化后的 result payload。
     * 供 TCP 服务端和 gRPC 桥接共同使用。
     */
    public static final class CommandResult {
        public final byte status;
        public final byte[] payload;

        public CommandResult(byte status, byte[] payload) {
            this.status = status;
            this.payload = payload != null ? payload : new byte[0];
        }
    }

    /**
     * 通用命令分发：根据 opCode 将 payload 派发到本地缓存、分布式路由、集群管理等处理路径。
     * <p>TCP 服务端和 gRPC 桥接共用此入口，保证两种协议的命令语义完全一致。
     *
     * @param flags    命令标志位（FLAG_NORMAL / FLAG_REPLICA 等）
     * @param opCode   操作码
     * @param payload  已解码前的原始参数负载（与 TCP 协议线上 payload 格式一致）
     * @param noReply  是否为 fire-and-forget（副本复制）模式；为 true 时返回 null
     * @return 命令结果（含 status 和 payload），noReply=true 时返回 null
     */
    public CommandResult dispatchCommand(byte flags, byte opCode, byte[] payload, boolean noReply) {
        try {
            if (noReply && distributedCache != null) {
                CacheProtocol.executeCommand(localCache, opCode, payload);
                return null;
            }
            Object result;
            byte status;
            if (isClusterOp(opCode)) {
                result = handleClusterOp(opCode, payload);
                status = CacheProtocol.STATUS_OK;
            } else if (distributedCache != null && opCode == CacheProtocol.OP_CLEAR) {
                distributedCache.clear();
                result = null;
                status = CacheProtocol.STATUS_OK;
            } else if (distributedCache != null && opCode == CacheProtocol.OP_CLEAR_OWNED_ONLY) {
                distributedCache.clearOwnedLocallyForRemote();
                result = null;
                status = CacheProtocol.STATUS_OK;
            } else if (distributedCache != null && DistributedCache.isStreamWriteOp(opCode)) {
                result = distributedCache.routeStreamWrite(opCode, payload);
                status = (result == null) ? CacheProtocol.STATUS_NULL : CacheProtocol.STATUS_OK;
            } else if (distributedCache != null && DistributedCache.isStreamReadOp(opCode)) {
                result = distributedCache.routeStreamRead(opCode, payload);
                status = (result == null) ? CacheProtocol.STATUS_NULL : CacheProtocol.STATUS_OK;
            } else if (distributedCache != null && isWriteOp(opCode)) {
                result = distributedCache.handleRemoteWrite(opCode, payload);
                status = (result == null) ? CacheProtocol.STATUS_NULL : CacheProtocol.STATUS_OK;
            } else {
                Cache<String, Object> target = localCache != null ? localCache : cache;
                result = CacheProtocol.executeCommand(target, opCode, payload);
                status = (result == null) ? CacheProtocol.STATUS_NULL : CacheProtocol.STATUS_OK;
            }
            byte[] respPayload = (result == null) ? new byte[0] : Codec.encodeValue(result);
            return new CommandResult(status, respPayload);
        } catch (Exception e) {
            log.error("执行命令失败 opCode={}", Integer.toHexString(opCode & 0xFF), e);
            String errMsg = e.getMessage() != null ? e.getMessage() : "unknown error";
            return new CommandResult(CacheProtocol.STATUS_ERROR, Codec.encodeValue(errMsg));
        }
    }

    private void acceptLoop() {
        while (running.get()) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                Socket client = socket;
                // 提交到池前先清理调用线程 ThreadLocal tx 状态（虽然 accept 线程不用 tx，防御性）
                threadPool.submit(() -> {
                    try {
                        handleClient(client);
                    } finally {
                        // 修复 #19：handler 线程会被池复用，下一次连接的请求若复用同线程，
                        // 上次 OP_MULTI 未 EXEC/DISCARD 残留 ThreadLocal 会污染下次请求语义。
                        clearTxState();
                    }
                });
            } catch (IOException e) {
                if (running.get()) log.warn("accept 异常", e);
                if (socket != null) try { socket.close(); } catch (IOException ignored) { }
            }
        }
    }

    private void clearTxState() {
        if (localCache != null) {
            localCache.clearThreadLocalTx();
        }
    }

    private void handleClient(Socket socket) {
        try (socket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            if (authToken != null && !authToken.isEmpty()) {
                int magic = in.readInt();
                if (magic != CacheProtocol.MAGIC) {
                    log.warn("认证阶段收到非法 magic");
                    return;
                }
                in.readByte(); // version
                in.readByte(); // flags
                byte opCode = in.readByte();
                in.readInt();  // requestId
                int payloadLen = in.readInt();
                byte[] payload = new byte[payloadLen];
                if (payloadLen > 0) in.readFully(payload);
                if (opCode != CacheProtocol.OP_AUTH || !authToken.equals(Codec.decodeValue(payload, String.class))) {
                    log.warn("客户端认证失败 from={}", socket.getRemoteSocketAddress());
                    byte[] errResp = CacheProtocol.encodeResponse(CacheProtocol.FLAG_NORMAL,
                            CacheProtocol.STATUS_ERROR, 0, Codec.encodeValue("auth failed"));
                    out.write(errResp);
                    out.flush();
                    return;
                }
                byte[] okResp = CacheProtocol.encodeResponse(CacheProtocol.FLAG_NORMAL,
                        CacheProtocol.STATUS_OK, 0, Codec.encodeValue("OK"));
                out.write(okResp);
                out.flush();
            }

            while (running.get()) {
                int magic = in.readInt();
                if (magic != CacheProtocol.MAGIC) {
                    log.warn("收到非法 magic: {}", Integer.toHexString(magic));
                    break;
                }
                byte version = in.readByte();
                byte flags = in.readByte();
                byte opCode = in.readByte();
                int requestId = in.readInt();
                int payloadLen = in.readInt();
                byte[] payload = new byte[payloadLen];
                if (payloadLen > 0) in.readFully(payload);

                boolean noReply = (flags & CacheProtocol.FLAG_REPLICA) != 0;

                try {
                    CommandResult cr = dispatchCommand(flags, opCode, payload, noReply);
                    if (noReply || cr == null) {
                        continue;
                    }
                    byte[] response = CacheProtocol.encodeResponse(CacheProtocol.FLAG_NORMAL, cr.status, requestId, cr.payload);
                    response = setRequestId(response, requestId);
                    out.write(response);
                    out.flush();
                } catch (Exception e) {
                    log.error("执行命令失败 opCode={}", Integer.toHexString(opCode & 0xFF), e);
                    if (!noReply) {
                        String errMsg = e.getMessage() != null ? e.getMessage() : "unknown error";
                        byte[] errPayload = Codec.encodeValue(errMsg);
                        byte[] resp = CacheProtocol.encodeResponse(
                                CacheProtocol.FLAG_NORMAL, CacheProtocol.STATUS_ERROR, requestId, errPayload);
                        out.write(resp);
                        out.flush();
                    }
                }
            }
        } catch (IOException e) {
            // 客户端断开连接
        }
    }

    private static boolean isWriteOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_PUT, CacheProtocol.OP_PUT_EXPIRED,
                 CacheProtocol.OP_REMOVE,
                 CacheProtocol.OP_HSET,
                 CacheProtocol.OP_LPUSH,
                 CacheProtocol.OP_RPUSH, CacheProtocol.OP_LPOP, CacheProtocol.OP_RPOP,
                 CacheProtocol.OP_SADD, CacheProtocol.OP_SREM,
                 CacheProtocol.OP_ZADD -> true;
            default -> false;
        };
    }

    /** 集群管理 opCode 范围判断 */
    private static boolean isClusterOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_PING, CacheProtocol.OP_PONG,
                 CacheProtocol.OP_NODE_JOIN, CacheProtocol.OP_NODE_LEAVE,
                 CacheProtocol.OP_PROMOTE,
                 CacheProtocol.OP_MIGRATE_PULL, CacheProtocol.OP_MIGRATE_PUSH,
                 CacheProtocol.OP_CLUSTER_STATUS, CacheProtocol.OP_GOSSIP -> true;
            default -> false;
        };
    }

    /**
     * 处理集群管理 opCode。
     * <p>所有集群 opCode 都依赖 membership；若 membership 未注入则返回错误。
     */
    private Object handleClusterOp(byte opCode, byte[] payload) {
        if (membership == null) {
            throw new CacheException("集群管理 opCode=" + Integer.toHexString(opCode & 0xFF)
                    + " 需要 membership，但本节点未启用动态成员管理");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        try {
            in.readByte(); // skip opCode
            return switch (opCode) {
                case CacheProtocol.OP_PING, CacheProtocol.OP_PONG, CacheProtocol.OP_GOSSIP -> handleGossip(opCode, in);
                case CacheProtocol.OP_NODE_JOIN -> handleNodeJoin(in);
                case CacheProtocol.OP_NODE_LEAVE -> handleNodeLeave(in);
                case CacheProtocol.OP_PROMOTE -> handlePromote(in);
                case CacheProtocol.OP_MIGRATE_PULL -> handleMigratePull(in);
                case CacheProtocol.OP_MIGRATE_PUSH -> handleMigratePush(in);
                case CacheProtocol.OP_CLUSTER_STATUS -> handleClusterStatus();
                default -> throw new CacheException("未实现的集群 opCode=" + Integer.toHexString(opCode & 0xFF));
            };
        } catch (IOException e) {
            throw new CacheException("解析集群 opCode payload 失败: " + e.getMessage(), e);
        }
    }

    /** PING/PONG/GOSSIP：合并对端 membership 快照，回应当前快照 */
    @SuppressWarnings("unchecked")
    private Object handleGossip(byte opCode, DataInputStream in) throws IOException {
        String senderNodeKey = Codec.decodeValue(in, String.class);
        Object epochObj = Codec.decodeValue(in);
        long senderEpoch = epochObj instanceof Number n ? n.longValue() : 0L;
        Object snapshotObj = Codec.decodeValue(in);
        List<List<Object>> snapshot = new ArrayList<>();
        if (snapshotObj instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof List<?> row) snapshot.add((List<Object>) (List<?>) row);
            }
        }
        // 触发对端心跳更新（若对端在 membership 中）
        if (senderNodeKey != null && !senderNodeKey.isEmpty()) {
            membership.touchHeartbeat(senderNodeKey);
        }
        boolean changed = membership.mergeSnapshot(snapshot);
        if (changed) {
            log.debug("gossip {} 收到对端 {} 快照合并，topology 变更", Integer.toHexString(opCode & 0xFF), senderNodeKey);
            // 通知 distributedCache 重建拓扑（异步、轻量）
            if (distributedCache != null) {
                distributedCache.applyTopologyFromMembership();
            }
        }
        // 回应当前 membership 快照（PONG 语义；PING 收到此响应即为对端视角）
        return membership.toSnapshot();
    }

    /** NODE_JOIN：将对端节点加入 membership，并应用新拓扑 */
    private Object handleNodeJoin(DataInputStream in) throws IOException {
        String host = Codec.decodeValue(in, String.class);
        Object portObj = Codec.decodeValue(in);
        Object weightObj = Codec.decodeValue(in);
        Object epochObj = Codec.decodeValue(in);
        int port = portObj instanceof Number n ? n.intValue() : 0;
        double weight = weightObj instanceof Number n2 ? n2.doubleValue() : 1.0;
        long epoch = epochObj instanceof Number n3 ? n3.longValue() : 0L;
        boolean changed = membership.addNode(host, port, weight);
        if (changed && distributedCache != null) {
            distributedCache.applyTopologyFromMembership();
        }
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("joined", changed);
        result.put("topologyVersion", membership.getTopologyVersion());
        result.put("aliveCount", membership.aliveCount());
        result.put("selfEpoch", epoch);
        return result;
    }

    /** NODE_LEAVE：将对端节点标记为 DEAD（但不立即移除，等 gossip 传播） */
    private Object handleNodeLeave(DataInputStream in) throws IOException {
        String host = Codec.decodeValue(in, String.class);
        Object portObj = Codec.decodeValue(in);
        int port = portObj instanceof Number n ? n.intValue() : 0;
        boolean changed = membership.removeNode(host, port);
        if (changed && distributedCache != null) {
            distributedCache.applyTopologyFromMembership();
        }
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("left", changed);
        result.put("topologyVersion", membership.getTopologyVersion());
        return result;
    }

    /** PROMOTE：人工 failover 通知，标记 failedNode 为 DEAD（cancelManual=false 由调用方控制） */
    private Object handlePromote(DataInputStream in) throws IOException {
        String failedNodeKey = Codec.decodeValue(in, String.class);
        String newPrimaryNodeKey = Codec.decodeValue(in, String.class);
        Object epochObj = Codec.decodeValue(in);
        long epoch = epochObj instanceof Number n ? n.longValue() : 0L;
        boolean changed = false;
        if (failedNodeKey != null && !failedNodeKey.isEmpty()) {
            changed = membership.markDead(failedNodeKey);
        }
        if (changed && distributedCache != null) {
            distributedCache.applyTopologyFromMembership();
        }
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("failedNodeKey", failedNodeKey);
        result.put("newPrimaryNodeKey", newPrimaryNodeKey);
        result.put("applied", changed);
        result.put("epoch", epoch);
        return result;
    }

    /**
     * MIGRATE_PULL：返回本节点持有的、应该迁移到目标节点的 KV entries。
     * <p>本方法只做"读取快照"，由调用方（新 primary）通过 MIGRATE_PUSH 写回。
     * 当前实现：返回所有 self 为 primary 的 key（保守策略，由调用方根据新 HRW 二次过滤）。
     * 返回 List of [key, value, remainingTtlMillis]。
     */
    @SuppressWarnings("unchecked")
    private Object handleMigratePull(DataInputStream in) throws IOException {
        if (localCache == null) return new ArrayList<>();
        String targetHost = Codec.decodeValue(in, String.class);
        Object portObj = Codec.decodeValue(in);
        int targetPort = portObj instanceof Number n ? n.intValue() : 0;
        ClusterNode target = membership.getNode(targetHost + ":" + targetPort);
        List<Object> entries = new ArrayList<>();
        if (target == null || distributedCache == null) return entries;
        RendezvousHash hrw = distributedCache.getHash();
        int replicas = distributedCache.getReplicas();
        ClusterNode self = distributedCache.getSelf();
        if (self == null) return entries;
        java.util.Set<String> keys = localCache.keys();
        for (String key : keys) {
            List<ClusterNode> owners = hrw.selectNodes(key, replicas);
            // 当目标节点是 key 的新 primary 时，本节点持有需要迁移的数据
            if (!owners.isEmpty() && owners.getFirst().equals(target)) {
                Object value = localCache.get(key);
                if (value == null) continue;
                long ttl = localCache.remainingTtlMillis(key);
                entries.add(List.of(key, value, ttl));
            }
        }
        return entries;
    }

    /** MIGRATE_PUSH：接收一批 KV entries 写入本地 store（不触发复制） */
    @SuppressWarnings("unchecked")
    private Object handleMigratePush(DataInputStream in) throws IOException {
        Object entriesObj = Codec.decodeValue(in);
        if (!(entriesObj instanceof List<?> list)) {
            Map<String, Object> result = new java.util.HashMap<>();
            result.put("written", 0);
            return result;
        }
        List<Object> entries = (List<Object>) (List<?>) list;
        int count;
        if (rebalanceManager != null) {
            count = rebalanceManager.handleMigratePush(entries);
        } else if (localCache != null) {
            count = 0;
            for (Object o : entries) {
                if (!(o instanceof List<?> row) || row.size() < 3) continue;
                String key = String.valueOf(row.get(0));
                Object value = row.get(1);
                Object ttlObj = row.get(2);
                long ttl = ttlObj instanceof Number n ? n.longValue() : Long.MAX_VALUE;
                if (ttl == Long.MAX_VALUE || ttl < 0) {
                    localCache.put(key, value);
                } else if (ttl > 0) {
                    localCache.put(key, value, ttl);
                }
                count++;
            }
        } else {
            count = 0;
        }
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("written", count);
        return result;
    }

    /** CLUSTER_STATUS：返回本节点视角的集群状态摘要 */
    private Object handleClusterStatus() {
        Map<String, Object> status = new java.util.LinkedHashMap<>();
        status.put("topologyVersion", membership.getTopologyVersion());
        status.put("aliveCount", membership.aliveCount());
        status.put("selfNodeKey", membership.getSelfNodeKey());
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (ClusterNode n : membership.getAllNodes()) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("nodeKey", n.nodeKey());
            m.put("host", n.getHost());
            m.put("port", n.getPort());
            m.put("weight", n.getWeight());
            m.put("status", n.getStatus().name());
            m.put("epoch", n.getEpoch());
            m.put("self", n.isSelf());
            m.put("lastHeartbeatMs", n.getLastHeartbeatMs());
            nodes.add(m);
        }
        status.put("nodes", nodes);
        if (failoverManager != null) {
            status.put("failover", failoverManager.getStatus());
        }
        if (rebalanceManager != null) {
            status.put("rebalance", rebalanceManager.getStatus());
        }
        if (distributedCache != null) {
            status.put("backlogs", distributedCache.getBacklogSizes());
            status.put("replicas", distributedCache.getReplicas());
        }
        return status;
    }

    private static byte[] setRequestId(byte[] frame, int requestId) {
        byte[] copy = frame.clone();
        copy[7] = (byte) (requestId >>> 24);
        copy[8] = (byte) (requestId >>> 16);
        copy[9] = (byte) (requestId >>> 8);
        copy[10] = (byte) requestId;
        return copy;
    }
}
