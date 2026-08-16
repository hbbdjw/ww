package com.ww.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import tools.jackson.databind.ObjectMapper;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.handler.RpcRequestHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 gRPC（cluster-rpc）的 {@link StorageRpcClient} 实现。
 *
 * <p>设计要点：
 * <ul>
 *   <li>控制面操作（ping/广播/head/delete/metadata 等）→ 一元 RPC，payload 为 JSON/字节</li>
 *   <li>数据面操作（put/replicate/get）→ 分块流式传输，每块最大 {@value #CHUNK_SIZE} 字节</li>
 *   <li>元数据通过 RpcRequest.headers 传递（与 HTTP 头语义一致）</li>
 *   <li>注册本地 gRPC handler：收到 RPC 请求后分派到 {@link RequestHandler}（业务层注入）</li>
 * </ul>
 *
 * <h3>serviceName 约定</h3>
 * <pre>
 *   storage.put            — 转发写（主节点接收）
 *   storage.replicate      — 副本复制
 *   storage.get            — 转发读
 *   storage.head           — 转发 head
 *   storage.delete         — 转发删除
 *   storage.rep-delete     — 副本删除
 *   storage.create-bucket  — 创建 bucket
 *   storage.delete-bucket  — 删除 bucket
 *   storage.list-objects   — 列出对象元数据
 *   storage.ping           — 心跳
 *   storage.topology       — 拓扑广播
 *   storage.maintenance    — 维护态广播
 *   storage.failover       — failover 广播
 *   storage.local-delete   — 本地删除（迁移）
 *   storage.reverse-reconcile — 反向同步
 *   storage.chunk          — 数据分块（get 拉取下一块）
 * </pre>
 */
public class GrpcStorageRpcClient implements StorageRpcClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcStorageRpcClient.class);

    static final int CHUNK_SIZE = 256 * 1024; // 256KB per chunk

    static final String SVC_PUT = "storage.put";
    static final String SVC_REPLICATE = "storage.replicate";
    static final String SVC_GET = "storage.get";
    static final String SVC_HEAD = "storage.head";
    static final String SVC_DELETE = "storage.delete";
    static final String SVC_REP_DELETE = "storage.rep-delete";
    static final String SVC_CREATE_BUCKET = "storage.create-bucket";
    static final String SVC_DELETE_BUCKET = "storage.delete-bucket";
    static final String SVC_LIST = "storage.list-objects";
    static final String SVC_PING = "storage.ping";
    static final String SVC_TOPOLOGY = "storage.topology";
    static final String SVC_MAINTENANCE = "storage.maintenance";
    static final String SVC_FAILOVER = "storage.failover";
    static final String SVC_LOCAL_DELETE = "storage.local-delete";
    static final String SVC_REVERSE_RECONCILE = "storage.reverse-reconcile";
    static final String SVC_CHUNK = "storage.chunk";

    // chunk header keys
    static final String H_CHUNK_STREAM_ID = "x-chunk-stream-id";
    static final String H_CHUNK_SEQ = "x-chunk-seq";
    static final String H_CHUNK_TOTAL_SIZE = "x-chunk-total-size";
    static final String H_CHUNK_IS_END = "x-chunk-end";
    static final String H_BUCKET = "x-bucket";
    static final String H_KEY = "x-key";
    static final String H_CONTENT_TYPE = "x-content-type";
    static final String H_LENGTH = "x-length";
    static final String H_REPLICAS = "x-replicas";
    static final String H_ENABLED = "x-enabled";
    static final String H_NODE_KEY = "x-node-key";
    static final String H_STATUS = "x-status";
    static final String H_EPOCH = "x-epoch";
    static final String H_RECURSIVE = "x-recursive";
    static final String H_ERROR = "x-error";

    private final RpcService rpcService;
    private final ObjectMapper objectMapper;
    private final long defaultTimeoutMs;

    /**
     * 本地处理器注册表：serviceName → 处理函数（由 DistributedStorage 等业务层注入）。
     * 与 HTTP controller 的 /internal/storage/* 端点一一对应。
     */
    private final Map<String, RequestHandler> localHandlers = new HashMap<>();

    public GrpcStorageRpcClient(RpcService rpcService, ObjectMapper objectMapper) {
        this(rpcService, objectMapper, 600_000L);
    }

    public GrpcStorageRpcClient(RpcService rpcService, ObjectMapper objectMapper, long defaultTimeoutMs) {
        this.rpcService = rpcService;
        this.objectMapper = objectMapper;
        this.defaultTimeoutMs = defaultTimeoutMs;
        registerGrpcHandlers();
    }

    /**
     * 注册本地业务处理器（替代 HTTP controller 的 /internal/storage/* 端点）。
     * 每种操作对应一个处理器，接收反序列化后的请求。
     */
    public void registerLocalHandler(String serviceName, RequestHandler handler) {
        localHandlers.put(serviceName, handler);
    }

    // ==================== StorageRpcClient 实现 ====================

    @Override
    public ObjectMetadata forwardPut(StorageNode primary, String bucket, String key,
                                     Supplier<InputStream> body, long length,
                                     String contentType, Map<String, String> userMeta) {
        String address = toAddr(primary);
        String streamId = UUID.randomUUID().toString();
        Map<String, String> baseHeaders = new HashMap<>();
        baseHeaders.put(H_BUCKET, bucket);
        baseHeaders.put(H_KEY, key);
        baseHeaders.put(H_CONTENT_TYPE, contentType != null ? contentType : "application/octet-stream");
        baseHeaders.put(H_CHUNK_TOTAL_SIZE, String.valueOf(length));
        if (userMeta != null) {
            for (Map.Entry<String, String> e : userMeta.entrySet()) {
                baseHeaders.put("x-meta-" + e.getKey(), e.getValue());
            }
        }
        try (InputStream in = body.get()) {
            sendChunks(address, SVC_PUT, streamId, in, length, baseHeaders);
        } catch (IOException e) {
            throw new StorageException("forwardPut 流异常: " + primary.nodeKey(), e);
        }
        Map<String, String> endHeaders = new HashMap<>(baseHeaders);
        endHeaders.put(H_CHUNK_STREAM_ID, streamId);
        endHeaders.put(H_CHUNK_IS_END, "true");
        RpcRequest endReq = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_PUT)
                .headers(endHeaders)
                .build();
        RpcResponse resp = rpcService.send(address, endReq, defaultTimeoutMs);
        ensureOk(resp, "forwardPut " + primary.nodeKey());
        return parseMetaFromHeaders(resp.getHeaders());
    }

    @Override
    public void replicatePut(StorageNode replica, String bucket, String key,
                             Supplier<InputStream> body, long length, ObjectMetadata meta) {
        String address = toAddr(replica);
        String streamId = UUID.randomUUID().toString();
        Map<String, String> baseHeaders = new HashMap<>();
        baseHeaders.put(H_BUCKET, bucket);
        baseHeaders.put(H_KEY, key);
        writeMetaToHeaders(baseHeaders, meta);
        baseHeaders.put(H_CHUNK_TOTAL_SIZE, String.valueOf(length));
        try (InputStream in = body.get()) {
            sendChunks(address, SVC_REPLICATE, streamId, in, length, baseHeaders);
        } catch (IOException e) {
            throw new StorageException("replicatePut 流异常: " + replica.nodeKey(), e);
        }
        Map<String, String> endHeaders = new HashMap<>(baseHeaders);
        endHeaders.put(H_CHUNK_STREAM_ID, streamId);
        endHeaders.put(H_CHUNK_IS_END, "true");
        RpcRequest endReq = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_REPLICATE)
                .headers(endHeaders)
                .build();
        RpcResponse resp = rpcService.send(address, endReq, defaultTimeoutMs);
        ensureOk(resp, "replicatePut " + replica.nodeKey());
    }

    @Override
    public void replicateDelete(StorageNode replica, String bucket, String key) {
        String address = toAddr(replica);
        Map<String, String> headers = Map.of(H_BUCKET, bucket, H_KEY, key);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_REP_DELETE)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 5_000);
        if (resp.getStatus() == RpcResponse.Status.ERROR && "not found".equalsIgnoreCase(resp.getErrorMessage())) {
            return; // 404 幂等：副本上已不存在视为成功
        }
        ensureOk(resp, "replicateDelete " + replica.nodeKey());
    }

    @Override
    public ReadHandle forwardGet(StorageNode primary, String bucket, String key) {
        String address = toAddr(primary);
        String streamId = UUID.randomUUID().toString();
        Map<String, String> headers = new HashMap<>();
        headers.put(H_BUCKET, bucket);
        headers.put(H_KEY, key);
        headers.put(H_CHUNK_STREAM_ID, streamId);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_GET)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, defaultTimeoutMs);
        if (!resp.isSuccess()) {
            if (resp.getStatus() == RpcResponse.Status.ERROR
                    && "not-found".equals(resp.getHeaders().get(H_ERROR))) {
                return null;
            }
            throw new StorageException("forwardGet 失败: " + resp.getStatus() + " @ " + primary.nodeKey());
        }
        ObjectMetadata meta = parseMetaFromHeaders(resp.getHeaders());
        InputStream stream = new ChunkedInputStream(address, streamId, resp.getPayload(),
                meta != null ? meta.getSize() : -1);
        if (meta == null) meta = new ObjectMetadata(key, -1, "application/octet-stream", null, 0, 0);
        return new GrpcRemoteObject(meta, stream);
    }

    @Override
    public ObjectMetadata forwardHead(StorageNode primary, String bucket, String key) {
        String address = toAddr(primary);
        Map<String, String> headers = Map.of(H_BUCKET, bucket, H_KEY, key);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_HEAD)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        if (!resp.isSuccess()) {
            if (resp.getStatus() == RpcResponse.Status.ERROR
                    && "not-found".equals(resp.getHeaders().get(H_ERROR))) {
                return null;
            }
            throw new StorageException("forwardHead 失败: " + resp.getStatus() + " @ " + primary.nodeKey());
        }
        return parseMetaFromHeaders(resp.getHeaders());
    }

    @Override
    public void forwardDelete(StorageNode primary, String bucket, String key) {
        String address = toAddr(primary);
        Map<String, String> headers = Map.of(H_BUCKET, bucket, H_KEY, key);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_DELETE)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 10_000);
        if (!resp.isSuccess() && !"not-found".equals(resp.getHeaders().get(H_ERROR))) {
            throw new StorageException("forwardDelete 失败: " + resp.getStatus() + " @ " + primary.nodeKey());
        }
    }

    @Override
    public void forwardCreateBucket(StorageNode node, String name) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of("name", name);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_CREATE_BUCKET)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        if (!resp.isSuccess()) {
            log.warn("forwardCreateBucket 非 OK: {} @ {}", resp.getStatus(), node.nodeKey());
        }
    }

    @Override
    public void forwardDeleteBucket(StorageNode node, String name, boolean recursive) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of("name", name, H_RECURSIVE, String.valueOf(recursive));
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_DELETE_BUCKET)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        if (!resp.isSuccess() && !"not-found".equals(resp.getHeaders().get(H_ERROR))) {
            log.warn("forwardDeleteBucket 非 OK: {} @ {}", resp.getStatus(), node.nodeKey());
        }
    }

    @Override
    public ObjectMetadata[] forwardListObjects(StorageNode node, String bucket) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of(H_BUCKET, bucket);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_LIST)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 30_000);
        if (!resp.isSuccess()) {
            if (resp.getStatus() == RpcResponse.Status.ERROR
                    && "not-found".equals(resp.getHeaders().get(H_ERROR))) {
                return new ObjectMetadata[0];
            }
            throw new StorageException("forwardListObjects 失败: " + resp.getStatus() + " @ " + node.nodeKey());
        }
        byte[] body = resp.getPayload();
        if (body == null || body.length == 0) return new ObjectMetadata[0];
        try {
            return objectMapper.readValue(body, ObjectMetadata[].class);
        } catch (Exception e) {
            throw new StorageException("解析 list 响应失败 @ " + node.nodeKey(), e);
        }
    }

    @Override
    public boolean broadcastTopology(StorageNode node, String topologyPayload, int replicas) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of(H_REPLICAS, String.valueOf(replicas));
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_TOPOLOGY)
                .headers(headers)
                .payload(topologyPayload.getBytes(StandardCharsets.UTF_8))
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        return resp.isSuccess();
    }

    @Override
    public boolean broadcastMaintenance(StorageNode node, boolean enabled) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of(H_ENABLED, String.valueOf(enabled));
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_MAINTENANCE)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        return resp.isSuccess();
    }

    @Override
    public boolean localDelete(StorageNode node, String bucket, String key) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of(H_BUCKET, bucket, H_KEY, key);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_LOCAL_DELETE)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        return resp.isSuccess() || "not-found".equals(resp.getHeaders().get(H_ERROR));
    }

    @Override
    public boolean ping(StorageNode node, long timeoutMs) {
        String address = toAddr(node);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_PING)
                .build();
        try {
            RpcResponse resp = rpcService.send(address, req, Math.max(500, timeoutMs));
            return resp.isSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean broadcastFailover(StorageNode node, String failedNodeKey, String statusName, long epoch) {
        String address = toAddr(node);
        Map<String, String> headers = Map.of(
                H_NODE_KEY, failedNodeKey,
                H_STATUS, statusName,
                H_EPOCH, String.valueOf(epoch));
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_FAILOVER)
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, req, 15_000);
        return resp.isSuccess();
    }

    @Override
    public boolean triggerReverseReconcile(StorageNode node) {
        String address = toAddr(node);
        RpcRequest req = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(SVC_REVERSE_RECONCILE)
                .build();
        RpcResponse resp = rpcService.send(address, req, 600_000);
        return resp.isSuccess();
    }

    // ==================== 内部工具方法 ====================

    private static String toAddr(StorageNode node) {
        return node.baseUrl().replace("http://", "");
    }

    private void sendChunks(String address, String serviceName, String streamId,
                            InputStream in, long totalSize, Map<String, String> baseHeaders) throws IOException {
        byte[] buf = new byte[CHUNK_SIZE];
        int seq = 0;
        int read;
        while ((read = in.read(buf)) != -1) {
            byte[] chunk = new byte[read];
            System.arraycopy(buf, 0, chunk, 0, read);
            Map<String, String> headers = new HashMap<>(baseHeaders);
            headers.put(H_CHUNK_STREAM_ID, streamId);
            headers.put(H_CHUNK_SEQ, String.valueOf(seq++));
            RpcRequest chunkReq = RpcRequest.newBuilder()
                    .requestId(UUID.randomUUID().toString())
                    .serviceName(serviceName)
                    .headers(headers)
                    .payload(chunk)
                    .build();
            RpcResponse resp = rpcService.send(address, chunkReq, defaultTimeoutMs);
            if (!resp.isSuccess()) {
                throw new StorageException("sendChunk 失败 seq=" + (seq - 1) + ": " + resp.getStatus());
            }
        }
    }

    private void ensureOk(RpcResponse resp, String context) {
        if (!resp.isSuccess()) {
            String err = resp.getErrorMessage() != null ? resp.getErrorMessage() : resp.getStatus().toString();
            throw new StorageException(context + " 失败: " + err);
        }
    }

    static void writeMetaToHeaders(Map<String, String> headers, ObjectMetadata meta) {
        if (meta == null) return;
        if (meta.getKey() != null) headers.put("x-obj-key", meta.getKey());
        if (meta.getContentType() != null) headers.put("x-obj-content-type", meta.getContentType());
        if (meta.getEtag() != null) headers.put("x-obj-etag", meta.getEtag());
        headers.put("x-obj-size", String.valueOf(meta.getSize()));
        headers.put("x-obj-created", String.valueOf(meta.getCreatedAt()));
        headers.put("x-obj-version", String.valueOf(meta.getVersion()));
        if (meta.getUserMetadata() != null) {
            for (Map.Entry<String, String> e : meta.getUserMetadata().entrySet()) {
                headers.put("x-obj-meta-" + e.getKey(), e.getValue() != null ? e.getValue() : "");
            }
        }
    }

    static ObjectMetadata parseMetaFromHeaders(Map<String, String> headers) {
        if (headers == null) return null;
        String etag = headers.get("x-obj-etag");
        if (etag == null) return null;
        ObjectMetadata m = new ObjectMetadata();
        m.setEtag(etag);
        m.setContentType(headers.get("x-obj-content-type"));
        m.setSize(parseLong(headers.get("x-obj-size"), -1));
        m.setCreatedAt(parseLong(headers.get("x-obj-created"), 0));
        m.setVersion(parseLong(headers.get("x-obj-version"), 0));
        String key = headers.get("x-obj-key");
        if (key != null) m.setKey(key);
        Map<String, String> um = new HashMap<>();
        for (Map.Entry<String, String> e : headers.entrySet()) {
            String name = e.getKey();
            if (name != null && name.startsWith("x-obj-meta-")) {
                um.put(name.substring("x-obj-meta-".length()), e.getValue());
            }
        }
        m.setUserMetadata(um);
        return m;
    }

    private static long parseLong(String s, long def) {
        if (s == null || s.isEmpty()) return def;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return def; }
    }

    private void registerGrpcHandlers() {
        RpcRequestHandler dispatcher = (req, ctx) -> {
            String svc = req.getServiceName();
            RequestHandler h = localHandlers.get(svc);
            if (h == null) {
                return RpcResponse.error(req.getRequestId(), "no storage handler for: " + svc);
            }
            try {
                return h.handle(req);
            } catch (Exception e) {
                log.error("storage gRPC handler 异常 svc={}", svc, e);
                Map<String, String> errHeaders = new HashMap<>();
                errHeaders.put(H_ERROR, e.getMessage() != null ? e.getMessage() : "internal");
                return RpcResponse.newBuilder()
                        .requestId(req.getRequestId())
                        .status(RpcResponse.Status.ERROR)
                        .headers(errHeaders)
                        .build();
            }
        };
        rpcService.registerHandler(SVC_PUT, dispatcher);
        rpcService.registerHandler(SVC_REPLICATE, dispatcher);
        rpcService.registerHandler(SVC_GET, dispatcher);
        rpcService.registerHandler(SVC_HEAD, dispatcher);
        rpcService.registerHandler(SVC_DELETE, dispatcher);
        rpcService.registerHandler(SVC_REP_DELETE, dispatcher);
        rpcService.registerHandler(SVC_CREATE_BUCKET, dispatcher);
        rpcService.registerHandler(SVC_DELETE_BUCKET, dispatcher);
        rpcService.registerHandler(SVC_LIST, dispatcher);
        rpcService.registerHandler(SVC_PING, dispatcher);
        rpcService.registerHandler(SVC_TOPOLOGY, dispatcher);
        rpcService.registerHandler(SVC_MAINTENANCE, dispatcher);
        rpcService.registerHandler(SVC_FAILOVER, dispatcher);
        rpcService.registerHandler(SVC_LOCAL_DELETE, dispatcher);
        rpcService.registerHandler(SVC_REVERSE_RECONCILE, dispatcher);
        rpcService.registerHandler(SVC_CHUNK, dispatcher);
    }

    // ==================== 内部接口与类 ====================

    @FunctionalInterface
    public interface RequestHandler {
        RpcResponse handle(RpcRequest request);
    }

    static class GrpcRemoteObject implements ReadHandle {
        private final ObjectMetadata meta;
        private final InputStream stream;

        GrpcRemoteObject(ObjectMetadata meta, InputStream stream) {
            this.meta = meta;
            this.stream = stream;
        }

        @Override
        public ObjectMetadata metadata() { return meta; }

        @Override
        public InputStream openStream() { return stream; }
    }

    /**
     * 分块输入流：首块已在 init 响应中携带，后续按需拉取。
     */
    private class ChunkedInputStream extends InputStream {
        private final String address;
        private final String streamId;
        private final long totalSize;
        private ByteArrayInputStream current;
        private int nextSeq;
        private boolean closed = false;
        private long received = 0;
        private boolean endSignaled = false;

        ChunkedInputStream(String address, String streamId, byte[] firstChunk, long totalSize) {
            this.address = address;
            this.streamId = streamId;
            this.totalSize = totalSize;
            this.nextSeq = 1;
            if (firstChunk != null && firstChunk.length > 0) {
                this.current = new ByteArrayInputStream(firstChunk);
                this.received = firstChunk.length;
            }
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int n = read(b, 0, 1);
            return n == -1 ? -1 : b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) return -1;
            if (current == null || current.available() == 0) {
                if (endSignaled || (totalSize > 0 && received >= totalSize)) return -1;
                if (!fetchNextChunk()) return -1;
            }
            return current.read(b, off, len);
        }

        private boolean fetchNextChunk() {
            Map<String, String> headers = new HashMap<>();
            headers.put(H_CHUNK_STREAM_ID, streamId);
            headers.put(H_CHUNK_SEQ, String.valueOf(nextSeq));
            RpcRequest req = RpcRequest.newBuilder()
                    .requestId(UUID.randomUUID().toString())
                    .serviceName(SVC_CHUNK)
                    .headers(headers)
                    .build();
            RpcResponse resp = rpcService.send(address, req, defaultTimeoutMs);
            if (!resp.isSuccess()) {
                if ("end-of-stream".equals(resp.getHeaders().get(H_ERROR))) {
                    endSignaled = true;
                    return false;
                }
                throw new StorageException("fetchChunk 失败 seq=" + nextSeq + ": " + resp.getStatus());
            }
            byte[] payload = resp.getPayload();
            boolean isEnd = "true".equals(resp.getHeaders().get(H_CHUNK_IS_END));
            if (payload != null && payload.length > 0) {
                current = new ByteArrayInputStream(payload);
                received += payload.length;
            } else {
                current = null;
            }
            nextSeq++;
            if (isEnd) {
                endSignaled = true;
                return current != null;
            }
            return current != null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            current = null;
        }
    }
}
