package com.ww.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import tools.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 节点间 HTTP 通信客户端：用于主节点转发与副本复制。
 *
 * <p>基于 JDK {@link HttpClient}，支持请求体/响应体流式传输，避免大对象全量驻留内存。
 * 内部端点约定见 {@code InternalReplicationController}：
 * <ul>
 *   <li>PUT  /internal/storage/object?bucket=&amp;key=  —— 转发到主节点写（主写+复制）</li>
 *   <li>POST /internal/storage/replicate?bucket=&amp;key= —— 主→副本复制</li>
 *   <li>GET  /internal/storage/object?bucket=&amp;key=  —— 转发读</li>
 *   <li>HEAD /internal/storage/object?bucket=&amp;key=  —— 转发 head</li>
 *   <li>DELETE /internal/storage/object?bucket=&amp;key=  —— 转发删除</li>
 * </ul>
 * 元数据通过响应/请求头 x-obj-* 传递。
 */
public class StorageNodeClient implements StorageRpcClient {

    private static final Logger log = LoggerFactory.getLogger(StorageNodeClient.class);

    private final HttpClient http;
    private final String authToken;
    private final ObjectMapper objectMapper;

    public StorageNodeClient(String authToken, ObjectMapper objectMapper) {
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.authToken = authToken;
        this.objectMapper = objectMapper;
    }

    // ---------------- 转发写（到主） ----------------

    /** 转发写请求到主节点，返回主节点计算后的元数据（ETag/version 等） */
    public ObjectMetadata forwardPut(StorageNode primary, String bucket, String key,
                                     Supplier<InputStream> body, long length,
                                     String contentType, Map<String, String> userMeta) {
        String url = endpoint(primary, "/internal/storage/object", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(10))
                .PUT(HttpRequest.BodyPublishers.ofInputStream(body));
        if (contentType != null) b.header("Content-Type", contentType);
        applyAuth(b);
        if (userMeta != null) {
            for (Map.Entry<String, String> e : userMeta.entrySet()) {
                b.header("x-obj-meta-" + e.getKey(), encodeMetaValue(e.getValue()));
            }
        }
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                throw new StorageException("主节点写失败: " + resp.statusCode() + " @ " + primary.nodeKey());
            }
            return readMetaFromHeaders(resp.headers());
        } catch (IOException e) {
            throw new StorageException("转发写 IO 异常: " + primary.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("转发写被中断: " + primary.nodeKey(), e);
        }
    }

    // ---------------- 复制（主→副本） ----------------

    /** 将主节点本地对象数据复制到副本节点（流式） */
    public void replicatePut(StorageNode replica, String bucket, String key,
                             Supplier<InputStream> body, long length, ObjectMetadata meta) {
        String url = endpoint(replica, "/internal/storage/replicate", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(10))
                .POST(HttpRequest.BodyPublishers.ofInputStream(body));
        writeMetaHeaders(b, meta);
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                throw new StorageException("副本复制失败: " + resp.statusCode() + " @ " + replica.nodeKey());
            }
        } catch (IOException e) {
            throw new StorageException("复制 IO 异常: " + replica.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("复制被中断: " + replica.nodeKey(), e);
        }
    }

    /**
     * 通知副本删除对象。
     * <p>使用专用端点 {@code DELETE /internal/storage/replicate}，副本端仅本地删除，
     * 不触发再复制/转发，避免循环调用。失败时抛出异常供调用方计数和入重试队列。
     */
    public void replicateDelete(StorageNode replica, String bucket, String key) {
        String url = replica.baseUrl() + "/internal/storage/replicate?bucket="
                + URLEncoder.encode(bucket, StandardCharsets.UTF_8)
                + "&key=" + URLEncoder.encode(key, StandardCharsets.UTF_8);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .DELETE();
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2 && resp.statusCode() != 404) {
                throw new StorageException("副本删除失败: " + resp.statusCode() + " @ " + replica.nodeKey());
            }
        } catch (IOException e) {
            throw new StorageException("副本删除 IO 异常: " + replica.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("副本删除被中断: " + replica.nodeKey(), e);
        }
    }

    // ---------------- 转发读 ----------------

    /** 转发读请求到主节点，返回流式句柄（调用方必须 close） */
    public RemoteObject forwardGet(StorageNode primary, String bucket, String key) {
        String url = endpoint(primary, "/internal/storage/object", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(10))
                .GET();
        applyAuth(b);
        try {
            HttpResponse<InputStream> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            if (resp.statusCode() == 404) {
                closeQuietly(resp.body());
                return null;
            }
            if (resp.statusCode() / 100 != 2) {
                closeQuietly(resp.body());
                throw new StorageException("主节点读失败: " + resp.statusCode() + " @ " + primary.nodeKey());
            }
            ObjectMetadata meta = readMetaFromHeaders(resp.headers());
            if (meta == null) {
                closeQuietly(resp.body());
                throw new StorageException("主节点读响应缺少元数据头，无法构造读取句柄: "
                        + primary.nodeKey());
            }
            return new RemoteObject(meta, resp.body());
        } catch (IOException e) {
            throw new StorageException("转发读 IO 异常: " + primary.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("转发读被中断: " + primary.nodeKey(), e);
        }
    }

    /** 转发 head（仅元数据） */
    public ObjectMetadata forwardHead(StorageNode primary, String bucket, String key) {
        String url = endpoint(primary, "/internal/storage/object", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .method("HEAD", HttpRequest.BodyPublishers.noBody());
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() == 404) return null;
            if (resp.statusCode() / 100 != 2) {
                throw new StorageException("主节点 head 失败: " + resp.statusCode() + " @ " + primary.nodeKey());
            }
            return readMetaFromHeaders(resp.headers());
        } catch (IOException e) {
            throw new StorageException("转发 head IO 异常: " + primary.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("转发 head 被中断: " + primary.nodeKey(), e);
        }
    }

    /** 转发删除（主节点本地删除 + 通知副本删除，快速响应） */
    public void forwardDelete(StorageNode primary, String bucket, String key) {
        String url = endpoint(primary, "/internal/storage/object", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .DELETE();
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2 && resp.statusCode() != 404) {
                throw new StorageException("主节点删除失败: " + resp.statusCode() + " @ " + primary.nodeKey());
            }
        } catch (IOException e) {
            throw new StorageException("转发删除 IO 异常: " + primary.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("转发删除被中断: " + primary.nodeKey(), e);
        }
    }

    // ---------------- Bucket 广播与 list 合并 ----------------

    /** 通知对端节点创建 Bucket（本地写） */
    public void forwardCreateBucket(StorageNode node, String name) {
        String url = node.baseUrl() + "/internal/storage/bucket?name="
                + URLEncoder.encode(name, StandardCharsets.UTF_8);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .PUT(HttpRequest.BodyPublishers.noBody());
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                log.warn("对端创建 Bucket 非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
            }
        } catch (Exception e) {
            log.warn("对端创建 Bucket 异常 @ {}", node.nodeKey(), e);
        }
    }

    // ---------------- 拓扑与维护态广播（扩缩容） ----------------

    /**
     * 向对端节点广播新拓扑：对端用收到的节点列表重建本地 NodeRouter。
     * <p>对端会根据自身 host:httpPort 重新标记 self，因此消息不含 self 标志。
     * @param node 目标节点
     * @param topologyPayload JSON 编码的节点列表 [{host, httpPort, weight}, ...]
     * @param replicas 副本数
     * @return 是否成功（2xx）
     */
    public boolean broadcastTopology(StorageNode node, String topologyPayload, int replicas) {
        String url = node.baseUrl() + "/internal/storage/topology?replicas=" + replicas;
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(topologyPayload));
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                log.warn("广播拓扑非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("广播拓扑异常 @ {}", node.nodeKey(), e);
            return false;
        }
    }

    /** 向对端广播维护态切换 */
    public boolean broadcastMaintenance(StorageNode node, boolean enabled) {
        String url = node.baseUrl() + "/internal/storage/maintenance?enabled=" + enabled;
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .POST(HttpRequest.BodyPublishers.noBody());
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                log.warn("广播维护态非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("广播维护态异常 @ {}", node.nodeKey(), e);
            return false;
        }
    }

    /**
     * 通知对端节点本地删除对象（不触发复制）：用于迁移完成后清理源端数据。
     */
    public boolean localDelete(StorageNode node, String bucket, String key) {
        String url = endpoint(node, "/internal/storage/object/local", bucket, key);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .DELETE();
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2 && resp.statusCode() != 404) {
                log.warn("本地删除非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("本地删除异常 @ {}", node.nodeKey(), e);
            return false;
        }
    }

    // ---------------- failover 心跳与事件广播 ----------------

    /**
     * 心跳探测对端节点是否存活。
     * @param node 目标节点
     * @param timeoutMs 超时（ms）
     * @return true=存活（2xx），false=不可达/超时
     */
    public boolean ping(StorageNode node, long timeoutMs) {
        String url = node.baseUrl() + "/internal/storage/ping";
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(Math.max(500, timeoutMs)))
                .GET();
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            return resp.statusCode() / 100 == 2;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 向对端广播 failover 事件：对端更新本地 nodeStatus + failoverEpoch + 重建路由。
     * @param node 目标节点
     * @param failedNodeKey 发生 failover 的节点 key
     * @param statusName 节点新状态（DEAD/QUARANTINE/ALIVE）
     * @param epoch failover epoch（对端用于丢弃旧广播）
     * @return 是否成功（2xx）
     */
    public boolean broadcastFailover(StorageNode node, String failedNodeKey, String statusName, long epoch) {
        String url = node.baseUrl() + "/internal/storage/failover?nodeKey="
                + URLEncoder.encode(failedNodeKey, StandardCharsets.UTF_8)
                + "&status=" + statusName + "&epoch=" + epoch;
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .POST(HttpRequest.BodyPublishers.noBody());
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                log.warn("广播 failover 非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("广播 failover 异常 @ {}", node.nodeKey(), e);
            return false;
        }
    }

    /**
     * 触发原主节点执行反向同步（DEAD 恢复后从新 primary 拉取落后数据）。
     * <p>原主收到此请求后执行 {@link DistributedStorage#reverseReconcile}，遍历本地对象，
     * 对 primary 已漂移的对象从新 primary 补齐数据。
     * @param node 目标节点（恢复的原主）
     * @return 是否成功（2xx）
     */
    public boolean triggerReverseReconcile(StorageNode node) {
        String url = node.baseUrl() + "/internal/storage/reverse-reconcile";
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(10))
                .POST(HttpRequest.BodyPublishers.noBody());
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2) {
                log.warn("触发反向同步非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("触发反向同步异常 @ {}", node.nodeKey(), e);
            return false;
        }
    }

    /** 通知对端节点删除 Bucket（本地删） */
    public void forwardDeleteBucket(StorageNode node, String name, boolean recursive) {
        String url = node.baseUrl() + "/internal/storage/bucket?name="
                + URLEncoder.encode(name, StandardCharsets.UTF_8) + "&recursive=" + recursive;
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .DELETE();
        applyAuth(b);
        try {
            HttpResponse<Void> resp = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
            if (resp.statusCode() / 100 != 2 && resp.statusCode() != 404) {
                log.warn("对端删除 Bucket 非 2xx: {} @ {}", resp.statusCode(), node.nodeKey());
            }
        } catch (Exception e) {
            log.warn("对端删除 Bucket 异常 @ {}", node.nodeKey(), e);
        }
    }

    /** 从对端节点拉取 Bucket 内全部对象元数据（用于分布式 list 合并） */
    public ObjectMetadata[] forwardListObjects(StorageNode node, String bucket) {
        String url = node.baseUrl() + "/internal/storage/objects?bucket="
                + URLEncoder.encode(bucket, StandardCharsets.UTF_8);
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET();
        applyAuth(b);
        try {
            HttpResponse<byte[]> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 404) return new ObjectMetadata[0];
            if (resp.statusCode() / 100 != 2) {
                throw new StorageException("对端 list 失败: " + resp.statusCode() + " @ " + node.nodeKey());
            }
            byte[] body = resp.body();
            if (body == null || body.length == 0) return new ObjectMetadata[0];
            return objectMapper.readValue(body, ObjectMetadata[].class);
        } catch (IOException e) {
            throw new StorageException("对端 list IO 异常 @ " + node.nodeKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("对端 list 被中断 @ " + node.nodeKey(), e);
        }
    }

    // ---------------- 内部工具 ----------------

    private void applyAuth(HttpRequest.Builder b) {
        if (authToken != null && !authToken.isEmpty()) {
            b.header("X-Storage-Token", authToken);
        }
    }

    private void writeMetaHeaders(HttpRequest.Builder b, ObjectMetadata meta) {
        if (meta == null) return;
        if (meta.getKey() != null) b.header("x-obj-key", encodeMetaValue(meta.getKey()));
        if (meta.getContentType() != null) b.header("x-obj-content-type", meta.getContentType());
        if (meta.getEtag() != null) b.header("x-obj-etag", meta.getEtag());
        b.header("x-obj-size", String.valueOf(meta.getSize()));
        b.header("x-obj-created", String.valueOf(meta.getCreatedAt()));
        b.header("x-obj-version", String.valueOf(meta.getVersion()));
        if (meta.getUserMetadata() != null) {
            for (Map.Entry<String, String> e : meta.getUserMetadata().entrySet()) {
                b.header("x-obj-meta-" + e.getKey(), encodeMetaValue(e.getValue()));
            }
        }
    }

    private ObjectMetadata readMetaFromHeaders(java.net.http.HttpHeaders headers) {
        // 优先 x-obj-etag（内部约定），回退标准 ETag 头以增强兼容性
        String etag = firstHeader(headers, "x-obj-etag");
        if (etag == null) etag = firstHeader(headers, "etag");
        if (etag == null) return null;
        ObjectMetadata m = new ObjectMetadata();
        m.setEtag(etag);
        m.setContentType(firstHeader(headers, "x-obj-content-type"));
        m.setSize(parseLong(firstHeader(headers, "x-obj-size"), -1));
        m.setCreatedAt(parseLong(firstHeader(headers, "x-obj-created"), 0));
        m.setVersion(parseLong(firstHeader(headers, "x-obj-version"), 0));
        String key = firstHeader(headers, "x-obj-key");
        if (key != null) m.setKey(decodeMetaValue(key));
        Map<String, String> um = new HashMap<>();
        for (Map.Entry<String, java.util.List<String>> e : headers.map().entrySet()) {
            String name = e.getKey();
            if (name != null && name.toLowerCase().startsWith("x-obj-meta-")) {
                String mk = name.substring("x-obj-meta-".length());
                if (!e.getValue().isEmpty()) {
                    um.put(mk, decodeMetaValue(e.getValue().getFirst()));
                }
            }
        }
        m.setUserMetadata(um);
        return m;
    }

    private static String firstHeader(java.net.http.HttpHeaders h, String name) {
        return h.firstValue(name).orElse(null);
    }

    private static long parseLong(String s, long def) {
        if (s == null || s.isEmpty()) return def;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return def; }
    }

    private static String encodeMetaValue(String v) {
        if (v == null) return "";
        // 非 ASCII 或含特殊字符进行 URL 编码，避免 HTTP 头解析问题
        boolean needEncode = false;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c > 127 || c < 32 || c == ':' || c == '\r' || c == '\n') { needEncode = true; break; }
        }
        return needEncode ? "U=" + URLEncoder.encode(v, StandardCharsets.UTF_8) : v;
    }

    private static String decodeMetaValue(String v) {
        if (v == null) return null;
        if (v.startsWith("U=")) {
            return URLDecoder.decode(v.substring(2), StandardCharsets.UTF_8);
        }
        return v;
    }

    private static String endpoint(StorageNode node, String path, String bucket, String key) {
        return node.baseUrl() + path + "?bucket=" + URLEncoder.encode(bucket, StandardCharsets.UTF_8)
                + "&key=" + URLEncoder.encode(key, StandardCharsets.UTF_8);
    }

    private static void closeQuietly(InputStream in) {
        if (in != null) {
            try { in.close(); } catch (IOException ignored) { }
        }
    }

    /** 远端读结果：元数据 + 流（调用方负责 close 以释放连接） */
    public static class RemoteObject implements ReadHandle {
        private final ObjectMetadata meta;
        private final InputStream stream;

        RemoteObject(ObjectMetadata meta, InputStream stream) {
            this.meta = meta;
            this.stream = stream;
        }

        @Override
        public ObjectMetadata metadata() { return meta; }

        @Override
        public InputStream openStream() { return stream; }
    }
}
