package com.ww.admin.controller;

import java.io.InputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ww.storage.DistributedStorage;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import com.ww.storage.ReadHandle;
import com.ww.storage.StorageException;
import com.ww.storage.StorageNode;
import com.ww.storage.StorageProperties;
import com.ww.storage.StorageTopologyManager;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * 节点间内部通信端点（挂载于 /internal/storage）。
 *
 * <p>仅由 {@code StorageNodeClient} 调用，不对外暴露给最终用户。承担：
 * <ul>
 *   <li>主节点转发写/读/删（非主节点 → 主节点）</li>
 *   <li>主→副本对象复制与删除</li>
 *   <li>Bucket 广播创建/删除</li>
 *   <li>分布式 list 拉取本地对象元数据</li>
 * </ul>
 * 元数据通过 x-obj-* 头传递。若配置 storage.authToken 则校验 X-Storage-Token。
 */
@Controller
@RequestMapping("/internal/storage")
public class InternalReplicationController {

    private static final Logger log = LoggerFactory.getLogger(InternalReplicationController.class);

    @Autowired
    private DistributedStorage storage;

    @Autowired
    private ObjectStore objectStore;

    @Autowired
    private StorageProperties storageProps;

    @Autowired(required = false)
    private StorageTopologyManager topologyManager;

    // ---------------- 对象：转发到主（主本地写+复制） ----------------

    @PutMapping("/object")
    public ResponseEntity<Void> forwardPut(@RequestParam String bucket,
                                           @RequestParam String key,
                                           HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            String contentType = request.getContentType();
            long length = request.getContentLengthLong();
            Map<String, String> userMeta = extractUserMeta(request);
            try (InputStream in = request.getInputStream()) {
                ObjectMetadata meta = storage.putObject(bucket, key, in, length, contentType, userMeta);
                return ResponseEntity.ok().headers(metaHeaders(meta)).build();
            }
        } catch (StorageException e) {
            log.warn("内部 put 失败: bucket={} key={}: {}", bucket, key, e.getMessage());
            return ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            log.error("内部 put 异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // ---------------- 对象：副本复制 ----------------

    @PostMapping("/replicate")
    public ResponseEntity<Void> replicate(@RequestParam String bucket,
                                          @RequestParam String key,
                                          HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            ObjectMetadata meta = readMetaFromHeaders(request);
            if (meta == null) {
                return ResponseEntity.badRequest().build();
            }
            long length = request.getContentLengthLong();
            try (InputStream in = request.getInputStream()) {
                storage.receiveReplica(bucket, key, in, length, meta);
            }
            return ResponseEntity.noContent().build();
        } catch (StorageException e) {
            log.warn("副本复制失败: bucket={} key={}: {}", bucket, key, e.getMessage());
            return ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            log.error("副本复制异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 副本节点接收主节点删除通知：仅本地删除，不触发再复制、不转发，避免循环。
     */
    @DeleteMapping("/replicate")
    public ResponseEntity<Void> replicateDelete(@RequestParam String bucket,
                                                 @RequestParam String key,
                                                 HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            storage.receiveReplicaDelete(bucket, key);
            return ResponseEntity.noContent().build();
        } catch (StorageException e) {
            log.warn("副本删除失败: bucket={} key={}: {}", bucket, key, e.getMessage());
            return ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            log.error("副本删除异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // ---------------- 对象：转发读 ----------------

    @GetMapping("/object")
    public ResponseEntity<?> forwardGet(@RequestParam String bucket,
                                        @RequestParam String key,
                                        HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        ReadHandle handle = storage.getObject(bucket, key);
        if (handle == null) return ResponseEntity.notFound().build();
        ObjectMetadata meta = handle.metadata();
        HttpHeaders headers = metaHeaders(meta);
        if (meta.getSize() >= 0) headers.setContentLength(meta.getSize());
        MediaType ct = MediaType.parseMediaType(
                meta.getContentType() == null ? "application/octet-stream" : meta.getContentType());
        // 本地读：handle.file() 非 null 时用 FileSystemResource，触发 Tomcat sendfile 零拷贝响应
        java.io.File localFile = handle.file();
        if (localFile != null && localFile.exists() && localFile.canRead()) {
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(ct)
                    .body(new org.springframework.core.io.FileSystemResource(localFile));
        }
        try {
            InputStream in = handle.openStream();
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(ct)
                    .body(new InputStreamResource(in));
        } catch (Exception e) {
            log.error("内部 get 异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @org.springframework.web.bind.annotation.RequestMapping(value = "/object", method = org.springframework.web.bind.annotation.RequestMethod.HEAD)
    public ResponseEntity<Void> forwardHead(@RequestParam String bucket,
                                            @RequestParam String key,
                                            HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        ObjectMetadata meta = storage.headObject(bucket, key);
        if (meta == null) return ResponseEntity.notFound().build();
        HttpHeaders headers = metaHeaders(meta);
        if (meta.getSize() >= 0) headers.setContentLength(meta.getSize());
        return ResponseEntity.ok().headers(headers).build();
    }

    @DeleteMapping("/object")
    public ResponseEntity<Void> forwardDelete(@RequestParam String bucket,
                                              @RequestParam String key,
                                              HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            storage.deleteObject(bucket, key);
            return ResponseEntity.noContent().build();
        } catch (StorageException e) {
            log.warn("内部 delete 失败: bucket={} key={}: {}", bucket, key, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    // ---------------- Bucket 广播 ----------------

    @PutMapping("/bucket")
    @ResponseBody
    public ResponseEntity<Void> createBucketLocal(@RequestParam String name,
                                                  HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            storage.localCreateBucket(name);
            return ResponseEntity.ok().build();
        } catch (StorageException e) {
            log.warn("内部 createBucket 失败: {}: {}", name, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/bucket")
    public ResponseEntity<Void> deleteBucketLocal(@RequestParam String name,
                                                  @RequestParam(defaultValue = "false") boolean recursive,
                                                  HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            storage.localDeleteBucket(name, recursive);
            return ResponseEntity.noContent().build();
        } catch (StorageException e) {
            log.warn("内部 deleteBucket 失败: {}: {}", name, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    // ---------------- list 拉取（本地对象元数据） ----------------

    @GetMapping("/objects")
    @ResponseBody
    public List<ObjectMetadata> listLocalObjects(@RequestParam String bucket,
                                                 HttpServletRequest request) {
        if (!checkAuth(request)) return List.of();
        // 全量返回不截断（Integer.MAX_VALUE 仅作无上限标记）：截断会导致大桶对象在合并时静默缺失，
        // 且 nextMarker 只从已合并子集产生，被截断的尾部无法通过分页到达。全局分页由发起方负责。
        ObjectStore.ListResult r = objectStore.list(bucket, null, null, Integer.MAX_VALUE);
        return r.getObjects();
    }

    // ---------------- 拓扑与维护态广播（扩缩容） ----------------

    /**
     * 接收对端广播的新拓扑：用收到的节点列表重建本地 NodeRouter。
     * <p>请求体为 JSON 节点列表 [{host, httpPort, weight}, ...]，不含 self 标志
     * （本节点按自身 host:httpPort 重新标记）。
     */
    @PostMapping("/topology")
    public ResponseEntity<Void> applyTopology(@RequestParam(defaultValue = "2") int replicas,
                                              HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        if (topologyManager == null) {
            return ResponseEntity.status(503).build();
        }
        try {
            String body = new String(request.getInputStream().readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
            List<StorageNode> nodes = topologyManager.decodeTopology(body);
            topologyManager.applyRemoteTopology(nodes, replicas);
            // 拓扑变更后刷新 DistributedStorage 的 nodeByKey 索引
            storage.refreshNodeIndex();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.warn("应用远端拓扑失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 接收对端广播的维护态切换：设置本节点维护态标志。
     * <p>维护态下客户端读写被拒绝，内部复制与迁移端点不受影响。
     */
    @PostMapping("/maintenance")
    public ResponseEntity<Void> setMaintenance(@RequestParam boolean enabled,
                                                HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        storage.setMaintenance(enabled);
        log.info("远端设置维护态: enabled={}", enabled);
        return ResponseEntity.ok().build();
    }

    /**
     * 本地删除对象（不触发复制）：供迁移完成后清理源端数据使用。
     * <p>与 {@link #forwardDelete} 不同，本端点直接调用 {@link ObjectStore#delete}，
     * 不通知副本节点（数据已迁移到新主，旧副本清理由反熵对账兜底）。
     */
    @DeleteMapping("/object/local")
    public ResponseEntity<Void> localDelete(@RequestParam String bucket,
                                             @RequestParam String key,
                                             HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            objectStore.delete(bucket, key);
            return ResponseEntity.noContent().build();
        } catch (Exception e) {
            log.warn("本地删除失败: bucket={} key={}: {}", bucket, key, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    // ---------------- failover 心跳与事件接收 ----------------

    /** 心跳探测端点：对端定期 GET 探活，2xx 即存活 */
    @GetMapping("/ping")
    public ResponseEntity<Void> ping(HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        return ResponseEntity.ok().build();
    }

    /**
     * 接收对端广播的 failover 事件：更新本地 nodeStatus + failoverEpoch + 重建路由。
     * @param nodeKey 发生 failover 的节点
     * @param status 新状态（DEAD/QUARANTINE/ALIVE）
     * @param epoch failover epoch
     */
    @PostMapping("/failover")
    public ResponseEntity<Void> applyFailover(@RequestParam String nodeKey,
                                               @RequestParam String status,
                                               @RequestParam(defaultValue = "0") long epoch,
                                               HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        if (topologyManager == null) {
            return ResponseEntity.status(503).build();
        }
        try {
            topologyManager.applyRemoteFailover(nodeKey, status, epoch);
            // 路由变更后刷新 DistributedStorage 的 nodeByKey 索引
            storage.refreshNodeIndex();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.warn("应用远端 failover 失败: {} {} epoch={}: {}", nodeKey, status, epoch, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 触发本节点执行反向同步（DEAD 恢复后从新 primary 拉取落后数据）。
     * <p>由 FailoverManager 在原主心跳恢复后调用，原主遍历本地对象，
     * 对 primary 已漂移的对象从新 primary 补齐数据。
     */
    @PostMapping("/reverse-reconcile")
    public ResponseEntity<Void> reverseReconcile(HttpServletRequest request) {
        if (!checkAuth(request)) return ResponseEntity.status(401).build();
        try {
            int diff = storage.reverseReconcile();
            log.info("反向同步完成: 补齐对象数={}", diff);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.warn("反向同步失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    // ==================== 工具 ====================

    private boolean checkAuth(HttpServletRequest request) {
        String token = storageProps.getAuthToken();
        if (token == null || token.isEmpty()) return true;
        return token.equals(request.getHeader("X-Storage-Token"));
    }

    private Map<String, String> extractUserMeta(HttpServletRequest request) {
        Map<String, String> map = new HashMap<>();
        Enumeration<String> names = request.getHeaderNames();
        if (names == null) return map;
        while (names.hasMoreElements()) {
            String h = names.nextElement();
            if (h != null && h.toLowerCase().startsWith("x-amz-meta-")) {
                map.put(h.substring("x-amz-meta-".length()), request.getHeader(h));
            }
        }
        return map;
    }

    private HttpHeaders metaHeaders(ObjectMetadata meta) {
        HttpHeaders headers = new HttpHeaders();
        // 使用 x-obj-* 命名约定，与 StorageNodeClient.readMetaFromHeaders / writeMetaHeaders 对齐
        if (meta.getEtag() != null) headers.set("x-obj-etag", meta.getEtag());
        if (meta.getContentType() != null) headers.set("x-obj-content-type", meta.getContentType());
        if (meta.getKey() != null) headers.set("x-obj-key", encodeHeaderValue(meta.getKey()));
        headers.set("x-obj-size", String.valueOf(meta.getSize()));
        headers.set("x-obj-created", String.valueOf(meta.getCreatedAt()));
        headers.set("x-obj-version", String.valueOf(meta.getVersion()));
        if (meta.getUserMetadata() != null) {
            for (Map.Entry<String, String> e : meta.getUserMetadata().entrySet()) {
                headers.set("x-obj-meta-" + e.getKey(), encodeHeaderValue(e.getValue()));
            }
        }
        return headers;
    }

    private ObjectMetadata readMetaFromHeaders(HttpServletRequest request) {
        String etag = request.getHeader("x-obj-etag");
        if (etag == null) return null;
        ObjectMetadata m = new ObjectMetadata();
        m.setEtag(etag);
        m.setContentType(request.getHeader("x-obj-content-type"));
        m.setSize(parseLong(request.getHeader("x-obj-size"), -1));
        m.setCreatedAt(parseLong(request.getHeader("x-obj-created"), 0));
        m.setVersion(parseLong(request.getHeader("x-obj-version"), 0));
        String key = decodeHeaderValue(request.getHeader("x-obj-key"));
        if (key != null) m.setKey(key);
        Map<String, String> um = new HashMap<>();
        Enumeration<String> names = request.getHeaderNames();
        if (names != null) {
            while (names.hasMoreElements()) {
                String h = names.nextElement();
                if (h != null && h.toLowerCase().startsWith("x-obj-meta-")) {
                    um.put(h.substring("x-obj-meta-".length()), decodeHeaderValue(request.getHeader(h)));
                }
            }
        }
        m.setUserMetadata(um);
        return m;
    }

    /** 对 HTTP 头值做编码：非 ASCII 字符加 "U=" 前缀并 URL 编码，与 StorageNodeClient.encodeMetaValue 保持一致 */
    private static String encodeHeaderValue(String v) {
        if (v == null) return "";
        boolean needEncode = false;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c > 127 || c < 32 || c == ':' || c == '\r' || c == '\n') { needEncode = true; break; }
        }
        if (!needEncode) return v;
        return "U=" + URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    /** 解码 HTTP 头值：先做 ISO-8859-1→UTF-8 转换，再处理 "U=" 前缀 URL 解码 */
    private static String decodeHeaderValue(String v) {
        if (v == null) return null;
        // Tomcat 默认 ISO-8859-1 读取 header，先转回 UTF-8
        String utf8 = new String(v.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        if (utf8.startsWith("U=")) {
            return URLDecoder.decode(utf8.substring(2), StandardCharsets.UTF_8);
        }
        return utf8;
    }

    private static long parseLong(String s, long def) {
        if (s == null || s.isEmpty()) return def;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return def; }
    }
}
