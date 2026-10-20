package com.ww.runtime.server.web;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.StorageGateway;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * STORAGE 能力网关端点：任意节点可接收，按 placement 自动路由到分区属主
 * （本机属主直连本地引擎，否则经 gRPC 转发），客户端无需感知集群拓扑。
 *
 * <ul>
 *   <li>{@code PUT/GET/HEAD/DELETE /storage/{bucket}/{key...}} 对象读写删
 *       （写带 fencing：非属主 409；{@code x-amz-meta-*} 请求头作为自定义元数据）；</li>
 *   <li>{@code GET /storage/{bucket}?prefix=&marker=&maxKeys=} fan-out 列举
 *       （全存活节点合并，同 key 取最高 version）；</li>
 *   <li>{@code PUT /storage/{bucket}} 建桶 / {@code DELETE /storage/{bucket}?recursive=}
 *       删桶（本地生效 + SyncBucket 广播）；</li>
 *   <li>{@code GET /storage} 列桶（返回 [{name, createdAt}]；{@code ?stats=true}
 *       额外聚合 objectCount/totalSize）；</li>
 * </ul>
 *
 * <p>大文件读写全流式（不整体入堆）；Range 响应手动处理，不依赖
 * ResourceRegion（Spring Framework 7 已移除其默认转换器）。</p>
 */
@RestController
@RequestMapping("/storage")
public class StorageRestController {

    private static final String META_HEADER_PREFIX = "x-amz-meta-";

    private final StorageGateway gateway;

    public StorageRestController(StorageGateway gateway) {
        this.gateway = gateway;
    }

    // ------------------------------------------------------------------
    // 桶操作
    // ------------------------------------------------------------------

    /** 列桶：GET /storage。默认返回 [{name, createdAt}]；?stats=true 额外经 fan-out list 聚合对象数与总大小。 */
    @GetMapping
    public ResponseEntity<Object> listBuckets(
            @RequestParam(value = "stats", defaultValue = "false") boolean stats) {
        List<Map<String, Object>> buckets = new ArrayList<>();
        for (com.ww.runtime.data.storage.engine.BucketInfo info : gateway.bucketInfos()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", info.getName());
            item.put("createdAt", info.getCreatedAt());
            if (stats) {
                fillBucketStats(info.getName(), item);
            }
            buckets.add(item);
        }
        return ResponseEntity.ok(Map.of("buckets", buckets));
    }

    /** 聚合单桶统计：复用 fan-out list 分页循环（maxKeys=1000/页，上限 100 页防御大桶拖死管理面）。 */
    private void fillBucketStats(String bucket, Map<String, Object> item) {
        long count = 0L;
        long totalSize = 0L;
        String marker = null;
        boolean estimate = false;
        for (int page = 0; page < 100; page++) {
            StorageGateway.ListResult r = gateway.list(bucket, null, marker, 1000);
            for (ObjectMetadata obj : r.objects()) {
                count++;
                totalSize += obj.getSize();
            }
            if (!r.truncated() || r.nextMarker() == null) {
                break;
            }
            marker = r.nextMarker();
            if (page == 99) {
                estimate = true; // 超过 10 万对象：计数停止，标记为近似值
            }
        }
        item.put("objectCount", count);
        item.put("totalSize", totalSize);
        item.put("objectCountEstimate", estimate);
    }

    /** 建桶：PUT /storage/{bucket}（幂等；广播失败由 .bucket 对账兜底）。 */
    @PutMapping("/{bucket}")
    public ResponseEntity<Object> createBucket(@PathVariable("bucket") String bucket) {
        gateway.createBucket(bucket);
        return ResponseEntity.ok(Map.of("bucket", bucket, "created", true));
    }

    /** 删桶：DELETE /storage/{bucket}?recursive=true（默认非空桶返回 409）。 */
    @DeleteMapping("/{bucket}")
    public ResponseEntity<Object> deleteBucket(@PathVariable("bucket") String bucket,
                                               @RequestParam(value = "recursive", defaultValue = "false")
                                               boolean recursive) {
        try {
            gateway.deleteBucket(bucket, recursive);
        } catch (com.ww.runtime.data.storage.engine.StorageException e) {
            // BucketStore 删除非空桶（未递归）等本地拒绝 → 409
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", e.getMessage()));
        }
        return ResponseEntity.noContent().build();
    }

    /** 列举桶内对象（fan-out 全集群合并）：GET /storage/{bucket}?prefix=&marker=&maxKeys=。 */
    @GetMapping("/{bucket}")
    public ResponseEntity<Object> list(@PathVariable("bucket") String bucket,
                                       @RequestParam(value = "prefix", required = false) String prefix,
                                       @RequestParam(value = "marker", required = false) String marker,
                                       @RequestParam(value = "maxKeys", defaultValue = "1000") int maxKeys) {
        StorageGateway.ListResult result = gateway.list(bucket, prefix, marker, maxKeys);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bucket", bucket);
        body.put("objects", result.objects());
        body.put("nextMarker", result.nextMarker());
        body.put("truncated", result.truncated());
        return ResponseEntity.ok(body);
    }

    // ------------------------------------------------------------------
    // 对象操作（网关转发）
    // ------------------------------------------------------------------

    /** 上传对象：body 为对象内容，Content-Type 与 x-amz-meta-* 头作为元数据。 */
    @PutMapping("/{bucket}/**")
    public ResponseEntity<Object> put(@PathVariable("bucket") String bucket,
                                      @RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false) String contentType,
                                      HttpServletRequest request) throws IOException {
        String key = extractKey(request, bucket);
        ObjectMetadata meta = gateway.put(bucket, key, contentType,
                userMetaFrom(request), request.getInputStream(), request.getContentLengthLong());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bucket", bucket);
        body.put("key", key);
        body.put("etag", meta.getEtag());
        body.put("version", meta.getVersion());
        body.put("size", meta.getSize());
        return ResponseEntity.ok(body);
    }

    /**
     * 下载对象（流式；支持 Range: bytes=start-end 单段；属主节点回源）。
     *
     * <p>Range 请求下推到网关/属主定位（远端不再传输前缀后丢弃），
     * 支持浏览器媒体元素边下边播与拖动定位。</p>
     */
    @GetMapping("/{bucket}/**")
    public ResponseEntity<StreamingResponseBody> get(@PathVariable("bucket") String bucket,
                                                     @RequestHeader(value = HttpHeaders.RANGE, required = false) String range,
                                                     HttpServletRequest request) throws IOException {
        String key = extractKey(request, bucket);
        if (range == null || !range.startsWith("bytes=")) {
            StorageGateway.FetchedObject obj = gateway.get(bucket, key);
            if (obj == null) {
                return ResponseEntity.notFound().build();
            }
            ObjectMetadata meta = obj.meta();
            InputStream in = obj.in();
            StreamingResponseBody body = out -> writeStream(in, meta.getSize(), out);
            return withObjectHeaders(ResponseEntity.ok(), meta, meta.getSize()).body(body);
        }

        // Range 请求：先 HEAD 取总大小校验区间，再下推网关按窗口定位读
        ObjectMetadata head = gateway.head(bucket, key);
        if (head == null) {
            return ResponseEntity.notFound().build();
        }
        long total = head.getSize();

        // Range 解析（仅支持单段 bytes=start-end / bytes=start- / bytes=-N；多段取首段）
        long start = 0L;
        long end = total > 0 ? total - 1 : 0L;
        String spec = range.substring("bytes=".length()).split(",")[0].trim();
        String[] parts = spec.split("-", -1);
        if (!parts[0].isEmpty()) {
            start = Long.parseLong(parts[0]);
            end = parts.length > 1 && !parts[1].isEmpty() ? Long.parseLong(parts[1]) : total - 1;
        } else if (parts.length > 1 && !parts[1].isEmpty()) {
            // suffix: bytes=-N 最后 N 字节
            long suffix = Long.parseLong(parts[1]);
            start = Math.max(0L, total - suffix);
            end = total - 1;
        }
        if (start < 0 || end >= total || start > end) {
            return ResponseEntity.status(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                    .header(HttpHeaders.CONTENT_RANGE, "bytes */" + total).build();
        }

        long length = end - start + 1;
        StorageGateway.FetchedObject obj = gateway.get(bucket, key, start, length);
        if (obj == null) {
            return ResponseEntity.notFound().build(); // head 与 get 之间被并发删除/缩短
        }
        InputStream in = obj.in();
        StreamingResponseBody body = out -> writeStream(in, length, out);
        return withObjectHeaders(ResponseEntity.status(HttpStatus.PARTIAL_CONTENT), obj.meta(), length)
                .header(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + total)
                .body(body);
    }

    /** 流式写：64KB 缓冲、按 {@code length} 截断（流已由网关定位到窗口起点）。 */
    private static void writeStream(InputStream in, long length, java.io.OutputStream out) throws IOException {
        try (in) {
            byte[] buf = new byte[64 * 1024];
            long remaining = length;
            while (remaining > 0) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
                if (n < 0) {
                    break;
                }
                out.write(buf, 0, n);
                remaining -= n;
            }
            out.flush();
        }
    }

    /** 对象内容响应公共头：Content-Type / ETag / Accept-Ranges / Content-Length / x-amz-meta-*。 */
    private static ResponseEntity.BodyBuilder withObjectHeaders(ResponseEntity.BodyBuilder builder,
                                                                ObjectMetadata meta, long length) {
        builder.header(HttpHeaders.CONTENT_TYPE, meta.getContentType())
                .header(HttpHeaders.ETAG, "\"" + meta.getEtag() + "\"")
                .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(length));
        if (meta.getUserMetadata() != null && !meta.getUserMetadata().isEmpty()) {
            meta.getUserMetadata().forEach((name, value) ->
                    builder.header(META_HEADER_PREFIX + name, value));
        }
        return builder;
    }

    /** HEAD：仅元数据（x-amz-meta-* 回带）。 */
    @RequestMapping(value = "/{bucket}/**", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(@PathVariable("bucket") String bucket,
                                     HttpServletRequest request) {
        String key = extractKey(request, bucket);
        ObjectMetadata meta = gateway.head(bucket, key);
        if (meta == null) {
            return ResponseEntity.notFound().build();
        }
        ResponseEntity.HeadersBuilder<?> builder = ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(meta.getSize()))
                .header(HttpHeaders.CONTENT_TYPE, meta.getContentType())
                .header(HttpHeaders.ETAG, "\"" + meta.getEtag() + "\"")
                .header(HttpHeaders.ACCEPT_RANGES, "bytes");
        if (meta.getUserMetadata() != null && !meta.getUserMetadata().isEmpty()) {
            meta.getUserMetadata().forEach((name, value) ->
                    builder.header(META_HEADER_PREFIX + name, value));
        }
        return builder.build();
    }

    /** 删除对象（属主端 fencing，副本经复制器墓碑异步删除）。 */
    @DeleteMapping("/{bucket}/**")
    public ResponseEntity<Object> delete(@PathVariable("bucket") String bucket,
                                         HttpServletRequest request) {
        String key = extractKey(request, bucket);
        boolean deleted = gateway.delete(bucket, key);
        return deleted ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    // ------------------------------------------------------------------
    // 工具与异常映射
    // ------------------------------------------------------------------

    /** 从请求 URI 提取对象 key（/storage/{bucket}/ 之后的全部路径，含 '/'，百分号解码）。 */
    private String extractKey(HttpServletRequest request, String bucket) {
        String uri = request.getRequestURI();
        String prefix = "/storage/" + bucket + "/";
        if (!uri.startsWith(prefix) || uri.length() <= prefix.length()) {
            throw new RuntimeFrameworkException("object key missing in URI: " + uri);
        }
        return decodePath(uri.substring(prefix.length()));
    }

    /** 路径段百分号解码（不处理 '+'，与 query 解码语义区分）。 */
    private static String decodePath(String s) {
        if (s.indexOf('%') < 0) {
            return s;
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length()) {
                int hi = Character.digit(s.charAt(i + 1), 16);
                int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) {
                    out.write((hi << 4) | lo);
                    i += 2;
                    continue;
                }
            }
            if (c < 0x80) {
                out.write(c);
            } else {
                byte[] enc = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                out.write(enc, 0, enc.length);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /** 收集 x-amz-meta-* 请求头为自定义元数据（无则 null）。 */
    private static Map<String, String> userMetaFrom(HttpServletRequest request) {
        Map<String, String> meta = null;
        for (java.util.Enumeration<String> names = request.getHeaderNames(); names.hasMoreElements(); ) {
            String name = names.nextElement();
            String lower = name.toLowerCase(java.util.Locale.ROOT);
            if (lower.startsWith(META_HEADER_PREFIX)) {
                if (meta == null) {
                    meta = new LinkedHashMap<>();
                }
                meta.put(lower.substring(META_HEADER_PREFIX.length()), request.getHeader(name));
            }
        }
        return meta;
    }

    /** fencing 拒绝（非属主/纪元过期）→ 409。 */
    @ExceptionHandler(StaleEpochException.class)
    public ResponseEntity<Object> staleEpoch(StaleEpochException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("message", e.getMessage()));
    }

    /** 冻结/未启动 → 423；其余框架异常（含网关转发失败）→ 503。 */
    @ExceptionHandler(RuntimeFrameworkException.class)
    public ResponseEntity<Object> framework(RuntimeFrameworkException e) {
        HttpStatus status = "frozen".equals(e.getMessage())
                ? HttpStatus.LOCKED : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(Map.of("message", e.getMessage()));
    }
}
