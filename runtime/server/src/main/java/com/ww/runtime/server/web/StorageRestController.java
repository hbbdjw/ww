package com.ww.runtime.server.web;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.StorageCapability;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * STORAGE 能力数据面端点（本地直连）：
 * PUT/GET/HEAD/DELETE {@code /storage/{bucket}/{key}} 与列举 {@code /storage/{bucket}}。
 *
 * <p>写操作在 {@link StorageCapability} 内带 placement epoch fencing（非属主节点拒绝）；
 * 客户端先经 {@code /rt/placement/STORAGE/lookup} 查询属主再直连本端点。
 *
 * <p>大文件读写全流式（不整体入堆）；Range 响应手动处理，不依赖
 * ResourceRegion（Spring Framework 7 已移除其默认转换器）。
 */
@RestController
@RequestMapping("/storage")
public class StorageRestController {

    private final StorageCapability storage;

    public StorageRestController(StorageCapability storage) {
        this.storage = storage;
    }

    /** 上传对象：body 为对象内容，Content-Type 作为元数据。 */
    @PutMapping("/{bucket}/**")
    public ResponseEntity<Object> put(@PathVariable("bucket") String bucket,
                                      @RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false) String contentType,
                                      HttpServletRequest request) throws IOException {
        String key = extractKey(request, bucket);
        ObjectMetadata meta = storage.putObject(bucket, key, request.getInputStream(),
                request.getContentLengthLong(), contentType);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bucket", bucket);
        body.put("key", key);
        body.put("etag", meta.getEtag());
        body.put("version", meta.getVersion());
        body.put("size", meta.getSize());
        return ResponseEntity.ok(body);
    }

    /** 下载对象（流式；支持 Range: bytes=start-end 单段）。 */
    @GetMapping("/{bucket}/**")
    public ResponseEntity<StreamingResponseBody> get(@PathVariable("bucket") String bucket,
                                                     @RequestHeader(value = HttpHeaders.RANGE, required = false) String range,
                                                     HttpServletRequest request) throws IOException {
        String key = extractKey(request, bucket);
        ObjectStore.StoredObject obj = storage.getObject(bucket, key);
        if (obj == null) {
            return ResponseEntity.notFound().build();
        }
        ObjectMetadata meta = obj.meta;
        long total = meta.getSize();

        // Range 解析（仅支持单段 bytes=start-end / bytes=start-）
        long start = 0L;
        long end = total > 0 ? total - 1 : 0L;
        HttpStatus status = HttpStatus.OK;
        if (range != null && range.startsWith("bytes=")) {
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
            status = HttpStatus.PARTIAL_CONTENT;
        }

        final long skip = start;
        final long length = Math.max(0L, end - start + 1);
        InputStream in = storage.openRead(obj);
        StreamingResponseBody body = out -> {
            try (in) {
                long skipped = 0L;
                while (skipped < skip) {
                    long s = in.skip(skip - skipped);
                    if (s <= 0) {
                        break;
                    }
                    skipped += s;
                }
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
        };
        return ResponseEntity.status(status)
                .header(HttpHeaders.CONTENT_TYPE, meta.getContentType())
                .header(HttpHeaders.ETAG, "\"" + meta.getEtag() + "\"")
                .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(length))
                .header(HttpHeaders.CONTENT_RANGE,
                        status == HttpStatus.PARTIAL_CONTENT ? "bytes " + start + "-" + end + "/" + total : null)
                .body(body);
    }

    /** HEAD：仅元数据。 */
    @org.springframework.web.bind.annotation.RequestMapping(value = "/{bucket}/**",
            method = org.springframework.web.bind.annotation.RequestMethod.HEAD)
    public ResponseEntity<Void> head(@PathVariable("bucket") String bucket,
                                     HttpServletRequest request) {
        String key = extractKey(request, bucket);
        ObjectMetadata meta = storage.head(bucket, key);
        if (meta == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(meta.getSize()))
                .header(HttpHeaders.CONTENT_TYPE, meta.getContentType())
                .header(HttpHeaders.ETAG, "\"" + meta.getEtag() + "\"")
                .build();
    }

    /** 删除对象（带 fencing）。 */
    @DeleteMapping("/{bucket}/**")
    public ResponseEntity<Object> delete(@PathVariable("bucket") String bucket,
                                         HttpServletRequest request) {
        String key = extractKey(request, bucket);
        boolean deleted = storage.deleteObject(bucket, key);
        return deleted ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    /** 列举桶内对象：GET /storage/{bucket}?prefix=&marker=&maxKeys= */
    @GetMapping("/{bucket}")
    public ResponseEntity<Object> list(@PathVariable("bucket") String bucket,
                                       @RequestParam(value = "prefix", required = false) String prefix,
                                       @RequestParam(value = "marker", required = false) String marker,
                                       @RequestParam(value = "maxKeys", defaultValue = "1000") int maxKeys) {
        storage.registerBucket(bucket);
        ObjectStore.ListResult result = storage.list(bucket, prefix, marker, maxKeys);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bucket", bucket);
        body.put("objects", result.getObjects());
        body.put("nextMarker", result.getNextMarker());
        body.put("truncated", result.isTruncated());
        return ResponseEntity.ok(body);
    }

    /** 从请求 URI 提取对象 key（/storage/{bucket}/ 之后的全部路径，含 '/'）。 */
    private String extractKey(HttpServletRequest request, String bucket) {
        String uri = request.getRequestURI();
        String prefix = "/storage/" + bucket + "/";
        if (!uri.startsWith(prefix) || uri.length() <= prefix.length()) {
            throw new RuntimeFrameworkException("object key missing in URI: " + uri);
        }
        return uri.substring(prefix.length());
    }

    /** fencing 拒绝（非属主/纪元过期）→ 409。 */
    @ExceptionHandler(StaleEpochException.class)
    public ResponseEntity<Object> staleEpoch(StaleEpochException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("message", e.getMessage()));
    }

    /** 冻结/未启动 → 423。 */
    @ExceptionHandler(RuntimeFrameworkException.class)
    public ResponseEntity<Object> framework(RuntimeFrameworkException e) {
        HttpStatus status = "frozen".equals(e.getMessage())
                ? HttpStatus.LOCKED : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(Map.of("message", e.getMessage()));
    }
}
