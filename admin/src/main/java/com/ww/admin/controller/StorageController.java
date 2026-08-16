package com.ww.admin.controller;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ww.admin.common.JsonResult;
import com.ww.storage.BucketInfo;
import com.ww.storage.DistributedStorage;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import com.ww.storage.ReadHandle;
import com.ww.storage.StorageException;

import jakarta.servlet.http.HttpServletRequest;

/**
 * S3 协议兼容的对象存储 HTTP API（挂载于 /storage）。
 *
 * <p>端点与 S3 语义对齐：
 * <ul>
 *   <li>PUT    /storage/buckets/{bucket}                    —— 创建 Bucket</li>
 *   <li>DELETE /storage/buckets/{bucket}?recursive=true     —— 删除 Bucket</li>
 *   <li>GET    /storage/buckets                             —— 列举 Bucket</li>
 *   <li>GET    /storage/buckets/{bucket}                    —— 获取 Bucket 元信息</li>
 *   <li>PUT    /storage/buckets/{bucket}/objects/**         —— 上传对象（流式）</li>
 *   <li>GET    /storage/buckets/{bucket}/objects/**         —— 下载对象（流式）</li>
 *   <li>HEAD   /storage/buckets/{bucket}/objects/**         —— 获取对象元信息</li>
 *   <li>DELETE /storage/buckets/{bucket}/objects/**         —— 删除对象</li>
 *   <li>GET    /storage/buckets/{bucket}/objects?prefix=&marker=&maxKeys= —— 列举对象</li>
 * </ul>
 * 自定义元数据通过 x-amz-meta-* 请求/响应头透传。
 */
@Controller
@RequestMapping("/storage")
public class StorageController {

    private static final Logger log = LoggerFactory.getLogger(StorageController.class);
    private static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.RFC_1123_DATE_TIME;

    @Autowired
    private DistributedStorage storage;

    /** 维护态校验：客户端入口拒绝读写，保证扩缩容迁移期间 read-after-write 一致性 */
    private void ensureNotMaintenance() {
        if (storage.isMaintenance()) {
            throw new StorageException("存储处于维护态（扩缩容迁移中），暂不可读写，请稍后再试");
        }
    }

    // ==================== Bucket ====================

    @PutMapping("/buckets/{bucket}")
    @ResponseBody
    public JsonResult createBucket(@PathVariable String bucket) {
        try {
            ensureNotMaintenance();
            BucketInfo info = storage.createBucket(bucket);
            return JsonResult.success(toBucketView(info));
        } catch (StorageException e) {
            log.warn("创建 Bucket 失败: {}", bucket, e);
            return JsonResult.error(e.getMessage());
        }
    }

    @DeleteMapping("/buckets/{bucket}")
    @ResponseBody
    public JsonResult deleteBucket(@PathVariable String bucket,
                                  @RequestParam(defaultValue = "false") boolean recursive) {
        try {
            ensureNotMaintenance();
            storage.deleteBucket(bucket, recursive);
            return JsonResult.success("已删除");
        } catch (StorageException e) {
            log.warn("删除 Bucket 失败: {}", bucket, e);
            return JsonResult.error(e.getMessage());
        }
    }

    @GetMapping("/buckets")
    @ResponseBody
    public JsonResult listBuckets() {
        ensureNotMaintenance();
        List<BucketInfo> list = storage.listBuckets();
        List<Object> views = new ArrayList<>();
        for (BucketInfo b : list) views.add(toBucketView(b));
        return JsonResult.success(views);
    }

    @GetMapping("/buckets/{bucket}")
    @ResponseBody
    public JsonResult headBucket(@PathVariable String bucket) {
        ensureNotMaintenance();
        BucketInfo info = storage.getBucketInfo(bucket);
        if (info == null) return JsonResult.warn("Bucket 不存在: " + bucket);
        return JsonResult.success(toBucketView(info));
    }

    // ==================== Object ====================
    // 对象操作提供两套端点：
    //   1) /buckets/{bucket}/objects/** —— S3 兼容（key 在 URL 路径中，受 URL 编码限制）
    //   2) /buckets/{bucket}/object     —— 管理后台专用（key 通过 X-Object-Key 请求头传递，支持任意字符）

    /**
     * 从请求头 X-Object-Key 提取对象 key。
     * <p>处理流程：
     * <ol>
     *   <li>Tomcat 默认按 ISO-8859-1 读取 header，先转回 UTF-8 字节再以 UTF-8 解码（兼容直连 raw UTF-8 场景，如 curl）</li>
     *   <li>前端为了在浏览器中传递含中文/全角等非 ASCII 字符的文件名，会对 header 值做 URL 编码（encodeURIComponent），
     *       因此需要再做一次 URLDecoder.decode 还原原始 key</li>
     * </ol>
     */
    private String resolveKeyFromHeader(HttpServletRequest request) {
        String raw = request.getHeader("X-Object-Key");
        if (raw == null) return null;
        // Step 1: ISO-8859-1 → UTF-8（兼容 raw UTF-8 直连）
        String utf8 = new String(raw.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        // Step 2: URL 解码（前端 encodeURIComponent 后的 ASCII 安全传输，兼容中文/全角/特殊字符）
        try {
            return URLDecoder.decode(utf8, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // 非 URL 编码的旧值或含非法 % 序列时降级返回 UTF-8 字符串
            return utf8;
        }
    }

    /** 上传对象（key 通过 X-Object-Key 请求头传递，支持中文文件名） */
    @PutMapping("/buckets/{bucket}/object")
    public ResponseEntity<JsonResult> putObjectByKey(@PathVariable String bucket,
                                                     HttpServletRequest request) {
        ensureNotMaintenance();
        String key = resolveKeyFromHeader(request);
        return doPutObject(bucket, key, request);
    }

    /** 上传对象（S3 兼容，key 在 URL 路径中） */
    @PutMapping("/buckets/{bucket}/objects/**")
    public ResponseEntity<JsonResult> putObject(@PathVariable String bucket,
                                                HttpServletRequest request) {
        ensureNotMaintenance();
        String key = extractKey(request, bucket);
        return doPutObject(bucket, key, request);
    }

    private ResponseEntity<JsonResult> doPutObject(String bucket, String key, HttpServletRequest request) {
        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest().body(JsonResult.warn("object key 为空，请通过 X-Object-Key 请求头或 URL 路径提供"));
        }
        try {
            String contentType = request.getContentType();
            long length = request.getContentLengthLong();
            Map<String, String> userMeta = extractUserMeta(request);
            try (InputStream in = request.getInputStream()) {
                ObjectMetadata meta = storage.putObject(bucket, key, in, length, contentType, userMeta);
                HttpHeaders headers = metaHeaders(meta);
                return ResponseEntity.ok().headers(headers).body(JsonResult.success(toObjectView(meta)));
            }
        } catch (StorageException e) {
            log.warn("上传对象失败: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().body(JsonResult.error(e.getMessage()));
        } catch (Exception e) {
            log.error("上传对象异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().body(JsonResult.error("上传失败: " + e.getMessage()));
        }
    }

    /** 下载对象（key 通过 X-Object-Key 请求头传递，支持中文文件名） */
    @GetMapping("/buckets/{bucket}/object")
    public ResponseEntity<?> getObjectByKey(@PathVariable String bucket,
                                            HttpServletRequest request) {
        ensureNotMaintenance();
        String key = resolveKeyFromHeader(request);
        log.debug("getObjectByKey: bucket={}, resolvedKey={}, rawHeader={}",
                bucket, key, request.getHeader("X-Object-Key"));
        return doGetObject(bucket, key, request);
    }

    /** 下载对象（S3 兼容，key 在 URL 路径中） */
    @GetMapping("/buckets/{bucket}/objects/**")
    public ResponseEntity<?> getObject(@PathVariable String bucket, HttpServletRequest request) {
        ensureNotMaintenance();
        String key = extractKey(request, bucket);
        return doGetObject(bucket, key, request);
    }

    private ResponseEntity<?> doGetObject(String bucket, String key, HttpServletRequest request) {
        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest().body(JsonResult.warn("object key 为空，请通过 X-Object-Key 请求头或 URL 路径提供"));
        }
        ReadHandle handle = storage.getObject(bucket, key);
        if (handle == null) {
            return ResponseEntity.notFound().build();
        }
        ObjectMetadata meta = handle.metadata();
        HttpHeaders headers = metaHeaders(meta);
        // 禁止浏览器缓存：所有对象共享同一 URL（仅 X-Object-Key 不同），
        // 缓存会导致点击查看/下载时返回上次请求的错误文件
        headers.setCacheControl("no-store, no-cache, must-revalidate");
        headers.setPragma("no-cache");
        headers.setExpires(0);
        // 声明支持 Range（<video>/<audio> 流式播放依赖分段请求）
        headers.set("Accept-Ranges", "bytes");
        MediaType ct = MediaType.parseMediaType(
                meta.getContentType() == null ? "application/octet-stream" : meta.getContentType());
        // 在 Content-Disposition 中设置 RFC 5987 编码文件名，支持中文下载名
        String fileName = key.contains("/") ? key.substring(key.lastIndexOf('/') + 1) : key;
        String encodedFileName = java.net.URLEncoder.encode(fileName, StandardCharsets.UTF_8).replace("+", "%20");
        headers.add(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + encodedFileName + "\"; filename*=UTF-8''" + encodedFileName);

        long totalSize = meta.getSize();
        long[] range = parseRange(request != null ? request.getHeader("Range") : null, totalSize);
        boolean isRange = range != null && range.length == 2;
        long start = isRange ? range[0] : 0;
        long length = isRange ? range[1] - range[0] + 1 : totalSize;
        if (range == RANGE_INVALID) {
            // 非法 Range → 416，并告知完整长度
            headers.set(HttpHeaders.CONTENT_RANGE, "bytes */" + Math.max(totalSize, 0));
            return ResponseEntity.status(416).headers(headers).build();
        }
        if (isRange) {
            headers.set(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + range[1] + "/" + totalSize);
            headers.setContentLength(length);
        } else if (totalSize >= 0) {
            headers.setContentLength(totalSize);
        }

        // 本地读：全量下载用 FileSystemResource 触发 Tomcat sendfile 零拷贝响应；
        // Range 分段请求统一走流式路径（Spring Framework 7 默认转换器不再支持 ResourceRegion）
        java.io.File localFile = handle.file();
        boolean localRead = localFile != null && localFile.exists() && localFile.canRead();
        if (localRead && !isRange) {
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(ct)
                    .body(new org.springframework.core.io.FileSystemResource(localFile));
        }
        // 流式读（本地 Range 或远端转发）：InputStreamResource 经缓冲区拷贝
        try {
            InputStream in = localRead
                    ? new java.io.FileInputStream(localFile)
                    : handle.openStream();
            if (start > 0) {
                in.skipNBytes(start);
            }
            InputStream body = isRange ? new BoundedInputStream(in, length) : in;
            return ResponseEntity.status(isRange ? 206 : 200)
                    .headers(headers)
                    .contentType(ct)
                    .body(new InputStreamResource(body));
        } catch (Exception e) {
            log.error("下载对象异常: bucket={} key={}", bucket, key, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /** 哨兵值：非法 Range（应响应 416） */
    private static final long[] RANGE_INVALID = new long[0];

    /**
     * 解析 Range 请求头（仅支持单区间 bytes=start-end 形式）。
     *
     * @return null 表示返回完整内容；RANGE_INVALID 表示范围非法（416）；
     *         否则返回 {start, end}（闭区间）
     */
    private long[] parseRange(String rangeHeader, long totalSize) {
        if (rangeHeader == null || !rangeHeader.startsWith("bytes=") || totalSize <= 0) {
            return null;
        }
        String spec = rangeHeader.substring("bytes=".length()).trim();
        // 多区间不处理，退化为完整响应
        if (spec.contains(",")) {
            return null;
        }
        try {
            if (spec.startsWith("-")) {
                // 后缀区间 bytes=-N：最后 N 个字节
                long suffix = Long.parseLong(spec.substring(1));
                if (suffix <= 0) return RANGE_INVALID;
                long start = Math.max(0, totalSize - suffix);
                return new long[]{start, totalSize - 1};
            }
            String[] parts = spec.split("-", 2);
            long start = Long.parseLong(parts[0]);
            long end = (parts.length > 1 && !parts[1].isEmpty())
                    ? Long.parseLong(parts[1]) : totalSize - 1;
            if (start > end || start >= totalSize) return RANGE_INVALID;
            end = Math.min(end, totalSize - 1);
            return new long[]{start, end};
        } catch (NumberFormatException e) {
            // 解析失败退化为完整响应
            return null;
        }
    }

    /** HEAD /storage/buckets/{bucket}/object —— 仅返回对象元信息头（请求头版本） */
    @org.springframework.web.bind.annotation.RequestMapping(
            value = "/buckets/{bucket}/object", method = org.springframework.web.bind.annotation.RequestMethod.HEAD)
    public ResponseEntity<Void> headObjectByKey(@PathVariable String bucket,
                                                HttpServletRequest request) {
        ensureNotMaintenance();
        String key = resolveKeyFromHeader(request);
        return doHeadObject(bucket, key);
    }

    /** HEAD /storage/buckets/{bucket}/objects/** —— 仅返回对象元信息头 */
    @org.springframework.web.bind.annotation.RequestMapping(
            value = "/buckets/{bucket}/objects/**", method = org.springframework.web.bind.annotation.RequestMethod.HEAD)
    public ResponseEntity<Void> headObject(@PathVariable String bucket, HttpServletRequest request) {
        ensureNotMaintenance();
        String key = extractKey(request, bucket);
        return doHeadObject(bucket, key);
    }

    private ResponseEntity<Void> doHeadObject(String bucket, String key) {
        if (key == null || key.isEmpty()) return ResponseEntity.badRequest().build();
        ObjectMetadata meta = storage.headObject(bucket, key);
        if (meta == null) return ResponseEntity.notFound().build();
        HttpHeaders headers = metaHeaders(meta);
        headers.set("Accept-Ranges", "bytes");
        if (meta.getSize() >= 0) headers.setContentLength(meta.getSize());
        return ResponseEntity.ok().headers(headers).build();
    }

    /** 删除对象（key 通过 X-Object-Key 请求头传递，支持中文文件名） */
    @DeleteMapping("/buckets/{bucket}/object")
    @ResponseBody
    public JsonResult deleteObjectByKey(@PathVariable String bucket,
                                        HttpServletRequest request) {
        ensureNotMaintenance();
        String key = resolveKeyFromHeader(request);
        return doDeleteObject(bucket, key);
    }

    /** 删除对象（S3 兼容，key 在 URL 路径中） */
    @DeleteMapping("/buckets/{bucket}/objects/**")
    @ResponseBody
    public JsonResult deleteObject(@PathVariable String bucket, HttpServletRequest request) {
        ensureNotMaintenance();
        String key = extractKey(request, bucket);
        return doDeleteObject(bucket, key);
    }

    private JsonResult doDeleteObject(String bucket, String key) {
        if (key == null || key.isEmpty()) return JsonResult.warn("object key 为空，请通过 X-Object-Key 请求头或 URL 路径提供");
        try {
            storage.deleteObject(bucket, key);
            return JsonResult.success("已删除");
        } catch (StorageException e) {
            log.warn("删除对象失败: bucket={} key={}", bucket, key, e);
            return JsonResult.error(e.getMessage());
        }
    }

    @GetMapping("/buckets/{bucket}/objects")
    @ResponseBody
    public JsonResult listObjects(@PathVariable String bucket,
                                  @RequestParam(required = false) String prefix,
                                  @RequestParam(required = false) String marker,
                                  @RequestParam(defaultValue = "1000") int maxKeys) {
        ensureNotMaintenance();
        try {
            ObjectStore.ListResult result = storage.listObjects(bucket, prefix, marker, maxKeys);
            List<Object> views = new ArrayList<>();
            for (ObjectMetadata m : result.getObjects()) views.add(toObjectView(m));
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("objects", views);
            data.put("nextMarker", result.getNextMarker());
            data.put("isTruncated", result.isTruncated());
            return JsonResult.success(data);
        } catch (StorageException e) {
            return JsonResult.error(e.getMessage());
        }
    }

    // ==================== 工具 ====================

    /** 从请求 URI 提取 object key（URL 解码） */
    private String extractKey(HttpServletRequest request, String bucket) {
        String uri = request.getRequestURI();
        String prefix = "/storage/buckets/" + bucket + "/objects/";
        int idx = uri.indexOf(prefix);
        if (idx < 0) return null;
        String raw = uri.substring(idx + prefix.length());
        if (raw.isEmpty()) return null;
        // key 可能含 '/'，整体解码
        return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    private Map<String, String> extractUserMeta(HttpServletRequest request) {
        Map<String, String> map = new LinkedHashMap<>();
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
        if (meta.getEtag() != null) headers.setETag(meta.getEtag());
        if (meta.getCreatedAt() > 0) {
            headers.set(HttpHeaders.LAST_MODIFIED,
                    HTTP_DATE.format(Instant.ofEpochMilli(meta.getCreatedAt()).atZone(ZoneId.of("GMT"))));
        }
        if (meta.getVersion() > 0) headers.set("x-obj-version", String.valueOf(meta.getVersion()));
        if (meta.getUserMetadata() != null) {
            for (Map.Entry<String, String> e : meta.getUserMetadata().entrySet()) {
                headers.set("x-amz-meta-" + e.getKey(), e.getValue());
            }
        }
        return headers;
    }

    private static Map<String, Object> toBucketView(BucketInfo b) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", b.getName());
        m.put("createdAt", b.getCreatedAt());
        m.put("version", b.getVersion());
        return m;
    }

    private static Map<String, Object> toObjectView(ObjectMetadata m) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("key", m.getKey());
        v.put("size", m.getSize());
        v.put("contentType", m.getContentType());
        v.put("etag", m.getEtag());
        v.put("createdAt", m.getCreatedAt());
        v.put("version", m.getVersion());
        v.put("userMetadata", m.getUserMetadata() == null ? Collections.emptyMap() : m.getUserMetadata());
        return v;
    }

    /** 限界输入流：最多读取 limit 字节后返回 EOF（用于 Range 远端转发读） */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        BoundedInputStream(InputStream delegate, long limit) {
            this.delegate = delegate;
            this.remaining = limit;
        }

        @Override
        public int read() throws java.io.IOException {
            if (remaining <= 0) return -1;
            int b = delegate.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws java.io.IOException {
            if (remaining <= 0) return -1;
            int n = delegate.read(buf, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }
    }
}
