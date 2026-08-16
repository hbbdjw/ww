package com.ww.admin.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import com.ww.admin.common.JsonResult;
import com.ww.storage.BucketInfo;
import com.ww.storage.DistributedStorage;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import com.ww.storage.StorageException;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 存储管理控制器：为管理后台提供存储桶浏览、统计和文件导航 API。
 *
 * <p>端点设计（挂载于 /storage/admin）：
 * <ul>
 *   <li>GET    /storage/admin/buckets                       —— 列举存储桶（含对象数、总大小）</li>
 *   <li>POST   /storage/admin/buckets/{bucket}              —— 创建存储桶</li>
 *   <li>DELETE /storage/admin/buckets/{bucket}?recursive=   —— 删除存储桶</li>
 *   <li>GET    /storage/admin/buckets/{bucket}/stats        —— 存储桶统计（对象数、总大小）</li>
 *   <li>GET    /storage/admin/buckets/{bucket}/entries?prefix=&amp;marker=&amp;maxKeys=
 *           —— 列举当前目录下的子文件夹和文件（以 / 为分隔符，仿文件管理器）</li>
 * </ul>
 *
 * <p>文件下载、删除、查看复用 S3 兼容端点：
 * <ul>
 *   <li>GET    /storage/buckets/{bucket}/objects/**         —— 下载/查看对象</li>
 *   <li>DELETE /storage/buckets/{bucket}/objects/**         —— 删除对象</li>
 * </ul>
 */
@RestController
@RequestMapping("/storage/admin")
@RequiresPermissions("storage:*")
public class StorageAdminController {

    private static final Logger log = LoggerFactory.getLogger(StorageAdminController.class);

    /** 列举条目时单次返回的最大条目数（文件夹 + 文件总数） */
    private static final int DEFAULT_MAX_KEYS = 200;
    private static final int MAX_MAX_KEYS = 2000;
    /** 内部扫描全量对象时的上限，防止超大桶 OOM */
    private static final int INTERNAL_SCAN_LIMIT = 50_000;
    /** 快照缓存 TTL（毫秒）：TTL 内翻页复用同一份扫描结果，避免每页都全集群扫描 */
    private static final long SNAPSHOT_TTL_MS = 30_000L;
    /** 快照缓存的最大桶数（LRU 淘汰） */
    private static final int MAX_CACHED_BUCKETS = 16;

    /** 桶级全量合并快照缓存（accessOrder=true 实现 LRU），避免每次分页请求都触发全集群扫描 */
    private final Map<String, BucketSnapshot> snapshotCache = Collections.synchronizedMap(
            new LinkedHashMap<String, BucketSnapshot>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, BucketSnapshot> eldest) {
                    return size() > MAX_CACHED_BUCKETS;
                }
            });

    /** 单个桶的全量合并快照（按 key 排序，受 INTERNAL_SCAN_LIMIT 截断） */
    private static final class BucketSnapshot {
        final List<ObjectMetadata> objects;
        final boolean truncated;
        final long createdAt;

        BucketSnapshot(List<ObjectMetadata> objects, boolean truncated, long createdAt) {
            this.objects = objects;
            this.truncated = truncated;
            this.createdAt = createdAt;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - createdAt > SNAPSHOT_TTL_MS;
        }
    }

    /** 分页条目包装：响应视图 + 分页排序键 */
    private static final class Entry {
        final Map<String, Object> view;
        final String sortKey;

        Entry(Map<String, Object> view, String sortKey) {
            this.view = view;
            this.sortKey = sortKey;
        }
    }

    @Autowired
    private DistributedStorage storage;

    // ==================== Bucket ====================

    /** 列举所有存储桶及其统计信息（对象数、总大小） */
    @GetMapping("/buckets")
    public JsonResult listBuckets(@RequestParam(defaultValue = "false") boolean refresh) {
        ensureNotMaintenance();
        List<BucketInfo> buckets = storage.listBuckets();
        List<Object> views = new ArrayList<>(buckets.size());
        for (BucketInfo b : buckets) {
            long[] stats = computeStats(getSnapshot(b.getName(), refresh));
            views.add(toBucketView(b, stats[0], stats[1]));
        }
        return JsonResult.success(views);
    }

    /** 创建存储桶 */
    @PostMapping("/buckets/{bucket}")
    public JsonResult createBucket(@PathVariable String bucket) {
        ensureNotMaintenance();
        try {
            BucketInfo info = storage.createBucket(bucket);
            return JsonResult.success(toBucketView(info, 0, 0), "创建成功");
        } catch (StorageException e) {
            log.warn("创建 Bucket 失败: {}", bucket, e);
            return JsonResult.error(e.getMessage());
        }
    }

    /** 删除存储桶 */
    @DeleteMapping("/buckets/{bucket}")
    public JsonResult deleteBucket(@PathVariable String bucket,
                                   @RequestParam(defaultValue = "false") boolean recursive) {
        ensureNotMaintenance();
        try {
            storage.deleteBucket(bucket, recursive);
            invalidateSnapshot(bucket);
            return JsonResult.success("删除成功");
        } catch (StorageException e) {
            log.warn("删除 Bucket 失败: {}", bucket, e);
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 删除文件夹（递归删除指定前缀下的所有对象）。
     * <p>prefix 必须以 / 结尾，表示文件夹路径。
     */
    @DeleteMapping("/buckets/{bucket}/folder")
    public JsonResult deleteFolder(@PathVariable String bucket,
                                   @RequestParam String prefix) {
        ensureNotMaintenance();
        if (prefix == null || prefix.isEmpty()) {
            return JsonResult.warn("文件夹前缀不能为空");
        }
        // 确保 prefix 以 / 结尾，防止误删同名前缀的其他对象
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        try {
            int count = storage.deletePrefix(bucket, prefix);
            invalidateSnapshot(bucket);
            return JsonResult.success("已删除 " + count + " 个对象");
        } catch (StorageException e) {
            log.warn("删除文件夹失败: bucket={} prefix={}", bucket, prefix, e);
            return JsonResult.error(e.getMessage());
        }
    }

    /** 获取单个存储桶的统计信息 */
    @GetMapping("/buckets/{bucket}/stats")
    public JsonResult bucketStats(@PathVariable String bucket,
                                  @RequestParam(defaultValue = "false") boolean refresh) {
        ensureNotMaintenance();
        if (!storage.bucketExists(bucket)) {
            return JsonResult.warn("Bucket 不存在: " + bucket);
        }
        long[] stats = computeStats(getSnapshot(bucket, refresh));
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("objectCount", stats[0]);
        data.put("totalSize", stats[1]);
        return JsonResult.success(data);
    }

    /**
     * 列举当前"目录"下的子文件夹和文件。
     *
     * <p>以 / 作为路径分隔符，将对象 key 按 prefix 之后的第一个 / 分组：
     * <ul>
     *   <li>含 / 的 key 归入"文件夹"（common prefix），文件夹名为 prefix 到第一个 / 之间的部分</li>
     *   <li>不含 / 的 key 为当前目录下的"文件"</li>
     * </ul>
     *
     * <p>返回结构：
     * <pre>
     * {
     *   folders: [{ name: "subdir/", prefix: "subdir/" }],
     *   files:   [{ key, name, size, contentType, etag, createdAt, version }],
     *   nextMarker: "...",
     *   isTruncated: false,
     *   indexTruncated: false
     * }
     * </pre>
     *
     * <p>分页说明：服务端基于不透明 marker 的游标分页（S3 风格），不支持跳页；
     * TTL 内的翻页请求命中快照缓存，不会重复触发全集群扫描。
     *
     * @param prefix  当前目录前缀（含尾部 /，根目录为空字符串）
     * @param marker  分页游标（上一页返回的 nextMarker，首页不传）
     * @param maxKeys 单次返回最大条目数
     * @param refresh 是否强制重建快照（数据变更后的首次请求应传 true）
     */
    @GetMapping("/buckets/{bucket}/entries")
    public JsonResult listEntries(@PathVariable String bucket,
                                  @RequestParam(required = false, defaultValue = "") String prefix,
                                  @RequestParam(required = false) String marker,
                                  @RequestParam(defaultValue = "200") int maxKeys,
                                  @RequestParam(defaultValue = "false") boolean refresh) {
        ensureNotMaintenance();
        if (maxKeys <= 0 || maxKeys > MAX_MAX_KEYS) {
            maxKeys = DEFAULT_MAX_KEYS;
        }
        if (prefix == null) prefix = "";

        try {
            // 命中快照缓存则直接复用，避免每页都执行一次全集群扫描
            BucketSnapshot snapshot = getSnapshot(bucket, refresh);

            // 收集子文件夹（TreeSet 去重 + 排序）和当前目录文件（TreeMap 按文件名排序）
            TreeSet<String> folderNames = new TreeSet<>();
            TreeMap<String, ObjectMetadata> fileMap = new TreeMap<>();

            for (ObjectMetadata m : snapshot.objects) {
                String key = m.getKey();
                if (key == null || key.length() <= prefix.length()) continue;
                if (!key.startsWith(prefix)) continue;
                String remainder = key.substring(prefix.length());
                int slashIdx = remainder.indexOf('/');
                if (slashIdx >= 0) {
                    // 是子文件夹中的对象（或"文件夹占位对象"）
                    String folderName = remainder.substring(0, slashIdx + 1);
                    folderNames.add(folderName);
                } else {
                    // 当前目录下的文件
                    fileMap.put(remainder, m);
                }
            }

            // 合并文件夹和文件为统一有序条目列表（文件夹在前）；
            // sortKey 带类型前缀，保证文件夹永远排在文件之前且 marker 无歧义
            List<Entry> entries = new ArrayList<>(folderNames.size() + fileMap.size());
            for (String folder : folderNames) {
                Map<String, Object> fe = new LinkedHashMap<>();
                fe.put("type", "folder");
                // 显示名称去掉末尾的 /
                String displayName = folder.endsWith("/") ? folder.substring(0, folder.length() - 1) : folder;
                fe.put("name", displayName);
                fe.put("prefix", prefix + folder);
                entries.add(new Entry(fe, "0/" + folder));
            }
            for (Map.Entry<String, ObjectMetadata> e : fileMap.entrySet()) {
                entries.add(new Entry(toFileEntry(prefix, e.getKey(), e.getValue()), "1/" + e.getKey()));
            }

            // 分页：定位起始条目。首页（marker 为空）从第一条开始；
            // marker 非空时定位到其后的第一条，marker 超出末尾则返回空页
            int startIdx = 0;
            if (marker != null && !marker.isEmpty()) {
                startIdx = entries.size();
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i).sortKey.compareTo(marker) > 0) {
                        startIdx = i;
                        break;
                    }
                }
            }

            int endIdx = Math.min(startIdx + maxKeys, entries.size());
            boolean truncated = endIdx < entries.size();
            String nextMarker = truncated ? entries.get(endIdx - 1).sortKey : null;

            List<Map<String, Object>> folders = new ArrayList<>();
            List<Map<String, Object>> files = new ArrayList<>();
            for (int i = startIdx; i < endIdx; i++) {
                Map<String, Object> entry = entries.get(i).view;
                if ("folder".equals(entry.get("type"))) {
                    folders.add(entry);
                } else {
                    files.add(entry);
                }
            }

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("folders", folders);
            data.put("files", files);
            data.put("nextMarker", nextMarker);
            data.put("isTruncated", truncated);
            // 快照本身被 INTERNAL_SCAN_LIMIT 截断时为 true，提示前端超出索引上限
            data.put("indexTruncated", snapshot.truncated);
            data.put("prefix", prefix);
            return JsonResult.success(data);
        } catch (StorageException e) {
            return JsonResult.error(e.getMessage());
        }
    }

    // ==================== 内部工具 ====================

    private void ensureNotMaintenance() {
        if (storage.isMaintenance()) {
            throw new StorageException("存储处于维护态（扩缩容迁移中），暂不可读写，请稍后再试");
        }
    }

    /**
     * 获取桶的全量合并快照（TTL 内命中缓存直接复用）。
     * <p>全集群扫描成本很高（磁盘遍历 + 各节点 RPC），分页/统计请求在 TTL 内
     * 共享同一份快照，避免重复扫描；数据变更后可传 refresh=true 强制重建。
     */
    private BucketSnapshot getSnapshot(String bucket, boolean refresh) {
        if (!refresh) {
            BucketSnapshot cached = snapshotCache.get(bucket);
            if (cached != null && !cached.isExpired()) {
                return cached;
            }
        }
        ObjectStore.ListResult result = storage.listObjects(bucket, null, null, INTERNAL_SCAN_LIMIT);
        BucketSnapshot snapshot = new BucketSnapshot(
                result.getObjects(), result.isTruncated(), System.currentTimeMillis());
        snapshotCache.put(bucket, snapshot);
        return snapshot;
    }

    /** 失效指定桶的快照（数据变更后调用） */
    private void invalidateSnapshot(String bucket) {
        snapshotCache.remove(bucket);
    }

    /**
     * 基于快照计算存储桶的对象数和总大小。
     * @return [objectCount, totalSize]
     */
    private long[] computeStats(BucketSnapshot snapshot) {
        long count = 0;
        long totalSize = 0;
        for (ObjectMetadata m : snapshot.objects) {
            count++;
            if (m.getSize() > 0) totalSize += m.getSize();
        }
        return new long[]{count, totalSize};
    }

    private static Map<String, Object> toBucketView(BucketInfo b, long objectCount, long totalSize) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", b.getName());
        m.put("createdAt", b.getCreatedAt());
        m.put("version", b.getVersion());
        m.put("objectCount", objectCount);
        m.put("totalSize", totalSize);
        return m;
    }

    private static Map<String, Object> toFileEntry(String prefix, String name, ObjectMetadata meta) {
        Map<String, Object> fe = new LinkedHashMap<>();
        fe.put("type", "file");
        fe.put("name", name);
        fe.put("key", meta.getKey());
        fe.put("size", meta.getSize());
        fe.put("contentType", meta.getContentType());
        fe.put("etag", meta.getEtag());
        fe.put("createdAt", meta.getCreatedAt());
        fe.put("version", meta.getVersion());
        return fe;
    }
}
