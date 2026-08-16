package com.ww.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import tools.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储数据迁移服务：扩缩容时把源节点（或全部节点）的对象按新 HRW 复制到新主节点，
 * 可选清理源端。支持进度持久化，迁移进程崩溃可续跑。
 *
 * <h3>三种迁移语义</h3>
 * <ul>
 *   <li>{@link #drainNode}：仅迁移指定源节点的对象到新主（用于 transfer/merge）</li>
 *   <li>{@link #rebalanceAll}：扫描所有节点，把每个本地不是新主的对象迁到新主（用于 scale-out）</li>
 *   <li>{@link #purgeNode}：迁移后清理源端本地对象文件</li>
 * </ul>
 *
 * <h3>一致性保证</h3>
 * 调用方须先进入维护态（{@link DistributedStorage#setMaintenance}）再调用本服务。
 * 复用 {@link ObjectStore#putReplica} 的 version 幂等校验，重放安全且可并发。
 * 进度文件记录已完成的 (bucket,key)，重启后续跑跳过已迁移项。
 */
public class StorageMigrationService {

    private static final Logger log = LoggerFactory.getLogger(StorageMigrationService.class);

    /** 迁移进度文件名 */
    private static final String PROGRESS_FILE = ".migration-progress.json";
    /** 单节点扫描 list 拉取上限（与 DistributedStorage.MERGE_SCAN_LIMIT 对齐） */
    private static final int SCAN_LIMIT = Integer.MAX_VALUE;

    private final StorageTopologyManager topologyManager;
    private final StorageRpcClient nodeClient;
    private final BucketStore bucketStore;
    private final ObjectStore objectStore;
    private final NodeRouter nodeRouter;
    private final ObjectMapper objectMapper;
    private final String progressDir;

    /** 迁移进度：已完成的 (bucket + \0 + key) 集合（持久化） */
    private final Map<String, Boolean> completedKeys = new ConcurrentHashMap<>();
    /** 最近一次迁移统计 */
    private volatile Map<String, Object> lastResult;
    private volatile boolean inProgress = false;

    public StorageMigrationService(StorageTopologyManager topologyManager,
                                   StorageRpcClient nodeClient,
                                   BucketStore bucketStore,
                                   ObjectStore objectStore,
                                   NodeRouter nodeRouter,
                                   ObjectMapper objectMapper,
                                   String progressDir) {
        this.topologyManager = topologyManager;
        this.nodeClient = nodeClient;
        this.bucketStore = bucketStore;
        this.objectStore = objectStore;
        this.nodeRouter = nodeRouter;
        this.objectMapper = objectMapper;
        this.progressDir = progressDir != null && !progressDir.isBlank()
                ? new File(progressDir).getAbsolutePath()
                : new File(".").getAbsolutePath();
        loadProgress();
    }

    // ==================== 迁移入口 ====================

    /**
     * 迁移指定源节点的全部对象到新 HRW 主节点。
     * <p>前提：调用方已移除 source 出 HRW 拓扑并进入维护态。
     * @param sourceNodeKey 源节点 nodeKey
     * @return 迁移报告
     */
    public synchronized Map<String, Object> drainNode(String sourceNodeKey) {
        return doMigrate(sourceNodeKey, false, false);
    }

    /**
     * 迁移指定源节点的全部对象到新主，并清理源端本地文件。
     * <p>用于 merge 场景。
     * @param sourceNodeKey 源节点 nodeKey
     * @return 迁移报告
     */
    public synchronized Map<String, Object> drainAndPurge(String sourceNodeKey) {
        return doMigrate(sourceNodeKey, true, false);
    }

    /**
     * 全量重平衡：扫描所有节点，把本地不是新 HRW 主的对象迁到新主。
     * <p>用于 scale-out 场景（新增节点后，约 1/(N+1) 的 key 新主是新节点）。
     * @return 迁移报告
     */
    public synchronized Map<String, Object> rebalanceAll() {
        return doMigrate(null, false, true);
    }

    /** 是否正在迁移 */
    public boolean isInProgress() { return inProgress; }

    /** 查询最近一次迁移结果 */
    public Map<String, Object> getLastResult() { return lastResult; }

    // ==================== 核心迁移逻辑 ====================

    /**
     * @param sourceNodeKey 源节点（drain 模式）；null 表示全量重平衡
     * @param purgeSource 是否清理源端
     * @param scanAll 是否扫描全部节点
     */
    private Map<String, Object> doMigrate(String sourceNodeKey, boolean purgeSource, boolean scanAll) {
        if (inProgress) {
            throw new StorageException("已有迁移任务在进行中");
        }
        inProgress = true;
        long startMs = System.currentTimeMillis();
        AtomicLong migrated = new AtomicLong(0);
        AtomicLong skipped = new AtomicLong(0);
        AtomicLong failed = new AtomicLong(0);
        AtomicLong bytes = new AtomicLong(0);
        List<String> failures = new ArrayList<>();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", scanAll ? "rebalanceAll" : (purgeSource ? "drainAndPurge" : "drainNode"));
        result.put("sourceNodeKey", sourceNodeKey);
        try {
            StorageNode source = sourceNodeKey != null ? topologyManager.getNode(sourceNodeKey) : null;
            if (sourceNodeKey != null && source == null) {
                throw new StorageException("源节点不在拓扑中: " + sourceNodeKey);
            }

            // 确定待扫描的节点列表
            List<StorageNode> nodesToScan;
            if (scanAll) {
                nodesToScan = new ArrayList<>(topologyManager.getNodes());
            } else {
                nodesToScan = new ArrayList<>();
                if (source != null) nodesToScan.add(source);
            }

            // 遍历所有 bucket
            List<BucketInfo> buckets = listAllBuckets();
            for (StorageNode scanNode : nodesToScan) {
                for (BucketInfo b : buckets) {
                    migrateBucketOnNode(scanNode, b.getName(), purgeSource,
                            migrated, skipped, failed, bytes, failures);
                }
            }

            long durationMs = System.currentTimeMillis() - startMs;
            result.put("migrated", migrated.get());
            result.put("skipped", skipped.get());
            result.put("failed", failed.get());
            result.put("bytes", bytes.get());
            result.put("durationMs", durationMs);
            result.put("topologyVersion", topologyManager.getTopologyVersion());
            result.put("failures", failures.size() > 20
                    ? failures.subList(0, 20) : failures);
            result.put("status", failed.get() == 0 ? "success" : "partial");
            log.info("迁移完成: mode={} migrated={} skipped={} failed={} bytes={} duration={}ms",
                    result.get("mode"), migrated.get(), skipped.get(), failed.get(), bytes.get(), durationMs);
            // 成功完成后清空进度文件
            if (failed.get() == 0) clearProgress();
        } catch (Exception e) {
            log.error("迁移异常", e);
            result.put("status", "error");
            result.put("error", e.getMessage());
            result.put("migrated", migrated.get());
            result.put("failed", failed.get());
        } finally {
            inProgress = false;
            lastResult = result;
        }
        return result;
    }

    /**
     * 迁移指定节点上指定 bucket 的所有对象。
     */
    private void migrateBucketOnNode(StorageNode scanNode, String bucket, boolean purgeSource,
                                     AtomicLong migrated, AtomicLong skipped, AtomicLong failed,
                                     AtomicLong bytes, List<String> failures) {
        ObjectMetadata[] objects;
        try {
            objects = listObjectsOnNode(scanNode, bucket);
        } catch (Exception e) {
            log.warn("拉取节点对象列表失败: node={} bucket={}: {}", scanNode.nodeKey(), bucket, e.getMessage());
            failures.add("list " + scanNode.nodeKey() + "/" + bucket + ": " + e.getMessage());
            failed.incrementAndGet();
            return;
        }
        for (ObjectMetadata m : objects) {
            String progressKey = scanNode.nodeKey() + "\0" + bucket + "\0" + m.getKey();
            if (completedKeys.containsKey(progressKey)) {
                skipped.incrementAndGet();
                continue;
            }
            try {
                long sz = migrateOneObject(scanNode, bucket, m.getKey(), m);
                if (sz >= 0) {
                    migrated.incrementAndGet();
                    bytes.addAndGet(sz);
                    if (purgeSource) {
                        purgeLocalObject(scanNode, bucket, m.getKey());
                    }
                } else {
                    skipped.incrementAndGet();
                }
                completedKeys.put(progressKey, true);
                if (completedKeys.size() % 100 == 0) persistProgress();
            } catch (Exception e) {
                failed.incrementAndGet();
                failures.add(scanNode.nodeKey() + "/" + bucket + "/" + m.getKey() + ": " + e.getMessage());
                log.warn("迁移对象失败: node={} bucket={} key={}: {}",
                        scanNode.nodeKey(), bucket, m.getKey(), e.getMessage());
            }
        }
    }

    /**
     * 迁移单个对象：从 scanNode 读取，复制到新 HRW 主节点。
     * @return 复制的字节数；-1 表示无需迁移（本节点已是新主）
     */
    private long migrateOneObject(StorageNode scanNode, String bucket, String key, ObjectMetadata meta) throws IOException {
        String routingKey = bucket + "/" + key;
        StorageNode newPrimary = nodeRouter.selectPrimary(routingKey);
        if (newPrimary == null) {
            throw new StorageException("无法计算新主: " + routingKey);
        }
        // 若 scanNode 本身仍是新主，无需迁移
        if (newPrimary.nodeKey().equals(scanNode.nodeKey())) {
            return -1;
        }
        // 从 scanNode 读取对象数据
        ReadHandle rh = nodeClient.forwardGet(scanNode, bucket, key);
        if (rh == null) {
            // 对象已被删除，跳过
            return -1;
        }
        ObjectMetadata srcMeta = rh.metadata();
        long size = srcMeta.getSize() > 0 ? srcMeta.getSize() : 0;
        try {
            InputStreamSupplier supplier = new InputStreamSupplier(rh);
            nodeClient.replicatePut(newPrimary, bucket, key, supplier, size, srcMeta);
            return size;
        } finally {
            closeQuietly(rh);
        }
    }

    /** 清理源端本地对象文件（不触发复制） */
    private void purgeLocalObject(StorageNode node, String bucket, String key) {
        if (node.isSelf()) {
            // 本地直接删
            objectStore.delete(bucket, key);
        } else {
            nodeClient.localDelete(node, bucket, key);
        }
    }

    // ==================== 节点对象列表获取 ====================

    /** 列出指定节点上指定 bucket 的全部对象元数据 */
    private ObjectMetadata[] listObjectsOnNode(StorageNode node, String bucket) {
        if (node.isSelf()) {
            ObjectStore.ListResult r = objectStore.list(bucket, null, null, SCAN_LIMIT);
            return r.getObjects().toArray(new ObjectMetadata[0]);
        }
        return nodeClient.forwardListObjects(node, bucket);
    }

    /** 列出所有 bucket（本地视角，假设集群 bucket 列表一致） */
    private List<BucketInfo> listAllBuckets() {
        return bucketStore.listBuckets();
    }

    // ==================== 进度持久化 ====================

    @SuppressWarnings("unchecked")
    private synchronized void loadProgress() {
        File f = new File(progressDir, PROGRESS_FILE);
        if (!f.exists()) return;
        try {
            List<String> keys = objectMapper.readValue(f, List.class);
            for (String k : keys) completedKeys.put(k, true);
            log.info("加载迁移进度: {} 条已完成", completedKeys.size());
        } catch (Exception e) {
            log.warn("加载迁移进度失败: {}", e.getMessage());
        }
    }

    private synchronized void persistProgress() {
        File f = new File(progressDir, PROGRESS_FILE);
        File tmp = new File(progressDir, PROGRESS_FILE + ".tmp");
        try {
            File dir = f.getParentFile();
            if (dir != null && !dir.exists() && !dir.mkdirs()) return;
            objectMapper.writeValue(tmp, new ArrayList<>(completedKeys.keySet()));
            if (!tmp.renameTo(f)) {
                if (f.exists() && !f.delete()) return;
                tmp.renameTo(f);
            }
        } catch (Exception e) {
            log.warn("持久化迁移进度失败: {}", e.getMessage());
        }
    }

    private synchronized void clearProgress() {
        completedKeys.clear();
        File f = new File(progressDir, PROGRESS_FILE);
        if (f.exists() && !f.delete()) {
            log.warn("无法删除迁移进度文件: {}", f);
        }
    }

    // ==================== 工具 ====================

    private static void closeQuietly(ReadHandle rh) {
        if (rh == null) return;
        // 本地读：openStream 返回新的 BufferedInputStream，需要关闭；
        // 远端读：openStream 返回 HTTP body 流，需要关闭以释放连接。
        try {
            InputStream s = rh.openStream();
            if (s != null) s.close();
        } catch (Exception ignored) { }
    }

    /**
     * 一次性 InputStream Supplier：包装 ReadHandle，首次调用返回流，
     * 重试场景下不支持重读（迁移单对象不内部重试，失败计入 failures 列表供外层重跑）。
     */
    private static class InputStreamSupplier implements java.util.function.Supplier<InputStream> {
        private final ReadHandle handle;
        private boolean supplied = false;

        InputStreamSupplier(ReadHandle handle) { this.handle = handle; }

        @Override
        public InputStream get() {
            if (supplied) {
                throw new StorageException("迁移流已消费，不支持重读（请重跑迁移 API）");
            }
            supplied = true;
            try {
                return handle.openStream();
            } catch (IOException e) {
                throw new StorageException("打开迁移数据流失败", e);
            }
        }
    }
}
