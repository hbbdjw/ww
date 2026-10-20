package com.ww.runtime.data.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.capability.CapabilityModule;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.engine.BucketInfo;
import com.ww.runtime.data.storage.engine.BucketStore;
import com.ww.runtime.data.storage.engine.ByteBufferPool;
import com.ww.runtime.data.storage.engine.DriveManager;
import com.ww.runtime.data.storage.engine.IdentityCryptoCodec;
import com.ww.runtime.data.storage.engine.MetadataIndex;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectMetadataCache;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.SetLocator;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.data.storage.engine.StorageProperties;

import tools.jackson.databind.ObjectMapper;

/**
 * STORAGE 能力：以 {@link ObjectStore} 为本地引擎的对象存储节点能力。
 *
 * <p>集群调度（分区放置/期望态收敛/对象迁移）全部由 runtime 框架驱动；
 * 节点内的 Set/Drive 本地分片仍由 {@link SetLocator} 负责（与 admin 同口径），
 * 节点间分区归属由 runtime placement 表决定。</p>
 *
 * <h2>fencing 写入规则</h2>
 * {@link #putObject} 前经 {@code view.checkWritable(STORAGE, partition, self, epoch)}
 * 校验属主/纪元/新属主生效时间/本地租约；生产装配使用本地缓存视图与租约，
 * 写路径不查询数据库。分区口径与调度器一致：{@code partitioner.applyAsInt(bucket + "/" + key)}。
 * 读/HEAD/列举不做 fencing（冻结/维护态仍可读）。
 *
 * <h2>副本数据面（网关/复制器调用，不做 fencing）</h2>
 * {@link #replicaPut} 直接经 {@link ObjectStore#putReplica} 落盘（version 防回退幂等）；
 * {@link #replicaDelete} 本地删除。副本节点非属主，fencing 校验天然不成立，故独立开口。
 *
 * <h2>桶元数据</h2>
 * 启动时经 {@link BucketStore} 扫描盘上 {@code .bucket} 文件重建桶注册表（跨重启持久）；
 * 建桶/删桶由网关 SyncBucket 广播到全部节点（幂等本地落盘）。
 *
 * <h2>迁移数据面</h2>
 * 单对象按 chunkSize 切片（{@link StorageChunkCodec}，仅 fin 片带终结符，
 * checkpoint 对齐对象边界）；导入端经 {@link ImportAggregator} 按 (bucket,key)
 * 流式聚合到暂存文件（零堆内聚合，T1），fin 片到达后经 {@link ObjectStore#putReplica}
 * 原子落盘（version 防回退天然幂等）。
 */
public final class StorageCapability implements CapabilityModule {

    private static final Logger LOG = LoggerFactory.getLogger(StorageCapability.class);

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final StorageProperties props;
    private final int chunkSize;

    private volatile DriveManager driveManager;
    private volatile ObjectStore objectStore;
    private volatile BucketStore bucketStore;
    private volatile ObjectMetadataCache metaCache;
    private volatile boolean frozen;
    /** 迁移 cutover 停写窗口：partition → 冻结到期（单调毫秒，TTL 自愈）。 */
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> frozenPartitions =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** 桶注册表：PUT/导入/建桶自动登记；start() 从盘上 .bucket 重建。 */
    private final Set<String> buckets = ConcurrentHashMap.newKeySet();
    /** 导入聚合（T1）：bucket\0key → 暂存文件流式追加，消灭堆内聚合；fin 片到达后提交并移除。 */
    private volatile ImportAggregator importAggregator;
    /** 主写/主删成功回调（复制器挂接；仅 {@link #putObject}/{@link #deleteObject} 触发）。 */
    private volatile WriteHook writeHook;
    /** T11：崩溃残留周期 GC 调度器 */
    private volatile java.util.concurrent.ScheduledExecutorService tempGcScheduler;
    /** T9：隐式建桶计数（告警模式观测；切严格模式前须评估） */
    private final java.util.concurrent.atomic.AtomicLong implicitBucketCreations =
            new java.util.concurrent.atomic.AtomicLong();

    // ------------------------------------------------------------------
    // P0-1 半同步 at-risk 观测（降级模式下已应答但副本未确认的写）
    // ------------------------------------------------------------------

    /** at-risk 环形缓冲容量（有界防 OOM；淘汰最旧）。 */
    private static final int AT_RISK_MAX_ENTRIES = 1_000;

    /** 有界环形缓冲：满时淘汰最旧（保最新观测）。 */
    private final java.util.Queue<AtRiskRecord> atRiskLog =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    /** at-risk 累计计数（含被淘汰条目，监控口径）。 */
    private final java.util.concurrent.atomic.AtomicLong atRiskTotal =
            new java.util.concurrent.atomic.AtomicLong();

    /** at-risk 观测记录：已应答客户端但超时内无副本确认的写（降级模式）。 */
    public record AtRiskRecord(String bucket, String key, long version, boolean delete,
                               long atMs, String reason) {
    }

    /** at-risk 累计总数（监控/测试）。 */
    public long atRiskTotal() {
        return atRiskTotal.get();
    }

    /** at-risk 近期快照（管理面/测试；最多 {@value #AT_RISK_MAX_ENTRIES} 条，旧→新）。 */
    public java.util.List<AtRiskRecord> atRiskSnapshot() {
        return java.util.List.copyOf(atRiskLog);
    }

    /**
     * 主写路径成功回调：本地直写与网关转发写（ForwardPut）统一触发，
     * 宿主挂接 {@link StorageReplicator} 实现写后异步复制。
     */
    public interface WriteHook {

        /** 主写落盘成功（元数据含最终 version/etag）。 */
        default void afterPut(String bucket, ObjectMetadata meta) {
        }

        /** 主删成功（对象原本存在）。 */
        default void afterDelete(String bucket, String key) {
        }

        /**
         * 主删成功（带被删对象版本）：版本随删除意图传播到副本，副本据此拒绝
         * 「过期删除」抹掉更高版本的写入。
         */
        default void afterDelete(String bucket, String key, long version) {
            afterDelete(bucket, key);
        }

        /**
         * P0-1 半同步复制：主写落盘后、应答客户端前调用——同步推送并在 timeout 内
         * 等待至少一个存活副本确认。
         *
         * <ul>
         *   <li>分区无副本行（RF=1）→ 空真返回 true；</li>
         *   <li>有副本行但无一存活 → 返回 false（降级信号，调用方按策略处理）；</li>
         *   <li>超时内无任一副本确认 → false；失败的复制仍入 backlog 异步补偿。</li>
         * </ul>
         * 默认实现退化为异步 {@link #afterPut} 并返回 true（兼容既有实现/测试）。
         */
        default boolean replicateSync(String bucket, ObjectMetadata meta, java.time.Duration timeout) {
            afterPut(bucket, meta);
            return true;
        }

        /** 半同步副本删除：语义同 {@link #replicateSync}。默认退化为异步 {@link #afterDelete}。 */
        default boolean replicateDeleteSync(String bucket, String key, java.time.Duration timeout) {
            afterDelete(bucket, key);
            return true;
        }

        /** 半同步副本删除（带版本护栏）。默认降级为无版本重载。 */
        default boolean replicateDeleteSync(String bucket, String key, long version,
                                            java.time.Duration timeout) {
            return replicateDeleteSync(bucket, key, timeout);
        }
    }

    /** 挂接写钩子（null 清除；幂等）。 */
    public void setWriteHook(WriteHook hook) {
        this.writeHook = hook;
    }

    /**
     * @param self        本节点标识
     * @param view        放置视图端口
     * @param partitioner (bucket+"/"+key) → 分区号函数（必须与调度器同口径）
     * @param props       storage 本地配置（drives/rootDir/ioBufferSize/maxObjectSize 等）
     * @param chunkSize   迁移分片大小（字节，建议与 migration.chunk-size 一致）
     */
    public StorageCapability(NodeId self,
                             PlacementView view,
                             ToIntFunction<String> partitioner,
                             StorageProperties props,
                             int chunkSize) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.props = Objects.requireNonNull(props, "props");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0, got " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    @Override
    public CapabilityType type() {
        return CapabilityType.STORAGE;
    }

    @Override
    public void start() {
        // nodeCount=1：runtime 中 StorageCapability 仅管理本地磁盘，跨节点路由由 PlacementEngine
        // 通过分区机制处理；本地 Set 划分使用单机语义（所有盘归一个 Set，哈希选盘做条带化）
        DriveManager dm = new DriveManager(props, 1);
        dm.init(); // Drive 挂载（目录就绪 + Set 划分）
        SetLocator locator = new SetLocator(dm);
        ByteBufferPool pool = new ByteBufferPool(props.getIoBufferSize(), props.getBufferPoolMaxSize());
        ObjectMapper mapper = new ObjectMapper();
        // T3：元数据索引容量上限止血（超限桶列举降级目录扫描，正确性优先）
        MetadataIndex index = new MetadataIndex(props.getMetadataIndexMaxEntries());
        ObjectStore store = new ObjectStore(dm, locator, new IdentityCryptoCodec(), mapper, pool, index);
        // T13：副本路径 fsync 可配置（默认与主路径一致）
        store.setReplicaSkipFsync(props.isReplicaSkipFsync());

        // 元数据索引一次性重建：列举/计数走索引定位，避免全盘扫描 .meta 文件
        store.rebuildIndex();

        // 桶注册表持久化恢复：扫描盘上 .bucket 文件重建（空桶/漏广播节点由对账兜底）
        BucketStore bs = new BucketStore(dm, mapper);
        this.buckets.clear();
        for (BucketInfo info : bs.listBuckets()) {
            this.buckets.add(info.getName());
        }
        // T9 启动期对账告警：对象索引存在而桶注册缺失（隐式建桶仅内存登记的历史残留）
        for (String indexedBucket : store.metadataIndex().bucketsSnapshot()) {
            if (!buckets.contains(indexedBucket)) {
                LOG.warn("T9 对象索引存在但桶注册缺失（历史隐式建桶残留，建议补建桶）: {}", indexedBucket);
            }
        }

        this.metaCache = props.getMetadataCacheMaxSize() > 0
                ? new ObjectMetadataCache(props.getMetadataCacheMaxSize())
                : null;
        // T1：导入暂存聚合器（启动清扫重启残留 + 周期清扫超时聚合）
        ImportAggregator aggregator = new ImportAggregator(
                new File(props.getRootDir(), ".import-tmp"),
                props.getImportStagingMaxAggregates(),
                props.getMaxObjectSize(),
                props.getImportStagingTimeoutMinutes());
        aggregator.start();
        this.importAggregator = aggregator;
        this.objectStore = store;
        this.bucketStore = bs;
        this.driveManager = dm;
        this.frozen = false;
        // T22：新 store 实例代数重新计数——缓存必须失效
        synchronized (snapshotLock) {
            cachedSnapshots = null;
            cachedGeneration = -1L;
        }

        // T11：崩溃残留启动清扫 + 周期 GC（年龄阈值防误删在途写）
        store.sweepTempFiles(props.getTempSweepAgeMillis());
        if (props.getTempGcIntervalMillis() > 0) {
            this.tempGcScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "storage-temp-gc");
                t.setDaemon(true);
                return t;
            });
            long interval = props.getTempGcIntervalMillis();
            this.tempGcScheduler.scheduleWithFixedDelay(
                    () -> store.sweepTempFiles(props.getTempSweepAgeMillis()),
                    interval, interval, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void warm() {
        // ObjectStore 元数据随读随加载（.meta 与数据同目录），无需预热
    }

    @Override
    public void drain() {
        frozen = true; // 维护态：停新写
    }

    @Override
    public void stop() {
        frozen = true;
        java.util.concurrent.ScheduledExecutorService gc = tempGcScheduler;
        if (gc != null) {
            gc.shutdownNow();
            this.tempGcScheduler = null;
        }
        ImportAggregator aggregator = importAggregator;
        if (aggregator != null) {
            aggregator.close();
        }
        this.importAggregator = null;
        if (metaCache != null) {
            metaCache.clear();
        }
        this.objectStore = null;
        this.bucketStore = null;
        this.driveManager = null;
    }

    @Override
    public void freeze(boolean frozen) {
        this.frozen = frozen;
    }

    // ------------------------------------------------------------------
    // 数据面 API（带 fencing）
    // ------------------------------------------------------------------

    /** 桶登记（幂等）：导出扫描与 REST 建桶共用。 */
    public void registerBucket(String bucket) {
        Objects.requireNonNull(bucket, "bucket");
        buckets.add(bucket);
    }

    /**
     * 带 fencing 的主写：仅当分区属主为本节点时通过。
     *
     * <p>流式落盘：不整对象入堆，经 {@link SizeLimitedInputStream} 兜底大小上限
     * （长度已知时前置校验，未知时超限读抛出），元数据写穿缓存。</p>
     *
     * @param userMetadata S3 自定义元数据（x-amz-meta-*），可 null
     */
    public ObjectMetadata putObject(String bucket, String key, InputStream data, long length,
                                    String contentType, Map<String, String> userMetadata)
            throws IOException {
        requireWritable();
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(data, "data");
        if (length > props.getMaxObjectSize()) {
            throw new RuntimeFrameworkException("object exceeds max size " + props.getMaxObjectSize()
                    + ": " + length);
        }
        checkOwner(partitionOf(bucket, key));
        ObjectStore store = requireStarted();
        ensureBucketForWrite(bucket);
        InputStream bounded = new SizeLimitedInputStream(data, props.getMaxObjectSize());
        ObjectStore.StoredObject stored = store.putPrimary(bucket, key,
                bounded, length, contentType, userMetadata);
        // 长流式写期间可能发生 cutover/分区冻结：落盘后、应答前再校验一次属主，
        // 避免“切主后仍向旧主 ack”的已确认写丢失（未 ack 的落盘由迁移清源回收）。
        checkOwner(partitionOf(bucket, key));
        if (metaCache != null) {
            metaCache.put(bucket, key, stored.meta);
        }
        WriteHook hook = writeHook;
        if (hook != null) {
            if (!props.isSemiSyncEnabled()) {
                hook.afterPut(bucket, stored.meta);
            } else if (!hook.replicateSync(bucket, stored.meta,
                    java.time.Duration.ofMillis(props.getSemiSyncTimeoutMs()))) {
                handleSemiSyncTimeout(bucket, key, stored.meta.getVersion(), false);
            }
        }
        return stored.meta;
    }

    /**
     * P0-1 半同步超时处理：fail-closed → 抛 {@link ReplicationTimeoutException}（503，
     * 宁可写失败也不应答可能随主节点死亡丢失的请求）；降级（默认）→ at-risk 观测后放行。
     */
    private void handleSemiSyncTimeout(String bucket, String key, long version, boolean delete) {
        String reason = "semi-sync replication timeout after " + props.getSemiSyncTimeoutMs()
                + "ms: no live replica ack for " + bucket + "/" + key;
        if (props.isSemiSyncFailClosed()) {
            throw new com.ww.runtime.common.ReplicationTimeoutException(reason);
        }
        recordAtRisk(bucket, key, version, delete, reason);
    }
    /** 降级模式记录 at-risk 观测（有界环形 + WARN，供监控/事后审计）。 */
    private void recordAtRisk(String bucket, String key, long version, boolean delete, String reason) {
        long total = atRiskTotal.incrementAndGet();
        AtRiskRecord record = new AtRiskRecord(bucket, key, version, delete,
                System.currentTimeMillis(), reason);
        atRiskLog.add(record);
        while (atRiskLog.size() > AT_RISK_MAX_ENTRIES) {
            atRiskLog.poll();
        }
        LOG.warn("P0-1 at-risk write acknowledged without replica ack (total={}): {}/{} v{} delete={}",
                total, bucket, key, version, delete);
    }

    /**
     * 读取对象（不做 fencing）。
     * <p>T18：GET 与 HEAD 共用元数据写穿缓存——命中缓存的 meta 直接定位数据文件，
     * 免每次 GET 读盘解析 {@code object.meta}（旧实现仅 HEAD 用缓存，GET 恒回盘）。
     * 缓存 miss 回源盘读并回填；删除/覆盖写经 {@link #deleteObject}/{@code putObject}
     * 的 write-through/invalidate 保持一致。</p>
     */
    public ObjectStore.StoredObject getObject(String bucket, String key) {
        ObjectStore store = objectStore;
        if (store == null) {
            return null;
        }
        ObjectMetadataCache cache = metaCache;
        if (cache != null) {
            ObjectMetadata hit = cache.get(bucket, key);
            if (hit != null) {
                ObjectStore.StoredObject cached = store.get(bucket, key, hit);
                if (cached != null) {
                    return cached;
                }
                // 缓存命中但数据文件已消失（并发删除）：失效缓存回退盘读
                cache.invalidate(bucket, key);
            }
        }
        ObjectStore.StoredObject obj = store.get(bucket, key);
        if (obj != null && cache != null) {
            cache.put(bucket, key, obj.meta);
        }
        return obj;
    }

    /** 引擎观测口（包内可见：T15/T26 计数、T11 清扫等管理与测试用途）。 */
    ObjectStore storeForTest() {
        return objectStore;
    }

    /** HEAD（不做 fencing；LRU 缓存命中免盘）。 */
    public ObjectMetadata head(String bucket, String key) {
        ObjectMetadataCache cache = metaCache;
        if (cache != null) {
            ObjectMetadata hit = cache.get(bucket, key);
            if (hit != null) {
                return hit;
            }
        }
        ObjectStore store = objectStore;
        if (store == null) {
            return null;
        }
        ObjectMetadata meta = store.head(bucket, key);
        if (meta != null && cache != null) {
            cache.put(bucket, key, meta);
        }
        return meta;
    }

    /** 带 fencing 的删除。 */
    public boolean deleteObject(String bucket, String key) {
        requireWritable();
        checkOwner(partitionOf(bucket, key));
        ObjectStore store = requireStarted();
        ObjectMetadata before = store.head(bucket, key);
        long deletedVersion = before == null ? -1L : before.getVersion();
        boolean deleted = store.delete(bucket, key);
        checkOwner(partitionOf(bucket, key));
        if (metaCache != null) {
            metaCache.invalidate(bucket, key);
        }
        if (deleted) {
            WriteHook hook = writeHook;
            if (hook != null) {
                if (!props.isSemiSyncEnabled()) {
                    hook.afterDelete(bucket, key, deletedVersion);
                } else if (!hook.replicateDeleteSync(bucket, key, deletedVersion,
                        java.time.Duration.ofMillis(props.getSemiSyncTimeoutMs()))) {
                    handleSemiSyncTimeout(bucket, key, deletedVersion, true);
                }
            }
        }
        return deleted;
    }

    /** 列举（不做 fencing）。 */
    public ObjectStore.ListResult list(String bucket, String prefix, String marker, int maxKeys) {
        return requireStarted().list(bucket, prefix, marker, maxKeys);
    }

    /** 打开读流（不做 fencing；调用方负责关闭）。 */
    public InputStream openRead(ObjectStore.StoredObject obj) throws IOException {
        return requireStarted().openRead(obj);
    }

    // ------------------------------------------------------------------
    // 副本数据面 API（网关转发/复制器调用，不做 fencing）
    // ------------------------------------------------------------------

    /**
     * T9 灰度开关：写路径的桶存在性检查。
     * <ul>
     *   <li>告警模式（默认，strictBucketCheck=false）：未注册桶照旧隐式登记，
     *       但 WARN + 计数——观察期收集真实流量中的隐式建桶场景；</li>
     *   <li>严格模式（strictBucketCheck=true）：未建桶的 PUT 直接拒绝（对齐 S3 语义）。</li>
     * </ul>
     */
    private void ensureBucketForWrite(String bucket) {
        if (buckets.contains(bucket)) {
            return;
        }
        if (props.isStrictBucketCheck()) {
            throw new RuntimeFrameworkException("bucket not exists: " + bucket);
        }
        implicitBucketCreations.incrementAndGet();
        LOG.warn("T9 隐式建桶（告警模式，累计 {} 次）: bucket={}", implicitBucketCreations.get(), bucket);
        buckets.add(bucket);
    }

    /** T9：隐式建桶累计计数（观测口；切严格模式前须确认归零）。 */
    public long getImplicitBucketCreations() {
        return implicitBucketCreations.get();
    }

    /** 副本写入：主节点预计算元数据直接落盘（version 防回退幂等）。 */
    public ObjectMetadata replicaPut(String bucket, String key, InputStream data,
                                      ObjectMetadata meta) {
        // T25：冻结态不约束主写但必须约束副本写——drain/维护态继续接受副本写
        // 会与迁移导出基线竞争，导致导出/校验口径漂移；漏掉的副本写由复制器 backlog 重放兜底
        if (frozen) {
            throw new RuntimeFrameworkException("frozen: replica write rejected");
        }
        ObjectStore store = requireStarted();
        buckets.add(bucket);
        // T12：流式大小兜底（防 meta.size 撒谎），上限校验在 putReplica 版本跳过后生效
        InputStream bounded = new SizeLimitedInputStream(data, props.getMaxObjectSize());
        ObjectStore.StoredObject stored = store.putReplica(bucket, key, bounded,
                meta.getSize(), meta, props.getMaxObjectSize());
        if (metaCache != null) {
            metaCache.put(bucket, key, stored.meta);
        }
        return stored.meta;
    }

    /** 副本删除（非属主节点本地删除）。 */
    public boolean replicaDelete(String bucket, String key) {
        return replicaDelete(bucket, key, -1L);
    }

    /**
     * 带版本护栏的副本删除：副本现有版本高于删除意图版本时拒绝（保留更新写入）；
     * {@code version <= 0} 为旧协议的无版本删除。
     */
    public boolean replicaDelete(String bucket, String key, long version) {
        ObjectStore store = requireStarted();
        if (version > 0) {
            ObjectMetadata existing = store.head(bucket, key);
            if (existing != null && existing.getVersion() > version) {
                LOG.info("stale replica delete ignored: {}/{} localV={} deleteV={}",
                        bucket, key, existing.getVersion(), version);
                return false;
            }
        }
        boolean deleted = store.delete(bucket, key);
        if (metaCache != null) {
            metaCache.invalidate(bucket, key);
        }
        return deleted;
    }

    /** 可用于反熵删除的权威性判定：owner/epoch/validAfter/本地租约/冻结全部通过。 */
    public boolean isWritablePrimary(int partition) {
        try {
            checkOwner(partition);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * 反熵/backlog 用权威性判定：owner/epoch/validAfter/本地租约通过即可，
     * 不受迁移冻结影响（冻结期本机仍是合法属主，只是暂停止写）。
     */
    public boolean isAuthoritativePrimary(int partition) {
        if (!view.isReady(type())) {
            return false;
        }
        try {
            view.checkWritable(type(), PartitionId.of(partition), self, -1L);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------
    // 桶操作（数据面自治：本地 .bucket 落盘 + 网关 SyncBucket 广播协调）
    // ------------------------------------------------------------------

    /** 本地建桶（幂等：已存在则登记后直接返回）。 */
    public void createBucketLocal(String bucket) {
        Objects.requireNonNull(bucket, "bucket");
        BucketStore store = requireBucketStore();
        if (!store.exists(bucket)) {
            store.createBucket(bucket);
        }
        buckets.add(bucket);
    }

    /** 本地删桶（recursive=true 强制清空）。 */
    public void deleteBucketLocal(String bucket, boolean recursive) {
        Objects.requireNonNull(bucket, "bucket");
        requireBucketStore().deleteBucket(bucket, recursive);
        buckets.remove(bucket);
        ObjectStore store = objectStore;
        if (store != null) {
            store.dropBucketIndex(bucket);
        }
        if (metaCache != null) {
            metaCache.invalidateBucket(bucket);
        }
    }

    /** 本地桶全集（注册表视角；列桶 fan-out 由网关合并）。 */
    public List<String> listBuckets() {
        BucketStore store = bucketStore;
        if (store != null) {
            for (BucketInfo info : store.listBuckets()) {
                buckets.add(info.getName());
            }
        }
        return sortedBuckets();
    }

    /**
     * 列出桶元信息（含 createdAt）：与 {@link #listBuckets()} 同语义，
     * 供管理面展示桶统计（对象数/总大小由网关层经 fan-out list 聚合）。
     */
    public List<BucketInfo> listBucketInfos() {
        BucketStore store = bucketStore;
        if (store == null) {
            return List.of();
        }
        List<BucketInfo> infos = new ArrayList<>(store.listBuckets());
        infos.sort(Comparator.comparing(BucketInfo::getName));
        return infos;
    }

    /**
     * 按前缀本地批量删除（不做 fencing：网关 fan-out 到全部节点，
     * 主/副本由各节点各自清理；返回删除的对象数）。
     */
    public long deletePrefix(String bucket, String prefix) {
        Objects.requireNonNull(bucket, "bucket");
        ObjectStore store = requireStarted();
        long deleted = 0L;
        for (ObjectMetadata meta : listAllObjects(bucket)) {
            if (prefix == null || meta.getKey().startsWith(prefix)) {
                if (store.delete(bucket, meta.getKey())) {
                    deleted++;
                }
                if (metaCache != null) {
                    metaCache.invalidate(bucket, meta.getKey());
                }
            }
        }
        return deleted;
    }

    // ------------------------------------------------------------------
    // 迁移数据面 API
    // ------------------------------------------------------------------

    /** 归属指定分区的对象总数（itemCount/verifyCount 共用口径）。 */
    public long partitionCount(int partition) {
        ObjectStore store = objectStore;
        if (store == null) {
            return 0L;
        }
        long count = 0L;
        for (String bucket : sortedBuckets()) {
            for (ObjectMetadata meta : listAll(bucket, store)) {
                if (partitionOf(bucket, meta.getKey()) == partition) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 导出归属指定分区的全部对象分片（对象按 bucket/key 字典序；
     * 单对象按 chunkSize 流式切片——仅末片带终结符——checkpoint 对齐对象边界）。
     *
     * <p><b>惰性流式</b>：返回的迭代器每次 {@code next()} 才从磁盘读至多一片数据，
     * 整个分区不会物化入堆，流式导出避免全量驻堆；
     * 调用方（迁移引擎 / gRPC FetchPartition 直流）边消费边推进，
     * 堆内占用恒定在单对象单片量级。</p>
     *
     * @param skipObjects 跳过前 N 个完整对象（断点续跑：checkpoint = 已完成对象数）
     */
    public Iterator<byte[]> exportPartition(int partition, long skipObjects) {
        return exportPartition(partition, skipObjects, null);
    }

    /**
     * 键型游标导出版本：游标存在时忽略位置断点（位置断点在并发写入下会错位），
     * 只导出严格位于游标 (bucket,key) 之后的存活对象。
     */
    public Iterator<byte[]> exportPartition(int partition, long skipObjects, String cursor) {
        if (skipObjects < 0) {
            throw new IllegalArgumentException("skipObjects must be >= 0, got " + skipObjects);
        }
        if (cursor != null && cursor.isBlank()) {
            cursor = null;
        }
        ObjectStore store = requireStarted();
        return new PartitionExportIterator(store, partition,
                cursor == null ? skipObjects : 0L, cursor);
    }

    /**
     * 分区导出惰性迭代器：磁盘逐对象读取、按 chunkSize 逐片产出。
     * 任一时刻堆内至多持有当前对象的一个读缓冲 + 一个编码片。
     * <p>T24：实现 {@link AutoCloseable}——迁移中止/异常/取消路径必须显式关闭，
     * 消灭当前对象读流的 FD 泄漏（契约见 {@code PartitionDataAdapter#closeExportChunks}）。
     */
    private final class PartitionExportIterator implements Iterator<byte[]>, AutoCloseable {

        private final ObjectStore store;
        private final int partition;
        private final Iterator<String> bucketIterator;
        private final String cursorBucket;
        private final String cursorKey;
        private long remainingSkips;
        private String currentBucket;
        private Iterator<ObjectMetadata> metaIterator;
        private ObjectStore.StoredObject currentObject;
        private InputStream objectStream;
        private byte[] readBuffer;
        private long offset;
        private byte[] pending;
        private boolean exhausted;

        PartitionExportIterator(ObjectStore store, int partition, long skipObjects) {
            this(store, partition, skipObjects, null);
        }

        PartitionExportIterator(ObjectStore store, int partition, long skipObjects, String cursor) {
            this.store = store;
            this.partition = partition;
            this.remainingSkips = skipObjects;
            if (cursor == null) {
                this.cursorBucket = null;
                this.cursorKey = null;
            } else {
                StorageChunkCodec.Cursor decoded = StorageChunkCodec.decodeCursor(cursor);
                this.cursorBucket = decoded.bucket();
                this.cursorKey = decoded.key();
            }
            this.bucketIterator = sortedBuckets().iterator();
            this.metaIterator = Collections.emptyIterator();
        }

        @Override
        public boolean hasNext() {
            if (pending == null && !exhausted) {
                pending = advance();
            }
            return pending != null;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] chunk = pending;
            pending = null;
            return chunk;
        }

        private byte[] advance() {
            while (true) {
                if (objectStream != null) {
                    return readNextChunk();
                }
                ObjectStore.StoredObject obj = nextMatchingObject();
                if (obj == null) {
                    exhausted = true;
                    return null;
                }
                currentObject = obj;
                try {
                    objectStream = store.openRead(obj);
                } catch (IOException e) {
                    currentObject = null;
                    throw new RuntimeFrameworkException("failed to read object "
                            + obj.meta.getKey(), e);
                }
                readBuffer = new byte[chunkSize];
                offset = 0L;
            }
        }

        /** 当前对象读出一片（末片读完即关流，迭代器自动切到下一对象）。 */
        private byte[] readNextChunk() {
            String key = currentObject.meta.getKey();
            try {
                int n = objectStream.readNBytes(readBuffer, 0, chunkSize);
                boolean fin = n < chunkSize;
                byte[] slice = new byte[n];
                System.arraycopy(readBuffer, 0, slice, 0, n);
                byte[] encoded = StorageChunkCodec.encodeChunk(
                        currentObject.bucket, currentObject.meta, slice, offset, fin);
                if (fin) {
                    closeCurrentObject();
                } else {
                    offset += n;
                }
                return encoded;
            } catch (IOException e) {
                closeCurrentObject();
                throw new RuntimeFrameworkException("failed to read object " + key, e);
            }
        }

        /** 按 bucket/key 字典序找下一个归属本分区、位于游标之后且未被断点跳过的存活对象。 */
        private ObjectStore.StoredObject nextMatchingObject() {
            while (true) {
                while (!metaIterator.hasNext()) {
                    if (!bucketIterator.hasNext()) {
                        return null;
                    }
                    currentBucket = bucketIterator.next();
                    // 游标之前的 bucket 整桶跳过（无需读 meta）
                    if (cursorBucket != null && currentBucket.compareTo(cursorBucket) < 0) {
                        continue;
                    }
                    metaIterator = listAll(currentBucket, store).iterator();
                }
                ObjectMetadata meta = metaIterator.next();
                if (partitionOf(currentBucket, meta.getKey()) != partition) {
                    continue;
                }
                if (cursorBucket != null) {
                    int bucketCompare = currentBucket.compareTo(cursorBucket);
                    if (bucketCompare < 0
                            || (bucketCompare == 0 && meta.getKey().compareTo(cursorKey) <= 0)) {
                        continue; // 已迁移对象（<= 游标）：并发写入下的稳定跳过
                    }
                }
                if (remainingSkips > 0) {
                    remainingSkips--; // 断点之前已完成迁移的对象整体跳过（无游标时的旧语义）
                    continue;
                }
                ObjectStore.StoredObject obj = store.get(currentBucket, meta.getKey());
                if (obj == null) {
                    continue; // 枚举与读取之间被删除
                }
                return obj;
            }
        }

        private void closeCurrentObject() {
            if (objectStream != null) {
                try {
                    objectStream.close();
                } catch (IOException ignored) {
                    // 读流关闭失败不影响已产出数据
                }
                objectStream = null;
            }
            currentObject = null;
            readBuffer = null;
        }

        /** T24：迁移中止/异常/取消路径强制关闭当前对象读流（FD 兜底）。 */
        @Override
        public void close() {
            closeCurrentObject();
            exhausted = true;
            pending = null;
        }
    }

    /**
     * 导入分片（迁移目标端，不做 fencing）：按 (bucket,key) 经 {@link ImportAggregator}
     * 流式聚合到暂存文件（零堆内聚合），fin 片到达后经 putReplica 原子落盘
     * （同 version 跳过、低 version 拒收）。
     *
     * @return 全部合法返回 true；分片非法/聚合残缺/偏移不连续/超限返回 false（内部已清聚合）
     */
    public boolean importChunks(List<byte[]> pieces) {
        Objects.requireNonNull(pieces, "pieces");
        ObjectStore store = requireStarted();
        ImportAggregator aggregator = requireAggregator();
        long max = props.getMaxObjectSize();
        for (byte[] piece : pieces) {
            StorageChunkView chunk = StorageChunkCodec.decodeChunk(piece);
            if (chunk == null) {
                return false;
            }
            String aggKey = chunk.bucket() + "\0" + chunk.key();
            // 偏移不连续/字节超限/并发超限 → 中止聚合并返回 false（迁移引擎重发可恢复）
            if (!aggregator.append(aggKey, chunk.data(), chunk.offset())) {
                return false;
            }
            if (chunk.fin()) {
                File staged = aggregator.finish(aggKey, chunk.meta().getSize());
                if (staged == null) {
                    return false; // 尺寸与元数据不符（聚合已中止清理）
                }
                try {
                    // 暂存文件流式导入：FileInputStream 走 ObjectStore 的 sendfile 快路径
                    try (InputStream in = new BufferedInputStream(new FileInputStream(staged))) {
                        store.putReplica(chunk.bucket(), chunk.key(), in,
                                staged.length(), chunk.meta(), max);
                    }
                } catch (IOException e) {
                    throw new StorageException("迁移导入暂存读取失败: "
                            + chunk.bucket() + "/" + chunk.key(), e);
                } finally {
                    staged.delete(); // 无论落盘成败均删暂存（失败由重发重建）
                }
                buckets.add(chunk.bucket()); // 迁入对象同样登记桶（导出扫描可见）
            }
        }
        return true;
    }

    /** 清空指定分区的全部对象（迁移源端在目标端确认后调用）。 */
    public void clearPartition(int partition) {
        ObjectStore store = requireStarted();
        for (String bucket : sortedBuckets()) {
            for (ObjectMetadata meta : listAll(bucket, store)) {
                if (partitionOf(bucket, meta.getKey()) == partition) {
                    store.delete(bucket, meta.getKey());
                    if (metaCache != null) {
                        metaCache.invalidate(bucket, meta.getKey());
                    }
                }
            }
        }
    }

    /**
     * 启动清扫：仅保留谓词 {@code keep} 通过的分区，删除其余分区对象。
     * 供宿主在放置表引导后裁剪重启恢复出的旧归属数据（对象即文件，删除即落盘生效）。
     *
     * @return 删除的对象数（0 表示无删除；能力未启动按 no-op 返回 0）
     */
    public long retainPartitions(IntPredicate keep) {
        Objects.requireNonNull(keep, "keep");
        ObjectStore store = objectStore;
        if (store == null) {
            return 0L;
        }
        long removed = 0L;
        for (String bucket : sortedBuckets()) {
            for (ObjectMetadata meta : listAll(bucket, store)) {
                if (!keep.test(partitionOf(bucket, meta.getKey()))) {
                    if (store.delete(bucket, meta.getKey())) {
                        removed++;
                    }
                    if (metaCache != null) {
                        metaCache.invalidate(bucket, meta.getKey());
                    }
                }
            }
        }
        return removed;
    }

    /**
     * 只读属主探测（数据面转发读路由纠偏用）：分区有属主且属主非本节点时返回 false。
     * 不做 fencing——冻结/维护态仍可读；无放置（单机退化）返回 true。
     */
    public boolean servesPartitionOf(String bucket, String key) {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(key, "key");
        if (!view.isReady(type())) {
            throw new RuntimeFrameworkException("placement view not ready for " + type());
        }
        Optional<PlacementView.Owner> owner = view.owner(type(), PartitionId.of(partitionOf(bucket, key)));
        return owner.isEmpty() || owner.get().node().equals(self);
    }

    /** 本节点全部桶的对象总数（索引计数，管理面每节点统计用，不做 fencing）。 */
    public long totalObjectCount() {
        return requireStarted().countAll();
    }

    /** 指定桶的对象数（索引计数，管理面统计用，不做 fencing）。 */
    public long countObjects(String bucket) {
        Objects.requireNonNull(bucket, "bucket");
        return requireStarted().countObjects(bucket);
    }

    // ------------------------------------------------------------------
    // 对外全量列举（复制器对账 / 管理端点）
    // ------------------------------------------------------------------

    /** 全量列举桶内对象（索引快照，按 key 排序；公开口径）。 */
    public List<ObjectMetadata> listAllObjects(String bucket) {
        return listAll(bucket, requireStarted());
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    private void checkOwner(int partition) {
        checkPartitionFrozen(partition);
        if (!view.isReady(type())) {
            throw new StaleEpochException("placement view not ready for " + type());
        }
        view.checkWritable(type(), PartitionId.of(partition), self, -1L);
    }

    /**
     * 迁移 cutover 停写：冻结该分区写入 {@code ttlMillis}（单调计时，到期自动解除）。
     * 由迁移引擎在最终 SYNCING 前调用。
     */
    public void freezePartition(int partition, long ttlMillis) {
        if (ttlMillis <= 0) {
            frozenPartitions.remove(partition); // ttl<=0 = 立即解冻（迁移失败/完成回收）
            return;
        }
        frozenPartitions.put(partition,
                System.nanoTime() / 1_000_000L + ttlMillis);
    }

    private void checkPartitionFrozen(int partition) {
        Long until = frozenPartitions.get(partition);
        if (until == null) {
            return;
        }
        if (System.nanoTime() / 1_000_000L < until) {
            throw new StaleEpochException("partition " + partition
                    + " frozen for migration cutover");
        }
        frozenPartitions.remove(partition, until);
    }

    private int partitionOf(String bucket, String key) {
        int partition = partitioner.applyAsInt(bucket + "/" + key);
        if (partition < 0) {
            throw new IllegalArgumentException("partitioner returned negative partition for "
                    + bucket + "/" + key + ": " + partition);
        }
        return partition;
    }

    private List<String> sortedBuckets() {
        List<String> sorted = new ArrayList<>(buckets);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

    /** 全量列举桶内对象（索引快照，按 key 排序）。 */
    private List<ObjectMetadata> listAll(String bucket, ObjectStore store) {
        synchronized (snapshotLock) {
            long gen = store.indexGeneration();
            if (cachedSnapshots == null || gen != cachedGeneration) {
                cachedSnapshots = new java.util.HashMap<>();
                cachedGeneration = gen;
            }
            List<ObjectMetadata> list = cachedSnapshots.get(bucket);
            if (list == null) {
                list = store.snapshot(bucket);
                cachedSnapshots.put(bucket, list);
            }
            return list;
        }
    }

    /**
     * T22 短期：按索引代数键控的桶快照复用——迁移流程（itemCount→export→clear→verify）
     * 对同一冻结源端的多次全量列举仅在代数变化时重扫（读不动代数 → 全免扫；
     * 任何写/删/清桶推进代数 → 自动失效重算），消除 O(操作数 × 桶对象数) 的重复快照拷贝。
     */
    private final Object snapshotLock = new Object();
    private long cachedGeneration = -1L;
    private Map<String, List<ObjectMetadata>> cachedSnapshots;

    private ObjectStore requireStarted() {
        ObjectStore store = objectStore;
        if (store == null) {
            throw new RuntimeFrameworkException("storage capability not started");
        }
        return store;
    }

    private ImportAggregator requireAggregator() {
        ImportAggregator aggregator = importAggregator;
        if (aggregator == null) {
            throw new RuntimeFrameworkException("storage capability not started");
        }
        return aggregator;
    }

    private BucketStore requireBucketStore() {
        BucketStore store = bucketStore;
        if (store == null) {
            throw new RuntimeFrameworkException("storage capability not started");
        }
        return store;
    }

    private void requireWritable() {
        if (frozen) {
            throw new RuntimeFrameworkException("frozen");
        }
        requireStarted();
    }

    /** 已登记桶快照（测试/管理端点用）。 */
    public Set<String> registeredBuckets() {
        return new LinkedHashSet<>(buckets);
    }

    /**
     * 大小受限读流：累计读取超过 maxBytes 即抛 {@link StorageException}
     * （未知长度上传的兜底上限；已知长度由调用方前置校验）。
     */
    static final class SizeLimitedInputStream extends InputStream {

        private final InputStream delegate;
        private final long maxBytes;
        private long consumed;

        SizeLimitedInputStream(InputStream delegate, long maxBytes) {
            this.delegate = delegate;
            this.maxBytes = maxBytes;
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b >= 0) {
                consumed++;
                checkLimit();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                consumed += n;
                checkLimit();
            }
            return n;
        }

        private void checkLimit() {
            if (consumed > maxBytes) {
                throw new StorageException("object exceeds max size " + maxBytes + ": " + consumed);
            }
        }
    }
}
