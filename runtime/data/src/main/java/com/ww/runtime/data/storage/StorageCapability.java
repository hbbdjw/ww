package com.ww.runtime.data.storage;

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
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectMetadataCache;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.SetLocator;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.data.storage.engine.StorageProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

import tools.jackson.databind.ObjectMapper;

/**
 * STORAGE 能力：以 {@link ObjectStore} 为本地引擎的对象存储节点能力。
 *
 * <p>集群调度（分区放置/期望态收敛/对象迁移）全部由 runtime 框架驱动；
 * 节点内的 Set/Drive 本地分片仍由 {@link SetLocator} 负责（与 admin 同口径），
 * 节点间分区归属由 runtime placement 表决定。</p>
 *
 * <h2>fencing 写入规则</h2>
 * {@link #putObject} 前校验 {@code view.owner(STORAGE, partition)}，
 * 分区口径与调度器一致：{@code partitioner.applyAsInt(bucket + "/" + key)}。
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
 * checkpoint 对齐对象边界）；导入端按 (bucket,key) 聚合分片，fin 片到达后
 * 经 {@link ObjectStore#putReplica} 原子落盘（version 防回退天然幂等）。
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
    /** 桶注册表：PUT/导入/建桶自动登记；start() 从盘上 .bucket 重建。 */
    private final Set<String> buckets = ConcurrentHashMap.newKeySet();
    /** 导入聚合：bucket\0key → 累积数据（fin 片到达后提交并移除）。 */
    private final Map<String, ByteArrayOutputStream> importBuffers = new ConcurrentHashMap<>();
    /** 主写/主删成功回调（复制器挂接；仅 {@link #putObject}/{@link #deleteObject} 触发）。 */
    private volatile WriteHook writeHook;

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
        ObjectStore store = new ObjectStore(dm, locator, new IdentityCryptoCodec(), mapper, pool);

        // 元数据索引一次性重建：列举/计数走索引定位，避免全盘扫描 .meta 文件
        store.rebuildIndex();

        // 桶注册表持久化恢复：扫描盘上 .bucket 文件重建（空桶/漏广播节点由对账兜底）
        BucketStore bs = new BucketStore(dm, mapper);
        this.buckets.clear();
        for (BucketInfo info : bs.listBuckets()) {
            this.buckets.add(info.getName());
        }

        this.metaCache = props.getMetadataCacheMaxSize() > 0
                ? new ObjectMetadataCache(props.getMetadataCacheMaxSize())
                : null;
        this.objectStore = store;
        this.bucketStore = bs;
        this.driveManager = dm;
        this.frozen = false;
        this.importBuffers.clear();
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
        importBuffers.clear();
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
        buckets.add(bucket);
        InputStream bounded = new SizeLimitedInputStream(data, props.getMaxObjectSize());
        ObjectStore.StoredObject stored = store.putPrimary(bucket, key,
                bounded, length, contentType, userMetadata);
        if (metaCache != null) {
            metaCache.put(bucket, key, stored.meta);
        }
        WriteHook hook = writeHook;
        if (hook != null) {
            hook.afterPut(bucket, stored.meta);
        }
        return stored.meta;
    }

    /** 读取对象（不做 fencing）。 */
    public ObjectStore.StoredObject getObject(String bucket, String key) {
        ObjectStore store = objectStore;
        return store == null ? null : store.get(bucket, key);
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
        boolean deleted = requireStarted().delete(bucket, key);
        if (metaCache != null) {
            metaCache.invalidate(bucket, key);
        }
        if (deleted) {
            WriteHook hook = writeHook;
            if (hook != null) {
                hook.afterDelete(bucket, key);
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

    /** 副本写入：主节点预计算元数据直接落盘（version 防回退幂等）。 */
    public ObjectMetadata replicaPut(String bucket, String key, InputStream data,
                                      ObjectMetadata meta) {
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
        boolean deleted = requireStarted().delete(bucket, key);
        if (metaCache != null) {
            metaCache.invalidate(bucket, key);
        }
        return deleted;
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
        if (skipObjects < 0) {
            throw new IllegalArgumentException("skipObjects must be >= 0, got " + skipObjects);
        }
        ObjectStore store = requireStarted();
        return new PartitionExportIterator(store, partition, skipObjects);
    }

    /**
     * 分区导出惰性迭代器：磁盘逐对象读取、按 chunkSize 逐片产出。
     * 任一时刻堆内至多持有当前对象的一个读缓冲 + 一个编码片。
     */
    private final class PartitionExportIterator implements Iterator<byte[]> {

        private final ObjectStore store;
        private final int partition;
        private final Iterator<String> bucketIterator;
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
            this.store = store;
            this.partition = partition;
            this.remainingSkips = skipObjects;
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

        /** 按 bucket/key 字典序找下一个归属本分区且未被断点跳过的存活对象。 */
        private ObjectStore.StoredObject nextMatchingObject() {
            while (true) {
                while (!metaIterator.hasNext()) {
                    if (!bucketIterator.hasNext()) {
                        return null;
                    }
                    currentBucket = bucketIterator.next();
                    metaIterator = listAll(currentBucket, store).iterator();
                }
                ObjectMetadata meta = metaIterator.next();
                if (partitionOf(currentBucket, meta.getKey()) != partition) {
                    continue;
                }
                if (remainingSkips > 0) {
                    remainingSkips--; // 断点之前已完成迁移的对象整体跳过
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
    }

    /**
     * 导入分片（迁移目标端，不做 fencing）：按 (bucket,key) 聚合，
     * fin 片到达后经 putReplica 原子落盘（同 version 跳过、低 version 拒收）。
     *
     * @return 全部合法返回 true；分片非法/聚合残缺/偏移不连续返回 false
     */
    public boolean importChunks(List<byte[]> pieces) {
        Objects.requireNonNull(pieces, "pieces");
        ObjectStore store = requireStarted();
        long max = props.getMaxObjectSize();
        for (byte[] piece : pieces) {
            StorageChunkView chunk = StorageChunkCodec.decodeChunk(piece);
            if (chunk == null) {
                return false;
            }
            String aggKey = chunk.bucket() + "\0" + chunk.key();
            ByteArrayOutputStream buf = importBuffers.computeIfAbsent(aggKey, k -> new ByteArrayOutputStream());
            if (chunk.offset() != buf.size()) {
                importBuffers.remove(aggKey);
                return false; // 偏移不连续（残缺聚合/乱序）
            }
            // T12：聚合累计上限——超限即失败并清聚合，防止无界堆内聚合
            if ((long) buf.size() + chunk.data().length > max) {
                importBuffers.remove(aggKey);
                LOG.warn("迁移导入聚合超出对象大小上限，中止：{}/{}（累计 {} > {}）",
                        chunk.bucket(), chunk.key(), (long) buf.size() + chunk.data().length, max);
                return false;
            }
            buf.write(chunk.data(), 0, chunk.data().length);
            if (chunk.fin()) {
                importBuffers.remove(aggKey);
                byte[] all = buf.toByteArray();
                if (all.length != chunk.meta().getSize()) {
                    return false; // 尺寸与元数据不符
                }
                store.putReplica(chunk.bucket(), chunk.key(), new ByteArrayInputStream(all),
                        all.length, chunk.meta(), max);
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
        Optional<PlacementView.Owner> owner = view.owner(type(), PartitionId.of(partition));
        if (owner.isEmpty() || !owner.get().node().equals(self)) {
            long actual = owner.map(PlacementView.Owner::epoch).orElse(-1L);
            throw new StaleEpochException(-1L, actual);
        }
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
        return store.snapshot(bucket);
    }

    private ObjectStore requireStarted() {
        ObjectStore store = objectStore;
        if (store == null) {
            throw new RuntimeFrameworkException("storage capability not started");
        }
        return store;
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
