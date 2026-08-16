package com.ww.runtime.data.storage;

import com.ww.runtime.capability.CapabilityModule;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.storage.ByteBufferPool;
import com.ww.storage.DriveManager;
import com.ww.storage.IdentityCryptoCodec;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import com.ww.storage.SetLocator;
import com.ww.storage.StorageProperties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToIntFunction;

import tools.jackson.databind.ObjectMapper;

/**
 * STORAGE 能力：以 {@link ObjectStore} 为本地引擎的对象存储节点能力。
 *
 * <p>集群调度（分区放置/期望态收敛/对象迁移）全部由 runtime 框架驱动；
 * 节点内的 Set/Drive 本地分片仍由 {@link SetLocator} 负责（与 admin 同口径），
 * 节点间分区归属由 runtime placement 表决定。
 *
 * <h2>fencing 写入规则</h2>
 * {@link #putObject} 前校验 {@code view.owner(STORAGE, partition)}，
 * 分区口径与调度器一致：{@code partitioner.applyAsInt(bucket + "/" + key)}。
 * 读/HEAD/列举不做 fencing（冻结/维护态仍可读）。
 *
 * <h2>迁移数据面</h2>
 * 单对象按 chunkSize 切片（{@link StorageChunkCodec}，仅 fin 片带终结符，
 * checkpoint 对齐对象边界）；导入端按 (bucket,key) 聚合分片，fin 片到达后
 * 经 {@link ObjectStore#putReplica} 原子落盘（version 防回退天然幂等）。
 */
public final class StorageCapability implements CapabilityModule {

    /** 单对象大小上限（迁移分片与 REST 上传共用口径）。 */
    public static final long MAX_OBJECT_SIZE = 16L * 1024 * 1024;

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final StorageProperties props;
    private final int chunkSize;

    private volatile DriveManager driveManager;
    private volatile ObjectStore objectStore;
    private volatile boolean frozen;
    /** 桶注册表：导出扫描的桶集合（PUT 时自动登记）。 */
    private final Set<String> buckets = ConcurrentHashMap.newKeySet();
    /** 导入聚合：bucket\0key → 累积数据（fin 片到达后提交并移除）。 */
    private final Map<String, ByteArrayOutputStream> importBuffers = new ConcurrentHashMap<>();

    /**
     * @param self        本节点标识
     * @param view        放置视图端口
     * @param partitioner (bucket+"/"+key) → 分区号函数（必须与调度器同口径）
     * @param props       storage 本地配置（drives/rootDir/ioBufferSize 等）
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
        this.objectStore = new ObjectStore(dm, locator, new IdentityCryptoCodec(),
                new ObjectMapper(), pool);
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
        this.objectStore = null;
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
     */
    public ObjectMetadata putObject(String bucket, String key, InputStream data, long length,
                                    String contentType) throws IOException {
        requireWritable();
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(key, "key");
        checkOwner(partitionOf(bucket, key));
        byte[] all = data.readAllBytes();
        if (all.length > MAX_OBJECT_SIZE) {
            throw new RuntimeFrameworkException("object exceeds max size " + MAX_OBJECT_SIZE
                    + ": " + all.length);
        }
        ObjectStore store = requireStarted();
        buckets.add(bucket);
        ObjectStore.StoredObject stored = store.putPrimary(bucket, key,
                new ByteArrayInputStream(all), all.length, contentType, null);
        return stored.meta;
    }

    /** 读取对象（不做 fencing）。 */
    public ObjectStore.StoredObject getObject(String bucket, String key) {
        ObjectStore store = objectStore;
        return store == null ? null : store.get(bucket, key);
    }

    /** HEAD（不做 fencing）。 */
    public ObjectMetadata head(String bucket, String key) {
        ObjectStore store = objectStore;
        return store == null ? null : store.head(bucket, key);
    }

    /** 带 fencing 的删除。 */
    public boolean deleteObject(String bucket, String key) {
        requireWritable();
        checkOwner(partitionOf(bucket, key));
        return requireStarted().delete(bucket, key);
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
     * 单对象按 chunkSize 切片，仅末片带终结符——checkpoint 对齐对象边界）。
     *
     * @param skipObjects 跳过前 N 个完整对象（断点续跑：checkpoint = 已完成对象数）
     */
    public List<byte[]> exportPartition(int partition, long skipObjects) {
        if (skipObjects < 0) {
            throw new IllegalArgumentException("skipObjects must be >= 0, got " + skipObjects);
        }
        ObjectStore store = requireStarted();
        List<byte[]> chunks = new ArrayList<>();
        long objectIndex = 0L;
        for (String bucket : sortedBuckets()) {
            for (ObjectMetadata meta : listAll(bucket, store)) {
                if (partitionOf(bucket, meta.getKey()) != partition) {
                    continue;
                }
                if (objectIndex++ < skipObjects) {
                    continue; // 断点之前已完成迁移的对象整体跳过
                }
                ObjectStore.StoredObject obj = store.get(bucket, meta.getKey());
                if (obj == null) {
                    continue; // 枚举与读取之间被删除
                }
                byte[] data = readFully(store, obj);
                if (data.length == 0) {
                    chunks.add(StorageChunkCodec.encodeChunk(bucket, meta, new byte[0], 0, true));
                    continue;
                }
                for (long offset = 0; offset < data.length; offset += chunkSize) {
                    int len = (int) Math.min(chunkSize, data.length - offset);
                    byte[] slice = new byte[len];
                    System.arraycopy(data, (int) offset, slice, 0, len);
                    boolean fin = offset + len >= data.length;
                    chunks.add(StorageChunkCodec.encodeChunk(bucket, meta, slice, offset, fin));
                }
            }
        }
        return chunks;
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
            buf.write(chunk.data(), 0, chunk.data().length);
            if (chunk.fin()) {
                importBuffers.remove(aggKey);
                byte[] all = buf.toByteArray();
                if (all.length != chunk.meta().getSize()) {
                    return false; // 尺寸与元数据不符
                }
                store.putReplica(chunk.bucket(), chunk.key(), new ByteArrayInputStream(all),
                        all.length, chunk.meta());
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
                }
            }
        }
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

    /** 全量列举桶内对象（翻页直至 truncated=false）。 */
    private List<ObjectMetadata> listAll(String bucket, ObjectStore store) {
        List<ObjectMetadata> all = new ArrayList<>();
        String marker = null;
        while (true) {
            ObjectStore.ListResult page = store.list(bucket, null, marker, 1000);
            all.addAll(page.getObjects());
            if (!page.isTruncated()) {
                return all;
            }
            marker = page.getNextMarker();
        }
    }

    private byte[] readFully(ObjectStore store, ObjectStore.StoredObject obj) {
        try (InputStream in = store.openRead(obj)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read object " + obj.meta.getKey(), e);
        }
    }

    private ObjectStore requireStarted() {
        ObjectStore store = objectStore;
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
}
