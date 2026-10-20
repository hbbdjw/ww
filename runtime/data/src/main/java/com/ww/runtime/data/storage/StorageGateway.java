package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.engine.BucketInfo;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.proto.ObjectChunkP;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;

/**
 * STORAGE 网关：任意节点接收 S3 请求后按 placement 自动路由——
 * 分区属主为本机走本地引擎，否则经 gRPC 转发到属主节点（客户端无需感知拓扑）。
 *
 * <h2>路由规则</h2>
 * <ul>
 *   <li>写（PUT/DELETE）：无放置或属主非本机 → Forward*（属主端 fencing 生效，
 *       stale-epoch 还原为 {@link StaleEpochException} → HTTP 409）；</li>
 *   <li>读（GET/HEAD）：路由到属主（read-after-write 严格一致，不降级读副本）；
 *       无放置时本地尝试（404 或 fencing 拒绝）；</li>
 *   <li><b>切主收敛（调用端无感）</b>：cutover 在途请求命中旧属主会被
 *       stale-epoch / not-owner 拒绝——网关强制刷新本地 placement 视图后
 *       <b>重取属主重试一次</b>收敛到新属主，仍失败才向客户端抛 409
 *       （控制面放置表为唯一真源；数据面读本地修订快照，不逐调用查库）。
 *       例外：PUT 远端转发路径的 fencing 拒绝不重试——上行流已部分消费，
 *       同流重试会截断对象（本地路径 checkOwner 在读流前执行，重试安全）；</li>
 *   <li>列举（ListBucket）：fan-out 到正在服务 STORAGE 的存活节点合并（每节点只回本机对象；
 *       能力已停用的节点不参与，避免「capability not started」拒绝）；</li>
 *   <li><b>P0-2 隔离门</b>：分区 DEGRADED（属主死亡且无存活副本，数据留盘押注重启）时
 *       读写一律抛 {@link com.ww.runtime.common.PartitionDegradedException}（503 快速失败），
 *       不静默空读/空写、不改写放置表；</li>
 *   <li>桶 CRUD：本地立即生效 + SyncBucket 广播（失败仅日志，.bucket 文件对账兜底）。</li>
 * </ul>
 *
 * <p>{@code client}/{@code directory} 为 null 时退化为本地单机模式（单测/单节点）。</p>
 */
public final class StorageGateway {

    private static final Logger LOG = LoggerFactory.getLogger(StorageGateway.class);

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final StorageCapability local;
    private final StorageGrpcClient client;
    private final StoragePeerDirectory directory;
    /** P0-2 分区隔离视图（可空：单测/单机模式不启用隔离）；带 TTL 的内存快照，热路径无 DB 访问。 */
    private final com.ww.runtime.capability.DegradedPartitionView degradedView;

    public StorageGateway(NodeId self,
                          PlacementView view,
                          ToIntFunction<String> partitioner,
                          StorageCapability local,
                          StorageGrpcClient client,
                          StoragePeerDirectory directory) {
        this(self, view, partitioner, local, client, directory, null);
    }

    public StorageGateway(NodeId self,
                          PlacementView view,
                          ToIntFunction<String> partitioner,
                          StorageCapability local,
                          StorageGrpcClient client,
                          StoragePeerDirectory directory,
                          com.ww.runtime.capability.DegradedPartitionView degradedView) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.local = Objects.requireNonNull(local, "local");
        this.client = client;
        this.directory = directory;
        this.degradedView = degradedView;
    }

    // ------------------------------------------------------------------
    // 对象数据面
    // ------------------------------------------------------------------

    /**
     * 网关写：路由到属主（本机或转发）。
     *
     * <p>本地 fencing 拒绝（cutover 在途）重取属主重试一次——checkOwner 在读流前执行，
     * 入参流未被消费，重试安全（可能转为远端转发）。远端转发的 fencing 拒绝
     * <b>不重试</b>：上行流已部分消费，同流重试会截断对象——保持 409 上抛由客户端决策。</p>
     */
    public ObjectMetadata put(String bucket, String key, String contentType,
                              Map<String, String> userMeta, InputStream data, long length)
            throws IOException {
        checkNotDegraded(bucket, key);
        NodeId owner = ownerNode(bucket, key);
        if (owner == null || owner.equals(self)) {
            try {
                return local.putObject(bucket, key, data, length, contentType, userMeta);
            } catch (StaleEpochException e) {
                view.refreshRoutingView();
                return putRouted(bucket, key, contentType, userMeta, data, length);
            }
        }
        return putRemote(owner, bucket, key, contentType, userMeta, data, length);
    }

    /** 重查属主后本地或转发（不再重试）。 */
    private ObjectMetadata putRouted(String bucket, String key, String contentType,
                                     Map<String, String> userMeta, InputStream data, long length)
            throws IOException {
        NodeId owner = ownerNode(bucket, key);
        if (owner == null || owner.equals(self)) {
            return local.putObject(bucket, key, data, length, contentType, userMeta);
        }
        return putRemote(owner, bucket, key, contentType, userMeta, data, length);
    }

    /**
     * P0-2 隔离门：分区属主死亡且无存活副本（DEGRADED）时拒绝数据面流量——
     * 放置保持指向死亡属主（数据留盘押注重启），不静默空读/空写。
     * 无视图（单机/测试）时直通。
     */
    private void checkNotDegraded(String bucket, String key) {
        if (degradedView == null) {
            return;
        }
        int partition = partitionOf(bucket, key);
        if (degradedView.isDegraded(CapabilityType.STORAGE.name(), partition)) {
            NodeId owner = ownerNode(bucket, key);
            throw new com.ww.runtime.common.PartitionDegradedException(
                    CapabilityType.STORAGE.name(), partition,
                    owner != null ? owner.value() : "unknown");
        }
    }

    /** 远端转发 + gRPC 状态翻译（fencing 拒绝经 translate 还原为 {@link StaleEpochException}）。 */
    private ObjectMetadata putRemote(NodeId owner, String bucket, String key, String contentType,
                                     Map<String, String> userMeta, InputStream data, long length)
            throws IOException {
        requireClient();
        try {
            return client.forwardPut(owner, bucket, key, contentType, userMeta, data, length);
        } catch (StatusRuntimeException e) {
            throw translate(e, "put", bucket + "/" + key);
        }
    }

    /** 网关读（全量对象）：语义同 {@link #get(String, String, long, long)}（offset=0、length=0）。 */
    public FetchedObject get(String bucket, String key) throws IOException {
        return get(bucket, key, 0L, 0L);
    }

    /**
     * 网关区间读：返回元数据 + <b>已定位到 {@code offset}</b> 的流（调用方负责关闭）。
     * 属主端尽量窗口化回传（新版属主精确截断到 {@code length}；本地与旧版属主回退路径
     * 可能多给，调用方自行按 {@code length} 截断）；length=0 为直到对象末尾。
     * 不存在返回 {@code null}；路由错位重取属主重试一次。
     */
    public FetchedObject get(String bucket, String key, long offset, long length) throws IOException {
        checkNotDegraded(bucket, key);
        try {
            return getOnce(bucket, key, offset, length);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return getOnce(bucket, key, offset, length);
        }
    }

    private FetchedObject getOnce(String bucket, String key, long offset, long length) throws IOException {
        NodeId owner = ownerNode(bucket, key);
        if (owner == null || owner.equals(self)) {
            if (owner != null && !local.servesPartitionOf(bucket, key)) {
                // ownerNode 查询与本地读之间发生切主：按路由错位语义重试
                throw new StaleEpochException("not-owner: partition of '" + bucket + "/" + key + "' moved");
            }
            ObjectStore.StoredObject obj = local.getObject(bucket, key);
            if (obj == null) {
                return null;
            }
            // offset 超出对象大小 → 空窗口（与远端属主语义一致）。
            // 不能依赖 skip 返回值判断：FileInputStream.skip 为 seek 语义，越过末尾仍返回请求值
            if (offset > 0 && offset >= obj.meta.getSize()) {
                return null;
            }
            InputStream in = local.openRead(obj);
            long skipped = 0L;
            try {
                while (skipped < offset) {
                    long s = in.skip(offset - skipped);
                    if (s <= 0) {
                        break;
                    }
                    skipped += s;
                }
            } catch (IOException e) {
                in.close();
                throw e;
            }
            if (skipped < offset) {
                // 对象比 offset 短（并发覆盖缩短）：按不存在处理，上层转 404
                in.close();
                return null;
            }
            return new FetchedObject(obj.meta, in);
        }
        requireClient();
        Iterator<ObjectChunkP> chunks;
        try {
            chunks = client.forwardGet(owner, bucket, key, offset, length);
        } catch (StatusRuntimeException e) {
            throw translate(e, "get", bucket + "/" + key);
        }
        // 首片前远端可能 NOT_FOUND（hasNext 抛出）
        ObjectChunkP first;
        try {
            if (!chunks.hasNext()) {
                return null;
            }
            first = chunks.next();
            // 旧版属主回退（不识别 offset/length，从 0 回放全量）：按片上 offset 快进丢弃，
            // 仅推进迭代器引用、不拷贝字节——保证滚动升级期间绝不错位返回字节
            while (first.getOffset() + first.getData().size() <= offset) {
                if (!chunks.hasNext()) {
                    return null;
                }
                first = chunks.next();
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return null;
            }
            throw translate(e, "get", bucket + "/" + key);
        }
        ObjectMetadata meta = new ObjectMetadata(key, first.getSize(),
                first.getContentType().isEmpty() ? null : first.getContentType(),
                first.getEtag().isEmpty() ? null : first.getEtag(),
                first.getCreatedAt(), first.getVersion());
        if (!first.getUserMetaMap().isEmpty()) {
            meta.setUserMetadata(Map.copyOf(first.getUserMetaMap()));
        }
        // 窗口起点落在首片中部时（旧版属主回退路径）从片内偏移开始消费
        return new FetchedObject(meta, new ChunkInputStream(first, chunks, (int) (offset - first.getOffset())));
    }

    /** 网关 HEAD：不存在返回 {@code null}；路由错位重取属主重试一次。 */
    public ObjectMetadata head(String bucket, String key) {
        checkNotDegraded(bucket, key);
        try {
            return headOnce(bucket, key);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return headOnce(bucket, key);
        }
    }

    private ObjectMetadata headOnce(String bucket, String key) {
        NodeId owner = ownerNode(bucket, key);
        if (owner == null || owner.equals(self)) {
            if (owner != null && !local.servesPartitionOf(bucket, key)) {
                throw new StaleEpochException("not-owner: partition of '" + bucket + "/" + key + "' moved");
            }
            return local.head(bucket, key);
        }
        requireClient();
        return client.forwardHead(owner, bucket, key).orElse(null);
    }

    /**
     * 网关删除：对象原本存在并已删除返回 true；fencing 拒绝抛 {@link StaleEpochException}。
     * 删除无流式载荷且幂等，本地/远端路径的路由错位均重取属主重试一次。
     */
    public boolean delete(String bucket, String key) {
        checkNotDegraded(bucket, key);
        try {
            return deleteOnce(bucket, key);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return deleteOnce(bucket, key);
        }
    }

    private boolean deleteOnce(String bucket, String key) {
        NodeId owner = ownerNode(bucket, key);
        if (owner == null || owner.equals(self)) {
            return local.deleteObject(bucket, key);
        }
        requireClient();
        try {
            return client.forwardDelete(owner, bucket, key);
        } catch (StatusRuntimeException e) {
            throw translate(e, "delete", bucket + "/" + key);
        }
    }

    /**
     * fan-out 列举：正在服务 STORAGE 的存活节点（含 self）各回本机 <b>至多 maxKeys 条</b>
     * （自 marker 排他起、匹配 prefix 的首页），按 key 合并去重（同 key 多节点取 version
     * 最高），排序截断。能力已停用的节点不参与（其数据已迁出且查询必被拒绝，
     * 见 {@link StoragePeerDirectory#servingPeers()}）。
     *
     * <p><b>单页正确性</b>：并集的前 maxKeys 条必然包含于「各节点各自前 maxKeys 条」的并集——
     * 任一进入合并前 K 的 key，在其所属节点上至多有 K-1 个更小的存活对象，必落在该节点首页内；
     * 因此每节点只需一页即可精确合并出前 maxKeys（节点端索引定位，单页成本与桶总量无关）。
     *
     * <p>T19：列举失败的节点记入结果 {@code missingPeers}（降级标记）而非整体失败，
     * 调用方可感知「结果可能缺节点」；翻页调用携带同一标记防 marker 永久跳过。
     * <p>T20：删除无墓碑的短期兜底——合并以<b>属主结果为准</b>：属主节点已响应且未报某 key，
     * 即使 stale 副本仍报该 key 也丢弃（防删除后复活）；属主未响应时保留 version 最大者。
     */
    public ListResult list(String bucket, String prefix, String marker, int maxKeys) {
        Map<String, ObjectMetadata> merged = new LinkedHashMap<>();
        // 本地单页（有界）
        Set<String> localKeys = new LinkedHashSet<>();
        for (ObjectMetadata meta : local.list(bucket, prefix, marker, maxKeys).getObjects()) {
            mergeEntry(merged, meta);
            localKeys.add(meta.getKey());
        }
        // 远端节点：仅 fan-out 到正在服务 STORAGE 的存活节点——能力已停用的节点
        // （如被禁用）查询必被「capability not started」拒绝，徒增告警与无效 RPC
        Map<NodeId, Set<String>> peerKeys = new LinkedHashMap<>();
        Set<NodeId> failedPeers = new LinkedHashSet<>();
        if (directory != null && client != null) {
            for (NodeId peer : directory.servingPeers()) {
                if (peer.equals(self)) {
                    continue;
                }
                try {
                    List<ObjectMetadata> metas = client.listBucket(peer, bucket, prefix, marker, maxKeys);
                    Set<String> keys = new LinkedHashSet<>();
                    for (ObjectMetadata meta : metas) {
                        mergeEntry(merged, meta);
                        keys.add(meta.getKey());
                    }
                    peerKeys.put(peer, keys);
                } catch (RuntimeException e) {
                    failedPeers.add(peer);
                    LOG.warn("fan-out list skipped peer {} bucket={}: {}", peer, bucket, e.getMessage());
                }
            }
            // T20：属主已响应且未报的 key 一律以属主为准剔除（防 stale 副本复活已删对象）；
            // 属主为本机时同口径——本地未报、仅 stale 副本报的 key 不得进入合并结果
            merged.keySet().removeIf(key -> {
                NodeId owner = ownerNode(bucket, key);
                if (owner == null) {
                    return false;
                }
                if (owner.equals(self)) {
                    return !localKeys.contains(key);
                }
                Set<String> ownerKeys = peerKeys.get(owner);
                return ownerKeys != null && !ownerKeys.contains(key);
            });
        }
        List<ObjectMetadata> objects = new ArrayList<>(merged.values());
        objects.sort(Comparator.comparing(ObjectMetadata::getKey));
        boolean truncated = objects.size() > maxKeys;
        if (truncated) {
            objects = new ArrayList<>(objects.subList(0, maxKeys));
        }
        List<String> missingPeers = failedPeers.stream().map(NodeId::toString).toList();
        return new ListResult(objects, truncated && !objects.isEmpty()
                ? objects.getLast().getKey() : null, truncated, missingPeers);
    }

    // ------------------------------------------------------------------
    // 桶操作（本地生效 + 广播收敛）
    // ------------------------------------------------------------------

    /** 建桶：本地落 .bucket + SyncBucket 广播（尽力而为，失败对账兜底）。 */
    public void createBucket(String bucket) {
        local.createBucketLocal(bucket);
        broadcastBucket(bucket, "create", false);
    }

    /**
     * 删桶（T10 顺序重构）：先 fan-out 清空各节点桶内副本数据并删远端桶，
     * <b>全部成功</b>后再删本地桶；任一远端失败即中止（本地保持不动，可重试）——
     * 旧实现本地先删 + 广播尽力而为，失败即跨节点分叉（远端桶/对象残留）。
     *
     * <p><b>非递归删桶先做集群级空判定</b>：对象按主分区散落在各节点，"桶是否为空"
     * 必须是全集群口径——任一节点（含本机）持有对象即抛 {@link StorageException}
     * （REST 映射 409，指明节点），任何破坏性操作都不发生；旧版把远端"桶非空"
     * 降级成泛化 aborted 丢失真实原因。节点不可达无法证明空 → 同样中止（可重试）。</p>
     *
     * @param recursive=true 强制清空桶内对象
     */
    public void deleteBucket(String bucket, boolean recursive) {
        if (directory == null || client == null) {
            // 本地单机模式：无 fan-out，直接删本地
            local.deleteBucketLocal(bucket, recursive);
            return;
        }
        if (!recursive) {
            List<String> nonEmpty = new ArrayList<>();
            List<String> unreachable = new ArrayList<>();
            if (local.countObjects(bucket) > 0) {
                nonEmpty.add(self.value());
            }
            for (NodeId peer : directory.servingPeers()) {
                if (peer.equals(self)) {
                    continue;
                }
                try {
                    if (client.countObjectsStrict(peer, bucket) > 0) {
                        nonEmpty.add(peer.value());
                    }
                } catch (RuntimeException e) {
                    unreachable.add(peer.value());
                }
            }
            if (!unreachable.isEmpty()) {
                throw new RuntimeFrameworkException("delete bucket '" + bucket
                        + "' aborted: cannot prove empty, peers unreachable " + unreachable
                        + " (nothing deleted, retry later)");
            }
            if (!nonEmpty.isEmpty()) {
                throw new StorageException("Bucket 非空，无法删除: " + bucket + "，持有对象的节点: " + nonEmpty
                        + "（可先清空对象或使用 recursive=true）");
            }
        }
        List<NodeId> failed = new ArrayList<>();
        for (NodeId peer : directory.servingPeers()) {
            if (peer.equals(self)) {
                continue;
            }
            try {
                // syncBucket 远端失败返回 false（不抛异常），两者都要判
                if (!client.syncBucket(peer, bucket, "delete", recursive)) {
                    failed.add(peer);
                }
            } catch (RuntimeException e) {
                failed.add(peer);
                LOG.warn("bucket delete fan-out to {} failed: {} : {}", peer, bucket, e.getMessage());
            }
        }
        if (!failed.isEmpty()) {
            // 中止：本地桶保持不动（未删），集群可重试收敛；不产生半删状态
            throw new RuntimeFrameworkException("delete bucket '" + bucket
                    + "' aborted: remote nodes failed " + failed + " (local kept, retry later)");
        }
        local.deleteBucketLocal(bucket, recursive);
    }

    /** 列桶：桶集经建桶/删桶广播在各节点收敛，本地视角即集群视图。 */
    public List<String> listBuckets() {
        return local.listBuckets();
    }

    /** 列桶元信息（含 createdAt）：与 {@link #listBuckets()} 同收敛语义，供管理面统计展示。 */
    public List<BucketInfo> bucketInfos() {
        return local.listBucketInfos();
    }

    /**
     * T21 桶对账（周期调用）：fan-out 列各节点桶集与本地比差——
     * 缺失补广播建桶（幂等）、多余告警留痕。桶版本号防竞态为后续演进。
     *
     * @return 对账报告（补建/多余的桶）
     */
    public BucketReconcileReport reconcileBuckets() {
        List<String> repaired = new ArrayList<>();
        List<String> extra = new ArrayList<>();
        if (directory == null || client == null) {
            return new BucketReconcileReport(repaired, extra);
        }
        Set<String> localBuckets = new LinkedHashSet<>(local.listBuckets());
        for (NodeId peer : directory.servingPeers()) {
            if (peer.equals(self)) {
                continue;
            }
            try {
                Set<String> peerBuckets = new LinkedHashSet<>(client.listBucketNames(peer));
                for (String bucket : localBuckets) {
                    if (!peerBuckets.contains(bucket)) {
                        // 缺失：幂等补建（副本端仅落 .bucket 登记）
                        try {
                            client.syncBucket(peer, bucket, "create", false);
                            repaired.add(peer + ":" + bucket);
                            LOG.warn("T21 桶对账：peer {} 缺桶 {}，已补建", peer, bucket);
                        } catch (RuntimeException e) {
                            LOG.warn("T21 桶对账：peer {} 补建桶 {} 失败: {}", peer, bucket, e.getMessage());
                        }
                    }
                }
                for (String bucket : peerBuckets) {
                    if (!localBuckets.contains(bucket)) {
                        extra.add(peer + ":" + bucket);
                        LOG.warn("T21 桶对账：peer {} 存在本节点未注册的桶 {}（可能为其他属主，留痕）",
                                peer, bucket);
                    }
                }
            } catch (RuntimeException e) {
                LOG.warn("T21 桶对账：peer {} 列桶失败: {}", peer, e.getMessage());
            }
        }
        return new BucketReconcileReport(repaired, extra);
    }

    /** T21 桶对账报告。 */
    public record BucketReconcileReport(List<String> repaired, List<String> extra) {
    }

    // ------------------------------------------------------------------
    // 结果类型
    // ------------------------------------------------------------------

    /** 网关读取结果：元数据 + 数据流。 */
    public record FetchedObject(ObjectMetadata meta, InputStream in) implements AutoCloseable {
        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** fan-out 列举结果（T19：missingPeers 为列举失败/降级节点列表，空 = 全节点结果完整）。 */
    public record ListResult(List<ObjectMetadata> objects, String nextMarker, boolean truncated,
                             List<String> missingPeers) {
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /** 属主节点：无放置返回 {@code null}（调用方退化为本地语义）。 */
    private NodeId ownerNode(String bucket, String key) {
        if (!view.isReady(CapabilityType.STORAGE)) {
            // 本地视图尚未完成首次加载：拒绝路由，不能把“未知”误判为“本地”
            throw new RuntimeFrameworkException("placement view not ready for STORAGE");
        }
        Optional<PlacementView.Owner> owner =
                view.owner(CapabilityType.STORAGE, PartitionId.of(partitionOf(bucket, key)));
        return owner.map(PlacementView.Owner::node).orElse(null);
    }

    private int partitionOf(String bucket, String key) {
        return partitioner.applyAsInt(bucket + "/" + key);
    }

    private void requireClient() {
        if (client == null) {
            throw new RuntimeFrameworkException("storage gateway is in local-only mode (no peer directory)");
        }
    }

    private void broadcastBucket(String bucket, String action, boolean recursive) {
        if (directory == null || client == null) {
            return;
        }
        // 仅广播到正在服务 STORAGE 的节点：能力停用的节点落桶（requireBucketStore）
        // 必被「capability not started」拒绝，广播本就无法生效，跳过以省去无效 RPC
        for (NodeId peer : directory.servingPeers()) {
            if (peer.equals(self)) {
                continue;
            }
            try {
                client.syncBucket(peer, bucket, action, recursive);
            } catch (RuntimeException e) {
                LOG.warn("bucket broadcast to {} failed: {} {} : {}", peer, action, bucket, e.getMessage());
            }
        }
    }

    private static void mergeEntry(Map<String, ObjectMetadata> merged, ObjectMetadata meta) {
        merged.merge(meta.getKey(), meta, (a, b) -> a.getVersion() >= b.getVersion() ? a : b);
    }

    /** gRPC 状态 → 框架异常：委托共享 {@link GatewayErrors#translate}（Cache/Storage 网关同约定）。 */
    private static RuntimeFrameworkException translate(StatusRuntimeException e, String op, String target) {
        return com.ww.runtime.data.GatewayErrors.translate(e, op, target);
    }

    /** 远端分片流 → InputStream（首片已在构造前取出；{@code initialPos} 支持从片内偏移消费）。 */
    private static final class ChunkInputStream extends InputStream {

        private ObjectChunkP current;
        /** current 对应的字节快照：换片时才 toByteArray，避免每次 read 全片拷贝。 */
        private byte[] currentData;
        private final Iterator<ObjectChunkP> rest;
        private int pos;

        ChunkInputStream(ObjectChunkP first, Iterator<ObjectChunkP> rest, int initialPos) {
            this.current = first;
            this.currentData = first.getData().toByteArray();
            this.rest = rest;
            this.pos = initialPos;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (current != null && pos >= currentData.length) {
                advance();
            }
            if (current == null) {
                return -1;
            }
            int n = Math.min(len, currentData.length - pos);
            System.arraycopy(currentData, pos, b, off, n);
            pos += n;
            return n;
        }

        private void advance() throws IOException {
            try {
                if (rest.hasNext()) {
                    current = rest.next();
                    currentData = current.getData().toByteArray();
                    pos = 0;
                } else {
                    current = null;
                    currentData = null;
                }
            } catch (StatusRuntimeException | UncheckedIOException e) {
                throw new IOException("remote chunk stream failed: " + e.getMessage(), e);
            }
        }
    }
}
