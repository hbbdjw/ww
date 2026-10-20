package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

/**
 * 写后副本复制器：主写落盘即应答（read-after-write 读主不受影响），
 * REPLICA 节点异步补齐——失败入内存 backlog 周期重放（指数退避），另以周期对账兜底。
 *
 * <h2>复制时序</h2>
 * <ol>
 *   <li>{@link #afterPut}/{@link #afterDelete}：查 {@link PlacementView#holders}
 *       取该分区 REPLICA 节点，虚拟线程并行 Replicate/ReplicateDelete；</li>
 *   <li>失败 → 入 backlog（有界，满时淘汰最旧以保最新状态；同 (node,bucket,key) 去重）；</li>
 *   <li>后台每 {@code backlogRetryMs} 重放：指数退避（基值=重放周期，上限 60s）；
 *       PUT 重放时本地对象已删则改发 ReplicateDelete（墓碑语义）；</li>
 *   <li>每 {@code reconcileIntervalMs} 对账：扫描本机为 PRIMARY 的分区对象，
 *       远端 HEAD 比对 version，落后/缺失则补复制（backlog 进程内丢失的全量兜底）。</li>
 * </ol>
 *
 * <h2>P0-1 半同步（可选）</h2>
 * {@link #replicateSync}/{@link #replicateDeleteSync} 由写路径在应答客户端前调用：
 * 并行推送全部存活副本并等待<b>首个</b>确认（≥1 副本语义，超时有界）——开启后
 * 「已 ack 的写随主节点死亡而静默丢失」的窗口从「无保证」收缩为「超时内且可观测」。
 *
 * <p>生命周期挂接 CapabilityModule 由宿主编排：start 启线程 / drain 停新写 /
 * stop 关闭（本类 start/stop 幂等）。</p>
 */
public final class StorageReplicator implements StorageCapability.WriteHook {

    private static final Logger LOG = LoggerFactory.getLogger(StorageReplicator.class);

    public static final long DEFAULT_BACKLOG_RETRY_MS = 5_000L;
    public static final long MAX_BACKOFF_MS = 60_000L;
    public static final long DEFAULT_RECONCILE_INTERVAL_MS = 5 * 60_000L;
    public static final int DEFAULT_BACKLOG_MAX_ENTRIES = 10_000;
    /** T14：对账摘要分页大小。 */
    static final int RECONCILE_PAGE_SIZE = 1_000;

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final StorageCapability capability;
    private final StorageReplicationClient client;
    private final StoragePeerDirectory directory;
    private final long backlogRetryMs;
    private final long reconcileIntervalMs;
    private final int backlogMaxEntries;

    /** 有界 backlog：满淘汰最旧；同 (node,bucket,key) 去重（新条目覆盖旧条目）。 */
    private final ArrayDeque<BacklogEntry> backlog = new ArrayDeque<>();
    private final Object backlogLock = new Object();
    /** 在途异步复制任务数（drain 屏障用：freeze 前等待其归零）。 */
    private final java.util.concurrent.atomic.AtomicInteger inFlightReplications =
            new java.util.concurrent.atomic.AtomicInteger();
    /** T21：桶对账钩子（网关 reconcileBuckets；为 null 则不调度） */
    private volatile Runnable bucketReconciler;
    /** T14：backlog 重放与周期对账分离线程池（旧共享 2 线程，长对账饿死高频重放）。 */
    private ScheduledExecutorService replayScheduler;
    private ScheduledExecutorService reconcileScheduler;
    private volatile boolean started;

    public StorageReplicator(NodeId self,
                             PlacementView view,
                             ToIntFunction<String> partitioner,
                             StorageCapability capability,
                             StorageReplicationClient client) {
        this(self, view, partitioner, capability, client, null,
                DEFAULT_BACKLOG_RETRY_MS, DEFAULT_RECONCILE_INTERVAL_MS,
                DEFAULT_BACKLOG_MAX_ENTRIES);
    }

    public StorageReplicator(NodeId self,
                             PlacementView view,
                             ToIntFunction<String> partitioner,
                             StorageCapability capability,
                             StorageReplicationClient client,
                             StoragePeerDirectory directory) {
        this(self, view, partitioner, capability, client, directory,
                DEFAULT_BACKLOG_RETRY_MS, DEFAULT_RECONCILE_INTERVAL_MS,
                DEFAULT_BACKLOG_MAX_ENTRIES);
    }

    public StorageReplicator(NodeId self,
                             PlacementView view,
                             ToIntFunction<String> partitioner,
                             StorageCapability capability,
                             StorageReplicationClient client,
                             StoragePeerDirectory directory,
                             long backlogRetryMs,
                             long reconcileIntervalMs,
                             int backlogMaxEntries) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.capability = Objects.requireNonNull(capability, "capability");
        this.client = Objects.requireNonNull(client, "client");
        this.directory = directory;
        if (backlogRetryMs <= 0 || reconcileIntervalMs <= 0) {
            throw new IllegalArgumentException("intervals must be > 0");
        }
        if (backlogMaxEntries <= 0) {
            throw new IllegalArgumentException("backlogMaxEntries must be > 0");
        }
        this.backlogRetryMs = backlogRetryMs;
        this.reconcileIntervalMs = reconcileIntervalMs;
        this.backlogMaxEntries = backlogMaxEntries;
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    /** 启动后台补偿线程（幂等）。 */
    public synchronized void start() {
        if (started) {
            return;
        }
        // T14：重放与对账各占独立线程，长耗时对账不再饿死 5s 级 backlog 重放
        replayScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "storage-replicator-replay");
            t.setDaemon(true);
            return t;
        });
        reconcileScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "storage-replicator-reconcile");
            t.setDaemon(true);
            return t;
        });
        replayScheduler.scheduleWithFixedDelay(this::safeReplayBacklog,
                backlogRetryMs, backlogRetryMs, TimeUnit.MILLISECONDS);
        reconcileScheduler.scheduleWithFixedDelay(this::safeReconcile,
                reconcileIntervalMs, reconcileIntervalMs, TimeUnit.MILLISECONDS);
        Runnable bucketHook = bucketReconciler;
        if (bucketHook != null) {
            reconcileScheduler.scheduleWithFixedDelay(this::safeReconcileBuckets,
                    reconcileIntervalMs, reconcileIntervalMs, TimeUnit.MILLISECONDS);
        }
        started = true;
        LOG.info("storage replicator started: backlogRetry={}ms reconcile={}ms backlogMax={}",
                backlogRetryMs, reconcileIntervalMs, backlogMaxEntries);
    }

    /** T21：挂接桶对账钩子（start() 前调用生效；典型为 gateway::reconcileBuckets）。 */
    public void setBucketReconciler(Runnable bucketReconciler) {
        this.bucketReconciler = bucketReconciler;
    }

    private void safeReconcileBuckets() {
        try {
            Runnable hook = bucketReconciler;
            if (hook != null) {
                hook.run();
            }
        } catch (RuntimeException e) {
            LOG.warn("bucket reconcile round failed: {}", e.getMessage());
        }
    }

    /** 停止后台线程（幂等；不清空 backlog——进程内条目由对账兜底）。 */
    public synchronized void stop() {
        if (!started) {
            return;
        }
        if (replayScheduler != null) {
            replayScheduler.shutdownNow();
            replayScheduler = null;
        }
        if (reconcileScheduler != null) {
            reconcileScheduler.shutdownNow();
            reconcileScheduler = null;
        }
        started = false;
        LOG.info("storage replicator stopped (backlog remaining: {})", backlogSize());
    }

    // ------------------------------------------------------------------
    // 写后复制（主落盘即应答，副本异步补齐）
    // ------------------------------------------------------------------

    /** 主写成功后的异步复制（fire-and-forget：虚拟线程并行推全部 REPLICA）。 */
    public void afterPut(String bucket, ObjectMetadata meta) {
        Objects.requireNonNull(meta, "meta");
        List<NodeId> replicas = replicasOf(bucket, meta.getKey());
        if (replicas.isEmpty()) {
            return;
        }
        for (NodeId replica : replicas) {
            inFlightReplications.incrementAndGet();
            Thread.startVirtualThread(() -> {
                try {
                    if (!replicateOne(replica, bucket, meta.getKey())) {
                        enqueue(replica, bucket, meta.getKey(), false, meta.getVersion());
                    }
                } finally {
                    inFlightReplications.decrementAndGet();
                }
            });
        }
    }

    /** 主删成功后的异步副本删除（兼容旧调用：无版本护栏）。 */
    public void afterDelete(String bucket, String key) {
        afterDelete(bucket, key, -1L);
    }

    /** 主删成功后的异步副本删除：删除意图携带被删版本，副本拒绝过期删除。 */
    public void afterDelete(String bucket, String key, long version) {
        List<NodeId> replicas = replicasOf(bucket, key);
        if (replicas.isEmpty()) {
            return;
        }
        for (NodeId replica : replicas) {
            inFlightReplications.incrementAndGet();
            Thread.startVirtualThread(() -> {
                try {
                    if (!client.replicateDelete(replica, bucket, key, version)) {
                        enqueue(replica, bucket, key, true, version);
                    }
                } finally {
                    inFlightReplications.decrementAndGet();
                }
            });
        }
    }

    // ------------------------------------------------------------------
    // P0-1 半同步复制（写路径阻塞等副本确认）
    // ------------------------------------------------------------------

    /**
     * 半同步复制：主写落盘后、应答客户端前调用——并行推送全部存活副本，
     * 在 timeout 内等待<b>第一个</b>确认（≥1 副本语义）。
     *
     * <ul>
     *   <li>分区无副本行（RF=1）→ 空真 true；</li>
     *   <li>有副本行但无一存活 → false（降级信号：不可复制也不可承诺）；</li>
     *   <li>超时内无确认 → false；各副本失败仍入 backlog 异步补偿（不丢复制意图）。</li>
     * </ul>
     */
    @Override
    public boolean replicateSync(String bucket, ObjectMetadata meta, java.time.Duration timeout) {
        List<NodeId> replicas = replicasOf(bucket, meta.getKey());
        if (replicas.isEmpty()) {
            // 无副本行（RF=1）空真；有副本行但无一存活 → 不承诺
            return !hasPlacementReplicas(partitionOf(bucket, meta.getKey()));
        }
        return awaitFirstAck(timeout, replicas,
                replica -> replicateOne(replica, bucket, meta.getKey()),
                (replica, ok) -> {
                    if (!ok) {
                        enqueue(replica, bucket, meta.getKey(), false, meta.getVersion());
                    }
                });
    }

    /** 半同步副本删除：语义同 {@link #replicateSync}（≥1 副本删除确认）。 */
    @Override
    public boolean replicateDeleteSync(String bucket, String key, java.time.Duration timeout) {
        return replicateDeleteSync(bucket, key, -1L, timeout);
    }

    /** 半同步副本删除（带版本护栏）：语义同 {@link #replicateSync}。 */
    @Override
    public boolean replicateDeleteSync(String bucket, String key, long version,
                                       java.time.Duration timeout) {
        List<NodeId> replicas = replicasOf(bucket, key);
        if (replicas.isEmpty()) {
            return !hasPlacementReplicas(partitionOf(bucket, key));
        }
        return awaitFirstAck(timeout, replicas,
                replica -> client.replicateDelete(replica, bucket, key, version),
                (replica, ok) -> {
                    if (!ok) {
                        enqueue(replica, bucket, key, true, version);
                    }
                });
    }

    /**
     * 并行推送 + 等首个确认的公共骨架：每个副本一个虚拟线程，任一线程成功即放行
     * （CountDownLatch）；全部失败/超时返回 false。onResult 在每个副本完成后
     * 同步调用（入 backlog 等补偿动作）。
     */
    private boolean awaitFirstAck(java.time.Duration timeout, List<NodeId> replicas,
                                  java.util.function.Predicate<NodeId> attempt,
                                  java.util.function.BiConsumer<NodeId, Boolean> onResult) {
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        for (NodeId replica : replicas) {
            Thread.startVirtualThread(() -> {
                boolean ok;
                try {
                    ok = attempt.test(replica);
                } catch (RuntimeException e) {
                    LOG.warn("semi-sync replicate to {} failed: {}", replica, e.getMessage());
                    ok = false;
                }
                onResult.accept(replica, ok);
                if (ok) {
                    latch.countDown();
                }
            });
        }
        try {
            long millis = Math.max(0L, timeout.toMillis());
            return latch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 放置表中该分区是否存在 REPLICA 行（RF&gt;1 判定；与 serving 过滤无关）。 */
    private boolean hasPlacementReplicas(int partition) {
        return view.holders(CapabilityType.STORAGE, PartitionId.of(partition)).size() > 1;
    }

    /** 当前 backlog 深度（监控/测试）。 */
    public int backlogSize() {
        synchronized (backlogLock) {
            return backlog.size();
        }
    }

    // ------------------------------------------------------------------
    // 补偿：backlog 重放 + 周期对账
    // ------------------------------------------------------------------

    /** 重放 backlog（同步执行，测试直接调用；周期任务经 safeReplayBacklog 包装）。 */
    public void replayBacklog() {
        replayBacklog(false);
    }

    /**
     * 排空复制屏障：强制重放 backlog（忽略退避）并等待在途复制完成。
     * 迁移/排空在 freeze(true) 前调用——副本提升前必须确保副本已追平，
     * 否则会提升一个落后于主节点的副本（已 ack 写丢失）。
     *
     * @return true = 已排空；false = 超时仍有残留（调用方按保守策略处理）
     */
    public boolean awaitDrained(long timeoutMillis) {
        long deadline = System.nanoTime() + Math.max(0L, timeoutMillis) * 1_000_000L;
        while (true) {
            try {
                replayBacklog(true);
            } catch (RuntimeException e) {
                LOG.warn("replication drain round failed: {}", e.getMessage());
            }
            if (inFlightReplications.get() == 0 && backlogSize() == 0) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                LOG.warn("replication drain timed out: backlog={} inFlight={}",
                        backlogSize(), inFlightReplications.get());
                return false;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private void replayBacklog(boolean force) {
        List<BacklogEntry> snapshot;
        synchronized (backlogLock) {
            snapshot = new ArrayList<>(backlog);
        }
        long now = System.currentTimeMillis();
        for (BacklogEntry entry : snapshot) {
            if (!force && now < entry.nextAttemptAtMs) {
                continue; // 退避未到
            }
            // P1-7：分区已迁走/失去属主身份后，旧源不得再重放复制意图——
            // 否则「本地对象已被迁移清源删除」会把旧 PUT 翻成墓碑，误删新主的副本数据。
            if (!capability.isAuthoritativePrimary(partitionOf(entry.bucket, entry.key))) {
                remove(entry);
                LOG.info("replication backlog dropped (no longer authoritative): {}", entry);
                continue;
            }
            if (!isServing(entry.node)) {
                defer(entry, now); // 目标节点能力未启动，推迟到下次重放
                continue;
            }
            boolean ok = replayEntry(entry);
            if (ok) {
                remove(entry);
            } else {
                defer(entry, now);
            }
        }
    }

    /**
     * 重放单条目：以<b>当前本地状态</b>为准重建意图（不盲信入队时的 delete 标志）——
     * 本地对象存在则复制当前版本；本地已删则发送带版本护栏的墓碑删除。
     * 这样 DELETE→PUT→backlog 乱序时，过期删除不会抹掉新写。
     */
    private boolean replayEntry(BacklogEntry entry) {
        ObjectStore.StoredObject obj = capability.getObject(entry.bucket, entry.key);
        if (obj != null) {
            return replicateOne(entry.node, entry.bucket, entry.key);
        }
        return client.replicateDelete(entry.node, entry.bucket, entry.key, entry.version);
    }

    /**
     * 周期对账（T14：分页摘要比对 + 独立线程池）。
     * <p>本机为 PRIMARY 的分区对象，按 (bucket, replica) 拉取远端<b>分页摘要</b>
     * （key+version+etag）一次比对，仅对差集（缺失/落后）对象补复制——把旧实现
     * 的每对象一次 HEAD（RPC O(对象×副本)）降为分页 O(页数)；客户端不支持摘要列举时
     * 降级逐对象 remoteHead（保持既有语义）。</p>
     */
    public void reconcile() {
        int repaired = 0;
        int pruned = 0;
        long scanned = 0L;
        // P1-5：本机为主的副本候选（含当前桶内已无对象的整分区删除场景）
        java.util.Map<Integer, Boolean> authorityCache = new java.util.HashMap<>();
        java.util.Set<NodeId> replicaCandidates = new java.util.HashSet<>();
        for (java.util.Map.Entry<Integer, List<PlacementView.Owner>> entry
                : view.holdersAll(CapabilityType.STORAGE).entrySet()) {
            if (!authority(entry.getKey(), authorityCache)) {
                continue;
            }
            for (PlacementView.Owner holder : entry.getValue()) {
                if (!holder.node().equals(self) && isServing(holder.node())) {
                    replicaCandidates.add(holder.node());
                }
            }
        }
        for (String bucket : capability.listBuckets()) {
            List<ObjectMetadata> locals = capability.listAllObjects(bucket);
            java.util.Set<String> keys = new java.util.HashSet<>();
            for (ObjectMetadata meta : locals) {
                keys.add(meta.getKey());
            }
            // T14：按副本缓存远端摘要页；null 值 = 该副本不支持摘要（走 HEAD 降级）
            java.util.Map<NodeId, java.util.Map<String, StorageReplicationClient.ObjectSummary>> summaryCache =
                    new java.util.HashMap<>();
            for (ObjectMetadata meta : locals) {
                int partition = partitionOf(bucket, meta.getKey());
                if (!isSelfPrimary(partition)) {
                    continue; // 只对账本机主分区
                }
                scanned++;
                for (NodeId replica : replicasOfPartition(partition)) {
                    replicaCandidates.add(replica);
                    try {
                        if (!isReplicaLagging(replica, bucket, meta, summaryCache)) {
                            continue;
                        }
                        if (replicateOne(replica, bucket, meta.getKey())) {
                            repaired++;
                        }
                    } catch (RuntimeException e) {
                        LOG.warn("replication reconcile skipped replica {} for {}/{}: {}",
                                replica, bucket, meta.getKey(), e.getMessage());
                    }
                }
            }
            // P1-5：删除副本上“本机权威分区已不存在”的对象（含整分区删除后的僵尸副本）。
            // 仅在本地租约有效（isAuthoritativePrimary）时执行，且删除携带远端版本护栏。
            for (NodeId replica : replicaCandidates) {
                java.util.Map<String, StorageReplicationClient.ObjectSummary> remote =
                        summaryCache.computeIfAbsent(replica, r -> fetchRemoteSummaries(r, bucket));
                if (remote == null) {
                    continue; // 不支持摘要列举（无法安全判定多余对象）
                }
                for (StorageReplicationClient.ObjectSummary summary : remote.values()) {
                    if (keys.contains(summary.key())) {
                        continue;
                    }
                    int partition = partitionOf(bucket, summary.key());
                    if (!authority(partition, authorityCache)) {
                        continue;
                    }
                    try {
                        if (client.replicateDelete(replica, bucket, summary.key(), summary.version())) {
                            pruned++;
                        }
                    } catch (RuntimeException e) {
                        LOG.warn("replication prune failed replica {} for {}/{}: {}",
                                replica, bucket, summary.key(), e.getMessage());
                    }
                }
            }
        }
        if (scanned > 0 || repaired > 0 || pruned > 0) {
            LOG.info("replication reconcile: scanned={} primary objects, repaired={} copies, pruned={} zombies",
                    scanned, repaired, pruned);
        }
    }

    /** 分区权威性缓存（owner/epoch/validAfter/本地租约；不含迁移冻结）。 */
    private boolean authority(int partition, java.util.Map<Integer, Boolean> cache) {
        return cache.computeIfAbsent(partition, capability::isAuthoritativePrimary);
    }

    /**
     * 判定副本是否落后：优先用缓存的分页摘要（一次拉全量→本地索引）比对 version，
     * 版本相同再比对 etag（T26 同版本异内容冲突可被修复）；摘要不可用
     * （客户端未实现/拉取失败）降级逐对象 remoteHead。
     */
    private boolean isReplicaLagging(NodeId replica, String bucket, ObjectMetadata local,
                                      java.util.Map<NodeId, java.util.Map<String,
                                              StorageReplicationClient.ObjectSummary>> summaryCache) {
        java.util.Map<String, StorageReplicationClient.ObjectSummary> remote =
                summaryCache.computeIfAbsent(replica, r -> fetchRemoteSummaries(r, bucket));
        if (remote != null) {
            StorageReplicationClient.ObjectSummary s = remote.get(local.getKey());
            if (s == null) {
                return true;
            }
            if (s.version() < local.getVersion()) {
                return true;
            }
            if (s.version() == local.getVersion()) {
                return !Objects.equals(s.etag(), local.getEtag());
            }
            LOG.warn("replica ahead of primary (version fork): {}/{} on {} remoteV={} localV={}",
                    bucket, local.getKey(), replica, s.version(), local.getVersion());
            return false;
        }
        // 降级：逐对象 HEAD（T4：仅 NOT_FOUND 返回 empty，其余异常上抛由调用方按副本容错）
        Optional<ObjectMetadata> head = client.remoteHead(replica, bucket, local.getKey());
        if (head.isEmpty()) {
            return true;
        }
        ObjectMetadata h = head.get();
        if (h.getVersion() < local.getVersion()) {
            return true;
        }
        if (h.getVersion() == local.getVersion()) {
            return !Objects.equals(h.getEtag(), local.getEtag());
        }
        LOG.warn("replica ahead of primary (version fork): {}/{} on {} remoteV={} localV={}",
                bucket, local.getKey(), replica, h.getVersion(), local.getVersion());
        return false;
    }

    /**
     * 分页拉取远端某桶全量摘要；返回 null 表示不支持或拉取失败（调用方走 HEAD 降级）。
     * 空桶返回空 map（非 null）——区别于「不支持」。
     */
    private java.util.Map<String, StorageReplicationClient.ObjectSummary> fetchRemoteSummaries(
            NodeId replica, String bucket) {
        java.util.Map<String, StorageReplicationClient.ObjectSummary> acc = new java.util.HashMap<>();
        String marker = "";
        for (int page = 0; page < 10_000; page++) { // 页上限防御：异常翻页不无限循环
            List<StorageReplicationClient.ObjectSummary> summaries;
            try {
                summaries = client.remoteList(replica, bucket, marker, RECONCILE_PAGE_SIZE);
            } catch (UnsupportedOperationException unsupported) {
                return null; // 客户端不支持摘要列举 → 降级
            } catch (RuntimeException e) {
                LOG.warn("T14 remoteList failed replica {} bucket {}: {}", replica, bucket, e.getMessage());
                return null;
            }
            for (StorageReplicationClient.ObjectSummary s : summaries) {
                acc.put(s.key(), s);
                marker = s.key();
            }
            if (summaries.size() < RECONCILE_PAGE_SIZE) {
                return acc; // 末页
            }
        }
        LOG.warn("T14 remoteList page limit reached for replica {} bucket {} (partial view {})",
                replica, bucket, acc.size());
        return acc;
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    private void safeReplayBacklog() {
        try {
            replayBacklog();
        } catch (RuntimeException e) {
            LOG.warn("backlog replay round failed: {}", e.getMessage());
        }
    }

    private void safeReconcile() {
        try {
            reconcile();
        } catch (RuntimeException e) {
            LOG.warn("replication reconcile round failed: {}", e.getMessage());
        }
    }

    private boolean replicateOne(NodeId node, String bucket, String key) {
        ObjectStore.StoredObject obj = capability.getObject(bucket, key);
        if (obj == null) {
            return true; // 本地已无此对象（并发删除），删除复制由墓碑路径负责
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            try (InputStream in = capability.openRead(obj)) {
                if (!client.replicateObject(node, bucket, obj.meta, in)) {
                    return false;
                }
            } catch (Exception e) {
                LOG.warn("local read for replication failed: {}/{}: {}", bucket, key, e.getMessage());
                return false;
            }
            // T6 复核：远端 version+etag 与本地一致才算成功；不一致重读重传一次
            Optional<ObjectMetadata> remote = Optional.empty();
            try {
                remote = client.remoteHead(node, bucket, key);
            } catch (RuntimeException e) {
                LOG.warn("T6 replication verify head failed: {}/{} on {}: {}",
                        bucket, key, node, e.getMessage());
                return false;
            }
            boolean matches = remote.isPresent()
                    && remote.get().getVersion() == obj.meta.getVersion()
                    && Objects.equals(remote.get().getEtag(), obj.meta.getEtag());
            if (matches) {
                return true;
            }
            LOG.warn("T6 复制复核不匹配（attempt={}）：{}/{} on {} remote={} expected={}",
                    attempt, bucket, key, node,
                    remote.map(m -> m.getVersion() + "/" + m.getEtag()).orElse("absent"),
                    obj.meta.getVersion() + "/" + obj.meta.getEtag());
            obj = capability.getObject(bucket, key);
            if (obj == null) {
                return true; // 复核期间被删除，墓碑路径接管
            }
        }
        return false;
    }

    /** 该对象分区 REPLICA 持有者（holders[0]=PRIMARY，剔除 self）。 */
    private List<NodeId> replicasOf(String bucket, String key) {
        return replicasOfPartition(partitionOf(bucket, key));
    }

    private List<NodeId> replicasOfPartition(int partition) {
        List<PlacementView.Owner> holders = view.holders(CapabilityType.STORAGE, PartitionId.of(partition));
        if (holders.size() <= 1) {
            return List.of(); // 单副本或无放置
        }
        PlacementView.Owner primary = holders.getFirst();
        return holders.stream()
                .map(PlacementView.Owner::node)
                .filter(node -> !node.equals(primary.node()) && !node.equals(self))
                .filter(this::isServing)
                .toList();
    }

    /** 节点正在服务 STORAGE 能力（directory 为空时退化为不过滤，兼容单测/单机模式）。 */
    private boolean isServing(NodeId node) {
        if (directory == null) {
            return true;
        }
        return directory.servingPeers().contains(node);
    }

    private boolean isSelfPrimary(int partition) {
        Optional<PlacementView.Owner> owner = view.owner(CapabilityType.STORAGE, PartitionId.of(partition));
        return owner.isPresent() && owner.get().node().equals(self);
    }

    private int partitionOf(String bucket, String key) {
        return partitioner.applyAsInt(bucket + "/" + key);
    }

    private void enqueue(NodeId node, String bucket, String key, boolean delete, long version) {
        synchronized (backlogLock) {
            backlog.removeIf(e -> e.matches(node, bucket, key)); // 去重：只保留最新
            backlog.addLast(new BacklogEntry(node, bucket, key, delete, version, backlogRetryMs));
            while (backlog.size() > backlogMaxEntries) {
                BacklogEntry evicted = backlog.pollFirst();
                LOG.warn("replication backlog full, evicted oldest: {}", evicted);
            }
        }
    }

    private void remove(BacklogEntry entry) {
        synchronized (backlogLock) {
            backlog.remove(entry);
        }
    }

    private void defer(BacklogEntry entry, long now) {
        entry.attempts++;
        long backoff = Math.min(MAX_BACKOFF_MS, backlogRetryMs * (1L << Math.min(entry.attempts, 16)));
        entry.nextAttemptAtMs = now + backoff;
    }

    /** backlog 条目：复制意图（node 维度），带退避元数据与版本护栏。 */
    private static final class BacklogEntry {

        private final NodeId node;
        private final String bucket;
        private final String key;
        private final boolean delete;
        /** 入队时对应的对象版本（删除意图的护栏；PUT 条目不依赖该值重放）。 */
        private final long version;
        private int attempts;
        private long nextAttemptAtMs;

        BacklogEntry(NodeId node, String bucket, String key, boolean delete, long version,
                     long retryMs) {
            this.node = node;
            this.bucket = bucket;
            this.key = key;
            this.delete = delete;
            this.version = version;
            this.nextAttemptAtMs = System.currentTimeMillis() + retryMs;
        }

        boolean matches(NodeId node, String bucket, String key) {
            return this.node.equals(node) && this.bucket.equals(bucket) && this.key.equals(key);
        }

        @Override
        public String toString() {
            return node + ":" + bucket + "/" + key + (delete ? " [delete]" : " [put]");
        }
    }
}
