package com.ww.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式对象存储门面：HRW 路由 + 主节点本地写 + 同步副本复制 + read-after-write 一致性。
 *
 * <h3>一致性模型</h3>
 * <ul>
 *   <li><b>写</b>：HRW 选主 → 主节点本地原子写盘（.meta 后于 .data 暴露）→ 同步复制到副本节点；
 *       主节点本地写成功即 ack，复制失败不阻断（进入待重试队列按指数退避重放，另有周期对账
 *       反熵补复制，避免副本冗余度长期不足）。主节点本地写可见后，任何后续读（必走主节点）
 *       即可读到最新值 —— read-after-write。</li>
 *   <li><b>读</b>：HRW 选主 → 仅从主节点读（PRIMARY_ONLY 语义）。主节点故障时该 key 暂不可读
 *       （不 fallback 副本，避免读到陈旧数据破坏一致性）。</li>
 *   <li><b>删</b>：HRW 选主 → 主本地删 → 通知副本删。</li>
 *   <li><b>list</b>：广播所有节点拉取本地对象元数据，按 key 合并去重（取最大 version），全局应用 prefix/marker/maxKeys。</li>
 * </ul>
 *
 * <h3>等层级节点</h3>
 * 任一节点都可接收任一请求；非主节点按 HRW 透明转发到主节点，对客户端无感。
 *
 * <h3>单机模式</h3>
 * cluster 未启用或仅 1 节点时，NodeRouter.isDistributed()=false，所有操作直接走本地 ObjectStore，
 * 无 HTTP 转发/复制，仍保证 read-after-write（本地原子写 + 主读=本地读）。
 *
 * <h3>性能优化（JDK 21）</h3>
 * <ul>
 *   <li>副本复制：使用 {@link Executors#newVirtualThreadPerTaskExecutor()} 替代固定线程池，
 *       每个副本一个虚拟线程，IO 阻塞不占用平台线程</li>
 *   <li>分布式 list：并行（虚拟线程）拉取所有对端对象元数据，替代串行轮询</li>
 *   <li>本地元数据缓存：HEAD/GET 命中已有对象时跳过磁盘 IO；写/删时本地失效</li>
 * </ul>
 */
public class DistributedStorage {

    private static final Logger log = LoggerFactory.getLogger(DistributedStorage.class);

    private final BucketStore bucketStore;
    private final ObjectStore objectStore;
    private final NodeRouter nodeRouter;
    private final StorageRpcClient nodeClient;
    private final ObjectMetadataCache metaCache;
    private final SetLocator setLocator;

    /** list 合并时每节点的拉取上限：Integer.MAX_VALUE 表示全量（扫描本就 O(n)，输出截断会造成大桶对象静默缺失） */
    private static final int MERGE_SCAN_LIMIT = Integer.MAX_VALUE;
    /** 每个副本节点的待重试复制队列上限，满时淘汰最旧（保留最新状态） */
    private static final int BACKLOG_MAX_PER_NODE = 10_000;
    /** 待重试复制回放周期 */
    private static final long REPLAY_INTERVAL_MS = 5_000L;
    /** 节点连续失败时的最大退避 */
    private static final long REPLAY_MAX_BACKOFF_MS = 60_000L;
    /** 周期对账（反熵）间隔：对比主备 version，落后/缺失则补复制 */
    private static final long RECONCILE_INTERVAL_MS = 300_000L;

    /** 按副本节点聚合的待重试复制队列（内层 key=bucket+\0+key，put 覆盖=保留最新；满淘汰最旧） */
    private final Map<String, Map<String, PendingReplica>> replicationBacklog = new ConcurrentHashMap<>();
    /** 节点下次可回放时间戳（指数退避） */
    private final Map<String, Long> nextReplayAllowedAt = new ConcurrentHashMap<>();
    /** 节点当前退避时长 */
    private final Map<String, Long> replayBackoffMs = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService repairExecutor;

    /** 共享虚拟线程执行器：供复制/广播/list 等并行 IO 任务复用，避免每次新建销毁 */
    private final ExecutorService sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 维护态标志：扩缩容迁移期间为 true。
     * <p>注意：本标志仅作记录与查询，<b>不在本类内拦截读写</b>。维护态拦截放在客户端入口
     * ({@code StorageController})，使得内部复制端点与迁移逻辑（{@code forwardGet} 等）
     * 在维护态下仍可正常工作，避免迁移无法读取源节点数据。
     */
    private volatile boolean maintenance = false;

    public DistributedStorage(BucketStore bucketStore, ObjectStore objectStore,
                              NodeRouter nodeRouter, StorageRpcClient nodeClient,
                              ObjectMetadataCache metaCache, SetLocator setLocator) {
        this.bucketStore = bucketStore;
        this.objectStore = objectStore;
        this.nodeRouter = nodeRouter;
        this.nodeClient = nodeClient;
        this.metaCache = metaCache;
        this.setLocator = setLocator;
        refreshNodeIndex();
    }

    /** 重建后刷新内部缓存引用（供复制补偿回放/对账定位副本） */
    public void refreshNodeIndex() {
        // nodeRouter 内部已持有最新节点视图，无需额外索引
    }

    // ==================== Bucket ====================

    public BucketInfo createBucket(String name) {
        // 本地创建（幂等：已存在则返回既有）
        BucketInfo info = localCreateBucket(name);
        // 广播到对端（best-effort 并行，保证后续各节点 put 时 bucket 已就绪）
        broadcastToPeers(n -> nodeClient.forwardCreateBucket(n, name), "createBucket");
        return info;
    }

    public void deleteBucket(String name, boolean recursive) {
        localDeleteBucket(name, recursive);
        // 失效该 bucket 下所有元数据缓存
        if (metaCache != null) metaCache.invalidateBucket(name);
        broadcastToPeers(n -> nodeClient.forwardDeleteBucket(n, name, recursive), "deleteBucket");
    }

    public boolean bucketExists(String name) {
        return bucketStore.exists(name);
    }

    public BucketInfo getBucketInfo(String name) {
        return bucketStore.getBucketInfo(name);
    }

    public List<BucketInfo> listBuckets() {
        return bucketStore.listBuckets();
    }

    /** 本地创建 Bucket（幂等），供内部端点调用 */
    public BucketInfo localCreateBucket(String name) {
        if (bucketStore.exists(name)) {
            return bucketStore.getBucketInfo(name);
        }
        return bucketStore.createBucket(name);
    }

    /** 本地删除 Bucket（幂等），供内部端点调用 */
    public void localDeleteBucket(String name, boolean recursive) {
        if (!bucketStore.exists(name)) return;
        bucketStore.deleteBucket(name, recursive);
    }

    // ==================== 维护态 ====================

    /** 是否处于维护态（扩缩容迁移中） */
    public boolean isMaintenance() { return maintenance; }

    /** 设置维护态（由管理面调用） */
    public void setMaintenance(boolean m) {
        this.maintenance = m;
        log.info("storage 维护态切换: maintenance={}", m);
    }

    // ==================== Object ====================

    /**
     * 写对象。路由到主节点：主节点本地写 + 同步复制到副本。
     * @return 主节点落盘后的元数据（含 ETag/version）
     */
    public ObjectMetadata putObject(String bucket, String key, InputStream data, long length,
                                    String contentType, Map<String, String> userMeta) {
        ensureBucket(bucket);
        String routingKey = routingKey(bucket, key);
        StorageNode primary = nodeRouter.selectPrimary(routingKey);
        ObjectMetadata result;
        if (primary == null || primary.isSelf() || !nodeRouter.isDistributed()) {
            // 本节点为主或单机：本地写 + 复制
            ObjectStore.StoredObject stored = objectStore.putPrimary(bucket, key, data, length, contentType, userMeta);
            replicateToObject(bucket, key, stored);
            result = stored.meta;
        } else {
            // 非主：转发到主节点
            result = nodeClient.forwardPut(primary, bucket, key, () -> data, length, contentType, userMeta);
        }
        // 本地缓存更新（仅本节点为主时 result 来自本地；转发时主节点自己会更新其缓存）
        if (metaCache != null && result != null) {
            metaCache.put(bucket, key, result);
        }
        return result;
    }

    /**
     * 读对象。仅从主节点读（read-after-write）。
     * @return 读取句柄，对象不存在返回 null；调用方负责关闭 openStream 返回的流。
     */
    public ReadHandle getObject(String bucket, String key) {
        String routingKey = routingKey(bucket, key);
        StorageNode primary = nodeRouter.selectPrimary(routingKey);
        if (primary == null || primary.isSelf() || !nodeRouter.isDistributed()) {
            // 本地读：优先走元数据缓存（HEAD 路径），未命中则读盘
            ObjectStore.StoredObject stored = objectStore.get(bucket, key);
            return stored == null ? null : new LocalReadHandle(stored, objectStore, metaCache);
        }
        return nodeClient.forwardGet(primary, bucket, key);
    }

    public ObjectMetadata headObject(String bucket, String key) {
        String routingKey = routingKey(bucket, key);
        StorageNode primary = nodeRouter.selectPrimary(routingKey);
        if (primary == null || primary.isSelf() || !nodeRouter.isDistributed()) {
            // 本地 HEAD：优先缓存
            if (metaCache != null) {
                ObjectMetadata cached = metaCache.get(bucket, key);
                if (cached != null) return cached;
            }
            ObjectMetadata meta = objectStore.head(bucket, key);
            if (meta != null && metaCache != null) metaCache.put(bucket, key, meta);
            return meta;
        }
        return nodeClient.forwardHead(primary, bucket, key);
    }

    public void deleteObject(String bucket, String key) {
        String routingKey = routingKey(bucket, key);
        StorageNode primary = nodeRouter.selectPrimary(routingKey);
        if (primary == null || primary.isSelf() || !nodeRouter.isDistributed()) {
            objectStore.delete(bucket, key);
            if (metaCache != null) metaCache.invalidate(bucket, key);
            replicateDelete(bucket, key);
            return;
        }
        nodeClient.forwardDelete(primary, bucket, key);
    }

    /**
     * 递归删除指定前缀下的所有对象（用于删除文件夹）。
     * <p>列举该前缀下所有对象并逐个调用 deleteObject（已包含主节点路由 + 副本复制逻辑）。
     * @param prefix 文件夹前缀（必须以 / 结尾）
     * @return 实际删除的对象数
     */
    public int deletePrefix(String bucket, String prefix) {
        ensureBucket(bucket);
        ObjectStore.ListResult result = listObjects(bucket, prefix, null, MERGE_SCAN_LIMIT);
        List<ObjectMetadata> objects = result.getObjects();
        int count = 0;
        for (ObjectMetadata m : objects) {
            // 双保险：listObjects 已按 prefix 过滤，但再次校验以防止边界情况
            if (m.getKey().startsWith(prefix)) {
                deleteObject(bucket, m.getKey());
                count++;
            }
        }
        // 失效该前缀下的所有缓存（批量失效）
        if (metaCache != null) metaCache.invalidateBucket(bucket);
        log.info("deletePrefix: bucket={} prefix={} deleted={} objects", bucket, prefix, count);
        return count;
    }

    /**
     * 列举对象：合并所有节点的本地对象元数据，按 key 去重（取最大 version），全局应用过滤。
     * <p>对端拉取使用虚拟线程并行，替代串行轮询。
     */
    public ObjectStore.ListResult listObjects(String bucket, String prefix, String marker, int maxKeys) {
        if (maxKeys <= 0) maxKeys = 1000;
        TreeMap<String, ObjectMetadata> merged = new TreeMap<>();
        // 本地
        // 本地：全量拉取不截断（截断会导致大桶对象在合并时静默缺失，且 nextMarker 无法到达被截断的尾部）
        ObjectStore.ListResult local = objectStore.list(bucket, null, null, MERGE_SCAN_LIMIT);
        for (ObjectMetadata m : local.getObjects()) {
            mergeVersion(merged, m);
        }
        // 对端：并行拉取（JDK 21+ 使用虚拟线程，JDK 17 fallback 到 cached thread pool）
        if (nodeRouter.isDistributed()) {
            List<StorageNode> peers = new ArrayList<>();
            for (StorageNode n : nodeRouter.getNodes()) {
                if (!n.isSelf()) peers.add(n);
            }
            if (!peers.isEmpty()) {
                ExecutorService pool = sharedExecutor;
                {
                    List<Future<ObjectMetadata[]>> futures = new ArrayList<>(peers.size());
                    for (StorageNode n : peers) {
                        futures.add(pool.submit(() -> nodeClient.forwardListObjects(n, bucket)));
                    }
                    for (Future<ObjectMetadata[]> f : futures) {
                        try {
                            ObjectMetadata[] arr = f.get(30, TimeUnit.SECONDS);
                            if (arr != null) for (ObjectMetadata m : arr) mergeVersion(merged, m);
                        } catch (Exception e) {
                            log.warn("拉取对端对象列表失败: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        // 全局过滤
        List<ObjectMetadata> out = new ArrayList<>();
        String nextMarker = null;
        boolean truncated = false;
        for (ObjectMetadata m : merged.values()) {
            if (prefix != null && !m.getKey().startsWith(prefix)) continue;
            if (marker != null && m.getKey().compareTo(marker) <= 0) continue;
            if (out.size() >= maxKeys) {
                truncated = true;
                nextMarker = out.getLast().getKey();
                break;
            }
            out.add(m);
        }
        return new ObjectStore.ListResult(out, nextMarker, truncated);
    }

    // ==================== 副本接收（供内部端点调用） ====================

    /** 副本节点收到主节点复制来的对象：使用主节点元数据落盘 */
    public ObjectMetadata receiveReplica(String bucket, String key, InputStream data, long length, ObjectMetadata meta) {
        ObjectStore.StoredObject stored = objectStore.putReplica(bucket, key, data, length, meta);
        // 副本侧也缓存元数据，加速本节点 HEAD（虽然 HEAD 走主，但 list 等场景仍可能用到）
        if (metaCache != null) metaCache.put(bucket, key, stored.meta);
        return stored.meta;
    }

    /**
     * 副本节点收到主节点删除通知：本地删除对象 + 失效缓存，不触发再复制、不转发。
     * <p>与 {@link #deleteObject} 不同，本方法不进行 HRW 路由、不通知副本，
     * 仅执行本地删除，用于主→副本单向删除复制，避免循环转发。
     */
    public void receiveReplicaDelete(String bucket, String key) {
        objectStore.delete(bucket, key);
        if (metaCache != null) metaCache.invalidate(bucket, key);
    }

    // ==================== 内部：复制 ====================

    /**
     * 主节点本地写完成后，将对象数据复制到各副本节点（同步，并行；失败告警不阻断）。
     * <p>使用 {@link Executors#newVirtualThreadPerTaskExecutor()}：每个副本一个虚拟线程，
     * IO 阻塞期间不占用平台线程（虚拟线程），天然支持大并发副本复制。
     * JDK 17 环境下降级为 cached thread pool。
     */
    private void replicateToObject(String bucket, String key, ObjectStore.StoredObject stored) {
        if (!nodeRouter.isDistributed()) return;
        List<StorageNode> replicas = nodeRouter.selectReplicas(routingKey(bucket, key));
        if (replicas.isEmpty()) return;
        final ExecutorService pool = sharedExecutor;
        {
            List<Future<?>> futures = new ArrayList<>(replicas.size());
            for (StorageNode replica : replicas) {
                futures.add(pool.submit(() -> {
                    Supplier<InputStream> supplier = () -> openFile(new FileRef(stored.dataFile));
                    try {
                        nodeClient.replicatePut(replica, bucket, key, supplier, stored.meta.getSize(), stored.meta);
                    } catch (Exception e) {
                        log.warn("复制到副本失败 bucket={} key={} @ {}: {}，已入待重试队列",
                                bucket, key, replica.nodeKey(), e.getMessage());
                        enqueueReplica(replica, bucket, key, stored.dataFile, stored.meta);
                    }
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(5, TimeUnit.MINUTES); } catch (Exception ignored) { }
            }
        }
    }

    /** 副本删除 HTTP 超时（秒）：单次删除通知最长等待时间 */
    private static final long REPLICA_DELETE_TIMEOUT_SEC = 5;
    /** 副本删除并行等待超时（秒）：等待所有副本响应的总时间，略大于单次 HTTP 超时 */
    private static final long REPLICA_DELETE_AWAIT_SEC = REPLICA_DELETE_TIMEOUT_SEC + 1;

    private void replicateDelete(String bucket, String key) {
        if (!nodeRouter.isDistributed()) return;
        // 先清除该 key 的待重试复制与删除（防止删除后补偿线程复活已删对象）
        purgeReplica(bucket, key);
        List<StorageNode> replicas = nodeRouter.selectReplicas(routingKey(bucket, key));
        if (replicas.isEmpty()) return;

        long start = System.currentTimeMillis();
        int replicaCount = replicas.size();
        // nodeKey -> latencyMs，负数表示失败
        Map<String, Long> latencies = new ConcurrentHashMap<>();
        AtomicInteger okCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        // 并行通知副本删除（best-effort，失败不阻断）
        ExecutorService pool = sharedExecutor;
        List<Future<?>> futures = new ArrayList<>(replicaCount);
        for (StorageNode replica : replicas) {
            futures.add(pool.submit(() -> {
                String nodeKey = replica.nodeKey();
                long t0 = System.nanoTime();
                boolean ok = false;
                try {
                    nodeClient.replicateDelete(replica, bucket, key);
                    ok = true;
                    okCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    log.warn("副本删除通知失败 bucket={} key={} @ {}: {}，已入待重试队列",
                            bucket, key, nodeKey, e.getMessage());
                    enqueueDelete(replica, bucket, key);
                } finally {
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                    latencies.put(nodeKey, ok ? elapsedMs : -elapsedMs);
                }
            }));
        }
        // 等待所有副本并行完成（每个最多 REPLICA_DELETE_TIMEOUT_SEC 秒）
        for (Future<?> f : futures) {
            try {
                f.get(REPLICA_DELETE_AWAIT_SEC, TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                // 超时未完成：取消任务（中断 HTTP 请求），计入失败并入重试队列
                f.cancel(true);
            } catch (Exception ignored) {
                // 其他异常（ExecutionException 等）已在 lambda 内计入 failCount
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        int ok = okCount.get();
        int fail = failCount.get();
        if (fail > 0 || elapsed >= SLOW_BROADCAST_THRESHOLD_MS) {
            List<String> failedNodes = new ArrayList<>();
            List<String> slowNodes = new ArrayList<>();
            for (Map.Entry<String, Long> e : latencies.entrySet()) {
                long lat = e.getValue();
                if (lat < 0) {
                    failedNodes.add(e.getKey() + "(" + (-lat) + "ms)");
                } else if (lat >= SLOW_BROADCAST_THRESHOLD_MS) {
                    slowNodes.add(e.getKey() + "=" + lat + "ms");
                }
            }
            log.warn("副本删除通知完成 bucket={} key={}: replicas={}, ok={}, fail={}, total={}ms{}{}",
                    bucket, key, replicaCount, ok, fail, elapsed,
                    fail > 0 ? ", failedNodes=" + failedNodes : "",
                    slowNodes.isEmpty() ? "" : ", slowNodes=" + slowNodes);
        } else {
            log.debug("副本删除通知完成 bucket={} key={}: replicas={}, ok={}, fail={}, total={}ms",
                    bucket, key, replicaCount, ok, fail, elapsed);
        }
    }

    // ==================== 复制失败补偿（backlog 重试 + 周期对账） ====================

    /**
     * 复制失败的补偿机制：
     * <ul>
     *   <li>入队：主写复制失败时，将 (bucket,key,dataFile,meta) 入对应副本节点的待重试队列
     *       （按 key 去重保留最新，满淘汰最旧）</li>
     *   <li>回放：后台线程周期重放；回放前校验本地对象仍存在且 version 未变
     *       （防止复活被删/被覆盖的对象旧数据）</li>
     *   <li>对账：周期扫描本地主对象，HEAD 比对副本 version，落后/缺失则补复制（反熵兜底）</li>
     * </ul>
     * 配合 {@link ObjectStore#putReplica} 的版本幂等保护，重放安全且可并发。
     */

    private void enqueueReplica(StorageNode replica, String bucket, String key,
                                java.io.File dataFile, ObjectMetadata meta) {
        Map<String, PendingReplica> q = backlogFor(replica.nodeKey());
        q.put(bucket + "\0" + key, new PendingReplica(bucket, key, dataFile, meta));
    }

    private void purgeReplica(String bucket, String key) {
        String k = bucket + "\0" + key;
        for (Map<String, PendingReplica> q : replicationBacklog.values()) {
            q.remove(k);
        }
        purgeDelete(bucket, key);
    }

    private Map<String, PendingReplica> backlogFor(String nodeKey) {
        return replicationBacklog.computeIfAbsent(nodeKey, k ->
                Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, false) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, PendingReplica> eldest) {
                        return size() > BACKLOG_MAX_PER_NODE;
                    }
                }));
    }

    /** 待重试删除队列：副本删除通知失败时入队，周期重放避免已删对象残留 */
    private final Map<String, Set<String>> deleteBacklog = new ConcurrentHashMap<>();
    /** 删除节点下次可回放时间戳 */
    private final Map<String, Long> deleteNextReplayAt = new ConcurrentHashMap<>();
    /** 删除节点当前退避时长 */
    private final Map<String, Long> deleteBackoffMs = new ConcurrentHashMap<>();

    private void enqueueDelete(StorageNode replica, String bucket, String key) {
        Set<String> q = deleteBacklog.computeIfAbsent(replica.nodeKey(),
                k -> Collections.synchronizedSet(new java.util.LinkedHashSet<>()));
        q.add(bucket + "\0" + key);
        if (q.size() > BACKLOG_MAX_PER_NODE) {
            synchronized (q) {
                java.util.Iterator<String> it = q.iterator();
                while (it.hasNext() && q.size() > BACKLOG_MAX_PER_NODE) {
                    it.next();
                    it.remove();
                }
            }
        }
    }

    private void purgeDelete(String bucket, String key) {
        String k = bucket + "\0" + key;
        for (Set<String> q : deleteBacklog.values()) {
            q.remove(k);
        }
    }

    /** 回放删除待重试队列：对副本重新发送删除通知，补偿之前失败的删除 */
    private void replayDeleteBacklog() {
        for (Map.Entry<String, Set<String>> e : deleteBacklog.entrySet()) {
            String nodeKey = e.getKey();
            StorageNode replica = nodeRouter.getNode(nodeKey);
            if (replica == null) continue;
            if (System.currentTimeMillis() < deleteNextReplayAt.getOrDefault(nodeKey, 0L)) continue;
            Set<String> q = e.getValue();
            if (q.isEmpty()) continue;
            List<String> pending;
            synchronized (q) {
                pending = new ArrayList<>(q);
            }
            boolean failed = false;
            for (String bk : pending) {
                String[] parts = bk.split("\0", 2);
                if (parts.length != 2) { q.remove(bk); continue; }
                String bkt = parts[0], objKey = parts[1];
                // 回放前校验本地状态：本地仍存在才重放删除
                if (objectStore.head(bkt, objKey) != null) {
                    q.remove(bk);
                    continue;
                }
                try {
                    nodeClient.replicateDelete(replica, bkt, objKey);
                    q.remove(bk);
                } catch (Exception ex) {
                    log.warn("重试删除失败 bucket={} key={} @ {}: {}", bkt, objKey, nodeKey, ex.getMessage());
                    failed = true;
                    break;
                }
            }
            if (failed) {
                long b = Math.min(REPLAY_MAX_BACKOFF_MS,
                        deleteBackoffMs.getOrDefault(nodeKey, REPLAY_INTERVAL_MS) * 2);
                deleteBackoffMs.put(nodeKey, b);
                deleteNextReplayAt.put(nodeKey, System.currentTimeMillis() + b);
            } else {
                deleteBackoffMs.put(nodeKey, REPLAY_INTERVAL_MS);
            }
        }
    }

    /** 回放复制失败待重试队列：对副本重新发送对象数据，按指数退避重试 */
    private void replayBacklog() {
        for (Map.Entry<String, Map<String, PendingReplica>> e : replicationBacklog.entrySet()) {
            String nodeKey = e.getKey();
            StorageNode replica = nodeRouter.getNode(nodeKey);
            if (replica == null) continue;
            if (System.currentTimeMillis() < nextReplayAllowedAt.getOrDefault(nodeKey, 0L)) continue;
            Map<String, PendingReplica> q = e.getValue();
            List<PendingReplica> pending;
            synchronized (q) {
                pending = new ArrayList<>(q.values());
            }
            if (pending.isEmpty()) continue;
            boolean failed = false;
            for (PendingReplica p : pending) {
                String k = p.bucket() + "\0" + p.key();
                // 回放前校验本地状态：对象被删除或 version 已变化则丢弃该待办
                ObjectMetadata cur = objectStore.head(p.bucket(), p.key());
                if (cur == null || cur.getVersion() != p.meta().getVersion()) {
                    q.remove(k);
                    continue;
                }
                try {
                    nodeClient.replicatePut(replica, p.bucket(), p.key(),
                            () -> openFile(new FileRef(p.dataFile())), p.meta().getSize(), p.meta());
                    q.remove(k);
                } catch (Exception ex) {
                    log.warn("重试复制失败 bucket={} key={} @ {}: {}", p.bucket(), p.key(), nodeKey, ex.getMessage());
                    failed = true;
                    break;
                }
            }
            if (failed) {
                long b = Math.min(REPLAY_MAX_BACKOFF_MS, replayBackoffMs.getOrDefault(nodeKey, REPLAY_INTERVAL_MS) * 2);
                replayBackoffMs.put(nodeKey, b);
                nextReplayAllowedAt.put(nodeKey, System.currentTimeMillis() + b);
            } else {
                replayBackoffMs.put(nodeKey, REPLAY_INTERVAL_MS);
            }
        }
    }

    /** 周期对账（反熵）：本地为主（primary）的对象，若副本缺失或 version 落后则补复制 */
    private void reconcile() {
        if (!nodeRouter.isDistributed()) return;
        try {
            doReconcile();
        } catch (Exception e) {
            log.warn("周期对账异常", e);
        }
    }

    /** 对账并发度：限制同时进行的副本补复制数量，防止带宽打爆 */
    private static final int RECONCILE_CONCURRENCY = 16;

    /**
     * 周期对账（反熵）：本地为主（primary）的对象，若副本缺失或 version 落后则补复制。
     * 复用 list 返回的元数据（含完整字段），避免二次读盘，仅通过 keySha1 推算数据文件路径。
     */
    private void doReconcile() {
        List<Runnable> tasks = new ArrayList<>();
        for (BucketInfo b : bucketStore.listBuckets()) {
            ObjectStore.ListResult r = objectStore.list(b.getName(), null, null, MERGE_SCAN_LIMIT);
            for (ObjectMetadata m : r.getObjects()) {
                if (!nodeRouter.isPrimary(routingKey(b.getName(), m.getKey()))) continue;
                String bucket = b.getName();
                String key = m.getKey();
                long version = m.getVersion();
                String sha1 = setLocator.keySha1(bucket, key);
                Drive drive = setLocator.locateBySha1(sha1);
                java.io.File df = drive.objectDataFile(bucket, sha1);
                long size = m.getSize();
                ObjectMetadata meta = m;
                List<StorageNode> replicas = nodeRouter.selectReplicas(routingKey(bucket, key));
                for (StorageNode replica : replicas) {
                    tasks.add(() -> {
                        try {
                            ObjectMetadata h = nodeClient.forwardHead(replica, bucket, key);
                            if (h != null && h.getVersion() >= version) return;
                            nodeClient.replicatePut(replica, bucket, key,
                                    () -> openFile(new FileRef(df)), size, meta);
                            log.info("对账补复制完成 bucket={} key={} @ {}", bucket, key, replica.nodeKey());
                        } catch (Exception e) {
                            log.warn("对账补复制失败 bucket={} key={} @ {}: {}",
                                    bucket, key, replica.nodeKey(), e.getMessage());
                        }
                    });
                }
            }
        }
        if (tasks.isEmpty()) return;
        // 虚拟线程并行执行，Semaphore 限流
        Semaphore sem = new Semaphore(RECONCILE_CONCURRENCY);
        ExecutorService pool = sharedExecutor;
        {
            List<Future<?>> futures = new ArrayList<>(tasks.size());
            for (Runnable task : tasks) {
                futures.add(pool.submit(() -> {
                    try {
                        sem.acquire();
                        try {
                            task.run();
                        } finally {
                            sem.release();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(10, TimeUnit.MINUTES); } catch (Exception ignored) { }
            }
        }
    }

    /**
     * 立即触发一次对账（不等 5min 周期）。供 failover 触发后调用，加速副本补齐。
     * <p>异步执行（虚拟线程），不阻塞调用方。
     */
    public void triggerReconcileNow() {
        if (!nodeRouter.isDistributed()) return;
        Thread.ofVirtual().name("storage-reconcile-now").start(() -> {
            try {
                log.info("failover 触发立即对账，开始");
                doReconcile();
                log.info("failover 触发立即对账，完成");
            } catch (Exception e) {
                log.warn("failover 触发立即对账失败", e);
            }
        });
    }

    /** 反向同步并发度：GET 传输数据，较低并发避免占满带宽 */
    private static final int REVERSE_RECONCILE_CONCURRENCY = 8;

    /**
     * 原主反向同步：本节点（恢复的原 primary）遍历本地对象，对新拓扑中不再是自己 primary 的对象，
     * 从新 primary 拉取 version 落后的数据补齐本地（作为副本）。
     * 若新 primary 上该对象已不存在（已删除），则删除本地陈旧副本，防止已删对象复活。
     */
    public int reverseReconcile() {
        if (!nodeRouter.isDistributed()) return 0;
        AtomicInteger diff = new AtomicInteger(0);
        AtomicInteger deleted = new AtomicInteger(0);
        log.info("反向同步开始：遍历本地对象，从新 primary 补齐落后数据或删除已删对象");
        List<Runnable> tasks = new ArrayList<>();
        try {
            for (BucketInfo b : bucketStore.listBuckets()) {
                ObjectStore.ListResult r = objectStore.list(b.getName(), null, null, MERGE_SCAN_LIMIT);
                for (ObjectMetadata m : r.getObjects()) {
                    String rk = routingKey(b.getName(), m.getKey());
                    StorageNode newPrimary = nodeRouter.selectPrimary(rk);
                    if (newPrimary == null || newPrimary.isSelf()) continue;
                    String bucket = b.getName();
                    String key = m.getKey();
                    long localVersion = m.getVersion();
                    tasks.add(() -> {
                        try {
                            ObjectMetadata remoteMeta = nodeClient.forwardHead(newPrimary, bucket, key);
                            if (remoteMeta == null) {
                                // 新 primary 上不存在：本地对象已是陈旧数据，删除防止已删对象复活
                                objectStore.delete(bucket, key);
                                deleted.incrementAndGet();
                                return;
                            }
                            if (remoteMeta.getVersion() > localVersion) {
                                ReadHandle ro = nodeClient.forwardGet(newPrimary, bucket, key);
                                if (ro != null) {
                                    try (InputStream in = ro.openStream()) {
                                        objectStore.putReplica(bucket, key, in,
                                                ro.metadata().getSize(), ro.metadata());
                                        diff.incrementAndGet();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.warn("反向同步失败 bucket={} key={}: {}", bucket, key, e.getMessage());
                        }
                    });
                }
            }
        } catch (Exception e) {
            log.warn("反向同步扫描异常", e);
            return diff.get();
        }
        if (!tasks.isEmpty()) {
            Semaphore sem = new Semaphore(REVERSE_RECONCILE_CONCURRENCY);
            ExecutorService pool = sharedExecutor;
            {
                List<Future<?>> futures = new ArrayList<>(tasks.size());
                for (Runnable task : tasks) {
                    futures.add(pool.submit(() -> {
                        try {
                            sem.acquire();
                            try { task.run(); } finally { sem.release(); }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    try { f.get(10, TimeUnit.MINUTES); } catch (Exception ignored) { }
                }
            }
        }
        log.info("反向同步完成：补齐对象数={}，删除陈旧对象数={}", diff.get(), deleted.get());
        return diff.get();
    }

    /** 启动复制补偿调度（复制失败重试 + 删除重试 + 周期对账）。分布式模式下由装配层调用。 */
    public synchronized void startRepair() {
        if (repairExecutor != null) return;
        repairExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "storage-repair");
            t.setDaemon(true);
            return t;
        });
        repairExecutor.scheduleWithFixedDelay(this::replayBacklog,
                REPLAY_INTERVAL_MS, REPLAY_INTERVAL_MS, TimeUnit.MILLISECONDS);
        repairExecutor.scheduleWithFixedDelay(this::replayDeleteBacklog,
                REPLAY_INTERVAL_MS, REPLAY_INTERVAL_MS, TimeUnit.MILLISECONDS);
        repairExecutor.scheduleWithFixedDelay(this::reconcile,
                RECONCILE_INTERVAL_MS, RECONCILE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("storage 复制补偿调度已启动: replay={}ms, reconcile={}ms", REPLAY_INTERVAL_MS, RECONCILE_INTERVAL_MS);
    }

    /** 停止复制补偿调度并关闭共享执行器 */
    public synchronized void stopRepair() {
        if (repairExecutor != null) {
            repairExecutor.shutdownNow();
            repairExecutor = null;
        }
        shutdownExecutor(sharedExecutor);
    }

    // ==================== 工具 ====================

    private void ensureBucket(String bucket) {
        if (!bucketStore.exists(bucket)) {
            throw new StorageException("Bucket 不存在: " + bucket);
        }
    }

    /** 慢广播阈值（ms）：单个节点操作超过此值将记录在慢节点列表中 */
    private static final long SLOW_BROADCAST_THRESHOLD_MS = 3000L;

    /**
     * 广播操作到所有对端节点（虚拟线程并行，best-effort）。
     * <p>性能日志：记录总耗时、成功/失败数、慢节点列表。
     */
    private void broadcastToPeers(Consumer<StorageNode> action, String opName) {
        if (!nodeRouter.isDistributed()) return;
        List<StorageNode> peers = new ArrayList<>();
        for (StorageNode n : nodeRouter.getNodes()) {
            if (!n.isSelf()) peers.add(n);
        }
        if (peers.isEmpty()) return;

        long batchStart = System.currentTimeMillis();
        int peerCount = peers.size();
        // nodeKey -> latencyMs，负数表示失败
        Map<String, Long> latencies = new ConcurrentHashMap<>();
        AtomicInteger okCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        ExecutorService pool = sharedExecutor;
        {
            List<Future<?>> futures = new ArrayList<>(peerCount);
            for (StorageNode n : peers) {
                futures.add(pool.submit(() -> {
                    String nodeKey = n.nodeKey();
                    long start = System.nanoTime();
                    boolean ok = false;
                    try {
                        action.accept(n);
                        ok = true;
                        okCount.incrementAndGet();
                    } catch (Exception e) {
                        failCount.incrementAndGet();
                        log.warn("{} 广播失败 @ {}: {}", opName, nodeKey, e.getMessage());
                    } finally {
                        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                        latencies.put(nodeKey, ok ? elapsedMs : -elapsedMs);
                    }
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(30, TimeUnit.SECONDS); } catch (Exception ignored) { }
            }
        }

        long batchElapsed = System.currentTimeMillis() - batchStart;
        int ok = okCount.get();
        int fail = failCount.get();
        List<String> slowNodes = new ArrayList<>();
        List<String> failedNodes = new ArrayList<>();
        for (Map.Entry<String, Long> e : latencies.entrySet()) {
            long lat = e.getValue();
            if (lat < 0) {
                failedNodes.add(e.getKey() + "(" + (-lat) + "ms)");
            } else if (lat >= SLOW_BROADCAST_THRESHOLD_MS) {
                slowNodes.add(e.getKey() + "=" + lat + "ms");
            }
        }
        if (fail > 0 || batchElapsed >= SLOW_BROADCAST_THRESHOLD_MS || !slowNodes.isEmpty()) {
            log.warn("{} 广播完成: nodes={}, ok={}, fail={}, total={}ms{}{}",
                    opName, peerCount, ok, fail, batchElapsed,
                    fail > 0 ? ", failedNodes=" + failedNodes : "",
                    slowNodes.isEmpty() ? "" : ", slowNodes=" + slowNodes);
        } else {
            log.debug("{} 广播完成: nodes={}, ok={}, fail={}, total={}ms",
                    opName, peerCount, ok, fail, batchElapsed);
        }
    }

    private static void shutdownExecutor(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
    }

    private static String routingKey(String bucket, String key) {
        return bucket + "/" + key;
    }

    private static void mergeVersion(TreeMap<String, ObjectMetadata> sink, ObjectMetadata m) {
        if (m == null || m.getKey() == null) return;
        ObjectMetadata existing = sink.get(m.getKey());
        if (existing == null || m.getVersion() > existing.getVersion()) {
            sink.put(m.getKey(), m);
        }
    }

    /** 打开本地数据文件为原始流。返回裸 {@link FileInputStream} 使副本复制路径
     *  可触发 FileChannel.transferFrom 零拷贝（sendfile），避免 BufferedInputStream 阻断 instanceof 判断 */
    private static InputStream openFile(FileRef ref) {
        try {
            return new FileInputStream(ref.file);
        } catch (IOException e) {
            throw new StorageException("打开本地数据文件失败: " + ref.file, e);
        }
    }

    /**
     * 本地读句柄：暴露 {@link StoredObject#dataFile} 供 Controller 触发 Tomcat sendfile 零拷贝响应；
     * {@link #openStream()} 仅在 sendfile 不可用时使用。
     */
    private static class LocalReadHandle implements ReadHandle {
        private final ObjectStore.StoredObject stored;
        private final ObjectStore objectStore;
        private final ObjectMetadataCache metaCache;

        LocalReadHandle(ObjectStore.StoredObject stored, ObjectStore objectStore, ObjectMetadataCache metaCache) {
            this.stored = stored;
            this.objectStore = objectStore;
            this.metaCache = metaCache;
        }

        @Override
        public ObjectMetadata metadata() {
            // 顺带回填缓存（GET 时未走 head 缓存路径）
            if (metaCache != null) metaCache.put(stored.bucket, stored.meta.getKey(), stored.meta);
            return stored.meta;
        }

        @Override
        public InputStream openStream() throws IOException {
            return objectStore.openRead(stored);
        }

        @Override
        public java.io.File file() {
            return stored.dataFile;
        }
    }

    /** 文件引用包装，便于 lambda 捕获 */
    private record FileRef(java.io.File file) { }

    /** 待重试复制的对象引用（入队时快照本地文件路径与主节点元数据） */
    private record PendingReplica(String bucket, String key, java.io.File dataFile, ObjectMetadata meta) { }
}
