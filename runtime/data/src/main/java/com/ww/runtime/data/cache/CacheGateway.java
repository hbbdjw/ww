package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.GatewayErrors;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToIntFunction;

/**
 * CACHE 网关：任意节点接收 KV 请求后按 placement 自动路由——
 * 分区属主为本机走本地引擎，否则经 gRPC 转发到属主节点（客户端无需感知拓扑）。
 *
 * <h2>路由规则</h2>
 * <ul>
 *   <li>写（PUT/PUT_IF_ABSENT/DELETE）：无放置或属主为本机 → 本地（属主端 fencing 生效）；
 *       属主在远端 → Forward*（远端 fencing 拒绝经 stale-epoch 标记还原为
 *       {@link StaleEpochException} → HTTP 409）；</li>
 *   <li>读（GET）：路由到属主（read-after-write 严格一致）；无放置时本地尝试；</li>
 *   <li><b>切主收敛（调用端无感）</b>：cutover 在途请求命中旧属主会被
 *       stale-epoch / not-owner 拒绝——网关强制刷新本地 placement 视图后
 *       <b>重取属主重试一次</b>收敛到新属主，仍失败才向客户端抛 409
 *       （控制面放置表为唯一真源；数据面读本地修订快照，不逐调用查库）；</li>
 *   <li>列举/计数（listKeys/countKeys）：fan-out 到正在服务 CACHE 的存活节点合并（每 key 仅属
 *       一个分区，合并去重后即为集群视图；能力已停用的节点不参与，单 peer 失败仅日志降级，
 *       与 StorageGateway.list 同约定）；</li>
 *   <li><b>P0-2 隔离门</b>：分区 DEGRADED（CACHE 为 RF=1 单副本，属主死亡且治愈宽限期
 *       赌输）时读写抛 {@link com.ww.runtime.common.PartitionDegradedException}（503），
 *       放置保持指向死亡属主押注重启，不再以空分区静默覆盖；</li>
 * </ul>
 *
 * <p>{@code client}/{@code directory} 为 null 时退化为本地单机模式（单测/单节点）。</p>
 *
 * <p>C17：管理面 fan-out（listKeys/countKeys/deleteByPrefix）使用专用有界池
 * {@code cache-fanout}——旧实现跑在 ForkJoinPool.commonPool 上，慢/宕 peer 的
 * 30s deadline 会把 commonPool 占满，饿死同 JVM 全部公共池任务（含 Stream 唤醒），
 * 造成节点级故障扩散。所有等待带超时（deadline+余量），池饱和时由
 * {@code join} 超时降级为单 peer 失败。</p>
 */
public final class CacheGateway implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheGateway.class);

    /** C17：fan-out 等待超时 = 客户端 deadline 30s + 余量。 */
    private static final long FANOUT_TIMEOUT_MS = 35_000L;

    /**
     * C17/§3.17：cache-fanout 专用池——小容量长阻塞场景（管理面低频），
     * 队列满由提交线程回退执行（CallerRuns：最坏退化为同步，不丢任务）。
     */
    private static final int FANOUT_POOL_SIZE = 4;
    private final java.util.concurrent.ThreadPoolExecutor fanoutExecutor;
    /** C17：fan-out 超时/降级计数（池指标，接入日志与监控）。 */
    private final java.util.concurrent.atomic.AtomicLong fanoutDegraded =
            new java.util.concurrent.atomic.AtomicLong();

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final CacheCapability local;
    private final CacheCommandExecutor localCommands;
    private final CacheGrpcClient client;
    private final CachePeerDirectory directory;
    /** P0-2 分区隔离视图（可空：单测/单机模式不启用隔离）；带 TTL 的内存快照，热路径无 DB 访问。 */
    private final com.ww.runtime.capability.DegradedPartitionView degradedView;

    public CacheGateway(NodeId self,
                        PlacementView view,
                        ToIntFunction<String> partitioner,
                        CacheCapability local,
                        CacheGrpcClient client,
                        CachePeerDirectory directory) {
        this(self, view, partitioner, local, client, directory, null);
    }

    public CacheGateway(NodeId self,
                        PlacementView view,
                        ToIntFunction<String> partitioner,
                        CacheCapability local,
                        CacheGrpcClient client,
                        CachePeerDirectory directory,
                        com.ww.runtime.capability.DegradedPartitionView degradedView) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.local = Objects.requireNonNull(local, "local");
        this.localCommands = new CacheCommandExecutor(local);
        this.client = client;
        this.directory = directory;
        this.degradedView = degradedView;
        java.util.concurrent.ThreadPoolExecutor pool = new java.util.concurrent.ThreadPoolExecutor(
                FANOUT_POOL_SIZE, FANOUT_POOL_SIZE, 60L, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(256),
                r -> {
                    Thread t = new Thread(r, "cache-fanout");
                    t.setDaemon(true);
                    return t;
                },
                new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        pool.allowCoreThreadTimeOut(true);
        this.fanoutExecutor = pool;
    }

    /** C17：fan-out 降级计数（观测口）。 */
    public long getFanoutDegraded() {
        return fanoutDegraded.get();
    }

    /** 停机释放 fan-out 池（幂等；daemon 线程本就随 JVM 退出，显式关闭缩短窗口）。 */
    @Override
    public void close() {
        fanoutExecutor.shutdownNow();
    }

    // ------------------------------------------------------------------
    // KV 数据面
    // ------------------------------------------------------------------

    /**
     * 网关写：路由到属主（本机或转发）。cutover 在途的 stale-epoch 拒绝
     * 重取属主重试一次（调用端无感），仍失败才抛 409 语义。
     */
    public void put(String key, Object value, long ttlMillis) {
        checkNotDegraded(key);
        try {
            putOnce(key, value, ttlMillis);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            putOnce(key, value, ttlMillis);
        }
    }

    /**
     * 网关“不存在才写”：路由到属主执行原子判断；stale-epoch 重取属主重试一次。
     *
     * @return {@code true} 表示本次写入；{@code false} 表示 key 已存在
     */
    public boolean putIfAbsent(String key, Object value, long ttlMillis) {
        checkNotDegraded(key);
        try {
            return putIfAbsentOnce(key, value, ttlMillis);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return putIfAbsentOnce(key, value, ttlMillis);
        }
    }

    /** 网关读：返回 null 表示不存在；路由错位重取属主重试一次。 */
    public Object get(String key) {
        checkNotDegraded(key);
        try {
            return getOnce(key);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return getOnce(key);
        }
    }

    /** 网关删除：返回 key 原本存在并已删除；路由错位重取属主重试一次。 */
    public boolean remove(String key) {
        checkNotDegraded(key);
        try {
            return removeOnce(key);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return removeOnce(key);
        }
    }

    /**
     * P0-2 隔离门：分区 DEGRADED（属主死亡且无存活副本——CACHE 为 RF=1 单副本，
     * 属主死亡即整体不可用）时拒绝数据面流量：放置保持指向死亡属主（数据留盘
     * 押注重启），不静默空读/空写。无视图（单机/测试）时直通。
     */
    private void checkNotDegraded(String key) {
        if (degradedView == null) {
            return;
        }
        int partition = partitioner.applyAsInt(key);
        if (degradedView.isDegraded(CapabilityType.CACHE.name(), partition)) {
            Optional<PlacementView.Owner> owner =
                    view.owner(CapabilityType.CACHE, PartitionId.of(partition));
            throw new com.ww.runtime.common.PartitionDegradedException(
                    CapabilityType.CACHE.name(), partition,
                    owner.map(o -> o.node().value()).orElse("unknown"));
        }
    }

    private void putOnce(String key, Object value, long ttlMillis) {
        NodeId owner = ownerNode(key);
        if (owner == null || owner.equals(self)) {
            local.put(key, value, ttlMillis);
            return;
        }
        requireClient();
        try {
            client.forwardPut(owner, key, value, ttlMillis);
        } catch (StatusRuntimeException e) {
            throw GatewayErrors.translate(e, "put", key);
        }
    }

    private boolean putIfAbsentOnce(String key, Object value, long ttlMillis) {
        NodeId owner = ownerNode(key);
        if (owner == null || owner.equals(self)) {
            return local.putIfAbsent(key, value, ttlMillis);
        }
        requireClient();
        try {
            return client.forwardPutIfAbsent(owner, key, value, ttlMillis);
        } catch (StatusRuntimeException e) {
            throw GatewayErrors.translate(e, "putIfAbsent", key);
        }
    }

    private Object getOnce(String key) {
        NodeId owner = ownerNode(key);
        if (owner == null || owner.equals(self)) {
            if (owner != null && !local.servesPartitionOf(key)) {
                // ownerNode 查询与本地读之间发生切主：按路由错位语义重试
                throw new StaleEpochException("not-owner: partition of '" + key + "' moved");
            }
            return local.get(key);
        }
        requireClient();
        try {
            return client.forwardGet(owner, key)
                    .map(CacheGrpcClient.CacheValue::value)
                    .orElse(null);
        } catch (StatusRuntimeException e) {
            throw GatewayErrors.translate(e, "get", key);
        }
    }

    private boolean removeOnce(String key) {
        NodeId owner = ownerNode(key);
        if (owner == null || owner.equals(self)) {
            return local.remove(key);
        }
        requireClient();
        try {
            return client.forwardDelete(owner, key);
        } catch (StatusRuntimeException e) {
            throw GatewayErrors.translate(e, "delete", key);
        }
    }

    // ------------------------------------------------------------------
    // 统一命令通道
    // ------------------------------------------------------------------

    /**
     * 统一命令入口：按命令首个 key 路由到属主；MGET 逐 key 路由，
     * MSET 要求同分区（否则 CROSSSLOT），STATS 聚合集群视图，SCAN 为节点本地。
     */
    public Object executeCommand(CacheCommandRequest request) {
        String command = request.command();
        if ("STATS".equals(command)) {
            return clusterStats();
        }
        if (CacheCommandSpec.isKeyless(command)) {
            return unwrap(localCommands.execute(request));
        }
        if ("MGET".equals(command)) {
            List<Object> values = new ArrayList<>();
            for (String key : CacheCommandSpec.keys(request)) {
                values.add(executeSingle(CacheCommandRequest.of("GET", key)));
            }
            return values;
        }
        if ("MSET".equals(command)) {
            List<String> keys = CacheCommandSpec.keys(request);
            if (keys.isEmpty()) {
                throw new CacheCommandException(CacheCommandException.INVALID_ARGUMENT,
                        "MSET requires key/value pairs");
            }
            int partition = partitionOf(keys.get(0));
            for (String key : keys) {
                checkNotDegraded(key);
                if (partitionOf(key) != partition) {
                    throw new CacheCommandException(CacheCommandException.CROSSSLOT,
                            "MSET keys span multiple partitions");
                }
            }
        }
        return executeSingle(request);
    }

    private Object executeSingle(CacheCommandRequest request) {
        List<String> keys = CacheCommandSpec.keys(request);
        if (keys.isEmpty()) {
            throw new CacheCommandException(CacheCommandException.INVALID_ARGUMENT,
                    request.command() + " requires a key");
        }
        String key = keys.get(0);
        checkNotDegraded(key);
        try {
            return routeCommandOnce(request, key);
        } catch (StaleEpochException e) {
            view.refreshRoutingView();
            return routeCommandOnce(request, key);
        }
    }

    private Object routeCommandOnce(CacheCommandRequest request, String key) {
        NodeId owner = ownerNode(key);
        if (owner == null || owner.equals(self)) {
            return unwrap(localCommands.execute(request));
        }
        requireClient();
        return client.forwardCommand(owner, request);
    }

    private static Object unwrap(CacheCommandResponse response) {
        if (!response.ok()) {
            throw new CacheCommandException(
                    response.code() == null ? CacheCommandException.COMMAND_FAILED : response.code(),
                    response.message());
        }
        return response.result();
    }

    /**
     * 集群统计：本机 + 各 serving peer 的 STATS 聚合成 nodes 列表；单 peer 失败
     * 只记录错误并计入 degraded，不拖垮整体观测。
     */
    public Map<String, Object> clusterStats() {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Object> nodes = new ArrayList<>();
        List<Object> errors = new ArrayList<>();
        Map<String, Object> selfStats = new LinkedHashMap<>();
        selfStats.put("nodeId", self.value());
        selfStats.put("stats", local.isStarted() ? local.stats() : Map.of("started", false));
        nodes.add(selfStats);

        if (directory != null && client != null) {
            List<NodeId> peers = directory.servingPeers().stream()
                    .filter(p -> !p.equals(self)).toList();
            if (!peers.isEmpty()) {
                List<java.util.concurrent.CompletableFuture<Map<String, Object>>> futures =
                        peers.stream()
                                .map(peer -> java.util.concurrent.CompletableFuture
                                        .supplyAsync(() -> {
                                            try {
                                                Object result = client.forwardCommand(peer,
                                                        CacheCommandRequest.of("STATS"));
                                                Map<String, Object> node = new LinkedHashMap<>();
                                                node.put("nodeId", peer.value());
                                                node.put("stats", result);
                                                return node;
                                            } catch (RuntimeException ex) {
                                                LOG.warn("fan-out stats skipped peer {}: {}",
                                                        peer, ex.getMessage());
                                                return null;
                                            }
                                        }, fanoutExecutor)
                                        .orTimeout(FANOUT_TIMEOUT_MS,
                                                java.util.concurrent.TimeUnit.MILLISECONDS))
                                .toList();
                for (var future : futures) {
                    try {
                        Map<String, Object> node = future.join();
                        if (node != null) {
                            nodes.add(node);
                        } else {
                            fanoutDegraded.incrementAndGet();
                        }
                    } catch (RuntimeException e) {
                        fanoutDegraded.incrementAndGet();
                        errors.add(e.toString());
                    }
                }
            }
        }
        out.put("nodes", nodes);
        out.put("errors", errors);
        out.put("degraded", fanoutDegraded.get());
        return out;
    }

    // ------------------------------------------------------------------
    // 管理面（fan-out 集群视图）
    // ------------------------------------------------------------------

    /**
     * fan-out 列举：正在服务 CACHE 的存活节点（含 self）各回本机 key，按 key 合并去重
     * （每 key 仅属一个分区，重复时保守取 TTL 较小者），字典序排序后截断到 limit。
     * 能力已停用的节点不参与（查询必被拒绝）；单 peer 失败仅日志跳过（部分失败降级，
     * 与 StorageGateway.list 同约定）。
     *
     * <p>本地能力未启动（调度器已停止且数据迁出）时跳过本地部分——本节点贡献
     * 0 条是正确语义，集群视图由存活的属主节点聚合（任意节点可调用）。</p>
     */
    public List<CacheCapability.CacheKeyEntry> listKeys(String prefix, int limit) {
        int capped = limit <= 0 ? 10_000 : Math.min(limit, 10_000);
        Map<String, CacheCapability.CacheKeyEntry> merged = new LinkedHashMap<>();
        if (local.isStarted()) {
            for (CacheCapability.CacheKeyEntry e : local.listKeys(prefix, capped)) {
                mergeEntry(merged, e);
            }
        }
        if (directory != null && client != null) {
            List<NodeId> peers = directory.servingPeers().stream()
                    .filter(p -> !p.equals(self)).toList();
            if (!peers.isEmpty()) {
                // 各 peer 并行 fan-out（C17：专用池 + 超时，慢/宕 peer 不拖垮公共池）
                List<java.util.concurrent.CompletableFuture<List<CacheCapability.CacheKeyEntry>>> futures =
                        peers.stream()
                                .map(peer -> java.util.concurrent.CompletableFuture
                                        .supplyAsync(() -> {
                                            try {
                                                return client.listKeys(peer, prefix, capped);
                                            } catch (RuntimeException ex) {
                                                LOG.warn("fan-out listKeys skipped peer {}: {}",
                                                        peer, ex.getMessage());
                                                return List.<CacheCapability.CacheKeyEntry>of();
                                            }
                                        }, fanoutExecutor)
                                        .orTimeout(FANOUT_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS))
                                .toList();
                for (var future : futures) {
                    List<CacheCapability.CacheKeyEntry> entries = joinOrEmpty(future, "listKeys");
                    for (CacheCapability.CacheKeyEntry e : entries) {
                        mergeEntry(merged, e);
                    }
                }
            }
        }
        List<CacheCapability.CacheKeyEntry> result = new ArrayList<>(merged.values());
        result.sort(Comparator.comparing(CacheCapability.CacheKeyEntry::key));
        if (result.size() > capped) {
            result = new ArrayList<>(result.subList(0, capped));
        }
        return result;
    }

    /**
     * fan-out 计数：本地 + 正在服务 CACHE 的各存活 peer 的 countKeys 求和
     * （每 key 仅属一个分区，求和即集群精确总数；能力已停用的节点不参与；
     * peer 失败降级计 0）。本地能力未启动时按 0 计（同 {@link #listKeys} 约定）。
     */
    public long countKeys(String prefix) {
        long total = local.isStarted() ? local.countKeys(prefix) : 0L;
        if (directory != null && client != null) {
            List<NodeId> peers = directory.servingPeers().stream()
                    .filter(p -> !p.equals(self)).toList();
            if (!peers.isEmpty()) {
                // 各 peer 并行 fan-out（C17：专用池 + 超时）
                List<java.util.concurrent.CompletableFuture<Long>> futures = peers.stream()
                        .map(peer -> java.util.concurrent.CompletableFuture
                                .supplyAsync(() -> {
                                    try {
                                        return client.countKeys(peer, prefix);
                                    } catch (RuntimeException ex) {
                                        LOG.warn("fan-out countKeys skipped peer {}: {}", peer, ex.getMessage());
                                        return 0L;
                                    }
                                }, fanoutExecutor)
                                .orTimeout(FANOUT_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS))
                        .toList();
                for (var future : futures) {
                    total += joinOrZero(future, "countKeys");
                }
            }
        }
        return total;
    }

    /** 本节点持有的 KV 总条数（本地视角）。 */
    public long totalItems() {
        return local.totalItems();
    }

    /**
     * C28：批量删除（管理面子树删除用）——逐 key 路由删除派发到 fan-out 专用池并行执行，
     * 超时/异常计入 failed（不回滚、不中断其余），消灭旧实现的串行长事务
     * （1 万 key × 远端 RPC 串占用 HTTP 线程数分钟）。
     *
     * @return {@code [deleted, failed]}
     */
    public long[] deleteKeys(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new long[]{0L, 0L};
        }
        List<java.util.concurrent.CompletableFuture<Boolean>> futures = new ArrayList<>(keys.size());
        for (String key : keys) {
            try {
                futures.add(java.util.concurrent.CompletableFuture
                        .supplyAsync(() -> {
                            try {
                                return remove(key);
                            } catch (RuntimeException e) {
                                LOG.debug("deleteKeys: remove failed for {}: {}", key, e.toString());
                                return Boolean.FALSE;
                            }
                        }, fanoutExecutor)
                        .orTimeout(FANOUT_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS));
            } catch (RuntimeException submitRejected) {
                // 提交期异常（理论不可达：CallerRuns）：计 failed
                futures.add(java.util.concurrent.CompletableFuture.completedFuture(Boolean.FALSE));
            }
        }
        long deleted = 0;
        long failed = 0;
        for (java.util.concurrent.CompletableFuture<Boolean> future : futures) {
            boolean ok;
            try {
                ok = Boolean.TRUE.equals(future.join());
            } catch (RuntimeException e) {
                fanoutDegraded.incrementAndGet();
                ok = false;
            }
            if (ok) {
                deleted++;
            } else {
                failed++;
            }
        }
        return new long[]{deleted, failed};
    }

    private static void mergeEntry(Map<String, CacheCapability.CacheKeyEntry> merged,
                                   CacheCapability.CacheKeyEntry e) {
        merged.merge(e.key(), e, (a, b) -> a.ttlMillis() <= b.ttlMillis() ? a : b);
    }

    /** C17：超时/异常降级为空列表（计数指标）。 */
    private java.util.List<CacheCapability.CacheKeyEntry> joinOrEmpty(
            java.util.concurrent.CompletableFuture<java.util.List<CacheCapability.CacheKeyEntry>> future,
            String op) {
        try {
            return future.join();
        } catch (RuntimeException e) {
            fanoutDegraded.incrementAndGet();
            LOG.warn("fan-out {} peer join failed (degraded): {}", op, e.toString());
            return java.util.List.of();
        }
    }

    /** C17：超时/异常降级为 0（计数指标）。 */
    private long joinOrZero(java.util.concurrent.CompletableFuture<Long> future, String op) {
        try {
            return future.join();
        } catch (RuntimeException e) {
            fanoutDegraded.incrementAndGet();
            LOG.warn("fan-out {} peer join failed (degraded): {}", op, e.toString());
            return 0L;
        }
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /** 属主节点：无放置返回 {@code null}（调用方退化为本地语义）。 */
    private NodeId ownerNode(String key) {
        if (!view.isReady(CapabilityType.CACHE)) {
            // 本地视图尚未完成首次加载：拒绝路由，不能把“未知”误判为“本地”
            throw new RuntimeFrameworkException("placement view not ready for CACHE");
        }
        Optional<PlacementView.Owner> owner =
                view.owner(CapabilityType.CACHE, PartitionId.of(partitionOf(key)));
        return owner.map(PlacementView.Owner::node).orElse(null);
    }

    private int partitionOf(String key) {
        return partitioner.applyAsInt(key);
    }

    private void requireClient() {
        if (client == null) {
            throw new RuntimeFrameworkException("cache gateway is in local-only mode (no peer directory)");
        }
    }
}
