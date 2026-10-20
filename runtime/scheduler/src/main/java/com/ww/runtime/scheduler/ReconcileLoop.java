package com.ww.runtime.scheduler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;

/**
 * 调谐循环（调度器核心）：以 leader 租约为前提，周期性地把集群从「期望态」
 * （desired，管理面经 {@link DesiredStateWriter} 写入）收敛到「实际态」
 * （actual + 放置表 + 节点能力状态机）。
 *
 * <h2>循环结构</h2>
 * <p>{@link #start()} 启动守护单线程：每周期先 {@code acquireOrRenew} 租约——
 * 失败则本周期跳过（非 leader 不产生任何副作用）；成功则依次调谐
 * {@code CapabilityStateStore.list()} 中出现过的每个 CapabilityType；
 * 之后睡眠 {@code min(renewIntervalMs, reconcileIntervalMs)}（既满足调谐节拍，
 * 又保证租约续约间隔不超时）。{@link #stop()} 中断退出。异常（含存储故障）
 * 一律记日志吞掉，绝不中断循环。</p>
 *
 * <h2>reconcile(type) 算法（每类型串行、幂等、可重入）</h2>
 * <ol>
 *   <li><b>引导</b>：放置表为空且 desired-ACTIVE∩alive 非空 →
 *       {@code replaceAll(compute(desiredAlive, rf))} 建立初始环
 *       （含 REPLICA 的多行放置，[0]=PRIMARY）；</li>
 *   <li><b>上线</b>：desiredOn − actualOn 中的存活节点逐个
 *       {@code startCapability} 成功后回写 actual=ACTIVE；</li>
 *   <li><b>下线</b>：actualOn − desiredOn 非空时——先 freeze(true) 停新写；
 *       目标环 = desiredOn∩alive；目标环为空则跳过迁移直接停止；否则
 *       {@code plan(current, target)} 过滤出 from∈待下线 的 moves，
 *       目标已持有副本的分区直接提升（无损免迁移，避免 cutover 违反放置主键），
 *       其余 {@code migration.migrateAndWait}（cutover 由 coordinator 内部执行）成功后
 *       <b>先复核租约</b>（{@link #stillLeader()}，失租即中止），再
 *       freeze(false)→stopCapability→actual=DISABLED；失败则保留现状并进入排空冷却，
 *       冷却期满再重试（避免亚秒级周期重复建任务引爆任务表）；</li>
 *   <li><b>扩容重平衡</b>：无待下线且当前环与 actualOn 目标环存在偏差时——
 *       源节点已不在存活集合的分区先作为<b>治愈迁移</b>免门控直接执行
 *       （不迁 = 数据不可用）：有存活副本者优先提升（无损、即时）；无副本的
 *       须等死亡节点越过 {@code healingGracePeriodMs} 治愈宽限期（押注节点重启
 *       回归，避免过早不可逆操作）；宽限期赌输后默认走 <b>P0-2 隔离</b>：放置保持
 *       指向死亡属主（数据留盘）、标记 DEGRADED 拒绝数据面流量，属主回归自动解除，
 *       运营可 force-adopt 显式接受丢失——不再以空分区覆盖放置表造成静默永久丢失
 *       （{@code healingQuarantineEnabled=false} 时回退旧的空分区迁移行为）；
 *       其余经 {@code shouldRebalance}（移动比例 + 冷却时间）门控后交迁移协调器执行，
 *       成功后复核租约再更新 lastRebalanceAt。</li>
 *   <li><b>副本集补足</b>：主环已收敛（无迁移 moves）时按目标环对比各分区
 *       REPLICA 行，缺失/偏离者经 {@code reassignReplicas} 重写——兜底
 *       promote/失败迁移遗留的「副本行被消耗后无人重建」泄漏（冷却门控节流）。
 *       副本数据由各能力 Replicator 对账按新 holders 补齐。</li>
 * </ol>
 *
 * <p>长操作（migrateAndWait）可能跨越租约 TTL：返回后一律经 {@link #stillLeader()}
 * 复核，失租即自降 leader 并中止后续副作用。残余风险：复核与副作用下发之间仍有
 * 微小竞态窗口、迁移期间 cutover 无法撤回；框架当前无 fencing token，
 * 亦无后台续约线程（与 TestClock 确定性测试不兼容，需先引入可注入调度器抽象）。</p>
 *
 * <p>单类型失败记日志继续下一类型。{@link #isLeader()} 与 {@link #snapshot()}
 * 供管理端点查询。</p>
 */
public final class ReconcileLoop {

    private static final Logger LOG = LoggerFactory.getLogger(ReconcileLoop.class);

    private final NodeId self;
    private final LeaseStore leaseStore;
    private final NodeRegistry registry;
    private final CapabilityStateStore capabilityStore;
    private final PlacementStore placementStore;
    private final PlacementEngine engine;
    private final RebalancePlanner planner;
    private final NodeCommandChannel commands;
    private final MigrationCoordinator migration;
    private final DecommissionDriver decommission;
    /** 怀疑票存储：多数仲裁 DEAD 提升的凭据层；null=未启用（本地直写 DEAD 语义）。 */
    private final com.ww.runtime.cluster.NodeSuspicionStore suspicionStore;
    /**
     * P0-2 分区隔离注册表：null=未装配（单测/旧行为）——此时无存活副本分区治愈
     * 回退为旧的空分区迁移路径（静默丢失，仅作回滚对照）。
     */
    private final com.ww.runtime.placement.DegradedPartitionStore degradedStore;
    private final SchedulerConfig config;
    private final Clock clock;

    private final ConcurrentHashMap<CapabilityType, Long> lastRebalanceAt = new ConcurrentHashMap<>();
    /** 排空（下线迁移）最近一次失败时刻：冷却窗口内不重复提交迁移，
     *  避免持续性失败时每周期（亚秒级）全量重建任务引爆任务表与日志。 */
    private final ConcurrentHashMap<CapabilityType, Long> lastDrainFailureAt = new ConcurrentHashMap<>();
    /** 调谐循环首次观察到节点死亡（持有分区但不在存活集）的时刻，用于治愈宽限期门控；
     *  节点复活（重回存活集）即移除，再次故障重新计时。 */
    private final ConcurrentHashMap<NodeId, Long> deadSince = new ConcurrentHashMap<>();
    /** 副本集补足最近一次执行时刻：全量放置对比成本不低，按重平衡冷却门控节流。 */
    private final ConcurrentHashMap<CapabilityType, Long> lastReplicaTopUpAt = new ConcurrentHashMap<>();
    /** 单周期副本补足上限：避免大批分区同周期集中改写拖长周期、危及租约。 */
    private static final int REPLICA_TOP_UP_BATCH_MAX = 256;
    // ==================== S4/S5 状态（§3.14 命令扇出治理与冻结补偿） ====================
    /** S4/§3.17：命令扇出批大小（与迁移批 256 同约定），批间同步续约。 */
    private static final int COMMAND_BATCH_SIZE = 256;
    /**
     * S4：单条命令等待超时 = 3×gRPC deadline。命令链（如 stopNode 的 freeze+stop 两次
     * 阻塞调用）各自受 deadline 封顶，等待超时仅放弃回收结果（钩子不被 interrupt，
     * 幂等收敛下周期兜底），线程终将随 deadline 释放。
     */
    private static final long COMMAND_TIMEOUT_MS = GrpcNodeCommandChannel.DEFAULT_DEADLINE_MILLIS * 3 + 2_000L;
    /** S5：上一周期排空判定集（toStop）。desired 回改后节点离开 toStop 即泄漏冻结候选（定向通道）。 */
    private final ConcurrentHashMap<CapabilityType, Set<NodeId>> lastToStopByType = new ConcurrentHashMap<>();
    /** S4：命令扇出专用池（command-exec，有界+拒绝记 false），禁用 commonPool。 */
    private final ExecutorService commandExecutor;
    private volatile Thread worker;
    private volatile boolean running;
    private volatile boolean leader;
    /** 当前租约纪元（P1-2 fencing token）：控制面副作用前复核。 */
    private volatile long leaderEpoch = -1L;

    public ReconcileLoop(NodeId self,
                         LeaseStore leaseStore,
                         NodeRegistry registry,
                         CapabilityStateStore capabilityStore,
                         PlacementStore placementStore,
                         PlacementEngine engine,
                         RebalancePlanner planner,
                         NodeCommandChannel commands,
                         MigrationCoordinator migration,
                         SchedulerConfig config,
                         Clock clock) {
        this(self, leaseStore, registry, capabilityStore, placementStore, engine, planner,
                commands, migration, new NoopDecommissionDriver(), config, clock, null, null);
    }

    public ReconcileLoop(NodeId self,
                         LeaseStore leaseStore,
                         NodeRegistry registry,
                         CapabilityStateStore capabilityStore,
                         PlacementStore placementStore,
                         PlacementEngine engine,
                         RebalancePlanner planner,
                         NodeCommandChannel commands,
                         MigrationCoordinator migration,
                         DecommissionDriver decommission,
                         SchedulerConfig config,
                         Clock clock) {
        this(self, leaseStore, registry, capabilityStore, placementStore, engine, planner,
                commands, migration, decommission, config, clock, null, null);
    }

    /**
     * 含怀疑票存储的构造器（退役驱动取 {@link NoopDecommissionDriver}）。
     */
    public ReconcileLoop(NodeId self,
                         LeaseStore leaseStore,
                         NodeRegistry registry,
                         CapabilityStateStore capabilityStore,
                         PlacementStore placementStore,
                         PlacementEngine engine,
                         RebalancePlanner planner,
                         NodeCommandChannel commands,
                         MigrationCoordinator migration,
                         SchedulerConfig config,
                         Clock clock,
                         com.ww.runtime.cluster.NodeSuspicionStore suspicionStore) {
        this(self, leaseStore, registry, capabilityStore, placementStore, engine, planner,
                commands, migration, new NoopDecommissionDriver(), config, clock, suspicionStore, null);
    }

    /**
     * 含怀疑票存储与隔离注册表的构造器（退役驱动取 {@link NoopDecommissionDriver}）——
     * P0-2 隔离治愈的标准装配形态（生产经 RuntimeAssembly 全参构造器注入）。
     */
    public ReconcileLoop(NodeId self,
                         LeaseStore leaseStore,
                         NodeRegistry registry,
                         CapabilityStateStore capabilityStore,
                         PlacementStore placementStore,
                         PlacementEngine engine,
                         RebalancePlanner planner,
                         NodeCommandChannel commands,
                         MigrationCoordinator migration,
                         SchedulerConfig config,
                         Clock clock,
                         com.ww.runtime.cluster.NodeSuspicionStore suspicionStore,
                         com.ww.runtime.placement.DegradedPartitionStore degradedStore) {
        this(self, leaseStore, registry, capabilityStore, placementStore, engine, planner,
                commands, migration, new NoopDecommissionDriver(), config, clock,
                suspicionStore, degradedStore);
    }

    /**
     * 全参构造器：{@code suspicionStore} 非空且 {@link SchedulerConfig#suspicionQuorumEnabled()}
     * 开启时，leader 每周期先执行多数仲裁 DEAD 提升（PFAIL→FAIL），再做能力调谐。
     *
     * <p>P0-2：{@code degradedStore} 非空且 {@link SchedulerConfig#healingQuarantineEnabled()}
     * 开启时，无存活副本分区的治愈走「DEGRADED 隔离」路径（放置留盘押注重启）；
     * 为空时回退旧的空分区迁移行为。</p>
     */
    public ReconcileLoop(NodeId self,
                         LeaseStore leaseStore,
                         NodeRegistry registry,
                         CapabilityStateStore capabilityStore,
                         PlacementStore placementStore,
                         PlacementEngine engine,
                         RebalancePlanner planner,
                         NodeCommandChannel commands,
                         MigrationCoordinator migration,
                         DecommissionDriver decommission,
                         SchedulerConfig config,
                         Clock clock,
                         com.ww.runtime.cluster.NodeSuspicionStore suspicionStore,
                         com.ww.runtime.placement.DegradedPartitionStore degradedStore) {
        this.self = Objects.requireNonNull(self, "self");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capabilityStore = Objects.requireNonNull(capabilityStore, "capabilityStore");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.commands = Objects.requireNonNull(commands, "commands");
        this.migration = Objects.requireNonNull(migration, "migration");
        this.decommission = Objects.requireNonNull(decommission, "decommission");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.suspicionStore = suspicionStore;
        this.degradedStore = degradedStore;
        // S4：专用命令扇出池——8 线程有界队列，拒绝=命令记 false 下周期重试；
        // 池线程命名 command-exec-N，与 §3.17 线程池规范统一
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                COMMAND_POOL_SIZE, COMMAND_POOL_SIZE, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(COMMAND_QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "command-exec-" + self.value());
                    t.setDaemon(true);
                    return t;
                });
        executor.allowCoreThreadTimeOut(true);
        this.commandExecutor = executor;
    }

    /** S4：命令扇出池大小/队列容量（数据面无长任务，命令阻塞以 gRPC deadline 封顶）。 */
    private static final int COMMAND_POOL_SIZE = 8;
    private static final int COMMAND_QUEUE_CAPACITY = 4096;

    /** 启动守护调谐线程（幂等拒绝：已运行时抛 IllegalStateException）。 */
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("ReconcileLoop already started");
        }
        running = true;
        worker = new Thread(this::loop, "runtime-scheduler-reconcile-" + self.value());
        worker.setDaemon(true);
        worker.start();
    }

    /** 停止循环：置停标志、中断工作线程并等待退出。 */
    public synchronized void stop() {
        running = false;
        Thread current = worker;
        worker = null;
        if (current != null) {
            current.interrupt();
            try {
                current.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        commandExecutor.shutdownNow();
        leader = false;
    }

    /** 本实例当前是否持有 leader 租约（最近一次周期视角）。 */
    public boolean isLeader() {
        return leader;
    }

    /**
     * 重置指定能力的重平衡冷却（管理面手动触发）：{@link #lastRebalanceAt} 清除后，
     * leader 实例的下个 reconcile 周期（≤ reconcileIntervalMs）将重新规划扩容重平衡
     * （minMoveRatio 移动比例门控仍然生效）。
     *
     * <p><b>leader 亲和限制</b>：{@code lastRebalanceAt} 为本实例内存态，仅 leader 实例的
     * reconcile 循环读取它——请求打到非 leader 节点时本调用无实际效果（重复调用无害，
     * 幂等）。调用方（管理端点）应以 {@link #isLeader()} 标注有效性。</p>
     */
    public void resetRebalanceCooldown(CapabilityType type) {
        lastRebalanceAt.remove(Objects.requireNonNull(type, "type"));
        lastReplicaTopUpAt.remove(type);
    }

    /**
     * 执行单个周期（租约检查 → 以 leader 身份逐类型调谐）。
     * 守护线程内部即循环调用本方法；宿主/测试也可外部驱动以获得确定性。
     *
     * @return 本周期是否以 leader 身份执行
     */
    public boolean runOnce() {
        boolean acquired = false;
        try {
            leaderEpoch = leaseStore.acquireOrRenewWithEpoch(self.value(), config.ttlMillis());
            acquired = leaderEpoch >= 0;
            leader = acquired;
            if (acquired) {
                // 多级故障确认（PFAIL→FAIL）：多数观察者怀疑票达法定人数 → 唯一提升 DEAD。
                // 先于一切调谐：后续 reconcile 的 listAlive/治愈迁移据此排除被仲裁节点。
                promoteConfirmedDead();
                // 本周期能力状态快照仅读取一次，供 knownTypes 与各类型 reconcile 复用，
                // 避免每周期重复全表扫描
                List<CapabilityRecord> allRecords = capabilityStore.list();
                // §3.12③+⑤ 启动孤儿清扫（能力感知，宽限 2×TTL 防慢启动误伤）：
                // JOINING 行超时且从未拉起任何能力（无 actual≠DISABLED 记录）→ 启动失败孤儿
                // （端口占用/自检前崩溃），tombstone 防其以「存活」身份污染伸缩视图；
                // 已承载运行中能力的 JOINING 行不误伤（状态推进由节点自身链路负责）
                sweepStaleJoiningOrphans(allRecords);
                for (CapabilityType type : knownTypes(allRecords)) {
                    try {
                        reconcile(type, allRecords);
                    } catch (Exception e) {
                        LOG.warn("reconcile failed for capability {} (type skipped, loop continues)", type, e);
                    }
                }
                // 排空/迁移可能耗时数分钟：进入退役推进前必须仍是当前纪元 leader
                if (stillLeader()) {
                    try {
                        decommission.advance();
                    } catch (Exception e) {
                        LOG.warn("decommission advance failed", e);
                    }
                }
            } else {
                LOG.debug("not leader (lease owned by {}), skip reconcile this cycle",
                        leaseStore.currentOwner().orElse("<none>"));
            }
        } catch (Exception e) {
            LOG.warn("scheduler cycle failed on {}; loop continues", self, e);
        }
        cachedSnapshot = null; // S17：每周期主动作废快照缓存
        return leader;
    }

    /**
     * 集群状态快照：管理端点视图（含副本视图）。
     *
     * <p>S17：节流 + 短缓存——UI/SSE 轮询频率可远超调谐周期，直连会把每能力
     * P×rf 行的放置表扫描放大成 DB 风暴。缓存以墙钟 TTL（1s）失效，
     * 每次 {@link #runOnce()} 主动作废（数据新鲜度以「至多落后一个轮询间隔」为界），
     * 测试语义不受时钟推进影响。</p>
     */
    public synchronized ClusterStatus snapshot() {
        ClusterStatus cached = cachedSnapshot;
        long now = System.currentTimeMillis();
        if (cached != null && now - cachedSnapshotAtMs < SNAPSHOT_CACHE_TTL_MS) {
            return cached;
        }
        ClusterStatus fresh = buildSnapshot();
        cachedSnapshot = fresh;
        cachedSnapshotAtMs = now;
        return fresh;
    }

    /** S17：快照短缓存 TTL（毫秒）。 */
    private static final long SNAPSHOT_CACHE_TTL_MS = 1_000L;
    private volatile ClusterStatus cachedSnapshot;
    private volatile long cachedSnapshotAtMs;

    /** 全量读取构建快照（S17：仅缓存未命中时执行）。 */
    private ClusterStatus buildSnapshot() {
        List<CapabilityRecord> capabilities = capabilityStore.list();
        Map<String, Map<Integer, String>> primariesByCapability = new LinkedHashMap<>();
        Map<String, Map<Integer, List<String>>> placementsByCapability = new LinkedHashMap<>();
        for (CapabilityType type : capabilities.stream()
                .map(CapabilityRecord::type)
                .distinct()
                .sorted(Comparator.comparing(Enum::name))
                .toList()) {
            Map<Integer, String> primaries = new LinkedHashMap<>();
            Map<Integer, List<String>> placements = new LinkedHashMap<>();
            for (Placement placement : placementStore.list(type.name())) {
                placements.computeIfAbsent(placement.partition(), p -> new ArrayList<>())
                        .add(placement.node().value());
                if (placement.isPrimary()) {
                    primaries.put(placement.partition(), placement.node().value());
                }
            }
            primariesByCapability.put(type.name(), primaries);
            placementsByCapability.put(type.name(), placements);
        }
        return new ClusterStatus(leader, leaseStore.currentOwner().orElse(null),
                capabilities, primariesByCapability, placementsByCapability);
    }

    private void loop() {
        long sleepMillis = Math.min(config.renewIntervalMs(), config.reconcileIntervalMs());
        while (running) {
            runOnce();
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        leader = false;
    }

    private List<CapabilityType> knownTypes(List<CapabilityRecord> allRecords) {
        return allRecords.stream()
                .map(CapabilityRecord::type)
                .distinct()
                .sorted(Comparator.comparing(Enum::name))
                .toList();
    }

    /**
     * §3.12③+⑤ 启动孤儿清扫（leader 每周期，能力感知，宽限 2×TTL）。
     *
     * <p>判定：JOINING 行的注册/心跳时刻超过宽限期，且该节点在能力仓库中<b>无任何记录</b>
     * （从未被纳入期望态、从未拉起）——即深审6 的启动失败孤儿（端口占用/自检前崩溃：
     * register 成功但后续链路中断）。此类行若滞留，会以「存活」身份进入
     * {@code listAlive}→放置候选，写命令石沉大海、治愈永无休止。</p>
     *
     * <p>不误伤：有任意能力记录的 JOINING 行一律不清扫——其状态推进归节点自身链路
     * （register→自检→ACTIVE）与退役流程负责；测试语义中「有能力但停在 JOINING」
     * 的构造亦不受影响。tombstone 幂等，重复判定无害。</p>
     */
    private void sweepStaleJoiningOrphans(List<CapabilityRecord> allRecords) {
        long now = clock.nowMillis();
        long grace = config.ttlMillis() * 2;
        for (NodeInfo info : registry.listAll()) {
            if (info.state() != NodeState.JOINING
                    || now - info.resources().updatedAtMillis() < grace) {
                continue; // 非 JOINING，或慢启动宽限期内
            }
            boolean hasAnyCapabilityRecord = allRecords.stream()
                    .anyMatch(r -> r.node().equals(info.id()));
            if (hasAnyCapabilityRecord) {
                continue;
            }
            if (!stillLeader()) {
                return;
            }
            try {
                registry.tombstone(info.id());
                LOG.warn("swept stale JOINING orphan {} (silent {}ms, no capability records)",
                        info.id(), now - info.resources().updatedAtMillis());
            } catch (RuntimeException e) {
                LOG.warn("stale JOINING orphan tombstone failed for {}: {}", info.id(), e.toString());
            }
        }
    }

    /**
     * 多级故障确认（PFAIL→FAIL）：leader 每周期把 TTL 内获得多数独立观察者怀疑票的
     * 存活节点提升为 DEAD。
     *
     * <p>法定人数 = {@code max(suspicionMinObservers, floor((N-1)/2)+1)}，N 为本周期
     * 开始时 {@code listAlive} 的节点数（含嫌疑节点本身；节点不会投自己，故观察者池为 N-1）：
     * 2 节点=1 票、3 节点=2 票、5 节点=3 票。N 在本方法入口快照一次，本周期多个提升
     * 不级联改变后续阈值。</p>
     *
     * <p>安全语义：注册表不存在/已 DEAD → 清票收敛；状态机拒绝（如节点恰好转 LEAVING
     * 的并发窗口）→ 本周期跳过保留下周期重试；仅 leader 执行（租约单一提升者，无 split-brain）。</p>
     */
    private void promoteConfirmedDead() {
        if (suspicionStore == null || !config.suspicionQuorumEnabled()) {
            return;
        }
        long now = clock.nowMillis();
        long ttl = config.suspicionVoteTtlMs();
        try {
            suspicionStore.pruneExpired(now, ttl);
        } catch (RuntimeException e) {
            LOG.warn("prune suspicion votes failed, skip quorum promotion this cycle: {}", e.toString());
            return;
        }
        Map<NodeId, Integer> counts;
        try {
            counts = suspicionStore.freshVoteCounts(now, ttl);
        } catch (RuntimeException e) {
            LOG.warn("read suspicion votes failed, skip quorum promotion this cycle: {}", e.toString());
            return;
        }
        if (counts.isEmpty()) {
            return;
        }
        int aliveCount = registry.listAlive().size();
        int required = Math.max(config.suspicionMinObservers(), (aliveCount - 1) / 2 + 1);
        // 观察者总数不超过 N-1：封顶防止存活集收缩后法定人数永远无法达成
        required = Math.min(required, Math.max(1, aliveCount - 1));
        for (Map.Entry<NodeId, Integer> entry : counts.entrySet()) {
            NodeId suspect = entry.getKey();
            int votes = entry.getValue();
            if (votes < required) {
                continue;
            }
            NodeInfo info;
            try {
                info = registry.get(suspect).orElse(null);
            } catch (RuntimeException e) {
                LOG.warn("quorum promotion read failed for {}: {}", suspect, e.toString());
                continue;
            }
            if (info == null || info.state() == NodeState.DEAD) {
                suspicionStore.clear(suspect); // 注册表已无此行或已判死：票收敛
                continue;
            }
            if (!stillLeader()) {
                return; // 计票后失租：本周期不再提升 DEAD，交由新 leader 接管
            }
            try {
                registry.markState(suspect, NodeState.DEAD);
                LOG.warn("node {} promoted to DEAD by leader quorum ({} >= {} fresh observer votes)",
                        suspect, votes, required);
            } catch (RuntimeException e) {
                // 状态机拒绝（并发转 LEAVING/重注册窗口等）：保留下周期重试，不清票
                LOG.warn("quorum promotion markState DEAD failed for {}: {}", suspect, e.toString());
                continue;
            }
            suspicionStore.clear(suspect);
        }
    }

    /**
     * R4 阶段化：单类型调谐拆为具名阶段方法（集合计算→引导→冻结补偿→上线→排空→治愈/重平衡），
     * 每阶段独立可测；语义与原单方法实现严格一致（幂等收敛、单周期至多推进一个排空/重平衡）。
     */
    private void reconcile(CapabilityType type, List<CapabilityRecord> allRecords) {
        String capability = type.name();

        // —— 阶段 1：集合计算 ——
        List<com.ww.runtime.common.NodeInfo> aliveInfos = registry.listAlive();
        Set<NodeId> alive = aliveInfos.stream()
                .map(com.ww.runtime.common.NodeInfo::id)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        // S7/P1-9：同周期节点视图（权重 + 故障域）直接取 listAlive 快照，
        // 消除 placementNodes 逐节点 registry.get 的 N+1 点查
        Map<NodeId, PlacementNode> nodeViews = new LinkedHashMap<>();
        for (com.ww.runtime.common.NodeInfo info : aliveInfos) {
            nodeViews.put(info.id(),
                    new PlacementNode(info.id(), info.weight(), info.zone()));
        }
        deadSince.keySet().removeAll(alive); // 复活节点清除死亡计时，再次故障重新计时
        List<CapabilityRecord> records = allRecords.stream()
                .filter(record -> record.type() == type)
                .toList();

        Set<NodeId> desiredOn = records.stream()
                .filter(record -> record.desired() == CapabilityState.ACTIVE)
                .map(CapabilityRecord::node)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<NodeId> actualOn = records.stream()
                .filter(record -> record.actual() == CapabilityState.ACTIVE)
                .map(CapabilityRecord::node)
                .filter(alive::contains)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<NodeId> desiredAlive = new LinkedHashSet<>(desiredOn);
        desiredAlive.retainAll(alive);
        // 能力 ACTIVE 集（记录口径）：迁移端点门控依据——滚动重启窗口内节点可能
        // membership 已 ACTIVE 而能力仍在启动（register 重置后 convergeStart 未完成），
        // 此时对其建/派迁移任务必然以 fetch "not started" 失败刷屏
        Set<NodeId> capabilityActive = records.stream()
                .filter(record -> record.actual() == CapabilityState.ACTIVE)
                .map(CapabilityRecord::node)
                .collect(Collectors.toCollection(java.util.HashSet::new));
        Set<NodeId> toStart = new LinkedHashSet<>(desiredOn);
        toStart.removeAll(actualOn);
        toStart.retainAll(alive);
        Set<NodeId> toStop = new LinkedHashSet<>(actualOn);
        toStop.removeAll(desiredOn);

        // —— 阶段 2：放置引导 ——
        bootstrapPlacement(type, capability, desiredAlive, nodeViews);

        // —— 阶段 3（S5）：冻结补偿（在上线之前：排空回改的泄漏冻结尽早解冻）——
        compensateStaleFreezes(type, actualOn, desiredOn, toStop);
        lastToStopByType.put(type, Set.copyOf(toStop));

        // —— 阶段 4：上线（desiredOn − actualOn）——
        startNodes(type, toStart, actualOn);
        // 本周期新启成功节点即时并入就绪集（actualOn 仅含 start 成功者），
        // 否则扩容后首个重平衡周期会被快照门控误延后
        capabilityActive.addAll(actualOn);

        // —— 阶段 5：下线（排空）——
        if (!toStop.isEmpty()) {
            drainNodes(type, capability, desiredAlive, actualOn, toStop, nodeViews, capabilityActive, alive);
            return; // 本周期已处理下线，扩容重平衡留待后续周期
        }

        // —— 阶段 6：治愈 / 扩容重平衡 ——
        healOrRebalance(type, capability, alive, actualOn, nodeViews, capabilityActive);
    }

    /** 放置引导：环为空且目标节点集非空 → 建立初始环（含 REPLICA 的多行放置）。 */
    private void bootstrapPlacement(CapabilityType type, String capability,
                                    Set<NodeId> desiredAlive, Map<NodeId, PlacementNode> nodeViews) {
        if (placementStore.primaries(capability).isEmpty() && !desiredAlive.isEmpty()) {
            Map<Integer, List<Placement>> computed = engine.compute(capability,
                    placementNodes(desiredAlive, nodeViews), config.partitionCount(), config.rfFor(capability));
            if (!stillLeader()) {
                return;
            }
            placementStore.replaceAll(capability, flatten(computed));
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.info("bootstrapped placement for {} over {} partitions across nodes {} (rf={})",
                    capability, config.partitionCount(), desiredAlive, config.rfFor(capability));
        }
    }

    /**
     * S5 冻结补偿（§3.14）：唯一解冻点在排空成功路径；排空失败保冻后 desired 回改，
     * 节点离开 toStop 将永久冻结（desired/actual 一致却停写，且不再有收敛路径触达）。
     *
     * <p><b>定向修复</b>：只解冻「上一周期在排空判定（freeze(true) 已下发）、本周期回到
     * actual∩desired」的节点——即 desired 回改的精确泄漏时刻，零误伤、稳态零流量。
     * leader 切换后本实例的判定历史虽丢失，但新 leader 只要观察到一次该节点仍在
     * toStop 即重新建档；仅「切换后一个周期内完成回改」的极小窗口漏修，
     * 代价是维持冻结现状（可人工解冻），以此换取稳态下不向全集群广播空解冻命令
     * （旧全量兜底方案在每冷却周期对所有 active 节点无脑 freeze(false)，
     * 稳态噪声大且日志误导——无冻结也刷屏）。</p>
     *
     * <p>toStop（actual−desired）天然排除在候选外，不会误解冻在迁节点；
     * 冻结语义（迁移前停新写）不受影响。freeze(false) 幂等，重复下发无害。</p>
     */
    private void compensateStaleFreezes(CapabilityType type, Set<NodeId> actualOn,
                                        Set<NodeId> desiredOn, Set<NodeId> toStop) {
        Set<NodeId> leaked = new LinkedHashSet<>(actualOn);
        leaked.retainAll(desiredOn);
        leaked.retainAll(lastToStopByType.getOrDefault(type, Set.of()));
        if (leaked.isEmpty()) {
            return;
        }
        LOG.warn("S5 freeze compensation: {} leaked-frozen node(s) unfrozen for {} {} "
                        + "(drain aborted then desired re-enabled)",
                leaked.size(), type, leaked);
        fanoutCommands(new ArrayList<>(leaked), node -> commands.freeze(node, type, false));
    }

    /** 上线阶段（S4：专用池并行 + 超时；原 commonPool supplyAsync 无超时路径废弃）。 */
    private void startNodes(CapabilityType type, Set<NodeId> toStart, Set<NodeId> actualOn) {
        if (toStart.isEmpty()) {
            return;
        }
        Map<NodeId, Boolean> results = fanoutCommands(new ArrayList<>(toStart),
                node -> commands.startCapability(node, type));
        results.forEach((node, ok) -> {
            if (ok && stillLeader()) {
                capabilityStore.setActual(node, type, CapabilityState.ACTIVE);
                actualOn.add(node);
                LOG.info("capability {} started on {}", type, node);
            } else {
                LOG.warn("start command for capability {} on {} failed; will retry next cycle",
                        type, node);
            }
        });
    }

    /**
     * 排空阶段：freeze(true) → 提升/迁移分流 → （成功后）副本集重算 + 停止 + actual 回写。
     * 失败保冻 + 冷却（既有语义不变）。
     */
    private void drainNodes(CapabilityType type, String capability, Set<NodeId> desiredAlive,
                            Set<NodeId> actualOn, Set<NodeId> toStop, Map<NodeId, PlacementNode> nodeViews,
                            Set<NodeId> capabilityActive, Set<NodeId> alive) {
        // S4：并行冻结（各节点相互独立）——冻结失败沿用旧语义不阻断（迁移读空/写冻结拒绝兜底）
        fanoutCommands(new ArrayList<>(toStop), node -> commands.freeze(node, type, true));
        if (desiredAlive.isEmpty()) {
            // 目标环为空：无处可迁也无需迁，直接排空停止。
            // 清空放置表（能力整体下线）：残留 owner 会指向已停节点——
            // 读写被 fencing 拒绝且再启用时产生 from=已停能力的迁移任务
            // （FAILED→冷却→重试循环）；清空后重新启用走引导路径正确重建环。
            if (!stillLeader()) {
                return;
            }
            placementStore.replaceAll(capability, List.of());
            stopNodes(toStop, type);
        } else {
            Map<Integer, NodeId> currentPrimaries = placementStore.primaries(capability);
            Map<Integer, NodeId> targetPrimaries =
                    engine.computePrimaries(placementNodes(desiredAlive, nodeViews), config.partitionCount());
            List<PartitionMove> moves = planner.plan(capability, currentPrimaries, targetPrimaries)
                    .stream()
                    .filter(move -> move.from() != null && toStop.contains(move.from()))
                    .toList();
            // 分流：排空目标已持有副本的分区直接提升（无损免迁移）。结构性场景：
            // 缩容排空的目标主 = 旧环第二顺位 = 副本持有者，强走 cutover 必违反放置主键。
            List<PartitionMove> toMigrate = new ArrayList<>();
            int promoted = 0;
            for (PartitionMove move : moves) {
                if (tryPromoteDrainTarget(capability, move)) {
                    promoted++;
                } else {
                    toMigrate.add(move);
                }
            }
            if (promoted > 0) {
                LOG.info("drain {} off {}: {} partitions promoted via live replica (no migration)",
                        capability, toStop, promoted);
            }
            // 端点就绪门控：迁移目标节点能力尚未 ACTIVE（滚动重启/启动收敛窗口）时，
            // fetch/import 必然 "capability not started"——本周期整体延后（保持冻结，
            // 不计失败冷却、不建任务），下轮端点就绪后再推进
            if (!toMigrate.isEmpty()
                    && toMigrate.stream().anyMatch(m -> !migrateReady(m, capabilityActive, alive))) {
                LOG.info("drain {} off {} deferred: endpoint capability not ACTIVE yet (startup window)",
                        capability, toStop);
                return;
            }
            boolean drained;
            if (toMigrate.isEmpty()) {
                drained = true;
            } else {
                // 排空失败冷却：仅对「曾失败」生效（无失败记录不拦截首轮），
                // 窗口内不重复提交，保持冻结，冷却期满再重试
                Long lastFailure = lastDrainFailureAt.get(type);
                if (lastFailure != null
                        && clock.nowMillis() - lastFailure < config.rebalanceCooldownMs()) {
                    LOG.debug("drain {} off {} deferred: within {}ms cooldown after last failure",
                            capability, toStop, config.rebalanceCooldownMs());
                    return;
                }
                drained = migrateInBatches(capability, toMigrate);
                if (!drained) {
                    lastDrainFailureAt.put(type, clock.nowMillis());
                    LOG.warn("migration for {} off {} failed; keep nodes frozen and retry after cooldown",
                            capability, toStop);
                }
            }
            if (drained) {
                if (!stillLeader()) {
                    return; // 长迁移期间失租：中止 stop/回写，避免与新 leader 双写互踩
                }
                // 全量 moves（含提升成功的分区）重算副本集：提升后旧副本行变主，
                // 副本集同样需按目标环重算。目标环放置一次性全量计算，各分区查表取用，
                // 避免逐分区重复全量计算；S8：修复集批量回写（32 分区/事务）
                Map<Integer, List<Placement>> computed = engine.compute(capability,
                        placementNodes(desiredAlive, nodeViews), config.partitionCount(), config.rfFor(capability));
                Map<Integer, List<NodeId>> fixes = new LinkedHashMap<>();
                for (PartitionMove move : moves) {
                    fixes.put(move.partition(), targetReplicasOf(computed, move.partition()));
                }
                if (!stillLeader()) {
                    return; // 副本集重算可能耗时：回写前再次复核
                }
                placementStore.reassignReplicasBatch(capability, fixes);
                stopNodes(toStop, type);
                lastDrainFailureAt.remove(type);
                lastRebalanceAt.put(type, clock.nowMillis());
                LOG.info("capability {} migrated off {} ({} partitions) and nodes stopped",
                        capability, toStop, moves.size());
            }
        }
    }

    /**
     * 迁移端点就绪判定：to 必须能力 ACTIVE；from 仅在其仍存活于 membership 时要求就绪——
     * 治愈的死亡源（不在 alive）由宽限期放行「空导出不可逆路径」，属既有语义不得门控。
     */
    private static boolean migrateReady(PartitionMove move, Set<NodeId> capabilityActive, Set<NodeId> alive) {
        return capabilityActive.contains(move.to())
                && (move.from() == null || !alive.contains(move.from())
                    || capabilityActive.contains(move.from()));
    }

    /**
     * 治愈/扩容重平衡阶段：故障治愈分流（免门控 + 副本提升优先 + 宽限期）与
     * 比例/冷却门控的扩容重平衡。语义与原实现一致；扩容分支附加端点就绪门控
     * （治愈的存活源同样受启——能力未就绪的"活"源 fetch 必然失败，延后即安全）。
     */
    private void healOrRebalance(CapabilityType type, String capability, Set<NodeId> alive,
                                 Set<NodeId> actualOn, Map<NodeId, PlacementNode> nodeViews,
                                 Set<NodeId> capabilityActive) {
        if (actualOn.isEmpty()) {
            return;
        }
        Map<Integer, NodeId> currentPrimaries = placementStore.primaries(capability);
        if (currentPrimaries.isEmpty()) {
            return; // 环尚未建立，下周期由引导步骤处理
        }
        Map<Integer, NodeId> targetPrimaries =
                engine.computePrimaries(placementNodes(actualOn, nodeViews), config.partitionCount());
        // P0-2：隔离注册表一次批量加载 + 属主回归自动解除（必须先于 plan——
        // 节点回归后放置已匹配目标环、moves 为空，治愈分支不会进入，
        // 若在此处之后清理将永远错过解除时机）
        boolean quarantine = quarantineActive();
        Set<Integer> alreadyDegraded = quarantine
                ? degradedStore.partitionsOf(capability) : Set.of();
        if (quarantine && !alreadyDegraded.isEmpty()) {
            clearRecoveredDegraded(capability, alive);
        }
        List<PartitionMove> moves = planner.plan(capability, currentPrimaries, targetPrimaries);
        if (moves.isEmpty()) {
            // 主环已收敛：补足缺失/偏离的副本集（promote/失败迁移遗留的副本行泄漏兜底）
            topUpReplicas(type, capability, actualOn, nodeViews);
            return;
        }
        // 故障治愈分流：源节点已不在存活集合的分区迁移不受比例/冷却门控（不迁 = 数据不可用）
        List<PartitionMove> healing = moves.stream()
                .filter(move -> move.from() != null && !alive.contains(move.from()))
                .toList();
        if (!healing.isEmpty()) {
            List<PartitionMove> pending = healing.stream()
                    .filter(move -> !alreadyDegraded.contains(move.partition()))
                    .toList();
            if (pending.isEmpty()) {
                return; // 全部已隔离：等属主回归或运营 force-adopt，不占冷却预算
            }
            // 优先副本提升（免迁移）：该分区存在存活 REPLICA 行 → promoteReplica + 重算副本集。
            // 提升后副本集按候选环一次性全量计算，各分区查表取用，避免逐分区重复全量计算
            Map<Integer, List<Placement>> computed = engine.compute(capability,
                    placementNodes(actualOn, nodeViews), config.partitionCount(), config.rfFor(capability));
            List<PartitionMove> degraded = new ArrayList<>();
            int promoted = 0;
            for (PartitionMove move : pending) {
                if (tryPromoteLiveReplica(capability, move.partition(), alive, computed)) {
                    promoted++;
                } else {
                    degraded.add(move);
                }
            }
            if (promoted > 0) {
                LOG.info("healing via replica promotion for {} done ({} partitions)", capability, promoted);
            }
            if (!degraded.isEmpty()) {
                // 治愈宽限期门控：DEAD 初期押注节点重启重入（数据仍在原盘），
                // 仅当 DEAD 持续超过宽限期才放行不可逆路径。
                long now = clock.nowMillis();
                List<PartitionMove> eligible = new ArrayList<>(degraded.size());
                for (PartitionMove move : degraded) {
                    long since = deadSince.computeIfAbsent(move.from(), node -> now);
                    if (now - since >= config.healingGracePeriodMs()) {
                        eligible.add(move);
                    }
                }
                if (eligible.size() < degraded.size()) {
                    LOG.info("healing for {} deferred: {}/{} partitions within {}ms grace period, "
                                    + "waiting for dead nodes to rejoin",
                            capability, degraded.size() - eligible.size(), degraded.size(),
                            config.healingGracePeriodMs());
                }
                if (!eligible.isEmpty()) {
                    if (quarantine) {
                        // P0-2 隔离治愈：放置保持指向死亡属主（数据留盘），标记 DEGRADED
                        // 拒绝数据面流量——不再以空分区覆盖放置表造成静默永久丢失。
                        // 属主重启回归即自动解除（数据原样恢复）；运营可 force-adopt 接受丢失。
                        int marked = 0;
                        for (PartitionMove move : eligible) {
                            if (alreadyDegraded.contains(move.partition())) {
                                continue; // 幂等：已隔离
                            }
                            degradedStore.mark(capability, move.partition(), move.from(),
                                    "no live replica; owner dead beyond healing grace "
                                            + config.healingGracePeriodMs() + "ms");
                            marked++;
                        }
                        if (marked > 0) {
                            LOG.warn("healing for {}: {} partitions QUARANTINED (DEGRADED, no live "
                                            + "replica) — placement kept on dead owner (data preserved "
                                            + "on disk), data-plane blocked until owner rejoins or "
                                            + "operator force-adopts",
                                    capability, marked);
                        }
                    } else if (migrateInBatches(capability, eligible)) {
                        LOG.warn("healing migration for {} done ({} partitions off dead nodes, "
                                + "no live replica — data loss window)", capability, eligible.size());
                    } else {
                        LOG.warn("healing migration for {} failed; retry next cycle", capability);
                    }
                }
            }
            return; // 治愈优先；剩余扩容重平衡留下周期，不更新 lastRebalanceAt（不占冷却预算）
        }
        // 端点就绪门控：扩容重平衡的任一 move 目标节点能力未 ACTIVE（启动窗口）则本周期延后，
        // 不占用冷却预算（避免对未就绪节点建 fetch 必失败的任务刷屏 + 任务表膨胀）
        if (moves.stream().anyMatch(m -> !migrateReady(m, capabilityActive, alive))) {
            LOG.info("scale-out rebalance for {} deferred: endpoint capability not ACTIVE yet (startup window)",
                    capability);
            return;
        }
        long lastRun = lastRebalanceAt.getOrDefault(type, 0L);
        if (!planner.shouldRebalance(moves.size(), config.partitionCount(), config.minMoveRatio(),
                lastRun, config.rebalanceCooldownMs(), clock.nowMillis())) {
            return;
        }
        if (migrateInBatches(capability, moves)) {
            if (!stillLeader()) {
                return; // 长迁移期间失租：不更新 lastRebalanceAt，交由新 leader 接管
            }
            // 主环变更后重算受影响分区的副本集（副本数据由 Replicator 对账补齐，不走 MigrationEngine）。
            // 目标环放置一次性全量计算，各分区查表取用；S8：批量回写
            Map<Integer, List<Placement>> computed = engine.compute(capability,
                    placementNodes(actualOn, nodeViews), config.partitionCount(), config.rfFor(capability));
            Map<Integer, List<NodeId>> fixes = new LinkedHashMap<>();
            for (PartitionMove move : moves) {
                fixes.put(move.partition(), targetReplicasOf(computed, move.partition()));
            }
            if (!stillLeader()) {
                return; // 副本集重算可能耗时：回写前再次复核
            }
            placementStore.reassignReplicasBatch(capability, fixes);
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.info("scale-out rebalance for {} applied ({} partitions moved)", capability, moves.size());
        } else {
            // 失败也占用冷却预算：否则 shouldRebalance 的冷却门控形同虚设，
            // leader 每 reconcile 周期（亚秒级）全量重试，每轮新建上千任务，任务表爆炸
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.warn("scale-out rebalance for {} failed; will retry after cooldown", capability);
        }
    }

    /**
     * S4：专用池并行命令扇出 + 等待超时 + 批间同步续约（批 256，同迁移约定）。
     * 超时/池拒绝/命令异常一律记 false（命令幂等，下周期收敛），
      * 钩子/阻塞调用不被 interrupt——仅放弃本周期等待。
     */
    private Map<NodeId, Boolean> fanoutCommands(List<NodeId> targets, Function<NodeId, Boolean> command) {
        Map<NodeId, Boolean> results = new LinkedHashMap<>();
        for (int i = 0; i < targets.size(); i += COMMAND_BATCH_SIZE) {
            if (i > 0) {
                leaderEpoch = leaseStore.acquireOrRenewWithEpoch(self.value(), config.ttlMillis());
                if (leaderEpoch < 0) {
                    leader = false;
                    LOG.warn("lease lost during command fan-out on {}; aborting remaining {} of {} targets",
                            self, targets.size() - i, targets.size());
                    for (int k = i; k < targets.size(); k++) {
                        results.put(targets.get(k), false);
                    }
                    break;
                }
            }
            List<NodeId> batch = targets.subList(i, Math.min(i + COMMAND_BATCH_SIZE, targets.size()));
            List<CompletableFuture<Boolean>> futures = new ArrayList<>(batch.size());
            for (NodeId node : batch) {
                try {
                    futures.add(CompletableFuture.supplyAsync(() -> command.apply(node), commandExecutor)
                            .orTimeout(COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS));
                } catch (RejectedExecutionException saturated) {
                    // S4：池饱和=拒绝即命令失败记 false，下周期重试（不引入调用方线程执行，
                    // 避免调谐线程被远端慢命令钉死）
                    LOG.warn("command-exec saturated, command deferred for {}: {}", node, saturated.toString());
                    futures.add(CompletableFuture.completedFuture(false));
                }
            }
            for (int j = 0; j < batch.size(); j++) {
                boolean ok;
                try {
                    ok = Boolean.TRUE.equals(futures.get(j).join());
                } catch (CompletionException | java.util.concurrent.CancellationException e) {
                    ok = false;
                }
                results.put(batch.get(j), ok);
            }
        }
        return results;
    }

    /** 分批迁移的批大小：单批耗时须明显小于租约 TTL，批间同步续约保证长迁移不丢租约。 */
    private static final int MIGRATION_BATCH_SIZE = 256;

    /**
     * 分批迁移 + 批间同步续约：整批 moves 同步 migrateAndWait 可能耗时远超租约 TTL，
     * 中途无续约则必然失租、leader 轮换并重复规划（走马灯风暴）。每批开始前同步
     * acquireOrRenew 续约；失租立即中止剩余批次（placement epoch fencing 兜底安全）。
     * 同步续约不引入后台线程，TestClock 确定性语义不变。
     */
    private boolean migrateInBatches(String capability, List<PartitionMove> moves) {
        for (int i = 0; i < moves.size(); i += MIGRATION_BATCH_SIZE) {
            leaderEpoch = leaseStore.acquireOrRenewWithEpoch(self.value(), config.ttlMillis());
            if (leaderEpoch < 0) {
                leader = false;
                LOG.warn("lease lost during batched migration on {}; aborting remaining {} of {} moves",
                        self, moves.size() - i, moves.size());
                return false;
            }
            List<PartitionMove> batch = moves.subList(i, Math.min(i + MIGRATION_BATCH_SIZE, moves.size()));
            if (!migration.migrateAndWait(capability, batch)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 长操作后复核租约：失租/纪元被接管即自降 leader 身份并中止后续副作用。
     * P1-2：不再只比 owner——leader_epoch 单调递增，僵尸 leader 的迟到写
     * 在复核时必然发现纪元不一致。
     */
    private boolean stillLeader() {
        boolean owned = leaderEpoch >= 0
                ? leaseStore.isCurrent(self.value(), leaderEpoch)
                : leaseStore.currentOwner().filter(o -> o.equals(self.value())).isPresent();
        if (!owned) {
            leader = false;
            leaderEpoch = -1L;
            LOG.warn("lease lost during long operation on {}; aborting side effects", self);
        }
        return owned;
    }

    /** 解冻 → 停止（drain 链在通道实现内）→ 回写 actual=DISABLED。 */
    private void stopNode(NodeId node, CapabilityType type) {
        commands.freeze(node, type, false);
        if (commands.stopCapability(node, type)) {
            if (stillLeader()) {
                capabilityStore.setActual(node, type, CapabilityState.DISABLED);
            }
            LOG.info("capability {} stopped on {}", type, node);
        } else {
            LOG.warn("stop command for capability {} on {} failed; will retry next cycle", type, node);
        }
    }

    /** 并行停止（S4：专用池 + 超时 + 批间续约；解冻→停止→回写链在任务内保持原子顺序）。 */
    private void stopNodes(Set<NodeId> nodes, CapabilityType type) {
        if (nodes.isEmpty()) {
            return;
        }
        fanoutCommands(new ArrayList<>(nodes), node -> {
            stopNode(node, type);
            return Boolean.TRUE;
        });
    }

    /**
     * 从本周期 listAlive 快照构造放置节点视图（S7/P1-9：权重与故障域同快照取用；
     * 快照缺失的节点按权重 1.0、无故障域兜底）。
     */
    private List<PlacementNode> placementNodes(Set<NodeId> nodes, Map<NodeId, PlacementNode> nodeViews) {
        return nodes.stream()
                .sorted(Comparator.comparing(NodeId::value))
                .map(node -> nodeViews.getOrDefault(node, new PlacementNode(node, 1.0d)))
                .toList();
    }

    /** compute 结果多行展平为 replaceAll 输入（按 partition 升序）。 */
    private static List<Placement> flatten(Map<Integer, List<Placement>> computed) {
        List<Placement> placements = new ArrayList<>(computed.size());
        for (Map.Entry<Integer, List<Placement>> entry : computed.entrySet()) {
            placements.addAll(entry.getValue());
        }
        placements.sort(Comparator.comparingInt(Placement::partition));
        return placements;
    }

    /**
     * 死主副本提升（免迁移治愈）：该分区存在存活 REPLICA 行时 promoteReplica
     * 并按调用方预计算的全量放置结果重算副本集；提升成功返回 true，
     * 无存活副本 / CAS 失败返回 false（调用方退化为迁移路径）。
     */
    private boolean tryPromoteLiveReplica(String capability, int partition,
                                          Set<NodeId> alive, Map<Integer, List<Placement>> computed) {
        List<Placement> rows = placementStore.placements(capability, partition);
        if (rows.isEmpty()) {
            return false;
        }
        Placement primary = rows.getFirst();
        NodeId liveReplica = null;
        for (int i = 1; i < rows.size(); i++) {
            if (alive.contains(rows.get(i).node())) {
                liveReplica = rows.get(i).node();
                break;
            }
        }
        if (liveReplica == null) {
            return false; // 无存活副本：数据丢失窗口，退化为迁移
        }
        if (!placementStore.promoteReplica(capability, partition, liveReplica, primary.epoch())) {
            return false; // CAS 失败（并发变更）：下周期重试
        }
        applyReplicaReassignment(capability, partition, computed);
        return true;
    }

    /** P0-2 隔离治愈是否生效：配置开关开启且注册表已装配。 */
    private boolean quarantineActive() {
        return config.healingQuarantineEnabled() && degradedStore != null;
    }

    /**
     * P0-2 属主回归自动解除隔离：隔离分区的属主重回存活集合 → 清除隔离标记。
     * 放置始终指向该属主（隔离期间未改挂），数据原样在盘，解除即恢复服务。
     */
    private void clearRecoveredDegraded(String capability, Set<NodeId> alive) {
        Map<Integer, NodeId> owners = degradedStore.ownersOf(capability);
        for (Map.Entry<Integer, NodeId> entry : owners.entrySet()) {
            if (alive.contains(entry.getValue())) {
                degradedStore.clear(capability, entry.getKey());
                LOG.info("P0-2 partition {}/{} un-quarantined: owner {} rejoined (data intact on disk)",
                        capability, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 排空副本提升（免迁移排空）：排空目标节点已持有该分区 REPLICA 行时直接提升
     * （数据已在目标，无损）。结构性场景：缩容排空的目标主 = 旧环第二顺位 =
     * 副本持有者，强走迁移 + cutover 会违反放置主键 (capability, partition_id, node_id)。
     * 与 {@link #tryPromoteLiveReplica} 的区别：提升目标必须是 {@code move.to()}（目标环节点），
     * 否则放置偏离目标环，下周期又产生新 moves。
     */
    private boolean tryPromoteDrainTarget(String capability, PartitionMove move) {
        List<Placement> rows = placementStore.placements(capability, move.partition());
        if (rows.isEmpty()) {
            return false;
        }
        Placement primary = rows.getFirst();
        boolean targetHoldsReplica = false;
        for (int i = 1; i < rows.size(); i++) {
            if (rows.get(i).node().equals(move.to())) {
                targetHoldsReplica = true;
                break;
            }
        }
        if (!targetHoldsReplica) {
            return false;
        }
        // CAS 失败（并发变更）转迁移路径，由 MigrationEngine CUTOVER 的提升兜底接手
        return placementStore.promoteReplica(capability, move.partition(), move.to(), primary.epoch());
    }

    /**
     * 按调用方预计算的全量放置结果重算指定分区的副本集（不动主不动 epoch，单分区路径）。
     * <p>批量场景（drain/rebalance/topUp）收集修复集后走
     * {@link PlacementStore#reassignReplicasBatch}（S8：32 分区/事务适度批量）。</p>
     */
    private void applyReplicaReassignment(String capability, int partition,
                                          Map<Integer, List<Placement>> computed) {
        placementStore.reassignReplicas(capability, partition, targetReplicasOf(computed, partition));
    }

    /** 从预计算目标环取指定分区的目标副本集（[0]=PRIMARY 剔除，其余为副本）。 */
    private static List<NodeId> targetReplicasOf(Map<Integer, List<Placement>> computed, int partition) {
        List<Placement> target = computed.getOrDefault(partition, List.of());
        List<NodeId> replicas = new ArrayList<>(Math.max(0, target.size() - 1));
        for (int i = 1; i < target.size(); i++) {
            replicas.add(target.get(i).node());
        }
        return replicas;
    }

    /**
     * 副本集补足（主环收敛后的兜底收敛）：按当前活跃节点重算目标环，逐分区对比
     * 现有 REPLICA 行与目标副本集，缺失/偏离者经 {@code reassignReplicas} 重写。
     *
     * <p><b>覆盖的泄漏路径</b>：{@code promoteReplica} 会把 REPLICA 行提升为 PRIMARY
     * （副本行被消耗），而副本集重建只发生在「调度器规划的迁移整批成功」之后——
     * 迁移批部分失败、任务经重试/续跑在 {@code MigrationEngine} 内直接提升完成、
     * 长迁移失租中止副作用等场景都会留下「只有主行、无副本行」的分区；
     * 主环收敛后本循环对此类分区重写副本集。补足后副本数据由各能力的
     * Replicator 对账按新 holders 推送补齐。
     *
     * <p><b>门控</b>：重平衡冷却节流（全量放置对比成本不低）；单周期至多重写
     * {@link #REPLICA_TOP_UP_BATCH_MAX} 个分区。只触碰主与目标环一致的分区——
     * 在迁分区的副本集由迁移流程负责，避免与在途任务互踩。
     */
    private void topUpReplicas(CapabilityType type, String capability, Set<NodeId> actualOn,
                               Map<NodeId, PlacementNode> nodeViews) {
        int rf = config.rfFor(capability);
        if (rf <= 1 || actualOn.size() < rf) {
            return; // 单副本能力无副本集可补；节点数不足以构成副本集时同样跳过
        }
        long now = clock.nowMillis();
        Long last = lastReplicaTopUpAt.get(type);
        if (last != null && now - last < config.rebalanceCooldownMs()) {
            return;
        }
        lastReplicaTopUpAt.put(type, now);
        Map<Integer, List<Placement>> computed = engine.compute(capability,
                placementNodes(actualOn, nodeViews), config.partitionCount(), rf);
        Map<Integer, List<Placement>> current = new LinkedHashMap<>();
        for (Placement placement : placementStore.list(capability)) {
            current.computeIfAbsent(placement.partition(), p -> new ArrayList<>()).add(placement);
        }
        Map<Integer, List<NodeId>> fixes = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<Placement>> entry : computed.entrySet()) {
            if (fixes.size() >= REPLICA_TOP_UP_BATCH_MAX) {
                break;
            }
            int partition = entry.getKey();
            List<Placement> target = entry.getValue();
            if (target.size() < 2) {
                continue;
            }
            NodeId currentPrimary = null;
            Set<NodeId> currentReplicas = new LinkedHashSet<>();
            for (Placement row : current.getOrDefault(partition, List.of())) {
                if (row.isPrimary()) {
                    currentPrimary = row.node();
                } else {
                    currentReplicas.add(row.node());
                }
            }
            if (currentPrimary == null || !currentPrimary.equals(target.getFirst().node())) {
                continue; // 主偏离目标环 = 在迁/待治愈分区，本步骤不触碰
            }
            Set<NodeId> targetReplicas = new LinkedHashSet<>();
            for (int i = 1; i < target.size(); i++) {
                targetReplicas.add(target.get(i).node());
            }
            if (currentReplicas.equals(targetReplicas)) {
                continue;
            }
            fixes.put(partition, new ArrayList<>(targetReplicas));
        }
        if (!fixes.isEmpty()) {
            if (!stillLeader()) {
                return; // 失租：副本集修复交由新 leader 下周期执行
            }
            placementStore.reassignReplicasBatch(capability, fixes); // S8：32 分区/事务适度批量
            LOG.info("replica top-up for {}: reassigned {} partitions toward rf={}",
                    capability, fixes.size(), rf);
        }
    }

    /**
     * 管理端点快照。
     *
     * @param leader               本实例最近一周期的 leader 身份
     * @param leaseOwner           当前租约持有者（无有效租约为 null）
     * @param capabilities         全量能力状态记录
     * @param primariesByCapability capability → (partition → primary 节点 id)
     * @param placementsByCapability capability → (partition → [PRIMARY, REPLICA…] 节点 id 列表)
     */
    public record ClusterStatus(boolean leader,
                                String leaseOwner,
                                List<CapabilityRecord> capabilities,
                                Map<String, Map<Integer, String>> primariesByCapability,
                                Map<String, Map<Integer, List<String>>> placementsByCapability) {
    }
}
