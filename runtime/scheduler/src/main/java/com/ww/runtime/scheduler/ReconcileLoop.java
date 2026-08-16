package com.ww.runtime.scheduler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import com.ww.runtime.common.PlacementRole;
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
 *       {@code replaceAll(computePrimaries(desiredAlive))} 建立初始环；</li>
 *   <li><b>上线</b>：desiredOn − actualOn 中的存活节点逐个
 *       {@code startCapability} 成功后回写 actual=ACTIVE；</li>
 *   <li><b>下线</b>：actualOn − desiredOn 非空时——先 freeze(true) 停新写；
 *       目标环 = desiredOn∩alive；目标环为空则跳过迁移直接停止；否则
 *       {@code plan(current, target)} 过滤出 from∈待下线 的 moves，
 *       {@code migration.migrateAndWait}（cutover 由 coordinator 内部执行）成功后
 *       <b>先复核租约</b>（{@link #stillLeader()}，失租即中止），再
 *       freeze(false)→stopCapability→actual=DISABLED；失败则保留现状下周期重试；</li>
 *   <li><b>扩容重平衡</b>：无待下线且当前环与 actualOn 目标环存在偏差时——
 *       源节点已不在存活集合的分区先作为<b>治愈迁移</b>免门控直接执行
 *       （不迁 = 数据不可用）；其余经 {@code shouldRebalance}（移动比例 + 冷却时间）
 *       门控后交迁移协调器执行，成功后复核租约再更新 lastRebalanceAt。</li>
 * </ol>
 *
 * <p>长操作（migrateAndWait）可能跨越租约 TTL：返回后一律经 {@link #stillLeader()}
 * 复核，失租即自降 leader 并中止后续副作用。残余风险：复核与副作用下发之间仍有
 * 微小竞态窗口、迁移期间 cutover 无法撤回；框架尚无 fencing token，fencing 与
 * 后台续约线程（与 TestClock 确定性测试不兼容，需先引入可注入调度器抽象）
 * 留作后续演进。</p>
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
    private final SchedulerConfig config;
    private final Clock clock;

    private final ConcurrentHashMap<CapabilityType, Long> lastRebalanceAt = new ConcurrentHashMap<>();
    private volatile Thread worker;
    private volatile boolean running;
    private volatile boolean leader;

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
        this.self = Objects.requireNonNull(self, "self");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capabilityStore = Objects.requireNonNull(capabilityStore, "capabilityStore");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.commands = Objects.requireNonNull(commands, "commands");
        this.migration = Objects.requireNonNull(migration, "migration");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

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
        leader = false;
    }

    /** 本实例当前是否持有 leader 租约（最近一次周期视角）。 */
    public boolean isLeader() {
        return leader;
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
            acquired = leaseStore.acquireOrRenew(self.value(), config.ttlMillis());
            leader = acquired;
            if (acquired) {
                for (CapabilityType type : knownTypes()) {
                    try {
                        reconcile(type);
                    } catch (Exception e) {
                        LOG.warn("reconcile failed for capability {} (type skipped, loop continues)", type, e);
                    }
                }
            } else {
                LOG.debug("not leader (lease owned by {}), skip reconcile this cycle",
                        leaseStore.currentOwner().orElse("<none>"));
            }
        } catch (Exception e) {
            LOG.warn("scheduler cycle failed on {}; loop continues", self, e);
        }
        return leader;
    }

    /**
     * 集群状态快照：管理端点视图。
     */
    public ClusterStatus snapshot() {
        List<CapabilityRecord> capabilities = capabilityStore.list();
        Map<String, Map<Integer, String>> primariesByCapability = new LinkedHashMap<>();
        for (CapabilityType type : capabilities.stream()
                .map(CapabilityRecord::type)
                .distinct()
                .sorted(Comparator.comparing(Enum::name))
                .toList()) {
            Map<Integer, String> primaries = new LinkedHashMap<>();
            placementStore.primaries(type.name())
                    .forEach((partition, node) -> primaries.put(partition, node.value()));
            primariesByCapability.put(type.name(), primaries);
        }
        return new ClusterStatus(leader, leaseStore.currentOwner().orElse(null),
                capabilities, primariesByCapability);
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

    private List<CapabilityType> knownTypes() {
        return capabilityStore.list().stream()
                .map(CapabilityRecord::type)
                .distinct()
                .sorted(Comparator.comparing(Enum::name))
                .toList();
    }

    private void reconcile(CapabilityType type) {
        String capability = type.name();
        Set<NodeId> alive = registry.listAlive().stream()
                .map(NodeInfo::id)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<CapabilityRecord> records = capabilityStore.list().stream()
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

        // (c) 放置引导：环为空且目标节点集非空
        if (placementStore.primaries(capability).isEmpty() && !desiredAlive.isEmpty()) {
            Map<Integer, NodeId> primaries =
                    engine.computePrimaries(placementNodes(desiredAlive), config.partitionCount());
            placementStore.replaceAll(capability, primaryPlacements(primaries));
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.info("bootstrapped placement for {} over {} partitions across nodes {}", 
                    capability, config.partitionCount(), desiredAlive);
        }

        // (d) 待上线：desiredOn − actualOn（存活）
        Set<NodeId> toStart = new LinkedHashSet<>(desiredOn);
        toStart.removeAll(actualOn);
        toStart.retainAll(alive);
        for (NodeId node : toStart) {
            if (commands.startCapability(node, type)) {
                capabilityStore.setActual(node, type, CapabilityState.ACTIVE);
                actualOn.add(node);
                LOG.info("capability {} started on {}", type, node);
            } else {
                LOG.warn("start command for capability {} on {} failed; will retry next cycle", type, node);
            }
        }

        // (e) 待下线：actualOn − desiredOn —— 先迁移后停止
        Set<NodeId> toStop = new LinkedHashSet<>(actualOn);
        toStop.removeAll(desiredOn);
        if (!toStop.isEmpty()) {
            for (NodeId node : toStop) {
                commands.freeze(node, type, true); // 冻结停新写，等待分区数据迁出
            }
            if (desiredAlive.isEmpty()) {
                // 目标环为空：无处可迁也无需迁，直接排空停止
                for (NodeId node : toStop) {
                    stopNode(node, type);
                }
            } else {
                Map<Integer, NodeId> currentPrimaries = placementStore.primaries(capability);
                Map<Integer, NodeId> targetPrimaries =
                        engine.computePrimaries(placementNodes(desiredAlive), config.partitionCount());
                List<PartitionMove> moves = planner.plan(capability, currentPrimaries, targetPrimaries)
                        .stream()
                        .filter(move -> move.from() != null && toStop.contains(move.from()))
                        .toList();
                if (migrateInBatches(capability, moves)) {
                    if (!stillLeader()) {
                        return; // 长迁移期间失租：中止 stop/回写，避免与新 leader 双写互踩
                    }
                    for (NodeId node : toStop) {
                        stopNode(node, type);
                    }
                    lastRebalanceAt.put(type, clock.nowMillis());
                    LOG.info("capability {} migrated off {} ({} partitions) and nodes stopped",
                            capability, toStop, moves.size());
                } else {
                    LOG.warn("migration for {} off {} failed; keep nodes frozen and retry next cycle",
                            capability, toStop);
                }
            }
            return; // 本周期已处理下线，扩容重平衡留待后续周期
        }

        // (f) 扩容重平衡：无待下线且当前环与 actualOn 目标环有偏差
        if (actualOn.isEmpty()) {
            return;
        }
        Map<Integer, NodeId> currentPrimaries = placementStore.primaries(capability);
        if (currentPrimaries.isEmpty()) {
            return; // 环尚未建立，下周期由引导步骤处理
        }
        Map<Integer, NodeId> targetPrimaries =
                engine.computePrimaries(placementNodes(actualOn), config.partitionCount());
        List<PartitionMove> moves = planner.plan(capability, currentPrimaries, targetPrimaries);
        if (moves.isEmpty()) {
            return;
        }
        // 故障治愈分流：源节点已不在存活集合的分区迁移不受比例/冷却门控（不迁 = 数据不可用）
        List<PartitionMove> healing = moves.stream()
                .filter(move -> move.from() != null && !alive.contains(move.from()))
                .toList();
        if (!healing.isEmpty()) {
            if (migrateInBatches(capability, healing)) {
                LOG.info("healing migration for {} done ({} partitions off dead nodes)", capability, healing.size());
            } else {
                LOG.warn("healing migration for {} failed; retry next cycle", capability);
            }
            return; // 治愈优先；剩余扩容重平衡留下周期，不更新 lastRebalanceAt（不占冷却预算）
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
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.info("scale-out rebalance for {} applied ({} partitions moved)", capability, moves.size());
        } else {
            // 失败也占用冷却预算：否则 shouldRebalance 的冷却门控形同虚设，
            // leader 每 reconcile 周期（亚秒级）全量重试，每轮新建上千任务，任务表爆炸
            lastRebalanceAt.put(type, clock.nowMillis());
            LOG.warn("scale-out rebalance for {} failed; will retry after cooldown", capability);
        }
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
            if (!leaseStore.acquireOrRenew(self.value(), config.ttlMillis())) {
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
     * 长操作后复核租约：失租即自降 leader 身份并中止后续副作用。
     * 残余风险：复核与副作用下发之间仍有微小竞态窗口（无 fencing token），
     * 但已消除「整段迁移时长」的大窗口。
     */
    private boolean stillLeader() {
        boolean owned = leaseStore.currentOwner().filter(o -> o.equals(self.value())).isPresent();
        if (!owned) {
            leader = false;
            LOG.warn("lease lost during long operation on {}; aborting side effects", self);
        }
        return owned;
    }

    /** 解冻 → 停止（drain 链在通道实现内）→ 回写 actual=DISABLED。 */
    private void stopNode(NodeId node, CapabilityType type) {
        commands.freeze(node, type, false);
        if (commands.stopCapability(node, type)) {
            capabilityStore.setActual(node, type, CapabilityState.DISABLED);
            LOG.info("capability {} stopped on {}", type, node);
        } else {
            LOG.warn("stop command for capability {} on {} failed; will retry next cycle", type, node);
        }
    }

    /** 从注册表取节点权重（缺省 1.0）构造放置节点视图。 */
    private List<PlacementNode> placementNodes(Set<NodeId> nodes) {
        return nodes.stream()
                .sorted(Comparator.comparing(NodeId::value))
                .map(node -> new PlacementNode(node,
                        registry.get(node).map(NodeInfo::weight).orElse(1.0d)))
                .toList();
    }

    private static List<Placement> primaryPlacements(Map<Integer, NodeId> primaries) {
        List<Placement> placements = new ArrayList<>(primaries.size());
        for (Map.Entry<Integer, NodeId> entry : primaries.entrySet()) {
            placements.add(new Placement(entry.getKey(), entry.getValue(), PlacementRole.PRIMARY, 0L));
        }
        placements.sort(Comparator.comparingInt(Placement::partition));
        return placements;
    }

    /**
     * 管理端点快照。
     *
     * @param leader               本实例最近一周期的 leader 身份
     * @param leaseOwner           当前租约持有者（无有效租约为 null）
     * @param capabilities         全量能力状态记录
     * @param primariesByCapability capability → (partition → primary 节点 id)
     */
    public record ClusterStatus(boolean leader,
                                String leaseOwner,
                                List<CapabilityRecord> capabilities,
                                Map<String, Map<Integer, String>> primariesByCapability) {
    }
}
