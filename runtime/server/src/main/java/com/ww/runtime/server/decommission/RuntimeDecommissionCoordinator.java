package com.ww.runtime.server.decommission;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.cluster.decommission.DecommissionRecord;
import com.ww.runtime.cluster.decommission.DecommissionState;
import com.ww.runtime.cluster.decommission.DecommissionStore;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.scheduler.DecommissionDriver;
import com.ww.runtime.scheduler.SchedulerConfig;

/**
 * 退役流程编排器（实现 {@link DecommissionDriver}）：
 * 由 {@link com.ww.runtime.scheduler.ReconcileLoop} 每周期调用 {@link #advance()}，
 * 推进全部活动退役记录的状态机。
 *
 * <p><b>S15（短期措施）</b>：advance 在 leader 调谐线程内同步执行，为避免占用调谐线程——
 * ① 单次 advance 内按能力缓存全量放置扫描（多记录不再重复拉取 P×rf 行）；
 * ② 副本集重算每能力每轮限额 {@link #REASSIGN_AWAY_BATCH_MAX} 分区，
 * 剩余留下轮（幂等重入），单周期耗时可控。独立线程 + 进度持久化为后续演进项。</p>
 *
 * <h2>DRAINING 阶段</h2>
 * <ol>
 *   <li>检查能力记录是否被外部重新启用 → FAILED</li>
 *   <li>排空门控：节点存活时等待 actual=DISABLED；已 DEAD/LEAVING 时跳过 actual 收敛</li>
 *   <li>放置表门控：PRIMARY 行仍引用该节点 → 继续等待；仅 REPLICA → reassignReplicas</li>
 *   <li>门控全通过 → REMOVING</li>
 * </ol>
 *
 * <h2>REMOVING 阶段（单 tick，每步幂等）</h2>
 * <ol>
 *   <li>markState(LEAVING) — try/catch 竞态</li>
 *   <li>tombstone → DEAD</li>
 *   <li>防御性放置表检查</li>
 *   <li>capabilityStore.delete(node)</li>
 *   <li>setState(COMPLETED)</li>
 * </ol>
 */
public final class RuntimeDecommissionCoordinator implements DecommissionDriver {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeDecommissionCoordinator.class);

    /** S15：单轮 advance 每能力副本集重算的分区上限（控制占用调谐线程的时长）。 */
    private static final int REASSIGN_AWAY_BATCH_MAX = 256;

    private final DecommissionStore store;
    private final NodeRegistry registry;
    private final CapabilityStateStore capabilityStore;
    private final PlacementStore placementStore;
    private final PlacementEngine engine;
    private final SchedulerConfig config;
    private final Clock clock;

    public RuntimeDecommissionCoordinator(DecommissionStore store,
                                          NodeRegistry registry,
                                          CapabilityStateStore capabilityStore,
                                          PlacementStore placementStore,
                                          PlacementEngine engine,
                                          SchedulerConfig config,
                                          Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capabilityStore = Objects.requireNonNull(capabilityStore, "capabilityStore");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void advance() {
        List<DecommissionRecord> active = store.listActive();
        if (active.isEmpty()) {
            return;
        }
        // S15：单轮 advance 内按能力缓存全量放置扫描（同能力多退役节点不再重复拉取）
        Map<String, List<Placement>> placementCache = new HashMap<>();
        for (DecommissionRecord record : active) {
            try {
                advanceOne(record, placementCache);
            } catch (Exception e) {
                LOG.warn("decommission advance failed for node {}: {}", record.node(), e.getMessage(), e);
            }
        }
    }

    private void advanceOne(DecommissionRecord record, Map<String, List<Placement>> placementCache) {
        switch (record.state()) {
            case DRAINING -> advanceDraining(record, placementCache);
            case REMOVING -> advanceRemoving(record, placementCache);
            default -> {
                // terminal states — no-op
            }
        }
    }

    /** S15：缓存版全量放置读取（仅本轮有效；跨周期状态以重读为准，幂等语义不变）。 */
    private List<Placement> cachedList(Map<String, List<Placement>> cache, String capability) {
        return cache.computeIfAbsent(capability, placementStore::list);
    }

    private void advanceDraining(DecommissionRecord record, Map<String, List<Placement>> placementCache) {
        NodeId node = record.node();

        List<CapabilityRecord> nodeRecords = capabilityStore.list().stream()
                .filter(r -> r.node().equals(node))
                .toList();

        for (CapabilityRecord cr : nodeRecords) {
            if (cr.desired() == CapabilityState.ACTIVE) {
                store.setState(node, DecommissionState.FAILED,
                        "期望态被外部重新启用: " + cr.type());
                LOG.info("decommission FAILED for node {}: capability {} re-enabled", node, cr.type());
                return;
            }
        }

        NodeInfo nodeInfo = registry.get(node).orElse(null);
        boolean nodeAlive = nodeInfo != null
                && nodeInfo.state() != NodeState.DEAD
                && nodeInfo.state() != NodeState.LEAVING;

        if (nodeAlive) {
            boolean allDisabled = nodeRecords.stream()
                    .allMatch(r -> r.actual() == CapabilityState.DISABLED);
            if (!allDisabled) {
                return;
            }
        }

        for (CapabilityRecord cr : nodeRecords) {
            CapabilityType type = cr.type();
            List<Placement> placements = cachedList(placementCache, type.name());
            List<Placement> nodePlacements = placements.stream()
                    .filter(p -> p.node().equals(node))
                    .toList();

            boolean hasPrimary = nodePlacements.stream().anyMatch(Placement::isPrimary);
            if (hasPrimary) {
                return;
            }

            boolean hasReplica = !nodePlacements.isEmpty();
            if (hasReplica) {
                reassignReplicasAway(node, type, placements);
                return; // 本轮限额处理；剩余副本行留待下轮（门控重入幂等）
            }
        }

        store.setState(node, DecommissionState.REMOVING, null);
        LOG.info("decommission DRAINING→REMOVING for node {}", node);
    }

    private void reassignReplicasAway(NodeId node, CapabilityType type, List<Placement> allPlacements) {
        // P3-2/P1-9：改用注册表实际权重与故障域（旧实现硬编码 1.0/无故障域，
        // 与 leader 加权目标环漂移，两套目标环反复互相改写副本行）
        List<PlacementNode> placementNodes = registry.listAlive().stream()
                .filter(info -> !info.id().equals(node))
                .sorted(java.util.Comparator.comparing(i -> i.id().value()))
                .map(info -> new PlacementNode(info.id(), info.weight(), info.zone()))
                .toList();

        if (placementNodes.isEmpty()) {
            return;
        }

        int rf = config.rfFor(type.name());
        var computed = engine.compute(type.name(), placementNodes, config.partitionCount(), rf);

        // S15：每轮限额——按分区序取前 REASSIGN_AWAY_BATCH_MAX 个受影响分区，
        // 防止退役大集群时单周期重算/重写全部 P 分区拖爆调谐节拍（危及租约）
        java.util.List<Integer> affectedPartitions = allPlacements.stream()
                .filter(p -> p.node().equals(node) && !p.isPrimary())
                .map(Placement::partition)
                .distinct()
                .sorted()
                .limit(REASSIGN_AWAY_BATCH_MAX)
                .toList();

        Map<Integer, java.util.List<NodeId>> fixes = new java.util.LinkedHashMap<>();
        for (int partition : affectedPartitions) {
            List<Placement> newPlacements = computed.get(partition);
            List<NodeId> newReplicas = newPlacements == null ? List.of()
                    : newPlacements.stream()
                    .filter(p -> !p.isPrimary())
                    .map(Placement::node)
                    .toList();
            fixes.put(partition, newReplicas);
        }
        placementStore.reassignReplicasBatch(type.name(), fixes); // S8：32 分区/事务
    }

    private void advanceRemoving(DecommissionRecord record, Map<String, List<Placement>> placementCache) {
        NodeId node = record.node();

        NodeInfo nodeInfo = registry.get(node).orElse(null);
        if (nodeInfo != null && nodeInfo.state() != NodeState.DEAD && nodeInfo.state() != NodeState.LEAVING) {
            try {
                registry.markState(node, NodeState.LEAVING);
            } catch (IllegalStateTransitionException e) {
                LOG.debug("markState(LEAVING) raced for node {}: {}", node, e.getMessage());
            }
        }

        registry.tombstone(node);

        for (CapabilityType type : CapabilityType.values()) {
            List<Placement> placements = cachedList(placementCache, type.name());
            boolean stillReferenced = placements.stream()
                    .anyMatch(p -> p.node().equals(node));
            if (stillReferenced) {
                store.setState(node, DecommissionState.FAILED,
                        "放置表仍存在该节点引用 (" + type + ")");
                LOG.warn("decommission FAILED for node {}: placement still references it for {}",
                        node, type);
                return;
            }
        }

        capabilityStore.delete(node);

        long now = clock.nowMillis();
        store.upsert(new DecommissionRecord(
                node, DecommissionState.COMPLETED, record.force(), null,
                record.startedAtMillis(), now, now));
        LOG.info("decommission COMPLETED for node {}", node);
    }
}
