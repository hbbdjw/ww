package com.ww.runtime.scheduler;

import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.RebalancePlanner;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 缺陷复现：节点故障触发的分区「治愈」迁移被 {@code minMoveRatio} 扩容门控拦截。
 *
 * <p>{@code ReconcileLoop.reconcile} 的步骤 (f) 把「故障节点分区迁走」与「扩容重平衡」
 * 混在同一条路径上，共用 {@code RebalancePlanner.shouldRebalance} 门控
 * （移动比例 ≥ minMoveRatio 且过冷却期）。大集群下单节点故障只影响约 1/N 的分区，
 * N 较大时 1/N &lt; minMoveRatio（默认 0.10 → N ≥ 11 即触发），故障节点上的分区
 * <b>永远不会被迁走</b>：放置表持续指向 DEAD 节点，归属它的分区读写全部不可用，
 * 直到更多节点接连故障凑够比例——可用性缺陷。</p>
 */
class DeadNodeHealingGateBugReproTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 256;
    private static final int NODE_COUNT = 16;
    private static final double MIN_MOVE_RATIO = 0.10d;
    private static final long COOLDOWN_MS = 30_000L;

    @Test
    void deadNodePartitionsAreNeverHealedWhenBelowMinMoveRatio() {
        TestClock clock = new TestClock(1_000L);
        InMemoryLeaseStore lease = new InMemoryLeaseStore(clock);
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        InMemoryCapabilityStateStore capabilities = new InMemoryCapabilityStateStore(clock);
        InMemoryPlacementStore placements = new InMemoryPlacementStore();
        PlacementEngine engine = new PlacementEngine();
        RecordingCommands commands = new RecordingCommands();
        CutoverMigrationCoordinator migration = new CutoverMigrationCoordinator(placements);
        DesiredStateWriter writer = new DesiredStateWriter(capabilities);
        ReconcileLoop loop = new ReconcileLoop(NodeId.of("scheduler"), lease, registry,
                capabilities, placements, engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 1, MIN_MOVE_RATIO, COOLDOWN_MS),
                clock);

        List<NodeId> nodes = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            NodeId node = NodeId.of("n" + i);
            nodes.add(node);
            registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                    NodeState.JOINING, 0L, 1.0d,
                    ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
            registry.markState(node, NodeState.ACTIVE);
            writer.setDesired(node, MOCK, CapabilityState.ACTIVE);
            capabilities.setActual(node, MOCK, CapabilityState.ACTIVE);
        }
        NodeId dead = nodes.get(5);

        // 首轮调谐：引导放置环（16 节点均分 256 分区）
        assertThat(loop.runOnce()).isTrue();
        Map<Integer, NodeId> before = placements.primaries(MOCK.name());
        assertThat(before).hasSize(PARTITIONS);
        long deadShare = before.values().stream().filter(dead::equals).count();
        // 前置条件：故障节点确实在环上持有分区，且占比 < minMoveRatio（触发门控的关键）
        assertThat(deadShare).as("rendezvous 均分下 n5 应持有分区").isGreaterThan(0);
        assertThat((double) deadShare / PARTITIONS)
                .as("单节点故障占比 ≈ 1/%d，低于门控 %.2f", NODE_COUNT, MIN_MOVE_RATIO)
                .isLessThan(MIN_MOVE_RATIO);

        // 节点 n5 宕机（DEAD），越过冷却期后连续多个调谐周期
        registry.tombstone(dead);
        for (int cycle = 0; cycle < 5; cycle++) {
            clock.advance(COOLDOWN_MS + 1_000L);
            assertThat(loop.runOnce()).isTrue();
        }

        Map<Integer, NodeId> after = placements.primaries(MOCK.name());
        long stillOnDead = after.values().stream().filter(dead::equals).count();
        assertThat(stillOnDead)
                .as("节点已 DEAD 且冷却期已过，其分区应被治愈迁走；实际 %d/%d 分区仍指向 DEAD 节点"
                        + "（shouldRebalance 的 minMoveRatio 门控拦截了故障治愈迁移）",
                        stillOnDead, PARTITIONS)
                .isZero();
    }
}
