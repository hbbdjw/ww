package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ReconcileLoopTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 16;
    private static final long COOLDOWN_MS = 30_000L;

    private static final NodeId SCHEDULER = NodeId.of("scheduler-node");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId C = NodeId.of("C");
    private static final NodeId D = NodeId.of("D");

    private TestClock clock;
    private InMemoryLeaseStore lease;
    private InMemoryNodeRegistry registry;
    private InMemoryCapabilityStateStore capabilities;
    private InMemoryPlacementStore placements;
    private PlacementEngine engine;
    private RecordingCommands commands;
    private CutoverMigrationCoordinator migration;
    private DesiredStateWriter writer;
    private ReconcileLoop loop;

    @BeforeEach
    void setUp() {
        clock = new TestClock(1_000L);
        lease = new InMemoryLeaseStore(clock);
        registry = new InMemoryNodeRegistry();
        capabilities = new InMemoryCapabilityStateStore(clock);
        placements = new InMemoryPlacementStore();
        engine = new PlacementEngine();
        commands = new RecordingCommands();
        migration = new CutoverMigrationCoordinator(placements);
        writer = new DesiredStateWriter(capabilities);
        loop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 1, 0.10d, COOLDOWN_MS),
                clock);
        for (NodeId node : List.of(A, B, C, D)) {
            register(node);
        }
    }

    private void register(NodeId node) {
        registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
    }

    private void activate(NodeId node) {
        writer.setDesired(node, MOCK, CapabilityState.ACTIVE);
        capabilities.setActual(node, MOCK, CapabilityState.ACTIVE);
    }

    private static List<Placement> primaryPlacements(Map<Integer, NodeId> primaries) {
        List<Placement> result = new ArrayList<>();
        primaries.forEach((partition, node) ->
                result.add(new Placement(partition, node, PlacementRole.PRIMARY, 0L)));
        result.sort(Comparator.comparingInt(Placement::partition));
        return result;
    }

    @Test
    void bootstrapsPlacementAndStartsCapabilities() {
        writer.setDesired(A, MOCK, CapabilityState.ACTIVE);
        writer.setDesired(B, MOCK, CapabilityState.ACTIVE);

        assertThat(loop.runOnce()).isTrue();

        assertThat(loop.isLeader()).isTrue();
        Map<Integer, NodeId> primaries = placements.primaries(MOCK.name());
        assertThat(primaries).hasSize(PARTITIONS);
        assertThat(primaries.values())
                .allMatch(node -> node.equals(A) || node.equals(B));
        assertThat(new HashSet<>(primaries.values())).containsExactlyInAnyOrder(A, B);

        assertThat(commands.started()).containsExactlyInAnyOrder(A.value(), B.value());
        assertThat(capabilities.get(A, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);
        assertThat(capabilities.get(B, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);

        ReconcileLoop.ClusterStatus status = loop.snapshot();
        assertThat(status.leader()).isTrue();
        assertThat(status.leaseOwner()).isEqualTo(SCHEDULER.value());
        assertThat(status.primariesByCapability()).containsKey(MOCK.name());
        assertThat(status.primariesByCapability().get(MOCK.name())).hasSize(PARTITIONS);
    }

    @Test
    void r5DisablingNodeMigratesOnlyItsPartitionsAway() {
        // 前置：MOCK 在 A/B 为 ACTIVE；C 已 desired-ACTIVE 未上线；placement 已建（环 = {A,B}）
        activate(A);
        activate(B);
        writer.setDesired(C, MOCK, CapabilityState.ACTIVE);
        placements.replaceAll(MOCK.name(), primaryPlacements(
                engine.computePrimaries(List.of(new PlacementNode(A, 1.0d), new PlacementNode(B, 1.0d)),
                        PARTITIONS)));

        Map<Integer, NodeId> before = placements.primaries(MOCK.name());
        Set<Integer> partitionsOfA = before.entrySet().stream()
                .filter(entry -> entry.getValue().equals(A))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        assertThat(partitionsOfA).isNotEmpty();

        // 管理面：排空 A
        writer.setDesired(A, MOCK, CapabilityState.DISABLED);

        assertThat(loop.runOnce()).isTrue();

        // 迁移 moves 全部 from=A，且恰好覆盖 A 的全部分区
        assertThat(migration.recorded).isNotEmpty();
        assertThat(migration.recorded)
                .allSatisfy(move -> assertThat(move.from()).isEqualTo(A));
        assertThat(migration.recorded).map(PartitionMove::partition)
                .containsExactlyInAnyOrderElementsOf(partitionsOfA);

        // A 成功下线：freeze(true) → 迁移 → freeze(false) → stop → actual=DISABLED
        assertThat(capabilities.get(A, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
        assertThat(commands.stopped()).containsExactly(A.value());
        assertThat(commands.freezes()).containsSubsequence("freeze:" + A.value() + ":true",
                "freeze:" + A.value() + ":false");

        // C 顺带完成上线
        assertThat(capabilities.get(C, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);

        // primaries 中不再有 A；A 的分区由 B/C 接管；其余分区不动
        Map<Integer, NodeId> after = placements.primaries(MOCK.name());
        assertThat(after).hasSize(PARTITIONS);
        assertThat(after.values()).noneMatch(A::equals);
        for (int partition : partitionsOfA) {
            assertThat(after.get(partition)).isIn(B, C);
        }
        before.forEach((partition, owner) -> {
            if (!owner.equals(A)) {
                assertThat(after.get(partition)).as("partition %d", partition).isEqualTo(owner);
            }
        });
    }

    @Test
    void scaleUpStartsNewNodeAndRebalancesAfterCooldown() {
        // 阶段 0：环建于 {B,C}，D 尚未登记为 desired-ACTIVE
        activate(B);
        activate(C);
        assertThat(loop.runOnce()).isTrue();
        Map<Integer, NodeId> bootstrap = placements.primaries(MOCK.name());
        assertThat(bootstrap).hasSize(PARTITIONS);
        assertThat(bootstrap.values()).allMatch(node -> node.equals(B) || node.equals(C));

        // 阶段 1：新节点 D 上线（desired=ACTIVE）——重平衡被冷却期挡住
        writer.setDesired(D, MOCK, CapabilityState.ACTIVE);
        assertThat(loop.runOnce()).isTrue();

        assertThat(capabilities.get(D, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);
        assertThat(migration.recorded).isEmpty();
        Map<Integer, NodeId> before = placements.primaries(MOCK.name());
        assertThat(before).isEqualTo(bootstrap);

        // 周期 2（冷却已过）：扩容重平衡发生
        clock.advance(COOLDOWN_MS + 1);
        assertThat(loop.runOnce()).isTrue();

        assertThat(migration.recorded).isNotEmpty();
        assertThat(migration.recorded)
                .allSatisfy(move -> assertThat(move.to()).isEqualTo(D));

        Map<Integer, NodeId> after = placements.primaries(MOCK.name());
        assertThat(after).hasSize(PARTITIONS);
        assertThat(after.values()).contains(D);

        // 未变分区不动：primary 与 epoch 均保持
        Set<Integer> moved = migration.recorded.stream()
                .map(PartitionMove::partition)
                .collect(Collectors.toSet());
        assertThat(moved).isNotEmpty();
        before.forEach((partition, owner) -> {
            if (!moved.contains(partition)) {
                assertThat(after.get(partition)).as("partition %d", partition).isEqualTo(owner);
                assertThat(placements.get(MOCK.name(), partition)).map(Placement::epoch)
                        .contains(0L);
            }
        });
    }

    @Test
    void nonLeaderProducesNoSideEffects() {
        assertThat(lease.acquireOrRenew("external-owner", 60_000L)).isTrue();
        activate(A);

        assertThat(loop.runOnce()).isFalse();

        assertThat(loop.isLeader()).isFalse();
        assertThat(placements.primaries(MOCK.name())).isEmpty();
        assertThat(commands.calls).isEmpty();
        assertThat(capabilities.get(A, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE); // 未被改动
        assertThat(lease.currentOwner()).contains("external-owner");
    }

    @Test
    void storeFailureDoesNotBreakReconcile() {
        writer.setDesired(A, MOCK, CapabilityState.ACTIVE);
        writer.setDesired(B, MOCK, CapabilityState.ACTIVE);

        FlakyCapabilityStore flaky = new FlakyCapabilityStore(capabilities, 1);
        ReconcileLoop flakyLoop = new ReconcileLoop(SCHEDULER, lease, registry, flaky,
                placements, engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 1, 0.10d, COOLDOWN_MS),
                clock);

        // 第 1 周期：list() 抛异常 → 被吞掉，不向上传播
        assertThat(flakyLoop.runOnce()).isTrue();
        assertThat(placements.primaries(MOCK.name())).isEmpty();

        // 第 2 周期：存储恢复，正常收敛
        assertThat(flakyLoop.runOnce()).isTrue();
        assertThat(placements.primaries(MOCK.name())).hasSize(PARTITIONS);
        assertThat(flakyLoop.isLeader()).isTrue();
    }
}
