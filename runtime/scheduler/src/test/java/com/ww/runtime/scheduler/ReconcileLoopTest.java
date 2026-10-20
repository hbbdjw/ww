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

    /** 全禁用（desired 全 DISABLED）：排空停止节点且<b>清空放置表</b>——残留 owner 指向已停节点会导致读写 503 与再启用迁移风暴。 */
    @Test
    void disablingAllNodesStopsCapabilityAndClearsPlacementRing() {
        activate(A);
        activate(B);
        assertThat(loop.runOnce()).isTrue();
        assertThat(placements.primaries(MOCK.name())).hasSize(PARTITIONS);

        // 第一轮：禁 A（B 仍 desired-ACTIVE）→ A 的分区正常迁移给 B 后停止
        writer.setDesired(A, MOCK, CapabilityState.DISABLED);
        assertThat(loop.runOnce()).isTrue();
        assertThat(capabilities.get(A, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
        assertThat(migration.recorded).isNotEmpty();
        assertThat(migration.recorded)
                .allSatisfy(move -> assertThat(move.from()).isEqualTo(A));

        // 第二轮：排空最后节点 B（desiredAlive 空）→ 无处可迁不产生迁移，直接停止
        int migrationsBeforeLastDrain = migration.recorded.size();
        writer.setDesired(B, MOCK, CapabilityState.DISABLED);
        assertThat(loop.runOnce()).isTrue();
        assertThat(commands.stopped()).containsExactlyInAnyOrder(A.value(), B.value());
        assertThat(capabilities.get(B, MOCK)).map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
        assertThat(migration.recorded).hasSize(migrationsBeforeLastDrain); // 最后一步无新迁移

        // 放置表清空：owner 不再指向已停节点；再启用走引导路径重建环
        assertThat(placements.primaries(MOCK.name())).isEmpty();
        assertThat(placements.list(MOCK.name())).isEmpty();

        // 再启用 A：引导重建（而非基于残留环规划 from=已停能力的迁移）
        activate(A);
        assertThat(loop.runOnce()).isTrue();
        Map<Integer, NodeId> rebuilt = placements.primaries(MOCK.name());
        assertThat(rebuilt).hasSize(PARTITIONS);
        assertThat(rebuilt.values()).allMatch(A::equals);
        assertThat(migration.recorded).hasSize(migrationsBeforeLastDrain); // 引导 replaceAll，不产生迁移任务
    }

    @Test
    void r5DisablingNodeMigratesOnlyItsPartitionsAway() {
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

    /**
     * STORAGE（rf=2）：引导放置含 PRIMARY+REPLICA 行；禁用节点 B 后其主分区经
     * <b>副本提升</b>排空（结构性场景：2 节点环中排空目标必为副本持有者，走迁移 +
     * cutover 会违反放置主键）、节点停止、actual=DISABLED，全程零迁移任务。
     */
    @Test
    void storageBootstrapsReplicasAndMigratesOnDisable() {
        CapabilityType storage = CapabilityType.STORAGE;
        ReconcileLoop storageLoop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities,
                placements, engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 2, 1, 0.10d, COOLDOWN_MS),
                clock);
        writer.setDesired(A, storage, CapabilityState.ACTIVE);
        capabilities.setActual(A, storage, CapabilityState.ACTIVE);
        writer.setDesired(B, storage, CapabilityState.ACTIVE);
        capabilities.setActual(B, storage, CapabilityState.ACTIVE);

        assertThat(storageLoop.runOnce()).isTrue();

        // 引导放置：每分区 PRIMARY + REPLICA 两行，副本异节点分布
        List<Placement> rows = placements.list(storage.name());
        assertThat(rows).hasSize(PARTITIONS * 2);
        Map<Integer, List<Placement>> byPartition = rows.stream()
                .collect(Collectors.groupingBy(Placement::partition));
        assertThat(byPartition).hasSize(PARTITIONS);
        byPartition.forEach((partition, partRows) -> {
            assertThat(partRows).hasSize(2);
            assertThat(partRows.get(0).isPrimary()).isTrue();
            assertThat(partRows.get(1).isPrimary()).isFalse();
            assertThat(partRows.get(0).node()).isNotEqualTo(partRows.get(1).node());
        });

        Map<Integer, NodeId> before = placements.primaries(storage.name());
        Set<Integer> partitionsOfB = before.entrySet().stream()
                .filter(entry -> entry.getValue().equals(B))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        assertThat(partitionsOfB).isNotEmpty();

        // 管理面：禁用 B → 其主分区副本提升排空后停止（目标 A 必持有副本，零迁移任务）
        writer.setDesired(B, storage, CapabilityState.DISABLED);
        assertThat(storageLoop.runOnce()).isTrue();

        assertThat(migration.recorded)
                .as("2 节点环排空目标即副本持有者，应全部提升完成，不产生迁移任务")
                .isEmpty();

        assertThat(capabilities.get(B, storage)).map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
        assertThat(commands.stopped()).containsExactly(B.value());
        assertThat(commands.freezes()).containsSubsequence("freeze:" + B.value() + ":true",
                "freeze:" + B.value() + ":false");

        Map<Integer, NodeId> after = placements.primaries(storage.name());
        assertThat(after).hasSize(PARTITIONS);
        assertThat(after.values()).noneMatch(B::equals);
        before.forEach((partition, owner) -> {
            if (!owner.equals(B)) {
                assertThat(after.get(partition)).as("partition %d", partition).isEqualTo(owner);
            }
        });
    }

    /**
     * 副本集补足：模拟 promote 消耗副本行后无人重建的泄漏（分区只剩 PRIMARY 行）。
     * 主环已收敛时，冷却期内不动作；冷却期过后的周期把缺失副本行重建到目标环节点。
     */
    @Test
    void replicaTopUpRestoresMissingReplicaRowsAfterConvergence() {
        CapabilityType storage = CapabilityType.STORAGE;
        ReconcileLoop storageLoop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities,
                placements, engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 2, 1, 0.10d, COOLDOWN_MS),
                clock);
        for (NodeId node : List.of(A, B, C)) {
            writer.setDesired(node, storage, CapabilityState.ACTIVE);
            capabilities.setActual(node, storage, CapabilityState.ACTIVE);
        }

        // 引导：每分区 PRIMARY + REPLICA 两行（此周期同时记录补足节流时间戳）
        assertThat(storageLoop.runOnce()).isTrue();
        assertThat(placements.list(storage.name())).hasSize(PARTITIONS * 2);

        // 模拟 promote 泄漏：删掉一个分区的全部副本行，只剩主行
        int victim = placements.primaries(storage.name()).keySet().iterator().next();
        placements.reassignReplicas(storage.name(), victim, List.of());
        assertThat(placements.list(storage.name())).hasSize(PARTITIONS * 2 - 1);

        // 冷却期内：补足不执行
        assertThat(storageLoop.runOnce()).isTrue();
        assertThat(placements.list(storage.name())).hasSize(PARTITIONS * 2 - 1);

        // 冷却期过后：缺失副本行重建（主不动，副本落在异节点）
        clock.advance(COOLDOWN_MS + 1);
        assertThat(storageLoop.runOnce()).isTrue();

        List<Placement> all = placements.list(storage.name());
        assertThat(all).hasSize(PARTITIONS * 2);
        List<Placement> victimRows = all.stream()
                .filter(p -> p.partition() == victim)
                .toList();
        assertThat(victimRows).hasSize(2);
        assertThat(victimRows.stream().filter(Placement::isPrimary).count()).isEqualTo(1);
        Placement replica = victimRows.stream()
                .filter(p -> !p.isPrimary())
                .findFirst()
                .orElseThrow();
        NodeId primaryNode = victimRows.stream()
                .filter(Placement::isPrimary)
                .map(Placement::node)
                .findFirst()
                .orElseThrow();
        assertThat(replica.node()).isNotEqualTo(primaryNode);
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
