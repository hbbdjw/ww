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
import com.ww.runtime.placement.InMemoryDegradedPartitionStore;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P0-2 隔离治愈单测：无存活副本分区（CACHE/MOCK 等 RF=1 能力，属主死亡即最后副本丢失）
 * 在治愈宽限期赌输后标记 DEGRADED 隔离——放置保持指向死亡属主（数据留盘押注重启）、
 * 零迁移、数据面拒绝服务；属主回归自动解除；开关关闭时回退旧的空分区迁移行为。
 */
class ReconcileLoopHealingQuarantineTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 16;

    private static final NodeId SCHEDULER = NodeId.of("scheduler-node");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    private TestClock clock;
    private InMemoryLeaseStore lease;
    private InMemoryNodeRegistry registry;
    private InMemoryCapabilityStateStore capabilities;
    private InMemoryPlacementStore placements;
    private InMemoryDegradedPartitionStore degraded;
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
        degraded = new InMemoryDegradedPartitionStore();
        engine = new PlacementEngine();
        commands = new RecordingCommands();
        migration = new CutoverMigrationCoordinator(placements);
        writer = new DesiredStateWriter(capabilities);
        loop = newLoop(0L, true);
        for (NodeId node : List.of(A, B)) {
            registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                    NodeState.JOINING, 0L, 1.0d,
                    ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
        }
    }

    /** healingGracePeriodMs=0 = 宽限期立即赌输；quarantine 控制隔离开关。 */
    private ReconcileLoop newLoop(long healingGracePeriodMs, boolean quarantine) {
        return new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                engine, new RebalancePlanner(), commands, migration,
                new SchedulerConfig(5_000L, 1_000L, 500L, PARTITIONS, 1, 1, 0.10d, 30_000L,
                        healingGracePeriodMs, 12_000L, 1, false, quarantine),
                clock, null, degraded);
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

    private Set<Integer> partitionsOf(NodeId node) {
        return placements.primaries(MOCK.name()).entrySet().stream()
                .filter(e -> e.getValue().equals(node))
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }

    /** 引导 A/B 双节点环并返回 B 的分区集合。 */
    private Set<Integer> bootstrapTwoNodeRing() {
        activate(A);
        activate(B);
        assertThat(loop.runOnce()).isTrue();
        assertThat(placements.primaries(MOCK.name())).hasSize(PARTITIONS);
        return partitionsOf(B);
    }

    @Test
    void deadNodePartitionsQuarantinedInsteadOfEmptyMigration() {
        Set<Integer> bPartitions = bootstrapTwoNodeRing();
        assertThat(bPartitions).isNotEmpty();
        Map<Integer, NodeId> before = placements.primaries(MOCK.name());

        registry.markState(B, NodeState.DEAD);
        assertThat(loop.runOnce()).isTrue();

        // 隔离：B 的全部分区标记 DEGRADED
        assertThat(degraded.partitionsOf(MOCK.name()))
                .containsExactlyInAnyOrderElementsOf(bPartitions);
        // 零迁移：不再以空分区覆盖放置表
        assertThat(migration.recorded).isEmpty();
        // 放置未改挂：owner 仍是 B（数据留盘，押注重启回归）
        Map<Integer, NodeId> after = placements.primaries(MOCK.name());
        assertThat(after).isEqualTo(before);
        bPartitions.forEach(p -> assertThat(after.get(p)).isEqualTo(B));
    }

    @Test
    void quarantinedPartitionsStayPutUntilOwnerRejoins() {
        Set<Integer> bPartitions = bootstrapTwoNodeRing();
        registry.markState(B, NodeState.DEAD);
        assertThat(loop.runOnce()).isTrue();
        assertThat(degraded.partitionsOf(MOCK.name()))
                .containsExactlyInAnyOrderElementsOf(bPartitions);

        // 后续周期：已隔离分区不触发任何动作（无重复标记、无迁移、放置不动）
        assertThat(loop.runOnce()).isTrue();
        assertThat(loop.runOnce()).isTrue();
        assertThat(migration.recorded).isEmpty();
        assertThat(placements.primaries(MOCK.name())).containsAllEntriesOf(
                bPartitions.stream()
                        .collect(java.util.stream.Collectors.toMap(p -> p, p -> B)));

        // 属主回归（DEAD → JOINING → ACTIVE，状态机路径）：自动解除隔离；
        // 放置始终指向 B → 数据原样恢复，零迁移
        registry.markState(B, NodeState.JOINING);
        registry.markState(B, NodeState.ACTIVE);
        assertThat(loop.runOnce()).isTrue();
        assertThat(degraded.partitionsOf(MOCK.name())).isEmpty();
        assertThat(migration.recorded).isEmpty();
        assertThat(placements.primaries(MOCK.name())).containsAllEntriesOf(
                bPartitions.stream()
                        .collect(java.util.stream.Collectors.toMap(p -> p, p -> B)));
    }

    @Test
    void withinGracePeriodNothingIsQuarantined() {
        loop = newLoop(30_000L, true);
        Set<Integer> bPartitions = bootstrapTwoNodeRing();
        registry.markState(B, NodeState.DEAD);

        // 宽限期内：押注重启，不隔离、不迁移、放置不动
        assertThat(loop.runOnce()).isTrue();
        assertThat(degraded.partitionsOf(MOCK.name())).isEmpty();
        assertThat(migration.recorded).isEmpty();

        // 宽限期赌输（时钟推进 30s）：进入隔离
        clock.advance(30_000L);
        assertThat(loop.runOnce()).isTrue();
        assertThat(degraded.partitionsOf(MOCK.name()))
                .containsExactlyInAnyOrderElementsOf(bPartitions);
        assertThat(migration.recorded).isEmpty();
    }

    @Test
    void quarantineDisabledFallsBackToEmptyMigration() {
        loop = newLoop(0L, false); // 回滚对照：旧行为
        bootstrapTwoNodeRing();
        registry.markState(B, NodeState.DEAD);

        assertThat(loop.runOnce()).isTrue();

        // 旧行为：空分区迁移（静默丢失路径），不产生隔离标记
        assertThat(degraded.partitionsOf(MOCK.name())).isEmpty();
        assertThat(migration.recorded).isNotEmpty();
        assertThat(migration.recorded)
                .allSatisfy(move -> assertThat(move.from()).isEqualTo(B));
        assertThat(placements.primaries(MOCK.name()).values()).noneMatch(B::equals);
    }

    @Test
    void storageDeadPrimaryWithLiveReplicaPromotesNotQuarantines() {
        CapabilityType storage = CapabilityType.STORAGE;
        ReconcileLoop storageLoop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities,
                placements, engine, new RebalancePlanner(), commands, migration,
                new SchedulerConfig(5_000L, 1_000L, 500L, PARTITIONS, 2, 1, 0.10d, 30_000L,
                        0L, 12_000L, 1, false, true),
                clock, null, degraded);
        writer.setDesired(A, storage, CapabilityState.ACTIVE);
        capabilities.setActual(A, storage, CapabilityState.ACTIVE);
        writer.setDesired(B, storage, CapabilityState.ACTIVE);
        capabilities.setActual(B, storage, CapabilityState.ACTIVE);
        assertThat(storageLoop.runOnce()).isTrue();
        assertThat(placements.placements(storage.name(), 0)).hasSize(2); // PRIMARY + REPLICA

        // B 死（rf=2）：其主分区有存活副本 → 提升，不进隔离
        registry.markState(B, NodeState.DEAD);
        assertThat(storageLoop.runOnce()).isTrue();

        assertThat(degraded.partitionsOf(storage.name())).isEmpty();
        assertThat(migration.recorded).isEmpty(); // 提升免迁移
        assertThat(placements.primaries(storage.name()).values()).noneMatch(B::equals);
        assertThat(capabilities.get(A, storage)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);
    }
}
