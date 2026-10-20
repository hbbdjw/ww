package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
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
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证 RF≥2 禁用节点排空的行为：排空目标 = 旧环第二顺位 = 该分区 REPLICA 行
 * 持有者（cutover 会违反放置主键 (capability, partition_id, node_id)）。
 * 当前验证行为：
 * <ul>
 *   <li>目标已持有副本的分区直接 {@code promoteReplica}（无损免迁移），
 *       排空单周期收敛，actual 回写 DISABLED；</li>
 *   <li>确需迁移的分区失败时进入排空冷却，冷却窗口内不重复提交任务。</li>
 * </ul>
 */
class DrainReplicaCollisionReproTest {

    private static final CapabilityType STORAGE = CapabilityType.STORAGE;
    private static final int PARTITIONS = 64;
    private static final long COOLDOWN_MS = 30_000L;

    private static final NodeId SCHEDULER = NodeId.of("scheduler-node");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId C = NodeId.of("C");
    private static final NodeId D = NodeId.of("D");
    /** 被禁用的节点。 */
    private static final NodeId E = NodeId.of("E");

    private TestClock clock;
    private InMemoryLeaseStore lease;
    private InMemoryNodeRegistry registry;
    private InMemoryCapabilityStateStore capabilities;
    private InMemoryPlacementStore placements;
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
        commands = new RecordingCommands();
        migration = new CutoverMigrationCoordinator(placements);
        writer = new DesiredStateWriter(capabilities);
        // rf=2：每分区 PRIMARY + 1 REPLICA 行（STORAGE 场景）
        loop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                new PlacementEngine(), new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 2, 0.10d, COOLDOWN_MS),
                clock);
        for (NodeId node : List.of(A, B, C, D, E)) {
            registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                    NodeState.JOINING, 0L, 1.0d,
                    ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
            writer.setDesired(node, STORAGE, CapabilityState.ACTIVE);
            capabilities.setActual(node, STORAGE, CapabilityState.ACTIVE);
        }
        // 引导 5 节点环（rf=2）
        assertThat(loop.runOnce()).isTrue();
        assertThat(placements.primaries(STORAGE.name())).hasSize(PARTITIONS);
        assertThat(placements.primaries(STORAGE.name()).values()).contains(E);
    }

    @Test
    void disablingNodeDrainsViaReplicaPromotionWithoutMigration() {
        long partitionsOnE = placements.primaries(STORAGE.name()).values().stream()
                .filter(E::equals).count();
        assertThat(partitionsOnE).isGreaterThan(0);

        writer.setDesired(E, STORAGE, CapabilityState.DISABLED);

        assertThat(loop.runOnce()).isTrue();

        // 结构性场景 100% 命中：E 为主的每个分区，排空目标都持有其副本行
        // → 全部提升完成，不应产生任何迁移任务
        assertThat(migration.recorded)
                .as("排空目标即副本持有者，应全部副本提升，不产生迁移任务")
                .isEmpty();

        // 排空完成：actual 回写 DISABLED、能力停止、冻结解除
        assertThat(capabilities.get(E, STORAGE)).map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
        assertThat(commands.stopped()).contains(E.value());
        assertThat(commands.freezes()).contains("freeze:" + E.value() + ":false");

        // 放置收敛到 4 节点目标环：无残留 E 属主；每分区 1 主 + 1 副本（异节点）
        Map<Integer, NodeId> expected = new PlacementEngine().computePrimaries(
                List.of(PlacementNode.of("A"), PlacementNode.of("B"),
                        PlacementNode.of("C"), PlacementNode.of("D")),
                PARTITIONS);
        assertThat(placements.primaries(STORAGE.name())).isEqualTo(expected);
        for (int partition = 0; partition < PARTITIONS; partition++) {
            List<Placement> rows = placements.placements(STORAGE.name(), partition);
            assertThat(rows).as("partition %d rows", partition).hasSize(2);
            assertThat(rows.getFirst().isPrimary()).isTrue();
            assertThat(rows.get(1).isPrimary()).isFalse();
            assertThat(rows.get(1).node()).isNotEqualTo(rows.getFirst().node());
        }

        // 第二周期幂等：无残留动作，迁移记录保持为空
        assertThat(loop.runOnce()).isTrue();
        assertThat(migration.recorded).isEmpty();
    }

    @Test
    void drainFailureEntersCooldownInsteadOfResubmittingEveryCycle() {
        // 人为制造「目标不持有副本」的分区：把 E 为主的两个分区的副本行从目标节点挪走，
        // 迫使它们走真正的迁移路径（其余分区仍结构性命中提升路径）
        Map<Integer, NodeId> target4 = new PlacementEngine().computePrimaries(
                List.of(PlacementNode.of("A"), PlacementNode.of("B"),
                        PlacementNode.of("C"), PlacementNode.of("D")),
                PARTITIONS);
        List<Integer> forcedMigrations = placements.primaries(STORAGE.name()).entrySet().stream()
                .filter(entry -> E.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .limit(2)
                .toList();
        assertThat(forcedMigrations).hasSize(2);
        for (int partition : forcedMigrations) {
            NodeId other = List.of(A, B, C, D).stream()
                    .filter(node -> !node.equals(target4.get(partition)))
                    .findFirst().orElseThrow();
            placements.reassignReplicas(STORAGE.name(), partition, List.of(other));
        }

        // 恒定失败的迁移协调器：记录每次提交
        List<PartitionMove> attempts = new CopyOnWriteArrayList<>();
        MigrationCoordinator failing = (capability, moves) -> {
            attempts.addAll(moves);
            return false;
        };
        ReconcileLoop failingLoop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities,
                placements, new PlacementEngine(), new RebalancePlanner(), commands, failing,
                SchedulerConfig.of(5_000L, 1_000L, 500L, PARTITIONS, 2, 0.10d, COOLDOWN_MS),
                clock);

        writer.setDesired(E, STORAGE, CapabilityState.DISABLED);
        assertThat(failingLoop.runOnce()).isTrue();

        // 首轮：提升路径处理结构性分区，仅被迫迁移的 2 个分区提交给协调器并失败
        assertThat(attempts).extracting(PartitionMove::partition)
                .containsExactlyInAnyOrderElementsOf(forcedMigrations);
        assertThat(capabilities.get(E, STORAGE)).map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE); // 排空失败：保持 ACTIVE 冻结

        // 冷却窗口内再跑两周期：不重复提交
        int before = attempts.size();
        clock.advance(1_000L);
        assertThat(failingLoop.runOnce()).isTrue();
        clock.advance(1_000L);
        assertThat(failingLoop.runOnce()).isTrue();
        assertThat(attempts).hasSize(before);

        // 冷却期满：允许重试
        clock.advance(COOLDOWN_MS);
        assertThat(failingLoop.runOnce()).isTrue();
        assertThat(attempts).hasSize(before + forcedMigrations.size());
    }
}
