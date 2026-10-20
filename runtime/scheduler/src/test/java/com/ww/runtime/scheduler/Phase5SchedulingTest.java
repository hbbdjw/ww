package com.ww.runtime.scheduler;

import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 阶段 5 验收（调度与成员生命周期，§3.12/§3.14/§5 阶段 5）：
 * <ul>
 *   <li>S5：排空失败后 desired 回改的泄漏冻结被定向幂等解冻（缩容僵死修复）；</li>
 *   <li>§3.12③⑤：JOINING 启动孤儿清扫（能力感知 + 慢启动宽限），有能力记录的 JOINING 不误伤；</li>
 *   <li>S17：管理快照节流短缓存（runOnce 主动失效）。</li>
 * </ul>
 */
@DisplayName("阶段 5 验收：S5 冻结补偿 / JOINING 孤儿清扫 / 快照节流")
class Phase5SchedulingTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 8;
    private static final long COOLDOWN_MS = 30_000L;
    private static final long TTL_MS = 5_000L;

    private static final NodeId SCHEDULER = NodeId.of("scheduler-node");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    private TestClock clock;
    private InMemoryLeaseStore lease;
    private InMemoryNodeRegistry registry;
    private InMemoryCapabilityStateStore capabilities;
    private com.ww.runtime.placement.InMemoryPlacementStore placements;
    private RecordingCommands commands;
    private DesiredStateWriter writer;

    @BeforeEach
    void setUp() {
        clock = new TestClock(1_000L);
        lease = new InMemoryLeaseStore(clock);
        registry = new InMemoryNodeRegistry();
        capabilities = new InMemoryCapabilityStateStore(clock);
        placements = new com.ww.runtime.placement.InMemoryPlacementStore();
        commands = new RecordingCommands();
        writer = new DesiredStateWriter(capabilities);
        for (NodeId node : List.of(A, B)) {
            registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                    NodeState.ACTIVE, 0L, 1.0d,
                    ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
        }
    }

    private ReconcileLoop newLoop(MigrationCoordinator migration) {
        return new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                new PlacementEngine(), new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(TTL_MS, 1_000L, 500L, PARTITIONS, 1, 0.10d, COOLDOWN_MS),
                clock);
    }

    private ReconcileLoop newLoop(MigrationCoordinator migration, NodeCommandChannel channel) {
        return new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                new PlacementEngine(), new RebalancePlanner(), channel, migration,
                SchedulerConfig.of(TTL_MS, 1_000L, 500L, PARTITIONS, 1, 0.10d, COOLDOWN_MS),
                clock);
    }

    /** 可控 start 结果的命令通道（模拟能力启动失败/成功的启动收敛窗口）。 */
    private static final class GatedCommands implements NodeCommandChannel {
        volatile boolean startOk;
        final java.util.List<String> calls = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public boolean startCapability(NodeId node, CapabilityType type) {
            calls.add("start:" + node.value() + ":" + startOk);
            return startOk;
        }

        @Override
        public boolean stopCapability(NodeId node, CapabilityType type) {
            calls.add("stop:" + node.value());
            return true;
        }

        @Override
        public boolean freeze(NodeId node, CapabilityType type, boolean frozen) {
            calls.add("freeze:" + node.value() + ":" + frozen);
            return true;
        }
    }

    private void activate(NodeId node) {
        writer.setDesired(node, MOCK, CapabilityState.ACTIVE);
        capabilities.setActual(node, MOCK, CapabilityState.ACTIVE);
    }

    // ==================== S5 冻结补偿 ====================

    @Test
    @DisplayName("S5：排空失败保冻后 desired 回改，定向幂等解冻使节点回到可收敛态")
    void drainFailureThenDesiredReEnableUnfreezes() {
        // 迁移恒失败：模拟排空中断（保持冻结）
        MigrationCoordinator alwaysFail = (capability, moves) -> false;
        ReconcileLoop loop = newLoop(alwaysFail);
        activate(A);
        activate(B);
        assertThat(loop.runOnce()).isTrue(); // 引导环（moves 空 → 无迁移压力）

        // B 排空：freeze(true) 已发、迁移失败 → 冻结保留
        writer.setDesired(B, MOCK, CapabilityState.DISABLED);
        assertThat(loop.runOnce()).isTrue();
        assertThat(commands.freezes()).contains("freeze:" + B.value() + ":true");

        // desired 回改 ACTIVE（缩容反悔）：旧缺陷中 B 将永久冻结、再无任何 freeze(false)
        writer.setDesired(B, MOCK, CapabilityState.ACTIVE);
        assertThat(loop.runOnce()).isTrue();
        assertThat(commands.freezes())
                .as("上一轮在排空、本轮回到 desired∩actual → 定向解冻必须下发")
                .contains("freeze:" + B.value() + ":false");
    }

    // ==================== §3.12③⑤ JOINING 孤儿清扫 ====================

    @Test
    @DisplayName("§3.12③⑤：超宽限且无能力记录的 JOINING 孤儿被 tombstone；慢启动期内与有能力记录者不误伤")
    void staleJoiningOrphanSweep() {
        ReconcileLoop loop = newLoop((capability, moves) -> true);
        NodeId orphan = NodeId.of("orphan");
        NodeId slowStarter = NodeId.of("slow");
        registry.register(NodeInfo.of(orphan, "host-o", 9100, NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
        registry.register(NodeInfo.of(slowStarter, "host-s", 9200, NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
        activate(slowStarter); // slow 已有能力记录 → 永不视为孤儿

        // 宽限期内（t0）：全部存活
        assertThat(loop.runOnce()).isTrue();
        assertThat(registry.get(orphan).orElseThrow().state()).isEqualTo(NodeState.JOINING);

        // 超过 2×TTL 宽限：orphan → DEAD（不进伸缩视图）；slow（有能力记录）保持
        clock.advance(TTL_MS * 2 + 1);
        assertThat(loop.runOnce()).isTrue();
        assertThat(registry.get(orphan).orElseThrow().state())
                .as("启动孤儿必须被 tombstone").isEqualTo(NodeState.DEAD);
        assertThat(registry.listAlive()).extracting(info -> info.id()).doesNotContain(orphan);
        assertThat(registry.get(slowStarter).orElseThrow().state())
                .as("有运行中能力的 JOINING 不得被清扫").isEqualTo(NodeState.JOINING);
    }

    // ==================== S17 快照节流 ====================

    @Test
    @DisplayName("S17：快照短缓存——连续读取共享同一结果，调谐周期主动失效重建")
    void snapshotIsThrottledAndInvalidatedPerCycle() {
        ReconcileLoop loop = newLoop((capability, moves) -> true);
        activate(A);
        assertThat(loop.runOnce()).isTrue();

        ReconcileLoop.ClusterStatus first = loop.snapshot();
        ReconcileLoop.ClusterStatus again = loop.snapshot();
        assertThat(again).as("TTL 内连续读取命中短缓存").isSameAs(first);

        loop.runOnce(); // 每周期主动作废快照缓存
        assertThat(loop.snapshot()).as("周期推进后必须重建快照").isNotSameAs(first);
    }

    // ==================== 端点就绪门控（重启窗口 fetch not started 治理） ====================

    @Test
    @DisplayName("门控：membership 已活但能力未 ACTIVE 的节点不派迁移；能力就绪后排空正常迁移")
    void migrationDeferredUntilEndpointCapabilityReady() {
        NodeId c = NodeId.of("C");
        registry.register(com.ww.runtime.common.NodeInfo.of(c, "host-C", 9300,
                NodeState.ACTIVE, 0L, 1.0d,
                com.ww.runtime.common.ResourceSnapshot.of(1.0d, 100, 10, 100, 10,
                        clock.nowMillis())));
        activate(A);
        activate(B);
        writer.setDesired(c, MOCK, CapabilityState.ACTIVE); // 能力未启动（actual 仍 DISABLED）

        CutoverMigrationCoordinator coordinator = new CutoverMigrationCoordinator(placements);
        GatedCommands gated = new GatedCommands(); // startOk=false：模拟启动收敛窗口
        ReconcileLoop loop = new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                new com.ww.runtime.placement.PlacementEngine(), new RebalancePlanner(),
                gated, coordinator,
                SchedulerConfig.of(TTL_MS, 1_000L, 500L, PARTITIONS, 1, 0.10d, COOLDOWN_MS),
                clock);

        // 周期1：环引导含 C；C 能力未就绪 → 重平衡被端点门控延后，零任务（fetch 必败场景被拦下）
        assertThat(loop.runOnce()).isTrue();
        assertThat(coordinator.recorded).as("端点能力未 ACTIVE 时不得建迁移").isEmpty();

        // 能力启动成功：环收敛（无需迁移），C 进入 actual=ACTIVE
        gated.startOk = true;
        assertThat(loop.runOnce()).isTrue();
        assertThat(capabilities.get(c, MOCK)
                .map(com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord::actual)
                .orElseThrow()).isEqualTo(CapabilityState.ACTIVE);
        assertThat(coordinator.recorded).as("环已一致，无谓迁移").isEmpty();

        // 正常排空 C：门控放行，迁移任务照建，最终环不含 C
        writer.setDesired(c, MOCK, CapabilityState.DISABLED);
        assertThat(loop.runOnce()).isTrue();
        assertThat(coordinator.recorded).as("端点就绪后排空迁移正常推进").isNotEmpty();
        assertThat(placements.primaries(MOCK.name()).values()).doesNotContain(c);
    }
}
