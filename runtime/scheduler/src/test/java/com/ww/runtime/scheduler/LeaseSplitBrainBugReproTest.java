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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证长迁移的租约复核语义：租约只在调谐周期<b>开始</b>时获取/续约，
 * 而 {@code migrateAndWait} 在周期内同步阻塞——迁移耗时超过租约 TTL 时，
 * 租约在周期中途过期，另一节点接管 leadership。
 *
 * <p>验证行为：旧 leader 迁移返回后先经 {@code stillLeader()} 复核租约，失租即
 * 自降 leader 身份并<b>中止</b>后续 freeze/stop 与写 actual 等副作用，不产生
 * 「双 leader」重叠窗口（其迁移前的 freeze 属失租前合法动作，不受影响）。
 * 残余风险：复核与副作用下发间仍有微小竞态窗口、迁移期间 cutover 无法撤回；
 * 框架当前无 fencing token，亦无后台续约线程（与 TestClock 确定性测试不兼容）。</p>
 */
class LeaseSplitBrainBugReproTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 16;
    private static final long TTL_MS = 5_000L;

    @Test
    void longMigrationAbortsStaleLeaderSideEffects() {
        TestClock clock = new TestClock(1_000L);
        InMemoryLeaseStore lease = new InMemoryLeaseStore(clock);
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        InMemoryCapabilityStateStore capabilities = new InMemoryCapabilityStateStore(clock);
        InMemoryPlacementStore placements = new InMemoryPlacementStore();
        PlacementEngine engine = new PlacementEngine();
        DesiredStateWriter writer = new DesiredStateWriter(capabilities);

        NodeId A = NodeId.of("A");
        NodeId B = NodeId.of("B");
        NodeId S1 = NodeId.of("sched-1");
        NodeId S2 = NodeId.of("sched-2");
        for (NodeId node : List.of(A, B)) {
            registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                    NodeState.JOINING, 0L, 1.0d,
                    ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
            registry.markState(node, NodeState.ACTIVE);
            writer.setDesired(node, MOCK, CapabilityState.ACTIVE);
            capabilities.setActual(node, MOCK, CapabilityState.ACTIVE);
        }

        RecordingCommands commands1 = new RecordingCommands();
        RecordingCommands commands2 = new RecordingCommands();
        SchedulerConfig config = SchedulerConfig.of(TTL_MS, 1_000L, 500L, PARTITIONS, 1, 0.10d, 30_000L);

        // 第二个调度器：在 loop1 迁移阻塞期间抢占过期的租约
        ReconcileLoop loop2 = new ReconcileLoop(S2, lease, registry, capabilities, placements,
                engine, new RebalancePlanner(), commands2,
                new CutoverMigrationCoordinator(placements), config, clock);

        // 第一个调度器的迁移协调器：模拟一次耗时超过租约 TTL 的迁移；
        // 「阻塞」期间让 loop2 运行一整个周期（等价于现实中的并发）。
        MigrationCoordinator slowMigration = (capability, moves) -> {
            clock.advance(TTL_MS + 1_000L);      // 迁移耗时 > TTL → 租约中途过期
            loop2.runOnce();                     // 新 leader 接管并开始调谐
            for (var move : moves) {
                placements.get(capability, move.partition()).ifPresent(current ->
                        placements.cutover(capability, move.partition(), move.to(), current.epoch()));
            }
            return true;
        };

        ReconcileLoop loop1 = new ReconcileLoop(S1, lease, registry, capabilities, placements,
                engine, new RebalancePlanner(), commands1, slowMigration, config, clock);

        // 引导环并确认 loop1 为 leader
        assertThat(loop1.runOnce()).isTrue();
        assertThat(loop1.isLeader()).isTrue();

        // 排空 A → 触发 (e) 下线迁移路径（慢迁移）
        writer.setDesired(A, MOCK, CapabilityState.DISABLED);
        // 迁移期间租约被 loop2 抢占，runOnce() 应正确返回 false（租约丢失）
        assertThat(loop1.runOnce()).isFalse();

        // 迁移返回后 loop1 复核租约失租、自降身份并中止 stop 等副作用
        assertThat(loop2.isLeader()).as("租约中途过期后 sched-2 接管 leadership").isTrue();
        assertThat(loop1.isLeader())
                .as("sched-1 迁移返回后复核租约失租，自降 leader 身份").isFalse();
        assertThat(commands1.stopped())
                .as("过期 leader 不得再下发 stop 副作用（迁移前的 freeze 属失租前合法动作）")
                .doesNotContain(A.value());
        assertThat(commands2.calls)
                .as("sched-2 作为唯一 leader 接管并下发过命令")
                .isNotEmpty();
    }
}
