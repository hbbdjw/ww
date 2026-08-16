package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.CapabilityModule;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 缺陷复现：能力生命周期命令缺少幂等收敛路径，导致两类「状态僵死」。
 *
 * <h2>场景 A：startCapability 对已 ACTIVE 能力非幂等</h2>
 * {@code GrpcNodeCommandChannel} 单命令 deadline 仅 5 秒，而 {@code CapabilityManager.start}
 * 是同步链（start + warm 钩子）。钩子耗时超过 deadline 时调度器收到 DEADLINE_EXCEEDED
 * 误判为失败，下周期重试——但节点上能力实际已到 ACTIVE，重试 start() 命中
 * ACTIVE→STARTING 非法跃迁，<b>永远</b>返回 false：调度器永远无法把 actual 收敛为
 * ACTIVE，desired/actual 永久偏差，重试风暴不止。
 *
 * <h2>场景 B：崩溃遗留的中间态（STARTING/WARMING/MIGRATING）不可收敛</h2>
 * actual 状态持久化在共享仓库；节点在启动链中途崩溃会遗留 STARTING。重启后
 * start 命令要求 DISABLED 起点、stop 命令只接受 STOPPING/DRAINING/FAILED——
 * 两条路径都拒绝，能力永久僵死在中间态，只能人工改库。
 */
class CommandIdempotencyDeadlockBugReproTest {

    private static final NodeId NODE = NodeId.of("node-x");
    private static final CapabilityType MOCK = CapabilityType.MOCK;

    /** 计数钩子的空实现模块。 */
    private static final class NoopModule implements CapabilityModule {
        @Override
        public CapabilityType type() {
            return MOCK;
        }

        @Override
        public void start() {
        }

        @Override
        public void warm() {
        }

        @Override
        public void drain() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void freeze(boolean frozen) {
        }
    }

    private InMemoryCapabilityStateStore store;
    private CapabilityManager manager;
    private LocalNodeCommandChannel channel;

    @BeforeEach
    void setUp() {
        store = new InMemoryCapabilityStateStore(new TestClock(1_000L));
        manager = new CapabilityManager(NODE, store, new TestClock(1_000L));
        manager.register(new NoopModule());
        channel = new LocalNodeCommandChannel(NODE, manager);
    }

    @Test
    void startCommandShouldBeIdempotentWhenAlreadyActive() {
        assertThat(channel.startCapability(NODE, MOCK)).isTrue();
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);

        // 重试场景：上一次命令实际已生效但调用方超时/丢 ack，调度器下周期重发。
        // 期望：已 ACTIVE 时幂等成功；实际：ACTIVE→STARTING 非法跃迁，恒 false。
        assertThat(channel.startCapability(NODE, MOCK))
                .as("能力已 ACTIVE 时重复 start 应幂等成功，否则调度器因超时重试后"
                        + "永远无法收敛 actual=ACTIVE（每周期重试、每周期失败）")
                .isTrue();
    }

    @Test
    void stuckStartingStateIsUnreachableByAnyCommand() {
        // 模拟节点在启动链中途崩溃：持久化仓库遗留 actual=STARTING
        store.setActual(NODE, MOCK, CapabilityState.STARTING);

        boolean startOk = channel.startCapability(NODE, MOCK);
        boolean stopOk = channel.stopCapability(NODE, MOCK);

        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.STARTING);
        // 期望：至少一条命令路径能把 STARTING 收敛掉；
        // 实际：start 要求 DISABLED、stop 只接受 STOPPING/DRAINING/FAILED，双双恒 false。
        assertThat(startOk || stopOk)
                .as("崩溃遗留的 STARTING 态必须可被 start 或 stop 之一收敛，"
                        + "否则能力永久僵死（start/stop 均非法跃迁，调度器无限重试）")
                .isTrue();
    }
}
