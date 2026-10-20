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
 * 验证能力生命周期命令的幂等收敛路径（convergeStart/convergeStop）
 * 覆盖两类「状态僵死」场景。
 *
 * <h2>场景 A：startCapability 对已 ACTIVE 能力幂等</h2>
 * {@code GrpcNodeCommandChannel} 单命令 deadline 仅 5 秒，而 {@code CapabilityManager.start}
 * 是同步链（start + warm 钩子）。钩子耗时超过 deadline 时调度器收到 DEADLINE_EXCEEDED
 * 误判为失败，下周期重试——但节点上能力实际已到 ACTIVE。convergeStart 对已在
 * 启动轨道（ACTIVE/WARMING/STARTING）的能力幂等成功（不改状态、不进钩子），
 * 保证重试不会命中非法跃迁，actual 可收敛为 ACTIVE。
 *
 * <h2>场景 B：崩溃遗留的中间态（STARTING/WARMING/MIGRATING）可被收敛</h2>
 * actual 状态持久化在共享仓库；节点在启动链中途崩溃会遗留 STARTING。重启后
 * 命令式 start 要求 DISABLED 起点、stop 只接受 STOPPING/DRAINING/FAILED，
 * 而 convergeStart 把启动轨道上的中间态视为已在轨道、幂等返回成功，
 * 调度器据此继续推进收敛。
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
        // 已 ACTIVE 时 convergeStart 幂等成功（不改状态、不进钩子）。
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
        // convergeStart 把启动轨道上的 STARTING 视为已在轨道，幂等返回成功，
        // 调度器据此继续推进收敛（命令式 start/stop 均拒绝 STARTING）。
        assertThat(startOk || stopOk)
                .as("崩溃遗留的 STARTING 态必须可被 start 或 stop 之一收敛，"
                        + "否则能力永久僵死（start/stop 均非法跃迁，调度器无限重试）")
                .isTrue();
    }
}
