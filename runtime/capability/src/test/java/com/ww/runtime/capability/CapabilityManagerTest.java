package com.ww.runtime.capability;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.TestClock;

class CapabilityManagerTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final NodeId SELF = NodeId.of("node-1");

    /** 测试侧独立硬编码的合法跃迁集合（与实现静态表对照做全矩阵断言）。 */
    private static final Set<String> SPEC_LEGAL_EDGES = Set.of(
            "DISABLED->STARTING",
            "STARTING->WARMING", "STARTING->FAILED",
            "WARMING->ACTIVE", "WARMING->FAILED",
            "ACTIVE->DRAINING", "ACTIVE->FAILED",
            "DRAINING->MIGRATING", "DRAINING->ACTIVE", "DRAINING->STOPPING", "DRAINING->FAILED",
            "MIGRATING->STOPPING", "MIGRATING->FAILED",
            "STOPPING->DISABLED", "STOPPING->FAILED",
            "FAILED->DISABLED");

    private TestClock clock;
    private InMemoryCapabilityStateStore store;
    private CapabilityManager manager;
    private RecordingModule module;

    @BeforeEach
    void setUp() {
        clock = new TestClock(1_000L);
        store = new InMemoryCapabilityStateStore(clock);
        manager = new CapabilityManager(SELF, store, clock);
        module = new RecordingModule();
        manager.register(module);
    }

    // ---- 跃迁表全矩阵 ----

    @Test
    void transitionTableMatrixMatchesSpecExactly() {
        for (CapabilityState from : CapabilityState.values()) {
            for (CapabilityState to : CapabilityState.values()) {
                String edge = from + "->" + to;
                boolean expected = SPEC_LEGAL_EDGES.contains(edge);
                assertThat(CapabilityManager.isLegalTransition(from, to))
                        .as("edge %s", edge)
                        .isEqualTo(expected);
            }
        }
    }

    @Test
    void commandMatrixLegalPassIllegalThrowsAndStateUnchanged() {
        for (CapabilityState from : CapabilityState.values()) {
            // start：仅 DISABLED 合法（同步链直达 ACTIVE）
            check(from, "start", () -> manager.start(MOCK), from == CapabilityState.DISABLED,
                    CapabilityState.ACTIVE);
            // drain：仅 ACTIVE
            check(from, "drain", () -> manager.drain(MOCK), from == CapabilityState.ACTIVE,
                    CapabilityState.DRAINING);
            // beginMigrate：仅 DRAINING
            check(from, "beginMigrate", () -> manager.beginMigrate(MOCK),
                    from == CapabilityState.DRAINING, CapabilityState.MIGRATING);
            // endMigrate：仅 MIGRATING
            check(from, "endMigrate", () -> manager.endMigrate(MOCK),
                    from == CapabilityState.MIGRATING, CapabilityState.STOPPING);
            // stop：接受 STOPPING / DRAINING / FAILED
            check(from, "stop", () -> manager.stop(MOCK),
                    from == CapabilityState.STOPPING || from == CapabilityState.DRAINING
                            || from == CapabilityState.FAILED,
                    CapabilityState.DISABLED);
            // cancelDrain：仅 DRAINING
            check(from, "cancelDrain", () -> manager.cancelDrain(MOCK),
                    from == CapabilityState.DRAINING, CapabilityState.ACTIVE);
            // markFailed：非 DISABLED 全部可达（FAILED→FAILED 幂等）
            check(from, "markFailed", () -> manager.markFailed(MOCK),
                    from != CapabilityState.DISABLED, CapabilityState.FAILED);
        }
    }

    /** 摆状态 → 执行命令 → 合法则到达目标态；非法则抛 IllegalStateTransitionException 且状态不变。 */
    private void check(CapabilityState from, String commandName, Runnable command,
                       boolean legal, CapabilityState expectedAfter) {
        store.setActual(SELF, MOCK, from);
        if (legal) {
            command.run();
            assertThat(manager.state(MOCK))
                    .as("%s from %s should reach %s", commandName, from, expectedAfter)
                    .isEqualTo(expectedAfter);
        } else {
            assertThatThrownBy(command::run)
                    .as("%s from %s must be rejected", commandName, from)
                    .isInstanceOf(IllegalStateTransitionException.class);
            assertThat(manager.state(MOCK))
                    .as("state must stay unchanged after illegal %s from %s", commandName, from)
                    .isEqualTo(from);
        }
    }

    // ---- manager 完整链 ----

    @Test
    void startChainRunsHooksInOrderAndReachesActive() {
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DISABLED);
        manager.start(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(module.calls).containsExactly("start", "warm");
        assertThat(store.get(SELF, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.actual()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.desired()).isEqualTo(CapabilityState.DISABLED);
        });
    }

    @Test
    void drainMigrateStopFullChain() {
        manager.start(MOCK);
        manager.drain(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DRAINING);
        manager.beginMigrate(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.MIGRATING);
        manager.endMigrate(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.STOPPING);
        manager.stop(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DISABLED);
        assertThat(module.calls).containsExactly("start", "warm", "drain", "stop");
    }

    @Test
    void stopAcceptsDrainingDirectly() {
        manager.start(MOCK);
        manager.drain(MOCK);
        manager.stop(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DISABLED);
        assertThat(module.calls).containsExactly("start", "warm", "drain", "stop");
    }

    @Test
    void cancelDrainReturnsToActive() {
        manager.start(MOCK);
        manager.drain(MOCK);
        manager.cancelDrain(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        // 恢复服务后可再次排水
        manager.drain(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DRAINING);
    }

    @Test
    void markFailedThenStopThenRestartChain() {
        module.failStart = true;
        assertThatThrownBy(() -> manager.start(MOCK))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.FAILED);

        int callsBeforeRecover = module.calls.size();
        manager.stop(MOCK); // FAILED→DISABLED，不调 stop 钩子
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.DISABLED);
        assertThat(module.calls).hasSize(callsBeforeRecover);

        module.failStart = false;
        manager.start(MOCK);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(module.calls).containsExactly("start", "start", "warm");
    }

    @Test
    void warmFailureMarksFailed() {
        module.failWarm = true;
        assertThatThrownBy(() -> manager.start(MOCK)).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.FAILED);
    }

    @Test
    void drainFailureMarksFailed() {
        store.setActual(SELF, MOCK, CapabilityState.ACTIVE);
        module.failDrain = true;
        assertThatThrownBy(() -> manager.drain(MOCK)).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.FAILED);
    }

    @Test
    void stopHookFailureMarksFailed() {
        store.setActual(SELF, MOCK, CapabilityState.STOPPING);
        module.failStop = true;
        assertThatThrownBy(() -> manager.stop(MOCK)).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.FAILED);
    }

    @Test
    void freezeDelegatesToModuleWithoutStateChange() {
        store.setActual(SELF, MOCK, CapabilityState.ACTIVE);
        manager.freeze(MOCK, true);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(module.lastFreeze).isTrue();
        manager.freeze(MOCK, false);
        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(module.lastFreeze).isFalse();
    }

    @Test
    void registerInitializesRecordAsDisabled() {
        assertThat(store.get(SELF, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.desired()).isEqualTo(CapabilityState.DISABLED);
            assertThat(record.actual()).isEqualTo(CapabilityState.DISABLED);
            assertThat(record.updatedAtMillis()).isEqualTo(1_000L);
        });
    }

    @Test
    void registerResetsStaleActualToDisabledOnProcessRestart() {
        // 模拟上一代进程遗留：desired=actual=ACTIVE（进程重启后模块实际未运行）
        store.setActual(SELF, MOCK, CapabilityState.ACTIVE);
        store.setDesired(SELF, MOCK, CapabilityState.ACTIVE);

        // 新进程 register：actual 归位 DISABLED，desired 保留供调度器重新拉起
        manager.register(module);

        assertThat(store.get(SELF, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.desired()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.actual()).isEqualTo(CapabilityState.DISABLED);
        });
        // 重复 register（幂等）：actual 已是 DISABLED 不再写入
        manager.register(module);
        assertThat(store.get(SELF, MOCK)).hasValueSatisfying(record ->
                assertThat(record.actual()).isEqualTo(CapabilityState.DISABLED));
    }

    @Test
    void modulesExposesRegisteredModules() {
        assertThat(manager.modules()).containsOnlyKeys(MOCK);
        assertThat(manager.modules().get(MOCK)).isSameAs(module);
    }

    @Test
    void unregisteredCommandThrowsNoSuchElement() {
        CapabilityManager empty = new CapabilityManager(SELF, store, clock);
        assertThatThrownBy(() -> empty.start(MOCK)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> empty.drain(MOCK)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> empty.markFailed(MOCK)).isInstanceOf(NoSuchElementException.class);
    }

    /** 可控的记录型模块：记录钩子调用顺序，可按需让任一钩子失败。 */
    private static final class RecordingModule implements CapabilityModule {

        final List<String> calls = new CopyOnWriteArrayList<>();
        volatile boolean failStart;
        volatile boolean failWarm;
        volatile boolean failDrain;
        volatile boolean failStop;
        volatile Boolean lastFreeze;

        @Override
        public CapabilityType type() {
            return CapabilityType.MOCK;
        }

        @Override
        public void start() throws Exception {
            calls.add("start");
            if (failStart) {
                throw new IllegalStateException("boom-start");
            }
        }

        @Override
        public void warm() {
            calls.add("warm");
            if (failWarm) {
                throw new IllegalStateException("boom-warm");
            }
        }

        @Override
        public void drain() {
            calls.add("drain");
            if (failDrain) {
                throw new IllegalStateException("boom-drain");
            }
        }

        @Override
        public void stop() {
            calls.add("stop");
            if (failStop) {
                throw new IllegalStateException("boom-stop");
            }
        }

        @Override
        public void freeze(boolean frozen) {
            calls.add("freeze:" + frozen);
            lastFreeze = frozen;
        }
    }
}
