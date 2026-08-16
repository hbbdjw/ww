package com.ww.runtime.migration;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationTaskState;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 状态机全矩阵校验：每个 from→to 组合的合法性与 check/canTransition/isTerminal 一致。
 */
class MigrationStateMachineTest {

    /** 期望的合法跃迁表（与实现定义一一对应，独立构造以交叉验证）。 */
    private static final Map<MigrationTaskState, Set<MigrationTaskState>> ALLOWED = Map.of(
            MigrationTaskState.PENDING, EnumSet.of(MigrationTaskState.PREPARING, MigrationTaskState.CANCELLED),
            MigrationTaskState.PREPARING, EnumSet.of(MigrationTaskState.SNAPSHOT, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.SNAPSHOT, EnumSet.of(MigrationTaskState.COPYING, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.COPYING, EnumSet.of(MigrationTaskState.SYNCING, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.SYNCING, EnumSet.of(MigrationTaskState.CUTOVER, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.CUTOVER, EnumSet.of(MigrationTaskState.VERIFYING, MigrationTaskState.FAILED),
            MigrationTaskState.VERIFYING, EnumSet.of(MigrationTaskState.COMPLETED, MigrationTaskState.FAILED),
            MigrationTaskState.COMPLETED, EnumSet.noneOf(MigrationTaskState.class),
            MigrationTaskState.FAILED, EnumSet.of(MigrationTaskState.PENDING),
            MigrationTaskState.CANCELLED, EnumSet.noneOf(MigrationTaskState.class));

    @Test
    void fullMatrixLegalTransitionsPass() {
        for (MigrationTaskState from : MigrationTaskState.values()) {
            for (MigrationTaskState to : MigrationTaskState.values()) {
                if (ALLOWED.get(from).contains(to)) {
                    assertThatCode(() -> MigrationStateMachine.check(from, to))
                            .as("legal transition %s -> %s must pass", from, to)
                            .doesNotThrowAnyException();
                }
            }
        }
    }

    @Test
    void fullMatrixIllegalTransitionsThrow() {
        for (MigrationTaskState from : MigrationTaskState.values()) {
            for (MigrationTaskState to : MigrationTaskState.values()) {
                if (!ALLOWED.get(from).contains(to)) {
                    assertThatThrownBy(() -> MigrationStateMachine.check(from, to))
                            .as("illegal transition %s -> %s must throw", from, to)
                            .isInstanceOf(IllegalStateTransitionException.class)
                            .hasMessageContaining(from.toString())
                            .hasMessageContaining(to.toString());
                }
            }
        }
    }

    @Test
    void canTransitionMatchesCheck() {
        for (MigrationTaskState from : MigrationTaskState.values()) {
            for (MigrationTaskState to : MigrationTaskState.values()) {
                assertThat(MigrationStateMachine.canTransition(from, to))
                        .as("canTransition(%s, %s)", from, to)
                        .isEqualTo(ALLOWED.get(from).contains(to));
            }
        }
    }

    @Test
    void nullArgumentsAreRejected() {
        assertThatThrownBy(() -> MigrationStateMachine.check(null, MigrationTaskState.PREPARING))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThatThrownBy(() -> MigrationStateMachine.check(MigrationTaskState.PENDING, null))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(MigrationStateMachine.canTransition(null, MigrationTaskState.PREPARING)).isFalse();
        assertThat(MigrationStateMachine.canTransition(MigrationTaskState.PENDING, null)).isFalse();
    }

    @Test
    void terminalStatesAreDetected() {
        assertThat(MigrationStateMachine.isTerminal(MigrationTaskState.COMPLETED)).isTrue();
        assertThat(MigrationStateMachine.isTerminal(MigrationTaskState.FAILED)).isTrue();
        assertThat(MigrationStateMachine.isTerminal(MigrationTaskState.CANCELLED)).isTrue();
        for (MigrationTaskState state : MigrationTaskState.values()) {
            if (!MigrationStateMachine.isTerminal(state)) {
                assertThat(ALLOWED.get(state)).isNotEmpty();
            }
        }
    }

    @Test
    void failedToPendingRetryIsTheOnlyTerminalExit() {
        for (MigrationTaskState terminal : new MigrationTaskState[]{
                MigrationTaskState.COMPLETED, MigrationTaskState.FAILED, MigrationTaskState.CANCELLED}) {
            for (MigrationTaskState to : MigrationTaskState.values()) {
                boolean expected = terminal == MigrationTaskState.FAILED && to == MigrationTaskState.PENDING;
                assertThat(MigrationStateMachine.canTransition(terminal, to))
                        .as("%s -> %s", terminal, to)
                        .isEqualTo(expected);
            }
        }
    }
}
