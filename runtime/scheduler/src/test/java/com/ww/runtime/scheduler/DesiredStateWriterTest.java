package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DesiredStateWriterTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    private final TestClock clock = new TestClock(1_000L);
    private CapabilityStateStore store;
    private DesiredStateWriter writer;

    @BeforeEach
    void setUp() {
        store = new InMemoryCapabilityStateStore(clock);
        writer = new DesiredStateWriter(store);
    }

    @Test
    void desiredOnlyAcceptsActiveOrDisabled() {
        for (CapabilityState state : CapabilityState.values()) {
            if (state == CapabilityState.ACTIVE || state == CapabilityState.DISABLED) {
                continue;
            }
            assertThatThrownBy(() -> writer.setDesired(A, CapabilityType.MOCK, state))
                    .as("desired=%s must be rejected", state)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatCode(() -> writer.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE))
                .doesNotThrowAnyException();
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.ACTIVE);
    }

    @Test
    void disablingIsAllowedWhileAnotherDesiredActiveNodeRemains() {
        store.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setDesired(B, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.ACTIVE);

        writer.setDesired(B, CapabilityType.MOCK, CapabilityState.DISABLED);

        assertThat(store.get(B, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.DISABLED);
    }

    @Test
    void disablingLastActiveNodeIsRejectedWhileCapabilityStillServing() {
        store.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setDesired(B, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.ACTIVE);

        writer.setDesired(B, CapabilityType.MOCK, CapabilityState.DISABLED); // A 仍 desired-ACTIVE：允许

        // 排空最后节点：desired-ACTIVE 将为 0 且仍有 actual-ACTIVE → 拒绝
        assertThatThrownBy(() -> writer.setDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.ACTIVE); // 写入被拒绝，状态不变
    }

    @Test
    void anyDesiredIsAllowedOnceEverythingIsDisabled() {
        store.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setDesired(B, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.ACTIVE);
        writer.setDesired(B, CapabilityType.MOCK, CapabilityState.DISABLED);

        // 全部 actual 已 DISABLE（能力已全部停止）
        store.setActual(A, CapabilityType.MOCK, CapabilityState.DISABLED);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.DISABLED);

        // 允许排空最后节点，也允许随后重新启用
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED);
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED);

        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.DISABLED);
    }

    /** 其余节点已实际停止后排空最后节点：允许（排空=丢弃缓存数据的明确意图）；此时仅剩自身的 actual-ACTIVE 不是保护理由。 */
    @Test
    void disablingLastNodeIsAllowedOnceOtherNodesActuallyStopped() {
        store.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setDesired(B, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.ACTIVE);

        // 先禁 B（A 仍 desired-ACTIVE）→ B 实际停止（迁移完成）
        writer.setDesired(B, CapabilityType.MOCK, CapabilityState.DISABLED);
        store.setActual(B, CapabilityType.MOCK, CapabilityState.DISABLED);

        // 排空最后节点 A：其他节点均已实际停止 → 允许
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED);
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.DISABLED);
    }

    // ------------------------------------------------------------------
    // 初始期望态（env 首启默认值，不覆盖管理面决策）
    // ------------------------------------------------------------------

    /** 首启（无记录）：setInitialDesired 写入 env 默认值并返回 true。 */
    @Test
    void setInitialDesiredWritesOnFirstBoot() {
        boolean written = writer.setInitialDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        assertThat(written).isTrue();
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.ACTIVE);
    }

    /**
     * 重启语义：UI 禁用落库 desired=DISABLED 后，重启时 env
     * （cacheDesired=true）经 setInitialDesired 不覆盖——节点保持禁用，
     * 不被 env 复活、不触发数据回流迁移。
     */
    @Test
    void setInitialDesiredDoesNotOverrideManagementDecisionOnRestart() {
        // 首启：env 写 ACTIVE
        assertThat(writer.setInitialDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE)).isTrue();
        store.setActual(A, CapabilityType.MOCK, CapabilityState.ACTIVE);

        // 管理面禁用（迁移完成、能力停止）
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED);
        store.setActual(A, CapabilityType.MOCK, CapabilityState.DISABLED);

        // 重启：env 再跑 setInitialDesired(ACTIVE) → no-op，desired 保持 DISABLED
        assertThat(writer.setInitialDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE)).isFalse();
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.DISABLED);

        // 管理面曾显式启用过的记录同样不被 env 改动（env 只作首启默认值）
        writer.setDesired(A, CapabilityType.MOCK, CapabilityState.ACTIVE);
        assertThat(writer.setInitialDesired(A, CapabilityType.MOCK, CapabilityState.DISABLED)).isFalse();
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::desired)
                .contains(CapabilityState.ACTIVE);
    }

    @Test
    void setInitialDesiredRejectsIntermediateStates() {
        for (CapabilityState state : CapabilityState.values()) {
            if (state == CapabilityState.ACTIVE || state == CapabilityState.DISABLED) {
                continue;
            }
            assertThatThrownBy(() -> writer.setInitialDesired(A, CapabilityType.MOCK, state))
                    .as("initial desired=%s must be rejected", state)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
