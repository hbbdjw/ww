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
}
