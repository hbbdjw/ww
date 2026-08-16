package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryCapabilityStateStoreTest {

    private static final NodeId NODE_A = NodeId.of("node-a");
    private static final NodeId NODE_B = NodeId.of("node-b");
    private static final CapabilityType MOCK = CapabilityType.MOCK;

    private TestClock clock;
    private InMemoryCapabilityStateStore store;

    @BeforeEach
    void setUp() {
        clock = new TestClock(1_000L);
        store = new InMemoryCapabilityStateStore(clock);
    }

    @Test
    void getReturnsEmptyForUnknownRecord() {
        assertThat(store.get(NODE_A, MOCK)).isEqualTo(Optional.empty());
        assertThat(store.list()).isEmpty();
    }

    @Test
    void setDesiredCreatesRecordWithDisabledActual() {
        store.setDesired(NODE_A, MOCK, CapabilityState.ACTIVE);
        assertThat(store.get(NODE_A, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.node()).isEqualTo(NODE_A);
            assertThat(record.type()).isEqualTo(MOCK);
            assertThat(record.desired()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.actual()).isEqualTo(CapabilityState.DISABLED);
            assertThat(record.updatedAtMillis()).isEqualTo(1_000L);
        });
    }

    @Test
    void setActualCreatesRecordWithDisabledDesired() {
        store.setActual(NODE_A, MOCK, CapabilityState.WARMING);
        assertThat(store.get(NODE_A, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.desired()).isEqualTo(CapabilityState.DISABLED);
            assertThat(record.actual()).isEqualTo(CapabilityState.WARMING);
        });
    }

    @Test
    void setDesiredAndActualUpdateIndependently() {
        store.setDesired(NODE_A, MOCK, CapabilityState.ACTIVE);
        clock.advance(100L);
        store.setActual(NODE_A, MOCK, CapabilityState.STARTING);
        assertThat(store.get(NODE_A, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.desired()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.actual()).isEqualTo(CapabilityState.STARTING);
            assertThat(record.updatedAtMillis()).isEqualTo(1_100L);
        });
        clock.advance(50L);
        store.setActual(NODE_A, MOCK, CapabilityState.ACTIVE);
        assertThat(store.get(NODE_A, MOCK)).hasValueSatisfying(record -> {
            assertThat(record.desired()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.actual()).isEqualTo(CapabilityState.ACTIVE);
            assertThat(record.updatedAtMillis()).isEqualTo(1_150L);
        });
    }

    @Test
    void listCoversAllNodesSorted() {
        store.setActual(NODE_B, MOCK, CapabilityState.ACTIVE);
        store.setActual(NODE_A, MOCK, CapabilityState.DRAINING);
        store.setDesired(NODE_A, MOCK, CapabilityState.DISABLED);

        assertThat(store.list()).hasSize(2);
        assertThat(store.list().get(0).node()).isEqualTo(NODE_A);
        assertThat(store.list().get(0).desired()).isEqualTo(CapabilityState.DISABLED);
        assertThat(store.list().get(0).actual()).isEqualTo(CapabilityState.DRAINING);
        assertThat(store.list().get(1).node()).isEqualTo(NODE_B);
        assertThat(store.list().get(1).actual()).isEqualTo(CapabilityState.ACTIVE);
    }
}
