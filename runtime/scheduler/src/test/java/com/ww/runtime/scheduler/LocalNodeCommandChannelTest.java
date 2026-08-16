package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LocalNodeCommandChannelTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    private final TestClock clock = new TestClock(1_000L);
    private CapabilityManager manager;
    private CapabilityStateStore store;
    private LocalNodeCommandChannel channel;

    @BeforeEach
    void setUp() {
        store = new InMemoryCapabilityStateStore(clock);
        manager = new CapabilityManager(A, store, clock);
        manager.register(new MockCapability(A, (cap, partition) -> Optional.empty(), key -> 0));
        channel = new LocalNodeCommandChannel(A, manager);
    }

    @Test
    void startCapabilityDelegatesToManagerStateMachine() {
        assertThat(channel.startCapability(A, CapabilityType.MOCK)).isTrue();
        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::actual)
                .contains(CapabilityState.ACTIVE);
    }

    @Test
    void commandsForOtherNodesAreRejected() {
        assertThat(channel.startCapability(B, CapabilityType.MOCK)).isFalse();
        assertThat(channel.stopCapability(B, CapabilityType.MOCK)).isFalse();
        assertThat(channel.freeze(B, CapabilityType.MOCK, true)).isFalse();
        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.DISABLED);
    }

    @Test
    void stopCapabilityWalksDrainChainToDisabled() {
        channel.startCapability(A, CapabilityType.MOCK);

        assertThat(channel.stopCapability(A, CapabilityType.MOCK)).isTrue();

        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.DISABLED);
        assertThat(store.get(A, CapabilityType.MOCK))
                .map(CapabilityRecord::actual)
                .contains(CapabilityState.DISABLED);
    }

    @Test
    void stopCapabilityIsIdempotentWhenAlreadyDisabled() {
        assertThat(channel.stopCapability(A, CapabilityType.MOCK)).isTrue();
        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.DISABLED);
    }

    @Test
    void freezeTogglesModuleWithoutStateChange() {
        channel.startCapability(A, CapabilityType.MOCK);

        assertThat(channel.freeze(A, CapabilityType.MOCK, true)).isTrue();
        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(channel.freeze(A, CapabilityType.MOCK, false)).isTrue();
        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.ACTIVE);
    }
}
