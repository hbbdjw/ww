package com.ww.runtime.server.cluster;

import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CachedClusterDirectoryTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    @Test
    void refreshesEndpointsAndServingSetsFromLocalSnapshot() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        registerActive(registry, A, 9101);
        registerActive(registry, B, 9102);

        TestClock clock = new TestClock(1_000);
        InMemoryCapabilityStateStore capabilities = new InMemoryCapabilityStateStore(clock);
        capabilities.setActual(A, CapabilityType.CACHE, CapabilityState.ACTIVE);
        capabilities.setActual(B, CapabilityType.STORAGE, CapabilityState.ACTIVE);

        CachedClusterDirectory directory = new CachedClusterDirectory(registry, capabilities, 1_000, true);
        assertThat(directory.isReady()).isFalse();

        directory.refreshNow();

        assertThat(directory.isReady()).isTrue();
        assertThat(directory.endpoint(A)).hasValueSatisfying(endpoint -> {
            assertThat(endpoint.host()).isEqualTo("host-A");
            assertThat(endpoint.grpcPort()).isEqualTo(9101);
        });
        assertThat(directory.alivePeers()).containsExactlyInAnyOrder(A, B);
        assertThat(directory.servingPeers(CapabilityType.CACHE)).containsExactly(A);
        assertThat(directory.servingPeers(CapabilityType.STORAGE)).containsExactly(B);

        registry.tombstone(B);
        directory.refreshNow();

        assertThat(directory.endpoint(B)).isEmpty();
        assertThat(directory.alivePeers()).containsExactly(A);
        assertThat(directory.servingPeers(CapabilityType.STORAGE)).isEmpty();
    }

    @Test
    void dbModeReadsThroughToStoresWithoutSnapshot() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        registerActive(registry, A, 9101);
        TestClock clock = new TestClock(1_000);
        InMemoryCapabilityStateStore capabilities = new InMemoryCapabilityStateStore(clock);
        capabilities.setActual(A, CapabilityType.CACHE, CapabilityState.ACTIVE);

        CachedClusterDirectory directory = new CachedClusterDirectory(
                registry, capabilities, 1_000, false);
        assertThat(directory.isReady()).isTrue();
        assertThat(directory.endpoint(A)).isPresent();
        assertThat(directory.servingPeers(CapabilityType.CACHE)).containsExactly(A);

        registry.tombstone(A);
        assertThat(directory.endpoint(A)).isEmpty();
        assertThat(directory.alivePeers()).isEmpty();
        assertThat(directory.servingPeers(CapabilityType.CACHE)).isEmpty();
    }

    private static void registerActive(InMemoryNodeRegistry registry, NodeId id, int grpcPort) {
        registry.register(NodeInfo.of(id, "host-" + id.value(), grpcPort,
                NodeState.JOINING, 0L, 1.0,
                ResourceSnapshot.of(1.0, 100, 10, 100, 10, 0L)));
        registry.markState(id, NodeState.ACTIVE);
    }
}
