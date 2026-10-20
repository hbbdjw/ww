package com.ww.runtime.server.placement;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CachedPlacementViewTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    @Test
    void loadsSnapshotOnceAndRoutesFromMemory() {
        CountingStore store = new CountingStore();
        CachedPlacementView view = new CachedPlacementView(store, new TestClock(1_000_000),
                500, 30_000, 100);

        assertThat(view.isReady(CapabilityType.CACHE)).isFalse();
        store.delegate.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 0),
                new Placement(0, B, PlacementRole.REPLICA, 0)));

        view.refreshNow();
        assertThat(view.isReady(CapabilityType.CACHE)).isTrue();
        assertThat(view.owner(CapabilityType.CACHE, PartitionId.of(0)))
                .hasValueSatisfying(owner -> {
                    assertThat(owner.node()).isEqualTo(A);
                    assertThat(owner.epoch()).isEqualTo(0);
                    assertThat(owner.validAfterMs()).isZero();
                });
        assertThat(view.holders(CapabilityType.CACHE, PartitionId.of(0)))
                .extracting(owner -> owner.node())
                .containsExactly(A, B);

        int listCalls = store.listCalls;
        for (int i = 0; i < 100; i++) {
            view.owner(CapabilityType.CACHE, PartitionId.of(0));
            view.holders(CapabilityType.CACHE, PartitionId.of(0));
        }
        assertThat(store.listCalls).isEqualTo(listCalls);
    }

    @Test
    void revisionChangeReloadsPlacement() {
        CountingStore store = new CountingStore();
        CachedPlacementView view = new CachedPlacementView(store, new TestClock(1_000_000),
                500, 30_000, 100);
        store.delegate.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 0)));
        view.refreshNow();
        assertThat(view.owner(CapabilityType.CACHE, PartitionId.of(0)).orElseThrow().node())
                .isEqualTo(A);

        int callsBeforeCutover = store.listCalls;
        assertThat(store.delegate.cutover("CACHE", 0, B, 0)).isTrue();
        view.refreshNow();

        assertThat(view.owner(CapabilityType.CACHE, PartitionId.of(0)).orElseThrow().node())
                .isEqualTo(B);
        assertThat(store.listCalls - callsBeforeCutover).isEqualTo(1);
    }

    @Test
    void writeFencingHonoursValidAfterAndLocalLease() {
        TestClock clock = new TestClock(1_000_000);
        CountingStore store = new CountingStore();
        CachedPlacementView view = new CachedPlacementView(store, clock, 500, 30_000, 100);
        store.delegate.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 0, clock.nowMillis() + 500)));
        view.refreshNow();

        view.setLeaseValidator((cap, partition, epoch, now) -> true);
        assertThatThrownBy(() -> view.checkWritable(CapabilityType.CACHE, PartitionId.of(0), A, -1))
                .isInstanceOf(StaleEpochException.class);

        clock.advance(600); // validAfter + safety margin 100
        view.checkWritable(CapabilityType.CACHE, PartitionId.of(0), A, -1);

        view.setLeaseValidator((cap, partition, epoch, now) -> false);
        assertThatThrownBy(() -> view.checkWritable(CapabilityType.CACHE, PartitionId.of(0), A, -1))
                .isInstanceOf(StaleEpochException.class)
                .hasMessageContaining("lease");
    }

    @Test
    void missingOrStaleOwnerFailsClosed() {
        CountingStore store = new CountingStore();
        CachedPlacementView view = new CachedPlacementView(store, new TestClock(1_000_000),
                500, 30_000, 100);
        view.refreshNow();
        view.setLeaseValidator((cap, partition, epoch, now) -> true);

        assertThatThrownBy(() -> view.checkWritable(CapabilityType.CACHE, PartitionId.of(1), A, -1))
                .isInstanceOf(StaleEpochException.class);

        store.delegate.replaceAll("CACHE", List.of(
                new Placement(1, B, PlacementRole.PRIMARY, 2)));
        view.refreshNow();
        assertThatThrownBy(() -> view.checkWritable(CapabilityType.CACHE, PartitionId.of(1), A, -1))
                .isInstanceOf(StaleEpochException.class);
        view.checkWritable(CapabilityType.CACHE, PartitionId.of(1), B, 2);
        assertThatThrownBy(() -> view.checkWritable(CapabilityType.CACHE, PartitionId.of(1), B, 1))
                .isInstanceOf(StaleEpochException.class);
    }

    /** 统计 list 调用次数；内部委托真实内存 store。 */
    private static final class CountingStore implements PlacementStore {

        private final InMemoryPlacementStore delegate = new InMemoryPlacementStore();
        private int listCalls;

        @Override
        public void replaceAll(String capability, List<Placement> placements) {
            delegate.replaceAll(capability, placements);
        }

        @Override
        public List<Placement> list(String capability) {
            listCalls++;
            return delegate.list(capability);
        }

        @Override
        public Optional<Placement> get(String capability, int partition) {
            return delegate.get(capability, partition);
        }

        @Override
        public List<Placement> placements(String capability, int partition) {
            return delegate.placements(capability, partition);
        }

        @Override
        public Map<Integer, NodeId> primaries(String capability) {
            return delegate.primaries(capability);
        }

        @Override
        public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
            return delegate.cutover(capability, partition, newNode, expectedEpoch);
        }

        @Override
        public boolean promoteReplica(String capability, int partition, NodeId newNode,
                                      long expectedEpoch) {
            return delegate.promoteReplica(capability, partition, newNode, expectedEpoch);
        }

        @Override
        public void reassignReplicas(String capability, int partition, List<NodeId> newReplicas) {
            delegate.reassignReplicas(capability, partition, newReplicas);
        }

        @Override
        public long revision(String capability) {
            return delegate.revision(capability);
        }

        @Override
        public Map<Integer, Long> renewLeases(String capability, List<Integer> partitions,
                                              NodeId owner, long epoch, long ttlMillis) {
            return delegate.renewLeases(capability, partitions, owner, epoch, ttlMillis);
        }
    }
}
