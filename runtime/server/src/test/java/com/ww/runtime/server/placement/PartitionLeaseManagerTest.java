package com.ww.runtime.server.placement;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionLeaseManagerTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");

    @Test
    void renewsAndExpiresLocalLeaseWithoutDatabaseReadOnValidation() {
        TestClock clock = new TestClock(1_000_000);
        InMemoryPlacementStore store = new InMemoryPlacementStore();
        store.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 0),
                new Placement(0, B, PlacementRole.REPLICA, 0)));
        CachedPlacementView view = new CachedPlacementView(store, clock, 500, 30_000, 100);
        view.refreshNow();

        PartitionLeaseManager manager = new PartitionLeaseManager(store, view, A, clock,
                1_000, 200, 100, 16, true);
        view.setLeaseValidator(manager);
        manager.renewNow();

        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 0,
                clock.nowMillis())).isTrue();
        clock.advance(899);
        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 0,
                clock.nowMillis())).isTrue();
        clock.advance(2); // 本地有效期 = 续约发起时间 + ttl − safetyMargin
        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 0,
                clock.nowMillis())).isFalse();

        manager.renewNow();
        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 0,
                clock.nowMillis())).isTrue();
    }

    @Test
    void ownershipLossInvalidatesLeaseAndWriteFencing() {
        TestClock clock = new TestClock(1_000_000);
        InMemoryPlacementStore store = new InMemoryPlacementStore();
        store.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 0),
                new Placement(0, B, PlacementRole.REPLICA, 0)));
        CachedPlacementView view = new CachedPlacementView(store, clock, 500, 30_000, 100);
        view.refreshNow();

        PartitionLeaseManager manager = new PartitionLeaseManager(store, view, A, clock,
                1_000, 200, 100, 16, true);
        view.setLeaseValidator(manager);
        manager.renewNow();
        view.checkWritable(CapabilityType.CACHE, PartitionId.of(0), A, -1);

        assertThat(store.promoteReplica("CACHE", 0, B, 0)).isTrue();
        view.refreshNow();
        manager.renewNow();

        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 0,
                clock.nowMillis())).isFalse();
        assertThatThrownBy(() ->
                view.checkWritable(CapabilityType.CACHE, PartitionId.of(0), A, -1))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void leasesAreEpochSpecific() {
        TestClock clock = new TestClock(1_000_000);
        InMemoryPlacementStore store = new InMemoryPlacementStore();
        store.replaceAll("CACHE", List.of(
                new Placement(0, A, PlacementRole.PRIMARY, 1))); // validAfter=0
        CachedPlacementView view = new CachedPlacementView(store, clock, 500, 30_000, 100);
        view.refreshNow();

        PartitionLeaseManager manager = new PartitionLeaseManager(store, view, A, clock,
                1_000, 200, 100, 16, true);
        view.setLeaseValidator(manager);
        manager.renewNow();
        assertThat(manager.isValid(CapabilityType.CACHE, PartitionId.of(0), 1,
                clock.nowMillis())).isTrue();
    }
}
