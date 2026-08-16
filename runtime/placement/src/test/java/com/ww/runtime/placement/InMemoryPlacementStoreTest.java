package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryPlacementStoreTest {

    private final PlacementEngine engine = new PlacementEngine();
    private final InMemoryPlacementStore store = new InMemoryPlacementStore();

    /** 每分区一行（primary），与 rt_partition_placement 主键 (capability, partition_id) 语义一致。 */
    private List<Placement> computed(int partitionCount) {
        return engine.compute("mock",
                        List.of(PlacementNode.of("A"), PlacementNode.of("B"), PlacementNode.of("C")),
                        partitionCount, 1)
                .values().stream()
                .map(List::getFirst)
                .toList();
    }

    // ---- replaceAll 后 primaries/get 一致性 ----

    @Test
    void replaceAllThenListGetAndPrimariesAreConsistent() {
        List<Placement> placements = computed(16);
        store.replaceAll("mock", placements);

        List<Placement> listed = store.list("mock");
        assertThat(listed).containsExactlyInAnyOrderElementsOf(placements);
        assertThat(listed).isSortedAccordingTo(Comparator.comparingInt(Placement::partition));

        Map<Integer, NodeId> primaries = store.primaries("mock");
        assertThat(primaries).hasSize(16);
        for (Placement placement : placements) {
            assertThat(store.get("mock", placement.partition())).contains(placement);
            if (placement.isPrimary()) {
                assertThat(primaries.get(placement.partition())).isEqualTo(placement.node());
            }
        }
    }

    @Test
    void replaceAllReplacesWholeSetAtomically() {
        store.replaceAll("mock", computed(8));
        store.replaceAll("mock", computed(2));

        List<Placement> listed = store.list("mock");
        assertThat(listed).hasSize(2); // 每分区一行
        assertThat(listed).allSatisfy(p -> assertThat(p.partition()).isLessThan(2));
        assertThat(store.primaries("mock")).hasSize(2);
    }

    @Test
    void replaceAllRejectsDuplicatePartitions() {
        assertThatThrownBy(() -> store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition 0");
    }

    @Test
    void unknownCapabilityOrPartitionIsEmpty() {
        assertThat(store.list("ghost")).isEmpty();
        assertThat(store.get("ghost", 0)).isEmpty();
        assertThat(store.primaries("ghost")).isEmpty();

        store.replaceAll("mock", computed(4));
        assertThat(store.get("mock", 99)).isEmpty();
        assertThat(store.cutover("ghost", 0, NodeId.of("X"), 0L)).isFalse();
        assertThat(store.cutover("mock", 99, NodeId.of("X"), 0L)).isFalse();
    }

    // ---- CAS 切主 ----

    @Test
    void cutoverSwapsNodeAndIncrementsEpoch() {
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L),
                new Placement(1, NodeId.of("B"), PlacementRole.PRIMARY, 0L)));

        assertThat(store.cutover("mock", 0, NodeId.of("C"), 0L)).isTrue();

        assertThat(store.get("mock", 0))
                .contains(new Placement(0, NodeId.of("C"), PlacementRole.PRIMARY, 1L));
        assertThat(store.get("mock", 1))
                .contains(new Placement(1, NodeId.of("B"), PlacementRole.PRIMARY, 0L));
        assertThat(store.primaries("mock")).containsEntry(0, NodeId.of("C"));
    }

    @Test
    void cutoverFailsOnStaleEpochAndLeavesStateUnchanged() {
        store.replaceAll("mock", List.of(new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L)));

        assertThat(store.cutover("mock", 0, NodeId.of("C"), 5L)).isFalse();
        assertThat(store.cutover("mock", 0, NodeId.of("C"), -1L)).isFalse();

        assertThat(store.get("mock", 0))
                .contains(new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L));
    }

    @Test
    void cutoverToSameNodeReturnsFalse() {
        store.replaceAll("mock", List.of(new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L)));

        assertThat(store.cutover("mock", 0, NodeId.of("A"), 0L)).isFalse();

        assertThat(store.get("mock", 0))
                .contains(new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L));
    }

    @Test
    void concurrentCutoverWithSameExpectedEpochExactlyOneWins() throws Exception {
        NodeId a = NodeId.of("A");
        NodeId b = NodeId.of("B");
        NodeId c = NodeId.of("C");
        store.replaceAll("mock", List.of(new Placement(0, a, PlacementRole.PRIMARY, 0L)));

        int rounds = 100;
        AtomicInteger totalWins = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < rounds; round++) {
                long expected = store.get("mock", 0).orElseThrow().epoch();
                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<Boolean> toB = pool.submit(() -> {
                    barrier.await();
                    return store.cutover("mock", 0, b, expected);
                });
                Future<Boolean> toC = pool.submit(() -> {
                    barrier.await();
                    return store.cutover("mock", 0, c, expected);
                });

                int wins = (toB.get() ? 1 : 0) + (toC.get() ? 1 : 0);
                assertThat(wins).as("round %d wins", round).isEqualTo(1);
                totalWins.addAndGet(wins);

                Placement after = store.get("mock", 0).orElseThrow();
                assertThat(after.epoch()).as("round %d epoch", round).isEqualTo(expected + 1);
                assertThat(after.node()).isIn(a, b, c);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(totalWins.get()).isEqualTo(rounds);
    }
}
