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

    /** 每分区一行（仅 PRIMARY），rt_partition_placement 多行放置的单行形态。 */
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
    void replaceAllRejectsDuplicatePrimaryPartitions() {
        assertThatThrownBy(() -> store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L),
                new Placement(0, NodeId.of("B"), PlacementRole.PRIMARY, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition 0");
    }

    // ---- v2 多行放置（每分区 PRIMARY + REPLICA 行）----

    @Test
    void multiRowReplaceAllAndPlacementsQuery() {
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 0L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 0L),
                new Placement(0, NodeId.of("C"), PlacementRole.REPLICA, 0L),
                new Placement(1, NodeId.of("B"), PlacementRole.PRIMARY, 0L)));

        List<Placement> rows0 = store.placements("mock", 0);
        assertThat(rows0).hasSize(3);
        assertThat(rows0.getFirst().isPrimary()).isTrue(); // [0]=PRIMARY
        assertThat(rows0.getFirst().node()).isEqualTo(NodeId.of("A"));
        assertThat(rows0.subList(1, 3)).allSatisfy(p -> assertThat(p.isPrimary()).isFalse());

        assertThat(store.placements("mock", 1)).hasSize(1);
        assertThat(store.placements("mock", 99)).isEmpty();
        assertThat(store.placements("ghost", 0)).isEmpty();

        // get 仍返回 PRIMARY 行（fencing 语义）
        assertThat(store.get("mock", 0)).contains(rows0.getFirst());
        assertThat(store.primaries("mock")).containsEntry(0, NodeId.of("A"));
    }

    // ---- promoteReplica（死主副本提升 CAS）----

    @Test
    void promoteReplicaPromotesReplicaDeletesPrimaryAndKeepsOtherReplicas() {
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 3L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 3L),
                new Placement(0, NodeId.of("C"), PlacementRole.REPLICA, 3L)));

        assertThat(store.promoteReplica("mock", 0, NodeId.of("B"), 3L)).isTrue();

        List<Placement> rows = store.placements("mock", 0);
        assertThat(rows).hasSize(2); // 原 PRIMARY 行删除、B 升主、C 保留
        assertThat(rows.getFirst()).isEqualTo(new Placement(0, NodeId.of("B"), PlacementRole.PRIMARY, 4L));
        assertThat(rows).contains(new Placement(0, NodeId.of("C"), PlacementRole.REPLICA, 3L));
        assertThat(store.primaries("mock")).containsEntry(0, NodeId.of("B"));
    }

    @Test
    void promoteReplicaFailsOnStaleEpochOrMissingReplica() {
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 3L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 3L)));

        // epoch 不符
        assertThat(store.promoteReplica("mock", 0, NodeId.of("B"), 2L)).isFalse();
        assertThat(store.promoteReplica("mock", 0, NodeId.of("B"), 4L)).isFalse();
        // 节点上无副本行
        assertThat(store.promoteReplica("mock", 0, NodeId.of("C"), 3L)).isFalse();
        // 分区不存在
        assertThat(store.promoteReplica("mock", 99, NodeId.of("B"), 3L)).isFalse();

        // 状态未被破坏
        assertThat(store.placements("mock", 0)).containsExactly(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 3L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 3L));
    }

    // ---- reassignReplicas（副本集重置）----

    @Test
    void reassignReplicasResetsReplicaSetWithoutTouchingPrimaryOrEpoch() {
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 5L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 5L)));

        store.reassignReplicas("mock", 0, List.of(NodeId.of("C"), NodeId.of("D"), NodeId.of("A")));

        List<Placement> rows = store.placements("mock", 0);
        assertThat(rows).containsExactly(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 5L), // 主不动 epoch 不动
                new Placement(0, NodeId.of("C"), PlacementRole.REPLICA, 5L), // 旧副本 B 移除
                new Placement(0, NodeId.of("D"), PlacementRole.REPLICA, 5L)); // 主节点 A 被排除
    }

    @Test
    void reassignReplicasOnUnknownPartitionIsNoop() {
        store.reassignReplicas("mock", 42, List.of(NodeId.of("C")));
        assertThat(store.list("mock")).isEmpty();
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
    void cutoverThrowsWhenTargetAlreadyHoldsReplicaRow() {
        // 主键 (capability, partition_id, node_id) 对齐：PG 上该 UPDATE 会 duplicate key。
        // 排空场景结构性命中：被禁节点为主的分区，目标主 = 旧环第二顺位 = 副本持有者。
        store.replaceAll("mock", List.of(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 2L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 2L)));

        assertThatThrownBy(() -> store.cutover("mock", 0, NodeId.of("B"), 2L))
                .isInstanceOf(com.ww.runtime.common.RuntimeFrameworkException.class)
                .hasMessageContaining("duplicate key");

        // 状态未被破坏：主/副本行与 epoch 均不变（正确出路是 promoteReplica）
        assertThat(store.placements("mock", 0)).containsExactly(
                new Placement(0, NodeId.of("A"), PlacementRole.PRIMARY, 2L),
                new Placement(0, NodeId.of("B"), PlacementRole.REPLICA, 2L));
        assertThat(store.promoteReplica("mock", 0, NodeId.of("B"), 2L)).isTrue();
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
