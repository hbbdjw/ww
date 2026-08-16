package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class RebalancePlannerTest {

    private static final int PARTITIONS = 4096;

    private final RebalancePlanner planner = new RebalancePlanner();
    private final PlacementEngine engine = new PlacementEngine();

    // ---- 最小移动 ----

    @Test
    void addingNodeMovesMinimalSetAllToNewNode() {
        Map<Integer, NodeId> before = engine.computePrimaries(
                List.of(PlacementNode.of("A"), PlacementNode.of("B")), PARTITIONS);
        Map<Integer, NodeId> after = engine.computePrimaries(
                List.of(PlacementNode.of("A"), PlacementNode.of("B"), PlacementNode.of("C")), PARTITIONS);

        List<RebalancePlanner.PartitionMove> moves = planner.plan("mock", before, after);

        // 无重复分区
        Set<Integer> movedPartitions = moves.stream()
                .map(RebalancePlanner.PartitionMove::partition)
                .collect(Collectors.toSet());
        assertThat(moves).hasSize(movedPartitions.size());

        for (int p = 0; p < PARTITIONS; p++) {
            if (after.get(p).equals(before.get(p))) {
                // 未变更的分区绝不出现在移动计划中
                assertThat(movedPartitions).as("partition %d unchanged", p).doesNotContain(p);
            } else {
                assertThat(movedPartitions).as("partition %d changed", p).contains(p);
            }
        }

        // HRW 性质：新增节点只赢得原本不属于它的分区，因此 to 恒为 C
        for (RebalancePlanner.PartitionMove move : moves) {
            assertThat(move.capability()).isEqualTo("mock");
            assertThat(move.from()).isEqualTo(before.get(move.partition()));
            assertThat(move.to()).isEqualTo(NodeId.of("C"));
        }

        // 期望约 1/3 分区切到 C（±15%）
        assertThat((double) moves.size())
                .isCloseTo(PARTITIONS / 3.0, within(0.15 * PARTITIONS / 3.0));
    }

    @Test
    void planKeepsUnchangedPartitionsAndMarksNewOnesWithNullFrom() {
        NodeId a = NodeId.of("A");
        NodeId b = NodeId.of("B");
        Map<Integer, NodeId> current = Map.of(0, a, 1, a);
        Map<Integer, NodeId> target = Map.of(0, a, 1, b, 2, b);

        List<RebalancePlanner.PartitionMove> moves = planner.plan("mock", current, target);

        assertThat(moves).containsExactly(
                new RebalancePlanner.PartitionMove("mock", 1, a, b),
                new RebalancePlanner.PartitionMove("mock", 2, null, b));
    }

    @Test
    void planReturnsEmptyWhenIdentical() {
        Map<Integer, NodeId> primaries = engine.computePrimaries(
                List.of(PlacementNode.of("A"), PlacementNode.of("B")), 32);
        assertThat(planner.plan("mock", primaries, primaries)).isEmpty();
    }

    // ---- shouldRebalance 阈值/cooldown 边界 ----

    @Test
    void shouldRebalanceHonorsMoveRatioAndCooldownBoundaries() {
        // ratio == minMoveRatio 视为达标；elapsed == cooldown 视为已冷却
        assertThat(planner.shouldRebalance(10, 100, 0.10, 1_000L, 500L, 1_500L)).isTrue();
        // ratio < minMoveRatio → false
        assertThat(planner.shouldRebalance(9, 100, 0.10, 1_000L, 500L, 2_000L)).isFalse();
        assertThat(planner.shouldRebalance(0, 100, 0.10, 1_000L, 500L, 2_000L)).isFalse();
        // 仍在 cooldown 内（elapsed < cooldown）→ false
        assertThat(planner.shouldRebalance(50, 100, 0.10, 1_000L, 500L, 1_499L)).isFalse();
        assertThat(planner.shouldRebalance(50, 100, 0.10, 1_000L, 3_600_000L, 1_500L)).isFalse();
        // ratio 达标 + 冷却耗尽 → true
        assertThat(planner.shouldRebalance(50, 100, 0.10, 1_000L, 500L, 2_000L)).isTrue();
        // 无分区可动 → false
        assertThat(planner.shouldRebalance(0, 0, 0.10, 0L, 0L, 1_000L)).isFalse();
    }
}
