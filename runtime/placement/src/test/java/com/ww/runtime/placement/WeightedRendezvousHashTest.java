package com.ww.runtime.placement;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class WeightedRendezvousHashTest {

    private static final int PARTITIONS = 4096;

    // ---- bootstrap 分布兼容：首选与测试内复刻数学逐分区一致 ----

    @Test
    void firstChoiceMatchesBootstrapMathForEqualWeights() throws Exception {
        assertFirstChoiceMatchesStub(1.0, 1.0, 1.0);
    }

    @Test
    void firstChoiceMatchesBootstrapMathForMixedWeights() throws Exception {
        assertFirstChoiceMatchesStub(1.0, 2.0, 1.0);
    }

    private void assertFirstChoiceMatchesStub(double... weights) throws Exception {
        List<HrwBootstrapStub.StubNode> stubs = HrwBootstrapStub.nodes(weights);
        List<PlacementNode> nodes = placementNodes(weights);
        WeightedRendezvousHash<PlacementNode> hrw = new WeightedRendezvousHash<>(nodes);
        for (int p = 0; p < PARTITIONS; p++) {
            String input = HrwBootstrapStub.partitionInput(p);
            List<PlacementNode> selected = hrw.selectNodes(input, 1);
            assertThat(selected).as("partition %d input %s", p, input).hasSize(1);
            assertThat(selected.getFirst().nodeKey())
                    .as("partition %d input %s", p, input)
                    .isEqualTo(HrwBootstrapStub.primaryNodeKey(input, stubs));
        }
    }

    // ---- 分布合理性 ----

    @Test
    void equalWeightNodesEachGetAboutOneThird() {
        Map<String, Long> counts = primaryCounts(1.0, 1.0, 1.0);
        assertThat(counts.keySet()).containsExactlyInAnyOrder("A", "B", "C");
        for (String key : new String[]{"A", "B", "C"}) {
            assertThat(counts.get(key) / (double) PARTITIONS)
                    .as("node %s share", key)
                    .isCloseTo(1.0 / 3, within(0.05));
        }
    }

    @Test
    void doubleWeightNodeTakesAboutTwoThirds() {
        Map<String, Long> counts = primaryCounts(1.0, 2.0);
        assertThat(counts.get("A") / (double) PARTITIONS).isCloseTo(1.0 / 3, within(0.05));
        assertThat(counts.get("B") / (double) PARTITIONS).isCloseTo(2.0 / 3, within(0.05));
    }

    private Map<String, Long> primaryCounts(double... weights) {
        WeightedRendezvousHash<PlacementNode> hrw = new WeightedRendezvousHash<>(placementNodes(weights));
        Map<String, Long> counts = new HashMap<>();
        for (int p = 0; p < PARTITIONS; p++) {
            counts.merge(hrw.selectNodes(HrwBootstrapStub.partitionInput(p), 1).getFirst().nodeKey(),
                    1L, Long::sum);
        }
        return counts;
    }

    // ---- 选择行为 ----

    @Test
    void selectNodesSortsByScoreDescendingAndCapsAtNodeCount() {
        WeightedRendezvousHash<PlacementNode> hrw =
                new WeightedRendezvousHash<>(placementNodes(1.0, 1.0, 1.0));
        String input = HrwBootstrapStub.partitionInput(0);

        List<PlacementNode> all = hrw.selectNodes(input, 99);
        assertThat(all).hasSize(3);
        assertThat(all).doesNotHaveDuplicates();

        List<PlacementNode> top2 = hrw.selectNodes(input, 2);
        assertThat(top2).hasSize(2);
        assertThat(top2.getFirst()).isEqualTo(all.getFirst());
        assertThat(top2).doesNotContain(all.getLast());

        assertThat(hrw.selectNodes(input, 0)).isEmpty();
    }

    @Test
    void emptyNodeSetReturnsEmptyList() {
        WeightedRendezvousHash<PlacementNode> hrw = new WeightedRendezvousHash<>(List.of());
        assertThat(hrw.selectNodes(HrwBootstrapStub.partitionInput(0), 3)).isEmpty();
    }

    private static List<PlacementNode> placementNodes(double... weights) {
        String[] keys = {"A", "B", "C"};
        List<PlacementNode> nodes = new ArrayList<>(weights.length);
        for (int i = 0; i < weights.length; i++) {
            nodes.add(PlacementNode.of(keys[i], weights[i]));
        }
        return nodes;
    }
}
