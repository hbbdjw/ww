package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PlacementEngineTest {

    private static final int PARTITIONS = 4096;

    private final PlacementEngine engine = new PlacementEngine();

    @Test
    void computeProducesPrimaryFirstAndReplicasWithZeroEpoch() {
        List<PlacementNode> nodes =
                List.of(PlacementNode.of("A"), PlacementNode.of("B"), PlacementNode.of("C"));

        Map<Integer, List<Placement>> placements = engine.compute("mock", nodes, 16, 2);

        assertThat(placements).hasSize(16);
        assertThat(placements.keySet())
                .containsExactlyInAnyOrderElementsOf(IntStream.range(0, 16).boxed().toList());
        for (int p = 0; p < 16; p++) {
            List<Placement> list = placements.get(p);
            assertThat(list).as("partition %d", p).hasSize(2);
            Placement primary = list.get(0);
            Placement replica = list.get(1);
            assertThat(primary.partition()).isEqualTo(p);
            assertThat(primary.role()).isEqualTo(PlacementRole.PRIMARY);
            assertThat(primary.isPrimary()).isTrue();
            assertThat(primary.epoch()).isZero();
            assertThat(replica.partition()).isEqualTo(p);
            assertThat(replica.role()).isEqualTo(PlacementRole.REPLICA);
            assertThat(replica.isPrimary()).isFalse();
            assertThat(replica.epoch()).isZero();
            assertThat(replica.node()).isNotEqualTo(primary.node());
        }
    }

    @Test
    void computeIsEmptyWhenNoNodes() {
        assertThat(engine.compute("mock", List.of(), 8, 2)).isEmpty();
        assertThat(engine.computePrimaries(List.of(), 8)).isEmpty();
    }

    @Test
    void replicationFactorIsCappedAtNodeCount() {
        List<PlacementNode> nodes = List.of(PlacementNode.of("A"), PlacementNode.of("B"));
        Map<Integer, List<Placement>> placements = engine.compute("mock", nodes, 4, 5);
        placements.forEach((partition, list) ->
                assertThat(list).as("partition %d", partition).hasSize(2));
    }

    @Test
    void computePrimariesMatchesBootstrapMath() throws Exception {
        List<HrwBootstrapStub.StubNode> stubs = HrwBootstrapStub.nodes(1.0, 2.0, 1.0);
        List<PlacementNode> nodes = List.of(
                PlacementNode.of("A", 1.0), PlacementNode.of("B", 2.0), PlacementNode.of("C", 1.0));

        Map<Integer, NodeId> primaries = engine.computePrimaries(nodes, PARTITIONS);

        assertThat(primaries).hasSize(PARTITIONS);
        for (int p = 0; p < PARTITIONS; p++) {
            assertThat(primaries.get(p))
                    .as("partition %d", p)
                    .isEqualTo(NodeId.of(HrwBootstrapStub.primaryNodeKey(
                            HrwBootstrapStub.partitionInput(p), stubs)));
        }
    }

    @Test
    void computePrimariesMatchesFirstElementOfCompute() {
        List<PlacementNode> nodes =
                List.of(PlacementNode.of("A"), PlacementNode.of("B"), PlacementNode.of("C"));
        Map<Integer, List<Placement>> full = engine.compute("mock", nodes, 64, 3);
        Map<Integer, NodeId> primaries = engine.computePrimaries(nodes, 64);

        assertThat(primaries).hasSize(64);
        for (int p = 0; p < 64; p++) {
            assertThat(primaries.get(p)).isEqualTo(full.get(p).getFirst().node());
        }
    }
}
