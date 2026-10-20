package com.ww.runtime.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueObjectsTest {

    private static ResourceSnapshot sampleSnapshot() {
        return ResourceSnapshot.of(12.5, 1000, 400, 2000, 800, 1_720_000_000_000L);
    }

    // ---- NodeId ----

    @Test
    void nodeIdOfCreatesEqualValues() {
        assertThat(NodeId.of("node-1")).isEqualTo(new NodeId("node-1"));
        assertThat(NodeId.of("node-1")).hasSameHashCodeAs(new NodeId("node-1"));
        assertThat(NodeId.of("node-2")).isNotEqualTo(NodeId.of("node-1"));
    }

    @Test
    void nodeIdRejectsNullOrBlank() {
        assertThatThrownBy(() -> NodeId.of(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeId.of("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nodeIdToStringReturnsRawValue() {
        assertThat(NodeId.of("node-1").toString()).isEqualTo("node-1");
    }

    // ---- PartitionId ----

    @Test
    void partitionIdOfCreatesEqualValues() {
        assertThat(PartitionId.of(3)).isEqualTo(new PartitionId(3));
        assertThat(PartitionId.of(3).value()).isEqualTo(3);
    }

    @Test
    void partitionIdRejectsNegative() {
        assertThatThrownBy(() -> PartitionId.of(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---- Epoch / Version ----

    @Test
    void epochAndVersionOfCreateEqualValues() {
        assertThat(Epoch.of(7)).isEqualTo(new Epoch(7));
        assertThat(Version.of(42)).isEqualTo(new Version(42));
    }

    @Test
    void epochAndVersionRejectNegative() {
        assertThatThrownBy(() -> Epoch.of(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.of(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void epochComparison() {
        assertThat(Epoch.of(1).isStalerThan(Epoch.of(2))).isTrue();
        assertThat(Epoch.of(2).isStalerThan(Epoch.of(1))).isFalse();
    }

    // ---- ResourceSnapshot ----

    @Test
    void resourceSnapshotOfCreatesEqualValues() {
        ResourceSnapshot a = sampleSnapshot();
        ResourceSnapshot b = ResourceSnapshot.of(12.5, 1000, 400, 2000, 800, 1_720_000_000_000L);
        assertThat(a).isEqualTo(b);
        assertThat(a.memUsedRatio()).isEqualTo(0.4);
        assertThat(a.diskUsedRatio()).isEqualTo(0.4);
    }

    @Test
    void resourceSnapshotRejectsInvalidCpuPercent() {
        assertThatThrownBy(() -> ResourceSnapshot.of(-0.1, 10, 1, 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ResourceSnapshot.of(100.1, 10, 1, 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ResourceSnapshot.of(Double.NaN, 10, 1, 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resourceSnapshotRejectsInvalidMemoryOrDiskUsage() {
        assertThatThrownBy(() -> ResourceSnapshot.of(1, 10, 11, 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("memory");
        assertThatThrownBy(() -> ResourceSnapshot.of(1, 10, 1, 10, 11, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disk");
    }

    // ---- NodeInfo ----

    @Test
    void nodeInfoOfCreatesEqualValues() {
        // 7 参工厂 httpPort 默认 0
        NodeInfo legacy = NodeInfo.of(NodeId.of("n1"), "host-1", 9090, NodeState.ACTIVE,
                5L, 1.5, sampleSnapshot());
        assertThat(legacy.httpPort()).isZero();

        NodeInfo a = NodeInfo.of(NodeId.of("n1"), "host-1", 9090, 8080, NodeState.ACTIVE,
                5L, 1.5, sampleSnapshot());
        NodeInfo b = new NodeInfo(NodeId.of("n1"), "host-1", 9090, 8080, NodeState.ACTIVE,
                5L, 1.5, sampleSnapshot());
        assertThat(a).isEqualTo(b);
        assertThat(a.id()).isEqualTo(NodeId.of("n1"));
        assertThat(a.state()).isEqualTo(NodeState.ACTIVE);
        assertThat(a.httpPort()).isEqualTo(8080);
    }

    @Test
    void nodeInfoRejectsInvalidArguments() {
        ResourceSnapshot snapshot = sampleSnapshot();
        assertThatThrownBy(() -> NodeInfo.of(null, "h", 1, NodeState.ACTIVE, 1, 1.0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeInfo.of(NodeId.of("n1"), " ", 1, NodeState.ACTIVE, 1, 1.0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeInfo.of(NodeId.of("n1"), "h", 65536, NodeState.ACTIVE, 1, 1.0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeInfo.of(NodeId.of("n1"), "h", 1, null, 1, 1.0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeInfo.of(NodeId.of("n1"), "h", 1, NodeState.ACTIVE, 1, 0.0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeInfo.of(NodeId.of("n1"), "h", 1, NodeState.ACTIVE, 1, 1.0, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
