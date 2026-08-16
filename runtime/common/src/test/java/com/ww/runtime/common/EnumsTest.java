package com.ww.runtime.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnumsTest {

    @Test
    void nodeStateConstants() {
        assertThat(NodeState.values()).containsExactly(
                NodeState.JOINING, NodeState.ACTIVE, NodeState.SUSPECTED, NodeState.DEAD, NodeState.LEAVING);
    }

    @Test
    void capabilityStateConstants() {
        assertThat(CapabilityState.values()).containsExactly(
                CapabilityState.DISABLED, CapabilityState.STARTING, CapabilityState.WARMING,
                CapabilityState.ACTIVE, CapabilityState.DRAINING, CapabilityState.MIGRATING,
                CapabilityState.STOPPING, CapabilityState.FAILED);
    }

    @Test
    void placementRoleConstants() {
        assertThat(PlacementRole.values()).containsExactly(PlacementRole.PRIMARY, PlacementRole.REPLICA);
    }

    @Test
    void migrationTaskStateConstants() {
        assertThat(MigrationTaskState.values()).containsExactly(
                MigrationTaskState.PENDING, MigrationTaskState.PREPARING, MigrationTaskState.SNAPSHOT,
                MigrationTaskState.COPYING, MigrationTaskState.SYNCING, MigrationTaskState.CUTOVER,
                MigrationTaskState.VERIFYING, MigrationTaskState.COMPLETED,
                MigrationTaskState.FAILED, MigrationTaskState.CANCELLED);
    }

    @Test
    void capabilityTypeParseAcceptsNameAndAlias() {
        assertThat(CapabilityType.parse("MOCK")).isEqualTo(CapabilityType.MOCK);
        assertThat(CapabilityType.parse("mock")).isEqualTo(CapabilityType.MOCK);
        assertThat(CapabilityType.parse(" Mock ")).isEqualTo(CapabilityType.MOCK);
        assertThat(CapabilityType.of("mock")).isEqualTo(CapabilityType.MOCK);
        assertThat(CapabilityType.of("MOCK").alias()).isEqualTo("mock");
    }

    @Test
    void capabilityTypeParseRejectsUnknown() {
        assertThatThrownBy(() -> CapabilityType.parse("nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope");
        assertThatThrownBy(() -> CapabilityType.parse(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
