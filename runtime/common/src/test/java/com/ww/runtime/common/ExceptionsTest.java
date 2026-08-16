package com.ww.runtime.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionsTest {

    @Test
    void allExtendRuntimeFrameworkException() {
        assertThat(new StaleEpochException("x")).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(new IllegalStateTransitionException("A", "B")).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(new LeaseAcquireException("x")).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(new MigrationException("x")).isInstanceOf(RuntimeFrameworkException.class);
        assertThat(new RuntimeFrameworkException("x")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void illegalStateTransitionCarriesFromAndTo() {
        IllegalStateTransitionException ex =
                new IllegalStateTransitionException(CapabilityState.ACTIVE, CapabilityState.DISABLED);
        assertThat(ex.getMessage())
                .contains("ACTIVE")
                .contains("DISABLED")
                .contains("->");
        assertThat(ex.from()).isEqualTo("ACTIVE");
        assertThat(ex.to()).isEqualTo("DISABLED");
    }

    @Test
    void illegalStateTransitionWithDetail() {
        IllegalStateTransitionException ex =
                new IllegalStateTransitionException("PENDING", "CUTOVER", "skip snapshot");
        assertThat(ex.getMessage()).contains("PENDING -> CUTOVER").contains("skip snapshot");
        assertThat(ex.from()).isEqualTo("PENDING");
        assertThat(ex.to()).isEqualTo("CUTOVER");
    }

    @Test
    void staleEpochCarriesEpochs() {
        StaleEpochException ex = new StaleEpochException(10L, 3L);
        assertThat(ex.getMessage()).contains("10").contains("3");
        assertThat(ex.expectedEpoch()).isEqualTo(10L);
        assertThat(ex.actualEpoch()).isEqualTo(3L);
    }
}
