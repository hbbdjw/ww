package com.ww.runtime.scheduler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchedulerConfigTest {

    @Test
    void defaultsMatchSpecification() {
        SchedulerConfig config = SchedulerConfig.of();
        assertThat(config.ttlMillis()).isEqualTo(10_000L);
        assertThat(config.renewIntervalMs()).isEqualTo(3_000L);
        assertThat(config.reconcileIntervalMs()).isEqualTo(2_000L);
        assertThat(config.partitionCount()).isEqualTo(4096);
        assertThat(config.replicationFactor()).isEqualTo(1);
        assertThat(config.minMoveRatio()).isEqualTo(0.10d);
        assertThat(config.rebalanceCooldownMs()).isEqualTo(30_000L);
    }

    @Test
    void ofAcceptsValidCustomValues() {
        assertThatCode(() -> SchedulerConfig.of(5_000L, 1_000L, 500L, 64, 2, 0.25d, 1_000L))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsNonPositiveTimeParameters() {
        assertThatThrownBy(() -> SchedulerConfig.of(0, 1_000, 500, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 0, 500, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 0, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsRenewIntervalNotBelowTtl() {
        assertThatThrownBy(() -> SchedulerConfig.of(1_000, 1_000, 500, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(1_000, 2_000, 500, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidTopologyParameters() {
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 500, 0, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 500, 64, 0, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidMoveRatioAndCooldown() {
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 500, 64, 1, 0, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 500, 64, 1, 1.5, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SchedulerConfig.of(5_000, 1_000, 500, 64, 1, 0.1, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compactConstructorValidatesToo() {
        assertThatThrownBy(() -> new SchedulerConfig(0, 1_000, 500, 64, 1, 0.1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
