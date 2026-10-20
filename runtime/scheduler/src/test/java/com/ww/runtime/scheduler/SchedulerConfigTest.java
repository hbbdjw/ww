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
        assertThat(config.cacheReplicationFactor()).isEqualTo(1);
        assertThat(config.minMoveRatio()).isEqualTo(0.10d);
        assertThat(config.rebalanceCooldownMs()).isEqualTo(30_000L);
        assertThat(config.healingGracePeriodMs()).isEqualTo(30_000L);
        // 多数仲裁默认关闭（传统本地直写语义），票 TTL/最小票数有规范默认值
        assertThat(config.suspicionVoteTtlMs()).isEqualTo(12_000L);
        assertThat(config.suspicionMinObservers()).isEqualTo(1);
        assertThat(config.suspicionQuorumEnabled()).isFalse();
    }

    @Test
    void suspicionQuorumSettingsRoundTripAndValidate() {
        SchedulerConfig enabled = new SchedulerConfig(10_000L, 3_000L, 2_000L, 64, 1, 1,
                0.1, 30_000L, 30_000L, 5_000L, 2, true);
        assertThat(enabled.suspicionQuorumEnabled()).isTrue();
        assertThat(enabled.suspicionVoteTtlMs()).isEqualTo(5_000L);
        assertThat(enabled.suspicionMinObservers()).isEqualTo(2);

        assertThatThrownBy(() -> new SchedulerConfig(10_000L, 3_000L, 2_000L, 64, 1, 1,
                0.1, 30_000L, 30_000L, 0, 1, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("suspicionVoteTtlMs");
        assertThatThrownBy(() -> new SchedulerConfig(10_000L, 3_000L, 2_000L, 64, 1, 1,
                0.1, 30_000L, 30_000L, 5_000L, 0, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("suspicionMinObservers");

        // 9 参兼容构造器：仲裁关闭
        SchedulerConfig compact = new SchedulerConfig(10_000L, 3_000L, 2_000L, 64, 1, 1,
                0.1, 30_000L, 30_000L);
        assertThat(compact.suspicionQuorumEnabled()).isFalse();
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
        assertThatThrownBy(() -> new SchedulerConfig(0, 1_000, 500, 64, 1, 1, 0.1, 1_000, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SchedulerConfig(5_000, 1_000, 500, 64, 1, 0, 0.1, 1_000, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SchedulerConfig(5_000, 1_000, 500, 64, 1, 1, 0.1, 1_000, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rfForUsesCacheSpecificFactor() {
        SchedulerConfig config = new SchedulerConfig(5_000, 1_000, 500, 64, 3, 1, 0.1, 1_000, 0);
        // 全局 rf=3（storage 等带复制器能力），CACHE 独立为 1（无复制器防幽灵副本）
        assertThat(config.rfFor("CACHE")).isEqualTo(1);
        assertThat(config.rfFor("STORAGE")).isEqualTo(3);
        assertThat(config.rfFor("MOCK")).isEqualTo(3);

        SchedulerConfig cacheRf2 = SchedulerConfig.of(5_000, 1_000, 500, 64, 3, 2, 0.1, 1_000);
        assertThat(cacheRf2.rfFor("CACHE")).isEqualTo(2);
    }
}
