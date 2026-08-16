package com.ww.runtime.membership;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MembershipConfigTest {

    @Test
    void ofAcceptsValidConfig() {
        MembershipConfig config = MembershipConfig.of(100L, 2, 4);
        assertThat(config.heartbeatIntervalMs()).isEqualTo(100L);
        assertThat(config.suspectAfterMisses()).isEqualTo(2);
        assertThat(config.deadAfterMisses()).isEqualTo(4);
        assertThat(config.suspectTimeoutMs()).isEqualTo(200L);
        assertThat(config.deadTimeoutMs()).isEqualTo(400L);
    }

    @Test
    void defaultsAreCanonicalValues() {
        MembershipConfig config = MembershipConfig.defaults();
        assertThat(config.heartbeatIntervalMs()).isEqualTo(1000L);
        assertThat(config.suspectAfterMisses()).isEqualTo(3);
        assertThat(config.deadAfterMisses()).isEqualTo(6);
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> MembershipConfig.of(0, 3, 6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeatIntervalMs");
        assertThatThrownBy(() -> MembershipConfig.of(-1, 3, 6))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissesBelowOne() {
        assertThatThrownBy(() -> MembershipConfig.of(100, 0, 6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("suspectAfterMisses");
    }

    @Test
    void rejectsDeadThresholdBelowSuspectThreshold() {
        assertThatThrownBy(() -> MembershipConfig.of(100, 4, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadAfterMisses");
    }

    @Test
    void allowsDeadThresholdEqualToSuspectThreshold() {
        assertThat(MembershipConfig.of(100, 3, 3).deadAfterMisses()).isEqualTo(3);
    }
}
