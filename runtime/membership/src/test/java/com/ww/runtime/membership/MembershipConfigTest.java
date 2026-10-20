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
        // Phi Accrual 默认参数
        assertThat(config.phiSuspectThreshold()).isEqualTo(3.0);
        assertThat(config.phiDeadThreshold()).isEqualTo(8.0);
        assertThat(config.deadConfirmations()).isEqualTo(2);
        assertThat(config.phiMaxSampleSize()).isEqualTo(200);
        assertThat(config.phiMinSamples()).isEqualTo(3);
        assertThat(config.phiMinStdDeviationMs()).isEqualTo(100L);
        assertThat(config.phiAcceptablePauseMs()).isZero();
    }

    @Test
    void compactFactoryFillsPhiDefaults() {
        MembershipConfig config = MembershipConfig.of(100L, 2, 4);
        assertThat(config.phiSuspectThreshold()).isEqualTo(3.0);
        assertThat(config.phiDeadThreshold()).isEqualTo(8.0);
        assertThat(config.deadConfirmations()).isEqualTo(2);
        assertThat(config.phiMinSamples()).isEqualTo(3);
        assertThat(config.phiMinStdDeviationMs()).isEqualTo(100L);
    }

    @Test
    void fullFactoryAcceptsCustomPhiSettings() {
        MembershipConfig config = MembershipConfig.of(
                500L, 3, 9, 4.0, 12.0, 3, 256, 5, 200L, 1_000L);
        assertThat(config.heartbeatIntervalMs()).isEqualTo(500L);
        assertThat(config.suspectAfterMisses()).isEqualTo(3);
        assertThat(config.deadAfterMisses()).isEqualTo(9);
        assertThat(config.phiSuspectThreshold()).isEqualTo(4.0);
        assertThat(config.phiDeadThreshold()).isEqualTo(12.0);
        assertThat(config.deadConfirmations()).isEqualTo(3);
        assertThat(config.phiMaxSampleSize()).isEqualTo(256);
        assertThat(config.phiMinSamples()).isEqualTo(5);
        assertThat(config.phiMinStdDeviationMs()).isEqualTo(200L);
        assertThat(config.phiAcceptablePauseMs()).isEqualTo(1_000L);
    }

    @Test
    void rejectsNonPositivePhiSuspectThreshold() {
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 0.0, 8.0, 2, 200, 3, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiSuspectThreshold");
    }

    @Test
    void rejectsDeadPhiBelowSuspectPhi() {
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 8.0, 3.0, 2, 200, 3, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiDeadThreshold");
    }

    @Test
    void rejectsDeadConfirmationsBelowOne() {
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 0, 200, 3, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadConfirmations");
    }

    @Test
    void rejectsPhiSamplingBounds() {
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 2, 0, 3, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiMaxSampleSize");
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 2, 200, 0, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiMinSamples");
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 2, 200, 3, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiMinStdDeviationMs");
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 2, 200, 3, 100, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiAcceptablePauseMs");
    }

    @Test
    void rejectsMinSamplesAboveMaxSampleSize() {
        assertThatThrownBy(() -> MembershipConfig.of(
                100, 3, 6, 3.0, 8.0, 2, 2, 3, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("phiMinSamples");
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
