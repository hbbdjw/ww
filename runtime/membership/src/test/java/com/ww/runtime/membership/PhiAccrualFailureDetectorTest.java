package com.ww.runtime.membership;

import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * {@link PhiAccrualFailureDetector} 单测：基于可控时钟验证心跳间隔统计、
 * φ 随静默单调上升、喂样本回落、最小标准差兜底与 acceptablePause 语义。
 */
class PhiAccrualFailureDetectorTest {

    private static final long INTERVAL = 1_000L;
    private static final long MIN_STD_DEV = 100L;
    private static final int MIN_SAMPLES = 3;

    private final TestClock clock = new TestClock(10_000L);

    private PhiAccrualFailureDetector newDetector() {
        return new PhiAccrualFailureDetector(200, MIN_SAMPLES, MIN_STD_DEV, 0L, clock);
    }

    /** 喂入 n 次严格按 INTERVAL 到达的心跳（首次仅建立时间戳）。 */
    private void beat(PhiAccrualFailureDetector d, int times) {
        for (int i = 0; i < times; i++) {
            clock.advance(INTERVAL);
            d.heartbeat();
        }
    }

    @Test
    void coldDetectorHasNoSignalAndIsNotReady() {
        PhiAccrualFailureDetector d = newDetector();
        assertThat(d.lastSeen()).isEqualTo(-1L);
        assertThat(d.isReady()).isFalse();
        // 冷启动不提供 φ：调用方据此回退固定超时判定
        assertThat(d.phi()).isZero();
    }

    @Test
    void becomesReadyAfterEnoughIntervalSamples() {
        PhiAccrualFailureDetector d = newDetector();
        d.heartbeat(); // 仅首个时间戳，零个间隔样本
        assertThat(d.isReady()).isFalse();
        beat(d, MIN_SAMPLES); // 产生 MIN_SAMPLES 个间隔
        assertThat(d.isReady()).isTrue();
        assertThat(d.lastSeen()).isEqualTo(clock.nowMillis());
    }

    @Test
    void healthyRhythmKeepsPhiLow() {
        PhiAccrualFailureDetector d = newDetector();
        beat(d, 10);
        // 刚收到心跳：diff=0，远小于均值，φ≈0
        assertThat(d.phi()).isLessThan(0.05);
        // 恰在预期节拍点：diff=mean=1000，y=0，P_later=0.5，φ=-log10(0.5)≈0.301
        assertThat(d.phiAt(clock.nowMillis() + INTERVAL)).isCloseTo(0.301, within(0.02));
    }

    @Test
    void phiGrowsMonotonicallyWithSilenceAndCrossesThresholds() {
        PhiAccrualFailureDetector d = newDetector();
        beat(d, 10);
        double atOneSigma = d.phiAt(clock.nowMillis() + INTERVAL + MIN_STD_DEV); // y=1
        // y=1：P_later≈0.1587，φ≈0.799
        assertThat(atOneSigma).isCloseTo(0.799, within(0.02));

        double atThreeSigma = d.phiAt(clock.nowMillis() + INTERVAL + 3 * MIN_STD_DEV); // y=3
        // y=3：P_later≈0.00135，φ≈2.87
        assertThat(atThreeSigma).isCloseTo(2.87, within(0.05));

        double atSixSigma = d.phiAt(clock.nowMillis() + INTERVAL + 6 * MIN_STD_DEV); // y=6
        // 深度静默：φ 极大但有限（概率下截断保护）
        assertThat(atSixSigma).isGreaterThan(8.0);
        assertThat(Double.isFinite(atSixSigma)).isTrue();

        // 单调性：静默越久越可疑
        assertThat(atThreeSigma).isGreaterThan(atOneSigma);
        assertThat(atSixSigma).isGreaterThan(atThreeSigma);
    }

    @Test
    void heartbeatDropsPhiBackToHealthy() {
        PhiAccrualFailureDetector d = newDetector();
        beat(d, 10);
        double high = d.phiAt(clock.nowMillis() + INTERVAL + 6 * MIN_STD_DEV);
        assertThat(high).isGreaterThan(8.0);

        clock.advance(INTERVAL + 6 * MIN_STD_DEV);
        d.heartbeat(); // 迟到的心跳终于到达
        // 新到达后 now==lastSeen → diff=0 < mean → φ≈0
        assertThat(d.phi()).isLessThan(0.05);
    }

    @Test
    void zeroVarianceIsFlooredByMinStdDeviation() {
        // 全部间隔严格相等 → 实测标准差为 0；不得因此产生无穷大 φ（检测器瞬间过敏）
        PhiAccrualFailureDetector d = newDetector();
        beat(d, 10);
        double phi = d.phiAt(clock.nowMillis() + INTERVAL + MIN_STD_DEV);
        assertThat(phi).isCloseTo(0.799, within(0.02));
    }

    @Test
    void acceptablePauseIsTolerated() {
        PhiAccrualFailureDetector pause2000 =
                new PhiAccrualFailureDetector(200, MIN_SAMPLES, MIN_STD_DEV, 2_000L, clock);
        beat(pause2000, 10);
        // 静默 2500ms：扣除 2000ms 可接受暂停后 diff 仅 500ms（y=-5）→ φ≈0
        assertThat(pause2000.phiAt(clock.nowMillis() + 2_500L)).isLessThan(0.05);
        // 暂停窗口耗尽后继续静默，φ 恢复上升
        assertThat(pause2000.phiAt(clock.nowMillis() + 2_000L + INTERVAL + 3 * MIN_STD_DEV))
                .isCloseTo(2.87, within(0.05));
    }

    @Test
    void sampleWindowIsBoundedToMaxSampleSize() {
        PhiAccrualFailureDetector d =
                new PhiAccrualFailureDetector(3, MIN_SAMPLES, MIN_STD_DEV, 0L, clock);
        beat(d, 20); // 不抛异常、不膨胀
        assertThat(d.isReady()).isTrue();
        // 稳态节拍点仍给出健康 φ（y=0 → 0.301）
        assertThat(d.phiAt(clock.nowMillis() + INTERVAL)).isCloseTo(0.301, within(0.02));
    }

    @Test
    void simultaneousOrReorderedHeartbeatsDoNotProduceNegativeIntervals() {
        PhiAccrualFailureDetector d = newDetector();
        d.heartbeat();
        d.heartbeat(); // 同一时刻重复信号
        clock.advance(INTERVAL);
        d.heartbeat();
        assertThat(d.isReady()).isFalse(); // 仅 1 个有效正间隔
        assertThat(Double.isFinite(d.phi())).isTrue();
    }
}
