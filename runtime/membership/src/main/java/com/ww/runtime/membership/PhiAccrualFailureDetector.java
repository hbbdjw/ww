package com.ww.runtime.membership;

import com.ww.runtime.common.Clock;

/**
 * 单 peer 的 Phi Accrual 失败检测器（Akka/Cassandra 同款模型）。
 *
 * <p>检测器不输出"死/活"的布尔裁决，而是输出连续怀疑度
 * {@code φ(t) = -log10(P_later(t))}：在"节点仍然健康、心跳间隔服从历史分布"
 * 的假设下，下一次心跳晚于当前静默时长 t 的概率的负对数。φ 越大越可疑
 * （φ=1≈10%、φ=3≈0.1%、φ=8≈亿分之一量级），阈值由上层按操作成本选择。</p>
 *
 * <p>间隔分布以正态分布近似（均值 μ + 标准差 σ，σ 取实测值与
 * {@code minStdDeviationMs} 的较大者，防止稳态零方差导致检测器瞬间过敏）；
 * {@code acceptablePauseMs} 提供一段不计静默的宽限（模拟 Akka
 * acceptable-heartbeat-pause，用于容忍 GC/调度暂停）。</p>
 *
 * <p>样本以定长环形窗口保留（{@code maxSampleSize}），统计量随窗口滚动更新；
 * 有效间隔样本数不足 {@code minSamples} 时 {@link #isReady()} 为 false，
 * 调用方应回退固定超时判定（暖窗保护）。</p>
 *
 * <p>线程安全：心跳喂入发生在 gRPC 回调线程、φ 读取发生在 tick 线程，
 * 全部读-改-写由实例内置锁串行化。所有时间戳取自注入的 {@link Clock}，
 * 支持 TestClock 确定性测试。</p>
 */
final class PhiAccrualFailureDetector {

    /** 概率下限保护：防止深度静默下取对数产生无穷大，φ 封顶约 300。 */
    private static final double MIN_PROBABILITY = 1e-300;

    private final int maxSampleSize;
    private final int minSamples;
    private final double minStdDeviationMs;
    private final long acceptablePauseMs;
    private final Clock clock;

    /** 环形间隔样本（毫秒，正整数）。 */
    private final long[] intervals;
    private int sampleCount;
    private int nextIndex;
    private long sum;
    private long sumSquares;

    private long lastTimestamp = -1L;

    PhiAccrualFailureDetector(int maxSampleSize,
                              int minSamples,
                              long minStdDeviationMs,
                              long acceptablePauseMs,
                              Clock clock) {
        if (maxSampleSize < 1) {
            throw new IllegalArgumentException("maxSampleSize must be >= 1, got " + maxSampleSize);
        }
        if (minSamples < 1) {
            throw new IllegalArgumentException("minSamples must be >= 1, got " + minSamples);
        }
        if (minStdDeviationMs <= 0) {
            throw new IllegalArgumentException(
                    "minStdDeviationMs must be > 0, got " + minStdDeviationMs);
        }
        if (acceptablePauseMs < 0) {
            throw new IllegalArgumentException(
                    "acceptablePauseMs must be >= 0, got " + acceptablePauseMs);
        }
        this.maxSampleSize = maxSampleSize;
        this.minSamples = minSamples;
        this.minStdDeviationMs = minStdDeviationMs;
        this.acceptablePauseMs = acceptablePauseMs;
        this.clock = clock;
        this.intervals = new long[maxSampleSize];
    }

    /** 记录一次存活信号（心跳请求或应答到达）。 */
    synchronized void heartbeat() {
        heartbeat(clock.nowMillis());
    }

    synchronized void heartbeat(long now) {
        if (lastTimestamp >= 0) {
            long interval = now - lastTimestamp;
            if (interval > 0) {
                addInterval(interval);
            }
            // interval <= 0（同刻重复信号/时钟回拨）：不污染间隔分布，
            // 仍把 lastTimestamp 推进到 now（回拨时收敛到当前时钟）。
        }
        lastTimestamp = now;
    }

    private void addInterval(long interval) {
        if (sampleCount < maxSampleSize) {
            intervals[nextIndex] = interval;
            nextIndex = (nextIndex + 1) % maxSampleSize;
            sampleCount++;
            sum += interval;
            sumSquares += interval * interval;
        } else {
            long old = intervals[nextIndex];
            intervals[nextIndex] = interval;
            nextIndex = (nextIndex + 1) % maxSampleSize;
            sum += interval - old;
            sumSquares += interval * interval - old * old;
        }
    }

    /** 最近一次存活信号时间；从未收到返回 -1。 */
    synchronized long lastSeen() {
        return lastTimestamp;
    }

    /** 是否已积累足够间隔样本可供 φ 判定（暖窗保护）。 */
    synchronized boolean isReady() {
        return lastTimestamp >= 0 && sampleCount >= minSamples;
    }

    /** 当前时刻相对最近一次信号的怀疑度。 */
    synchronized double phi() {
        return phiAt(clock.nowMillis());
    }

    /** 指定时刻的怀疑度（测试与确定性驱动用）。 */
    synchronized double phiAt(long now) {
        if (lastTimestamp < 0 || sampleCount == 0) {
            return 0.0;
        }
        double diff = (double) (now - lastTimestamp) - (double) acceptablePauseMs;
        if (diff <= 0) {
            return 0.0; // 仍在可接受暂停窗口内
        }
        double mean = (double) sum / sampleCount;
        double variance = Math.max(0.0, (double) sumSquares / sampleCount - mean * mean);
        double stdDev = Math.max(Math.sqrt(variance), minStdDeviationMs);
        double y = (diff - mean) / stdDev;
        // P_later = 1 - CDF_normal(y)：健康但下一次心跳晚于 diff 的概率
        double probabilityLater = Math.max(MIN_PROBABILITY, 1.0 - normalCdf(y));
        return -Math.log10(probabilityLater);
    }

    /** 标准正态分布累积函数：0.5 * (1 + erf(y / √2))。 */
    private static double normalCdf(double y) {
        return 0.5 * (1.0 + erf(y / Math.sqrt(2.0)));
    }

    /**
     * erf 的 Abramowitz &amp; Stegun 7.1.26 近似（|误差| &lt; 1.5e-7），
     * 免引入 commons-math 依赖。
     */
    private static double erf(double x) {
        double sign = x < 0 ? -1.0 : 1.0;
        double abs = Math.abs(x);
        double t = 1.0 / (1.0 + 0.3275911 * abs);
        double a1 = 0.254829592;
        double a2 = -0.284496736;
        double a3 = 1.421413741;
        double a4 = -1.453152027;
        double a5 = 1.061405429;
        double poly = t * (a1 + t * (a2 + t * (a3 + t * (a4 + t * a5))));
        return sign * (1.0 - poly * Math.exp(-abs * abs));
    }
}
