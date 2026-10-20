package com.ww.runtime.membership;

/**
 * 成员关系模块配置：心跳周期与故障检测阈值。
 *
 * <p>故障检测有两套阈值：</p>
 * <ol>
 *   <li><b>固定超时（暖窗回退）</b>：{@code suspectAfterMisses}/{@code deadAfterMisses}
 *       乘以心跳间隔。Phi 检测器样本不足（冷启动/重连）时沿用此判定；</li>
 *   <li><b>Phi Accrual（稳态主路径）</b>：检测器按历史心跳间隔分布输出连续怀疑度 φ，
 *       φ 超 {@code phiSuspectThreshold} 判 SUSPECTED，连续 {@code deadConfirmations}
 *       个检测周期 φ 超 {@code phiDeadThreshold} 才升级 DEAD。</li>
 * </ol>
 *
 * @param heartbeatIntervalMs    心跳间隔（毫秒），须为正
 * @param suspectAfterMisses     连续未收到消息多少个周期后将 peer 标记为 SUSPECTED（暖窗回退阈值）
 * @param deadAfterMisses        连续未收到消息多少个周期后将 peer 标记为 DEAD（暖窗回退阈值）
 * @param phiSuspectThreshold    SUSPECTED 的 φ 阈值（默认 3，约 0.1% 误判概率）
 * @param phiDeadThreshold       DEAD 的 φ 阈值（默认 8，Cassandra/Akka 同级，误判概率约亿分之一）
 * @param deadConfirmations      φ 连续多少个检测周期超过死阈值才升级 DEAD（防抖，默认 2）
 * @param phiMaxSampleSize       φ 检测器心跳间隔滑动窗口容量（默认 200）
 * @param phiMinSamples          启用 φ 判定所需的最少间隔样本数（暖窗保护，默认 3）
 * @param phiMinStdDeviationMs   间隔标准差下限（毫秒，默认 100）：防止稳态零方差令检测器过敏
 * @param phiAcceptablePauseMs   可接受暂停（毫秒，默认 0）：该窗口内的静默不计入怀疑度（容忍 GC/调度停顿）
 * @param localDeadWriteback     本地判定 DEAD 是否直接回写宿主注册表（默认 true=传统单观察者语义；
 *                               多级仲裁模式置 false：本地视图/事件/投票照常，DEAD 只能由 leader
 *                               按多数怀疑票提升，避免非对称分区单点击穿治愈）
 */
public record MembershipConfig(long heartbeatIntervalMs,
                               int suspectAfterMisses,
                               int deadAfterMisses,
                               double phiSuspectThreshold,
                               double phiDeadThreshold,
                               int deadConfirmations,
                               int phiMaxSampleSize,
                               int phiMinSamples,
                               long phiMinStdDeviationMs,
                               long phiAcceptablePauseMs,
                               boolean localDeadWriteback) {

    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000L;
    public static final int DEFAULT_SUSPECT_AFTER_MISSES = 3;
    public static final int DEFAULT_DEAD_AFTER_MISSES = 6;

    public static final double DEFAULT_PHI_SUSPECT_THRESHOLD = 3.0d;
    public static final double DEFAULT_PHI_DEAD_THRESHOLD = 8.0d;
    public static final int DEFAULT_DEAD_CONFIRMATIONS = 2;
    public static final int DEFAULT_PHI_MAX_SAMPLE_SIZE = 200;
    public static final int DEFAULT_PHI_MIN_SAMPLES = 3;
    public static final long DEFAULT_PHI_MIN_STD_DEVIATION_MS = 100L;
    public static final long DEFAULT_PHI_ACCEPTABLE_PAUSE_MS = 0L;

    public MembershipConfig {
        validate(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses,
                phiSuspectThreshold, phiDeadThreshold, deadConfirmations,
                phiMaxSampleSize, phiMinSamples, phiMinStdDeviationMs, phiAcceptablePauseMs);
    }

    /** 紧凑工厂：仅固定超时参数，Phi 参数取默认值。 */
    public static MembershipConfig of(long heartbeatIntervalMs, int suspectAfterMisses, int deadAfterMisses) {
        return of(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses,
                DEFAULT_PHI_SUSPECT_THRESHOLD, DEFAULT_PHI_DEAD_THRESHOLD, DEFAULT_DEAD_CONFIRMATIONS,
                DEFAULT_PHI_MAX_SAMPLE_SIZE, DEFAULT_PHI_MIN_SAMPLES,
                DEFAULT_PHI_MIN_STD_DEVIATION_MS, DEFAULT_PHI_ACCEPTABLE_PAUSE_MS);
    }

    /** 全参工厂（本地 DEAD 直写回写开启，传统单观察者语义）。 */
    public static MembershipConfig of(long heartbeatIntervalMs,
                                      int suspectAfterMisses,
                                      int deadAfterMisses,
                                      double phiSuspectThreshold,
                                      double phiDeadThreshold,
                                      int deadConfirmations,
                                      int phiMaxSampleSize,
                                      int phiMinSamples,
                                      long phiMinStdDeviationMs,
                                      long phiAcceptablePauseMs) {
        return of(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses,
                phiSuspectThreshold, phiDeadThreshold, deadConfirmations,
                phiMaxSampleSize, phiMinSamples, phiMinStdDeviationMs, phiAcceptablePauseMs, true);
    }

    /** 全参工厂（含多级仲裁模式下的本地 DEAD 回写开关）。 */
    public static MembershipConfig of(long heartbeatIntervalMs,
                                      int suspectAfterMisses,
                                      int deadAfterMisses,
                                      double phiSuspectThreshold,
                                      double phiDeadThreshold,
                                      int deadConfirmations,
                                      int phiMaxSampleSize,
                                      int phiMinSamples,
                                      long phiMinStdDeviationMs,
                                      long phiAcceptablePauseMs,
                                      boolean localDeadWriteback) {
        return new MembershipConfig(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses,
                phiSuspectThreshold, phiDeadThreshold, deadConfirmations,
                phiMaxSampleSize, phiMinSamples, phiMinStdDeviationMs, phiAcceptablePauseMs,
                localDeadWriteback);
    }

    public static MembershipConfig defaults() {
        return of(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_SUSPECT_AFTER_MISSES, DEFAULT_DEAD_AFTER_MISSES);
    }

    /** SUSPECTED 判定阈值（毫秒，暖窗回退路径）。 */
    public long suspectTimeoutMs() {
        return suspectAfterMisses * heartbeatIntervalMs;
    }

    /** DEAD 判定阈值（毫秒，暖窗回退路径）。 */
    public long deadTimeoutMs() {
        return deadAfterMisses * heartbeatIntervalMs;
    }

    private static void validate(long heartbeatIntervalMs,
                                 int suspectAfterMisses,
                                 int deadAfterMisses,
                                 double phiSuspectThreshold,
                                 double phiDeadThreshold,
                                 int deadConfirmations,
                                 int phiMaxSampleSize,
                                 int phiMinSamples,
                                 long phiMinStdDeviationMs,
                                 long phiAcceptablePauseMs) {
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive, got " + heartbeatIntervalMs);
        }
        if (suspectAfterMisses < 1) {
            throw new IllegalArgumentException("suspectAfterMisses must be >= 1, got " + suspectAfterMisses);
        }
        if (deadAfterMisses < suspectAfterMisses) {
            throw new IllegalArgumentException(
                    "deadAfterMisses must be >= suspectAfterMisses, got " + deadAfterMisses + " < " + suspectAfterMisses);
        }
        if (!Double.isFinite(phiSuspectThreshold) || phiSuspectThreshold <= 0) {
            throw new IllegalArgumentException(
                    "phiSuspectThreshold must be positive finite, got " + phiSuspectThreshold);
        }
        if (!Double.isFinite(phiDeadThreshold) || phiDeadThreshold < phiSuspectThreshold) {
            throw new IllegalArgumentException("phiDeadThreshold must be finite and >= phiSuspectThreshold ("
                    + phiSuspectThreshold + "), got " + phiDeadThreshold);
        }
        if (deadConfirmations < 1) {
            throw new IllegalArgumentException("deadConfirmations must be >= 1, got " + deadConfirmations);
        }
        if (phiMaxSampleSize < 1) {
            throw new IllegalArgumentException("phiMaxSampleSize must be >= 1, got " + phiMaxSampleSize);
        }
        if (phiMinSamples < 1) {
            throw new IllegalArgumentException("phiMinSamples must be >= 1, got " + phiMinSamples);
        }
        if (phiMinSamples > phiMaxSampleSize) {
            throw new IllegalArgumentException("phiMinSamples (" + phiMinSamples
                    + ") must be <= phiMaxSampleSize (" + phiMaxSampleSize + ")");
        }
        if (phiMinStdDeviationMs <= 0) {
            throw new IllegalArgumentException(
                    "phiMinStdDeviationMs must be > 0, got " + phiMinStdDeviationMs);
        }
        if (phiAcceptablePauseMs < 0) {
            throw new IllegalArgumentException(
                    "phiAcceptablePauseMs must be >= 0, got " + phiAcceptablePauseMs);
        }
    }
}
