package com.ww.runtime.scheduler;

/**
 * 调度器配置。
 *
 * <param-validation>
 * 全部参数经 {@link #of} 或紧凑构造器校验：时间参数为正、partitionCount/replicationFactor
 * 为正、minMoveRatio ∈ (0, 1]、rebalanceCooldownMs ≥ 0，且 renewIntervalMs &lt; ttlMillis
 * （否则租约总在续约前过期，leadership 抖动）。
 * </param-validation>
 *
 * @param ttlMillis          租约时长（默认 10000ms）
 * @param renewIntervalMs    租约续约间隔上限（默认 3000ms）
 * @param reconcileIntervalMs 调谐周期（默认 2000ms）
 * @param partitionCount     分区总数（默认 4096）
 * @param replicationFactor  每分区副本数（默认 1）
 * @param minMoveRatio       触发扩容重平衡的最小移动比例（默认 0.10）
 * @param rebalanceCooldownMs 重平衡冷却时间（默认 30000ms）
 */
public record SchedulerConfig(long ttlMillis,
                              long renewIntervalMs,
                              long reconcileIntervalMs,
                              int partitionCount,
                              int replicationFactor,
                              double minMoveRatio,
                              long rebalanceCooldownMs) {

    public SchedulerConfig {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        if (renewIntervalMs <= 0) {
            throw new IllegalArgumentException("renewIntervalMs must be > 0, got " + renewIntervalMs);
        }
        if (reconcileIntervalMs <= 0) {
            throw new IllegalArgumentException("reconcileIntervalMs must be > 0, got " + reconcileIntervalMs);
        }
        if (renewIntervalMs >= ttlMillis) {
            throw new IllegalArgumentException("renewIntervalMs (" + renewIntervalMs
                    + ") must be < ttlMillis (" + ttlMillis + ") or the lease always expires first");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0, got " + partitionCount);
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be > 0, got " + replicationFactor);
        }
        if (!Double.isFinite(minMoveRatio) || minMoveRatio <= 0 || minMoveRatio > 1) {
            throw new IllegalArgumentException("minMoveRatio must be in (0, 1], got " + minMoveRatio);
        }
        if (rebalanceCooldownMs < 0) {
            throw new IllegalArgumentException("rebalanceCooldownMs must be >= 0, got " + rebalanceCooldownMs);
        }
    }

    /** 默认配置工厂。 */
    public static SchedulerConfig of() {
        return new SchedulerConfig(10_000L, 3_000L, 2_000L, 4096, 1, 0.10d, 30_000L);
    }

    /** 全参校验工厂。 */
    public static SchedulerConfig of(long ttlMillis,
                                     long renewIntervalMs,
                                     long reconcileIntervalMs,
                                     int partitionCount,
                                     int replicationFactor,
                                     double minMoveRatio,
                                     long rebalanceCooldownMs) {
        return new SchedulerConfig(ttlMillis, renewIntervalMs, reconcileIntervalMs,
                partitionCount, replicationFactor, minMoveRatio, rebalanceCooldownMs);
    }
}
