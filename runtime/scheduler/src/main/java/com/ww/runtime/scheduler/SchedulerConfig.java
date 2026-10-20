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
 * @param replicationFactor  每分区副本数（默认 1；STORAGE 等带复制器的能力使用）
 * @param cacheReplicationFactor CACHE 专属副本数（默认 1——cache 无复制器，
 *                            rf&gt;1 会产生幽灵副本，故障提升即静默丢数据）
 * @param minMoveRatio       触发扩容重平衡的最小移动比例（默认 0.10）
 * @param rebalanceCooldownMs 重平衡冷却时间（默认 30000ms）
 * @param healingGracePeriodMs 治愈宽限期（默认 30000ms）：节点被判 DEAD 后，其无存活副本
 *                            分区的「降级治愈迁移」（本质是放弃旧数据重建空分区）须等 DEAD
 *                            状态持续超过该期限才放行——期限内押注节点重启重入（数据仍在
 *                            原盘）。副本提升不受宽限期约束（数据在存活副本上，提升无损）。
 * @param suspicionVoteTtlMs 怀疑票有效期（默认 12000ms）：超期未刷新的票不计入法定人数
 * @param suspicionMinObservers 法定人数下限（默认 1）：最终阈值 = max(该下限, floor((N-1)/2)+1)
 * @param suspicionQuorumEnabled 是否开启多数仲裁 DEAD（默认 false=本地 φ 判定直写 DEAD 的传统语义；
 *                            true 时 DEAD 只能由 leader 按 TTL 内多数观察者怀疑票提升）
 * @param healingQuarantineEnabled P0-2 隔离治愈开关（默认 true）：无存活副本分区在治愈宽限期
 *                           赌输后<b>标记 DEGRADED 隔离</b>——放置保持指向死亡属主（数据留盘
 *                           押注重启回归自动解除），数据面拒绝服务；false = 旧行为（空分区迁移，
 *                           静默永久丢失，仅供回滚对照）
 */
public record SchedulerConfig(long ttlMillis,
                              long renewIntervalMs,
                              long reconcileIntervalMs,
                              int partitionCount,
                              int replicationFactor,
                              int cacheReplicationFactor,
                              double minMoveRatio,
                              long rebalanceCooldownMs,
                              long healingGracePeriodMs,
                              long suspicionVoteTtlMs,
                              int suspicionMinObservers,
                              boolean suspicionQuorumEnabled,
                              boolean healingQuarantineEnabled) {

    /** 兼容构造器：不使用多数仲裁（票 TTL 12s、最小票数 1、开关关闭）。 */
    public SchedulerConfig(long ttlMillis,
                           long renewIntervalMs,
                           long reconcileIntervalMs,
                           int partitionCount,
                           int replicationFactor,
                           int cacheReplicationFactor,
                           double minMoveRatio,
                           long rebalanceCooldownMs,
                           long healingGracePeriodMs) {
        this(ttlMillis, renewIntervalMs, reconcileIntervalMs, partitionCount, replicationFactor,
                cacheReplicationFactor, minMoveRatio, rebalanceCooldownMs, healingGracePeriodMs,
                12_000L, 1, false, true);
    }

    /** 兼容构造器：不含隔离开关（默认开启隔离）。 */
    public SchedulerConfig(long ttlMillis,
                           long renewIntervalMs,
                           long reconcileIntervalMs,
                           int partitionCount,
                           int replicationFactor,
                           int cacheReplicationFactor,
                           double minMoveRatio,
                           long rebalanceCooldownMs,
                           long healingGracePeriodMs,
                           long suspicionVoteTtlMs,
                           int suspicionMinObservers,
                           boolean suspicionQuorumEnabled) {
        this(ttlMillis, renewIntervalMs, reconcileIntervalMs, partitionCount, replicationFactor,
                cacheReplicationFactor, minMoveRatio, rebalanceCooldownMs, healingGracePeriodMs,
                suspicionVoteTtlMs, suspicionMinObservers, suspicionQuorumEnabled, true);
    }

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
        if (cacheReplicationFactor <= 0) {
            throw new IllegalArgumentException("cacheReplicationFactor must be > 0, got "
                    + cacheReplicationFactor);
        }
        if (!Double.isFinite(minMoveRatio) || minMoveRatio <= 0 || minMoveRatio > 1) {
            throw new IllegalArgumentException("minMoveRatio must be in (0, 1], got " + minMoveRatio);
        }
        if (rebalanceCooldownMs < 0) {
            throw new IllegalArgumentException("rebalanceCooldownMs must be >= 0, got " + rebalanceCooldownMs);
        }
        if (healingGracePeriodMs < 0) {
            throw new IllegalArgumentException("healingGracePeriodMs must be >= 0, got " + healingGracePeriodMs);
        }
        if (suspicionVoteTtlMs <= 0) {
            throw new IllegalArgumentException("suspicionVoteTtlMs must be > 0, got " + suspicionVoteTtlMs);
        }
        if (suspicionMinObservers < 1) {
            throw new IllegalArgumentException("suspicionMinObservers must be >= 1, got " + suspicionMinObservers);
        }
    }

    /** 默认配置工厂。 */
    public static SchedulerConfig of() {
        return new SchedulerConfig(10_000L, 3_000L, 2_000L, 4096, 1, 1, 0.10d, 30_000L, 30_000L);
    }

    /** 全参校验工厂（治愈宽限期取 0 = 立即治愈）。 */
    public static SchedulerConfig of(long ttlMillis,
                                     long renewIntervalMs,
                                     long reconcileIntervalMs,
                                     int partitionCount,
                                     int replicationFactor,
                                     double minMoveRatio,
                                     long rebalanceCooldownMs) {
        return new SchedulerConfig(ttlMillis, renewIntervalMs, reconcileIntervalMs,
                partitionCount, replicationFactor, 1, minMoveRatio, rebalanceCooldownMs, 0L);
    }

    /** 全参校验工厂（含 cache 专属副本数；治愈宽限期取 0 = 立即治愈）。 */
    public static SchedulerConfig of(long ttlMillis,
                                     long renewIntervalMs,
                                     long reconcileIntervalMs,
                                     int partitionCount,
                                     int replicationFactor,
                                     int cacheReplicationFactor,
                                     double minMoveRatio,
                                     long rebalanceCooldownMs) {
        return new SchedulerConfig(ttlMillis, renewIntervalMs, reconcileIntervalMs,
                partitionCount, replicationFactor, cacheReplicationFactor,
                minMoveRatio, rebalanceCooldownMs, 0L);
    }

    /**
     * 按能力取副本数：CACHE → {@link #cacheReplicationFactor}，其余 →
     * {@link #replicationFactor}。后续能力需要专属 rf 时沿本模式扩展。
     */
    public int rfFor(String capability) {
        if ("CACHE".equals(capability)) {
            return cacheReplicationFactor;
        }
        return replicationFactor;
    }
}
