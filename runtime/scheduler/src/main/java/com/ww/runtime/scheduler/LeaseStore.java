package com.ww.runtime.scheduler;

import java.util.Optional;

/**
 * 调度器 leader 租约存储：保证同一时刻整个集群至多一个 {@link ReconcileLoop}
 * 以 leader 身份执行调谐（reconcile）。
 *
 * <p>{@link #acquireOrRenew(String, long)} 的原子语义：</p>
 * <ul>
 *   <li>租约无主，或已过期（{@code expiresAt < now}）→ 调用方抢占成为新 owner；</li>
 *   <li>调用方已是 owner → 续约（延长过期时刻）；</li>
 *   <li>他人持有且未过期 → 返回 {@code false}，租约保持不变。</li>
 * </ul>
 *
 * <p>实现必须线程安全；{@link InMemoryLeaseStore} 供测试/单机演示，
 * {@link JdbcLeaseStore} 供生产（PostgreSQL 单行表）。</p>
 */
public interface LeaseStore {

    /**
     * 抢占或续约租约（原子）。
     *
     * @param owner     竞争者标识（通常为 NodeId.value）
     * @param ttlMillis 租约时长（毫秒，须为正）
     * @return 是否持有租约（成功抢占或续约）
     */
    boolean acquireOrRenew(String owner, long ttlMillis);

    /**
     * 抢占或续约并返回单调递增的 leader 纪元（fencing token）：
     * 每次属主变更（含过期后被他节点接管）纪元 +1，同一 owner 续约保持纪元不变。
     *
     * @return 当前纪元；未获得租约返回 -1
     */
    default long acquireOrRenewWithEpoch(String owner, long ttlMillis) {
        return acquireOrRenew(owner, ttlMillis) ? 0L : -1L;
    }

    /**
     * 校验 {@code (owner, epoch)} 仍是当前有效 leader（控制面副作用前调用）。
     * 默认实现无从校验，返回 true（内存/测试实现按需覆写）。
     */
    default boolean isCurrent(String owner, long epoch) {
        return true;
    }

    /**
     * 当前租约持有者。
     *
     * @return 有效租约的 owner；无租约或已过期返回 {@link Optional#empty()}
     */
    Optional<String> currentOwner();
}
