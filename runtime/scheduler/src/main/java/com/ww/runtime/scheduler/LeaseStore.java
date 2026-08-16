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
     * 当前租约持有者。
     *
     * @return 有效租约的 owner；无租约或已过期返回 {@link Optional#empty()}
     */
    Optional<String> currentOwner();
}
