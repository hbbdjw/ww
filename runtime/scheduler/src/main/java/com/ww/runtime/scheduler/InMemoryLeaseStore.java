package com.ww.runtime.scheduler;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.SystemClock;

import java.util.Objects;
import java.util.Optional;

/**
 * {@link LeaseStore} 的进程内实现：单把互斥锁保护 owner/expiresAt 两个字段，
 * 抢占/续约/读取均为原子操作，线程安全。
 *
 * <p>过期语义与 {@link JdbcLeaseStore} 对齐：{@code expiresAtMillis < now} 视为过期
 * （恰好等于视为仍持有），因此宕机 leader 的租约最迟在 TTL 后即可被接管。</p>
 */
public final class InMemoryLeaseStore implements LeaseStore {

    private final Clock clock;
    private final Object lock = new Object();
    private String owner;
    private long expiresAtMillis;
    private long leaderEpoch;

    public InMemoryLeaseStore() {
        this(SystemClock.instance());
    }

    public InMemoryLeaseStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean acquireOrRenew(String owner, long ttlMillis) {
        return acquireOrRenewWithEpoch(owner, ttlMillis) >= 0;
    }

    @Override
    public long acquireOrRenewWithEpoch(String owner, long ttlMillis) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be null or blank");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        long now = clock.nowMillis();
        synchronized (lock) {
            if (this.owner != null && this.owner.equals(owner)) {
                expiresAtMillis = now + ttlMillis;
                return leaderEpoch;
            }
            if (this.owner == null || expiresAtMillis < now) {
                this.owner = owner;
                expiresAtMillis = now + ttlMillis;
                return ++leaderEpoch; // 每次换主单调递增（fencing token）
            }
            return -1L;
        }
    }

    @Override
    public boolean isCurrent(String owner, long epoch) {
        synchronized (lock) {
            return this.owner != null && this.owner.equals(owner)
                    && leaderEpoch == epoch && expiresAtMillis >= clock.nowMillis();
        }
    }

    @Override
    public Optional<String> currentOwner() {
        synchronized (lock) {
            if (owner == null || expiresAtMillis < clock.nowMillis()) {
                return Optional.empty();
            }
            return Optional.of(owner);
        }
    }
}
