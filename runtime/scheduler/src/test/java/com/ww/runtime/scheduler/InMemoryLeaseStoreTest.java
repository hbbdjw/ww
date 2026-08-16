package com.ww.runtime.scheduler;

import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryLeaseStoreTest {

    private static final long TTL = 100L;

    private final TestClock clock = new TestClock(1_000L);
    private final InMemoryLeaseStore store = new InMemoryLeaseStore(clock);

    @Test
    void freshAcquireSucceedsAndExposesOwner() {
        assertThat(store.acquireOrRenew("node-a", TTL)).isTrue();
        assertThat(store.currentOwner()).contains("node-a");
    }

    @Test
    void sameOwnerRenewsLease() {
        assertThat(store.acquireOrRenew("node-a", TTL)).isTrue();
        clock.advance(TTL - 1); // 仍在有效期内
        assertThat(store.acquireOrRenew("node-a", TTL)).isTrue();
        assertThat(store.currentOwner()).contains("node-a");
        clock.advance(TTL - 1); // 续约后仍有效
        assertThat(store.currentOwner()).contains("node-a");
    }

    @Test
    void otherOwnerIsRejectedWhileLeaseValid() {
        assertThat(store.acquireOrRenew("node-a", TTL)).isTrue();
        clock.advance(TTL - 1);
        assertThat(store.acquireOrRenew("node-b", TTL)).isFalse();
        assertThat(store.currentOwner()).contains("node-a");
    }

    @Test
    void leaseExpiresExactlyAtBoundaryThenTakeoverWithinTwoTtl() {
        assertThat(store.acquireOrRenew("node-a", TTL)).isTrue();
        clock.advance(TTL); // 恰好到过期时刻：expiresAt < now 为假 → 仍持有
        assertThat(store.currentOwner()).contains("node-a");
        assertThat(store.acquireOrRenew("node-b", TTL)).isFalse();

        clock.advance(1); // 过期（接管耗时 ≤ 2×TTL）
        assertThat(store.acquireOrRenew("node-b", TTL)).isTrue();
        assertThat(store.currentOwner()).contains("node-b");
    }

    @Test
    void currentOwnerIsEmptyWhenNoLeaseOrExpired() {
        assertThat(store.currentOwner()).isEmpty();
        store.acquireOrRenew("node-a", TTL);
        clock.advance(2 * TTL);
        assertThat(store.currentOwner()).isEqualTo(Optional.empty());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> store.acquireOrRenew(" ", TTL))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.acquireOrRenew("node-a", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentAcquireLeavesExactlyOneOwner() throws Exception {
        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < rounds; i++) {
                CountDownLatch start = new CountDownLatch(2);
                Future<Boolean> a = pool.submit(() -> race(start, "node-a"));
                Future<Boolean> b = pool.submit(() -> race(start, "node-b"));
                boolean wonA = a.get();
                boolean wonB = b.get();
                clock.advance(2 * TTL); // 每轮过期，下一轮重新竞争
                assertThat(wonA ^ wonB)
                        .as("exactly one of the two competitors must hold the lease in round %d", i)
                        .isTrue();
            }
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean race(CountDownLatch start, String owner) {
        start.countDown();
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return store.acquireOrRenew(owner, TTL);
    }
}
