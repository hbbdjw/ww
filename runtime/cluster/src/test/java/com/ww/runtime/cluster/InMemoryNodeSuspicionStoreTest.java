package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link InMemoryNodeSuspicionStore} 单测：怀疑票的计票、TTL 过期、
 * 撤票（recall）、判死后清票与周期清扫。
 */
class InMemoryNodeSuspicionStoreTest {

    private static final NodeId X = NodeId.of("X");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId C = NodeId.of("C");

    private final TestClock clock = new TestClock(10_000L);

    private InMemoryNodeSuspicionStore store() {
        return new InMemoryNodeSuspicionStore(clock);
    }

    @Test
    void distinctObserversCountOnceAndRefreshesTimestamp() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 3.5);
        store.vote(X, A, 5.0); // 同一观察者重复投票：覆盖不叠加
        store.vote(X, B, 4.0);
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 12_000L)).isEqualTo(2);
    }

    @Test
    void votesOutsideTtlDoNotCount() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 3.0);
        clock.advance(5_000L);
        store.vote(X, B, 3.0); // B 的票更新鲜
        // now=15000，ttl=6000，截止 9000：A 在 10000 仍新鲜
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 6_000L)).isEqualTo(2);
        clock.advance(5_000L);
        // now=20000，截止 14000：A(10000) 过期，B(15000) 新鲜
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 6_000L)).isEqualTo(1);
    }

    @Test
    void freshVoteCountsGroupsBySuspect() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 3.0);
        store.vote(X, B, 3.0);
        store.vote(C, A, 8.0);
        Map<NodeId, Integer> counts = store.freshVoteCounts(clock.nowMillis(), 12_000L);
        assertThat(counts).containsEntry(X, 2).containsEntry(C, 1);
    }

    @Test
    void recallRemovesOnlyTheObserverVote() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 3.0);
        store.vote(X, B, 3.0);
        store.recall(X, A);
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 12_000L)).isEqualTo(1);
        store.recall(X, B);
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 12_000L)).isZero();
        // 撤回不存在的票幂等
        store.recall(X, C);
    }

    @Test
    void clearRemovesAllVotesForSuspect() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 8.0);
        store.vote(X, B, 8.0);
        store.vote(C, A, 8.0);
        store.clear(X);
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 12_000L)).isZero();
        assertThat(store.freshVoteCount(C, clock.nowMillis(), 12_000L)).isEqualTo(1);
    }

    @Test
    void pruneExpiredDropsOnlyStaleRows() {
        InMemoryNodeSuspicionStore store = store();
        store.vote(X, A, 3.0); // 10000
        clock.advance(8_000L);
        store.vote(X, B, 3.0); // 18000
        store.vote(C, A, 3.0); // 18000
        // now=18000，截止 12000（ttl=6000）：A→X 过期，其余新鲜
        store.pruneExpired(clock.nowMillis(), 6_000L);
        Map<NodeId, Integer> counts = store.freshVoteCounts(clock.nowMillis(), 6_000L);
        assertThat(counts).containsEntry(X, 1).containsEntry(C, 1);
    }

    @Test
    void unknownSuspectHasZeroVotes() {
        InMemoryNodeSuspicionStore store = store();
        assertThat(store.freshVoteCount(X, clock.nowMillis(), 1_000L)).isZero();
        assertThat(store.freshVoteCounts(clock.nowMillis(), 1_000L)).isEmpty();
    }
}
