package com.ww.runtime.cluster;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.SystemClock;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link NodeSuspicionStore} 的进程内实现：
 * {@code suspect -> (observer -> Vote(phi, updatedAtMillis))} 双层并发映射，
 * 全部读-改-写在单个 suspect 的内层 Map 上原子完成，线程安全。
 *
 * <p>时间戳取注入的 {@link Clock}（生产 SystemClock，测试 TestClock）。</p>
 */
public final class InMemoryNodeSuspicionStore implements NodeSuspicionStore {

    private record Vote(double phi, long updatedAtMillis) {
    }

    private final Clock clock;
    private final Map<NodeId, Map<NodeId, Vote>> votes = new ConcurrentHashMap<>();

    public InMemoryNodeSuspicionStore() {
        this(SystemClock.instance());
    }

    public InMemoryNodeSuspicionStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void vote(NodeId suspect, NodeId observer, double phi) {
        Objects.requireNonNull(suspect, "suspect");
        Objects.requireNonNull(observer, "observer");
        votes.computeIfAbsent(suspect, k -> new ConcurrentHashMap<>())
                .put(observer, new Vote(phi, clock.nowMillis()));
    }

    @Override
    public void recall(NodeId suspect, NodeId observer) {
        Objects.requireNonNull(suspect, "suspect");
        Objects.requireNonNull(observer, "observer");
        Map<NodeId, Vote> perObserver = votes.get(suspect);
        if (perObserver != null) {
            perObserver.remove(observer);
        }
    }

    @Override
    public int freshVoteCount(NodeId suspect, long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        Map<NodeId, Vote> perObserver = votes.get(Objects.requireNonNull(suspect, "suspect"));
        if (perObserver == null) {
            return 0;
        }
        long cutoff = now - ttlMillis;
        long[] count = {0L};
        perObserver.values().forEach(vote -> {
            if (vote.updatedAtMillis() >= cutoff) {
                count[0]++;
            }
        });
        return (int) count[0];
    }

    @Override
    public Map<NodeId, Integer> freshVoteCounts(long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        long cutoff = now - ttlMillis;
        Map<NodeId, Integer> result = new LinkedHashMap<>();
        votes.forEach((suspect, perObserver) -> {
            long count = perObserver.values().stream()
                    .filter(vote -> vote.updatedAtMillis() >= cutoff)
                    .count();
            if (count > 0) {
                result.put(suspect, (int) count);
            }
        });
        return result;
    }

    @Override
    public void clear(NodeId suspect) {
        votes.remove(Objects.requireNonNull(suspect, "suspect"));
    }

    @Override
    public void pruneExpired(long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        long cutoff = now - ttlMillis;
        votes.values().forEach(perObserver ->
                perObserver.values().removeIf(vote -> vote.updatedAtMillis() < cutoff));
    }
}
