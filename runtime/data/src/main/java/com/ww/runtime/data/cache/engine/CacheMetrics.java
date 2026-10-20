package com.ww.runtime.data.cache.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 缓存引擎轻量指标：命中/未命中、过期、驱逐、命令计数与延迟直方图。
 *
 * <p>全部使用 {@link LongAdder}/{@link AtomicLong}，避免读统计阻塞数据面；
 * 定位是进程内可观测性，不替代外部指标系统。</p>
 */
public final class CacheMetrics {

    private static final long[] BUCKET_BOUNDS_NANOS = {
            1_000_000L, 5_000_000L, 10_000_000L, 50_000_000L,
            100_000_000L, 500_000_000L, 1_000_000_000L
    };
    private static final String[] BUCKET_LABELS = {
            "<=1ms", "<=5ms", "<=10ms", "<=50ms", "<=100ms", "<=500ms", "<=1s", ">1s"
    };

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder expired = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder totalCommands = new LongAdder();
    private final LongAdder totalCommandNanos = new LongAdder();
    private final AtomicLong maxCommandNanos = new AtomicLong();
    private final ConcurrentHashMap<String, LongAdder> commandCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, LongAdder> commandsPerSecond = new ConcurrentHashMap<>();
    private final LongAdder[] latencyBuckets = new LongAdder[BUCKET_LABELS.length];
    private static final int RATE_WINDOW_SECONDS = 60;

    public CacheMetrics() {
        for (int i = 0; i < latencyBuckets.length; i++) {
            latencyBuckets[i] = new LongAdder();
        }
    }

    public void hit() {
        hits.increment();
    }

    public void miss() {
        misses.increment();
    }

    public void expired(long count) {
        if (count > 0) {
            expired.add(count);
        }
    }

    public void eviction() {
        evictions.increment();
    }

    public void error() {
        errors.increment();
    }

    public void recordCommand(String command, long nanos) {
        String name = command == null || command.isEmpty() ? "UNKNOWN" : command;
        commandCounts.computeIfAbsent(name, k -> new LongAdder()).increment();
        totalCommands.increment();
        long second = System.currentTimeMillis() / 1000L;
        commandsPerSecond.computeIfAbsent(second, k -> new LongAdder()).increment();
        totalCommandNanos.add(nanos);
        maxCommandNanos.accumulateAndGet(nanos, Math::max);
        int bucket = BUCKET_LABELS.length - 1;
        for (int i = 0; i < BUCKET_BOUNDS_NANOS.length; i++) {
            if (nanos <= BUCKET_BOUNDS_NANOS[i]) {
                bucket = i;
                break;
            }
        }
        latencyBuckets[bucket].increment();
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        long h = hits.sum();
        long m = misses.sum();
        out.put("hits", h);
        out.put("misses", m);
        out.put("hitRate", h + m == 0 ? 0.0d : (double) h / (h + m));
        out.put("expired", expired.sum());
        out.put("evictions", evictions.sum());
        out.put("errors", errors.sum());
        out.put("commandsTotal", totalCommands.sum());
        long nowSecond = System.currentTimeMillis() / 1000L;
        long qps1s = commandsPerSecond.getOrDefault(nowSecond, new LongAdder()).sum();
        long windowSum = 0L;
        for (long sec = nowSecond - RATE_WINDOW_SECONDS + 1; sec <= nowSecond; sec++) {
            LongAdder counter = commandsPerSecond.get(sec);
            if (counter != null) {
                windowSum += counter.sum();
            }
        }
        out.put("qps1s", qps1s);
        out.put("qps60s", (double) windowSum / RATE_WINDOW_SECONDS);
        // 低频清理，避免每秒桶无限增长
        commandsPerSecond.keySet().removeIf(sec -> sec < nowSecond - RATE_WINDOW_SECONDS * 2);
        Map<String, Object> commands = new LinkedHashMap<>();
        commandCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> commands.put(e.getKey(), e.getValue().sum()));
        out.put("commands", commands);
        Map<String, Object> latency = new LinkedHashMap<>();
        long count = totalCommands.sum();
        latency.put("count", count);
        latency.put("avgNanos", count == 0 ? 0L : totalCommandNanos.sum() / count);
        latency.put("maxNanos", maxCommandNanos.get());
        Map<String, Object> buckets = new LinkedHashMap<>();
        for (int i = 0; i < BUCKET_LABELS.length; i++) {
            buckets.put(BUCKET_LABELS[i], latencyBuckets[i].sum());
        }
        latency.put("buckets", buckets);
        out.put("latency", latency);
        return out;
    }
}
