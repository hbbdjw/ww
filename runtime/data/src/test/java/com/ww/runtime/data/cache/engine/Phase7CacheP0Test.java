package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P0 验收：类型标记（JSON Map vs Hash）、keyspace/容器新命令、SCAN/Stream 可见性、
 * 近似内存水位、AOF HDEL/ZREM 回放与统计。
 */
@DisplayName("P0 缓存暴露 / 内存治理 / 可观测性")
class Phase7CacheP0Test {

    @TempDir
    File dir;

    @Test
    @DisplayName("JSON Map 保持 KV 语义，不被当成 Hash")
    void jsonMapIsKvNotHash() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("a", 1);
        cache.put("j", json);

        assertThat(cache.type("j")).isEqualTo("string");
        assertThat(cache.hgetAll("j")).isEmpty();
        assertThat(cache.getAllHashEntries()).isEmpty();
        assertThat(cache.getAllKeyValueEntries())
                .extracting(Map.Entry::getKey)
                .contains("j");

        cache.hset("j", "b", 2);
        assertThat(cache.type("j")).isEqualTo("hash");
        assertThat(cache.hget("j", "b")).isEqualTo(2);
        assertThat(cache.getAllHashEntries())
                .extracting(Map.Entry::getKey)
                .contains("j");
    }

    @Test
    @DisplayName("keyspace 与原子 KV 命令：TTL/INCR/CAS/PERSIST")
    void keyspaceAndAtomicCommands() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("k", "10", 60_000L);

        assertThat(cache.exists("k")).isTrue();
        assertThat(cache.type("k")).isEqualTo("string");
        assertThat(cache.ttlMillis("k")).isBetween(1L, 60_000L);
        assertThat(cache.touch("k")).isTrue();

        assertThat(cache.incrBy("k", 5L)).isEqualTo(15L);
        assertThat(cache.get("k")).isEqualTo(15L);
        assertThat(cache.ttlMillis("k")).as("INCR 保留 TTL").isBetween(1L, 60_000L);

        assertThat(cache.persist("k")).isTrue();
        assertThat(cache.ttlMillis("k")).isEqualTo(-1L);
        assertThat(cache.cas("k", 15L, 20L)).isTrue();
        assertThat(cache.cas("k", 15L, 30L)).isFalse();
        assertThat(cache.get("k")).isEqualTo(20L);

        assertThat(cache.incrBy("counter", 1L)).isEqualTo(1L);
        assertThat(cache.decrBy("counter", 2L)).isEqualTo(-1L);
        assertThat(cache.type("missing")).isEqualTo("none");
        assertThat(cache.ttlMillis("missing")).isEqualTo(-2L);
    }

    @Test
    @DisplayName("容器新命令：HDEL/HLEN/HEXISTS、LLEN/LRANGE/LINDEX、SCARD/SISMEMBER、ZCARD/ZSCORE/ZRANK/ZREM")
    void containerCommands() {
        SimpleCache<String, Object> cache = new SimpleCache<>();

        cache.hset("h", "a", 1);
        cache.hset("h", "b", 2);
        assertThat(cache.hlen("h")).isEqualTo(2L);
        assertThat(cache.hexists("h", "a")).isTrue();
        assertThat(cache.hdel("h", "a")).isEqualTo(1L);
        assertThat(cache.hgetAll("h")).containsEntry("b", 2);
        assertThat(cache.hdel("h", "b")).isEqualTo(1L);
        assertThat(cache.type("h")).isEqualTo("none");

        cache.rpush("l", "a", "b", "c");
        assertThat(cache.llen("l")).isEqualTo(3L);
        assertThat(cache.lrange("l", 0, -1)).containsExactly("a", "b", "c");
        assertThat(cache.lindex("l", -1)).isEqualTo("c");
        assertThat(cache.lpop("l")).isEqualTo("a");
        assertThat(cache.rpop("l")).isEqualTo("c");
        assertThat(cache.rpop("l")).isEqualTo("b");
        assertThat(cache.type("l")).isEqualTo("none");

        assertThat(cache.saddCount("s", "a", "b", "a")).isEqualTo(2L);
        assertThat(cache.scard("s")).isEqualTo(2L);
        assertThat(cache.sismember("s", "a")).isTrue();
        assertThat(cache.sremCount("s", "a", "b")).isEqualTo(2L);
        assertThat(cache.type("s")).isEqualTo("none");

        cache.zadd("z", 1.0, "a");
        cache.zadd("z", 2.0, "b");
        assertThat(cache.zcard("z")).isEqualTo(2L);
        assertThat(cache.zscore("z", "b")).isEqualTo(2.0);
        assertThat(cache.zrank("z", "b")).isEqualTo(1L);
        assertThat(cache.zrem("z", "a")).isEqualTo(1L);
        assertThat(cache.zrem("z", "b")).isEqualTo(1L);
        assertThat(cache.type("z")).isEqualTo("none");
    }

    @Test
    @DisplayName("SCAN/key 索引包含 Stream；DEL/TYPE/TTL 对 Stream 生效")
    void scanAndStreamKeyVisibility() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("a1", "v");
        cache.put("a2", "v");
        cache.xadd("s1", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());

        assertThat(cache.type("s1")).isEqualTo("stream");
        assertThat(cache.exists("s1")).isTrue();
        assertThat(cache.ttlMillis("s1")).isEqualTo(-1L);
        assertThat(cache.allKeys()).contains("a1", "a2", "s1");

        Map<String, Object> first = cache.scan("", null, 2);
        assertThat((List<?>) first.get("keys")).hasSize(2);
        assertThat(first.get("done")).isEqualTo(false);

        Map<String, Object> second = cache.scan((String) first.get("cursor"), null, 2);
        assertThat((List<Object>) second.get("keys")).contains("s1");
        assertThat(second.get("done")).isEqualTo(true);

        assertThat(cache.persist("s1")).isFalse();
        cache.remove("s1");
        assertThat(cache.type("s1")).isEqualTo("none");
        assertThat(cache.allKeys()).doesNotContain("s1");
    }

    @Test
    @DisplayName("近似字节水位触发 LRU 驱逐；单值超限拒绝")
    void memoryWatermarkEvicts() {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 1000, new StreamStore(), 300L);
        String value = "x".repeat(100);
        for (int i = 0; i < 10; i++) {
            cache.put("k" + i, value);
        }
        assertThat(cache.getEstimatedBytes()).isLessThanOrEqualTo(300L);
        assertThat(cache.getByteEvictions()).isGreaterThan(0L);
        assertThat(cache.size()).isLessThan(10);
        assertThat(cache.get("k9")).isEqualTo(value);

        SimpleCache<String, Object> small = new SimpleCache<>(null, 10, new StreamStore(), 100L);
        assertThatThrownBy(() -> small.put("big", "x".repeat(100)))
                .isInstanceOf(CacheException.class)
                .hasMessageContaining("maxMemoryBytes");
    }

    @Test
    @DisplayName("AOF 回放：HDEL/ZREM 生效，普通 Map KV 往返不被误当 Hash")
    void aofReplayHdelZrem() throws Exception {
        AofProperties props = new AofProperties();
        props.setEnabled(true);
        props.setDir(dir.getPath());
        props.setAppendfsync("no");
        props.setAutoRewrite(false);

        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(props, cache);
        aof.start();
        cache.setAofService(aof);
        cache.hset("h", "a", 1);
        cache.hset("h", "b", 2);
        cache.hdel("h", "a");
        cache.zadd("z", 1.0, "a");
        cache.zadd("z", 2.0, "b");
        cache.zrem("z", "a");
        cache.put("json", Map.of("x", 1));
        aof.stop();

        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(props, restored);
        aof2.start();
        aof2.replay(restored);

        assertThat(restored.hgetAll("h")).containsEntry("b", 2).doesNotContainKey("a");
        assertThat(restored.zcard("z")).isEqualTo(1L);
        assertThat(restored.zscore("z", "b")).isEqualTo(2.0);
        assertThat(restored.type("json")).isEqualTo("string");
        assertThat(restored.get("json")).isInstanceOf(Map.class);
        aof2.stop();
    }

    @Test
    @DisplayName("MULTI/EXEC 锁住排队命令涉及的 key（不再只锁 WATCH key）")
    void execLocksQueuedKeys() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        Method lockFor = SimpleCache.class.getDeclaredMethod("lockFor", Object.class);
        lockFor.setAccessible(true);
        ReentrantLock lock = (ReentrantLock) lockFor.invoke(cache, "k");
        lock.lock();

        CountDownLatch queued = new CountDownLatch(1);
        AtomicBoolean done = new AtomicBoolean(false);
        Thread tx = new Thread(() -> {
            cache.multi();
            cache.put("k", "v");
            queued.countDown();
            cache.exec();
            done.set(true);
        });
        tx.start();
        assertThat(queued.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        assertThat(done.get()).as("exec 应阻塞在 key 条带锁上").isFalse();

        lock.unlock();
        tx.join(2000);
        assertThat(done.get()).isTrue();
        assertThat(cache.get("k")).isEqualTo("v");
    }

    @Test
    @DisplayName("统计：命中率/命令/内存字段可见")
    @SuppressWarnings("unchecked")
    void statsExposeMetrics() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("k", "v");
        cache.get("k");
        cache.get("missing");

        Map<String, Object> stats = cache.stats();
        assertThat(stats).containsKeys("keys", "estimatedBytes", "maxMemoryBytes",
                "lruEvictions", "metrics");
        Map<String, Object> metrics = (Map<String, Object>) stats.get("metrics");
        assertThat(((Number) metrics.get("hits")).longValue()).isGreaterThanOrEqualTo(1L);
        assertThat(((Number) metrics.get("misses")).longValue()).isGreaterThanOrEqualTo(1L);
        assertThat(metrics).containsKeys("latency", "qps1s", "qps60s");
    }
}
