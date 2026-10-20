package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SimpleCache putIfAbsent")
class SimpleCachePutIfAbsentTest {

    @TempDir
    File dir;

    @Test
    @DisplayName("key 不存在时写入并返回 true，TTL 生效")
    void writesWhenAbsent() {
        SimpleCache<String, Object> cache = new SimpleCache<>();

        assertThat(cache.putIfAbsent("k", "v", 60_000)).isTrue();

        assertThat(cache.get("k")).isEqualTo("v");
        assertThat(cache.remainingTtlMillis("k")).isBetween(1L, 60_000L);
    }

    @Test
    @DisplayName("key 已存在时不覆盖值与 TTL，返回 false")
    void keepsExistingValueAndTtl() throws InterruptedException {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        assertThat(cache.putIfAbsent("k", "first", 60_000)).isTrue();

        assertThat(cache.putIfAbsent("k", "second", 1)).isFalse();

        Thread.sleep(20);
        assertThat(cache.get("k")).isEqualTo("first");
        assertThat(cache.remainingTtlMillis("k")).isGreaterThan(1_000L);
    }

    @Test
    @DisplayName("已过期条目视为不存在并允许覆盖")
    void treatsExpiredEntryAsAbsent() throws InterruptedException {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("k", "stale", 1);
        Thread.sleep(10);

        assertThat(cache.putIfAbsent("k", "fresh", 0)).isTrue();

        assertThat(cache.get("k")).isEqualTo("fresh");
    }

    @Test
    @DisplayName("getWithLoader 的空值标记视为不存在，允许真实值写入")
    void replacesNullMarker() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        assertThat(cache.getWithLoader("k", () -> null, 0)).isNull();
        assertThat(cache.get("k")).isNull();

        assertThat(cache.putIfAbsent("k", "real", 0)).isTrue();

        assertThat(cache.get("k")).isEqualTo("real");
    }

    @Test
    @DisplayName("并发 NX 恰好一个成功")
    void concurrentPutIfAbsentHasSingleWinner() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                String value = "v" + i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return cache.putIfAbsent("k", value, 0);
                }));
            }
            start.countDown();

            int winners = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(5, TimeUnit.SECONDS)) {
                    winners++;
                }
            }
            assertThat(winners).isEqualTo(1);
            assertThat(String.valueOf(cache.get("k"))).matches("v\\d+");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("失败的 NX 不触碰 LRU；成功写入按 maxSize 驱逐最老条目")
    void failedPutIfAbsentDoesNotDisturbLru() {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 2);
        cache.put("a", 1);
        cache.put("b", 2);

        assertThat(cache.putIfAbsent("a", 9, 0)).isFalse();
        assertThat(cache.putIfAbsent("c", 3, 0)).isTrue();

        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.keys()).containsExactlyInAnyOrder("b", "c");
    }

    // ------------------------------------------------------------------
    // AOF：只记录实际写入
    // ------------------------------------------------------------------

    @Test
    @DisplayName("成功的 NX 落盘，失败的 NX 不落盘（回放不覆写已有值）")
    void aofLogsOnlySuccessfulPutIfAbsent() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        assertThat(cache.putIfAbsent("k", "first", 0)).isTrue();
        assertThat(cache.putIfAbsent("k", "second", 0)).isFalse();
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        try {
            assertThat(restored.get("k")).isEqualTo("first");
        } finally {
            holder[0].stop();
        }
    }

    @Test
    @DisplayName("成功的 NX 带 TTL 落盘，回放后剩余 TTL 保持")
    void aofPersistsTtlOfSuccessfulPutIfAbsent() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        assertThat(cache.putIfAbsent("t", "v", 60_000)).isTrue();
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        try {
            assertThat(restored.get("t")).isEqualTo("v");
            assertThat(restored.remainingTtlMillis("t")).isBetween(1L, 60_000L);
        } finally {
            holder[0].stop();
        }
    }

    @Test
    @DisplayName("事务内失败的 NX 不记 AOF，回放不得复活新值")
    void failedPutIfAbsentInTransactionIsNotReplayed() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.put("k", "original");

        cache.multi();
        assertThat(cache.putIfAbsent("k", "ghost", 0)).isFalse(); // 入队
        assertThat(cache.exec()).containsExactly(false);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        try {
            assertThat(restored.get("k")).isEqualTo("original");
        } finally {
            holder[0].stop();
        }
    }

    @Test
    @DisplayName("事务内成功的 NX 记 AOF，回放恢复")
    void successfulPutIfAbsentInTransactionIsReplayed() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);

        cache.multi();
        assertThat(cache.putIfAbsent("k", "v", 0)).isFalse(); // 入队
        assertThat(cache.exec()).containsExactly(true);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        try {
            assertThat(restored.get("k")).isEqualTo("v");
        } finally {
            holder[0].stop();
        }
    }

    private AofProperties props() {
        AofProperties p = new AofProperties();
        p.setEnabled(true);
        p.setDir(dir.getPath());
        p.setAppendfsync("no");
        return p;
    }

    private SimpleCache<String, Object> startWriting(AofService[] holder) {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(props(), cache);
        try {
            aof.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cache.setAofService(aof);
        holder[0] = aof;
        return cache;
    }

    private SimpleCache<String, Object> replayFromDisk(AofService[] holder) {
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof = new AofService(props(), restored);
        try {
            aof.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        aof.replay(restored);
        holder[0] = aof;
        return restored;
    }
}
