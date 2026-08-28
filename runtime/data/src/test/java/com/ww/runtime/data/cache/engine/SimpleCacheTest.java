package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SimpleCache} 结构簿记测试。
 */
@DisplayName("SimpleCache 结构簿记")
class SimpleCacheTest {

    /** 反射读取私有索引：泄漏断言需要直接观测内部簿记。 */
    @SuppressWarnings("unchecked")
    private static Map<Object, Object> zsetIndex(SimpleCache<?, ?> cache) throws Exception {
        java.lang.reflect.Field f = SimpleCache.class.getDeclaredField("zsetMemberIndex");
        f.setAccessible(true);
        return (Map<Object, Object>) f.get(cache);
    }

    @Test
    @DisplayName("C32：损坏的巨大集合 count 解码不得 OOM，快速失败")
    void hugeCollectionCountDoesNotOom() {
        // TYPE_LIST + count=0x7FFFFFFF + 无元素字节：预分配不钳制则直接 OOM
        byte[] malformed = new byte[] { 0x10, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };

        long start = System.nanoTime();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Codec.decodeValue(malformed))
                .isInstanceOf(RuntimeException.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).as("解码应立即失败而非长时间分配/循环").isLessThan(5_000);
    }

    @Test
    @DisplayName("C8：remove 清理 zset 成员反向索引")
    void removeCleansZsetIndex() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.zadd("z", 1.0d, "m");
        assertThat(zsetIndex(cache)).containsKey("z");

        cache.remove("z");
        assertThat(zsetIndex(cache)).isEmpty();
    }

    @Test
    @DisplayName("C8：LRU 驱逐清理 zset 成员反向索引")
    void lruEvictionCleansZsetIndex() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 2);
        cache.zadd("z", 1.0d, "m");
        cache.put("k2", "v2");
        assertThat(zsetIndex(cache)).containsKey("z");

        cache.put("k3", "v3"); // 超出 maxSize，最旧的 z 被驱逐
        assertThat(cache.getAllZSetEntries()).isEmpty();
        assertThat(zsetIndex(cache)).isEmpty();
    }

    @Test
    @DisplayName("C8：过期批量清理清理 zset 成员反向索引")
    void evictExpiredCleansZsetIndex() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.zadd("z", 1.0d, "m");
        cache.expire("z", 1);
        Thread.sleep(10);

        int evicted = cache.evictExpired();
        assertThat(evicted).isEqualTo(1);
        assertThat(zsetIndex(cache)).isEmpty();
    }

    @Test
    @DisplayName("C8：clear 清空 zset 成员反向索引")
    void clearCleansZsetIndex() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.zadd("z", 1.0d, "m");

        cache.clear();
        assertThat(zsetIndex(cache)).isEmpty();
    }

    @Test
    @DisplayName("C8：读路径惰性过期清理 zset 成员反向索引")
    void lazyExpiryCleansZsetIndex() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.zadd("z", 1.0d, "m");
        cache.expire("z", 1);
        Thread.sleep(10);

        assertThat(cache.get("z")).isNull(); // doGet 过期分支触发清理
        assertThat(zsetIndex(cache)).isEmpty();
    }
}
