package com.ww.cache;

/**
 * 缓存条目，包装缓存值和过期时间。
 */
public class CacheEntry<V> {

    private final V value;
    private final long expireTime;

    /**
     * @param value      缓存值
     * @param ttlMillis  过期时间（毫秒），0 表示永不过期
     */
    public CacheEntry(V value, long ttlMillis) {
        this.value = value;
        this.expireTime = ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : Long.MAX_VALUE;
    }

    /**
     * 用精确过期时间戳构造条目。供 listReplace 等"剩余 TTL 同步"场景使用，
     * 避免通过 ttl + now 重新计算导致少量漂移。 {@code 缺陷15修复}
     */
    public static <V> CacheEntry<V> withExpireTime(V value, long expireTime) {
        return new CacheEntry<>(value, expireTime, true);
    }

    private CacheEntry(V value, long expireTime, boolean isAbsolute) {
        this.value = value;
        this.expireTime = isAbsolute ? expireTime : Long.MAX_VALUE;
    }

    public V getValue() {
        return value;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expireTime;
    }

    public long getExpireTime() {
        return expireTime;
    }
}
