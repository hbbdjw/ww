package com.ww.admin.security;

import com.ww.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Shiro {@link org.apache.shiro.cache.CacheManager} 实现：基于分布式缓存。
 * <p>
 * 为每个 Shiro 缓存名创建独立的 {@link ShiroDistributedCache} 实例（前缀隔离）。
 * 装配到 {@link org.apache.shiro.mgt.SecurityManager} 后，Shiro 自动将其注入 Realm，
 * 实现授权信息（角色+权限）的集群共享。
 * <p>
 * 默认 TTL 30 分钟，作为权限变更后缓存自动过期 的安全网。
 * 精确失效由 {@link AuthzCacheService} 在权限变更点主动触发。
 */
@Component
public class ShiroDistributedCacheManager implements org.apache.shiro.cache.CacheManager {

    private final Cache<String, Object> distributedCache;
    private final long defaultTtlMillis;

    /** 授权缓存键前缀，供 AuthzCacheService 引用 */
    public static final String AUTHZ_CACHE_PREFIX = "shiro:authz:";

    @Autowired
    public ShiroDistributedCacheManager(Cache<String, Object> distributedCache) {
        this(distributedCache, 30 * 60 * 1000L);
    }

    ShiroDistributedCacheManager(Cache<String, Object> distributedCache, long defaultTtlMillis) {
        this.distributedCache = distributedCache;
        this.defaultTtlMillis = defaultTtlMillis;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> org.apache.shiro.cache.Cache<K, V> getCache(String name) throws CacheException {
        String prefix = "shiro:" + name + ":";
        return (org.apache.shiro.cache.Cache<K, V>)
                new ShiroDistributedCache(distributedCache, prefix, defaultTtlMillis);
    }

    /**
     * 直接访问底层分布式缓存（供 CacheSessionDAO 等需要精确控制 TTL 的组件使用）。
     */
    public Cache<String, Object> getDistributedCache() {
        return distributedCache;
    }

    public long getDefaultTtlMillis() {
        return defaultTtlMillis;
    }
}
