package com.ww.admin.security;

import com.ww.cache.Cache;
import org.apache.shiro.cache.CacheException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Shiro {@link org.apache.shiro.cache.Cache} 适配器：将分布式缓存包装为 Shiro 缓存接口。
 * <p>
 * 所有写入带默认 TTL（安全网），避免授权信息永驻缓存导致权限变更后不生效。
 * 缓存键通过前缀隔离不同 Shiro 缓存（授权缓存、Session 缓存等）。
 * <p>
 * 注意：底层分布式缓存使用 String 键，本适配器将 Object 键通过 {@code toString()} 转换。
 * 授权缓存键由 {@link DbRealm#getAuthorizationCacheKey} 返回 username（String），兼容此转换。
 */
public class ShiroDistributedCache implements org.apache.shiro.cache.Cache<Object, Object> {

    private final Cache<String, Object> delegate;
    private final String prefix;
    private final long defaultTtlMillis;

    public ShiroDistributedCache(Cache<String, Object> delegate, String prefix, long defaultTtlMillis) {
        this.delegate = delegate;
        this.prefix = prefix;
        this.defaultTtlMillis = defaultTtlMillis;
    }

    private String key(Object k) {
        return prefix + (k == null ? "null" : k.toString());
    }

    @Override
    public Object get(Object key) throws CacheException {
        try {
            return delegate.get(key(key));
        } catch (Exception e) {
            throw new CacheException("读取缓存失败: " + key, e);
        }
    }

    @Override
    public Object put(Object key, Object value) throws CacheException {
        try {
            Object old = delegate.get(key(key));
            delegate.put(key(key), value, defaultTtlMillis);
            return old;
        } catch (Exception e) {
            throw new CacheException("写入缓存失败: " + key, e);
        }
    }

    /**
     * 带显式 TTL 的写入（供 SessionDAO 等需要精确过期的场景使用）。
     */
    public void putWithTtl(Object key, Object value, long ttlMillis) {
        delegate.put(key(key), value, ttlMillis);
    }

    @Override
    public Object remove(Object key) throws CacheException {
        try {
            Object old = delegate.get(key(key));
            delegate.remove(key(key));
            return old;
        } catch (Exception e) {
            throw new CacheException("删除缓存失败: " + key, e);
        }
    }

    @Override
    public void clear() throws CacheException {
        try {
            for (String k : delegate.keys()) {
                if (k.startsWith(prefix)) {
                    delegate.remove(k);
                }
            }
        } catch (Exception e) {
            throw new CacheException("清空缓存失败: " + prefix, e);
        }
    }

    @Override
    public int size() {
        int n = 0;
        for (String k : delegate.keys()) {
            if (k.startsWith(prefix)) n++;
        }
        return n;
    }

    @Override
    public Set<Object> keys() {
        Set<Object> result = new HashSet<>();
        for (String k : delegate.keys()) {
            if (k.startsWith(prefix)) {
                result.add(k.substring(prefix.length()));
            }
        }
        return result;
    }

    @Override
    public Collection<Object> values() {
        java.util.List<Object> result = new ArrayList<>();
        for (String k : delegate.keys()) {
            if (k.startsWith(prefix)) {
                Object v = delegate.get(k);
                if (v != null) result.add(v);
            }
        }
        return result;
    }
}
