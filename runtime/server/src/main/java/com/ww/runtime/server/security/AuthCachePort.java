package com.ww.runtime.server.security;

/**
 * 认证缓存端口：会话与权限数据的 KV 存储抽象（带 TTL）。
 *
 * <p>由集群 CACHE 能力经网关实现，使会话服务可在单测中使用内存桩，
 * 不直接依赖具体网关类型。</p>
 */
public interface AuthCachePort {

    /** 写入，ttlMillis &gt; 0 时相对过期，&lt;= 0 永不过期。 */
    void put(String key, Object value, long ttlMillis);

    /** 读取，不存在或已过期返回 null。 */
    Object get(String key);

    /** 删除（不存在时幂等无操作）。 */
    void remove(String key);
}
