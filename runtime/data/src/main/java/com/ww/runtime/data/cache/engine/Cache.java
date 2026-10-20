package com.ww.runtime.data.cache.engine;

/**
 * 缓存接口，定义基本的缓存操作
 */
public interface Cache<K, V> {

    /**
     * 存入缓存
     *
     * @param key       键
     * @param value     值
     * @param ttlMillis 过期时间（毫秒），0 表示永不过期
     */
    void put(K key, V value, long ttlMillis);

    /**
     * 存入缓存（永不过期）
     */
    void put(K key, V value);

    /**
     * 仅当 key 不存在（或已过期）时写入；已存在时不修改值、TTL 与 LRU 顺序。
     * <p>事务（MULTI）中入队并返回 {@code false}，实际结果见 {@link #exec()}。
     *
     * @return {@code true} 表示本次调用完成写入；{@code false} 表示 key 已存在
     */
    boolean putIfAbsent(K key, V value, long ttlMillis);

    /**
     * 仅当 key 不存在（或已过期）时写入（永不过期）。
     */
    default boolean putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, 0L);
    }

    /**
     * 获取缓存值，过期返回 null
     */
    V get(K key);

    /**
     * 获取缓存值，若不存在则使用 loader 加载并写入缓存（互斥锁保护，防止缓存击穿）。
     * <p>当多个线程同时请求同一未命中 key 时，仅一个线程执行 loader，其余线程等待结果。
     *
     * @param key    缓存键
     * @param loader 缓存未命中时的数据加载函数；返回 null 表示数据不存在，将缓存空标记防止穿透
     * @param ttlMillis 过期时间（毫秒），0 表示永不过期
     */
    V getWithLoader(K key, java.util.function.Supplier<V> loader, long ttlMillis);

    /**
     * 删除缓存
     */
    void remove(K key);

    /**
     * 清除所有缓存
     */
    void clear();

    /**
     * 当前缓存数量（含已过期未清理的条目）
     */
    int size();

    /**
     * 返回所有 key 的集合
     */
    java.util.Set<K> keys();

    // ==================== Hash 操作 ====================

    /**
     * 将哈希表 key 中的字段 field 设置为 value
     */
    void hset(K key, String field, V value);

    /**
     * 获取哈希表 key 中指定字段 field 的值
     */
    V hget(K key, String field);

    /**
     * 获取哈希表 key 中多个字段的值，按顺序返回，不存在的字段为 null
     */
    java.util.List<V> hmget(K key, String... fields);

    /**
     * 获取哈希表 key 中所有字段和值
     */
    java.util.Map<String, V> hgetAll(K key);

    // ==================== List 操作 ====================

    /**
     * 向列表头部插入值（支持多个，按顺序依次插入头部）
     */
    void lpush(K key, V... values);

    /**
     * 向列表尾部追加值（支持多个，按顺序依次追加）
     */
    void rpush(K key, V... values);

    /**
     * 从列表头部弹出值，列表为空返回 null
     */
    V lpop(K key);

    /**
     * 从列表尾部弹出值，列表为空返回 null
     */
    V rpop(K key);

    // ==================== Set 操作 ====================

    /**
     * 向集合中添加成员（支持多个）
     */
    void sadd(K key, V... members);

    /**
     * 获取集合中所有成员
     */
    java.util.Set<V> smembers(K key);

    /**
     * 从集合中移除成员（支持多个）
     */
    void srem(K key, V... members);

    // ==================== Sorted Set 操作 ====================

    /**
     * 向有序集合添加成员及分数
     */
    void zadd(K key, double score, V member);

    /**
     * 按索引范围获取成员（分数升序），start/stop 为 0 起始索引，负数表示从末尾倒数，-1 表示最后一个
     */
    java.util.List<V> zrange(K key, long start, long stop);

    /**
     * 按分数范围获取成员（升序），min/max 为包含边界
     */
    java.util.List<V> zrangeByScore(K key, double min, double max);

    // ==================== 事务操作 ====================

    /**
     * 开启事务，之后所有写操作将被入队，在 exec 时原子执行
     */
    void multi();

    /**
     * 执行事务中的所有命令，返回每个命令的结果列表。若被 watch 的键在事务期间被修改，返回 null
     */
    java.util.List<Object> exec();

    /**
     * 放弃事务，清空命令队列
     */
    void discard();

    /**
     * 监视指定键，在 exec 时若任一被监视的键被修改，事务将中止
     */
    void watch(K... keys);
}
