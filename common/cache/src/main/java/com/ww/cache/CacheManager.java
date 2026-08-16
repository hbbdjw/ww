package com.ww.cache;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * 缓存管理器，注入 Spring 管理的 Cache Bean，提供便捷静态方法
 */
@Component
public class CacheManager {

    // 使用 volatile 保证多线程可见性，防止指令重排序导致的可见性问题
    private static volatile Cache<String, Object> cache;

    public CacheManager(Cache<String, Object> defaultCache) {
        CacheManager.cache = defaultCache;
    }

    public static Cache<String, Object> getCache() {
        return cache;
    }

    public static <K, V> Cache<K, V> newCache() {
        return new SimpleCache<>();
    }

    /**
     * 获取远程缓存代理，通过 TCP 协议连接远程节点
     *
     * @param host 远程主机
     * @param port 远程端口
     */
    public static Cache<String, Object> getRemoteCache(String host, int port) {
        return new RemoteCache(host, port);
    }

    /**
     * 获取远程缓存代理（使用默认端口 8700）
     */
    public static Cache<String, Object> getRemoteCache(String host) {
        return new RemoteCache(host, 8700);
    }

    /**
     * 获取远程缓存代理（带认证 token）
     */
    public static Cache<String, Object> getRemoteCache(String host, int port, String authToken) {
        return new RemoteCache(new CacheClient(host, port, authToken));
    }

    /**
     * 获取远程缓存代理（使用自定义 CacheRpcClient，支持 gRPC 等协议）
     */
    public static Cache<String, Object> getRemoteCache(CacheRpcClient client) {
        return new RemoteCache(client);
    }

    // ==================== 便捷静态方法 ====================

    public static void put(String key, Object value) {
        cache.put(key, value);
    }

    public static void put(String key, Object value, long ttlMillis) {
        cache.put(key, value, ttlMillis);
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(String key) {
        return (T) cache.get(key);
    }

    public static void remove(String key) {
        cache.remove(key);
    }

    public static void clear() {
        cache.clear();
    }

    public static int size() {
        return cache.size();
    }

    public static java.util.Set<String> keys() {
        return cache.keys();
    }

    // ==================== Hash 操作 ====================

    public static void hset(String key, String field, Object value) {
        cache.hset(key, field, value);
    }

    @SuppressWarnings("unchecked")
    public static <T> T hget(String key, String field) {
        return (T) cache.hget(key, field);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> hmget(String key, String... fields) {
        return (List<T>) cache.hmget(key, fields);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> hgetAll(String key) {
        return cache.hgetAll(key);
    }

    // ==================== List 操作 ====================

    public static void lpush(String key, Object... values) {
        cache.lpush(key, values);
    }

    public static void rpush(String key, Object... values) {
        cache.rpush(key, values);
    }

    @SuppressWarnings("unchecked")
    public static <T> T lpop(String key) {
        return (T) cache.lpop(key);
    }

    @SuppressWarnings("unchecked")
    public static <T> T rpop(String key) {
        return (T) cache.rpop(key);
    }

    // ==================== Set 操作 ====================

    public static void sadd(String key, Object... members) {
        cache.sadd(key, members);
    }

    @SuppressWarnings("unchecked")
    public static <T> java.util.Set<T> smembers(String key) {
        return (java.util.Set<T>) cache.smembers(key);
    }

    public static void srem(String key, Object... members) {
        cache.srem(key, members);
    }

    // ==================== Sorted Set 操作 ====================

    public static void zadd(String key, double score, Object member) {
        cache.zadd(key, score, member);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> zrange(String key, long start, long stop) {
        return (List<T>) cache.zrange(key, start, stop);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> zrangeByScore(String key, double min, double max) {
        return (List<T>) cache.zrangeByScore(key, min, max);
    }

    // ==================== 事务操作 ====================

    public static void multi() {
        cache.multi();
    }

    public static List<Object> exec() {
        return cache.exec();
    }

    public static void discard() {
        cache.discard();
    }

    public static void watch(String... keys) {
        cache.watch(keys);
    }
}
