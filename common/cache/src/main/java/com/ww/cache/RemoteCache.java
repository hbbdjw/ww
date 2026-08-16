package com.ww.cache;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 远程缓存代理：通过 CacheRpcClient 将操作转发到远程节点
 */
public class RemoteCache implements Cache<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(RemoteCache.class);

    private final CacheRpcClient client;

    public RemoteCache(String host, int port) {
        this.client = new CacheClient(host, port);
    }

    public RemoteCache(CacheRpcClient client) {
        this.client = client;
    }

    public CacheRpcClient getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

    private byte[] send(byte opCode, byte[] args) {
        try {
            return client.send(CacheProtocol.FLAG_NORMAL, opCode, args, "远程调用");
        } catch (IOException e) {
            throw new CacheException("remote cache error: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T decode(byte[] payload) {
        if (payload == null || payload.length == 0) return null;
        return (T) Codec.decodeValue(payload);
    }

    @Override
    public void put(String key, Object value, long ttlMillis) {
        send(CacheProtocol.OP_PUT_EXPIRED, CacheProtocol.encodeArgs(CacheProtocol.OP_PUT_EXPIRED, key, value, ttlMillis));
    }

    @Override
    public void put(String key, Object value) {
        send(CacheProtocol.OP_PUT, CacheProtocol.encodeArgs(CacheProtocol.OP_PUT, key, value));
    }

    @Override
    public Object get(String key) {
        return decode(send(CacheProtocol.OP_GET, CacheProtocol.encodeArgs(CacheProtocol.OP_GET, key)));
    }

    /**
     * RemoteCache 版的 getWithLoader：先远程读，未命中则在本地执行 loader 并通过 OP_PUT 写回远端。
     * 注意：远端节点的互斥锁无法跨进程保护 loader 执行，存在并发重复加载可能。
     */
    @Override
    public Object getWithLoader(String key, java.util.function.Supplier<Object> loader, long ttlMillis) {
        Object value = get(key);
        if (value != null) {
            return value;
        }
        value = loader.get();
        if (value != null) {
            if (ttlMillis > 0) {
                put(key, value, ttlMillis);
            } else {
                put(key, value);
            }
        }
        return value;
    }

    @Override
    public void remove(String key) {
        send(CacheProtocol.OP_REMOVE, CacheProtocol.encodeArgs(CacheProtocol.OP_REMOVE, key));
    }

    @Override
    public void clear() {
        send(CacheProtocol.OP_CLEAR, new byte[]{CacheProtocol.OP_CLEAR});
    }

    @Override
    public int size() {
        byte[] resp = send(CacheProtocol.OP_SIZE, new byte[]{CacheProtocol.OP_SIZE});
        Object result = decode(resp);
        return result instanceof Number n ? n.intValue() : 0;
    }

    @Override
    public Set<String> keys() {
        byte[] resp = send(CacheProtocol.OP_KEYS, CacheProtocol.encodeArgs(CacheProtocol.OP_KEYS));
        return this.<Set<String>>decode(resp);
    }

    @Override
    public void hset(String key, String field, Object value) {
        send(CacheProtocol.OP_HSET, CacheProtocol.encodeArgs(CacheProtocol.OP_HSET, key, field, value));
    }

    @Override
    public Object hget(String key, String field) {
        return decode(send(CacheProtocol.OP_HGET, CacheProtocol.encodeArgs(CacheProtocol.OP_HGET, key, field)));
    }

    @Override
    public List<Object> hmget(String key, String... fields) {
        return this.<List<Object>>decode(send(CacheProtocol.OP_HMGET, CacheProtocol.encodeArgs(CacheProtocol.OP_HMGET, key, fields)));
    }

    @Override
    public Map<String, Object> hgetAll(String key) {
        return this.<Map<String, Object>>decode(send(CacheProtocol.OP_HGETALL, CacheProtocol.encodeArgs(CacheProtocol.OP_HGETALL, key)));
    }

    @Override
    public void lpush(String key, Object... values) {
        send(CacheProtocol.OP_LPUSH, CacheProtocol.encodeArgs(CacheProtocol.OP_LPUSH, key, values));
    }

    @Override
    public void rpush(String key, Object... values) {
        send(CacheProtocol.OP_RPUSH, CacheProtocol.encodeArgs(CacheProtocol.OP_RPUSH, key, values));
    }

    @Override
    public Object lpop(String key) {
        return decode(send(CacheProtocol.OP_LPOP, CacheProtocol.encodeArgs(CacheProtocol.OP_LPOP, key)));
    }

    @Override
    public Object rpop(String key) {
        return decode(send(CacheProtocol.OP_RPOP, CacheProtocol.encodeArgs(CacheProtocol.OP_RPOP, key)));
    }

    @Override
    public void sadd(String key, Object... members) {
        send(CacheProtocol.OP_SADD, CacheProtocol.encodeArgs(CacheProtocol.OP_SADD, key, members));
    }

    @Override
    public Set<Object> smembers(String key) {
        return this.<Set<Object>>decode(send(CacheProtocol.OP_SMEMBERS, CacheProtocol.encodeArgs(CacheProtocol.OP_SMEMBERS, key)));
    }

    @Override
    public void srem(String key, Object... members) {
        send(CacheProtocol.OP_SREM, CacheProtocol.encodeArgs(CacheProtocol.OP_SREM, key, members));
    }

    @Override
    public void zadd(String key, double score, Object member) {
        send(CacheProtocol.OP_ZADD, CacheProtocol.encodeArgs(CacheProtocol.OP_ZADD, key, score, member));
    }

    @Override
    public List<Object> zrange(String key, long start, long stop) {
        return this.<List<Object>>decode(send(CacheProtocol.OP_ZRANGE, CacheProtocol.encodeArgs(CacheProtocol.OP_ZRANGE, key, start, stop)));
    }

    @Override
    public List<Object> zrangeByScore(String key, double min, double max) {
        return this.<List<Object>>decode(send(CacheProtocol.OP_ZRANGEBYSCORE, CacheProtocol.encodeArgs(CacheProtocol.OP_ZRANGEBYSCORE, key, min, max)));
    }

    @Override
    public void multi() {
        send(CacheProtocol.OP_MULTI, new byte[]{CacheProtocol.OP_MULTI});
    }

    @Override
    public List<Object> exec() {
        return this.<List<Object>>decode(send(CacheProtocol.OP_EXEC, new byte[]{CacheProtocol.OP_EXEC}));
    }

    @Override
    public void discard() {
        send(CacheProtocol.OP_DISCARD, new byte[]{CacheProtocol.OP_DISCARD});
    }

    @Override
    public void watch(String... keys) {
        send(CacheProtocol.OP_WATCH, CacheProtocol.encodeArgs(CacheProtocol.OP_WATCH, (Object) keys));
    }
}
