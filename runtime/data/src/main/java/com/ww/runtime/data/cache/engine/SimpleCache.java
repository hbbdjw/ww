package com.ww.runtime.data.cache.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于 ConcurrentHashMap 的内存缓存实现，支持过期删除及事务。
 *
 * <p>WATCH 局限性说明：WATCH/MULTI/EXEC 的乐观锁基于本进程 versions 表。
 * 跨节点的写（转发到远端属主）不会自动 bump 本进程 versions；网关转发路径
 * 通过 {@link #bumpVersionForRemoteWrite(Object)} 显式同步 bump 本地版本，使同进程 WATCH
 * 仍可检测到 forward 写。但跨进程 WATCH（例如两个 client 直接操作不同节点）无法跨进程感知，
 * 这是分布式 cache 的固有约束，应用层不应在跨节点场景使用 WATCH 乐观锁。
 */
public class SimpleCache<K, V> implements Cache<K, V> {

    private final Map<K, CacheEntry<V>> store = new ConcurrentHashMap<>();
    private final Map<K, Long> versions = new ConcurrentHashMap<>();
    private final ThreadLocal<TxContext<K>> txHolder = new ThreadLocal<>();
    private AofService aofService;
    private java.util.concurrent.ScheduledExecutorService evictionScheduler;
    private final int maxSize;
    /**
     * O(1) LRU 缓存淘汰：基于 {@link LinkedHashMap} access-order 模式。
     * <p>{@code removeEldestEntry} 在 {@code put} 期间自动触发，保证 lruMap 与 store 大小一致。
     * <p>写路径（put/remove/clear）持锁；热路径 {@link #lruTouch}（每次 GET）
     * 使用 {@code tryLock}——锁竞争时跳过触碰直接返回，避免读路径阻塞；
     * 竞争下 LRU 退化为近似淘汰，正确性不受影响。
     */
    private final ReentrantLock lruLock = new ReentrantLock();
    private final LinkedHashMap<K, Boolean> lruMap = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<K, Boolean> eldest) {
            if (size() > maxSize) {
                store.remove(eldest.getKey());
                versions.remove(eldest.getKey());
                return true;
            }
            return false;
        }
    };
    /** 集合操作（hset/lpush/rpush/sadd/zadd/srem）条带锁：防止并发 TOCTOU 创建重复集合或类型冲突 */
    private static final int COLL_LOCK_STRIPES = 64;
    @SuppressWarnings("unchecked")
    private final ReentrantLock[] collLocks = new ReentrantLock[COLL_LOCK_STRIPES];
    {
        for (int i = 0; i < COLL_LOCK_STRIPES; i++) collLocks[i] = new ReentrantLock();
    }
    private ReentrantLock lockFor(Object key) {
        return collLocks[((key.hashCode() & 0x7FFFFFFF) % COLL_LOCK_STRIPES)];
    }
    /** Stream 子系统：独立于 KV store，避免污染 keys() 与 LRU。 */
    private final StreamStore streamStore = new StreamStore();
    private static final int DEFAULT_MAX_SIZE = 100_000;
    /** 缓存击穿防护：per-key 互斥锁，使用 WeakHashMap 自动清理 */
    private final Map<K, Object> loaderLocks = Collections.synchronizedMap(new java.util.WeakHashMap<>());
    /** 缓存穿透防护：空值标记，避免反复查询不存在的 key */
    private static final Object NULL_MARKER = new Object();
    /** 空值缓存默认 TTL（秒），防止永久缓存不存在的 key */
    private static final long NULL_CACHE_TTL_MS = 60_000L;

    public SimpleCache() {
        this(null, DEFAULT_MAX_SIZE);
    }

    public SimpleCache(AofService aofService) {
        this(aofService, DEFAULT_MAX_SIZE);
    }

    public SimpleCache(AofService aofService, int maxSize) {
        this.aofService = aofService;
        this.maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_SIZE;
    }

    /**
     * 设置AofService（用于解决循环依赖：AofService需要Cache引用进行重写）
     */
    public void setAofService(AofService aofService) {
        // 允许设置，但只能设置非null值一次
        if (this.aofService == null && aofService != null) {
            this.aofService = aofService;
        }
    }

    /**
     * 启动后台过期清理。
     * 默认依赖被动 lazy 过期；启用此调度后可降低孤儿条目堆积压力。
     */
    public void startEvictionScheduler(long intervalMillis) {
        if (intervalMillis <= 0) return;
        if (evictionScheduler != null) return;
        evictionScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-evict");
            t.setDaemon(true);
            return t;
        });
        evictionScheduler.scheduleAtFixedRate(this::safeEvict, intervalMillis, intervalMillis,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void safeEvict() {
        try {
            evictExpired();
        } catch (Exception e) {
            // 静默吞异常，避免调度器因单次异常停止
        }
    }

    public void stopEvictionScheduler() {
        if (evictionScheduler != null) {
            evictionScheduler.shutdownNow();
            evictionScheduler = null;
        }
    }

    /**
     * 为已存在的 key 设置剩余 TTL（毫秒）；{@code <=0} 表示永不过期。
     * <p>供迁移导入路径在重建容器（hset/rpush/sadd/zadd 本身不携带 TTL）后恢复
     * 源端剩余 TTL 使用；key 不存在时 no-op。</p>
     */
    public void expire(K key, long ttlMillis) {
        if (key == null) {
            return;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return;
        }
        long expireTime = ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : Long.MAX_VALUE;
        store.replace(key, entry, CacheEntry.withExpireTime(entry.getValue(), expireTime));
        lruPut(key);
    }

    /**
     * 清理当前线程的 ThreadLocal tx 状态，供服务端 handler 线程复用前调用。
     */
    public void clearThreadLocalTx() {
        TxContext<K> tx = txHolder.get();
        if (tx != null) {
            txHolder.remove();
        }
    }

    /**
     * 返回 key 的剩余 TTL（毫秒），永不过期返回 Long.MAX_VALUE，不存在返回 -1，
     * 已过期返回 0。供 OP_LIST_REPLACE 同步剩余 TTL 使用。
     */
    public long remainingTtlMillis(K key) {
        CacheEntry<V> entry = store.get(key);
        if (entry == null) return -1;
        if (entry.isExpired()) return 0;
        if (entry.getExpireTime() == Long.MAX_VALUE) return Long.MAX_VALUE;
        return Math.max(0, entry.getExpireTime() - System.currentTimeMillis());
    }

    /**
     * WATCH 兼容：跨节点 forward 写后 bump 本进程版本号，使同进程 watch 可检测到该写。
     */
    public void bumpVersionForRemoteWrite(K key) {
        bumpVersion(key);
    }

    // ==================== 事务上下文 ====================

    private static class TxContext<K> {
        boolean active;
        final List<QueuedCommand> commands = new ArrayList<>();
        final Set<K> watchedKeys = new HashSet<>();
        final Map<K, Long> watchedVersions = new HashMap<>();
    }

    private static class QueuedCommand {
        private final java.util.function.Supplier<Object> action;
        final Object key;
        final String aofOp;
        final String aofKey;
        final Object[] aofArgs;

        QueuedCommand(java.util.function.Supplier<Object> action) {
            this(action, null, null, null, null);
        }

        QueuedCommand(java.util.function.Supplier<Object> action, Object key, String aofOp, String aofKey, Object[] aofArgs) {
            this.action = action;
            this.key = key;
            this.aofOp = aofOp;
            this.aofKey = aofKey;
            this.aofArgs = aofArgs;
        }

        Object execute() {
            return action.get();
        }
    }

    // ==================== 版本管理 ====================

    private void bumpVersion(K key) {
        versions.merge(key, 1L, Long::sum);
    }

    private long getVersion(K key) {
        return versions.getOrDefault(key, 0L);
    }

    // ==================== AOF 辅助 ====================

    private void logAof(String op, K key, Object... args) {
        if (aofService != null) {
            aofService.logTyped(op, key != null ? key.toString() : "", args);
        }
    }

    // ==================== 事务辅助 ====================

    private void queueOrExec(K key, String aofOp, Object[] aofArgs, java.util.function.Supplier<Object> action) {
        TxContext<K> tx = txHolder.get();
        if (tx != null && tx.active) {
            tx.commands.add(new QueuedCommand(action, key, aofOp, key != null ? key.toString() : "", aofArgs));
        } else {
            action.get();
            bumpVersion(key);
            logAof(aofOp, key, aofArgs);
        }
    }

    private Object queueOrExecReturn(K key, String aofOp, Object[] aofArgs, java.util.function.Supplier<Object> action) {
        TxContext<K> tx = txHolder.get();
        if (tx != null && tx.active) {
            tx.commands.add(new QueuedCommand(action, key, aofOp, key != null ? key.toString() : "", aofArgs));
            return null;
        }
        Object result = action.get();
        if (aofOp != null) {
            bumpVersion(key);
            logAof(aofOp, key, aofArgs);
        }
        return result;
    }

    // ==================== LRU 辅助方法（O(1)；读路径 tryLock 免阻塞） ====================

    /** GET 命中时触碰 LRU：将 key 移到链表尾部（最近使用）。竞争时跳过——淘汰顺序近似即可 */
    private void lruTouch(K key) {
        if (lruLock.tryLock()) {
            try { lruMap.get(key); } finally { lruLock.unlock(); }
        }
    }

    /** PUT 时更新 LRU：加入或移动到尾部，超限由 removeEldestEntry 自动淘汰 */
    private void lruPut(K key) {
        lruLock.lock();
        try { lruMap.put(key, Boolean.TRUE); } finally { lruLock.unlock(); }
    }

    /** REMOVE 时从 LRU 移除 */
    private void lruRemove(K key) {
        lruLock.lock();
        try { lruMap.remove(key); } finally { lruLock.unlock(); }
    }

    /** CLEAR 时清空整个 LRU */
    private void lruClear() {
        lruLock.lock();
        try { lruMap.clear(); } finally { lruLock.unlock(); }
    }

    // ==================== 基础操作 ====================

    @Override
    public void put(K key, V value, long ttlMillis) {
        queueOrExec(key, "PUT_EXPIRED", new Object[]{value, ttlMillis}, () -> {
            store.put(key, new CacheEntry<>(value, ttlMillis));
            lruPut(key);
            return null;
        });
    }

    @Override
    public void put(K key, V value) {
        queueOrExec(key, "PUT", new Object[]{value}, () -> {
            store.put(key, new CacheEntry<>(value, 0));
            lruPut(key);
            return null;
        });
    }

    @Override
    public V get(K key) {
        // 读操作不记录 AOF
        return (V) queueOrExecReturn(key, null, null, () -> doGet(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getWithLoader(K key, java.util.function.Supplier<V> loader, long ttlMillis) {
        // 1. 先尝试快速读取
        V cached = doGet(key);
        if (cached != null) {
            return unwrapNullMarker(cached);
        }
        // 2. 缓存未命中，获取 per-key 互斥锁防止击穿
        Object lock = loaderLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            // 双重检查：等待锁期间可能已被其他线程填充
            cached = doGet(key);
            if (cached != null) {
                return unwrapNullMarker(cached);
            }
            // 3. 执行 loader 加载数据
            V value = loader.get();
            // 4. 写入缓存；null 用空标记替代防止穿透
            if (value == null) {
                store.put(key, new CacheEntry<>((V) NULL_MARKER, NULL_CACHE_TTL_MS));
                lruPut(key);
            } else {
                long ttl = ttlMillis > 0 ? ttlMillis : 0;
                store.put(key, new CacheEntry<>(value, ttl));
                lruPut(key);
            }
            return value;
        }
    }

    /** 解包空值标记：如果缓存值是 NULL_MARKER 则返回 null */
    @SuppressWarnings("unchecked")
    private V unwrapNullMarker(V value) {
        return value == NULL_MARKER ? null : value;
    }

    private V doGet(K key) {
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.remove(key);
            versions.remove(key);
            lruRemove(key);
            return null;
        }
        lruTouch(key);
        V value = entry.getValue();
        return value == NULL_MARKER ? null : value;
    }

    @Override
    public void remove(K key) {
        queueOrExec(key, "REMOVE", new Object[0], () -> {
            store.remove(key);
            versions.remove(key);
            lruRemove(key);
            // 联动清理同名的 stream，保持与 Redis DEL stream 行为一致
            if (key != null) {
                streamStore.removeStream(key.toString());
            }
            return null;
        });
    }

    @Override
    public void clear() {
        TxContext<K> tx = txHolder.get();
        if (tx != null && tx.active) {
            tx.commands.add(new QueuedCommand(() -> {
                store.clear();
                versions.clear();
                lruClear();
                streamStore.clear();
                return null;
            }, null, "CLEAR", "", new Object[0]));
            return;        }
        store.clear();
        versions.clear();
        lruClear();
        streamStore.clear();
        logAof("CLEAR", null);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public java.util.Set<K> keys() {
        return Collections.unmodifiableSet(new java.util.HashSet<>(store.keySet()));
    }

    // ==================== Hash 操作 ====================

    @Override
    @SuppressWarnings("unchecked")
    public void hset(K key, String field, V value) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            queueOrExec(key, "HSET", new Object[]{field, value}, () -> {
                CacheEntry<V> entry = store.get(key);
                Map<String, V> hash;
                if (entry == null || entry.isExpired()) {
                    hash = new ConcurrentHashMap<>();
                    store.put(key, new CacheEntry<>((V) hash, 0));
                    lruPut(key);
                } else {
                    V val = entry.getValue();
                    if (val instanceof Map) {
                        hash = (Map<String, V>) val;
                    } else {
                        hash = new ConcurrentHashMap<>();
                        store.put(key, new CacheEntry<>((V) hash, 0));
                        lruPut(key);
                    }
                }
                hash.put(field, value);
                return null;
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V hget(K key, String field) {
        return (V) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (val instanceof Map) {
                return ((Map<String, V>) val).get(field);
            }
            return null;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<V> hmget(K key, String... fields) {
        return (List<V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (!(val instanceof Map)) {
                List<V> result = new ArrayList<>(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    result.add(null);
                }
                return result;
            }
            Map<String, V> hash = (Map<String, V>) val;
            List<V> result = new ArrayList<>(fields.length);
            for (String field : fields) {
                result.add(hash.get(field));
            }
            return result;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, V> hgetAll(K key) {
        return (Map<String, V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (val instanceof Map) {
                return new ConcurrentHashMap<>((Map<String, V>) val);
            }
            return Collections.emptyMap();
        });
    }

    // ==================== List 操作 ====================

    @Override
    @SuppressWarnings("unchecked")
    public void lpush(K key, V... values) {
        queueOrExec(key, "LPUSH", new Object[]{values}, () -> {
            doLpush(key, values);
            return null;
        });
    }

    /**
     * LPUSH 内部实现：持条带锁，原子完成集合创建与写入，避免并发 TOCTOU 竞态。
     */
    @SuppressWarnings("unchecked")
    private void doLpush(K key, V... values) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            ConcurrentLinkedDeque<V> deque;
            if (entry == null || entry.isExpired()) {
                deque = new ConcurrentLinkedDeque<>();
                store.put(key, new CacheEntry<>((V) deque, 0));
                lruPut(key);
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentLinkedDeque) {
                    deque = (ConcurrentLinkedDeque<V>) val;
                } else {
                    deque = new ConcurrentLinkedDeque<>();
                    store.put(key, new CacheEntry<>((V) deque, 0));
                    lruPut(key);
                }
            }
            for (V value : values) {
                deque.addFirst(value);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void rpush(K key, V... values) {
        queueOrExec(key, "RPUSH", new Object[]{values}, () -> {
            doRpush(key, values);
            return null;
        });
    }

    /**
     * RPUSH 内部实现：持条带锁，消除 TOCTOU 竞态。
     * @see #doLpush(Object, Object[])
     */
    @SuppressWarnings("unchecked")
    private void doRpush(K key, V... values) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            ConcurrentLinkedDeque<V> deque;
            if (entry == null || entry.isExpired()) {
                deque = new ConcurrentLinkedDeque<>();
                store.put(key, new CacheEntry<>((V) deque, 0));
                lruPut(key);
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentLinkedDeque) {
                    deque = (ConcurrentLinkedDeque<V>) val;
                } else {
                    deque = new ConcurrentLinkedDeque<>();
                    store.put(key, new CacheEntry<>((V) deque, 0));
                    lruPut(key);
                }
            }
            for (V value : values) {
                deque.addLast(value);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V lpop(K key) {
        return (V) queueOrExecReturn(key, "LPOP", new Object[0], () -> {
            V val = doGet(key);
            if (!(val instanceof ConcurrentLinkedDeque)) {
                return null;
            }
            ConcurrentLinkedDeque<V> deque = (ConcurrentLinkedDeque<V>) val;
            return deque.pollFirst();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public V rpop(K key) {
        return (V) queueOrExecReturn(key, "RPOP", new Object[0], () -> {
            V val = doGet(key);
            if (!(val instanceof ConcurrentLinkedDeque)) {
                return null;
            }
            ConcurrentLinkedDeque<V> deque = (ConcurrentLinkedDeque<V>) val;
            return deque.pollLast();
        });
    }

    // ==================== Set 操作 ====================

    @Override
    @SuppressWarnings("unchecked")
    public void sadd(K key, V... members) {
        queueOrExec(key, "SADD", new Object[]{members}, () -> {
            doSadd(key, members);
            return null;
        });
    }

    /**
     * SADD 内部实现：持条带锁，消除 TOCTOU 竞态。
     * @see #doLpush(Object, Object[])
     */
    @SuppressWarnings("unchecked")
    private void doSadd(K key, V... members) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            java.util.Set<V> set;
            if (entry == null || entry.isExpired()) {
                set = ConcurrentHashMap.newKeySet();
                store.put(key, new CacheEntry<>((V) set, 0));
                lruPut(key);
            } else {
                V val = entry.getValue();
                if (val instanceof java.util.Set) {
                    set = (java.util.Set<V>) val;
                } else {
                    set = ConcurrentHashMap.newKeySet();
                    store.put(key, new CacheEntry<>((V) set, 0));
                    lruPut(key);
                }
            }
            for (V member : members) {
                set.add(member);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public java.util.Set<V> smembers(K key) {
        return (java.util.Set<V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (val instanceof java.util.Set) {
                return new java.util.HashSet<>((java.util.Set<V>) val);
            }
            return Collections.emptySet();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void srem(K key, V... members) {
        queueOrExec(key, "SREM", new Object[]{members}, () -> {
            V val = doGet(key);
            if (!(val instanceof java.util.Set)) {
                return null;
            }
            java.util.Set<V> set = (java.util.Set<V>) val;
            for (V member : members) {
                set.remove(member);
            }
            return null;
        });
    }

    // ==================== Sorted Set 操作 ====================

    @Override
    @SuppressWarnings("unchecked")
    public void zadd(K key, double score, V member) {
        queueOrExec(key, "ZADD", new Object[]{score, member}, () -> {
            doZadd(key, score, member);
            return null;
        });
    }

    /** ZSET member→score 反向索引（按 key 维度），用于 O(log n) 更新已有 member 的分数 */
    private final ConcurrentHashMap<K, ConcurrentHashMap<V, Double>> zsetMemberIndex = new ConcurrentHashMap<>();

    /**
     * ZADD 内部实现：持条带锁，消除 TOCTOU 竞态。
     * <p>分数桶使用线程安全的 {@code ConcurrentHashMap.newKeySet()}，
     * 并通过 {@link #zsetMemberIndex} 反向索引实现 O(1) 的 member 定位。
     */
    @SuppressWarnings("unchecked")
    private void doZadd(K key, double score, V member) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            ConcurrentSkipListMap<Double, Set<V>> zset;
            ConcurrentHashMap<V, Double> memberIdx;
            if (entry == null || entry.isExpired()) {
                zset = new ConcurrentSkipListMap<>();
                store.put(key, new CacheEntry<>((V) zset, 0));
                memberIdx = new ConcurrentHashMap<>();
                zsetMemberIndex.put(key, memberIdx);
                lruPut(key);
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentSkipListMap) {
                    zset = (ConcurrentSkipListMap<Double, Set<V>>) val;
                    memberIdx = zsetMemberIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
                } else {
                    zset = new ConcurrentSkipListMap<>();
                    store.put(key, new CacheEntry<>((V) zset, 0));
                    memberIdx = new ConcurrentHashMap<>();
                    zsetMemberIndex.put(key, memberIdx);
                    lruPut(key);
                }
            }
            // O(1) 定位旧分数桶并移除
            Double oldScore = memberIdx.put(member, score);
            if (oldScore != null) {
                Set<V> bucket = zset.get(oldScore);
                if (bucket != null) {
                    bucket.remove(member);
                    if (bucket.isEmpty()) zset.remove(oldScore);
                }
            }
            // 使用线程安全的 Set（ConcurrentHashMap.newKeySet）
            zset.computeIfAbsent(score, k -> ConcurrentHashMap.newKeySet()).add(member);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<V> zrange(K key, long start, long stop) {
        return (List<V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (!(val instanceof ConcurrentSkipListMap)) {
                return Collections.emptyList();
            }
            ConcurrentSkipListMap<Double, java.util.Set<V>> zset = (ConcurrentSkipListMap<Double, java.util.Set<V>>) val;
            // 先算总数（负下标需要），再只收集 [s, e] 区间，避免全量物化
            int totalSize = 0;
            for (java.util.Set<V> members : zset.values()) {
                totalSize += members.size();
            }
            if (totalSize == 0) {
                return Collections.emptyList();
            }
            int s = start < 0 ? totalSize + (int) start : (int) start;
            int e = stop < 0 ? totalSize + (int) stop : (int) stop;
            if (s < 0) s = 0;
            if (e >= totalSize) e = totalSize - 1;
            if (s > e) {
                return Collections.emptyList();
            }
            List<V> result = new ArrayList<>(e - s + 1);
            int idx = 0;
            for (java.util.Set<V> members : zset.values()) {
                for (V member : members) {
                    if (idx >= s && idx <= e) {
                        result.add(member);
                    }
                    if (idx > e) {
                        return result;
                    }
                    idx++;
                }
            }
            return result;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<V> zrangeByScore(K key, double min, double max) {
        return (List<V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (!(val instanceof ConcurrentSkipListMap)) {
                return Collections.emptyList();
            }
            ConcurrentSkipListMap<Double, java.util.Set<V>> zset = (ConcurrentSkipListMap<Double, java.util.Set<V>>) val;
            List<V> result = new ArrayList<>();
            for (java.util.Set<V> members : zset.subMap(min, true, max, true).values()) {
                result.addAll(members);
            }
            return result;
        });
    }

    // ==================== 事务操作 ====================

    @Override
    public void multi() {
        // 清理可能泄漏的旧事务上下文，防止上次未 discard 的事务残留
        txHolder.remove();
        TxContext<K> tx = new TxContext<>();
        tx.active = true;
        txHolder.set(tx);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> exec() {
        TxContext<K> tx = txHolder.get();
        if (tx == null) {
            return Collections.emptyList();
        }
        // 无论成功与否，清理 ThreadLocal 防止泄漏
        txHolder.remove();
        if (!tx.active) {
            return Collections.emptyList();
        }
        // 对 watched key 加 per-key 条带锁，防止并发写破坏乐观锁检测，
        // 不相关 key 的事务可并行执行；按 identityHashCode 排序加锁，避免死锁。
        List<K> keysToLock = new ArrayList<>(tx.watchedKeys);
        keysToLock.sort((a, b) -> Integer.compare(System.identityHashCode(a), System.identityHashCode(b)));
        List<ReentrantLock> heldLocks = new ArrayList<>(keysToLock.size());
        for (K k : keysToLock) {
            ReentrantLock lock = lockFor(k);
            lock.lock();
            heldLocks.add(lock);
        }
        try {
            // 检查 WATCH 键是否被修改
            for (K watchedKey : tx.watchedKeys) {
                long currentVersion = getVersion(watchedKey);
                long watchedVersion = tx.watchedVersions.getOrDefault(watchedKey, -1L);
                if (currentVersion != watchedVersion) {
                    return null;
                }
            }
            // 执行所有命令，并递增版本
            List<Object> results = new ArrayList<>(tx.commands.size());
            for (QueuedCommand cmd : tx.commands) {
                Object result = cmd.execute();
                results.add(result);
                if (cmd.key != null) {
                    bumpVersion((K) cmd.key);
                }
                // 事务内的写操作记录 AOF（类型化参数，与非事务路径字节级同构）
                if (cmd.aofOp != null && aofService != null) {
                    aofService.logTyped(cmd.aofOp, cmd.aofKey, cmd.aofArgs);
                }
            }
            return results;
        } finally {
            for (int i = heldLocks.size() - 1; i >= 0; i--) {
                heldLocks.get(i).unlock();
            }
        }
    }

    @Override
    public void discard() {
        txHolder.remove();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void watch(K... keys) {
        // txHolder 是 ThreadLocal，无并发访问风险；版本快照读取无需加锁
        // （exec 通过 per-key 锁保证 WATCH→EXEC 期间的互斥）。
        TxContext<K> tx = txHolder.get();
        if (tx == null) {
            tx = new TxContext<>();
            txHolder.set(tx);
        }
        for (K key : keys) {
            tx.watchedKeys.add(key);
            tx.watchedVersions.put(key, getVersion(key));
        }
    }

    /**
     * 主动清理所有已过期的条目
     *
     * @return 清理数量
     */
    public int evictExpired() {
        int count = 0;
        Iterator<Map.Entry<K, CacheEntry<V>>> it = store.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<K, CacheEntry<V>> entry = it.next();
            if (entry.getValue().isExpired()) {
                it.remove();
                versions.remove(entry.getKey());
                lruRemove(entry.getKey());
                count++;
            }
        }
        return count;
    }

    /**
     * 用给定列表替换指定 key 的整个列表状态（用于 lpop/rpop 的副本状态同步）。
     * 保留剩余 TTL，避免替换后 TTL=0 永驻。
     *
     * @param key            缓存键
     * @param items          pop 后剩余列表
     * @param remainingTtlMillis 剩余 TTL；<=0 表示永不过期；Long.MAX_VALUE 同意为永不过期；-1 表示 key 不存在
     */
    @SuppressWarnings("unchecked")
    public void listReplace(K key, List<V> items, long remainingTtlMillis) {
        long now = System.currentTimeMillis();
        long expireTime;
        if (remainingTtlMillis == Long.MAX_VALUE || remainingTtlMillis < 0) {
            expireTime = Long.MAX_VALUE;
        } else {
            expireTime = now + Math.max(0, remainingTtlMillis);
        }
        ConcurrentLinkedDeque<V> deque = new ConcurrentLinkedDeque<>(items);
        // CacheEntry 在构造时计算 expireTime = now + ttl；为精确传递剩余 TTL，
        // 这里用反射式直接构造一个过期时间已知的 CacheEntry。
        CacheEntry<V> entry = CacheEntry.withExpireTime((V) deque, expireTime);
        store.put(key, entry);
        lruPut(key);
    }

    /**
     * 兼容旧调用：保留 TTL=0（永不过期）。
     *
     * @deprecated 请使用 {@link #listReplace(Object, List, long)} 显式传递剩余 TTL。
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public void listReplace(K key, List<V> items) {
        listReplace(key, items, Long.MAX_VALUE);
    }

    // ==================== Stream 子系统 ====================

    /**
     * 获取 Stream 存储。供数据面协议层通过 instanceof 判断后调用，
     * 与 {@link #listReplace(Object, List, long)} 同样走"扩展方法"路径而非 Cache 接口。
     */
    public StreamStore getStreamStore() {
        return streamStore;
    }

    /**
     * XADD 包装：调用 streamStore 后记录 AOF（含最终生成的 ID，保证回放后 ID 一致）。
     */
    @SuppressWarnings("unchecked")
    public StreamId xadd(String key, StreamId idReq, Map<String, String> fields, XAddOptions options) {
        StreamId finalId = streamStore.xadd(key, idReq, fields, options);
        if (finalId != null && key != null) {
            logAof("XADD", (K) key, finalId.getMsTime(), finalId.getSeq(), fields);
        }
        return finalId;
    }

    /**
     * XDEL 包装：记录 AOF，参数为扁平化的 [ms1, seq1, ms2, seq2, ...] Long 列表。
     */
    @SuppressWarnings("unchecked")
    public long xdel(String key, StreamId... ids) {
        long removed = streamStore.xdel(key, ids);
        if (removed > 0 && key != null) {
            List<Long> flat = new ArrayList<>();
            for (StreamId id : ids) {
                if (id == null) continue;
                flat.add(id.getMsTime());
                flat.add(id.getSeq());
            }
            logAof("XDEL", (K) key, flat);
        }
        return removed;
    }

    /**
     * XTRIM 包装：记录 AOF，参数为 maxLen 与 approximate。
     */
    @SuppressWarnings("unchecked")
    public long xtrim(String key, long maxLen, boolean approximate) {
        long removed = streamStore.xtrim(key, maxLen, approximate);
        if (removed > 0 && key != null) {
            logAof("XTRIM", (K) key, maxLen, approximate);
        }
        return removed;
    }

    /**
     * AOF 回放专用：直接以指定 ID 追加消息，不记录 AOF、不触发裁剪。
     */
    public void xaddRaw(String key, StreamId id, Map<String, String> fields) {
        streamStore.xaddRaw(key, id, fields);
    }

    /**
     * XGROUP CREATE 包装：调用 streamStore 后记录 AOF（含 startId，回放后组起始位置一致）。
     */
    @SuppressWarnings("unchecked")
    public boolean xgroupCreate(String key, String groupName, StreamId startId, boolean mkstream) {
        boolean created = streamStore.xgroupCreate(key, groupName, startId, mkstream);
        if (created && key != null) {
            logAof("XGROUP_CREATE", (K) key, groupName,
                    startId != null ? startId.getMsTime() : 0L,
                    startId != null ? startId.getSeq() : 0L,
                    mkstream);
        }
        return created;
    }

    /**
     * XGROUP CREATE 回放专用：不记录 AOF。
     */
    public void xgroupCreateRaw(String key, String groupName, StreamId startId, boolean mkstream) {
        streamStore.xgroupCreate(key, groupName, startId, mkstream);
    }

    /**
     * XACK 包装：记录 AOF，参数为扁平化的 [ms1, seq1, ms2, seq2, ...] Long 列表。
     */
    @SuppressWarnings("unchecked")
    public long xack(String key, String groupName, StreamId... ids) {
        long acked = streamStore.xack(key, groupName, ids);
        if (acked > 0 && key != null) {
            List<Long> flat = new ArrayList<>();
            for (StreamId id : ids) {
                if (id == null) continue;
                flat.add(id.getMsTime());
                flat.add(id.getSeq());
            }
            logAof("XACK", (K) key, groupName, flat);
        }
        return acked;
    }

    /**
     * XACK 回放专用：直接调 streamStore，不记录 AOF。
     */
    public void xackRaw(String key, String groupName, List<Long> flatIds) {
        if (flatIds == null || flatIds.isEmpty()) return;
        StreamId[] ids = new StreamId[flatIds.size() / 2];
        for (int i = 0; i + 1 < flatIds.size(); i += 2) {
            ids[i / 2] = new StreamId(flatIds.get(i), flatIds.get(i + 1));
        }
        streamStore.xack(key, groupName, ids);
    }

    // ==================== 消费者组状态持久化 ====================

    /**
     * XGROUP DESTROY 包装：记录 AOF。重启后回放可恢复"组已删除"语义。
     */
    @SuppressWarnings("unchecked")
    public boolean xgroupDestroy(String key, String groupName) {
        boolean destroyed = streamStore.xgroupDestroy(key, groupName);
        if (destroyed && key != null) {
            logAof("XGROUP_DESTROY", (K) key, groupName);
        }
        return destroyed;
    }

    /**
     * XGROUP CREATECONSUMER 包装：记录 AOF。
     */
    @SuppressWarnings("unchecked")
    public int xgroupCreateConsumer(String key, String groupName, String consumerName) {
        int created = streamStore.xgroupCreateConsumer(key, groupName, consumerName);
        if (created > 0 && key != null) {
            logAof("XGROUP_CREATECONSUMER", (K) key, groupName, consumerName);
        }
        return created;
    }

    /**
     * XGROUP DELCONSUMER 包装：记录 AOF。
     */
    @SuppressWarnings("unchecked")
    public long xgroupDelConsumer(String key, String groupName, String consumerName) {
        long removed = streamStore.xgroupDelConsumer(key, groupName, consumerName);
        if (removed >= 0 && key != null) {
            // 即使 removed=0（consumer 不存在），也记录以保持幂等回放语义
            logAof("XGROUP_DELCONSUMER", (K) key, groupName, consumerName);
        }
        return removed;
    }

    /**
     * XCLAIM 包装：调用 streamStore 后记录 AOF（仅当实际 claim 数 > 0）。
     * <p>AOF 参数：groupName, newConsumerName, [ms1, seq1, ms2, seq2, ...]。
     * 回放时调 {@link #xclaimRaw} 强制转移所有权，不做 idle 检查。
     */
    @SuppressWarnings("unchecked")
    public List<PelEntry> xclaim(String key, String groupName, String consumerName,
                                  long minIdleMs, StreamId... ids) {
        List<PelEntry> claimed = streamStore.xclaim(key, groupName, consumerName, minIdleMs, ids);
        if (claimed != null && !claimed.isEmpty() && key != null) {
            List<Long> flat = new ArrayList<>();
            for (PelEntry p : claimed) {
                flat.add(p.getId().getMsTime());
                flat.add(p.getId().getSeq());
            }
            logAof("XCLAIM", (K) key, groupName, consumerName, flat);
        }
        return claimed;
    }

    /**
     * XAUTOCLAIM 包装：调用 streamStore 后记录 AOF。
     * <p>与 XCLAIM 共用 AOF op {@code XCLAIM}（副作用相同：转移所有权 + deliveryCount+1）。
     */
    @SuppressWarnings("unchecked")
    public XAutoClaimResult xautoclaim(String key, String groupName, String consumerName,
                                        long minIdleMs, StreamId start, long count) {
        XAutoClaimResult result = streamStore.xautoclaim(key, groupName, consumerName,
                minIdleMs, start, count);
        if (result != null && result.getClaimed() != null && !result.getClaimed().isEmpty() && key != null) {
            List<Long> flat = new ArrayList<>();
            for (PelEntry p : result.getClaimed()) {
                flat.add(p.getId().getMsTime());
                flat.add(p.getId().getSeq());
            }
            logAof("XCLAIM", (K) key, groupName, consumerName, flat);
        }
        return result;
    }

    /**
     * XREADGROUP 包装：调用 streamStore 后，若为 {@code ">"} 模式且投递数 > 0，
     * 记录 XDELIVER AOF（含 lastDeliveredId 与投递的 ID 列表）。
     * <p>这是消费者组状态持久化的核心——保证重启后 lastDeliveredId 与 PEL 一致。
     * <p>注意：PEL 模式（readNew=false）不推进 lastDeliveredId、不新增 PEL，
     * 仅触发重投递更新 PEL 元数据（claimTo），故也按 XCLAIM 语义记录 AOF。
     *
     * @return 投递的 StreamEntry 列表
     */
    @SuppressWarnings("unchecked")
    public List<StreamEntry> xreadGroup(String key, String groupName, String consumerName,
                                         boolean readNew, StreamId fromId, long count, long blockMs)
            throws InterruptedException {
        List<StreamEntry> delivered = streamStore.xreadGroup(key, groupName, consumerName,
                readNew, fromId, count, blockMs);
        if (delivered == null || delivered.isEmpty() || key == null) {
            return delivered;
        }
        if (readNew) {
            // ">" 模式：记录 XDELIVER，包含 lastDeliveredId 与所有投递 ID
            StreamId lastDelivered = null;
            // 取投递的最后一条 ID 作为 lastDeliveredId（StreamStore 内部已 advance）
            // 通过 xinfoGroups 取最新 lastDeliveredId 避免重复计算
            StreamGroupsInfo info = streamStore.xinfoGroups(key);
            if (info != null && info.getGroups() != null) {
                for (StreamGroupsInfo.GroupSummary g : info.getGroups()) {
                    if (groupName.equals(g.getName())) {
                        lastDelivered = g.getLastDeliveredId();
                        break;
                    }
                }
            }
            List<Long> flat = new ArrayList<>();
            for (StreamEntry e : delivered) {
                flat.add(e.getId().getMsTime());
                flat.add(e.getId().getSeq());
            }
            logAof("XDELIVER", (K) key, groupName, consumerName,
                    lastDelivered != null ? lastDelivered.getMsTime() : 0L,
                    lastDelivered != null ? lastDelivered.getSeq() : 0L,
                    flat);
        } else {
            // PEL 模式：重投递会 claimTo 更新 consumer 与 deliveryCount，
            // 按 XCLAIM 语义记录（newConsumer 就是当前 consumer）
            List<Long> flat = new ArrayList<>();
            for (StreamEntry e : delivered) {
                flat.add(e.getId().getMsTime());
                flat.add(e.getId().getSeq());
            }
            logAof("XCLAIM", (K) key, groupName, consumerName, flat);
        }
        return delivered;
    }

    // ==================== 消费者组 AOF 回放专用 Raw 方法 ====================

    /** XGROUP DESTROY 回放专用。 */
    public void xgroupDestroyRaw(String key, String groupName) {
        streamStore.xgroupDestroy(key, groupName);
    }

    /** XGROUP CREATECONSUMER 回放专用。 */
    public void xgroupCreateConsumerRaw(String key, String groupName, String consumerName) {
        streamStore.xgroupCreateConsumer(key, groupName, consumerName);
    }

    /** XGROUP DELCONSUMER 回放专用。 */
    public void xgroupDelConsumerRaw(String key, String groupName, String consumerName) {
        streamStore.xgroupDelConsumer(key, groupName, consumerName);
    }

    /** XCLAIM / XAUTOCLAIM 回放专用：直接调 streamStore.xclaimRaw。 */
    public void xclaimRaw(String key, String groupName, String consumerName, List<Long> flatIds) {
        if (flatIds == null || flatIds.isEmpty()) return;
        List<StreamId> ids = new ArrayList<>();
        for (int i = 0; i + 1 < flatIds.size(); i += 2) {
            ids.add(new StreamId(flatIds.get(i), flatIds.get(i + 1)));
        }
        streamStore.xclaimRaw(key, groupName, consumerName, ids);
    }

    /** XDELIVER 回放专用：参数为扁平化 ID 列表 + lastDeliveredId。 */
    public void xdeliverRaw(String key, String groupName, String consumerName,
                            long lastMs, long lastSeq, List<Long> flatIds) {
        List<StreamId> ids = new ArrayList<>();
        if (flatIds != null) {
            for (int i = 0; i + 1 < flatIds.size(); i += 2) {
                ids.add(new StreamId(flatIds.get(i), flatIds.get(i + 1)));
            }
        }
        streamStore.xdeliverRaw(key, groupName, consumerName, new StreamId(lastMs, lastSeq), ids);
    }

    // ==================== AOF 重写辅助方法 ====================

    /**
     * 获取所有普通KV key（非容器类型：String/Number/Boolean/自定义对象等）
     */
    @SuppressWarnings("unchecked")
    public List<Map.Entry<K, CacheEntry<V>>> getAllKeyValueEntries() {
        List<Map.Entry<K, CacheEntry<V>>> result = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<V>> e : store.entrySet()) {
            V val = e.getValue().getValue();
            if (val != null && !(val instanceof Map) && !(val instanceof ConcurrentLinkedDeque)
                    && !(val instanceof Set) && !(val instanceof ConcurrentSkipListMap)) {
                result.add(e);
            }
        }
        return result;
    }

    /**
     * 获取所有Hash类型的key
     */
    @SuppressWarnings("unchecked")
    public List<Map.Entry<K, Map<String, V>>> getAllHashEntries() {
        List<Map.Entry<K, Map<String, V>>> result = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<V>> e : store.entrySet()) {
            V val = e.getValue().getValue();
            // ZSet 的实现类 ConcurrentSkipListMap 也是 Map，必须排除，
            // 否则 AOF 重写会把 score 桶当 Hash field 写出毒记录
            if (val instanceof Map && !(val instanceof ConcurrentSkipListMap)) {
                result.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), (Map<String, V>) val));
            }
        }
        return result;
    }

    /**
     * 获取所有List类型的key
     */
    @SuppressWarnings("unchecked")
    public List<Map.Entry<K, ConcurrentLinkedDeque<V>>> getAllListEntries() {
        List<Map.Entry<K, ConcurrentLinkedDeque<V>>> result = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<V>> e : store.entrySet()) {
            V val = e.getValue().getValue();
            if (val instanceof ConcurrentLinkedDeque) {
                result.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), (ConcurrentLinkedDeque<V>) val));
            }
        }
        return result;
    }

    /**
     * 获取所有Set类型的key
     */
    @SuppressWarnings("unchecked")
    public List<Map.Entry<K, Set<V>>> getAllSetEntries() {
        List<Map.Entry<K, Set<V>>> result = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<V>> e : store.entrySet()) {
            V val = e.getValue().getValue();
            if (val instanceof Set) {
                result.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), (Set<V>) val));
            }
        }
        return result;
    }

    /**
     * 获取所有ZSet类型的key
     */
    @SuppressWarnings("unchecked")
    public List<Map.Entry<K, ConcurrentSkipListMap<Double, java.util.Set<V>>>> getAllZSetEntries() {
        List<Map.Entry<K, ConcurrentSkipListMap<Double, java.util.Set<V>>>> result = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<V>> e : store.entrySet()) {
            V val = e.getValue().getValue();
            if (val instanceof ConcurrentSkipListMap) {
                result.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(),
                        (ConcurrentSkipListMap<Double, java.util.Set<V>>) val));
            }
        }
        return result;
    }
}
