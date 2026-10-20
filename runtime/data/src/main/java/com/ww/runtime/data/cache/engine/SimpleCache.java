package com.ww.runtime.data.cache.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(SimpleCache.class);

    private final Map<K, CacheEntry<V>> store = new ConcurrentHashMap<>();
    private final Map<K, Long> versions = new ConcurrentHashMap<>();
    private final ThreadLocal<TxContext<K>> txHolder = new ThreadLocal<>();
    private AofService aofService;
    private java.util.concurrent.ScheduledExecutorService evictionScheduler;
    private final int maxSize;
    /** 近似内存水位上限（字节）；0 表示不启用字节维度淘汰。 */
    private final long maxMemoryBytes;
    /** 每 key 的近似字节数（含 key/容器节点开销的粗估）。 */
    private final Map<K, Long> entryBytes = new ConcurrentHashMap<>();
    private final AtomicLong estimatedBytes = new AtomicLong();
    private final AtomicLong byteEvictions = new AtomicLong();
    /** 全量 key 索引（含 Stream key），供 SCAN/管理面 O(logN+count) 遍历。 */
    private final ConcurrentSkipListSet<String> keyIndex = new ConcurrentSkipListSet<>();
    private final CacheMetrics metrics = new CacheMetrics();
    private static final long SIZE_BASE = 64L;
    private static final int SIZE_DEPTH_LIMIT = 8;
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
                if (removeStoreEntry(eldest.getKey())) {
                    metrics.eviction();
                    // C16：容量淘汰告警计数（限频告警，默认上限行为不变）
                    long evictions = lruEvictions.incrementAndGet();
                    long now = System.currentTimeMillis();
                    if (now - lastLruEvictWarnAt > 60_000L) {
                        lastLruEvictWarnAt = now;
                        LOG.warn("C16 缓存容量淘汰：maxSize={}，累计驱逐 {} 次", maxSize, evictions);
                    }
                }
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
    private final StreamStore streamStore;
    private static final int DEFAULT_MAX_SIZE = 100_000;
    /** 缓存击穿防护：per-key 互斥锁（C42：ConcurrentHashMap 替代 WeakHashMap——弱键锁对象可能被回收导致击穿保护退化） */
    private final Map<K, Object> loaderLocks = new ConcurrentHashMap<>();
    /** 缓存穿透防护：空值标记，避免反复查询不存在的 key */
    private static final Object NULL_MARKER = new Object();
    /** 空值缓存默认 TTL（秒），防止永久缓存不存在的 key */
    private static final long NULL_CACHE_TTL_MS = 60_000L;
    /** C24 日志模式：类型冲突静默覆盖计数（观察期后转 WRONGTYPE 拒绝） */
    private final AtomicLong wrongTypeOverwrites = new AtomicLong();
    /** C16：LRU 容量淘汰计数（超限告警载体） */
    private final AtomicLong lruEvictions = new AtomicLong();
    private volatile long lastLruEvictWarnAt = 0L;

    public SimpleCache() {
        this(null, DEFAULT_MAX_SIZE);
    }

    public SimpleCache(AofService aofService) {
        this(aofService, DEFAULT_MAX_SIZE);
    }

    public SimpleCache(AofService aofService, int maxSize) {
        this(aofService, maxSize, new StreamStore());
    }

    /**
     * C16 第一步：Stream 子系统上限可配置注入（maxLen/pel/maxStreams &lt;=0 走引擎默认或不限）。
     */
    public SimpleCache(AofService aofService, int maxSize, StreamStore streamStore) {
        this(aofService, maxSize, streamStore, 0L);
    }

    /**
     * @param maxMemoryBytes 近似内存水位上限（字节）；&lt;=0 表示不启用字节维度淘汰。
     */
    public SimpleCache(AofService aofService, int maxSize, StreamStore streamStore, long maxMemoryBytes) {
        this.aofService = aofService;
        this.maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_SIZE;
        this.streamStore = streamStore != null ? streamStore : new StreamStore();
        this.maxMemoryBytes = Math.max(0L, maxMemoryBytes);
    }

    // ==================== 类型标记 / key 索引 / 近似内存 ====================

    private void indexKey(K key) {
        if (key != null) {
            keyIndex.add(key.toString());
        }
    }

    /** 仅当 KV 与 Stream 都不存在时从索引摘除，避免误删同名 Stream key。 */
    private void unindexKey(K key) {
        if (key == null) {
            return;
        }
        String s = key.toString();
        if (!store.containsKey(key) && !streamStore.hasStream(s)) {
            keyIndex.remove(s);
        }
    }

    static long estimateStringBytes(String s) {
        return s == null ? 16L : 40L + s.length() * 2L;
    }

    static long estimateHashEntry(String field, Object value) {
        return 48L + estimateStringBytes(field) + estimateValueBytes(value, 0);
    }

    static long estimateListItem(Object value) {
        return 32L + estimateValueBytes(value, 0);
    }

    static long estimateSetMember(Object member) {
        return 48L + estimateValueBytes(member, 0);
    }

    static long estimateZsetMember(Object member) {
        return 64L + estimateValueBytes(member, 0);
    }

    /**
     * 近似值大小：容器递归求和，未知 Serializable 退化为 Codec 编码长度。
     * 仅用于内存水位，不要求精确；嵌套深度超过上限后按 64 字节粗估。
     */
    static long estimateValueBytes(Object value) {
        return estimateValueBytes(value, 0);
    }

    private static long estimateValueBytes(Object value, int depth) {
        if (value == null || value == NULL_MARKER) {
            return 16L;
        }
        if (value instanceof String s) {
            return estimateStringBytes(s);
        }
        if (value instanceof Number || value instanceof Boolean) {
            return 24L;
        }
        if (depth >= SIZE_DEPTH_LIMIT) {
            return SIZE_BASE;
        }
        if (value instanceof ConcurrentSkipListMap<?, ?> zset) {
            long total = SIZE_BASE;
            for (Map.Entry<?, ?> bucket : zset.entrySet()) {
                total += 32L;
                if (bucket.getValue() instanceof Set<?> members) {
                    for (Object member : members) {
                        total += estimateZsetMember(member);
                    }
                }
            }
            return total;
        }
        if (value instanceof HashValue<?> hash) {
            long total = SIZE_BASE;
            for (Map.Entry<?, ?> e : hash.entrySet()) {
                total += 48L + estimateStringBytes(String.valueOf(e.getKey()))
                        + estimateValueBytes(e.getValue(), depth + 1);
            }
            return total;
        }
        if (value instanceof ConcurrentLinkedDeque<?> deque) {
            long total = SIZE_BASE;
            for (Object item : deque) {
                total += 32L + estimateValueBytes(item, depth + 1);
            }
            return total;
        }
        if (value instanceof Set<?> set) {
            long total = SIZE_BASE;
            for (Object member : set) {
                total += 48L + estimateValueBytes(member, depth + 1);
            }
            return total;
        }
        if (value instanceof Map<?, ?> map) {
            long total = SIZE_BASE;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                total += 48L + estimateStringBytes(String.valueOf(e.getKey()))
                        + estimateValueBytes(e.getValue(), depth + 1);
            }
            return total;
        }
        if (value instanceof List<?> list) {
            long total = SIZE_BASE;
            for (Object item : list) {
                total += 32L + estimateValueBytes(item, depth + 1);
            }
            return total;
        }
        if (value instanceof java.io.Serializable) {
            try {
                return SIZE_BASE + Codec.encodeValue(value).length;
            } catch (RuntimeException e) {
                return 128L;
            }
        }
        return 128L;
    }

    private void setEntryBytes(K key, long bytes) {
        long[] delta = {0L};
        entryBytes.compute(key, (k, old) -> {
            long base = old == null ? 0L : old;
            long next = Math.max(0L, bytes);
            delta[0] = next - base;
            return next;
        });
        if (delta[0] != 0L) {
            estimatedBytes.addAndGet(delta[0]);
        }
    }

    private void addEntryBytes(K key, long delta) {
        if (delta == 0L) {
            return;
        }
        long[] actual = {0L};
        entryBytes.compute(key, (k, old) -> {
            long base = old == null ? 0L : old;
            long next = Math.max(0L, base + delta);
            actual[0] = next - base;
            return next;
        });
        if (actual[0] != 0L) {
            estimatedBytes.addAndGet(actual[0]);
        }
    }

    private void removeEntryBytes(K key) {
        Long removed = entryBytes.remove(key);
        if (removed != null) {
            estimatedBytes.addAndGet(-removed);
        }
    }

    private void putEntry(K key, CacheEntry<V> entry) {
        store.put(key, entry);
        indexKey(key);
        setEntryBytes(key, estimateValueBytes(entry.getValue()));
    }

    /** 删除 KV 条目并清理版本/索引/内存簿记；不触碰 LRU（调用方按场景处理）。 */
    private boolean removeStoreEntry(K key) {
        CacheEntry<V> removed = store.remove(key);
        if (removed == null) {
            return false;
        }
        versions.remove(key);
        zsetMemberIndex.remove(key);
        removeEntryBytes(key);
        unindexKey(key);
        return true;
    }

    private void removeMetaOnly(K key) {
        versions.remove(key);
        zsetMemberIndex.remove(key);
        removeEntryBytes(key);
        unindexKey(key);
    }

    public long getEstimatedBytes() {
        return estimatedBytes.get();
    }

    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }

    public long getByteEvictions() {
        return byteEvictions.get();
    }

    public int getIndexedKeyCount() {
        return keyIndex.size();
    }

    /** 全部 key（含 Stream）。管理面/SCAN 使用；{@link #keys()} 保持 KV 视图兼容。 */
    public Set<String> allKeys() {
        return Collections.unmodifiableSet(new java.util.HashSet<>(keyIndex));
    }

    public CacheMetrics metrics() {
        return metrics;
    }

    /**
     * 本地游标扫描：cursor 为空表示从头开始，返回的 cursor 作为下一页入参；
     * 仅扫描本节点索引，集群级遍历仍走 {@code CacheGateway.listKeys} fan-out。
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> scan(String cursor, String prefix, int count) {
        int limit = Math.max(1, Math.min(count <= 0 ? 100 : count, 10_000));
        String p = prefix == null || prefix.isEmpty() ? null : prefix;
        java.util.NavigableSet<String> tail = cursor == null || cursor.isEmpty()
                ? keyIndex : keyIndex.tailSet(cursor, false);
        List<String> keys = new ArrayList<>(Math.min(limit, 128));
        boolean done = true;
        String last = null;
        Iterator<String> it = tail.iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (p != null && !key.startsWith(p)) {
                continue;
            }
            if (!keyAlive(key)) {
                continue;
            }
            if (keys.size() >= limit) {
                done = false;
                break;
            }
            keys.add(key);
            last = key;
        }
        if (!it.hasNext() && keys.size() < limit) {
            done = true;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cursor", done || last == null ? "" : last);
        out.put("keys", keys);
        out.put("done", done);
        return out;
    }

    @SuppressWarnings("unchecked")
    private boolean keyAlive(String key) {
        if (streamStore.hasStream(key)) {
            return true;
        }
        CacheEntry<V> entry = store.get((K) key);
        if (entry == null) {
            return false;
        }
        if (entry.isExpired()) {
            removeStoreEntry((K) key);
            lruRemove((K) key);
            metrics.expired(1);
            return false;
        }
        return true;
    }

    /** 本节点统计快照（内存 + 指标 + 引擎计数器）。 */
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("keys", keyIndex.size());
        out.put("kvKeys", store.size());
        out.put("streams", streamStore.streamCount());
        out.put("estimatedBytes", estimatedBytes.get());
        out.put("maxMemoryBytes", maxMemoryBytes);
        out.put("byteEvictions", byteEvictions.get());
        out.put("lruEvictions", lruEvictions.get());
        out.put("wrongTypeOverwrites", wrongTypeOverwrites.get());
        out.put("pelCapRejected", streamStore.getPelCapRejected());
        out.put("streamCapRejected", streamStore.getStreamCapRejected());
        out.put("wrongTypeMode", "log");
        out.put("metrics", metrics.snapshot());
        return out;
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
     * 为已存在的 key 设置剩余 TTL（毫秒）；{@code <=0} 表示清除 TTL（永不过期）。
     * <p>供迁移导入路径在重建容器（hset/rpush/sadd/zadd 本身不携带 TTL）后恢复
     * 源端剩余 TTL 使用；key 不存在时 no-op。</p>
     * <p>C7：容器 TTL 持久化——记 AOF（v2 文件落 OP_EXPIRE 绝对时间戳），跨重启保持。</p>
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
        logAof("EXPIRE", key, ttlMillis);
    }

    /**
     * 回放专用（C6/C7）：以绝对过期时间戳写入（epoch 毫秒；{@code Long.MAX_VALUE} 永不过期）。
     * 不记 AOF——仅 AOF 回放路径调用（回放期间 aofService 未挂接）。
     */
    public void putWithExpireAt(K key, V value, long expireTimeMs) {
        putEntry(key, CacheEntry.withExpireTime(value, expireTimeMs));
        lruPut(key);
    }

    /**
     * 回放专用（C7）：以绝对过期时间戳设置 TTL；key 不存在时 no-op。不记 AOF。
     */
    public void expireAt(K key, long expireTimeMs) {
        if (key == null) {
            return;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return;
        }
        store.replace(key, entry, CacheEntry.withExpireTime(entry.getValue(), expireTimeMs));
        lruPut(key);
    }

    /**
     * AOF 重写用（C7）：key 的绝对过期时间戳（epoch 毫秒）；
     * {@code -1} key 不存在；{@code Long.MAX_VALUE} 永不过期。
     */
    public long expireTimeAt(K key) {
        CacheEntry<V> entry = store.get(key);
        return entry == null ? -1L : entry.getExpireTime();
    }

    /** 空值标记访问（包内可见）：AOF 重写跳过瞬态 NULL_MARKER 条目。 */
    static Object nullMarker() {
        return NULL_MARKER;
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

    /**
     * C24 日志模式：容器写路径发现异类型既有值将被覆盖时告警计数。
     * Redis 语义为 WRONGTYPE 报错；当前为观察期——先收集真实流量中的覆盖场景，
     * 确认迁移回放等路径无覆盖依赖后转拒绝。null 与空值标记不视为类型冲突。
     */
    private void warnWrongTypeOverwrite(K key, Object existing, String command) {
        if (existing == null || existing == NULL_MARKER) {
            return;
        }
        wrongTypeOverwrites.incrementAndGet();
        LOG.warn("C24 WRONGTYPE（日志模式）：key={} 已存在 {} 类型，被 {} 覆盖（覆盖次数 {}）",
                key, existing.getClass().getSimpleName(), command, wrongTypeOverwrites.get());
    }

    /** C24 类型冲突覆盖计数（管理面观测；转拒绝模式前须确认该值稳定）。 */
    public long getWrongTypeOverwrites() {
        return wrongTypeOverwrites.get();
    }

    /** C16 容量淘汰计数（LRU maxSize 驱逐次数，容量告警载体）。 */
    public long getLruEvictions() {
        return lruEvictions.get();
    }

    // ==================== 事务辅助 ====================

    private void queueOrExec(K key, String aofOp, Object[] aofArgs, java.util.function.Supplier<Object> action) {
        requireAofEncodableKey(key, aofOp);
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
        requireAofEncodableKey(key, aofOp);
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

    /** 写操作前置：key 无法编码进 AOF 时拒绝整个操作（读操作 aofOp==null 不校验）。 */
    private void requireAofEncodableKey(K key, String aofOp) {
        if (aofOp != null && aofService != null) {
            aofService.requireEncodableKey(key != null ? key.toString() : "");
        }
    }

    // ==================== LRU 辅助方法（O(1)；读路径 tryLock 免阻塞） ====================

    /** GET 命中时触碰 LRU：将 key 移到链表尾部（最近使用）。竞争时跳过——淘汰顺序近似即可 */
    private void lruTouch(K key) {
        if (lruLock.tryLock()) {
            try { lruMap.get(key); } finally { lruLock.unlock(); }
        }
    }

    /**
     * PUT 时更新 LRU：加入或移动到尾部，超限由 removeEldestEntry 自动淘汰。
     * <p>C21：容量将满时先从最老端小范围扫描（≤{@link #EXPIRED_EVICT_SCAN} 条）
     * 优先驱逐已过期条目——短 TTL 洪水场景下过期条目不再挤占 LRU 名额、把热点 key
     * 挤出缓存（主动清理周期 60s，窗口足够致命）。</p>
     */
    private void lruPut(K key) {
        lruLock.lock();
        try {
            if (!lruMap.containsKey(key) && lruMap.size() >= maxSize) {
                evictExpiredEldestWithinScan();
            }
            lruMap.put(key, Boolean.TRUE);
            enforceMemoryLimit();
        } finally {
            lruLock.unlock();
        }
    }

    /** 近似字节水位驱逐：从 LRU 最老端循环驱逐，直到回到水位以内或无可驱逐条目。 */
    private void enforceMemoryLimit() {
        if (maxMemoryBytes <= 0) {
            return;
        }
        while (estimatedBytes.get() > maxMemoryBytes) {
            Iterator<Map.Entry<K, Boolean>> it = lruMap.entrySet().iterator();
            if (!it.hasNext()) {
                break;
            }
            K victim = it.next().getKey();
            it.remove();
            if (removeStoreEntry(victim)) {
                lruEvictions.incrementAndGet();
                byteEvictions.incrementAndGet();
                metrics.eviction();
                long now = System.currentTimeMillis();
                if (now - lastLruEvictWarnAt > 60_000L) {
                    lastLruEvictWarnAt = now;
                    LOG.warn("内存水位淘汰：maxMemoryBytes={}，estimatedBytes={}，累计驱逐 {} 次",
                            maxMemoryBytes, estimatedBytes.get(), lruEvictions.get());
                }
            }
        }
    }

    /** C21：从最老端至多扫描 N 条，驱逐首个已过期条目（在 lruLock 内、removeEldestEntry 之外）。 */
    private static final int EXPIRED_EVICT_SCAN = 32;

    private void evictExpiredEldestWithinScan() {
        Iterator<Map.Entry<K, Boolean>> it = lruMap.entrySet().iterator();
        for (int i = 0; i < EXPIRED_EVICT_SCAN && it.hasNext(); i++) {
            Map.Entry<K, Boolean> candidate = it.next();
            CacheEntry<V> entry = store.get(candidate.getKey());
            if (entry != null && entry.isExpired()) {
                K stale = candidate.getKey();
                it.remove();
                removeStoreEntry(stale);
                lruEvictions.incrementAndGet();
                byteEvictions.incrementAndGet();
                metrics.expired(1);
                metrics.eviction();
                long now = System.currentTimeMillis();
                if (now - lastLruEvictWarnAt > 60_000L) {
                    lastLruEvictWarnAt = now;
                    LOG.warn("C16/C21 容量淘汰（过期优先）：maxSize={}，累计驱逐 {} 次", maxSize, lruEvictions.get());
                }
                return;
            }
        }
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
        requireValueFitsMaxMemory(value);
        queueOrExec(key, "PUT_EXPIRED", new Object[]{value, ttlMillis}, () -> {
            putEntry(key, new CacheEntry<>(value, ttlMillis));
            lruPut(key);
            return null;
        });
    }

    @Override
    public void put(K key, V value) {
        requireValueFitsMaxMemory(value);
        queueOrExec(key, "PUT", new Object[]{value}, () -> {
            putEntry(key, new CacheEntry<>(value, 0));
            lruPut(key);
            return null;
        });
    }

    private void requireValueFitsMaxMemory(V value) {
        if (maxMemoryBytes <= 0) {
            return;
        }
        long bytes = estimateValueBytes(value);
        if (bytes > maxMemoryBytes) {
            throw new CacheException("value size " + bytes + " exceeds maxMemoryBytes " + maxMemoryBytes);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, long ttlMillis) {
        requireValueFitsMaxMemory(value);
        String aofOp = ttlMillis > 0 ? "PUT_EXPIRED" : "PUT";
        Object[] aofArgs = ttlMillis > 0 ? new Object[]{value, ttlMillis} : new Object[]{value};
        requireAofEncodableKey(key, aofOp);
        TxContext<K> tx = txHolder.get();
        if (tx != null && tx.active) {
            // 成功与否到 exec 才确定：版本与 AOF 必须在 action 内按实际结果提交。
            // 交给 exec 无条件记录会把失败的 NX 落成 PUT，回放时凭空复活该值。
            tx.commands.add(new QueuedCommand(() -> {
                boolean applied = doPutIfAbsent(key, value, ttlMillis);
                if (applied) {
                    bumpVersion(key);
                    logAof(aofOp, key, aofArgs);
                }
                return applied;
            }));
            return false;
        }
        boolean applied = doPutIfAbsent(key, value, ttlMillis);
        if (applied) {
            bumpVersion(key);
            logAof(aofOp, key, aofArgs);
        }
        return applied;
    }

    /**
     * 原子“不存在才写”：{@code compute} 在同一 key 上串行化，保证并发 NX 只有一个成功。
     * <p>空值标记（{@link #NULL_MARKER}）是穿透保护的内部产物，对外等价于不存在，
     * 允许真实写入覆盖。
     */
    private boolean doPutIfAbsent(K key, V value, long ttlMillis) {
        boolean[] applied = {false};
        store.compute(key, (k, existing) -> {
            if (existing != null && !existing.isExpired()
                    && existing.getValue() != NULL_MARKER) {
                return existing;
            }
            applied[0] = true;
            return new CacheEntry<>(value, ttlMillis);
        });
        if (applied[0]) {
            indexKey(key);
            setEntryBytes(key, estimateValueBytes(value));
            lruPut(key);
        }
        return applied[0];
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        // 读操作不记录 AOF；对外解包空值标记（C9）
        return (V) queueOrExecReturn(key, null, null, () -> unwrapNullMarker(doGet(key)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getWithLoader(K key, java.util.function.Supplier<V> loader, long ttlMillis) {
        // 1. 先尝试快速读取（C9：doGet 返回原始值，NULL_MARKER 非 null → 命中空值缓存，
        //    直接返回 null 不走 loader——穿透防护恢复）
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
                putEntry(key, new CacheEntry<>((V) NULL_MARKER, NULL_CACHE_TTL_MS));
                lruPut(key);
            } else {
                requireValueFitsMaxMemory(value);
                long ttl = ttlMillis > 0 ? ttlMillis : 0;
                putEntry(key, new CacheEntry<>(value, ttl));
                lruPut(key);
                // C42：回填与 put 语义对齐——补记 AOF（空值标记为瞬态不落盘，重启后由 loader 重建）
                if (ttl > 0) {
                    logAof("PUT_EXPIRED", key, value, ttl);
                } else {
                    logAof("PUT", key, value);
                }
            }
            return value;
        }
    }

    /** 解包空值标记：如果缓存值是 NULL_MARKER 则返回 null */
    @SuppressWarnings("unchecked")
    private V unwrapNullMarker(V value) {
        return value == NULL_MARKER ? null : value;
    }

    /**
     * 内部读取：返回<b>原始值</b>（含 NULL_MARKER，C9）——由调用方按语义各自解包。
     * 容器读路径（hget/lpop/...）对 marker 的 instanceof 判别自然返回空，无需解包。
     */
    private V doGet(K key) {
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            metrics.miss();
            return null;
        }
        if (entry.isExpired()) {
            removeStoreEntry(key);
            lruRemove(key);
            metrics.expired(1);
            metrics.miss();
            return null;
        }
        metrics.hit();
        lruTouch(key);
        return entry.getValue();
    }

    @Override
    public void remove(K key) {
        queueOrExec(key, "REMOVE", new Object[0], () -> {
            removeStoreEntry(key);
            lruRemove(key);
            // 联动清理同名的 stream，保持与 Redis DEL stream 行为一致
            if (key != null) {
                streamStore.removeStream(key.toString());
                unindexKey(key);
            }
            return null;
        });
    }

    /**
     * 批量删除（C30 清扫批量化）：内存联动清理与单删一致；AOF 侧 v2 文件记
     * {@code REMOVE_MULTI} 聚合记录（消灭启动清扫逐 key 记 REMOVE 的回放大军与重写风暴），
     * v1 存量文件回退逐条 REMOVE。仅供管理/清扫路径（无事务语义）。
     *
     * @return 实际提交删除的 key 数
     */
    public int removeMany(java.util.Collection<K> keys) {
        if (keys.isEmpty()) {
            return 0;
        }
        for (K key : keys) {
            removeStoreEntry(key);
            lruRemove(key);
            if (key != null) {
                streamStore.removeStream(key.toString());
                unindexKey(key);
            }
        }
        if (aofService != null) {
            if (aofService.isBulkRecordsEnabled()) {
                List<String> strKeys = new ArrayList<>(keys.size());
                for (K key : keys) {
                    strKeys.add(key != null ? key.toString() : "");
                }
                aofService.logTyped("REMOVE_MULTI", "", strKeys);
            } else {
                for (K key : keys) {
                    logAof("REMOVE", key);
                }
            }
        }
        return keys.size();
    }

    @Override
    public void clear() {
        TxContext<K> tx = txHolder.get();
        if (tx != null && tx.active) {
            tx.commands.add(new QueuedCommand(() -> {
                clearAll();
                return null;
            }, null, "CLEAR", "", new Object[0]));
            return;        }
        clearAll();
        logAof("CLEAR", null);
    }

    private void clearAll() {
        store.clear();
        versions.clear();
        lruClear();
        zsetMemberIndex.clear();
        streamStore.clear();
        entryBytes.clear();
        estimatedBytes.set(0L);
        keyIndex.clear();
    }

    @Override
    public int size() {
        return store.size();
    }

    /** KV store 的 key 视图（不含 Stream）；全量视图见 {@link #allKeys()}。 */
    @Override
    public java.util.Set<K> keys() {
        return Collections.unmodifiableSet(new java.util.HashSet<>(store.keySet()));
    }

    // ==================== Keyspace / 原子 KV 命令 ====================

    public boolean exists(K key) {
        if (key == null) {
            return false;
        }
        if (streamStore.hasStream(key.toString())) {
            return true;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            metrics.miss();
            return false;
        }
        if (entry.isExpired()) {
            removeStoreEntry(key);
            lruRemove(key);
            metrics.expired(1);
            metrics.miss();
            return false;
        }
        metrics.hit();
        lruTouch(key);
        return true;
    }

    public String type(K key) {
        if (key == null) {
            return "none";
        }
        if (streamStore.hasStream(key.toString())) {
            return "stream";
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return "none";
        }
        if (entry.isExpired()) {
            removeStoreEntry(key);
            lruRemove(key);
            metrics.expired(1);
            return "none";
        }
        V value = entry.getValue();
        if (value instanceof HashValue) {
            return "hash";
        }
        if (value instanceof ConcurrentLinkedDeque) {
            return "list";
        }
        if (value instanceof Set) {
            return "set";
        }
        if (value instanceof ConcurrentSkipListMap) {
            return "zset";
        }
        return "string";
    }

    /** 剩余 TTL：-2 不存在，-1 永不过期，&gt;=0 剩余毫秒；Stream 视为永不过期。 */
    public long ttlMillis(K key) {
        if (key == null) {
            return -2L;
        }
        if (streamStore.hasStream(key.toString())) {
            return -1L;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return -2L;
        }
        if (entry.isExpired()) {
            removeStoreEntry(key);
            lruRemove(key);
            metrics.expired(1);
            return -2L;
        }
        if (entry.getExpireTime() == Long.MAX_VALUE) {
            return -1L;
        }
        return Math.max(0L, entry.getExpireTime() - System.currentTimeMillis());
    }

    /** PERSIST：key 存在且有 TTL 时清除 TTL，返回是否实际清除。 */
    public boolean persist(K key) {
        if (key == null || streamStore.hasStream(key.toString())) {
            return false;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null || entry.isExpired() || entry.getExpireTime() == Long.MAX_VALUE) {
            return false;
        }
        expire(key, 0L);
        return true;
    }

    /** TOUCH：刷新 LRU 热度，不改变 TTL。 */
    public boolean touch(K key) {
        if (key == null) {
            return false;
        }
        if (streamStore.hasStream(key.toString())) {
            return true;
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        lruTouch(key);
        return true;
    }

    public long incrBy(K key, long delta) {
        Object result = queueOrExecReturn(key, null, null, () -> doIncrAndLog(key, delta));
        return result instanceof Number n ? n.longValue() : 0L;
    }

    public long decrBy(K key, long delta) {
        return incrBy(key, -delta);
    }

    @SuppressWarnings("unchecked")
    private long doIncrAndLog(K key, long delta) {
        requireAofEncodableKey(key, "PUT");
        long[] result = {0L};
        long[] expire = {Long.MAX_VALUE};
        store.compute(key, (k, existing) -> {
            long current = 0L;
            if (existing != null && !existing.isExpired()) {
                Object value = existing.getValue();
                if (value instanceof Number n) {
                    current = n.longValue();
                    expire[0] = existing.getExpireTime();
                } else if (value instanceof String s) {
                    try {
                        current = Long.parseLong(s);
                    } catch (NumberFormatException e) {
                        throw new CacheException("value is not an integer");
                    }
                    expire[0] = existing.getExpireTime();
                } else if (value != NULL_MARKER) {
                    throw new CacheException("WRONGTYPE: increment target is not an integer");
                }
            }
            final long next;
            try {
                next = Math.addExact(current, delta);
            } catch (ArithmeticException e) {
                throw new CacheException("increment or decrement would overflow");
            }
            result[0] = next;
            return CacheEntry.withExpireTime((V) Long.valueOf(next), expire[0]);
        });
        indexKey(key);
        setEntryBytes(key, estimateValueBytes(result[0]));
        lruPut(key);
        long remaining = expire[0] == Long.MAX_VALUE ? 0L
                : Math.max(0L, expire[0] - System.currentTimeMillis());
        if (remaining > 0L) {
            logAof("PUT_EXPIRED", key, result[0], remaining);
        } else {
            logAof("PUT", key, result[0]);
        }
        return result[0];
    }

    /** CAS：key 存在且当前值与 expected 相等时替换，保留原 TTL。 */
    public boolean cas(K key, V expected, V newValue) {
        requireValueFitsMaxMemory(newValue);
        Object result = queueOrExecReturn(key, null, null,
                () -> doCasAndLog(key, expected, newValue));
        return Boolean.TRUE.equals(result);
    }

    private boolean doCasAndLog(K key, V expected, V newValue) {
        requireAofEncodableKey(key, "PUT");
        boolean[] applied = {false};
        long[] expire = {Long.MAX_VALUE};
        store.compute(key, (k, existing) -> {
            if (existing == null || existing.isExpired()
                    || existing.getValue() == NULL_MARKER
                    || !Objects.equals(existing.getValue(), expected)) {
                return existing;
            }
            expire[0] = existing.getExpireTime();
            applied[0] = true;
            return CacheEntry.withExpireTime(newValue, expire[0]);
        });
        if (!applied[0]) {
            return false;
        }
        indexKey(key);
        setEntryBytes(key, estimateValueBytes(newValue));
        lruPut(key);
        long remaining = expire[0] == Long.MAX_VALUE ? 0L
                : Math.max(0L, expire[0] - System.currentTimeMillis());
        if (remaining > 0L) {
            logAof("PUT_EXPIRED", key, newValue, remaining);
        } else {
            logAof("PUT", key, newValue);
        }
        return true;
    }

    public List<V> mget(List<K> keys) {
        List<V> result = new ArrayList<>(keys.size());
        for (K key : keys) {
            result.add(get(key));
        }
        return result;
    }

    /** MSET：多 key 写入；要求调用方保证同一分区，原子性仅到单 key。 */
    public long mset(Map<K, V> entries) {
        for (Map.Entry<K, V> e : entries.entrySet()) {
            requireValueFitsMaxMemory(e.getValue());
            requireAofEncodableKey(e.getKey(), "PUT");
        }
        List<ReentrantLock> locks = new ArrayList<>();
        List<Integer> lockIds = new ArrayList<>();
        for (K key : entries.keySet()) {
            ReentrantLock lock = lockFor(key);
            int id = System.identityHashCode(lock);
            if (!lockIds.contains(id)) {
                lockIds.add(id);
                locks.add(lock);
            }
        }
        locks.sort(java.util.Comparator.comparingInt(System::identityHashCode));
        for (ReentrantLock lock : locks) {
            lock.lock();
        }
        try {
            for (Map.Entry<K, V> e : entries.entrySet()) {
                put(e.getKey(), e.getValue());
            }
            return entries.size();
        } finally {
            for (int i = locks.size() - 1; i >= 0; i--) {
                locks.get(i).unlock();
            }
        }
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
                HashValue<V> hash;
                if (entry == null || entry.isExpired()) {
                    hash = new HashValue<>();
                    putEntry(key, new CacheEntry<>((V) hash, 0));
                } else {
                    V val = entry.getValue();
                    if (val instanceof HashValue) {
                        hash = (HashValue<V>) val;
                    } else {
                        warnWrongTypeOverwrite(key, val, "HSET");
                        hash = new HashValue<>();
                        putEntry(key, new CacheEntry<>((V) hash, 0));
                    }
                }
                V old = hash.put(field, value);
                addEntryBytes(key, estimateHashEntry(field, value)
                        - (old == null ? 0L : estimateHashEntry(field, old)));
                lruPut(key);
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
            if (val instanceof HashValue) {
                return ((HashValue<V>) val).get(field);
            }
            return null;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<V> hmget(K key, String... fields) {
        return (List<V>) queueOrExecReturn(key, null, null, () -> {
            V val = doGet(key);
            if (!(val instanceof HashValue)) {
                List<V> result = new ArrayList<>(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    result.add(null);
                }
                return result;
            }
            HashValue<V> hash = (HashValue<V>) val;
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
            if (val instanceof HashValue) {
                return new ConcurrentHashMap<>((HashValue<V>) val);
            }
            return Collections.emptyMap();
        });
    }

    /** HDEL：返回实际删除的 field 数；删空后按 Redis 语义移除 key。 */
    public long hdel(K key, String... fields) {
        Object result = queueOrExecReturn(key, "HDEL",
                new Object[]{java.util.Arrays.asList(fields)}, () -> doHdel(key, fields));
        return result instanceof Number n ? n.longValue() : 0L;
    }

    private long doHdel(K key, String... fields) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof HashValue)) {
                return 0L;
            }
            @SuppressWarnings("unchecked")
            HashValue<V> hash = (HashValue<V>) entry.getValue();
            long removed = 0L;
            for (String field : fields) {
                V old = hash.remove(field);
                if (old != null) {
                    removed++;
                    addEntryBytes(key, -estimateHashEntry(field, old));
                }
            }
            if (removed > 0) {
                if (hash.isEmpty()) {
                    removeStoreEntry(key);
                    lruRemove(key);
                } else {
                    lruPut(key);
                }
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public long hlen(K key) {
        V val = doGet(key);
        return val instanceof HashValue ? ((HashValue<?>) val).size() : 0L;
    }

    public boolean hexists(K key, String field) {
        V val = doGet(key);
        return val instanceof HashValue && ((HashValue<?>) val).containsKey(field);
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
                putEntry(key, new CacheEntry<>((V) deque, 0));
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentLinkedDeque) {
                    deque = (ConcurrentLinkedDeque<V>) val;
                } else {
                    warnWrongTypeOverwrite(key, val, "LPUSH");
                    deque = new ConcurrentLinkedDeque<>();
                    putEntry(key, new CacheEntry<>((V) deque, 0));
                }
            }
            for (V value : values) {
                deque.addFirst(value);
                addEntryBytes(key, estimateListItem(value));
            }
            lruPut(key);
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
                putEntry(key, new CacheEntry<>((V) deque, 0));
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentLinkedDeque) {
                    deque = (ConcurrentLinkedDeque<V>) val;
                } else {
                    warnWrongTypeOverwrite(key, val, "RPUSH");
                    deque = new ConcurrentLinkedDeque<>();
                    putEntry(key, new CacheEntry<>((V) deque, 0));
                }
            }
            for (V value : values) {
                deque.addLast(value);
                addEntryBytes(key, estimateListItem(value));
            }
            lruPut(key);
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
            V popped = deque.pollFirst();
            if (popped != null) {
                addEntryBytes(key, -estimateListItem(popped));
                if (deque.isEmpty()) {
                    removeStoreEntry(key);
                    lruRemove(key);
                } else {
                    lruPut(key);
                }
            }
            return popped;
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
            V popped = deque.pollLast();
            if (popped != null) {
                addEntryBytes(key, -estimateListItem(popped));
                if (deque.isEmpty()) {
                    removeStoreEntry(key);
                    lruRemove(key);
                } else {
                    lruPut(key);
                }
            }
            return popped;
        });
    }

    public long llen(K key) {
        V val = doGet(key);
        return val instanceof ConcurrentLinkedDeque ? ((ConcurrentLinkedDeque<?>) val).size() : 0L;
    }

    @SuppressWarnings("unchecked")
    public List<V> lrange(K key, long start, long stop) {
        V val = doGet(key);
        if (!(val instanceof ConcurrentLinkedDeque)) {
            return Collections.emptyList();
        }
        ConcurrentLinkedDeque<V> deque = (ConcurrentLinkedDeque<V>) val;
        int size = deque.size();
        int s = start < 0 ? size + (int) start : (int) start;
        int e = stop < 0 ? size + (int) stop : (int) stop;
        if (s < 0) s = 0;
        if (e >= size) e = size - 1;
        if (s > e || s >= size) {
            return Collections.emptyList();
        }
        List<V> result = new ArrayList<>(Math.max(0, e - s + 1));
        int idx = 0;
        for (V item : deque) {
            if (idx > e) break;
            if (idx >= s) result.add(item);
            idx++;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public V lindex(K key, long index) {
        V val = doGet(key);
        if (!(val instanceof ConcurrentLinkedDeque)) {
            return null;
        }
        ConcurrentLinkedDeque<V> deque = (ConcurrentLinkedDeque<V>) val;
        int size = deque.size();
        int idx = index < 0 ? size + (int) index : (int) index;
        if (idx < 0 || idx >= size) {
            return null;
        }
        int i = 0;
        for (V item : deque) {
            if (i++ == idx) return item;
        }
        return null;
    }

    // ==================== Set 操作 ====================

    @Override
    public void sadd(K key, V... members) {
        saddCount(key, members);
    }

    /** SADD：返回实际新增成员数（已存在成员不计数）。 */
    public long saddCount(K key, V... members) {
        Object result = queueOrExecReturn(key, "SADD", new Object[]{members},
                () -> doSadd(key, members));
        return result instanceof Number n ? n.longValue() : 0L;
    }

    /**
     * SADD 内部实现：持条带锁，消除 TOCTOU 竞态。
     * @see #doLpush(Object, Object[])
     */
    @SuppressWarnings("unchecked")
    private long doSadd(K key, V... members) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            java.util.Set<V> set;
            if (entry == null || entry.isExpired()) {
                set = ConcurrentHashMap.newKeySet();
                putEntry(key, new CacheEntry<>((V) set, 0));
            } else {
                V val = entry.getValue();
                if (val instanceof java.util.Set) {
                    set = (java.util.Set<V>) val;
                } else {
                    warnWrongTypeOverwrite(key, val, "SADD");
                    set = ConcurrentHashMap.newKeySet();
                    putEntry(key, new CacheEntry<>((V) set, 0));
                }
            }
            long added = 0L;
            for (V member : members) {
                if (set.add(member)) {
                    added++;
                    addEntryBytes(key, estimateSetMember(member));
                }
            }
            if (added > 0) {
                lruPut(key);
            }
            return added;
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
    public void srem(K key, V... members) {
        sremCount(key, members);
    }

    /** SREM：返回实际移除成员数。 */
    public long sremCount(K key, V... members) {
        Object result = queueOrExecReturn(key, "SREM", new Object[]{members},
                () -> doSrem(key, members));
        return result instanceof Number n ? n.longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    private long doSrem(K key, V... members) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof java.util.Set)) {
                return 0L;
            }
            java.util.Set<V> set = (java.util.Set<V>) entry.getValue();
            long removed = 0L;
            for (V member : members) {
                if (set.remove(member)) {
                    removed++;
                    addEntryBytes(key, -estimateSetMember(member));
                }
            }
            if (removed > 0) {
                if (set.isEmpty()) {
                    removeStoreEntry(key);
                    lruRemove(key);
                } else {
                    lruPut(key);
                }
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public long scard(K key) {
        V val = doGet(key);
        return val instanceof java.util.Set ? ((java.util.Set<?>) val).size() : 0L;
    }

    public boolean sismember(K key, V member) {
        V val = doGet(key);
        return val instanceof java.util.Set && ((java.util.Set<?>) val).contains(member);
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
                putEntry(key, new CacheEntry<>((V) zset, 0));
                memberIdx = new ConcurrentHashMap<>();
                zsetMemberIndex.put(key, memberIdx);
            } else {
                V val = entry.getValue();
                if (val instanceof ConcurrentSkipListMap) {
                    zset = (ConcurrentSkipListMap<Double, Set<V>>) val;
                    memberIdx = zsetMemberIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
                } else {
                    warnWrongTypeOverwrite(key, val, "ZADD");
                    zset = new ConcurrentSkipListMap<>();
                    putEntry(key, new CacheEntry<>((V) zset, 0));
                    memberIdx = new ConcurrentHashMap<>();
                    zsetMemberIndex.put(key, memberIdx);
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
            } else {
                addEntryBytes(key, estimateZsetMember(member));
            }
            // 使用线程安全的 Set（ConcurrentHashMap.newKeySet）
            zset.computeIfAbsent(score, k -> ConcurrentHashMap.newKeySet()).add(member);
            lruPut(key);
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
            // C23：总数优先取 member→score 反向索引（O(1)，负索引必需）——大 ZSet
            // 取前 10 条不再先 O(N) 全遍历；无索引（纯读未写过）回退一次求和
            int totalSize;
            ConcurrentHashMap<V, Double> memberIdx = zsetMemberIndex.get(key);
            if (memberIdx != null) {
                totalSize = memberIdx.size();
            } else {
                totalSize = 0;
                for (java.util.Set<V> members : zset.values()) {
                    totalSize += members.size();
                }
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

    /** ZREM：返回实际移除成员数；删空后移除 key。 */
    public long zrem(K key, V... members) {
        List<Object> args = new ArrayList<>(members.length);
        for (V member : members) args.add(member);
        Object result = queueOrExecReturn(key, "ZREM", new Object[]{args},
                () -> doZrem(key, members));
        return result instanceof Number n ? n.longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    private long doZrem(K key, V... members) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            CacheEntry<V> entry = store.get(key);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof ConcurrentSkipListMap)) {
                return 0L;
            }
            ConcurrentSkipListMap<Double, Set<V>> zset =
                    (ConcurrentSkipListMap<Double, Set<V>>) entry.getValue();
            ConcurrentHashMap<V, Double> memberIdx = zsetMemberIndex.get(key);
            long removed = 0L;
            for (V member : members) {
                Double oldScore = memberIdx == null ? null : memberIdx.remove(member);
                if (oldScore == null) {
                    continue;
                }
                Set<V> bucket = zset.get(oldScore);
                if (bucket != null) {
                    bucket.remove(member);
                    if (bucket.isEmpty()) zset.remove(oldScore);
                }
                removed++;
                addEntryBytes(key, -estimateZsetMember(member));
            }
            if (removed > 0) {
                if (zset.isEmpty()) {
                    removeStoreEntry(key);
                    lruRemove(key);
                } else {
                    lruPut(key);
                }
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public long zcard(K key) {
        V val = doGet(key);
        if (!(val instanceof ConcurrentSkipListMap)) {
            return 0L;
        }
        ConcurrentHashMap<V, Double> memberIdx = zsetMemberIndex.get(key);
        if (memberIdx != null) {
            return memberIdx.size();
        }
        long total = 0L;
        for (Set<V> members : ((ConcurrentSkipListMap<Double, Set<V>>) val).values()) {
            total += members.size();
        }
        return total;
    }

    public Double zscore(K key, V member) {
        if (!(doGet(key) instanceof ConcurrentSkipListMap)) {
            return null;
        }
        ConcurrentHashMap<V, Double> memberIdx = zsetMemberIndex.get(key);
        return memberIdx == null ? null : memberIdx.get(member);
    }

    /** ZRANK：同分 member 按字符串序稳定排序，找不到返回 -1。 */
    @SuppressWarnings("unchecked")
    public long zrank(K key, V member) {
        V val = doGet(key);
        if (!(val instanceof ConcurrentSkipListMap)) {
            return -1L;
        }
        ConcurrentSkipListMap<Double, Set<V>> zset =
                (ConcurrentSkipListMap<Double, Set<V>>) val;
        long rank = 0L;
        for (Set<V> bucket : zset.values()) {
            if (bucket.contains(member)) {
                List<V> sameScore = new ArrayList<>(bucket);
                sameScore.sort(java.util.Comparator.comparing(String::valueOf));
                for (V candidate : sameScore) {
                    if (Objects.equals(candidate, member)) {
                        return rank;
                    }
                    rank++;
                }
                return -1L;
            }
            rank += bucket.size();
        }
        return -1L;
    }

    // ==================== 事务操作 ====================

    @Override
    public void multi() {
        // C10：复用既有事务上下文，保留 watch() 建立的 watchedKeys/watchedVersions——
        // 旧实现 txHolder.remove() 把 WATCH 前置状态一并丢弃，WATCH→MULTI→EXEC 乐观锁静默失效。
        // 仅重置命令队列与 active 标志（顺带清理上次未 discard 的残留命令）。
        TxContext<K> tx = txHolder.get();
        if (tx == null) {
            tx = new TxContext<>();
            txHolder.set(tx);
        }
        tx.commands.clear();
        tx.active = true;
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
        // 对 watched key 与全部排队命令 key 加条带锁：只锁 watched key 时，
        // 未 WATCH 的 key 上的事务命令会与并发写交错，MULTI 的“原子执行”名不副实。
        // 按锁对象 identityHashCode 排序并去重，避免不同 key 映射到同一条带时重复加锁/死锁。
        List<K> keysToLock = new ArrayList<>(tx.watchedKeys);
        for (QueuedCommand cmd : tx.commands) {
            if (cmd.key != null) {
                keysToLock.add((K) cmd.key);
            }
        }
        List<ReentrantLock> locks = new ArrayList<>();
        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (K k : keysToLock) {
            ReentrantLock lock = lockFor(k);
            if (seen.add(System.identityHashCode(lock))) {
                locks.add(lock);
            }
        }
        locks.sort((a, b) -> Integer.compare(System.identityHashCode(a), System.identityHashCode(b)));
        List<ReentrantLock> heldLocks = new ArrayList<>(locks.size());
        for (ReentrantLock lock : locks) {
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
                removeMetaOnly(entry.getKey());
                lruRemove(entry.getKey());
                metrics.expired(1);
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
        putEntry(key, entry);
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
            keyIndex.add(key);
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
        if (key != null) {
            keyIndex.add(key);
        }
    }

    /**
     * XGROUP CREATE 包装：调用 streamStore 后记录 AOF（含 startId，回放后组起始位置一致）。
     */
    @SuppressWarnings("unchecked")
    public boolean xgroupCreate(String key, String groupName, StreamId startId, boolean mkstream) {
        boolean created = streamStore.xgroupCreate(key, groupName, startId, mkstream);
        if (created && key != null) {
            if (streamStore.hasStream(key)) {
                keyIndex.add(key);
            }
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
        if (key != null && streamStore.hasStream(key)) {
            keyIndex.add(key);
        }
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
            List<Integer> counts = new ArrayList<>();
            for (PelEntry p : claimed) {
                flat.add(p.getId().getMsTime());
                flat.add(p.getId().getSeq());
                counts.add(p.getDeliveryCount()); // C38：投递计数随记录落盘
            }
            logAof("XCLAIM", (K) key, groupName, consumerName, flat, counts);
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
            List<Integer> counts = new ArrayList<>();
            for (PelEntry p : result.getClaimed()) {
                flat.add(p.getId().getMsTime());
                flat.add(p.getId().getSeq());
                counts.add(p.getDeliveryCount()); // C38：投递计数随记录落盘
            }
            logAof("XCLAIM", (K) key, groupName, consumerName, flat, counts);
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
        // C26：改用带元数据的调用——lastDeliveredId 取本次调用实际推进值，不再反查 xinfoGroups
        // （并发投递时反查可能拿到别人推进后的 id，回放后中间消息永不投递）
        StreamStore.GroupReadResult result = streamStore.xreadGroupDetailed(key, groupName, consumerName,
                readNew, fromId, count, blockMs);
        List<StreamEntry> delivered = result.getEntries();
        if (delivered == null || delivered.isEmpty() || key == null) {
            return delivered;
        }
        if (readNew) {
            // ">" 模式：记录 XDELIVER，包含实际推进的 lastDeliveredId 与所有投递 ID
            StreamId lastDelivered = result.getLastDeliveredId();
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
            // 按 XCLAIM 语义记录（newConsumer 就是当前 consumer）；
            // C38：携带各条精确 deliveryCount，回放不虚增（死信判断依赖）
            List<Long> flat = new ArrayList<>();
            for (StreamEntry e : delivered) {
                flat.add(e.getId().getMsTime());
                flat.add(e.getId().getSeq());
            }
            List<Integer> counts = result.getDeliveryCounts();
            logAof("XCLAIM", (K) key, groupName, consumerName, flat,
                    counts != null ? counts : Collections.emptyList());
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
        xclaimRaw(key, groupName, consumerName, flatIds, null);
    }

    /** XCLAIM 回放（C38）：带精确投递计数恢复。 */
    public void xclaimRaw(String key, String groupName, String consumerName,
                          List<Long> flatIds, List<Integer> counts) {
        if (flatIds == null || flatIds.isEmpty()) return;
        List<StreamId> ids = new ArrayList<>();
        for (int i = 0; i + 1 < flatIds.size(); i += 2) {
            ids.add(new StreamId(flatIds.get(i), flatIds.get(i + 1)));
        }
        streamStore.xclaimRaw(key, groupName, consumerName, ids, counts);
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
            // 只有 HashValue 是 Hash 容器；普通 Map 是 KV（HTTP type=json）值，必须参与重写/迁移。
            if (val != null && !(val instanceof HashValue) && !(val instanceof ConcurrentLinkedDeque)
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
            if (val instanceof HashValue) {
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
