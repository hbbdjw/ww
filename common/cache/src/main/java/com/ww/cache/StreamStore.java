package com.ww.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stream 存储：管理多个 Stream 的追加日志、范围查询、删除与裁剪。
 *
 * <p>核心数据结构：每个 stream 一个 {@link ConcurrentSkipListMap}，按 {@link StreamId} 升序追加。
 * 选择跳表而非链表是为了支持 O(log n) 的 XRANGE / XREVRANGE 范围查询。
 *
 * <p>P0 暂不实现消费者组、阻塞读；P1 接入。
 *
 * <p>线程安全：所有写操作通过 per-stream synchronized 块串行化，保证 ID 单调递增与 length 计数一致；
 * 读操作走 {@link ConcurrentSkipListMap} 的弱一致快照，允许并发读到稍早状态。
 *
 * <p>容量保护：单 stream 默认上限 {@link #DEFAULT_MAX_STREAM_LEN}，防止 OOM（对齐 project_memory 中
 * 缓存 LRU/上限约束）。XADD 时按 MAXLEN 或默认上限裁剪。
 */
public class StreamStore {

    /** 单 stream 默认最大条目数；XADD 时若未显式指定 MAXLEN 则按此裁剪。 */
    static final long DEFAULT_MAX_STREAM_LEN = 1_000_000L;
    /** 阻塞 XREAD 单次最大阻塞时长（30 秒），防止客户端误传超大 blockMs 占用线程过久。 */
    static final long MAX_BLOCK_MS = 30_000L;
    /** 同时阻塞 XREAD 的最大并发数；超过则降级为非阻塞读。
     *  对齐 project_memory 约束：阻塞读不能打爆 CacheServer 工作线程池（默认 64）。 */
    private static final java.util.concurrent.Semaphore BLOCKING_PERMITS =
            new java.util.concurrent.Semaphore(32);

    private final Map<String, Stream> streams = new ConcurrentHashMap<>();
    private final long defaultMaxLen;

    public StreamStore() {
        this(DEFAULT_MAX_STREAM_LEN);
    }

    public StreamStore(long defaultMaxLen) {
        this.defaultMaxLen = defaultMaxLen > 0 ? defaultMaxLen : DEFAULT_MAX_STREAM_LEN;
    }

    // ==================== 内部 Stream 状态 ====================

    private static final class Stream {
        final ConcurrentSkipListMap<StreamId, StreamEntry> entries = new ConcurrentSkipListMap<>();
        /** 最近一次生成的 ID（无论是否被 XDEL/XTRIM 移除），保证 ID 单调递增。 */
        volatile StreamId lastGeneratedId = null;
        /** 当前可见条目数；与 entries.size() 在并发下可能短暂不一致，但 XADD 持锁更新保证最终一致。 */
        final AtomicLong length = new AtomicLong(0L);
        /** 消费者组集合，按 groupName 索引。 */
        final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
        /** 阻塞 XREAD 等待队列：每个 waiter 是一个未完成的 future。XADD 成功后逐个 complete。 */
        final java.util.List<java.util.concurrent.CompletableFuture<List<StreamEntry>>> waiters =
                new java.util.concurrent.CopyOnWriteArrayList<>();
    }

    private Stream getOrCreate(String key) {
        return streams.computeIfAbsent(key, k -> new Stream());
    }

    private Stream getOrNull(String key) {
        return streams.get(key);
    }

    // ==================== XADD ====================

    /**
     * 向 stream 追加一条消息。
     *
     * @param key     stream 名
     * @param idReq   请求的 ID；若为 {@link StreamId#AUTO} 则由服务端按当前时间生成
     * @param fields  字段对（不可为空）
     * @param options XADD 选项（MAXLEN / NOMKSTREAM）
     * @return 写入后的最终 ID；若 NOMKSTREAM 且 stream 不存在则返回 null
     */
    public StreamId xadd(String key, StreamId idReq, Map<String, String> fields, XAddOptions options) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("XADD key 不能为空");
        }
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("XADD fields 不能为空");
        }
        XAddOptions opts = options != null ? options : XAddOptions.defaults();

        Stream stream;
        if (opts.isNomkstream()) {
            stream = getOrNull(key);
            if (stream == null) {
                return null;
            }
        } else {
            stream = getOrCreate(key);
        }

        synchronized (stream) {
            StreamId finalId;
            if (idReq == null || idReq == StreamId.AUTO) {
                finalId = StreamId.nextAfter(stream.lastGeneratedId);
            } else {
                // 显式 ID：必须严格大于 lastGeneratedId（与 Redis 一致），否则抛错
                if (stream.lastGeneratedId != null && idReq.compareTo(stream.lastGeneratedId) <= 0) {
                    throw new IllegalArgumentException(
                            "XADD ID " + idReq + " 不大于当前最大 ID " + stream.lastGeneratedId);
                }
                finalId = idReq;
            }
            StreamEntry entry = new StreamEntry(finalId, fields);
            stream.entries.put(finalId, entry);
            stream.lastGeneratedId = finalId;
            stream.length.incrementAndGet();

            // 裁剪：显式 MAXLEN 优先，否则按默认上限兜底
            long trimTo = opts.hasTrim() ? opts.getMaxLen() : defaultMaxLen;
            if (trimTo > 0 && stream.length.get() > trimTo) {
                trimToCount(stream, trimTo, opts.isApproximate());
            }
            // 唤醒阻塞 XREAD 等待者（在 synchronized 外执行 complete，避免阻塞 XADD）
            // 仅收集，稍后统一通知
            java.util.List<java.util.concurrent.CompletableFuture<List<StreamEntry>>> toWake = null;
            if (!stream.waiters.isEmpty()) {
                toWake = new ArrayList<>(stream.waiters);
                stream.waiters.clear();
            }
            final java.util.List<java.util.concurrent.CompletableFuture<List<StreamEntry>>> finalWake = toWake;
            if (finalWake != null) {
                // 异步唤醒：避免在 XADD 持锁路径内执行 waiter 的回调链
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    for (java.util.concurrent.CompletableFuture<List<StreamEntry>> w : finalWake) {
                        if (!w.isDone()) {
                            // 每个 waiter 自带读取起点，重新读一次
                            try {
                                w.complete(null); // null 表示让 caller 重读
                            } catch (Throwable ignored) {}
                        }
                    }
                });
            }
            return finalId;
        }
    }

    /**
     * AOF 回放专用：直接以指定 ID 追加，不触发裁剪、不做单调校验（回放顺序由 AOF 顺序保证）。
     * 仅更新 lastGeneratedId 与 length。
     */
    public void xaddRaw(String key, StreamId id, Map<String, String> fields) {
        if (key == null || id == null) return;
        Stream stream = getOrCreate(key);
        synchronized (stream) {
            if (stream.entries.putIfAbsent(id, new StreamEntry(id, fields)) == null) {
                stream.length.incrementAndGet();
            }
            if (stream.lastGeneratedId == null || id.compareTo(stream.lastGeneratedId) > 0) {
                stream.lastGeneratedId = id;
            }
        }
    }

    // ==================== XLEN ====================

    public long xlen(String key) {
        Stream stream = getOrNull(key);
        return stream == null ? 0L : stream.length.get();
    }

    // ==================== XRANGE / XREVRANGE ====================

    /**
     * 按 ID 升序返回 [min, max] 范围内的条目，count &lt;=0 表示无限制。
     */
    public List<StreamEntry> xrange(String key, StreamId min, StreamId max, long count) {
        Stream stream = getOrNull(key);
        if (stream == null) return Collections.emptyList();
        StreamId lo = min != null ? min : StreamId.MIN;
        StreamId hi = max != null ? max : StreamId.MAX;
        NavigableMap<StreamId, StreamEntry> sub = stream.entries.subMap(lo, true, hi, true);
        List<StreamEntry> result = new ArrayList<>();
        long limit = count > 0 ? count : Long.MAX_VALUE;
        for (StreamEntry e : sub.values()) {
            if (result.size() >= limit) break;
            result.add(e);
        }
        return result;
    }

    /**
     * 按 ID 降序返回 [min, max] 范围内的条目，count &lt;=0 表示无限制。
     * 与 Redis XREVRANGE 一致：参数顺序仍为 (max, min)，但内部按降序遍历。
     */
    public List<StreamEntry> xrevrange(String key, StreamId max, StreamId min, long count) {
        Stream stream = getOrNull(key);
        if (stream == null) return Collections.emptyList();
        StreamId lo = min != null ? min : StreamId.MIN;
        StreamId hi = max != null ? max : StreamId.MAX;
        NavigableMap<StreamId, StreamEntry> sub = stream.entries.subMap(lo, true, hi, true).descendingMap();
        List<StreamEntry> result = new ArrayList<>();
        long limit = count > 0 ? count : Long.MAX_VALUE;
        for (StreamEntry e : sub.values()) {
            if (result.size() >= limit) break;
            result.add(e);
        }
        return result;
    }

    // ==================== XDEL ====================

    /**
     * 删除指定 ID，返回实际删除数量。
     */
    public long xdel(String key, StreamId... ids) {
        if (ids == null || ids.length == 0) return 0L;
        Stream stream = getOrNull(key);
        if (stream == null) return 0L;
        long removed = 0;
        synchronized (stream) {
            for (StreamId id : ids) {
                if (id == null) continue;
                if (stream.entries.remove(id) != null) {
                    removed++;
                }
            }
            stream.length.addAndGet(-removed);
        }
        return removed;
    }

    // ==================== XTRIM ====================

    /**
     * 按 MAXLEN 裁剪：保留最近 maxLen 条。
     *
     * @param approximate true 表示允许少量偏差（性能优先），false 表示精确裁剪
     * @return 实际删除条数
     */
    public long xtrim(String key, long maxLen, boolean approximate) {
        if (maxLen <= 0) return 0L;
        Stream stream = getOrNull(key);
        if (stream == null) return 0L;
        synchronized (stream) {
            return trimToCount(stream, maxLen, approximate);
        }
    }

    private long trimToCount(Stream stream, long maxLen, boolean approximate) {
        long current = stream.length.get();
        if (current <= maxLen) return 0L;
        long toRemove = current - maxLen;
        // approximate 模式：少删一些（约 10% 偏差上限），避免每次 XADD 都触发裁剪
        if (approximate && toRemove > 1) {
            // 简化策略：近似时按 toRemove * 0.9 取整下限，至少 1
            toRemove = Math.max(1L, (long) (toRemove * 0.9));
        }
        long removed = 0;
        Iterator<Map.Entry<StreamId, StreamEntry>> it = stream.entries.entrySet().iterator();
        while (it.hasNext() && removed < toRemove) {
            it.next();
            it.remove();
            removed++;
        }
        stream.length.addAndGet(-removed);
        return removed;
    }

    // ==================== XINFO ====================

    public StreamInfo xinfo(String key) {
        Stream stream = getOrNull(key);
        if (stream == null) return null;
        Map.Entry<StreamId, StreamEntry> first = stream.entries.firstEntry();
        Map.Entry<StreamId, StreamEntry> last = stream.entries.lastEntry();
        return new StreamInfo(
                stream.length.get(),
                first != null ? first.getKey() : null,
                last != null ? last.getKey() : null,
                first != null ? first.getValue() : null,
                last != null ? last.getValue() : null,
                stream.groups.size()
        );
    }

    // ==================== 阻塞 XREAD ====================

    /**
     * 阻塞 XREAD：从 {@code key} 的 stream 中读取 ID 严格大于 {@code from} 的条目，最多 {@code count} 条。
     *
     * <p>语义：
     * <ul>
     *   <li>{@code blockMs <= 0}：非阻塞，立即返回当前可见结果（可能为空）</li>
     *   <li>{@code blockMs > 0}：若无新数据则挂起当前线程最多 blockMs；XADD 时会异步唤醒</li>
     * </ul>
     *
     * <p>实现：基于 per-stream 的 {@link java.util.concurrent.CompletableFuture} 等待队列。
     * XADD 完成后异步 complete，waiter 醒来后重读 stream 取最新数据。
     * 不占用 SimpleCache 内部线程池，由调用方线程自行阻塞。
     *
     * @return 读到的条目列表；若超时且无数据则返回空列表
     */
    public List<StreamEntry> xread(String key, StreamId from, long count, long blockMs) throws InterruptedException {
        if (key == null) return Collections.emptyList();
        StreamId start = from != null ? from : StreamId.MIN;
        long limit = count > 0 ? count : Long.MAX_VALUE;
        // 1) 钳制 blockMs 上限
        long effectiveBlock = blockMs > 0 ? Math.min(blockMs, MAX_BLOCK_MS) : 0L;
        // 2) 并发限额：tryAcquire 失败则降级为非阻塞，避免打爆工作线程池
        boolean acquired = effectiveBlock > 0 && BLOCKING_PERMITS.tryAcquire();
        if (effectiveBlock > 0 && !acquired) {
            effectiveBlock = 0L; // 降级
        }

        // 第一次非阻塞读
        List<StreamEntry> result = readAfter(key, start, limit);
        if (!result.isEmpty() || effectiveBlock <= 0) {
            if (acquired) BLOCKING_PERMITS.release();
            return result;
        }

        // 进入阻塞：注册 waiter
        Stream stream = getOrNull(key);
        if (stream == null) {
            // stream 不存在：创建一个空 stream 以便注册 waiter（与 Redis MKSTREAM 类似语义）
            stream = getOrCreate(key);
        }
        java.util.concurrent.CompletableFuture<List<StreamEntry>> waiter = new java.util.concurrent.CompletableFuture<>();
        stream.waiters.add(waiter);

        try {
            // 双检：注册 waiter 后再读一次，避免漏掉注册前到达的 XADD
            result = readAfter(key, start, limit);
            if (!result.isEmpty()) {
                return result;
            }
            // 阻塞等待
            try {
                List<StreamEntry> woke = waiter.get(effectiveBlock, java.util.concurrent.TimeUnit.MILLISECONDS);
                // woke 为 null 表示被 XADD 唤醒，重新读一次
                if (woke == null) {
                    return readAfter(key, start, limit);
                }
                return woke;
            } catch (java.util.concurrent.TimeoutException e) {
                return Collections.emptyList();
            } catch (java.util.concurrent.ExecutionException e) {
                return Collections.emptyList();
            }
        } finally {
            stream.waiters.remove(waiter);
            if (acquired) BLOCKING_PERMITS.release();
        }
    }

    /**
     * 读取 stream 中 ID 严格大于 start 的前 limit 条。
     */
    private List<StreamEntry> readAfter(String key, StreamId start, long limit) {
        Stream stream = getOrNull(key);
        if (stream == null) return Collections.emptyList();
        // tailMap(start, false) → ID 严格大于 start
        NavigableMap<StreamId, StreamEntry> sub = stream.entries.tailMap(start, false);
        List<StreamEntry> result = new ArrayList<>();
        for (StreamEntry e : sub.values()) {
            if (result.size() >= limit) break;
            result.add(e);
        }
        return result;
    }

    // ==================== XGROUP ====================

    /**
     * XGROUP CREATE key group startId [MKSTREAM]。
     *
     * @param startId  通常为 {@link StreamId#MIN}（消费全量）或 lastId（仅消费新消息，对应 Redis {@code $}）
     * @param mkstream 若 stream 不存在则自动创建空 stream
     * @return true 创建成功；false group 已存在
     */
    public boolean xgroupCreate(String key, String groupName, StreamId startId, boolean mkstream) {
        Stream stream = getOrNull(key);
        if (stream == null) {
            if (!mkstream) {
                throw new IllegalStateException("XGROUP CREATE: stream " + key + " 不存在");
            }
            stream = getOrCreate(key);
        }
        ConsumerGroup group = new ConsumerGroup(groupName, startId != null ? startId : StreamId.MIN);
        return stream.groups.putIfAbsent(groupName, group) == null;
    }

    /**
     * XGROUP DESTROY key group。
     *
     * @return true 删除成功；false group 不存在
     */
    public boolean xgroupDestroy(String key, String groupName) {
        Stream stream = getOrNull(key);
        if (stream == null) return false;
        return stream.groups.remove(groupName) != null;
    }

    /**
     * XGROUP CREATECONSUMER key group consumer。
     *
     * @return 1 创建成功；0 已存在
     */
    public int xgroupCreateConsumer(String key, String groupName, String consumerName) {
        Stream stream = getOrNull(key);
        if (stream == null) {
            throw new IllegalStateException("XGROUP CREATECONSUMER: stream " + key + " 不存在");
        }
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) {
            throw new IllegalStateException("XGROUP CREATECONSUMER: group " + groupName + " 不存在");
        }
        if (group.getConsumers().containsKey(consumerName)) {
            return 0;
        }
        group.getOrCreateConsumer(consumerName);
        return 1;
    }

    /**
     * XGROUP DELCONSUMER key group consumer。
     *
     * @return 该消费者的 pending 数量（删除前）
     */
    public long xgroupDelConsumer(String key, String groupName, String consumerName) {
        Stream stream = getOrNull(key);
        if (stream == null) return 0L;
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return 0L;
        Consumer consumer = group.getConsumers().remove(consumerName);
        if (consumer == null) return 0L;
        long removed = consumer.pendingCount();
        // 同步从 PEL 中移除该消费者持有的所有 pending ID
        for (StreamId id : new ArrayList<>(consumer.pendingIdsView())) {
            group.getPel().remove(id);
        }
        return removed;
    }

    // ==================== XREADGROUP ====================

    /**
     * XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] STREAMS key id。
     *
     * <p>{@code id = ">"} 表示读取未投递给该组的新消息（推进 lastDeliveredId）；
     * 其他 ID 表示读取该 consumer 的 PEL 中 ID 大于指定值的消息（不推进 lastDeliveredId）。
     *
     * @param readNew   true 表示 {@code ">"} 模式
     * @param fromId    readNew=false 时的 PEL 起点（仅当 readNew=false 有效）
     * @param count     最多读取数量；&lt;=0 表示无限制（实际默认 100 防爆）
     * @param blockMs   阻塞毫秒；&lt;=0 表示非阻塞
     */
    public List<StreamEntry> xreadGroup(String key, String groupName, String consumerName,
                                        boolean readNew, StreamId fromId, long count, long blockMs)
            throws InterruptedException {
        Stream stream = getOrNull(key);
        if (stream == null) return Collections.emptyList();
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) {
            throw new IllegalStateException("XREADGROUP: group " + groupName + " 不存在");
        }
        Consumer consumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        consumer.touch(now);

        long limit = count > 0 ? count : 100L;

        if (readNew) {
            // ">" 模式：从 lastDeliveredId 之后取新消息
            return readNewForGroup(stream, group, consumer, limit, blockMs, now);
        }

        // PEL 模式：从 consumer 的 pendingIds 中读取 ID > fromId 的条目
        StreamId start = fromId != null ? fromId : StreamId.MIN;
        List<StreamEntry> result = new ArrayList<>();
        for (StreamId id : consumer.pendingIdsView().tailSet(start, false)) {
            StreamEntry e = stream.entries.get(id);
            if (e == null) continue; // 已被 XDEL
            result.add(e);
            // 重投递：更新 PEL 元数据
            PelEntry pel = group.getPel().get(id);
            if (pel != null) {
                pel.claimTo(consumerName, now);
            }
            if (result.size() >= limit) break;
        }
        return result;
    }

    private List<StreamEntry> readNewForGroup(Stream stream, ConsumerGroup group, Consumer consumer,
                                              long limit, long blockMs, long now) throws InterruptedException {
        // 钳制 blockMs 上限 + 并发限额
        long effectiveBlock = blockMs > 0 ? Math.min(blockMs, MAX_BLOCK_MS) : 0L;
        boolean acquired = effectiveBlock > 0 && BLOCKING_PERMITS.tryAcquire();
        if (effectiveBlock > 0 && !acquired) {
            effectiveBlock = 0L;
        }

        StreamId lastDelivered = group.getLastDeliveredId();
        // 第一次非阻塞读：取 lastDeliveredId 之后的新消息
        List<StreamEntry> result = readAfterForGroup(stream, group, consumer, lastDelivered, limit, now);
        if (!result.isEmpty() || effectiveBlock <= 0) {
            if (acquired) BLOCKING_PERMITS.release();
            return result;
        }
        // 阻塞：复用 stream 级 waiters（与普通 XREAD 共享唤醒通道）
        java.util.concurrent.CompletableFuture<List<StreamEntry>> waiter = new java.util.concurrent.CompletableFuture<>();
        stream.waiters.add(waiter);
        try {
            // 双检
            StreamId last = group.getLastDeliveredId();
            result = readAfterForGroup(stream, group, consumer, last, limit, now);
            if (!result.isEmpty()) return result;
            try {
                waiter.get(effectiveBlock, java.util.concurrent.TimeUnit.MILLISECONDS);
                // 唤醒后重读
                StreamId lastAfter = group.getLastDeliveredId();
                return readAfterForGroup(stream, group, consumer, lastAfter, limit, now);
            } catch (java.util.concurrent.TimeoutException e) {
                return Collections.emptyList();
            } catch (java.util.concurrent.ExecutionException e) {
                return Collections.emptyList();
            }
        } finally {
            stream.waiters.remove(waiter);
            if (acquired) BLOCKING_PERMITS.release();
        }
    }

    private List<StreamEntry> readAfterForGroup(Stream stream, ConsumerGroup group, Consumer consumer,
                                                StreamId from, long limit, long now) {
        NavigableMap<StreamId, StreamEntry> sub = stream.entries.tailMap(from, false);
        List<StreamEntry> result = new ArrayList<>();
        for (Map.Entry<StreamId, StreamEntry> e : sub.entrySet()) {
            StreamId id = e.getKey();
            StreamEntry entry = e.getValue();
            // 写入 PEL 与 consumer.pendingIds
            PelEntry pel = new PelEntry(id, consumer.getName(), now);
            group.getPel().put(id, pel);
            consumer.addPending(id);
            group.advanceLastDelivered(id);
            result.add(entry);
            if (result.size() >= limit) break;
        }
        return result;
    }

    // ==================== XACK ====================

    /**
     * XACK key group id [id ...]：从 PEL 中确认并移除指定 ID。
     *
     * @return 实际 ack 的数量
     */
    public long xack(String key, String groupName, StreamId... ids) {
        if (ids == null || ids.length == 0) return 0L;
        Stream stream = getOrNull(key);
        if (stream == null) return 0L;
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return 0L;
        long acked = 0;
        for (StreamId id : ids) {
            if (id == null) continue;
            PelEntry pel = group.getPel().remove(id);
            if (pel != null) {
                acked++;
                Consumer c = group.getConsumers().get(pel.getConsumer());
                if (c != null) {
                    c.removePending(id);
                }
            }
        }
        return acked;
    }

    // ==================== XPENDING ====================

    /**
     * XPENDING key group [IDLE minIdleTime] start end count [consumer]。
     *
     * <p>无 consumer 参数 → 概要形态；有 consumer → 详细形态。
     */
    public PendingSummary xpending(String key, String groupName,
                                   StreamId start, StreamId end, long count,
                                   String consumerName, long minIdleMs) {
        Stream stream = getOrNull(key);
        if (stream == null) return null;
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return null;
        long now = System.currentTimeMillis();

        if (consumerName == null || consumerName.isEmpty()) {
            // 概要形态
            Map<String, Long> consumerCounts = new LinkedHashMap<>();
            for (Map.Entry<String, Consumer> ce : group.getConsumers().entrySet()) {
                consumerCounts.put(ce.getKey(), (long) ce.getValue().pendingCount());
            }
            StreamId firstId = group.getPel().isEmpty() ? null : group.getPel().firstKey();
            StreamId lastId = group.getPel().isEmpty() ? null : group.getPel().lastKey();
            return new PendingSummary(group.getPel().size(), firstId, lastId, consumerCounts);
        }

        // 详细形态：按 [start, end] 范围、count 上限、minIdle 过滤、consumer 过滤
        Consumer consumer = group.getConsumers().get(consumerName);
        if (consumer == null) {
            return new PendingSummary(0L, Collections.emptyList());
        }
        StreamId lo = start != null ? start : StreamId.MIN;
        StreamId hi = end != null ? end : StreamId.MAX;
        java.util.NavigableSet<StreamId> ids = consumer.pendingIdsView().subSet(lo, true, hi, true);
        List<PelEntry> entries = new ArrayList<>();
        long limit = count > 0 ? count : Long.MAX_VALUE;
        for (StreamId id : ids) {
            PelEntry pel = group.getPel().get(id);
            if (pel == null) continue;
            if (minIdleMs > 0 && pel.idleTimeMs(now) < minIdleMs) continue;
            entries.add(pel);
            if (entries.size() >= limit) break;
        }
        return new PendingSummary(entries.size(), entries);
    }

    // ==================== XCLAIM ====================

    /**
     * XCLAIM key group consumer minIdleTime id [id ...]：将 PEL 中超过 minIdleTime 的消息转移给新 consumer。
     *
     * @return 实际 claim 的 PelEntry 列表
     */
    public List<PelEntry> xclaim(String key, String groupName, String consumerName,
                                 long minIdleMs, StreamId... ids) {
        if (ids == null || ids.length == 0) return Collections.emptyList();
        Stream stream = getOrNull(key);
        if (stream == null) return Collections.emptyList();
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return Collections.emptyList();
        Consumer newConsumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        newConsumer.touch(now);

        List<PelEntry> claimed = new ArrayList<>();
        for (StreamId id : ids) {
            if (id == null) continue;
            PelEntry pel = group.getPel().get(id);
            if (pel == null) continue;
            if (pel.idleTimeMs(now) < minIdleMs) continue;
            // 从原 consumer 移除
            Consumer oldConsumer = group.getConsumers().get(pel.getConsumer());
            if (oldConsumer != null) {
                oldConsumer.removePending(id);
            }
            // 转移给新 consumer
            pel.claimTo(consumerName, now);
            newConsumer.addPending(id);
            claimed.add(pel);
        }
        return claimed;
    }

    /**
     * XAUTOCLAIM key group consumer minIdleTime start [count]：自动认领 PEL 中超过 minIdleTime 的消息。
     *
     * @return [0] = 下一次扫描的 cursor（StreamId，MAX 表示已扫描完）；[1] = 实际 claim 的 PelEntry 列表
     *         当前返回结构用 XAutoClaimResult 包装
     */
    public XAutoClaimResult xautoclaim(String key, String groupName, String consumerName,
                                       long minIdleMs, StreamId start, long count) {
        Stream stream = getOrNull(key);
        if (stream == null) return new XAutoClaimResult(StreamId.MAX, Collections.emptyList());
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return new XAutoClaimResult(StreamId.MAX, Collections.emptyList());
        Consumer newConsumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        newConsumer.touch(now);

        StreamId lo = start != null ? start : StreamId.MIN;
        long limit = count > 0 ? count : 100L;
        java.util.NavigableMap<StreamId, PelEntry> sub = group.getPel().tailMap(lo, true);

        List<PelEntry> claimed = new ArrayList<>();
        StreamId nextCursor = StreamId.MAX;
        for (Map.Entry<StreamId, PelEntry> e : sub.entrySet()) {
            if (claimed.size() >= limit) {
                nextCursor = e.getKey();
                break;
            }
            PelEntry pel = e.getValue();
            if (pel.idleTimeMs(now) < minIdleMs) continue;
            Consumer oldConsumer = group.getConsumers().get(pel.getConsumer());
            if (oldConsumer != null) {
                oldConsumer.removePending(e.getKey());
            }
            pel.claimTo(consumerName, now);
            newConsumer.addPending(e.getKey());
            claimed.add(pel);
        }
        return new XAutoClaimResult(nextCursor, claimed);
    }

    // ==================== XINFO GROUPS / CONSUMERS ====================

    public StreamGroupsInfo xinfoGroups(String key) {
        Stream stream = getOrNull(key);
        if (stream == null) return null;
        List<StreamGroupsInfo.GroupSummary> list = new ArrayList<>();
        for (ConsumerGroup g : stream.groups.values()) {
            list.add(new StreamGroupsInfo.GroupSummary(
                    g.getName(),
                    g.consumerCount(),
                    g.pelSize(),
                    g.getLastDeliveredId()));
        }
        return new StreamGroupsInfo(list);
    }

    public StreamConsumersInfo xinfoConsumers(String key, String groupName) {
        Stream stream = getOrNull(key);
        if (stream == null) return null;
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return null;
        long now = System.currentTimeMillis();
        List<StreamConsumersInfo.ConsumerSummary> list = new ArrayList<>();
        for (Consumer c : group.getConsumers().values()) {
            long idle = Math.max(0L, now - c.getActiveTimeMs());
            list.add(new StreamConsumersInfo.ConsumerSummary(
                    c.getName(),
                    c.pendingCount(),
                    idle));
        }
        return new StreamConsumersInfo(list);
    }

    // ==================== 管理操作 ====================

    /**
     * 删除整个 stream（用于 cache.remove(key) 联动清理）。
     *
     * @return 是否删除了 stream
     */
    public boolean removeStream(String key) {
        return streams.remove(key) != null;
    }

    /**
     * 清空所有 stream（用于 cache.clear() 联动清理）。
     */
    public void clear() {
        streams.clear();
    }

    // ==================== AOF 回放专用 Raw 方法 ====================
    //
    // 这些方法供 AofService 回放使用，绕过 idleTimeMs 等运行时检查，
    // 强制恢复消费者组状态。SimpleCache 的包装层会调用这些 raw 方法，
    // 避免回放时再次写 AOF 形成循环。

    /**
     * XCLAIM 回放：强制将指定 ID 的 PEL 所有权转移给新 consumer（不做 idle 检查）。
     * <p>用于 AOF 回放恢复 XCLAIM / XAUTOCLAIM 的副作用。
     */
    public void xclaimRaw(String key, String groupName, String consumerName, List<StreamId> ids) {
        if (ids == null || ids.isEmpty()) return;
        Stream stream = getOrNull(key);
        if (stream == null) return;
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return;
        Consumer newConsumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        newConsumer.touch(now);
        for (StreamId id : ids) {
            if (id == null) continue;
            PelEntry pel = group.getPel().get(id);
            if (pel == null) continue;
            Consumer oldConsumer = group.getConsumers().get(pel.getConsumer());
            if (oldConsumer != null) {
                oldConsumer.removePending(id);
            }
            pel.claimTo(consumerName, now);
            newConsumer.addPending(id);
        }
    }

    /**
     * XDELIVER 回放：恢复一次 XREADGROUP ">" 投递产生的状态变更。
     * <p>副作用：推进 lastDeliveredId + 为每条 deliveredIds 写入 PEL + 注册到 consumer.pendingIds。
     * <p>若 PEL 已存在同 ID（重复回放），保留原投递时间不覆盖；若 consumer 不存在则自动创建。
     */
    public void xdeliverRaw(String key, String groupName, String consumerName,
                            StreamId lastDelivered, List<StreamId> deliveredIds) {
        if (consumerName == null || consumerName.isEmpty()) return;
        Stream stream = getOrCreate(key);
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return;
        Consumer consumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        consumer.touch(now);
        if (deliveredIds != null) {
            for (StreamId id : deliveredIds) {
                if (id == null) continue;
                PelEntry existed = group.getPel().get(id);
                if (existed == null) {
                    group.getPel().put(id, new PelEntry(id, consumerName, now));
                    consumer.addPending(id);
                } else {
                    // 重复回放：仅补登记 consumer.pendingIds，不覆盖原投递时间
                    consumer.addPending(id);
                }
            }
        }
        if (lastDelivered != null) {
            group.advanceLastDelivered(lastDelivered);
        }
    }

    /**
     * 当前 stream 数量。
     */
    public int streamCount() {
        return streams.size();
    }

    /**
     * AOF 回放专用：恢复单条 PEL 条目到指定消费者。
     * <p>P2-9 修复：AOF 重写时持久化了每个 consumer 的 PEL（已投递未 XACK 的消息 ID），
     * 回放时通过此方法重建 PEL 状态。与 {@link #xdeliverRaw} 类似，但不推进 lastDeliveredId。
     *
     * @param key          stream key
     * @param groupName    消费者组名
     * @param consumerName 消费者名（不存在则自动创建）
     * @param id           PEL 对应的消息 ID
     */
    public void restorePel(String key, String groupName, String consumerName, StreamId id) {
        if (key == null || groupName == null || consumerName == null || id == null) return;
        Stream stream = getOrCreate(key);
        ConsumerGroup group = stream.groups.get(groupName);
        if (group == null) return;
        Consumer consumer = group.getOrCreateConsumer(consumerName);
        long now = System.currentTimeMillis();
        consumer.touch(now);
        // putIfAbsent：不覆盖已有的 PEL 条目（幂等回放）
        PelEntry existed = group.getPel().putIfAbsent(id, new PelEntry(id, consumerName, now));
        if (existed == null) {
            consumer.addPending(id);
        }
    }

    /**
     * 列出所有 stream 的 key（不可变副本）。
     */
    public List<String> streamKeys() {
        return List.copyOf(streams.keySet());
    }

    // ==================== AOF 重写辅助方法 ====================

    /**
     * 获取指定stream的所有entries（用于AOF重写）
     */
    public Iterable<Map.Entry<StreamId, StreamEntry>> getStreamEntries(String key) {
        Stream s = streams.get(key);
        if (s == null) return Collections.emptyList();
        return s.entries.entrySet();
    }

    /**
     * 获取指定stream的所有消费者组
     */
    public Map<String, ConsumerGroup> getStreamGroups(String key) {
        Stream s = streams.get(key);
        if (s == null) return Collections.emptyMap();
        return s.groups;
    }

    /**
     * 获取指定消费者组的PEL（pending entries list）
     */
    public Map<StreamId, PelEntry> getGroupPel(String key, String groupName) {
        Stream s = streams.get(key);
        if (s == null) return Collections.emptyMap();
        ConsumerGroup g = s.groups.get(groupName);
        if (g == null) return Collections.emptyMap();
        return g.getPel();
    }

    /**
     * 获取stream长度
     */
    public long getStreamLength(String key) {
        Stream s = streams.get(key);
        if (s == null) return 0;
        return s.length.get();
    }
}
