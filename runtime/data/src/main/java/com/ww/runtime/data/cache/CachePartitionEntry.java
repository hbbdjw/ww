package com.ww.runtime.data.cache;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CACHE 能力的分区内条目视图（迁移载体）：key + 类型标记 + 剩余 TTL + 类型化载荷。
 *
 * <p>类型按值类型判别（与 AOF 重写的 getAll*Entries 同口径）：
 * {@link com.ww.runtime.data.cache.engine.HashValue}→HASH、Deque→LIST、Set→SET、
 * SkipListMap→ZSET、其余→KV；Stream 独立 StreamStore → STREAM。
 * {@code type=json} 的 KV 持有普通 Map/List 值，按 KV 往返，不再与 HASH 混淆。</p>
 *
 * <p>载荷类型均实现 Serializable（经 {@link CacheChunkCodec} 的
 * ObjectOutputStream 通道编码）。ZSet 用 {@code (score, member)} 有序列表保序；
 * Stream 快照含 entries 与消费组（lastDeliveredId + 消费者 PEL）。</p>
 *
 * @param key                缓存键
 * @param kind               条目类型
 * @param remainingTtlMillis 剩余存活毫秒：{@code <=0} 永不过期，{@code >0} 剩余 TTL
 * @param payload            类型化载荷（KV: 值本体；HASH: Map&lt;String,Object&gt;；
 *                           LIST/SET: List&lt;Object&gt;（SET 为确定性排序后成员列表）；
 *                           ZSET: List&lt;ScoredMember&gt;；STREAM: StreamSnapshot）
 */
public record CachePartitionEntry(String key, Kind kind, long remainingTtlMillis, Object payload) {

    public CachePartitionEntry {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(kind, "kind");
    }

    /** 兼容旧构造：plain KV 条目（value 即载荷）。 */
    public CachePartitionEntry(String key, long remainingTtlMillis, Object value) {
        this(key, Kind.KV, remainingTtlMillis, value);
    }

    /** 条目类型标记。 */
    public enum Kind {
        KV, HASH, LIST, SET, ZSET, STREAM
    }

    /** ZSet 成员（score, member）：按 score 升序（同 score 按 member 序）导出。 */
    public record ScoredMember(double score, Object member) implements Serializable {
    }

    /**
     * Stream 快照（与 AofService.doRewrite 的持久化口径一致）：
     * entries（ID 升序）+ 消费组（lastDeliveredId + 消费者 PEL）。
     */
    public record StreamSnapshot(List<SnapshotEntry> entries,
                                  List<GroupSnapshot> groups) implements Serializable {
    }

    /** 单条 stream 消息。 */
    public record SnapshotEntry(long idMs, long idSeq,
                                Map<String, String> fields) implements Serializable {
    }

    /** 消费组快照：组名 + lastDeliveredId + 各消费者及其 PEL。 */
    public record GroupSnapshot(String name, long lastDeliveredMs, long lastDeliveredSeq,
                                 List<ConsumerSnapshot> consumers) implements Serializable {
    }

    /** 消费者快照：名称 + 持有的 PEL 条目 ID 列表（扁平 [ms, seq, ms, seq...]）。 */
    public record ConsumerSnapshot(String name, List<Long> pelIds) implements Serializable {
    }
}
