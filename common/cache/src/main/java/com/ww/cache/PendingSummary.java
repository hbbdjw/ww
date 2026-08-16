package com.ww.cache;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * XPENDING 返回结果。
 *
 * <p>分两种形态：
 * <ul>
 *   <li>概要形态（无 consumer 参数）：返回 [total, firstId, lastId, consumers]</li>
 *   <li>详细形态（带 consumer + start/end/count）：返回 PelEntry 列表</li>
 * </ul>
 * 当前合并到一个对象，由 fields 是否为 null 区分。
 */
public final class PendingSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long total;
    private final StreamId firstId;
    private final StreamId lastId;
    /** 每个消费者的 pending 计数；概要形态下使用。 */
    private final Map<String, Long> consumerCounts;
    /** 详细形态下的 PEL 条目列表；概要形态为 null。 */
    private final List<PelEntry> entries;

    /** 概要形态构造。 */
    public PendingSummary(long total, StreamId firstId, StreamId lastId, Map<String, Long> consumerCounts) {
        this(total, firstId, lastId, consumerCounts, null);
    }

    /** 详细形态构造。 */
    public PendingSummary(long total, List<PelEntry> entries) {
        this(total, null, null, null, entries);
    }

    private PendingSummary(long total,
                           StreamId firstId,
                           StreamId lastId,
                           Map<String, Long> consumerCounts,
                           List<PelEntry> entries) {
        this.total = total;
        this.firstId = firstId;
        this.lastId = lastId;
        this.consumerCounts = consumerCounts;
        this.entries = entries;
    }

    public long getTotal() { return total; }
    public StreamId getFirstId() { return firstId; }
    public StreamId getLastId() { return lastId; }
    public Map<String, Long> getConsumerCounts() { return consumerCounts; }
    public List<PelEntry> getEntries() { return entries; }
    public boolean isDetailed() { return entries != null; }

    @Override
    public String toString() {
        return "PendingSummary{total=" + total
                + ", firstId=" + firstId
                + ", lastId=" + lastId
                + ", consumers=" + consumerCounts
                + ", entries=" + (entries != null ? entries.size() : "null") + '}';
    }
}
