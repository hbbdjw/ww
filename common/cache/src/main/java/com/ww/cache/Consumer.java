package com.ww.cache;

import java.io.Serializable;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 消费者组中的单个消费者状态。
 *
 * <p>对应 Redis XINFO CONSUMERS 输出：name / pending / idle。
 *
 * <p>线程安全：pendingIds 使用 {@link ConcurrentSkipListSet} 支持并发 XACK / XCLAIM。
 * seenTimeMs / activeTimeMs 用 volatile 保证可见性。
 */
public final class Consumer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    /** 创建时间。 */
    private final long seenTimeMs;
    /** 最近一次活动时间（XREADGROUP / XCLAIM / XACK），用于 XINFO idle 计算。 */
    private volatile long activeTimeMs;
    /** 该消费者持有的 PEL ID 集合（与所属 group 的 pel 共享 ID 空间）。 */
    private final ConcurrentSkipListSet<StreamId> pendingIds = new ConcurrentSkipListSet<>();

    public Consumer(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Consumer name 不能为空");
        }
        this.name = name;
        this.seenTimeMs = System.currentTimeMillis();
        this.activeTimeMs = this.seenTimeMs;
    }

    public String getName() {
        return name;
    }

    public long getSeenTimeMs() {
        return seenTimeMs;
    }

    public long getActiveTimeMs() {
        return activeTimeMs;
    }

    public void touch(long nowMs) {
        this.activeTimeMs = nowMs;
    }

    public int pendingCount() {
        return pendingIds.size();
    }

    public boolean addPending(StreamId id) {
        return pendingIds.add(id);
    }

    public boolean removePending(StreamId id) {
        return pendingIds.remove(id);
    }

    public java.util.NavigableSet<StreamId> pendingIdsView() {
        return pendingIds;
    }

    @Override
    public String toString() {
        return "Consumer{name=" + name + ", pending=" + pendingIds.size() + '}';
    }
}
