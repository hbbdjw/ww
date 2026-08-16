package com.ww.cache;

import java.io.Serializable;

/**
 * Pending Entries List 单条记录：表示一条已投递但尚未 XACK 的消息。
 *
 * <p>对应 Redis Stream PEL：记录投递给哪个消费者、何时投递、投递次数。
 * 用于 XCLAIM / XAUTOCLAIM 故障恢复：超过 minIdleTime 的消息可被其他消费者认领。
 *
 * <p>不可变字段：id；可变字段：consumer / deliveredTimeMs / deliveryCount（XCLAIM 时更新）。
 */
public final class PelEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StreamId id;
    private volatile String consumer;
    private volatile long deliveredTimeMs;
    private volatile int deliveryCount;

    public PelEntry(StreamId id, String consumer, long deliveredTimeMs) {
        if (id == null) throw new IllegalArgumentException("PelEntry id 不能为空");
        this.id = id;
        this.consumer = consumer;
        this.deliveredTimeMs = deliveredTimeMs;
        this.deliveryCount = 1;
    }

    public StreamId getId() {
        return id;
    }

    public String getConsumer() {
        return consumer;
    }

    public long getDeliveredTimeMs() {
        return deliveredTimeMs;
    }

    public int getDeliveryCount() {
        return deliveryCount;
    }

    /** 计算空闲时间（毫秒）。 */
    public long idleTimeMs(long nowMs) {
        return Math.max(0L, nowMs - deliveredTimeMs);
    }

    /** XCLAIM 时更新消费者与投递时间，deliveryCount+1。 */
    public synchronized void claimTo(String newConsumer, long nowMs) {
        this.consumer = newConsumer;
        this.deliveredTimeMs = nowMs;
        this.deliveryCount++;
    }

    @Override
    public String toString() {
        return "PelEntry{id=" + id
                + ", consumer=" + consumer
                + ", delivered=" + deliveredTimeMs
                + ", count=" + deliveryCount + '}';
    }
}
