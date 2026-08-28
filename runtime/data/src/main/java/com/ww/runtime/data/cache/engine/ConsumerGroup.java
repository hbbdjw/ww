package com.ww.runtime.data.cache.engine;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 消费者组：管理 lastDeliveredId、PEL 与消费者集合。
 *
 * <p>对应 Redis XGROUP 的核心状态。线程安全：pel 与 consumers 用并发集合，
 * lastDeliveredId 用 volatile + synchronized 更新（XREADGROUP 时同步推进）。
 *
 * <p>持久化：group 状态经 AOF 重写持久化（XGROUP_CREATE / XGROUP_CREATECONSUMER / XPEL_ENTRY），
 * 运行期投递与确认通过 XDELIVER / XCLAIM / XACK 增量记录，重启回放后可完整恢复。
 */
public final class ConsumerGroup implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    /** 最近一次投递的 ID；初始为创建时指定的 startId（{@link StreamId#MIN} 或流当前尾 ID）。 */
    private volatile StreamId lastDeliveredId;
    /** Pending Entries List：已投递未确认的消息，按 StreamId 排序。 */
    private final ConcurrentSkipListMap<StreamId, PelEntry> pel = new ConcurrentSkipListMap<>();
    /** 消费者集合，按 name 索引。 */
    private final ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    public ConsumerGroup(String name, StreamId lastDeliveredId) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("ConsumerGroup name 不能为空");
        }
        this.name = name;
        this.lastDeliveredId = lastDeliveredId != null ? lastDeliveredId : StreamId.MIN;
    }

    public String getName() {
        return name;
    }

    public StreamId getLastDeliveredId() {
        return lastDeliveredId;
    }

    public ConcurrentSkipListMap<StreamId, PelEntry> getPel() {
        return pel;
    }

    public ConcurrentHashMap<String, Consumer> getConsumers() {
        return consumers;
    }

    public Consumer getOrCreateConsumer(String consumerName) {
        return consumers.computeIfAbsent(consumerName, Consumer::new);
    }

    public Consumer getConsumer(String consumerName) {
        return consumers.get(consumerName);
    }

    /** 投递成功后推进 lastDeliveredId（XREADGROUP ">" 与 XDELIVER 回放均调用；要求新 ID 严格大于当前值）。 */
    public synchronized void advanceLastDelivered(StreamId id) {
        if (lastDeliveredId == null || id.compareTo(lastDeliveredId) > 0) {
            lastDeliveredId = id;
        }
    }

    public int consumerCount() {
        return consumers.size();
    }

    public int pelSize() {
        return pel.size();
    }
}
