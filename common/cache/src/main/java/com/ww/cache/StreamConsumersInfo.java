package com.ww.cache;

import java.io.Serializable;
import java.util.List;

/**
 * XINFO CONSUMERS 返回的消费者列表快照。
 *
 * <p>每个消费者含：name / pending / idle（毫秒）。
 */
public final class StreamConsumersInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ConsumerSummary> consumers;

    public StreamConsumersInfo(List<ConsumerSummary> consumers) {
        this.consumers = consumers;
    }

    public List<ConsumerSummary> getConsumers() {
        return consumers;
    }

    public static final class ConsumerSummary implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int pending;
        private final long idleMs;

        public ConsumerSummary(String name, int pending, long idleMs) {
            this.name = name;
            this.pending = pending;
            this.idleMs = idleMs;
        }

        public String getName() { return name; }
        public int getPending() { return pending; }
        public long getIdleMs() { return idleMs; }

        @Override
        public String toString() {
            return "ConsumerSummary{name=" + name
                    + ", pending=" + pending
                    + ", idleMs=" + idleMs + '}';
        }
    }
}
