package com.ww.cache;

import java.io.Serializable;
import java.util.List;

/**
 * XINFO GROUPS 返回的消费者组列表快照。
 *
 * <p>每个组含：name / consumers / pending / last-delivered-id。
 */
public final class StreamGroupsInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<GroupSummary> groups;

    public StreamGroupsInfo(List<GroupSummary> groups) {
        this.groups = groups;
    }

    public List<GroupSummary> getGroups() {
        return groups;
    }

    /** 单个消费者组的摘要信息。 */
    public static final class GroupSummary implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int consumers;
        private final int pending;
        private final StreamId lastDeliveredId;

        public GroupSummary(String name, int consumers, int pending, StreamId lastDeliveredId) {
            this.name = name;
            this.consumers = consumers;
            this.pending = pending;
            this.lastDeliveredId = lastDeliveredId;
        }

        public String getName() { return name; }
        public int getConsumers() { return consumers; }
        public int getPending() { return pending; }
        public StreamId getLastDeliveredId() { return lastDeliveredId; }

        @Override
        public String toString() {
            return "GroupSummary{name=" + name
                    + ", consumers=" + consumers
                    + ", pending=" + pending
                    + ", lastDeliveredId=" + lastDeliveredId + '}';
        }
    }
}
