package com.ww.runtime.membership;

import com.ww.runtime.common.NodeInfo;

/**
 * 成员视图变更事件。
 *
 * @param type 事件类型：{@link #NODE_ACTIVE} / {@link #NODE_SUSPECTED} / {@link #NODE_DEAD} / {@link #NODE_UPDATED}
 * @param node 事件发生后的最新节点信息快照
 */
public record MemberEvent(String type, NodeInfo node) {

    public static final String NODE_ACTIVE = "NODE_ACTIVE";
    public static final String NODE_SUSPECTED = "NODE_SUSPECTED";
    public static final String NODE_DEAD = "NODE_DEAD";
    public static final String NODE_UPDATED = "NODE_UPDATED";

    public MemberEvent {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("MemberEvent.type must not be null or blank");
        }
        if (node == null) {
            throw new IllegalArgumentException("MemberEvent.node must not be null");
        }
    }
}
