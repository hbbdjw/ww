package com.ww.rpc.cluster;

/**
 * 拓扑变更监听器：成员表发生变更（节点加入/退出/状态变更）时回调。
 *
 * <p>各业务模块（cache/storage）可实现此接口，在拓扑变更时
 * 触发 Rebalance、Failover、路由重建等操作。
 */
@FunctionalInterface
public interface TopologyChangeListener {

    /**
     * 拓扑变更事件类型。
     */
    enum EventType {
        /** 新节点加入 */
        NODE_ADDED,
        /** 节点退出 */
        NODE_REMOVED,
        /** 节点状态变更（ALIVE↔SUSPECT↔DEAD） */
        STATUS_CHANGED,
        /** 节点权重变更 */
        WEIGHT_CHANGED
    }

    /**
     * 拓扑变更回调。
     *
     * @param eventType 事件类型
     * @param member    变更的成员
     * @param table     当前成员表快照
     */
    void onTopologyChange(EventType eventType, ClusterMember member, MembershipTable table);
}
