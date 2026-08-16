package com.ww.rpc.cluster;

/**
 * 节点存活状态。
 */
public enum MemberStatus {
    /** 健康：心跳正常，可路由 */
    ALIVE,
    /** 疑似下线：心跳超时，等待更多证据 */
    SUSPECT,
    /** 已下线：被判定为永久故障，排除出路由 */
    DEAD,
    /** 未知：启动初期或刚加入 */
    UNKNOWN
}
