package com.ww.rpc.core;

/**
 * RPC 请求类型：区分不同类别的请求，供路由/重定向策略使用。
 *
 * <p>通用设计：不绑定具体业务模块，各模块可通过 serviceName 进一步区分操作。
 */
public enum RequestType {
    /** 写请求（需路由到主节点/Leader）。 */
    WRITE,
    /** 读请求（可从从节点/Follower 读取，视一致性配置而定）。 */
    READ,
    /** 心跳/健康检查。 */
    HEARTBEAT,
    /** 集群管理操作（节点加入/退出/拓扑变更）。 */
    CLUSTER_ADMIN,
    /** 数据复制/同步（主→副本）。 */
    REPLICATION
}
