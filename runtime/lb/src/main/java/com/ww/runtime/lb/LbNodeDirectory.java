package com.ww.runtime.lb;

import java.util.List;

/**
 * 节点目录端口：返回 LB 视角的集群节点快照。
 *
 * <p>宿主侧（runtime-server）适配 NodeRegistry：仅 ACTIVE 节点标记 active=true。
 * runtime-lb 不主动发起任何健康探测，判活完全依据注册表状态与心跳机制。</p>
 */
@FunctionalInterface
public interface LbNodeDirectory {

    /** 当前节点视图（调用方应使用定时刷新的缓存快照，避免代理热路径直查库）。 */
    List<LbNodeView> snapshot();
}
