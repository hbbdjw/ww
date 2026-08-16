package com.ww.runtime.scheduler;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;

/**
 * 节点命令通道端口：调度器向<b>任意节点</b>下发能力生命周期命令的统一入口。
 *
 * <p>本模块提供两类实现，宿主可组合使用：</p>
 * <ul>
 *   <li>{@link LocalNodeCommandChannel}：进程内委托本节点 {@code CapabilityManager}，
 *       仅当目标节点 == self 时生效；</li>
 *   <li>{@link GrpcNodeCommandChannel}：经 ClusterControl gRPC 远程下发（host:grpcPort
 *       通道缓存 + 截止时间），node==self 时自动回退本地通道。</li>
 * </ul>
 *
 * <p>实现约定：命令失败（节点不可达/状态机拒绝/超时）一律返回 {@code false}，
 * 不抛异常——调度器据此在下个调谐周期重试（幂等收敛）。</p>
 */
public interface NodeCommandChannel {

    /**
     * 启动节点上的能力（远端语义：同步走完 DISABLED→STARTING→WARMING→ACTIVE）。
     *
     * @return 命令是否成功
     */
    boolean startCapability(NodeId node, CapabilityType type);

    /**
     * 停止节点上的能力（实现内部负责 drain 链：ACTIVE→DRAINING→STOPPING→DISABLED）。
     *
     * @return 命令是否成功
     */
    boolean stopCapability(NodeId node, CapabilityType type);

    /**
     * 冻结/解冻节点上的能力（维护开关，不参与状态机跃迁；迁移前冻结以停新写）。
     *
     * @return 命令是否成功
     */
    boolean freeze(NodeId node, CapabilityType type, boolean frozen);
}
