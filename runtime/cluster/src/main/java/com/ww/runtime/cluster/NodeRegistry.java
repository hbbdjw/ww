package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;

import java.util.List;
import java.util.Optional;

/**
 * 节点注册表：集群视角的节点目录（成员、生命周期状态与资源视图）。
 *
 * <ul>
 *   <li>{@link #register(NodeInfo)}：upsert 语义。无论节点此前处于何状态，
 *       一律重置为 {@code JOINING} 且 {@code state_epoch} 清零（节点重启恢复语义：
 *       同一 NodeId 再次 register 后状态机可重新推进）。</li>
 *   <li>{@link #heartbeat(NodeId, ResourceSnapshot)}：刷新心跳时间与资源快照，
 *       心跳时刻取 {@code snapshot.updatedAtMillis()}。未知节点视为参数错误。</li>
 *   <li>{@link #markState(NodeId, NodeState)}：按 {@link NodeStateMachine} 校验合法跃迁，
 *       通过后 {@code state_epoch} + 1。非法跃迁抛 {@code IllegalStateTransitionException}。</li>
 *   <li>{@link #tombstone(NodeId)}：置 {@code DEAD} 并保留行（幂等，已 DEAD 则不变化）。</li>
 * </ul>
 *
 * <p>实现必须线程安全。</p>
 */
public interface NodeRegistry {

    /** 注册/重注册节点（upsert）：强制 state=JOINING、state_epoch 重置为 0。 */
    void register(NodeInfo info);

    /** 刷新节点心跳与资源快照。节点不存在时抛出 IllegalArgumentException。 */
    void heartbeat(NodeId id, ResourceSnapshot snapshot);

    /** 推进节点状态：校验合法跃迁并使 state_epoch + 1，非法跃迁抛 IllegalStateTransitionException。 */
    void markState(NodeId id, NodeState newState);

    /** 查询单个节点。 */
    Optional<NodeInfo> get(NodeId id);

    /** 存活节点视图：过滤掉 DEAD 与 LEAVING。 */
    List<NodeInfo> listAlive();

    /** 全量节点视图（含 DEAD 墓碑行）。 */
    List<NodeInfo> listAll();

    /** 置 DEAD 并保留行（幂等）。 */
    void tombstone(NodeId id);
}
