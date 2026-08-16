package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 放置存储：按 capability 保存分区放置决策。
 *
 * <p>两类写入语义：</p>
 * <ul>
 *   <li>{@link #replaceAll}：全量重算替换（初始化/重建），不保留旧纪元信息；</li>
 *   <li>{@link #cutover}：单分区切主的 CAS 更新——仅当当前 epoch == expectedEpoch
 *       且 node != newNode 时 node→newNode、epoch+1，返回是否成功。</li>
 * </ul>
 */
public interface PlacementStore {

    /**
     * 以给定放置集整体替换该 capability 的全部放置（原子替换，语义=初始化/重建）。
     *
     * <p>受存储主键 (capability, partition_id) 约束，每个分区至多一条放置
     * （重复分区视为非法输入）；{@link #get} 返回该分区唯一行。</p>
     */
    void replaceAll(String capability, List<Placement> placements);

    /** 该 capability 的全部放置（按 partition 升序；无数据返回空表）。 */
    List<Placement> list(String capability);

    /** 查询指定分区的任意角色放置行（primary 或 replica）。 */
    Optional<Placement> get(String capability, int partition);

    /** partition → primary 节点映射（无数据返回空 map）。 */
    Map<Integer, NodeId> primaries(String capability);

    /**
     * 单分区 CAS 切换放置节点：仅当该分区当前放置 epoch == expectedEpoch
     * 且 node != newNode 时，将 node 替换为 newNode 并 epoch+1。
     *
     * @return 是否成功（epoch 不符 / 节点未变 / 分区不存在均返回 false）
     */
    boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch);
}
