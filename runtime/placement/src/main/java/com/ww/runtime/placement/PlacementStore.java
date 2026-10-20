package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 放置存储：按 capability 保存分区放置决策（v2 多行主键）。
 *
 * <p><b>v2 语义：</b>每个分区至多一个 PRIMARY 行 + 0..N 个 REPLICA 行
 * （存储主键 {@code (capability, partition_id, node_id)}）。</p>
 *
 * <p>写入语义：</p>
 * <ul>
 *   <li>{@link #replaceAll}：全量重算替换（初始化/重建），每分区多行
 *       （[0]=PRIMARY 其余 REPLICA），不保留旧纪元信息；</li>
 *   <li>{@link #cutover}：单分区切主的 CAS 更新——仅当 PRIMARY 行 epoch == expectedEpoch
 *       且 node != newNode 时 node→newNode、epoch+1（仅 PRIMARY 行生效）；</li>
 *   <li>{@link #promoteReplica}：死主副本提升 CAS——指定 REPLICA 行升 PRIMARY、
 *       原 PRIMARY 行删除、epoch+1（免迁移故障转移，数据已在副本上）；</li>
 *   <li>{@link #reassignReplicas}：重置副本集，不动主不动 epoch
 *       （副本数据由 Replicator 对账补齐，不走 MigrationEngine）。</li>
 * </ul>
 */
public interface PlacementStore {

    /**
     * 以给定放置集整体替换该 capability 的全部放置（原子替换，语义=初始化/重建）。
     *
     * <p>每分区至多一条 PRIMARY 行（重复 PRIMARY 分区视为非法输入）；
     * {@link #get} 返回该分区 PRIMARY 行。</p>
     */
    void replaceAll(String capability, List<Placement> placements);

    /** 该 capability 的全部放置（按 partition 升序、同分区内 PRIMARY 在前；无数据返回空表）。 */
    List<Placement> list(String capability);

    /** 查询指定分区的 PRIMARY 放置行。 */
    Optional<Placement> get(String capability, int partition);

    /** 查询指定分区的全部角色行（[0]=PRIMARY，其余 REPLICA；无数据返回空表）。 */
    List<Placement> placements(String capability, int partition);

    /** partition → primary 节点映射（无数据返回空 map）。 */
    Map<Integer, NodeId> primaries(String capability);

    /**
     * 单分区 CAS 切换放置节点：仅当该分区 PRIMARY 行 epoch == expectedEpoch
     * 且 node != newNode 时，将 node 替换为 newNode 并 epoch+1（不动 REPLICA 行）。
     *
     * <p><b>唯一约束：</b>{@code newNode} 已持有该分区任意行（典型为 REPLICA）时
     * 改写违反放置主键 (capability, partition_id, node_id)，实现抛异常而非返回 false；
     * 该场景的正确出路是 {@link #promoteReplica}（数据已在目标，无损免迁移）。</p>
     *
     * @return 是否成功（epoch 不符 / 节点未变 / 分区不存在均返回 false）
     */
    boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch);

    /**
     * 死主副本提升（CAS）：仅当该分区 PRIMARY 行 epoch == expectedEpoch
     * 且 {@code newNode} 持有该分区 REPLICA 行时——REPLICA 行升 PRIMARY（epoch+1）、
     * 原 PRIMARY 行删除。
     *
     * @return 是否成功（epoch 不符 / 副本行不存在 / 分区不存在均返回 false）
     */
    boolean promoteReplica(String capability, int partition, NodeId newNode, long expectedEpoch);

    /**
     * 重置指定分区的副本集：删除全部 REPLICA 行并写入 newReplicas
     * （自动去重并排除 PRIMARY 节点；不动 PRIMARY 行、不动 epoch）。
     */
    void reassignReplicas(String capability, int partition, List<NodeId> newReplicas);

    /**
     * 批量重置多个分区的副本集（S8 适度批量）：语义等于逐分区
     * {@link #reassignReplicas}；JDBC 实现按 32 分区/事务分组提交
     * （控制事务体量与行锁持有时长，失败粒度为批）。
     */
    default void reassignReplicasBatch(String capability, Map<Integer, List<NodeId>> fixes) {
        if (fixes == null || fixes.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<NodeId>> entry : fixes.entrySet()) {
            reassignReplicas(capability, entry.getKey(), entry.getValue());
        }
    }

    /**
     * 该 capability 放置数据的修订号（单调递增；无记录返回 0）。
     *
     * <p>数据面本地视图通过轮询修订号判断是否需要重载，避免每次请求查询数据库。
     * 所有写路径（replaceAll/cutover/promoteReplica/reassignReplicas*）必须推进修订号；
     * 租约续约不改变路由/纪元，不推进修订号。</p>
     */
    default long revision(String capability) {
        return 0L;
    }

    /**
     * 续约指定分区的 PRIMARY 租约（批量）。
     *
     * <p>仅当行仍由 {@code owner} 持有、epoch 匹配且 {@code valid_after_ms} 已到达时
     * 才续约；返回续约成功的 partition → leaseUntilMillis。数据面只使用返回集合判断
     * 本地租约是否仍有效，续约失败的分区应在本地租约到期后停止写入（fail-closed）。</p>
     *
     * <p>默认实现不支持租约（返回空表）：仅用于测试桩；生产实现应为 JDBC/InMemory。</p>
     */
    default Map<Integer, Long> renewLeases(String capability, List<Integer> partitions,
                                           NodeId owner, long epoch, long ttlMillis) {
        return Map.of();
    }

    /**
     * 数据库时钟（毫秒）：用于把 {@code valid_after_ms}（DB 时钟写入）换算到本机单调时间域，
     * 消除跨主机墙钟偏移对切主 fencing 的影响。不支持时返回 -1（调用方回退到墙钟）。
     */
    default long dbNowMillis() {
        return -1L;
    }

    /** 主动释放指定分区的 PRIMARY 租约（优雅停机用；默认 no-op）。 */
    default void releaseLeases(String capability, List<Integer> partitions, NodeId owner) {
    }
}
