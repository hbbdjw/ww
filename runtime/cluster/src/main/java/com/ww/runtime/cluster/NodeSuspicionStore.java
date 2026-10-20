package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;

import java.util.Map;

/**
 * 节点怀疑票存储：Phi Accrual 多级故障确认的"PFAIL→FAIL 仲裁"凭据层。
 *
 * <p>每个 observer 节点对其本地 φ 检测器判为 SUSPECTED 的 suspect 投一张票
 * （{@link #vote}，周期性重投刷新时间戳）；恢复正常则撤票（{@link #recall}）。
 * 调度器 leader 按 TTL 内的<b>不同 observer 数量</b>计票，达到法定人数后
 * 才把 suspect 提升为注册表 DEAD（多数仲裁，避免单观察者非对称分区误判
 * 引爆治愈/重平衡）。</p>
 *
 * <p>实现必须线程安全。{@code (suspect, observer)} 为票的唯一键：
 * 同一观察者重复投票只更新 φ 与时间戳，不叠加票数。</p>
 */
public interface NodeSuspicionStore {

    /**
     * 投/刷新一张怀疑票（upsert）。
     *
     * @param suspect  被怀疑节点
     * @param observer 投票观察者
     * @param phi      投票时刻的 φ 怀疑度（仅观测用）
     */
    void vote(NodeId suspect, NodeId observer, double phi);

    /** 撤回某观察者对 suspect 的票（幂等）。 */
    void recall(NodeId suspect, NodeId observer);

    /** TTL 内（{@code updatedAt >= now - ttlMillis}）不同观察者对 suspect 的新鲜票数。 */
    int freshVoteCount(NodeId suspect, long now, long ttlMillis);

    /**
     * 全部 suspect 的新鲜票数映射（TTL 内至少一张票者才出现），供 leader 周期扫描。
     */
    Map<NodeId, Integer> freshVoteCounts(long now, long ttlMillis);

    /** 删除某 suspect 的全部票（leader 判死提升后收敛/节点复活清理）。 */
    void clear(NodeId suspect);

    /** 清除 TTL 外的过期票行（防止撤票丢失/观察者静默死亡导致行无限滞留）。 */
    void pruneExpired(long now, long ttlMillis);
}
