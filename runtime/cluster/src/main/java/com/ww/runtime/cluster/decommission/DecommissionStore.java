package com.ww.runtime.cluster.decommission;

import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Optional;

/**
 * 退役记录仓库：持久化节点退役流程状态。
 *
 * <p>每个节点至多一条记录（PK = node_id）。非终态记录（DRAINING/REMOVING）
 * 由 coordinator 周期性推进；终态记录（COMPLETED/FAILED）保留供查询。</p>
 */
public interface DecommissionStore {

    /** 查询指定节点的退役记录。 */
    Optional<DecommissionRecord> get(NodeId node);

    /** 列出全部非终态记录（DRAINING + REMOVING）。 */
    List<DecommissionRecord> listActive();

    /** 插入或更新退役记录（upsert 语义）。 */
    void upsert(DecommissionRecord record);

    /** 更新状态与诊断信息（同时刷新 updated_at）。 */
    void setState(NodeId node, DecommissionState state, String detail);
}
