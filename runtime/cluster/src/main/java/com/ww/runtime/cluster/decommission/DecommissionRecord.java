package com.ww.runtime.cluster.decommission;

import com.ww.runtime.common.NodeId;

/**
 * 退役记录值对象。
 *
 * @param node              退役目标节点
 * @param state             退役状态
 * @param force             是否强制退役（最后节点有数据时丢弃数据）
 * @param detail            诊断信息（FAILED 时记录原因）
 * @param startedAtMillis   退役开始时间
 * @param updatedAtMillis   最后更新时间
 * @param completedAtMillis 完成时间（终态时设置）
 */
public record DecommissionRecord(
        NodeId node,
        DecommissionState state,
        boolean force,
        String detail,
        long startedAtMillis,
        long updatedAtMillis,
        Long completedAtMillis) {
}
