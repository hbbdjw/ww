package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.Objects;

/**
 * 分区隔离记录：属主（死亡节点）无存活副本时，治愈宽限期赌输后标记，
 * 放置表保持指向属主（数据留盘），数据面拒绝服务直至属主回归或人工 force-adopt。
 *
 * @param capability 能力名（与放置表同口径，如 STORAGE/CACHE/MOCK）
 * @param partition  分区号
 * @param owner      隔离时该分区的 PRIMARY 持有者（死亡节点）
 * @param sinceMs    标记时刻（epoch millis）
 * @param reason     隔离原因（可读文本，管理面展示）
 */
public record DegradedPartition(String capability, int partition, NodeId owner,
                                long sinceMs, String reason) {

    public DegradedPartition {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(owner, "owner");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got: " + partition);
        }
        if (sinceMs < 0) {
            throw new IllegalArgumentException("sinceMs must be >= 0, got: " + sinceMs);
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be blank");
        }
    }
}
