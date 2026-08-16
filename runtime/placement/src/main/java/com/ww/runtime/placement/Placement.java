package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;

import java.util.Objects;

/**
 * 单个分区的一条放置决策：目标节点 + 角色 + 纪元。
 *
 * <p>epoch 用于放置行的乐观并发控制：全量重算（replaceAll）后为 0，
 * 单分区切主（cutover）每成功一次 +1。</p>
 */
public record Placement(int partition, NodeId node, PlacementRole role, long epoch) {

    public Placement {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got " + partition);
        }
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(role, "role");
    }

    /** 是否为主副本。 */
    public boolean isPrimary() {
        return role == PlacementRole.PRIMARY;
    }
}
