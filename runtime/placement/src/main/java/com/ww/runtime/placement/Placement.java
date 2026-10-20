package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;

import java.util.Objects;

/**
 * 单个分区的一条放置决策：目标节点 + 角色 + 纪元 + 新属主可写时间。
 *
 * <p>epoch 用于放置行的乐观并发控制：全量重算（replaceAll）后为 0，
 * 单分区切主（cutover）每成功一次 +1。</p>
 *
 * <p>{@code validAfterMs} 由切主路径写入：新属主在旧属主租约过期前不得接受写入，
 * 防止本地缓存 fencing 下的新旧属主重叠写。0 表示立即生效（初始化/内存模式）。</p>
 */
public record Placement(int partition, NodeId node, PlacementRole role, long epoch,
                        long validAfterMs) {

    /** 兼容构造：无延迟生效（初始化、内存模式与既有测试）。 */
    public Placement(int partition, NodeId node, PlacementRole role, long epoch) {
        this(partition, node, role, epoch, 0L);
    }

    public Placement {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got " + partition);
        }
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(role, "role");
        if (validAfterMs < 0) {
            throw new IllegalArgumentException("validAfterMs must be >= 0, got " + validAfterMs);
        }
    }

    /** 是否为主副本。 */
    public boolean isPrimary() {
        return role == PlacementRole.PRIMARY;
    }
}
