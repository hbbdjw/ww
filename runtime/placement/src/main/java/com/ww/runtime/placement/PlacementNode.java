package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.Objects;

/**
 * 放置计算用的节点视图：节点标识 + 容量权重 + 故障域。
 *
 * @param zone 故障域（机架/可用区/主机组）；空串 = 未配置，放置不做反亲和
 */
public record PlacementNode(NodeId id, double weight, String zone) implements WeightedNode {

    public PlacementNode {
        Objects.requireNonNull(id, "id");
        if (Double.isNaN(weight) || weight <= 0.0) {
            throw new IllegalArgumentException("weight must be > 0, got " + weight);
        }
        if (zone == null) {
            zone = "";
        }
    }

    /** 兼容构造：无故障域信息。 */
    public PlacementNode(NodeId id, double weight) {
        this(id, weight, "");
    }

    public static PlacementNode of(String id, double weight) {
        return new PlacementNode(NodeId.of(id), weight);
    }

    /** 等权（weight=1.0）便捷工厂。 */
    public static PlacementNode of(String id) {
        return new PlacementNode(NodeId.of(id), 1.0);
    }

    @Override
    public String nodeKey() {
        return id.value();
    }
}
