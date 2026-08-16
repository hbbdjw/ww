package com.ww.runtime.placement;

/**
 * 加权哈希环中的节点抽象：稳定 nodeKey + 容量权重。
 *
 * <p>与具体注册表实现解耦——runtime-placement 的 {@link PlacementNode}
 * 以及宿主侧任意持有 id/weight 的节点模型均可适配。</p>
 */
public interface WeightedNode {

    /** 参与 hash 输入的节点唯一键（如 node id 字符串）。 */
    String nodeKey();

    /** 容量权重，越大期望分到的分区越多；约定 &gt; 0。 */
    double weight();
}
