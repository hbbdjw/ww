package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;

import java.util.Optional;

/**
 * 放置视图端口（防依赖倒置）：能力模块只依赖本接口获取"某能力某分区当前归谁"，
 * 而不直接依赖 placement 模块；宿主在组装时用 placement 的实际实现适配本端口。
 *
 * <p>典型用途：fencing 校验。能力模块在写入前调用 {@link #owner}，
 * 校验（1）该分区已有 owner；（2）owner 是本节点；（3）纪元与请求一致，
 * 任一不满足即拒绝写入（参见 {@link MockCapability}）。
 *
 * <p>适配示例（宿主侧）：
 * <pre>{@code
 * PlacementView view = (cap, partition) -> placementEngine
 *         .lookupOwner(cap, partition)
 *         .map(a -> new PlacementView.Owner(a.nodeId(), a.epoch()));
 * }</pre>
 */
public interface PlacementView {

    /**
     * 查询指定能力在指定分区上的当前属主。
     *
     * @param cap       能力类型
     * @param partition 分区
     * @return 属主（含纪元）；分区尚无属主时返回 {@link Optional#empty()}
     */
    Optional<Owner> owner(CapabilityType cap, PartitionId partition);

    /**
     * 分区属主：节点 + 纪元。
     *
     * @param node  属主节点
     * @param epoch 属主生效纪元（单调递增，用于 fencing 旧写）
     */
    record Owner(NodeId node, long epoch) {

        public Owner {
            if (node == null) {
                throw new IllegalArgumentException("Owner.node must not be null");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("Owner.epoch must be >= 0, got " + epoch);
            }
        }
    }
}
