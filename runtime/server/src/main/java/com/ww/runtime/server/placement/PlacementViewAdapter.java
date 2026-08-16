package com.ww.runtime.server.placement;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.placement.PlacementStore;

import java.util.Objects;
import java.util.Optional;

/**
 * {@link PlacementView} 的宿主适配：把 placement 模块的 {@link PlacementStore}
 * 适配为 capability 模块期望的「某能力某分区当前归谁」端口。
 *
 * <p>行存在时透传属主节点与纪元（fencing 语义）；行缺失（分区尚未建立放置）返回 empty。</p>
 */
public final class PlacementViewAdapter implements PlacementView {

    private final PlacementStore placementStore;

    public PlacementViewAdapter(PlacementStore placementStore) {
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
    }

    @Override
    public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
        Objects.requireNonNull(cap, "cap");
        Objects.requireNonNull(partition, "partition");
        return placementStore.get(cap.name(), partition.value())
                .map(row -> new Owner(row.node(), row.epoch()));
    }
}
