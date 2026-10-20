package com.ww.runtime.server.placement;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.placement.PlacementStore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link PlacementView} 的数据库直连适配（{@code runtime.route.mode=db} 的回滚路径）。
 *
 * <p>默认装配使用 {@link CachedPlacementView}：数据面读本地修订快照，写路径校验本地租约；
 * 本类保留为逐调用查询 placement 表的保守回退，便于灰度/故障回滚。</p>
 *
 * <p>行存在时透传属主节点、纪元与 validAfter；行缺失（分区尚未建立放置）返回 empty。
 * {@link #holders} 透传分区全部角色行（[0]=PRIMARY 其余 REPLICA），供写后复制取副本目标。</p>
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
                .map(row -> new Owner(row.node(), row.epoch(), row.validAfterMs()));
    }

    @Override
    public List<Owner> holders(CapabilityType cap, PartitionId partition) {
        Objects.requireNonNull(cap, "cap");
        Objects.requireNonNull(partition, "partition");
        return placementStore.placements(cap.name(), partition.value()).stream()
                .map(row -> new Owner(row.node(), row.epoch(), row.validAfterMs()))
                .toList();
    }
}
