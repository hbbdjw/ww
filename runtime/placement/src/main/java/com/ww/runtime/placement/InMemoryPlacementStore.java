package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内 {@link PlacementStore} 实现：线程安全。
 *
 * <p>每个 capability 持有一个不可变 {@link Snapshot}，replaceAll 构建完整
 * 快照后单次替换引用（原子整体替换）；读方法拿到快照引用后读取，天然一致。
 * cutover 在 {@link ConcurrentHashMap#compute} 的原子语义内完成
 * 「epoch 校验 + 节点比对 + 派生新快照」的 CAS。</p>
 */
public final class InMemoryPlacementStore implements PlacementStore {

    /** 一个 capability 的不可变放置快照（三份派生视图一并固化）。 */
    private record Snapshot(List<Placement> placements,
                            Map<Integer, Placement> byPartition,
                            Map<Integer, NodeId> primaries) {
    }

    private final ConcurrentHashMap<String, Snapshot> byCapability = new ConcurrentHashMap<>();

    @Override
    public void replaceAll(String capability, List<Placement> placements) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(placements, "placements");
        byCapability.put(capability, snapshotOf(placements));
    }

    @Override
    public List<Placement> list(String capability) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        return snapshot == null ? List.of() : snapshot.placements();
    }

    @Override
    public Optional<Placement> get(String capability, int partition) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        return snapshot == null ? Optional.empty() : Optional.ofNullable(snapshot.byPartition().get(partition));
    }

    @Override
    public Map<Integer, NodeId> primaries(String capability) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        return snapshot == null ? Map.of() : snapshot.primaries();
    }

    @Override
    public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        boolean[] success = {false};
        byCapability.compute(capability, (cap, snapshot) -> {
            if (snapshot == null) {
                return null;
            }
            Placement current = snapshot.byPartition().get(partition);
            if (current == null || current.epoch() != expectedEpoch || current.node().equals(newNode)) {
                return snapshot;
            }
            success[0] = true;
            Placement updated = new Placement(partition, newNode, current.role(), current.epoch() + 1);
            return withPlacement(snapshot, updated);
        });
        return success[0];
    }

    private static Snapshot snapshotOf(List<Placement> placements) {
        List<Placement> sorted = placements.stream()
                .sorted(Comparator.comparingInt(Placement::partition))
                .toList();
        Map<Integer, Placement> byPartition = new LinkedHashMap<>();
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        for (Placement placement : sorted) {
            Objects.requireNonNull(placement, "placement");
            if (byPartition.putIfAbsent(placement.partition(), placement) != null) {
                throw new IllegalArgumentException(
                        "duplicate placement for partition " + placement.partition());
            }
            if (placement.isPrimary()) {
                primaries.put(placement.partition(), placement.node());
            }
        }
        return new Snapshot(List.copyOf(sorted),
                Collections.unmodifiableMap(byPartition),
                Collections.unmodifiableMap(primaries));
    }

    private static Snapshot withPlacement(Snapshot snapshot, Placement updated) {
        List<Placement> replaced = new ArrayList<>(snapshot.placements().size());
        for (Placement placement : snapshot.placements()) {
            replaced.add(placement.partition() == updated.partition() ? updated : placement);
        }
        return snapshotOf(replaced);
    }
}
