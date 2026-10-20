package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内 {@link DegradedPartitionStore}（store=memory 单机模式与单测使用）。
 *
 * <p>语义与 JDBC 实现一致：mark 幂等刷新，clear no-op 容错，
 * 查询返回不可变快照。</p>
 */
public final class InMemoryDegradedPartitionStore implements DegradedPartitionStore {

    /** capability → (partition → record)。 */
    private final Map<String, Map<Integer, DegradedPartition>> store = new ConcurrentHashMap<>();

    @Override
    public void mark(String capability, int partition, NodeId owner, String reason) {
        Objects.requireNonNull(capability, "capability");
        store.computeIfAbsent(capability, k -> new ConcurrentHashMap<>())
                .put(partition, new DegradedPartition(capability, partition, owner,
                        System.currentTimeMillis(), reason));
    }

    @Override
    public void clear(String capability, int partition) {
        Map<Integer, DegradedPartition> byCapability = store.get(capability);
        if (byCapability != null) {
            byCapability.remove(partition);
        }
    }

    @Override
    public Set<Integer> partitionsOf(String capability) {
        Map<Integer, DegradedPartition> byCapability = store.get(capability);
        return byCapability == null ? Set.of() : Set.copyOf(byCapability.keySet());
    }

    @Override
    public List<DegradedPartition> list(String capability) {
        Map<Integer, DegradedPartition> byCapability = store.get(capability);
        if (byCapability == null) {
            return List.of();
        }
        List<DegradedPartition> out = new ArrayList<>(byCapability.values());
        out.sort((a, b) -> Integer.compare(a.partition(), b.partition()));
        return List.copyOf(out);
    }

    @Override
    public List<DegradedPartition> listAll() {
        List<DegradedPartition> out = new ArrayList<>();
        for (Map<Integer, DegradedPartition> byCapability : store.values()) {
            out.addAll(byCapability.values());
        }
        out.sort((a, b) -> {
            int byCapability = a.capability().compareTo(b.capability());
            return byCapability != 0 ? byCapability : Integer.compare(a.partition(), b.partition());
        });
        return List.copyOf(out);
    }

    @Override
    public boolean isDegraded(String capability, int partition) {
        Map<Integer, DegradedPartition> byCapability = store.get(capability);
        return byCapability != null && byCapability.containsKey(partition);
    }

    @Override
    public Map<Integer, NodeId> ownersOf(String capability) {
        Map<Integer, DegradedPartition> byCapability = store.get(capability);
        if (byCapability == null) {
            return Map.of();
        }
        Map<Integer, NodeId> out = new LinkedHashMap<>();
        byCapability.forEach((partition, record) -> out.put(partition, record.owner()));
        return Map.copyOf(out);
    }
}
