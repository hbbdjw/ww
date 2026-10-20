package com.ww.runtime.cluster.decommission;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link DecommissionStore} 的进程内实现：测试与单机演示用。
 */
public final class InMemoryDecommissionStore implements DecommissionStore {

    private final ConcurrentHashMap<NodeId, DecommissionRecord> records = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryDecommissionStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<DecommissionRecord> get(NodeId node) {
        Objects.requireNonNull(node, "node");
        return Optional.ofNullable(records.get(node));
    }

    @Override
    public List<DecommissionRecord> listActive() {
        return records.values().stream()
                .filter(r -> !r.state().terminal())
                .toList();
    }

    @Override
    public void upsert(DecommissionRecord record) {
        Objects.requireNonNull(record, "record");
        records.put(record.node(), record);
    }

    @Override
    public void setState(NodeId node, DecommissionState state, String detail) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(state, "state");
        records.computeIfPresent(node, (key, current) -> new DecommissionRecord(
                current.node(), state, current.force(), detail,
                current.startedAtMillis(), clock.nowMillis(),
                state.terminal() ? clock.nowMillis() : current.completedAtMillis()));
    }
}
