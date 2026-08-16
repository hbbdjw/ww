package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link CapabilityStateStore} 的进程内实现：测试与单机演示用。
 * 通过 {@code store.get(...).isEmpty()} 判断记录是否已创建；
 * 首次 setDesired/setActual 时另一侧状态取默认 DISABLED。
 */
public final class InMemoryCapabilityStateStore implements CapabilityStateStore {

    private record Key(NodeId node, CapabilityType type) {
    }

    private final Clock clock;
    private final Map<Key, CapabilityRecord> records = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public InMemoryCapabilityStateStore(Clock clock) {
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<CapabilityRecord> get(NodeId node, CapabilityType type) {
        require(node, type);
        return Optional.ofNullable(records.get(new Key(node, type)));
    }

    @Override
    public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        require(node, type, desired);
        mutate(node, type, record -> new CapabilityRecord(
                record.node(), record.type(), desired, record.actual(), clock.nowMillis()));
    }

    @Override
    public void setActual(NodeId node, CapabilityType type, CapabilityState actual) {
        require(node, type, actual);
        mutate(node, type, record -> new CapabilityRecord(
                record.node(), record.type(), record.desired(), actual, clock.nowMillis()));
    }

    @Override
    public List<CapabilityRecord> list() {
        return records.values().stream()
                .sorted(Comparator.comparing(CapabilityRecord::node, Comparator.comparing(NodeId::value))
                        .thenComparing(CapabilityRecord::type, Comparator.comparing(Enum::name)))
                .toList();
    }

    private void mutate(NodeId node, CapabilityType type,
                        java.util.function.UnaryOperator<CapabilityRecord> updater) {
        lock.lock();
        try {
            Key key = new Key(node, type);
            CapabilityRecord current = records.get(key);
            if (current == null) {
                current = new CapabilityRecord(node, type,
                        CapabilityState.DISABLED, CapabilityState.DISABLED, clock.nowMillis());
            }
            records.put(key, updater.apply(current));
        } finally {
            lock.unlock();
        }
    }

    private static void require(NodeId node, CapabilityType type) {
        java.util.Objects.requireNonNull(node, "node");
        java.util.Objects.requireNonNull(type, "type");
    }

    private static void require(NodeId node, CapabilityType type, CapabilityState state) {
        require(node, type);
        java.util.Objects.requireNonNull(state, "state");
    }
}
