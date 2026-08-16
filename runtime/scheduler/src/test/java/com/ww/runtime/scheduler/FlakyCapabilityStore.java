package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** 测试用装饰器：前 N 次 list() 抛 RuntimeException，之后透传。 */
final class FlakyCapabilityStore implements CapabilityStateStore {

    private final CapabilityStateStore delegate;
    private final AtomicInteger failuresLeft;

    FlakyCapabilityStore(CapabilityStateStore delegate, int failures) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.failuresLeft = new AtomicInteger(failures);
    }

    @Override
    public Optional<CapabilityRecord> get(NodeId node, CapabilityType type) {
        return delegate.get(node, type);
    }

    @Override
    public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        delegate.setDesired(node, type, desired);
    }

    @Override
    public void setActual(NodeId node, CapabilityType type, CapabilityState actual) {
        delegate.setActual(node, type, actual);
    }

    @Override
    public List<CapabilityRecord> list() {
        if (failuresLeft.getAndDecrement() > 0) {
            throw new RuntimeException("simulated store failure");
        }
        return delegate.list();
    }
}
