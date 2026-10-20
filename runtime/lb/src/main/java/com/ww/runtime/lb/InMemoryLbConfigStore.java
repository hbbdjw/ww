package com.ww.runtime.lb;

import java.util.Objects;

/**
 * 进程内 {@link LbConfigStore}（store=memory 单机演示与单测使用）。
 */
public final class InMemoryLbConfigStore implements LbConfigStore {

    private LbConfig current;

    public InMemoryLbConfigStore() {
        this(LbConfig.defaultConfig());
    }

    public InMemoryLbConfigStore(LbConfig initial) {
        this.current = Objects.requireNonNull(initial, "initial");
    }

    @Override
    public synchronized LbConfig get() {
        return current;
    }

    @Override
    public synchronized LbConfig update(LbConfig next, long expectedVersion) {
        Objects.requireNonNull(next, "next");
        if (current.version() != expectedVersion) {
            throw new LbConfigConflictException(expectedVersion, current.version());
        }
        current = next.withVersion(expectedVersion + 1);
        return current;
    }
}
