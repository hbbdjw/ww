package com.ww.runtime.data;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.ww.runtime.migration.PartitionDataAdapter;

/**
 * 多能力分区数据适配器：按 {@link PartitionDataAdapter} 各方法的 capability
 * 入参分发到具体能力的子适配器（MOCK/CACHE/STORAGE）。
 *
 * <p>MigrationEngine 与 MigrationServer 注入本组合适配器后，同一迁移引擎
 * 即可驱动所有能力的分区数据搬迁；MOCK 分支直接委托原
 * {@code MockPartitionAdapter} 实例，既有行为零变化。
 */
public final class CompositePartitionAdapter implements PartitionDataAdapter {

    private final Map<String, PartitionDataAdapter> delegates;

    public CompositePartitionAdapter(Map<String, PartitionDataAdapter> delegates) {
        Objects.requireNonNull(delegates, "delegates");
        if (delegates.isEmpty()) {
            throw new IllegalArgumentException("delegates must not be empty");
        }
        this.delegates = Map.copyOf(delegates);
    }

    @Override
    public long itemCount(String capability, int partition) {
        return delegate(capability).itemCount(capability, partition);
    }

    @Override
    public List<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
        return delegate(capability).exportChunks(capability, partition, fromCheckpoint);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
        return delegate(capability).importChunks(capability, partition, chunks);
    }

    @Override
    public long verifyCount(String capability, int partition) {
        return delegate(capability).verifyCount(capability, partition);
    }

    private PartitionDataAdapter delegate(String capability) {
        Objects.requireNonNull(capability, "capability");
        PartitionDataAdapter adapter = delegates.get(capability.toUpperCase(Locale.ROOT));
        if (adapter == null) {
            throw new com.ww.runtime.common.MigrationException(
                    "no partition data adapter registered for capability: " + capability);
        }
        return adapter;
    }
}
