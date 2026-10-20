package com.ww.runtime.data;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntPredicate;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.migration.PartitionDataAdapter;

/**
 * 多能力分区数据适配器：按 {@link PartitionDataAdapter} 各方法的 capability
 * 入参分发到具体能力的子适配器（MOCK/CACHE/STORAGE）。
 *
 * <p>MigrationEngine 与 MigrationServer 注入本组合适配器后，同一迁移引擎
 * 即可驱动所有能力的分区数据搬迁；MOCK 分支直接委托原
 * {@code MockPartitionAdapter} 实例，既有行为零变化。</p>
 *
 * <p><b>能力扩展</b>：新能力（如后续 STORAGE 的源端清理/启动清扫）只需在其
 * PartitionDataAdapter 实现中覆写 {@code clearPartition/retainPartitions/compact}，
 * 本组合器与调度/迁移/清扫框架无需任何改动。</p>
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

    /** 已注册能力集合（启动清扫器遍历用）。 */
    public Set<String> capabilities() {
        return delegates.keySet();
    }

    @Override
    public long itemCount(String capability, int partition) {
        return delegate(capability).itemCount(capability, partition);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
        return delegate(capability).exportChunks(capability, partition, fromCheckpoint);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                         String cursor) {
        return delegate(capability).exportChunks(capability, partition, fromCheckpoint, cursor);
    }

    @Override
    public String cursorOf(String capability, byte[] piece) {
        return delegate(capability).cursorOf(capability, piece);
    }

    @Override
    public boolean supportsCursorResume() {
        return delegates.values().stream().anyMatch(PartitionDataAdapter::supportsCursorResume);
    }

    @Override
    public boolean freezePartition(String capability, int partition, long ttlMillis) {
        return delegate(capability).freezePartition(capability, partition, ttlMillis);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
        return delegate(capability).importChunks(capability, partition, chunks);
    }

    @Override
    public long verifyCount(String capability, int partition) {
        return delegate(capability).verifyCount(capability, partition);
    }

    @Override
    public boolean clearPartition(String capability, int partition, NodeId source) {
        return delegate(capability).clearPartition(capability, partition, source);
    }

    @Override
    public long retainPartitions(String capability, IntPredicate keep) {
        return delegate(capability).retainPartitions(capability, keep);
    }

    @Override
    public void compact(String capability) {
        delegate(capability).compact(capability);
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
