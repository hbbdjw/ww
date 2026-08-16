package com.ww.runtime.data.storage;

import com.ww.runtime.migration.PartitionDataAdapter;

import java.util.List;
import java.util.Objects;

/**
 * {@link PartitionDataAdapter} 的 STORAGE 实现：委托 {@link StorageCapability}
 * 完成分区级对象迁移。
 *
 * <p>checkpoint 语义 = 已完成迁移的对象数（{@link StorageChunkCodec} 仅 fin 片
 * 带终结符，{@code ChunkCodec.countEntries} 统计的即完整对象数），
 * 断点续跑自对象边界恢复，聚合不残缺。
 */
public final class StoragePartitionAdapter implements PartitionDataAdapter {

    private final StorageCapability capability;

    public StoragePartitionAdapter(StorageCapability capability) {
        this.capability = Objects.requireNonNull(capability, "capability");
    }

    @Override
    public long itemCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }

    @Override
    public List<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint) {
        return capability.exportPartition(partition, fromCheckpoint);
    }

    @Override
    public boolean importChunks(String capabilityName, int partition, List<byte[]> chunks) {
        return capability.importChunks(chunks);
    }

    @Override
    public long verifyCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }
}
