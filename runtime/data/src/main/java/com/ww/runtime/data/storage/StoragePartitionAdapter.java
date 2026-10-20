package com.ww.runtime.data.storage;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.migration.PartitionDataAdapter;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.IntPredicate;

/**
 * {@link PartitionDataAdapter} 的 STORAGE 实现：委托 {@link StorageCapability}
 * 完成分区级对象迁移。
 *
 * <p>checkpoint 语义 = 已完成迁移的对象数（{@link StorageChunkCodec} 仅 fin 片
 * 带终结符，{@code ChunkCodec.countEntries} 统计的即完整对象数），
 * 断点续跑自对象边界恢复，聚合不残缺。
 *
 * <p><b>可选扩展</b>：覆写 {@code clearPartition}（迁移完成后源端清理）与
 * {@code retainPartitions}（启动清扫）；{@code compact} 保持 default no-op——
 * 存储为对象即文件，{@code ObjectStore.delete} 即时删文件并清理空目录，
 * 无 AOF 类派生持久化结构可压实。
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
    public Iterator<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint) {
        return capability.exportPartition(partition, fromCheckpoint);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint,
                                         String cursor) {
        return capability.exportPartition(partition, fromCheckpoint,
                cursor == null || cursor.isBlank() ? null : cursor);
    }

    @Override
    public String cursorOf(String capabilityName, byte[] piece) {
        return StorageChunkCodec.cursorOf(piece);
    }

    @Override
    public boolean supportsCursorResume() {
        return true;
    }

    @Override
    public boolean freezePartition(String capabilityName, int partition, long ttlMillis) {
        capability.freezePartition(partition, ttlMillis);
        return true;
    }

    @Override
    public boolean importChunks(String capabilityName, int partition, List<byte[]> chunks) {
        return capability.importChunks(chunks);
    }

    @Override
    public long verifyCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }

    /** 迁移完成后清理源端分区对象（委托 {@link StorageCapability#clearPartition}）。 */
    @Override
    public boolean clearPartition(String capabilityName, int partition, NodeId source) {
        capability.clearPartition(partition);
        return true;
    }

    /** 启动清扫（委托 {@link StorageCapability#retainPartitions}）。 */
    @Override
    public long retainPartitions(String capabilityName, IntPredicate keep) {
        return capability.retainPartitions(keep);
    }
}
