package com.ww.runtime.data.cache;

import com.ww.runtime.migration.PartitionDataAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link PartitionDataAdapter} 的 CACHE 实现：委托 {@link CacheCapability}
 * 完成分区级 plain KV 的导出/导入（行格式见 {@link CacheChunkCodec}）。
 *
 * <p>导出按 key 字典序（CacheCapability.exportPartition 已排序）自
 * {@code fromCheckpoint} 起逐条切片（一条目一分片），保证断点续跑跨调用/跨重启的确定性；
 * 导入走 {@link CacheCapability#importPartition}（Map 语义天然幂等 upsert）。
 */
public final class CachePartitionAdapter implements PartitionDataAdapter {

    private final CacheCapability capability;

    public CachePartitionAdapter(CacheCapability capability) {
        this.capability = Objects.requireNonNull(capability, "capability");
    }

    @Override
    public long itemCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }

    @Override
    public List<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint) {
        if (fromCheckpoint < 0) {
            throw new IllegalArgumentException("fromCheckpoint must be >= 0, got " + fromCheckpoint);
        }
        List<CachePartitionEntry> entries = capability.exportPartition(partition);
        List<byte[]> chunks = new ArrayList<>();
        for (int i = (int) Math.min(fromCheckpoint, entries.size()); i < entries.size(); i++) {
            chunks.add(CacheChunkCodec.encodeEntry(entries.get(i)));
        }
        return chunks;
    }

    @Override
    public boolean importChunks(String capabilityName, int partition, List<byte[]> chunks) {
        Objects.requireNonNull(chunks, "chunks");
        List<CachePartitionEntry> entries = new ArrayList<>();
        for (byte[] chunk : chunks) {
            CachePartitionEntry parsed = CacheChunkCodec.decodeChunk(chunk);
            if (parsed == null) {
                return false;
            }
            entries.add(parsed);
        }
        try {
            capability.importPartition(partition, entries);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public long verifyCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }
}
