package com.ww.runtime.data.cache;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.migration.PartitionDataAdapter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.IntPredicate;

/**
 * {@link PartitionDataAdapter} 的 CACHE 实现：委托 {@link CacheCapability}
 * 完成分区级全类型（KV/Hash/List/Set/ZSet/Stream）的导出/导入/清理/清扫
 * （行格式见 {@link CacheChunkCodec}）。
 *
 * <p>导出按 (key, kind) 排序自 {@code fromCheckpoint} 起逐条切片（一条目一分片），
 * 保证断点续跑跨调用/跨重启的确定性；导入走
 * {@link CacheCapability#importPartition}（整 key 替换，天然幂等）。</p>
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
    public Iterator<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint) {
        return exportChunks(capabilityName, partition, fromCheckpoint, (String) null);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint,
                                         String cursor) {
        if (fromCheckpoint < 0) {
            throw new IllegalArgumentException("fromCheckpoint must be >= 0, got " + fromCheckpoint);
        }
        if (cursor != null && cursor.isBlank()) {
            cursor = null;
        }
        List<CachePartitionEntry> entries = capability.exportPartition(partition);
        // 游标存在时以游标为准（位置断点可能已因并发插入/删除而错位，不能再叠加跳过）
        int start = cursor != null
                ? 0
                : (int) Math.min(fromCheckpoint, entries.size());
        if (cursor != null) {
            while (start < entries.size() && !CacheChunkCodec.isAfterCursor(entries.get(start), cursor)) {
                start++;
            }
        }
        int first = start;
        // 惰性逐条编码：条目在迭代取用时才编码为分片，避免将导出内容整体物化为堆副本
        return new Iterator<>() {
            private int index = first;

            @Override
            public boolean hasNext() {
                return index < entries.size();
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return CacheChunkCodec.encodeEntry(entries.get(index++));
            }
        };
    }

    @Override
    public String cursorOf(String capabilityName, byte[] piece) {
        return CacheChunkCodec.cursorOf(piece);
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
            if (isCapabilityPrecondition(e)) {
                // 启动/冻结窗口（能力未 start / frozen）：原样放行保留 cause，
                // MigrationServer 据此映射 FAILED_PRECONDITION，引擎按「端点未就绪，稍后重试」
                // 降级（INFO + 断点保留），而非吞成 false 触发无意义的 3×3 重发与 WARN 堆栈
                throw e;
            }
            return false;
        }
    }

    /** 能力未就绪类失败（启动/冻结窗口）判定：沿异常链 message 识别，与 MigrationServer 同口径。 */
    private static boolean isCapabilityPrecondition(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && (m.contains("capability not started") || m.contains("frozen"))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long verifyCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }

    /** 迁移完成后清理源端分区数据（委托 {@link CacheCapability#clearPartition}）。 */
    @Override
    public boolean clearPartition(String capabilityName, int partition, NodeId source) {
        capability.clearPartition(partition);
        return true;
    }

    /** 启动清扫（委托 {@link CacheCapability#retainPartitions}）。 */
    @Override
    public long retainPartitions(String capabilityName, IntPredicate keep) {
        return capability.retainPartitions(keep);
    }

    /** 持久化压实（委托 {@link CacheCapability#compactAof}）。 */
    @Override
    public void compact(String capabilityName) {
        capability.compactAof();
    }
}
