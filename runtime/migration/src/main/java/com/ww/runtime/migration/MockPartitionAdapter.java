package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link PartitionDataAdapter} 的 Mock 实现：委托<b>同一</b> {@link MockCapability}
 * 实例完成导出/导入（同进程源/目标直连场景），条目序列化为简单行格式
 * {@code key + 0x01 + value + 0x02}（UTF-8）。
 *
 * <p>导出按 key 字典序排序后自 {@code fromCheckpoint} 起逐条切片（一条目一分片），
 * 保证断点续跑跨调用/跨重启的确定性；导入直接走
 * {@link MockCapability#importPartition}（Map 语义天然幂等 upsert）。</p>
 */
public final class MockPartitionAdapter implements PartitionDataAdapter {

    private final MockCapability capability;

    public MockPartitionAdapter(MockCapability capability) {
        this.capability = Objects.requireNonNull(capability, "capability");
    }

    @Override
    public long itemCount(String capabilityName, int partition) {
        return capability.partitionCount(partition);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capabilityName, int partition, long fromCheckpoint) {
        if (fromCheckpoint < 0) {
            throw new IllegalArgumentException("fromCheckpoint must be >= 0, got " + fromCheckpoint);
        }
        List<Map.Entry<String, String>> entries = new ArrayList<>(capability.exportPartition(partition));
        entries.sort(Comparator.comparing(Map.Entry::getKey));
        int start = (int) Math.min(fromCheckpoint, entries.size());
        // 惰性逐条编码（Mock 数据量小，条目列表本身已在内存）
        return new Iterator<>() {
            private int index = start;

            @Override
            public boolean hasNext() {
                return index < entries.size();
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                Map.Entry<String, String> entry = entries.get(index++);
                return encodeEntry(entry.getKey(), entry.getValue());
            }
        };
    }

    @Override
    public boolean importChunks(String capabilityName, int partition, List<byte[]> chunks) {
        Objects.requireNonNull(chunks, "chunks");
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (byte[] chunk : chunks) {
            List<Map.Entry<String, String>> parsed = decodeChunk(chunk);
            if (parsed == null) {
                return false;
            }
            entries.addAll(parsed);
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

    /** 迁移完成后清理源端分区数据（委托 {@link MockCapability#clearPartition}）。 */
    @Override
    public boolean clearPartition(String capabilityName, int partition,
                                   com.ww.runtime.common.NodeId source) {
        capability.clearPartition(partition);
        return true;
    }

    /** key + 0x01 + value + 0x02（UTF-8）。 */
    static byte[] encodeEntry(String key, String value) {
        String line = key + Character.toString(ChunkCodec.SEP_KV) + value
                + Character.toString(ChunkCodec.SEP_ENTRY);
        return line.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 行格式反解；任一条目缺少分隔符/终结符即视为非法，返回 null（调用方判 false）。
     */
    static List<Map.Entry<String, String>> decodeChunk(byte[] chunk) {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        int cursor = 0;
        while (cursor < chunk.length) {
            int entryEnd = indexOf(chunk, ChunkCodec.SEP_ENTRY, cursor);
            if (entryEnd < 0) {
                return null; // 未终结的不完整条目
            }
            int kvSep = indexOf(chunk, ChunkCodec.SEP_KV, cursor);
            if (kvSep < 0 || kvSep > entryEnd) {
                return null; // 缺 key/value 分隔符
            }
            String key = new String(chunk, cursor, kvSep - cursor, StandardCharsets.UTF_8);
            String value = new String(chunk, kvSep + 1, entryEnd - kvSep - 1, StandardCharsets.UTF_8);
            entries.add(Map.entry(key, value));
            cursor = entryEnd + 1;
        }
        return entries;
    }

    private static int indexOf(byte[] haystack, byte needle, int from) {
        for (int i = from; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }
}
