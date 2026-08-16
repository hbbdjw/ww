package com.ww.runtime.data.cache;

import com.ww.runtime.migration.ChunkCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CacheChunkCodec 编解码往返 + CachePartitionAdapter 断点偏移/非法分片。
 */
class CachePartitionAdapterTest {

    @Test
    void codecRoundTripForTypedValues() {
        roundTrip("k1", 0L, "string-value");
        roundTrip("k2", 60_000L, 42);
        roundTrip("k3", -1L, 3.14d);
        roundTrip("k4", 0L, Boolean.TRUE);
        roundTrip("k5", 0L, 9_999_999_999L);
        roundTrip("k6", 0L, null);
        // 值含控制字符也不破坏行格式（base64 隔离）
        roundTrip("k7", 0L, "包含\u0001与\u0002的值");
        roundTrip("中文键", 123L, "unicode-ok");
    }

    private void roundTrip(String key, long ttl, Object value) {
        byte[] chunk = CacheChunkCodec.encodeEntry(new CachePartitionEntry(key, ttl, value));
        // 一条目一分片：恰一个终结符（ChunkCodec.countEntries 语义一致）
        assertThat(ChunkCodec.countEntries(chunk)).isEqualTo(1L);
        CachePartitionEntry decoded = CacheChunkCodec.decodeChunk(chunk);
        assertThat(decoded).isNotNull();
        assertThat(decoded.key()).isEqualTo(key);
        assertThat(decoded.remainingTtlMillis()).isEqualTo(ttl);
        assertThat(decoded.value()).isEqualTo(value);
    }

    @Test
    void decodeRejectsMalformedChunks() {
        assertThat(CacheChunkCodec.decodeChunk(null)).isNull();
        assertThat(CacheChunkCodec.decodeChunk(new byte[0])).isNull();
        assertThat(CacheChunkCodec.decodeChunk("no-separator".getBytes())).isNull();
        // 缺终结符
        byte[] noTerminator = CacheChunkCodec.encodeEntry(new CachePartitionEntry("k", 0L, "v"));
        byte[] truncated = new byte[noTerminator.length - 1];
        System.arraycopy(noTerminator, 0, truncated, 0, truncated.length);
        assertThat(CacheChunkCodec.decodeChunk(truncated)).isNull();
        // 非法 base64
        assertThat(CacheChunkCodec.decodeChunk("k\u0001 0\u0001!!!not-base64!!!\u0002".getBytes())).isNull();
    }

    @Test
    void adapterExportRespectsCheckpointOffset() throws Exception {
        CacheCapability capability = newSelfOwnedCapability();
        capability.put("a1", "v1", 0L);
        capability.put("a2", "v2", 0L);
        capability.put("a3", "v3", 0L);
        CachePartitionAdapter adapter = new CachePartitionAdapter(capability);

        assertThat(adapter.itemCount("CACHE", 0)).isEqualTo(3L);
        assertThat(adapter.exportChunks("CACHE", 0, 0L)).hasSize(3);
        assertThat(adapter.exportChunks("CACHE", 0, 2L)).hasSize(1); // 跳过前 2 条
        assertThat(adapter.exportChunks("CACHE", 0, 10L)).isEmpty(); // 超出全量
        assertThat(adapter.verifyCount("CACHE", 0)).isEqualTo(3L);
    }

    @Test
    void adapterImportRoundTripAndRejectsInvalid() throws Exception {
        CacheCapability source = newSelfOwnedCapability();
        source.put("a1", "v1", 0L);
        source.put("a2", 123, 0L);
        CachePartitionAdapter sourceAdapter = new CachePartitionAdapter(source);
        List<byte[]> chunks = sourceAdapter.exportChunks("CACHE", 0, 0L);

        CacheCapability target = newSelfOwnedCapability();
        CachePartitionAdapter targetAdapter = new CachePartitionAdapter(target);
        assertThat(targetAdapter.importChunks("CACHE", 0, chunks)).isTrue();
        assertThat(target.get("a1")).isEqualTo("v1");
        assertThat(target.get("a2")).isEqualTo(123);

        // 非法分片 → false
        assertThat(targetAdapter.importChunks("CACHE", 0, List.of("garbage".getBytes()))).isFalse();
    }

    private CacheCapability newSelfOwnedCapability() throws Exception {
        CacheCapability capability = new CacheCapability(
                com.ww.runtime.common.NodeId.of("node-1"),
                (cap, partition) -> java.util.Optional.of(
                        new com.ww.runtime.capability.PlacementView.Owner(
                                com.ww.runtime.common.NodeId.of("node-1"), 1L)),
                key -> key.charAt(0) - 'a', false, null);
        capability.start();
        return capability;
    }
}
