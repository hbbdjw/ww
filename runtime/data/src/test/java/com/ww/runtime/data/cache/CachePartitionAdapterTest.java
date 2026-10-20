package com.ww.runtime.data.cache;

import com.ww.runtime.migration.ChunkCodec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CacheChunkCodec v2 编解码往返（全类型 kind）+ CachePartitionAdapter
 * 断点偏移/非法分片/容器导入幂等。
 */
class CachePartitionAdapterTest {

    @Test
    void codecRoundTripForTypedKvValues() {
        roundTripKv("k1", 0L, "string-value");
        roundTripKv("k2", 60_000L, 42);
        roundTripKv("k3", -1L, 3.14d);
        roundTripKv("k4", 0L, Boolean.TRUE);
        roundTripKv("k5", 0L, 9_999_999_999L);
        roundTripKv("k6", 0L, null);
        // 值含控制字符也不破坏行格式（base64 隔离）
        roundTripKv("k7", 0L, "包含\u0001与\u0002的值");
        roundTripKv("中文键", 123L, "unicode-ok");
    }

    @Test
    void codecRoundTripForContainerKinds() {
        Map<String, Object> hash = new LinkedHashMap<>();
        hash.put("f1", "v1");
        hash.put("f2", 42);
        roundTrip(new CachePartitionEntry("h1", CachePartitionEntry.Kind.HASH, 10_000L, hash));

        roundTrip(new CachePartitionEntry("l1", CachePartitionEntry.Kind.LIST, 0L,
                List.of("a", "b", "c")));
        roundTrip(new CachePartitionEntry("s1", CachePartitionEntry.Kind.SET, 0L,
                List.of("m1", "m2")));
        roundTrip(new CachePartitionEntry("z1", CachePartitionEntry.Kind.ZSET, 0L,
                List.of(new CachePartitionEntry.ScoredMember(1.5, "m1"),
                        new CachePartitionEntry.ScoredMember(2.5, "m2"))));

        CachePartitionEntry.StreamSnapshot snapshot = new CachePartitionEntry.StreamSnapshot(
                List.of(new CachePartitionEntry.SnapshotEntry(1000L, 1L, Map.of("f", "v"))),
                List.of(new CachePartitionEntry.GroupSnapshot("g1", 1000L, 1L,
                        List.of(new CachePartitionEntry.ConsumerSnapshot("c1",
                                List.of(1000L, 1L))))));
        roundTrip(new CachePartitionEntry("t1", CachePartitionEntry.Kind.STREAM, 0L, snapshot));
    }

    private void roundTripKv(String key, long ttl, Object value) {
        roundTrip(new CachePartitionEntry(key, CachePartitionEntry.Kind.KV, ttl, value));
    }

    private void roundTrip(CachePartitionEntry entry) {
        byte[] chunk = CacheChunkCodec.encodeEntry(entry);
        // 一条目一分片：恰一个终结符（ChunkCodec.countEntries 语义一致）
        assertThat(ChunkCodec.countEntries(chunk)).isEqualTo(1L);
        CachePartitionEntry decoded = CacheChunkCodec.decodeChunk(chunk);
        assertThat(decoded).isNotNull();
        assertThat(decoded.key()).isEqualTo(entry.key());
        assertThat(decoded.kind()).isEqualTo(entry.kind());
        assertThat(decoded.remainingTtlMillis()).isEqualTo(entry.remainingTtlMillis());
        assertThat(decoded.payload()).isEqualTo(entry.payload());
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
        // 非法 base64（v2：key 0x01 kind 0x01 ttl 0x01 payload 0x02）
        assertThat(CacheChunkCodec.decodeChunk(
                "k\u0001K\u00010\u0001!!!not-base64!!!\u0002".getBytes())).isNull();
        // 未知 kind 标记
        assertThat(CacheChunkCodec.decodeChunk(
                "k\u0001X\u00010\u0001QUJD\u0002".getBytes())).isNull();
    }

    @Test
    void adapterExportRespectsCheckpointOffset() throws Exception {
        CacheCapability capability = newSelfOwnedCapability();
        capability.put("a1", "v1", 0L);
        capability.put("a2", "v2", 0L);
        capability.put("a3", "v3", 0L);
        CachePartitionAdapter adapter = new CachePartitionAdapter(capability);

        assertThat(adapter.itemCount("CACHE", 0)).isEqualTo(3L);
        assertThat(drain(adapter.exportChunks("CACHE", 0, 0L))).hasSize(3);
        assertThat(drain(adapter.exportChunks("CACHE", 0, 2L))).hasSize(1); // 跳过前 2 条
        assertThat(drain(adapter.exportChunks("CACHE", 0, 10L))).isEmpty(); // 超出全量
        assertThat(adapter.verifyCount("CACHE", 0)).isEqualTo(3L);
    }

    @Test
    void adapterImportRoundTripAndRejectsInvalid() throws Exception {
        CacheCapability source = newSelfOwnedCapability();
        source.put("a1", "v1", 0L);
        source.put("a2", 123, 0L);
        CachePartitionAdapter sourceAdapter = new CachePartitionAdapter(source);
        List<byte[]> chunks = drain(sourceAdapter.exportChunks("CACHE", 0, 0L));

        CacheCapability target = newSelfOwnedCapability();
        CachePartitionAdapter targetAdapter = new CachePartitionAdapter(target);
        assertThat(targetAdapter.importChunks("CACHE", 0, chunks)).isTrue();
        assertThat(target.get("a1")).isEqualTo("v1");
        assertThat(target.get("a2")).isEqualTo(123);

        // 非法分片 → false
        assertThat(targetAdapter.importChunks("CACHE", 0, List.of("garbage".getBytes()))).isFalse();
    }

    /**
     * 启动收敛窗口：能力已注册但未 start（cache==null），importPartition 抛
     * "cache capability not started"。该前置条件异常必须原样放行（保留语义与 cause），
     * 由 MigrationServer 映射为 FAILED_PRECONDITION——此前被无条件吞成 false，
     * 接收端只能回 "import failed"，引擎误按硬失败打 WARN+堆栈并耗尽重试。
     */
    @Test
    void adapterImportPropagatesCapabilityNotStartedPrecondition() {
        CacheCapability notStarted = new CacheCapability(
                com.ww.runtime.common.NodeId.of("node-x"),
                (cap, partition) -> java.util.Optional.empty(),
                key -> 0, false, null); // 故意不调 start()
        CachePartitionAdapter adapter = new CachePartitionAdapter(notStarted);
        byte[] chunk = CacheChunkCodec.encodeEntry(
                new CachePartitionEntry("k1", CachePartitionEntry.Kind.KV, 0L, "v1"));

        assertThatThrownBy(() -> adapter.importChunks("CACHE", 0, List.of(chunk)))
                .isInstanceOf(com.ww.runtime.common.RuntimeFrameworkException.class)
                .hasMessageContaining("not started");
    }

    /** 容器导入幂等：重导不产生重复成员（整 key 替换语义）。 */
    @Test
    void adapterContainerImportIsIdempotent() throws Exception {
        CacheCapability target = newSelfOwnedCapability();
        CachePartitionAdapter adapter = new CachePartitionAdapter(target);
        CachePartitionEntry listEntry = new CachePartitionEntry("a1",
                CachePartitionEntry.Kind.LIST, 0L, List.of("x", "y"));
        byte[] chunk = CacheChunkCodec.encodeEntry(listEntry);

        assertThat(adapter.importChunks("CACHE", 0, List.of(chunk))).isTrue();
        assertThat(adapter.importChunks("CACHE", 0, List.of(chunk))).isTrue(); // 重导

        List<CachePartitionEntry> exported = target.exportPartition(0);
        assertThat(exported).hasSize(1);
        assertThat(exported.get(0).kind()).isEqualTo(CachePartitionEntry.Kind.LIST);
        assertThat(exported.get(0).payload()).isEqualTo(List.of("x", "y")); // 无重复
    }

    /** 物化惰性导出流（测试断言用）。 */
    private static List<byte[]> drain(Iterator<byte[]> chunks) {
        List<byte[]> all = new ArrayList<>();
        chunks.forEachRemaining(all::add);
        return all;
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
