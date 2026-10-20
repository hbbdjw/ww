package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.placement.Partitioner;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MockPartitionAdapter：行格式编解码、断点跳过、幂等 upsert 导入、对账计数。
 */
class MockPartitionAdapterTest {

    private static final int PARTITION_COUNT = 4096;

    private final MockCapability capability = new MockCapability(
            NodeId.of("node-a"), emptyView(), key -> Partitioner.partition(key, PARTITION_COUNT));
    private final MockPartitionAdapter adapter = new MockPartitionAdapter(capability);

    private static PlacementView emptyView() {
        return (cap, partition) -> Optional.empty();
    }

    /** 归一收集指定分区的 count 条 KV 并预填。 */
    private List<Map.Entry<String, String>> fill(int partition, int count) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(count);
        for (int i = 0; entries.size() < count; i++) {
            String key = "adapter-key-" + partition + "-" + i;
            if (Partitioner.partition(key, PARTITION_COUNT) == partition) {
                entries.add(Map.entry(key, "value-" + key));
            }
        }
        capability.importPartition(partition, entries);
        return entries;
    }

    private static int partitionOfSeed(String seed) {
        return Partitioner.partition(seed, PARTITION_COUNT);
    }

    @Test
    void itemCountAndVerifyCountMatchPartitionCount() {
        int partition = partitionOfSeed("count-seed");
        fill(partition, 30);
        assertThat(adapter.itemCount("mock", partition)).isEqualTo(30L);
        assertThat(adapter.verifyCount("mock", partition)).isEqualTo(30L);
        assertThat(adapter.itemCount("mock", partitionOfSeed("other-seed"))).isZero();
    }

    @Test
    void exportChunksReturnsOnePiecePerEntrySorted() {
        int partition = partitionOfSeed("export-seed");
        List<Map.Entry<String, String>> entries = fill(partition, 25);

        List<byte[]> chunks = drain(adapter.exportChunks("mock", partition, 0L));
        assertThat(chunks).hasSize(25);

        List<String> keys = new ArrayList<>();
        for (byte[] chunk : chunks) {
            List<Map.Entry<String, String>> decoded = MockPartitionAdapter.decodeChunk(chunk);
            assertThat(decoded).hasSize(1);
            keys.add(decoded.get(0).getKey());
        }
        List<String> expected = entries.stream()
                .map(Map.Entry::getKey)
                .sorted(Comparator.naturalOrder())
                .toList();
        assertThat(keys).containsExactlyElementsOf(expected); // 字典序稳定（断点续跑前提）
    }

    @Test
    void exportChunksFromCheckpointSkipsFirstNEntries() {
        int partition = partitionOfSeed("skip-seed");
        fill(partition, 40);

        List<byte[]> full = drain(adapter.exportChunks("mock", partition, 0L));
        List<byte[]> tail = drain(adapter.exportChunks("mock", partition, 17L));
        assertThat(tail).hasSize(40 - 17);
        assertThat(tail).containsExactlyElementsOf(full.subList(17, full.size()));

        assertThat(drain(adapter.exportChunks("mock", partition, 40L))).isEmpty();
        assertThat(drain(adapter.exportChunks("mock", partition, 100L))).isEmpty(); // 越界钳制
    }

    @Test
    void importChunksRoundTripsAfterClear() {
        int partition = partitionOfSeed("roundtrip");
        List<Map.Entry<String, String>> entries = fill(partition, 20);
        List<byte[]> chunks = drain(adapter.exportChunks("mock", partition, 0L));

        capability.clearPartition(partition); // 清空后仅靠分片数据恢复
        assertThat(capability.partitionCount(partition)).isZero();

        assertThat(adapter.importChunks("mock", partition, chunks)).isTrue();
        assertThat(capability.partitionCount(partition)).isEqualTo(20L);
        assertThat(capability.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(entries);
    }

    @Test
    void importChunksIsIdempotentUpsert() {
        int partition = partitionOfSeed("idempotent-seed");
        List<Map.Entry<String, String>> entries = fill(partition, 15);
        List<byte[]> pieces = drain(adapter.exportChunks("mock", partition, 0L));

        // 同一批分片重复导入三次：条数不变、无重复键（Map upsert 语义）
        for (int i = 0; i < 3; i++) {
            assertThat(adapter.importChunks("mock", partition, pieces)).isTrue();
        }
        assertThat(capability.partitionCount(partition)).isEqualTo(15L);
        assertThat(capability.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(entries);
    }

    @Test
    void importChunksRejectsMalformedAndForeignPartitionData() {
        int partition = partitionOfSeed("malformed-seed");
        // 缺条目终结符 → 解析失败 → false
        assertThat(adapter.importChunks("mock", partition,
                List.of(new byte[]{'k', 0x01, 'v'}))).isFalse();
        // 缺 key/value 分隔符 → false
        assertThat(adapter.importChunks("mock", partition,
                List.of(new byte[]{'k', 0x02}))).isFalse();

        // key 不属于目标分区 → MockCapability.importPartition 归属校验拒绝 → false
        String key = "foreign-key-x";
        int keyPartition = Partitioner.partition(key, PARTITION_COUNT);
        int target = (keyPartition + 1) % PARTITION_COUNT; // 必然 ≠ key 所属分区
        assertThat(adapter.importChunks("mock", target,
                List.of(MockPartitionAdapter.encodeEntry(key, "v")))).isFalse();
        // 目标分区未被污染
        assertThat(capability.partitionCount(target)).isZero();
        assertThat(capability.partitionCount(partition)).isZero();
    }

    /** 物化惰性导出流（测试断言用）。 */
    private static List<byte[]> drain(Iterator<byte[]> chunks) {
        List<byte[]> all = new ArrayList<>();
        chunks.forEachRemaining(all::add);
        return all;
    }
}
