package com.ww.runtime.data.storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.StorageProperties;

/**
 * StorageCapability：fencing 写入、读写删列举与对象级迁移（分片/聚合/幂等/断点）。
 */
class StorageCapabilityTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId OTHER = NodeId.of("node-2");

    /** (bucket/key) 首字母决定分区：b*→0, k*→1。 */
    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(0) - 'a';
    }

    @TempDir
    Path tempDir;

    private StubView view;
    private StorageCapability capability;

    @BeforeEach
    void setUp() {
        view = new StubView();
        capability = new StorageCapability(SELF, view, StorageCapabilityTest::partitioner,
                props(tempDir.resolve("src")), 8); // chunkSize=8 便于触发多分片
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
    }

    private StorageProperties props(Path drive) {
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(drive.toString()));
        props.setRootDir(drive.toString());
        return props;
    }

    /** 物化惰性导出流（测试断言用）。 */
    private static List<byte[]> drain(Iterator<byte[]> chunks) {
        List<byte[]> all = new ArrayList<>();
        chunks.forEachRemaining(all::add);
        return all;
    }

    // ---- fencing ----

    @Test
    void typeIsStorage() {
        assertThat(capability.type()).isEqualTo(CapabilityType.STORAGE);
    }

    @Test
    void putRejectsWhenOwnerIsAnotherNode() {
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> putObj("b1", "k1", "data"))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.head("b1", "k1")).isNull();
    }

    @Test
    void putRejectsWhenNoOwner() {
        view.owner = Optional.empty();
        assertThatThrownBy(() -> putObj("b1", "k1", "data"))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void putGetHeadDeleteRoundTrip() throws IOException {
        ObjectMetadata meta = putObj("b1", "k1", "hello-storage");
        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(meta.getEtag()).isNotBlank();

        ObjectStore.StoredObject obj = capability.getObject("b1", "k1");
        assertThat(obj).isNotNull();
        try (InputStream in = capability.openRead(obj)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo("hello-storage");
        }
        assertThat(capability.head("b1", "k1").getSize()).isEqualTo(13L);

        // 版本递增
        ObjectMetadata v2 = putObj("b1", "k1", "updated");
        assertThat(v2.getVersion()).isEqualTo(2L);

        // 删除带 fencing
        assertThat(capability.deleteObject("b1", "k1")).isTrue();
        assertThat(capability.head("b1", "k1")).isNull();

        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.deleteObject("b1", "k1"))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void frozenRejectsWritesButAllowsReads() throws IOException {
        putObj("b1", "k1", "v");
        capability.drain();
        assertThatThrownBy(() -> putObj("b1", "k2", "v"))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");
        assertThat(capability.head("b1", "k1")).isNotNull(); // 读不受冻结影响
    }

    // ---- 迁移数据面 ----

    @Test
    void exportImportRoundTripWithChunking() throws IOException {
        // 26 字节对象 + chunkSize=8 → 4 片（仅末片 fin）
        putObj("b1", "k1", "0123456789abcdefghijklmnop");
        putObj("b1", "k2", "tiny");

        // bucket 首字母 'b' → 分区 1
        List<byte[]> chunks = drain(capability.exportPartition(1, 0L));
        assertThat(chunks).hasSize(5); // k1 四片 + k2 一片

        // 导入到“另一节点”实例
        StorageCapability target = new StorageCapability(
                OTHER, (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                StorageCapabilityTest::partitioner, props(tempDir.resolve("dst")), 8);
        target.start();
        assertThat(target.importChunks(chunks)).isTrue();

        ObjectStore.StoredObject obj = target.getObject("b1", "k1");
        assertThat(obj).isNotNull();
        try (InputStream in = target.openRead(obj)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo("0123456789abcdefghijklmnop");
        }
        assertThat(target.head("b1", "k1").getVersion()).isEqualTo(1L);
        assertThat(target.partitionCount(1)).isEqualTo(2L);

        // 重复导入幂等（putReplica 同 version 跳过）
        assertThat(target.importChunks(chunks)).isTrue();
        assertThat(target.head("b1", "k1").getVersion()).isEqualTo(1L);

        // T1：导入完成后暂存目录无残留（流式落盘不得泄漏暂存文件）
        File staging = tempDir.resolve("dst").resolve(".import-tmp").toFile();
        File[] leftovers = staging.listFiles((d, n) -> n.endsWith(".mig"));
        assertThat(leftovers == null ? new File[0] : leftovers).isEmpty();
    }

    @Test
    void exportSkipObjectsAlignsCheckpoint() throws IOException {
        putObj("b1", "k1", "aaa");
        putObj("b1", "k2", "bbb");
        assertThat(capability.partitionCount(1)).isEqualTo(2L);

        // 跳过第 1 个对象（checkpoint=1）→ 只剩 k2 的分片
        List<byte[]> chunks = drain(capability.exportPartition(1, 1L));
        StorageChunkView first = StorageChunkCodec.decodeChunk(chunks.get(0));
        assertThat(first).isNotNull();
        assertThat(first.key()).isEqualTo("k2");
        assertThat(first.fin()).isTrue();
    }

    @Test
    void importRejectsBrokenChunks() throws IOException {
        putObj("b1", "k1", "0123456789");
        List<byte[]> chunks = drain(capability.exportPartition(1, 0L));
        assertThat(chunks).hasSize(2);

        StorageCapability target = new StorageCapability(
                OTHER, (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                StorageCapabilityTest::partitioner, props(tempDir.resolve("dst2")), 8);
        target.start();

        // 跳过首片直接喂末片 → 偏移不连续 → false（聚合已中止，无残留）
        assertThat(target.importChunks(List.of(chunks.get(1)))).isFalse();
        // 非法分片 → false
        assertThat(target.importChunks(List.of("garbage".getBytes()))).isFalse();
        // 完整重喂恢复
        assertThat(target.importChunks(chunks)).isTrue();
        assertThat(target.head("b1", "k1")).isNotNull();
        // T1：失败与成功路径均不得残留暂存文件
        File staging = tempDir.resolve("dst2").resolve(".import-tmp").toFile();
        File[] leftovers = staging.listFiles((d, n) -> n.endsWith(".mig"));
        assertThat(leftovers == null ? new File[0] : leftovers).isEmpty();
    }

    @Test
    void clearPartitionRemovesOnlyTargetPartition() throws IOException {
        putObj("b1", "k1", "v"); // 分区 1（bucket 首字母 'b'）
        putObj("b1", "k2", "v"); // 分区 1
        putObj("c1", "k9", "v"); // 分区 2（'c'）
        capability.clearPartition(1);
        assertThat(capability.partitionCount(1)).isZero();
        assertThat(capability.head("b1", "k1")).isNull();
        assertThat(capability.head("c1", "k9")).isNotNull(); // 其他分区不受影响
    }

    // ---- S3 语义补全（阶段三）----

    @Test
    void putObjectStoresUserMetadata() throws IOException {
        byte[] bytes = "with-meta".getBytes(StandardCharsets.UTF_8);
        capability.putObject("b1", "k1", new ByteArrayInputStream(bytes), bytes.length,
                "application/json", java.util.Map.of("x-amz-meta-owner", "alice"));

        ObjectMetadata meta = capability.head("b1", "k1");
        assertThat(meta.getUserMetadata()).containsEntry("x-amz-meta-owner", "alice");
        assertThat(meta.getContentType()).isEqualTo("application/json");
    }

    @Test
    void putObjectRejectsKnownOversize() {
        assertThatThrownBy(() -> capability.putObject("b1", "k1",
                new ByteArrayInputStream(new byte[0]), Long.MAX_VALUE, "text/plain", null))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("max size");
    }

    @Test
    void putObjectRejectsUnknownLengthOversizeByStreamLimit() {
        StorageProperties p = props(tempDir.resolve("limited"));
        p.setMaxObjectSize(8L);
        StorageCapability capped = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, p, 4);
        capped.start();
        byte[] big = "0123456789abcdef".getBytes(StandardCharsets.UTF_8); // 16B > 8B
        // length=-1（未知长度）→ SizeLimitedInputStream 兜底拒绝
        assertThatThrownBy(() -> capped.putObject("b1", "k1",
                new ByteArrayInputStream(big), -1L, "text/plain", null))
                .isInstanceOf(com.ww.runtime.data.storage.engine.StorageException.class)
                .hasMessageContaining("max size");
        assertThat(capped.head("b1", "k1")).isNull();
    }

    @Test
    void bucketRegistryRecoversFromDiskOnRestart() {
        capability.createBucketLocal("persist-bucket");
        putObjRoundTripIgnoreFence();
        assertThat(capability.registeredBuckets()).contains("persist-bucket");

        // 重启（stop→start）：桶注册表从盘上 .bucket 重建
        capability.stop();
        capability.start();
        assertThat(capability.registeredBuckets()).contains("persist-bucket");
        assertThat(capability.listBuckets()).contains("persist-bucket");
    }

    @Test
    void metadataIndexRebuildsFromDiskOnRestart() throws IOException {
        putObj("b1", "a", "v");
        putObj("b1", "b", "v");
        putObj("b1", "ba", "v");
        putObj("b1", "bb", "v");
        putObj("b1", "c", "v");

        // 重启（stop→start）：元数据索引从盘上 object.meta 一次性重建
        capability.stop();
        capability.start();

        assertThat(capability.listAllObjects("b1")).extracting(ObjectMetadata::getKey)
                .containsExactly("a", "b", "ba", "bb", "c");
        assertThat(capability.countObjects("b1")).isEqualTo(5L);
        // prefix 为 startsWith 语义（重建后不变）："b" 匹配 b/ba/bb，不含 "c"
        var page = capability.list("b1", "b", null, 100);
        assertThat(page.getObjects()).extracting(ObjectMetadata::getKey).containsExactly("b", "ba", "bb");
    }

    /** 桶持久化用例辅助：放一个对象使 bucket 目录有内容（分区 1 属主=SELF 已在 setUp 配置）。 */
    private void putObjRoundTripIgnoreFence() {
        try {
            putObj("persist-bucket", "k1", "seed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void deleteBucketLocalClearsBucketAndRegistry() throws IOException {
        capability.createBucketLocal("gone");
        putObj("gone", "k1", "v");
        assertThat(capability.head("gone", "k1")).isNotNull();

        capability.deleteBucketLocal("gone", true);
        assertThat(capability.registeredBuckets()).doesNotContain("gone");
        assertThat(capability.head("gone", "k1")).isNull(); // 缓存随桶失效
    }

    @Test
    void deletePrefixRemovesMatchingKeysOnly() throws IOException {
        putObj("b1", "logs/a", "v");
        putObj("b1", "logs/b", "v");
        putObj("b1", "keep-c", "v");

        long deleted = capability.deletePrefix("b1", "logs/");
        assertThat(deleted).isEqualTo(2L);
        assertThat(capability.head("b1", "logs/a")).isNull();
        assertThat(capability.head("b1", "logs/b")).isNull();
        assertThat(capability.head("b1", "keep-c")).isNotNull();
    }

    @Test
    void headCacheInvalidatedAfterDelete() throws IOException {
        putObj("b1", "k1", "v1");
        assertThat(capability.head("b1", "k1")).isNotNull(); // 写穿缓存
        assertThat(capability.deleteObject("b1", "k1")).isTrue();
        assertThat(capability.head("b1", "k1")).isNull(); // 删除失效缓存后回源
    }

    @Test
    void replicaPutAndReplicaDeleteBypassFencing() throws IOException {
        // 副本路径：owner 是 OTHER 也能落盘（非属主节点接收复制）
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        byte[] bytes = "replica-data".getBytes(StandardCharsets.UTF_8);
        ObjectMetadata primaryMeta = new ObjectMetadata("k1", bytes.length, "text/plain",
                "etag-x", 12345L, 7L);
        ObjectMetadata stored = capability.replicaPut("b1", "k1",
                new ByteArrayInputStream(bytes), primaryMeta);
        assertThat(stored.getVersion()).isEqualTo(7L); // 主预计算 version 原样落盘

        assertThat(capability.replicaDelete("b1", "k1")).isTrue();
        assertThat(capability.head("b1", "k1")).isNull();
    }

    @Test
    void replicaPutRejectsNewOversizeWrite() {
        StorageProperties p = props(tempDir.resolve("replica-cap1"));
        p.setMaxObjectSize(8L);
        StorageCapability capped = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, p, 4);
        capped.start();
        byte[] big = new byte[16]; // 16B > 8B 上限
        ObjectMetadata meta = new ObjectMetadata("k1", big.length, "text/plain", "etag", 1L, 3L);

        assertThatThrownBy(() -> capped.replicaPut("b1", "k1",
                new ByteArrayInputStream(big), meta))
                .isInstanceOf(com.ww.runtime.data.storage.engine.StorageException.class)
                .hasMessageContaining("max size");
        assertThat(capped.head("b1", "k1")).isNull();
    }

    @Test
    void replicaPutIdempotentSkipPrecedesSizeCheck() throws IOException {
        StorageProperties p = props(tempDir.resolve("replica-cap2"));
        p.setMaxObjectSize(8L);
        StorageCapability capped = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, p, 4);
        capped.start();

        // 合法存量小对象（版本 5）
        byte[] small = "tiny".getBytes(StandardCharsets.UTF_8);
        ObjectMetadata v5 = new ObjectMetadata("k1", small.length, "text/plain", "etag-s", 1L, 5L);
        capped.replicaPut("b1", "k1", new ByteArrayInputStream(small), v5);

        // 冗余重放：版本不高于既有（5），但自报超限尺寸——应幂等跳过而非被大小拒绝
        ObjectMetadata replay = new ObjectMetadata("k1", 9999L, "text/plain", "etag-s", 1L, 5L);
        ObjectMetadata stored = capped.replicaPut("b1", "k1",
                new ByteArrayInputStream(new byte[0]), replay);
        assertThat(stored.getVersion()).isEqualTo(5L);
        assertThat(capped.head("b1", "k1").getSize()).isEqualTo(small.length); // 数据未被覆盖
    }

    @Test
    void importChunksRejectsOversizeAggregation() throws IOException {
        // 源端正常上限：导出 20B 对象（chunkSize=8 → 8/8/4 三片，末片 fin）
        putObj("b1", "k1", "0123456789abcdefghij");
        List<byte[]> chunks = drain(capability.exportPartition(1, 0L));
        assertThat(chunks).hasSize(3);

        // 目标端上限 8B：聚合累计超限即失败，不整对象入堆
        StorageProperties p = props(tempDir.resolve("import-cap"));
        p.setMaxObjectSize(8L);
        StorageCapability target = new StorageCapability(OTHER,
                (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                StorageCapabilityTest::partitioner, p, 8);
        target.start();
        assertThat(target.importChunks(chunks)).isFalse();
        assertThat(target.head("b1", "k1")).isNull();
    }

    @Test
    void listAllObjectsReturnsEveryKey() throws IOException {
        putObj("b1", "a", "v");
        putObj("b1", "b", "v");
        putObj("b1", "c", "v");
        List<ObjectMetadata> all = capability.listAllObjects("b1");
        assertThat(all).extracting(ObjectMetadata::getKey).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("T3：索引容量满后列举降级目录扫描，对象仍完整可见（正确性优先）")
    void indexCapacityOverflowDegradesListingWithoutDataLoss() throws IOException {
        StorageProperties capped = props(tempDir.resolve("capped"));
        capped.setMetadataIndexMaxEntries(2); // 极小上限：第 3 个对象即触发降级
        StorageCapability cappedCap = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, capped, 8);
        cappedCap.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
        try {
            for (String key : new String[]{"a", "b", "c", "d"}) {
                byte[] bytes = ("v-" + key).getBytes(StandardCharsets.UTF_8);
                cappedCap.putObject("b1", key, new ByteArrayInputStream(bytes),
                        bytes.length, "text/plain", null);
            }
            // 降级桶：列举经目录扫描仍全量可见且按 key 排序（语义不变）
            assertThat(cappedCap.listAllObjects("b1"))
                    .extracting(ObjectMetadata::getKey).containsExactly("a", "b", "c", "d");
            assertThat(cappedCap.countObjects("b1")).isEqualTo(4L);
            assertThat(cappedCap.totalObjectCount()).isEqualTo(4L);
            // 读路径不受索引降级影响；删除后经扫描同样不再可见
            assertThat(cappedCap.head("b1", "d")).isNotNull();
            assertThat(cappedCap.deleteObject("b1", "d")).isTrue();
            assertThat(cappedCap.listAllObjects("b1"))
                    .extracting(ObjectMetadata::getKey).containsExactly("a", "b", "c");
        } finally {
            cappedCap.stop();
        }
    }

    // ---- 动态调度：清扫 / 属主探测 / 计数 ----

    @Test
    void retainPartitionsRemovesUnownedAndInvalidatesCache() throws IOException {
        putObj("b1", "k1", "keep");  // 分区 1（桶首字母 'b'）
        putObj("c1", "k9", "sweep"); // 分区 2（'c'）
        assertThat(capability.head("c1", "k9")).isNotNull(); // 预热写穿缓存

        long removed = capability.retainPartitions(p -> p == 1);
        assertThat(removed).isEqualTo(1L);
        assertThat(capability.partitionCount(1)).isEqualTo(1L); // 保留分区不受影响
        assertThat(capability.partitionCount(2)).isZero();
        assertThat(capability.head("b1", "k1")).isNotNull();
        assertThat(capability.head("c1", "k9")).isNull(); // 元数据缓存同步失效
    }

    @Test
    void retainPartitionsNoOpWhenNotStarted() {
        StorageCapability stopped = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, props(tempDir.resolve("idle")), 8);
        assertThat(stopped.retainPartitions(p -> true)).isZero();
    }

    @Test
    void objectCountApisCoverAllAndPerBucket() throws IOException {
        putObj("b1", "k1", "v");
        putObj("b1", "k2", "v");
        putObj("c1", "k9", "v");
        assertThat(capability.totalObjectCount()).isEqualTo(3L);
        assertThat(capability.countObjects("b1")).isEqualTo(2L);
        assertThat(capability.countObjects("c1")).isEqualTo(1L);
    }

    @Test
    void servesPartitionOfReflectsPlacement() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
        assertThat(capability.servesPartitionOf("b1", "k1")).isTrue();
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThat(capability.servesPartitionOf("b1", "k1")).isFalse();
        view.owner = Optional.empty(); // 无放置 → 单机退化
        assertThat(capability.servesPartitionOf("b1", "k1")).isTrue();
    }

    private ObjectMetadata putObj(String bucket, String key, String data) throws IOException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return capability.putObject(bucket, key, new ByteArrayInputStream(bytes),
                bytes.length, "text/plain", null);
    }

    // ---- P0-1 半同步复制（写路径等副本确认） ----

    /** 记录型写钩子：afterPut/afterDelete 异步路径记账；replicateSync 返回可注入的确认结果。 */
    private static final class RecordingHook implements StorageCapability.WriteHook {
        final List<String> asyncPuts = new ArrayList<>();
        final List<String> asyncDeletes = new ArrayList<>();
        volatile boolean syncAck = true;
        int syncCalls;

        @Override
        public void afterPut(String bucket, ObjectMetadata meta) {
            asyncPuts.add(bucket + "/" + meta.getKey());
        }

        @Override
        public void afterDelete(String bucket, String key) {
            asyncDeletes.add(bucket + "/" + key);
        }

        @Override
        public boolean replicateSync(String bucket, ObjectMetadata meta, java.time.Duration timeout) {
            syncCalls++;
            return syncAck;
        }

        @Override
        public boolean replicateDeleteSync(String bucket, String key, java.time.Duration timeout) {
            syncCalls++;
            return syncAck;
        }
    }

    @Test
    void semiSyncDisabledKeepsAsyncHookSemantics() throws IOException {
        RecordingHook hook = new RecordingHook();
        capability.setWriteHook(hook);

        putObj("b1", "k1", "async-mode");

        assertThat(hook.asyncPuts).containsExactly("b1/k1");
        assertThat(hook.syncCalls).isZero(); // 半同步关：不走同步钩子
    }

    @Test
    void semiSyncEnabledWaitsForReplicaAckBeforeReturning() throws IOException {
        StorageProperties semi = props(tempDir.resolve("semi"));
        semi.setSemiSyncEnabled(true);
        StorageCapability semiCapability = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, semi, 8);
        semiCapability.start();
        RecordingHook hook = new RecordingHook();
        semiCapability.setWriteHook(hook);

        ObjectMetadata meta = semiCapability.putObject("b1", "k1",
                new ByteArrayInputStream("semi".getBytes(StandardCharsets.UTF_8)),
                4L, "text/plain", null);

        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(hook.syncCalls).isEqualTo(1); // 写路径确实走了半同步
        assertThat(hook.asyncPuts).isEmpty(); // 半同步开：不再 fire-and-forget
        assertThat(semiCapability.atRiskTotal()).isZero();
    }

    @Test
    void semiSyncTimeoutFailClosedRejectsWrite() throws IOException {
        StorageProperties semi = props(tempDir.resolve("fail-closed"));
        semi.setSemiSyncEnabled(true);
        semi.setSemiSyncFailClosed(true);
        StorageCapability semiCapability = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, semi, 8);
        semiCapability.start();
        RecordingHook hook = new RecordingHook();
        hook.syncAck = false; // 副本未确认
        semiCapability.setWriteHook(hook);

        assertThatThrownBy(() -> semiCapability.putObject("b1", "k1",
                new ByteArrayInputStream("lost?".getBytes(StandardCharsets.UTF_8)),
                6L, "text/plain", null))
                .isInstanceOf(com.ww.runtime.common.ReplicationTimeoutException.class)
                .hasMessageContaining("semi-sync");
        // fail-closed：本地已落盘但拒绝应答——数据留待对账/人工，客户端明确知道失败
        assertThat(semiCapability.atRiskTotal()).isZero();
    }

    @Test
    void semiSyncTimeoutDegradeRecordsAtRiskAndAcknowledges() throws IOException {
        StorageProperties semi = props(tempDir.resolve("degrade"));
        semi.setSemiSyncEnabled(true); // failClosed 默认 false = 降级应答
        StorageCapability semiCapability = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, semi, 8);
        semiCapability.start();
        RecordingHook hook = new RecordingHook();
        hook.syncAck = false;
        semiCapability.setWriteHook(hook);

        ObjectMetadata meta = semiCapability.putObject("b1", "k1",
                new ByteArrayInputStream("at-risk".getBytes(StandardCharsets.UTF_8)),
                7L, "text/plain", null);

        // 降级：照常应答，但 at-risk 观测记录（总数 + 快照）
        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(semiCapability.atRiskTotal()).isEqualTo(1L);
        assertThat(semiCapability.atRiskSnapshot()).hasSize(1);
        assertThat(semiCapability.atRiskSnapshot().getFirst().bucket()).isEqualTo("b1");
        assertThat(semiCapability.atRiskSnapshot().getFirst().key()).isEqualTo("k1");
        assertThat(semiCapability.atRiskSnapshot().getFirst().version()).isEqualTo(1L);
        assertThat(semiCapability.atRiskSnapshot().getFirst().delete()).isFalse();
    }

    @Test
    void semiSyncDeleteTimeoutDegradeRecordsAtRisk() {
        StorageProperties semi = props(tempDir.resolve("degrade-del"));
        semi.setSemiSyncEnabled(true);
        StorageCapability semiCapability = new StorageCapability(SELF, view,
                StorageCapabilityTest::partitioner, semi, 8);
        semiCapability.start();
        RecordingHook hook = new RecordingHook();
        semiCapability.setWriteHook(hook);
        try {
            semiCapability.putObject("b1", "k1",
                    new ByteArrayInputStream("bye".getBytes(StandardCharsets.UTF_8)),
                    3L, "text/plain", null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        assertThat(semiCapability.atRiskTotal()).isZero(); // put 阶段副本已确认

        hook.syncAck = false; // 删除时副本失联
        assertThat(semiCapability.deleteObject("b1", "k1")).isTrue();
        assertThat(semiCapability.atRiskTotal()).isEqualTo(1L);
        assertThat(semiCapability.atRiskSnapshot().getFirst().delete()).isTrue();
    }

    /** 可变桩视图。 */
    private static final class StubView implements PlacementView {
        volatile Optional<Owner> owner = Optional.empty();

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return owner;
        }
    }
}
