package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.storage.ObjectMetadata;
import com.ww.storage.ObjectStore;
import com.ww.storage.StorageProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        List<byte[]> chunks = capability.exportPartition(1, 0L);
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
    }

    @Test
    void exportSkipObjectsAlignsCheckpoint() throws IOException {
        putObj("b1", "k1", "aaa");
        putObj("b1", "k2", "bbb");
        assertThat(capability.partitionCount(1)).isEqualTo(2L);

        // 跳过第 1 个对象（checkpoint=1）→ 只剩 k2 的分片
        List<byte[]> chunks = capability.exportPartition(1, 1L);
        StorageChunkView first = StorageChunkCodec.decodeChunk(chunks.get(0));
        assertThat(first).isNotNull();
        assertThat(first.key()).isEqualTo("k2");
        assertThat(first.fin()).isTrue();
    }

    @Test
    void importRejectsBrokenChunks() throws IOException {
        putObj("b1", "k1", "0123456789");
        List<byte[]> chunks = capability.exportPartition(1, 0L);
        assertThat(chunks).hasSize(2);

        StorageCapability target = new StorageCapability(
                OTHER, (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                StorageCapabilityTest::partitioner, props(tempDir.resolve("dst2")), 8);
        target.start();

        // 跳过首片直接喂末片 → 偏移不连续 → false
        assertThat(target.importChunks(List.of(chunks.get(1)))).isFalse();
        // 非法分片 → false
        assertThat(target.importChunks(List.of("garbage".getBytes()))).isFalse();
        // 完整重喂恢复
        assertThat(target.importChunks(chunks)).isTrue();
        assertThat(target.head("b1", "k1")).isNotNull();
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

    private ObjectMetadata putObj(String bucket, String key, String data) throws IOException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return capability.putObject(bucket, key, new ByteArrayInputStream(bytes),
                bytes.length, "text/plain");
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
