package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.data.storage.engine.StorageProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * StoragePartitionAdapter：迁移完成后源端清理（clearPartition）与
 * 启动清扫（retainPartitions）的委托语义。
 */
class StoragePartitionAdapterTest {

    private static final NodeId SELF = NodeId.of("node-1");

    /** (bucket/key) 首字母决定分区：b*→1, c*→2。 */
    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(0) - 'a';
    }

    @TempDir
    Path tempDir;

    private StorageCapability capability;
    private StoragePartitionAdapter adapter;

    @BeforeEach
    void setUp() {
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(tempDir.toString()));
        props.setRootDir(tempDir.toString());
        capability = new StorageCapability(SELF,
                (cap, partition) -> Optional.of(new PlacementView.Owner(SELF, 1L)),
                StoragePartitionAdapterTest::partitioner, props, 8);
        capability.start();
        adapter = new StoragePartitionAdapter(capability);
    }

    @Test
    void clearPartitionRemovesSourceObjectsAfterMigration() throws Exception {
        putObj("b1", "k1", "v1");
        putObj("b1", "k2", "v2");
        putObj("c1", "k9", "other");

        assertThat(adapter.clearPartition("STORAGE", 1, SELF)).isTrue();
        assertThat(capability.partitionCount(1)).isZero();
        assertThat(capability.head("b1", "k1")).isNull();
        assertThat(capability.head("b1", "k2")).isNull();
        assertThat(capability.partitionCount(2)).isEqualTo(1L); // 其他分区不受影响
    }

    @Test
    void retainPartitionsDelegatesAndReturnsRemovedCount() throws Exception {
        putObj("b1", "k1", "keep");
        putObj("c1", "k9", "sweep");

        assertThat(adapter.retainPartitions("STORAGE", p -> p == 1)).isEqualTo(1L);
        assertThat(capability.head("b1", "k1")).isNotNull();
        assertThat(capability.head("c1", "k9")).isNull();
    }

    private void putObj(String bucket, String key, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        capability.putObject(bucket, key, new ByteArrayInputStream(bytes),
                bytes.length, "text/plain", null);
    }
}
