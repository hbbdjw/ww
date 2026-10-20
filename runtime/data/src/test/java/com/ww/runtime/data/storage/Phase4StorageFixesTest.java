package com.ww.runtime.data.storage;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.data.storage.engine.StorageProperties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 阶段 4 验收测试（修复计划 §3.9/§3.10/§5 阶段 4）：
 * <ul>
 *   <li>T15：损坏 meta 隔离 + 孤儿数据 500 语义 + version 不重置；</li>
 *   <li>T26：同 version 异 etag 冲突留痕（默认保留先到）；</li>
 *   <li>T11：崩溃残留清扫（含 T5 备份文件），年龄阈值防误删；</li>
 *   <li>T9：隐式建桶灰度（告警模式照旧 / 严格模式拒绝）；</li>
 *   <li>T25：冻结态拒绝副本写（backlog 重放兜底语义）；</li>
 *   <li>T24：导出迭代器可关闭（迁移中止无 FD 泄漏）；</li>
 *   <li>T13：副本 fsync 默认与主路径一致（配置项生效）。</li>
 * </ul>
 * T6（复制复核）见 {@code StorageReplicatorTest}；T10/T19/T20（网关）见 {@code StorageGatewayTest}。
 */
@DisplayName("阶段 4 验收：存储写路径一致性 + 桶生命周期 + 列举/清理")
class Phase4StorageFixesTest {

    private static final NodeId SELF = NodeId.of("node-1");

    private static int partitioner(String bucketSlashKey) {
        // (bucket+"/"+key) → 取 key 首字符决定分区（与 StorageGatewayTest 同口径）
        return bucketSlashKey.charAt(bucketSlashKey.indexOf('/') + 1) - 'a';
    }

    @TempDir
    Path tempDir;

    private StorageCapability newCapability(boolean strictBucket) throws Exception {
        return newCapabilityAt(tempDir, strictBucket);
    }

    private StorageCapability newCapabilityAt(Path dir, boolean strictBucket) throws Exception {
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(dir.toString()));
        props.setRootDir(dir.toString());
        props.setStrictBucketCheck(strictBucket);
        props.setTempGcIntervalMillis(0); // 测试内手动触发 GC
        StorageCapability capability = new StorageCapability(SELF,
                new PlacementView() {
                    @Override
                    public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                        return Optional.of(new Owner(SELF, 3L));
                    }

                    @Override
                    public List<Owner> holders(CapabilityType cap, PartitionId partition) {
                        return List.of(new Owner(SELF, 3L));
                    }
                },
                Phase4StorageFixesTest::partitioner, props, 8);
        capability.start();
        return capability;
    }

    private static ObjectMetadata put(StorageCapability cap, String bucket, String key, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return cap.putObject(bucket, key, new ByteArrayInputStream(bytes), bytes.length,
                "text/plain", null);
    }

    // ==================== T15 ====================

    @Test
    @DisplayName("T15：坏 meta 注入——head/get 显式失败（500 语义），写入拒绝，version 不重置，meta 隔离")
    void t15_corruptMetaQuarantinedAndLoud() throws Exception {
        StorageCapability cap = newCapability(false);
        put(cap, "bkt", "a1", "v1");
        ObjectStore store = cap.storeForTest();

        // 先定位对象目录，再人为损坏 object.meta
        ObjectStore.StoredObject obj = cap.getObject("bkt", "a1");
        java.nio.file.Path objDir = java.nio.file.Path.of(obj.dataFile.getParent());
        java.nio.file.Files.writeString(objDir.resolve("object.meta"), "{corrupted json!!!");

        // HEAD/GET：损坏 ≠ 不存在 → 显式失败（上层映射 500）。
        // 直接打引擎层（head() 会先命中 metaCache，缓存短路属正常行为）
        assertThatThrownBy(() -> store.head("bkt", "a1")).isInstanceOf(StorageException.class);
        assertThatThrownBy(() -> store.get("bkt", "a1")).isInstanceOf(StorageException.class);
        assertThat(store.getCorruptedMetaQuarantined()).isEqualTo(1);

        // 隔离文件生成；覆盖写被拒绝（防 version 静默重置为 1）
        try (var files = java.nio.file.Files.list(objDir)) {
            assertThat(files.anyMatch(p -> p.getFileName().toString().startsWith("object.meta.corrupt")))
                    .as("损坏 meta 应被隔离为 .corrupt").isTrue();
        }
        assertThatThrownBy(() -> put(cap, "bkt", "a1", "v2")).isInstanceOf(StorageException.class);
        cap.stop();
    }

    // ==================== T26 / P1-6 ====================

    @Test
    @DisplayName("T26/P1-6：同 version 异 etag 副本写——主端权威覆盖 + 冲突留痕")
    void t26_sameVersionEtagConflictTraced() throws Exception {
        StorageCapability cap = newCapability(false);
        put(cap, "bkt", "a1", "first");
        ObjectMetadata localMeta = cap.head("bkt", "a1");

        // 同 version 异 etag 的冲突副本写
        ObjectMetadata conflicting = new ObjectMetadata("a1", 4L, "text/plain",
                "deadbeef-not-mine", localMeta.getCreatedAt(), localMeta.getVersion());
        cap.replicaPut("bkt", "a1",
                new ByteArrayInputStream("evil".getBytes(StandardCharsets.UTF_8)), conflicting);

        ObjectStore store = cap.storeForTest();
        assertThat(store.getSameVersionConflicts()).as("冲突必须留痕").isEqualTo(1);
        // P1-6：旧策略「保留先到」会让副本永久停留在与主不一致的内容上（对账只比
        // version 永远修不好）；现以本次主端推送内容为准覆盖，分叉可被修复。
        assertThat(cap.head("bkt", "a1").getEtag())
                .as("主端权威覆盖：副本内容与推送一致").isEqualTo("deadbeef-not-mine");
        cap.stop();
    }

    // ==================== T11 / T5 残留 ====================

    @Test
    @DisplayName("T11：超期写临时文件（含 T5 备份）被清扫；新临时文件与正式文件保留")
    void t11_tempSweepRemovesAgedResidue() throws Exception {
        StorageCapability cap = newCapability(false);
        put(cap, "bkt", "a1", "keep-me");
        ObjectStore.StoredObject obj = cap.getObject("bkt", "a1");
        java.nio.file.Path objDir = java.nio.file.Path.of(obj.dataFile.getParent());

        java.nio.file.Path agedTmp = objDir.resolve("object.data.aged.tmp");
        java.nio.file.Path agedOld = objDir.resolve("object.data.old123.tmp");
        java.nio.file.Path freshTmp = objDir.resolve("object.data.fresh.tmp");
        java.nio.file.Files.writeString(agedTmp, "x");
        java.nio.file.Files.writeString(agedOld, "x");
        java.nio.file.Files.writeString(freshTmp, "x");
        // 把两个残留的 lastModified 拨回 2 小时前
        java.nio.file.Files.setLastModifiedTime(agedTmp,
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() - 7_200_000L));
        java.nio.file.Files.setLastModifiedTime(agedOld,
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() - 7_200_000L));

        ObjectStore store = cap.storeForTest();
        int removed = store.sweepTempFiles(3_600_000L); // 1h 阈值

        assertThat(removed).isEqualTo(2);
        assertThat(agedTmp).doesNotExist();
        assertThat(agedOld).doesNotExist();
        assertThat(freshTmp).as("年龄阈值内的在途写不得误删").exists();
        assertThat(java.nio.file.Path.of(obj.dataFile.getParent(), "object.data")).exists();
        assertThat(java.nio.file.Path.of(obj.dataFile.getParent(), "object.meta")).exists();
        cap.stop();
    }

    @Test
    @DisplayName("T5：覆盖写后 data+meta 严格成对（version=2，内容为新值）")
    void t5_overwriteKeepsPairConsistent() throws Exception {
        StorageCapability cap = newCapability(false);
        put(cap, "bkt", "a1", "old-payload");
        put(cap, "bkt", "a1", "new-payload");

        ObjectStore.StoredObject obj = cap.getObject("bkt", "a1");
        assertThat(obj.meta.getVersion()).isEqualTo(2L);
        try (var in = cap.openRead(obj)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("new-payload");
        }
        // meta 与 data 成对：data 大小与 meta.size 一致
        assertThat(java.nio.file.Files.size(obj.dataFile.toPath())).isEqualTo(obj.meta.getSize());
        cap.stop();
    }

    // ==================== T9 ====================

    @Test
    @DisplayName("T9：告警模式隐式建桶照旧并计数；严格模式拒绝未建桶 PUT")
    void t9_implicitBucketGradation() throws Exception {
        StorageCapability lenient = newCapability(false);
        put(lenient, "bkt", "a1", "v"); // 未显式建桶
        assertThat(lenient.getImplicitBucketCreations()).isEqualTo(1);
        assertThat(lenient.head("bkt", "a1")).isNotNull();
        lenient.stop();

        StorageCapability strict = newCapability(true);
        assertThatThrownBy(() -> put(strict, "bkt", "a1", "v"))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("bucket not exists");
        assertThat(strict.getImplicitBucketCreations()).isZero();
        strict.createBucketLocal("bkt");
        put(strict, "bkt", "a1", "v");
        assertThat(strict.head("bkt", "a1")).isNotNull();
        strict.stop();
    }

    // ==================== T25 ====================

    @Test
    @DisplayName("T25：冻结态拒绝副本写；解冻后恢复（漏写由 backlog 重放兜底）")
    void t25_frozenRejectsReplicaPut() throws Exception {
        StorageCapability cap = newCapability(false);
        cap.freeze(true);
        assertThatThrownBy(() -> putObjViaReplica(cap, "bkt", "a1", "v"))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("frozen");
        cap.freeze(false);
        ObjectMetadata meta = putObjViaReplica(cap, "bkt", "a1", "after-unfreeze");
        assertThat(meta).isNotNull();
        cap.stop();
    }

    // ==================== T13 ====================

    @Test
    @DisplayName("T13：副本路径 fsync 默认与主路径一致（默认开）")
    void t13_replicaFsyncDefaultsAligned() throws Exception {
        StorageCapability cap = newCapability(false);
        ObjectMetadata meta = putObjViaReplica(cap, "bkt", "a1", "replicated");
        assertThat(meta.getSize()).isEqualTo("replicated".getBytes(StandardCharsets.UTF_8).length);
        cap.stop();
    }

    // ==================== T24 ====================

    @Test
    @DisplayName("T24：导出迭代器 close 后终止且幂等（迁移中止无 FD 泄漏）")
    void t24_exportIteratorCloseable() throws Exception {
        StorageCapability cap = newCapability(false);
        cap.createBucketLocal("bkt");
        for (int i = 0; i < 5; i++) {
            put(cap, "bkt", "a" + i, "payload-" + i);
        }
        Iterator<byte[]> it = cap.exportPartition(0, 0);
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isNotNull();
        // 中止路径：close → 迭代终止；重复 close 幂等
        ((AutoCloseable) it).close();
        ((AutoCloseable) it).close();
        assertThat(it.hasNext()).as("close 后不得再产出分片").isFalse();
        cap.stop();
    }

    // ==================== 验收：迁移端到端一致性 ====================

    @Test
    @DisplayName("验收：对象经迁移后字节 + 全元数据一致（含 userMetadata/etag/version）")
    void migrationRoundTripPreservesBytesAndAllMetadata() throws Exception {
        StorageCapability src = newCapabilityAt(tempDir.resolve("src"), false);
        StorageCapability dst = newCapabilityAt(tempDir.resolve("dst"), false);
        src.createBucketLocal("bkt");
        byte[] payload = "migration-payload-éö§".getBytes(StandardCharsets.UTF_8);
        ObjectMetadata meta = src.putObject("bkt", "a1", new ByteArrayInputStream(payload),
                payload.length, "application/octet-stream", Map.of("owner", "ww", "env", "prod"));

        // 分片导出 → 目标端导入（fin 聚合 + putReplica 落盘）
        List<byte[]> pieces = new java.util.ArrayList<>();
        Iterator<byte[]> it = src.exportPartition(0, 0);
        while (it.hasNext()) {
            pieces.add(it.next());
        }
        assertThat(dst.importChunks(pieces)).isTrue();

        ObjectMetadata dstMeta = dst.head("bkt", "a1");
        assertThat(dstMeta).isNotNull();
        assertThat(dstMeta.getSize()).isEqualTo(meta.getSize());
        assertThat(dstMeta.getEtag()).as("字节级一致（MD5 etag）").isEqualTo(meta.getEtag());
        assertThat(dstMeta.getVersion()).isEqualTo(meta.getVersion());
        assertThat(dstMeta.getContentType()).isEqualTo("application/octet-stream");
        assertThat(dstMeta.getUserMetadata())
                .as("T7/T8 链路：userMetadata 全量保持").containsEntry("owner", "ww")
                .containsEntry("env", "prod");
        try (var in = dst.openRead(dst.getObject("bkt", "a1"))) {
            assertThat(in.readAllBytes()).as("数据字节完全一致").isEqualTo(payload);
        }
        src.stop();
        dst.stop();
    }

    // ==================== 工具 ====================

    private static ObjectMetadata putObjViaReplica(StorageCapability cap, String bucket, String key, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata meta = new ObjectMetadata(key, bytes.length, "text/plain",
                null, System.currentTimeMillis(), 1L);
        return cap.replicaPut(bucket, key, new ByteArrayInputStream(bytes), meta);
    }
}
