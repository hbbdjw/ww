package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CacheCapability：fencing 写入、生命周期钩子与分区级迁移 API。
 */
class CacheCapabilityTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId OTHER = NodeId.of("node-2");

    /** key 首字母决定分区：a*→0, b*→1。 */
    private static int partitioner(String key) {
        return key.charAt(0) - 'a';
    }

    private StubView view;
    private CacheCapability capability;

    @BeforeEach
    void setUp() throws Exception {
        view = new StubView();
        capability = new CacheCapability(SELF, view, CacheCapabilityTest::partitioner, false, null);
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
    }

    // ---- fencing ----

    @Test
    void typeIsCache() {
        assertThat(capability.type()).isEqualTo(CapabilityType.CACHE);
    }

    @Test
    void putRejectsWhenPartitionHasNoOwner() {
        view.owner = Optional.empty();
        assertThatThrownBy(() -> capability.put("a1", "v", 0L))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.get("a1")).isNull();
    }

    @Test
    void putRejectsWhenOwnerIsAnotherNode() {
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.put("a1", "v", 0L))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void putAcceptsWhenOwnerIsSelf() {
        capability.put("a1", "v1", 0L);
        capability.put("a2", 42, 60_000L); // 带 TTL
        assertThat(capability.get("a1")).isEqualTo("v1");
        assertThat(capability.get("a2")).isEqualTo(42);
        assertThat(capability.totalItems()).isEqualTo(2L);
    }

    @Test
    void putIfAbsentRespectsFencingAndWritesOnlyWhenAbsent() {
        assertThat(capability.putIfAbsent("a1", "v1", 0L)).isTrue();
        assertThat(capability.putIfAbsent("a1", "v2", 60_000L)).isFalse();
        assertThat(capability.get("a1")).isEqualTo("v1");

        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.putIfAbsent("b1", "v", 0L))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.get("b1")).isNull();

        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
        capability.freeze(true);
        assertThatThrownBy(() -> capability.putIfAbsent("a2", "v", 0L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");
    }

    @Test
    void removeRespectsFencingAndDeletes() {
        capability.put("a1", "v1", 0L);
        assertThat(capability.remove("a1")).isTrue();
        assertThat(capability.get("a1")).isNull();

        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.remove("b1"))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void drainAndFreezeRejectWritesButAllowReads() {
        capability.put("a1", "v1", 0L);
        capability.drain();
        assertThatThrownBy(() -> capability.put("a2", "v", 0L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");
        assertThat(capability.get("a1")).isEqualTo("v1"); // 读不受冻结影响

        capability.freeze(false);
        capability.put("a2", "v2", 0L); // 解冻恢复写
        assertThat(capability.get("a2")).isEqualTo("v2");
    }

    @Test
    void stopClearsDataAndRestartIsReentrant() throws Exception {
        capability.put("a1", "v1", 0L);
        capability.stop();
        assertThat(capability.totalItems()).isZero();

        capability.start();
        capability.put("a1", "v2", 0L);
        assertThat(capability.get("a1")).isEqualTo("v2");
        capability.warm(); // 不抛
    }

    // ---- 迁移数据面 ----

    @Test
    void exportImportClearPartitionRoundTrip() {
        capability.put("a1", "v1", 0L);
        capability.put("a2", "v2", 0L);
        capability.put("b1", "v3", 0L); // 分区 1，不应被导出

        List<CachePartitionEntry> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(2);
        assertThat(exported.get(0).key()).isEqualTo("a1"); // 字典序
        assertThat(exported.get(1).key()).isEqualTo("a2");
        assertThat(capability.partitionCount(0)).isEqualTo(2L);
        assertThat(capability.partitionCount(1)).isEqualTo(1L);

        // 导入到“另一节点”的同能力实例
        CacheCapability target = newTarget();
        target.importPartition(0, exported);
        assertThat(target.get("a1")).isEqualTo("v1");
        assertThat(target.get("a2")).isEqualTo("v2");
        assertThat(target.partitionCount(0)).isEqualTo(2L);

        // 导入键不属于该分区 → 拒绝
        assertThatThrownBy(() -> target.importPartition(0,
                List.of(new CachePartitionEntry("b1", 0L, "x"))))
                .isInstanceOf(IllegalArgumentException.class);

        // 源端清空分区
        capability.clearPartition(0);
        assertThat(capability.partitionCount(0)).isZero();
        assertThat(capability.get("b1")).isEqualTo("v3"); // 其他分区不受影响
    }

    @Test
    void exportPreservesTtl() {
        capability.put("a1", "v", 60_000L);
        List<CachePartitionEntry> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(1);
        assertThat(exported.get(0).remainingTtlMillis()).isBetween(1L, 60_000L);

        capability.put("a2", "v", 0L); // 永不过期 → 归一为 <=0
        exported = capability.exportPartition(0);
        CachePartitionEntry noTtl = exported.stream()
                .filter(e -> e.key().equals("a2")).findFirst().orElseThrow();
        assertThat(noTtl.remainingTtlMillis()).isLessThanOrEqualTo(0L);
    }

    // ---- AOF 持久化（回放接线） ----

    @Test
    void aofReplayRestoresDataAcrossRestartWithoutDuplication() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("cache-aof-it");
        try {
            CacheCapability original = newAofCapability(dir);
            original.put("a1", "persisted", 0L);
            original.put("a2", 42, 60_000L);
            original.stop();

            // 重启：同目录新实例回放恢复（含 TTL）
            CacheCapability restarted = newAofCapability(dir);
            assertThat(restarted.get("a1")).isEqualTo("persisted");
            assertThat(restarted.get("a2")).isEqualTo(42);
            assertThat(restarted.remainingTtlMillis("a2")).isBetween(1L, 60_000L);

            // 回放写不重复记日志：重启后再写一条 → stop → 再重启，数据仍精确（无翻倍副作用）
            restarted.put("a3", "third", 0L);
            restarted.stop();
            CacheCapability third = newAofCapability(dir);
            assertThat(third.get("a1")).isEqualTo("persisted");
            assertThat(third.get("a3")).isEqualTo("third");
            assertThat(third.countKeys(null)).isEqualTo(3L);
            third.stop();
        } finally {
            deleteRecursively(dir.toFile());
        }
    }

    @Test
    void aofReplayRestoresImportedContainersAndStreams() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("cache-aof-full");
        try {
            CachePartitionEntry.StreamSnapshot snapshot = new CachePartitionEntry.StreamSnapshot(
                    List.of(new CachePartitionEntry.SnapshotEntry(1000L, 1L, Map.of("f", "v"))),
                    List.of(new CachePartitionEntry.GroupSnapshot("g", 1000L, 1L,
                            List.of(new CachePartitionEntry.ConsumerSnapshot("c",
                                    List.of(1000L, 1L))))));
            CacheCapability target = newAofCapability(dir);
            // 全类型导入（模拟迁移目标端落地）
            target.importPartition(0, List.of(
                    new CachePartitionEntry("a1", CachePartitionEntry.Kind.KV, 0L, "v1"),
                    new CachePartitionEntry("a2", CachePartitionEntry.Kind.HASH, 0L,
                            new java.util.LinkedHashMap<>(Map.of("f", "hv"))),
                    new CachePartitionEntry("a3", CachePartitionEntry.Kind.LIST, 0L,
                            List.of("x", "y")),
                    new CachePartitionEntry("a4", CachePartitionEntry.Kind.STREAM, 0L,
                            snapshot)));
            target.stop();

            // 重启回放：导入时补记的等价 AOF 命令恢复全部类型
            CacheCapability restarted = newAofCapability(dir);
            List<CachePartitionEntry> restored = restarted.exportPartition(0);
            assertThat(restored).hasSize(4);
            assertThat(restored.stream().map(CachePartitionEntry::kind))
                    .containsExactlyInAnyOrder(
                            CachePartitionEntry.Kind.KV, CachePartitionEntry.Kind.HASH,
                            CachePartitionEntry.Kind.LIST, CachePartitionEntry.Kind.STREAM);
            assertThat(restarted.get("a1")).isEqualTo("v1");
            restarted.stop();
        } finally {
            deleteRecursively(dir.toFile());
        }
    }

    // ---- 全类型迁移 + 清扫 ----

    @Test
    void fullTypeExportImportRoundTrip() throws Exception {
        CachePartitionEntry.StreamSnapshot streamSnapshot = new CachePartitionEntry.StreamSnapshot(
                List.of(new CachePartitionEntry.SnapshotEntry(1000L, 1L, Map.of("k", "v")),
                        new CachePartitionEntry.SnapshotEntry(1000L, 2L, Map.of("k2", "v2"))),
                List.of(new CachePartitionEntry.GroupSnapshot("g1", 1000L, 2L,
                        List.of(new CachePartitionEntry.ConsumerSnapshot("c1",
                                List.of(1000L, 1L))))));
        capability.importPartition(0, List.of(
                new CachePartitionEntry("a1", CachePartitionEntry.Kind.KV, 0L, "v1"),
                // 注：TTL=0（往返精确相等断言不受时间漂移影响；TTL 语义另见 exportPreservesTtl）
                new CachePartitionEntry("a2", CachePartitionEntry.Kind.HASH, 0L,
                        new java.util.LinkedHashMap<>(Map.of("f1", "v1", "f2", 2))),
                new CachePartitionEntry("a3", CachePartitionEntry.Kind.LIST, 0L,
                        List.of("l1", "l2", "l3")),
                new CachePartitionEntry("a4", CachePartitionEntry.Kind.SET, 0L,
                        List.of("m1", "m2")),
                new CachePartitionEntry("a5", CachePartitionEntry.Kind.ZSET, 0L,
                        List.of(new CachePartitionEntry.ScoredMember(1.0, "z1"),
                                new CachePartitionEntry.ScoredMember(2.0, "z2"))),
                new CachePartitionEntry("a6", CachePartitionEntry.Kind.STREAM, 0L,
                        streamSnapshot)));
        capability.put("b1", "other-partition", 0L);

        List<CachePartitionEntry> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(6);
        assertThat(exported.stream().map(CachePartitionEntry::kind).distinct())
                .containsExactlyInAnyOrder(CachePartitionEntry.Kind.values());
        assertThat(capability.partitionCount(0)).isEqualTo(6L);

        // 目标端导入 → 再导出，内容一致（幂等往返）
        CacheCapability target = newTarget();
        target.importPartition(0, exported);
        List<CachePartitionEntry> reExported = target.exportPartition(0);
        assertThat(reExported).isEqualTo(exported);

        // 源端清理分区 0：全类型清除，分区 1 不受影响
        capability.clearPartition(0);
        assertThat(capability.partitionCount(0)).isZero();
        assertThat(capability.get("b1")).isEqualTo("other-partition");
    }

    @Test
    void retainPartitionsKeepsOwnedAndClearsStale() {
        capability.put("a1", "owned", 0L);   // 分区 0
        capability.put("b1", "stale", 0L);   // 分区 1

        long removed = capability.retainPartitions(p -> p == 0);
        assertThat(removed).isEqualTo(1L);
        assertThat(capability.get("a1")).isEqualTo("owned");
        assertThat(capability.get("b1")).isNull();
    }

    @Test
    void servesPartitionOfTracksOwnership() {
        assertThat(capability.servesPartitionOf("a1")).isTrue(); // owner=self
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThat(capability.servesPartitionOf("a1")).isFalse(); // owner=other
        view.owner = Optional.empty();
        assertThat(capability.servesPartitionOf("a1")).isTrue(); // 无放置（单机退化）
    }

    /**
     * 管理面列举口径：包含任意值类型——json 对象值（Map）与 null 值的 key
     * 写入成功后必须在 listKeys/countKeys 可见（否则「新增成功但列表无数据」）。
     */
    @Test
    void listKeysAndCountIncludeAllValueTypes() throws Exception {
        capability.put("a1", "plain", 0L);
        // json 对象值（HTTP type=json 落库为 LinkedHashMap）+ null 值（迁移/导入路径可产生）
        capability.importPartition(0, List.of(
                new CachePartitionEntry("a2", CachePartitionEntry.Kind.KV, 0L,
                        new java.util.LinkedHashMap<>(Map.of("f", "v"))),
                new CachePartitionEntry("a3", CachePartitionEntry.Kind.KV, 0L, null)));
        capability.put("b1", "other-partition", 0L); // 分区 1：前缀过滤的对照

        assertThat(capability.listKeys(null, 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1", "a2", "a3", "b1");
        assertThat(capability.listKeys("a", 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1", "a2", "a3");
        assertThat(capability.listKeys("a", 2))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1", "a2"); // 排序后截断：子集确定
        assertThat(capability.countKeys(null)).isEqualTo(4L);
        assertThat(capability.countKeys("a")).isEqualTo(3L);
    }

    @Test
    void listKeysAndCountExcludeExpiredEntries() throws Exception {
        capability.put("a1", "live", 60_000L);
        capability.put("a2", "expired", 1L);
        capability.put("a3", "forever", 0L);
        Thread.sleep(10);

        assertThat(capability.listKeys(null, 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1", "a3");
        assertThat(capability.countKeys(null)).isEqualTo(2L);
    }

    private CacheCapability newAofCapability(java.nio.file.Path dir) throws Exception {
        CacheCapability capability = new CacheCapability(
                SELF, (cap, partition) -> Optional.of(new PlacementView.Owner(SELF, 1L)),
                CacheCapabilityTest::partitioner, true, dir.toFile());
        capability.start();
        return capability;
    }

    private static void deleteRecursively(java.io.File file) {
        java.io.File[] children = file.listFiles();
        if (children != null) {
            for (java.io.File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private CacheCapability newTarget() {
        try {
            CacheCapability target = new CacheCapability(
                    OTHER, (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                    CacheCapabilityTest::partitioner, false, null);
            target.start();
            return target;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 可变桩视图：测试中直接改 owner 字段。 */
    private static final class StubView implements PlacementView {
        volatile Optional<Owner> owner = Optional.empty();

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return owner;
        }
    }
}
