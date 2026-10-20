package com.ww.runtime.server.cache;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.data.CompositePartitionAdapter;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CachePartitionAdapter;
import com.ww.runtime.data.cache.CachePartitionEntry;
import com.ww.runtime.migration.InMemoryMigrationTaskStore;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.migration.MockPartitionAdapter;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PartitionSweeper keep 规则：属主分区保留、非属主清除、FAILED（可重试）任务分区保留、
 * COMPLETED 任务源分区清除、无放置表跳过、不支持清扫的能力 no-op。
 */
class PartitionSweeperTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId OTHER = NodeId.of("node-2");

    /** key 首字母决定分区：a*→0, b*→1。 */
    private static int partitioner(String key) {
        return key.charAt(0) - 'a';
    }

    private final InMemoryPlacementStore placement = new InMemoryPlacementStore();
    private final MigrationTaskStore taskStore = new InMemoryMigrationTaskStore();
    private CacheCapability capability;
    private CompositePartitionAdapter composite;

    @BeforeEach
    void setUp() throws Exception {
        capability = new CacheCapability(SELF,
                (cap, partition) -> Optional.of(new PlacementView.Owner(SELF, 1L)),
                PartitionSweeperTest::partitioner, false, null);
        capability.start();
        composite = new CompositePartitionAdapter(Map.of(
                "CACHE", (com.ww.runtime.migration.PartitionDataAdapter) new CachePartitionAdapter(capability),
                "MOCK", new MockPartitionAdapter(new MockCapability(SELF,
                        (cap, partition) -> Optional.empty(),
                        PartitionSweeperTest::partitioner))));
    }

    private PartitionSweeper sweeper() {
        return new PartitionSweeper(composite, placement, taskStore, SELF);
    }

    /** 分区 0 → owner，分区 1 → other（与 partitioner 的 a*→0 / b*→1 对齐）。 */
    private void placePartition0To(NodeId owner) {
        placement.replaceAll("CACHE", List.of(
                new Placement(0, owner, PlacementRole.PRIMARY, 0L),
                new Placement(1, OTHER, PlacementRole.PRIMARY, 0L)));
    }

    @Test
    void sweepRemovesNonOwnedPartitionsAndKeepsOwned() {
        placePartition0To(SELF); // 分区 0 归本节点，分区 1 归他人
        capability.importPartition(0, List.of(new CachePartitionEntry("a-owned", 0L, "keep")));
        capability.importPartition(1, List.of(new CachePartitionEntry("b-stale", 0L, "drop")));

        sweeper().sweepAll();

        assertThat(capability.get("a-owned")).isEqualTo("keep");
        assertThat(capability.get("b-stale")).isNull();
    }

    @Test
    void sweepKeepsPartitionsOfFailedTasksInvolvingSelf() {
        placePartition0To(OTHER); // 分区 1 归他人
        capability.importPartition(1, List.of(new CachePartitionEntry("b-partial", 0L, "partial-import")));

        // FAILED 任务（self 为目标）：断点续跑将恢复——部分导入数据必须保留
        long taskId = taskStore.create("CACHE", 1, OTHER, SELF);
        taskStore.transition(taskId, MigrationTaskState.PREPARING);
        taskStore.transition(taskId, MigrationTaskState.FAILED);

        sweeper().sweepAll();

        assertThat(capability.get("b-partial")).isEqualTo("partial-import");
    }

    @Test
    void sweepClearsSourcePartitionOfCompletedTask() {
        placePartition0To(OTHER);
        capability.importPartition(1, List.of(new CachePartitionEntry("b-migrated", 0L, "already-moved")));

        // COMPLETED 任务（self 为源，目标已对账）→ 源端残留可清
        long taskId = taskStore.create("CACHE", 1, SELF, OTHER);
        for (MigrationTaskState state : List.of(MigrationTaskState.PREPARING,
                MigrationTaskState.SNAPSHOT, MigrationTaskState.COPYING, MigrationTaskState.SYNCING,
                MigrationTaskState.CUTOVER, MigrationTaskState.VERIFYING, MigrationTaskState.COMPLETED)) {
            taskStore.transition(taskId, state);
        }

        sweeper().sweepAll();

        assertThat(capability.get("b-migrated")).isNull();
    }

    @Test
    void sweepSkipsWhenPlacementRingAbsent() {
        // CACHE 无放置行（环未引导）——无法判定归属，绝不删除
        capability.importPartition(0, List.of(new CachePartitionEntry("a-any", 0L, "unknown-ownership")));

        sweeper().sweepAll();

        assertThat(capability.get("a-any")).isEqualTo("unknown-ownership");
    }

    @Test
    void sweepIsNoOpForCapabilitiesWithoutRetainSupport() {
        // MOCK 未实现 retainPartitions（default 0）——不抛异常、不影响 CACHE 清扫
        placePartition0To(SELF);
        placement.replaceAll("MOCK",
                List.of(new Placement(0, OTHER, PlacementRole.PRIMARY, 0L)));
        capability.importPartition(0, List.of(new CachePartitionEntry("a-keep", 0L, "v")));

        sweeper().sweepAll();

        assertThat(capability.get("a-keep")).isEqualTo("v");
    }

    @Test
    void sweepKeepsDegradedPartitionsEvenWhenNotOwned() {
        // P0-2：分区 1 已迁归他人，但处于 DEGRADED 隔离（最后副本在死亡属主页上）——
        // 本节点磁盘数据是抢救源，绝不可按"非归属残留"删除
        placePartition0To(OTHER); // 分区 0 归 SELF，分区 1 归 OTHER
        capability.importPartition(1, List.of(new CachePartitionEntry("b-quarantined", 0L, "last-copy")));

        com.ww.runtime.placement.InMemoryDegradedPartitionStore degraded =
                new com.ww.runtime.placement.InMemoryDegradedPartitionStore();
        degraded.mark("CACHE", 1, OTHER, "no live replica");

        new PartitionSweeper(composite, placement, taskStore, SELF, degraded).sweepAll();

        assertThat(capability.get("b-quarantined")).isEqualTo("last-copy");
    }

    @Test
    void sweepClearsDegradedPartitionDataAfterForceAdopt() {
        // 对照：force-adopt 后隔离标记清除（放置已改挂）→ 遗留数据按残留正常清理
        placePartition0To(OTHER);
        capability.importPartition(1, List.of(new CachePartitionEntry("b-orphan", 0L, "loss-accepted")));

        com.ww.runtime.placement.InMemoryDegradedPartitionStore degraded =
                new com.ww.runtime.placement.InMemoryDegradedPartitionStore();
        // 无隔离标记（已 force-adopt 清除）→ 非归属数据可清
        new PartitionSweeper(composite, placement, taskStore, SELF, degraded).sweepAll();

        assertThat(capability.get("b-orphan")).isNull();
    }
}
