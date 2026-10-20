package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Partitioner;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MigrationEngine 端到端（同进程 Mock 源/目标）：
 * 多分区迁移 + CAS 切主、断点续跑对账零差、cutover CAS 失败、导入重试上限、取消与复用。
 */
class MigrationEngineTest {

    private static final int PARTITION_COUNT = 4096;
    private static final NodeId NODE_A = NodeId.of("node-a");
    private static final NodeId NODE_B = NodeId.of("node-b");

    /** 限流睡眠为 0 的快速配置（8GB/s + 4KB 分片）。 */
    private static MigrationConfig fastConfig() {
        return MigrationConfig.of(4, 8L * 1024 * 1024 * 1024, 4096, 8);
    }

    private static MockCapability mockCapability() {
        return new MockCapability(NODE_A, emptyView(), key -> Partitioner.partition(key, PARTITION_COUNT));
    }

    private static PlacementView emptyView() {
        return (cap, partition) -> Optional.empty();
    }

    private static int partitionOf(String seed) {
        return Partitioner.partition(seed, PARTITION_COUNT);
    }

    /** 收集 n 个互不相同的分区号（种子展开）。 */
    private static List<Integer> distinctPartitions(String prefix, int n) {
        List<Integer> result = new ArrayList<>(n);
        for (int i = 0; result.size() < n; i++) {
            int p = Partitioner.partition(prefix + "-" + i, PARTITION_COUNT);
            if (!result.contains(p)) {
                result.add(p);
            }
        }
        return result;
    }

    /** 归一收集指定分区的 count 条 KV 并预填（key 经 Partitioner.partition(key, 4096) 归一）。 */
    private static List<Map.Entry<String, String>> fill(MockCapability capability,
                                                        int partition, int count, String prefix) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(count);
        for (int i = 0; entries.size() < count; i++) {
            String key = prefix + "-" + partition + "-" + i;
            if (Partitioner.partition(key, PARTITION_COUNT) == partition) {
                entries.add(Map.entry(key, "value-" + key));
            }
        }
        capability.importPartition(partition, entries);
        return entries;
    }

    private static InMemoryPlacementStore placementsOn(int... partitions) {
        InMemoryPlacementStore store = new InMemoryPlacementStore();
        List<Placement> placements = new ArrayList<>();
        for (int partition : partitions) {
            placements.add(new Placement(partition, NODE_A, PlacementRole.PRIMARY, 0L));
        }
        store.replaceAll("mock", placements);
        return store;
    }

    @Test
    void migrateAndWaitMovesThreePartitionsWithCutoverEpochBump() {
        List<Integer> partitions = distinctPartitions("multi", 3);
        MockCapability capability = mockCapability();
        for (int p : partitions) {
            fill(capability, p, 60, "mk");
        }
        InMemoryPlacementStore placement = placementsOn(
                partitions.stream().mapToInt(Integer::intValue).toArray());
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        // NoClear：同进程 Mock 源/目标为同一实例，关闭源端清理以保留数据供断言
        MigrationEngine engine = new MigrationEngine(store,
                new NoClearAdapter(new MockPartitionAdapter(capability)),
                placement, fastConfig(), new TestClock());

        List<PartitionMove> moves = partitions.stream()
                .map(p -> new PartitionMove("mock", p, NODE_A, NODE_B))
                .toList();

        assertThat(engine.migrateAndWait("mock", moves)).isTrue();

        assertThat(store.listAll()).hasSize(3);
        for (MigrationTask task : store.listAll()) {
            assertThat(task.state()).isEqualTo(MigrationTaskState.COMPLETED);
            assertThat(task.checkpointOffset()).isEqualTo(60L);
            assertThat(task.itemsDone()).isEqualTo(60L);
            assertThat(task.checkpointJson()).contains("sourceCount").contains("60");
        }
        Map<Integer, NodeId> primaries = placement.primaries("mock");
        for (int p : partitions) {
            assertThat(primaries).containsEntry(p, NODE_B); // primary 已切换
            assertThat(placement.get("mock", p).orElseThrow().epoch())
                    .isEqualTo(1L); // CAS 成功一次 → epoch+1
            assertThat(capability.partitionCount(p)).isEqualTo(60L);
        }
        assertThat(store.listActive()).isEmpty();
    }

    @Test
    void cutoverPromotesWhenTargetAlreadyHoldsReplicaRow() {
        // 排空/治愈降级的结构性场景：目标节点已持有该分区 REPLICA 行，
        // 直接 cutover 会违反放置主键 (capability, partition_id, node_id)，
        // CUTOVER 阶段应改走副本提升（无损免迁移）。
        int partition = partitionOf("promote-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 10, "pr");
        InMemoryPlacementStore placement = new InMemoryPlacementStore();
        placement.replaceAll("mock", List.of(
                new Placement(partition, NODE_A, PlacementRole.PRIMARY, 0L),
                new Placement(partition, NODE_B, PlacementRole.REPLICA, 0L)));
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store,
                new NoClearAdapter(new MockPartitionAdapter(capability)),
                placement, fastConfig(), new TestClock());

        assertThat(engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)))).isTrue();

        MigrationTask task = store.listAll().get(0);
        assertThat(task.state()).isEqualTo(MigrationTaskState.COMPLETED);
        // 提升语义：原主行删除、B 副本行升主（epoch+1），无残留重复行
        List<Placement> rows = placement.placements("mock", partition);
        assertThat(rows).containsExactly(new Placement(partition, NODE_B, PlacementRole.PRIMARY, 1L));
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
    }

    @Test
    void sourcePartitionClearedAfterCompletion() {
        int partition = partitionOf("clear-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 30, "cl");
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store, new MockPartitionAdapter(capability),
                placement, fastConfig(), new TestClock());

        assertThat(engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)))).isTrue();

        MigrationTask task = store.listAll().get(0);
        assertThat(task.state()).isEqualTo(MigrationTaskState.COMPLETED);
        // COMPLETED 后源端分区被清理（尽力而为）：同进程 Mock 即目标数据一并清空
        assertThat(capability.partitionCount(partition)).isZero();
        assertThat(store.listActive()).isEmpty();
    }

    @Test
    void resumeAfterMidCopyFailureReachesCompletionWithExactReconciliation() {
        int partition = partitionOf("resume-seed");
        MockCapability capability = mockCapability();
        List<Map.Entry<String, String>> before = fill(capability, partition, 500, "rk");
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();

        // 首跑：COPYING 阶段 itemsDone 达 200 时模拟一次崩溃（包装 adapter 抛出）
        FailOnceAt200Adapter flaky = new FailOnceAt200Adapter(
                new NoClearAdapter(new MockPartitionAdapter(capability)));
        MigrationEngine firstEngine = new MigrationEngine(store, flaky, placement,
                fastConfig(), new TestClock());
        assertThat(firstEngine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)))).isFalse();

        MigrationTask failed = store.listAll().get(0);
        assertThat(failed.state()).isEqualTo(MigrationTaskState.FAILED);
        assertThat(failed.checkpointOffset()).isEqualTo(200L); // 断点恰停在第 200 条

        // 模拟重启：同一 store + 正常 adapter 的新引擎续跑（NoClear 保留源数据供对账）
        MigrationEngine resumedEngine = new MigrationEngine(store,
                new NoClearAdapter(new MockPartitionAdapter(capability)),
                placement, fastConfig(), new TestClock());
        resumedEngine.resumeAll();

        MigrationTask done = store.get(failed.taskId()).orElseThrow();
        assertThat(done.state()).isEqualTo(MigrationTaskState.COMPLETED);
        assertThat(done.checkpointOffset()).isEqualTo(500L);
        assertThat(done.itemsDone()).isEqualTo(500L);

        // 对账零差：条数精确一致、键值完全一致、无重复键（Map 语义）
        assertThat(capability.partitionCount(partition)).isEqualTo(500L);
        assertThat(capability.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(before);
        // 续跑完成切主：epoch 0 → 1
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
        assertThat(placement.get("mock", partition).orElseThrow().epoch()).isEqualTo(1L);
    }

    @Test
    void cutoverCasFailureMarksTaskFailedWithoutSwitchingPrimary() {
        int partition = partitionOf("cas-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 30, "ck");
        InMemoryPlacementStore realPlacement = placementsOn(partition);
        CutoverDenyStore placement = new CutoverDenyStore(realPlacement);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store, new MockPartitionAdapter(capability),
                placement, fastConfig(), new TestClock());

        boolean result = engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)));

        assertThat(result).isFalse();
        MigrationTask task = store.listAll().get(0);
        assertThat(task.state()).isEqualTo(MigrationTaskState.FAILED);
        assertThat(task.checkpointOffset()).isEqualTo(30L); // 数据面已完成，卡在 CUTOVER
        assertThat(realPlacement.primaries("mock")).containsEntry(partition, NODE_A); // 未切主
        assertThat(realPlacement.get("mock", partition).orElseThrow().epoch()).isZero();
        assertThat(store.listActive()).isEmpty();
    }

    @Test
    void persistentImportFailureMarksTaskFailedAfterRetries() {
        int partition = partitionOf("import-fail-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 10, "fk");
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store,
                new AlwaysFailImportAdapter(new MockPartitionAdapter(capability)),
                placement, fastConfig(), new TestClock());

        assertThat(engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)))).isFalse();

        MigrationTask task = store.listAll().get(0);
        assertThat(task.state()).isEqualTo(MigrationTaskState.FAILED);
        assertThat(task.checkpointOffset()).isZero(); // 无任何成功分片
    }

    @Test
    void duplicateExecutorAbandonsWhenTaskAlreadyCompletedByAnother() {
        // 多节点共享 DB 的结构性并发：任务已被并发执行者推进到 COMPLETED（其 TransferStream
        // 会以 "already terminal" 拒绝本执行者的迟到分片）。本执行者导入失败时应放弃——
        // 任务保持 COMPLETED，不得打出 "failed at COMPLETED" 假警报或尝试 FAILED 跃迁。
        int partition = partitionOf("dup-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 10, "dk");
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store,
                new CompleteOnFirstImportAdapter(store),
                placement, fastConfig(), new TestClock());

        assertThat(engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)))).isTrue();

        assertThat(store.listAll()).hasSize(1);
        assertThat(store.listAll().get(0).state()).isEqualTo(MigrationTaskState.COMPLETED);
    }

    @Test
    void cancelMarksPendingTaskCancelledAndStaysTerminal() {
        int partition = partitionOf("cancel-seed");
        MockCapability capability = mockCapability();
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store, new MockPartitionAdapter(capability),
                placement, fastConfig(), new TestClock());

        long taskId = store.create("mock", partition, NODE_A, NODE_B);
        assertThat(engine.cancel(taskId)).isTrue();
        assertThat(store.get(taskId).orElseThrow().state()).isEqualTo(MigrationTaskState.CANCELLED);

        assertThat(engine.cancel(taskId)).isFalse(); // 终态不可再取消
        assertThat(store.get(999L)).isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> engine.cancel(999L))
                .isInstanceOf(com.ww.runtime.common.MigrationException.class);

        engine.resumeAll(); // CANCELLED 不属于续跑范围
        assertThat(store.get(taskId).orElseThrow().state()).isEqualTo(MigrationTaskState.CANCELLED);
        assertThat(store.listActive()).isEmpty();
    }

    @Test
    void migrateAndWaitReusesActiveTaskInsteadOfCreatingDuplicate() {
        int partition = partitionOf("reuse-seed");
        MockCapability capability = mockCapability();
        fill(capability, partition, 20, "uk");
        InMemoryPlacementStore placement = placementsOn(partition);
        InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
        MigrationEngine engine = new MigrationEngine(store, new MockPartitionAdapter(capability),
                placement, fastConfig(), new TestClock());

        long preCreated = store.create("mock", partition, NODE_A, NODE_B); // 预置 PENDING 任务
        boolean result = engine.migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)));

        assertThat(result).isTrue();
        assertThat(store.listAll()).hasSize(1); // 复用活动任务，未重复建
        MigrationTask task = store.listAll().get(0);
        assertThat(task.taskId()).isEqualTo(preCreated);
        assertThat(task.state()).isEqualTo(MigrationTaskState.COMPLETED);
    }

    // ------------------------------------------------------------------
    // 测试桩
    // ------------------------------------------------------------------

    /**
     * 关闭源端清理的包装：同进程 Mock 源/目标为同一实例，COMPLETED 后的
     * clearPartition 会把（共享的）数据一并清空——对账类测试用它保留数据。
     */
    private static final class NoClearAdapter implements PartitionDataAdapter {

        private final PartitionDataAdapter delegate;

        NoClearAdapter(PartitionDataAdapter delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public long itemCount(String capability, int partition) {
            return delegate.itemCount(capability, partition);
        }

        @Override
        public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            return delegate.exportChunks(capability, partition, fromCheckpoint);
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            return delegate.importChunks(capability, partition, chunks);
        }

        @Override
        public long verifyCount(String capability, int partition) {
            return delegate.verifyCount(capability, partition);
        }

        @Override
        public boolean clearPartition(String capability, int partition,
                                      com.ww.runtime.common.NodeId source) {
            return true; // 不清理
        }
    }

    /** COPYING 阶段累计导入 ≥ 200 条时抛一次异常（模拟节点崩溃）。 */
    private static final class FailOnceAt200Adapter implements PartitionDataAdapter {

        private final PartitionDataAdapter delegate;
        private long imported;
        private boolean alreadyFailed;

        FailOnceAt200Adapter(PartitionDataAdapter delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public long itemCount(String capability, int partition) {
            return delegate.itemCount(capability, partition);
        }

        @Override
        public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            return delegate.exportChunks(capability, partition, fromCheckpoint);
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            if (!alreadyFailed && imported >= 200L) {
                alreadyFailed = true;
                throw new IllegalStateException("simulated crash at checkpoint " + imported);
            }
            boolean ok = delegate.importChunks(capability, partition, chunks);
            if (ok) {
                for (byte[] chunk : chunks) {
                    imported += ChunkCodec.countEntries(chunk);
                }
            }
            return ok;
        }

        @Override
        public long verifyCount(String capability, int partition) {
            return delegate.verifyCount(capability, partition);
        }
    }

    /** importChunks 恒 false（触发引擎单片 3 次重试后 FAILED）。 */
    private static final class AlwaysFailImportAdapter implements PartitionDataAdapter {

        private final PartitionDataAdapter delegate;

        AlwaysFailImportAdapter(PartitionDataAdapter delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public long itemCount(String capability, int partition) {
            return delegate.itemCount(capability, partition);
        }

        @Override
        public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            return delegate.exportChunks(capability, partition, fromCheckpoint);
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            return false;
        }

        @Override
        public long verifyCount(String capability, int partition) {
            return delegate.verifyCount(capability, partition);
        }
    }

    /**
     * 模拟并发双执行者：首次 importChunks 时由「执行者 A」把任务合法推进到
     * COMPLETED（COPYING→SYNCING→CUTOVER→VERIFYING→COMPLETED），随后导入恒
     * false——模拟执行者 B 的迟到分片被对端 TransferStream 以终态拒绝。
     */
    private static final class CompleteOnFirstImportAdapter implements PartitionDataAdapter {

        private final MigrationTaskStore store;
        private boolean completedByOther;

        CompleteOnFirstImportAdapter(MigrationTaskStore store) {
            this.store = Objects.requireNonNull(store);
        }

        @Override
        public long itemCount(String capability, int partition) {
            return 1;
        }

        @Override
        public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            return List.of(new byte[]{0x01, 0x02}).iterator();
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            if (!completedByOther) {
                completedByOther = true;
                long taskId = store.listActive().get(0).taskId();
                store.transition(taskId, MigrationTaskState.SYNCING);
                store.transition(taskId, MigrationTaskState.CUTOVER);
                store.transition(taskId, MigrationTaskState.VERIFYING);
                store.transition(taskId, MigrationTaskState.COMPLETED);
            }
            return false;
        }

        @Override
        public long verifyCount(String capability, int partition) {
            return 0;
        }
    }

    /** 预置 epoch 不符的 cutover 效果：全部委派真实 store，但 cutover 恒 false。 */
    private static final class CutoverDenyStore implements PlacementStore {

        private final PlacementStore delegate;

        CutoverDenyStore(PlacementStore delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void replaceAll(String capability, List<Placement> placements) {
            delegate.replaceAll(capability, placements);
        }

        @Override
        public List<Placement> list(String capability) {
            return delegate.list(capability);
        }

        @Override
        public Optional<Placement> get(String capability, int partition) {
            return delegate.get(capability, partition);
        }

        @Override
        public List<Placement> placements(String capability, int partition) {
            return delegate.placements(capability, partition);
        }

        @Override
        public Map<Integer, NodeId> primaries(String capability) {
            return delegate.primaries(capability);
        }

        @Override
        public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
            return false; // 模拟 CAS 竞争失败（epoch 已被并发修改）
        }

        @Override
        public boolean promoteReplica(String capability, int partition, NodeId newNode, long expectedEpoch) {
            return delegate.promoteReplica(capability, partition, newNode, expectedEpoch);
        }

        @Override
        public void reassignReplicas(String capability, int partition, List<NodeId> newReplicas) {
            delegate.reassignReplicas(capability, partition, newReplicas);
        }
    }
}
