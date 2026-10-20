package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Partitioner;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RoutingPartitionAdapter 跨节点迁移端到端（进程内三节点：源 A / 目标 B / 执行节点 C）：
 * 数据经 FetchPartition 从源拉取、经 TransferStream 推送到目标、CountPartition/ClearPartition
 * RPC 生效、死源按空分区迁移、from=null 引导型 move、执行节点自身为源/目标的本地直连路径。
 */
class RoutingPartitionAdapterTest {

    private static final int PARTITION_COUNT = 4096;
    private static final NodeId NODE_A = NodeId.of("node-a");
    private static final NodeId NODE_B = NodeId.of("node-b");
    private static final NodeId NODE_C = NodeId.of("node-c"); // 迁移执行节点（leader 视角）

    private final InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
    private final InMemoryPlacementStore placement = new InMemoryPlacementStore();

    private final MockCapability capabilityA = mockCapability(NODE_A);
    private final MockCapability capabilityB = mockCapability(NODE_B);
    private final MockCapability capabilityC = mockCapability(NODE_C); // 执行节点本地数据面

    private GrpcServerHost hostA;
    private GrpcServerHost hostB;
    private GrpcServerHost hostC;
    private RoutingPartitionAdapter routing;
    private final Map<NodeId, RoutingPartitionAdapter.Endpoint> endpoints = new ConcurrentHashMap<>();

    private static MockCapability mockCapability(NodeId node) {
        return new MockCapability(node, emptyView(),
                key -> Partitioner.partition(key, PARTITION_COUNT));
    }

    private static PlacementView emptyView() {
        return (cap, partition) -> Optional.empty();
    }

    private static int partitionOf(String seed) {
        return Partitioner.partition(seed, PARTITION_COUNT);
    }

    /** 归一收集指定分区的 count 条 KV 并预填到给定能力实例。 */
    private List<Map.Entry<String, String>> fill(MockCapability capability, int partition,
                                                 int count, String prefix) {
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

    @BeforeEach
    void setUp() throws Exception {
        hostA = startHost(new MockPartitionAdapter(capabilityA));
        hostB = startHost(new MockPartitionAdapter(capabilityB));
        hostC = startHost(new MockPartitionAdapter(capabilityC));
        endpoints.put(NODE_A, new RoutingPartitionAdapter.Endpoint("localhost", hostA.getPort()));
        endpoints.put(NODE_B, new RoutingPartitionAdapter.Endpoint("localhost", hostB.getPort()));
        endpoints.put(NODE_C, new RoutingPartitionAdapter.Endpoint("localhost", hostC.getPort()));
        Function<NodeId, Optional<RoutingPartitionAdapter.Endpoint>> resolver =
                node -> Optional.ofNullable(endpoints.get(node));
        routing = new RoutingPartitionAdapter(new MockPartitionAdapter(capabilityC), NODE_C, resolver);
    }

    private GrpcServerHost startHost(MockPartitionAdapter adapter) throws Exception {
        GrpcServerHost host = new GrpcServerHost();
        host.addService(new MigrationServer(adapter, store, new TestClock()));
        host.start(0);
        return host;
    }

    @AfterEach
    void tearDown() throws Exception {
        routing.close();
        stopHost(hostA);
        stopHost(hostB);
        stopHost(hostC);
    }

    private static void stopHost(GrpcServerHost host) throws Exception {
        host.stop();
        host.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }

    private MigrationEngine engine() {
        return new MigrationEngine(store, routing, placement,
                MigrationConfig.of(4, 8L * 1024 * 1024 * 1024, 4096, 8), new TestClock());
    }

    private void placeOwner(int partition, NodeId owner) {
        placement.replaceAll("mock", List.of(new Placement(partition, owner, PlacementRole.PRIMARY, 0L)));
    }

    @Test
    void crossNodeMigrationMovesDataFromSourceToTargetAndClearsSource() {
        int partition = partitionOf("cross-seed");
        List<Map.Entry<String, String>> before = fill(capabilityA, partition, 40, "ck");
        placeOwner(partition, NODE_A);

        boolean ok = engine().migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)));

        assertThat(ok).isTrue();
        MigrationTask task = store.listAll().get(0);
        assertThat(task.state()).isEqualTo(MigrationTaskState.COMPLETED);
        assertThat(task.itemsDone()).isEqualTo(40L);

        // 数据落到目标 B；源 A 分区被清理（cutover 后 fencing 保证无新写竞态）
        assertThat(capabilityB.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(before);
        assertThat(capabilityA.partitionCount(partition)).isZero();
        // 切主生效：primary A→B，epoch+1
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
        assertThat(placement.get("mock", partition).orElseThrow().epoch()).isEqualTo(1L);
    }

    @Test
    void deadSourceIsTreatedAsEmptyPartition() {
        int partition = partitionOf("dead-seed");
        fill(capabilityA, partition, 20, "dk");
        placeOwner(partition, NODE_A);
        endpoints.remove(NODE_A); // 源不可解析（非 ACTIVE/已下线）

        boolean ok = engine().migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_B)));

        // 数据丢失窗口（与调度器治愈迁移「no live replica」同语义）：按空分区收敛
        assertThat(ok).isTrue();
        assertThat(store.listAll().get(0).state()).isEqualTo(MigrationTaskState.COMPLETED);
        assertThat(capabilityB.partitionCount(partition)).isZero();
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
    }

    @Test
    void nullSourceBootstrapMoveConvergesWithoutData() {
        int partition = partitionOf("boot-seed");
        placeOwner(partition, NODE_A);

        boolean ok = engine().migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, null, NODE_B)));

        assertThat(ok).isTrue();
        assertThat(store.listAll().get(0).state()).isEqualTo(MigrationTaskState.COMPLETED);
        assertThat(store.listAll().get(0).itemsDone()).isZero();
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
    }

    @Test
    void executorLocalPathMigratesOwnDataToRemoteTarget() {
        int partition = partitionOf("exec-seed");
        List<Map.Entry<String, String>> before = fill(capabilityC, partition, 15, "ek");
        placeOwner(partition, NODE_C);

        boolean ok = engine().migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_C, NODE_B)));

        assertThat(ok).isTrue();
        // source==self 走本地直连导出；目标 B 远端推送导入；源 C 清理
        assertThat(capabilityB.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(before);
        assertThat(capabilityC.partitionCount(partition)).isZero();
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_B);
    }

    @Test
    void remoteTargetPathMigratesDataIntoExecutorLocally() {
        int partition = partitionOf("into-exec-seed");
        List<Map.Entry<String, String>> before = fill(capabilityA, partition, 12, "ik");
        placeOwner(partition, NODE_A);

        boolean ok = engine().migrateAndWait("mock",
                List.of(new PartitionMove("mock", partition, NODE_A, NODE_C)));

        assertThat(ok).isTrue();
        // target==self 走本地直连导入
        assertThat(capabilityC.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(before);
        assertThat(capabilityA.partitionCount(partition)).isZero();
        assertThat(placement.primaries("mock")).containsEntry(partition, NODE_C);
    }
}
