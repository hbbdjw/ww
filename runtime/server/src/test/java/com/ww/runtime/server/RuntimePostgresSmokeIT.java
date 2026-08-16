package com.ww.runtime.server;

import com.ww.runtime.capability.CapabilitySchema;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.capability.JdbcCapabilityStateStore;
import com.ww.runtime.cluster.ClusterSchema;
import com.ww.runtime.cluster.JdbcNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.migration.MigrationSchema;
import com.ww.runtime.placement.JdbcPlacementStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementNode;
import com.ww.runtime.placement.PlacementSchema;
import com.ww.runtime.scheduler.JdbcLeaseStore;
import com.ww.runtime.scheduler.SchedulerSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testcontainers PostgreSQL 冒烟集成测试（failsafe：integration-test/verify 阶段执行）。
 *
 * <p>跨 5 个模块的 Jdbc*Store 在<b>真实 PostgreSQL</b> 上的最小闭环验证：
 * Schema 建表幂等、节点注册状态机、租约互斥、放置表原子替换与 cutover CAS、
 * 能力期望/实际态往返。语义细节（并发竞争、乐观锁重试等）仍由各模块单测保证。</p>
 *
 * <p>{@code disabledWithoutDocker = true}：本机无 Docker（或 DOCKER_HOST 不可达）时
 * 整类 SKIPPED 而非失败（Task 11 约定：Docker 不可用不视为失败）。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class RuntimePostgresSmokeIT {

    @Container
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("rt")
                    .withUsername("rt")
                    .withPassword("rt");

    private static DataSource dataSource;

    @BeforeAll
    static void bootstrapSchemasAndDataSource() {
        dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        // 全部 5 个 Schema 幂等建表（后续各用例依赖表已存在）
        ClusterSchema.bootstrap(dataSource);
        CapabilitySchema.bootstrap(dataSource);
        PlacementSchema.bootstrap(dataSource);
        SchedulerSchema.bootstrap(dataSource);
        MigrationSchema.bootstrap(dataSource);
    }

    /** ① 5 个 Schema.bootstrap（重复执行验证幂等）后 6 张表全部存在于 public schema。 */
    @Test
    void schemasBootstrapCreateAllSixTables() throws SQLException {
        // 二次 bootstrap：CREATE TABLE IF NOT EXISTS 幂等
        ClusterSchema.bootstrap(dataSource);
        CapabilitySchema.bootstrap(dataSource);
        PlacementSchema.bootstrap(dataSource);
        SchedulerSchema.bootstrap(dataSource);
        MigrationSchema.bootstrap(dataSource);

        Set<String> tables = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        assertThat(tables).contains(
                "rt_node",
                "rt_cluster_config",
                "rt_capability_state",
                "rt_partition_placement",
                "rt_scheduler_lease",
                "rt_migration_task");
    }

    /** ② JdbcNodeRegistry：register（JOINING/epoch=0）→ markState ACTIVE → get 断言。 */
    @Test
    void nodeRegistryRegisterMarkStateActiveAndGet() {
        JdbcNodeRegistry registry = new JdbcNodeRegistry(dataSource);
        NodeId id = NodeId.of("smoke-node-1");
        registry.register(NodeInfo.of(id, "localhost", 9101, NodeState.JOINING, 0L, 1.0,
                ResourceSnapshot.of(1.5, 100L, 50L, 1_000L, 500L, 1L)));

        registry.markState(id, NodeState.ACTIVE);

        Optional<NodeInfo> loaded = registry.get(id);
        assertThat(loaded).isPresent();
        assertThat(loaded.get().id()).isEqualTo(id);
        assertThat(loaded.get().state()).isEqualTo(NodeState.ACTIVE);
        assertThat(loaded.get().stateEpoch()).isEqualTo(1L); // register 置 0，markState 乐观锁 +1
        assertThat(loaded.get().host()).isEqualTo("localhost");
        assertThat(loaded.get().grpcPort()).isEqualTo(9101);
        assertThat(loaded.get().weight()).isEqualTo(1.0);
    }

    /** ③ JdbcLeaseStore：owner-a 持有期内 owner-b 竞争失败（恰一胜），owner-a 续约成功。 */
    @Test
    void leaseTwoOwnersCompeteExactlyOneWinsAndHolderRenews() {
        JdbcLeaseStore storeA = new JdbcLeaseStore(dataSource);
        JdbcLeaseStore storeB = new JdbcLeaseStore(dataSource);

        assertThat(storeA.acquireOrRenew("owner-a", 60_000L)).isTrue();
        assertThat(storeB.acquireOrRenew("owner-b", 60_000L))
                .as("owner-a 租约有效期内 owner-b 抢占必须失败（恰好一个持有者）")
                .isFalse();
        assertThat(storeA.acquireOrRenew("owner-a", 60_000L))
                .as("持有者续约必须成功")
                .isTrue();
        assertThat(storeA.currentOwner()).contains("owner-a");
        assertThat(storeB.currentOwner()).contains("owner-a");
    }

    /** ④ computePrimaries(2 节点) → replaceAll → primaries 一致 → cutover 单分区 epoch+1。 */
    @Test
    void placementComputeReplaceAllAndCutoverBumpsEpoch() {
        NodeId n1 = NodeId.of("smoke-node-1");
        NodeId n2 = NodeId.of("smoke-node-2");
        PlacementEngine engine = new PlacementEngine();
        Map<Integer, NodeId> computed = engine.computePrimaries(
                List.of(new PlacementNode(n1, 1.0), new PlacementNode(n2, 1.0)), 64);

        assertThat(computed).hasSize(64);
        assertThat(new HashSet<>(computed.values()))
                .as("主副本 owner 必须落在候选两节点内")
                .isSubsetOf(n1, n2);

        JdbcPlacementStore store = new JdbcPlacementStore(dataSource);
        List<Placement> placements = new ArrayList<>(computed.size());
        computed.forEach((partition, node) ->
                placements.add(new Placement(partition, node, PlacementRole.PRIMARY, 0L)));
        store.replaceAll("MOCK", placements);

        assertThat(store.primaries("MOCK"))
                .as("replaceAll 后读回必须与全量计算结果一致")
                .isEqualTo(computed);

        int partition = 0;
        NodeId currentOwner = computed.get(partition);
        NodeId newOwner = currentOwner.equals(n1) ? n2 : n1;
        assertThat(store.cutover("MOCK", partition, newOwner, 0L))
                .as("expectedEpoch=0 的 CAS 切换必须成功")
                .isTrue();
        Optional<Placement> after = store.get("MOCK", partition);
        assertThat(after).isPresent();
        assertThat(after.get().node()).isEqualTo(newOwner);
        assertThat(after.get().epoch()).isEqualTo(1L); // cutover 成功后 epoch+1
        assertThat(store.cutover("MOCK", partition, currentOwner, 0L))
                .as("旧 epoch 的重复 CAS 必须失败")
                .isFalse();
    }

    /** ⑤ JdbcCapabilityStateStore：setDesired/setActual 写入后 get 往返一致。 */
    @Test
    void capabilityStateStoreDesiredActualRoundTrip() {
        JdbcCapabilityStateStore store = new JdbcCapabilityStateStore(dataSource);
        NodeId node = NodeId.of("smoke-node-1");

        store.setDesired(node, CapabilityType.MOCK, CapabilityState.ACTIVE);
        store.setActual(node, CapabilityType.MOCK, CapabilityState.ACTIVE);

        Optional<CapabilityRecord> record = store.get(node, CapabilityType.MOCK);
        assertThat(record).isPresent();
        assertThat(record.get().node()).isEqualTo(node);
        assertThat(record.get().type()).isEqualTo(CapabilityType.MOCK);
        assertThat(record.get().desired()).isEqualTo(CapabilityState.ACTIVE);
        assertThat(record.get().actual()).isEqualTo(CapabilityState.ACTIVE);

        store.setDesired(node, CapabilityType.MOCK, CapabilityState.DISABLED);
        assertThat(store.get(node, CapabilityType.MOCK))
                .hasValueSatisfying(r -> assertThat(r.desired()).isEqualTo(CapabilityState.DISABLED));
    }
}
