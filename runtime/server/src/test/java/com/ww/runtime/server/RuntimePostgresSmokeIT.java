package com.ww.runtime.server;

import com.ww.runtime.capability.CapabilitySchema;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.capability.JdbcCapabilityStateStore;
import com.ww.runtime.cluster.ClusterSchema;
import com.ww.runtime.cluster.JdbcNodeRegistry;
import com.ww.runtime.lb.JdbcLbConfigStore;
import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbConfigConflictException;
import com.ww.runtime.lb.LbSchema;
import com.ww.runtime.lb.LbStrategy;
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
import com.ww.runtime.server.security.JdbcRbacStore;
import com.ww.runtime.server.security.RbacSchema;
import com.ww.runtime.server.security.RbacStore;
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
        RbacSchema.bootstrap(dataSource);
        LbSchema.bootstrap(dataSource);
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
        RbacSchema.bootstrap(dataSource);
        LbSchema.bootstrap(dataSource);

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
                "rt_placement_revision",
                "rt_scheduler_lease",
                "rt_migration_task",
                "sys_dept",
                "sys_user",
                "sys_role",
                "sys_menu",
                "sys_user_role",
                "sys_role_menu",
                "rt_lb_config");

        // rt_node 幂等加列 http_port 必须存在（旧库 ALTER 升级路径）
        Set<String> nodeColumns = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name FROM information_schema.columns "
                             + "WHERE table_schema='public' AND table_name='rt_node'");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                nodeColumns.add(rs.getString(1));
            }
        }
        assertThat(nodeColumns).contains("http_port");

        Set<String> placementColumns = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name FROM information_schema.columns "
                             + "WHERE table_schema='public' AND table_name='rt_partition_placement'");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                placementColumns.add(rs.getString(1));
            }
        }
        assertThat(placementColumns).contains("lease_until_ms", "valid_after_ms");
    }

    /** ⑥ JdbcRbacStore：部门/角色/菜单/用户 + 关系绑定往返，loadPermissions 聚合权限标识。 */
    @Test
    void rbacStoreRelationRoundTripAndPermissions() {
        JdbcRbacStore rbac = new JdbcRbacStore(dataSource);
        long now = System.currentTimeMillis();

        long deptId = rbac.createDept(new RbacStore.Dept(0, 0L, "冒烟公司", 0,
                null, null, 1, now, now));
        long roleId = rbac.createRole(new RbacStore.Role(0, "管理员", "rbac_admin",
                0, 1, 1, null, now, now));
        long menuId = rbac.createMenu(new RbacStore.Menu(0, 0L, "用户管理", "C",
                null, "system:user:list", null, 1, 1, 1));
        long buttonId = rbac.createMenu(new RbacStore.Menu(0, 0L, "新增", "F",
                null, "system:user:add", null, 2, 1, 1));

        String encoded = new org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder()
                .encode("pass123");
        long userId = rbac.createUser(new RbacStore.UserAccount(0, deptId, "smoke_user",
                "冒烟用户", encoded, null, null, 0, 1, null, null, now, now));

        rbac.bindRoleMenus(roleId, List.of(menuId, buttonId));
        rbac.bindUserRoles(userId, List.of(roleId));

        assertThat(rbac.findUserByUsername("smoke_user")).isPresent();
        assertThat(rbac.findRoleIdsByUser(userId)).containsExactly(roleId);
        assertThat(rbac.findMenuIdsByRole(roleId)).containsExactlyInAnyOrder(menuId, buttonId);
        assertThat(rbac.findUserIdsByRole(roleId)).containsExactly(userId);

        RbacStore.UserPermissions permissions = rbac.loadPermissions(userId);
        assertThat(permissions.roleKeys()).containsExactly("rbac_admin");
        assertThat(permissions.perms()).containsExactlyInAnyOrder("system:user:list", "system:user:add");
    }

    /** ② JdbcNodeRegistry：register（JOINING/epoch=0）→ markState ACTIVE → get 断言；httpPort 往返。 */
    @Test
    void nodeRegistryRegisterMarkStateActiveAndGet() {
        JdbcNodeRegistry registry = new JdbcNodeRegistry(dataSource);
        NodeId id = NodeId.of("smoke-node-1");
        registry.register(NodeInfo.of(id, "localhost", 9101, 8091, NodeState.JOINING, 0L, 1.0,
                ResourceSnapshot.of(1.5, 100L, 50L, 1_000L, 500L, 1L)));

        registry.markState(id, NodeState.ACTIVE);

        Optional<NodeInfo> loaded = registry.get(id);
        assertThat(loaded).isPresent();
        assertThat(loaded.get().id()).isEqualTo(id);
        assertThat(loaded.get().state()).isEqualTo(NodeState.ACTIVE);
        assertThat(loaded.get().stateEpoch()).isEqualTo(1L); // register 置 0，markState 乐观锁 +1
        assertThat(loaded.get().host()).isEqualTo("localhost");
        assertThat(loaded.get().grpcPort()).isEqualTo(9101);
        assertThat(loaded.get().httpPort()).isEqualTo(8091); // LB 反代寻址端口随注册往返
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

    /** ④b 放置修订号 + 租约：续约 SQL 生效，切主后旧属主续约失败且新属主带 validAfter 屏障。 */
    @Test
    void placementRevisionLeaseRenewalAndCutoverBarrier() {
        JdbcPlacementStore store = new JdbcPlacementStore(dataSource);
        NodeId n1 = NodeId.of("lease-node-1");
        NodeId n2 = NodeId.of("lease-node-2");
        store.replaceAll("LEASE", List.of(new Placement(0, n1, PlacementRole.PRIMARY, 0L)));
        long replaceRevision = store.revision("LEASE");
        assertThat(replaceRevision).isGreaterThan(0L);

        assertThat(store.renewLeases("LEASE", List.of(0), n1, 0L, 5_000L))
                .containsKey(0);

        assertThat(store.cutover("LEASE", 0, n2, 0L)).isTrue();
        assertThat(store.revision("LEASE")).isGreaterThan(replaceRevision);
        assertThat(store.get("LEASE", 0)).hasValueSatisfying(placement -> {
            assertThat(placement.node()).isEqualTo(n2);
            assertThat(placement.epoch()).isEqualTo(1L);
            assertThat(placement.validAfterMs())
                    .as("旧属主存在活动租约切主时必须设置新属主等待屏障")
                    .isGreaterThan(0L);
        });
        assertThat(store.renewLeases("LEASE", List.of(0), n1, 0L, 5_000L))
                .as("切主后旧属主续约必须失败")
                .isEmpty();
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

    /** ⑦ JdbcLbConfigStore：缺行播种默认配置 → CAS 更新成功且 version+1 → 旧版本冲突 409。 */
    @Test
    void lbConfigStoreSeedUpdateAndCasConflict() {
        JdbcLbConfigStore store = new JdbcLbConfigStore(dataSource);

        LbConfig seeded = store.get();
        assertThat(seeded.enabled()).isFalse();
        assertThat(seeded.pathPrefixes()).containsExactly("/cache/**", "/storage/**");
        long seedVersion = seeded.version();

        LbConfig enabled = new LbConfig(true, "smoke-node-1", LbStrategy.WEIGHTED, false,
                seeded.pathPrefixes(), java.util.Map.of("smoke-node-2", 3.0),
                seeded.configRefreshMs(), seeded.nodeViewRefreshMs(),
                seeded.connectTimeoutMs(), seeded.requestTimeoutMs(), seedVersion);
        LbConfig updated = store.update(enabled, seedVersion);
        assertThat(updated.version()).isEqualTo(seedVersion + 1);
        assertThat(updated.enabled()).isTrue();
        assertThat(updated.strategy()).isEqualTo(LbStrategy.WEIGHTED);
        assertThat(updated.selfServe()).isFalse();
        assertThat(updated.weightOverrides()).containsEntry("smoke-node-2", 3.0);

        // 另一实例读到新版本
        assertThat(new JdbcLbConfigStore(dataSource).get().version()).isEqualTo(seedVersion + 1);

        // 用过期版本更新必须冲突
        org.junit.jupiter.api.Assertions.assertThrows(LbConfigConflictException.class,
                () -> store.update(enabled, seedVersion));
    }
}
