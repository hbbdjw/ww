package com.ww.runtime.server.config;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.CapabilitySchema;
import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.cluster.ClusterSchema;
import com.ww.runtime.cluster.NodeRuntime;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.storage.StorageCapability;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.membership.MembershipService;
import com.ww.runtime.migration.MigrationEngine;
import com.ww.runtime.migration.MigrationSchema;
import com.ww.runtime.migration.MigrationServer;
import com.ww.runtime.placement.PlacementSchema;
import com.ww.runtime.scheduler.DesiredStateWriter;
import com.ww.runtime.scheduler.GrpcNodeCommandChannel;
import com.ww.runtime.scheduler.ReconcileLoop;
import com.ww.runtime.scheduler.SchedulerSchema;
import com.ww.runtime.server.RuntimeProperties;
import com.ww.runtime.server.grpc.ClusterControlServer;
import com.ww.runtime.server.grpc.HealthServer;
import com.ww.runtime.server.log.LogSchema;
import com.ww.runtime.server.log.LogStreamGrpcServer;
import com.ww.runtime.server.security.RbacSchema;
import com.ww.runtime.server.security.RbacSeeder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * 运行时启动/停机编排（SmartLifecycle，phase = MAX_VALUE：Web 服务器就绪后最后启动、
 * 停机时最先执行）。启动序列中任一步失败即抛异常终止容器启动（Fail-Fast）。
 *
 * <h2>启动序列</h2>
 * <ol>
 *   <li>自检：store 模式；jdbc 模式下 PG 可达（SELECT 1）+ 5 个 Schema bootstrap
 *       （cluster/capability/placement/scheduler/migration，全部 CREATE TABLE IF NOT EXISTS 幂等）；</li>
 *   <li>gRPC 服务注册（<b>必须在 host.start 之前</b>）：MigrationServer /
 *       ClusterControlServer / HealthServer 经 host.addService 挂载；
 *       MembershipService 除外——其 start() 内部才 addService（见第 3 步）；</li>
 *   <li>membershipService.start()——其内部把自身 addService 到 host 并启动心跳线程。
 *       <b>顺序约束（读源码确认）</b>：GrpcServerHost 一旦 start 便拒绝 addService，而
 *       NodeRuntime.start() 内部会执行 host.start(port)，因此 membership 的 start 必须
 *       提前到 nodeRuntime.start() 之前执行；</li>
 *   <li>nodeRuntime.start()：注册 JOINING → host.start（配置端口或随机）→ ACTIVE → 心跳线程；</li>
 *   <li>capabilityManager.register(mockCapability)（MigrationEngine 数据面依赖它）；
 *       若 runtime.capabilities.mock-desired=true 则经 DesiredStateWriter 把本节点
 *       MOCK 期望态写为 ACTIVE；</li>
 *   <li>reconcileLoop.runOnce()（确定性首查：抢 leader 租约 + 首轮收敛，先于守护线程
 *       无并发）→ reconcileLoop.start()（守护调谐循环）；</li>
 *   <li>migrationEngine.resumeAll()：断点续跑上次未完成的迁移任务。</li>
 * </ol>
 *
 * <h2>停机序列（倒序，全部幂等）</h2>
 * {@code reconcileLoop.stop → membership.stop → grpcCommandChannel.close →
 * nodeRuntime.shutdown（LEAVING → 停心跳 → host.stop → tombstone DEAD）}。
 */
public final class RuntimeBootstrapper implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeBootstrapper.class);

    /**
     * 启动收敛等待参数：首次调谐（runOnce）前，等待 membership 心跳发现对等节点，
     * 避免基于不完整的成员视图将尚未启动的节点误判为 DEAD 而触发大规模治愈迁移。
     * - 最大等待 3s（约 6 个心跳周期，500ms/次），超时后继续启动不阻塞
     * - 视图稳定判定：连续 2 次采样（间隔 500ms）的 ACTIVE 节点数不变即认为稳定
     */
    private static final long STARTUP_CONVERGE_MAX_WAIT_MS = 3000;
    private static final long STARTUP_CONVERGE_POLL_MS = 500;
    private static final int STARTUP_CONVERGE_STABLE_POLLS = 2;

    private final ObjectProvider<DataSource> dataSource;
    private final RuntimeProperties props;
    private final NodeId self;
    private final GrpcServerHost grpcHost;
    private final MembershipService membershipService;
    private final MigrationServer migrationServer;
    private final ClusterControlServer clusterControlServer;
    private final HealthServer healthServer;
    private final LogStreamGrpcServer logStreamGrpcServer;
    private final NodeRuntime nodeRuntime;
    private final CapabilityManager capabilityManager;
    private final CapabilityStateStore capabilityStateStore;
    private final MockCapability mockCapability;
    private final CacheCapability cacheCapability;
    private final StorageCapability storageCapability;
    private final com.ww.runtime.data.cache.CacheGrpcServer cacheGrpcServer;
    private final com.ww.runtime.data.cache.CacheGrpcClient cacheGrpcClient;
    private final com.ww.runtime.data.storage.StorageGrpcServer storageGrpcServer;
    private final com.ww.runtime.data.storage.StorageReplicator storageReplicator;
    private final com.ww.runtime.data.storage.StorageGrpcClient storageGrpcClient;
    private final DesiredStateWriter desiredStateWriter;
    private final GrpcNodeCommandChannel grpcCommandChannel;
    private final MigrationEngine migrationEngine;
    private final ReconcileLoop reconcileLoop;
    private final com.ww.runtime.server.cache.PartitionSweeper partitionSweeper;
    private final com.ww.runtime.migration.RoutingPartitionAdapter routingPartitionAdapter;
    private final RbacSeeder rbacSeeder;
    private final com.ww.runtime.server.lb.LbRefresher lbRefresher;

    private volatile boolean running;

    public RuntimeBootstrapper(ObjectProvider<DataSource> dataSource,
                               RuntimeProperties props,
                               NodeId self,
                               GrpcServerHost grpcHost,
                               MembershipService membershipService,
                               MigrationServer migrationServer,
                               ClusterControlServer clusterControlServer,
                               HealthServer healthServer,
                               LogStreamGrpcServer logStreamGrpcServer,
                               NodeRuntime nodeRuntime,
                               CapabilityManager capabilityManager,
                               CapabilityStateStore capabilityStateStore,
                               MockCapability mockCapability,
                               CacheCapability cacheCapability,
                               StorageCapability storageCapability,
                               com.ww.runtime.data.cache.CacheGrpcServer cacheGrpcServer,
                               com.ww.runtime.data.cache.CacheGrpcClient cacheGrpcClient,
                               com.ww.runtime.data.storage.StorageGrpcServer storageGrpcServer,
                               com.ww.runtime.data.storage.StorageReplicator storageReplicator,
                               com.ww.runtime.data.storage.StorageGrpcClient storageGrpcClient,
                               DesiredStateWriter desiredStateWriter,
                               GrpcNodeCommandChannel grpcCommandChannel,
                               MigrationEngine migrationEngine,
                               ReconcileLoop reconcileLoop,
                               com.ww.runtime.server.cache.PartitionSweeper partitionSweeper,
                               com.ww.runtime.migration.RoutingPartitionAdapter routingPartitionAdapter,
                               RbacSeeder rbacSeeder,
                               com.ww.runtime.server.lb.LbRefresher lbRefresher) {
        this.dataSource = dataSource;
        this.props = props;
        this.self = self;
        this.grpcHost = grpcHost;
        this.membershipService = membershipService;
        this.migrationServer = migrationServer;
        this.clusterControlServer = clusterControlServer;
        this.healthServer = healthServer;
        this.logStreamGrpcServer = logStreamGrpcServer;
        this.nodeRuntime = nodeRuntime;
        this.capabilityManager = capabilityManager;
        this.capabilityStateStore = capabilityStateStore;
        this.mockCapability = mockCapability;
        this.cacheCapability = cacheCapability;
        this.storageCapability = storageCapability;
        this.cacheGrpcServer = cacheGrpcServer;
        this.cacheGrpcClient = cacheGrpcClient;
        this.storageGrpcServer = storageGrpcServer;
        this.storageReplicator = storageReplicator;
        this.storageGrpcClient = storageGrpcClient;
        this.desiredStateWriter = desiredStateWriter;
        this.grpcCommandChannel = grpcCommandChannel;
        this.migrationEngine = migrationEngine;
        this.reconcileLoop = reconcileLoop;
        this.partitionSweeper = partitionSweeper;
        this.routingPartitionAdapter = routingPartitionAdapter;
        this.rbacSeeder = rbacSeeder;
        this.lbRefresher = lbRefresher;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        LOG.info("runtime-server bootstrapping: nodeId={}, store={}", self, props.store());

        // (1) 自检 + DDL bootstrap（jdbc 模式；异常快速失败）
        bootstrapStore();

        // (2) gRPC 服务注册：host.start 之前（GrpcServerHost 启动后拒绝 addService）。
        //     MembershipService 不在此列——其 start() 内部自行 addService（见第 3 步）。
        grpcHost.addService(migrationServer);
        grpcHost.addService(clusterControlServer);
        grpcHost.addService(healthServer);
        grpcHost.addService(logStreamGrpcServer);
        grpcHost.addService(storageGrpcServer);
        grpcHost.addService(cacheGrpcServer);

        // (3) membership 先于 host.start：其 start() 内部 addService，
        //     而 NodeRuntime.start()（第 4 步）会启动 host，二者顺序不可交换。
        membershipService.start();

        // (4) 注册 JOINING → host 绑定端口（支持随机端口）→ ACTIVE → 心跳线程
        try {
            nodeRuntime.start();
        } catch (java.io.IOException | RuntimeException e) {
            // 深审6 失败路径清理：NodeRuntime.start 内部已 tombstone 自身注册行
            // 并回收宿主；membership 心跳已先启动，一并停掉避免向已死宿主继续发包
            try {
                membershipService.stop();
            } catch (RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw new IllegalStateException("node runtime failed to start: " + self, e);
        }

        // (4.5) LB 配置/节点视图刷新守护：注册表已含本节点 ACTIVE 行，立即加载配置并周期轮询。
        //       默认 enabled=false，启动本身不改变任何数据面行为。
        lbRefresher.start();

        // (5) 注册能力模块 + 初始期望态（仅首启写 env 值——已有记录=管理面已显式设置，
        //     env 不覆盖，否则「UI 禁用节点 → 重启被 env 复活」破坏期望态持久化语义）。
        //     注意：setInitialDesired 必须先于 register（register 会为无记录能力创建
        //     DISABLED 记录，先行则首启判断永远失效）。
        if (props.capabilities().mockDesired()) {
            desiredStateWriter.setInitialDesired(self, CapabilityType.MOCK, CapabilityState.ACTIVE);
        }
        capabilityManager.register(mockCapability);
        if (props.capabilities().cacheDesired()) {
            desiredStateWriter.setInitialDesired(self, CapabilityType.CACHE, CapabilityState.ACTIVE);
        }
        capabilityManager.register(cacheCapability);
        if (props.capabilities().storageDesired()) {
            desiredStateWriter.setInitialDesired(self, CapabilityType.STORAGE, CapabilityState.ACTIVE);
        }
        capabilityManager.register(storageCapability);

        // (5.2) 副本复制器：主写/主删成功钩子 + 后台补偿（backlog 重放/周期对账）
        storageCapability.setWriteHook(storageReplicator);
        storageReplicator.start();
        // P1-3：freeze(true) 前先排空复制 backlog（在途 + 退避条目），
        // 确保随后的副本提升/缩容排空不会提升一个落后于主节点的副本。
        // 3s < gRPC 命令 deadline(5s)：超时未排空也不阻断冻结（安全方向），下轮续跑。
        capabilityManager.setFreezeHook(CapabilityType.STORAGE,
                () -> storageReplicator.awaitDrained(3_000L));

        // (5.5) 本地能力自收敛：desired=ACTIVE 而 actual=DISABLED 的能力同步拉起（convergeStart 幂等）。
        // 否则非 leader 节点在 (7) resumeAll 跑遗留迁移任务时会先于 leader 的 start 命令
        // 执行该能力数据面，连环 "capability not started" 失败刷屏。
        for (CapabilityType type : capabilityManager.modules().keySet()) {
            capabilityStateStore.get(self, type).ifPresent(record -> {
                if (record.desired() == CapabilityState.ACTIVE && record.actual() == CapabilityState.DISABLED) {
                    try {
                        capabilityManager.convergeStart(type);
                        LOG.info("local capability {} auto-started on bootstrap (desired=ACTIVE)", type);
                    } catch (Exception e) {
                        LOG.warn("local capability {} auto-start failed; scheduler will retry: {}", type, e.getMessage());
                    }
                }
            });
        }

        // (5.8) 启动收敛等待：给心跳协议留出发现对等节点的时间窗口，
        //       避免首个 leader 基于不完整视图触发不必要的治愈迁移风暴
        awaitMembershipConvergence();

        // (6) 调度调谐：先手动 runOnce 完成确定性首查（抢租约 + 首轮收敛），再启动守护循环
        boolean leader = reconcileLoop.runOnce();
        reconcileLoop.start();

        // (6.5) 启动清扫：放置表已引导、在途任务可见——裁剪 AOF 回放出的旧归属数据
        //      （含 clearPartition 失败的兜底）。FAILED 任务分区保留（resumeAll 断点续跑依赖）。
        partitionSweeper.sweepAll();

        // (7) 断点续跑未完成迁移任务
        migrationEngine.resumeAll();

        running = true;
        LOG.info("runtime-server started: nodeId={}, grpcPort={}, leader={} (first probe)", self,
                nodeRuntime.grpcPort(), leader);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("runtime-server stopping: nodeId={}", self);

        lbRefresher.stop(); // 最先停：停机过程中不再分发到正在退出的节点
        reconcileLoop.stop();
        storageReplicator.stop();
        storageGrpcServer.close();
        storageGrpcClient.close();
        cacheGrpcServer.close();
        cacheGrpcClient.close();
        routingPartitionAdapter.close();
        membershipService.stop();
        grpcCommandChannel.close();
        nodeRuntime.shutdown(); // LEAVING → 停心跳 → host.stop → tombstone DEAD

        LOG.info("runtime-server stopped: nodeId={}", self);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** 最后启动、最先停机（依赖 Web 容器与 DataSource 自动装配已就绪）。 */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /** 启动自检：store 模式日志；jdbc 模式 PG SELECT 1 可达性检查 + Schema bootstrap + RBAC 播种。 */
    private void bootstrapStore() {
        if ("memory".equals(props.store())) {
            LOG.info("store=memory: in-memory repositories, schema bootstrap and PG self-check skipped");
            rbacSeeder.seedIfEmpty();
            rbacSeeder.ensureLatestMenus();
            return;
        }
        DataSource ds = dataSource.getObject();
        pgSelfCheck(ds);
        ClusterSchema.bootstrap(ds);
        CapabilitySchema.bootstrap(ds);
        PlacementSchema.bootstrap(ds);
        com.ww.runtime.placement.DegradedPartitionSchema.bootstrap(ds); // P0-2 隔离注册表
        SchedulerSchema.bootstrap(ds);
        MigrationSchema.bootstrap(ds);
        LogSchema.bootstrap(ds);
        RbacSchema.bootstrap(ds);
        com.ww.runtime.lb.LbSchema.bootstrap(ds);
        LOG.info("DDL bootstrap completed (cluster/capability/placement/degraded/scheduler/migration/log/rbac/lb)");
        // 跨节点互斥播种：多节点同时首启时避免并发插入 admin/超管角色撞唯一约束导致启动失败；
        // 增量菜单补齐同处互斥区（sys_menu.path 无唯一约束，防止多节点并发插入重复菜单）
        RbacSchema.seedExclusively(ds, () -> {
            rbacSeeder.seedIfEmpty();
            rbacSeeder.ensureLatestMenus();
        });
    }

    private void pgSelfCheck(DataSource ds) {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (!rs.next()) {
                throw new IllegalStateException("PostgreSQL self-check failed: SELECT 1 returned no rows");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("PostgreSQL self-check failed (SELECT 1): " + e.getMessage(), e);
        }
        LOG.info("PostgreSQL reachable (SELECT 1 ok)");
    }

    /**
     * 启动收敛等待：轮询 membership 视图，等待 ACTIVE 节点数连续稳定若干次或达到最大等待时间。
     * 不等价于"等所有节点到齐"——部分节点确实宕机时不会永久阻塞，超时时限到即继续。
     * 目的仅为给同批次启动的对等节点留出心跳发现窗口，避免误判为 DEAD 触发治愈迁移风暴。
     */
    private void awaitMembershipConvergence() {
        long deadline = System.currentTimeMillis() + STARTUP_CONVERGE_MAX_WAIT_MS;
        int stableCount = 0;
        int lastActiveCount = -1;
        int finalActiveCount = 1; // at least self is active

        while (System.currentTimeMillis() < deadline) {
            Map<NodeId, NodeInfo> view = membershipService.view();
            long activeCount = view.values().stream()
                    .filter(n -> n.state() == NodeState.ACTIVE)
                    .count();
            finalActiveCount = (int) activeCount;

            if (activeCount == lastActiveCount) {
                stableCount++;
                if (stableCount >= STARTUP_CONVERGE_STABLE_POLLS) {
                    LOG.info("membership converged: {} active nodes after {}ms",
                            activeCount, STARTUP_CONVERGE_MAX_WAIT_MS - (deadline - System.currentTimeMillis()));
                    return;
                }
            } else {
                stableCount = 0;
                lastActiveCount = (int) activeCount;
            }

            try {
                Thread.sleep(STARTUP_CONVERGE_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("membership convergence wait interrupted; proceeding with {} active nodes", activeCount);
                return;
            }
        }

        LOG.info("membership convergence wait timed out after {}ms; proceeding with {} active nodes",
                STARTUP_CONVERGE_MAX_WAIT_MS, finalActiveCount);
    }
}
