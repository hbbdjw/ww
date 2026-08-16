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
    private final DesiredStateWriter desiredStateWriter;
    private final GrpcNodeCommandChannel grpcCommandChannel;
    private final MigrationEngine migrationEngine;
    private final ReconcileLoop reconcileLoop;

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
                               DesiredStateWriter desiredStateWriter,
                               GrpcNodeCommandChannel grpcCommandChannel,
                               MigrationEngine migrationEngine,
                               ReconcileLoop reconcileLoop) {
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
        this.desiredStateWriter = desiredStateWriter;
        this.grpcCommandChannel = grpcCommandChannel;
        this.migrationEngine = migrationEngine;
        this.reconcileLoop = reconcileLoop;
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

        // (3) membership 先于 host.start：其 start() 内部 addService，
        //     而 NodeRuntime.start()（第 4 步）会启动 host，二者顺序不可交换。
        membershipService.start();

        // (4) 注册 JOINING → host 绑定端口（支持随机端口）→ ACTIVE → 心跳线程
        try {
            nodeRuntime.start();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("node runtime failed to start: " + self, e);
        }

        // (5) 注册能力模块 + 可选自动写期望态（此时本节点已 ACTIVE，调度器可拉起）
        capabilityManager.register(mockCapability);
        if (props.capabilities().mockDesired()) {
            desiredStateWriter.setDesired(self, CapabilityType.MOCK, CapabilityState.ACTIVE);
        }
        capabilityManager.register(cacheCapability);
        if (props.capabilities().cacheDesired()) {
            desiredStateWriter.setDesired(self, CapabilityType.CACHE, CapabilityState.ACTIVE);
        }
        capabilityManager.register(storageCapability);
        if (props.capabilities().storageDesired()) {
            desiredStateWriter.setDesired(self, CapabilityType.STORAGE, CapabilityState.ACTIVE);
        }

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

        reconcileLoop.stop();
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

    /** 启动自检：store 模式日志；jdbc 模式 PG SELECT 1 可达性检查 + 5 个 Schema bootstrap。 */
    private void bootstrapStore() {
        if ("memory".equals(props.store())) {
            LOG.info("store=memory: in-memory repositories, schema bootstrap and PG self-check skipped");
            return;
        }
        DataSource ds = dataSource.getObject();
        pgSelfCheck(ds);
        ClusterSchema.bootstrap(ds);
        CapabilitySchema.bootstrap(ds);
        PlacementSchema.bootstrap(ds);
        SchedulerSchema.bootstrap(ds);
        MigrationSchema.bootstrap(ds);
        LogSchema.bootstrap(ds);
        LOG.info("DDL bootstrap completed (cluster/capability/placement/scheduler/migration/log)");
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
