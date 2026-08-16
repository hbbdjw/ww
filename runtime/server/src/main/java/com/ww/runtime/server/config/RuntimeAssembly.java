package com.ww.runtime.server.config;

import java.io.File;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.capability.JdbcCapabilityStateStore;
import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.cluster.JdbcNodeRegistry;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.cluster.NodeRuntime;
import com.ww.runtime.cluster.OsResourceMonitor;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.data.CompositePartitionAdapter;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CachePartitionAdapter;
import com.ww.runtime.data.storage.StorageCapability;
import com.ww.runtime.data.storage.StoragePartitionAdapter;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.membership.MembershipService;
import com.ww.runtime.membership.PeerDirectory;
import com.ww.runtime.membership.StateSink;
import com.ww.runtime.migration.InMemoryMigrationTaskStore;
import com.ww.runtime.migration.JdbcMigrationTaskStore;
import com.ww.runtime.migration.MigrationEngine;
import com.ww.runtime.migration.MigrationServer;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.migration.MockPartitionAdapter;
import com.ww.runtime.migration.PartitionDataAdapter;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.JdbcPlacementStore;
import com.ww.runtime.placement.Partitioner;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner;
import com.ww.runtime.scheduler.DesiredStateWriter;
import com.ww.runtime.scheduler.GrpcNodeCommandChannel;
import com.ww.runtime.scheduler.InMemoryLeaseStore;
import com.ww.runtime.scheduler.JdbcLeaseStore;
import com.ww.runtime.scheduler.LeaseStore;
import com.ww.runtime.scheduler.LocalNodeCommandChannel;
import com.ww.runtime.scheduler.ReconcileLoop;
import com.ww.runtime.server.RuntimeProperties;
import com.ww.runtime.server.grpc.ClusterControlServer;
import com.ww.runtime.server.grpc.HealthServer;
import com.ww.runtime.server.migration.RuntimeMigrationCoordinator;
import com.ww.runtime.server.placement.PlacementViewAdapter;

/**
 * 运行时 Bean 装配（唯一的宿主 @Configuration）：全部核心类以构造注入方式组装为
 * Spring Bean，核心模块零 Spring 依赖。
 *
 * <p>Bean 只负责「构造」；带顺序的启动/停机编排见 {@link RuntimeBootstrapper}（SmartLifecycle）。</p>
 *
 * <h2>持久化双模式（{@code runtime.store}）</h2>
 * <ul>
 *   <li>{@code jdbc}（默认）：DataSource 由 spring-boot-starter-jdbc 自动装配（HikariCP），
 *       全部仓库使用 Jdbc* 实现，启动时执行 5 个 Schema.bootstrap（见 RuntimeBootstrapper）；</li>
 *   <li>{@code memory}：全部仓库使用 InMemory* 进程内实现，不建任何 JDBC 仓库、
 *       不执行 DDL、不触碰数据库（单测/单机演示）。</li>
 * </ul>
 */
@Configuration
public class RuntimeAssembly {

    // ------------------------------------------------------------------
    // 基础设施
    // ------------------------------------------------------------------

    @Bean
    Clock systemClock() {
        return SystemClock.instance();
    }

    /** 节点标识：来自配置 runtime.node.id（环境变量 RUNTIME_NODE_ID 可覆盖）。 */
    @Bean
    NodeId selfNodeId(RuntimeProperties props) {
        return NodeId.of(props.node().id());
    }

    /** gRPC 宿主：服务注册（addService）与端口绑定（start）的顺序约束由 RuntimeBootstrapper 编排。 */
    @Bean
    GrpcServerHost grpcServerHost() {
        return new GrpcServerHost();
    }

    /** OS 资源采样：CPU/内存取系统 MXBean，磁盘采样当前工作目录所在卷。 */
    @Bean
    OsResourceMonitor resourceMonitor() {
        return new OsResourceMonitor(new File("."));
    }

    /** 日志订阅/广播总线：批量写入器 publish，SSE/gRPC 推送层 subscribe。 */
    @Bean
    com.ww.runtime.log.LogBus logBus() {
        return new com.ww.runtime.log.LogBus();
    }

    // ------------------------------------------------------------------
    // membership（gRPC 服务端 + 心跳客户端一体）
    // ------------------------------------------------------------------

    /**
     * 成员关系服务。
     *
     * <p><b>addService 时机约束</b>：其 {@code start()} 内部才把自身 addService 到
     * {@link GrpcServerHost}，而 host 一旦 start 便拒绝注册——因此 RuntimeBootstrapper
     * 保证 {@code membershipService.start()} 先于 {@code nodeRuntime.start()}
     * （后者内部执行 {@code host.start(port)}）。</p>
     *
     * <p>端口适配：PeerDirectory=registry.listAlive() 排除 self；StateSink=registry.markState；
     * selfInfo=优先注册表当前视图（含最新 stateEpoch/状态），未注册（启动早期）用本地信息兜底。</p>
     */
    @Bean
    MembershipService membershipService(NodeId self,
                                        NodeRegistry registry,
                                        GrpcServerHost host,
                                        NodeRuntime nodeRuntime,
                                        OsResourceMonitor monitor,
                                        RuntimeProperties props,
                                        Clock clock) {
        Supplier<com.ww.runtime.common.NodeInfo> selfInfo = () -> registry.get(self)
                .orElseGet(() -> com.ww.runtime.common.NodeInfo.of(self, props.node().host(),
                        nodeRuntime.grpcPort(), NodeState.JOINING, 0L,
                        props.node().weight(), monitor.snapshot()));
        PeerDirectory peerDirectory = () -> registry.listAlive().stream()
                .filter(info -> !info.id().equals(self))
                .toList();
        // 同态幂等：注册表已是目标态时跳过 markState（状态机禁止同态跃迁，若直接调用会因
        // gossip 视图抖动反复抛 IllegalStateTransitionException，产生大量 WARN 噪声）
        StateSink stateSink = (id, state) -> {
            NodeState current = registry.get(id).map(com.ww.runtime.common.NodeInfo::state).orElse(null);
            if (current == state) {
                return;
            }
            registry.markState(id, state);
        };
        return new MembershipService(self, selfInfo, peerDirectory, stateSink, host,
                props.toMembershipConfig(), clock);
    }

    // ------------------------------------------------------------------
    // gRPC 服务端（除 MembershipService 外均由 RuntimeBootstrapper 在 host.start 前挂载）
    // ------------------------------------------------------------------

    @Bean
    MigrationServer migrationServer(CompositePartitionAdapter adapter,
                                    MigrationTaskStore store,
                                    Clock clock) {
        return new MigrationServer(adapter, store, clock);
    }

    /** ClusterControl 服务端：接收调度器（GrpcNodeCommandChannel）下发的能力生命周期命令。 */
    @Bean
    ClusterControlServer clusterControlServer(NodeId self, CapabilityManager manager) {
        return new ClusterControlServer(self, manager);
    }

    /** Health 服务端：healthy=true，detail=节点 id 与 leader 状态。 */
    @Bean
    HealthServer healthServer(NodeId self, ReconcileLoop reconcileLoop) {
        return new HealthServer(self, reconcileLoop);
    }

    /** 日志流 gRPC 服务端：对端 hub 经 server-streaming 订阅本节点日志（只推本节点，不递归 fan-out）。 */
    @Bean
    com.ww.runtime.server.log.LogStreamGrpcServer logStreamGrpcServer(com.ww.runtime.log.LogBus logBus) {
        return new com.ww.runtime.server.log.LogStreamGrpcServer(logBus);
    }

    // ------------------------------------------------------------------
    // 节点运行时（注册 / gRPC 端口绑定 / 周期心跳；start 由 RuntimeBootstrapper 驱动）
    // ------------------------------------------------------------------

    @Bean
    NodeRuntime nodeRuntime(NodeId self,
                            RuntimeProperties props,
                            NodeRegistry registry,
                            GrpcServerHost host,
                            OsResourceMonitor monitor,
                            Clock clock) {
        return new NodeRuntime(self, props.node().host(), props.node().grpcPort(),
                props.node().weight(), registry, host, monitor, clock, props.heartbeatIntervalMs());
    }

    // ------------------------------------------------------------------
    // placement / capability
    // ------------------------------------------------------------------

    @Bean
    PlacementEngine placementEngine() {
        return new PlacementEngine();
    }

    @Bean
    RebalancePlanner rebalancePlanner() {
        return new RebalancePlanner();
    }

    /** capability 的 PlacementView 端口适配：placement 行 → Owner(node, epoch)。 */
    @Bean
    PlacementViewAdapter placementViewAdapter(PlacementStore placementStore) {
        return new PlacementViewAdapter(placementStore);
    }

    /** Mock 能力（fencing 演示 + 迁移数据面），key→分区映射与调度器同口径。 */
    @Bean
    MockCapability mockCapability(NodeId self,
                                  PlacementViewAdapter placementView,
                                  RuntimeProperties props) {
        return new MockCapability(self, placementView,
                key -> Partitioner.partition(key, props.partitionCount()));
    }

    @Bean
    MockPartitionAdapter mockPartitionAdapter(MockCapability mockCapability) {
        return new MockPartitionAdapter(mockCapability);
    }

    // ------------------------------------------------------------------
    // 数据面能力：CACHE（SimpleCache 包装）与 STORAGE（ObjectStore 包装）
    // ------------------------------------------------------------------

    /** CACHE 能力：分区口径与调度器一致（Partitioner）；AOF 目录/开关来自 capabilities 配置。 */
    @Bean
    CacheCapability cacheCapability(NodeId self,
                                    PlacementViewAdapter placementView,
                                    RuntimeProperties props) {
        return new CacheCapability(self, placementView,
                key -> Partitioner.partition(key, props.partitionCount()),
                props.capabilities().cacheAofEnabled(),
                new File(props.capabilities().cacheDataDir()));
    }

    /** STORAGE 本地配置：runtime.capabilities.storage-* 映射为 common/storage 的 StorageProperties。 */
    @Bean
    com.ww.storage.StorageProperties storageProperties(RuntimeProperties props) {
        com.ww.storage.StorageProperties sp = new com.ww.storage.StorageProperties();
        String drives = props.capabilities().storageDrives();
        if (drives != null && !drives.isBlank()) {
            sp.setDrives(java.util.Arrays.stream(drives.split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList());
        }
        sp.setRootDir(props.capabilities().storageDataDir());
        return sp;
    }

    /** STORAGE 能力：节点内 Set/Drive 分片仍由 SetLocator 负责，节点间分区归 runtime placement。 */
    @Bean
    StorageCapability storageCapability(NodeId self,
                                        PlacementViewAdapter placementView,
                                        RuntimeProperties props,
                                        com.ww.storage.StorageProperties storageProperties) {
        return new StorageCapability(self, placementView,
                bucketSlashKey -> Partitioner.partition(bucketSlashKey, props.partitionCount()),
                storageProperties, props.toMigrationConfig().chunkSize());
    }

    @Bean
    CachePartitionAdapter cachePartitionAdapter(CacheCapability cacheCapability) {
        return new CachePartitionAdapter(cacheCapability);
    }

    @Bean
    StoragePartitionAdapter storagePartitionAdapter(StorageCapability storageCapability) {
        return new StoragePartitionAdapter(storageCapability);
    }

    /** 多能力分区数据适配器：MigrationEngine/MigrationServer 注入本组合，按 capability 分发。 */
    @Bean
    CompositePartitionAdapter compositePartitionAdapter(MockPartitionAdapter mock,
                                                        CachePartitionAdapter cache,
                                                        StoragePartitionAdapter storage) {
        return new CompositePartitionAdapter(java.util.Map.of(
                "MOCK", (PartitionDataAdapter) mock,
                "CACHE", cache,
                "STORAGE", storage));
    }

    @Bean
    CapabilityManager capabilityManager(NodeId self, CapabilityStateStore store, Clock clock) {
        return new CapabilityManager(self, store, clock);
    }

    // ------------------------------------------------------------------
    // 节点命令通道：GrpcNodeCommandChannel 内建 self 本地回退（node==self 走
    // LocalNodeCommandChannel 进程内直连，其余经 NodeRegistry 解析 host:grpcPort 远程下发）
    // ------------------------------------------------------------------

    @Bean
    LocalNodeCommandChannel localNodeCommandChannel(NodeId self, CapabilityManager manager) {
        return new LocalNodeCommandChannel(self, manager);
    }

    @Bean
    GrpcNodeCommandChannel grpcNodeCommandChannel(NodeId self,
                                                  LocalNodeCommandChannel local,
                                                  NodeRegistry registry) {
        return new GrpcNodeCommandChannel(self, local, registry);
    }

    // ------------------------------------------------------------------
    // migration / scheduler
    // ------------------------------------------------------------------

    @Bean
    MigrationEngine migrationEngine(MigrationTaskStore store,
                                    CompositePartitionAdapter adapter,
                                    PlacementStore placementStore,
                                    RuntimeProperties props,
                                    Clock clock) {
        return new MigrationEngine(store, adapter, placementStore, props.toMigrationConfig(), clock);
    }

    /** MigrationCoordinator 适配器：scheduler 的迁移协调端口委托 MigrationEngine.migrateAndWait。 */
    @Bean
    RuntimeMigrationCoordinator runtimeMigrationCoordinator(MigrationEngine engine) {
        return new RuntimeMigrationCoordinator(engine);
    }

    @Bean
    ReconcileLoop reconcileLoop(NodeId self,
                                LeaseStore leaseStore,
                                NodeRegistry registry,
                                CapabilityStateStore capabilityStore,
                                PlacementStore placementStore,
                                PlacementEngine engine,
                                RebalancePlanner planner,
                                GrpcNodeCommandChannel commands,
                                RuntimeMigrationCoordinator migration,
                                RuntimeProperties props,
                                Clock clock) {
        return new ReconcileLoop(self, leaseStore, registry, capabilityStore, placementStore,
                engine, planner, commands, migration, props.toSchedulerConfig(), clock);
    }

    /** 期望态写入器（§60：管理面只写期望态，含清空最后 ACTIVE 节点保护）。 */
    @Bean
    DesiredStateWriter desiredStateWriter(CapabilityStateStore store) {
        return new DesiredStateWriter(store);
    }

    // ------------------------------------------------------------------
    // 启动/停机编排（SmartLifecycle：phase=MAX_VALUE，最后启动、最先停机）
    // ------------------------------------------------------------------

    @Bean
    RuntimeBootstrapper runtimeBootstrapper(ObjectProvider<DataSource> dataSource,
                                            RuntimeProperties props,
                                            NodeId self,
                                            GrpcServerHost grpcHost,
                                            MembershipService membershipService,
                                            MigrationServer migrationServer,
                                            ClusterControlServer clusterControlServer,
                                            HealthServer healthServer,
                                            com.ww.runtime.server.log.LogStreamGrpcServer logStreamGrpcServer,
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
        return new RuntimeBootstrapper(dataSource, props, self, grpcHost, membershipService,
                migrationServer, clusterControlServer, healthServer, logStreamGrpcServer, nodeRuntime,
                capabilityManager, capabilityStateStore, mockCapability, cacheCapability, storageCapability,
                desiredStateWriter, grpcCommandChannel,
                migrationEngine, reconcileLoop);
    }

    // ------------------------------------------------------------------
    // 持久化双模式：jdbc（默认）/ memory
    // ------------------------------------------------------------------

    /** jdbc 模式：DataSource 由 starter-jdbc 自动装配（HikariCP + spring.datasource.*）。 */
    @Configuration
    @ConditionalOnProperty(name = "runtime.store", havingValue = "jdbc", matchIfMissing = true)
    static class JdbcStoreConfiguration {

        @Bean
        JdbcNodeRegistry nodeRegistry(DataSource dataSource) {
            return new JdbcNodeRegistry(dataSource);
        }

        @Bean
        JdbcCapabilityStateStore capabilityStateStore(DataSource dataSource, Clock clock) {
            return new JdbcCapabilityStateStore(dataSource, clock);
        }

        @Bean
        JdbcPlacementStore placementStore(DataSource dataSource) {
            return new JdbcPlacementStore(dataSource);
        }

        @Bean
        JdbcLeaseStore leaseStore(DataSource dataSource, Clock clock) {
            return new JdbcLeaseStore(dataSource, clock);
        }

        @Bean
        JdbcMigrationTaskStore migrationTaskStore(DataSource dataSource, Clock clock) {
            return new JdbcMigrationTaskStore(dataSource, clock);
        }
    }

    /** memory 模式：全部进程内实现，不建 DataSource、不执行 DDL（单测/单机演示）。 */
    @Configuration
    @ConditionalOnProperty(name = "runtime.store", havingValue = "memory")
    static class MemoryStoreConfiguration {

        @Bean
        InMemoryNodeRegistry nodeRegistry() {
            return new InMemoryNodeRegistry();
        }

        @Bean
        InMemoryCapabilityStateStore capabilityStateStore(Clock clock) {
            return new InMemoryCapabilityStateStore(clock);
        }

        @Bean
        InMemoryPlacementStore placementStore() {
            return new InMemoryPlacementStore();
        }

        @Bean
        InMemoryLeaseStore leaseStore(Clock clock) {
            return new InMemoryLeaseStore(clock);
        }

        @Bean
        InMemoryMigrationTaskStore migrationTaskStore(Clock clock) {
            return new InMemoryMigrationTaskStore(clock);
        }
    }
}
