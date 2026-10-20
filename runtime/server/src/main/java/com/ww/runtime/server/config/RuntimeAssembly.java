package com.ww.runtime.server.config;

import java.io.File;
import java.util.List;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.capability.JdbcCapabilityStateStore;
import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.cluster.InMemoryNodeSuspicionStore;
import com.ww.runtime.cluster.JdbcNodeRegistry;
import com.ww.runtime.cluster.JdbcNodeSuspicionStore;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.cluster.NodeSuspicionStore;
import com.ww.runtime.cluster.NodeRuntime;
import com.ww.runtime.cluster.OsResourceMonitor;
import com.ww.runtime.cluster.decommission.DecommissionStore;
import com.ww.runtime.cluster.decommission.InMemoryDecommissionStore;
import com.ww.runtime.cluster.decommission.JdbcDecommissionStore;
import com.ww.runtime.common.CapabilityType;
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
import com.ww.runtime.membership.MemberEvent;
import com.ww.runtime.membership.MembershipService;
import com.ww.runtime.membership.PeerDirectory;
import com.ww.runtime.membership.StateSink;
import com.ww.runtime.membership.SuspicionSink;
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
import com.ww.runtime.scheduler.DecommissionDriver;
import com.ww.runtime.scheduler.DesiredStateWriter;
import com.ww.runtime.scheduler.GrpcNodeCommandChannel;
import com.ww.runtime.scheduler.InMemoryLeaseStore;
import com.ww.runtime.scheduler.JdbcLeaseStore;
import com.ww.runtime.scheduler.LeaseStore;
import com.ww.runtime.scheduler.LocalNodeCommandChannel;
import com.ww.runtime.scheduler.ReconcileLoop;
import com.ww.runtime.server.RuntimeProperties;
import com.ww.runtime.server.cluster.CachedClusterDirectory;
import com.ww.runtime.server.decommission.DecommissionService;
import com.ww.runtime.server.decommission.RuntimeDecommissionCoordinator;
import com.ww.runtime.server.grpc.ClusterControlServer;
import com.ww.runtime.server.grpc.HealthServer;
import com.ww.runtime.server.migration.RuntimeMigrationCoordinator;
import com.ww.runtime.server.placement.CachedPlacementView;
import com.ww.runtime.server.placement.PartitionLeaseManager;
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

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(RuntimeAssembly.class);

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

    /**
     * S19：gRPC 服务端有界执行器——默认 cached 线程池无界，控制面钩子（能力 start/stop）
     * 卡死会拖出线程膨胀；命名有界池（8 核心/64 上限/队列 1024），拒绝由 grpc 回压。
     */
    @Bean(name = "grpcServerExecutor", destroyMethod = "shutdown")
    java.util.concurrent.ThreadPoolExecutor grpcServerExecutor() {
        java.util.concurrent.ThreadPoolExecutor pool = new java.util.concurrent.ThreadPoolExecutor(
                8, 64, 60L, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(1024),
                r -> {
                    Thread t = new Thread(r, "grpc-server-worker");
                    t.setDaemon(true);
                    return t;
                });
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    /** gRPC 宿主：服务注册（addService）与端口绑定（start）的顺序约束由 RuntimeBootstrapper 编排。 */
    @Bean
    GrpcServerHost grpcServerHost(
            @org.springframework.beans.factory.annotation.Qualifier("grpcServerExecutor")
            java.util.concurrent.ThreadPoolExecutor grpcServerExecutor) {
        // S19：统一挂载有界执行器（控制面/数据面共用宿主）
        return new GrpcServerHost().setExecutor(grpcServerExecutor);
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
     * <p>端口适配：PeerDirectory=registry.listAlive() 排除 self；StateSink=registry.markState
     * （S12 纪元防护：他报回写比较注册表 state_epoch，陈旧决定丢弃；直证自报豁免）；
     * selfInfo=优先注册表当前视图（含最新 stateEpoch/状态），未注册（启动早期）用本地信息兜底。</p>
     */
    @Bean
    MembershipService membershipService(NodeId self,
                                        NodeRegistry registry,
                                        GrpcServerHost host,
                                        NodeRuntime nodeRuntime,
                                        OsResourceMonitor monitor,
                                        GrpcNodeCommandChannel grpcNodeCommandChannel,
                                        NodeSuspicionStore suspicionStore,
                                        RuntimeProperties props,
                                        Clock clock) {
        Supplier<com.ww.runtime.common.NodeInfo> selfInfo = () -> registry.get(self)
                .orElseGet(() -> com.ww.runtime.common.NodeInfo.of(self, props.node().host(),
                        nodeRuntime.grpcPort(), NodeState.JOINING, 0L,
                        props.node().weight(), monitor.snapshot()));
        PeerDirectory peerDirectory = () -> registry.listAlive().stream()
                .filter(info -> !info.id().equals(self))
                .toList();
        StateSink stateSink = new StateSink() {
            @Override
            public void apply(NodeId id, NodeState newState) {
                apply(id, newState, -1L, false);
            }

            @Override
            public void apply(NodeId id, NodeState newState, long observedEpoch, boolean directEvidence) {
                com.ww.runtime.common.NodeInfo current = registry.get(id).orElse(null);
                if (current == null) {
                    return; // 注册表已无此行（被清理），无需回写
                }
                // S12 纪元防护：判定基于 observedEpoch 的视图；注册表已推进（更高纪元=更新的
                // 事实，含重新注册/他实例回写）则本条为陈旧决定，丢弃。
                // 豁免：直证自报——节点自身是自身状态的绝对权威，且重新注册会重置纪元，
                // 按数字比较会把合法复活一并拦死（红线：防护不拦自报）。
                if (!directEvidence && observedEpoch >= 0 && current.stateEpoch() > observedEpoch) {
                    LOG.warn("S12: stale state write-back dropped for {} -> {} "
                                    + "(observedEpoch={} < registryEpoch={})",
                            id, newState, observedEpoch, current.stateEpoch());
                    return;
                }
                // 同态幂等：注册表已是目标态时跳过 markState（状态机禁止同态跃迁，若直接调用会因
                // gossip 视图抖动反复抛 IllegalStateTransitionException，产生大量 WARN 噪声）
                if (current.state() == newState) {
                    return;
                }
                registry.markState(id, newState);
            }
        };
        // 多级仲裁桥接：membership 只产出"本节点对 peer 的怀疑票"，票表在共享注册表，
        // 由 leader ReconcileLoop 计票达法定人数后唯一提升 DEAD（PFAIL→FAIL）。
        SuspicionSink suspicionSink = new SuspicionSink() {
            @Override
            public void vote(NodeId suspect, double phi) {
                try {
                    suspicionStore.vote(suspect, self, phi);
                } catch (RuntimeException e) {
                    LOG.warn("suspicion vote failed for {} by {}: {}", suspect, self, e.toString());
                }
            }

            @Override
            public void recall(NodeId suspect) {
                try {
                    suspicionStore.recall(suspect, self);
                } catch (RuntimeException e) {
                    LOG.debug("suspicion recall failed for {} by {}: {}", suspect, self, e.toString());
                }
            }
        };
        MembershipService service = new MembershipService(self, selfInfo, peerDirectory, stateSink,
                host, props.toMembershipConfig(), clock, suspicionSink);
        // A3：成员事件驱动驱逐已下线节点的命令通道（通道缓存不再随节点历史单调增长）；
        // 节点复活后调度器重新下发命令时通道按 target 自动重建
        service.addListener(event -> {
            if (MemberEvent.NODE_DEAD.equals(event.type())) {
                grpcNodeCommandChannel.evictNode(event.node().id());
            }
        });
        return service;
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
                            Clock clock,
                            @Value("${server.port:8080}") int httpPort) {
        return new NodeRuntime(self, props.node().host(), props.node().grpcPort(),
                httpPort, props.node().weight(), registry, host, monitor, clock,
                props.heartbeatIntervalMs())
                .withZone(props.node().zone());
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

    /**
     * 数据面 placement 视图：
     * cache=本地修订快照 + 本地租约（Phase 1+2，默认）；db=每次查库（回滚开关）。
     */
    @Bean
    @Primary
    PlacementView dataPlacementView(RuntimeProperties props,
                                    PlacementStore placementStore,
                                    CachedPlacementView cachedPlacementView) {
        if ("db".equals(props.route().mode())) {
            return new PlacementViewAdapter(placementStore);
        }
        return cachedPlacementView;
    }

    /** 放置本地快照：后台轮询修订号，数据面读写只查内存。 */
    @Bean
    CachedPlacementView cachedPlacementView(PlacementStore placementStore,
                                            Clock clock,
                                            RuntimeProperties props) {
        return new CachedPlacementView(placementStore, clock,
                props.route().viewRefreshMs(),
                props.route().fullRefreshMs(),
                props.fence().safetyMarginMs());
    }

    /** 本地写租约：后台批量续约，写路径只校验内存有效期。 */
    @Bean
    PartitionLeaseManager partitionLeaseManager(PlacementStore placementStore,
                                                CachedPlacementView cachedPlacementView,
                                                NodeId self,
                                                Clock clock,
                                                RuntimeProperties props) {
        return new PartitionLeaseManager(placementStore, cachedPlacementView, self, clock,
                props.fence().leaseTtlMs(),
                props.fence().leaseRenewMs(),
                props.fence().safetyMarginMs(),
                props.fence().leaseBatchSize(),
                "cache".equals(props.route().mode()));
    }

    /** 节点端点/能力 serving 集合缓存：数据面 fan-out 与转发不再同步查注册表。 */
    @Bean
    CachedClusterDirectory cachedClusterDirectory(NodeRegistry registry,
                                                  CapabilityStateStore capabilityStateStore,
                                                  RuntimeProperties props) {
        return new CachedClusterDirectory(registry, capabilityStateStore,
                props.route().directoryRefreshMs(),
                "cache".equals(props.route().mode()));
    }

    /** Mock 能力（fencing 演示 + 迁移数据面），key→分区映射与调度器同口径。 */
    @Bean
    MockCapability mockCapability(NodeId self,
                                  PlacementView placementView,
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

    /** CACHE 能力：分区口径与调度器一致（Partitioner）；AOF 目录/开关/重写缓冲上限来自 capabilities 配置。 */
    @Bean
    CacheCapability cacheCapability(NodeId self,
                                    PlacementView placementView,
                                    RuntimeProperties props) {
        return new CacheCapability(self, placementView,
                key -> Partitioner.partition(key, props.partitionCount()),
                props.capabilities().cacheAofEnabled(),
                new File(props.capabilities().cacheDataDir()),
                props.capabilities().cacheAofRewriteBufferMaxBytes())
                .withCapacityLimits(
                        props.capabilities().cacheMaxSize(),
                        props.capabilities().cacheStreamMaxLen(),
                        props.capabilities().cachePelMaxEntries(),
                        props.capabilities().cacheMaxStreams(),
                        props.capabilities().cacheMaxMemoryBytes());
    }

    /** STORAGE 本地配置：runtime.capabilities.storage-* 映射为 storage 引擎的 StorageProperties。 */
    @Bean
    com.ww.runtime.data.storage.engine.StorageProperties storageProperties(RuntimeProperties props) {
        com.ww.runtime.data.storage.engine.StorageProperties sp = new com.ww.runtime.data.storage.engine.StorageProperties();
        String drives = props.capabilities().storageDrives();
        if (drives != null && !drives.isBlank()) {
            sp.setDrives(java.util.Arrays.stream(drives.split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList());
        }
        sp.setRootDir(props.capabilities().storageDataDir());
        sp.setMaxObjectSize(props.capabilities().storageMaxObjectSize());
        sp.setMetadataIndexMaxEntries(props.capabilities().storageMetadataIndexMaxEntries());
        sp.setImportStagingMaxAggregates(props.capabilities().storageImportStagingMaxAggregates());
        sp.setImportStagingTimeoutMinutes(props.capabilities().storageImportStagingTimeoutMinutes());
        // P0-1 半同步复制（默认关；开启后主写落盘须等 ≥1 副本确认才应答）
        sp.setSemiSyncEnabled(props.capabilities().storageSemiSyncEnabled());
        sp.setSemiSyncTimeoutMs(props.capabilities().storageSemiSyncTimeoutMs());
        sp.setSemiSyncFailClosed(props.capabilities().storageSemiSyncFailClosed());
        return sp;
    }

    /**
     * P0-2 分区隔离数据面视图：DegradedPartitionStore 的 TTL 内存快照，
     * 注入 Storage/Cache 网关热路径（@PostConstruct 启动周期刷新）。
     */
    @Bean
    com.ww.runtime.server.degraded.DegradedPartitionCache degradedPartitionCache(
            com.ww.runtime.placement.DegradedPartitionStore degradedPartitionStore) {
        return new com.ww.runtime.server.degraded.DegradedPartitionCache(degradedPartitionStore);
    }

    /** STORAGE 能力：节点内 Set/Drive 分片仍由 SetLocator 负责，节点间分区归 runtime placement。 */
    @Bean
    StorageCapability storageCapability(NodeId self,
                                        PlacementView placementView,
                                        RuntimeProperties props,
                                        com.ww.runtime.data.storage.engine.StorageProperties storageProperties) {
        return new StorageCapability(self, placementView,
                bucketSlashKey -> Partitioner.partition(bucketSlashKey, props.partitionCount()),
                storageProperties, props.toMigrationConfig().chunkSize());
    }

    // ------------------------------------------------------------------
    // STORAGE 数据面：gRPC 服务端/客户端 + 副本复制器 + 网关
    // ------------------------------------------------------------------

    /** 对等节点目录端口适配：本地目录快照 → gRPC 数据面端点（热路径不查库）。 */
    @Bean
    com.ww.runtime.data.storage.StoragePeerDirectory storagePeerDirectory(
            CachedClusterDirectory directory) {
        return new com.ww.runtime.data.storage.StoragePeerDirectory() {
            @Override
            public java.util.Optional<PeerEndpoint> endpoint(NodeId node) {
                return directory.endpoint(node)
                        .map(info -> new PeerEndpoint(info.host(), info.grpcPort()));
            }

            @Override
            public java.util.List<NodeId> alivePeers() {
                return directory.alivePeers();
            }

            @Override
            public List<NodeId> servingPeers() {
                return directory.servingPeers(CapabilityType.STORAGE);
            }
        };
    }

    /** 存储 gRPC 客户端：网关转发/复制/fan-out 列举共用（通道按 target 缓存）。 */
    @Bean
    com.ww.runtime.data.storage.StorageGrpcClient storageGrpcClient(
            com.ww.runtime.data.storage.StoragePeerDirectory directory) {
        return new com.ww.runtime.data.storage.StorageGrpcClient(directory);
    }

    /** 存储 gRPC 服务端：ForwardPut/Get/Head/Delete + Replicate* + ListBucket + SyncBucket。 */
    @Bean
    com.ww.runtime.data.storage.StorageGrpcServer storageGrpcServer(StorageCapability capability,
                                                                    RuntimeProperties props) {
        return new com.ww.runtime.data.storage.StorageGrpcServer(capability,
                props.toMigrationConfig().chunkSize());
    }

    /** 副本复制器：写后异步复制 + backlog 重放 + 周期对账（start/stop 由 Bootstrapper 编排）。 */
    @Bean
    com.ww.runtime.data.storage.StorageReplicator storageReplicator(NodeId self,
                                                                    PlacementView placementView,
                                                                    StorageCapability capability,
                                                                    com.ww.runtime.data.storage.StorageGrpcClient client,
                                                                    com.ww.runtime.data.storage.StoragePeerDirectory directory,
                                                                    com.ww.runtime.data.storage.StorageGateway gateway,
                                                                    RuntimeProperties props) {
        com.ww.runtime.data.storage.StorageReplicator replicator =
                new com.ww.runtime.data.storage.StorageReplicator(self, placementView,
                        bucketSlashKey -> Partitioner.partition(bucketSlashKey, props.partitionCount()),
                        capability, client, directory);
        // T21：周期桶对账（fan-out 比差 + 缺失补建），随 replicator 调度器运行
        replicator.setBucketReconciler(gateway::reconcileBuckets);
        return replicator;
    }

    /** STORAGE 网关：REST 层按 placement 自动路由（本地/转发），fan-out 列举与桶广播。 */
    @Bean
    com.ww.runtime.data.storage.StorageGateway storageGateway(NodeId self,
                                                              PlacementView placementView,
                                                              StorageCapability capability,
                                                              com.ww.runtime.data.storage.StorageGrpcClient client,
                                                              com.ww.runtime.data.storage.StoragePeerDirectory directory,
                                                              RuntimeProperties props,
                                                              com.ww.runtime.server.degraded.DegradedPartitionCache degradedCache) {
        return new com.ww.runtime.data.storage.StorageGateway(self, placementView,
                bucketSlashKey -> Partitioner.partition(bucketSlashKey, props.partitionCount()),
                capability, client, directory, degradedCache);
    }

    // ------------------------------------------------------------------
    // CACHE 数据面：gRPC 服务端/客户端 + 网关（对齐 STORAGE 数据面装配）
    // ------------------------------------------------------------------

    /** 缓存对等节点目录端口适配：本地目录快照 → gRPC 数据面端点（热路径不查库）。 */
    @Bean
    com.ww.runtime.data.cache.CachePeerDirectory cachePeerDirectory(
            CachedClusterDirectory directory) {
        return new com.ww.runtime.data.cache.CachePeerDirectory() {
            @Override
            public java.util.Optional<PeerEndpoint> endpoint(NodeId node) {
                return directory.endpoint(node)
                        .map(info -> new PeerEndpoint(info.host(), info.grpcPort()));
            }

            @Override
            public java.util.List<NodeId> alivePeers() {
                return directory.alivePeers();
            }

            @Override
            public List<NodeId> servingPeers() {
                return directory.servingPeers(CapabilityType.CACHE);
            }
        };
    }

    /** 缓存 gRPC 客户端：网关转发（通道按 target 缓存）。 */
    @Bean
    com.ww.runtime.data.cache.CacheGrpcClient cacheGrpcClient(
            com.ww.runtime.data.cache.CachePeerDirectory directory) {
        return new com.ww.runtime.data.cache.CacheGrpcClient(directory);
    }

    /** 缓存 gRPC 服务端：ForwardPut/Get/Delete（属主端 fencing 生效）。 */
    @Bean
    com.ww.runtime.data.cache.CacheGrpcServer cacheGrpcServer(CacheCapability capability) {
        return new com.ww.runtime.data.cache.CacheGrpcServer(capability);
    }

    /** CACHE 网关：REST 层按 placement 自动路由（本地/转发），fan-out 列举/计数，客户端无需感知拓扑。 */
    @Bean
    com.ww.runtime.data.cache.CacheGateway cacheGateway(NodeId self,
                                                        PlacementView placementView,
                                                        CacheCapability capability,
                                                        com.ww.runtime.data.cache.CacheGrpcClient client,
                                                        com.ww.runtime.data.cache.CachePeerDirectory directory,
                                                        RuntimeProperties props,
                                                        com.ww.runtime.server.degraded.DegradedPartitionCache degradedCache) {
        return new com.ww.runtime.data.cache.CacheGateway(self, placementView,
                key -> Partitioner.partition(key, props.partitionCount()),
                capability, client, directory, degradedCache);
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

    /**
     * 节点感知路由适配器：迁移引擎经它按任务 source/target 把数据面调用路由到
     * 「本地组合适配器 | 远端节点」——跨节点数据搬迁的核心接线（能力无关）。
     * resolver 从 NodeRegistry 解析 ACTIVE 节点的 gRPC 端点。
     */
    @Bean
    com.ww.runtime.migration.RoutingPartitionAdapter routingPartitionAdapter(
            CompositePartitionAdapter composite,
            NodeId self,
            NodeRegistry registry) {
        return new com.ww.runtime.migration.RoutingPartitionAdapter(composite, self,
                node -> registry.get(node)
                        .filter(info -> info.state() == NodeState.ACTIVE)
                        .map(info -> new com.ww.runtime.migration.RoutingPartitionAdapter.Endpoint(
                                info.host(), info.grpcPort())));
    }

    /** 启动清扫器：放置表引导后裁剪本机各能力的非属主分区数据（能力无关）。
     * P0-2：DEGRADED 隔离分区（死亡属主页上的最后副本）纳入保留规则，绝不误删。 */
    @Bean
    com.ww.runtime.server.cache.PartitionSweeper partitionSweeper(
            CompositePartitionAdapter composite,
            com.ww.runtime.placement.PlacementStore placementStore,
            com.ww.runtime.migration.MigrationTaskStore migrationTaskStore,
            NodeId self,
            com.ww.runtime.placement.DegradedPartitionStore degradedPartitionStore) {
        return new com.ww.runtime.server.cache.PartitionSweeper(
                composite, placementStore, migrationTaskStore, self, degradedPartitionStore);
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
                                    com.ww.runtime.migration.RoutingPartitionAdapter adapter,
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
                                DecommissionDriver decommission,
                                NodeSuspicionStore suspicionStore,
                                com.ww.runtime.placement.DegradedPartitionStore degradedPartitionStore,
                                RuntimeProperties props,
                                Clock clock) {
        return new ReconcileLoop(self, leaseStore, registry, capabilityStore, placementStore,
                engine, planner, commands, migration, decommission, props.toSchedulerConfig(), clock,
                suspicionStore, degradedPartitionStore);
    }

    /** 期望态写入器（§60：管理面只写期望态，含清空最后 ACTIVE 节点保护）。 */
    @Bean
    DesiredStateWriter desiredStateWriter(CapabilityStateStore store) {
        return new DesiredStateWriter(store);
    }

    /** 退役流程编排器（由 ReconcileLoop 每周期驱动）。 */
    @Bean
    RuntimeDecommissionCoordinator runtimeDecommissionCoordinator(DecommissionStore decommissionStore,
                                                                   NodeRegistry registry,
                                                                   CapabilityStateStore capabilityStore,
                                                                   PlacementStore placementStore,
                                                                   PlacementEngine engine,
                                                                   RuntimeProperties props,
                                                                   Clock clock) {
        return new RuntimeDecommissionCoordinator(decommissionStore, registry, capabilityStore,
                placementStore, engine, props.toSchedulerConfig(), clock);
    }

    /** 退役准入与预检服务（REST 端点调用）。 */
    @Bean
    DecommissionService decommissionService(NodeRegistry registry,
                                            CapabilityStateStore capabilityStore,
                                            PlacementStore placementStore,
                                            DesiredStateWriter desiredStateWriter,
                                            DecommissionStore decommissionStore,
                                            CacheCapability cacheCapability,
                                            com.ww.runtime.data.cache.CacheGrpcClient cacheGrpcClient,
                                            StorageCapability storageCapability,
                                            com.ww.runtime.data.storage.StorageGrpcClient storageGrpcClient,
                                            MigrationTaskStore migrationTaskStore,
                                            NodeId self,
                                            Clock clock) {
        return new DecommissionService(registry, capabilityStore, placementStore, desiredStateWriter,
                decommissionStore, cacheCapability, cacheGrpcClient, storageCapability, storageGrpcClient,
                migrationTaskStore, self, clock);
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
                                            com.ww.runtime.server.security.RbacSeeder rbacSeeder,
                                            com.ww.runtime.server.lb.LbRefresher lbRefresher) {
        return new RuntimeBootstrapper(dataSource, props, self, grpcHost, membershipService,
                migrationServer, clusterControlServer, healthServer, logStreamGrpcServer, nodeRuntime,
                capabilityManager, capabilityStateStore, mockCapability, cacheCapability, storageCapability,
                cacheGrpcServer, cacheGrpcClient, storageGrpcServer, storageReplicator, storageGrpcClient,
                desiredStateWriter, grpcCommandChannel,
                migrationEngine, reconcileLoop, partitionSweeper, routingPartitionAdapter, rbacSeeder,
                lbRefresher);
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
        com.ww.runtime.placement.JdbcDegradedPartitionStore degradedPartitionStore(DataSource dataSource) {
            return new com.ww.runtime.placement.JdbcDegradedPartitionStore(dataSource);
        }

        @Bean
        JdbcLeaseStore leaseStore(DataSource dataSource, Clock clock) {
            return new JdbcLeaseStore(dataSource, clock);
        }

        @Bean
        JdbcMigrationTaskStore migrationTaskStore(DataSource dataSource, Clock clock) {
            return new JdbcMigrationTaskStore(dataSource, clock);
        }

        @Bean
        JdbcDecommissionStore decommissionStore(DataSource dataSource, Clock clock) {
            return new JdbcDecommissionStore(dataSource, clock);
        }

        @Bean
        JdbcNodeSuspicionStore nodeSuspicionStore(DataSource dataSource, Clock clock) {
            return new JdbcNodeSuspicionStore(dataSource, clock);
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
        com.ww.runtime.placement.InMemoryDegradedPartitionStore degradedPartitionStore() {
            return new com.ww.runtime.placement.InMemoryDegradedPartitionStore();
        }

        @Bean
        InMemoryLeaseStore leaseStore(Clock clock) {
            return new InMemoryLeaseStore(clock);
        }

        @Bean
        InMemoryMigrationTaskStore migrationTaskStore(Clock clock) {
            return new InMemoryMigrationTaskStore(clock);
        }

        @Bean
        InMemoryDecommissionStore decommissionStore(Clock clock) {
            return new InMemoryDecommissionStore(clock);
        }

        @Bean
        InMemoryNodeSuspicionStore nodeSuspicionStore(Clock clock) {
            return new InMemoryNodeSuspicionStore(clock);
        }
    }
}
