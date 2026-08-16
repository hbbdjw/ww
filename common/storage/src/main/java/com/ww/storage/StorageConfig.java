package com.ww.storage;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import tools.jackson.databind.ObjectMapper;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 存储模块装配：复用 cluster.* 集群拓扑构建 StorageNode 视图，组装 Drive/Set、本地存储、
 * 节点间 HTTP 客户端与分布式门面。
 *
 * <p>集群节点 HTTP 端口推导：优先使用 cluster.nodes[].httpPort（显式配置）；
 * 否则 httpPort = cluster.nodes[].port + httpPortOffset，offset 默认 = server.port - cache.port
 * （对称部署：cache TCP port 与 HTTP server.port 间距一致，故推导成立）。
 */
@Configuration
public class StorageConfig {

    private static final Logger log = LoggerFactory.getLogger(StorageConfig.class);

    private final StorageProperties storageProps;
    private final ClusterProperties clusterProps;
    private final CachePortProperties cachePortProps;
    private final ObjectMapper objectMapper;
    private final int serverPort;

    private DriveManager driveManager;
    private SetLocator setLocator;
    private CryptoCodec cryptoCodec;
    private BucketStore bucketStore;
    private ObjectStore objectStore;
    private NodeRouter nodeRouter;
    private StorageRpcClient nodeClient;
    private ByteBufferPool bufferPool;
    private ObjectMetadataCache metaCache;
    private DistributedStorage distributedStorage;
    private StorageTopologyManager topologyManager;
    private StorageMigrationService migrationService;
    private StorageFailoverManager failoverManager;

    public StorageConfig(StorageProperties storageProps,
                         ClusterProperties clusterProps,
                         CachePortProperties cachePortProps,
                         ObjectMapper objectMapper,
                         @Value("${server.port:8080}") int serverPort) {
        this.storageProps = storageProps;
        this.clusterProps = clusterProps;
        this.cachePortProps = cachePortProps;
        this.objectMapper = objectMapper;
        this.serverPort = serverPort;
    }

    @PostConstruct
    public void init() {
        // 1. 构建 StorageNode 视图（复用 cluster 配置）
        List<StorageNode> nodes = buildStorageNodes();
        int replicas = clusterProps.isEnabled() ? Math.max(1, clusterProps.getReplicas()) : 1;
        this.nodeRouter = new NodeRouter(nodes, replicas);
        log.info("storage 节点路由: distributed={}, nodes={}, replicas={}, self={}",
                nodeRouter.isDistributed(), nodes.size(), replicas, nodeRouter.getSelf());

        // 2. Drive / Set
        this.driveManager = new DriveManager(storageProps, nodes.size());
        this.driveManager.init();
        this.setLocator = new SetLocator(driveManager);

        // 3. 加密（预留）
        this.cryptoCodec = new IdentityCryptoCodec();
        if (storageProps.isCryptoEnabled()) {
            log.warn("storage.cryptoEnabled=true，但当前仅提供 IdentityCryptoCodec（透传），加密为预留扩展点");
        }

        // 4. 直接内存 IO 缓冲池 + 元数据缓存（容量<=0 时退化为容量 1 的 LRU，保证 Bean 非 null）
        this.bufferPool = new ByteBufferPool(storageProps.getIoBufferSize(), storageProps.getBufferPoolMaxSize());
        int cacheMax = Math.max(1, storageProps.getMetadataCacheMaxSize());
        this.metaCache = new ObjectMetadataCache(cacheMax);
        log.info("storage IO 池: bufferSize={}, poolMax={}, metaCacheMax={}",
                bufferPool.getBufferSize(), bufferPool.getMaxPoolSize(), cacheMax);

        // 5. 本地存储
        this.bucketStore = new BucketStore(driveManager, objectMapper);
        this.objectStore = new ObjectStore(driveManager, setLocator, cryptoCodec, objectMapper, bufferPool);

        // 6. 分布式
        this.nodeClient = new StorageNodeClient(storageProps.getAuthToken(), objectMapper);
        this.distributedStorage = new DistributedStorage(bucketStore, objectStore, nodeRouter, nodeClient, metaCache, setLocator);
        if (nodeRouter.isDistributed()) {
            // 启动复制失败补偿（backlog 重试 + 周期对账）
            distributedStorage.startRepair();
        }

        // 7. 动态拓扑管理 + 迁移服务（扩缩容）
        this.topologyManager = new StorageTopologyManager(nodeRouter, nodeClient, objectMapper,
                storageProps.getRootDir());
        this.migrationService = new StorageMigrationService(topologyManager, nodeClient, bucketStore,
                objectStore, nodeRouter, objectMapper, storageProps.getRootDir());

        // 8. failover 管理（心跳探测 + 自动故障切换 + 原主反向同步）
        this.failoverManager = new StorageFailoverManager(topologyManager, nodeClient, distributedStorage);
        if (nodeRouter.isDistributed()) {
            failoverManager.start();
        }
        log.info("storage 模块装配完成");
    }

    @PreDestroy
    public void destroy() {
        if (failoverManager != null) {
            failoverManager.stop();
        }
        if (distributedStorage != null) {
            distributedStorage.stopRepair();
        }
    }

    /**
     * 从 cluster.* 构建 StorageNode 列表。单机模式（cluster 未启用或无节点）构造单节点。
     */
    private List<StorageNode> buildStorageNodes() {
        int selfCachePort = cachePortProps.getPort();
        int offset = storageProps.getHttpPortOffset();
        if (offset == 0 && serverPort > 0 && selfCachePort > 0) {
            offset = serverPort - selfCachePort;
        }

        if (!clusterProps.isEnabled() || clusterProps.getNodes() == null || clusterProps.getNodes().isEmpty()) {
            // 单机模式
            String host = getLocalHost();
            StorageNode self = new StorageNode(host, serverPort > 0 ? serverPort : 8080, 1.0, true);
            log.info("storage 单机模式: {}", self);
            return List.of(self);
        }

        // 集群模式：先找到 self（cache port 匹配）
        ClusterNode selfCluster = null;
        for (ClusterNode n : clusterProps.getNodes()) {
            if (n.getPort() == selfCachePort) {
                selfCluster = n;
                break;
            }
        }
        if (selfCluster == null) {
            // 回退：第一个节点作为 self
            selfCluster = clusterProps.getNodes().getFirst();
            log.warn("无法按 cache.port={} 精确匹配本节点，回退到首个节点作为 self: {}",
                    selfCachePort, selfCluster);
        }

        List<StorageNode> result = new ArrayList<>();
        for (ClusterNode cn : clusterProps.getNodes()) {
            int httpPort = cn.getHttpPort() > 0 ? cn.getHttpPort() : cn.getPort() + offset;
            if (httpPort <= 0) httpPort = serverPort > 0 ? serverPort : 8080;
            boolean self = (cn == selfCluster) || (cn.getPort() == selfCluster.getPort()
                    && cn.getHost().equals(selfCluster.getHost()));
            result.add(new StorageNode(cn.getHost(), httpPort, cn.getWeight() > 0 ? cn.getWeight() : 1.0, self));
        }
        return result;
    }

    private static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    // ==================== Beans ====================

    @Bean
    public DistributedStorage distributedStorage() { return distributedStorage; }

    @Bean
    public BucketStore bucketStore() { return bucketStore; }

    @Bean
    public ObjectStore objectStore() { return objectStore; }

    @Bean
    public DriveManager driveManager() { return driveManager; }

    @Bean
    public SetLocator setLocator() { return setLocator; }

    @Bean
    public NodeRouter nodeRouter() { return nodeRouter; }

    @Bean
    public StorageRpcClient storageNodeClient() { return nodeClient; }

    @Bean
    public CryptoCodec cryptoCodec() { return cryptoCodec; }

    @Bean
    public ByteBufferPool byteBufferPool() { return bufferPool; }

    @Bean
    public ObjectMetadataCache objectMetadataCache() { return metaCache; }

    @Bean
    public StorageTopologyManager storageTopologyManager() { return topologyManager; }

    @Bean
    public StorageMigrationService storageMigrationService() { return migrationService; }

    @Bean
    public StorageFailoverManager storageFailoverManager() { return failoverManager; }
}
