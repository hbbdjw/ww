package com.ww.admin.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.function.Function;

import com.ww.cache.AofProperties;
import com.ww.cache.AofService;
import com.ww.cache.Cache;
import com.ww.cache.CacheClient;
import com.ww.cache.CacheRpcBridge;
import com.ww.cache.CacheRpcClient;
import com.ww.cache.CacheServer;
import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.FailoverManager;
import com.ww.cache.GrpcCacheRpcClient;
import com.ww.cache.RebalanceManager;
import com.ww.cache.SimpleCache;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.common.RendezvousHash;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.grpc.GrpcRpcService;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 缓存配置：创建缓存实例、注入 AOF 持久化、启动 TCP 服务、集群模式
 */
@Configuration
public class CacheConfig {

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

    private final AofService aofService;
    private final SimpleCache<String, Object> rawCache;
    private final Cache<String, Object> exposedCache;
    private final CachePortProperties cachePortProperties;
    private final ClusterProperties clusterProperties;
    private DistributedCache distributedCache;
    private CacheServer cacheServer;
    /** 内部 gRPC 服务（仅 cluster.isGrpcEnabled() 时非空） */
    private RpcService internalRpcService;
    /** cache gRPC 桥接器（bindRpcService 后非空） */
    private CacheRpcBridge cacheRpcBridge;
    /** 动态成员管理（仅 cluster.membershipEnabled=true 时非空） */
    private ClusterMembership membership;
    /** failover 管理器（仅 cluster.failoverEnabled=true 时非空） */
    private FailoverManager failoverManager;
    /** 数据重平衡管理器（仅 cluster.rebalanceEnabled=true 时非空） */
    private RebalanceManager rebalanceManager;

    public CacheConfig(AofProperties aofProperties,
                       CachePortProperties cachePortProperties,
                       ClusterProperties clusterProperties) {
        // 1. 先创建 rawCache（aofService 暂为 null，后面注入）
        this.rawCache = new SimpleCache<>(null);
        // 2. 创建 AofService，传入 rawCache 用于重写时遍历数据
        this.aofService = new AofService(aofProperties, rawCache);
        // 3. 反向注入 aofService 到 rawCache
        this.rawCache.setAofService(aofService);
        this.cachePortProperties = cachePortProperties;
        this.clusterProperties = clusterProperties;

        if (clusterProperties.isEnabled() && !clusterProperties.getNodes().isEmpty()) {
            // 缺陷3修复：确保副本数 >= 2，否则无备份节点
            int replicas = clusterProperties.getReplicas();
            if (replicas < 2) {
                log.warn("集群 replicas={} 小于 2，无备份节点，已自动修正为 2", replicas);
                clusterProperties.setReplicas(2);
            }
            markSelfNode(clusterProperties.getNodes(), cachePortProperties.getPort());
            RendezvousHash hrw = new RendezvousHash(clusterProperties.getNodes());
            ClusterNode self = findSelf(clusterProperties.getNodes());
            log.info("集群模式启用，节点数: {}, 副本数: {}, 本节点: {}, 协议模式: {}",
                    clusterProperties.getNodes().size(), clusterProperties.getReplicas(), self,
                    clusterProperties.getProtocol());

            // 按需创建内部 gRPC 服务（节点间通信用）
            Function<ClusterNode, ? extends CacheRpcClient> clientFactory;
            String authToken = cachePortProperties.getAuthToken();
            if (clusterProperties.isGrpcEnabled()) {
                int grpcPort = clusterProperties.getInternalPort();
                // 给 self 节点设置 grpcPort，便于其他节点寻址
                self.setGrpcPort(grpcPort);
                GrpcRpcService grpcSvc = new GrpcRpcService(self.getHost(), grpcPort);
                // 初始化 peer 列表
                List<String> peers = clusterProperties.getNodes().stream()
                        .filter(n -> !n.isSelf())
                        .map(n -> {
                            int gp = n.getGrpcPort() > 0 ? n.getGrpcPort() : grpcPort;
                            return n.getHost() + ":" + gp;
                        })
                        .toList();
                grpcSvc.addPeers(peers);
                this.internalRpcService = grpcSvc;
                log.info("内部 gRPC 服务初始化完成, 监听端口: {}, peers={}", grpcPort, peers);

                clientFactory = node -> new GrpcCacheRpcClient(
                        node.getHost(),
                        node.getGrpcPort() > 0 ? node.getGrpcPort() : grpcPort,
                        authToken, grpcSvc);
            } else {
                // 默认 TCP 协议，保持原有行为
                clientFactory = node -> new CacheClient(node.getHost(), node.getPort(), authToken);
            }

            this.distributedCache = new DistributedCache(rawCache, hrw, clusterProperties.getReplicas(), self,
                    authToken, clientFactory);
            this.exposedCache = distributedCache;

            // 装配动态成员管理（如果启用）
            if (clusterProperties.isMembershipEnabled()) {
                this.membership = new ClusterMembership(clusterProperties.getNodes(), self);
                this.distributedCache.setMembership(membership);
                log.info("动态成员管理已启用 (topologyVersion={})", membership.getTopologyVersion());

                if (clusterProperties.isFailoverEnabled()) {
                    this.failoverManager = new FailoverManager(membership,
                            authToken,
                            clusterProperties.getHeartbeatIntervalMs(),
                            clusterProperties.getSuspectThreshold(),
                            clusterProperties.getDeadThreshold(),
                            clientFactory);
                    this.distributedCache.setFailoverManager(failoverManager);
                    log.info("Failover 自动检测已启用: interval={}ms, suspectThreshold={}, deadThreshold={}",
                            clusterProperties.getHeartbeatIntervalMs(),
                            clusterProperties.getSuspectThreshold(),
                            clusterProperties.getDeadThreshold());
                }

                if (clusterProperties.isRebalanceEnabled()) {
                    long scanIntervalMs = clusterProperties.getRebalanceScanIntervalSec() * 1000L;
                    this.rebalanceManager = new RebalanceManager(membership, distributedCache, rawCache,
                            authToken,
                            scanIntervalMs,
                            clientFactory);
                    this.distributedCache.setRebalanceManager(rebalanceManager);
                    log.info("数据重平衡已启用: scanInterval={}s", clusterProperties.getRebalanceScanIntervalSec());
                }
            }
        } else {
            log.warn("集群模式未启用: enabled={}, nodes.size={}",
                    clusterProperties.isEnabled(), clusterProperties.getNodes().size());
            this.exposedCache = rawCache;
        }
        this.cacheServer = new CacheServer(cachePortProperties.getPort(), exposedCache,
                cachePortProperties.getAuthToken());
        // 注入集群管理组件到 CacheServer，使其能处理 PING/JOIN/PROMOTE/MIGRATE/STATUS
        if (membership != null) cacheServer.setMembership(membership);
        if (failoverManager != null) cacheServer.setFailoverManager(failoverManager);
        if (rebalanceManager != null) cacheServer.setRebalanceManager(rebalanceManager);
        // 如果启用 gRPC，注册 cache.command / cache.replica handler 到内部 gRPC 服务
        if (internalRpcService != null) {
            this.cacheRpcBridge = cacheServer.bindRpcService(internalRpcService);
        }
    }

    @Bean
    @Primary
    public Cache<String, Object> defaultCache() {
        return exposedCache;
    }

    /**
     * 暴露本地 SimpleCache 供 HTTP Stream API 使用。
     * <p>Stream 操作在集群模式下仍走 TCP（HRW 路由），HTTP API 仅用于单机调试/测试。
     */
    @Bean
    public SimpleCache<String, Object> localSimpleCache() {
        return rawCache;
    }

    @Bean
    public AofService aofService() {
        return aofService;
    }

    @Bean
    public CacheServer cacheServer() {
        return cacheServer;
    }

    /**
     * 暴露 ClusterMembership（可能为 null）供管理面 Controller 使用。
     * <p>未启用集群动态成员管理时返回 null；消费方用 {@code @Autowired(required=false)} 注入。
     */
    @Bean
    public ClusterMembership clusterMembership() {
        return membership;
    }

    /** 暴露 FailoverManager（可能为 null）供管理面 Controller 使用 */
    @Bean
    public FailoverManager failoverManager() {
        return failoverManager;
    }

    /** 暴露 RebalanceManager（可能为 null）供管理面 Controller 使用 */
    @Bean
    public RebalanceManager rebalanceManager() {
        return rebalanceManager;
    }

    /** 暴露 DistributedCache（可能为 null）供管理面 Controller 使用 */
    @Bean
    public DistributedCache distributedCache() {
        return distributedCache;
    }

    @PostConstruct
    public void init() throws java.io.IOException {
        if (distributedCache != null) {
            // 缺陷10修复：分布式模式下 AOF 回放前已有 HRW 哈希环，
            // 通过 keyFilter 只回放 self 节点作为 primary 的 key，
            // 避免本地残留非 owner 数据导致 backup fallback 读到陈旧值。
            java.util.function.Predicate<String> ownerFilter = buildReplayFilter();
            aofService.replay(rawCache, ownerFilter);
        } else {
            // 非集群模式：全量回放
            aofService.replay(rawCache);
        }
        aofService.start();
        // 缺陷I修复：开启后台过期清理调度，每 60 秒扫描一次
        rawCache.startEvictionScheduler(60_000L);

        // 启动内部 gRPC 服务（如果启用）
        if (internalRpcService != null) {
            internalRpcService.start();
            log.info("内部 gRPC 服务已启动 (port={})", clusterProperties.getInternalPort());
        }

        // 启动 TCP 服务（protocol=grpc 模式下禁用 TCP 外部监听）
        if (clusterProperties.isTcpEnabled()) {
            cacheServer.start();
        } else {
            log.info("protocol=grpc 模式，跳过 TCP 监听 (外部端口 {})", cachePortProperties.getPort());
        }

        if (distributedCache != null) {
            distributedCache.startHealthCheck();
        }
        // 启动 failover 探测调度
        if (failoverManager != null) {
            failoverManager.start();
        }
        // 启动 rebalance 扫描调度
        if (rebalanceManager != null) {
            rebalanceManager.start();
        }
    }

    /**
     * 构造回调过滤器：分布式模式下仅回放 self 节点作为 primary 的 key。
     * 非 primary 的 key（即 self 为 backup 或不在 owners 中）由其 primary 节点持有，
     * 本节点若回放将造成非 owner 数据残留，跨进程 stale read 等问题。
     */
    private java.util.function.Predicate<String> buildReplayFilter() {
        RendezvousHash hrw = distributedCache.getHash();
        int replicas = distributedCache.getReplicas();
        ClusterNode self = distributedCache.getSelf();
        return key -> {
            if (key == null || self == null) return false;
            List<ClusterNode> owners = hrw.selectNodes(key, replicas);
            return !owners.isEmpty() && owners.getFirst().equals(self);
        };
    }

    @PreDestroy
    public void destroy() {
        if (rebalanceManager != null) {
            rebalanceManager.stop();
        }
        if (failoverManager != null) {
            failoverManager.stop();
        }
        if (cacheServer != null) {
            cacheServer.stop();
        }
        if (distributedCache != null) {
            distributedCache.shutdown();
        }
        if (internalRpcService != null) {
            internalRpcService.shutdown();
            log.info("内部 gRPC 服务已关闭");
        }
        rawCache.stopEvictionScheduler();
        aofService.stop();
    }

    /**
     * 标记集群节点列表中哪个是本节点。
     * <p>缺陷16修复：改进节点识别，尝试多种地址匹配方式，避免仅按端口匹配的 fallback。
     */
    private void markSelfNode(List<ClusterNode> nodes, int localPort) {
        String localHost = getLocalHost();
        // 第一轮：精确匹配 host + port
        for (ClusterNode node : nodes) {
            if (node.getPort() == localPort && isLocalAddress(node.getHost(), localHost)) {
                node.setSelf(true);
                return;
            }
        }
        // 第二轮：通过 InetAddress 解析进行地址比对
        for (ClusterNode node : nodes) {
            if (node.getPort() != localPort) continue;
            try {
                java.net.InetAddress nodeAddr = java.net.InetAddress.getByName(node.getHost());
                if (nodeAddr.isLoopbackAddress() || nodeAddr.getHostAddress().equals(localHost)) {
                    node.setSelf(true);
                    return;
                }
                // 检查是否为本机任意网卡地址
                for (java.net.NetworkInterface ni : java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces())) {
                    for (java.net.InterfaceAddress ia : ni.getInterfaceAddresses()) {
                        if (ia.getAddress().getHostAddress().equals(nodeAddr.getHostAddress())) {
                            node.setSelf(true);
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("解析节点地址失败: {}", node.getHost(), e);
            }
        }
        // 第三轮：仅匹配 port（fallback，带警告）
        for (ClusterNode node : nodes) {
            if (node.getPort() == localPort) {
                log.warn("无法精确匹配本节点，使用 port-only fallback: {}", node);
                node.setSelf(true);
                return;
            }
        }
    }

    private static ClusterNode findSelf(List<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (node.isSelf()) return node;
        }
        return nodes.isEmpty() ? null : nodes.getFirst();
    }

    private static boolean isLocalAddress(String host, String localHost) {
        if ("127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host)) return true;
        if (host.equals(localHost)) return true;
        return false;
    }

    private static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
