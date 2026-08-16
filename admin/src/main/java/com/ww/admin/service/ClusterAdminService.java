package com.ww.admin.service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.FailoverManager;
import com.ww.cache.RebalanceManager;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.common.RendezvousHash;

import jakarta.annotation.PreDestroy;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 集群管理服务：节点列表/在线探测、CPU 内存利用率采集、master 角色转移。
 *
 * <p>本服务供管理面 Controller（{@code /cluster/*}）使用。所有集群组件均为可选注入：
 * 未启用动态成员管理（cluster.membershipEnabled=false）时，节点列表退化为静态配置
 * （cluster.nodes），在线状态仍可通过 TCP 实时探测获得。
 *
 * <p>指标采集说明：
 * <ul>
 *   <li>{@link #localMetrics()} 仅返回本节点 JVM/系统指标（JMX 快照），不递归访问对端，
 *       避免集群内互相调用造成死循环；</li>
 *   <li>{@link #nodesOverview()} 汇总整个集群：在线探测用 TCP 直连（2s 超时），
 *       对端指标通过对端节点的 /cluster/metrics HTTP 端点 fan-out 获取；</li>
 * </ul>
 *
 * <p>对端 admin HTTP 端口推导与 storage 模块一致：优先 {@code cluster.nodes[].httpPort}，
 * 否则 {@code node.port + (server.port - cache.port)}（对称部署推导成立）。
 *
 * <p>master 角色转移语义：本系统按 key 通过 HRW 选出 primary（master），不存在全局单 master。
 * 因此"转移 master 角色"实现为协调式 failover：将源节点标记 DEAD（manual，禁止自动恢复）→
 * 重建 HRW（源节点的 key 由剩余存活节点中评分最高者接管）→ 触发数据重平衡迁移数据，
 * 并统计源节点 key 的新 primary 分布。撤销操作请调用 {@code /cluster/cancelFailover}
 * （仅恢复源节点 ALIVE，其角色在 HRW 规则下自动回到源节点，属预期行为）。
 */
@Service
public class ClusterAdminService {

    private static final Logger log = LoggerFactory.getLogger(ClusterAdminService.class);

    /** 在线探测 TCP 连接超时（毫秒） */
    private static final int PROBE_TIMEOUT_MS = 2000;
    /** 对端指标 HTTP 请求超时（毫秒） */
    private static final int PEER_METRICS_TIMEOUT_MS = 3000;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(PEER_METRICS_TIMEOUT_MS))
            .build();

    @Autowired(required = false)
    private ClusterMembership membership;

    @Autowired(required = false)
    private FailoverManager failoverManager;

    @Autowired(required = false)
    private RebalanceManager rebalanceManager;

    @Autowired(required = false)
    private DistributedCache distributedCache;

    @Autowired
    private CachePortProperties cachePortProperties;

    @Autowired
    private ClusterProperties clusterProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${server.port:8080}")
    private int serverPort;

    private volatile ExecutorService ioPool;

    // ==================== 节点列表 + 在线状态 ====================

    /**
     * 集群状态总览：拓扑版本、存活数、节点列表、failover/rebalance 状态。
     * <p>与 {@link #nodesOverview()} 的区别：不做 TCP 探测与指标采集，代价低，适合高频调用（SSE 周期推送）。
     * <p>membership 未启用时降级为静态 cluster.nodes 视图，并返回 {@code membershipEnabled=false}
     * 供前端提示（而非 warn 响应导致前端报错）。
     */
    public Map<String, Object> statusOverview() {
        List<ClusterNode> nodes = currentNodeList();
        Map<String, Object> result = new LinkedHashMap<>();
        boolean membershipEnabled = membership != null;
        result.put("membershipEnabled", membershipEnabled);
        result.put("topologyVersion", membershipEnabled ? membership.getTopologyVersion() : 0L);
        result.put("aliveCount", membershipEnabled ? membership.aliveCount() : nodes.size());
        String selfKey = membershipEnabled ? membership.getSelfNodeKey() : findSelfKey(nodes);
        result.put("selfNodeKey", selfKey != null ? selfKey : "");

        List<Map<String, Object>> nodeMaps = new ArrayList<>();
        for (ClusterNode n : nodes) {
            nodeMaps.add(nodeToMap(n));
        }
        result.put("nodes", nodeMaps);

        if (failoverManager != null) {
            result.put("failover", failoverManager.getStatus());
        } else {
            result.put("failover", Map.of("enabled", false));
        }
        if (rebalanceManager != null) {
            result.put("rebalance", rebalanceManager.getStatus());
        } else {
            result.put("rebalance", Map.of("enabled", false));
        }
        return result;
    }

    /**
     * 集群节点总览：节点列表 + 在线状态（TCP 实时探测）+ 每节点 CPU/内存指标。
     * <p>在线探测与指标采集并行执行，单节点失败不影响其他节点。
     */
    public Map<String, Object> nodesOverview() {
        List<ClusterNode> nodes = currentNodeList();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", System.currentTimeMillis());
        result.put("topologyVersion", membership != null ? membership.getTopologyVersion() : 0L);
        result.put("selfNodeKey", membership != null ? membership.getSelfNodeKey() : findSelfKey(nodes));

        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        for (ClusterNode n : nodes) {
            futures.add(ioPool().submit(() -> buildNodeView(n)));
        }
        List<Map<String, Object>> views = new ArrayList<>();
        for (Future<Map<String, Object>> f : futures) {
            try {
                views.add(f.get(8, TimeUnit.SECONDS));
            } catch (Exception e) {
                views.add(Map.of("error", "节点视图采集失败: " + e.getMessage()));
            }
        }
        result.put("nodes", views);
        return result;
    }

    /** 构建单节点视图：基础信息 + 在线状态 + 指标 */
    private Map<String, Object> buildNodeView(ClusterNode n) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("nodeKey", n.nodeKey());
        view.put("host", n.getHost());
        view.put("port", n.getPort());
        view.put("weight", n.getWeight());
        view.put("self", n.isSelf());
        view.put("status", n.getStatus().name());
        view.put("epoch", n.getEpoch());
        view.put("lastHeartbeatMs", n.getLastHeartbeatMs());
        view.put("storageEnabled", n.isStorageEnabled());
        view.put("cacheEnabled", n.isCacheEnabled());
        if (n.isSelf()) {
            view.put("online", true);
            view.put("latencyMs", 0);
            view.put("metrics", localMetrics());
        } else {
            long t0 = System.nanoTime();
            boolean online = tcpReachable(n.getHost(), n.getPort());
            view.put("online", online);
            view.put("latencyMs", online ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) : -1);
            view.put("metrics", online ? fetchPeerMetrics(n) : Map.of("error", "节点不可达，无法采集指标"));
        }
        return view;
    }

    /** TCP 直连探测节点是否在线（2s 超时） */
    private boolean tcpReachable(String host, int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), PROBE_TIMEOUT_MS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** 获取对端节点本机指标（POST {host}:{adminHttpPort}/cluster/metrics） */
    private Map<String, Object> fetchPeerMetrics(ClusterNode n) {
        int port = adminHttpPort(n);
        String url = "http://" + n.getHost() + ":" + port + "/cluster/metrics";
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(PEER_METRICS_TIMEOUT_MS))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode root = objectMapper.readTree(resp.body());
            JsonNode data = root.get("data");
            if (data != null && data.isObject()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = objectMapper.convertValue(data, Map.class);
                m.put("_source", url);
                return m;
            }
            return Map.of("error", "对端指标 data 缺失: " + root);
        } catch (Exception e) {
            log.debug("获取对端指标失败 {}: {}", url, e.getMessage());
            return Map.of("error", "无法获取对端指标: " + e.getMessage());
        }
    }

    // ==================== CPU / 内存利用率 ====================

    /**
     * 本节点 CPU/内存利用率（JMX 快照）。
     * <p>仅返回本机数据，供管理面直接查询与对端节点 fan-out 使用（不做递归采集）。
     */
    public Map<String, Object> localMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeKey", findSelfKey(currentNodeList()));
        m.put("timestamp", System.currentTimeMillis());

        // CPU
        com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        Map<String, Object> cpu = new LinkedHashMap<>();
        cpu.put("availableProcessors", os.getAvailableProcessors());
        cpu.put("systemCpuLoad", round2(os.getSystemCpuLoad()));
        cpu.put("processCpuLoad", round2(os.getProcessCpuLoad()));
        cpu.put("systemLoadAverage", round2(os.getSystemLoadAverage()));
        m.put("cpu", cpu);

        // 内存
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mem.getHeapMemoryUsage();
        Map<String, Object> memory = new LinkedHashMap<>();
        memory.put("heapUsedMb", toMb(heap.getUsed()));
        memory.put("heapCommittedMb", toMb(heap.getCommitted()));
        memory.put("heapMaxMb", toMb(heap.getMax()));
        memory.put("heapUsedPercent", heapPercent(heap));
        memory.put("nonHeapUsedMb", toMb(mem.getNonHeapMemoryUsage().getUsed()));
        long totalMem = os.getTotalMemorySize();
        long freeMem = os.getFreeMemorySize();
        memory.put("systemTotalMb", toMb(totalMem));
        memory.put("systemFreeMb", toMb(freeMem));
        memory.put("systemUsedPercent",
                totalMem > 0 ? round2((totalMem - freeMem) * 100.0 / totalMem) : -1);
        m.put("memory", memory);

        // JVM
        Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("uptimeSec", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        jvm.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        m.put("jvm", jvm);

        // 缓存本地视图（分布式下仅本节点数据）
        Map<String, Object> cache = new LinkedHashMap<>();
        if (distributedCache != null && distributedCache.getLocalCache() != null) {
            cache.put("keyCount", distributedCache.getLocalCache().size());
            cache.put("backlogSizes", distributedCache.getBacklogSizes());
        }
        m.put("cache", cache);
        return m;
    }

    // ==================== master 角色转移 ====================

    /**
     * 将源节点的 master（primary）角色转移到其他节点。
     * <p>实现：源节点标记 DEAD（manual failover，禁止自动恢复）→ 重建 HRW →
     * 触发数据重平衡 → 统计源节点 key 的新 primary 分布（基于本节点缓存快照）。
     *
     * @param sourceNodeKey 源节点 nodeKey（其 master 角色被转移走），必填
     * @param targetNodeKey 目标节点 nodeKey（期望接管角色），可选；最终接管者由 HRW 在存活节点中确定
     * @return 转移结果；若含 {@code error} 键表示校验失败（由调用方转为 warn 响应）
     */
    public Map<String, Object> transferMaster(String sourceNodeKey, String targetNodeKey) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (membership == null) {
            result.put("error", "集群动态成员管理未启用（cluster.membershipEnabled=false）");
            return result;
        }
        if (failoverManager == null) {
            result.put("error", "Failover 管理未启用（cluster.failoverEnabled=false）");
            return result;
        }
        ClusterNode source = membership.getNode(sourceNodeKey);
        if (source == null) {
            result.put("error", "源节点不在 membership 中: " + sourceNodeKey);
            return result;
        }
        if (source.isSelf()) {
            result.put("error", "不能转移自身节点的 master 角色");
            return result;
        }
        if (targetNodeKey != null && !targetNodeKey.isBlank()) {
            ClusterNode target = membership.getNode(targetNodeKey);
            if (target == null) {
                result.put("error", "目标节点不在 membership 中: " + targetNodeKey);
                return result;
            }
            if (target.nodeKey().equals(source.nodeKey())) {
                result.put("error", "源节点与目标节点不能相同");
                return result;
            }
            if (!target.isAlive()) {
                result.put("error", "目标节点不在线，无法接管 master 角色: " + targetNodeKey);
                return result;
            }
        }

        // 0. 转移前：统计本节点缓存中由源节点担任 primary 的 key（HRW 快照）
        List<String> sourceKeys = keysOwnedBy(sourceNodeKey);

        // 1. 标记源节点 DEAD（manual failover 语义：不会自动恢复，需 cancelFailover 撤销）
        failoverManager.manualFailover(sourceNodeKey);

        // 2. 重建 HRW，让源节点的 key 由存活节点接管
        distributedCache.applyTopologyFromMembership();

        // 3. 触发一次数据重平衡（将本节点上不再由本节点担任 primary 的 key 推给新 primary）
        long migrated = -1;
        if (rebalanceManager != null) {
            try {
                migrated = rebalanceManager.rebalanceNow();
            } catch (Exception e) {
                log.error("transferMaster 触发重平衡失败", e);
                migrated = -1;
            }
        }

        // 4. 统计源节点 key 的新 primary 分布
        Map<String, Long> newPrimaryCount = new LinkedHashMap<>();
        long targetTookOver = 0;
        RendezvousHash hrw = distributedCache.getHash();
        int replicas = distributedCache.getReplicas();
        for (String key : sourceKeys) {
            List<ClusterNode> owners = hrw.selectNodes(key, replicas);
            if (owners.isEmpty()) continue;
            String primaryKey = owners.getFirst().nodeKey();
            newPrimaryCount.merge(primaryKey, 1L, Long::sum);
            if (primaryKey.equals(targetNodeKey)) targetTookOver++;
        }

        result.put("sourceNodeKey", sourceNodeKey);
        result.put("targetNodeKey", targetNodeKey != null && !targetNodeKey.isBlank() ? targetNodeKey : null);
        result.put("transferred", true);
        result.put("topologyVersion", membership.getTopologyVersion());
        result.put("aliveCount", membership.aliveCount());
        result.put("migratedKeys", migrated);
        result.put("sourceKeysHeldLocally", sourceKeys.size());
        result.put("newPrimaryDistribution", newPrimaryCount);
        result.put("targetTookOverKeys", targetTookOver);
        result.put("note", "接管者由 HRW 在存活节点中确定；节点间经 gossip 传播（heartbeatInterval），"
                + "数据经各节点 rebalance 扫描迁移，撤销请调用 /cluster/cancelFailover");
        return result;
    }

    /** 统计本节点缓存中、转移前由指定节点担任 primary 的 key 列表 */
    private List<String> keysOwnedBy(String nodeKey) {
        List<String> keys = new ArrayList<>();
        if (distributedCache == null || distributedCache.getLocalCache() == null) return keys;
        RendezvousHash hrw = distributedCache.getHash();
        int replicas = distributedCache.getReplicas();
        for (String key : distributedCache.getLocalCache().keys()) {
            List<ClusterNode> owners = hrw.selectNodes(key, replicas);
            if (!owners.isEmpty() && owners.getFirst().nodeKey().equals(nodeKey)) {
                keys.add(key);
            }
        }
        return keys;
    }

    // ==================== 节点能力开关 ====================

    /**
     * 切换节点存储/缓存能力标记。
     *
     * @param nodeKey     目标节点 nodeKey
     * @param capability  能力名称："storage" 或 "cache"
     * @param enabled     是否启用
     * @return 切换结果；若含 {@code error} 键表示校验失败
     */
    public Map<String, Object> toggleNodeCapability(String nodeKey, String capability, boolean enabled) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (membership == null) {
            result.put("error", "集群动态成员管理未启用（cluster.membershipEnabled=false）");
            return result;
        }
        ClusterNode node = membership.getNode(nodeKey);
        if (node == null) {
            result.put("error", "节点不在 membership 中: " + nodeKey);
            return result;
        }
        switch (capability) {
            case "storage" -> node.setStorageEnabled(enabled);
            case "cache" -> node.setCacheEnabled(enabled);
            default -> {
                result.put("error", "未知能力类型: " + capability + "，支持 storage / cache");
                return result;
            }
        }
        result.put("nodeKey", nodeKey);
        result.put("capability", capability);
        result.put("enabled", enabled);
        result.put("topologyVersion", membership.getTopologyVersion());
        log.info("节点能力切换: nodeKey={}, capability={}, enabled={}", nodeKey, capability, enabled);
        return result;
    }

    // ==================== 内部工具 ====================

    /** 当前节点列表：优先动态 membership，退化为静态配置 */
    private List<ClusterNode> currentNodeList() {
        if (membership != null) return membership.getAllNodes();
        if (clusterProperties != null
                && clusterProperties.getNodes() != null
                && !clusterProperties.getNodes().isEmpty()) {
            return clusterProperties.getNodes();
        }
        return List.of();
    }

    /** 找到本节点 nodeKey；无 self 标记时返回 null */
    private static String findSelfKey(List<ClusterNode> nodes) {
        for (ClusterNode n : nodes) {
            if (n.isSelf()) return n.nodeKey();
        }
        return null;
    }

    /** ClusterNode → 管理面展示 Map（含状态/epoch/心跳/能力标记） */
    public static Map<String, Object> nodeToMap(ClusterNode n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeKey", n.nodeKey());
        m.put("host", n.getHost());
        m.put("port", n.getPort());
        m.put("weight", n.getWeight());
        m.put("self", n.isSelf());
        m.put("status", n.getStatus().name());
        m.put("epoch", n.getEpoch());
        m.put("lastHeartbeatMs", n.getLastHeartbeatMs());
        m.put("storageEnabled", n.isStorageEnabled());
        m.put("cacheEnabled", n.isCacheEnabled());
        return m;
    }

    /** 对端 admin HTTP 端口推导：优先显式 httpPort，否则 node.port + (server.port - cache.port) */
    private int adminHttpPort(ClusterNode n) {
        if (n.getHttpPort() > 0) return n.getHttpPort();
        int cachePort = cachePortProperties != null ? cachePortProperties.getPort() : 0;
        int offset = (cachePort > 0 && serverPort > 0) ? serverPort - cachePort : 0;
        int port = n.getPort() + offset;
        return port > 0 ? port : serverPort;
    }

    /** 懒加载 IO 线程池（探测/采集对端指标用，JDK 21 虚拟线程承载高并发 HTTP 调用） */
    private ExecutorService ioPool() {
        ExecutorService p = ioPool;
        if (p == null) {
            synchronized (this) {
                if (ioPool == null) {
                    ioPool = Executors.newVirtualThreadPerTaskExecutor();
                }
                p = ioPool;
            }
        }
        return p;
    }

    @PreDestroy
    public void shutdown() {
        ExecutorService p = ioPool;
        if (p != null) p.shutdownNow();
    }

    private static long toMb(long bytes) {
        return bytes < 0 ? -1 : bytes / (1024 * 1024);
    }

    private static int heapPercent(MemoryUsage heap) {
        long denominator = heap.getMax() > 0 ? heap.getMax() : heap.getCommitted();
        if (denominator <= 0) return -1;
        return (int) Math.round(heap.getUsed() * 100.0 / denominator);
    }

    /** 保留 2 位小数；负数（不可用）原样返回 */
    private static double round2(double v) {
        if (v < 0) return v;
        return Math.round(v * 100.0) / 100.0;
    }
}
