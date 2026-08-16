package com.ww.admin.service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ww.cache.Cache;
import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.SimpleCache;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;

import jakarta.annotation.PreDestroy;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 缓存管理服务：为管理面（/cache/admin/*）提供类 Redis 的缓存浏览能力。
 *
 * <p>核心能力：
 * <ul>
 *   <li>节点总览：集群节点列表 + 在线状态 + 每节点本地统计（keyCount/CPU/堆内存），
 *       对端节点经 HTTP fan-out（/internal/cache/localStats）采集；</li>
 *   <li>Key 列表：按节点聚合本地 key（/internal/cache/localKeys fan-out），支持关键字过滤；</li>
 *   <li>Value 读取：定位持有节点后读取本地值与剩余 TTL；</li>
 *   <li>写入/删除：走 {@link Cache}（集群模式下 DistributedCache 自动 HRW 路由 + 副本复制）。</li>
 * </ul>
 *
 * <p>对端 admin HTTP 端口推导与集群管理一致：优先 {@code cluster.nodes[].httpPort}，
 * 否则 {@code node.port + (server.port - cache.port)}。节点间调用携带
 * {@code X-Cache-Token}（取值 {@code cache.auth-token}，为空不校验），
 * 对应端点挂载在 Shiro 放行的 /internal/** 路径下。
 */
@Service
public class CacheAdminService {

    private static final Logger log = LoggerFactory.getLogger(CacheAdminService.class);

    /** 对端内部 HTTP 请求超时（毫秒） */
    private static final int PEER_TIMEOUT_MS = 3000;
    /** 节点间认证请求头 */
    private static final String TOKEN_HEADER = "X-Cache-Token";

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(PEER_TIMEOUT_MS))
            .build();

    @Autowired(required = false)
    private ClusterMembership membership;

    @Autowired(required = false)
    private DistributedCache distributedCache;

    @Autowired
    private Cache<String, Object> cache;

    @Autowired
    private SimpleCache<String, Object> localSimpleCache;

    @Autowired
    private CachePortProperties cachePortProperties;

    @Autowired
    private ClusterProperties clusterProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${server.port:8080}")
    private int serverPort;

    private volatile ExecutorService ioPool;

    // ==================== 节点总览 ====================

    /**
     * 缓存节点总览：节点列表 + 在线状态 + 每节点本地统计。
     * <p>对端统计经 /internal/cache/localStats fan-out 获取，采集成功即视为在线；
     * 单节点失败不影响其他节点。
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
        long totalKeyCount = 0;
        int onlineCount = 0;
        for (Future<Map<String, Object>> f : futures) {
            try {
                Map<String, Object> view = f.get(8, TimeUnit.SECONDS);
                views.add(view);
                if (Boolean.TRUE.equals(view.get("online"))) {
                    onlineCount++;
                    Object stats = view.get("stats");
                    if (stats instanceof Map<?, ?> m && m.get("keyCount") instanceof Number kc) {
                        totalKeyCount += kc.longValue();
                    }
                }
            } catch (Exception e) {
                views.add(Map.of("online", false, "error", "节点视图采集失败: " + e.getMessage()));
            }
        }
        result.put("nodes", views);
        result.put("onlineCount", onlineCount);
        result.put("totalKeyCount", totalKeyCount);
        return result;
    }

    /** 构建单节点视图：基础信息 + 在线状态 + 本地统计 */
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
        if (n.isSelf()) {
            view.put("online", true);
            view.put("stats", localStats());
        } else {
            Map<String, Object> stats = fetchPeerStats(n);
            boolean online = stats != null;
            view.put("online", online);
            view.put("stats", online ? stats : null);
            if (!online) {
                view.put("error", "节点不可达");
            }
        }
        return view;
    }

    /**
     * 本节点本地统计（JMX 快照 + 本地缓存视图）。
     * <p>仅返回本机数据，供管理面直接查询与对端节点 fan-out 使用（不做递归采集）。
     */
    public Map<String, Object> localStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("keyCount", localSimpleCache.size());

        com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        m.put("processCpuLoad", round2(os.getProcessCpuLoad()));
        m.put("systemCpuLoad", round2(os.getSystemCpuLoad()));

        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        m.put("heapUsedMb", toMb(heap.getUsed()));
        m.put("heapMaxMb", toMb(heap.getMax()));
        m.put("heapUsedPercent", heap.getMax() > 0
                ? round2(heap.getUsed() * 100.0 / heap.getMax()) : -1);

        m.put("uptimeSec", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        m.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        return m;
    }

    // ==================== Key 列表 ====================

    /**
     * 聚合 key 列表：nodeKey 为空聚合全部节点，否则仅查指定节点。
     * <p>每条记录附带持有节点 nodeKey；支持关键字过滤（大小写不敏感包含匹配），
     * 结果按 key 字典序排序并截断到 limit。
     *
     * @return {keys: [{key, nodeKey}], total: 过滤后总数, truncated: 是否截断}
     */
    public Map<String, Object> listKeys(String nodeKey, String keyword, int limit) {
        List<ClusterNode> all = currentNodeList();
        List<ClusterNode> targets = new ArrayList<>();
        for (ClusterNode n : all) {
            if (nodeKey == null || nodeKey.isEmpty() || n.nodeKey().equals(nodeKey)) {
                targets.add(n);
            }
        }
        if (nodeKey != null && !nodeKey.isEmpty() && targets.isEmpty()) {
            return Map.of("keys", List.of(), "total", 0, "truncated", false,
                    "error", "未知节点: " + nodeKey);
        }

        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>();
        for (ClusterNode n : targets) {
            futures.add(ioPool().submit(() -> fetchNodeKeys(n)));
        }
        List<Map<String, Object>> entries = new ArrayList<>();
        for (Future<List<Map<String, Object>>> f : futures) {
            try {
                entries.addAll(f.get(8, TimeUnit.SECONDS));
            } catch (Exception e) {
                log.warn("聚合 key 列表失败: {}", e.getMessage());
            }
        }

        String kw = keyword == null ? "" : keyword.trim().toLowerCase();
        if (!kw.isEmpty()) {
            entries.removeIf(e -> !((String) e.get("key")).toLowerCase().contains(kw));
        }
        entries.sort(Comparator.comparing(e -> (String) e.get("key")));

        int total = entries.size();
        int cap = limit > 0 ? limit : 2000;
        boolean truncated = total > cap;
        if (truncated) {
            entries = entries.subList(0, cap);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("keys", entries);
        result.put("total", total);
        result.put("truncated", truncated);
        return result;
    }

    /** 本节点本地 key 列表（供内部端点与本机聚合使用） */
    public List<String> localKeys() {
        Set<String> keys = localSimpleCache.keys();
        List<String> sorted = new ArrayList<>(keys);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

    /** 获取单节点的 key 列表：self 直接本地读取，对端 fan-out /internal/cache/localKeys */
    private List<Map<String, Object>> fetchNodeKeys(ClusterNode n) {
        List<String> keys;
        if (n.isSelf()) {
            keys = localKeys();
        } else {
            Object data = fetchPeerData(n, "/internal/cache/localKeys", "GET", null);
            if (!(data instanceof List<?> list)) return List.of();
            keys = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o != null) keys.add(o.toString());
            }
        }
        List<Map<String, Object>> entries = new ArrayList<>(keys.size());
        for (String k : keys) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("key", k);
            e.put("nodeKey", n.nodeKey());
            entries.add(e);
        }
        return entries;
    }

    // ==================== Value 读取 ====================

    /**
     * 读取 key 的值与剩余 TTL：优先在指定节点查找，未指定则扫描全部节点。
     * <p>首个 exists=true 的结果即返回（副本可能同时持有，取先响应者）。
     *
     * @return {exists, key, value, ttlMillis, nodeKey}；ttlMillis 语义：
     *         -1 不存在 / Long.MAX_VALUE 永不过期 / &gt;=0 剩余毫秒
     */
    public Map<String, Object> getValue(String key, String nodeKey) {
        List<ClusterNode> all = currentNodeList();
        List<ClusterNode> targets = new ArrayList<>();
        for (ClusterNode n : all) {
            if (nodeKey == null || nodeKey.isEmpty() || n.nodeKey().equals(nodeKey)) {
                targets.add(n);
            }
        }
        // 指定节点不存在时兜底扫全部节点
        if (targets.isEmpty()) {
            targets.addAll(all);
        }

        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        for (ClusterNode n : targets) {
            futures.add(ioPool().submit(() -> fetchNodeValue(n, key)));
        }
        for (Future<Map<String, Object>> f : futures) {
            try {
                Map<String, Object> v = f.get(8, TimeUnit.SECONDS);
                if (v != null && Boolean.TRUE.equals(v.get("exists"))) {
                    return v;
                }
            } catch (Exception e) {
                log.warn("读取 key 失败: key={}, {}", key, e.getMessage());
            }
        }
        Map<String, Object> miss = new LinkedHashMap<>();
        miss.put("exists", false);
        miss.put("key", key);
        miss.put("value", null);
        miss.put("ttlMillis", -1L);
        miss.put("nodeKey", null);
        return miss;
    }

    /** 本节点本地读取 key 的值与 TTL（供内部端点与本机查询使用） */
    public Map<String, Object> localValue(String key) {
        Object v = localSimpleCache.get(key);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("key", key);
        r.put("exists", v != null);
        r.put("value", v);
        r.put("ttlMillis", v != null ? localSimpleCache.remainingTtlMillis(key) : -1L);
        return r;
    }

    /** 获取单节点上的 key 值：self 直接本地读取，对端 fan-out /internal/cache/localValue */
    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchNodeValue(ClusterNode n, String key) {
        if (n.isSelf()) {
            Map<String, Object> v = localValue(key);
            v.put("nodeKey", n.nodeKey());
            return v;
        }
        Object data = fetchPeerData(n, "/internal/cache/localValue", "POST", Map.of("key", key));
        if (!(data instanceof Map<?, ?> m)) return null;
        Map<String, Object> v = new LinkedHashMap<>((Map<String, Object>) m);
        v.put("nodeKey", n.nodeKey());
        return v;
    }

    // ==================== 写入 / 删除 ====================

    /**
     * 写入 key-value：value 先尝试按 JSON 解析（对象/数组/数字/布尔），失败则保留原始字符串。
     * <p>走 {@link Cache} 接口：集群模式下由 DistributedCache 完成 HRW 路由与副本复制。
     *
     * @param ttlMillis 过期时间（毫秒），&lt;=0 表示永不过期
     */
    public void saveValue(String key, String rawValue, long ttlMillis) {
        Object value = parseValue(rawValue);
        if (ttlMillis > 0) {
            cache.put(key, value, ttlMillis);
        } else {
            cache.put(key, value);
        }
    }

    /** 删除 key：集群模式下由 DistributedCache 路由到主节点并同步副本 */
    public void deleteKey(String key) {
        cache.remove(key);
    }

    /** 尝试将原始字符串解析为 JSON 结构，失败返回原字符串 */
    private Object parseValue(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return "";
        char c = s.charAt(0);
        // 仅对明显是 JSON 结构的文本尝试解析，避免把普通字符串误转成数字/布尔
        if (c == '{' || c == '[' || c == '"'
                || s.equals("true") || s.equals("false") || s.equals("null")
                || looksNumeric(s)) {
            try {
                return objectMapper.readValue(s, Object.class);
            } catch (Exception ignored) {
                // 解析失败按字符串处理
            }
        }
        return raw;
    }

    private static boolean looksNumeric(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ==================== 节点间 HTTP fan-out ====================

    /** 获取对端节点本地统计（GET /internal/cache/localStats），失败返回 null */
    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchPeerStats(ClusterNode n) {
        Object data = fetchPeerData(n, "/internal/cache/localStats", "GET", null);
        return data instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
    }

    /**
     * 调用对端节点内部端点并返回 JsonResult.data；请求失败/超时/非 success 返回 null。
     */
    private Object fetchPeerData(ClusterNode n, String path, String method, Map<String, Object> body) {
        String url = "http://" + n.getHost() + ":" + adminHttpPort(n) + path;
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(PEER_TIMEOUT_MS))
                    .header("Content-Type", "application/json");
            String token = cachePortProperties.getAuthToken();
            if (token != null && !token.isEmpty()) {
                builder.header(TOKEN_HEADER, token);
            }
            if ("POST".equalsIgnoreCase(method)) {
                String json = body == null ? "{}" : objectMapper.writeValueAsString(body);
                builder.POST(HttpRequest.BodyPublishers.ofString(json));
            } else {
                builder.GET();
            }
            HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode root = objectMapper.readTree(resp.body());
            JsonNode type = root.get("type");
            if (type == null || !"success".equals(type.asString())) {
                log.debug("对端内部接口非 success 响应 {}: {}", url, resp.body());
                return null;
            }
            JsonNode data = root.get("data");
            if (data == null || data.isNull()) return null;
            if (data.isObject()) return objectMapper.convertValue(data, Map.class);
            if (data.isArray()) return objectMapper.convertValue(data, List.class);
            return data.asString();
        } catch (Exception e) {
            log.debug("调用对端内部接口失败 {}: {}", url, e.getMessage());
            return null;
        }
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

    /** 对端 admin HTTP 端口推导：优先显式 httpPort，否则 node.port + (server.port - cache.port) */
    private int adminHttpPort(ClusterNode n) {
        if (n.getHttpPort() > 0) return n.getHttpPort();
        int cachePort = cachePortProperties != null ? cachePortProperties.getPort() : 0;
        int offset = (cachePort > 0 && serverPort > 0) ? serverPort - cachePort : 0;
        int port = n.getPort() + offset;
        return port > 0 ? port : serverPort;
    }

    /** 懒加载 IO 线程池（fan-out 对端节点用，JDK 21 虚拟线程承载高并发 HTTP 调用） */
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

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static long toMb(long bytes) {
        return bytes / (1024 * 1024);
    }

    @PreDestroy
    public void destroy() {
        ExecutorService p = ioPool;
        if (p != null) {
            p.shutdownNow();
        }
    }
}
