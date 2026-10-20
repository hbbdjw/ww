package com.ww.runtime.lb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 负载均衡集群级配置（单行，存 rt_lb_config，version 乐观锁）。
 *
 * <ul>
 *   <li>{@code enabled}：总开关。false 时代理过滤器直通（上游已有 LBS/Nginx 的形态）；</li>
 *   <li>{@code entryNodeId}：页面指定的唯一流量入口节点；启用时必填；</li>
 *   <li>{@code strategy}：分发策略（轮询/平滑权重/随机）；</li>
 *   <li>{@code selfServe}：入口节点自身是否参与处理部分请求；</li>
 *   <li>{@code pathPrefixes}：页面可配的代理路径前缀（Spring PathPattern 语法，
 *       如 {@code /cache/**}）；控制面/静态资源前缀被硬保护，拒绝保存；</li>
 *   <li>{@code weightOverrides}：按节点覆盖 LB 权重，留空（缺键）= 用节点注册权重；
 *       覆盖只影响 LB，不影响 placement；</li>
 *   <li>{@code configRefreshMs/nodeViewRefreshMs}：配置轮询与注册表节点视图刷新周期；</li>
 *   <li>{@code connectTimeoutMs/requestTimeoutMs}：反代 HttpClient 超时（请求总时长，
 *       默认 1h，对齐大文件上传下载/媒体流）；</li>
 *   <li>{@code version}：乐观锁版本，每次成功更新 +1。</li>
 * </ul>
 */
public record LbConfig(
        boolean enabled,
        String entryNodeId,
        LbStrategy strategy,
        boolean selfServe,
        List<String> pathPrefixes,
        Map<String, Double> weightOverrides,
        long configRefreshMs,
        long nodeViewRefreshMs,
        long connectTimeoutMs,
        long requestTimeoutMs,
        long version) {

    /** 精确保护项：即使页面配置也绝不允许代理。 */
    private static final Set<String> EXACT_PROTECTED = Set.of("/", "/index.html", "/favicon.ico");

    /** 保护前缀根：控制面/登录/静态资源。 */
    private static final List<String> PROTECTED_ROOTS = List.of("/rt", "/api", "/assets");

    public LbConfig {
        if (strategy == null) {
            throw new IllegalArgumentException("lb strategy must not be null");
        }
        if (enabled && (entryNodeId == null || entryNodeId.isBlank())) {
            throw new IllegalArgumentException("entryNodeId must not be blank when LB is enabled");
        }
        entryNodeId = entryNodeId == null ? null : entryNodeId.trim();

        List<String> normalizedPrefixes = new ArrayList<>();
        if (pathPrefixes != null) {
            for (String raw : pathPrefixes) {
                if (raw == null || raw.isBlank()) {
                    continue;
                }
                String prefix = raw.trim();
                if (!prefix.startsWith("/")) {
                    throw new IllegalArgumentException("path prefix must start with '/': " + prefix);
                }
                if (isProtectedPrefix(prefix)) {
                    throw new IllegalArgumentException("path prefix is protected and must not be proxied: " + prefix);
                }
                if (!normalizedPrefixes.contains(prefix)) {
                    normalizedPrefixes.add(prefix);
                }
            }
        }
        pathPrefixes = List.copyOf(normalizedPrefixes);

        TreeMap<String, Double> normalizedWeights = new TreeMap<>();
        if (weightOverrides != null) {
            for (Map.Entry<String, Double> e : weightOverrides.entrySet()) {
                String nodeId = e.getKey();
                Double w = e.getValue();
                if (nodeId == null || nodeId.isBlank()) {
                    throw new IllegalArgumentException("weight override nodeId must not be blank");
                }
                if (w == null || !Double.isFinite(w) || w <= 0) {
                    throw new IllegalArgumentException(
                            "weight override for " + nodeId + " must be positive finite, got " + w);
                }
                normalizedWeights.put(nodeId.trim(), w);
            }
        }
        weightOverrides = Map.copyOf(normalizedWeights);

        if (configRefreshMs <= 0) {
            throw new IllegalArgumentException("configRefreshMs must be > 0, got " + configRefreshMs);
        }
        if (nodeViewRefreshMs <= 0) {
            throw new IllegalArgumentException("nodeViewRefreshMs must be > 0, got " + nodeViewRefreshMs);
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be > 0, got " + connectTimeoutMs);
        }
        if (requestTimeoutMs <= 0) {
            throw new IllegalArgumentException("requestTimeoutMs must be > 0, got " + requestTimeoutMs);
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0, got " + version);
        }
    }

    /** 出厂默认：关闭；仅数据面前缀；轮询；入口自身参与；超时对齐 Tomcat 流式语义。 */
    public static LbConfig defaultConfig() {
        return new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**", "/storage/**"), Map.of(),
                3000L, 1000L, 3000L, 3_600_000L, 0L);
    }

    /** 供存储层在 CAS 成功后回填新版本。 */
    public LbConfig withVersion(long newVersion) {
        return new LbConfig(enabled, entryNodeId, strategy, selfServe, pathPrefixes, weightOverrides,
                configRefreshMs, nodeViewRefreshMs, connectTimeoutMs, requestTimeoutMs, newVersion);
    }

    /** 节点的 LB 有效权重：页面覆盖优先，否则取节点注册权重。 */
    public double effectiveWeight(String nodeId, double registeredWeight) {
        Double override = weightOverrides.get(nodeId);
        return override != null ? override : registeredWeight;
    }

    /**
     * 判断前缀是否命中硬保护清单（控制面/登录/静态资源/根路径），任何情况下都不允许代理。
     */
    public static boolean isProtectedPrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return true;
        }
        String p = prefix.trim().toLowerCase(Locale.ROOT);
        if (EXACT_PROTECTED.contains(p)) {
            return true;
        }
        for (String root : PROTECTED_ROOTS) {
            if (p.equals(root) || p.startsWith(root + "/")) {
                return true;
            }
        }
        return false;
    }

    /** 便于 Jackson/测试构造：忽略 version 的覆盖便捷方法。 */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("enabled", enabled);
        map.put("entryNodeId", entryNodeId);
        map.put("strategy", strategy.name());
        map.put("selfServe", selfServe);
        map.put("pathPrefixes", pathPrefixes);
        map.put("weightOverrides", weightOverrides);
        map.put("configRefreshMs", configRefreshMs);
        map.put("nodeViewRefreshMs", nodeViewRefreshMs);
        map.put("connectTimeoutMs", connectTimeoutMs);
        map.put("requestTimeoutMs", requestTimeoutMs);
        map.put("version", version);
        return map;
    }
}
