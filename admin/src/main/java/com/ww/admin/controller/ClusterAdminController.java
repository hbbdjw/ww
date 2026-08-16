package com.ww.admin.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ww.admin.common.JsonResult;
import com.ww.admin.service.ClusterAdminService;
import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.FailoverManager;
import com.ww.cache.RebalanceManager;

/**
 * 集群管理面 HTTP API：查询集群状态、手动 failover、触发 rebalance、节点加入/退出。
 *
 * <p>所有依赖组件均为可选注入：未启用集群动态管理（cluster.membershipEnabled=false）时
 * 返回 JsonResult.warn 提示运维打开相应配置，而不是抛 500。
 *
 * <p>路由约定：所有端点都走 POST + JSON body（与 CacheController 风格一致）。
 */
@Controller
@RequestMapping("/cluster")
public class ClusterAdminController {

    private static final Logger log = LoggerFactory.getLogger(ClusterAdminController.class);

    @Autowired(required = false)
    private ClusterMembership membership;

    @Autowired(required = false)
    private FailoverManager failoverManager;

    @Autowired(required = false)
    private RebalanceManager rebalanceManager;

    @Autowired(required = false)
    private DistributedCache distributedCache;

    @Autowired(required = false)
    private ClusterAdminService clusterAdminService;

    // ==================== 查询类 ====================

    /**
     * 集群节点列表总览：节点信息 + 是否在线（TCP 实时探测）+ 每节点 CPU/内存指标。
     * <p>未启用动态成员管理时按静态 cluster.nodes 返回，仍可探测在线状态。
     */
    @PostMapping("/nodes")
    @ResponseBody
    public JsonResult nodes() {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        return JsonResult.success(clusterAdminService.nodesOverview());
    }

    /**
     * 本节点 CPU/内存利用率（JMX 快照）。
     * <p>集群总览（含各节点指标聚合）请调用 /cluster/nodes。
     */
    @PostMapping("/metrics")
    @ResponseBody
    public JsonResult metrics() {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        return JsonResult.success(clusterAdminService.localMetrics());
    }

    /**
     * 集群状态总览：拓扑版本、节点列表、failover 状态、rebalance 状态。
     * <p>供运维大盘展示。membership 未启用时降级为静态 cluster.nodes 视图
     * （返回 success + membershipEnabled=false，前端据此提示而非报错）。
     */
    @PostMapping("/status")
    @ResponseBody
    public JsonResult status() {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        return JsonResult.success(clusterAdminService.statusOverview());
    }

    /** 查询当前拓扑节点列表（精简视图，含 DEAD 节点；membership 未启用时退化为静态配置） */
    @PostMapping("/topology")
    @ResponseBody
    public JsonResult topology() {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        Map<String, Object> result = clusterAdminService.statusOverview();
        // topology 只需要拓扑部分，去掉 failover/rebalance 摘要
        result.remove("failover");
        result.remove("rebalance");
        result.remove("aliveCount");
        return JsonResult.success(result);
    }

    // ==================== Failover ====================

    /**
     * 手动触发 failover：立即把指定节点标记为 DEAD，路由重写由 DistributedCache 完成。
     * <p>请求体：{"nodeKey": "host:port"}
     */
    @PostMapping("/failover")
    @ResponseBody
    public JsonResult failover(@RequestBody Map<String, Object> map) {
        if (failoverManager == null) {
            return JsonResult.warn("Failover 管理未启用（cluster.failoverEnabled=false）");
        }
        String nodeKey = readString(map, "nodeKey");
        if (nodeKey == null || nodeKey.isEmpty()) {
            return JsonResult.warn("缺少参数 nodeKey");
        }
        try {
            boolean changed = failoverManager.manualFailover(nodeKey);
            // 触发 DistributedCache 重建 HRW，让后续请求立即跳过 DEAD 节点
            applyTopologySafe();
            return JsonResult.success(Map.of("nodeKey", nodeKey, "changed", changed));
        } catch (Exception e) {
            log.error("手动 failover 失败: nodeKey={}", nodeKey, e);
            return JsonResult.warn("failover 失败: " + e.getMessage());
        }
    }

    /**
     * 取消手动 failover：让节点恢复 ALIVE，重新纳入路由。
     * <p>请求体：{"nodeKey": "host:port"}
     */
    @PostMapping("/cancelFailover")
    @ResponseBody
    public JsonResult cancelFailover(@RequestBody Map<String, Object> map) {
        if (failoverManager == null) {
            return JsonResult.warn("Failover 管理未启用");
        }
        String nodeKey = readString(map, "nodeKey");
        if (nodeKey == null || nodeKey.isEmpty()) {
            return JsonResult.warn("缺少参数 nodeKey");
        }
        try {
            boolean changed = failoverManager.cancelManualFailover(nodeKey);
            applyTopologySafe();
            return JsonResult.success(Map.of("nodeKey", nodeKey, "changed", changed));
        } catch (Exception e) {
            log.error("取消 failover 失败: nodeKey={}", nodeKey, e);
            return JsonResult.warn("取消 failover 失败: " + e.getMessage());
        }
    }

    /**
     * 将源节点的 master（primary）角色转移到其他节点。
     * <p>请求体：{"sourceNodeKey": "host:port", "targetNodeKey": "host:port"}，targetNodeKey 可选。
     * <p>语义：源节点标记 DEAD（manual failover）→ 重建 HRW → 触发数据重平衡；
     * 源节点 key 的新 primary 由 HRW 在存活节点中确定。撤销请调用 /cluster/cancelFailover。
     */
    @PostMapping("/transferMaster")
    @ResponseBody
    public JsonResult transferMaster(@RequestBody Map<String, Object> map) {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        String sourceNodeKey = readString(map, "sourceNodeKey");
        if (sourceNodeKey == null || sourceNodeKey.isEmpty()) {
            return JsonResult.warn("缺少参数 sourceNodeKey");
        }
        String targetNodeKey = readString(map, "targetNodeKey");
        try {
            Map<String, Object> r = clusterAdminService.transferMaster(sourceNodeKey, targetNodeKey);
            if (r.containsKey("error")) {
                return JsonResult.warn(String.valueOf(r.get("error")));
            }
            return JsonResult.success(r);
        } catch (Exception e) {
            log.error("transferMaster 失败: source={}, target={}", sourceNodeKey, targetNodeKey, e);
            return JsonResult.warn("transferMaster 失败: " + e.getMessage());
        }
    }

    // ==================== Rebalance ====================

    /**
     * 立即触发一次重平衡：扫描本地 KV，把不属于本节点的 key 推送给新 primary。
     * <p>请求体：可空。返回本次迁移的 key 数量与统计信息。
     */
    @PostMapping("/rebalance")
    @ResponseBody
    public JsonResult rebalance(@RequestBody(required = false) Map<String, Object> map) {
        if (rebalanceManager == null) {
            return JsonResult.warn("Rebalance 管理未启用（cluster.rebalanceEnabled=false）");
        }
        try {
            long migrated = rebalanceManager.rebalanceNow();
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("migratedKeys", migrated);
            data.put("status", rebalanceManager.getStatus());
            return JsonResult.success(data);
        } catch (Exception e) {
            log.error("rebalance 失败", e);
            return JsonResult.warn("rebalance 失败: " + e.getMessage());
        }
    }

    // ==================== 节点加入/退出 ====================

    /**
     * 动态加入节点：将新节点纳入 membership 并触发拓扑重建。
     * <p>请求体：{"host": "...", "port": 1234, "weight": 1.0}
     * <p>加入后需调用 /cluster/applyTopology 让 HRW 重建。
     */
    @PostMapping("/join")
    @ResponseBody
    public JsonResult join(@RequestBody Map<String, Object> map) {
        if (membership == null) {
            return JsonResult.warn("集群动态成员管理未启用");
        }
        String host = readString(map, "host");
        if (host == null || host.isEmpty()) return JsonResult.warn("缺少参数 host");
        Integer port = readInt(map, "port");
        if (port == null) return JsonResult.warn("缺少参数 port");
        Double weight = readDouble(map, "weight");
        double w = weight != null ? weight : 1.0d;
        try {
            boolean changed = membership.addNode(host, port, w);
            boolean topoApplied = applyTopologySafe();
            return JsonResult.success(Map.of(
                    "nodeKey", host + ":" + port,
                    "added", changed,
                    "topologyApplied", topoApplied,
                    "topologyVersion", membership.getTopologyVersion()));
        } catch (Exception e) {
            log.error("节点加入失败: {}:{}", host, port, e);
            return JsonResult.warn("节点加入失败: " + e.getMessage());
        }
    }

    /**
     * 让节点退出：标记为 DEAD，从路由中排除。
     * <p>请求体：{"host": "...", "port": 1234}
     * <p>注意：不能让本节点退出。
     */
    @PostMapping("/leave")
    @ResponseBody
    public JsonResult leave(@RequestBody Map<String, Object> map) {
        if (membership == null) {
            return JsonResult.warn("集群动态成员管理未启用");
        }
        String host = readString(map, "host");
        if (host == null || host.isEmpty()) return JsonResult.warn("缺少参数 host");
        Integer port = readInt(map, "port");
        if (port == null) return JsonResult.warn("缺少参数 port");
        try {
            boolean changed = membership.removeNode(host, port);
            boolean topoApplied = applyTopologySafe();
            return JsonResult.success(Map.of(
                    "nodeKey", host + ":" + port,
                    "removed", changed,
                    "topologyApplied", topoApplied,
                    "topologyVersion", membership.getTopologyVersion()));
        } catch (Exception e) {
            log.error("节点退出失败: {}:{}", host, port, e);
            return JsonResult.warn("节点退出失败: " + e.getMessage());
        }
    }

    /**
     * 把节点标记为 SUSPECT（人工触发，不影响 epoch 但触发拓扑版本递增）。
     * <p>请求体：{"nodeKey": "host:port"}
     */
    @PostMapping("/suspect")
    @ResponseBody
    public JsonResult suspect(@RequestBody Map<String, Object> map) {
        if (membership == null) {
            return JsonResult.warn("集群动态成员管理未启用");
        }
        String nodeKey = readString(map, "nodeKey");
        if (nodeKey == null || nodeKey.isEmpty()) return JsonResult.warn("缺少参数 nodeKey");
        try {
            boolean changed = membership.markSuspect(nodeKey);
            return JsonResult.success(Map.of("nodeKey", nodeKey, "changed", changed,
                    "topologyVersion", membership.getTopologyVersion()));
        } catch (Exception e) {
            return JsonResult.warn("标记 SUSPECT 失败: " + e.getMessage());
        }
    }

    /**
     * 把节点恢复为 ALIVE（人工触发，例如已知节点恢复后立即纳入路由）。
     * <p>若该节点处于手动 failover（manualDead 标记会阻止自动探测恢复），
     * 同时清除标记，否则 markAlive 后节点仍不会被重新探测纳入路由。
     * <p>请求体：{"nodeKey": "host:port"}
     */
    @PostMapping("/alive")
    @ResponseBody
    public JsonResult alive(@RequestBody Map<String, Object> map) {
        if (membership == null) {
            return JsonResult.warn("集群动态成员管理未启用");
        }
        String nodeKey = readString(map, "nodeKey");
        if (nodeKey == null || nodeKey.isEmpty()) return JsonResult.warn("缺少参数 nodeKey");
        try {
            boolean cancelled = false;
            if (failoverManager != null) {
                cancelled = failoverManager.cancelManualFailover(nodeKey);
            }
            boolean changed = membership.markAlive(nodeKey);
            boolean topoApplied = applyTopologySafe();
            return JsonResult.success(Map.of("nodeKey", nodeKey, "changed", changed,
                    "manualFailoverCancelled", cancelled,
                    "topologyApplied", topoApplied,
                    "topologyVersion", membership.getTopologyVersion()));
        } catch (Exception e) {
            return JsonResult.warn("标记 ALIVE 失败: " + e.getMessage());
        }
    }

    // ==================== 显式应用拓扑 ====================

    /**
     * 切换节点存储/缓存能力标记。
     * <p>请求体：{"nodeKey": "host:port", "capability": "storage|cache", "enabled": true|false}
     */
    @PostMapping("/capability")
    @ResponseBody
    public JsonResult capability(@RequestBody Map<String, Object> map) {
        if (clusterAdminService == null) {
            return JsonResult.warn("集群管理服务未启用");
        }
        String nodeKey = readString(map, "nodeKey");
        if (nodeKey == null || nodeKey.isEmpty()) return JsonResult.warn("缺少参数 nodeKey");
        String capability = readString(map, "capability");
        if (capability == null || capability.isEmpty()) return JsonResult.warn("缺少参数 capability");
        Boolean enabled = map != null && map.get("enabled") instanceof Boolean b ? b : null;
        if (enabled == null) return JsonResult.warn("缺少参数 enabled");
        try {
            Map<String, Object> r = clusterAdminService.toggleNodeCapability(nodeKey, capability, enabled);
            if (r.containsKey("error")) {
                return JsonResult.warn(String.valueOf(r.get("error")));
            }
            return JsonResult.success(r);
        } catch (Exception e) {
            log.error("切换节点能力失败: nodeKey={}, capability={}", nodeKey, capability, e);
            return JsonResult.warn("切换节点能力失败: " + e.getMessage());
        }
    }

    // ==================== 显式应用拓扑 ====================

    /**
     * 显式应用 membership 当前视图到 DistributedCache：用 alive 节点重建 HRW。
     * <p>请求体：可空。
     * <p>通常 /join、/leave、/failover、/alive 已经自动调用，此接口供运维兜底。
     */
    @PostMapping("/applyTopology")
    @ResponseBody
    public JsonResult applyTopology(@RequestBody(required = false) Map<String, Object> map) {
        if (membership == null) {
            return JsonResult.warn("集群动态成员管理未启用");
        }
        if (distributedCache == null) {
            return JsonResult.warn("DistributedCache 未启用");
        }
        try {
            distributedCache.applyTopologyFromMembership();
            return JsonResult.success(Map.of(
                    "topologyVersion", membership.getTopologyVersion(),
                    "aliveCount", membership.aliveCount()));
        } catch (Exception e) {
            log.error("应用拓扑失败", e);
            return JsonResult.warn("应用拓扑失败: " + e.getMessage());
        }
    }

    // ==================== 内部工具 ====================

    /** 安全调用 DistributedCache.applyTopologyFromMembership，未启用时返回 false */
    private boolean applyTopologySafe() {
        if (distributedCache == null || membership == null) return false;
        try {
            distributedCache.applyTopologyFromMembership();
            return true;
        } catch (Exception e) {
            log.warn("applyTopologyFromMembership 失败: {}", e.getMessage());
            return false;
        }
    }

    private static String readString(Map<String, Object> map, String key) {
        if (map == null) return null;
        Object v = map.get(key);
        return v == null ? null : v.toString();
    }

    private static Integer readInt(Map<String, Object> map, String key) {
        if (map == null) return null;
        return switch (map.get(key)) {
            case null -> null;
            case Number n -> n.intValue();
            case String s -> {
                try { yield Integer.parseInt(s); } catch (NumberFormatException e) { yield null; }
            }
            default -> null;
        };
    }

    private static Double readDouble(Map<String, Object> map, String key) {
        if (map == null) return null;
        return switch (map.get(key)) {
            case null -> null;
            case Number n -> n.doubleValue();
            case String s -> {
                try { yield Double.parseDouble(s); } catch (NumberFormatException e) { yield null; }
            }
            default -> null;
        };
    }
}
