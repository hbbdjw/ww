package com.ww.admin.controller;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.ww.admin.common.JsonResult;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.storage.DistributedStorage;
import com.ww.storage.StorageException;
import com.ww.storage.StorageFailoverManager;
import com.ww.storage.StorageMigrationService;
import com.ww.storage.StorageNode;
import com.ww.storage.StorageTopologyManager;

/**
 * 存储集群管理 API：动态扩缩容。
 *
 * <h3>操作流程</h3>
 * <pre>
 * 1. PUT  /storage/cluster/maintenance?enabled=true   挂维护牌（拒客户端读写）
 * 2. POST /storage/cluster/scale-out | transfer | merge  执行拓扑变更 + 数据迁移
 * 3. PUT  /storage/cluster/maintenance?enabled=false  解除维护态
 * </pre>
 *
 * <h3>一致性保证</h3>
 * 维护态期间拒绝客户端读写，保证 read-after-write。
 * 内部复制与迁移端点不受维护态影响，迁移可正常读取源节点数据并写入新主。
 *
 * <p>所有 API 均从任一存活节点发起，拓扑变更会广播到全部对端。
 */
@RestController
@RequestMapping("/storage/cluster")
public class StorageClusterController {

    private static final Logger log = LoggerFactory.getLogger(StorageClusterController.class);

    @Autowired
    private DistributedStorage storage;

    @Autowired(required = false)
    private StorageTopologyManager topologyManager;

    @Autowired(required = false)
    private StorageMigrationService migrationService;

    @Autowired(required = false)
    private StorageFailoverManager failoverManager;

    @Autowired(required = false)
    private ClusterProperties clusterProperties;

    @Autowired
    private CachePortProperties cachePortProperties;

    @Value("${server.port:8080}")
    private int serverPort;

    // ==================== 维护态 ====================

    /**
     * 设置维护态并广播到全集群。
     * <p>维护态下 {@link StorageController} 拒绝所有客户端读写。
     */
    @PutMapping("/maintenance")
    @ResponseBody
    public JsonResult setMaintenance(@RequestParam boolean enabled) {
        try {
            storage.setMaintenance(enabled);
            if (topologyManager != null) {
                topologyManager.broadcastMaintenance(enabled);
            }
            return JsonResult.success("维护态: " + enabled);
        } catch (Exception e) {
            log.error("设置维护态失败", e);
            return JsonResult.error(e.getMessage());
        }
    }

    @GetMapping("/maintenance")
    @ResponseBody
    public JsonResult getMaintenance() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("maintenance", storage.isMaintenance());
        return JsonResult.success(data);
    }

    // ==================== 拓扑查询 ====================

    @GetMapping("/topology")
    @ResponseBody
    public JsonResult getTopology() {
        if (topologyManager == null) return JsonResult.error("拓扑管理未启用");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("version", topologyManager.getTopologyVersion());
        data.put("replicas", topologyManager.getReplicas());
        List<Object> nodes = new ArrayList<>();
        for (StorageNode n : topologyManager.getNodes()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("nodeKey", n.nodeKey());
            m.put("host", n.getHost());
            m.put("httpPort", n.getHttpPort());
            m.put("weight", n.getWeight());
            m.put("self", n.isSelf());
            // 从 ClusterProperties 读取能力标记：storage 节点标识为 host:httpPort，
            // 与 cache 的 host:cachePort 不同，须按 host+httpPort 对齐同一物理节点。
            ClusterNode cn = findClusterNode(n);
            m.put("storageEnabled", cn != null ? cn.isStorageEnabled() : true);
            m.put("cacheEnabled", cn != null ? cn.isCacheEnabled() : true);
            // 附带 cache 侧 nodeKey，便于管理面按统一键合并同一物理节点
            if (cn != null) {
                m.put("cacheNodeKey", cn.nodeKey());
            }
            nodes.add(m);
        }
        data.put("nodes", nodes);
        return JsonResult.success(data);
    }

    // ==================== 扩缩容操作 ====================

    /**
     * 添加存储节点（scale-out）。
     * <p>流程：加入拓扑 → 重建 HRW → 广播 → 全量重平衡（把新主是新节点的 key 迁过去）。
     * <p>调用前应先挂维护态；调用后解除维护态。
     *
     * @param host 新节点 host
     * @param httpPort 新节点 HTTP 端口
     * @param weight 权重（默认 1.0）
     * @param rebalance 是否自动触发数据重平衡（默认 true）
     */
    @PostMapping("/scale-out")
    @ResponseBody
    public JsonResult scaleOut(@RequestParam String host,
                               @RequestParam int httpPort,
                               @RequestParam(defaultValue = "1.0") double weight,
                               @RequestParam(defaultValue = "true") boolean rebalance) {
        if (topologyManager == null) return JsonResult.error("拓扑管理未启用");
        if (!storage.isMaintenance()) {
            return JsonResult.warn("请先挂维护态: PUT /storage/cluster/maintenance?enabled=true");
        }
        try {
            log.info("扩容请求: host={} httpPort={} weight={}", host, httpPort, weight);
            topologyManager.addNode(host, httpPort, weight);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("topologyVersion", topologyManager.getTopologyVersion());
            result.put("nodeAdded", host + ":" + httpPort);
            if (rebalance && migrationService != null) {
                result.put("rebalance", migrationService.rebalanceAll());
            }
            return JsonResult.success(result);
        } catch (StorageException e) {
            log.warn("扩容失败", e);
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 转移存储节点数据（transfer）：把源节点的数据迁到其他节点，源节点保留在集群中。
     * <p>流程：从 HRW 移除源 → 重建 HRW → 广播 → 迁移源节点全部对象到新主。
     * <p>调用前应先挂维护态。
     *
     * @param sourceNodeKey 源节点 nodeKey（host:httpPort）
     */
    @PostMapping("/transfer")
    @ResponseBody
    public JsonResult transfer(@RequestParam String sourceNodeKey) {
        if (topologyManager == null) return JsonResult.error("拓扑管理未启用");
        if (migrationService == null) return JsonResult.error("迁移服务未启用");
        if (!storage.isMaintenance()) {
            return JsonResult.warn("请先挂维护态: PUT /storage/cluster/maintenance?enabled=true");
        }
        try {
            log.info("转移请求: source={}", sourceNodeKey);
            // 从拓扑移除源（但不物理下线）：HRW 重建后源不再作为主
            topologyManager.removeNode(sourceNodeKey);
            // 迁移源节点数据到新主（不清理源端文件，源节点保留数据作备份）
            Map<String, Object> migrateResult = migrationService.drainNode(sourceNodeKey);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("topologyVersion", topologyManager.getTopologyVersion());
            result.put("sourceNodeKey", sourceNodeKey);
            result.put("migration", migrateResult);
            return JsonResult.success(result);
        } catch (StorageException e) {
            log.warn("转移失败", e);
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 合并存储节点（merge）：把源节点数据迁到其他节点，清理源端，并从集群移除。
     * <p>流程：从拓扑移除源 → 重建 HRW → 广播 → 迁移源数据到新主并清理源端。
     * <p>调用前应先挂维护态；调用后可物理下线源节点进程。
     *
     * @param sourceNodeKey 源节点 nodeKey（host:httpPort）
     * @param purgeSource 是否清理源端文件（默认 true）
     */
    @PostMapping("/merge")
    @ResponseBody
    public JsonResult merge(@RequestParam String sourceNodeKey,
                            @RequestParam(defaultValue = "true") boolean purgeSource) {
        if (topologyManager == null) return JsonResult.error("拓扑管理未启用");
        if (migrationService == null) return JsonResult.error("迁移服务未启用");
        if (!storage.isMaintenance()) {
            return JsonResult.warn("请先挂维护态: PUT /storage/cluster/maintenance?enabled=true");
        }
        try {
            log.info("合并请求: source={} purge={}", sourceNodeKey, purgeSource);
            // 从拓扑移除源：HRW 重建后源不再作为主或副本
            topologyManager.removeNode(sourceNodeKey);
            // 迁移源节点数据到新主，并按 purgeSource 决定是否清理源端
            Map<String, Object> migrateResult = purgeSource
                    ? migrationService.drainAndPurge(sourceNodeKey)
                    : migrationService.drainNode(sourceNodeKey);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("topologyVersion", topologyManager.getTopologyVersion());
            result.put("sourceNodeKey", sourceNodeKey);
            result.put("removed", true);
            result.put("migration", migrateResult);
            return JsonResult.success(result);
        } catch (StorageException e) {
            log.warn("合并失败", e);
            return JsonResult.error(e.getMessage());
        }
    }

    // ==================== 迁移状态 ====================

    @GetMapping("/migration/status")
    @ResponseBody
    public JsonResult migrationStatus() {
        if (migrationService == null) return JsonResult.error("迁移服务未启用");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("inProgress", migrationService.isInProgress());
        data.put("lastResult", migrationService.getLastResult());
        return JsonResult.success(data);
    }

    // ==================== failover ====================

    /**
     * 手动强制 failover 指定节点。
     * <p>触发：标记 DEAD + 路由重建 + failoverEpoch++ + 立即对账。
     * <p>与自动 failover 等效，适用于运维预判故障或维护时手动隔离节点。
     * @param nodeKey 要 failover 的节点（host:httpPort）
     */
    @PostMapping("/failover")
    @ResponseBody
    public JsonResult forceFailover(@RequestParam String nodeKey) {
        if (failoverManager == null) return JsonResult.error("failover 管理未启用");
        try {
            boolean ok = failoverManager.forceFailover(nodeKey);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeKey", nodeKey);
            data.put("failoverTriggered", ok);
            data.put("epoch", failoverManager.getFailoverEpoch());
            return ok ? JsonResult.success(data) : JsonResult.error("failover 失败: " + nodeKey);
        } catch (Exception e) {
            log.warn("手动 failover 失败: {}", nodeKey, e);
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 取消手动 failover：直接将节点转 ALIVE，跳过反向同步。
     * <p>适用于：手动 failover 后确认节点正常，或维护完成后恢复节点。
     * @param nodeKey 节点
     */
    @PostMapping("/failover/cancel")
    @ResponseBody
    public JsonResult cancelFailover(@RequestParam String nodeKey) {
        if (failoverManager == null) return JsonResult.error("failover 管理未启用");
        try {
            boolean ok = failoverManager.cancelFailover(nodeKey);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeKey", nodeKey);
            data.put("restored", ok);
            data.put("epoch", failoverManager.getFailoverEpoch());
            return JsonResult.success(data);
        } catch (Exception e) {
            log.warn("取消 failover 失败: {}", nodeKey, e);
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 查询 failover 状态：epoch + 各节点健康状态 + 连续失败次数。
     */
    @GetMapping("/failover/status")
    @ResponseBody
    public JsonResult failoverStatus() {
        if (failoverManager == null) return JsonResult.error("failover 管理未启用");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("epoch", failoverManager.getFailoverEpoch());
        data.put("nodes", failoverManager.healthSnapshot());
        return JsonResult.success(data);
    }

    /**
     * 节点健康状态：各节点 ALIVE/SUSPECT/DEAD/QUARANTINE + 连续失败次数。
     */
    @GetMapping("/health")
    @ResponseBody
    public JsonResult health() {
        if (failoverManager == null) return JsonResult.error("failover 管理未启用");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("epoch", failoverManager.getFailoverEpoch());
        data.put("nodes", failoverManager.healthSnapshot());
        return JsonResult.success(data);
    }

    // ==================== 内部工具 ====================

    /** 从 ClusterProperties 中按 host+httpPort 匹配对应的 ClusterNode（用于读取能力标记与 cacheNodeKey） */
    private ClusterNode findClusterNode(StorageNode n) {
        if (clusterProperties == null) return null;
        return clusterProperties.findByHostHttpPort(n.getHost(), n.getHttpPort(),
                cachePortProperties != null ? cachePortProperties.getPort() : 0, serverPort);
    }
}
