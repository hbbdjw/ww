package com.ww.admin.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.storage.DistributedStorage;
import com.ww.storage.StorageFailoverManager;
import com.ww.storage.StorageNode;
import com.ww.storage.StorageTopologyManager;

import jakarta.annotation.PreDestroy;
import tools.jackson.databind.ObjectMapper;

/**
 * 集群状态 SSE 推送 hub：前端订阅 {@code GET /cluster/status/stream} 后，
 * 由本服务周期采集集群聚合状态并广播给所有订阅者，替代前端轮询。
 *
 * <p>每帧 data 载荷为 JSON 聚合快照：
 * <ul>
 *   <li>{@code overview}：集群状态总览（拓扑版本/存活数/节点列表/failover/rebalance，
 *       复用 {@link ClusterAdminService#statusOverview()}）；</li>
 *   <li>{@code cacheNodes}：缓存节点视图（TCP 在线探测 + 每节点指标，
 *       复用 {@link ClusterAdminService#nodesOverview()}）；</li>
 *   <li>{@code storage}：存储集群视图（topology / health / maintenance，未启用时 enabled=false）。</li>
 * </ul>
 *
 * <p>采集与广播在单一调度线程执行（scheduleWithFixedDelay），天然避免上一轮
 * TCP 探测未完成时下一轮叠加；同一份快照广播给所有订阅者，多订阅者不重复采集。
 * 无订阅者时调度器暂停，首个订阅到达时恢复，避免空转开销。
 */
@Service
public class ClusterStatusStreamHub {

    private static final Logger log = LoggerFactory.getLogger(ClusterStatusStreamHub.class);

    /** 推送周期（秒）：固定延迟，上一轮采集完成后间隔再执行 */
    private static final long PUSH_INTERVAL_SEC = 3;

    @Autowired
    private ClusterAdminService clusterAdminService;

    @Autowired(required = false)
    private DistributedStorage storage;

    @Autowired(required = false)
    private StorageTopologyManager topologyManager;

    @Autowired(required = false)
    private StorageFailoverManager storageFailoverManager;

    @Autowired(required = false)
    private ClusterProperties clusterProperties;

    @Autowired
    private CachePortProperties cachePortProperties;

    @Value("${server.port:8080}")
    private int serverPort;

    @Autowired
    private ObjectMapper objectMapper;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final Object schedulerLock = new Object();
    private ScheduledExecutorService scheduler;

    /** 订阅集群状态流：注册 emitter 并确保采集调度已启动（立即推送首帧） */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        ensureScheduler();
        // 立即触发一次采集（订阅后无需等待整个周期），异步执行避免阻塞 servlet 线程
        scheduler.execute(this::tick);
        return emitter;
    }

    // ==================== 采集与广播 ====================

    /** 单轮采集：构建聚合快照并广播；异常只记录不中断后续轮次 */
    private void tick() {
        if (emitters.isEmpty()) return;
        String payload;
        try {
            payload = objectMapper.writeValueAsString(buildSnapshot());
        } catch (Exception e) {
            log.warn("集群状态快照序列化失败：{}", e.getMessage());
            return;
        }
        broadcast(payload);
    }

    /** 聚合快照：overview + cacheNodes + storage */
    private Map<String, Object> buildSnapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("timestamp", System.currentTimeMillis());
        try {
            snap.put("overview", clusterAdminService.statusOverview());
        } catch (Exception e) {
            snap.put("overview", Map.of("error", e.getMessage()));
        }
        try {
            snap.put("cacheNodes", clusterAdminService.nodesOverview());
        } catch (Exception e) {
            snap.put("cacheNodes", Map.of("error", e.getMessage()));
        }
        snap.put("storage", storageSnapshot());
        return snap;
    }

    /** 存储集群视图：topology / health / maintenance，组件未启用时降级 */
    private Map<String, Object> storageSnapshot() {
        Map<String, Object> storage = new LinkedHashMap<>();
        boolean enabled = topologyManager != null;
        storage.put("enabled", enabled);
        if (this.storage != null) {
            try {
                storage.put("maintenance", this.storage.isMaintenance());
            } catch (Exception e) {
                storage.put("maintenance", false);
            }
        }
        if (enabled) {
            try {
                Map<String, Object> topology = new LinkedHashMap<>();
                topology.put("version", topologyManager.getTopologyVersion());
                topology.put("replicas", topologyManager.getReplicas());
                List<Object> nodes = new ArrayList<>();
                for (StorageNode n : topologyManager.getNodes()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("nodeKey", n.nodeKey());
                    m.put("host", n.getHost());
                    m.put("httpPort", n.getHttpPort());
                    m.put("weight", n.getWeight());
                    m.put("self", n.isSelf());
                    ClusterNode cn = findClusterNode(n.getHost(), n.getHttpPort());
                    m.put("storageEnabled", cn != null ? cn.isStorageEnabled() : true);
                    m.put("cacheEnabled", cn != null ? cn.isCacheEnabled() : true);
                    // storage 节点标识为 host:httpPort，与 cache 的 host:cachePort 不同；
                    // 附带 cache 侧 nodeKey 供前端按统一键合并同一物理节点（避免一节点两行）。
                    if (cn != null) {
                        m.put("cacheNodeKey", cn.nodeKey());
                    }
                    nodes.add(m);
                }
                topology.put("nodes", nodes);
                storage.put("topology", topology);
            } catch (Exception e) {
                storage.put("topology", Map.of("error", e.getMessage()));
            }
        }
        if (storageFailoverManager != null) {
            try {
                Map<String, Object> health = new LinkedHashMap<>();
                health.put("epoch", storageFailoverManager.getFailoverEpoch());
                List<Map<String, Object>> healthNodes = storageFailoverManager.healthSnapshot();
                // 健康快照同样以 host:httpPort 标识节点：补充 cacheNodeKey 供前端合并
                for (Map<String, Object> hm : healthNodes) {
                    Object hostObj = hm.get("host");
                    Object portObj = hm.get("httpPort");
                    if (hostObj instanceof String host && portObj instanceof Number port) {
                        ClusterNode cn = findClusterNode(host, port.intValue());
                        if (cn != null) hm.put("cacheNodeKey", cn.nodeKey());
                    }
                }
                health.put("nodes", healthNodes);
                storage.put("health", health);
            } catch (Exception e) {
                storage.put("health", Map.of("error", e.getMessage()));
            }
        }
        return storage;
    }

    /** 广播一帧 JSON；发送失败（连接已断）的订阅者移除 */
    private void broadcast(String payload) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().data(payload));
            } catch (Exception e) {
                emitters.remove(emitter);
                try {
                    emitter.complete();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 按 host+httpPort 匹配 ClusterProperties 中的集群节点（用于读取能力标记与 cacheNodeKey）。
     * <p>storage 节点标识为 host:httpPort，cache 节点标识为 host:cachePort，不能直接比对，
     * 需经 {@link ClusterProperties#findByHostHttpPort(String, int, int, int)} 对齐同一物理节点。
     */
    private ClusterNode findClusterNode(String host, int httpPort) {
        if (clusterProperties == null) return null;
        return clusterProperties.findByHostHttpPort(host, httpPort,
                cachePortProperties != null ? cachePortProperties.getPort() : 0, serverPort);
    }

    // ==================== 调度生命周期 ====================

    /** 懒启动采集调度器：无订阅者时空闲，首个订阅到达时启动 */
    private void ensureScheduler() {
        synchronized (schedulerLock) {
            if (scheduler != null && !scheduler.isShutdown()) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cluster-status-stream");
                t.setDaemon(true);
                return t;
            });
            // 固定延迟：一轮采集（含最长 8s 的 TCP 探测）完成后再间隔 3s 执行，避免叠加
            scheduler.scheduleWithFixedDelay(this::tick,
                    PUSH_INTERVAL_SEC, PUSH_INTERVAL_SEC, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void shutdown() {
        synchronized (schedulerLock) {
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.complete();
            } catch (Exception ignore) {
            }
        }
        emitters.clear();
    }
}
