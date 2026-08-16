package com.ww.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.ww.common.ClusterNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failover 管理：周期性探测集群节点，对超时节点标记 SUSPECT，对持续不可达节点标记 DEAD。
 *
 * <p>核心策略：
 * <ul>
 *   <li>每个探测周期对每个非 self 节点发 OP_PING；成功则 touchHeartbeat，失败累计 missedCount</li>
 *   <li>missedCount >= suspectThreshold 标记 SUSPECT</li>
 *   <li>missedCount >= deadThreshold 标记 DEAD，触发 membership.topologyVersion 递增</li>
 *   <li>DistributedCache 路由时跳过 DEAD 节点，自动将 backup 提升为新 primary</li>
 *   <li>原主恢复（gossip 收到心跳）后，markAlive 自动让路由重新纳入</li>
 * </ul>
 *
 * <p>"提升"在路由层完成：HRW 选节点时过滤 DEAD，剩余节点中的第一个即为新 primary，
 * 无需显式 PROMOTE 消息（避免脑裂下双 primary）。PROMOTE 消息保留供人工 failover 通知。
 *
 * <p>本类不做 quorum 投票（节点少时不可行），改为"悲观检测"：
 * 一旦本节点认为某节点 DEAD，立即从路由中排除；其他节点通过 gossip 同步状态。
 */
public class FailoverManager {

    private static final Logger log = LoggerFactory.getLogger(FailoverManager.class);

    /** 默认探测周期 5 秒 */
    static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000L;
    /** 默认 SUSPECT 阈值：连续 2 次失败 */
    static final int DEFAULT_SUSPECT_THRESHOLD = 2;
    /** 默认 DEAD 阈值：连续 3 次失败 */
    static final int DEFAULT_DEAD_THRESHOLD = 3;

    private final ClusterMembership membership;
    private final String authToken;
    private final Function<ClusterNode, ? extends CacheRpcClient> clientFactory;
    private final long heartbeatIntervalMs;
    private final int suspectThreshold;
    private final int deadThreshold;
    /** nodeKey → 连续失败次数 */
    private final Map<String, Integer> missedCount = new ConcurrentHashMap<>();
    /** nodeKey → 最近一次成功心跳时间（用于陈旧性监控） */
    private final Map<String, Long> lastSuccessMs = new ConcurrentHashMap<>();
    /** 手动 failover 标记：nodeKey → Boolean true（被人工强制 DEAD） */
    private final Map<String, Boolean> manualDead = new ConcurrentHashMap<>();
    /**
     * P1-6 修复：心跳连接池（nodeKey → client），复用 TCP 连接避免每次心跳新建/关闭连接。
     * 连接在发送失败时关闭并移除，下次心跳时自动重建。
     */
    private final Map<String, CacheRpcClient> heartbeatClients = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    /** 当前 failover 状态摘要（供管理面查询） */
    private volatile Map<String, Object> lastStatus = new ConcurrentHashMap<>();

    public FailoverManager(ClusterMembership membership, String authToken,
                           long heartbeatIntervalMs, int suspectThreshold, int deadThreshold) {
        this(membership, authToken, heartbeatIntervalMs, suspectThreshold, deadThreshold,
                node -> new CacheClient(node.getHost(), node.getPort(), authToken));
    }

    public FailoverManager(ClusterMembership membership, String authToken,
                           long heartbeatIntervalMs, int suspectThreshold, int deadThreshold,
                           Function<ClusterNode, ? extends CacheRpcClient> clientFactory) {
        this.membership = membership;
        this.authToken = authToken;
        this.clientFactory = clientFactory != null ? clientFactory
                : node -> new CacheClient(node.getHost(), node.getPort(), authToken);
        this.heartbeatIntervalMs = heartbeatIntervalMs > 0 ? heartbeatIntervalMs : DEFAULT_HEARTBEAT_INTERVAL_MS;
        this.suspectThreshold = Math.max(1, suspectThreshold);
        this.deadThreshold = Math.max(suspectThreshold + 1, deadThreshold);
    }

    /** 默认参数构造 */
    public FailoverManager(ClusterMembership membership, String authToken) {
        this(membership, authToken, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_SUSPECT_THRESHOLD, DEFAULT_DEAD_THRESHOLD);
    }

    /** 启动心跳检测调度 */
    public synchronized void start() {
        if (running) return;
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-failover");
            t.setDaemon(true);
            return t;
        });
        // 起始延迟短，便于测试快速生效
        scheduler.scheduleAtFixedRate(this::probeAll,
                Math.min(heartbeatIntervalMs, 1000L), heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        log.info("FailoverManager 启动: interval={}ms, suspectThreshold={}, deadThreshold={}",
                heartbeatIntervalMs, suspectThreshold, deadThreshold);
    }

    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try { scheduler.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            scheduler = null;
        }
        // P1-6：停止时关闭所有心跳连接池
        for (CacheRpcClient client : heartbeatClients.values()) {
            try { client.close(); } catch (Exception ignored) {}
        }
        heartbeatClients.clear();
    }

    public boolean isRunning() { return running; }

    /** 手动 failover：立即把指定节点标记为 DEAD，路由重写由 DistributedCache 完成 */
    public boolean manualFailover(String nodeKey) {
        if (nodeKey == null || nodeKey.isEmpty()) return false;
        ClusterNode n = membership.getNode(nodeKey);
        if (n == null) {
            log.warn("manualFailover: 节点 {} 不在 membership 中", nodeKey);
            return false;
        }
        if (n.isSelf()) {
            throw new CacheException("manualFailover: 不能强制 failover 自身节点");
        }
        manualDead.put(nodeKey, Boolean.TRUE);
        boolean changed = membership.markDead(nodeKey);
        log.warn("manualFailover: 已强制标记 {} 为 DEAD (changed={})", nodeKey, changed);
        return changed;
    }

    /** 取消手动 failover（节点恢复后允许重新纳入路由） */
    public boolean cancelManualFailover(String nodeKey) {
        Boolean removed = manualDead.remove(nodeKey);
        if (removed == null) return false;
        membership.markAlive(nodeKey);
        log.info("取消 manual failover: {}", nodeKey);
        return true;
    }

    /** 主动向对端发 PING 并合并其 membership 快照（也被调度器调用） */
    public void probeAll() {
        if (!running) return;
        ClusterNode self = membership.getSelf();
        if (self == null) return;
        List<ClusterNode> nodes = membership.getAllNodes();
        for (ClusterNode n : nodes) {
            if (n.isSelf()) continue;
            // 手动 DEAD 节点不自动探测恢复
            if (Boolean.TRUE.equals(manualDead.get(n.nodeKey()))) continue;
            probeNode(n);
        }
        updateStatus();
    }

    /**
     * P1-6 修复：复用连接池中的 TCP 连接，避免每次心跳新建/关闭连接的三次握手开销。
     * 发送失败时关闭并移除连接，下次探测时自动重建。
     */
    private void probeNode(ClusterNode node) {
        String nodeKey = node.nodeKey();
        CacheRpcClient client = heartbeatClients.computeIfAbsent(nodeKey,
                k -> clientFactory.apply(node));
        try {
            long startEpoch = membership.getSelf().getEpoch();
            byte[] payload = CacheProtocol.encodeGossipArgs(
                    CacheProtocol.OP_PING,
                    membership.getSelfNodeKey(),
                    startEpoch,
                    membership.toSnapshot());
            byte[] resp = client.send(CacheProtocol.FLAG_NORMAL, CacheProtocol.OP_PING, payload, "failover-ping");
            // 响应 payload 应为 PONG 的 membership 快照
            Object decoded = Codec.decodeValue(resp);
            if (decoded instanceof List<?> list) {
                @SuppressWarnings("unchecked")
                List<List<Object>> snapshot = (List<List<Object>>) (List<?>) list;
                membership.mergeSnapshot(snapshot);
            }
            membership.touchHeartbeat(nodeKey);
            missedCount.remove(nodeKey);
            lastSuccessMs.put(nodeKey, System.currentTimeMillis());
        } catch (IOException e) {
            // 连接失败：关闭并从池中移除，下次探测时自动重建
            heartbeatClients.remove(nodeKey);
            try { client.close(); } catch (Exception ignored) {}
            int misses = missedCount.merge(nodeKey, 1, Integer::sum);
            if (misses == suspectThreshold) {
                membership.markSuspect(nodeKey);
                log.warn("节点 {} 心跳连续 {} 次失败，标记 SUSPECT", nodeKey, misses);
            } else if (misses >= deadThreshold) {
                membership.markDead(nodeKey);
                log.error("节点 {} 心跳连续 {} 次失败，标记 DEAD，触发 failover", nodeKey, misses);
            } else {
                log.debug("节点 {} 心跳失败 {}/{}: {}", nodeKey, misses, deadThreshold, e.getMessage());
            }
        }
    }

    private void updateStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("timestamp", System.currentTimeMillis());
        status.put("topologyVersion", membership.getTopologyVersion());
        status.put("aliveCount", membership.aliveCount());

        List<Map<String, Object>> nodeStatuses = new ArrayList<>();
        for (ClusterNode n : membership.getAllNodes()) {
            Map<String, Object> m = new ConcurrentHashMap<>();
            m.put("nodeKey", n.nodeKey());
            m.put("self", n.isSelf());
            m.put("status", n.getStatus().name());
            m.put("epoch", n.getEpoch());
            m.put("weight", n.getWeight());
            m.put("lastHeartbeatMs", n.getLastHeartbeatMs());
            m.put("missedCount", missedCount.getOrDefault(n.nodeKey(), 0));
            m.put("lastSuccessMs", lastSuccessMs.getOrDefault(n.nodeKey(), 0L));
            m.put("manualDead", Boolean.TRUE.equals(manualDead.get(n.nodeKey())));
            nodeStatuses.add(m);
        }
        status.put("nodes", nodeStatuses);
        this.lastStatus = status;
    }

    /** 管理面查询：最近一次探测状态摘要 */
    public Map<String, Object> getStatus() {
        if (lastStatus.isEmpty()) updateStatus();
        return lastStatus;
    }

    /** 测试辅助：强制对指定节点累计失败次数 */
    void injectMissForTest(String nodeKey) {
        missedCount.merge(nodeKey, 1, Integer::sum);
    }
}
