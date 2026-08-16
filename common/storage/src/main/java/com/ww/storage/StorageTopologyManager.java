package com.ww.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

/**
 * 存储集群拓扑管理：运行时维护节点视图，支持动态扩缩容（添加/移除节点）+ 拓扑广播 + 持久化。
 *
 * <p>与 cache 模块的 {@code ClusterMembership} 解耦：storage 按 HTTP host:port 索引节点，
 * 维护自身独立的动态拓扑。所有节点等层级，任一节点发起的拓扑变更会广播到全部对端。
 *
 * <h3>核心流程</h3>
 * <ol>
 *   <li>运维调用 {@code /storage/cluster/*} API 触发扩缩容</li>
 *   <li>本管理器修改内存拓扑（add/remove）</li>
 *   <li>调用 {@link NodeRouter#rebuild} 重建本地 HRW</li>
 *   <li>广播新拓扑到所有对端（{@code /internal/storage/topology}），对端重建各自 HRW</li>
 *   <li>持久化拓扑到 {@code cluster-topology.json}，重启后恢复</li>
 * </ol>
 *
 * <h3>维护态</h3>
 * 扩缩容迁移期间设置维护态，{@link DistributedStorage} 拒绝客户端读写，
 * 保证 read-after-write 一致性（避免迁移中双 primary）。
 */
public class StorageTopologyManager {

    private static final Logger log = LoggerFactory.getLogger(StorageTopologyManager.class);

    /** 拓扑持久化文件名 */
    private static final String TOPOLOGY_FILE = "cluster-topology.json";

    private final NodeRouter nodeRouter;
    private final StorageRpcClient nodeClient;
    private final ObjectMapper objectMapper;
    private final int replicas;
    /** 拓扑持久化目录（storage 根目录） */
    private final File topologyDir;
    /** 拓扑版本号：每次成功变更递增 */
    private volatile long topologyVersion = 1;
    /** 当前节点列表（运行时权威源），按 nodeKey 索引便于增删 */
    private final Map<String, StorageNode> nodeIndex = new ConcurrentHashMap<>();
    /** 节点健康状态（failover 用），未记录视为 ALIVE */
    private final Map<String, NodeStatus> nodeStatus = new ConcurrentHashMap<>();
    /** failover epoch：每次 markDead 递增，用于脑裂 fencing 与调试 */
    private volatile long failoverEpoch = 0;

    public StorageTopologyManager(NodeRouter nodeRouter, StorageRpcClient nodeClient,
                                  ObjectMapper objectMapper, String topologyRootDir) {
        this.nodeRouter = nodeRouter;
        this.nodeClient = nodeClient;
        this.objectMapper = objectMapper;
        this.replicas = nodeRouter.getReplicas();
        this.topologyDir = topologyRootDir != null && !topologyRootDir.isBlank()
                ? new File(topologyRootDir).getAbsoluteFile()
                : new File(".").getAbsoluteFile();
        // 初始化索引（来自 NodeRouter 启动时构建的视图）
        for (StorageNode n : nodeRouter.getNodes()) {
            nodeIndex.put(n.nodeKey(), n);
        }
        // 尝试从持久化文件恢复（若存在且节点数更多，则采用持久化版本）
        loadPersistedTopology();
    }

    // ==================== 查询 ====================

    public synchronized List<StorageNode> getNodes() {
        return new ArrayList<>(nodeIndex.values());
    }

    public synchronized StorageNode getNode(String nodeKey) {
        return nodeIndex.get(nodeKey);
    }

    public long getTopologyVersion() { return topologyVersion; }

    public int getReplicas() { return replicas; }

    public StorageNode getSelf() { return nodeRouter.getSelf(); }

    // ==================== failover ====================

    /** failover epoch（每次 markDead 递增） */
    public long getFailoverEpoch() { return failoverEpoch; }

    /** 查询节点健康状态（未记录返回 null，调用方按 ALIVE 处理） */
    public NodeStatus getNodeStatus(String nodeKey) {
        return nodeStatus.get(nodeKey);
    }

    /** 设置节点状态（供 FailoverManager 在 SUSPECT/ALIVE 切换时调用，不触发路由重建） */
    public void setNodeStatus(String nodeKey, NodeStatus status) {
        NodeStatus old = nodeStatus.put(nodeKey, status);
        if (old != status) {
            log.info("节点状态变更: {} {} → {}", nodeKey, old, status);
        }
    }

    /**
     * 标记节点 DEAD：从可路由节点移除 + failoverEpoch++ + 重建路由 + 广播。
     * <p>nodeIndex 保留（便于恢复时反向同步定位原主），仅 NodeRouter 不再选它。
     * @param nodeKey 形如 host:httpPort
     */
    public synchronized void markDead(String nodeKey) {
        if (!nodeIndex.containsKey(nodeKey)) {
            log.warn("markDead: 节点不在拓扑中 {}", nodeKey);
            return;
        }
        nodeStatus.put(nodeKey, NodeStatus.DEAD);
        failoverEpoch++;
        rebuildRouter("markDead " + nodeKey + " epoch=" + failoverEpoch);
        broadcastFailover(nodeKey);
        log.warn("节点标记 DEAD: {} epoch={}", nodeKey, failoverEpoch);
    }

    /**
     * 标记节点恢复：DEAD → QUARANTINE（隔离，不路由）或 QUARANTINE → ALIVE（纳入路由）。
     * @param nodeKey 节点
     * @param quarantine true=进入 QUARANTINE（反向同步期间不路由）；false=转 ALIVE（纳入路由）
     */
    public synchronized void markAlive(String nodeKey, boolean quarantine) {
        if (!nodeIndex.containsKey(nodeKey)) {
            log.warn("markAlive: 节点不在拓扑中 {}", nodeKey);
            return;
        }
        NodeStatus target = quarantine ? NodeStatus.QUARANTINE : NodeStatus.ALIVE;
        nodeStatus.put(nodeKey, target);
        rebuildRouter("markAlive " + nodeKey + " quarantine=" + quarantine);
        if (!quarantine) {
            broadcastFailover(nodeKey);
            log.info("节点恢复 ALIVE: {}", nodeKey);
        } else {
            log.info("节点进入 QUARANTINE: {}（反向同步中，不纳入路由）", nodeKey);
        }
    }

    /**
     * 广播 failover 事件到对端：对端更新本地 nodeStatus + failoverEpoch + 重建路由。
     * <p>跳过 self 与 {@code nodeKey} 本身：当事件主体就是已下线节点时，再向其发 HTTP 必然失败，
     * 避免产生误导性的 WARN 日志。其余存活节点仍会收到广播；gossip 也会将状态最终一致。
     * 使用虚拟线程异步发送，避免在持锁状态下同步 HTTP 阻塞拓扑操作。
     */
    private void broadcastFailover(String nodeKey) {
        NodeStatus st = nodeStatus.get(nodeKey);
        long epoch = failoverEpoch;
        for (StorageNode n : getNodes()) {
            if (n.isSelf() || n.nodeKey().equals(nodeKey)) continue;
            Thread.ofVirtual().name("topo-failover-" + n.nodeKey()).start(
                    () -> nodeClient.broadcastFailover(n, nodeKey, st.name(), epoch));
        }
    }

    /**
     * 接收对端广播的 failover 事件：更新本地状态 + 重建路由。
     */
    public synchronized void applyRemoteFailover(String nodeKey, String statusName, long epoch) {
        if (!nodeIndex.containsKey(nodeKey)) {
            log.warn("收到 failover 广播但节点不在拓扑中: {}", nodeKey);
            return;
        }
        NodeStatus st;
        try {
            st = NodeStatus.valueOf(statusName);
        } catch (Exception e) {
            log.warn("收到未知节点状态: {}，忽略", statusName);
            return;
        }
        // epoch 落后则忽略（防旧广播覆盖新状态）
        if (epoch < failoverEpoch) {
            log.warn("收到旧 epoch failover 广播: epoch={} < current={}，忽略", epoch, failoverEpoch);
            return;
        }
        failoverEpoch = epoch;
        nodeStatus.put(nodeKey, st);
        rebuildRouter("remoteFailover " + nodeKey + " → " + st + " epoch=" + epoch);
        log.info("应用远端 failover: {} → {} epoch={}", nodeKey, st, epoch);
    }

    /**
     * 返回可路由节点（ALIVE/SUSPECT），供 NodeRouter 重建使用。
     * <p>self 永远纳入（本节点不会 failover 自身）。
     */
    private List<StorageNode> getRoutableNodes() {
        List<StorageNode> routable = new ArrayList<>();
        StorageNode self = getSelf();
        boolean selfIncluded = false;
        for (StorageNode n : nodeIndex.values()) {
            NodeStatus st = nodeStatus.getOrDefault(n.nodeKey(), NodeStatus.ALIVE);
            if (st.isRoutable()) {
                routable.add(n);
                if (n.isSelf()) selfIncluded = true;
            }
        }
        // self 永远纳入路由（本节点不 failover 自身）
        if (!selfIncluded) {
            routable.add(self);
        }
        return routable;
    }

    // ==================== 扩缩容 ====================

    /**
     * 添加节点：纳入拓扑并广播。
     * @param host 节点 host
     * @param httpPort 节点 HTTP 端口
     * @param weight 权重（>0）
     * @return 是否触发拓扑变更
     */
    public synchronized boolean addNode(String host, int httpPort, double weight) {
        if (weight <= 0) throw new StorageException("addNode: weight 必须 > 0");
        String nodeKey = host + ":" + httpPort;
        if (nodeIndex.containsKey(nodeKey)) {
            log.warn("节点已存在，忽略 addNode: {}", nodeKey);
            return false;
        }
        // self 标志由各节点本地判定（这里广播不含 self，对端按自身 host:port 标记）
        boolean isSelf = host.equals(getSelf().getHost()) && httpPort == getSelf().getHttpPort();
        StorageNode n = new StorageNode(host, httpPort, weight, isSelf);
        nodeIndex.put(nodeKey, n);
        rebuildAndBroadcast("addNode " + nodeKey);
        persistTopology();
        return true;
    }

    /**
     * 移除节点：从拓扑移除并广播。注意：移除 self 不被允许。
     * @param nodeKey 形如 host:httpPort
     * @return 是否触发拓扑变更
     */
    public synchronized boolean removeNode(String nodeKey) {
        StorageNode self = getSelf();
        if (nodeKey.equals(self.nodeKey())) {
            throw new StorageException("removeNode: 不能移除自身节点: " + nodeKey);
        }
        StorageNode removed = nodeIndex.remove(nodeKey);
        if (removed == null) {
            log.warn("节点不在拓扑中，忽略 removeNode: {}", nodeKey);
            return false;
        }
        rebuildAndBroadcast("removeNode " + nodeKey);
        persistTopology();
        return true;
    }

    // ==================== 维护态广播 ====================

    /**
     * 广播维护态到所有对端节点（self 本地已设置）。
     * @return 成功广播的节点数
     */
    public int broadcastMaintenance(boolean enabled) {
        int ok = 1; // self 已由调用方设置
        for (StorageNode n : getNodes()) {
            if (n.isSelf()) continue;
            if (nodeClient.broadcastMaintenance(n, enabled)) ok++;
        }
        log.info("维护态广播完成: enabled={} 成功 {}/{}", enabled, ok, nodeIndex.size());
        return ok;
    }

    // ==================== 拓扑接收（对端调用） ====================

    /**
     * 接收对端广播的拓扑并重建本地 NodeRouter。
     * <p>对端发来的节点列表不含 self 标志，本节点按自身 host:httpPort 重新标记。
     * @param nodes 收到的节点列表
     * @param replicas 副本数
     */
    public synchronized void applyRemoteTopology(List<StorageNode> nodes, int replicas) {
        if (nodes == null || nodes.isEmpty()) {
            log.warn("收到空拓扑广播，忽略");
            return;
        }
        StorageNode self = getSelf();
        // 重新标记 self
        List<StorageNode> rebased = new ArrayList<>(nodes.size());
        boolean selfFound = false;
        for (StorageNode n : nodes) {
            boolean isSelf = n.getHost().equals(self.getHost())
                    && n.getHttpPort() == self.getHttpPort();
            if (isSelf) selfFound = true;
            rebased.add(new StorageNode(n.getHost(), n.getHttpPort(), n.getWeight(), isSelf));
        }
        if (!selfFound) {
            log.error("收到拓扑广播但未含 self，拒绝应用: self={}", self);
            return;
        }
        // 更新本地索引（保留已有 status，新节点默认 ALIVE）
        nodeIndex.clear();
        for (StorageNode n : rebased) {
            nodeIndex.put(n.nodeKey(), n);
            nodeStatus.computeIfAbsent(n.nodeKey(), k -> NodeStatus.ALIVE);
        }
        // 清理已移除节点的 status
        nodeStatus.keySet().retainAll(nodeIndex.keySet());
        rebuildRouter("applyRemoteTopology nodes=" + rebased.size());
        log.info("应用远端拓扑: version={} nodes={}", topologyVersion, rebased.size());
        // 不回写持久化文件（避免双向广播循环）；若需持久化由发起节点负责
    }

    // ==================== 内部工具 ====================

    /** 重建本地 NodeRouter（仅用可路由节点 ALIVE/SUSPECT，self 永远纳入） */
    private void rebuildRouter(String reason) {
        List<StorageNode> routable = getRoutableNodes();
        nodeRouter.rebuild(routable, replicas);
        topologyVersion++;
        log.info("路由重建: reason={} version={} routable={}/{} epoch={}",
                reason, topologyVersion, routable.size(), nodeIndex.size(), failoverEpoch);
    }

    /** 重建路由 + 异步广播全量拓扑到对端（扩缩容用），不阻塞持锁线程 */
    private void rebuildAndBroadcast(String reason) {
        rebuildRouter(reason);
        List<StorageNode> snapshot = new ArrayList<>(nodeIndex.values());
        String payload = encodeTopology(snapshot);
        for (StorageNode n : snapshot) {
            if (n.isSelf()) continue;
            Thread.ofVirtual().name("topo-broadcast-" + n.nodeKey()).start(
                    () -> nodeClient.broadcastTopology(n, payload, replicas));
        }
    }

    /** 拓扑节点转为 JSON 友好结构（不含 self 标志，对端按自身身份重新标记） */
    private List<Map<String, Object>> toNodeMaps(List<StorageNode> nodes) {
        List<Map<String, Object>> arr = new ArrayList<>(nodes.size());
        for (StorageNode n : nodes) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("host", n.getHost());
            m.put("httpPort", n.getHttpPort());
            m.put("weight", n.getWeight());
            arr.add(m);
        }
        return arr;
    }

    /** 编码拓扑为 JSON（不含 self 标志，对端按自身身份重新标记） */
    private String encodeTopology(List<StorageNode> nodes) {
        try {
            return objectMapper.writeValueAsString(toNodeMaps(nodes));
        } catch (Exception e) {
            throw new StorageException("编码拓扑失败", e);
        }
    }

    /** 从 JSON 解码拓扑（接收端用） */
    @SuppressWarnings("unchecked")
    public List<StorageNode> decodeTopology(String json) {
        try {
            List<Map<String, Object>> arr = objectMapper.readValue(json, List.class);
            List<StorageNode> result = new ArrayList<>(arr.size());
            for (Map<String, Object> m : arr) {
                String host = String.valueOf(m.get("host"));
                int httpPort = ((Number) m.get("httpPort")).intValue();
                double weight = ((Number) m.get("weight")).doubleValue();
                // self 由调用方按本地身份重新标记
                result.add(new StorageNode(host, httpPort, weight, false));
            }
            return result;
        } catch (Exception e) {
            throw new StorageException("解码拓扑失败: " + json, e);
        }
    }

    // ==================== 持久化 ====================

    /** 持久化当前拓扑到 cluster-topology.json */
    private synchronized void persistTopology() {
        File f = new File(topologyDir, TOPOLOGY_FILE);
        File tmp = new File(topologyDir, TOPOLOGY_FILE + ".tmp");
        try {
            if (!topologyDir.exists() && !topologyDir.mkdirs()) {
                log.warn("无法创建拓扑持久化目录: {}", topologyDir);
                return;
            }
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("version", topologyVersion);
            root.put("replicas", replicas);
            root.put("nodes", toNodeMaps(getNodes()));
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp, root);
            if (!tmp.renameTo(f)) {
                // Windows 可能需要先删除目标
                if (f.exists() && !f.delete()) {
                    log.warn("无法删除旧拓扑文件: {}", f);
                }
                if (!tmp.renameTo(f)) {
                    log.warn("拓扑文件改名失败: {} -> {}", tmp, f);
                }
            }
            log.info("拓扑已持久化: {} version={}", f, topologyVersion);
        } catch (Exception e) {
            log.warn("持久化拓扑失败", e);
        }
    }

    /** 启动时从 cluster-topology.json 恢复（若存在且节点数 >= 当前） */
    @SuppressWarnings("unchecked")
    private void loadPersistedTopology() {
        File f = new File(topologyDir, TOPOLOGY_FILE);
        if (!f.exists()) return;
        try {
            Map<String, Object> root = objectMapper.readValue(f, Map.class);
            Object nodesValue = root.get("nodes");
            if (nodesValue == null) return;
            // 兼容旧版本：早期实现误把 nodes 序列化为 JSON 字符串，此处解回列表
            List<Map<String, Object>> arr;
            if (nodesValue instanceof List) {
                arr = (List<Map<String, Object>>) nodesValue;
            } else if (nodesValue instanceof String) {
                arr = objectMapper.readValue((String) nodesValue, List.class);
            } else {
                log.warn("持久化拓扑 nodes 字段类型未知: {}，忽略恢复", nodesValue.getClass());
                return;
            }
            if (arr.isEmpty()) return;
            StorageNode self = getSelf();
            List<StorageNode> restored = new ArrayList<>(arr.size());
            boolean selfFound = false;
            for (Map<String, Object> m : arr) {
                String host = String.valueOf(m.get("host"));
                int httpPort = ((Number) m.get("httpPort")).intValue();
                double weight = ((Number) m.get("weight")).doubleValue();
                boolean isSelf = host.equals(self.getHost()) && httpPort == self.getHttpPort();
                if (isSelf) selfFound = true;
                restored.add(new StorageNode(host, httpPort, weight, isSelf));
            }
            if (!selfFound) {
                log.warn("持久化拓扑未含 self，忽略恢复: self={}", self);
                return;
            }
            // 仅当持久化节点数 > 当前才采用（防止误覆盖运行时已构建的拓扑）
            if (restored.size() < nodeIndex.size()) {
                log.info("持久化拓扑节点数 {} <= 当前 {}，不恢复", restored.size(), nodeIndex.size());
                return;
            }
            int savedReplicas = root.containsKey("replicas")
                    ? ((Number) root.get("replicas")).intValue() : replicas;
            nodeIndex.clear();
            for (StorageNode n : restored) {
                nodeIndex.put(n.nodeKey(), n);
                nodeStatus.computeIfAbsent(n.nodeKey(), k -> NodeStatus.ALIVE);
            }
            rebuildRouter("loadPersistedTopology nodes=" + restored.size());
            Object v = root.get("version");
            if (v instanceof Number) topologyVersion = ((Number) v).longValue();
            log.info("从持久化恢复拓扑: version={} nodes={}", topologyVersion, restored.size());
        } catch (Exception e) {
            log.warn("加载持久化拓扑失败，忽略: {}", f, e);
        }
    }
}
