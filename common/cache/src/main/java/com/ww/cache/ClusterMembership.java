package com.ww.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ww.common.ClusterNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群成员管理：维护动态节点视图，支持加入/退出/gossip 合并。
 *
 * <p>每个进程持有一个 ClusterMembership 实例。gossip 收到对端 membership 快照后调用
 * {@link #mergeSnapshot} 合并；HTTP 管理面调用 {@link #addNode}/{@link #removeNode} 触发拓扑变更。
 *
 * <p>拓扑版本号 (topologyVersion) 在每次成功变更后递增，用于让 RebalanceManager 检测
 * "需要迁移"与"避免重复迁移"。
 *
 * <p>线程安全：所有 mutating 方法都加 lock；读方法返回快照。
 */
public class ClusterMembership {

    private static final Logger log = LoggerFactory.getLogger(ClusterMembership.class);

    private final Object lock = new Object();
    /** nodeKey → 节点视图；保留所有已知节点（含 DEAD），便于 gossip 合并 epoch */
    private final Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();
    /** 当前拓扑版本号，每次节点加入/退出/failover 触发变更时递增 */
    private volatile long topologyVersion = 0;
    /** 本节点 nodeKey */
    private final String selfNodeKey;

    public ClusterMembership(List<ClusterNode> initialNodes, ClusterNode self) {
        if (self == null) {
            throw new CacheException("ClusterMembership: self 节点不能为空");
        }
        this.selfNodeKey = self.nodeKey();
        for (ClusterNode n : initialNodes) {
            ClusterNode copy = copyNode(n);
            if (n.isSelf() || n.nodeKey().equals(selfNodeKey)) {
                copy.setSelf(true);
                copy.setStatus(ClusterNode.Status.ALIVE);
                copy.setLastHeartbeatMs(System.currentTimeMillis());
            }
            nodes.put(copy.nodeKey(), copy);
        }
        // 保证 self 一定在
        if (!nodes.containsKey(selfNodeKey)) {
            ClusterNode selfCopy = copyNode(self);
            selfCopy.setSelf(true);
            selfCopy.setStatus(ClusterNode.Status.ALIVE);
            selfCopy.setLastHeartbeatMs(System.currentTimeMillis());
            nodes.put(selfNodeKey, selfCopy);
        }
        this.topologyVersion = 1;
    }

    /** 获取当前所有节点快照（含 DEAD 节点，便于上层判断） */
    public List<ClusterNode> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    /** 获取当前可路由节点（ALIVE 状态）快照 */
    public List<ClusterNode> getAliveNodes() {
        List<ClusterNode> result = new ArrayList<>();
        for (ClusterNode n : nodes.values()) {
            if (n.isAlive()) result.add(copyNode(n));
        }
        return result;
    }

    /** 获取当前可路由节点数（ALIVE 状态） */
    public int aliveCount() {
        int c = 0;
        for (ClusterNode n : nodes.values()) if (n.isAlive()) c++;
        return c;
    }

    public long getTopologyVersion() {
        return topologyVersion;
    }

    public String getSelfNodeKey() {
        return selfNodeKey;
    }

    public ClusterNode getSelf() {
        return nodes.get(selfNodeKey);
    }

    public ClusterNode getNode(String nodeKey) {
        ClusterNode n = nodes.get(nodeKey);
        return n == null ? null : copyNode(n);
    }

    /**
     * 添加节点：若已存在则更新 weight/epoch。返回是否触发拓扑变更。
     * <p>新增节点 epoch = max(现有 epoch) + 1。
     */
    public boolean addNode(String host, int port, double weight) {
        if (weight <= 0) {
            throw new CacheException("addNode: weight 必须 > 0");
        }
        String key = host + ":" + port;
        synchronized (lock) {
            ClusterNode existing = nodes.get(key);
            if (existing != null) {
                if (existing.getStatus() == ClusterNode.Status.DEAD) {
                    existing.setStatus(ClusterNode.Status.ALIVE);
                    existing.setEpoch(nextEpoch());
                    existing.setLastHeartbeatMs(System.currentTimeMillis());
                    topologyVersion++;
                    log.info("节点复活: {} (topologyVersion={})", key, topologyVersion);
                    return true;
                }
                if (existing.getWeight() != weight) {
                    existing.setWeight(weight);
                    existing.setEpoch(nextEpoch());
                    topologyVersion++;
                    log.info("节点权重更新: {} weight={} (topologyVersion={})", key, weight, topologyVersion);
                    return true;
                }
                return false;
            }
            ClusterNode n = new ClusterNode(host, port, weight);
            n.setEpoch(nextEpoch());
            n.setLastHeartbeatMs(System.currentTimeMillis());
            nodes.put(key, n);
            topologyVersion++;
            log.info("节点加入: {} weight={} (topologyVersion={})", key, weight, topologyVersion);
            return true;
        }
    }

    /** 移除节点（标记为 DEAD，保留 epoch 以便 gossip 合并拒绝旧版本） */
    public boolean removeNode(String host, int port) {
        String key = host + ":" + port;
        synchronized (lock) {
            ClusterNode n = nodes.get(key);
            if (n == null) return false;
            if (n.isSelf()) {
                throw new CacheException("removeNode: 不能移除自身节点");
            }
            if (n.getStatus() == ClusterNode.Status.DEAD) return false;
            n.setStatus(ClusterNode.Status.DEAD);
            n.setEpoch(nextEpoch());
            topologyVersion++;
            log.info("节点退出: {} (topologyVersion={})", key, topologyVersion);
            return true;
        }
    }

    /**
     * 标记节点为 SUSPECT（心跳超时但未确认下线）。
     * @return 是否状态发生变更
     */
    public boolean markSuspect(String nodeKey) {
        synchronized (lock) {
            ClusterNode n = nodes.get(nodeKey);
            if (n == null || n.isSelf()) return false;
            if (n.getStatus() == ClusterNode.Status.SUSPECT) return false;
            n.setStatus(ClusterNode.Status.SUSPECT);
            n.setEpoch(nextEpoch());
            topologyVersion++;
            log.warn("节点标记 SUSPECT: {} (topologyVersion={})", nodeKey, topologyVersion);
            return true;
        }
    }

    /**
     * 标记节点为 DEAD（确认下线）。failover 完成后由 FailoverManager 调用。
     * @return 是否状态发生变更
     */
    public boolean markDead(String nodeKey) {
        synchronized (lock) {
            ClusterNode n = nodes.get(nodeKey);
            if (n == null || n.isSelf()) return false;
            if (n.getStatus() == ClusterNode.Status.DEAD) return false;
            n.setStatus(ClusterNode.Status.DEAD);
            n.setEpoch(nextEpoch());
            topologyVersion++;
            log.warn("节点标记 DEAD: {} (topologyVersion={})", nodeKey, topologyVersion);
            return true;
        }
    }

    /** 标记节点恢复 ALIVE（failover 后原主恢复时调用） */
    public boolean markAlive(String nodeKey) {
        synchronized (lock) {
            ClusterNode n = nodes.get(nodeKey);
            if (n == null) return false;
            if (n.getStatus() == ClusterNode.Status.ALIVE) return false;
            n.setStatus(ClusterNode.Status.ALIVE);
            n.setEpoch(nextEpoch());
            n.setLastHeartbeatMs(System.currentTimeMillis());
            topologyVersion++;
            log.info("节点恢复 ALIVE: {} (topologyVersion={})", nodeKey, topologyVersion);
            return true;
        }
    }

    /** 更新指定节点的心跳时间戳 */
    public boolean touchHeartbeat(String nodeKey) {
        ClusterNode n = nodes.get(nodeKey);
        if (n == null) return false;
        n.setLastHeartbeatMs(System.currentTimeMillis());
        if (n.getStatus() == ClusterNode.Status.SUSPECT) {
            synchronized (lock) {
                n.setStatus(ClusterNode.Status.ALIVE);
                n.setEpoch(nextEpoch());
                topologyVersion++;
                log.info("SUSPECT 节点收到心跳恢复 ALIVE: {}", nodeKey);
            }
        }
        return true;
    }

    /**
     * 合并远端 gossip 快照：对每个节点取 epoch 较大者，状态以 epoch 较大者为准。
     * <p>合并可能引入本节点未知的节点（节点加入场景）。
     * @return 是否发生变更
     */
    @SuppressWarnings("unchecked")
    public boolean mergeSnapshot(List<List<Object>> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) return false;
        boolean changed = false;
        synchronized (lock) {
            for (List<Object> entry : snapshot) {
                if (entry == null || entry.size() < 6) continue;
                String host = String.valueOf(entry.get(0));
                int port = ((Number) entry.get(1)).intValue();
                double weight = ((Number) entry.get(2)).doubleValue();
                String statusStr = String.valueOf(entry.get(3));
                long epoch = ((Number) entry.get(4)).longValue();
                long lastHb = ((Number) entry.get(5)).longValue();

                String key = host + ":" + port;
                ClusterNode local = nodes.get(key);
                ClusterNode.Status remoteStatus = parseStatus(statusStr);

                if (local == null) {
                    ClusterNode n = new ClusterNode(host, port, weight);
                    n.setStatus(remoteStatus);
                    n.setEpoch(epoch);
                    n.setLastHeartbeatMs(lastHb);
                    nodes.put(key, n);
                    changed = true;
                    log.info("gossip 引入新节点: {} status={} epoch={}", key, remoteStatus, epoch);
                } else if (epoch > local.getEpoch()) {
                    local.setWeight(weight);
                    local.setStatus(remoteStatus);
                    local.setEpoch(epoch);
                    local.setLastHeartbeatMs(Math.max(local.getLastHeartbeatMs(), lastHb));
                    changed = true;
                } else if (epoch == local.getEpoch() && remoteStatus != local.getStatus()) {
                    // 同 epoch 但状态不同：取"更悲观"的状态
                    if (statusRank(remoteStatus) > statusRank(local.getStatus())) {
                        local.setStatus(remoteStatus);
                        changed = true;
                    }
                }
            }
            if (changed) {
                topologyVersion++;
            }
        }
        return changed;
    }

    /** 输出 membership 快照用于 gossip 协议传输 */
    public List<List<Object>> toSnapshot() {
        List<List<Object>> result = new ArrayList<>();
        for (ClusterNode n : nodes.values()) {
            result.add(List.of(
                    n.getHost(),
                    n.getPort(),
                    n.getWeight(),
                    n.getStatus().name(),
                    n.getEpoch(),
                    n.getLastHeartbeatMs()
            ));
        }
        return result;
    }

    private long nextEpoch() {
        long max = 0;
        for (ClusterNode n : nodes.values()) {
            if (n.getEpoch() > max) max = n.getEpoch();
        }
        return max + 1;
    }

    private static ClusterNode.Status parseStatus(String s) {
        try {
            return ClusterNode.Status.valueOf(s);
        } catch (Exception e) {
            return ClusterNode.Status.ALIVE;
        }
    }

    /** 状态悲观度排序：ALIVE < SUSPECT < DEAD */
    private static int statusRank(ClusterNode.Status s) {
        return switch (s) {
            case ALIVE -> 0;
            case SUSPECT -> 1;
            case DEAD -> 2;
        };
    }

    private static ClusterNode copyNode(ClusterNode src) {
        ClusterNode n = new ClusterNode(src.getHost(), src.getPort(), src.getWeight());
        n.setSelf(src.isSelf());
        n.setStatus(src.getStatus());
        n.setEpoch(src.getEpoch());
        n.setLastHeartbeatMs(src.getLastHeartbeatMs());
        return n;
    }

    /** 仅用于测试：重置 topologyVersion 不影响功能 */
    void resetTopologyVersionForTest() {
        topologyVersion = 0;
    }
}
