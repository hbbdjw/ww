package com.ww.rpc.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群成员表：内存中的路由拓扑，记录所有已知节点及其状态。
 *
 * <p>设计要点：
 * <ul>
 *   <li>线程安全：所有写操作同步；读操作返回快照</li>
 *   <li>epoch 机制：每次变更递增 epoch，用于 gossip 合并与陈旧信息检测</li>
 *   <li>监听器：拓扑变更时通知 {@link TopologyChangeListener}</li>
 * </ul>
 */
public class MembershipTable {

    private static final Logger LOG = LoggerFactory.getLogger(MembershipTable.class);

    /** nodeId → member */
    private final Map<String, ClusterMember> members = new LinkedHashMap<>();

    /** 本节点 ID */
    private final String selfNodeId;

    /** 拓扑版本号，每次变更递增 */
    private volatile long topologyVersion = 0;

    /** 拓扑变更监听器 */
    private final List<TopologyChangeListener> listeners = new CopyOnWriteArrayList<>();

    public MembershipTable(String selfNodeId) {
        this.selfNodeId = Objects.requireNonNull(selfNodeId, "selfNodeId");
    }

    public String getSelfNodeId() {
        return selfNodeId;
    }

    /**
     * 添加或更新成员。
     */
    public synchronized ClusterMember addOrUpdate(ClusterMember member) {
        Objects.requireNonNull(member, "member");
        ClusterMember existing = members.get(member.getNodeId());
        long newEpoch = nextEpoch();

        if (existing != null) {
            boolean changed = false;
            if (existing.getStatus() != member.getStatus()) {
                existing.setStatus(member.getStatus());
                changed = true;
            }
            if (Double.compare(existing.getWeight(), member.getWeight()) != 0) {
                existing.setWeight(member.getWeight());
                changed = true;
            }
            existing.setEpoch(newEpoch);
            if (changed) {
                topologyVersion++;
                LOG.info("成员更新: {}", existing);
                notifyListeners(TopologyChangeListener.EventType.STATUS_CHANGED, existing);
            }
            return existing;
        }

        member.setEpoch(newEpoch);
        members.put(member.getNodeId(), member);
        topologyVersion++;
        LOG.info("成员加入: {}", member);
        notifyListeners(TopologyChangeListener.EventType.NODE_ADDED, member);
        return member;
    }

    /**
     * 移除成员。
     */
    public synchronized ClusterMember remove(String nodeId) {
        ClusterMember removed = members.remove(nodeId);
        if (removed != null) {
            topologyVersion++;
            LOG.info("成员移除: {}", removed);
            notifyListeners(TopologyChangeListener.EventType.NODE_REMOVED, removed);
        }
        return removed;
    }

    /**
     * 标记成员状态。
     */
    public synchronized void setStatus(String nodeId, MemberStatus status) {
        ClusterMember m = members.get(nodeId);
        if (m != null && m.getStatus() != status) {
            MemberStatus oldStatus = m.getStatus();
            m.setStatus(status);
            m.setEpoch(nextEpoch());
            topologyVersion++;
            LOG.info("成员状态变更: {} ({} -> {})", nodeId, oldStatus, status);
            notifyListeners(TopologyChangeListener.EventType.STATUS_CHANGED, m);
        }
    }

    /**
     * 更新成员权重。
     */
    public synchronized void setWeight(String nodeId, double weight) {
        ClusterMember m = members.get(nodeId);
        if (m != null && Double.compare(m.getWeight(), weight) != 0) {
            m.setWeight(weight);
            m.setEpoch(nextEpoch());
            topologyVersion++;
            LOG.info("成员权重变更: {} -> weight={}", nodeId, weight);
            notifyListeners(TopologyChangeListener.EventType.WEIGHT_CHANGED, m);
        }
    }

    /**
     * 更新心跳时间。
     */
    public void touchHeartbeat(String nodeId) {
        ClusterMember m = members.get(nodeId);
        if (m != null) {
            m.setLastHeartbeatMs(System.currentTimeMillis());
            if (m.getStatus() == MemberStatus.SUSPECT) {
                setStatus(nodeId, MemberStatus.ALIVE);
            }
        }
    }

    /**
     * 添加拓扑变更监听器。
     */
    public void addListener(TopologyChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除拓扑变更监听器。
     */
    public void removeListener(TopologyChangeListener listener) {
        listeners.remove(listener);
    }

    /**
     * 获取指定成员。
     */
    public synchronized ClusterMember get(String nodeId) {
        return members.get(nodeId);
    }

    /**
     * 获取本节点。
     */
    public synchronized ClusterMember getSelf() {
        return members.get(selfNodeId);
    }

    /**
     * 获取全部成员（快照，不可变）。
     */
    public synchronized Collection<ClusterMember> getAll() {
        return Collections.unmodifiableCollection(new LinkedHashMap<>(members).values());
    }

    /**
     * 获取全部 ALIVE 成员。
     */
    public synchronized Collection<ClusterMember> getAliveMembers() {
        return members.values().stream()
                .filter(ClusterMember::isAlive)
                .toList();
    }

    /**
     * ALIVE 成员数。
     */
    public synchronized int aliveCount() {
        return (int) members.values().stream().filter(ClusterMember::isAlive).count();
    }

    /**
     * 成员总数。
     */
    public synchronized int size() {
        return members.size();
    }

    /**
     * 本节点是否 ALIVE。
     */
    public synchronized boolean isSelfAlive() {
        ClusterMember self = getSelf();
        return self != null && self.isAlive();
    }

    public long getTopologyVersion() {
        return topologyVersion;
    }

    /**
     * 根据地址（host:port）查找成员。
     */
    public synchronized ClusterMember findByAddress(String address) {
        for (ClusterMember m : members.values()) {
            if (m.getAddress().equals(address)) {
                return m;
            }
        }
        return null;
    }

    private long nextEpoch() {
        long max = 0;
        for (ClusterMember m : members.values()) {
            if (m.getEpoch() > max) max = m.getEpoch();
        }
        return max + 1;
    }

    private void notifyListeners(TopologyChangeListener.EventType type, ClusterMember member) {
        for (TopologyChangeListener l : listeners) {
            try {
                l.onTopologyChange(type, member, this);
            } catch (Exception e) {
                LOG.warn("拓扑监听器执行异常", e);
            }
        }
    }
}
