package com.ww.rpc.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群成员信息：唯一标识一个节点，记录角色与可用性。
 *
 * <p>设计要点：
 * <ul>
 *   <li>{@code nodeId} 全局唯一（格式：host:port）</li>
 *   <li>{@code host} + {@code rpcPort} 用于 RPC 连接</li>
 *   <li>支持可选的权重（供 HRW 哈希等负载均衡算法使用）</li>
 *   <li>支持自定义属性（extensions）用于各业务模块扩展（如角色标记 leader/follower）</li>
 * </ul>
 */
public final class ClusterMember {

    private final String nodeId;
    private final String host;
    private final int rpcPort;
    private volatile double weight;
    private volatile MemberStatus status;
    private volatile boolean self;
    private volatile long epoch;
    private volatile long lastHeartbeatMs;
    private final Map<String, String> extensions = new ConcurrentHashMap<>();

    public ClusterMember(String host, int rpcPort) {
        this(host, rpcPort, 1.0);
    }

    public ClusterMember(String host, int rpcPort, double weight) {
        this.host = Objects.requireNonNull(host, "host");
        if (rpcPort <= 0 || rpcPort > 65535) {
            throw new IllegalArgumentException("invalid port: " + rpcPort);
        }
        this.rpcPort = rpcPort;
        this.nodeId = host + ":" + rpcPort;
        this.weight = weight;
        this.status = MemberStatus.UNKNOWN;
        this.self = false;
        this.epoch = 0;
        this.lastHeartbeatMs = System.currentTimeMillis();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    /** RPC 地址：host:port */
    public String getAddress() {
        return host + ":" + rpcPort;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public MemberStatus getStatus() {
        return status;
    }

    public void setStatus(MemberStatus status) {
        this.status = status;
    }

    public boolean isSelf() {
        return self;
    }

    public void setSelf(boolean self) {
        this.self = self;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getLastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    public void setLastHeartbeatMs(long lastHeartbeatMs) {
        this.lastHeartbeatMs = lastHeartbeatMs;
    }

    /** 是否为可路由状态（ALIVE） */
    public boolean isAlive() {
        return status == MemberStatus.ALIVE;
    }

    public String getHeader(String key) {
        return extensions.get(key);
    }

    public void setHeader(String key, String value) {
        if (value == null) {
            extensions.remove(key);
        } else {
            extensions.put(key, value);
        }
    }

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(new HashMap<>(extensions));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterMember that)) return false;
        return Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "ClusterMember{" + nodeId
                + ", weight=" + weight
                + ", status=" + status
                + (self ? ", self" : "")
                + ", epoch=" + epoch
                + (extensions.isEmpty() ? "" : ", ext=" + extensions)
                + "}";
    }
}
