package com.ww.common;

import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 集群配置，绑定 application.yml 中 cluster.*。
 *
 * <p>运维注意 {@code 缺陷3修复} {@code 缺陷6修复}：所有节点的 cluster 配置必须在 enabled=true 时保持一致
 * （相同节点列表、相同 replicas），否则不同节点会以不同的 owner 集合执行复制，
 * 导致部分 key 在某些节点上从未被复制或被错误复制到无关节点。
 */
@Component
@ConfigurationProperties(prefix = "cluster")
public class ClusterProperties {

    private static final Logger log = LoggerFactory.getLogger(ClusterProperties.class);

    /** 是否启用集群模式 */
    private boolean enabled = false;

    /**
     * 副本组大小（含主节点），默认 2 表示 1 主 + 1 备。
     * <p>必须 >= 2 才有备份节点；值为 1 表示无备份（仅主节点）。
     */
    private int replicas = 2;

    /** 集群节点列表 */
    private List<ClusterNode> nodes = new ArrayList<>();

    /** 是否启用动态成员管理（节点发现 + gossip） */
    private boolean membershipEnabled = false;

    /** 是否启用 failover 自动检测 */
    private boolean failoverEnabled = false;

    /** 是否启用数据重平衡 */
    private boolean rebalanceEnabled = false;

    /** 心跳探测周期（毫秒） */
    private long heartbeatIntervalMs = 5000L;

    /** SUSPECT 阈值：连续失败次数 */
    private int suspectThreshold = 2;

    /** DEAD 阈值：连续失败次数（>= suspectThreshold+1） */
    private int deadThreshold = 3;

    /** rebalance 扫描周期（秒） */
    private long rebalanceScanIntervalSec = 30L;

    /**
     * 内部节点通信协议模式：
     * <ul>
     *   <li>{@code tcp} — 仅使用自定义 TCP 协议（默认，向后兼容）</li>
     *   <li>{@code grpc} — 使用 cluster-rpc gRPC；节点需配置 internalPort</li>
     *   <li>{@code both} — 同时监听 TCP 和 gRPC（灰度迁移用）</li>
     * </ul>
     */
    private String protocol = "tcp";

    /** 内部 gRPC 端口（节点间通信），仅在 protocol=grpc/both 时生效。 */
    private int internalPort = 9000;

    /** gRPC 请求默认超时（毫秒） */
    private long grpcTimeoutMs = 10_000L;

    @PostConstruct
    public void printConfig() {
        log.info("cluster 配置绑定: enabled={}, replicas={}, nodes={}, protocol={}, internalPort={}",
                enabled, replicas, nodes, protocol, internalPort);
        if (enabled) {
            validateOnEnabled();
        }
    }

    /**
     * 集群启用前置校验。CacheConfig 在构造 DistributedCache 时也会再校验一次，
     * 这里更早地 fail-fast 便于运维在启动时立刻看到错误而非运行时发现。
     */
    private void validateOnEnabled() {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("cluster.enabled=true 但 nodes 为空");
        }
        if (replicas < 1) {
            throw new IllegalStateException("cluster.replicas 必须 >= 1，当前=" + replicas);
        }
        if (replicas > nodes.size()) {
            throw new IllegalStateException(
                    "cluster.replicas=" + replicas + " 超过节点数 " + nodes.size()
                            + "（运维需保证所有节点配置一致，否则不同节点复制范围不同会产生拓扑错乱）");
        }
        for (ClusterNode n : nodes) {
            if (n.getWeight() <= 0) {
                throw new IllegalStateException(
                        "cluster node " + n.nodeKey() + " weight 必须 > 0，当前=" + n.getWeight());
            }
        }
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public List<ClusterNode> getNodes() { return nodes; }
    public void setNodes(List<ClusterNode> nodes) { this.nodes = nodes; }

    public boolean isMembershipEnabled() { return membershipEnabled; }
    public void setMembershipEnabled(boolean membershipEnabled) { this.membershipEnabled = membershipEnabled; }

    public boolean isFailoverEnabled() { return failoverEnabled; }
    public void setFailoverEnabled(boolean failoverEnabled) { this.failoverEnabled = failoverEnabled; }

    public boolean isRebalanceEnabled() { return rebalanceEnabled; }
    public void setRebalanceEnabled(boolean rebalanceEnabled) { this.rebalanceEnabled = rebalanceEnabled; }

    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }

    public int getSuspectThreshold() { return suspectThreshold; }
    public void setSuspectThreshold(int suspectThreshold) { this.suspectThreshold = suspectThreshold; }

    public int getDeadThreshold() { return deadThreshold; }
    public void setDeadThreshold(int deadThreshold) { this.deadThreshold = deadThreshold; }

    public long getRebalanceScanIntervalSec() { return rebalanceScanIntervalSec; }
    public void setRebalanceScanIntervalSec(long rebalanceScanIntervalSec) { this.rebalanceScanIntervalSec = rebalanceScanIntervalSec; }

    public String getProtocol() { return protocol; }
    public void setProtocol(String protocol) { this.protocol = protocol; }

    public int getInternalPort() { return internalPort; }
    public void setInternalPort(int internalPort) { this.internalPort = internalPort; }

    public long getGrpcTimeoutMs() { return grpcTimeoutMs; }
    public void setGrpcTimeoutMs(long grpcTimeoutMs) { this.grpcTimeoutMs = grpcTimeoutMs; }

    /**
     * 按 host + httpPort 匹配一个集群节点（storage 视图 → cache 视图的桥接）。
     *
     * <p>storage 模块的节点标识是 {@code host:httpPort}，而 cache/membership 的节点标识是
     * {@code host:cachePort}，两者不能直接比对。本方法按 storage 的 {@code httpPort} 对齐
     * 同一物理节点：节点配置显式指定 {@code httpPort} 时直接比对，否则按对称部署推导
     * （{@code cachePort + (serverPort - cachePort)}，与 {@code StorageConfig} 的推导规则一致）。
     *
     * @param host       storage 节点 host
     * @param httpPort   storage 节点 HTTP 端口
     * @param cachePort  本节点 cache TCP 端口（用于推导 httpPort 偏移）
     * @param serverPort 本节点 admin HTTP 端口（用于推导 httpPort 偏移）
     * @return 匹配的 ClusterNode；未匹配返回 null
     */
    public ClusterNode findByHostHttpPort(String host, int httpPort, int cachePort, int serverPort) {
        if (host == null || httpPort <= 0 || nodes == null) return null;
        int offset = (cachePort > 0 && serverPort > 0) ? serverPort - cachePort : 0;
        for (ClusterNode cn : nodes) {
            if (!cn.getHost().equals(host)) continue;
            int derived = cn.getHttpPort() > 0 ? cn.getHttpPort() : cn.getPort() + offset;
            if (derived == httpPort) return cn;
        }
        return null;
    }

    /**
     * @return true 表示需要启动 gRPC 服务（protocol=grpc 或 both）
     */
    public boolean isGrpcEnabled() {
        return "grpc".equalsIgnoreCase(protocol) || "both".equalsIgnoreCase(protocol);
    }

    /**
     * @return true 表示需要保留 TCP 服务（protocol=tcp 或 both）
     */
    public boolean isTcpEnabled() {
        return !"grpc".equalsIgnoreCase(protocol);
    }
}
