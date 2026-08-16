package com.ww.common;

/**
 * 集群节点
 */
public class ClusterNode {

    /** 节点存活状态 */
    public enum Status {
        /** 健康：心跳正常，可读写 */
        ALIVE,
        /** 疑似下线：心跳超时，等待更多证据 */
        SUSPECT,
        /** 已下线：被判定为永久故障，触发 failover 后排除出路由 */
        DEAD
    }

    private String host;
    private int port;
    private double weight;
    private boolean self;

    /**
     * 该节点的 HTTP 端口（可选，默认 0）。
     * <p>cache 模块不使用此字段（它使用 TCP port）；storage 模块通过 HTTP 与对端节点通信时使用。
     * 若配置未设置（=0），storage 模块会按 (port + httpPortOffset) 推导。
     */
    private int httpPort;

    /**
     * 该节点的内部 gRPC 端口（cluster-rpc 节点间通信，可选，默认 0）。
     * 若为 0 表示使用默认端口 9000 或与外部端口相同（取决于部署方式）。
     */
    private int grpcPort;

    /** 节点状态，默认 ALIVE */
    private volatile Status status = Status.ALIVE;
    /** 节点 epoch：每次状态变更（加入/退出/failover）递增，用于检测陈旧信息 */
    private volatile long epoch;
    /** 最近一次心跳时间（ms），由 gossip 健康检测维护 */
    private volatile long lastHeartbeatMs;

    /** 是否启用存储功能（默认 true） */
    private boolean storageEnabled = true;
    /** 是否启用缓存功能（默认 true） */
    private boolean cacheEnabled = true;

    public ClusterNode() {}

    public ClusterNode(String host, int port, double weight) {
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.lastHeartbeatMs = System.currentTimeMillis();
    }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public double getWeight() { return weight; }
    public void setWeight(double weight) { this.weight = weight; }

    public int getHttpPort() { return httpPort; }
    public void setHttpPort(int httpPort) { this.httpPort = httpPort; }

    public int getGrpcPort() { return grpcPort; }
    public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }

    /**
     * 获取 gRPC 连接地址（host:grpcPort），若 grpcPort 未配置则降级到 port。
     */
    public String grpcAddress() {
        int gp = grpcPort > 0 ? grpcPort : port;
        return host + ":" + gp;
    }

    public boolean isSelf() { return self; }
    public void setSelf(boolean self) { this.self = self; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }

    public long getEpoch() { return epoch; }
    public void setEpoch(long epoch) { this.epoch = epoch; }

    public long getLastHeartbeatMs() { return lastHeartbeatMs; }
    public void setLastHeartbeatMs(long lastHeartbeatMs) { this.lastHeartbeatMs = lastHeartbeatMs; }

    public boolean isStorageEnabled() { return storageEnabled; }
    public void setStorageEnabled(boolean storageEnabled) { this.storageEnabled = storageEnabled; }

    public boolean isCacheEnabled() { return cacheEnabled; }
    public void setCacheEnabled(boolean cacheEnabled) { this.cacheEnabled = cacheEnabled; }

    /** 用于 HRW hash 计算的节点标识 */
    public String nodeKey() {
        return host + ":" + port;
    }

    /** 节点是否可路由：仅 ALIVE 状态可参与读写 */
    public boolean isAlive() {
        return status == Status.ALIVE;
    }

    @Override
    public String toString() {
        return nodeKey() + (self ? "(self)" : "") + "[w=" + weight + ",s=" + status + ",e=" + epoch + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterNode that)) return false;
        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }
}
