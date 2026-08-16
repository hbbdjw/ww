package com.ww.storage;

/**
 * 存储节点视图：在 ClusterNode 之上补充 HTTP 端口，用于 storage 跨节点 HTTP 通信。
 *
 * <p>所有节点均为等层级关系（peer）：任一节点都可接收任一请求，再按 HRW 路由到主节点。
 * self 标记当前进程对应的节点。
 */
public class StorageNode {

    private final String host;
    private final int httpPort;
    private final double weight;
    private final boolean self;

    public StorageNode(String host, int httpPort, double weight, boolean self) {
        if (host == null || host.isEmpty()) {
            throw new StorageException("StorageNode host 不能为空");
        }
        if (httpPort <= 0) {
            throw new StorageException("StorageNode httpPort 必须 > 0，host=" + host);
        }
        if (weight <= 0) {
            throw new StorageException("StorageNode weight 必须 > 0，host=" + host);
        }
        this.host = host;
        this.httpPort = httpPort;
        this.weight = weight;
        this.self = self;
    }

    public String getHost() { return host; }

    public int getHttpPort() { return httpPort; }

    public double getWeight() { return weight; }

    public boolean isSelf() { return self; }

    /** 用于 HRW hash 与节点比对 */
    public String nodeKey() {
        return host + ":" + httpPort;
    }

    /** HTTP base url，例如 http://127.0.0.1:8081 */
    public String baseUrl() {
        return "http://" + host + ":" + httpPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StorageNode that)) return false;
        return httpPort == that.httpPort && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + httpPort;
    }

    @Override
    public String toString() {
        return nodeKey() + (self ? "(self)" : "") + "[w=" + weight + "]";
    }
}
