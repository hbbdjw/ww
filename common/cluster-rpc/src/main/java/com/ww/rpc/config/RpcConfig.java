package com.ww.rpc.config;

/**
 * RPC 服务配置项。
 */
public class RpcConfig {

    private String host = "0.0.0.0";
    private int port = 9000;
    private long defaultTimeoutMs = 5000L;
    private boolean useTls = false;
    private int ioThreads = 0; // 0 = 默认 (Netty 自动配置)
    private int workerThreads = 0;
    private long heartbeatIntervalMs = 2000L;
    private long heartbeatTimeoutMs = 10000L;
    private int maxInboundMessageSize = 16 * 1024 * 1024; // 16MB

    public RpcConfig() {}

    public RpcConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public long getDefaultTimeoutMs() { return defaultTimeoutMs; }
    public void setDefaultTimeoutMs(long defaultTimeoutMs) { this.defaultTimeoutMs = defaultTimeoutMs; }

    public boolean isUseTls() { return useTls; }
    public void setUseTls(boolean useTls) { this.useTls = useTls; }

    public int getIoThreads() { return ioThreads; }
    public void setIoThreads(int ioThreads) { this.ioThreads = ioThreads; }

    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }

    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }

    public long getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }
    public void setHeartbeatTimeoutMs(long heartbeatTimeoutMs) { this.heartbeatTimeoutMs = heartbeatTimeoutMs; }

    public int getMaxInboundMessageSize() { return maxInboundMessageSize; }
    public void setMaxInboundMessageSize(int maxInboundMessageSize) { this.maxInboundMessageSize = maxInboundMessageSize; }

    @Override
    public String toString() {
        return "RpcConfig{host=" + host + ", port=" + port
                + ", defaultTimeoutMs=" + defaultTimeoutMs + '}';
    }
}
