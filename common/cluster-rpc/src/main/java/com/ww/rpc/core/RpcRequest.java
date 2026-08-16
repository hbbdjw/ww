package com.ww.rpc.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * RPC 请求：节点间通信的统一请求模型。
 *
 * <p>框架无关设计：不绑定 gRPC/Protobuf，核心逻辑可独立测试。
 * 网络层（如 gRPC）负责将此对象与线协议进行双向转换。
 *
 * <p>核心字段：
 * <ul>
 *   <li>{@code requestId} — 唯一标识，用于链路追踪与幂等</li>
 *   <li>{@code serviceName} — 服务名，用于分发到对应的 handler（如 "cache.put"）</li>
 *   <li>{@code type} — 请求类型（WRITE/READ/HEARTBEAT/CLUSTER_ADMIN/REPLICATION）</li>
 *   <li>{@code payload} — 请求体（字节，由各业务模块自行序列化）</li>
 *   <li>{@code headers} — 元数据（traceId、源节点、认证 token 等）</li>
 * </ul>
 */
public final class RpcRequest {

    private final String requestId;
    private final String serviceName;
    private final RequestType type;
    private final byte[] payload;
    private final Map<String, String> headers;

    private RpcRequest(String requestId, String serviceName, RequestType type,
                       byte[] payload, Map<String, String> headers) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
        this.type = Objects.requireNonNull(type, "type");
        this.payload = payload == null ? new byte[0] : payload;
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getRequestId() {
        return requestId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public RequestType getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getHeader(String key) {
        return headers.get(key);
    }

    public String getHeader(String key, String defaultValue) {
        return headers.getOrDefault(key, defaultValue);
    }

    @Override
    public String toString() {
        return "RpcRequest{id=" + requestId + ", svc=" + serviceName
                + ", type=" + type + ", size=" + payload.length + "}";
    }

    /**
     * 便捷创建方法：快速构建 HEARTBEAT 请求。
     */
    public static RpcRequest heartbeat(String requestId) {
        return newBuilder()
                .requestId(requestId)
                .serviceName("cluster.ping")
                .type(RequestType.HEARTBEAT)
                .payload(new byte[0])
                .build();
    }

    public static final class Builder {
        private String requestId;
        private String serviceName;
        private RequestType type = RequestType.WRITE;
        private byte[] payload;
        private Map<String, String> headers = new HashMap<>();

        private Builder() {}

        public Builder requestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder type(RequestType type) {
            this.type = type;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            if (headers != null) {
                this.headers = new HashMap<>(headers);
            }
            return this;
        }

        public Builder header(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public RpcRequest build() {
            return new RpcRequest(requestId, serviceName, type, payload, headers);
        }
    }
}
