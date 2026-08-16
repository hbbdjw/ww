package com.ww.rpc.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * RPC 响应：节点间通信的统一响应模型。
 *
 * <p>状态码（对应 proto 定义）：
 * <ul>
 *   <li>{@code OK} — 成功</li>
 *   <li>{@code NOT_LEADER} — 非主节点，需重定向到 {@link #leaderHint}</li>
 *   <li>{@code NO_LEADER} — 集群无主（选举中）</li>
 *   <li>{@code TIMEOUT} — 请求超时</li>
 *   <li>{@code UNAVAILABLE} — 目标节点不可达</li>
 *   <li>{@code ERROR} — 业务/内部错误</li>
 * </ul>
 */
public final class RpcResponse {

    public enum Status {
        OK(0),
        NOT_LEADER(1),
        NO_LEADER(2),
        TIMEOUT(3),
        UNAVAILABLE(4),
        ERROR(5);

        private final int code;

        Status(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static Status fromCode(int code) {
            return switch (code) {
                case 0 -> OK;
                case 1 -> NOT_LEADER;
                case 2 -> NO_LEADER;
                case 3 -> TIMEOUT;
                case 4 -> UNAVAILABLE;
                default -> ERROR;
            };
        }
    }

    private final String requestId;
    private final Status status;
    private final byte[] payload;
    private final String leaderHint;
    private final String errorMessage;
    private final Map<String, String> headers;

    private RpcResponse(String requestId, Status status, byte[] payload,
                        String leaderHint, String errorMessage, Map<String, String> headers) {
        this.requestId = requestId;
        this.status = status;
        this.payload = payload == null ? new byte[0] : payload;
        this.leaderHint = leaderHint;
        this.errorMessage = errorMessage;
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static RpcResponse ok(String requestId, byte[] payload) {
        return newBuilder().requestId(requestId).status(Status.OK).payload(payload).build();
    }

    public static RpcResponse notLeader(String requestId, String leaderHint) {
        return newBuilder()
                .requestId(requestId)
                .status(Status.NOT_LEADER)
                .leaderHint(leaderHint)
                .errorMessage("not leader, redirect to: " + (leaderHint == null ? "<unknown>" : leaderHint))
                .build();
    }

    public static RpcResponse noLeader(String requestId) {
        return newBuilder()
                .requestId(requestId)
                .status(Status.NO_LEADER)
                .errorMessage("no leader available")
                .build();
    }

    public static RpcResponse error(String requestId, String message) {
        return newBuilder()
                .requestId(requestId)
                .status(Status.ERROR)
                .errorMessage(message)
                .build();
    }

    public static RpcResponse timeout(String requestId, String message) {
        return newBuilder()
                .requestId(requestId)
                .status(Status.TIMEOUT)
                .errorMessage(message)
                .build();
    }

    public static RpcResponse unavailable(String requestId, String message) {
        return newBuilder()
                .requestId(requestId)
                .status(Status.UNAVAILABLE)
                .errorMessage(message)
                .build();
    }

    public String getRequestId() {
        return requestId;
    }

    public Status getStatus() {
        return status;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getLeaderHint() {
        return leaderHint;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getHeader(String key) {
        return headers.get(key);
    }

    public boolean isSuccess() {
        return status == Status.OK;
    }

    public boolean isRedirect() {
        return status == Status.NOT_LEADER;
    }

    @Override
    public String toString() {
        return "RpcResponse{id=" + requestId + ", status=" + status
                + (leaderHint != null ? ", leaderHint=" + leaderHint : "")
                + (errorMessage != null ? ", error=" + errorMessage : "")
                + ", size=" + payload.length + "}";
    }

    public static final class Builder {
        private String requestId;
        private Status status = Status.OK;
        private byte[] payload;
        private String leaderHint;
        private String errorMessage;
        private Map<String, String> headers = new HashMap<>();

        private Builder() {}

        public Builder requestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder status(Status status) {
            this.status = status;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public Builder leaderHint(String leaderHint) {
            this.leaderHint = leaderHint;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
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

        public RpcResponse build() {
            return new RpcResponse(requestId, status, payload, leaderHint, errorMessage, headers);
        }
    }
}
