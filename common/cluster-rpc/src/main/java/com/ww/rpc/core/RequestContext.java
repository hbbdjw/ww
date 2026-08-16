package com.ww.rpc.core;

import java.net.SocketAddress;
import java.util.Map;

/**
 * 请求上下文：提供请求来源、超时、认证等元信息。
 *
 * <p>在 {@link com.ww.rpc.handler.RpcRequestHandler#handle(RpcRequest, RequestContext)} 中
 * 可通过此对象获取请求的源节点信息。
 */
public interface RequestContext {

    /**
     * @return 源节点地址（host:port）
     */
    String getRemoteAddress();

    /**
     * @return 请求剩余超时时间（毫秒），-1 表示无限制
     */
    long getRemainingTimeoutMs();

    /**
     * 获取请求头。
     */
    String getHeader(String key);

    /**
     * 获取所有请求头（不可变）。
     */
    Map<String, String> getHeaders();

    /**
     * 获取源节点 ID（如果已知）。
     */
    default String getSourceNodeId() {
        return getHeader("x-source-node-id");
    }

    /**
     * 获取 trace ID。
     */
    default String getTraceId() {
        return getHeader("x-trace-id");
    }
}
