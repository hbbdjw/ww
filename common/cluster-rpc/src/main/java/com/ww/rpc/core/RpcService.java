package com.ww.rpc.core;

import com.ww.rpc.handler.RpcRequestHandler;

/**
 * 集群 RPC 服务接口：定义节点间与客户端到节点的 RPC 调用契约。
 *
 * <p>框架无关设计：不绑定 gRPC/Netty，核心逻辑可独立测试。
 * 网络层实现（如 {@code GrpcRpcService}）作为 Adapter 桥接此接口。
 *
 * <p>核心能力：
 * <ul>
 *   <li>{@link #send} — 向指定节点发送 RPC 请求</li>
 *   <li>{@link #handle} — 本地处理请求（路由/重定向逻辑）</li>
 *   <li>{@link #registerHandler} — 注册业务请求处理器</li>
 * </ul>
 */
public interface RpcService {

    /**
     * 向指定节点发送 RPC 请求。
     *
     * @param targetNodeId 目标节点 ID（通常为 host:port）
     * @param request RPC 请求
     * @param timeoutMs 超时毫秒
     * @return RPC 响应
     * @throws RpcTimeoutException 请求超时
     * @throws RpcUnavailableException 目标节点不可达
     */
    RpcResponse send(String targetNodeId, RpcRequest request, long timeoutMs);

    /**
     * 本地处理请求：由服务端网络层收到请求后调用，分发到注册的 handler。
     *
     * @param request RPC 请求
     * @return RPC 响应
     */
    RpcResponse handle(RpcRequest request);

    /**
     * 注册请求处理器。
     *
     * @param serviceName 服务名（如 "cache.put"、"storage.replicate"）
     * @param handler 请求处理器
     */
    void registerHandler(String serviceName, RpcRequestHandler handler);

    /**
     * 启动 RPC 服务（开始监听端口）。
     *
     * @throws java.io.IOException 端口绑定失败等网络异常
     */
    void start() throws java.io.IOException;

    /**
     * 关闭 RPC 服务。
     */
    void shutdown();

    /**
     * @return 服务是否正在运行
     */
    boolean isRunning();
}
