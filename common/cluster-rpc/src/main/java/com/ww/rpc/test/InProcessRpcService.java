package com.ww.rpc.test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ww.rpc.cluster.MembershipTable;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.handler.RequestHandlerRegistry;
import com.ww.rpc.handler.RpcRequestHandler;

/**
 * 进程内（In-Process）RpcService 实现：直接调用 handler、不经过网络层。
 *
 * <p>用途：
 * <ul>
 *   <li>单元测试：避免启动真实端口</li>
 *   <li>单机模式：节点在同一 JVM 内直接调用</li>
 * </ul>
 *
 * <p>支持模拟对端：通过 {@link #installPeer(String, InProcessRpcService)} 将另一个 InProcessRpcService
 * 注册为对端，send() 时直接转发到对端的 handle() 方法。
 */
public class InProcessRpcService implements RpcService {

    private final String selfAddress;
    private final RequestHandlerRegistry registry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** nodeId → 对端实例 */
    private final Map<String, InProcessRpcService> peers = new ConcurrentHashMap<>();

    public InProcessRpcService(String selfAddress) {
        this(selfAddress, new RequestHandlerRegistry());
    }

    public InProcessRpcService(String selfAddress, RequestHandlerRegistry registry) {
        this.selfAddress = selfAddress;
        this.registry = registry;
    }

    @Override
    public RpcResponse send(String targetNodeId, RpcRequest request, long timeoutMs) {
        if (targetNodeId.equals(selfAddress)) {
            return handle(request);
        }
        InProcessRpcService peer = peers.get(targetNodeId);
        if (peer == null) {
            return RpcResponse.unavailable(request.getRequestId(),
                    "no peer registered for: " + targetNodeId);
        }
        return peer.handle(request);
    }

    @Override
    public RpcResponse handle(RpcRequest request) {
        return registry.handle(request);
    }

    @Override
    public void registerHandler(String serviceName, RpcRequestHandler handler) {
        registry.register(serviceName, handler);
    }

    @Override
    public void start() throws IOException {
        running.set(true);
    }

    @Override
    public void shutdown() {
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public String getSelfAddress() {
        return selfAddress;
    }

    /**
     * 注册对端实例，用于 send() 时直接转发。
     */
    public void installPeer(String nodeId, InProcessRpcService peer) {
        peers.put(nodeId, peer);
    }

    public void removePeer(String nodeId) {
        peers.remove(nodeId);
    }

    public Collection<String> listServices() {
        return registry.listServices();
    }
}
