package com.ww.rpc.grpc;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ww.rpc.cluster.ClusterMember;
import com.ww.rpc.cluster.MemberStatus;
import com.ww.rpc.cluster.MembershipTable;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.handler.RequestHandlerRegistry;
import com.ww.rpc.handler.RpcRequestHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 gRPC 的 RpcService 实现：整合 GrpcServer、GrpcClient 和 MembershipTable，
 * 是各业务模块直接面向的主入口类。
 *
 * <p>额外提供 sendToLeader、broadcast、addPeer/removePeer 等便捷方法（不在 RpcService 接口上），
 * 供业务模块直接使用。
 */
public class GrpcRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcRpcService.class);

    private final GrpcServer server;
    private final GrpcClient client;
    private final MembershipTable memberTable;
    private final RequestHandlerRegistry handlerRegistry;
    private final String selfNodeId;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public GrpcRpcService(String selfHost, int selfPort) {
        this(selfHost, selfPort, new RequestHandlerRegistry(), new GrpcClient());
    }

    public GrpcRpcService(String selfHost, int selfPort,
                          RequestHandlerRegistry registry,
                          GrpcClient client) {
        this.selfNodeId = selfHost + ":" + selfPort;
        this.handlerRegistry = registry;
        this.memberTable = new MembershipTable(selfNodeId);
        this.client = client;
        this.server = new GrpcServer(selfPort, handlerRegistry);

        ClusterMember self = new ClusterMember(selfHost, selfPort);
        self.setSelf(true);
        self.setStatus(MemberStatus.ALIVE);
        memberTable.addOrUpdate(self);
    }

    public GrpcRpcService(String selfHost, int selfPort,
                          List<String> peerAddresses) {
        this(selfHost, selfPort);
        addPeers(peerAddresses);
    }

    @Override
    public void start() throws IOException {
        if (started.compareAndSet(false, true)) {
            server.start();
            LOG.info("GrpcRpcService 启动完成，selfId={}", selfNodeId);
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            try {
                server.close();
                client.close();
            } catch (Exception e) {
                LOG.warn("关闭 GrpcRpcService 异常", e);
            }
            LOG.info("GrpcRpcService 已关闭");
        }
    }

    @Override
    public boolean isRunning() {
        return started.get() && server.isRunning();
    }

    @Override
    public RpcResponse send(String targetAddress, RpcRequest request, long timeoutMs) {
        return client.send(targetAddress, request, timeoutMs);
    }

    /**
     * 向当前 leader 节点发送请求，支持一次自动重定向。
     */
    public RpcResponse sendToLeader(RpcRequest request, long timeoutMs) {
        String leader = getCurrentLeader();
        if (leader == null) {
            return RpcResponse.noLeader(request.getRequestId());
        }
        RpcResponse resp = client.send(leader, request, timeoutMs);
        if (resp.isRedirect() && resp.getLeaderHint() != null) {
            setCurrentLeader(resp.getLeaderHint());
            return client.send(resp.getLeaderHint(), request, timeoutMs);
        }
        return resp;
    }

    @Override
    public RpcResponse handle(RpcRequest request) {
        return handlerRegistry.handle(request);
    }

    @Override
    public void registerHandler(String serviceName, RpcRequestHandler handler) {
        handlerRegistry.register(serviceName, handler);
    }

    public Collection<String> listServices() {
        return handlerRegistry.listServices();
    }

    public MembershipTable getMembershipTable() {
        return memberTable;
    }

    public String getSelfAddress() {
        return selfNodeId;
    }

    public void addPeer(String host, int port) {
        ClusterMember m = new ClusterMember(host, port);
        m.setStatus(MemberStatus.ALIVE);
        memberTable.addOrUpdate(m);
    }

    public void removePeer(String host, int port) {
        memberTable.remove(host + ":" + port);
    }

    /**
     * 批量添加对端（初始化集群时使用）。
     */
    public void addPeers(List<String> addresses) {
        if (addresses == null) return;
        for (String addr : addresses) {
            String[] parts = addr.split(":", 2);
            if (parts.length == 2) {
                try {
                    addPeer(parts[0], Integer.parseInt(parts[1]));
                } catch (NumberFormatException e) {
                    LOG.warn("非法对端地址：{}，已跳过", addr);
                }
            }
        }
    }

    /**
     * 向所有 ALIVE 非自身节点广播（fire-and-collect）。
     */
    public Collection<RpcResponse> broadcast(RpcRequest request, long timeoutMs) {
        Collection<ClusterMember> peers = memberTable.getAliveMembers();
        if (peers.isEmpty()) return Collections.emptyList();
        return peers.stream()
                .filter(m -> !m.isSelf())
                .map(m -> client.send(m.getAddress(), request, timeoutMs))
                .toList();
    }

    private String getCurrentLeader() {
        for (ClusterMember m : memberTable.getAll()) {
            if ("leader".equals(m.getHeader("role")) && m.isAlive()) {
                return m.getAddress();
            }
        }
        return null;
    }

    private void setCurrentLeader(String leaderAddr) {
        for (ClusterMember m : memberTable.getAll()) {
            if (m.getAddress().equals(leaderAddr)) {
                m.setHeader("role", "leader");
            } else if ("leader".equals(m.getHeader("role"))) {
                m.setHeader("role", null);
            }
        }
    }
}
