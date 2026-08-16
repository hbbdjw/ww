package com.ww.rpc.grpc;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.ww.rpc.core.RpcException;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcTimeoutException;
import com.ww.rpc.core.RpcUnavailableException;
import com.ww.rpc.grpc.proto.RpcMessage;
import com.ww.rpc.grpc.proto.RpcResponseMessage;
import com.ww.rpc.grpc.proto.UnifiedRpcServiceGrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/**
 * gRPC 客户端：管理到各节点的 Channel 连接池，提供同步 RPC 调用能力。
 */
public class GrpcClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcClient.class);

    /** target address (host:port) → ManagedChannel */
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private final long defaultTimeoutMs;
    private final boolean usePlaintext;

    public GrpcClient() {
        this(5000L, true);
    }

    public GrpcClient(long defaultTimeoutMs, boolean usePlaintext) {
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.usePlaintext = usePlaintext;
    }

    /**
     * 获取或创建到指定地址的 Channel。
     */
    private ManagedChannel getOrCreateChannel(String targetAddress) {
        return channels.computeIfAbsent(targetAddress, addr -> {
            String host;
            int port;
            String[] parts = addr.split(":", 2);
            host = parts[0];
            port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9000;

            ManagedChannelBuilder<?> builder = NettyChannelBuilder.forAddress(host, port);
            if (usePlaintext) {
                builder.usePlaintext();
            }
            return builder.build();
        });
    }

    /**
     * 同步发送一元 RPC 请求。
     */
    public RpcResponse send(String targetAddress, RpcRequest request, long timeoutMs) {
        ManagedChannel channel = getOrCreateChannel(targetAddress);
        UnifiedRpcServiceGrpc.UnifiedRpcServiceBlockingStub stub = UnifiedRpcServiceGrpc.newBlockingStub(channel);

        long effectiveTimeout = timeoutMs > 0 ? timeoutMs : defaultTimeoutMs;
        stub = stub.withDeadlineAfter(effectiveTimeout, TimeUnit.MILLISECONDS);

        RpcMessage protoRequest = Marshaller.toProto(request);

        try {
            RpcResponseMessage protoResponse = stub.unaryCall(protoRequest);
            return Marshaller.fromProto(protoResponse);
        } catch (StatusRuntimeException e) {
            return mapGrpcError(e, request.getRequestId());
        } catch (Exception e) {
            throw new RpcUnavailableException("send to " + targetAddress + " failed: " + e.getMessage(), e);
        }
    }

    private RpcResponse mapGrpcError(StatusRuntimeException e, String requestId) {
        return switch (e.getStatus().getCode()) {
            case DEADLINE_EXCEEDED -> RpcResponse.timeout(requestId, e.getStatus().getDescription());
            case UNAVAILABLE -> RpcResponse.unavailable(requestId, e.getStatus().getDescription());
            default -> RpcResponse.error(requestId, e.getStatus().getCode() + ": " + e.getStatus().getDescription());
        };
    }

    /**
     * 检查到指定地址的连接是否就绪。
     */
    public boolean isConnected(String targetAddress) {
        ManagedChannel channel = channels.get(targetAddress);
        if (channel == null) return false;
        ConnectivityState state = channel.getState(false);
        return state == ConnectivityState.READY;
    }

    /**
     * 关闭到指定地址的连接。
     */
    public void disconnect(String targetAddress) {
        ManagedChannel channel = channels.remove(targetAddress);
        if (channel != null) {
            shutdownChannel(channel);
        }
    }

    /**
     * 关闭所有连接。
     */
    @Override
    public void close() {
        for (ManagedChannel ch : channels.values()) {
            shutdownChannel(ch);
        }
        channels.clear();
        LOG.info("GrpcClient 所有连接已关闭");
    }

    private void shutdownChannel(ManagedChannel channel) {
        try {
            channel.shutdown();
            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
