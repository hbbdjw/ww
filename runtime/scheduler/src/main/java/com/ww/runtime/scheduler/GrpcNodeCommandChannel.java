package com.ww.runtime.scheduler;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.proto.Ack;
import com.ww.runtime.proto.ClusterControlGrpc;
import com.ww.runtime.proto.FreezeReq;
import com.ww.runtime.proto.StartCapabilityReq;
import com.ww.runtime.proto.StopCapabilityReq;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * {@link NodeCommandChannel} 的 gRPC 实现：经 ClusterControl 服务向远端节点下发命令。
 *
 * <p>节点地址从 {@link NodeRegistry} 解析（host + grpcPort），按 {@code host:port}
 * 缓存 {@link ManagedChannel}；每次调用创建 5 秒截止时间的 blocking stub。
 * {@code node == self} 时回退到注入的 {@link LocalNodeCommandChannel}（本进程直连，
 * 不绕网络）。{@link #close()} 释放全部缓存通道。</p>
 *
 * <p>当前使用 plaintext（集群内网假设）；TLS 由宿主按部署环境扩展。</p>
 */
public final class GrpcNodeCommandChannel implements NodeCommandChannel, AutoCloseable {

    /** 默认单命令截止时间（毫秒）。 */
    public static final long DEFAULT_DEADLINE_MILLIS = 5_000L;

    private static final Logger LOG = LoggerFactory.getLogger(GrpcNodeCommandChannel.class);

    private final NodeId self;
    private final LocalNodeCommandChannel local;
    private final NodeRegistry registry;
    private final long deadlineMillis;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public GrpcNodeCommandChannel(NodeId self, LocalNodeCommandChannel local, NodeRegistry registry) {
        this(self, local, registry, DEFAULT_DEADLINE_MILLIS);
    }

    public GrpcNodeCommandChannel(NodeId self, LocalNodeCommandChannel local, NodeRegistry registry,
                                  long deadlineMillis) {
        this.self = Objects.requireNonNull(self, "self");
        this.local = Objects.requireNonNull(local, "local");
        this.registry = Objects.requireNonNull(registry, "registry");
        if (deadlineMillis <= 0) {
            throw new IllegalArgumentException("deadlineMillis must be > 0, got " + deadlineMillis);
        }
        this.deadlineMillis = deadlineMillis;
    }

    @Override
    public boolean startCapability(NodeId node, CapabilityType type) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        if (self.equals(node)) {
            return local.startCapability(node, type);
        }
        ClusterControlGrpc.ClusterControlBlockingStub stub = stubFor(node);
        if (stub == null) {
            return false;
        }
        return execute(stub, s -> s.startCapability(StartCapabilityReq.newBuilder()
                .setNodeId(node.value())
                .setCapability(type.name())
                .build()));
    }

    @Override
    public boolean stopCapability(NodeId node, CapabilityType type) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        if (self.equals(node)) {
            return local.stopCapability(node, type);
        }
        ClusterControlGrpc.ClusterControlBlockingStub stub = stubFor(node);
        if (stub == null) {
            return false;
        }
        return execute(stub, s -> s.stopCapability(StopCapabilityReq.newBuilder()
                .setNodeId(node.value())
                .setCapability(type.name())
                .build()));
    }

    @Override
    public boolean freeze(NodeId node, CapabilityType type, boolean frozen) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        if (self.equals(node)) {
            return local.freeze(node, type, frozen);
        }
        ClusterControlGrpc.ClusterControlBlockingStub stub = stubFor(node);
        if (stub == null) {
            return false;
        }
        return execute(stub, s -> s.freeze(FreezeReq.newBuilder()
                .setNodeId(node.value())
                .setCapability(type.name())
                .setFrozen(frozen)
                .build()));
    }

    /** 释放全部缓存通道（幂等）。 */
    @Override
    public void close() {
        channels.forEach((target, channel) -> channel.shutdown());
        channels.forEach((target, channel) -> {
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                    LOG.warn("grpc channel to {} did not terminate in time", target);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("interrupted while closing grpc channel to {}", target);
            }
        });
        channels.clear();
    }

    private ClusterControlGrpc.ClusterControlBlockingStub stubFor(NodeId node) {
        NodeInfo info = registry.get(node).orElse(null);
        if (info == null) {
            LOG.debug("node {} unknown to registry, cannot dispatch command", node);
            return null;
        }
        if (info.grpcPort() <= 0) {
            LOG.debug("node {} has no usable grpc port ({})", node, info.grpcPort());
            return null;
        }
        String target = info.host() + ":" + info.grpcPort();
        ManagedChannel channel = channels.computeIfAbsent(target,
                key -> ManagedChannelBuilder.forTarget(key).usePlaintext().build());
        return ClusterControlGrpc.newBlockingStub(channel)
                .withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
    }

    private boolean execute(ClusterControlGrpc.ClusterControlBlockingStub stub,
                            Function<ClusterControlGrpc.ClusterControlBlockingStub, Ack> call) {
        try {
            Ack ack = call.apply(stub);
            if (!ack.getOk()) {
                LOG.warn("remote node rejected command: {}", ack.getMessage());
            }
            return ack.getOk();
        } catch (StatusRuntimeException e) {
            LOG.warn("grpc command failed: {} ({})", e.getStatus().getCode(), e.getMessage());
            return false;
        }
    }
}
