package com.ww.runtime.server.grpc;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.proto.Ack;
import com.ww.runtime.proto.ClusterControlGrpc;
import com.ww.runtime.proto.FreezeReq;
import com.ww.runtime.proto.StartCapabilityReq;
import com.ww.runtime.proto.StopCapabilityReq;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * ClusterControl gRPC 服务端：接收调度器（{@code GrpcNodeCommandChannel}）下发的
 * 能力生命周期命令，委托本节点 {@link CapabilityManager} 执行。
 *
 * <p>应答约定：命令成功 → {@code Ack ok=true}；能力未注册 → {@code ok=false}
 * （message 含 "not registered"）；能力类型解析失败或状态机拒绝等任何异常 →
 * {@code ok=false} 且 message 为异常信息——调用方据此在下个调谐周期重试。</p>
 */
public final class ClusterControlServer extends ClusterControlGrpc.ClusterControlImplBase {

    private final NodeId self;
    private final CapabilityManager manager;

    public ClusterControlServer(NodeId self, CapabilityManager manager) {
        this.self = Objects.requireNonNull(self, "self");
        this.manager = Objects.requireNonNull(manager, "manager");
    }

    @Override
    public void startCapability(StartCapabilityReq request, StreamObserver<Ack> responseObserver) {
        respond(responseObserver, () ->
                manager.convergeStart(parseAndCheck(request.getCapability(), request.getNodeId())));
    }

    @Override
    public void stopCapability(StopCapabilityReq request, StreamObserver<Ack> responseObserver) {
        respond(responseObserver, () ->
                manager.convergeStop(parseAndCheck(request.getCapability(), request.getNodeId())));
    }

    @Override
    public void freeze(FreezeReq request, StreamObserver<Ack> responseObserver) {
        respond(responseObserver, () ->
                manager.freeze(parseAndCheck(request.getCapability(), request.getNodeId()), request.getFrozen()));
    }

    /** 能力类型解析（失败抛 IllegalArgumentException）+ 目标节点校验（仅接受本节点）。 */
    private CapabilityType parseAndCheck(String capability, String nodeId) {
        CapabilityType type = CapabilityType.parse(capability);
        if (nodeId != null && !nodeId.isBlank() && !self.value().equals(nodeId)) {
            throw new IllegalArgumentException(
                    "command targets node " + nodeId + " but this server is " + self.value());
        }
        return type;
    }

    private static void respond(StreamObserver<Ack> responseObserver, Runnable command) {
        boolean ok;
        String message = "";
        try {
            command.run();
            ok = true;
        } catch (RuntimeException e) {
            ok = false;
            message = String.valueOf(e.getMessage());
        }
        responseObserver.onNext(Ack.newBuilder().setOk(ok).setMessage(message).build());
        responseObserver.onCompleted();
    }
}
