package com.ww.runtime.server.grpc;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.proto.HealthGrpc;
import com.ww.runtime.proto.HealthReq;
import com.ww.runtime.proto.HealthRes;
import com.ww.runtime.scheduler.ReconcileLoop;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * Health gRPC 服务端：进程存活即健康（{@code healthy=true}），
 * detail 携带节点 id 与最近一周期的 leader 状态。
 */
public final class HealthServer extends HealthGrpc.HealthImplBase {

    private final NodeId self;
    private final ReconcileLoop reconcileLoop;

    public HealthServer(NodeId self, ReconcileLoop reconcileLoop) {
        this.self = Objects.requireNonNull(self, "self");
        this.reconcileLoop = Objects.requireNonNull(reconcileLoop, "reconcileLoop");
    }

    @Override
    public void check(HealthReq request, StreamObserver<HealthRes> responseObserver) {
        responseObserver.onNext(HealthRes.newBuilder()
                .setHealthy(true)
                .setDetail("node=" + self.value() + ", leader=" + reconcileLoop.isLeader())
                .build());
        responseObserver.onCompleted();
    }
}
