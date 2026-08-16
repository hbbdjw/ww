package com.ww.rpc.grpc;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.grpc.proto.RpcMessage;
import com.ww.rpc.grpc.proto.RpcResponseMessage;
import com.ww.rpc.grpc.proto.UnifiedRpcServiceGrpc;
import com.ww.rpc.handler.RequestHandlerRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * gRPC 服务端：暴露 UnifiedRpcService，接收入站 RPC 请求并分派到对应 handler。
 */
public class GrpcServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

    private final int port;
    private final RequestHandlerRegistry handlerRegistry;
    private Server server;

    public GrpcServer(int port, RequestHandlerRegistry handlerRegistry) {
        this.port = port;
        this.handlerRegistry = handlerRegistry;
    }

    /**
     * 启动服务端。
     */
    public void start() throws IOException {
        server = NettyServerBuilder.forPort(port)
                .addService(new UnifiedRpcServiceImpl(handlerRegistry))
                .build()
                .start();
        LOG.info("GrpcServer 启动，监听端口 {}", port);
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return server != null && !server.isShutdown();
    }

    /**
     * 阻塞直到服务关闭。
     */
    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server.shutdown();
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
                LOG.info("GrpcServer 已关闭（端口 {})", port);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
    }

    /**
     * gRPC 服务实现：接收 RpcMessage，根据 serviceName 分派到 RequestHandlerRegistry。
     */
    static final class UnifiedRpcServiceImpl extends UnifiedRpcServiceGrpc.UnifiedRpcServiceImplBase {

        private final RequestHandlerRegistry registry;

        UnifiedRpcServiceImpl(RequestHandlerRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void unaryCall(RpcMessage request, StreamObserver<RpcResponseMessage> responseObserver) {
            String serviceName = request.getServiceName();
            String requestId = request.getRequestId();

            try {
                RpcRequest rpcRequest = Marshaller.fromProto(request);
                if (!registry.hasHandler(serviceName)) {
                    RpcResponse notFound = RpcResponse.error(requestId,
                            "no handler registered for service: " + serviceName);
                    responseObserver.onNext(Marshaller.toProto(notFound));
                    responseObserver.onCompleted();
                    return;
                }

                RpcResponse rpcResponse = registry.handle(rpcRequest);
                responseObserver.onNext(Marshaller.toProto(rpcResponse));
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("处理 RPC 请求异常 serviceName={}, requestId={}", serviceName, requestId, e);
                RpcResponse err = RpcResponse.error(requestId, "internal error: " + e.getMessage());
                responseObserver.onNext(Marshaller.toProto(err));
                responseObserver.onCompleted();
            }
        }
    }
}
