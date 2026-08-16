package com.ww.runtime.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 轻量 gRPC 服务器宿主：聚合多个 {@link BindableService} 并管理生命周期。
 * <p>支持 port 0（由操作系统分配随机端口，启动后经 {@link #getPort()} 读取实际端口）。</p>
 */
public final class GrpcServerHost {

    private final List<BindableService> services = new ArrayList<>();

    private Server server;
    private boolean running;

    public synchronized GrpcServerHost addService(BindableService service) {
        if (service == null) {
            throw new IllegalArgumentException("service must not be null");
        }
        if (running) {
            throw new IllegalStateException("GrpcServerHost already started");
        }
        services.add(service);
        return this;
    }

    public synchronized void start(int port) throws IOException {
        if (running) {
            throw new IllegalStateException("GrpcServerHost already started");
        }
        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        for (BindableService service : services) {
            builder.addService(service);
        }
        server = builder.build().start();
        running = true;
    }

    public synchronized void stop() {
        running = false;
        Server current = server;
        if (current != null) {
            current.shutdown();
        }
    }

    public synchronized void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        Server current = server;
        if (current != null) {
            current.awaitTermination(timeout, unit);
        }
    }

    public synchronized int getPort() {
        if (!running || server == null) {
            throw new IllegalStateException("GrpcServerHost not started");
        }
        return server.getPort();
    }

    public synchronized boolean isRunning() {
        return running;
    }
}
