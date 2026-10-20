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
 *
 * <p>S19：支持注入有界执行器（{@link #setExecutor}）——默认 grpc 内建 cached 线程池无界，
 * 单个卡死的控制面钩子（能力 start/stop）即可拖出线程膨胀；生产装配统一注入命名有界池。
 * 须在 {@code start()} 前调用；未注入时保持既有默认行为（测试/单机场景）。</p>
 */
public final class GrpcServerHost {

    /**
     * 默认单消息上限 8MB：存储转发（ForwardPut/Replicate）分片为 4MB 数据 + proto 头字段，
     * gRPC 默认 4MB 会拒收（RST_STREAM CANCEL），须为分片留出余量。
     */
    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 8 * 1024 * 1024;

    private final List<BindableService> services = new ArrayList<>();

    /** S19：宿主侧有界执行器（null = grpc 默认无界池）。 */
    private java.util.concurrent.Executor executor;

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

    /** S19：注入服务端执行器（仅 start() 前有效）。 */
    public synchronized GrpcServerHost setExecutor(java.util.concurrent.Executor executor) {
        if (running) {
            throw new IllegalStateException("GrpcServerHost already started");
        }
        this.executor = executor;
        return this;
    }

    public synchronized void start(int port) throws IOException {
        start(port, DEFAULT_MAX_INBOUND_MESSAGE_SIZE);
    }

    public synchronized void start(int port, int maxInboundMessageSize) throws IOException {
        if (running) {
            throw new IllegalStateException("GrpcServerHost already started");
        }
        if (maxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("maxInboundMessageSize must be > 0, got " + maxInboundMessageSize);
        }
        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .maxInboundMessageSize(maxInboundMessageSize);
        if (executor != null) {
            builder.executor(executor);
        }
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
