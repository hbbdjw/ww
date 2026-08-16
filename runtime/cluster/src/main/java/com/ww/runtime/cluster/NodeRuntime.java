package com.ww.runtime.cluster;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.grpc.GrpcServerHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单节点运行时：封装本节点的注册、gRPC 宿主生命周期、状态推进与周期心跳。
 *
 * <p>启动序列：register(JOINING) → 启动 gRPC 宿主（若未启动，port 0 支持随机端口，
 * 绑定后以实际端口重新 upsert）→ 自检通过后 markState(ACTIVE) → 启动心跳守护线程，
 * 周期执行 {@code registry.heartbeat(self, monitor.snapshot())}。</p>
 *
 * <p>优雅退出序列：markState(LEAVING) → 停心跳 → gRPC 宿主 stop → tombstone(DEAD，保留行)。
 * shutdown 幂等，重复调用无副作用。</p>
 */
public final class NodeRuntime {

    private static final Logger log = LoggerFactory.getLogger(NodeRuntime.class);

    private final NodeId self;
    private final String host;
    private final int configuredGrpcPort;
    private final double weight;
    private final NodeRegistry registry;
    private final GrpcServerHost grpcHost;
    private final ResourceMonitor monitor;
    private final Clock clock;
    private final long heartbeatIntervalMs;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private volatile ScheduledExecutorService heartbeatExecutor;

    public NodeRuntime(NodeId self,
                       String host,
                       int grpcPort,
                       double weight,
                       NodeRegistry registry,
                       GrpcServerHost grpcHost,
                       ResourceMonitor monitor,
                       Clock clock,
                       long heartbeatIntervalMs) {
        this.self = Objects.requireNonNull(self, "self");
        this.host = Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (grpcPort < 0 || grpcPort > 65535) {
            throw new IllegalArgumentException("grpcPort must be in [0, 65535], got " + grpcPort);
        }
        if (!Double.isFinite(weight) || weight <= 0) {
            throw new IllegalArgumentException("weight must be positive finite, got " + weight);
        }
        this.registry = Objects.requireNonNull(registry, "registry");
        this.grpcHost = Objects.requireNonNull(grpcHost, "grpcHost");
        this.monitor = Objects.requireNonNull(monitor, "monitor");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive, got " + heartbeatIntervalMs);
        }
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.configuredGrpcPort = grpcPort;
        this.weight = weight;
    }

    /**
     * 启动本节点：注册 JOINING → 启动 gRPC 宿主（若未启动）→ 自检 → ACTIVE → 启动心跳。
     */
    public synchronized void start() throws IOException {
        if (started.get()) {
            throw new IllegalStateException("NodeRuntime already started: " + self);
        }
        ResourceSnapshot initialSnapshot = monitor.snapshot();
        registry.register(NodeInfo.of(self, host, configuredGrpcPort,
                NodeState.JOINING, 0L, weight, initialSnapshot));

        if (!grpcHost.isRunning()) {
            grpcHost.start(configuredGrpcPort);
        }
        int actualPort = grpcHost.getPort();
        if (actualPort != configuredGrpcPort) {
            // 随机端口（0）绑定成功：以实际端口重新 upsert 注册信息（仍为 JOINING/epoch 0）
            registry.register(NodeInfo.of(self, host, actualPort,
                    NodeState.JOINING, 0L, weight, initialSnapshot));
        }
        if (!grpcHost.isRunning() || actualPort <= 0) {
            throw new IllegalStateException("gRPC self-check failed for " + self + ", port=" + actualPort);
        }
        registry.markState(self, NodeState.ACTIVE);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rt-heartbeat-" + self.value());
            thread.setDaemon(true);
            return thread;
        });
        heartbeatExecutor = executor;
        executor.scheduleAtFixedRate(this::heartbeatOnce,
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);

        started.set(true);
        log.info("node {} started: grpcPort={}, state=ACTIVE, heartbeatIntervalMs={}",
                self, actualPort, heartbeatIntervalMs);
    }

    /** 实际监听端口：宿主已启动时返回绑定端口（支持随机端口），否则返回构造时的配置端口。 */
    public int grpcPort() {
        return grpcHost.isRunning() ? grpcHost.getPort() : configuredGrpcPort;
    }

    /**
     * 优雅退出：markState(LEAVING) → 停心跳 → 停 gRPC 宿主 → tombstone(DEAD)。幂等。
     * <p>无论 start 是否成功均执行清理：启动失败残留的 JOINING 孤儿记录由此路径
     * tombstone 收敛（各步骤对未启动场景天然空安全：executor 判空、registry.get
     * 判空、tombstone 对不存在记录 no-op、GrpcServerHost.stop 对未启动实例安全）。</p>
     */
    public synchronized void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        // 1. LEAVING（仅当前状态允许该跃迁时推进；JOINING/DEAD 等场景直接由 tombstone 收敛）
        registry.get(self).ifPresent(info -> {
            NodeState state = info.state();
            if (state == NodeState.ACTIVE || state == NodeState.SUSPECTED) {
                try {
                    registry.markState(self, NodeState.LEAVING);
                } catch (RuntimeException e) {
                    // 并发推进（如已被判 DEAD）不应阻断退出路径
                    log.warn("node {} markState(LEAVING) skipped: {}", self, e.getMessage());
                }
            }
        });

        // 2. 停心跳线程
        ScheduledExecutorService executor = heartbeatExecutor;
        heartbeatExecutor = null;
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 3. 停 gRPC 宿主
        grpcHost.stop();

        // 4. tombstone：置 DEAD 并保留行
        registry.tombstone(self);
        log.info("node {} shut down: state=DEAD", self);
    }

    /** 单次心跳：任何异常只记日志，不中断调度循环。 */
    private void heartbeatOnce() {
        try {
            registry.heartbeat(self, monitor.snapshot());
        } catch (Exception e) {
            log.warn("node {} heartbeat failed, will retry: {}", self, e.toString(), e);
        }
    }
}
