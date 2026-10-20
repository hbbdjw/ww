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
 * 绑定后以实际端口重新 upsert）→ 自检通过后 markState(ACTIVE) → 启动后台采样与心跳守护线程。
 * 心跳线程仅读采样缓存（S13：文件系统采样不再钉住心跳节拍）；
 * 心跳线程同时做自我状态监测（S2：被判 DEAD 且进程健康 → 自动重注册复活）。</p>
 *
 * <p>启动失败内联回滚（深审6）：register 之后的任何失败均 tombstone 自身，
 * 不留 JOINING 孤儿污染伸缩视图。</p>
 *
 * <p>优雅退出序列：markState(LEAVING) → 停心跳 → gRPC 宿主 stop → tombstone(DEAD，保留行)。
 * shutdown 幂等，重复调用无副作用。</p>
 */
public final class NodeRuntime {

    private static final Logger log = LoggerFactory.getLogger(NodeRuntime.class);

    /** S2 自我状态监测间隔 = 心跳间隔 × 5（§3.12：周期查自身注册行，DEAD 即自动重注册）。 */
    private static final int SELF_CHECK_TICKS = 5;

    private final NodeId self;
    private final String host;
    private final int configuredGrpcPort;
    private final int httpPort;
    private final double weight;
    /** 故障域（P1-9）：随注册/心跳传播，放置副本反亲和用；空串 = 未配置。 */
    private volatile String zone = "";
    private final NodeRegistry registry;
    private final GrpcServerHost grpcHost;
    private final ResourceMonitor monitor;
    private final Clock clock;
    private final long heartbeatIntervalMs;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private volatile ScheduledExecutorService heartbeatExecutor;
    private volatile ScheduledExecutorService samplerExecutor;
    /** S13：最近一次后台采样结果；心跳只读该缓存（首次心跳前由 start 的初始快照兜底）。 */
    private volatile ResourceSnapshot latestSnapshot;
    private final java.util.concurrent.atomic.AtomicLong heartbeatTicks =
            new java.util.concurrent.atomic.AtomicLong();

    public NodeRuntime(NodeId self,
                       String host,
                       int grpcPort,
                       int httpPort,
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
        if (httpPort < 0 || httpPort > 65535) {
            throw new IllegalArgumentException("httpPort must be in [0, 65535], got " + httpPort);
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
        this.httpPort = httpPort;
        this.weight = weight;
    }

    /** 故障域（机架/可用区）：注册前设置，空串 = 未配置（放置不做反亲和）。 */
    public synchronized NodeRuntime withZone(String zone) {
        this.zone = zone == null ? "" : zone;
        return this;
    }

    /**
     * 启动本节点：注册 JOINING → 启动 gRPC 宿主（若未启动）→ 自检 → ACTIVE → 启动采样/心跳。
     * <p>深审6：register 之后的任何失败在异常路径内联 tombstone 自身（异常照常上抛），
     * 不留 JOINING 孤儿。</p>
     */
    public synchronized void start() throws IOException {
        if (started.get()) {
            throw new IllegalStateException("NodeRuntime already started: " + self);
        }
        ResourceSnapshot initialSnapshot = monitor.snapshot();
        this.latestSnapshot = initialSnapshot;
        registry.register(NodeInfo.of(self, host, configuredGrpcPort, httpPort,
                NodeState.JOINING, 0L, weight, initialSnapshot, zone));

        boolean hostPreRunning = grpcHost.isRunning();
        try {
            if (!hostPreRunning) {
                grpcHost.start(configuredGrpcPort);
            }
            int actualPort = grpcHost.getPort();
            if (actualPort != configuredGrpcPort) {
                // 随机端口（0）绑定成功：以实际端口重新 upsert 注册信息（仍为 JOINING/epoch 0）
                registry.register(NodeInfo.of(self, host, actualPort, httpPort,
                        NodeState.JOINING, 0L, weight, initialSnapshot, zone));
            }
            if (!grpcHost.isRunning() || actualPort <= 0) {
                throw new IllegalStateException("gRPC self-check failed for " + self + ", port=" + actualPort);
            }
            registry.markState(self, NodeState.ACTIVE);
        } catch (IOException | RuntimeException startFailure) {
            // 深审6 内联回滚：注册行已存在则必须 tombstone，孤儿 JOINING 会进入
            // listAlive → 放置候选（写命令石沉大海、治愈永无休止）；清理失败不掩盖原异常
            if (!hostPreRunning) {
                try {
                    grpcHost.stop(); // 仅回收本路径启动的宿主
                } catch (RuntimeException cleanup) {
                    startFailure.addSuppressed(cleanup);
                }
            }
            try {
                registry.tombstone(self);
            } catch (RuntimeException cleanup) {
                startFailure.addSuppressed(cleanup);
            }
            throw startFailure;
        }

        // S13：资源采样移到独立线程——File.getTotalSpace/UsableSpace 在盘满/网络盘场景
        // 可阻塞秒级，旧实现与心跳同线程，拖垮续约节拍引发误判 DEAD
        ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rt-sampler-" + self.value());
            thread.setDaemon(true);
            return thread;
        });
        samplerExecutor = sampler;
        sampler.scheduleWithFixedDelay(this::sampleOnce,
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rt-heartbeat-" + self.value());
            thread.setDaemon(true);
            return thread;
        });
        heartbeatExecutor = executor;
        executor.scheduleAtFixedRate(this::heartbeatOnce,
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);

        started.set(true);
        int boundPort = grpcHost.isRunning() ? grpcHost.getPort() : configuredGrpcPort;
        log.info("node {} started: grpcPort={}, state=ACTIVE, heartbeatIntervalMs={}",
                self, boundPort, heartbeatIntervalMs);
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

        // 2. 停采样与心跳线程
        ScheduledExecutorService sampler = samplerExecutor;
        samplerExecutor = null;
        if (sampler != null) {
            sampler.shutdownNow();
        }
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

    /** S13：后台采样任务——任何异常只记日志，保留上一份快照继续供心跳读取。 */
    private void sampleOnce() {
        try {
            latestSnapshot = monitor.snapshot();
        } catch (Exception e) {
            log.warn("node {} resource sampling failed, keeping last snapshot: {}", self, e.toString());
        }
    }

    /** 单次心跳：读采样缓存上报（S13 不再同步触碰文件系统）；异常只记日志，不中断调度循环。 */
    private void heartbeatOnce() {
        try {
            ResourceSnapshot snapshot = latestSnapshot;
            if (snapshot == null) {
                snapshot = monitor.snapshot();
                latestSnapshot = snapshot;
            }
            registry.heartbeat(self, snapshot);
            // S2 自我监测：每 SELF_CHECK_TICKS 拍检查自身注册行——被误判 DEAD
            // （长 GC/网络抖动超 deadTimeout）且进程健康 → 自动重注册复活
            if (heartbeatTicks.incrementAndGet() % SELF_CHECK_TICKS == 0
                    && started.get() && grpcHost.isRunning()) {
                registry.get(self).ifPresent(info -> {
                    if (info.state() == NodeState.DEAD) {
                        log.warn("node {} found DEAD in registry while healthy (misjudged pause), "
                                + "auto re-registering", self);
                        reRegister();
                    }
                });
            }
        } catch (Exception e) {
            log.warn("node {} heartbeat failed, will retry: {}", self, e.toString(), e);
        }
    }

    /**
     * S2（§3.12①）：自动重注册复活。register 重置 state_epoch（合法复活路径：
     * DEAD→JOINING 仅经重新注册），自检后立即归位 ACTIVE；心跳线程已在运行，
     * 自报直证即在各 peer 视图复活。
     * <p>管理端点（revive）与启动期自检失败后的恢复均复用本方法；幂等由状态机保证。</p>
     */
    public synchronized void reRegister() {
        ResourceSnapshot snapshot = monitor.snapshot();
        latestSnapshot = snapshot;
        registry.register(NodeInfo.of(self, host, grpcPort(), httpPort,
                NodeState.JOINING, 0L, weight, snapshot, zone));
        registry.markState(self, NodeState.ACTIVE);
        log.info("node {} re-registered after DEAD verdict (epoch reset), state=ACTIVE", self);
    }
}
