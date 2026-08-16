package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeRuntimeTest {

    private GrpcServerHost host;
    private InMemoryNodeRegistry registry;
    private TestClock clock;
    private SequenceMonitor monitor;
    private NodeRuntime runtime;

    @AfterEach
    void tearDown() {
        if (runtime != null) {
            runtime.shutdown();
        }
        if (host != null) {
            host.stop();
            try {
                host.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private NodeRuntime newRuntime(long heartbeatIntervalMs, long monitorFailureCount) {
        registry = new InMemoryNodeRegistry();
        clock = new TestClock(1_000L);
        monitor = new SequenceMonitor(clock, monitorFailureCount);
        host = new GrpcServerHost();
        runtime = new NodeRuntime(NodeId.of("n1"), "localhost", 0, 1.0,
                registry, host, monitor, clock, heartbeatIntervalMs);
        return runtime;
    }

    private Optional<NodeInfo> selfInfo() {
        return registry.get(NodeId.of("n1"));
    }

    // ---- start ----

    @Test
    void startRegistersThenActivatesWithActualRandomPort() throws Exception {
        NodeRuntime rt = newRuntime(1_000L, 0L);
        assertThat(rt.grpcPort()).isZero(); // 启动前返回配置端口

        rt.start();

        NodeInfo info = selfInfo().orElseThrow();
        assertThat(info.state()).isEqualTo(NodeState.ACTIVE);
        assertThat(info.stateEpoch()).isEqualTo(1L); // JOINING(0) -> ACTIVE(1)
        assertThat(info.host()).isEqualTo("localhost");
        assertThat(info.weight()).isEqualTo(1.0);

        assertThat(host.isRunning()).isTrue();
        assertThat(rt.grpcPort()).isPositive();
        assertThat(info.grpcPort()).isEqualTo(rt.grpcPort()); // 注册表记录实际端口
    }

    @Test
    void startReusesPreStartedHostAndRegistersActualPort() throws Exception {
        registry = new InMemoryNodeRegistry();
        clock = new TestClock(1_000L);
        monitor = new SequenceMonitor(clock, 0L);
        host = new GrpcServerHost();
        host.start(0);
        int boundPort = host.getPort();

        runtime = new NodeRuntime(NodeId.of("n1"), "localhost", 0, 1.0,
                registry, host, monitor, clock, 60_000L);
        runtime.start();

        assertThat(runtime.grpcPort()).isEqualTo(boundPort);
        assertThat(selfInfo().orElseThrow().grpcPort()).isEqualTo(boundPort);
        assertThat(selfInfo().orElseThrow().state()).isEqualTo(NodeState.ACTIVE);
    }

    @Test
    void doubleStartThrowsIllegalState() throws Exception {
        NodeRuntime rt = newRuntime(1_000L, 0L);
        rt.start();
        assertThatThrownBy(rt::start).isInstanceOf(IllegalStateException.class);
    }

    // ---- heartbeat ----

    @Test
    void heartbeatLoopPublishesSnapshotsAndSurvivesMonitorFailures() throws Exception {
        // 调用 1 = 注册快照；心跳从调用 2 起，其中 2、3 抛异常（仅记日志），4 起成功
        NodeRuntime rt = newRuntime(20L, 2L);
        rt.start();

        await(() -> selfInfo().map(i -> i.resources().memUsedBytes()).orElse(0L) >= 4L);

        assertThat(monitor.calls.get()).isGreaterThanOrEqualTo(4L);
        assertThat(selfInfo().orElseThrow().state()).isEqualTo(NodeState.ACTIVE);
    }

    @Test
    void shutdownStopsHeartbeatLoop() throws Exception {
        NodeRuntime rt = newRuntime(30L, 0L);
        rt.start();
        await(() -> selfInfo().map(i -> i.resources().memUsedBytes()).orElse(0L) >= 2L);

        rt.shutdown();
        long callsAfterShutdown = monitor.calls.get();

        Thread.sleep(200L); // 远大于心跳周期，不应再有采样
        assertThat(monitor.calls.get()).isEqualTo(callsAfterShutdown);
    }

    // ---- shutdown ----

    @Test
    void shutdownGoesLeavingThenDeadAndStopsHost() throws Exception {
        NodeRuntime rt = newRuntime(1_000L, 0L);
        rt.start();
        assertThat(selfInfo().orElseThrow().state()).isEqualTo(NodeState.ACTIVE);

        rt.shutdown();

        NodeInfo dead = selfInfo().orElseThrow();
        assertThat(dead.state()).isEqualTo(NodeState.DEAD);
        assertThat(dead.stateEpoch()).isEqualTo(3L); // ACTIVE(1) -> LEAVING(2) -> DEAD(3)
        assertThat(host.isRunning()).isFalse();
        assertThat(rt.grpcPort()).isZero(); // 未运行回退到配置端口

        // 幂等：重复 shutdown 无副作用
        rt.shutdown();
        assertThat(selfInfo().orElseThrow().stateEpoch()).isEqualTo(3L);
    }

    @Test
    void shutdownBeforeStartIsNoOp() {
        NodeRuntime rt = newRuntime(1_000L, 0L);
        rt.shutdown();
        assertThat(selfInfo()).isEmpty();
        assertThat(host.isRunning()).isFalse();
    }

    // ---- helpers ----

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("condition not met within 10s");
    }

    /** 序列化监控桩：memUsed = 调用序号，可配置从第 2 次调用起连续失败若干次。 */
    private static final class SequenceMonitor implements ResourceMonitor {

        private final TestClock clock;
        private final long failureCount;
        final AtomicLong calls = new AtomicLong();

        SequenceMonitor(TestClock clock, long failureCount) {
            this.clock = clock;
            this.failureCount = failureCount;
        }

        @Override
        public ResourceSnapshot snapshot() {
            long n = calls.incrementAndGet();
            if (failureCount > 0 && n >= 2 && n < 2 + failureCount) {
                throw new IllegalStateException("simulated monitor failure #" + n);
            }
            return ResourceSnapshot.of(5.0, 1_000_000L, n, 2_000_000L, n, clock.nowMillis());
        }
    }
}
