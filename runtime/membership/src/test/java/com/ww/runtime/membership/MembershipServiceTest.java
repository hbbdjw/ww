package com.ww.runtime.membership;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.MembershipEvent;
import com.ww.runtime.proto.MembershipGrpc;
import com.ww.runtime.proto.WatchReq;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * 进程内双实例互连集成测试 + 视图合并规则单测。
 * 短周期：interval=100ms，suspect=2（200ms），dead=4（400ms）。
 */
class MembershipServiceTest {

    private static final long INTERVAL_MS = 100L;
    private static final int SUSPECT_MISSES = 2;
    private static final int DEAD_MISSES = 4;

    private final List<GrpcServerHost> hosts = new ArrayList<>();
    private final List<MembershipService> services = new ArrayList<>();
    private final List<ManagedChannel> watchChannels = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (MembershipService service : services) {
            try {
                service.stop();
            } catch (RuntimeException ignored) {
                // teardown 尽力而为
            }
        }
        for (GrpcServerHost host : hosts) {
            host.stop();
            try {
                host.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (ManagedChannel channel : watchChannels) {
            channel.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // 测试节点装置
    // ------------------------------------------------------------------

    private static final class TestNode {
        final String id;
        final GrpcServerHost host = new GrpcServerHost();
        final MembershipService service;
        final List<MemberEvent> events = new CopyOnWriteArrayList<>();
        final Map<NodeId, List<NodeState>> sinkCalls = new ConcurrentHashMap<>();

        TestNode(String id, PeerDirectory directory) {
            this(id, directory, null);
        }

        TestNode(String id, PeerDirectory directory, java.util.function.IntSupplier selfPort) {
            this.id = id;
            this.service = new MembershipService(
                    NodeId.of(id),
                    () -> info(id, "localhost",
                            selfPort != null ? selfPort.getAsInt() : host.getPort(),
                            NodeState.ACTIVE, 0L),
                    directory,
                    (nodeId, state) -> sinkCalls
                            .computeIfAbsent(nodeId, k -> new CopyOnWriteArrayList<>())
                            .add(state),
                    host,
                    MembershipConfig.of(INTERVAL_MS, SUSPECT_MISSES, DEAD_MISSES),
                    SystemClock.instance());
            this.service.addListener(events::add);
        }

        NodeState stateOf(String nodeId) {
            NodeInfo info = service.view().get(NodeId.of(nodeId));
            return info == null ? null : info.state();
        }

        List<String> eventTypesFor(String nodeId) {
            return events.stream()
                    .filter(e -> e.node().id().value().equals(nodeId))
                    .map(MemberEvent::type)
                    .toList();
        }
    }

    private static NodeInfo info(String id, String host, int port, NodeState state, long epoch) {
        return NodeInfo.of(NodeId.of(id), host, port, state, epoch, 1.0,
                ResourceSnapshot.of(1.0, 1000, 100, 10_000, 1_000, 0L));
    }

    /** 对端目录：惰性读取对端实际端口；对端未启动时返回空列表（视为暂时不可达，不心跳）。 */
    private static List<NodeInfo> aliveInfoOf(TestNode other) {
        try {
            return List.of(info(other.id, "localhost", other.host.getPort(), NodeState.ACTIVE, 0L));
        } catch (IllegalStateException e) {
            return List.of();
        }
    }

    /** 创建互相指向对方的两个节点（不启动）。 */
    private TestNode[] createPair() {
        TestNode[] nodes = new TestNode[2];
        nodes[0] = new TestNode("A", () -> aliveInfoOf(nodes[1]));
        nodes[1] = new TestNode("B", () -> aliveInfoOf(nodes[0]));
        for (TestNode node : nodes) {
            services.add(node.service);
            hosts.add(node.host);
        }
        return nodes;
    }

    private void startAll(TestNode... nodes) throws IOException {
        // 先注册服务（service.start 内部 addService），再启动 host
        for (TestNode node : nodes) {
            node.service.start();
        }
        for (TestNode node : nodes) {
            node.host.start(0);
        }
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting: " + what, e);
            }
        }
        fail("timeout after " + timeoutMs + "ms waiting for: " + what);
    }

    private void awaitMutuallyActive(TestNode a, TestNode b) {
        awaitTrue(() -> a.stateOf("B") == NodeState.ACTIVE && b.stateOf("A") == NodeState.ACTIVE,
                10_000, "both nodes mutually ACTIVE");
    }

    // ------------------------------------------------------------------
    // 集成：互心跳
    // ------------------------------------------------------------------

    @Test
    void mutualHeartbeatMakesBothViewsContainEachOtherAsActive() throws Exception {
        TestNode[] pair = createPair();
        TestNode a = pair[0];
        TestNode b = pair[1];
        startAll(a, b);

        awaitMutuallyActive(a, b);

        Map<NodeId, NodeInfo> viewA = a.service.view();
        assertThat(viewA).containsKeys(NodeId.of("A"), NodeId.of("B"));
        assertThat(viewA.get(NodeId.of("B")).state()).isEqualTo(NodeState.ACTIVE);
        assertThat(viewA.get(NodeId.of("B")).grpcPort()).isEqualTo(b.host.getPort());

        Map<NodeId, NodeInfo> viewB = b.service.view();
        assertThat(viewB).containsKeys(NodeId.of("A"), NodeId.of("B"));
        assertThat(viewB.get(NodeId.of("A")).state()).isEqualTo(NodeState.ACTIVE);
        assertThat(viewB.get(NodeId.of("A")).grpcPort()).isEqualTo(a.host.getPort());

        assertThat(a.eventTypesFor("B")).contains(MemberEvent.NODE_ACTIVE);
        assertThat(b.eventTypesFor("A")).contains(MemberEvent.NODE_ACTIVE);
    }

    @Test
    void stopWaitsForInFlightHeartbeatTick() throws Exception {
        // A4：首次 peers() 阻塞在 gate 监视器上（monitor 等待不受 shutdownNow 中断影响），
        // 确定性地把一次心跳 tick 停在 directory.peers() 内
        Object gate = new Object();
        CountDownLatch entered = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        PeerDirectory blockingDir = () -> {
            if (first.compareAndSet(true, false)) {
                entered.countDown();
                synchronized (gate) {
                    // 等待主线程释放 gate
                }
            }
            return List.of();
        };

        TestNode node = new TestNode("solo", blockingDir);
        services.add(node.service);
        hosts.add(node.host);

        java.util.concurrent.CompletableFuture<Void> stopFuture =
                new java.util.concurrent.CompletableFuture<>();
        synchronized (gate) {
            node.service.start();
            node.host.start(0);
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("tick 进入 peers()").isTrue();
            // tick 被 gate 羁留：stop() 的 awaitTermination 必须等它，持锁期间不可能返回
            Thread stopper = new Thread(() -> {
                try {
                    node.service.stop();
                    stopFuture.complete(null);
                } catch (Throwable t) {
                    stopFuture.completeExceptionally(t);
                }
            });
            stopper.start();
            try {
                stopFuture.get(200, TimeUnit.MILLISECONDS);
                fail("stop() 应等待在途心跳 tick 终结");
            } catch (java.util.concurrent.TimeoutException expected) {
                // stop 尚未返回——正在等待在途 tick（预期）
            }
        }
        // gate 释放：tick 终结，stop 随后完成
        stopFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    void stopClearsChannelsAndIsIdempotent() throws Exception {
        // S21：先显式等待出站通道建立（视图 ACTIVE 可由入站心跳单方面确立，
        // 不保证本节点已建出站通道），再停机断言清空且不重建
        TestNode[] pair = createPair();
        startAll(pair);
        awaitChannelsNonEmpty(pair[0].service);
        assertThat(failureCountsOf(pair[0].service)).isNotNull();

        pair[0].service.stop();
        assertThat(channelsOf(pair[0].service)).isEmpty();
        assertThat(failureCountsOf(pair[0].service)).isEmpty();

        // 停机后执行器已终结：等待若干心跳周期，通道不得被重建
        Thread.sleep(INTERVAL_MS * 3);
        assertThat(channelsOf(pair[0].service)).isEmpty();

        pair[0].service.stop(); // 幂等
        assertThat(channelsOf(pair[0].service)).isEmpty();
    }

    /** 轮询等待出站通道建立（心跳每 {@value #INTERVAL_MS}ms 一轮，必然在有限轮内建通道）。 */
    private void awaitChannelsNonEmpty(MembershipService service) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (!channelsOf(service).isEmpty()) {
                    return;
                }
            } catch (Exception e) {
                throw new IllegalStateException("failed to read channels", e);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for channels", e);
            }
        }
        fail("timeout waiting for outbound channel to be established");
    }

    private static Map<?, ?> channelsOf(MembershipService service) throws Exception {
        java.lang.reflect.Field f = MembershipService.class.getDeclaredField("channels");
        f.setAccessible(true);
        return (Map<?, ?>) f.get(service);
    }

    private static Map<?, ?> failureCountsOf(MembershipService service) throws Exception {
        java.lang.reflect.Field f = MembershipService.class.getDeclaredField("consecutiveSendFailures");
        f.setAccessible(true);
        return (Map<?, ?>) f.get(service);
    }

    // ------------------------------------------------------------------
    // 集成：疑似 → 死亡（时序）
    // ------------------------------------------------------------------

    @Test
    void silentPeerIsSuspectedThenDeadInOrder() throws Exception {
        TestNode[] pair = createPair();
        TestNode a = pair[0];
        TestNode b = pair[1];
        startAll(a, b);
        awaitMutuallyActive(a, b);

        // B 完全静默：停心跳客户端并停 gRPC 服务；A 的目录仍视 B 为存活
        b.service.stop();
        b.host.stop();
        b.host.awaitTermination(3, TimeUnit.SECONDS);

        awaitTrue(() -> a.stateOf("B") == NodeState.SUSPECTED, 5_000, "A marks B SUSPECTED");
        awaitTrue(() -> a.stateOf("B") == NodeState.DEAD, 8_000, "A marks B DEAD");

        // 事件时序：先 SUSPECTED 后 DEAD
        List<String> types = a.eventTypesFor("B");
        int suspectIdx = types.indexOf(MemberEvent.NODE_SUSPECTED);
        int deadIdx = types.indexOf(MemberEvent.NODE_DEAD);
        assertThat(suspectIdx).as("SUSPECTED event fired: %s", types).isGreaterThanOrEqualTo(0);
        assertThat(deadIdx).as("DEAD event fired after SUSPECTED: %s", types).isGreaterThan(suspectIdx);

        // 状态回写时序：SUSPECTED 先于 DEAD
        List<NodeState> sink = a.sinkCalls.getOrDefault(NodeId.of("B"), List.of());
        assertThat(sink.indexOf(NodeState.SUSPECTED)).isGreaterThanOrEqualTo(0);
        assertThat(sink.indexOf(NodeState.DEAD)).isGreaterThan(sink.indexOf(NodeState.SUSPECTED));

        // A 自身仍存活于自身视图
        assertThat(a.stateOf("A")).isEqualTo(NodeState.ACTIVE);
    }

    // ------------------------------------------------------------------
    // 集成：恢复
    // ------------------------------------------------------------------

    @Test
    void recoveredPeerBecomesActiveAgainAfterRestart() throws Exception {
        TestNode[] pair = createPair();
        TestNode a = pair[0];
        TestNode b = pair[1];
        startAll(a, b);
        awaitMutuallyActive(a, b);

        // B 下线 → A 判 DEAD
        b.service.stop();
        b.host.stop();
        b.host.awaitTermination(3, TimeUnit.SECONDS);
        awaitTrue(() -> a.stateOf("B") == NodeState.DEAD, 8_000, "A marks B DEAD");
        List<String> typesBeforeRestart = a.eventTypesFor("B");
        int deadIdx = typesBeforeRestart.indexOf(MemberEvent.NODE_DEAD);
        assertThat(deadIdx).isGreaterThanOrEqualTo(0);

        // B 重启（随机新端口；A 的目录惰性读取新端口后自动重建 channel）
        b.service.start();
        b.host.start(0);

        awaitTrue(() -> a.stateOf("B") == NodeState.ACTIVE, 15_000, "B recovers to ACTIVE in A's view");

        // NODE_ACTIVE 事件出现在 NODE_DEAD 之后（恢复事件：视图经节点自报更新）
        List<String> types = a.eventTypesFor("B");
        assertThat(types.lastIndexOf(MemberEvent.NODE_ACTIVE))
                .as("recovery NODE_ACTIVE after NODE_DEAD: %s", types)
                .isGreaterThan(types.indexOf(MemberEvent.NODE_DEAD));

        // 状态回写不包含 DEAD→ACTIVE 复活：DEAD 为终态，复活须由节点重新注册
        // （DEAD→JOINING→ACTIVE），membership 只负责检测死亡，不负责复活回写
        List<NodeState> sink = a.sinkCalls.getOrDefault(NodeId.of("B"), List.of());
        assertThat(sink.indexOf(NodeState.DEAD)).isGreaterThanOrEqualTo(0);
        assertThat(sink.lastIndexOf(NodeState.ACTIVE))
                .as("membership must not write back DEAD -> ACTIVE revival: %s", sink)
                .isLessThan(sink.indexOf(NodeState.DEAD));
    }

    // ------------------------------------------------------------------
    // 集成：Watch 推送
    // ------------------------------------------------------------------

    @Test
    void watchClientReceivesMembershipEvents() throws Exception {
        TestNode[] pair = createPair();
        TestNode a = pair[0];
        TestNode b = pair[1];

        // 先只启动 A 并挂上 Watch，再启动 B，保证 B 加入事件必然发生在 Watch 建立之后
        a.service.start();
        a.host.start(0);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", a.host.getPort())
                .usePlaintext()
                .build();
        watchChannels.add(channel);

        CountDownLatch firstEvent = new CountDownLatch(1);
        List<MembershipEvent> received = new CopyOnWriteArrayList<>();
        MembershipGrpc.newStub(channel).watch(WatchReq.newBuilder().build(), new StreamObserver<>() {
            @Override
            public void onNext(MembershipEvent event) {
                received.add(event);
                firstEvent.countDown();
            }

            @Override
            public void onError(Throwable t) {
                // 服务端停止时可能收到结束，忽略
            }

            @Override
            public void onCompleted() {
                // 流结束
            }
        });

        b.service.start();
        b.host.start(0);

        awaitTrue(() -> a.stateOf("B") == NodeState.ACTIVE, 10_000, "B appears ACTIVE in A's view");
        assertThat(firstEvent.await(5, TimeUnit.SECONDS))
                .as("watch client should receive at least one event").isTrue();

        assertThat(received).isNotEmpty();
        assertThat(received.get(0).getType()).isEqualTo(MemberEvent.NODE_ACTIVE);
        assertThat(received.get(0).getNode().getNodeId()).isEqualTo("B");
    }

    // ------------------------------------------------------------------
    // 单测：悲观合并规则
    // ------------------------------------------------------------------

    @Test
    void pessimisticMergeRules() {
        TestNode s = new TestNode("S", List::of, () -> 9000);
        services.add(s.service);
        hosts.add(s.host);
        // 只启动服务执行器（dispatcher），不启动 gRPC host，直接驱动 merge
        s.service.start();
        try {
            MembershipService svc = s.service;

            // 缺失 → 采纳，新节点事件按状态映射
            svc.merge(info("X", "localhost", 9001, NodeState.ACTIVE, 5L), false);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.ACTIVE);
            awaitTrue(() -> s.eventTypesFor("X").size() >= 1, 2000, "event 1 (NODE_ACTIVE)");
            assertThat(s.eventTypesFor("X")).containsExactly(MemberEvent.NODE_ACTIVE);

            // 同 epoch：SUSPECTED（更悲观）胜 ACTIVE
            svc.merge(info("X", "localhost", 9001, NodeState.SUSPECTED, 5L), false);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.SUSPECTED);
            awaitTrue(() -> s.eventTypesFor("X").size() >= 2, 2000, "event 2 (NODE_SUSPECTED)");
            assertThat(s.eventTypesFor("X")).containsExactly(MemberEvent.NODE_ACTIVE, MemberEvent.NODE_SUSPECTED);
            assertThat(s.sinkCalls).doesNotContainKey(NodeId.of("X")); // gossip 合并不回写，仅本地故障检测回写

            // 同 epoch：ACTIVE（较不悲观）不覆盖 SUSPECTED
            svc.merge(info("X", "localhost", 9001, NodeState.ACTIVE, 5L), false);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.SUSPECTED);

            // 更高 epoch 覆盖（即使状态较不悲观）
            svc.merge(info("X", "localhost", 9001, NodeState.ACTIVE, 6L), false);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.ACTIVE);
            assertThat(svc.view().get(NodeId.of("X")).stateEpoch()).isEqualTo(6L);
            awaitTrue(() -> s.eventTypesFor("X").size() >= 3, 2000, "event 3 (epoch override NODE_ACTIVE)");

            // 例外：节点自报告（直证）同 epoch ACTIVE 覆盖本地 gossip SUSPECTED
            svc.merge(info("X", "localhost", 9001, NodeState.SUSPECTED, 6L), false);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.SUSPECTED);
            awaitTrue(() -> s.eventTypesFor("X").size() >= 4, 2000, "event 4 (gossip SUSPECTED)");
            svc.merge(info("X", "localhost", 9001, NodeState.ACTIVE, 6L), true);
            assertThat(svc.view().get(NodeId.of("X")).state()).isEqualTo(NodeState.ACTIVE);
            awaitTrue(() -> s.eventTypesFor("X").size() >= 5, 2000, "event 5 (self-report recovery)");
            assertThat(s.eventTypesFor("X")).containsExactly(
                    MemberEvent.NODE_ACTIVE,
                    MemberEvent.NODE_SUSPECTED,
                    MemberEvent.NODE_ACTIVE,
                    MemberEvent.NODE_SUSPECTED,
                    MemberEvent.NODE_ACTIVE);

            // 恢复（回到 ACTIVE）时回写宿主：高 epoch gossip 恢复一次 + 直证恢复一次
            List<NodeState> sink = s.sinkCalls.getOrDefault(NodeId.of("X"), List.of());
            assertThat(sink).containsExactly(NodeState.ACTIVE, NodeState.ACTIVE);

            // 自身条目不被他报覆盖
            svc.merge(info("S", "localhost", 9001, NodeState.DEAD, 99L), false);
            assertThat(svc.view().get(NodeId.of("S")).state()).isEqualTo(NodeState.ACTIVE);
            assertThat(svc.view().get(NodeId.of("S")).stateEpoch()).isZero();
        } finally {
            s.service.stop();
        }
    }
}
