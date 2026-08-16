package com.ww.runtime.membership;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.HeartbeatAck;
import com.ww.runtime.proto.HeartbeatReq;
import com.ww.runtime.proto.MembershipEvent;
import com.ww.runtime.proto.MembershipGrpc;
import com.ww.runtime.proto.NodeInfoP;
import com.ww.runtime.proto.ResourceSnapshotP;
import com.ww.runtime.proto.WatchReq;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * 成员关系服务：gRPC Membership 服务端 + 心跳客户端一体。
 *
 * <p>心跳协议：每周期对 {@link PeerDirectory} 返回的各 peer 建立双向流，发送一条携带自身信息的
 * {@code HeartbeatReq} 并半关闭；对端对每条请求合并自报信息（直证规则）后回一条携带其全量视图的
 * {@code HeartbeatAck}。收到 ack 后按 gossip 悲观合并规则吸收对端视图。</p>
 *
 * <p>合并规则：</p>
 * <ul>
 *   <li>本地缺失 → 采纳；</li>
 *   <li>{@code stateEpoch} 更高 → 采纳；</li>
 *   <li>同 epoch → 更悲观状态胜（ACTIVE(0) &lt; 其他(1) &lt; DEAD(2)）；</li>
 *   <li>例外：信息来自节点自身直报（心跳自报，或 ack 中 member 即 ack 来源节点自身）时，
 *       始终采纳（源节点是自身状态的绝对权威；不比 epoch，避免重启后 epoch 重置
 *       被残留高 epoch DEAD 条目永久压制）。</li>
 * </ul>
 *
 * <p>故障检测：为每个 peer 记录最近收到消息时间（收到对端心跳、或本节点发出的请求得到 ack 均算）。
 * 静默超过 {@code suspectAfterMisses * interval} → SUSPECTED；超过 {@code deadAfterMisses * interval}
 * → DEAD；重新收到消息 → ACTIVE。每次状态变化均回写 {@link StateSink} 并广播事件。</p>
 *
 * <p>生命周期：宿主必须先调用 {@link #start()}（内部将本服务 {@code addService} 到宿主
 * {@link GrpcServerHost}），再调用 {@code host.start(port)}，最后由宿主自行 {@code host.stop()}。
 * {@code GrpcServerHost} 未启动时才允许 addService，顺序由宿主控制。</p>
 */
public final class MembershipService extends MembershipGrpc.MembershipImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MembershipService.class);

    /** 连续发送失败多少次后重建 peer channel（见 consecutiveSendFailures）。 */
    private static final int MAX_CONSECUTIVE_SEND_FAILURES = 3;

    private final NodeId self;
    private final Supplier<NodeInfo> selfInfo;
    private final PeerDirectory directory;
    private final StateSink stateSink;
    private final GrpcServerHost host;
    private final MembershipConfig config;
    private final Clock clock;

    private final Map<NodeId, NodeInfo> view = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> lastHeard = new ConcurrentHashMap<>();
    private final Map<NodeId, PeerChannel> channels = new ConcurrentHashMap<>();
    /** 每 peer 连续发送失败计数（收到 ack 清零）：超阈值重建 channel，
     *  规避 JVM DNS 负缓存导致 channel 永久卡在 Unable to resolve host 的问题。 */
    private final Map<NodeId, Integer> consecutiveSendFailures = new ConcurrentHashMap<>();
    private final List<Consumer<MemberEvent>> listeners = new CopyOnWriteArrayList<>();
    private final List<StreamObserver<MembershipEvent>> watchers = new CopyOnWriteArrayList<>();
    private final Object viewLock = new Object();

    private ScheduledExecutorService heartbeatExecutor;
    private ExecutorService eventDispatcher;
    private volatile boolean started;
    private boolean serviceRegistered;

    public MembershipService(NodeId self,
                             Supplier<NodeInfo> selfInfo,
                             PeerDirectory directory,
                             StateSink stateSink,
                             GrpcServerHost host,
                             MembershipConfig config,
                             Clock clock) {
        this.self = java.util.Objects.requireNonNull(self, "self");
        this.selfInfo = java.util.Objects.requireNonNull(selfInfo, "selfInfo");
        this.directory = java.util.Objects.requireNonNull(directory, "directory");
        this.stateSink = java.util.Objects.requireNonNull(stateSink, "stateSink");
        this.host = java.util.Objects.requireNonNull(host, "host");
        this.config = java.util.Objects.requireNonNull(config, "config");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    /**
     * 启动服务：将本 gRPC 服务注册到宿主 {@link GrpcServerHost}（宿主随后自行 {@code host.start(port)}），
     * 并启动心跳客户端守护线程（单执行器）。
     * <p>注意：{@code GrpcServerHost} 只能在未启动时注册服务，故宿主必须先调用本方法再启动 host。</p>
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        if (!serviceRegistered) {
            host.addService(this);
            serviceRegistered = true;
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "membership-heartbeat-" + self.value());
            t.setDaemon(true);
            return t;
        });
        eventDispatcher = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "membership-events-" + self.value());
            t.setDaemon(true);
            return t;
        });
        refreshSelf();
        heartbeatExecutor.scheduleWithFixedDelay(
                MembershipService.this::heartbeatTick, 0L, config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        started = true;
        LOG.info("membership service started: self={}, interval={}ms", self, config.heartbeatIntervalMs());
    }

    /** 停止服务：停心跳线程、关闭全部 peer channel、完成 Watch 流。视图与计时状态清空。 */
    public synchronized void stop() {
        started = false;
        ScheduledExecutorService hb = heartbeatExecutor;
        heartbeatExecutor = null;
        if (hb != null) {
            hb.shutdownNow();
        }
        for (PeerChannel pc : channels.values()) {
            pc.close();
        }
        channels.clear();
        List<StreamObserver<MembershipEvent>> toComplete = new ArrayList<>(watchers);
        watchers.clear();
        ExecutorService dispatcher = eventDispatcher;
        eventDispatcher = null;
        if (dispatcher != null) {
            dispatcher.shutdown();
            try {
                if (!dispatcher.awaitTermination(2, TimeUnit.SECONDS)) {
                    dispatcher.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                dispatcher.shutdownNow();
            }
        }
        for (StreamObserver<MembershipEvent> w : toComplete) {
            try {
                w.onCompleted();
            } catch (RuntimeException e) {
                LOG.debug("watch observer complete failed on stop: {}", e.toString());
            }
        }
        view.clear();
        lastHeard.clear();
        LOG.info("membership service stopped: self={}", self);
    }

    /** 当前视图的不可变快照（含自身）。 */
    public Map<NodeId, NodeInfo> view() {
        return Map.copyOf(view);
    }

    /** 注册成员事件监听器；监听器在独立分发线程调用，不得长时间阻塞。 */
    public void addListener(Consumer<MemberEvent> listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    // ------------------------------------------------------------------
    // gRPC 服务端
    // ------------------------------------------------------------------

    @Override
    public StreamObserver<HeartbeatReq> heartbeat(StreamObserver<HeartbeatAck> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatReq request) {
                NodeInfo reported = fromProto(request.getSelf());
                if (reported != null) {
                    lastHeard.put(reported.id(), clock.nowMillis());
                    merge(reported, true);
                }
                responseObserver.onNext(buildAck());
            }

            @Override
            public void onError(Throwable t) {
                LOG.debug("heartbeat stream from peer terminated: {}", t.toString());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void watch(WatchReq request, StreamObserver<MembershipEvent> responseObserver) {
        watchers.add(responseObserver);
        if (responseObserver instanceof ServerCallStreamObserver<?> sco) {
            sco.setOnCancelHandler(() -> watchers.remove(responseObserver));
        }
        LOG.debug("watch registered, total={}", watchers.size());
    }

    private HeartbeatAck buildAck() {
        refreshSelf();
        HeartbeatAck.Builder builder = HeartbeatAck.newBuilder();
        for (NodeInfo info : view.values()) {
            builder.addMembers(toProto(info));
        }
        return builder.build();
    }

    // ------------------------------------------------------------------
    // 心跳客户端 + 故障检测
    // ------------------------------------------------------------------

    private void heartbeatTick() {
        if (!started) {
            return;
        }
        try {
            refreshSelf();
            List<NodeInfo> peers;
            try {
                peers = directory.peers();
            } catch (RuntimeException e) {
                LOG.warn("peer directory failed: {}", e.toString());
                peers = List.of();
            }
            Set<NodeId> activeIds = new HashSet<>();
            for (NodeInfo peer : peers) {
                if (peer == null || peer.id().equals(self)) {
                    continue;
                }
                activeIds.add(peer.id());
                try {
                    sendHeartbeat(peer);
                } catch (RuntimeException e) {
                    LOG.debug("send heartbeat to {} failed: {}", peer.id(), e.toString());
                }
            }
            // 目录中已消失的节点：关闭其 channel
            for (Iterator<Map.Entry<NodeId, PeerChannel>> it = channels.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<NodeId, PeerChannel> entry = it.next();
                if (!activeIds.contains(entry.getKey())) {
                    entry.getValue().close();
                    it.remove();
                }
            }
            detectFailures(clock.nowMillis());
        } catch (RuntimeException e) {
            LOG.warn("heartbeat tick error: {}", e.toString());
        }
    }

    private void sendHeartbeat(NodeInfo peer) {
        PeerChannel pc = channelFor(peer);
        NodeId peerId = peer.id();
        MembershipGrpc.MembershipStub stub = MembershipGrpc.newStub(pc.channel);
        StreamObserver<HeartbeatReq> requestStream = stub.heartbeat(new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatAck ack) {
                long heardAt = clock.nowMillis();
                lastHeard.put(peerId, heardAt);
                consecutiveSendFailures.remove(peerId);
                for (NodeInfoP memberP : ack.getMembersList()) {
                    NodeInfo member = fromProto(memberP);
                    if (member != null) {
                        // 例外：member 即心跳对端自身 → 对端自报，走直证规则
                        merge(member, member.id().equals(peerId));
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.debug("heartbeat to {} failed: {}", peerId, t.toString());
                pc.resetConnectBackoff();
                // 连续失败超阈值：判定 channel 已进入不可自愈状态（典型如首次 DNS 解析失败后
                // JVM 负缓存永久生效，对端后启动也永远解析不到），关闭并从缓存移除，
                // 下个 tick 的 channelFor 将重建新 channel 重新解析。
                int fails = consecutiveSendFailures.merge(peerId, 1, Integer::sum);
                if (fails >= MAX_CONSECUTIVE_SEND_FAILURES) {
                    LOG.warn("heartbeat to {} failed {} times consecutively, recreating channel", peerId, fails);
                    channels.remove(peerId, pc);
                    pc.close();
                    consecutiveSendFailures.remove(peerId);
                }
            }

            @Override
            public void onCompleted() {
                // 单轮请求-应答结束
            }
        });
        requestStream.onNext(HeartbeatReq.newBuilder().setSelf(toProto(currentSelfInfo())).build());
        requestStream.onCompleted();
    }

    private PeerChannel channelFor(NodeInfo peer) {
        PeerChannel existing = channels.get(peer.id());
        if (existing != null && existing.host.equals(peer.host()) && existing.port == peer.grpcPort()) {
            return existing;
        }
        if (existing != null) {
            existing.close();
        }
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(peer.host(), peer.grpcPort())
                .usePlaintext()
                .build();
        PeerChannel created = new PeerChannel(peer.host(), peer.grpcPort(), channel);
        channels.put(peer.id(), created);
        return created;
    }

    private void detectFailures(long now) {
        for (NodeInfo info : view.values()) {
            NodeId id = info.id();
            if (id.equals(self)) {
                continue;
            }
            // 仅监控曾直接通信过的节点（心跳对端）；纯 gossip 得知的节点无本地信号，不做判定
            Long last = lastHeard.get(id);
            if (last == null) {
                continue;
            }
            long silence = now - last;
            if (silence > config.deadTimeoutMs()) {
                transitionLocal(id, NodeState.DEAD);
            } else if (silence > config.suspectTimeoutMs()) {
                if (info.state() != NodeState.SUSPECTED && info.state() != NodeState.DEAD) {
                    transitionLocal(id, NodeState.SUSPECTED);
                }
            } else if (info.state() == NodeState.SUSPECTED || info.state() == NodeState.DEAD) {
                transitionLocal(id, NodeState.ACTIVE);
            }
        }
    }

    // ------------------------------------------------------------------
    // 视图合并（gossip 悲观合并 + 直证例外）
    // ------------------------------------------------------------------

    /**
     * 合并一条节点信息到本地视图；返回是否触发事件。
     *
     * @param incoming         对端报告的节点信息
     * @param directFromSource true 表示该信息来自节点自身直报（心跳自报 / ack 中对端自身条目）
     */
    void merge(NodeInfo incoming, boolean directFromSource) {
        if (incoming.id().equals(self)) {
            // 自身信息以本地为准，忽略他报
            return;
        }
        MemberEvent event = null;
        boolean recoveredToActive = false;
        synchronized (viewLock) {
            NodeInfo local = view.get(incoming.id());
            if (local == null) {
                view.put(incoming.id(), incoming);
                event = new MemberEvent(eventType(incoming.state()), incoming);
            } else if (accept(local, incoming, directFromSource) && !incoming.equals(local)) {
                view.put(incoming.id(), incoming);
                event = new MemberEvent(
                        local.state() == incoming.state() ? MemberEvent.NODE_UPDATED : eventType(incoming.state()),
                        incoming);
                // 恢复：先前被判 SUSPECTED/DEAD 的节点重新报来 ACTIVE → 回写宿主
                recoveredToActive = incoming.state() == NodeState.ACTIVE && local.state() != NodeState.ACTIVE;
            }
        }
        if (recoveredToActive) {
            try {
                stateSink.apply(incoming.id(), NodeState.ACTIVE);
            } catch (RuntimeException e) {
                LOG.warn("stateSink apply failed for {} -> ACTIVE: {}", incoming.id(), e.toString());
            }
        }
        if (event != null) {
            LOG.debug("member view updated: {} -> {}", event.type(), event.node().id());
            dispatch(event);
        }
    }

    private static boolean accept(NodeInfo local, NodeInfo incoming, boolean directFromSource) {
        if (directFromSource) {
            // 源节点自报：自身状态绝对权威（直连心跳流证明其存活）。
            // 不比 epoch：节点重启后 register 会重置 epoch，若要求 epoch 不低于本地，
            // peer 视图里残留的高 epoch DEAD 条目会永久压制重启节点的自报，导致 DEAD 无法自愈。
            return true;
        }
        if (incoming.stateEpoch() > local.stateEpoch()) {
            return true;
        }
        if (incoming.stateEpoch() < local.stateEpoch()) {
            return false;
        }
        return pessimism(incoming.state()) > pessimism(local.state());
    }

    /** 悲观序：ACTIVE(0) &lt; 其他(1) &lt; DEAD(2)。 */
    private static int pessimism(NodeState state) {
        return switch (state) {
            case ACTIVE -> 0;
            case DEAD -> 2;
            default -> 1;
        };
    }

    private static String eventType(NodeState state) {
        return switch (state) {
            case ACTIVE -> MemberEvent.NODE_ACTIVE;
            case SUSPECTED -> MemberEvent.NODE_SUSPECTED;
            case DEAD -> MemberEvent.NODE_DEAD;
            default -> MemberEvent.NODE_UPDATED;
        };
    }

    /** 本地故障判定导致的状态迁移（不提升 epoch，等待节点自报恢复）。 */
    private void transitionLocal(NodeId id, NodeState target) {
        NodeInfo updated;
        synchronized (viewLock) {
            NodeInfo current = view.get(id);
            if (current == null || current.state() == target) {
                return;
            }
            updated = NodeInfo.of(current.id(), current.host(), current.grpcPort(), target,
                    current.stateEpoch(), current.weight(), current.resources());
            view.put(id, updated);
        }
        LOG.debug("peer {} transitioned to {} (local detection)", id, target);
        try {
            stateSink.apply(id, target);
        } catch (RuntimeException e) {
            LOG.warn("stateSink apply failed for {} -> {}: {}", id, target, e.toString());
        }
        dispatch(new MemberEvent(eventType(target), updated));
    }

    // ------------------------------------------------------------------
    // 事件分发
    // ------------------------------------------------------------------

    private void dispatch(MemberEvent event) {
        ExecutorService dispatcher = eventDispatcher;
        if (dispatcher == null) {
            return;
        }
        try {
            dispatcher.execute(() -> {
                for (Consumer<MemberEvent> listener : listeners) {
                    try {
                        listener.accept(event);
                    } catch (RuntimeException e) {
                        LOG.warn("membership listener failed: {}", e.toString());
                    }
                }
                MembershipEvent proto = MembershipEvent.newBuilder()
                        .setType(event.type())
                        .setNode(toProto(event.node()))
                        .build();
                // CopyOnWriteArrayList 的迭代器不支持 remove()：遍历快照收集失效 watcher，
                // 循环不中断，结束后按值统一移除（与 watch() 的 setOnCancelHandler 同语义）
                List<StreamObserver<MembershipEvent>> dead = new ArrayList<>();
                for (StreamObserver<MembershipEvent> watcher : watchers) {
                    try {
                        if (watcher instanceof ServerCallStreamObserver<?> sco && sco.isCancelled()) {
                            dead.add(watcher);
                            continue;
                        }
                        watcher.onNext(proto);
                    } catch (RuntimeException e) {
                        dead.add(watcher);
                        LOG.debug("watcher removed after error: {}", e.toString());
                    }
                }
                if (!dead.isEmpty()) {
                    watchers.removeAll(dead);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.debug("event dropped (dispatcher stopped): {}", event.type());
        }
    }

    // ------------------------------------------------------------------
    // 自身信息
    // ------------------------------------------------------------------

    private void refreshSelf() {
        try {
            NodeInfo info = selfInfo.get();
            if (info != null) {
                view.put(self, info);
            }
        } catch (RuntimeException e) {
            LOG.warn("selfInfo supplier failed: {}", e.toString());
        }
    }

    private NodeInfo currentSelfInfo() {
        NodeInfo info = view.get(self);
        if (info != null) {
            return info;
        }
        refreshSelf();
        info = view.get(self);
        if (info == null) {
            throw new IllegalStateException("self info unavailable");
        }
        return info;
    }

    // ------------------------------------------------------------------
    // proto 转换
    // ------------------------------------------------------------------

    private static NodeInfoP toProto(NodeInfo info) {
        ResourceSnapshot r = info.resources();
        return NodeInfoP.newBuilder()
                .setNodeId(info.id().value())
                .setHost(info.host())
                .setGrpcPort(info.grpcPort())
                .setState(info.state().name())
                .setStateEpoch(info.stateEpoch())
                .setWeight(info.weight())
                .setResources(ResourceSnapshotP.newBuilder()
                        .setCpuPercent(r.cpuPercent())
                        .setMemTotalBytes(r.memTotalBytes())
                        .setMemUsedBytes(r.memUsedBytes())
                        .setDiskTotalBytes(r.diskTotalBytes())
                        .setDiskUsedBytes(r.diskUsedBytes())
                        .setUpdatedAtMillis(r.updatedAtMillis()))
                .build();
    }

    private static NodeInfo fromProto(NodeInfoP p) {
        if (p == null || p.getNodeId().isBlank()) {
            return null;
        }
        try {
            NodeState state = NodeState.valueOf(p.getState());
            ResourceSnapshotP r = p.getResources();
            ResourceSnapshot resources = ResourceSnapshot.of(
                    r.getCpuPercent(), r.getMemTotalBytes(), r.getMemUsedBytes(),
                    r.getDiskTotalBytes(), r.getDiskUsedBytes(), r.getUpdatedAtMillis());
            return NodeInfo.of(NodeId.of(p.getNodeId()), p.getHost(), p.getGrpcPort(),
                    state, p.getStateEpoch(), p.getWeight(), resources);
        } catch (RuntimeException e) {
            LOG.debug("drop malformed NodeInfoP: {}", e.toString());
            return null;
        }
    }

    /** 到某一 peer 的缓存 channel；节点地址变化时重建。 */
    private static final class PeerChannel {
        final String host;
        final int port;
        final ManagedChannel channel;

        PeerChannel(String host, int port, ManagedChannel channel) {
            this.host = host;
            this.port = port;
            this.channel = channel;
        }

        void close() {
            channel.shutdown();
        }

        void resetConnectBackoff() {
            try {
                channel.resetConnectBackoff();
            } catch (RuntimeException ignored) {
                // channel 已关闭等场景可忽略
            }
        }
    }
}
