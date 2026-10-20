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
import java.util.concurrent.ThreadPoolExecutor;
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

    /** S10：vanished-DEAD 出清保留窗口 = deadTimeoutMs 的轮数倍数。 */
    private static final int EVICT_AFTER_DEAD_VANISHED_ROUNDS = 10;

    private final NodeId self;
    private final Supplier<NodeInfo> selfInfo;
    private final PeerDirectory directory;
    private final StateSink stateSink;
    private final GrpcServerHost host;
    private final MembershipConfig config;
    private final Clock clock;
    /** 多级仲裁怀疑票回写（NOOP=未开启，本地 DEAD 直写注册表）。 */
    private final SuspicionSink suspicionSink;

    private final Map<NodeId, NodeInfo> view = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> lastHeard = new ConcurrentHashMap<>();
    /**
     * 每 peer 的 Phi Accrual 检测器：入站心跳与出站 ack 均喂样本。
     * 暖窗期（样本不足）不参与判定，由固定超时阈值兜底。
     */
    private final Map<NodeId, PhiAccrualFailureDetector> detectors = new ConcurrentHashMap<>();
    /** 每 peer 连续 φ≥死阈值的检测周期数；φ 回落即清零，达到 deadConfirmations 才升级 DEAD。 */
    private final Map<NodeId, Integer> deadOverThresholdRounds = new ConcurrentHashMap<>();
    private final Map<NodeId, PeerChannel> channels = new ConcurrentHashMap<>();
    /** 每 peer 连续发送失败计数（收到 ack 清零）：超阈值重建 channel，
     *  规避 JVM DNS 负缓存导致 channel 永久卡在 Unable to resolve host 的问题。 */
    private final Map<NodeId, Integer> consecutiveSendFailures = new ConcurrentHashMap<>();
    private final List<Consumer<MemberEvent>> listeners = new CopyOnWriteArrayList<>();
    private final List<StreamObserver<MembershipEvent>> watchers = new CopyOnWriteArrayList<>();
    private final Object viewLock = new Object();
    /** S10：节点「从目录消失且 DEAD」的起始时刻（连续计时，回归即重置）。 */
    private final Map<NodeId, Long> vanishedDeadSince = new ConcurrentHashMap<>();
    /** S1②：每 peer 已确认的应答视图版本（请求时回带；channel 重建/节点驱逐时清零强制全量）。 */
    private final Map<NodeId, Long> knownPeerVersions = new ConcurrentHashMap<>();

    private ScheduledExecutorService heartbeatExecutor;
    private ExecutorService eventDispatcher;
    /** S1③：心跳派发有界池（替代旧单线程 tick 内串行发送）——tick 只入队，
     *  发送扇出到独立命名池，tick 不再随 peer 数线性膨胀阻塞。 */
    private ExecutorService heartbeatDispatcher;
    /**
     * S1②：视图版本号——每次视图实质变化（新节点/状态跃迁/地址等实质变更）自增，
     * 纯资源字段波动不计（资源每次心跳都在动，计入则版本化失效）。
     * ack 携带本版本；请求方带 known_ack_version：两者相等即视图无新信息，
     * 回空成员列表（保留自报 merge + lastHeard 存活证明），全量视图带宽从每 tick O(N²)
     * 降为变更期 O(N²)——故障检测时序完全不变（lastHeard/merge 照常）。
     */
    private final java.util.concurrent.atomic.AtomicLong viewVersion =
            new java.util.concurrent.atomic.AtomicLong();
    private volatile boolean started;
    private boolean serviceRegistered;

    public MembershipService(NodeId self,
                             Supplier<NodeInfo> selfInfo,
                             PeerDirectory directory,
                             StateSink stateSink,
                             GrpcServerHost host,
                             MembershipConfig config,
                             Clock clock) {
        this(self, selfInfo, directory, stateSink, host, config, clock, SuspicionSink.NOOP);
    }

    public MembershipService(NodeId self,
                             Supplier<NodeInfo> selfInfo,
                             PeerDirectory directory,
                             StateSink stateSink,
                             GrpcServerHost host,
                             MembershipConfig config,
                             Clock clock,
                             SuspicionSink suspicionSink) {
        this.self = java.util.Objects.requireNonNull(self, "self");
        this.selfInfo = java.util.Objects.requireNonNull(selfInfo, "selfInfo");
        this.directory = java.util.Objects.requireNonNull(directory, "directory");
        this.stateSink = java.util.Objects.requireNonNull(stateSink, "stateSink");
        this.host = java.util.Objects.requireNonNull(host, "host");
        this.config = java.util.Objects.requireNonNull(config, "config");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
        this.suspicionSink = java.util.Objects.requireNonNull(suspicionSink, "suspicionSink");
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
        // S1③：派发池有界（§3.17 命名规范），队列满时由 tick 线程回退执行（不丢发送）
        heartbeatDispatcher = new ThreadPoolExecutor(2, 4, 60L, TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(512),
                r -> {
                    Thread t = new Thread(r, "membership-dispatch-" + self.value());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        ((ThreadPoolExecutor) heartbeatDispatcher).allowCoreThreadTimeOut(true);
        invalidateSelfInfoCache(); // 重启场景：绕过限频窗，确保首刷执行
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
            try {
                // A4：等待在途心跳 tick 终结，避免其在通道清理后重建 channel 造成泄漏
                hb.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // S1③：等派发池排空在途发送后再关通道（A4 语义延伸：杜绝关通道后仍有任务重建）
        ExecutorService hbd = heartbeatDispatcher;
        heartbeatDispatcher = null;
        if (hbd != null) {
            hbd.shutdown();
            try {
                if (!hbd.awaitTermination(2, TimeUnit.SECONDS)) {
                    hbd.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                hbd.shutdownNow();
            }
        }
        for (PeerChannel pc : channels.values()) {
            pc.close();
        }
        channels.clear();
        consecutiveSendFailures.clear();
        knownPeerVersions.clear();
        vanishedDeadSince.clear();
        lastSelfRefreshAt = 0L;
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
        // 优雅退出：撤回本节点投出的全部未决怀疑票，避免离开集群后旧票在 TTL 内
        // 继续凑齐法定人数误杀健康节点（TTL 清扫是兜底，主动撤票收敛更快）。
        for (NodeInfo info : view.values()) {
            if (info.state() == NodeState.SUSPECTED || info.state() == NodeState.DEAD) {
                try {
                    suspicionSink.recall(info.id());
                } catch (RuntimeException e) {
                    LOG.debug("recall suspicion on stop failed for {}: {}", info.id(), e.toString());
                }
            }
        }
        view.clear();
        lastHeard.clear();
        detectors.clear();
        deadOverThresholdRounds.clear();
        lastUpdatedEventAt.clear();
        LOG.info("membership service stopped: self={}", self);
    }

    /** 当前视图的不可变快照（含自身）。 */
    public Map<NodeId, NodeInfo> view() {
        return Map.copyOf(view);
    }

    /** S1②：当前视图版本号（包内可见，测试/观测用）。 */
    long viewVersion() {
        return viewVersion.get();
    }

    /** S1②：对某 peer 已确认的视图版本（包内可见，测试用；未确认返回 -1）。 */
    long knownPeerVersion(NodeId peer) {
        return knownPeerVersions.getOrDefault(peer, -1L);
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
                    recordSignal(reported.id(), clock.nowMillis());
                    merge(reported, true);
                }
                // S1①：buildAck 不再 refreshSelf——旧实现每条入站心跳回源一次
                // registry.get(self)（DB 点查），全集群 O(N²/tick)；self 由本节点 tick 刷新
                responseObserver.onNext(buildAck(request.getKnownAckVersion()));
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

    /**
     * S1②：版本协商应答。请求方已知版本恰等于本节点当前版本 → 视图无新信息，
     * 回空成员（自报 merge 与 lastHeard 存活证明已在先完成，故障检测时序不变）；
     * 否则回全量视图 + 当前版本（版本先行读取：构建期间的新变更会被对端下次请求补齐，
     * 不会丢更新）。
     */
    private HeartbeatAck buildAck(long requesterKnownVersion) {
        long v = viewVersion.get();
        HeartbeatAck.Builder builder = HeartbeatAck.newBuilder().setAckVersion(v);
        if (requesterKnownVersion == v) {
            return builder.build();
        }
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
            ExecutorService dispatch = heartbeatDispatcher;
            for (NodeInfo peer : peers) {
                if (peer == null || peer.id().equals(self)) {
                    continue;
                }
                activeIds.add(peer.id());
                final NodeInfo target = peer;
                try {
                    // S1③：发送派发到独立有界池——tick 线程不再被逐 peer 的
                    // channel 构建/stub 创建串行拖慢（派发本身非阻塞，ack 在 gRPC 回调线程处理）
                    if (dispatch != null) {
                        dispatch.execute(() -> {
                            try {
                                sendHeartbeat(target);
                            } catch (RuntimeException e) {
                                LOG.debug("send heartbeat to {} failed: {}",
                                        target.id(), e.toString());
                            }
                        });
                    } else {
                        sendHeartbeat(peer); // 池未就绪（停机中）：回退直发
                    }
                } catch (RuntimeException e) {
                    LOG.debug("dispatch heartbeat to {} failed: {}", peer.id(), e.toString());
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
            // S10 周期驱逐：目录（注册表存活集）已移除且视图持续判定 DEAD 超过保留窗口的节点出清，
            // 视图不再随节点历史规模单调增长（退役/缩容后的墓碑行永久滞留问题）
            evictVanishedDead(activeIds, clock.nowMillis());
        } catch (RuntimeException e) {
            LOG.warn("heartbeat tick error: {}", e.toString());
        }
    }

    /**
     * S10：从目录消失且持续 DEAD 超过保留窗口的节点移出视图与全部 per-node 计时状态。
     *
     * <p>保留窗口（{@code deadTimeoutMs * 10}）确保 DEAD 判定在视图内可被稳定观察/收敛，
     * 不会「判 DEAD 的同一 tick 即出清」——退役墓碑行的最终清理不受影响，只是延迟到窗口之后。
     * 窗口起点记录在 {@link #vanishedDeadSince}：节点回到目录或状态转活即重置。</p>
     */
    private void evictVanishedDead(Set<NodeId> directoryIds, long now) {
        synchronized (viewLock) {
            for (Iterator<Map.Entry<NodeId, NodeInfo>> it = view.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<NodeId, NodeInfo> entry = it.next();
                NodeId id = entry.getKey();
                boolean vanished = !id.equals(self) && !directoryIds.contains(id);
                boolean dead = entry.getValue().state() == NodeState.DEAD;
                if (!vanished || !dead) {
                    vanishedDeadSince.remove(id); // 回归目录或转活：重置计时
                    continue;
                }
                Long since = vanishedDeadSince.putIfAbsent(id, now);
                if (since == null) {
                    continue; // 首次观察到「消失且 DEAD」，开始计时
                }
                if (now - since < config.deadTimeoutMs() * EVICT_AFTER_DEAD_VANISHED_ROUNDS) {
                    continue; // 保留窗口内，留给上层收敛/观察
                }
                it.remove();
                lastHeard.remove(id);
                detectors.remove(id);
                deadOverThresholdRounds.remove(id);
                consecutiveSendFailures.remove(id);
                lastUpdatedEventAt.remove(id);
                knownPeerVersions.remove(id);
                vanishedDeadSince.remove(id);
                LOG.debug("evicted vanished DEAD node {} from view (window {}ms)",
                        id, config.deadTimeoutMs() * EVICT_AFTER_DEAD_VANISHED_ROUNDS);
            }
        }
    }

    private void sendHeartbeat(NodeInfo peer) {
        PeerChannel pc = channelFor(peer);
        if (pc == null) {
            return; // 已停机（S21 守卫），不再发送
        }
        NodeId peerId = peer.id();
        MembershipGrpc.MembershipStub stub = MembershipGrpc.newStub(pc.channel);
        StreamObserver<HeartbeatReq> requestStream = stub.heartbeat(new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatAck ack) {
                long heardAt = clock.nowMillis();
                recordSignal(peerId, heardAt);
                consecutiveSendFailures.remove(peerId);
                // S1②：记录对端视图版本；空成员 = 对端无新信息（存活证明已完成），跳过合并
                knownPeerVersions.put(peerId, ack.getAckVersion());
                if (ack.getMembersCount() == 0) {
                    return;
                }
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
                    // S1②：通道重建后对端可能已重启（版本归零），清 known 强制下次全量
                    knownPeerVersions.remove(peerId);
                }
            }

            @Override
            public void onCompleted() {
                // 单轮请求-应答结束
            }
        });
        requestStream.onNext(HeartbeatReq.newBuilder()
                .setSelf(toProto(currentSelfInfo()))
                .setKnownAckVersion(knownPeerVersions.getOrDefault(peerId, -1L))
                .build());
        requestStream.onCompleted();
    }

    /**
     * 取/建对端通道。S1③后由派发池并发调用——用 {@code channels.compute} 保证
     * 同 peer 通道重建的单原子性（旧实现 check-then-put 在并发下会创建两个通道泄漏一个）。
     * <p>{@code started} 守卫：停机后返回 null 不再建通道（S21）——
     * 配合 {@link #stop()} 先 {@code awaitTermination} 等待在途 tick/派发任务终结、
     * 再清理通道的顺序，杜绝 stop 后心跳重建通道导致的泄漏。</p>
     */
    private PeerChannel channelFor(NodeInfo peer) {
        if (!started) {
            return null; // 已停机：不再建通道
        }
        return channels.compute(peer.id(), (id, existing) -> {
            if (existing != null && existing.host.equals(peer.host()) && existing.port == peer.grpcPort()) {
                return existing;
            }
            if (existing != null) {
                existing.close();
                knownPeerVersions.remove(id); // 地址变化：旧确认版本作废
            }
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(peer.host(), peer.grpcPort())
                    .usePlaintext()
                    .build();
            return new PeerChannel(peer.host(), peer.grpcPort(), channel);
        });
    }

    /**
     * 记录一次来自 peer 的存活信号（入站心跳 / 出站 ack 同一入口）：
     * 更新 {@code lastHeard} 并喂入该 peer 的 φ 检测器。检测器懒建，
     * 全部时间戳取注入时钟，支持 TestClock 确定性测试。
     */
    void recordSignal(NodeId id, long heardAt) {
        lastHeard.put(id, heardAt);
        detectorFor(id).heartbeat(heardAt);
    }

    private PhiAccrualFailureDetector detectorFor(NodeId id) {
        return detectors.computeIfAbsent(id, k -> new PhiAccrualFailureDetector(
                config.phiMaxSampleSize(),
                config.phiMinSamples(),
                config.phiMinStdDeviationMs(),
                config.phiAcceptablePauseMs(),
                clock));
    }

    /** 某 peer 当前的 φ 怀疑度（无检测器/无信号返回 0）；供管理端可观测与测试。 */
    public double phiOf(NodeId id) {
        PhiAccrualFailureDetector detector = detectors.get(id);
        return detector == null ? 0.0 : detector.phi();
    }

    void detectFailures(long now) {
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
            NodeState before = info.state();
            PhiAccrualFailureDetector detector = detectors.get(id);
            double phi = detector == null ? 0.0 : detector.phiAt(now);
            if (detector != null && detector.isReady()) {
                detectByPhi(id, before, phi);
            } else {
                // 暖窗期无分布可依：投票 φ 取怀疑阈值（语义=固定超时已构成怀疑）
                phi = config.phiSuspectThreshold();
                detectByFixedTimeout(id, before, now - last);
            }
            // 多级仲裁凭据：疑似/死亡期间每周期刷新投票（保持 TTL 新鲜），恢复撤票。
            // 纯 gossip 得知的节点不会到达这里（lastHeard 为空），故票全部基于本地直接信号。
            NodeState after = view.get(id).state();
            if (after == NodeState.SUSPECTED || after == NodeState.DEAD) {
                suspicionSink.vote(id, phi);
            } else if (before == NodeState.SUSPECTED && after == NodeState.ACTIVE) {
                suspicionSink.recall(id);
            }
            // DEAD 不在本地检测路径复活：DEAD 为终态，复活必须经重新注册
            // （DEAD→JOINING→ACTIVE）。「重新收到消息」可能是残留重传/旧流回放，
            // 直接 DEAD→ACTIVE 既是非法跃迁，也会跳过注册流程的元数据重置。
        }
    }

    /**
     * 暖窗回退：φ 检测器样本不足（冷启动/重连）时沿用固定静默时长判定，
     * 语义与改造前一致（先 SUSPECTED 后 DEAD）。
     */
    private void detectByFixedTimeout(NodeId id, NodeState current, long silence) {
        if (silence > config.deadTimeoutMs()) {
            if (current != NodeState.SUSPECTED && current != NodeState.DEAD) {
                transitionLocal(id, NodeState.SUSPECTED); // 保证事件先疑似后死亡
            }
            transitionLocal(id, NodeState.DEAD);
        } else if (silence > config.suspectTimeoutMs()) {
            if (current != NodeState.SUSPECTED && current != NodeState.DEAD) {
                transitionLocal(id, NodeState.SUSPECTED);
            }
        } else if (current == NodeState.SUSPECTED) {
            deadOverThresholdRounds.remove(id);
            transitionLocal(id, NodeState.ACTIVE);
        }
    }

    /**
     * φ 主路径：φ≥死阈值须连续 {@code deadConfirmations} 个检测周期，
     * 且首个超阈周期先置 SUSPECTED（保证 NODE_SUSPECTED 先于 NODE_DEAD）；
     * φ 回落即清零连续计数，健康区间 SUSPECTED→ACTIVE 恢复。
     */
    private void detectByPhi(NodeId id, NodeState current, double phi) {
        if (phi >= config.phiDeadThreshold()) {
            if (current != NodeState.SUSPECTED && current != NodeState.DEAD) {
                transitionLocal(id, NodeState.SUSPECTED);
            }
            int rounds = deadOverThresholdRounds.merge(id, 1, Integer::sum);
            if (rounds >= config.deadConfirmations()) {
                transitionLocal(id, NodeState.DEAD);
            }
        } else {
            deadOverThresholdRounds.remove(id);
            if (phi >= config.phiSuspectThreshold()) {
                if (current != NodeState.SUSPECTED && current != NodeState.DEAD) {
                    transitionLocal(id, NodeState.SUSPECTED);
                }
            } else if (current == NodeState.SUSPECTED) {
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
        boolean eventIsStateChange = false;
        boolean materiallyChanged = false;
        synchronized (viewLock) {
            NodeInfo local = view.get(incoming.id());
            if (local == null) {
                view.put(incoming.id(), incoming);
                event = new MemberEvent(eventType(incoming.state()), incoming);
                eventIsStateChange = true;
                materiallyChanged = true;
            } else if (accept(local, incoming, directFromSource) && !incoming.equals(local)) {
                view.put(incoming.id(), incoming);
                boolean stateChanged = incoming.state() != local.state();
                materiallyChanged = stateChanged || hasMaterialChange(local, incoming);
                // S10 字段级比较：仅资源波动的更新不再产生事件（心跳周期级风暴源头）；
                // 地址/权重/纪元等实质变更产生 NODE_UPDATED，且按 suspectTimeoutMs 节流
                if (stateChanged) {
                    event = new MemberEvent(eventType(incoming.state()), incoming);
                    eventIsStateChange = true;
                } else if (materiallyChanged && allowUpdatedEvent(incoming.id())) {
                    event = new MemberEvent(MemberEvent.NODE_UPDATED, incoming);
                }
                // 恢复：先前被判 SUSPECTED 的节点重新报来 ACTIVE → 回写宿主。
                // 排除 DEAD：DEAD 为终态，复活节点须重新注册（DEAD→JOINING→ACTIVE），
                // gossip 直接复活会触发非法跃迁且跳过注册流程的元数据重置
                recoveredToActive = incoming.state() == NodeState.ACTIVE
                        && local.state() != NodeState.ACTIVE
                        && local.state() != NodeState.DEAD;
            }
        }
        if (materiallyChanged) {
            viewVersion.incrementAndGet(); // S1②：实质变更推进版本（资源波动不推进）
        }
        if (recoveredToActive) {
            // 恢复早于下一检测 tick 经由 gossip/直证到达：同步撤票，避免旧怀疑票滞留至 TTL
            suspicionSink.recall(incoming.id());
            deadOverThresholdRounds.remove(incoming.id());
            try {
                // S12：携带判定时刻纪元与直证标记——自报（directFromSource 且条目即来源节点）
                // 豁免纪元防护；陈旧他报回写被宿主防护丢弃
                stateSink.apply(incoming.id(), NodeState.ACTIVE,
                        incoming.stateEpoch(), directFromSource);
            } catch (RuntimeException e) {
                LOG.warn("stateSink apply failed for {} -> ACTIVE: {}", incoming.id(), e.toString());
            }
        }
        if (event != null) {
            if (eventIsStateChange) {
                lastUpdatedEventAt.remove(incoming.id());
            }
            LOG.debug("member view updated: {} -> {}", event.type(), event.node().id());
            dispatch(event);
        }
    }

    /** S10：实质字段比较（资源除外——资源每次心跳都在动，不得驱动事件）。 */
    private static boolean hasMaterialChange(NodeInfo local, NodeInfo incoming) {
        return !local.host().equals(incoming.host())
                || local.grpcPort() != incoming.grpcPort()
                || Double.compare(local.weight(), incoming.weight()) != 0
                || local.stateEpoch() != incoming.stateEpoch();
    }

    /** S10：NODE_UPDATED 节流窗口（每节点至多一次/suspectTimeoutMs）。 */
    private final Map<NodeId, Long> lastUpdatedEventAt = new ConcurrentHashMap<>();

    private boolean allowUpdatedEvent(NodeId id) {
        long now = clock.nowMillis();
        Long last = lastUpdatedEventAt.get(id);
        if (last != null && now - last < config.suspectTimeoutMs()) {
            return false;
        }
        lastUpdatedEventAt.put(id, now);
        return true;
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
                    current.stateEpoch(), current.weight(), current.resources(), current.zone());
            view.put(id, updated);
            viewVersion.incrementAndGet(); // S1②：本地状态跃迁推进版本，对端下次请求拿全量
        }
        LOG.debug("peer {} transitioned to {} (local detection)", id, target);
        // 多级仲裁模式下本地 DEAD 不直写注册表：仅更新视图/事件/投票，DEAD 提升由
        // leader 按多数怀疑票唯一执行（SUSPECTED 回写保留，供摘流与可见性）。
        if (target != NodeState.DEAD || config.localDeadWriteback()) {
            try {
                // S12：携带判定时刻视图纪元——注册表期间被重新注册/更高纪元回写过 → 宿主丢弃陈旧决定
                stateSink.apply(id, target, updated.stateEpoch(), false);
            } catch (RuntimeException e) {
                LOG.warn("stateSink apply failed for {} -> {}: {}", id, target, e.toString());
            }
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

    /**
     * S1①：refreshSelf 限频——每心跳间隔至多回源一次 selfInfo.get()（registry 点查），
     * tick 内多次调用命中时间窗缓存；启动首刷与状态推进外部失效不受窗影响。
     */
    private volatile long lastSelfRefreshAt;

    private void invalidateSelfInfoCache() {
        lastSelfRefreshAt = 0L;
    }

    private void refreshSelf() {
        long now = clock.nowMillis();
        if (now - lastSelfRefreshAt < config.heartbeatIntervalMs()) {
            return; // 本周期已刷新（同 tick 多次调用去重）
        }
        try {
            NodeInfo info = selfInfo.get();
            if (info != null) {
                NodeInfo previous = view.put(self, info);
                // S1②：自身实质变化（状态推进/地址/权重/纪元）推进视图版本，
                // 使对端下次请求拿到全量视图（资源波动不推进——否则版本化失效）
                if (previous != info && (previous == null
                        || previous.state() != info.state()
                        || !previous.host().equals(info.host())
                        || previous.grpcPort() != info.grpcPort()
                        || Double.compare(previous.weight(), info.weight()) != 0
                        || previous.stateEpoch() != info.stateEpoch())) {
                    viewVersion.incrementAndGet();
                }
            }
        } catch (RuntimeException e) {
            LOG.warn("selfInfo supplier failed: {}", e.toString());
        } finally {
            lastSelfRefreshAt = now;
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
                .setZone(info.zone())
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
                    state, p.getStateEpoch(), p.getWeight(), resources, p.getZone());
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
