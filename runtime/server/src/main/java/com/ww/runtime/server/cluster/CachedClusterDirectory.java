package com.ww.runtime.server.cluster;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据面节点目录缓存：节点端点 + 各能力 serving 集合的本地快照。
 *
 * <p>替代数据面转发/端点解析/fan-out 中的 {@code NodeRegistry} 与
 * {@code CapabilityStateStore} 同步查询。后台按固定周期刷新，失败保留上一份快照
 * （last-good）；数据面只读 volatile 快照，不做数据库访问。</p>
 *
 * <p>状态新鲜度由刷新周期约束（默认 1s）：节点转 DEAD 或能力停用后，最长一个周期内
 * 仍可能被选为转发目标；请求会得到连接失败/能力未启动错误，属于 fail-closed，
 * 不会造成静默数据错路由。</p>
 */
public final class CachedClusterDirectory {

    private static final Logger LOG = LoggerFactory.getLogger(CachedClusterDirectory.class);
    private static final long FAILURE_LOG_EVERY = 60L;

    private final NodeRegistry registry;
    private final CapabilityStateStore capabilityStateStore;
    private final long refreshIntervalMs;
    private final boolean cacheEnabled;

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(Snapshot.empty());
    private final AtomicLong consecutiveFailures = new AtomicLong();

    private volatile ScheduledExecutorService executor;

    public CachedClusterDirectory(NodeRegistry registry,
                                  CapabilityStateStore capabilityStateStore,
                                  long refreshIntervalMs,
                                  boolean cacheEnabled) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capabilityStateStore = Objects.requireNonNull(capabilityStateStore, "capabilityStateStore");
        if (refreshIntervalMs <= 0) {
            throw new IllegalArgumentException("refreshIntervalMs must be > 0, got " + refreshIntervalMs);
        }
        this.refreshIntervalMs = refreshIntervalMs;
        this.cacheEnabled = cacheEnabled;
    }

    @PostConstruct
    public synchronized void start() {
        if (executor != null || !cacheEnabled) {
            return;
        }
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-cluster-directory-refresh");
            t.setDaemon(true);
            return t;
        });
        executor = exec;
        exec.scheduleWithFixedDelay(this::safeRefresh, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public synchronized void stop() {
        ScheduledExecutorService exec = executor;
        executor = null;
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    /** 立即同步刷新（测试/管理面变更后主动触发）。 */
    public void refreshNow() {
        List<NodeInfo> alive = registry.listAlive();
        List<CapabilityStateStore.CapabilityRecord> records = capabilityStateStore.list();

        Map<NodeId, NodeInfo> nodes = new HashMap<>();
        Set<NodeId> aliveIds = new HashSet<>();
        for (NodeInfo info : alive) {
            nodes.put(info.id(), info);
            aliveIds.add(info.id());
        }

        Map<CapabilityType, Set<NodeId>> serving = new EnumMap<>(CapabilityType.class);
        for (CapabilityStateStore.CapabilityRecord record : records) {
            if (record.actual() != CapabilityState.ACTIVE || !aliveIds.contains(record.node())) {
                continue;
            }
            serving.computeIfAbsent(record.type(), ignored -> new HashSet<>()).add(record.node());
        }

        Map<CapabilityType, Set<NodeId>> frozen = new EnumMap<>(CapabilityType.class);
        for (Map.Entry<CapabilityType, Set<NodeId>> entry : serving.entrySet()) {
            frozen.put(entry.getKey(), Set.copyOf(entry.getValue()));
        }
        snapshot.set(new Snapshot(Map.copyOf(nodes), Map.copyOf(frozen), true));
        consecutiveFailures.set(0);
    }

    public boolean isReady() {
        return !cacheEnabled || snapshot.get().loaded();
    }

    /** 节点数据面端点：仅 ACTIVE 节点可解析。 */
    public Optional<Endpoint> endpoint(NodeId node) {
        Objects.requireNonNull(node, "node");
        if (!cacheEnabled) {
            return registry.get(node)
                    .filter(info -> info.state() == NodeState.ACTIVE)
                    .map(info -> new Endpoint(info.host(), info.grpcPort()));
        }
        NodeInfo info = snapshot.get().nodes().get(node);
        if (info == null || info.state() != NodeState.ACTIVE) {
            return Optional.empty();
        }
        return Optional.of(new Endpoint(info.host(), info.grpcPort()));
    }

    /** 集群存活节点全集（含 self；调用方自行短路本地）。 */
    public List<NodeId> alivePeers() {
        if (!cacheEnabled) {
            return registry.listAlive().stream().map(NodeInfo::id).toList();
        }
        return List.copyOf(snapshot.get().nodes().keySet());
    }

    /** 正在服务指定能力且存活的节点（含 self）。 */
    public List<NodeId> servingPeers(CapabilityType type) {
        if (!cacheEnabled) {
            Set<NodeId> alive = registry.listAlive().stream()
                    .map(NodeInfo::id)
                    .collect(java.util.stream.Collectors.toSet());
            return capabilityStateStore.list().stream()
                    .filter(record -> record.type() == type)
                    .filter(record -> record.actual() == CapabilityState.ACTIVE)
                    .map(CapabilityStateStore.CapabilityRecord::node)
                    .filter(alive::contains)
                    .toList();
        }
        Set<NodeId> peers = snapshot.get().serving().get(type);
        return peers == null ? List.of() : List.copyOf(peers);
    }

    public int aliveCount() {
        return snapshot.get().nodes().size();
    }

    private void safeRefresh() {
        try {
            refreshNow();
        } catch (RuntimeException e) {
            long failures = consecutiveFailures.incrementAndGet();
            if (failures == 1 || failures % FAILURE_LOG_EVERY == 0) {
                LOG.warn("cluster directory refresh failed (keeping last snapshot, failures={}): {}",
                        failures, e.getMessage());
            }
        }
    }

    public record Endpoint(String host, int grpcPort) {

        public Endpoint {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (grpcPort <= 0) {
                throw new IllegalArgumentException("grpcPort must be > 0, got " + grpcPort);
            }
        }

        public String target() {
            return host + ":" + grpcPort;
        }
    }

    private record Snapshot(Map<NodeId, NodeInfo> nodes,
                            Map<CapabilityType, Set<NodeId>> serving,
                            boolean loaded) {

        static Snapshot empty() {
            return new Snapshot(Map.of(), Map.of(), false);
        }
    }
}
