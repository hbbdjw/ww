package com.ww.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储节点故障检测与自动 failover（方案 B）。
 *
 * <p>复用 cache 模块 {@code FailoverManager} 的范式：周期心跳探测 + 状态机 + 路由过滤。
 * 针对 storage 持久化特性补充：立即 reconcile（降 RPO）+ 原主反向同步（防数据陈旧）+
 * QUARANTINE 隔离（防脑裂）。
 *
 * <h3>故障检测</h3>
 * 周期（{@value #HEARTBEAT_INTERVAL_MS}ms）HTTP PING 对端：
 * <ul>
 *   <li>连续 {@value #SUSPECT_THRESHOLD} 次失败 → SUSPECT（仍路由）</li>
 *   <li>连续 {@value #DEAD_THRESHOLD} 次失败 → DEAD（路由过滤 + failoverEpoch++ + 立即对账）</li>
 * </ul>
 *
 * <h3>failover 触发（DEAD 时）</h3>
 * <ol>
 *   <li>{@link StorageTopologyManager#markDead} —— 路由过滤 + failoverEpoch++</li>
 *   <li>广播 failover 事件到对端（{@code /internal/storage/failover}）</li>
 *   <li>{@link DistributedStorage#triggerReconcileNow} —— 立即补副本（不等 5min 周期）</li>
 * </ol>
 *
 * <h3>原主恢复（DEAD → QUARANTINE → ALIVE）</h3>
 * <ol>
 *   <li>心跳恢复 → markAlive(quarantine=true) → QUARANTINE（不纳入路由）</li>
 *   <li>触发原主反向 reconcile（{@code /internal/storage/reverse-reconcile}）</li>
 *   <li>原主从新 primary 拉取 version 落后的对象补齐本地</li>
 *   <li>校验通过 → markAlive(quarantine=false) → ALIVE（纳入路由）</li>
 * </ol>
 *
 * <h3>脑裂防护</h3>
 * <ul>
 *   <li>DEAD/QUARANTINE 节点不纳入 {@link NodeRouter}，{@code selectPrimary} 不会选到原主</li>
 *   <li>原主恢复后进入 QUARANTINE，反向同步完成前不接路由</li>
 *   <li>分区期间原主可能仍有写入（无法完全防），恢复后由 LWW via version 兜底（旧版本被覆盖）</li>
 * </ul>
 *
 * <h3>生存能力</h3>
 * survive loss of 1 storage node while continuing read/write，
 * RTO ≈ {@value #HEARTBEAT_INTERVAL_MS}ms × {@value #DEAD_THRESHOLD} = 15s，
 * RPO ≤ 副本对账延迟（reconcile 立即触发）+ 分区期间未复制的写。
 */
public class StorageFailoverManager {

    private static final Logger log = LoggerFactory.getLogger(StorageFailoverManager.class);

    /** 心跳探测周期（ms） */
    private static final long HEARTBEAT_INTERVAL_MS = 5_000L;
    /** 心跳请求超时（ms） */
    private static final long HEARTBEAT_TIMEOUT_MS = 3_000L;
    /** 标记 SUSPECT 的连续失败次数 */
    private static final int SUSPECT_THRESHOLD = 2;
    /** 标记 DEAD 的连续失败次数（触发 failover） */
    private static final int DEAD_THRESHOLD = 3;
    /** QUARANTINE 反向同步超时（ms），超时强制转 ALIVE */
    private static final long QUARANTINE_TIMEOUT_MS = 300_000L;

    private final StorageTopologyManager topologyManager;
    private final StorageRpcClient nodeClient;
    private final DistributedStorage distributedStorage;

    /** 节点连续心跳失败次数 */
    private final Map<String, AtomicInteger> failureCount = new ConcurrentHashMap<>();
    /** QUARANTINE 进入时间戳（用于超时强制转 ALIVE） */
    private final Map<String, Long> quarantineSince = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> heartbeatTask;

    public StorageFailoverManager(StorageTopologyManager topologyManager,
                                  StorageRpcClient nodeClient,
                                  DistributedStorage distributedStorage) {
        this.topologyManager = topologyManager;
        this.nodeClient = nodeClient;
        this.distributedStorage = distributedStorage;
    }

    /** 启动心跳探测（分布式模式下由装配层调用） */
    public synchronized void start() {
        if (executor != null) return;
        executor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "storage-failover");
            t.setDaemon(true);
            return t;
        });
        heartbeatTask = executor.scheduleWithFixedDelay(this::heartbeatAll,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("storage failover 心跳探测已启动: interval={}ms, suspect={}, dead={}",
                HEARTBEAT_INTERVAL_MS, SUSPECT_THRESHOLD, DEAD_THRESHOLD);
    }

    public synchronized void stop() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    // ==================== 心跳探测 ====================

    /** 慢心跳阈值（ms）：超过此值的单个节点 ping 将打印 warn 日志 */
    private static final long SLOW_HEARTBEAT_THRESHOLD_MS = 1000L;

    /**
     * 心跳探测：使用虚拟线程并行 ping 所有对端节点。
     * 串行 N 个节点最坏延迟 N × timeout，并行后降为 1 × timeout（≈3s）。
     * <p>性能日志：记录总耗时、成功/失败数、慢节点列表。
     * <p>心跳完成后检查 QUARANTINE 节点是否需要重试反向同步或超时强制转 ALIVE。
     */
    private void heartbeatAll() {
        try {
            List<StorageNode> peers = new ArrayList<>();
            for (StorageNode n : topologyManager.getNodes()) {
                if (!n.isSelf()) peers.add(n);
            }
            if (peers.isEmpty()) return;

            long batchStart = System.currentTimeMillis();
            int peerCount = peers.size();
            // 收集各节点 ping 耗时：nodeKey -> latencyMs，负数表示失败
            Map<String, Long> latencies = new ConcurrentHashMap<>();

            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>(peerCount);
                for (StorageNode n : peers) {
                    futures.add(pool.submit(() -> heartbeatWithTiming(n, latencies)));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get(HEARTBEAT_TIMEOUT_MS + 1000, TimeUnit.MILLISECONDS);
                    } catch (Exception ignored) {
                        // 单个节点 ping 异常不影响其他节点
                    }
                }
            }

            long batchElapsed = System.currentTimeMillis() - batchStart;
            int ok = 0, fail = 0;
            List<String> slowNodes = new ArrayList<>();
            for (Map.Entry<String, Long> e : latencies.entrySet()) {
                long lat = e.getValue();
                if (lat < 0) {
                    fail++;
                } else {
                    ok++;
                    if (lat >= SLOW_HEARTBEAT_THRESHOLD_MS) {
                        slowNodes.add(e.getKey() + "=" + lat + "ms");
                    }
                }
            }
            if (batchElapsed > HEARTBEAT_INTERVAL_MS / 2 || fail > 0 || !slowNodes.isEmpty()) {
                // 批次耗时长、有失败、或有慢节点时打印 warn
                log.warn("心跳批次完成: nodes={}, ok={}, fail={}, total={}ms{}{}",
                        peerCount, ok, fail, batchElapsed,
                        fail > 0 ? ", failedNodes=" + collectFailedNodes(latencies) : "",
                        slowNodes.isEmpty() ? "" : ", slowNodes=" + slowNodes);
            } else {
                log.debug("心跳批次完成: nodes={}, ok={}, fail={}, total={}ms",
                        peerCount, ok, fail, batchElapsed);
            }
            // 检查 QUARANTINE 节点：重试反向同步或超时强制转 ALIVE
            checkQuarantineNodes();
        } catch (Exception e) {
            log.warn("心跳探测周期异常", e);
        }
    }

    /** 带耗时统计的心跳：将延迟写入 latencies map（负数表示失败） */
    private void heartbeatWithTiming(StorageNode node, Map<String, Long> latencies) {
        String key = node.nodeKey();
        long start = System.nanoTime();
        boolean ok = false;
        try {
            ok = heartbeat0(node);
        } catch (Exception e) {
            ok = false;
        } finally {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            latencies.put(key, ok ? elapsedMs : -elapsedMs);
        }
    }

    private List<String> collectFailedNodes(Map<String, Long> latencies) {
        List<String> failed = new ArrayList<>();
        for (Map.Entry<String, Long> e : latencies.entrySet()) {
            if (e.getValue() < 0) {
                failed.add(e.getKey() + "(" + (-e.getValue()) + "ms)");
            }
        }
        return failed;
    }

    private boolean heartbeat0(StorageNode node) {
        String key = node.nodeKey();
        NodeStatus current = topologyManager.getNodeStatus(key);
        if (current == null) current = NodeStatus.ALIVE;

        boolean ok;
        try {
            ok = nodeClient.ping(node, HEARTBEAT_TIMEOUT_MS);
        } catch (Exception e) {
            ok = false;
        }

        if (ok) {
            failureCount.remove(key);
            onHeartbeatOk(node, current);
        } else {
            int fails = failureCount.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
            onHeartbeatFail(node, current, fails);
        }
        return ok;
    }

    private void onHeartbeatOk(StorageNode node, NodeStatus current) {
        String key = node.nodeKey();
        if (current == NodeStatus.SUSPECT) {
            topologyManager.setNodeStatus(key, NodeStatus.ALIVE);
            log.info("节点恢复健康: {} SUSPECT → ALIVE", key);
            return;
        }
        if (current == NodeStatus.DEAD) {
            // 原主恢复，进入 QUARANTINE
            topologyManager.markAlive(key, true);
            distributedStorage.refreshNodeIndex();
            quarantineSince.put(key, System.currentTimeMillis());
            log.info("故障节点恢复: {} DEAD → QUARANTINE，开始反向同步", key);
            triggerReverseReconcile(node);
        }
    }

    private void onHeartbeatFail(StorageNode node, NodeStatus current, int fails) {
        String key = node.nodeKey();
        if (current == NodeStatus.DEAD || current == NodeStatus.QUARANTINE) {
            // 已下线/隔离节点的心跳失败不处理
            return;
        }
        if (fails >= DEAD_THRESHOLD) {
            log.warn("节点连续 {} 次心跳失败，触发 failover: {}", fails, key);
            triggerFailover(node);
        } else if (fails >= SUSPECT_THRESHOLD && current == NodeStatus.ALIVE) {
            topologyManager.setNodeStatus(key, NodeStatus.SUSPECT);
            log.warn("节点心跳失败 {} 次: {} ALIVE → SUSPECT", fails, key);
        }
    }

    // ==================== failover 触发 ====================

    /** 自动 failover：标记 DEAD + 路由重建 + 广播 + 立即对账 */
    private void triggerFailover(StorageNode node) {
        String key = node.nodeKey();
        try {
            topologyManager.markDead(key);
            distributedStorage.refreshNodeIndex();
        } catch (Exception e) {
            log.error("markDead 失败: {}", key, e);
        }
        // 立即触发副本补齐（不等 5min 周期）
        try {
            distributedStorage.triggerReconcileNow();
            log.info("failover 后立即对账已触发: {}", key);
        } catch (Exception e) {
            log.warn("failover 后立即对账失败: {}", key, e);
        }
    }

    // ==================== 原主反向同步 ====================

    /** 触发原主反向 reconcile（异步，避免阻塞心跳线程） */
    private void triggerReverseReconcile(StorageNode node) {
        String key = node.nodeKey();
        Thread.ofVirtual().name("storage-reverse-reconcile-" + key).start(() -> {
            try {
                log.info("触发反向同步: {}", key);
                boolean ok = nodeClient.triggerReverseReconcile(node);
                if (ok) {
                    topologyManager.markAlive(key, false);
                    distributedStorage.refreshNodeIndex();
                    quarantineSince.remove(key);
                    log.info("节点恢复完成: {} QUARANTINE → ALIVE", key);
                } else {
                    log.warn("反向同步返回失败，保持 QUARANTINE: {}", key);
                    checkQuarantineTimeout(key);
                }
            } catch (Exception e) {
                log.warn("反向同步异常: {}，保持 QUARANTINE", key, e);
                checkQuarantineTimeout(key);
            }
        });
    }

    /**
     * 检查所有 QUARANTINE 节点：若反向同步触发请求已丢失或超时，
     * 重新触发反向同步；若 QUARANTINE 超时则强制转 ALIVE，防止节点永久隔离。
     */
    private void checkQuarantineNodes() {
        for (StorageNode n : topologyManager.getNodes()) {
            if (n.isSelf()) continue;
            String key = n.nodeKey();
            if (topologyManager.getNodeStatus(key) != NodeStatus.QUARANTINE) continue;
            Long since = quarantineSince.get(key);
            if (since != null && System.currentTimeMillis() - since > QUARANTINE_TIMEOUT_MS) {
                log.warn("QUARANTINE 超时 {}ms，强制转 ALIVE: {}", QUARANTINE_TIMEOUT_MS, key);
                topologyManager.markAlive(key, false);
                distributedStorage.refreshNodeIndex();
                quarantineSince.remove(key);
            } else if (since == null) {
                // QUARANTINE 状态但无记录时间戳，重新触发反向同步
                quarantineSince.put(key, System.currentTimeMillis());
                triggerReverseReconcile(n);
            }
        }
    }

    private void checkQuarantineTimeout(String key) {
        Long since = quarantineSince.get(key);
        if (since != null && System.currentTimeMillis() - since > QUARANTINE_TIMEOUT_MS) {
            log.warn("QUARANTINE 超时 {}ms，强制转 ALIVE: {}", QUARANTINE_TIMEOUT_MS, key);
            topologyManager.markAlive(key, false);
            quarantineSince.remove(key);
        }
    }

    // ==================== 手动 failover API ====================

    /** 手动强制 failover 指定节点 */
    public boolean forceFailover(String nodeKey) {
        StorageNode node = topologyManager.getNode(nodeKey);
        if (node == null) {
            log.warn("手动 failover 失败：节点不存在 {}", nodeKey);
            return false;
        }
        if (node.isSelf()) {
            log.warn("手动 failover 失败：不能 failover 自身 {}", nodeKey);
            return false;
        }
        NodeStatus st = topologyManager.getNodeStatus(nodeKey);
        if (st == NodeStatus.DEAD) {
            log.warn("手动 failover：节点已 DEAD {}", nodeKey);
            return true;
        }
        log.info("手动 failover: {}", nodeKey);
        failureCount.computeIfAbsent(nodeKey, k -> new AtomicInteger(0)).set(DEAD_THRESHOLD);
        triggerFailover(node);
        return true;
    }

    /** 取消手动 failover（直接转 ALIVE，跳过反向同步） */
    public boolean cancelFailover(String nodeKey) {
        StorageNode node = topologyManager.getNode(nodeKey);
        if (node == null) return false;
        NodeStatus st = topologyManager.getNodeStatus(nodeKey);
        if (st != NodeStatus.DEAD && st != NodeStatus.QUARANTINE) {
            log.warn("取消 failover：节点非 DEAD/QUARANTINE {}", nodeKey);
            return true;
        }
        log.info("取消手动 failover: {} 直接转 ALIVE", nodeKey);
        topologyManager.markAlive(nodeKey, false);
        distributedStorage.refreshNodeIndex();
        failureCount.remove(nodeKey);
        quarantineSince.remove(nodeKey);
        return true;
    }

    // ==================== 健康状态查询 ====================

    /** 健康状态快照 */
    public List<Map<String, Object>> healthSnapshot() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (StorageNode n : topologyManager.getNodes()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("nodeKey", n.nodeKey());
            m.put("host", n.getHost());
            m.put("httpPort", n.getHttpPort());
            m.put("self", n.isSelf());
            m.put("status", topologyManager.getNodeStatus(n.nodeKey()));
            m.put("failures", failureCount.getOrDefault(n.nodeKey(), new AtomicInteger(0)).get());
            out.add(m);
        }
        return out;
    }

    /** failover epoch（来自拓扑管理器） */
    public long getFailoverEpoch() {
        return topologyManager.getFailoverEpoch();
    }
}
