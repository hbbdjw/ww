package com.ww.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据重平衡管理：当拓扑变化（节点加入/退出/failover）时，把不再属于本节点的 key 推送给新 primary。
 *
 * <p>策略：
 * <ul>
 *   <li>定时（默认 30s）扫描 membership.topologyVersion，与上次已重平衡版本比较</li>
 *   <li>版本不同则触发一次 {@link #rebalanceNow()}：遍历本地 KV，对每个 key 计算新 owners</li>
 *   <li>若 self 不是新 primary → 把 key/value/remainingTtl 通过 OP_MIGRATE_PUSH 推到新 primary</li>
 *   <li>推送成功后从本地 store 删除（避免双写）</li>
 *   <li>Stream 数据：HRW 路由保证新写落到新 primary，旧 stream 数据保留在旧 primary 上，
 *       由应用层显式迁移或容忍读陈旧。此为已知限制，符合 project_memory 中
 *       "stream consumer group metadata 不持久化" 的现有约束</li>
 * </ul>
 *
 * <p>幂等性：每个 key 推送是 fire-and-forget，但本地删除前会确认 send 成功。
 * 若网络抖动导致重发，目标节点的 put 是覆盖语义，不会损坏数据。
 *
 * <p>背压：单次重平衡每批最多 {@link #BATCH_SIZE} 个 key，避免大集群拓扑变更时一次性推送过多。
 */
public class RebalanceManager {

    private static final Logger log = LoggerFactory.getLogger(RebalanceManager.class);

    static final long DEFAULT_SCAN_INTERVAL_MS = 30_000L;
    static final int BATCH_SIZE = 500;
    static final long MAX_ENTRIES_PER_PUSH = 200;

    private final ClusterMembership membership;
    private final DistributedCache distributedCache;
    private final SimpleCache<String, Object> localCache;
    private final String authToken;
    private final Function<ClusterNode, ? extends CacheRpcClient> clientFactory;
    private final long scanIntervalMs;

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile long lastRebalancedVersion = -1;
    private final AtomicLong totalMigratedKeys = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private volatile long lastRebalanceMs = 0;
    private volatile long lastRebalanceDurationMs = 0;
    private volatile long lastRebalanceMigratedKeys = 0;
    private volatile String lastError = null;
    private volatile boolean inProgress = false;

    public RebalanceManager(ClusterMembership membership,
                            DistributedCache distributedCache,
                            SimpleCache<String, Object> localCache,
                            String authToken,
                            long scanIntervalMs) {
        this(membership, distributedCache, localCache, authToken, scanIntervalMs,
                node -> new CacheClient(node.getHost(), node.getPort(), authToken));
    }

    public RebalanceManager(ClusterMembership membership,
                            DistributedCache distributedCache,
                            SimpleCache<String, Object> localCache,
                            String authToken,
                            long scanIntervalMs,
                            Function<ClusterNode, ? extends CacheRpcClient> clientFactory) {
        this.membership = membership;
        this.distributedCache = distributedCache;
        this.localCache = localCache;
        this.authToken = authToken;
        this.clientFactory = clientFactory != null ? clientFactory
                : node -> new CacheClient(node.getHost(), node.getPort(), authToken);
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
    }

    public RebalanceManager(ClusterMembership membership,
                            DistributedCache distributedCache,
                            SimpleCache<String, Object> localCache,
                            String authToken) {
        this(membership, distributedCache, localCache, authToken, DEFAULT_SCAN_INTERVAL_MS);
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-rebalance");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::scanAndRebalance,
                Math.min(scanIntervalMs, 5000L), scanIntervalMs, TimeUnit.MILLISECONDS);
        log.info("RebalanceManager 启动: scanInterval={}ms, batchSize={}", scanIntervalMs, BATCH_SIZE);
    }

    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try { scheduler.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            scheduler = null;
        }
    }

    public boolean isRunning() { return running; }

    /** 定时扫描入口：检测 topologyVersion 变化则触发重平衡 */
    void scanAndRebalance() {
        if (!running) return;
        if (inProgress) return;
        long currentVersion = membership.getTopologyVersion();
        if (currentVersion == lastRebalancedVersion) return;
        try {
            rebalanceNow();
            lastRebalancedVersion = currentVersion;
        } catch (Exception e) {
            log.warn("rebalance 扫描失败: {}", e.getMessage(), e);
            lastError = e.getMessage();
        }
    }

    /**
     * 立即执行一次重平衡：扫描本地所有 KV key，把不属于本节点的推送给新 primary。
     * @return 本次迁移的 key 数量
     */
    public synchronized long rebalanceNow() {
        if (inProgress) {
            log.debug("rebalance 已在进行中，跳过");
            return 0;
        }
        inProgress = true;
        long startMs = System.currentTimeMillis();
        long migrated = 0;
        try {
            RendezvousHash hrw = distributedCache.getHash();
            int replicas = distributedCache.getReplicas();
            ClusterNode self = distributedCache.getSelf();
            if (self == null) {
                log.warn("rebalance: self 为空，跳过");
                return 0;
            }

            java.util.Set<String> snapshot = localCache.keys();
            List<String> toMigrate = new ArrayList<>();
            for (String key : snapshot) {
                List<ClusterNode> owners = hrw.selectNodes(key, replicas);
                // 迁移条件：self 不是新 primary。
                // 拓扑变更后，新 primary 必须持有权威副本，因此即便 self 仍是 backup，
                // 也需要把数据 push 给新 primary（backup 副本由后续写复制重建，
                // 这里优先保证数据落到 primary，避免读不到）。
                // 当 replicas > aliveNodes 时，owners 仅含 alive 节点，self 作为 backup
                // 也会被判定为 "非 primary" 从而触发 push，符合重平衡语义。
                ClusterNode primary = owners.isEmpty() ? null : owners.getFirst();
                boolean isPrimary = primary != null && primary.equals(self);
                if (!isPrimary) toMigrate.add(key);
            }

            if (toMigrate.isEmpty()) {
                lastRebalanceMs = System.currentTimeMillis();
                lastRebalanceDurationMs = System.currentTimeMillis() - startMs;
                lastRebalanceMigratedKeys = 0;
                return 0;
            }

            log.info("rebalance: 本地 {} 个 key 需迁移到新 primary (topologyVersion={})",
                    toMigrate.size(), membership.getTopologyVersion());

            // 分批迁移
            int batchCount = 0;
            for (int i = 0; i < toMigrate.size(); i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, toMigrate.size());
                List<String> batch = toMigrate.subList(i, end);
                migrated += migrateBatch(batch, hrw, replicas);
                batchCount++;
                totalBatches.incrementAndGet();
            }
            totalMigratedKeys.addAndGet(migrated);
            lastRebalanceMs = System.currentTimeMillis();
            lastRebalanceDurationMs = System.currentTimeMillis() - startMs;
            lastRebalanceMigratedKeys = migrated;
            lastError = null;
            log.info("rebalance 完成: 迁移 {} 个 key, 耗时 {}ms, 批次 {}",
                    migrated, lastRebalanceDurationMs, batchCount);
            return migrated;
        } catch (Exception e) {
            lastError = e.getMessage();
            log.error("rebalance 异常", e);
            throw e;
        } finally {
            inProgress = false;
        }
    }

    /** 迁移一批 key：按新 primary 分组，每组通过单次 OP_MIGRATE_PUSH 推送 */
    private long migrateBatch(List<String> keys, RendezvousHash hrw, int replicas) {
        // 按新 primary nodeKey 分组
        Map<String, List<String>> byPrimary = new LinkedHashMap<>();
        Map<String, ClusterNode> primaryNodes = new LinkedHashMap<>();
        for (String key : keys) {
            List<ClusterNode> owners = hrw.selectNodes(key, replicas);
            if (owners.isEmpty()) continue;
            ClusterNode primary = owners.getFirst();
            byPrimary.computeIfAbsent(primary.nodeKey(), k -> new ArrayList<>()).add(key);
            primaryNodes.put(primary.nodeKey(), primary);
        }

        long migrated = 0;
        for (Map.Entry<String, List<String>> e : byPrimary.entrySet()) {
            ClusterNode primary = primaryNodes.get(e.getKey());
            if (primary.isSelf()) continue; // 不应发生（已在 caller 过滤）
            List<String> keyList = e.getValue();
            // 拆成更小的子批，避免单帧过大
            for (int i = 0; i < keyList.size(); i += (int) MAX_ENTRIES_PER_PUSH) {
                int end = Math.min(i + (int) MAX_ENTRIES_PER_PUSH, keyList.size());
                List<String> sub = keyList.subList(i, end);
                migrated += pushAndDelete(primary, sub);
            }
        }
        return migrated;
    }

    /** 把指定 keys 推送到 primary，推送成功的从本地删除 */
    private long pushAndDelete(ClusterNode primary, List<String> keys) {
        List<Object> entries = new ArrayList<>(keys.size());
        List<String> pushedKeys = new ArrayList<>(keys.size());
        for (String key : keys) {
            Object value = localCache.get(key);
            if (value == null) continue; // 已被删除或过期
            long ttl = localCache.remainingTtlMillis(key);
            // [key, value, remainingTtlMillis]
            entries.add(List.of(key, value, ttl));
            pushedKeys.add(key);
        }
        if (entries.isEmpty()) return 0;

        CacheRpcClient client = clientFactory.apply(primary);
        try {
            byte[] payload = CacheProtocol.encodeMigratePushArgs(entries);
            client.send(CacheProtocol.FLAG_NORMAL, CacheProtocol.OP_MIGRATE_PUSH, payload, "migrate-push");
        } catch (IOException e) {
            log.warn("迁移到 {} 失败，跳过 {} 个 key: {}", primary.nodeKey(), pushedKeys.size(), e.getMessage());
            lastError = "migrate to " + primary.nodeKey() + " failed: " + e.getMessage();
            return 0;
        } finally {
            client.close();
        }
        // 推送成功后本地删除
        for (String key : pushedKeys) {
            localCache.remove(key);
        }
        return pushedKeys.size();
    }

    /**
     * 处理远端 OP_MIGRATE_PUSH 请求：把收到的 key/value 列表写入本地 store。
     * <p>注意：直接走 localCache.put，不再触发复制（因为这是 rebalance 流量，不是业务写）。
     * @param entries List of [key, value, remainingTtlMillis]
     * @return 成功写入的 key 数量
     */
    @SuppressWarnings("unchecked")
    public int handleMigratePush(List<Object> entries) {
        if (entries == null || entries.isEmpty()) return 0;
        int count = 0;
        for (Object o : entries) {
            if (!(o instanceof List<?> row) || row.size() < 3) continue;
            String key = String.valueOf(row.get(0));
            Object value = row.get(1);
            Object ttlObj = row.get(2);
            long ttl;
            if (ttlObj instanceof Number n) {
                ttl = n.longValue();
            } else {
                ttl = Long.MAX_VALUE;
            }
            try {
                if (ttl == Long.MAX_VALUE || ttl < 0) {
                    localCache.put(key, value);
                } else if (ttl == 0) {
                    // 已过期，跳过
                    continue;
                } else {
                    localCache.put(key, value, ttl);
                }
                count++;
            } catch (Exception e) {
                log.warn("handleMigratePush 写入 key={} 失败: {}", key, e.getMessage());
            }
        }
        return count;
    }

    /** 管理面查询：重平衡状态摘要 */
    public Map<String, Object> getStatus() {
        // 使用 HashMap 而非 ConcurrentHashMap：本方法是只读快照，且 lastError 可能为 null
        // （ConcurrentHashMap 不允许 null value）
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("running", running);
        m.put("inProgress", inProgress);
        m.put("scanIntervalMs", scanIntervalMs);
        m.put("lastRebalancedVersion", lastRebalancedVersion);
        m.put("currentTopologyVersion", membership.getTopologyVersion());
        m.put("lastRebalanceMs", lastRebalanceMs);
        m.put("lastRebalanceDurationMs", lastRebalanceDurationMs);
        m.put("lastRebalanceMigratedKeys", lastRebalanceMigratedKeys);
        m.put("totalMigratedKeys", totalMigratedKeys.get());
        m.put("totalBatches", totalBatches.get());
        m.put("lastError", lastError);
        return m;
    }
}
