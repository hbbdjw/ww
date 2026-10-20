package com.ww.runtime.server.degraded;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.capability.DegradedPartitionView;
import com.ww.runtime.placement.DegradedPartition;
import com.ww.runtime.placement.DegradedPartitionStore;

/**
 * P0-2 分区隔离的数据面视图：{@link DegradedPartitionStore} 的带 TTL 内存快照，
 * 供 Storage/Cache 网关热路径查询（无 DB 访问，一次 map + set 查找）。
 *
 * <p>刷新周期 1s（隔离标记由 leader 治愈产生，秒级延迟可接受——窗口内请求的
 * 失败形态是连接拒绝而非快速 503，语义正确性不受影响）；管理面变更
 * （force-adopt）后经 {@link #refreshNow()} 立即生效。</p>
 *
 * <p>生命周期由 Spring 管理（{@link #start()}/{@link #stop()} 幂等）；守护线程随 JVM 退出。</p>
 */
public final class DegradedPartitionCache implements DegradedPartitionView {

    private static final Logger LOG = LoggerFactory.getLogger(DegradedPartitionCache.class);

    /** 快照刷新周期（毫秒）。 */
    public static final long REFRESH_INTERVAL_MS = 1_000L;

    private final DegradedPartitionStore store;

    /** capability → 隔离分区集合（整体替换发布，读写无锁）。 */
    private volatile Map<String, Set<Integer>> snapshot = Map.of();

    private ScheduledExecutorService executor;

    public DegradedPartitionCache(DegradedPartitionStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** 启动：立即刷新一次 + 周期刷新（幂等）。 */
    @PostConstruct
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-degraded-cache");
            t.setDaemon(true);
            return t;
        });
        exec.scheduleWithFixedDelay(this::safeRefresh,
                REFRESH_INTERVAL_MS, REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        executor = exec;
        safeRefresh();
    }

    /** 停止（幂等）。 */
    @PreDestroy
    public synchronized void stop() {
        ScheduledExecutorService exec = executor;
        if (exec == null) {
            return;
        }
        executor = null;
        exec.shutdownNow();
    }

    /** 立即刷新（管理面变更后调用）。 */
    public void refreshNow() {
        safeRefresh();
    }

    /** 当前快照中的隔离分区总数（监控/测试）。 */
    public int degradedCount() {
        int total = 0;
        for (Set<Integer> partitions : snapshot.values()) {
            total += partitions.size();
        }
        return total;
    }

    @Override
    public boolean isDegraded(String capability, int partition) {
        Set<Integer> partitions = snapshot.get(capability);
        return partitions != null && partitions.contains(partition);
    }

    private void safeRefresh() {
        try {
            Map<String, Set<Integer>> next = new HashMap<>();
            for (DegradedPartition record : store.listAll()) {
                next.computeIfAbsent(record.capability(), k -> new java.util.HashSet<>())
                        .add(record.partition());
            }
            next.replaceAll((k, v) -> Set.copyOf(v));
            snapshot = Map.copyOf(next);
        } catch (RuntimeException e) {
            // 刷新失败保留上一快照：隔离标记至多延迟一个周期生效/解除，
            // 数据面失败形态退化为连接拒绝（语义正确），不清空快照误放行
            LOG.warn("degraded partition cache refresh failed (keeping last snapshot): {}", e.getMessage());
        }
    }
}
