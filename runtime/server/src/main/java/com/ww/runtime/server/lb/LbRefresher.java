package com.ww.runtime.server.lb;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbConfigStore;

/**
 * LB 配置与节点视图的轻量定时刷新（纯本地缓存/共享库轮询，非网络探测）。
 *
 * <ul>
 *   <li>每 {@code nodeViewRefreshMs}（默认 1s）经 {@link NodeRegistryLbDirectory#refresh()}
 *       刷新 ACTIVE 节点快照；</li>
 *   <li>每 {@code configRefreshMs}（默认 3s）从 {@link LbConfigStore} 拉取最新配置，
 *       原子发布给选择器与过滤器（含 PathPattern 重建）；</li>
 *   <li>{@link #refreshNow()} 供管理端点 PUT 成功后立即刷新本机。</li>
 * </ul>
 */
public final class LbRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(LbRefresher.class);

    private final LbConfigStore store;
    private final NodeRegistryLbDirectory directory;
    private final LbProxyFilter filter;

    private ScheduledExecutorService executor;

    public LbRefresher(LbConfigStore store,
                       NodeRegistryLbDirectory directory,
                       LbProxyFilter filter) {
        this.store = Objects.requireNonNull(store, "store");
        this.directory = Objects.requireNonNull(directory, "directory");
        this.filter = Objects.requireNonNull(filter, "filter");
    }

    /** 启动：立即加载一次配置与节点视图，再排定两个守护周期任务。 */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        LbConfig initial = store.get();
        filter.acceptConfig(initial); // 单一配置通道：过滤器内部同步给选择器
        directory.refresh();

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-lb-refresher");
            t.setDaemon(true);
            return t;
        });
        long configPeriod = Math.max(500L, initial.configRefreshMs());
        long nodePeriod = Math.max(200L, initial.nodeViewRefreshMs());
        exec.scheduleAtFixedRate(this::safeRefreshNodes, nodePeriod, nodePeriod, TimeUnit.MILLISECONDS);
        exec.scheduleAtFixedRate(this::safeRefreshConfig, configPeriod, configPeriod, TimeUnit.MILLISECONDS);
        executor = exec;
        LOG.info("lb refresher started: configRefreshMs={}, nodeViewRefreshMs={}", configPeriod, nodePeriod);
    }

    /** 停机：守护线程立即退出（start/stop 幂等）。 */
    public synchronized void stop() {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        executor = null;
    }

    /** 立即从存储刷新一次配置（管理写后调用）；失败返回 false 由调用方决定提示。 */
    public boolean refreshNow() {
        try {
            publish(store.get());
            return true;
        } catch (RuntimeException e) {
            LOG.warn("lb config immediate refresh failed: {}", e.getMessage());
            return false;
        }
    }

    private void safeRefreshConfig() {
        try {
            publish(store.get());
        } catch (RuntimeException e) {
            LOG.warn("lb config refresh failed (keeping last good config): {}", e.getMessage());
        }
    }

    private void safeRefreshNodes() {
        try {
            directory.refresh();
        } catch (RuntimeException e) {
            LOG.warn("lb node view refresh failed (keeping last snapshot): {}", e.getMessage());
        }
    }

    private void publish(LbConfig config) {
        filter.acceptConfig(config);
    }
}
