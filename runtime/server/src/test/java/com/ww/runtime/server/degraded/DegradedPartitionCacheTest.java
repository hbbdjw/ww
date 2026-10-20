package com.ww.runtime.server.degraded;

import com.ww.runtime.capability.DegradedPartitionView;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.placement.DegradedPartitionStore;
import com.ww.runtime.placement.InMemoryDegradedPartitionStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DegradedPartitionCache：TTL 快照视图的装载/刷新/失败保留语义（数据面热路径端口）。
 */
class DegradedPartitionCacheTest {

    private static final NodeId DEAD = NodeId.of("node-0");

    private InMemoryDegradedPartitionStore store;
    private DegradedPartitionCache cache;

    @BeforeEach
    void setUp() {
        store = new InMemoryDegradedPartitionStore();
        cache = new DegradedPartitionCache(store);
    }

    @AfterEach
    void tearDown() {
        cache.stop();
    }

    @Test
    void startLoadsSnapshotAndIsDegradedReflectsStore() {
        store.mark("CACHE", 5, DEAD, "no live replica");
        store.mark("STORAGE", 9, DEAD, "no live replica");

        cache.start();

        DegradedPartitionView view = cache;
        assertThat(view.isDegraded("CACHE", 5)).isTrue();
        assertThat(view.isDegraded("STORAGE", 9)).isTrue();
        assertThat(view.isDegraded("CACHE", 6)).isFalse();
        assertThat(view.isDegraded("MOCK", 5)).isFalse();
        assertThat(cache.degradedCount()).isEqualTo(2);
    }

    @Test
    void refreshNowPicksUpMarkAndClear() {
        cache.start();
        assertThat(cache.isDegraded("CACHE", 5)).isFalse();

        store.mark("CACHE", 5, DEAD, "r");
        cache.refreshNow();
        assertThat(cache.isDegraded("CACHE", 5)).isTrue();

        store.clear("CACHE", 5);
        cache.refreshNow();
        assertThat(cache.isDegraded("CACHE", 5)).isFalse();
        assertThat(cache.degradedCount()).isZero();
    }

    @Test
    void periodicRefreshPicksUpStoreChanges() throws Exception {
        cache.start();

        store.mark("CACHE", 7, DEAD, "r");
        // 周期刷新（1s）兜底：不显式 refreshNow 也应最终可见
        long deadline = System.currentTimeMillis() + 5_000;
        while (!cache.isDegraded("CACHE", 7) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(cache.isDegraded("CACHE", 7)).isTrue();
    }

    @Test
    void refreshFailureKeepsLastSnapshot() {
        cache.start();
        store.mark("CACHE", 5, DEAD, "r");
        cache.refreshNow();
        assertThat(cache.isDegraded("CACHE", 5)).isTrue();

        // 存储故障：快照不清空（隔离标记至多延迟生效/解除，不误放行）
        DegradedPartitionCache failing = new DegradedPartitionCache(new DegradedPartitionStore() {
            @Override
            public void mark(String capability, int partition, NodeId owner, String reason) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void clear(String capability, int partition) {
                throw new IllegalStateException("boom");
            }

            @Override
            public java.util.Set<Integer> partitionsOf(String capability) {
                throw new IllegalStateException("boom");
            }

            @Override
            public java.util.List<com.ww.runtime.placement.DegradedPartition> list(String capability) {
                throw new IllegalStateException("boom");
            }

            @Override
            public java.util.List<com.ww.runtime.placement.DegradedPartition> listAll() {
                throw new IllegalStateException("boom");
            }

            @Override
            public boolean isDegraded(String capability, int partition) {
                throw new IllegalStateException("boom");
            }

            @Override
            public java.util.Map<Integer, NodeId> ownersOf(String capability) {
                throw new IllegalStateException("boom");
            }
        });
        try {
            failing.start();
            failing.refreshNow();
            // 首刷即失败：空快照但不抛异常
            assertThat(failing.isDegraded("CACHE", 5)).isFalse();
        } finally {
            failing.stop();
        }
    }
}
