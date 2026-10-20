package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * InMemoryDegradedPartitionStore 单测：mark 幂等 / clear 容错 / 查询口径 /
 * 记录校验（与 JDBC 实现同语义契约）。
 */
class InMemoryDegradedPartitionStoreTest {

    private static final NodeId DEAD = NodeId.of("node-0");

    private InMemoryDegradedPartitionStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryDegradedPartitionStore();
    }

    @Test
    void markThenQueryRoundTrip() {
        store.mark("CACHE", 7, DEAD, "no live replica");

        assertThat(store.isDegraded("CACHE", 7)).isTrue();
        assertThat(store.isDegraded("CACHE", 8)).isFalse();
        assertThat(store.isDegraded("STORAGE", 7)).isFalse();
        assertThat(store.partitionsOf("CACHE")).containsExactly(7);
        assertThat(store.ownersOf("CACHE")).containsExactly(Map.entry(7, DEAD));

        List<DegradedPartition> list = store.list("CACHE");
        assertThat(list).hasSize(1);
        assertThat(list.getFirst().capability()).isEqualTo("CACHE");
        assertThat(list.getFirst().partition()).isEqualTo(7);
        assertThat(list.getFirst().owner()).isEqualTo(DEAD);
        assertThat(list.getFirst().reason()).isEqualTo("no live replica");
        assertThat(list.getFirst().sinceMs()).isGreaterThan(0L);
    }

    @Test
    void markIsIdempotentUpsert() {
        store.mark("CACHE", 7, DEAD, "first");
        store.mark("CACHE", 7, DEAD, "second");

        assertThat(store.partitionsOf("CACHE")).containsExactly(7);
        assertThat(store.list("CACHE").getFirst().reason()).isEqualTo("second");
    }

    @Test
    void clearRemovesSinglePartitionAndToleratesMissing() {
        store.mark("CACHE", 7, DEAD, "r");
        store.mark("CACHE", 9, DEAD, "r");

        store.clear("CACHE", 7);
        assertThat(store.partitionsOf("CACHE")).containsExactly(9);

        store.clear("CACHE", 123); // 不存在：no-op 不抛
        store.clear("STORAGE", 9); // 能力不存在：no-op 不抛
        assertThat(store.partitionsOf("CACHE")).containsExactly(9);
    }

    @Test
    void listAllSpansCapabilitiesSorted() {
        store.mark("STORAGE", 3, DEAD, "r");
        store.mark("CACHE", 12, DEAD, "r");
        store.mark("CACHE", 4, DEAD, "r");

        List<DegradedPartition> all = store.listAll();
        assertThat(all).hasSize(3);
        assertThat(all.get(0).capability()).isEqualTo("CACHE");
        assertThat(all.get(0).partition()).isEqualTo(4);
        assertThat(all.get(1).partition()).isEqualTo(12);
        assertThat(all.get(2).capability()).isEqualTo("STORAGE");
    }

    @Test
    void emptyQueriesReturnEmptyViews() {
        assertThat(store.partitionsOf("CACHE")).isEmpty();
        assertThat(store.list("CACHE")).isEmpty();
        assertThat(store.listAll()).isEmpty();
        assertThat(store.ownersOf("CACHE")).isEmpty();
        assertThat(store.isDegraded("CACHE", 1)).isFalse();
    }

    @Test
    void recordRejectsInvalidInput() {
        assertThatThrownBy(() -> new DegradedPartition("CACHE", -1, DEAD, 1L, "r"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DegradedPartition("CACHE", 1, DEAD, 1L, "  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DegradedPartition(null, 1, DEAD, 1L, "r"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void snapshotsAreImmutableCopies() {
        store.mark("CACHE", 7, DEAD, "r");
        Set<Integer> partitions = store.partitionsOf("CACHE");
        store.clear("CACHE", 7);
        assertThat(partitions).containsExactly(7); // 快照不受后续变更影响
    }
}
