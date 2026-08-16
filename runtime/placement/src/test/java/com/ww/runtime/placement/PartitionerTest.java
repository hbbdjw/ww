package com.ww.runtime.placement;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionerTest {

    @Test
    void partitionIsStableForSameKeyAndCount() {
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            int first = Partitioner.partition(key, 4096);
            assertThat(Partitioner.partition(key, 4096))
                    .as("key %s", key)
                    .isEqualTo(first);
        }
    }

    @Test
    void partitionStaysWithinBounds() {
        for (int partitionCount : new int[]{1, 2, 7, 64, 4096}) {
            for (int i = 0; i < 5000; i++) {
                int p = Partitioner.partition("k" + i, partitionCount);
                assertThat(p)
                        .as("partitionCount=%d key=k%d", partitionCount, i)
                        .isBetween(0, partitionCount - 1);
            }
        }
    }

    @Test
    void singlePartitionAlwaysZero() {
        assertThat(Partitioner.partition("anything", 1)).isZero();
        assertThat(Partitioner.partition("other", 1)).isZero();
    }

    @Test
    void differentKeysSpreadAcrossPartitions() {
        Set<Integer> distinct = new HashSet<>();
        for (int i = 0; i < 4096; i++) {
            distinct.add(Partitioner.partition("spread-" + i, 16));
        }
        assertThat(distinct).hasSize(16);
    }

    @Test
    void rejectsNonPositivePartitionCount() {
        assertThatThrownBy(() -> Partitioner.partition("key", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Partitioner.partition("key", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
