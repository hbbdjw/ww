package com.ww.runtime.data;

import com.ww.runtime.common.MigrationException;
import com.ww.runtime.migration.PartitionDataAdapter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CompositePartitionAdapter：按 capability 分发、未知 capability 抛异常。
 */
class CompositePartitionAdapterTest {

    /** 记录最后一次调用的 capability 名。 */
    private static final class RecordingAdapter implements PartitionDataAdapter {
        final AtomicReference<String> lastCapability = new AtomicReference<>();
        final long count;

        RecordingAdapter(long count) {
            this.count = count;
        }

        @Override
        public long itemCount(String capability, int partition) {
            lastCapability.set(capability);
            return count;
        }

        @Override
        public List<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            lastCapability.set(capability);
            return List.of();
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            lastCapability.set(capability);
            return true;
        }

        @Override
        public long verifyCount(String capability, int partition) {
            lastCapability.set(capability);
            return count;
        }
    }

    @Test
    void dispatchesByCapabilityName() {
        RecordingAdapter mock = new RecordingAdapter(10L);
        RecordingAdapter cache = new RecordingAdapter(20L);
        RecordingAdapter storage = new RecordingAdapter(30L);
        CompositePartitionAdapter composite = new CompositePartitionAdapter(Map.of(
                "MOCK", mock, "CACHE", cache, "STORAGE", storage));

        assertThat(composite.itemCount("MOCK", 0)).isEqualTo(10L);
        assertThat(mock.lastCapability.get()).isEqualTo("MOCK");

        assertThat(composite.itemCount("CACHE", 0)).isEqualTo(20L);
        assertThat(cache.lastCapability.get()).isEqualTo("CACHE");

        assertThat(composite.verifyCount("STORAGE", 0)).isEqualTo(30L);
        assertThat(storage.lastCapability.get()).isEqualTo("STORAGE");

        // 大小写归一
        assertThat(composite.itemCount("cache", 0)).isEqualTo(20L);

        assertThat(composite.exportChunks("MOCK", 0, 0L)).isEmpty();
        assertThat(composite.importChunks("CACHE", 0, List.of())).isTrue();
    }

    @Test
    void unknownCapabilityThrowsMigrationException() {
        CompositePartitionAdapter composite = new CompositePartitionAdapter(
                Map.of("MOCK", new RecordingAdapter(1L)));
        assertThatThrownBy(() -> composite.itemCount("UNKNOWN", 0))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("UNKNOWN");
    }

    @Test
    void emptyDelegatesRejected() {
        assertThatThrownBy(() -> new CompositePartitionAdapter(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompositePartitionAdapter(null))
                .isInstanceOf(NullPointerException.class);
    }
}
