package com.ww.runtime.lb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryLbConfigStoreTest {

    @Test
    void startsFromDefault() {
        InMemoryLbConfigStore store = new InMemoryLbConfigStore();
        assertThat(store.get()).isEqualTo(LbConfig.defaultConfig());
    }

    @Test
    void casUpdateIncrementsVersion() {
        InMemoryLbConfigStore store = new InMemoryLbConfigStore();
        LbConfig enabled = new LbConfig(true, "node-1", LbStrategy.RANDOM, true,
                LbConfig.defaultConfig().pathPrefixes(),
                java.util.Map.of(), 3000, 1000, 3000, 3_600_000L, 0);
        LbConfig updated = store.update(enabled, 0);
        assertThat(updated.version()).isEqualTo(1);
        assertThat(updated.enabled()).isTrue();
        assertThat(updated.strategy()).isEqualTo(LbStrategy.RANDOM);
        assertThat(store.get().version()).isEqualTo(1);
    }

    @Test
    void staleVersionConflicts() {
        InMemoryLbConfigStore store = new InMemoryLbConfigStore();
        store.update(LbConfig.defaultConfig(), 0);
        assertThatThrownBy(() -> store.update(LbConfig.defaultConfig(), 0))
                .isInstanceOf(LbConfigConflictException.class)
                .satisfies(e -> assertThat(((LbConfigConflictException) e).actualVersion()).isEqualTo(1));
    }
}
