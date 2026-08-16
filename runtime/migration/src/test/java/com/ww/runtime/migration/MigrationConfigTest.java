package com.ww.runtime.migration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 限流/并发配置校验：of 校验 + 默认值 + 限流睡眠计算。
 */
class MigrationConfigTest {

    @Test
    void defaultsMatchSpec() {
        MigrationConfig config = MigrationConfig.defaults();
        assertThat(config.maxConcurrentTasks()).isEqualTo(2);
        assertThat(config.maxBytesPerSecond()).isEqualTo(64L * 1024 * 1024);
        assertThat(config.chunkSize()).isEqualTo(4 * 1024 * 1024);
        assertThat(config.ackWindow()).isEqualTo(8);
    }

    @Test
    void ofAcceptsValidValues() {
        MigrationConfig config = MigrationConfig.of(4, 1024, 512, 16);
        assertThat(config.maxConcurrentTasks()).isEqualTo(4);
        assertThat(config.maxBytesPerSecond()).isEqualTo(1024L);
        assertThat(config.chunkSize()).isEqualTo(512);
        assertThat(config.ackWindow()).isEqualTo(16);
    }

    @Test
    void ofRejectsNonPositiveValues() {
        assertThatThrownBy(() -> MigrationConfig.of(0, 1024, 512, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentTasks");
        assertThatThrownBy(() -> MigrationConfig.of(-1, 1024, 512, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MigrationConfig.of(2, 0, 512, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytesPerSecond");
        assertThatThrownBy(() -> MigrationConfig.of(2, -10, 512, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MigrationConfig.of(2, 1024, 0, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize");
        assertThatThrownBy(() -> MigrationConfig.of(2, 1024, 512, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackWindow");
    }

    @Test
    void throttleMillisPerChunkMatchesByteRatio() {
        // 4MB / 64MBps = 62.5ms → 整数截断 62
        assertThat(MigrationConfig.defaults().throttleMillisPerChunk()).isEqualTo(62L);
        // 高速率下截断为 0（单测用配置，不产生真实睡眠）
        MigrationConfig fast = MigrationConfig.of(2, 8L * 1024 * 1024 * 1024, 4096, 8);
        assertThat(fast.throttleMillisPerChunk()).isZero();
        // 1MB / 1MBps = 1000ms
        MigrationConfig slow = MigrationConfig.of(1, 1024 * 1024, 1024 * 1024, 1);
        assertThat(slow.throttleMillisPerChunk()).isEqualTo(1000L);
    }
}
