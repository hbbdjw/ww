package com.ww.runtime.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LogBuffer：有界队列投递/批量消费/丢弃计数/重配置迁移。
 */
class LogBufferTest {

    @BeforeEach
    void setUp() {
        LogBuffer.configure(10_000);
        LogBuffer.clear();
    }

    @AfterEach
    void tearDown() {
        LogBuffer.clear();
    }

    private static LogRecord record(String message) {
        return new LogRecord(null, "INFO", "logger", "thread", message, null, "node-1",
                System.currentTimeMillis());
    }

    @Test
    void offerAndPollBatchRoundTrip() {
        LogBuffer.offer(record("m1"));
        LogBuffer.offer(record("m2"));
        LogBuffer.offer(record("m3"));

        List<LogRecord> batch = LogBuffer.pollBatch(10, 50);
        assertThat(batch).extracting(LogRecord::message).containsExactly("m1", "m2", "m3");
        assertThat(LogBuffer.size()).isZero();
    }

    @Test
    void pollBatchReturnsEmptyOnTimeout() {
        List<LogRecord> batch = LogBuffer.pollBatch(10, 30);
        assertThat(batch).isEmpty();
    }

    @Test
    void offerBeyondCapacityDropsAndCounts() {
        LogBuffer.configure(2);
        long droppedBefore = LogBuffer.droppedCount();
        assertThat(LogBuffer.offer(record("a"))).isTrue();
        assertThat(LogBuffer.offer(record("b"))).isTrue();
        assertThat(LogBuffer.offer(record("c"))).isFalse(); // 队列满丢弃
        assertThat(LogBuffer.droppedCount()).isEqualTo(droppedBefore + 1);
        assertThat(LogBuffer.size()).isEqualTo(2);
    }

    @Test
    void configureMigratesBufferedRecords() {
        LogBuffer.offer(record("keep-1"));
        LogBuffer.offer(record("keep-2"));

        LogBuffer.configure(100); // 重建队列并迁移已有日志

        List<LogRecord> batch = LogBuffer.pollBatch(10, 50);
        assertThat(batch).extracting(LogRecord::message).containsExactly("keep-1", "keep-2");
    }

    @Test
    void configureWithInvalidCapacityFallsBackToDefault() {
        LogBuffer.offer(record("x"));
        LogBuffer.configure(0); // 非法容量回退默认值，不丢已有日志
        assertThat(LogBuffer.pollBatch(10, 50)).hasSize(1);
    }
}
