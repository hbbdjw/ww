package com.ww.admin.service.log;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * LogBuffer 有界缓冲队列单元测试：覆盖入队顺序、阻塞批量取、满时丢弃计数、重建迁移。
 */
class LogBufferTest {

    @AfterEach
    void resetBuffer() {
        LogBuffer.clear();
        LogBuffer.configure(10_000);
    }

    @Test
    void offerAndPollBatchReturnsInOrder() {
        LogBuffer.configure(100);
        LogBuffer.offer(record("INFO", "msg-1"));
        LogBuffer.offer(record("INFO", "msg-2"));
        LogBuffer.offer(record("WARN", "msg-3"));

        List<LogRecord> batch = LogBuffer.pollBatch(10, 0);
        assertThat(batch).hasSize(3);
        assertThat(batch.get(0).message()).isEqualTo("msg-1");
        assertThat(batch.get(1).message()).isEqualTo("msg-2");
        assertThat(batch.get(2).message()).isEqualTo("msg-3");
    }

    @Test
    void pollBatchEmptyTimesOut() {
        LogBuffer.configure(10);
        long t0 = System.currentTimeMillis();
        List<LogRecord> batch = LogBuffer.pollBatch(10, 100);
        long elapsed = System.currentTimeMillis() - t0;
        assertThat(batch).isEmpty();
        assertThat(elapsed).isGreaterThanOrEqualTo(50);
    }

    @Test
    void offerDropsWhenFullAndCounts() {
        LogBuffer.configure(2);
        long before = LogBuffer.droppedCount();
        assertThat(LogBuffer.offer(record("INFO", "1"))).isTrue();
        assertThat(LogBuffer.offer(record("INFO", "2"))).isTrue();
        assertThat(LogBuffer.offer(record("INFO", "3"))).isFalse(); // 满，丢弃
        assertThat(LogBuffer.droppedCount()).isEqualTo(before + 1);
    }

    @Test
    void pollBatchRespectsMaxLimit() {
        LogBuffer.configure(100);
        for (int i = 0; i < 10; i++) {
            LogBuffer.offer(record("INFO", "msg-" + i));
        }
        List<LogRecord> batch = LogBuffer.pollBatch(4, 0);
        assertThat(batch).hasSize(4);
    }

    @Test
    void configureMigratesExisting() {
        LogBuffer.configure(100);
        LogBuffer.offer(record("INFO", "old"));
        LogBuffer.configure(10); // 重建队列，迁移旧数据
        List<LogRecord> batch = LogBuffer.pollBatch(10, 0);
        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).message()).isEqualTo("old");
    }

    private static LogRecord record(String level, String message) {
        return new LogRecord(level, "logger", "thread", message, null, "node", System.currentTimeMillis());
    }
}
