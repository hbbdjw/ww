package com.ww.runtime.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DbLogAppender：正常采集、SUPPRESSED 抑制、日志处理线程名过滤、超长截断、堆栈采集。
 */
class DbLogAppenderTest {

    private LoggerContext context;
    private DbLogAppender appender;

    @BeforeEach
    void setUp() {
        LogBuffer.configure(10_000);
        LogBuffer.clear();
        DbLogAppender.setSuppressed(false);
        DbLogAppender.setNodeKey("node-1");

        context = new LoggerContext();
        Logger logger = context.getLogger("test.logger");
        appender = new DbLogAppender();
        appender.setContext(context);
        appender.start();
        appender.setNodeKey("node-1");
    }

    @AfterEach
    void tearDown() {
        DbLogAppender.setSuppressed(false);
        LogBuffer.clear();
    }

    private LoggingEvent event(String threadName, Level level, String message, Throwable thrown) {
        LoggingEvent event = new LoggingEvent(
                "com.x.Caller", context.getLogger("test.logger"), level, message, thrown, null);
        event.setThreadName(threadName);
        return event;
    }

    @Test
    void normalEventIsCollectedWithNodeKey() {
        appender.append(event("http-exec-1", Level.INFO, "hello log", null));

        List<LogRecord> batch = LogBuffer.pollBatch(10, 50);
        assertThat(batch).hasSize(1);
        LogRecord record = batch.get(0);
        assertThat(record.level()).isEqualTo("INFO");
        assertThat(record.loggerName()).isEqualTo("test.logger");
        assertThat(record.threadName()).isEqualTo("http-exec-1");
        assertThat(record.message()).isEqualTo("hello log");
        assertThat(record.node()).isEqualTo("node-1");
        assertThat(record.stackTrace()).isNull();
        assertThat(record.timestamp()).isPositive();
    }

    @Test
    void suppressedThreadDropsEvents() {
        DbLogAppender.setSuppressed(true);
        try {
            appender.append(event("any-thread", Level.INFO, "should drop", null));
        } finally {
            DbLogAppender.setSuppressed(false);
        }
        assertThat(LogBuffer.pollBatch(10, 30)).isEmpty();
    }

    @Test
    void excludedThreadPrefixesDropEvents() {
        appender.append(event("log-batch-writer", Level.WARN, "writer self log", null));
        appender.append(event("log-sse-heartbeat-1", Level.INFO, "heartbeat self log", null));
        appender.append(event("log-batch-writer-extra", Level.INFO, "prefix match", null));
        // 业务线程正常采集
        appender.append(event("virtual-123", Level.INFO, "business log", null));

        List<LogRecord> batch = LogBuffer.pollBatch(10, 50);
        assertThat(batch).extracting(LogRecord::message).containsExactly("business log");
    }

    @Test
    void longMessageIsTruncated() {
        String longMessage = "x".repeat(20_000);
        appender.append(event("t", Level.INFO, longMessage, null));

        LogRecord record = LogBuffer.pollBatch(10, 50).get(0);
        assertThat(record.message()).hasSize(8192);
    }

    @Test
    void stackTraceIsCaptured() {
        appender.append(event("t", Level.ERROR, "boom", new IllegalStateException("cause")));

        LogRecord record = LogBuffer.pollBatch(10, 50).get(0);
        assertThat(record.stackTrace()).contains("IllegalStateException").contains("cause");
    }

    @Test
    void nodeKeyDefaultsToUnknownBeforeInjection() {
        DbLogAppender.setNodeKey(null);
        assertThat(DbLogAppender.nodeKey()).isEqualTo("unknown");
        DbLogAppender.setNodeKey("");
        assertThat(DbLogAppender.nodeKey()).isEqualTo("unknown");
    }
}
