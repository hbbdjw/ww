package com.ww.admin.service.log;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * DbLogAppender 采集转换单元测试：验证 logback 事件转 LogRecord 并入队。
 */
class DbLogAppenderTest {

    @BeforeEach
    void resetBuffer() {
        LogBuffer.clear();
        LogBuffer.configure(10_000);
        DbLogAppender.setNodeKey("unknown");
        DbLogAppender.setSuppressed(false);
    }

    @Test
    void appendConvertsToLogRecord() {
        DbLogAppender appender = new DbLogAppender();

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getFormattedMessage()).thenReturn("hello world");
        when(event.getLoggerName()).thenReturn("com.ww.Test");
        when(event.getThreadName()).thenReturn("main");
        when(event.getThrowableProxy()).thenReturn(null);
        when(event.getTimeStamp()).thenReturn(123456789L);

        appender.append(event);

        // 测试环境下其他测试产生的日志也会进入全局 LogBuffer，按 message 过滤出本次 append 的记录
        List<LogRecord> batch = LogBuffer.pollBatch(1000, 0);
        LogRecord r = batch.stream()
                .filter(item -> "hello world".equals(item.message()))
                .findFirst()
                .orElse(null);
        assertThat(r).isNotNull();
        assertThat(r.level()).isEqualTo("INFO");
        assertThat(r.loggerName()).isEqualTo("com.ww.Test");
        assertThat(r.threadName()).isEqualTo("main");
        assertThat(r.stackTrace()).isNull();
        assertThat(r.timestamp()).isEqualTo(123456789L);
    }

    @Test
    void appendUsesConfiguredNodeKey() {
        DbLogAppender.setNodeKey("127.0.0.1:8701");
        DbLogAppender appender = new DbLogAppender();

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.WARN);
        when(event.getFormattedMessage()).thenReturn("warn msg");
        when(event.getLoggerName()).thenReturn("com.ww.Test");
        when(event.getThreadName()).thenReturn("main");
        when(event.getThrowableProxy()).thenReturn(null);
        when(event.getTimeStamp()).thenReturn(1L);

        appender.append(event);

        List<LogRecord> batch = LogBuffer.pollBatch(1000, 0);
        LogRecord r = batch.stream()
                .filter(item -> "warn msg".equals(item.message()))
                .findFirst()
                .orElse(null);
        assertThat(r).isNotNull();
        assertThat(r.node()).isEqualTo("127.0.0.1:8701");
    }

    @Test
    void appendSkipsWhenSuppressed() {
        DbLogAppender appender = new DbLogAppender();
        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getFormattedMessage()).thenReturn("suppressed msg");
        when(event.getLoggerName()).thenReturn("com.ww.Test");
        when(event.getThreadName()).thenReturn("main");
        when(event.getThrowableProxy()).thenReturn(null);
        when(event.getTimeStamp()).thenReturn(1L);

        // 落库/推送线程抑制期间，日志不应入队（防自引用循环）
        DbLogAppender.setSuppressed(true);
        try {
            appender.append(event);
        } finally {
            DbLogAppender.setSuppressed(false);
        }

        List<LogRecord> batch = LogBuffer.pollBatch(1000, 0);
        assertThat(batch.stream().anyMatch(item -> "suppressed msg".equals(item.message()))).isFalse();
    }

    @Test
    void appendSkipsExcludedThreadAndCollectsP6spy() {
        DbLogAppender appender = new DbLogAppender();

        // 防死循环策略：按线程名前缀（log-batch-writer / log-sse-heartbeat）排除日志处理线程自身输出；
        // p6spy SQL 日志在业务线程上正常采集（不再按 loggerName 排除）
        ILoggingEvent writerEvent = mock(ILoggingEvent.class);
        when(writerEvent.getLevel()).thenReturn(Level.INFO);
        when(writerEvent.getFormattedMessage()).thenReturn("writer internal msg");
        when(writerEvent.getLoggerName()).thenReturn("com.ww.Test");
        when(writerEvent.getThreadName()).thenReturn("log-batch-writer");
        when(writerEvent.getThrowableProxy()).thenReturn(null);
        when(writerEvent.getTimeStamp()).thenReturn(1L);

        ILoggingEvent p6spyEvent = mock(ILoggingEvent.class);
        when(p6spyEvent.getLevel()).thenReturn(Level.INFO);
        when(p6spyEvent.getFormattedMessage()).thenReturn("sql trace");
        when(p6spyEvent.getLoggerName()).thenReturn("p6spy");
        when(p6spyEvent.getThreadName()).thenReturn("main");
        when(p6spyEvent.getThrowableProxy()).thenReturn(null);
        when(p6spyEvent.getTimeStamp()).thenReturn(1L);

        appender.append(writerEvent);
        appender.append(p6spyEvent);

        List<LogRecord> batch = LogBuffer.pollBatch(1000, 0);
        assertThat(batch.stream().anyMatch(item -> "writer internal msg".equals(item.message()))).isFalse();
        assertThat(batch.stream().anyMatch(item -> "sql trace".equals(item.message()))).isTrue();
    }
}
