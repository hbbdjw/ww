package com.ww.runtime.log;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LogBus：四字段关键字过滤、级别/节点过滤、大小写不敏感、onClose 回调、异常订阅自动移除。
 */
class LogBusTest {

    private static LogRecord record(String level, String logger, String message,
                                    String node, String stack) {
        return new LogRecord(null, level, logger, "thread-1", message, stack, node,
                System.currentTimeMillis());
    }

    @Test
    void publishBroadcastsToMatchingSubscription() {
        LogBus bus = new LogBus();
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        bus.subscribe(null, null, null, received::add, null);

        bus.publish(List.of(record("INFO", "a.B", "hello", "node-1", null)));
        assertThat(received).hasSize(1);
        assertThat(bus.subscriberCount()).isEqualTo(1);
    }

    @Test
    void levelFilterIsCaseInsensitive() {
        LogBus bus = new LogBus();
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        bus.subscribe("warn", null, null, received::add, null);

        bus.publish(List.of(
                record("WARN", "a", "m1", "n1", null),
                record("INFO", "a", "m2", "n1", null)));
        assertThat(received).extracting(LogRecord::message).containsExactly("m1");
    }

    @Test
    void nodeFilterIsExactMatch() {
        LogBus bus = new LogBus();
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        bus.subscribe(null, "node-2", null, received::add, null);

        bus.publish(List.of(
                record("INFO", "a", "m1", "node-1", null),
                record("INFO", "a", "m2", "node-2", null),
                record("INFO", "a", "m3", "node-22", null))); // 前缀相同但非精确匹配
        assertThat(received).extracting(LogRecord::message).containsExactly("m2");
    }

    @Test
    void keywordMatchesAllFourFieldsCaseInsensitively() {
        LogBus bus = new LogBus();

        // 命中 message
        assertThat(matchesKeyword(bus, "HELLO", record("INFO", "a", "say hello", "n1", null))).isTrue();
        // 命中 loggerName
        assertThat(matchesKeyword(bus, "MYLOGGER", record("INFO", "com.x.MyLogger", "m", "n1", null))).isTrue();
        // 命中 node（对齐既有规范：node 字段参与关键字匹配）
        assertThat(matchesKeyword(bus, "NODE-2", record("INFO", "a", "m", "node-2", null))).isTrue();
        // 命中 stackTrace
        assertThat(matchesKeyword(bus, "NULLPOINTER", record("ERROR", "a", "m", "n1", "java.lang.NullPointerException: x"))).isTrue();
        // 全不命中
        assertThat(matchesKeyword(bus, "zzz", record("INFO", "a", "m", "n1", null))).isFalse();
    }

    private boolean matchesKeyword(LogBus bus, String keyword, LogRecord record) {
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        LogBus.Subscription sub = bus.subscribe(null, null, keyword, received::add, null);
        try {
            bus.publish(List.of(record));
            return !received.isEmpty();
        } finally {
            bus.unsubscribe(sub);
        }
    }

    @Test
    void unsubscribeTriggersOnCloseCallback() {
        LogBus bus = new LogBus();
        AtomicBoolean closed = new AtomicBoolean(false);
        LogBus.Subscription sub = bus.subscribe(null, null, null, r -> {
        }, () -> closed.set(true));

        bus.unsubscribe(sub);
        assertThat(closed).isTrue();
        assertThat(bus.subscriberCount()).isZero();

        // 幂等：重复退订不重复触发
        bus.unsubscribe(sub);
        assertThat(closed).isTrue();
    }

    @Test
    void failingSinkIsRemovedAutomatically() {
        LogBus bus = new LogBus();
        bus.subscribe(null, null, null, r -> {
            throw new IllegalStateException("downstream gone");
        }, null);

        bus.publish(List.of(record("INFO", "a", "m", "n1", null)));
        assertThat(bus.subscriberCount()).isZero();
    }

    @Test
    void stopRemovesAllSubscriptionsAndFiresCallbacks() {
        LogBus bus = new LogBus();
        AtomicBoolean closed = new AtomicBoolean(false);
        bus.subscribe(null, null, null, r -> {
        }, () -> closed.set(true));

        bus.stop();
        assertThat(bus.subscriberCount()).isZero();
        assertThat(closed).isTrue();
    }
}
