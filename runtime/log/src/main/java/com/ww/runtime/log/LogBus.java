package com.ww.runtime.log;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 日志订阅/广播总线（Spring-free）：维护订阅者集合，把批量写入器产出的日志
 * 按订阅过滤条件广播给所有匹配监听器。
 *
 * <p>订阅者集合用 {@link CopyOnWriteArrayList}（订阅/退订低频、广播高频）。
 * 心跳保活与具体传输载体（SSE/gRPC）由宿主适配层负责，本总线只关心
 * 「过滤 + 分发」。
 *
 * <h2>过滤语义</h2>
 * <ul>
 *   <li>{@code level}：忽略大小写的精确匹配（可空 = 全部级别）；</li>
 *   <li>{@code node}：精确匹配（可空 = 全部节点）；</li>
 *   <li>{@code keyword}：忽略大小写匹配 {@code message/loggerName/node/stackTrace}
 *       四字段任意命中（可空 = 不过滤）。</li>
 * </ul>
 */
public final class LogBus {

    private final CopyOnWriteArrayList<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    /**
     * 注册一个订阅。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空）
     * @param sink    匹配日志的接收器
     * @param onClose 订阅结束回调（可空）；供聚合 hub 关闭上游连接
     * @return 订阅句柄（用于 {@link #unsubscribe}）
     */
    public Subscription subscribe(String level, String node, String keyword,
                                  Consumer<LogRecord> sink, Runnable onClose) {
        Objects.requireNonNull(sink, "sink");
        Subscription sub = new Subscription(
                normalize(level), normalize(node), keyword == null ? null : keyword.toLowerCase(Locale.ROOT),
                sink, onClose);
        subscriptions.add(sub);
        return sub;
    }

    /** 移除订阅并触发 onClose 回调（幂等）。 */
    public void unsubscribe(Subscription sub) {
        if (sub == null) {
            return;
        }
        if (subscriptions.remove(sub)) {
            Runnable onClose = sub.onClose();
            if (onClose != null) {
                try {
                    onClose.run();
                } catch (RuntimeException ignored) {
                    // 关闭回调异常不影响总线
                }
            }
        }
    }

    /** 批量广播：逐条按订阅过滤分发；监听器抛异常即移除该订阅。 */
    public void publish(List<LogRecord> records) {
        if (records == null || records.isEmpty() || subscriptions.isEmpty()) {
            return;
        }
        for (LogRecord record : records) {
            for (Subscription sub : subscriptions) {
                if (!matches(sub, record)) {
                    continue;
                }
                try {
                    sub.sink().accept(record);
                } catch (RuntimeException e) {
                    unsubscribe(sub);
                }
            }
        }
    }

    /** 当前订阅者数（供监控/调试）。 */
    public int subscriberCount() {
        return subscriptions.size();
    }

    /** 停止：移除全部订阅（触发各自 onClose）。 */
    public void stop() {
        for (Subscription sub : subscriptions) {
            unsubscribe(sub);
        }
    }

    private static boolean matches(Subscription sub, LogRecord r) {
        if (sub.level() != null && !sub.level().equalsIgnoreCase(r.level())) {
            return false;
        }
        if (sub.node() != null && !sub.node().equals(r.node())) {
            return false;
        }
        if (sub.keyword() != null) {
            return containsIgnoreCase(r.message(), sub.keyword())
                    || containsIgnoreCase(r.loggerName(), sub.keyword())
                    || containsIgnoreCase(r.node(), sub.keyword())
                    || containsIgnoreCase(r.stackTrace(), sub.keyword());
        }
        return true;
    }

    /** keyword 已预先小写化；value 逐段小写比较避免整串分配（简单实现：直接 toLowerCase）。 */
    private static boolean containsIgnoreCase(String value, String lowerKeyword) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(lowerKeyword);
    }

    private static String normalize(String value) {
        return (value == null || value.isEmpty()) ? null : value;
    }

    /**
     * 订阅句柄：过滤条件 + 接收器 + 关闭回调。
     */
    public record Subscription(String level, String node, String keyword,
                               Consumer<LogRecord> sink, Runnable onClose) {
    }
}
