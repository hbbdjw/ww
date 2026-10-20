package com.ww.runtime.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.AppenderBase;

/**
 * 日志采集 appender：将 logback 日志事件转换为 {@link LogRecord} 并投递到 {@link LogBuffer}。
 * <p>
 * 该 appender 由 logback 配置实例化（非 Spring 管理），因此只做轻量转换与入队，
 * 绝不在业务线程上执行任何 IO；真正的批量落库由宿主侧后台线程完成。
 * <p>
 * 节点标识通过 {@link #setNodeKey(String)} 在应用启动后由宿主注入（runtime NodeId）；
 * 在注入前的启动早期日志以 {@code unknown} 标记。
 * <p>
 * <b>防死循环策略</b>（仅阻止日志记录线程自身的输出进入采集链路，其余线程的所有日志全部采集）：
 * <ol>
 *   <li>{@link #SUPPRESSED}：宿主批量写入器在落库期间调用 {@link #setSuppressed(boolean)} 抑制，
 *       打断「写库 → JDBC SQL 日志 → 采集 → 再写库」的自引用循环；</li>
 *   <li>{@link #EXCLUDED_THREAD_PREFIXES}：日志处理线程（batch-writer、SSE heartbeat）自身产生的日志
 *       直接丢弃，避免其 warn/info 日志又回流进采集队列。</li>
 * </ol>
 */
public class DbLogAppender extends AppenderBase<ILoggingEvent> {

    private static final int MAX_MESSAGE_LENGTH = 8192;
    private static final int MAX_LOGGER_LENGTH = 255;
    private static final int MAX_THREAD_LENGTH = 128;
    private static final int MAX_STACK_LENGTH = 16384;

    /**
     * 重入保护：日志处理线程在执行落库期间调用 {@link #setSuppressed(boolean)}，
     * 本线程产生的日志直接丢弃，打断「写库 → 写库自身产生的 SQL 日志 → 再写库」的自引用循环。
     * <p>广播（publish）阶段不会执行 SQL，无需抑制；其余业务线程永远为 FALSE。
     */
    private static final ThreadLocal<Boolean> SUPPRESSED = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * 日志处理线程名排除列表：这些线程自身产生的日志（warn/info 等）一律不采集。
     * <p>用于过滤日志处理线程本身的内部运行日志，避免采集队列回流自身输出。
     * 业务请求线程、虚拟线程、定时调度线程等不在列表中，所有日志正常采集。
     */
    private static final String[] EXCLUDED_THREAD_PREFIXES = {
            "log-batch-writer",
            "log-sse-heartbeat"
    };

    private static volatile String nodeKey = "unknown";

    /**
     * 设置本机节点标识（runtime NodeId），由宿主批量写入器启动时调用。
     */
    public static void setNodeKey(String key) {
        nodeKey = (key == null || key.isEmpty()) ? "unknown" : key;
    }

    /** 当前节点标识（测试/诊断用）。 */
    public static String nodeKey() {
        return nodeKey;
    }

    /**
     * 开启/关闭当前线程的采集抑制，由宿主批量写入器在落库期间调用；
     * 仅打断"写库 → 写库自身产生的 SQL 日志 → 再写库"的自引用循环，不影响广播与业务线程。
     */
    public static void setSuppressed(boolean suppressed) {
        SUPPRESSED.set(suppressed);
    }

    @Override
    protected void append(ILoggingEvent event) {
        // 落库期间日志处理线程产生的日志直接丢弃，打断自引用循环
        if (SUPPRESSED.get()) {
            return;
        }
        String rawThread = event.getThreadName();
        if (rawThread != null && isExcludedThread(rawThread)) {
            return;
        }
        try {
            String level = event.getLevel() == null ? "UNKNOWN" : event.getLevel().toString();
            String message = truncate(event.getFormattedMessage(), MAX_MESSAGE_LENGTH);
            String loggerName = truncate(event.getLoggerName(), MAX_LOGGER_LENGTH);
            String threadName = truncate(rawThread, MAX_THREAD_LENGTH);
            String stackTrace = null;
            if (event.getThrowableProxy() != null) {
                stackTrace = truncate(ThrowableProxyUtil.asString(event.getThrowableProxy()), MAX_STACK_LENGTH);
            }
            LogRecord record = new LogRecord(
                    null, level, loggerName, threadName, message, stackTrace, nodeKey, event.getTimeStamp());
            LogBuffer.offer(record);
        } catch (Exception e) {
            // 采集失败绝不能影响业务线程与日志主链路
            addError("DbLogAppender 采集日志失败", e);
        }
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    /** 日志处理线程名是否被排除：命中 {@link #EXCLUDED_THREAD_PREFIXES} 中任意前缀则排除。 */
    private static boolean isExcludedThread(String threadName) {
        for (String prefix : EXCLUDED_THREAD_PREFIXES) {
            if (threadName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
