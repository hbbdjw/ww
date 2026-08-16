package com.ww.admin.service.log;

/**
 * 日志采集记录（轻量 DTO）。
 * <p>
 * 由 {@link DbLogAppender} 从 logback 的 {@code ILoggingEvent} 转换后投递到 {@link LogBuffer}，
 * 再由 {@link LogBatchWriter} 批量落库。{@code timestamp} 保留日志真实产生时刻（毫秒），
 * 落库时写回 {@code SysLog.createdDate}，避免批量缓冲延迟导致时间偏移。
 *
 * @param level      日志级别
 * @param loggerName Logger 名称
 * @param threadName 线程名
 * @param message    消息正文
 * @param stackTrace 异常堆栈（可空）
 * @param node       集群节点标识（host:port）
 * @param timestamp  日志产生时刻（epoch 毫秒）
 */
public record LogRecord(
        String level,
        String loggerName,
        String threadName,
        String message,
        String stackTrace,
        String node,
        long timestamp
) {
}
