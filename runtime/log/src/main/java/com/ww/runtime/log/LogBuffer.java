package com.ww.runtime.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 日志有界缓冲队列（进程内单例）。
 * <p>
 * logback 的 {@link DbLogAppender} 运行在业务线程，若直接写库会拖慢请求；这里用有界队列解耦：
 * 业务线程仅 {@link #offer(LogRecord)}，队列满时丢弃该条并计数（绝不阻塞业务线程），
 * 由宿主后台批量写入线程 {@link #pollBatch} 消费。
 */
public final class LogBuffer {

    /** 默认队列容量（未配置时）。 */
    private static final int DEFAULT_CAPACITY = 10_000;

    private static volatile BlockingQueue<LogRecord> queue = new LinkedBlockingQueue<>(DEFAULT_CAPACITY);

    /** 因队列满而被丢弃的日志条数。 */
    private static final AtomicLong DROPPED = new AtomicLong();

    private LogBuffer() {
    }

    /**
     * 启动时按配置重建队列，并迁移旧队列中已缓冲的日志，避免启动早期日志丢失。
     */
    public static void configure(int capacity) {
        if (capacity <= 0) {
            capacity = DEFAULT_CAPACITY;
        }
        BlockingQueue<LogRecord> previous = queue;
        BlockingQueue<LogRecord> next = new LinkedBlockingQueue<>(capacity);
        // 限制转移量，避免目标队列满时 drainTo 抛 IllegalStateException（超出部分丢弃）
        previous.drainTo(next, capacity);
        queue = next;
    }

    /** 清空队列（测试/停机辅助）。 */
    public static void clear() {
        queue.clear();
    }

    /**
     * 非阻塞投递一条日志；队列满时丢弃并计数。
     *
     * @return true 表示成功入队
     */
    public static boolean offer(LogRecord record) {
        boolean accepted = queue.offer(record);
        if (!accepted) {
            DROPPED.incrementAndGet();
        }
        return accepted;
    }

    /**
     * 批量取出日志：阻塞等待至多 {@code timeoutMs} 取第一条，随后尽量凑满 {@code max} 条。
     * 超时且队列为空时返回空列表。
     */
    public static List<LogRecord> pollBatch(int max, long timeoutMs) {
        // 初始容量设上限，避免调用方传入极大 max（如停机 drain 全部）时一次性分配巨额内存
        int initialCapacity = Math.max(1, Math.min(max, 10_000));
        List<LogRecord> batch = new ArrayList<>(initialCapacity);
        try {
            LogRecord first = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (first != null) {
                batch.add(first);
                queue.drainTo(batch, max - 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return batch;
    }

    /** 已丢弃的日志条数。 */
    public static long droppedCount() {
        return DROPPED.get();
    }

    /** 当前队列中的待消费日志条数。 */
    public static int size() {
        return queue.size();
    }
}
