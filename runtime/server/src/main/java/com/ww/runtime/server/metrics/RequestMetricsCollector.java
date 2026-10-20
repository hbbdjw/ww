package com.ww.runtime.server.metrics;

import java.util.concurrent.atomic.AtomicLongArray;

import org.springframework.stereotype.Component;

/**
 * 节点入站 HTTP 请求计数器（按整秒分桶）。
 *
 * <p>固定长度环形缓冲（默认保留最近 {@value #WINDOW_SECONDS} 个整秒），
 * 内存占用恒定，不随运行时间增长（监控指标有界，杜绝无界堆积导致 OOM）。</p>
 *
 * <p>每个槽位记录「归属 epochSecond」与该秒计数：跨秒首次写入时进入短临界区
 * 归零并翻转账属，之后整秒内全部为 volatile 读 + CAS 自增的无锁热路径；
 * 归属翻转顺序固定为「先归零、后挂账」，保证自增线程要么阻塞到翻账完成、
 * 要么看到新账后从 0 起计，不丢计数。</p>
 */
@Component
public final class RequestMetricsCollector {

    /** 默认保留窗口（秒）：覆盖 hub 拉取延迟与节点间小幅时钟漂移。 */
    public static final int WINDOW_SECONDS = 120;

    /** 无数据：目标秒桶已滚出保留窗口，或该秒尚未产生任何计数。 */
    public static final long NO_DATA = -1L;

    private final int window;

    /** 槽位归属的 epochSecond（0 为初始值；epochSecond 恒正，不构成歧义）。 */
    private final AtomicLongArray owners;

    /** 各槽位当前计数。 */
    private final AtomicLongArray counts;

    public RequestMetricsCollector() {
        this(WINDOW_SECONDS);
    }

    RequestMetricsCollector(int windowSeconds) {
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds must be positive, got " + windowSeconds);
        }
        this.window = windowSeconds;
        this.owners = new AtomicLongArray(windowSeconds);
        this.counts = new AtomicLongArray(windowSeconds);
    }

    /** 记录一次入站请求（以当前墙钟秒归桶）。 */
    public void record() {
        recordAt(System.currentTimeMillis() / 1000L);
    }

    /** 记录一次入站请求并显式指定归桶秒（测试用）。 */
    void recordAt(long epochSecond) {
        int index = slot(epochSecond);
        rollIfNeeded(index, epochSecond);
        counts.incrementAndGet(index);
    }

    /**
     * 读取指定秒的请求数。
     *
     * @return 该秒计数；桶已滚出窗口/该秒无任何记录时返回 {@link #NO_DATA}
     */
    public long countForSecond(long epochSecond) {
        int index = slot(epochSecond);
        return owners.get(index) == epochSecond ? counts.get(index) : NO_DATA;
    }

    /** 上一个已封账完整秒的 epochSecond（当前进行中的秒不参与封账统计）。 */
    public static long lastCompletedSecond() {
        return System.currentTimeMillis() / 1000L - 1L;
    }

    private void rollIfNeeded(int index, long epochSecond) {
        if (owners.get(index) == epochSecond) {
            return;
        }
        synchronized (this) {
            if (owners.get(index) != epochSecond) {
                // 顺序不可交换：必须先清零再挂账，自增线程仅在看到新账主后才无锁自增
                counts.set(index, 0);
                owners.set(index, epochSecond);
            }
        }
    }

    private int slot(long epochSecond) {
        return (int) Math.floorMod(epochSecond, window);
    }
}
