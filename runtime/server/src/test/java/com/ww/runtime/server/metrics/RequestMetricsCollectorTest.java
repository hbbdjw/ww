package com.ww.runtime.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class RequestMetricsCollectorTest {

    @Test
    void countsRequestsWithinSameSecond() {
        RequestMetricsCollector collector = new RequestMetricsCollector(60);
        long second = 1_000_000L;

        collector.recordAt(second);
        collector.recordAt(second);
        collector.recordAt(second);

        assertThat(collector.countForSecond(second)).isEqualTo(3L);
    }

    @Test
    void rollsBucketAtSecondBoundaryAndKeepsHistory() {
        RequestMetricsCollector collector = new RequestMetricsCollector(60);

        collector.recordAt(100L);
        collector.recordAt(100L);
        collector.recordAt(101L);

        assertThat(collector.countForSecond(100L)).isEqualTo(2L);
        assertThat(collector.countForSecond(101L)).isEqualTo(1L);
    }

    @Test
    void returnsNoDataForUnknownSecond() {
        RequestMetricsCollector collector = new RequestMetricsCollector(60);
        collector.recordAt(100L);

        // 从未有过请求的秒
        assertThat(collector.countForSecond(200L)).isEqualTo(RequestMetricsCollector.NO_DATA);
    }

    @Test
    void evictsAfterWindowRollsOver() {
        int window = 4;
        RequestMetricsCollector collector = new RequestMetricsCollector(window);

        collector.recordAt(1L);
        collector.recordAt(2L);
        // 前进一个完整窗口：秒 1 的槽位被秒 5 复用，旧数据必须滚走
        collector.recordAt(5L);

        assertThat(collector.countForSecond(5L)).isEqualTo(1L);
        assertThat(collector.countForSecond(1L)).isEqualTo(RequestMetricsCollector.NO_DATA);
        // 秒 2 与秒 6 同槽但尚未滚到 6，秒 2 仍可读
        collector.recordAt(6L);
        assertThat(collector.countForSecond(2L)).isEqualTo(RequestMetricsCollector.NO_DATA);
        assertThat(collector.countForSecond(6L)).isEqualTo(1L);
    }

    @Test
    void concurrentRecordingWithinOneSecondLosesNoCounts() throws Exception {
        RequestMetricsCollector collector = new RequestMetricsCollector(8);
        int threads = 8;
        int perThread = 5_000;
        long second = 77L;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicLong errors = new AtomicLong();
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        for (int i = 0; i < perThread; i++) {
                            collector.recordAt(second);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                });
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(errors.get()).isZero();
        assertThat(collector.countForSecond(second)).isEqualTo((long) threads * perThread);
    }

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new RequestMetricsCollector(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
