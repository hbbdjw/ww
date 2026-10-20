package com.ww.runtime.data.cache.engine;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AOF 重写原子性状态机回归测试（C3 + C12 + C36，修复计划 §3.2）。
 *
 * <p>锚定语义：
 * <ul>
 *   <li>C3：重写换文件窗口内到达的命令零丢失（08-27 锁收窄曾引入该丢写窗口）；</li>
 *   <li>C12：双写缓冲超限 → 中止本次重写 + 告警计数，数据零丢失；</li>
 *   <li>C36：stop() 与在途重写并发不挂死。</li>
 * </ul>
 *
 * <p>混沌类场景允许真实线程与短轮询等待（与 {@link AofServiceTest#getClass()} 同风格，
 * IO 场景不受 TestClock 确定性约束限制）。
 */
@DisplayName("AofService 重写换文件状态机（C3/C12/C36）")
class AofServiceRewriteSwapTest {

    @TempDir
    File dir;

    private AofProperties props() {
        AofProperties p = new AofProperties();
        p.setEnabled(true);
        p.setDir(dir.getPath());
        p.setAppendfsync("no");
        p.setAutoRewrite(false); // 重写由用例显式触发，避免自动重写干扰断言
        return p;
    }

    private SimpleCache<String, Object> start(AofService[] holder, AofProperties p) {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        try {
            aof.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cache.setAofService(aof);
        holder[0] = aof;
        return cache;
    }

    /** 回放阶段：新开缓存从同一目录回放（回放期间不挂 AOF，与 CacheCapability 启动顺序一致）。 */
    private SimpleCache<String, Object> replayFromDisk(AofService[] holder, AofProperties p) {
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof = new AofService(p, restored);
        try {
            aof.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        aof.replay(restored);
        holder[0] = aof;
        return restored;
    }

    private static void awaitRewriteFinish(AofService aof) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (aof.getStatus().isRewriting() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(aof.getStatus().isRewriting()).as("rewrite should finish in time").isFalse();
    }

    @Test
    @DisplayName("C3：并发写 + 重写换文件，重启回放零丢命令")
    void concurrentWritesDuringRewriteSurviveSwap() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = start(holder, p);

        // 预写快照数据：重写快照阶段产生真实工作量，拉长换文件前的窗口
        int seed = 5_000;
        for (int i = 0; i < seed; i++) {
            cache.put("seed-" + i, "v" + i);
        }

        // 后台持续写入（覆盖快照期/排空期/换文件窗口全阶段）
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> writers = new ArrayList<>();
        int writerCount = 4;
        CountDownLatch started = new CountDownLatch(writerCount);
        for (int w = 0; w < writerCount; w++) {
            final int wid = w;
            Thread t = new Thread(() -> {
                started.countDown();
                int i = 0;
                while (!stop.get()) {
                    cache.put("w-" + wid + "-" + (i++), "x");
                }
            }, "aof-swap-writer-" + w);
            t.setDaemon(true);
            t.start();
            writers.add(t);
        }
        started.await();

        holder[0].bgRewriteAof();
        // 重写进行中让写者持续加压，直至换文件完成
        awaitRewriteFinish(holder[0]);
        stop.set(true);
        for (Thread t : writers) {
            t.join(5_000);
        }
        // 停写后再写一批：验证换文件后新流承接正常
        int tail = 500;
        for (int i = 0; i < tail; i++) {
            cache.put("tail-" + i, "t" + i);
        }
        holder[0].stop();

        // 回放断言：种子 + 并发窗口内写入（每线程至少落盘 1 条）+ 尾部全量可见
        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        for (int i = 0; i < seed; i++) {
            assertThat(restored.get("seed-" + i)).as("seed-%d", i).isEqualTo("v" + i);
        }
        for (int w = 0; w < writerCount; w++) {
            assertThat(restored.get("w-" + w + "-0"))
                    .as("writer %d 的首条写入不得在换文件窗口蒸发", w).isEqualTo("x");
        }
        for (int i = 0; i < tail; i++) {
            assertThat(restored.get("tail-" + i)).as("tail-%d", i).isEqualTo("t" + i);
        }
        holder[0].stop();
    }

    @Test
    @DisplayName("C12：双写缓冲超限中止重写，告警计数 > 0 且数据零丢失")
    void rewriteAbortsOnBufferOverflowWithoutDataLoss() throws Exception {
        AofProperties p = props();
        // 上限压到极小：重写期间的双写必然触发超限中止
        p.setRewriteBufferMaxBytes(64L);
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = start(holder, p);

        int seed = 200;
        for (int i = 0; i < seed; i++) {
            cache.put("k-" + i, "value-" + i);
        }

        // 后台写者：确保重写快照/排空期间双写缓冲迅速超限
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread writer = new Thread(() -> {
            int i = 0;
            while (!stop.get()) {
                cache.put("hot-" + (i++), "h");
            }
        }, "aof-overflow-writer");
        writer.setDaemon(true);
        writer.start();

        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);
        stop.set(true);
        writer.join(5_000);

        // 中止计数 > 0（可观测性红线：超限不得静默）
        assertThat(holder[0].getStatus().getRewriteAbortedOnOverflow())
                .as("超限中止必须计数告警").isGreaterThan(0);

        int tail = 200;
        for (int i = 0; i < tail; i++) {
            cache.put("post-" + i, "p" + i);
        }
        holder[0].stop();

        // 中止 = 旧文件保持不变 + 缓冲留存，重启回放零丢失
        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        for (int i = 0; i < seed; i++) {
            assertThat(restored.get("k-" + i)).as("k-%d", i).isEqualTo("value-" + i);
        }
        assertThat(restored.get("hot-0")).as("重写窗口写入不得丢失").isEqualTo("h");
        for (int i = 0; i < tail; i++) {
            assertThat(restored.get("post-" + i)).as("post-%d", i).isEqualTo("p" + i);
        }
        holder[0].stop();
    }

    @Test
    @DisplayName("C36：stop() 与在途重写并发不挂死")
    void stopDuringRewriteDoesNotDeadlock() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = start(holder, p);

        // 大快照拉长重写时长，制造 stop 与在途重写的交叠窗口
        for (int i = 0; i < 20_000; i++) {
            cache.put("big-" + i, "payload-" + i);
        }
        holder[0].bgRewriteAof();

        Thread stopper = new Thread(holder[0]::stop, "aof-stopper");
        stopper.start();
        stopper.join(40_000); // 30s 重写等待上限 + 余量
        assertThat(stopper.isAlive()).as("stop() 不得挂死").isFalse();
    }
}
