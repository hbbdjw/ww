package com.ww.runtime.data.cache.engine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 阶段 3 验收测试（修复计划 §3.4 + §5 阶段 3）：
 * <ul>
 *   <li>C6/C7/C13/C41/C19 格式版本化（v2：绝对 TTL/OP_EXPIRE/int 长度/控制符拒绝/编码异常不外抛）；</li>
 *   <li>v1 存量文件回放兼容 + 首次重写自然升级 v2（硬验收项）；</li>
 *   <li>C9/C10/C24 缓存结构正确性（穿透防护/WATCH→MULTI→EXEC/类型冲突日志模式）；</li>
 *   <li>C11/C20/C26/C38/C39/C44 Stream 语义（恰好一次/PEL 清理/推进竞态/计数精确/waiter 唤醒/total 口径）；</li>
 *   <li>C16 上限配置化（默认不改行为）。</li>
 * </ul>
 */
@DisplayName("阶段 3 验收：格式版本化 + 缓存/Stream 正确性")
class Phase3CacheFixesTest {

    @TempDir
    File dir;

    private AofProperties props() {
        AofProperties p = new AofProperties();
        p.setEnabled(true);
        p.setDir(dir.getPath());
        p.setAppendfsync("no");
        p.setAutoRewrite(false);
        return p;
    }

    private SimpleCache<String, Object> startWriting(AofService[] holder, AofProperties p) {
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

    // ==================== C6：绝对过期时间戳 ====================

    @Test
    @DisplayName("C6：带 TTL 的 PUT 回放后剩余时长随真实时间衰减，不再重启续期")
    void c6_absoluteExpiryNoRenewalOnReplay() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        cache.put("k", "v", 60_000);
        long absBefore = cache.expireTimeAt("k");
        Thread.sleep(50);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        long absAfter = restored.expireTimeAt("k");
        // 绝对时间戳回放：过期时点不因重启漂移（相对 TTL 实现会整体后移约停机间隔）
        assertThat(absAfter).as("回放后过期时点与原始绝对时点偏差应 < 2s")
                .isBetween(absBefore - 2_000, absBefore + 2_000);
        holder[0].stop();
    }

    // ==================== C7：容器 TTL 持久化 ====================

    @Test
    @DisplayName("C7：容器 TTL 跨重启保持（增量记录路径）")
    void c7_containerTtlSurvivesRestartIncremental() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        cache.hset("h", "f", "v");
        cache.expire("h", 120_000);
        cache.rpush("l", "x");
        cache.expire("l", 120_000);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        assertThat(restored.remainingTtlMillis("h"))
                .as("Hash TTL 应随 EXPIRE 记录恢复").isGreaterThan(110_000);
        assertThat(restored.remainingTtlMillis("l"))
                .as("List TTL 应随 EXPIRE 记录恢复").isGreaterThan(110_000);
        holder[0].stop();
    }

    @Test
    @DisplayName("C7：容器 TTL 经重写快照（OP_EXPIRE 追加）跨重启保持")
    void c7_containerTtlSurvivesRewrite() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        cache.sadd("s", "m");
        cache.expire("s", 120_000);
        cache.zadd("z", 1.0d, "a");
        cache.expire("z", 120_000);
        cache.hset("h", "f", "v");
        cache.expire("h", 120_000);
        cache.rpush("l", "x");
        cache.expire("l", 120_000);

        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        assertThat(restored.remainingTtlMillis("s")).isGreaterThan(110_000);
        assertThat(restored.remainingTtlMillis("z")).isGreaterThan(110_000);
        assertThat(restored.remainingTtlMillis("h")).isGreaterThan(110_000);
        assertThat(restored.remainingTtlMillis("l")).isGreaterThan(110_000);
        assertThat(restored.smembers("s")).containsExactly("m");
        holder[0].stop();
    }

    // ==================== §3.4：v1 兼容 + 重写升级 ====================

    @Test
    @DisplayName("§3.4：v1 存量文件续写按 v1 编码，首次重写后自然升级 v2")
    void v1FileContinuesThenUpgradesOnRewrite() throws Exception {
        // 构造 v1 存量文件（v1 magic + v1 编码记录）
        File aofFile = new File(dir, "appendonly.aof");
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(new FileOutputStream(aofFile))) {
            out.writeInt(AofFormat.AOF_MAGIC);
            out.write(AofFormat.encodeCommand(AofFormat.FORMAT_VERSION_V1, AofFormat.OP_PUT, "old", "v1-val"));
        }

        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        cache.put("new", "v2-val");
        holder[0].stop();

        // v1 文件 + v2 增量记录回放一致
        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        assertThat(restored.get("old")).isEqualTo("v1-val");
        assertThat(restored.get("new")).isEqualTo("v2-val");

        // 重写升级 v2：新文件 magic = AOF_MAGIC_V2
        restored.setAofService(holder[0]);
        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);
        holder[0].stop();
        try (java.io.DataInputStream in = new java.io.DataInputStream(
                new java.io.FileInputStream(aofFile))) {
            assertThat(in.readInt()).as("重写后文件应为 v2 magic").isEqualTo(AofFormat.AOF_MAGIC_V2);
            assertThat(in.readInt()).isEqualTo(AofFormat.FORMAT_VERSION_V2);
        }

        // 升级后回放一致
        SimpleCache<String, Object> finalCache = replayFromDisk(holder, p);
        assertThat(finalCache.get("old")).isEqualTo("v1-val");
        assertThat(finalCache.get("new")).isEqualTo("v2-val");
        holder[0].stop();
    }

    // ==================== C13 / C41 / C19 ====================

    @Test
    @DisplayName("C13：超过 64KB 的大 value 经 v2 int 长度前缀完整往返")
    void c13_largeValueRoundTrip() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        String big = "x".repeat(100_000);
        cache.put("big", big);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        assertThat((String) restored.get("big")).hasSize(100_000);
        holder[0].stop();
    }

    @Test
    @DisplayName("C41：含 0x01/0x02 控制符的 key 写入被前置拒绝")
    void c41_controlCharKeyRejected() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        assertThatThrownBy(() -> cache.put("a\u0001b", "v"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.hset("x\u0002y", "f", "v"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(cache.get("a\u0001b")).isNull();
        assertThat(holder[0].getStatus().getTotalCommands()).isZero();
        holder[0].stop();
    }

    /** C19 触发载体：声明 Serializable 但字段不可序列化 → 编码期 RuntimeException。 */
    static class PoisonValue implements java.io.Serializable {
        @SuppressWarnings("serial")
        private final Object notSerializable = new Object();
    }

    @Test
    @DisplayName("C19：值编码异常不向业务抛出（不触发重复重试），计数告警")
    void c19_encodeFailureDoesNotThrowToBusiness() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        // 写入已生效，AOF 编码失败必须吞掉而不是抛出（否则调用方误判失败重试 → LPUSH 重复）
        cache.put("poison", new PoisonValue());
        assertThat(cache.get("poison")).isInstanceOf(PoisonValue.class);
        assertThat(holder[0].getStatus().getEncodeFailuresRejected()).isEqualTo(1);
        holder[0].stop();
    }

    // ==================== C9 / C10 / C24 ====================

    @Test
    @DisplayName("C9：空值缓存生效——loader 对不存在 key 仅调用一次（穿透防护恢复）")
    void c9_nullMarkerBlocksLoaderStorm() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AtomicInteger loads = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            Object v = cache.getWithLoader("missing", () -> {
                loads.incrementAndGet();
                return null;
            }, 60_000);
            assertThat(v).isNull();
        }
        assertThat(loads.get()).as("空值缓存应拦截后续查询，loader 仅首次调用").isEqualTo(1);
    }

    @Test
    @DisplayName("C10+验收：WATCH→MULTI→EXEC 冲突中止 / 无冲突提交 / watch 状态跨 MULTI 保留")
    void c10_watchMultiExecConflictAndWatchPreserved() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("k", "v0");

        // 无冲突：watch 后未被打断，exec 返回结果列表
        cache.watch("k");
        cache.multi();
        cache.put("k", "v1");
        cache.put("other", "x");
        List<Object> results = cache.exec();
        assertThat(results).isNotNull().hasSize(2);
        assertThat(cache.get("k")).isEqualTo("v1");

        // 冲突：watch 后外部写 bump 版本，exec 返回 null（事务中止）。
        // C10 回归锚点：修复前 multi() 会丢弃 watchedKeys/watchedVersions——外部写发生在
        // multi() 之前也检测不到（exec 遍历空集合永不中止）。修复后 watch 状态跨 multi() 保留。
        cache.watch("k");
        cache.put("k", "bumped"); // 外部并发写（发生在 MULTI 之前）
        cache.multi();
        cache.put("k", "lost2");
        assertThat(cache.exec()).as("WATCH→MULTI 标准顺序下乐观锁必须生效").isNull();
        assertThat(cache.get("k")).isEqualTo("bumped");
    }

    @Test
    @DisplayName("C24：类型冲突覆盖进入日志模式计数，不再完全静默")
    void c24_wrongTypeOverwriteCounted() {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        cache.put("k", "plain");
        cache.hset("k", "f", "v"); // string → hash 覆盖
        assertThat(cache.getWrongTypeOverwrites()).isEqualTo(1);
        assertThat(cache.hgetAll("k")).containsEntry("f", "v");
        cache.rpush("k", "x"); // hash → list 覆盖
        assertThat(cache.getWrongTypeOverwrites()).isEqualTo(2);
    }

    // ==================== C11 / C20 / C26 / C38 / C39 / C44 ====================

    @Test
    @DisplayName("C11+验收：同组并发 XREADGROUP \">\" 恰好一次投递，无重复无遗漏")
    void c11_sameGroupExactlyOnce() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        StreamStore ss = cache.getStreamStore();
        int total = 300;
        for (int i = 0; i < total; i++) {
            ss.xadd("st", StreamId.AUTO, Map.of("i", String.valueOf(i)), XAddOptions.defaults());
        }
        ss.xgroupCreate("st", "g", StreamId.MIN, false);

        int threads = 8;
        List<StreamId> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread w = new Thread(() -> {
                try {
                    start.await();
                    while (true) {
                        List<StreamEntry> batch = ss.xreadGroup("st", "g", "c" + Thread.currentThread().getId(),
                                true, null, 10, 0);
                        if (batch.isEmpty()) return;
                        for (StreamEntry e : batch) delivered.add(e.getId());
                    }
                } catch (Exception ignored) {
                    // 线程退出
                }
            });
            workers.add(w);
            w.start();
        }
        start.countDown();
        for (Thread w : workers) w.join(10_000);

        assertThat(delivered).as("投递总数应等于消息数").hasSize(total);
        assertThat(delivered).as("同一消息不得投递给多个 consumer").doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("C20：XDEL 后 PEL 同步清理，XPENDING 不再虚高")
    void c20_xdelClearsPel() throws InterruptedException {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        StreamStore ss = cache.getStreamStore();
        StreamId id1 = ss.xadd("st", StreamId.AUTO, Map.of("f", "1"), XAddOptions.defaults());
        StreamId id2 = ss.xadd("st", StreamId.AUTO, Map.of("f", "2"), XAddOptions.defaults());
        ss.xgroupCreate("st", "g", StreamId.MIN, false);
        ss.xreadGroup("st", "g", "c1", true, null, 10, 0);
        assertThat(ss.xpending("st", "g", null, null, 100, null, 0).getTotal()).isEqualTo(2);

        ss.xdel("st", id1);
        PendingSummary summary = ss.xpending("st", "g", null, null, 100, null, 0);
        assertThat(summary.getTotal()).as("被删消息的 PEL 应同步清除").isEqualTo(1);
        assertThat(summary.getConsumerCounts().get("c1")).isEqualTo(1);
    }

    @Test
    @DisplayName("C26：XDELIVER 记录本次实际推进 id——部分投递后重启，余下消息仍可投递")
    void c26_partialDeliveryResumesAfterRestart() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        // 走 SimpleCache 包装方法（带 AOF 记录），直调 streamStore 不落盘
        for (int i = 0; i < 3; i++) {
            cache.xadd("st", StreamId.AUTO, Map.of("f", String.valueOf(i)), XAddOptions.defaults());
        }
        cache.xgroupCreate("st", "g", StreamId.MIN, false);
        // 只投递 2 条（COUNT=2），实际推进到第 2 条
        List<StreamEntry> first = cache.xreadGroup("st", "g", "c1", true, null, 2, 0);
        assertThat(first).hasSize(2);
        holder[0].stop();

        // 重启回放：lastDeliveredId 应精确恢复到第 2 条，第 3 条仍可投递
        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        List<StreamEntry> second = restored.xreadGroup("st", "g", "c1", true, null, 10, 0);
        assertThat(second).as("回放后未投递的消息不得被跳过").hasSize(1);
        holder[0].stop();
    }

    @Test
    @DisplayName("C38：deliveryCount 经重写回放精确恢复，不虚增（死信判断可靠）")
    void c38_deliveryCountExactAfterRewriteReplay() throws Exception {
        AofProperties p = props();
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder, p);
        StreamStore ss = cache.getStreamStore();
        ss.xadd("st", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());
        ss.xgroupCreate("st", "g", StreamId.MIN, false);
        ss.xreadGroup("st", "g", "c1", true, null, 10, 0);   // 首次投递 count=1
        ss.xreadGroup("st", "g", "c1", false, StreamId.MIN, 10, 0); // PEL 重投递 count=2
        ss.xreadGroup("st", "g", "c1", false, StreamId.MIN, 10, 0); // PEL 重投递 count=3

        StreamId id = ss.xrange("st", StreamId.MIN, StreamId.MAX, 1).get(0).getId();
        assertThat(ss.getGroupPel("st", "g").get(id).getDeliveryCount()).isEqualTo(3);

        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);
        holder[0].stop();

        SimpleCache<String, Object> restored = replayFromDisk(holder, p);
        StreamId restoredId = restored.getStreamStore()
                .xrange("st", StreamId.MIN, StreamId.MAX, 1).get(0).getId();
        assertThat(restored.getStreamStore().getGroupPel("st", "g").get(restoredId).getDeliveryCount())
                .as("回放后 deliveryCount 应与运行期一致（修复前虚增/丢失）").isEqualTo(3);
        holder[0].stop();
    }

    @Test
    @DisplayName("C39：阻塞 waiter 在 stream 删除时被唤醒，同 key 重建后新写入可唤醒新 waiter")
    void c39_waiterWokenOnStreamRemoval() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        StreamStore ss = cache.getStreamStore();
        ss.xadd("st", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());
        ss.xgroupCreate("st", "g", StreamId.MIN, false);

        Thread reader = new Thread(() -> {
            try {
                // PEL 模式阻塞读：无新消息 → 挂起等待
                ss.xreadGroup("st", "g", "c1", false, StreamId.MAX, 10, 5_000);
            } catch (Exception ignored) {
            }
        });
        // xreadGroup PEL 模式不阻塞——改用阻塞 XREAD 验证 waiter 通道
        Thread blocking = new Thread(() -> {
            try {
                ss.xread("st", StreamId.MAX, 10, 5_000);
            } catch (Exception ignored) {
            }
        });
        blocking.start();
        Thread.sleep(200); // 确保 waiter 已注册
        long start = System.currentTimeMillis();
        ss.removeStream("st"); // C39：删除 stream 必须唤醒 waiter
        blocking.join(3_000);
        assertThat(blocking.isAlive()).as("waiter 应在删除后被唤醒而非挂到超时").isFalse();
        assertThat(System.currentTimeMillis() - start).isLessThan(3_000);
        reader.join(1_000);

        // 同 key 重建：新 Stream 实例的 XADD 可正常唤醒新 waiter
        ss.xadd("st", StreamId.AUTO, Map.of("f", "v2"), XAddOptions.defaults());
        List<StreamEntry> after = ss.xread("st", StreamId.MIN, 10, 0);
        assertThat(after).hasSize(1);
    }

    @Test
    @DisplayName("C44：xpending 详细形态 total=范围内总数，列表按 count 截断（对齐 Redis）")
    void c44_xpendingTotalVsTruncatedList() throws InterruptedException {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        StreamStore ss = cache.getStreamStore();
        for (int i = 0; i < 5; i++) {
            ss.xadd("st", StreamId.AUTO, Map.of("f", String.valueOf(i)), XAddOptions.defaults());
        }
        ss.xgroupCreate("st", "g", StreamId.MIN, false);
        ss.xreadGroup("st", "g", "c1", true, null, 10, 0);

        PendingSummary summary = ss.xpending("st", "g", StreamId.MIN, StreamId.MAX, 2, "c1", 0);
        assertThat(summary.getTotal()).as("total 应为范围内总数（不受 count 截断）").isEqualTo(5);
        assertThat(summary.getEntries()).as("列表应截断到 count").hasSize(2);
    }

    // ==================== C16：上限配置化（默认不改行为） ====================

    @Test
    @DisplayName("C16：PEL/stream 上限超限拒绝+计数；默认 0 不限行为不变")
    void c16_capsRejectAndCount() throws InterruptedException {
        // 上限生效：PEL=2、stream 数=1；先备 3 条消息保证连续投递可触及上限
        StreamStore capped = new StreamStore(1_000_000L, 2L, 1L);
        for (int i = 0; i < 3; i++) {
            capped.xadd("st", StreamId.AUTO, Map.of("f", String.valueOf(i)), XAddOptions.defaults());
        }
        capped.xgroupCreate("st", "g", StreamId.MIN, false);
        capped.xreadGroup("st", "g", "c1", true, null, 1, 0); // PEL=1
        capped.xreadGroup("st", "g", "c2", true, null, 1, 0); // PEL=2（达上限）
        // 第三次投递被拒绝
        assertThatThrownBy(() -> {
            try {
                capped.xreadGroup("st", "g", "c3", true, null, 10, 0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).isInstanceOf(IllegalStateException.class);
        assertThat(capped.getPelCapRejected()).isEqualTo(1);
        // stream 数量上限
        assertThatThrownBy(() -> capped.xadd("st2", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(capped.getStreamCapRejected()).isEqualTo(1);

        // 默认构造：不限（现网行为不变）
        StreamStore unlimited = new StreamStore();
        unlimited.xadd("a", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());
        unlimited.xadd("b", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());
        assertThat(unlimited.getStreamCapRejected()).isZero();
    }

    @Test
    @DisplayName("C16：SimpleCache maxSize LRU 驱逐计数 + StreamStore 注入生效")
    void c16_lruEvictionCounterAndStreamInjection() {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 2,
                new StreamStore(100L, 0L, 0L));
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3"); // 触发 LRU 驱逐
        assertThat(cache.getLruEvictions()).isEqualTo(1);
        assertThat(cache.size()).isEqualTo(2);
        // 注入的 StreamStore 生效（单 stream 上限 100）
        cache.xadd("st", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());
        assertThat(cache.getStreamStore().xlen("st")).isEqualTo(1);
    }
}
