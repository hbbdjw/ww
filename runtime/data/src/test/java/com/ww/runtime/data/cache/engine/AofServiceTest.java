package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AofService} 写入-重启-回放往返基线测试。
 *
 * <p>锚定当前即正确的持久化语义（KV/容器/流+消费者组），
 * 作为后续 C1/C2/C5 等修复的行为基准；修复类用例随对应提交加入。
 */
@DisplayName("AofService 写入-回放往返")
class AofServiceTest {

    @TempDir
    File dir;

    private AofProperties props() {
        AofProperties p = new AofProperties();
        p.setEnabled(true);
        p.setDir(dir.getPath());
        p.setAppendfsync("no");
        return p;
    }

    /** 写阶段：建缓存、挂 AOF、执行写入、停止（落盘）。 */
    private SimpleCache<String, Object> startWriting(AofService[] holder) {
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(props(), cache);
        try {
            aof.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cache.setAofService(aof);
        holder[0] = aof;
        return cache;
    }

    private void stopAof(AofService aof) {
        aof.stop();
    }

    /** 回放阶段：新开缓存从同一目录回放（回放期间不挂 AOF，与 CacheCapability 启动顺序一致）。 */
    private SimpleCache<String, Object> replayFromDisk(AofService[] holder) {
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof = new AofService(props(), restored);
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
            Thread.sleep(20);
        }
        assertThat(aof.getStatus().isRewriting()).as("rewrite should finish in time").isFalse();
    }

    @Test
    @DisplayName("KV 与 TTL：重启回放后值与存活状态保持")
    void kvRoundTrip() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.put("k1", "v1");
        cache.put("k2", 42);
        cache.put("k3", "v3", 60_000);
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        assertThat(restored.get("k1")).isEqualTo("v1");
        assertThat(restored.get("k2")).isEqualTo(42);
        assertThat(restored.get("k3")).isEqualTo("v3");
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("Hash：字段与值类型完整往返")
    void hashRoundTrip() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.hset("h", "f1", "v1");
        cache.hset("h", "f2", 7L);
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("f1", "v1");
        expected.put("f2", 7L);
        assertThat(restored.hgetAll("h")).isEqualTo(expected);
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("List：LPUSH/RPUSH 顺序完整往返")
    void listRoundTrip() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.rpush("l", "a", "b");
        cache.lpush("l", "z");
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        List<Map.Entry<String, ConcurrentLinkedDeque<Object>>> entries = restored.getAllListEntries();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getKey()).isEqualTo("l");
        assertThat(entries.get(0).getValue().toArray()).containsExactly("z", "a", "b");
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("Set：成员完整往返")
    void setRoundTrip() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.sadd("s", "m1", "m2");
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        assertThat(restored.smembers("s")).containsExactlyInAnyOrder("m1", "m2");
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("ZSet 增量记录（不经重写）：score 与成员完整往返")
    void zsetIncrementalRoundTrip() {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.zadd("z", 1.5d, "a");
        cache.zadd("z", 2.5d, "b");
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        List<Map.Entry<String, ConcurrentSkipListMap<Double, Set<Object>>>> entries =
                restored.getAllZSetEntries();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getKey()).isEqualTo("z");
        assertThat(entries.get(0).getValue()).containsKeys(1.5d, 2.5d);
        assertThat(entries.get(0).getValue().get(1.5d)).containsExactly("a");
        assertThat(entries.get(0).getValue().get(2.5d)).containsExactly("b");
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("Stream+消费者组+PEL：重启后消息、lastDeliveredId 与 PEL 完整恢复")
    void streamGroupPelRoundTrip() throws Exception {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        StreamId id1 = cache.xadd("st", StreamId.AUTO, Map.of("f", "v1"), XAddOptions.defaults());
        StreamId id2 = cache.xadd("st", StreamId.AUTO, Map.of("f", "v2"), XAddOptions.defaults());
        cache.xgroupCreate("st", "g1", StreamId.MIN, false);
        List<StreamEntry> delivered = cache.xreadGroup("st", "g1", "c1", true, null, 10, 0);
        assertThat(delivered).hasSize(2);
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        StreamStore ss = restored.getStreamStore();

        List<StreamEntry> entries = ss.xrange("st", StreamId.MIN, StreamId.MAX, 100);
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getId()).isEqualTo(id1);
        assertThat(entries.get(0).getFields()).containsEntry("f", "v1");
        assertThat(entries.get(1).getId()).isEqualTo(id2);

        ConsumerGroup group = ss.getStreamGroups("st").get("g1");
        assertThat(group).isNotNull();
        assertThat(group.getLastDeliveredId()).isEqualTo(id2);
        assertThat(group.getConsumers()).containsKey("c1");
        assertThat(group.getConsumers().get("c1").pendingIdsView()).containsExactlyInAnyOrder(id1, id2);
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("C5：中段 CRC 失配停止回放并截断，截断后新写入可回放")
    void crcMismatchTruncatesAndAllowsFutureWrites() throws Exception {
        // 写入三条完好记录
        AofProperties p = props();
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        cache.setAofService(aof);
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");
        aof.stop();

        // 翻转第二条记录值区一字节（CRC 失配但可解码，确保走校验分支而非解析分支）
        File aofFile = new File(dir, "appendonly.aof");
        byte[] rec1 = AofFormat.encodeCommand(AofFormat.OP_PUT, "k1", "v1");
        byte[] rec2 = AofFormat.encodeCommand(AofFormat.OP_PUT, "k2", "v2");
        int corruptAt = 4 + rec1.length + (rec2.length - 5);
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(aofFile, "rw")) {
            raf.seek(corruptAt);
            int b = raf.read();
            raf.seek(corruptAt);
            raf.write(b ^ 0xFF);
        }

        // 全量回放：停止于损坏点前 + 物理截断到最后完好边界
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(p, restored);
        aof2.start();
        aof2.replay(restored);
        assertThat(restored.get("k1")).isEqualTo("v1");
        assertThat(restored.get("k2")).as("CRC 失配记录不得应用").isNull();
        assertThat(restored.get("k3")).as("损坏点之后的记录不得应用").isNull();
        assertThat(aof2.getStatus().getCrcFailures()).isEqualTo(1);
        assertThat(aofFile.length()).as("文件应截断至第一条记录边界").isEqualTo(4 + rec1.length);

        // 截断后续写应落在新边界，重启可回放（不截断则新记录永不可达）
        restored.setAofService(aof2);
        restored.put("k4", "v4");
        aof2.stop();

        SimpleCache<String, Object> finalCache = new SimpleCache<>();
        AofService aof3 = new AofService(p, finalCache);
        aof3.start();
        aof3.replay(finalCache);
        assertThat(finalCache.get("k1")).isEqualTo("v1");
        assertThat(finalCache.get("k4")).isEqualTo("v4");
        assertThat(finalCache.get("k2")).isNull();
        assertThat(finalCache.get("k3")).isNull();
        aof3.stop();
    }

    @Test
    @DisplayName("C5：尾部残记录（崩溃写一半）截断至最后完好边界")
    void truncatedTailRecordHandled() throws Exception {
        AofProperties p = props();
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        cache.setAofService(aof);
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        aof.stop();

        // 截短文件至第二条记录中间，模拟崩溃写一半
        File aofFile = new File(dir, "appendonly.aof");
        byte[] rec1 = AofFormat.encodeCommand(AofFormat.OP_PUT, "k1", "v1");
        byte[] rec2 = AofFormat.encodeCommand(AofFormat.OP_PUT, "k2", "v2");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(aofFile, "rw")) {
            raf.setLength(4 + rec1.length + rec2.length / 2);
        }

        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(p, restored);
        aof2.start();
        aof2.replay(restored);
        assertThat(restored.get("k1")).isEqualTo("v1");
        assertThat(restored.get("k2")).isNull();
        assertThat(aof2.getStatus().getCrcFailures()).isEqualTo(1);
        assertThat(aofFile.length()).isEqualTo(4 + rec1.length);
        aof2.stop();
    }

    @Test
    @DisplayName("C5：谓词回放路径遇损坏仅停止，不得改动文件")
    void predicateReplayDoesNotTruncate() throws Exception {
        AofProperties p = props();
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        cache.setAofService(aof);
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        aof.stop();

        File aofFile = new File(dir, "appendonly.aof");
        long lenBefore = aofFile.length();
        byte[] rec1 = AofFormat.encodeCommand(AofFormat.OP_PUT, "k1", "v1");
        int corruptAt = 4 + rec1.length + 3; // 破坏第二条记录的头部
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(aofFile, "rw")) {
            raf.seek(corruptAt);
            int b = raf.read();
            raf.seek(corruptAt);
            raf.write(b ^ 0xFF);
        }

        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(p, restored);
        aof2.start();
        aof2.replay(restored, k -> true);
        assertThat(restored.get("k1")).isEqualTo("v1");
        assertThat(aof2.getStatus().getCrcFailures()).isEqualTo(1);
        assertThat(aofFile.length()).as("谓词回放不得物理截断文件").isEqualTo(lenBefore);
        aof2.stop();
    }

    @Test
    @DisplayName("C4：always 策略每写即落盘，重启回放等价")
    void alwaysFsyncRoundTrip() throws Exception {
        AofProperties p = props();
        p.setAppendfsync("always");

        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        cache.setAofService(aof);
        cache.put("a1", "v1");
        cache.put("a2", 42);

        // 不经 stop 直接读盘：always 策略每写 flush+sync，记录应已在磁盘文件中
        byte[] raw = java.nio.file.Files.readAllBytes(new File(dir, "appendonly.aof").toPath());
        assertThat(raw.length).as("always 策略写入后字节应已落盘").isGreaterThan(4);
        aof.stop();

        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(p, restored);
        aof2.start();
        aof2.replay(restored);
        assertThat(restored.get("a1")).isEqualTo("v1");
        assertThat(restored.get("a2")).isEqualTo(42);
        aof2.stop();
    }

    @Test
    @DisplayName("C2：事务内混合类型参数重启回放后类型与值逐一相等")
    void transactionMixedTypesRoundTrip() throws Exception {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.multi();
        cache.put("t1", 42);
        cache.put("t2", 99L);
        cache.put("t3", 3.14d);
        cache.put("t4", true);
        cache.put("t5", "ttl-val", 60_000);
        cache.hset("th", "f1", 7L);
        cache.zadd("tz", 2.5d, "m");
        cache.rpush("tl", "a", "b");
        cache.exec();
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        assertThat(restored.get("t1")).isEqualTo(42);
        assertThat(restored.get("t2")).isEqualTo(99L);
        assertThat(restored.get("t3")).isEqualTo(3.14d);
        assertThat(restored.get("t4")).isEqualTo(true);
        assertThat(restored.get("t5")).as("PUT_EXPIRED 的 TTL 回放后不得归零").isEqualTo("ttl-val");
        assertThat(restored.hgetAll("th")).containsEntry("f1", 7L);
        List<Map.Entry<String, ConcurrentSkipListMap<Double, Set<Object>>>> zentries =
                restored.getAllZSetEntries();
        assertThat(zentries).hasSize(1);
        assertThat(zentries.get(0).getValue().get(2.5d)).containsExactly("m");
        List<Map.Entry<String, ConcurrentLinkedDeque<Object>>> lentries = restored.getAllListEntries();
        assertThat(lentries).hasSize(1);
        assertThat(lentries.get(0).getValue().toArray()).containsExactly("a", "b");
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("C1：ZSet 经重写后回放完整恢复，不再被误当 Hash 写出毒记录")
    void zsetRewriteRoundTrip() throws Exception {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.zadd("z", 1.5d, "a");
        cache.zadd("z", 2.5d, "b");

        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        List<Map.Entry<String, ConcurrentSkipListMap<Double, Set<Object>>>> entries =
                restored.getAllZSetEntries();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getValue()).containsKeys(1.5d, 2.5d);
        assertThat(entries.get(0).getValue().get(1.5d)).containsExactly("a");
        assertThat(entries.get(0).getValue().get(2.5d)).containsExactly("b");
        // 修复前：getAllHashEntries 会把 ZSet 当 Hash 重写为 OP_HSET（field 为 Double），
        // 回放触发 ClassCastException 中断；修复后无毒记录
        assertThat(holder[0].getStatus().getPoisonRecordsSkipped()).isZero();
        stopAof(holder[0]);
    }

    @Test
    @DisplayName("C1：存量毒记录跳过+计数，后续完好记录正常应用")
    void poisonRecordSkippedAndCounted() throws Exception {
        // 手工构造存量带毒文件：毒记录模拟旧版把 ZSet score 桶误写为 OP_HSET
        // （field 为 Double → 回放 hset 的 (String) 强转抛 ClassCastException）
        File aofFile = new File(dir, "appendonly.aof");
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(
                new java.io.FileOutputStream(aofFile))) {
            out.writeInt(AofFormat.AOF_MAGIC);
            out.write(AofFormat.encodeCommand(AofFormat.OP_HSET, "zkey", 1.5d, Set.of("a")));
            out.write(AofFormat.encodeCommand(AofFormat.OP_PUT, "ok", "v"));
        }

        AofProperties p = props();
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof = new AofService(p, restored);
        aof.start();
        aof.replay(restored);

        assertThat(restored.get("ok")).as("毒记录之后的完好记录仍应应用").isEqualTo("v");
        assertThat(restored.get("zkey")).isNull();
        assertThat(aof.getStatus().getPoisonRecordsSkipped()).isEqualTo(1);
        aof.stop();
    }

    @Test
    @DisplayName("重写后增量写入：快照 + 增量记录共同保证回放一致（普通类型）")
    void rewriteThenIncrementalRoundTrip() throws Exception {
        AofService[] holder = new AofService[1];
        SimpleCache<String, Object> cache = startWriting(holder);
        cache.put("kv1", "before-rewrite");
        cache.hset("h", "f", 1L);
        cache.rpush("l", "x");
        cache.sadd("s", "m");
        cache.xadd("st", StreamId.AUTO, Map.of("f", "v"), XAddOptions.defaults());

        holder[0].bgRewriteAof();
        awaitRewriteFinish(holder[0]);

        cache.put("kv2", "after-rewrite");
        stopAof(holder[0]);

        SimpleCache<String, Object> restored = replayFromDisk(holder);
        assertThat(restored.get("kv1")).isEqualTo("before-rewrite");
        assertThat(restored.get("kv2")).isEqualTo("after-rewrite");
        assertThat(restored.hgetAll("h")).containsEntry("f", 1L);
        assertThat(restored.getAllListEntries()).hasSize(1);
        assertThat(restored.smembers("s")).containsExactly("m");
        assertThat(restored.getStreamStore().xrange("st", StreamId.MIN, StreamId.MAX, 100)).hasSize(1);
        stopAof(holder[0]);
    }
}
