package com.ww.runtime.data.cache.engine;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 阶段 6 验收（缓存引擎侧，§2.1/§3.4 延伸）：
 * <ul>
 *   <li>C21：容量淘汰过期优先——短 TTL 洪水不再挤掉热点 key；</li>
 *   <li>C29：迁移 Stream 导入以 XADD_BULK 批量记录（回放等价、命令数坍缩）；</li>
 *   <li>C30：清扫批量删除以 REMOVE_MULTI 聚合记录（回放等价）。</li>
 * </ul>
 */
@DisplayName("阶段 6 验收：C21 过期优先淘汰 / C29·C30 批量 AOF 记录")
class Phase6CacheEngineTest {

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

    // ==================== C21 ====================

    @Test
    @DisplayName("C21：容量将满时优先驱逐近侧已过期条目，热点 key 不被短 TTL 洪水挤掉")
    void expiredEntriesEvictedBeforeHotKeys() throws Exception {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 2, new StreamStore());
        cache.put("hot", "stay");
        cache.put("expiring", "gone", 1L); // 1ms TTL
        Thread.sleep(30);
        cache.put("second", "keep"); // 将满触发扫描：过期条目先出，hot 不受伤

        assertThat(cache.get("expiring")).as("过期条目应被优先驱逐").isNull();
        assertThat(cache.get("hot")).isEqualTo("stay");
        assertThat(cache.get("second")).isEqualTo("keep");
        assertThat(cache.getLruEvictions()).as("一次过期优先驱逐 + 一次常规驱逐均计入").isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("C21 兜底：无近侧过期条目时仍按 LRU 语义淘汰最老")
    void plainLruEvictionStillWorks() {
        SimpleCache<String, Object> cache = new SimpleCache<>(null, 2, new StreamStore());
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3"); // 无过期条目可驱逐 → 驱逐最老的 a
        assertThat(cache.get("a")).isNull();
        assertThat(cache.get("b")).isEqualTo("2");
        assertThat(cache.get("c")).isEqualTo("3");
    }

    // ==================== C29 / C30 ====================

    @Test
    @DisplayName("C29+C30：XADD_BULK/REMOVE_MULTI 单条记录 = 多条命令效果，v2 回放等价")
    void bulkRecordsReplayEquivalently() throws Exception {
        AofProperties p = props();
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        cache.setAofService(aof);
        assertThat(aof.isBulkRecordsEnabled()).as("新文件即 v2").isTrue();

        long commandsBefore = aof.getStatus().getTotalCommands();
        // 预置普通 key（先写后删，回放顺序才正确）
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");
        // 100 条 Stream 消息 = 1 条聚合记录（旧实现 100 条）
        List<Long> flat = new ArrayList<>();
        List<Map<String, String>> fields = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            flat.add(1_700_000_000L + i);
            flat.add(0L);
            Map<String, String> f = new LinkedHashMap<>();
            f.put("seq", String.valueOf(i));
            fields.add(f);
        }
        aof.logTyped("XADD_BULK", "st", flat, fields);
        // 批量删除记录
        aof.logTyped("REMOVE_MULTI", "", List.of("k1", "k3"));
        assertThat(aof.getStatus().getTotalCommands())
                .as("3 PUT + 两条聚合记录承载 105 条命令效果").isEqualTo(commandsBefore + 5);

        aof.stop();

        // 回放等价性
        SimpleCache<String, Object> restored = new SimpleCache<>();
        AofService aof2 = new AofService(p, restored);
        aof2.start();
        aof2.replay(restored);
        StreamStore ss = restored.getStreamStore();
        assertThat(ss.xrange("st", StreamId.MIN, StreamId.MAX, 200)).hasSize(100);
        assertThat(restored.get("k1")).isNull();
        assertThat(restored.get("k2")).isEqualTo("v2");
        assertThat(restored.get("k3")).isNull();
        assertThat(aof2.getStatus().getPoisonRecordsSkipped()).as("批量记录回放无毒").isZero();
        aof2.stop();
    }

    @Test
    @DisplayName("C29：v1 存量文件 isBulkRecordsEnabled=false（防批量记录毒化旧回放器）")
    void v1FileDisablesBulk() throws Exception {
        // 构造 v1 文件头（无版本字段），服务启动应识别 v1 并禁用批量
        File aofFile = new File(dir, "appendonly.aof");
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(
                new java.io.FileOutputStream(aofFile))) {
            out.writeInt(AofFormat.AOF_MAGIC);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        AofProperties p = props();
        SimpleCache<String, Object> cache = new SimpleCache<>();
        AofService aof = new AofService(p, cache);
        aof.start();
        assertThat(aof.isBulkRecordsEnabled()).as("v1 文件必须禁用批量记录").isFalse();
        aof.stop();
    }
}
