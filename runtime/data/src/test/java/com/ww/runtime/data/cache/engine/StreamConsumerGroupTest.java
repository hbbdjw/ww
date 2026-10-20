package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link StreamStore} P1 消费者组 + 阻塞 XREAD 测试。
 *
 * <p>覆盖：XGROUP CREATE/DESTROY/CREATECONSUMER/DELCONSUMER、XREADGROUP（&gt; 与 PEL 模式）、
 * XACK、XPENDING（概要/详细）、XCLAIM、XAUTOCLAIM、XINFO GROUPS/CONSUMERS、阻塞 XREAD（含异步唤醒）。
 */
@DisplayName("StreamStore P1 消费者组与阻塞读")
class StreamConsumerGroupTest {

    private StreamStore store;

    @BeforeEach
    void setUp() {
        store = new StreamStore();
    }

    private Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    private void seed(String key, int count) {
        for (int i = 0; i < count; i++) {
            store.xadd(key, new StreamId(1000L + i, 0L), fields("k" + i, "v" + i), XAddOptions.defaults());
        }
    }

    @Nested
    @DisplayName("XGROUP 生命周期")
    class XGroupLifecycle {

        @Test
        @DisplayName("XGROUP CREATE 在 stream 不存在时若 MKSTREAM 应自动创建")
        void shouldCreateWithMkstream() {
            boolean created = store.xgroupCreate("absent", "g1", StreamId.MIN, true);
            assertThat(created).isTrue();
        }

        @Test
        @DisplayName("XGROUP CREATE 在 stream 不存在且无 MKSTREAM 应抛错")
        void shouldFailWithoutMkstream() {
            assertThatThrownBy(() -> store.xgroupCreate("absent", "g1", StreamId.MIN, false))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("不存在");
        }

        @Test
        @DisplayName("XGROUP CREATE 同名 group 应返回 false")
        void shouldRejectDuplicateGroup() {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            boolean second = store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            assertThat(second).isFalse();
        }

        @Test
        @DisplayName("XGROUP DESTROY 应返回 true，再删返回 false")
        void shouldDestroyGroup() {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            assertThat(store.xgroupDestroy("s1", "g1")).isTrue();
            assertThat(store.xgroupDestroy("s1", "g1")).isFalse();
        }

        @Test
        @DisplayName("XGROUP CREATECONSUMER/DELCONSUMER 应增删消费者")
        void shouldCreateAndDeleteConsumer() {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            assertThat(store.xgroupCreateConsumer("s1", "g1", "c1")).isEqualTo(1);
            assertThat(store.xgroupCreateConsumer("s1", "g1", "c1")).isEqualTo(0); // 已存在
            // 未投递消息的 consumer 删除应返回 0
            assertThat(store.xgroupDelConsumer("s1", "g1", "c1")).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("XREADGROUP '>' 新消息模式")
    class XReadGroupNew {

        @Test
        @DisplayName("'>' 应投递 lastDeliveredId 之后的新消息并写入 PEL")
        void shouldDeliverNewMessagesAndWritePel() throws InterruptedException {
            seed("s1", 3);  // 生成 1000, 1001, 1002
            store.xgroupCreate("s1", "g1", new StreamId(1001L, 0L), false);

            // lastDeliveredId=1001，'>' 应投递 ID > 1001 的消息，即仅 1002
            List<StreamEntry> delivered = store.xreadGroup("s1", "g1", "c1",
                    true, null, 10, 0);
            assertThat(delivered).hasSize(1);
            assertThat(delivered.get(0).getId()).isEqualTo(new StreamId(1002L, 0L));

            // PEL 应有 1 条
            StreamGroupsInfo groupsInfo = store.xinfoGroups("s1");
            assertThat(groupsInfo.getGroups()).hasSize(1);
            StreamGroupsInfo.GroupSummary g = groupsInfo.getGroups().get(0);
            assertThat(g.getPending()).isEqualTo(1);
            assertThat(g.getConsumers()).isEqualTo(1);
        }

        @Test
        @DisplayName("不同消费者共享 group 的 lastDeliveredId 推进")
        void shouldShareLastDeliveredIdAmongConsumers() throws InterruptedException {
            seed("s1", 4);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);

            // c1 取走 1000、1001
            List<StreamEntry> c1 = store.xreadGroup("s1", "g1", "c1", true, null, 2, 0);
            assertThat(c1).hasSize(2);
            // c2 取走后续 1002、1003（lastDeliveredId 已被 c1 推进到 1001）
            List<StreamEntry> c2 = store.xreadGroup("s1", "g1", "c2", true, null, 10, 0);
            assertThat(c2).hasSize(2);
            assertThat(c2.get(0).getId()).isEqualTo(new StreamId(1002L, 0L));
        }
    }

    @Nested
    @DisplayName("XREADGROUP PEL 模式")
    class XReadGroupPel {

        @Test
        @DisplayName("PEL 模式（readNew=false）应返回 consumer 的 pending 消息")
        void shouldReadPendingForConsumer() throws InterruptedException {
            seed("s1", 2);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 10, 0); // 投递给 c1

            // PEL 模式从 0 开始读 c1 的 pending
            List<StreamEntry> pending = store.xreadGroup("s1", "g1", "c1",
                    false, StreamId.MIN, 10, 0);
            assertThat(pending).hasSize(2);
        }

        @Test
        @DisplayName("PEL 模式只返回指定 consumer 的 pending，不返回其他 consumer 的")
        void shouldOnlyReturnOwnPending() throws InterruptedException {
            seed("s1", 2);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 1, 0); // c1 取 1000
            store.xreadGroup("s1", "g1", "c2", true, null, 1, 0); // c2 取 1001

            List<StreamEntry> c1Pending = store.xreadGroup("s1", "g1", "c1",
                    false, StreamId.MIN, 10, 0);
            assertThat(c1Pending).hasSize(1);
            assertThat(c1Pending.get(0).getId()).isEqualTo(new StreamId(1000L, 0L));
        }
    }

    @Nested
    @DisplayName("XACK")
    class XAck {

        @Test
        @DisplayName("XACK 应从 PEL 与 consumer.pendingIds 移除")
        void shouldAckAndRemoveFromPel() throws InterruptedException {
            seed("s1", 2);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            List<StreamEntry> delivered = store.xreadGroup("s1", "g1", "c1", true, null, 10, 0);

            long acked = store.xack("s1", "g1", delivered.get(0).getId());
            assertThat(acked).isEqualTo(1L);

            // PEL 应剩 1
            StreamGroupsInfo.GroupSummary g = store.xinfoGroups("s1").getGroups().get(0);
            assertThat(g.getPending()).isEqualTo(1);
        }

        @Test
        @DisplayName("XACK 不存在的 ID 应返回 0")
        void shouldReturnZeroForAbsentId() throws InterruptedException {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            long acked = store.xack("s1", "g1", new StreamId(9999L, 0L));
            assertThat(acked).isZero();
        }
    }

    @Nested
    @DisplayName("XPENDING")
    class XPending {

        @Test
        @DisplayName("概要形态应返回 total / firstId / lastId / consumerCounts")
        void shouldReturnSummaryForm() throws InterruptedException {
            seed("s1", 3);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 2, 0);
            store.xreadGroup("s1", "g1", "c2", true, null, 1, 0);

            PendingSummary summary = store.xpending("s1", "g1",
                    null, null, -1, null, 0);
            assertThat(summary).isNotNull();
            assertThat(summary.getTotal()).isEqualTo(3L);
            assertThat(summary.getFirstId()).isEqualTo(new StreamId(1000L, 0L));
            assertThat(summary.getLastId()).isEqualTo(new StreamId(1002L, 0L));
            assertThat(summary.getConsumerCounts())
                    .containsEntry("c1", 2L)
                    .containsEntry("c2", 1L);
        }

        @Test
        @DisplayName("详细形态应返回指定 consumer 的 PelEntry 列表")
        void shouldReturnDetailedForm() throws InterruptedException {
            seed("s1", 2);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 2, 0);

            PendingSummary detail = store.xpending("s1", "g1",
                    StreamId.MIN, StreamId.MAX, 10, "c1", 0);
            assertThat(detail.isDetailed()).isTrue();
            assertThat(detail.getEntries()).hasSize(2);
            assertThat(detail.getEntries().get(0).getConsumer()).isEqualTo("c1");
            assertThat(detail.getEntries().get(0).getDeliveryCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("XCLAIM / XAUTOCLAIM")
    class XClaim {

        @Test
        @DisplayName("XCLAIM 应在 minIdleTime 满足时转移消息给新 consumer")
        void shouldTransferPendingOnClaim() throws Exception {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 10, 0);

            // 等待一小段让 idle 时间超过 minIdle=10ms
            Thread.sleep(20);

            List<PelEntry> claimed = store.xclaim("s1", "g1", "c2", 10,
                    new StreamId(1000L, 0L));
            assertThat(claimed).hasSize(1);
            assertThat(claimed.get(0).getConsumer()).isEqualTo("c2");
            assertThat(claimed.get(0).getDeliveryCount()).isEqualTo(2); // 转移后 +1

            // 原 c1 的 pending 应清空
            StreamConsumersInfo consumers = store.xinfoConsumers("s1", "g1");
            StreamConsumersInfo.ConsumerSummary c1Summary = consumers.getConsumers().stream()
                    .filter(c -> c.getName().equals("c1")).findFirst().orElseThrow();
            assertThat(c1Summary.getPending()).isZero();
        }

        @Test
        @DisplayName("XCLAIM 在 minIdleTime 不满足时应不转移")
        void shouldNotTransferWhenIdleTooShort() throws InterruptedException {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 10, 0);

            // minIdle=10000ms，刚投递的消息空闲时间不足
            List<PelEntry> claimed = store.xclaim("s1", "g1", "c2", 10_000,
                    new StreamId(1000L, 0L));
            assertThat(claimed).isEmpty();
        }

        @Test
        @DisplayName("XAUTOCLAIM 应分页扫描并认领超时消息")
        void shouldAutoClaim() throws Exception {
            seed("s1", 3);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 3, 0);

            Thread.sleep(20);

            XAutoClaimResult result = store.xautoclaim("s1", "g1", "c2", 10,
                    StreamId.MIN, 10);
            assertThat(result.getClaimed()).hasSize(3);
            assertThat(result.hasMore()).isFalse();
            assertThat(result.getNextCursor()).isEqualTo(StreamId.MAX);
        }
    }

    @Nested
    @DisplayName("XINFO GROUPS / CONSUMERS")
    class XInfoGroupsConsumers {

        @Test
        @DisplayName("XINFO GROUPS 应返回所有组摘要")
        void shouldReturnGroupsSummary() throws InterruptedException {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xgroupCreate("s1", "g2", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 1, 0);

            StreamGroupsInfo info = store.xinfoGroups("s1");
            assertThat(info.getGroups()).hasSize(2);
            StreamGroupsInfo.GroupSummary g1 = info.getGroups().stream()
                    .filter(g -> g.getName().equals("g1")).findFirst().orElseThrow();
            assertThat(g1.getPending()).isEqualTo(1);
            assertThat(g1.getConsumers()).isEqualTo(1);
        }

        @Test
        @DisplayName("XINFO CONSUMERS 应返回组内消费者与 idle 时间")
        void shouldReturnConsumersSummary() throws InterruptedException {
            seed("s1", 1);
            store.xgroupCreate("s1", "g1", StreamId.MIN, false);
            store.xreadGroup("s1", "g1", "c1", true, null, 1, 0);

            StreamConsumersInfo info = store.xinfoConsumers("s1", "g1");
            assertThat(info.getConsumers()).hasSize(1);
            StreamConsumersInfo.ConsumerSummary c = info.getConsumers().get(0);
            assertThat(c.getName()).isEqualTo("c1");
            assertThat(c.getPending()).isEqualTo(1);
            assertThat(c.getIdleMs()).isGreaterThanOrEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("阻塞 XREAD")
    class BlockingXRead {

        @Test
        @DisplayName("非阻塞 XREAD 在无数据时应立即返回空")
        void shouldReturnEmptyImmediatelyWhenNonBlocking() throws InterruptedException {
            List<StreamEntry> result = store.xread("s1", StreamId.MIN, 10, 0);
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("阻塞 XREAD 在数据已存在时应立即返回（不阻塞）")
        void shouldReturnImmediatelyWhenDataExists() throws InterruptedException {
            seed("s1", 2);
            List<StreamEntry> result = store.xread("s1", StreamId.MIN, 10, 5000);
            assertThat(result).hasSize(2);
        }

        @Test
        @DisplayName("阻塞 XREAD 在数据到达后应被异步唤醒")
        void shouldBeWokenUpWhenDataArrives() throws Exception {
            // 阻塞 XREAD from = 一个较小但当前 stream 无数据的 ID
            // （stream s2 不存在 → 第一次非阻塞读为空 → 注册 waiter → 进入阻塞）
            AtomicReference<List<StreamEntry>> wokenRef = new AtomicReference<>();
            CompletableFuture<Void> readFuture = CompletableFuture.runAsync(() -> {
                try {
                    // from = 1-0，确保第一次非阻塞读为空（stream 不存在）
                    List<StreamEntry> r = store.xread("s2", new StreamId(1L, 0), 10, 3000);
                    wokenRef.set(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // 等待 waiter 注册（双检后进入阻塞）
            Thread.sleep(200);
            // XADD 一条新消息（自动 ID 必然大于 1-0），应唤醒 waiter
            store.xadd("s2", StreamId.AUTO, fields("k", "wake"), XAddOptions.defaults());

            // 等待唤醒完成
            readFuture.get(2, TimeUnit.SECONDS);
            assertThat(wokenRef.get()).isNotNull();
            assertThat(wokenRef.get()).hasSize(1);
            assertThat(wokenRef.get().get(0).getFields().get("k")).isEqualTo("wake");
        }

        @Test
        @DisplayName("阻塞 XREAD 在超时后应返回空列表")
        void shouldReturnEmptyOnTimeout() throws InterruptedException {
            long start = System.currentTimeMillis();
            List<StreamEntry> result = store.xread("absent", StreamId.MIN, 10, 200);
            long elapsed = System.currentTimeMillis() - start;
            assertThat(result).isEmpty();
            // 至少阻塞了 200ms（允许调度误差，>150ms 即可）
            assertThat(elapsed).isGreaterThanOrEqualTo(150L);
        }
    }
}
