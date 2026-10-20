package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link StreamStore} P0 基础操作单元测试。
 *
 * <p>覆盖：XADD（含自动/显式 ID、MAXLEN 裁剪、NOMKSTREAM）、XLEN、XRANGE/XREVRANGE、XDEL、XTRIM、XINFO。
 */
@DisplayName("StreamStore P0 基础操作")
class StreamStoreTest {

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

    @Nested
    @DisplayName("XADD 自动 ID 生成")
    class XAddAutoId {

        @Test
        @DisplayName("连续 XADD 应产生严格递增的 ID")
        void shouldGenerateMonotonicIds() {
            StreamId id1 = store.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
            StreamId id2 = store.xadd("s1", StreamId.AUTO, fields("k", "v2"), XAddOptions.defaults());
            StreamId id3 = store.xadd("s1", StreamId.AUTO, fields("k", "v3"), XAddOptions.defaults());

            assertThat(id1).isNotNull();
            assertThat(id2).isNotNull();
            assertThat(id3).isNotNull();
            assertThat(id1.compareTo(id2)).isLessThan(0);
            assertThat(id2.compareTo(id3)).isLessThan(0);
        }

        @Test
        @DisplayName("不同 stream 的 ID 应独立生成")
        void shouldGenerateIndependentIdsPerStream() {
            StreamId s1Id = store.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
            StreamId s2Id = store.xadd("s2", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());

            // 两个 stream 的第一个 ID 时间戳可能相同（同一毫秒），但互不影响
            assertThat(s1Id).isNotNull();
            assertThat(s2Id).isNotNull();
            assertThat(store.xlen("s1")).isEqualTo(1L);
            assertThat(store.xlen("s2")).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("XADD 显式 ID")
    class XAddExplicitId {

        @Test
        @DisplayName("显式 ID 大于当前最大 ID 应成功")
        void shouldAcceptExplicitIdGreaterThanLast() {
            store.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
            store.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());
            assertThat(store.xlen("s1")).isEqualTo(2L);
        }

        @Test
        @DisplayName("显式 ID 小于等于当前最大 ID 应抛错")
        void shouldRejectIdNotGreaterThanLast() {
            store.xadd("s1", new StreamId(2000L, 5L), fields("k", "v1"), XAddOptions.defaults());
            assertThatThrownBy(() ->
                    store.xadd("s1", new StreamId(2000L, 5L), fields("k", "v2"), XAddOptions.defaults()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("不大于当前最大 ID");
        }
    }

    @Nested
    @DisplayName("XADD NOMKSTREAM")
    class XAddNoMkStream {

        @Test
        @DisplayName("NOMKSTREAM 在 stream 不存在时应返回 null")
        void shouldReturnNullWhenStreamNotExists() {
            StreamId id = store.xadd("absent", StreamId.AUTO, fields("k", "v"),
                    XAddOptions.builder().nomkstream(true).build());
            assertThat(id).isNull();
            assertThat(store.xlen("absent")).isEqualTo(0L);
        }

        @Test
        @DisplayName("NOMKSTREAM 在 stream 存在时应正常追加")
        void shouldAppendWhenStreamExists() {
            store.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
            StreamId id = store.xadd("s1", StreamId.AUTO, fields("k", "v2"),
                    XAddOptions.builder().nomkstream(true).build());
            assertThat(id).isNotNull();
            assertThat(store.xlen("s1")).isEqualTo(2L);
        }
    }

    @Nested
    @DisplayName("XADD MAXLEN 裁剪")
    class XAddMaxLen {

        @Test
        @DisplayName("MAXLEN=3 时追加第 4 条应裁剪掉最旧条目")
        void shouldTrimToMaxLen() {
            for (int i = 0; i < 4; i++) {
                store.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i),
                        XAddOptions.builder().maxLen(3).build());
            }
            assertThat(store.xlen("s1")).isEqualTo(3L);
            // 最旧的 v0 应被裁剪
            List<StreamEntry> entries = store.xrange("s1", StreamId.MIN, StreamId.MAX, -1);
            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).getId()).isEqualTo(new StreamId(1001L, 0L));
        }

        @Test
        @DisplayName("默认上限（100w）不应在少量 XADD 时触发裁剪")
        void shouldNotTrimBelowDefaultLimit() {
            for (int i = 0; i < 100; i++) {
                store.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i), XAddOptions.defaults());
            }
            assertThat(store.xlen("s1")).isEqualTo(100L);
        }
    }

    @Nested
    @DisplayName("XRANGE / XREVRANGE")
    class XRange {

        @BeforeEach
        void seed() {
            for (int i = 0; i < 5; i++) {
                store.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i), XAddOptions.defaults());
            }
        }

        @Test
        @DisplayName("XRANGE 全范围应返回所有条目升序")
        void shouldReturnAllAscending() {
            List<StreamEntry> entries = store.xrange("s1", StreamId.MIN, StreamId.MAX, -1);
            assertThat(entries).hasSize(5);
            assertThat(entries.get(0).getId()).isEqualTo(new StreamId(1000L, 0L));
            assertThat(entries.get(4).getId()).isEqualTo(new StreamId(1004L, 0L));
        }

        @Test
        @DisplayName("XRANGE 范围查询应只返回 [min, max] 区间")
        void shouldReturnRangeOnly() {
            List<StreamEntry> entries = store.xrange("s1",
                    new StreamId(1001L, 0L), new StreamId(1003L, 0L), -1);
            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).getId()).isEqualTo(new StreamId(1001L, 0L));
            assertThat(entries.get(2).getId()).isEqualTo(new StreamId(1003L, 0L));
        }

        @Test
        @DisplayName("XRANGE count 应限制返回数量")
        void shouldRespectCountLimit() {
            List<StreamEntry> entries = store.xrange("s1", StreamId.MIN, StreamId.MAX, 2);
            assertThat(entries).hasSize(2);
        }

        @Test
        @DisplayName("XREVRANGE 应返回降序")
        void shouldReturnDescending() {
            List<StreamEntry> entries = store.xrevrange("s1", StreamId.MAX, StreamId.MIN, -1);
            assertThat(entries).hasSize(5);
            assertThat(entries.get(0).getId()).isEqualTo(new StreamId(1004L, 0L));
            assertThat(entries.get(4).getId()).isEqualTo(new StreamId(1000L, 0L));
        }

        @Test
        @DisplayName("不存在的 stream 应返回空列表")
        void shouldReturnEmptyForAbsentStream() {
            assertThat(store.xrange("absent", StreamId.MIN, StreamId.MAX, -1)).isEmpty();
        }
    }

    @Nested
    @DisplayName("XDEL")
    class XDel {

        @Test
        @DisplayName("XDEL 应移除指定 ID 并返回删除数")
        void shouldDeleteById() {
            StreamId id1 = store.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
            StreamId id2 = store.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());

            long removed = store.xdel("s1", id1);
            assertThat(removed).isEqualTo(1L);
            assertThat(store.xlen("s1")).isEqualTo(1L);
            assertThat(store.xrange("s1", StreamId.MIN, StreamId.MAX, -1))
                    .extracting(e -> e.getId())
                    .containsExactly(id2);
        }

        @Test
        @DisplayName("XDEL 不存在的 ID 应返回 0")
        void shouldReturnZeroForAbsentId() {
            store.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
            long removed = store.xdel("s1", new StreamId(9999L, 0L));
            assertThat(removed).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("XTRIM")
    class XTrim {

        @Test
        @DisplayName("XTRIM 应保留最近 maxLen 条")
        void shouldTrimToLatest() {
            for (int i = 0; i < 10; i++) {
                store.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i), XAddOptions.defaults());
            }
            long removed = store.xtrim("s1", 3, false);
            assertThat(removed).isEqualTo(7L);
            assertThat(store.xlen("s1")).isEqualTo(3L);
            List<StreamEntry> entries = store.xrange("s1", StreamId.MIN, StreamId.MAX, -1);
            assertThat(entries.get(0).getId()).isEqualTo(new StreamId(1007L, 0L));
        }

        @Test
        @DisplayName("XTRIM 当前数 &lt;= maxLen 应不删除")
        void shouldNotTrimWhenBelowMaxLen() {
            store.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
            long removed = store.xtrim("s1", 10, false);
            assertThat(removed).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("XINFO")
    class XInfo {

        @Test
        @DisplayName("XINFO 应返回 length / firstId / lastId / groups")
        void shouldReturnStreamInfo() {
            StreamId id1 = store.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
            StreamId id2 = store.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());

            StreamInfo info = store.xinfo("s1");
            assertThat(info).isNotNull();
            assertThat(info.getLength()).isEqualTo(2L);
            assertThat(info.getFirstId()).isEqualTo(id1);
            assertThat(info.getLastId()).isEqualTo(id2);
            assertThat(info.getGroupsCount()).isZero();
        }

        @Test
        @DisplayName("XINFO 不存在的 stream 应返回 null")
        void shouldReturnNullForAbsentStream() {
            assertThat(store.xinfo("absent")).isNull();
        }
    }

    @Nested
    @DisplayName("ID 解析")
    class StreamIdParse {

        @Test
        @DisplayName("StreamId.parse 应支持 - / + / * / ms-seq / ms")
        void shouldParseVariousFormats() {
            assertThat(StreamId.parse("-")).isEqualTo(StreamId.MIN);
            assertThat(StreamId.parse("+")).isEqualTo(StreamId.MAX);
            assertThat(StreamId.parse("*")).isEqualTo(StreamId.AUTO);
            assertThat(StreamId.parse("1700000000000-5")).isEqualTo(new StreamId(1700000000000L, 5L));
            assertThat(StreamId.parse("1700000000000")).isEqualTo(new StreamId(1700000000000L, 0L));
        }
    }
}
