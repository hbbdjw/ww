package com.ww.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CacheServer + CacheClient 端到端 TCP 测试（单机模式）。
 *
 * <p>启动一个真实的 CacheServer 监听随机端口，通过 CacheClient 发送 stream 命令验证
 * 协议编解码、服务端分发、AOF 记录的完整链路。
 *
 * <p>分布式路由测试见 {@link StreamDistributedRouteTest}。
 */
@DisplayName("Stream TCP 端到端")
class StreamTcpEndToEndTest {

    private CacheServer server;
    private SimpleCache<String, Object> localCache;
    private int port;
    private CacheClient client;

    @BeforeEach
    void setUp() throws Exception {
        localCache = new SimpleCache<>(null);
        // 选择随机可用端口
        try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        server = new CacheServer(port, localCache);
        server.start();
        // 等待服务启动
        Thread.sleep(200);
        client = new CacheClient("127.0.0.1", port);
    }

    @AfterEach
    void tearDown() {
        try { if (client != null) client.close(); } catch (Exception ignored) {}
        if (server != null) server.stop();
    }

    private Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    @Test
    @DisplayName("XADD → XLEN → XRANGE 端到端应正确返回")
    void shouldXAddXLenXRangeOverTcp() throws Exception {
        StreamId id1 = client.xadd("s1", new StreamId(1000L, 0L), fields("k1", "v1"), XAddOptions.defaults());
        StreamId id2 = client.xadd("s1", new StreamId(2000L, 0L), fields("k2", "v2"), XAddOptions.defaults());

        assertThat(id1).isEqualTo(new StreamId(1000L, 0L));
        assertThat(id2).isEqualTo(new StreamId(2000L, 0L));

        long len = client.xlen("s1");
        assertThat(len).isEqualTo(2L);

        List<StreamEntry> entries = client.xrange("s1", StreamId.MIN, StreamId.MAX, -1);
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getFields()).containsEntry("k1", "v1");
        assertThat(entries.get(1).getFields()).containsEntry("k2", "v2");
    }

    @Test
    @DisplayName("XADD 自动 ID 端到端应返回服务端生成的 ID")
    void shouldGenerateIdServerSideOverTcp() throws Exception {
        StreamId id1 = client.xadd("s1", StreamId.AUTO, fields("k", "v1"), XAddOptions.defaults());
        StreamId id2 = client.xadd("s1", StreamId.AUTO, fields("k", "v2"), XAddOptions.defaults());

        assertThat(id1).isNotNull();
        assertThat(id2).isNotNull();
        assertThat(id1.compareTo(id2)).isLessThan(0);
        assertThat(client.xlen("s1")).isEqualTo(2L);
    }

    @Test
    @DisplayName("XADD MAXLEN 端到端应触发裁剪")
    void shouldTrimOverTcp() throws Exception {
        for (int i = 0; i < 5; i++) {
            client.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i),
                    XAddOptions.builder().maxLen(3).build());
        }
        assertThat(client.xlen("s1")).isEqualTo(3L);
    }

    @Test
    @DisplayName("XDEL 端到端应删除指定 ID")
    void shouldXDelOverTcp() throws Exception {
        StreamId id1 = client.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
        client.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());

        long removed = client.xdel("s1", id1);
        assertThat(removed).isEqualTo(1L);
        assertThat(client.xlen("s1")).isEqualTo(1L);
    }

    @Test
    @DisplayName("XTRIM 端到端应裁剪")
    void shouldXTrimOverTcp() throws Exception {
        for (int i = 0; i < 10; i++) {
            client.xadd("s1", new StreamId(1000L + i, 0L), fields("k", "v" + i), XAddOptions.defaults());
        }
        long removed = client.xtrim("s1", 3, false);
        assertThat(removed).isEqualTo(7L);
        assertThat(client.xlen("s1")).isEqualTo(3L);
    }

    @Test
    @DisplayName("XGROUP CREATE + XREADGROUP + XACK 端到端")
    void shouldXGroupCreateReadGroupAckOverTcp() throws Exception {
        client.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
        client.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());

        boolean created = client.xgroupCreate("s1", "g1", StreamId.MIN, false);
        assertThat(created).isTrue();

        // 通过协议直接调用 XREADGROUP（client 便捷方法未提供 XREADGROUP）
        byte[] resp = client.send(CacheProtocol.OP_XREADGROUP,
                CacheProtocol.encodeXReadGroupArgs("s1", "g1", "c1", true, null, 10, 0));
        Object v = Codec.decodeValue(resp);
        assertThat(v).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<StreamEntry> delivered = (List<StreamEntry>) (List<?>) v;
        assertThat(delivered).hasSize(2);

        // XACK 第一条
        long acked = client.xack("s1", "g1", new StreamId(1000L, 0L));
        assertThat(acked).isEqualTo(1L);
    }

    @Test
    @DisplayName("非阻塞 XREAD 端到端应返回已有数据")
    void shouldXReadNonBlockingOverTcp() throws Exception {
        client.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());

        List<StreamEntry> result = client.xread("s1", StreamId.MIN, 10, 0);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getFields()).containsEntry("k", "v1");
    }

    @Test
    @DisplayName("XREVRANGE 端到端应返回降序")
    void shouldXRevRangeOverTcp() throws Exception {
        client.xadd("s1", new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
        client.xadd("s1", new StreamId(2000L, 0L), fields("k", "v2"), XAddOptions.defaults());

        List<StreamEntry> entries = client.xrevrange("s1", StreamId.MAX, StreamId.MIN, -1);
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getId()).isEqualTo(new StreamId(2000L, 0L));
        assertThat(entries.get(1).getId()).isEqualTo(new StreamId(1000L, 0L));
    }
}
