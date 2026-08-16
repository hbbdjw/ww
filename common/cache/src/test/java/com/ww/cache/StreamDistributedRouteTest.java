package com.ww.cache;

import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stream 分布式路由端到端测试。
 *
 * <p>启动 3 个 CacheServer 节点组成集群，验证：
 * <ul>
 *   <li>stream 写操作按 HRW 路由到正确 primary</li>
 *   <li>非 primary 节点收到 stream 写时转发到 primary</li>
 *   <li>stream 读操作转发到 primary 执行</li>
 *   <li>同一 stream key 的所有写都落在同一 primary（ID 单调）</li>
 * </ul>
 */
@DisplayName("Stream 分布式路由")
class StreamDistributedRouteTest {

    private NodeCtx n1, n2, n3;

    private static class NodeCtx {
        SimpleCache<String, Object> localCache;
        DistributedCache distributedCache;
        CacheServer server;
        ClusterNode node;
        int port;
    }

    @BeforeEach
    void setUp() throws Exception {
        n1 = newNode();
        n2 = newNode();
        n3 = newNode();

        // 选端口
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
                n.port = probe.getLocalPort();
            }
        }

        // 每个节点有自己的 ClusterNode 视图：自己的 node 标 self=true，其他标 self=false
        // HRW 哈希基于 host:port，只要各节点看到的 host:port 一致，路由结果就一致
        n1.node = makeNode(n1.port, true);
        n1.distributedCache = new DistributedCache(n1.localCache,
                new RendezvousHash(makeAllNodes(n1.port, n2.port, n3.port, 0)), 3, n1.node);

        n2.node = makeNode(n2.port, true);
        n2.distributedCache = new DistributedCache(n2.localCache,
                new RendezvousHash(makeAllNodes(n1.port, n2.port, n3.port, 1)), 3, n2.node);

        n3.node = makeNode(n3.port, true);
        n3.distributedCache = new DistributedCache(n3.localCache,
                new RendezvousHash(makeAllNodes(n1.port, n2.port, n3.port, 2)), 3, n3.node);

        // 启动 server
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            n.server = new CacheServer(n.port, n.distributedCache);
            n.server.start();
        }
        Thread.sleep(300);
    }

    private NodeCtx newNode() {
        NodeCtx n = new NodeCtx();
        n.localCache = new SimpleCache<>(null);
        return n;
    }

    /** 构造单个 ClusterNode，self 标记本节点是否为自身 */
    private static ClusterNode makeNode(int port, boolean self) {
        ClusterNode n = new ClusterNode("127.0.0.1", port, 1.0);
        n.setSelf(self);
        return n;
    }

    /** 构造所有节点列表，selfIndex 指定哪个节点是"自身" */
    private static List<ClusterNode> makeAllNodes(int p1, int p2, int p3, int selfIndex) {
        return List.of(
                makeNode(p1, selfIndex == 0),
                makeNode(p2, selfIndex == 1),
                makeNode(p3, selfIndex == 2)
        );
    }

    @AfterEach
    void tearDown() {
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            if (n.server != null) n.server.stop();
        }
    }

    private Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    /** 找到 key 的 primary 节点 */
    private NodeCtx primaryOf(String key) {
        List<ClusterNode> owners = n1.distributedCache.getHash().selectNodes(key, 3);
        ClusterNode primary = owners.get(0);
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            if (n.node.equals(primary)) return n;
        }
        throw new AssertionError("找不到 primary: " + primary);
    }

    @Test
    @DisplayName("连接任意节点发送 XADD，数据应落在 HRW primary 上")
    void shouldRouteXAddToPrimary() throws Exception {
        String key = "stream-routed-1";
        NodeCtx primary = primaryOf(key);

        // 连接到非 primary 节点发送 XADD（验证转发）
        NodeCtx client = null;
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            if (n != primary) { client = n; break; }
        }
        assertThat(client).isNotNull();

        try (CacheClient c = new CacheClient("127.0.0.1", client.port)) {
            StreamId id = c.xadd(key, new StreamId(1000L, 0L), fields("k", "v"), XAddOptions.defaults());
            assertThat(id).isEqualTo(new StreamId(1000L, 0L));
        }

        // primary 本地应有这条数据
        assertThat(primary.localCache.getStreamStore().xlen(key)).isEqualTo(1L);
    }

    @Test
    @DisplayName("连接 primary 发送 XADD，backup 节点应异步复制到")
    void shouldReplicateXAddToBackups() throws Exception {
        String key = "stream-replicated-1";
        List<ClusterNode> owners = n1.distributedCache.getHash().selectNodes(key, 3);
        NodeCtx primary = primaryOf(key);

        try (CacheClient c = new CacheClient("127.0.0.1", primary.port)) {
            c.xadd(key, new StreamId(1000L, 0L), fields("k", "v1"), XAddOptions.defaults());
        }

        // 等待异步复制
        Thread.sleep(500);

        // 所有节点最终都应有数据（replicas=3）
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            assertThat(n.localCache.getStreamStore().xlen(key))
                    .as("节点 %s 应有复制数据", n.node.nodeKey())
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("XRANGE 读操作应转发到 primary 执行")
    void shouldRouteXRangeToPrimary() throws Exception {
        String key = "stream-read-1";
        NodeCtx primary = primaryOf(key);

        // 在 primary 直接写入
        primary.localCache.xadd(key, new StreamId(1000L, 0L), fields("k", "v1"), null);
        primary.localCache.xadd(key, new StreamId(2000L, 0L), fields("k", "v2"), null);

        // 连接到非 primary 节点读
        NodeCtx client = null;
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            if (n != primary) { client = n; break; }
        }

        try (CacheClient c = new CacheClient("127.0.0.1", client.port)) {
            List<StreamEntry> entries = c.xrange(key, StreamId.MIN, StreamId.MAX, -1);
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).getFields()).containsEntry("k", "v1");
        }
    }

    @Test
    @DisplayName("同一 stream key 的多次 XADD 都路由到同一 primary（ID 单调）")
    void shouldRouteAllWritesToSamePrimary() throws Exception {
        String key = "stream-monotonic-1";
        NodeCtx primary = primaryOf(key);

        StreamId prev = null;
        // 连接到每个节点轮流发送 XADD
        for (NodeCtx n : new NodeCtx[]{n1, n2, n3}) {
            try (CacheClient c = new CacheClient("127.0.0.1", n.port)) {
                StreamId id = c.xadd(key, StreamId.AUTO, fields("k", "v"), XAddOptions.defaults());
                if (prev != null) {
                    assertThat(id.compareTo(prev)).as("ID 应单调递增").isGreaterThan(0);
                }
                prev = id;
            }
        }

        // primary 应持有 3 条
        assertThat(primary.localCache.getStreamStore().xlen(key)).isEqualTo(3L);
    }

    @Test
    @DisplayName("isStreamWriteOp / isStreamReadOp 应正确识别所有 stream opCode")
    void shouldClassifyStreamOpCodes() {
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XADD)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XDEL)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XTRIM)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XGROUP_CREATE)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XACK)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XCLAIM)).isTrue();
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_XAUTOCLAIM)).isTrue();

        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XLEN)).isTrue();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XRANGE)).isTrue();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XREAD)).isTrue();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XREADGROUP)).isTrue();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XPENDING)).isTrue();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_XINFO_GROUPS)).isTrue();

        // 非 stream opCode 应返回 false
        assertThat(DistributedCache.isStreamWriteOp(CacheProtocol.OP_PUT)).isFalse();
        assertThat(DistributedCache.isStreamReadOp(CacheProtocol.OP_GET)).isFalse();
    }
}
