package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.grpc.GrpcServerHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CacheGateway 双节点回环端到端：本地直连 / 转发属主 / TTL 保持 /
 * 视图不一致时 fencing 经 gRPC 还原为 409 语义 / 单机退化。
 */
class CacheGatewayTest {

    private static final NodeId N1 = NodeId.of("node-1");
    private static final NodeId N2 = NodeId.of("node-2");

    /** key 首字母决定分区：a*→0(N1 主), b*→1(N2 主)。 */
    private static int partitioner(String key) {
        return key.charAt(0) - 'a';
    }

    private RoutingView viewN1;
    private RoutingView viewN2;
    private CacheCapability capN1;
    private CacheCapability capN2;
    private GrpcServerHost hostN1;
    private GrpcServerHost hostN2;
    private CacheGrpcClient client;
    private CacheGateway gatewayN1;

    @BeforeEach
    void setUp() throws Exception {
        viewN1 = new RoutingView();
        viewN2 = new RoutingView();
        capN1 = startCapability(N1, viewN1);
        capN2 = startCapability(N2, viewN2);

        hostN1 = startHost(capN1);
        hostN2 = startHost(capN2);

        Map<NodeId, Integer> ports = new ConcurrentHashMap<>(Map.of(N1, hostN1.getPort(), N2, hostN2.getPort()));
        client = new CacheGrpcClient(new CachePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                Integer port = ports.get(node);
                return port == null ? Optional.empty()
                        : Optional.of(new PeerEndpoint("127.0.0.1", port));
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }
        });

        // 初始放置：分区 0 → N1 主，分区 1 → N2 主（两侧视图一致）
        viewN1.owner(N1, N2);
        viewN2.owner(N1, N2);

        gatewayN1 = new CacheGateway(N1, viewN1, CacheGatewayTest::partitioner, capN1, client, directoryOf());
    }

    @AfterEach
    void tearDown() {
        client.close();
        hostN1.stop();
        hostN2.stop();
        capN1.stop();
        capN2.stop();
    }

    // ------------------------------------------------------------------
    // KV 路由
    // ------------------------------------------------------------------

    @Test
    void putAndGetRouteToLocalWhenOwnerIsSelf() {
        gatewayN1.put("a1", "local", 0);

        assertThat(capN1.get("a1")).isEqualTo("local");
        assertThat(capN2.get("a1")).isNull(); // 未转发
        assertThat(gatewayN1.get("a1")).isEqualTo("local");
    }

    @Test
    void putForwardsToOwnerNode() {
        gatewayN1.put("b1", "remote-value", 0);

        assertThat(capN2.get("b1")).isEqualTo("remote-value"); // 属主落盘
        assertThat(capN1.get("b1")).isNull(); // 本地无
        assertThat(gatewayN1.get("b1")).isEqualTo("remote-value"); // 读同样转发
    }

    @Test
    void forwardedPutPreservesTtl() throws Exception {
        gatewayN1.put("b2", "ttl-value", 60_000);

        long ttl = capN2.remainingTtlMillis("b2");
        assertThat(ttl).isBetween(1L, 60_000L);
    }

    @Test
    void putIfAbsentRoutesLocallyAndForwardsToOwner() {
        assertThat(gatewayN1.putIfAbsent("a1", "local-1", 0)).isTrue();
        assertThat(gatewayN1.putIfAbsent("a1", "local-2", 0)).isFalse();
        assertThat(capN1.get("a1")).isEqualTo("local-1");

        assertThat(gatewayN1.putIfAbsent("b1", "remote-1", 0)).isTrue();
        assertThat(gatewayN1.putIfAbsent("b1", "remote-2", 0)).isFalse();
        assertThat(capN2.get("b1")).isEqualTo("remote-1");
    }

    @Test
    void cutoverInFlightPutIfAbsentRetriesToNewOwner() {
        viewN1.owner(N1, N1);
        viewN2.owner(N1, N1);

        PlacementView flip = new PlacementView() {
            private int calls;

            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                if (partition.value() == 1 && ++calls > 1) {
                    return Optional.of(new Owner(N1, 4L));
                }
                return Optional.of(new Owner(partition.value() == 0 ? N1 : N2, 3L));
            }
        };
        CacheGateway racing = new CacheGateway(N1, flip, CacheGatewayTest::partitioner,
                capN1, client, directoryOf());

        assertThat(racing.putIfAbsent("b1", "converged", 0)).isTrue();
        assertThat(racing.putIfAbsent("b1", "duplicate", 0)).isFalse();

        assertThat(capN1.get("b1")).isEqualTo("converged");
        assertThat(capN2.get("b1")).isNull();
    }

    @Test
    void deleteForwardsToOwner() {
        gatewayN1.put("b3", "doomed", 0);
        assertThat(gatewayN1.remove("b3")).isTrue();
        assertThat(capN2.get("b3")).isNull();
        assertThat(gatewayN1.remove("b3")).isFalse(); // 已删
    }

    @Test
    void getMissingKeyReturnsNullViaForward() {
        assertThat(gatewayN1.get("b-nope")).isNull();
    }

    @Test
    void forwardedPutWithDivergentViewSurfacesAsStaleEpoch() {
        // 网关侧视图：分区 1 属主=N2；N2 本地视图：属主=N1（视图不一致）→ 属主端 fencing 拒绝
        viewN2.owner(N2, N1);
        assertThatThrownBy(() -> gatewayN1.put("b1", "x", 0))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capN2.get("b1")).isNull();
    }

    /**
     * P0-2 隔离门：CACHE 为 RF=1 单副本——属主死亡且治愈宽限期赌输后分区 DEGRADED，
     * 读写一律快速失败（{@link com.ww.runtime.common.PartitionDegradedException} → 503），
     * 放置保持指向死亡属主押注重启，不静默空读/空写。
     */
    @Test
    void degradedPartitionRejectsReadsAndWrites() {
        // 分区 0（a* → N1 主）隔离；分区 1（b* → N2 主）正常
        CacheGateway quarantined = new CacheGateway(N1, viewN1, CacheGatewayTest::partitioner,
                capN1, client, directoryOf(),
                (capability, partition) -> partition == 0);

        assertThatThrownBy(() -> quarantined.put("a1", "x", 0))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class)
                .hasMessageContaining("quarantined");
        assertThatThrownBy(() -> quarantined.get("a1"))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class);
        assertThatThrownBy(() -> quarantined.remove("a1"))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class);
        assertThat(capN1.get("a1")).isNull(); // 隔离门在路由前生效，本地未落盘

        // 未隔离分区（分区 1 → N2 主）正常路由
        assertThat(quarantined.get("b-nope")).isNull();
    }

    // ------------------------------------------------------------------
    // 切主收敛（调用端无感：stale-epoch/not-owner 单次重取属主重试）
    // ------------------------------------------------------------------

    /** cutover 在途的写：首次转发命中旧属主被 fencing 拒绝 → 重试收敛到新属主，客户端无感。 */
    @Test
    void cutoverInFlightPutRetriesToNewOwner() {
        // 两侧最终态视图：分区 1 属主=N1（cutover 已生效）
        viewN1.owner(N1, N1);
        viewN2.owner(N1, N1);

        // 网关首次查询返回旧属主 N2（在途缓存/竞态），之后返回新属主 N1
        PlacementView flip = new PlacementView() {
            private int calls;

            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                if (partition.value() == 1 && ++calls > 1) {
                    return Optional.of(new Owner(N1, 4L));
                }
                return Optional.of(new Owner(partition.value() == 0 ? N1 : N2, 3L));
            }
        };
        CacheGateway racing = new CacheGateway(N1, flip, CacheGatewayTest::partitioner,
                capN1, client, directoryOf());

        racing.put("b1", "converged", 0); // 不抛：内部重试一次收敛

        assertThat(capN1.get("b1")).isEqualTo("converged"); // 新属主持有数据
        assertThat(capN2.get("b1")).isNull();
    }

    /** cutover 在途的读：旧属主 not-owner 拒绝 → 重试收敛到新属主读到数据。 */
    @Test
    void cutoverInFlightGetRetriesToNewOwner() {
        // 数据在 N1（新属主），两侧视图一致指向 N1
        viewN1.owner(N1, N1);
        viewN2.owner(N1, N1);
        gatewayN1.put("b1", "read-me", 0);
        assertThat(capN1.get("b1")).isEqualTo("read-me");

        PlacementView flip = new PlacementView() {
            private int calls;

            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                if (partition.value() == 1 && ++calls > 1) {
                    return Optional.of(new Owner(N1, 4L));
                }
                return Optional.of(new Owner(partition.value() == 0 ? N1 : N2, 3L));
            }
        };
        CacheGateway racing = new CacheGateway(N1, flip, CacheGatewayTest::partitioner,
                capN1, client, directoryOf());

        // 首次路由到旧属主 N2 → forwardGet not-owner 拒绝 → 重试路由到 N1 本地读
        assertThat(racing.get("b1")).isEqualTo("read-me");
    }

    @Test
    void localOnlyModeRejectsForward() {
        CacheGateway localOnly = new CacheGateway(N1, viewN1, CacheGatewayTest::partitioner, capN1, null, null);
        assertThatThrownBy(() -> localOnly.put("b1", "x", 0))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("local-only");
    }

    @Test
    void listKeysFansOutAcrossNodes() {
        gatewayN1.put("a1", "v1", 0);   // N1 主
        gatewayN1.put("b1", "v2", 0);   // N2 主（转发）
        gatewayN1.put("b2", "v3", 0);   // N2 主（转发）

        List<CacheCapability.CacheKeyEntry> all = gatewayN1.listKeys(null, 100);
        assertThat(all).extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1", "b1", "b2"); // 集群视图，字典序

        // 前缀过滤同样跨节点
        assertThat(gatewayN1.listKeys("b", 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("b1", "b2");
    }

    @Test
    void countKeysSumsAcrossNodes() {
        gatewayN1.put("a1", "v1", 0);
        gatewayN1.put("b1", "v2", 0);
        gatewayN1.put("b2", "v3", 0);

        assertThat(gatewayN1.countKeys(null)).isEqualTo(3L);
        assertThat(gatewayN1.countKeys("b")).isEqualTo(2L);
        assertThat(gatewayN1.countKeys("zz")).isZero();
    }

    @Test
    void listKeysDegradesWhenPeerUnreachable() {
        gatewayN1.put("a1", "v1", 0);
        gatewayN1.put("b1", "v2", 0);

        // N2 端口不可达的目录：fan-out 失败降级，仅返回本地部分
        CacheGrpcClient degradedClient = new CacheGrpcClient(new CachePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                return Optional.of(new PeerEndpoint("127.0.0.1", 1)); // 不可达端口
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }
        });
        try {
            CacheGateway degraded = new CacheGateway(N1, viewN1, CacheGatewayTest::partitioner,
                    capN1, degradedClient, directoryOf());
            assertThat(degraded.listKeys(null, 100))
                    .extracting(CacheCapability.CacheKeyEntry::key)
                    .containsExactly("a1"); // 仅本地，不整体失败
        } finally {
            degradedClient.close();
        }
    }

    @Test
    void localOnlyListKeysStaysLocal() {
        gatewayN1.put("a1", "v1", 0);
        gatewayN1.put("b1", "v2", 0); // 转发到 N2

        CacheGateway localOnly = new CacheGateway(N1, viewN1, CacheGatewayTest::partitioner, capN1, null, null);
        assertThat(localOnly.listKeys(null, 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("a1"); // 无 directory 退化本地
        assertThat(localOnly.totalItems()).isEqualTo(1L);
    }

    // ------------------------------------------------------------------
    // 本地能力停止（节点被禁用、数据已迁出）：网关退化为纯转发/聚合
    // ------------------------------------------------------------------

    /**
     * 禁用 node-1 的 CACHE 后经 8091 访问缓存管理（listKeys/countKeys）不报错：
     * 本地部分按 0 条跳过，集群视图由存活属主节点（N2）聚合——任意节点可调用。
     */
    @Test
    void listKeysAndCountTolerateLocalCapabilityStopped() {
        // 数据全部位于 N2 属主分区（b*→1）
        gatewayN1.put("b1", "v1", 0);
        gatewayN1.put("b2", "v2", 0);

        // 模拟调度器停止 node-1 能力（数据已迁出；gRPC server 仍在）
        capN1.stop();
        assertThat(capN1.isStarted()).isFalse();

        // 经 node-1 的网关列举/计数：不抛 "cache capability not started"
        assertThat(gatewayN1.listKeys(null, 100))
                .extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("b1", "b2");
        assertThat(gatewayN1.countKeys(null)).isEqualTo(2L);
        assertThat(gatewayN1.countKeys("b")).isEqualTo(2L);
        assertThat(gatewayN1.countKeys("zz")).isZero();

        // 读写经属主转发不受影响
        assertThat(gatewayN1.get("b1")).isEqualTo("v1");
        gatewayN1.put("b3", "v3", 0);
        assertThat(capN2.get("b3")).isEqualTo("v3");
    }

    // ------------------------------------------------------------------
    // 统一命令通道
    // ------------------------------------------------------------------

    @Test
    void commandForwardingRoutesContainersToOwner() {
        assertThat(gatewayN1.executeCommand(CacheCommandRequest.of("HSET", "b1", "f", "v")))
                .isEqualTo(1L);
        assertThat(((Map<?, ?>) capN2.get("b1")).get("f")).isEqualTo("v");
        assertThat(gatewayN1.executeCommand(CacheCommandRequest.of("HGET", "b1", "f")))
                .isEqualTo("v");
        assertThat(gatewayN1.executeCommand(CacheCommandRequest.of("TYPE", "b1")))
                .isEqualTo("hash");
    }

    @Test
    void commandMgetRoutesPerKeyAndMsetRejectsCrossPartition() {
        gatewayN1.put("a1", "A", 0);
        gatewayN1.put("b1", "B", 0);

        assertThat(gatewayN1.executeCommand(CacheCommandRequest.of("MGET", "a1", "b1", "zz")))
                .isEqualTo(java.util.Arrays.asList("A", "B", null));

        assertThatThrownBy(() -> gatewayN1.executeCommand(
                CacheCommandRequest.of("MSET", "a1", "x", "b1", "y")))
                .isInstanceOf(CacheCommandException.class)
                .hasMessageContaining("multiple partitions");
    }

    @Test
    void commandMsetSamePartitionWritesLocally() {
        assertThat(gatewayN1.executeCommand(
                CacheCommandRequest.of("MSET", "a1", "x", "a2", "y"))).isEqualTo(2L);
        assertThat(capN1.get("a1")).isEqualTo("x");
        assertThat(capN1.get("a2")).isEqualTo("y");
    }

    @Test
    void clusterStatsIncludesAllServingNodes() {
        gatewayN1.put("a1", "A", 0);
        Map<String, Object> stats = gatewayN1.clusterStats();
        assertThat((List<?>) stats.get("nodes")).hasSize(2);
        assertThat(stats).containsKey("errors");
    }

    private CachePeerDirectory directoryOf() {
        return new CachePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                int port = node.equals(N1) ? hostN1.getPort() : hostN2.getPort();
                return Optional.of(new PeerEndpoint("127.0.0.1", port));
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }
        };
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    private CacheCapability startCapability(NodeId self, RoutingView view) throws Exception {
        CacheCapability capability = new CacheCapability(self, view,
                CacheGatewayTest::partitioner, false, null);
        capability.start();
        return capability;
    }

    private GrpcServerHost startHost(CacheCapability capability) throws Exception {
        GrpcServerHost host = new GrpcServerHost();
        host.addService(new CacheGrpcServer(capability));
        host.start(0);
        return host;
    }

    /** 可路由桩视图：分区 0/1 → 各自 owner。 */
    private static final class RoutingView implements PlacementView {
        private volatile NodeId partition0Owner = N1;
        private volatile NodeId partition1Owner = N2;

        void owner(NodeId p0, NodeId p1) {
            this.partition0Owner = p0;
            this.partition1Owner = p1;
        }

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            NodeId node = partition.value() == 0 ? partition0Owner : partition1Owner;
            return Optional.of(new Owner(node, 3L));
        }
    }
}
