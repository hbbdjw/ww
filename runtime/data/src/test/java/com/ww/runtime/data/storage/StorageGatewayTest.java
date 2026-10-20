package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.data.storage.engine.StorageProperties;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.ObjectChunkP;
import com.ww.runtime.proto.ObjectReqP;
import com.ww.runtime.proto.StorageGrpc;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * StorageGateway 双节点回环端到端：本地直连 / 转发属主 / fan-out 列举合并 /
 * 桶广播 / 视图不一致时 fencing 经 gRPC 还原为 409 语义。
 */
class StorageGatewayTest {

    private static final NodeId N1 = NodeId.of("node-1");
    private static final NodeId N2 = NodeId.of("node-2");

    /** key 首字母决定分区（bucket 固定 'b'）：a*→0(N1 主), b*→1(N2 主)。 */
    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(bucketSlashKey.indexOf('/') + 1) - 'a';
    }

    @TempDir
    Path tempDir;

    private RoutingView viewN1;
    private RoutingView viewN2;
    private StorageCapability capN1;
    private StorageCapability capN2;
    private GrpcServerHost hostN1;
    private GrpcServerHost hostN2;
    private StorageGrpcClient client;
    private StorageGateway gatewayN1;

    @BeforeEach
    void setUp() throws Exception {
        viewN1 = new RoutingView();
        viewN2 = new RoutingView();
        capN1 = startCapability(N1, viewN1, tempDir.resolve("n1"));
        capN2 = startCapability(N2, viewN2, tempDir.resolve("n2"));

        hostN1 = startHost(capN1);
        hostN2 = startHost(capN2);

        Map<NodeId, Integer> ports = new ConcurrentHashMap<>(Map.of(N1, hostN1.getPort(), N2, hostN2.getPort()));
        client = new StorageGrpcClient(new StoragePeerDirectory() {
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

        gatewayN1 = new StorageGateway(N1, viewN1, StorageGatewayTest::partitioner,
                capN1, client, directoryOf());
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
    // 对象路由
    // ------------------------------------------------------------------

    @Test
    void putAndGetRouteToLocalWhenOwnerIsSelf() throws Exception {
        gatewayN1.put("bkt", "a1", "text/plain", null, stream("local"), 5L);

        assertThat(capN1.head("bkt", "a1")).isNotNull();
        assertThat(capN2.head("bkt", "a1")).isNull(); // 未转发
        assertThat(readAll(gatewayN1.get("bkt", "a1"))).isEqualTo("local");
    }

    @Test
    void putForwardsToOwnerNode() throws Exception {
        ObjectMetadata meta = gatewayN1.put("bkt", "b1", "text/plain",
                Map.of("owner", "n1-perspective"), stream("remote"), 6L);

        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(capN2.head("bkt", "b1")).isNotNull(); // 属主落盘
        assertThat(capN1.head("bkt", "b1")).isNull(); // 本地无
        assertThat(capN2.head("bkt", "b1").getUserMetadata()).containsEntry("owner", "n1-perspective");
    }

    /**
     * 验证超过 gRPC 默认单消息上限（4MB）的多分片对象经转发上传/读回：
     * 上行 4MB 数据分片 + proto 头与下行分片均正常通过。
     */
    @Test
    void forwardedPutAndGetOfMultiChunkPayloadExceedingDefaultGrpcMessageLimit() throws Exception {
        byte[] payload = new byte[5 * 1024 * 1024 + 7]; // 2 片：4MB + 1MB+7B
        new java.util.Random(42).nextBytes(payload);

        ObjectMetadata meta = gatewayN1.put("bkt", "b1", "application/octet-stream",
                null, new ByteArrayInputStream(payload), payload.length);

        assertThat(meta.getSize()).isEqualTo(payload.length);
        assertThat(capN2.head("bkt", "b1")).isNotNull(); // 属主落盘
        StorageGateway.FetchedObject obj = gatewayN1.get("bkt", "b1");
        assertThat(obj).isNotNull();
        try (obj; InputStream in = obj.in()) {
            assertThat(in.readAllBytes()).isEqualTo(payload); // 下行分片同样超 4MB
        }
    }

    @Test
    void getAndHeadForwardToOwnerAndTranslateMissing() throws Exception {
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("via-owner"), 9L);

        assertThat(readAll(gatewayN1.get("bkt", "b1"))).isEqualTo("via-owner");
        assertThat(gatewayN1.head("bkt", "b1")).isNotNull();
        assertThat(gatewayN1.head("bkt", "b1").getSize()).isEqualTo(9L);
        assertThat(gatewayN1.get("bkt", "nope")).isNull();
        assertThat(gatewayN1.head("bkt", "nope")).isNull();
    }

    /**
     * 区间读（边下边播下推）：本地/远端属主均返回已定位到窗口起点的流；
     * 远端属主窗口下推（不从 0 回放全量再丢前缀）；length=0 直到对象末尾；
     * offset 超出对象大小统一返回 null（上层转 404）。
     */
    @Test
    void rangeGetReturnsPositionedWindowOnBothOwners() throws Exception {
        gatewayN1.put("bkt", "a1", "text/plain", null, stream("0123456789"), 10L); // 分区 0 → N1 本地
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("abcdefghij"), 10L); // 分区 1 → N2 远端

        // 本地属主：skip 定位到 offset，按 length 截断由调用方负责（此处按长读 4 字节）
        StorageGateway.FetchedObject local = gatewayN1.get("bkt", "a1", 3L, 4L);
        assertThat(local.meta().getSize()).isEqualTo(10L); // 元数据仍为全对象大小
        assertThat(readWindow(local, 4)).isEqualTo("3456");

        // 远端属主：窗口下推，首片即窗口起点
        assertThat(readWindow(gatewayN1.get("bkt", "b1", 5L, 5L), 5)).isEqualTo("fghij");

        // length=0：直到对象末尾
        assertThat(readWindow(gatewayN1.get("bkt", "b1", 7L, 0L), 0)).isEqualTo("hij");

        // offset 超出对象大小 → null
        assertThat(gatewayN1.get("bkt", "a1", 100L, 10L)).isNull();
        assertThat(gatewayN1.get("bkt", "b1", 100L, 10L)).isNull();
    }

    /**
     * 旧版属主回退（滚动升级窗口）：属主不识别 offset/length、从 0 回放全量，
     * 网关按片上 offset 快进丢弃前缀（不拷贝字节），仍返回正确窗口——绝不错位。
     */
    @Test
    void rangedGetFallsBackWhenOwnerIgnoresRange() throws Exception {
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("abcdefghij"), 10L); // N2 属主

        // 旧版属主：委托真实服务端但清零 offset/length，等价于不识别窗口读的旧版本节点
        // （StorageGrpcServer 为 final，经 StorageImplBase 组合包装而非继承）
        StorageGrpcServer realServer = new StorageGrpcServer(capN2, 8);
        GrpcServerHost legacyHost = new GrpcServerHost();
        legacyHost.addService(new StorageGrpc.StorageImplBase() {
            @Override
            public void forwardGet(ObjectReqP request, StreamObserver<ObjectChunkP> responseObserver) {
                realServer.forwardGet(ObjectReqP.newBuilder(request)
                        .clearOffset().clearLength().build(), responseObserver);
            }
        });
        legacyHost.start(0);
        try {
            StorageGrpcClient legacyClient = new StorageGrpcClient(new StoragePeerDirectory() {
                @Override
                public Optional<PeerEndpoint> endpoint(NodeId node) {
                    int port = node.equals(N1) ? hostN1.getPort() : legacyHost.getPort();
                    return Optional.of(new PeerEndpoint("127.0.0.1", port));
                }

                @Override
                public List<NodeId> alivePeers() {
                    return List.of(N1, N2);
                }
            });
            try {
                StorageGateway legacyGw = new StorageGateway(N1, viewN1, StorageGatewayTest::partitioner,
                        capN1, legacyClient, directoryOf());
                // 窗口起点落在片中部（chunkSize=8，首片 [0,8)）
                assertThat(readWindow(legacyGw.get("bkt", "b1", 5L, 5L), 5)).isEqualTo("fghij");
                // 窗口起点即对象起点
                assertThat(readWindow(legacyGw.get("bkt", "b1", 0L, 3L), 3)).isEqualTo("abc");
            } finally {
                legacyClient.close();
            }
        } finally {
            legacyHost.stop();
        }
    }

    @Test
    void deleteForwardsToOwner() throws Exception {
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("doomed"), 6L);
        assertThat(gatewayN1.delete("bkt", "b1")).isTrue();
        assertThat(capN2.head("bkt", "b1")).isNull();
        assertThat(gatewayN1.delete("bkt", "b1")).isFalse(); // 已删
    }

    @Test
    void forwardedPutWithDivergentViewSurfacesAsStaleEpoch() {
        // 网关侧视图：分区 1 属主=N2；N2 本地视图：属主=N1（视图不一致）→ 属主端 fencing 拒绝
        viewN2.owner(N2, N1);
        assertThatThrownBy(() -> gatewayN1.put("bkt", "b1", "text/plain", null, stream("x"), 1L))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capN2.head("bkt", "b1")).isNull();
    }

    /**
     * P0-2 隔离门：分区 DEGRADED（属主死亡且无存活副本，放置留盘押注重启）时
     * 读写一律快速失败（{@link com.ww.runtime.common.PartitionDegradedException} → 503），
     * 不静默空读/空写；未隔离分区不受影响。
     */
    @Test
    void degradedPartitionRejectsReadsAndWrites() throws Exception {
        // 分区 0（a* → N1 主）隔离；分区 1（b* → N2 主）正常
        StorageGateway quarantined = new StorageGateway(N1, viewN1, StorageGatewayTest::partitioner,
                capN1, client, directoryOf(),
                (capability, partition) -> partition == 0);

        assertThatThrownBy(() -> quarantined.put("bkt", "a1", "text/plain", null, stream("x"), 1L))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class)
                .hasMessageContaining("quarantined");
        assertThatThrownBy(() -> quarantined.get("bkt", "a1"))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class);
        assertThatThrownBy(() -> quarantined.head("bkt", "a1"))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class);
        assertThatThrownBy(() -> quarantined.delete("bkt", "a1"))
                .isInstanceOf(com.ww.runtime.common.PartitionDegradedException.class);
        assertThat(capN1.head("bkt", "a1")).isNull(); // 隔离门在路由前生效，本地未落盘

        // 未隔离分区（分区 1 → N2 主）正常路由
        assertThat(quarantined.head("bkt", "b1")).isNull(); // 不存在但走正常路由（不抛隔离异常）
    }

    // ------------------------------------------------------------------
    // 切主收敛（调用端无感：stale-epoch/not-owner 单次重取属主重试）
    // ------------------------------------------------------------------

    /**
     * cutover 在途写（本地路径）：首次本地写被 fencing 拒绝（checkOwner 在读流前，
     * 入参流未消费）→ 重取属主重试转为远端转发，收敛到新属主且对象完整。
     */
    @Test
    void cutoverInFlightPutRetriesLocalRejectionToRemoteOwner() throws Exception {
        // viewN1/viewN2 分区 1 属主=N2（setUp 默认）；网关视图翻转：首次 N1（旧）其后 N2（新）
        StorageGateway racing = new StorageGateway(N1, flipView(N1, N2),
                StorageGatewayTest::partitioner, capN1, client, directoryOf());

        ObjectMetadata meta = racing.put("bkt", "b1", "text/plain", null, stream("converged"), 9L);

        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(capN2.head("bkt", "b1")).isNotNull(); // 新属主持有数据
        assertThat(capN1.head("bkt", "b1")).isNull();
        assertThat(readAll(gatewayN1.get("bkt", "b1"))).isEqualTo("converged"); // 读回无截断
    }

    /**
     * cutover 在途删除：首次转发命中旧属主被 fencing 拒绝
     * （ok=false + stale-epoch 标记）→ 重试收敛到新属主，返回真实删除语义。
     */
    @Test
    void cutoverInFlightDeleteRetriesToNewOwner() throws Exception {
        // 阶段一：分区 1 属主=N2，数据经转发写入
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("doomed"), 6L);
        assertThat(capN2.head("bkt", "b1")).isNotNull();

        // 阶段二：模拟 cutover + 迁移完成——放置指向 N1，数据已在 N1
        viewN1.owner(N1, N1);
        viewN2.owner(N1, N1);
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("moved"), 5L);
        assertThat(capN1.head("bkt", "b1")).isNotNull();

        // 阶段三：网关视图翻转（首次 N2 旧，其后 N1 新）
        StorageGateway racing = new StorageGateway(N1, flipView(N2, N1),
                StorageGatewayTest::partitioner, capN1, client, directoryOf());

        assertThat(racing.delete("bkt", "b1")).isTrue(); // 重试收敛，真实删除
        assertThat(capN1.head("bkt", "b1")).isNull();
    }

    /**
     * cutover 在途读：网关视图仍指向本机但本地能力视图已切换
     * （servesPartitionOf not-owner 纠偏）→ 重试收敛到新属主读到数据。
     */
    @Test
    void cutoverInFlightGetRetriesLocalNotOwnerToNewOwner() throws Exception {
        // 数据在 N2（分区 1 属主=N2，viewN1/viewN2 setUp 默认一致）
        gatewayN1.put("bkt", "b1", "text/plain", null, stream("read-me"), 7L);

        // 网关视图翻转：首次 N1（旧）其后 N2（新）；capN1 的 viewN1 恒指 N2 → 本地探测 not-owner
        StorageGateway racing = new StorageGateway(N1, flipView(N1, N2),
                StorageGatewayTest::partitioner, capN1, client, directoryOf());

        assertThat(readAll(racing.get("bkt", "b1"))).isEqualTo("read-me");
        assertThat(racing.head("bkt", "b1").getSize()).isEqualTo(7L);
    }

    // ------------------------------------------------------------------
    // fan-out 列举与桶广播
    // ------------------------------------------------------------------

    @Test
    void listMergesBothNodesByFanout() throws Exception {
        // 同桶对象分布于两节点（key 首字母决定属主）
        gatewayN1.put("bkt", "a-key", "text/plain", null, stream("1"), 1L); // N1 主
        gatewayN1.put("bkt", "b-key", "text/plain", null, stream("22"), 2L); // N2 主
        // N2 视角直写自己的主分区
        new StorageGateway(N2, viewN2, StorageGatewayTest::partitioner,
                capN2, client, directoryOf())
                .put("bkt", "b-key2", "text/plain", null, stream("333"), 3L);

        StorageGateway.ListResult all = gatewayN1.list("bkt", null, null, 100);
        assertThat(all.objects()).extracting(ObjectMetadata::getKey)
                .containsExactly("a-key", "b-key", "b-key2");
        assertThat(all.truncated()).isFalse();

        StorageGateway.ListResult paged = gatewayN1.list("bkt", null, null, 2);
        assertThat(paged.objects()).hasSize(2);
        assertThat(paged.truncated()).isTrue();
        assertThat(paged.nextMarker()).isEqualTo("b-key");

        StorageGateway.ListResult prefixed = gatewayN1.list("bkt", "a-", null, 100);
        assertThat(prefixed.objects()).extracting(ObjectMetadata::getKey)
                .containsExactly("a-key");
    }

    @Test
    void bucketCreateAndDeleteBroadcastToPeer() {
        gatewayN1.createBucket("shared");
        assertThat(capN1.registeredBuckets()).contains("shared");
        assertThat(capN2.registeredBuckets()).contains("shared"); // SyncBucket 广播

        gatewayN1.deleteBucket("shared", true);
        assertThat(capN1.registeredBuckets()).doesNotContain("shared");
        assertThat(capN2.registeredBuckets()).doesNotContain("shared");
    }

    /**
     * 验证 fan-out 列举只查询「正在服务 STORAGE」的节点（{@code servingPeers()}），
     * 不向能力已停用的存活节点发起查询（避免「capability not started」拒绝与告警）。
     * N2 存活但不在 servingPeers 中，其对象对列举不可见（未被查询）。
     */
    @Test
    void listFanoutSkipsAliveButNonServingPeer() throws Exception {
        gatewayN1.put("bkt", "a-key", "text/plain", null, stream("1"), 1L);  // N1 主（本地）
        gatewayN1.put("bkt", "b-key", "text/plain", null, stream("22"), 2L); // N2 主（转发）
        assertThat(capN2.head("bkt", "b-key")).isNotNull();

        // 目录：N2 仍存活（alivePeers），但已不再服务 STORAGE（servingPeers 排除它）
        StoragePeerDirectory n2NotServing = new StoragePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                return clientDirectoryEndpoint(node);
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }

            @Override
            public List<NodeId> servingPeers() {
                return List.of(N1);
            }
        };
        StorageGateway gateway = new StorageGateway(N1, viewN1, StorageGatewayTest::partitioner,
                capN1, client, n2NotServing);

        // 仅本地（N1）对象可见：N2 未被 fan-out 查询
        assertThat(gateway.list("bkt", null, null, 100).objects())
                .extracting(ObjectMetadata::getKey)
                .containsExactly("a-key");
    }

    /**
     * T10：删桶顺序重构——任一远端节点删除失败即中止（本地桶保持不动，可重试），
     * 不再出现"本地已删、远端残留"的跨节点分叉。
     */
    @Test
    void deleteBucketAbortsWhenRemoteFails() throws Exception {
        gatewayN1.createBucket("shared");
        assertThat(capN1.registeredBuckets()).contains("shared");
        assertThat(capN2.registeredBuckets()).contains("shared");

        // N2 能力停止 → SyncBucket 删除在远端失败（ok=false）
        capN2.stop();

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> gatewayN1.deleteBucket("shared", true))
                .isInstanceOf(com.ww.runtime.common.RuntimeFrameworkException.class)
                .hasMessageContaining("aborted");
        // 本地桶保持未删（全或无语义，可重试收敛）
        assertThat(capN1.registeredBuckets()).contains("shared");
    }

    /**
     * T20：删除无墓碑的合并兜底——属主已删除的 key，即使 stale 副本仍在列举中上报，
     * 也不得复活（合并以属主结果为准）。本机属主（localKeys 未含）与远端属主两个分支各验其一。
     */
    @Test
    void listSuppressesStaleReplicaResurrection() throws Exception {
        // a-key 属主 = N1（本机）
        ObjectMetadata meta = gatewayN1.put("bkt", "a-key", "text/plain", null, stream("v"), 1L);
        assertThat(gatewayN1.list("bkt", null, null, 100).objects())
                .extracting(ObjectMetadata::getKey).containsExactly("a-key");

        // 主节点删除；stale 副本（N2）仍持有旧版本
        gatewayN1.delete("bkt", "a-key");
        capN2.replicaPut("bkt", "a-key",
                new java.io.ByteArrayInputStream("stale".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                meta);

        // 修复前：N2 上报的 stale 条目进入合并结果（已删对象复活）
        assertThat(gatewayN1.list("bkt", null, null, 100).objects())
                .as("属主（本机）未上报的 key 不得由 stale 副本复活")
                .extracting(ObjectMetadata::getKey).isEmpty();
    }

    /** T19：列举失败的节点记入 missingPeers 降级标记，而非静默缺失。 */
    @Test
    void listMarksFailedPeersAsMissing() throws Exception {
        gatewayN1.put("bkt", "a-key", "text/plain", null, stream("1"), 1L);

        // client 与目录均把 N2 指向不可达端口 → fan-out 对 N2 失败（N1 正常）
        StoragePeerDirectory deadN2 = new StoragePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                if (node.equals(N2)) {
                    return Optional.of(new PeerEndpoint("127.0.0.1", 1)); // 不可达
                }
                return clientDirectoryEndpoint(node);
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }

            @Override
            public List<NodeId> servingPeers() {
                return List.of(N1, N2);
            }
        };
        StorageGrpcClient dyingClient = new StorageGrpcClient(deadN2);
        try {
            StorageGateway gateway = new StorageGateway(N1, viewN1, StorageGatewayTest::partitioner,
                    capN1, dyingClient, deadN2);

            StorageGateway.ListResult result = gateway.list("bkt", null, null, 100);
            assertThat(result.objects()).extracting(ObjectMetadata::getKey).containsExactly("a-key");
            assertThat(result.missingPeers()).as("失败节点必须显式降级标记").isNotEmpty();
        } finally {
            dyingClient.close();
        }
    }

    /**
     * T21 桶对账：N2 桶注册缺失（漏广播/盘丢）→ 周期对账发现差集并幂等补建。
     */
    @Test
    void reconcileBucketsReRegistersMissingPeerBucket() {
        gatewayN1.createBucket("shared");
        assertThat(capN2.registeredBuckets()).contains("shared");

        // 模拟 N2 桶注册丢失（本地直删，绕过广播）
        capN2.deleteBucketLocal("shared", false);
        assertThat(capN2.registeredBuckets()).doesNotContain("shared");

        StorageGateway.BucketReconcileReport report = gatewayN1.reconcileBuckets();
        assertThat(report.repaired()).as("对账应补建缺失桶").isNotEmpty();
        assertThat(capN2.registeredBuckets()).contains("shared");
    }

    /**
     * T10 强化：非递归删桶的"桶是否为空"必须是集群级口径——对象按主分区散在各节点，
     * 本地视角为空但远端持有 → 在任何破坏性操作发生前以 StorageException（REST 409）
     * 拒绝并指明持有节点；清理对象后方可删除。
     */
    @Test
    void deleteBucketNonRejectsWhenRemoteHoldsObjects() throws Exception {
        gatewayN1.createBucket("mixed");
        // b-key 属主 = N2（分区 1）：转发写只在 N2 落对象，N1 本地计数为 0
        gatewayN1.put("mixed", "b-key", "text/plain", null, stream("payload"), 7L);
        assertThat(capN1.countObjects("mixed")).as("本地视角为空").isZero();
        assertThat(capN2.countObjects("mixed")).as("对象实际在 N2").isEqualTo(1L);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> gatewayN1.deleteBucket("mixed", false))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("node-2")
                .hasMessageContaining("非空");
        // 未发生任何删除：对象与两侧桶注册均原样
        assertThat(capN2.head("mixed", "b-key")).isNotNull();
        assertThat(capN1.registeredBuckets()).contains("mixed");
        assertThat(capN2.registeredBuckets()).contains("mixed");

        // 清掉集群对象后非递归删除成功（全或无语义仍成立）
        gatewayN1.delete("mixed", "b-key");
        gatewayN1.deleteBucket("mixed", false);
        assertThat(capN1.registeredBuckets()).doesNotContain("mixed");
        assertThat(capN2.registeredBuckets()).doesNotContain("mixed");
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    private StoragePeerDirectory directoryOf() {
        return new StoragePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                return client == null ? Optional.empty() : clientDirectoryEndpoint(node);
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(N1, N2);
            }
        };
    }

    private Optional<StoragePeerDirectory.PeerEndpoint> clientDirectoryEndpoint(NodeId node) {
        int port = node.equals(N1) ? hostN1.getPort() : hostN2.getPort();
        return Optional.of(new StoragePeerDirectory.PeerEndpoint("127.0.0.1", port));
    }

    private static ByteArrayInputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String readAll(StorageGateway.FetchedObject obj) throws IOException {
        try (obj; InputStream in = obj.in()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** 从已定位的流读窗口：maxBytes>0 按长截断（本地属主路径不自行截断）；0 读到末尾。 */
    private static String readWindow(StorageGateway.FetchedObject obj, int maxBytes) throws IOException {
        try (obj; InputStream in = obj.in()) {
            byte[] data = maxBytes > 0 ? in.readNBytes(maxBytes) : in.readAllBytes();
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private StorageCapability startCapability(NodeId self, RoutingView view, Path dir) throws Exception {
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(dir.toString()));
        props.setRootDir(dir.toString());
        StorageCapability capability = new StorageCapability(self, view,
                StorageGatewayTest::partitioner, props, 8);
        capability.start();
        return capability;
    }

    private GrpcServerHost startHost(StorageCapability capability) throws Exception {
        GrpcServerHost host = new GrpcServerHost();
        host.addService(new StorageGrpcServer(capability, 8));
        host.start(0);
        return host;
    }

    /** 翻转视图：分区 1 首次查询返回旧属主，其后返回新属主（模拟 cutover 在途竞态）。 */
    private static PlacementView flipView(NodeId staleP1, NodeId freshP1) {
        return new PlacementView() {
            private int calls;

            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                if (partition.value() == 1) {
                    return Optional.of(new Owner(++calls == 1 ? staleP1 : freshP1, 3L));
                }
                return Optional.of(new Owner(N1, 3L));
            }
        };
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

        @Override
        public List<Owner> holders(CapabilityType cap, PartitionId partition) {
            return List.of(owner(cap, partition).orElseThrow());
        }
    }
}
