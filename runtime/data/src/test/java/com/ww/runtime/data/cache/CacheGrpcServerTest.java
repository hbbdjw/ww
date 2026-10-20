package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.grpc.GrpcServerHost;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CacheGrpcServer gRPC 端到端（进程内随机端口 + CacheGrpcClient 回环）：
 * ForwardPut/Get/Delete 全语义（含 TTL）、fencing 拒绝映射为 stale-epoch。
 */
class CacheGrpcServerTest {

    private static final NodeId SELF = NodeId.of("node-1");

    private StubView view;
    private CacheCapability capability;
    private GrpcServerHost host;
    private CacheGrpcClient client;

    @BeforeEach
    void setUp() throws Exception {
        view = new StubView();
        capability = new CacheCapability(SELF, view, key -> 0, false, null);
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));

        host = new GrpcServerHost();
        host.addService(new CacheGrpcServer(capability));
        host.start(0);

        client = new CacheGrpcClient(new CachePeerDirectory() {
            @Override
            public Optional<PeerEndpoint> endpoint(NodeId node) {
                return Optional.of(new PeerEndpoint("127.0.0.1", host.getPort()));
            }

            @Override
            public List<NodeId> alivePeers() {
                return List.of(SELF);
            }
        });
    }

    @AfterEach
    void tearDown() {
        client.close();
        host.stop();
        capability.stop();
    }

    // ---- 网关转发 ----

    @Test
    void forwardPutGetDeleteRoundTrip() {
        client.forwardPut(SELF, "k1", "value-1", 0);
        assertThat(capability.get("k1")).isEqualTo("value-1");

        Optional<CacheGrpcClient.CacheValue> got = client.forwardGet(SELF, "k1");
        assertThat(got).isPresent();
        assertThat(got.get().value()).isEqualTo("value-1");
        assertThat(got.get().ttlMillis()).isLessThanOrEqualTo(0); // 永不过期归一

        assertThat(client.forwardDelete(SELF, "k1")).isTrue();
        assertThat(client.forwardDelete(SELF, "k1")).isFalse(); // 已删
        assertThat(capability.get("k1")).isNull();
    }

    @Test
    void forwardPutWithTtlReturnsRemainingTtl() {
        client.forwardPut(SELF, "k2", "ttl-value", 60_000);

        Optional<CacheGrpcClient.CacheValue> got = client.forwardGet(SELF, "k2");
        assertThat(got).isPresent();
        assertThat(got.get().ttlMillis()).isBetween(1L, 60_000L);
    }

    @Test
    void forwardPutIfAbsentReturnsAppliedAndKeepsExisting() {
        assertThat(client.forwardPutIfAbsent(SELF, "k-nx", "v1", 0)).isTrue();
        assertThat(client.forwardPutIfAbsent(SELF, "k-nx", "v2", 0)).isFalse();
        assertThat(capability.get("k-nx")).isEqualTo("v1");
    }

    @Test
    void forwardPutIfAbsentRejectsNonOwnerWithFencing() {
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        assertThatThrownBy(() -> client.forwardPutIfAbsent(SELF, "k1", "x", 0))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("stale-epoch");
        assertThat(capability.get("k1")).isNull();
    }

    @Test
    void forwardGetMissingKeyReturnsEmpty() {
        assertThat(client.forwardGet(SELF, "nope")).isEmpty();
    }

    @Test
    void executeCommandRoundTripForContainerOperations() {
        assertThat(client.forwardCommand(SELF, CacheCommandRequest.of("HSET", "h1", "f", "v")))
                .isEqualTo(1L);
        assertThat(client.forwardCommand(SELF, CacheCommandRequest.of("HGET", "h1", "f")))
                .isEqualTo("v");
        assertThat(client.forwardCommand(SELF, CacheCommandRequest.of("TYPE", "h1")))
                .isEqualTo("hash");
        assertThat(client.forwardCommand(SELF, CacheCommandRequest.of("STATS")))
                .isInstanceOf(java.util.Map.class);
    }

    @Test
    void executeCommandMapsSemanticErrors() {
        assertThatThrownBy(() -> client.forwardCommand(SELF,
                CacheCommandRequest.of("NOPE", "k1")))
                .isInstanceOf(CacheCommandException.class)
                .satisfies(e -> assertThat(((CacheCommandException) e).code())
                        .isEqualTo(CacheCommandException.UNKNOWN_COMMAND));
    }

    @Test
    void executeCommandRejectsNonOwnerWithNotOwnerMarker() {
        client.forwardPut(SELF, "k-cmd", "v", 0);
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        assertThatThrownBy(() -> client.forwardCommand(SELF,
                CacheCommandRequest.of("GET", "k-cmd")))
                .isInstanceOf(StaleEpochException.class)
                .hasMessageContaining("not-owner");
    }

    /** 读路由纠偏：属主已非本节点（cutover 在途）→ ABORTED not-owner（网关重取属主重试收敛）。 */
    @Test
    void forwardGetRejectsNonOwnerWithNotOwnerMarker() {
        client.forwardPut(SELF, "k-routed", "v", 0);
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        assertThatThrownBy(() -> client.forwardGet(SELF, "k-routed"))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("not-owner");
        // 数据仍在（仅拒绝错误路由的读，不删除）
        assertThat(capability.get("k-routed")).isEqualTo("v");
    }

    @Test
    void forwardPutRejectsNonOwnerWithFencing() {
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        assertThatThrownBy(() -> client.forwardPut(SELF, "k1", "x", 0))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("stale-epoch"); // ABORTED + stale-epoch 标记（网关还原 409）
        assertThat(capability.get("k1")).isNull();
    }

    @Test
    void forwardDeleteRejectsNonOwnerWithFencing() {
        capability.put("k1", "v", 0); // 本地直写（属主还是自己时）
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        assertThatThrownBy(() -> client.forwardDelete(SELF, "k1"))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("stale-epoch");
        assertThat(capability.get("k1")).isEqualTo("v"); // 未被删除
    }

    // ---- 管理面：列举 / 计数 ----

    @Test
    void listKeysStreamsLocalEntriesWithPrefixFilter() {
        capability.put("user:1", "a", 0);
        capability.put("user:2", "b", 60_000);
        capability.put("order:1", "c", 0);

        List<CacheCapability.CacheKeyEntry> all = client.listKeys(SELF, null, 100);
        assertThat(all).extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("order:1", "user:1", "user:2"); // 字典序

        List<CacheCapability.CacheKeyEntry> prefixed = client.listKeys(SELF, "user:", 100);
        assertThat(prefixed).extracting(CacheCapability.CacheKeyEntry::key)
                .containsExactly("user:1", "user:2");
        // TTL 归一：永不过期 <=0，有限 TTL 在区间内
        assertThat(prefixed.get(0).ttlMillis()).isLessThanOrEqualTo(0);
        assertThat(prefixed.get(1).ttlMillis()).isBetween(1L, 60_000L);
    }

    @Test
    void countKeysReturnsPrefixMatchCount() {
        capability.put("user:1", "a", 0);
        capability.put("user:2", "b", 0);
        capability.put("order:1", "c", 0);

        assertThat(client.countKeys(SELF, null)).isEqualTo(3L);
        assertThat(client.countKeys(SELF, "user:")).isEqualTo(2L);
        assertThat(client.countKeys(SELF, "zz")).isZero();
    }

    /** 能力未启动（节点被禁用、数据已迁出）：listKeys 返回空流、countKeys 返回 0——该节点贡献 0 条是正确语义（网关 fan-out 聚合用），不报 INTERNAL。 */
    @Test
    void listKeysAndCountKeysReturnEmptyWhenCapabilityStopped() {
        capability.put("user:1", "a", 0);
        capability.stop();
        assertThat(capability.isStarted()).isFalse();

        assertThat(client.listKeys(SELF, null, 100)).isEmpty();
        assertThat(client.countKeys(SELF, null)).isZero();
        assertThat(client.countKeys(SELF, "user:")).isZero();
    }

    /** 可变桩视图：全分区统一 owner。 */
    private static final class StubView implements PlacementView {
        volatile Optional<Owner> owner = Optional.empty();

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return owner;
        }
    }
}
