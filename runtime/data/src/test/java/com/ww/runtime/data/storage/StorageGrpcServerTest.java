package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.StorageProperties;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.ObjectChunkP;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * StorageGrpcServer gRPC 端到端（进程内随机端口 + StorageGrpcClient 回环）：
 * ForwardPut/Get/Head/Delete 全语义（含 userMeta/多分片）、Replicate 无 fencing 落盘、
 * ListBucket 全量翻页、SyncBucket 建删桶。
 */
class StorageGrpcServerTest {

    private static final NodeId SELF = NodeId.of("node-1");

    /** (bucket/key) 首字母决定分区：b*→1。 */
    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(0) - 'a';
    }

    @TempDir
    Path tempDir;

    private StubView view;
    private StorageProperties props;
    private StorageCapability capability;
    private GrpcServerHost host;
    private StorageGrpcServer server;
    private StorageGrpcClient client;

    @BeforeEach
    void setUp() throws Exception {
        view = new StubView();
        props = new StorageProperties();
        props.setDrives(List.of(tempDir.toString()));
        props.setRootDir(tempDir.toString());
        capability = new StorageCapability(SELF, view, StorageGrpcServerTest::partitioner,
                props, 8); // chunkSize=8 触发多分片
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));

        host = new GrpcServerHost();
        server = new StorageGrpcServer(capability, 8);
        host.addService(server);
        host.start(0);

        client = new StorageGrpcClient(new StoragePeerDirectory() {
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
    }

    // ---- 网关转发 ----

    @Test
    void forwardPutGetHeadDeleteRoundTrip() throws Exception {
        byte[] payload = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8); // 20B → 3 片
        ObjectMetadata meta = client.forwardPut(SELF, "b1", "k1", "text/plain",
                Map.of("x-amz-meta-team", "runtime"),
                new ByteArrayInputStream(payload), payload.length);

        assertThat(meta.getVersion()).isEqualTo(1L);
        assertThat(meta.getSize()).isEqualTo(payload.length);

        // 本地引擎视角（属主节点落盘）
        ObjectMetadata local = capability.head("b1", "k1");
        assertThat(local).isNotNull();
        assertThat(local.getUserMetadata()).containsEntry("x-amz-meta-team", "runtime");

        // ForwardGet：分片回传重组
        assertThat(reassemble(client.forwardGet(SELF, "b1", "k1"))).isEqualTo(payload);

        // ForwardHead
        Optional<ObjectMetadata> head = client.forwardHead(SELF, "b1", "k1");
        assertThat(head).isPresent();
        assertThat(head.get().getSize()).isEqualTo(payload.length);

        // ForwardDelete
        assertThat(client.forwardDelete(SELF, "b1", "k1")).isTrue();
        assertThat(client.forwardDelete(SELF, "b1", "k1")).isFalse(); // 已删
        assertThat(capability.head("b1", "k1")).isNull();
    }

    @Test
    void forwardPutEmptyObjectRoundTrip() throws Exception {
        ObjectMetadata meta = client.forwardPut(SELF, "b1", "empty", "text/plain", null,
                new ByteArrayInputStream(new byte[0]), 0);
        assertThat(meta.getSize()).isZero();

        assertThat(reassemble(client.forwardGet(SELF, "b1", "empty"))).isEmpty();
        assertThat(client.forwardHead(SELF, "b1", "empty")).isPresent();
    }

    @Test
    void forwardGetMissingObjectFailsWithNotFound() {
        Iterator<ObjectChunkP> stream = client.forwardGet(SELF, "b1", "nope");
        assertThatThrownBy(stream::hasNext)
                .isInstanceOf(StatusRuntimeException.class);
    }

    @Test
    void forwardGetRangeReturnsWindow() throws Exception {
        byte[] payload = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8); // 20B → 3 片
        client.forwardPut(SELF, "b1", "k1", "text/plain", null,
                new ByteArrayInputStream(payload), payload.length);

        Iterator<ObjectChunkP> stream = client.forwardGet(SELF, "b1", "k1", 5L, 10L);
        assertThat(stream.hasNext()).isTrue();
        ObjectChunkP first = stream.next();
        assertThat(first.getOffset()).isEqualTo(5L); // 首片即窗口起点（对象内绝对偏移）
        ByteArrayOutputStream window = new ByteArrayOutputStream();
        window.write(first.getData().toByteArray());
        while (stream.hasNext()) {
            window.write(stream.next().getData().toByteArray());
        }
        assertThat(window.toByteArray()).isEqualTo("56789abcde".getBytes(StandardCharsets.UTF_8)); // payload[5,15)
    }

    @Test
    void forwardGetZeroOffsetLengthReturnsWhole() throws Exception {
        byte[] payload = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8);
        client.forwardPut(SELF, "b1", "k1", "text/plain", null,
                new ByteArrayInputStream(payload), payload.length);

        assertThat(reassemble(client.forwardGet(SELF, "b1", "k1", 0L, 0L))).isEqualTo(payload);
    }

    @Test
    void forwardGetOffsetBeyondSizeReturnsEmpty() throws Exception {
        byte[] payload = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8);
        client.forwardPut(SELF, "b1", "k1", "text/plain", null,
                new ByteArrayInputStream(payload), payload.length);

        assertThat(reassemble(client.forwardGet(SELF, "b1", "k1", 100L, 10L))).isEmpty();
    }

    @Test
    void forwardPutRejectsNonOwnerWithFencing() {
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other"), 3L));
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> client.forwardPut(SELF, "b1", "k1", "text/plain", null,
                new ByteArrayInputStream(payload), payload.length))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("stale-epoch"); // ABORTED + stale-epoch 标记（网关还原 409）
        assertThat(capability.head("b1", "k1")).isNull();
    }

    // ---- 副本接收 ----

    @Test
    void replicateStoresObjectWithoutFencing() throws Exception {
        // 本节点对分区 1 非属主（模拟副本节点接收主节点复制）
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("primary-node"), 3L));
        byte[] payload = "replicated-content".getBytes(StandardCharsets.UTF_8);
        ObjectMetadata primaryMeta = new ObjectMetadata("k1", payload.length, "text/plain",
                "etag-from-primary", 42L, 9L);

        boolean ok = clientReplicate(primaryMeta, payload);
        assertThat(ok).isTrue();

        ObjectMetadata local = capability.head("b1", "k1");
        assertThat(local).isNotNull();
        assertThat(local.getVersion()).isEqualTo(9L); // 主预计算 version 原样保留
        assertThat(local.getEtag()).isEqualTo("etag-from-primary");

        // ReplicateDelete
        assertThat(client.replicateDelete(SELF, "b1", "k1")).isTrue();
        assertThat(capability.head("b1", "k1")).isNull();
    }

    @Test
    void replicatePreservesUserMetadata() throws Exception {
        // 本节点为副本，接收主节点复制
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("primary-node"), 3L));
        byte[] payload = "meta-preserved".getBytes(StandardCharsets.UTF_8);
        ObjectMetadata primaryMeta = new ObjectMetadata("k2", payload.length, "text/plain", "etag-m", 42L, 5L);
        primaryMeta.setUserMetadata(Map.of("x-amz-meta-owner", "ww", "x-amz-meta-env", "prod"));

        assertThat(clientReplicate(primaryMeta, payload)).isTrue();

        ObjectMetadata local = capability.head("b1", "k2");
        assertThat(local).isNotNull();
        assertThat(local.getUserMetadata())
                .containsEntry("x-amz-meta-owner", "ww")
                .containsEntry("x-amz-meta-env", "prod");
    }

    @Test
    @Timeout(10)
    void replicateIdempotentSkipDoesNotPinCallbackThread() throws Exception {
        // 本节点为副本（属主在别处），接收复制
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("primary-node"), 3L));
        // 120B → chunkSize=8 → 15 片，远超聚合队列容量(8)：
        // 幂等跳过时落盘线程提前退出且不消费流，修复前多余分片会钉死回调线程
        byte[] payload = new byte[120];
        java.util.Arrays.fill(payload, (byte) 'r');

        ObjectMetadata v2 = new ObjectMetadata("k1", payload.length, "text/plain", "etag-v2", 42L, 2L);
        assertThat(clientReplicate(v2, payload)).isTrue();
        assertThat(capability.head("b1", "k1").getVersion()).isEqualTo(2L);

        // 重放旧版本：putReplica 幂等跳过（不读流即应答）
        ObjectMetadata v1 = new ObjectMetadata("k1", payload.length, "text/plain", "etag-v1", 41L, 1L);
        assertThat(clientReplicate(v1, payload)).isTrue();

        // 版本不回退，且后续无关 RPC 正常（节点未被钉死的回调线程拖垮）
        assertThat(capability.head("b1", "k1").getVersion()).isEqualTo(2L);
        assertThat(client.forwardHead(SELF, "b1", "k1")).isPresent();

        // T2 指标访问器可用（具体计数取决于传输层是否在应答后仍投递分片，不做定量断言）
        assertThat(server.chunksDiscardedAfterAbort()).isGreaterThanOrEqualTo(0L);
        assertThat(server.streamsAbortedOnPushTimeout()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @Timeout(10)
    void forwardPutOversizeEarlyRejectDoesNotPinCallbackThread() {
        // 超限在落盘线程启动后即刻拒绝（前置大小校验，未消费数据流）
        props.setMaxObjectSize(4);
        byte[] payload = new byte[40]; // 5 片 > 队列容量
        java.util.Arrays.fill(payload, (byte) 'x');

        assertThatThrownBy(() -> client.forwardPut(SELF, "b1", "big", "application/octet-stream",
                null, new ByteArrayInputStream(payload), payload.length))
                .isInstanceOf(StatusRuntimeException.class);

        // 对象未落盘，且后续无关 RPC 正常
        assertThat(capability.head("b1", "big")).isNull();
        assertThat(client.forwardHead(SELF, "b1", "big")).isEmpty();
    }

    // ---- fan-out 列举 / 桶同步 ----

    @Test
    void listBucketReturnsSingleBoundedPage() throws Exception {
        putLocal("b1", "key-a", "v1");
        putLocal("b1", "key-b", "v2");
        putLocal("b1", "key-c", "v3");

        // 单页有界回传：服务端不做内部翻页（网关按「每节点一页」合并截断）
        List<ObjectMetadata> page1 = client.listBucket(SELF, "b1", null, null, 1);
        assertThat(page1).extracting(ObjectMetadata::getKey).containsExactly("key-a");

        // marker 排他推进取下一页
        List<ObjectMetadata> page2 = client.listBucket(SELF, "b1", null, "key-a", 100);
        assertThat(page2).extracting(ObjectMetadata::getKey).containsExactly("key-b", "key-c");

        List<ObjectMetadata> prefixed = client.listBucket(SELF, "b1", "key-a", null, 100);
        assertThat(prefixed).extracting(ObjectMetadata::getKey).containsExactly("key-a");
    }

    @Test
    void syncBucketCreatesAndDeletesLocally() {
        assertThat(client.syncBucket(SELF, "b1", "create", false)).isTrue();
        assertThat(capability.registeredBuckets()).contains("b1");

        assertThat(client.syncBucket(SELF, "b1", "delete", true)).isTrue();
        assertThat(capability.registeredBuckets()).doesNotContain("b1");
    }

    // ---- 对象计数（管理面每节点统计） ----

    @Test
    void countObjectsStrictReturnsAllBucketAndPerBucketCounts() throws Exception {
        putLocal("b1", "k1", "v1");
        putLocal("b1", "k2", "v2");
        putLocal("c1", "k9", "v3");

        assertThat(client.countObjectsStrict(SELF, null)).isEqualTo(3L); // 空 = 全部桶
        assertThat(client.countObjectsStrict(SELF, "")).isEqualTo(3L);
        assertThat(client.countObjectsStrict(SELF, "b1")).isEqualTo(2L);
        assertThat(client.countObjectsStrict(SELF, "c1")).isEqualTo(1L);
    }

    @Test
    void countObjectsStrictFailsWhenCapabilityNotStarted() {
        capability.stop();
        // 严格语义：能力未启动上抛错误（管理面标记不可达），而非降级 0
        assertThatThrownBy(() -> client.countObjectsStrict(SELF, null))
                .isInstanceOf(StatusRuntimeException.class);
    }

    // ---- 工具 ----

    private void putLocal(String bucket, String key, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        capability.putObject(bucket, key, new ByteArrayInputStream(bytes),
                bytes.length, "text/plain", null);
    }

    private boolean clientReplicate(ObjectMetadata meta, byte[] payload) {
        return client.replicateObject(SELF, "b1", meta, new ByteArrayInputStream(payload));
    }

    private static byte[] reassemble(Iterator<ObjectChunkP> stream) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long expectedOffset = 0L;
        while (stream.hasNext()) {
            ObjectChunkP chunk = stream.next();
            assertThat(chunk.getOffset()).isEqualTo(expectedOffset); // 顺序无缺口
            byte[] data = chunk.getData().toByteArray();
            out.writeBytes(data);
            expectedOffset += data.length;
        }
        return out.toByteArray();
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
