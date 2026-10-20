package com.ww.runtime.data.storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.data.storage.engine.ByteBufferPool;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
import com.ww.runtime.data.storage.engine.StorageException;
import com.ww.runtime.data.storage.engine.StorageProperties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 阶段 6 验收（规模化与性能，§3.16/§2.3）：
 * <ul>
 *   <li>T14：对账走分页摘要比对（RPC 从每对象 HEAD 降为每桶一页），摘要不支持时降级 HEAD；</li>
 *   <li>T16：直接内存池在借总量上限（背压拒绝而非无界堆外）；</li>
 *   <li>T18：GET 元数据写穿缓存复用（免每次 GET 盘读 meta）；</li>
 *   <li>T22：分区列举快照按索引代数复用（读免重扫、写自动失效）。</li>
 * </ul>
 */
@DisplayName("阶段 6 验收：T14 摘要对账 / T16 池上限 / T18 写穿 / T22 快照复用")
class Phase6StorageFixesTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId REPLICA = NodeId.of("node-r");

    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(bucketSlashKey.indexOf('/') + 1) - 'a';
    }

    @TempDir
    Path tempDir;

    /** 本机恒为 PRIMARY；holders=[self, REPLICA]（触发副本对账）。 */
    private static final class PrimaryView implements PlacementView {
        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return Optional.of(new Owner(SELF, 1L));
        }

        @Override
        public List<Owner> holders(CapabilityType cap, PartitionId partition) {
            return List.of(new Owner(SELF, 1L), new Owner(REPLICA, 1L));
        }
    }

    /** 支持摘要列举的桩客户端：remoteList 服务化（marker 排他、分页），remoteHead 记录调用。 */
    private static final class SummaryClient implements StorageReplicationClient {
        final List<String> replicated = new CopyOnWriteArrayList<>();
        final List<String> headCalls = new CopyOnWriteArrayList<>();
        /** 远端已持有对象（完整 meta 保真，供 T6 复核比对 version+etag） */
        final java.util.Map<String, ObjectMetadata> remote =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean replicateObject(NodeId node, String bucket, ObjectMetadata meta, InputStream data) {
            replicated.add(meta.getKey());
            remote.put(meta.getKey(), meta);
            return true;
        }

        @Override
        public boolean replicateDelete(NodeId node, String bucket, String key) {
            remote.remove(key);
            return true;
        }

        @Override
        public Optional<ObjectMetadata> remoteHead(NodeId node, String bucket, String key) {
            headCalls.add(key);
            return Optional.ofNullable(remote.get(key));
        }

        @Override
        public List<ObjectSummary> remoteList(NodeId node, String bucket, String marker, int maxKeys) {
            return remote.values().stream()
                    .filter(m -> marker == null || marker.isEmpty() || m.getKey().compareTo(marker) > 0)
                    .sorted(Comparator.comparing(ObjectMetadata::getKey))
                    .limit(maxKeys)
                    .map(m -> new ObjectSummary(m.getKey(), m.getVersion(), m.getEtag()))
                    .toList();
        }

        @Override
        public void close() {
        }
    }

    private StorageCapability newCapability(Path dir) throws Exception {
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(dir.toString()));
        props.setRootDir(dir.toString());
        props.setTempGcIntervalMillis(0);
        StorageCapability capability = new StorageCapability(SELF, new PrimaryView(),
                Phase6StorageFixesTest::partitioner, props, 8);
        capability.start();
        return capability;
    }

    private static ObjectMetadata put(StorageCapability cap, String bucket, String key, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return cap.putObject(bucket, key, new ByteArrayInputStream(bytes), bytes.length, "text/plain", null);
    }

    // ==================== T14 ====================

    @Test
    @DisplayName("T14：对账以摘要页比对——落后/缺失对象补复制，全程零逐对象 HEAD")
    void reconcileComparesSummariesWithoutPerObjectHead() throws Exception {
        StorageCapability cap = newCapability(tempDir.resolve("t14"));
        put(cap, "bkt", "a1", "one");   // 副本已持有同版本
        put(cap, "bkt", "a2", "two");   // 副本缺失 → 应补
        SummaryClient client = new SummaryClient();
        client.remote.put("a1", cap.head("bkt", "a1")); // 副本已持有 a1 同版本

        StorageReplicator replicator = new StorageReplicator(SELF, new PrimaryView(),
                Phase6StorageFixesTest::partitioner, cap, client, null, 10L, 60_000L, 100);
        replicator.reconcile();

        assertThat(client.replicated).as("仅缺失对象被补").containsExactly("a2");
        // T14：对账比较阶段零逐对象 HEAD——remoteHead 仅出现 replicateOne 的 T6 复核一次
        assertThat(client.headCalls).as("对账经摘要完成，不得逐对象 HEAD 比较")
                .containsExactly("a2");

        // 幂等：补齐后再对账零修复
        client.replicated.clear();
        replicator.reconcile();
        assertThat(client.replicated).isEmpty();
        cap.stop();
    }

    // ==================== T16 ====================

    @Test
    @DisplayName("T16：直接内存池在借达到上限后限时拒绝（背压），归还后恢复")
    void poolCapsOutstandingBorrowsWithBackpressure() {
        ByteBufferPool pool = new ByteBufferPool(1024, 2, 20); // 20ms 等待窗（测试注入）
        ByteBufferPool.LeasedBuffer a = pool.borrow();
        ByteBufferPool.LeasedBuffer b = pool.borrow();
        assertThatThrownBy(pool::borrow)
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("上限");
        assertThat(pool.getBorrowTimeouts()).isEqualTo(1);
        a.close();
        try (ByteBufferPool.LeasedBuffer c = pool.borrow()) {
            assertThat(c.buffer()).isNotNull();
        }
        b.close();
    }

    // ==================== T18 ====================

    @Test
    @DisplayName("T18：GET 元数据写穿缓存——命中缓存的 meta 对象句柄复用，数据缺失时失效回退")
    void getIsServedFromWriteThroughMetaCache() throws Exception {
        StorageCapability cap = newCapability(tempDir.resolve("t18"));
        put(cap, "bkt", "a1", "payload");

        ObjectStore.StoredObject first = cap.getObject("bkt", "a1"); // 盘读 + 回填
        ObjectStore.StoredObject second = cap.getObject("bkt", "a1"); // 缓存命中（同一 meta 实例）
        assertThat(second).isNotNull();
        assertThat(second.meta).as("缓存写穿：meta 实例复用").isSameAs(first.meta);

        // 引擎层：带缓存 meta 的读在数据文件缺失时返回 null（供缓存失效路径）
        ObjectStore store = cap.storeForTest();
        assertThat(store.get("bkt", "missing", first.meta)).isNull();
        cap.stop();
    }

    // ==================== T22 ====================

    @Test
    @DisplayName("T22：桶全量列举按索引代数复用——读免重扫，写推进代数自动失效")
    void listAllSnapshotsAreReusedPerGeneration() throws Exception {
        StorageCapability cap = newCapability(tempDir.resolve("t22"));
        put(cap, "bkt", "a1", "one");
        put(cap, "bkt", "a2", "two");

        List<ObjectMetadata> first = cap.listAllObjects("bkt");
        assertThat(cap.listAllObjects("bkt")).as("无写代数不变 → 复用同一快照实例")
                .isSameAs(first);

        put(cap, "bkt", "a3", "three"); // 写推进代数
        List<ObjectMetadata> second = cap.listAllObjects("bkt");
        assertThat(second).hasSize(3).isNotSameAs(first);
        cap.stop();
    }
}
