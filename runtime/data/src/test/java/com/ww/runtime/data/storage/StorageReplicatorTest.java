package com.ww.runtime.data.storage;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.StorageProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * StorageReplicator 单测（mock 客户端）：成功复制 / 失败入 backlog 重放 /
 * 删除墓碑 / 对账补齐 / 单副本短路 / backlog 有界淘汰。
 */
class StorageReplicatorTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId REPLICA = NodeId.of("node-r");

    /** (bucket/key) 首字母决定分区：b*→1。 */
    private static int partitioner(String bucketSlashKey) {
        return bucketSlashKey.charAt(0) - 'a';
    }

    @TempDir
    Path tempDir;

    private StubView view;
    private StorageCapability capability;
    private StubClient client;
    private StorageReplicator replicator;

    @BeforeEach
    void setUp() throws Exception {
        view = new StubView();
        StorageProperties props = new StorageProperties();
        props.setDrives(List.of(tempDir.toString()));
        props.setRootDir(tempDir.toString());
        capability = new StorageCapability(SELF, view, StorageReplicatorTest::partitioner,
                props, 8);
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));

        client = new StubClient();
        replicator = new StorageReplicator(SELF, view, StorageReplicatorTest::partitioner,
                capability, client, null, 10L, 60_000L, 100);
    }

    // ---- 成功路径 ----

    @Test
    void afterPutReplicatesToReplicaHolders() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "hello");

        replicator.afterPut("b1", meta);
        await(() -> !client.replicated.isEmpty());

        assertThat(client.replicated).hasSize(1);
        assertThat(client.replicated.getFirst().node()).isEqualTo(REPLICA);
        assertThat(client.replicated.getFirst().meta().getVersion()).isEqualTo(1L);
        assertThat(client.replicated.getFirst().data()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void singleReplicationFactorSkipsReplication() throws Exception {
        view.replicas = List.of(); // holders 仅 PRIMARY
        ObjectMetadata meta = putObj("b1", "k1", "solo");

        replicator.afterPut("b1", meta);
        Thread.sleep(100); // 无副本可推：静默短路

        assertThat(client.replicated).isEmpty();
        assertThat(replicator.backlogSize()).isZero();
    }

    @Test
    void afterDeleteReplicatesTombstone() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "bye");
        replicator.afterPut("b1", meta);
        await(() -> !client.replicated.isEmpty());
        capability.deleteObject("b1", "k1");

        replicator.afterDelete("b1", "k1");
        await(() -> !client.deletes.isEmpty());

        assertThat(client.deletes).containsExactly(REPLICA);
    }

    // ---- 失败入 backlog 与重放 ----

    @Test
    void failedPutGoesToBacklogAndReplayPushesIt() throws Exception {
        view.replicas = List.of(REPLICA);
        client.failReplicate.set(1); // 首次失败
        ObjectMetadata meta = putObj("b1", "k1", "retry-me");

        replicator.afterPut("b1", meta);
        await(() -> replicator.backlogSize() == 1);

        Thread.sleep(50); // 退避（基值 10ms）到期
        replicator.replayBacklog();

        assertThat(replicator.backlogSize()).isZero();
        assertThat(client.replicated).hasSize(1); // 首次失败未落桩，重放成功一次
        assertThat(client.replicated.getFirst().data()).isEqualTo("retry-me".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void replaySendsTombstoneWhenLocalObjectGone() throws Exception {
        view.replicas = List.of(REPLICA);
        client.failReplicate.set(1);
        ObjectMetadata meta = putObj("b1", "k1", "deleted-later");

        replicator.afterPut("b1", meta);
        await(() -> replicator.backlogSize() == 1);
        capability.deleteObject("b1", "k1"); // 本地已删 → 重放改发墓碑

        Thread.sleep(50);
        replicator.replayBacklog();

        assertThat(replicator.backlogSize()).isZero();
        assertThat(client.deletes).contains(REPLICA); // 墓碑语义
    }

    @Test
    void replayDefersWhenStillFailing() throws Exception {
        view.replicas = List.of(REPLICA);
        client.failReplicate.set(99); // 持续失败
        ObjectMetadata meta = putObj("b1", "k1", "keep-failing");

        replicator.afterPut("b1", meta);
        await(() -> replicator.backlogSize() == 1);

        replicator.replayBacklog(); // 立即重放：仍在退避期外（首次入队即到期基值 10ms 后）
        Thread.sleep(20);
        replicator.replayBacklog();

        assertThat(replicator.backlogSize()).isEqualTo(1); // 退避保留
    }

    @Test
    void backlogEvictsOldestWhenFull() throws Exception {
        StorageReplicator tiny = new StorageReplicator(SELF, view, StorageReplicatorTest::partitioner,
                capability, client, null, 10L, 60_000L, 2);
        view.replicas = List.of(REPLICA);
        client.failReplicate.set(999);

        for (String key : List.of("k1", "k2", "k3")) {
            ObjectMetadata meta = putObj("b1", key, "v");
            tiny.afterPut("b1", meta);
        }
        await(() -> tiny.backlogSize() == 2);

        assertThat(tiny.backlogSize()).isEqualTo(2); // 淘汰最旧保最新
    }

    // ---- 对账 ----

    @Test
    void reconcileRepairsLaggingReplica() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "primary-copy");
        // 副本从未收到（模拟复制丢失）：无 afterPut，直接对账
        // T4/T6：远端缺失 → HEAD 为 empty；补复制后按 version+etag 复核

        replicator.reconcile();

        assertThat(client.heads).isNotEmpty();
        assertThat(client.replicated).hasSize(1);
        assertThat(client.replicated.getFirst().data())
                .isEqualTo("primary-copy".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void reconcileSkipsUpToDateReplica() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "in-sync");
        // 远端已持有同 version+etag 副本（T6 复核口径）
        client.remoteStore.put("b1/k1", meta);

        replicator.reconcile();

        assertThat(client.heads).isNotEmpty();
        assertThat(client.replicated).isEmpty(); // 无需补齐
    }

    @Test
    void reconcileSkipsNonPrimaryPartitions() throws Exception {
        view.owner = Optional.of(new PlacementView.Owner(NodeId.of("other-primary"), 3L));
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObjViaReplica("b1", "k1", "not-mine");

        replicator.reconcile();

        assertThat(client.heads).isEmpty(); // 非本机主分区不对账
        assertThat(client.replicated).isEmpty();
    }

    // ---- P0-1 半同步复制 ----

    @Test
    void replicateSyncAcksWhenReplicaConfirms() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "semi-sync-ok");

        assertThat(replicator.replicateSync("b1", meta, java.time.Duration.ofSeconds(2))).isTrue();
        assertThat(client.replicated).hasSize(1);
        assertThat(replicator.backlogSize()).isZero();
    }

    @Test
    void replicateSyncFalseOnTimeoutAndEnqueuesBacklog() throws Exception {
        view.replicas = List.of(REPLICA);
        client.failReplicate.set(999);
        ObjectMetadata meta = putObj("b1", "k1", "semi-sync-timeout");

        assertThat(replicator.replicateSync("b1", meta, java.time.Duration.ofMillis(300))).isFalse();
        // 失败复制意图不丢：入 backlog 异步补偿
        await(() -> replicator.backlogSize() == 1);
    }

    @Test
    void replicateSyncVacuousTrueWhenNoReplicaRows() throws Exception {
        view.replicas = List.of(); // holders 仅 PRIMARY（RF=1）：空真
        ObjectMetadata meta = putObj("b1", "k1", "solo");

        assertThat(replicator.replicateSync("b1", meta, java.time.Duration.ofMillis(200))).isTrue();
        assertThat(client.replicated).isEmpty();
    }

    @Test
    void replicateSyncFalseWhenReplicaNotServing() throws Exception {
        view.replicas = List.of(REPLICA); // 放置有副本行……
        StorageReplicator withDirectory = new StorageReplicator(SELF, view,
                StorageReplicatorTest::partitioner, capability, client,
                new StubDirectory(List.of()), 10L, 60_000L, 100); // ……但无一存活
        ObjectMetadata meta = putObj("b1", "k1", "replica-down");

        assertThat(withDirectory.replicateSync("b1", meta, java.time.Duration.ofMillis(200))).isFalse();
        assertThat(client.replicated).isEmpty();
    }

    @Test
    void replicateDeleteSyncAcksWhenReplicaConfirms() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "to-delete");
        capability.deleteObject("b1", "k1");

        assertThat(replicator.replicateDeleteSync("b1", "k1", java.time.Duration.ofSeconds(2))).isTrue();
        assertThat(client.deletes).containsExactly(REPLICA);
    }

    @Test
    void replicateDeleteSyncFalseOnTimeout() throws Exception {
        view.replicas = List.of(REPLICA);
        ObjectMetadata meta = putObj("b1", "k1", "to-delete-2");
        capability.deleteObject("b1", "k1");
        client.failReplicate.set(999); // replicateDelete 读同一失败闸门

        assertThat(replicator.replicateDeleteSync("b1", "k1", java.time.Duration.ofMillis(300))).isFalse();
        await(() -> replicator.backlogSize() == 1);
    }

    // ---- 工具 ----

    private ObjectMetadata putObj(String bucket, String key, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return capability.putObject(bucket, key, new ByteArrayInputStream(bytes),
                bytes.length, "text/plain", null);
    }

    /** 非属主写入路径（replicaPut 无 fencing），用于构造“本地持有但非主”的对象。 */
    private ObjectMetadata putObjViaReplica(String bucket, String key, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata meta = new ObjectMetadata(key, bytes.length, "text/plain",
                null, System.currentTimeMillis(), 1L);
        return capability.replicaPut(bucket, key, new ByteArrayInputStream(bytes), meta);
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 5s");
            }
            Thread.sleep(20);
        }
    }

    /** 可变桩视图：全分区统一 owner + holders=[PRIMARY, replicas...]。 */
    private static final class StubView implements PlacementView {
        volatile Optional<Owner> owner = Optional.empty();
        volatile List<NodeId> replicas = List.of();

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return owner;
        }

        @Override
        public List<Owner> holders(CapabilityType cap, PartitionId partition) {
            Owner primary = owner.orElse(new Owner(SELF, 0L));
            return new java.util.ArrayList<>() {{
                add(primary);
                for (NodeId r : replicas) {
                    add(new Owner(r, primary.epoch()));
                }
            }};
        }
    }

    /** 桩对等目录：servingPeers 可控（模拟「放置有副本行但节点不存活」）。 */
    private record StubDirectory(List<NodeId> serving) implements StoragePeerDirectory {

        @Override
        public Optional<PeerEndpoint> endpoint(NodeId node) {
            return Optional.empty();
        }

        @Override
        public List<NodeId> alivePeers() {
            return serving;
        }

        @Override
        public List<NodeId> servingPeers() {
            return serving;
        }
    }

    /** 记录型桩客户端：可注入失败次数；远端状态按 (bucket/key) 记录（T6 复核按 version+etag）。 */
    private static final class StubClient implements StorageReplicationClient {
        record Replicated(NodeId node, ObjectMetadata meta, byte[] data) {}

        final List<Replicated> replicated = new CopyOnWriteArrayList<>();
        final List<NodeId> deletes = new CopyOnWriteArrayList<>();
        final List<NodeId> heads = new CopyOnWriteArrayList<>();
        final AtomicInteger failReplicate = new AtomicInteger();
        /** T6：远端对象状态（复制成功后记录；remoteHead 按此应答） */
        final java.util.concurrent.ConcurrentHashMap<String, ObjectMetadata> remoteStore =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean replicateObject(NodeId node, String bucket, ObjectMetadata meta, InputStream data) {
            if (failReplicate.getAndDecrement() > 0) {
                return false;
            }
            try {
                replicated.add(new Replicated(node, meta, data.readAllBytes()));
                remoteStore.put(bucket + "/" + meta.getKey(), meta);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public boolean replicateDelete(NodeId node, String bucket, String key) {
            deletes.add(node);
            remoteStore.remove(bucket + "/" + key);
            return failReplicate.get() <= 0;
        }

        @Override
        public Optional<ObjectMetadata> remoteHead(NodeId node, String bucket, String key) {
            heads.add(node);
            return Optional.ofNullable(remoteStore.get(bucket + "/" + key));
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
