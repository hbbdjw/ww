package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.DataChunkP;
import com.ww.runtime.proto.ProgressReport;
import com.ww.runtime.proto.ProgressSummary;
import com.ww.runtime.proto.MigrationGrpc;
import com.ww.runtime.proto.TaskAck;
import com.ww.runtime.proto.TaskControlReq;
import com.ww.runtime.placement.Partitioner;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MigrationServer gRPC 端到端（进程内随机端口）：
 * FetchPartition 远端拉取与本地导出一致、TransferStream 校验/重发/进度持久化、
 * ControlTask 取消、ReportProgress 上报。
 */
class MigrationServerTest {

    private static final int PARTITION_COUNT = 4096;
    private static final NodeId NODE_A = NodeId.of("node-a");
    private static final NodeId NODE_B = NodeId.of("node-b");

    private final MockCapability capability = new MockCapability(
            NODE_A, emptyView(), key -> Partitioner.partition(key, PARTITION_COUNT));
    private final MockPartitionAdapter local = new MockPartitionAdapter(capability);
    private final InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();

    private GrpcServerHost host;
    private RemotePartitionAdapter remote;
    private ManagedChannel rawChannel;
    private MigrationGrpc.MigrationStub asyncStub;
    private MigrationGrpc.MigrationBlockingStub blockingStub;

    private static PlacementView emptyView() {
        return (cap, partition) -> Optional.empty();
    }

    private static int partitionOf(String seed) {
        return Partitioner.partition(seed, PARTITION_COUNT);
    }

    /** 归一收集指定分区的 count 条 KV 并预填。 */
    private List<Map.Entry<String, String>> fill(int partition, int count, String prefix) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(count);
        for (int i = 0; entries.size() < count; i++) {
            String key = prefix + "-" + partition + "-" + i;
            if (Partitioner.partition(key, PARTITION_COUNT) == partition) {
                entries.add(Map.entry(key, "value-" + key));
            }
        }
        capability.importPartition(partition, entries);
        return entries;
    }

    @BeforeEach
    void setUp() throws Exception {
        host = new GrpcServerHost();
        host.addService(new MigrationServer(local, store, new TestClock()));
        host.start(0);
        int port = host.getPort();
        remote = new RemotePartitionAdapter("localhost", port);
        rawChannel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        asyncStub = MigrationGrpc.newStub(rawChannel);
        blockingStub = MigrationGrpc.newBlockingStub(rawChannel);
    }

    @AfterEach
    void tearDown() throws Exception {
        remote.close();
        rawChannel.shutdownNow();
        host.stop();
        host.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void fetchPartitionPullsFullDataMatchingLocalExport() {
        int partition = partitionOf("fetch-full");
        fill(partition, 100, "fk");

        List<byte[]> viaRemote = remote.exportChunks("mock", partition, 0L);
        List<byte[]> viaLocal = local.exportChunks("mock", partition, 0L);

        assertThat(viaRemote).hasSameSizeAs(viaLocal);
        for (int i = 0; i < viaLocal.size(); i++) {
            assertThat(viaRemote.get(i)).containsExactly(viaLocal.get(i));
        }
        assertThat(remote.itemCount("mock", partition)).isEqualTo(100L);
        assertThat(remote.verifyCount("mock", partition)).isEqualTo(100L);
    }

    @Test
    void fetchPartitionHonorsFromCheckpoint() {
        int partition = partitionOf("fetch-skip");
        fill(partition, 60, "sk");

        List<byte[]> viaRemote = remote.exportChunks("mock", partition, 25L);
        List<byte[]> viaLocal = local.exportChunks("mock", partition, 25L);

        assertThat(viaRemote).hasSameSizeAs(viaLocal);
        for (int i = 0; i < viaLocal.size(); i++) {
            assertThat(viaRemote.get(i)).containsExactly(viaLocal.get(i));
        }
    }

    @Test
    void fetchPartitionOfEmptyPartitionReturnsNoData() {
        int empty = partitionOf("fetch-empty");
        assertThat(remote.exportChunks("mock", empty, 0L)).isEmpty();
        assertThat(remote.itemCount("mock", empty)).isZero();
    }

    @Test
    void transferStreamImportsChunksAndPersistsProgress() {
        // 同进程源/目标语义：同一分区号（真实迁移=同分区跨节点），先清空再经传输流恢复
        int partition = partitionOf("ts-src");
        List<Map.Entry<String, String>> entries = fill(partition, 40, "tk");
        List<byte[]> chunks = local.exportChunks("mock", partition, 0L);
        capability.clearPartition(partition);
        assertThat(capability.partitionCount(partition)).isZero();

        long taskId = store.create("mock", partition, NODE_A, NODE_B);
        int port = host.getPort();
        try (RemotePartitionAdapter bound = new RemotePartitionAdapter("localhost", port, taskId)) {
            assertThat(bound.importChunks("mock", partition, chunks)).isTrue();
        }

        assertThat(capability.partitionCount(partition)).isEqualTo(40L); // 分区已恢复
        assertThat(capability.exportPartition(partition))
                .containsExactlyInAnyOrderElementsOf(entries);

        MigrationTask task = store.get(taskId).orElseThrow();
        assertThat(task.checkpointOffset()).isEqualTo(40L); // fin 后进度已持久化
        assertThat(task.itemsDone()).isEqualTo(40L);
        assertThat(task.state()).isEqualTo(MigrationTaskState.PENDING); // 状态由引擎推进，不改
    }

    @Test
    void transferStreamRejectsBadChecksumThenAcceptsResend() throws Exception {
        int target = partitionOf("ts-bad");
        long taskId = store.create("mock", target, NODE_A, NODE_B);
        String key = keyOfPartition(target);
        byte[] data = MockPartitionAdapter.encodeEntry(key, "v1");

        CountDownLatch done = new CountDownLatch(1);
        List<ChunkAckP> acks = new CopyOnWriteArrayList<>();
        StreamObserver<DataChunkP> request = asyncStub.transferStream(new StreamObserver<>() {
            @Override
            public void onNext(ChunkAckP ack) {
                acks.add(ack);
            }

            @Override
            public void onError(Throwable t) {
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });

        // 1) 篡改 checksum → ok=false 且 ack_seq 指向该分片（要求重发）
        request.onNext(chunk(taskId, 0, false, data, "deadbeef"));
        // 2) 重发正确 checksum 的 fin 片 → ok=true
        request.onNext(chunk(taskId, 0, true, data, ChunkCodec.sha256Hex(data)));
        request.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(acks).hasSize(2);
        assertThat(acks.get(0).getOk()).isFalse();
        assertThat(acks.get(0).getAckSeq()).isEqualTo(0);
        assertThat(acks.get(1).getOk()).isTrue();
        assertThat(acks.get(1).getAckSeq()).isEqualTo(0);

        assertThat(capability.partitionCount(target)).isEqualTo(1L); // 仅重发片生效
        assertThat(capability.get(key)).isEqualTo("v1");
        assertThat(store.get(taskId).orElseThrow().checkpointOffset()).isEqualTo(1L);
    }

    @Test
    void transferStreamRejectsUnknownTask() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        List<ChunkAckP> acks = new CopyOnWriteArrayList<>();
        StreamObserver<DataChunkP> request = asyncStub.transferStream(new StreamObserver<>() {
            @Override
            public void onNext(ChunkAckP ack) {
                acks.add(ack);
            }

            @Override
            public void onError(Throwable t) {
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });
        request.onNext(chunk(999L, 0, true, new byte[]{'x'}, ChunkCodec.sha256Hex(new byte[]{'x'})));
        request.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(acks).hasSize(1);
        assertThat(acks.get(0).getOk()).isFalse();
        assertThat(acks.get(0).getMessage()).contains("unknown task");
    }

    @Test
    void controlTaskCancelsActiveTaskOnly() {
        long taskId = store.create("mock", partitionOf("ctl"), NODE_A, NODE_B);

        TaskAck ack = blockingStub.controlTask(TaskControlReq.newBuilder()
                .setTaskId(taskId).setAction("cancel").build());
        assertThat(ack.getOk()).isTrue();
        assertThat(store.get(taskId).orElseThrow().state()).isEqualTo(MigrationTaskState.CANCELLED);

        // 已终态：再取消 → ok=false
        TaskAck again = blockingStub.controlTask(TaskControlReq.newBuilder()
                .setTaskId(taskId).setAction("cancel").build());
        assertThat(again.getOk()).isFalse();

        // 未知 action → ok=false
        TaskAck unsupported = blockingStub.controlTask(TaskControlReq.newBuilder()
                .setTaskId(taskId).setAction("pause").build());
        assertThat(unsupported.getOk()).isFalse();
    }

    @Test
    void reportProgressSavesEachReport() throws Exception {
        long taskId = store.create("mock", partitionOf("prog"), NODE_A, NODE_B);

        CountDownLatch done = new CountDownLatch(1);
        List<ProgressSummary> summaries = new CopyOnWriteArrayList<>();
        StreamObserver<ProgressReport> request = asyncStub.reportProgress(new StreamObserver<>() {
            @Override
            public void onNext(ProgressSummary summary) {
                summaries.add(summary);
            }

            @Override
            public void onError(Throwable t) {
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });
        request.onNext(ProgressReport.newBuilder()
                .setTaskId(taskId).setState("COPYING")
                .setCheckpointOffset(10).setItemsDone(10).build());
        request.onNext(ProgressReport.newBuilder()
                .setTaskId(taskId).setState("COPYING")
                .setCheckpointOffset(20).setItemsDone(20).build());
        request.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(summaries).hasSize(1);
        assertThat(summaries.get(0).getOk()).isTrue();

        MigrationTask task = store.get(taskId).orElseThrow();
        assertThat(task.checkpointOffset()).isEqualTo(20L);
        assertThat(task.itemsDone()).isEqualTo(20L);
    }

    private String keyOfPartition(int partition) {
        for (int i = 0; ; i++) {
            String key = "key-of-" + partition + "-" + i;
            if (Partitioner.partition(key, PARTITION_COUNT) == partition) {
                return key;
            }
        }
    }

    private static DataChunkP chunk(long taskId, int seq, boolean fin, byte[] data, String checksum) {
        return DataChunkP.newBuilder()
                .setTaskId(taskId)
                .setSeq(seq)
                .setFin(fin)
                .setData(ByteString.copyFrom(data))
                .setChecksum(checksum)
                .build();
    }
}
