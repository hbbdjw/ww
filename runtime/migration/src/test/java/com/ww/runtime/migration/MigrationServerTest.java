package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.ClearReqP;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        List<byte[]> viaRemote = drain(remote.exportChunks("mock", partition, 0L));
        List<byte[]> viaLocal = drain(local.exportChunks("mock", partition, 0L));

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

        List<byte[]> viaRemote = drain(remote.exportChunks("mock", partition, 25L));
        List<byte[]> viaLocal = drain(local.exportChunks("mock", partition, 25L));

        assertThat(viaRemote).hasSameSizeAs(viaLocal);
        for (int i = 0; i < viaLocal.size(); i++) {
            assertThat(viaRemote.get(i)).containsExactly(viaLocal.get(i));
        }
    }

    @Test
    void fetchPartitionOfEmptyPartitionReturnsNoData() {
        int empty = partitionOf("fetch-empty");
        assertThat(drain(remote.exportChunks("mock", empty, 0L))).isEmpty();
        assertThat(remote.itemCount("mock", empty)).isZero();
    }

    @Test
    @DisplayName("端点能力未就绪：fetch/count 以 FAILED_PRECONDITION 语义状态码应答（启动收敛窗口）")
    void fetchPartitionReportsCapabilityNotStartedAsFailedPrecondition() throws Exception {
        PartitionDataAdapter notStarted = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                throw new com.ww.runtime.common.RuntimeFrameworkException("storage capability not started");
            }

            @Override
            public java.util.Iterator<byte[]> exportChunks(String capability, int partition,
                                                           long fromCheckpoint) {
                throw new com.ww.runtime.common.RuntimeFrameworkException("storage capability not started");
            }

            @Override
            public boolean importChunks(String capability, int partition, java.util.List<byte[]> chunks) {
                return false;
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }
        };
        GrpcServerHost h2 = new GrpcServerHost();
        h2.addService(new MigrationServer(notStarted, store, new TestClock()));
        h2.start(0);
        ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", h2.getPort())
                .usePlaintext().build();
        try {
            MigrationGrpc.MigrationBlockingStub stub = MigrationGrpc.newBlockingStub(ch);
            assertThatThrownBy(() -> stub.fetchPartition(com.ww.runtime.proto.FetchReq.newBuilder()
                    .setCapability("mock").setPartitionId(1).build()).hasNext())
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .satisfies(t -> assertThat(((io.grpc.StatusRuntimeException) t).getStatus().getCode())
                            .isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION));
            assertThatThrownBy(() -> stub.countPartition(com.ww.runtime.proto.CountReqP.newBuilder()
                    .setCapability("mock").setPartitionId(1).build()))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .satisfies(t -> assertThat(((io.grpc.StatusRuntimeException) t).getStatus().getCode())
                            .isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION));
        } finally {
            ch.shutdownNow();
            h2.stop();
        }
    }

    @Test
    void transferStreamImportsChunksAndPersistsProgress() {
        // 同进程源/目标语义：同一分区号（真实迁移=同分区跨节点），先清空再经传输流恢复
        int partition = partitionOf("ts-src");
        List<Map.Entry<String, String>> entries = fill(partition, 40, "tk");
        List<byte[]> chunks = drain(local.exportChunks("mock", partition, 0L));
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
    @DisplayName("端点能力未就绪：transferStream 以 FAILED_PRECONDITION 终止流（启动收敛窗口）")
    void transferStreamReportsCapabilityNotStartedAsFailedPrecondition() throws Exception {
        PartitionDataAdapter notStarted = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                return 0;
            }

            @Override
            public Iterator<byte[]> exportChunks(String capability, int partition,
                                                 long fromCheckpoint) {
                return java.util.Collections.emptyIterator();
            }

            @Override
            public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
                throw new com.ww.runtime.common.RuntimeFrameworkException(
                        "cache capability not started");
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }
        };
        Throwable terminal = sendToThrowingServer(notStarted);

        assertThat(terminal).isInstanceOf(io.grpc.StatusRuntimeException.class);
        assertThat(((io.grpc.StatusRuntimeException) terminal).getStatus().getCode())
                .isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION);
    }

    @Test
    @DisplayName("导入期非前置条件异常：transferStream 以 INTERNAL 终止流（而非让异常逃逸成 UNKNOWN）")
    void transferStreamReportsUnexpectedImportFailureAsInternal() throws Exception {
        PartitionDataAdapter broken = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                return 0;
            }

            @Override
            public Iterator<byte[]> exportChunks(String capability, int partition,
                                                 long fromCheckpoint) {
                return java.util.Collections.emptyIterator();
            }

            @Override
            public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
                throw new IllegalStateException("disk full");
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }
        };
        Throwable terminal = sendToThrowingServer(broken);

        assertThat(terminal).isInstanceOf(io.grpc.StatusRuntimeException.class);
        assertThat(((io.grpc.StatusRuntimeException) terminal).getStatus().getCode())
                .isEqualTo(io.grpc.Status.Code.INTERNAL);
    }

    /** 独立 host + 独立任务库：发送一个合法 fin 片，等待流终止并返回客户端收到的 onError。 */
    private Throwable sendToThrowingServer(PartitionDataAdapter adapter) throws Exception {
        InMemoryMigrationTaskStore throwStore = new InMemoryMigrationTaskStore();
        long taskId = throwStore.create("mock", 1, NODE_A, NODE_B);
        GrpcServerHost h2 = new GrpcServerHost();
        h2.addService(new MigrationServer(adapter, throwStore, new TestClock()));
        h2.start(0);
        ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", h2.getPort())
                .usePlaintext().build();
        try {
            byte[] data = MockPartitionAdapter.encodeEntry("throw-key", "v1");
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> terminal = new AtomicReference<>();
            StreamObserver<DataChunkP> request = MigrationGrpc.newStub(ch)
                    .transferStream(new StreamObserver<>() {
                        @Override
                        public void onNext(ChunkAckP ack) {
                        }

                        @Override
                        public void onError(Throwable t) {
                            terminal.set(t);
                            done.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            done.countDown();
                        }
                    });
            request.onNext(chunk(taskId, 0, true, data, ChunkCodec.sha256Hex(data)));
            request.onCompleted();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            return terminal.get();
        } finally {
            ch.shutdownNow();
            h2.stop();
            h2.awaitTermination(5, TimeUnit.SECONDS);
        }
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
    void transferStreamRejectsChunksForTerminalTask() throws Exception {
        // 取消/完成安全：任务已终态（并发执行者已 COMPLETED）后到达的分片必须被拒绝。
        // 这是「迟到的重复执行者」导入失败的源头——引擎侧按已终态放弃并降级 INFO（不再误报）。
        int partition = partitionOf("ts-terminal");
        long taskId = store.create("mock", partition, NODE_A, NODE_B);
        for (MigrationTaskState state : List.of(MigrationTaskState.PREPARING,
                MigrationTaskState.SNAPSHOT, MigrationTaskState.COPYING,
                MigrationTaskState.SYNCING, MigrationTaskState.CUTOVER,
                MigrationTaskState.VERIFYING, MigrationTaskState.COMPLETED)) {
            store.transition(taskId, state);
        }

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
        byte[] data = MockPartitionAdapter.encodeEntry(keyOfPartition(partition), "v1");
        request.onNext(chunk(taskId, 0, true, data, ChunkCodec.sha256Hex(data)));
        request.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(acks).hasSize(1);
        assertThat(acks.get(0).getOk()).isFalse();
        assertThat(acks.get(0).getMessage()).contains("already terminal");
        assertThat(capability.partitionCount(partition)).isZero(); // 终态后数据不得写入
    }

    @Test
    void clearPartitionExceptionCarriesExceptionClassInAck() throws Exception {
        // 回归：message 为 null 的异常（如 NPE）此前日志与 ack 只见 ": null"，根因丢失；
        // 现在必须携带异常类名，引擎侧（RemotePartitionAdapter）与对端日志才可诊断
        PartitionDataAdapter throwing = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                return 0;
            }

            @Override
            public Iterator<byte[]> exportChunks(String capability, int partition,
                                                 long fromCheckpoint) {
                return java.util.Collections.<byte[]>emptyIterator();
            }

            @Override
            public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
                return true;
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }

            @Override
            public boolean clearPartition(String capability, int partition, NodeId source) {
                throw new NullPointerException();
            }
        };
        GrpcServerHost throwingHost = new GrpcServerHost();
        throwingHost.addService(new MigrationServer(throwing, store, new TestClock()));
        throwingHost.start(0);
        ManagedChannel ch = ManagedChannelBuilder
                .forAddress("localhost", throwingHost.getPort()).usePlaintext().build();
        try {
            TaskAck ack = MigrationGrpc.newBlockingStub(ch).clearPartition(ClearReqP.newBuilder()
                    .setCapability("mock").setPartitionId(partitionOf("clear-npe")).build());
            assertThat(ack.getOk()).isFalse();
            assertThat(ack.getMessage()).contains("NullPointerException");
        } finally {
            ch.shutdownNow();
            throwingHost.stop();
            throwingHost.awaitTermination(5, TimeUnit.SECONDS);
        }
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

    /** 物化惰性导出流（测试断言用）。 */
    private static List<byte[]> drain(Iterator<byte[]> chunks) {
        List<byte[]> all = new ArrayList<>();
        chunks.forEachRemaining(all::add);
        return all;
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
