package com.ww.runtime.migration;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.DataChunkP;
import com.ww.runtime.proto.FetchReq;
import com.ww.runtime.proto.MigrationGrpc;
import com.ww.runtime.proto.ProgressReport;
import com.ww.runtime.proto.ProgressSummary;
import com.ww.runtime.proto.TaskAck;
import com.ww.runtime.proto.TaskControlReq;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 迁移 gRPC 服务端：挂在<b>数据持有节点</b>上，暴露四类 RPC。
 *
 * <ul>
 *   <li><b>FetchPartition</b>（server 流）：按 {@code from_checkpoint} 调本地
 *       {@link PartitionDataAdapter#exportChunks} 分片推送，每片携带累计
 *       checkpoint_offset（条目偏移）与 checksum；调用方为
 *       {@link RemotePartitionAdapter}；</li>
 *   <li><b>TransferStream</b>（bidi 接收端）：逐片校验 checksum，不符回
 *       {@code ok=false}（ack_seq=seq）要求重发；通过则单片 import、累计断点、
 *       回 {@code ok=true}；fin 后回最终 ack 并持久化进度；</li>
 *   <li><b>ControlTask</b>：action=cancel/cancelled 时置 store 状态 CANCELLED；</li>
 *   <li><b>ReportProgress</b>（client 流）：逐条落进度，返回汇总。</li>
 * </ul>
 */
public final class MigrationServer extends MigrationGrpc.MigrationImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationServer.class);

    private final PartitionDataAdapter localAdapter;
    private final MigrationTaskStore store;
    private final Clock clock;

    public MigrationServer(PartitionDataAdapter localAdapter, MigrationTaskStore store, Clock clock) {
        this.localAdapter = Objects.requireNonNull(localAdapter, "localAdapter");
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void fetchPartition(FetchReq request, StreamObserver<DataChunkP> responseObserver) {
        try {
            List<byte[]> pieces = localAdapter.exportChunks(
                    request.getCapability(), request.getPartitionId(), request.getFromCheckpoint());
            if (pieces.isEmpty()) {
                // 无剩余数据：发一个空数据 fin 片表达流结束
                responseObserver.onNext(DataChunkP.newBuilder()
                        .setTaskId(0L)
                        .setSeq(0)
                        .setFin(true)
                        .setData(ByteString.EMPTY)
                        .setChecksum(ChunkCodec.sha256Hex(new byte[0]))
                        .setCheckpointOffset(request.getFromCheckpoint())
                        .build());
            } else {
                long cumulative = request.getFromCheckpoint();
                for (int i = 0; i < pieces.size(); i++) {
                    byte[] data = pieces.get(i);
                    cumulative += ChunkCodec.countEntries(data);
                    responseObserver.onNext(DataChunkP.newBuilder()
                            .setTaskId(0L)
                            .setSeq(i)
                            .setFin(i == pieces.size() - 1)
                            .setData(ByteString.copyFrom(data))
                            .setChecksum(ChunkCodec.sha256Hex(data))
                            .setCheckpointOffset(cumulative)
                            .build());
                }
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("fetchPartition failed: capability={} partition={}: {}",
                    request.getCapability(), request.getPartitionId(), e.getMessage());
            responseObserver.onError(Status.INTERNAL
                    .withDescription("fetch failed: " + e.getMessage())
                    .asException());
        }
    }

    @Override
    public StreamObserver<DataChunkP> transferStream(StreamObserver<ChunkAckP> responseObserver) {
        return new StreamObserver<>() {
            private long taskId = -1L;
            private long checkpoint;
            private long items;
            private boolean progressLoaded;

            @Override
            public void onNext(DataChunkP chunk) {
                taskId = chunk.getTaskId();
                Optional<MigrationTask> found = store.get(taskId);
                if (found.isEmpty()) {
                    ack(chunk.getSeq(), false, "unknown task: " + taskId);
                    return;
                }
                MigrationTask task = found.get();
                if (MigrationStateMachine.isTerminal(task.state())) {
                    ack(chunk.getSeq(), false, "task " + taskId + " already terminal: " + task.state());
                    return;
                }
                if (!progressLoaded) {
                    checkpoint = task.checkpointOffset();
                    items = task.itemsDone();
                    progressLoaded = true;
                }
                byte[] data = chunk.getData().toByteArray();
                if (!ChunkCodec.verify(data, chunk.getChecksum())) {
                    ack(chunk.getSeq(), false, "checksum mismatch, resend seq " + chunk.getSeq());
                    return;
                }
                boolean imported = localAdapter.importChunks(
                        task.capability(), task.partition(), List.of(data));
                if (!imported) {
                    ack(chunk.getSeq(), false, "import failed, resend seq " + chunk.getSeq());
                    return;
                }
                long entries = ChunkCodec.countEntries(data);
                checkpoint += entries;
                items += entries;
                if (chunk.getFin()) {
                    store.saveProgress(taskId, checkpoint, items, null);
                    ack(chunk.getSeq(), true, "completed at checkpoint " + checkpoint);
                } else {
                    ack(chunk.getSeq(), true, "ok");
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("transferStream error from client: {}", String.valueOf(t.getMessage()));
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private void ack(int seq, boolean ok, String message) {
                responseObserver.onNext(ChunkAckP.newBuilder()
                        .setTaskId(taskId)
                        .setAckSeq(seq)
                        .setOk(ok)
                        .setMessage(message == null ? "" : message)
                        .build());
            }
        };
    }

    @Override
    public void controlTask(TaskControlReq request, StreamObserver<TaskAck> responseObserver) {
        String action = request.getAction() == null ? "" : request.getAction().trim().toLowerCase();
        long taskId = request.getTaskId();
        try {
            switch (action) {
                case "cancel", "cancelled" -> {
                    Optional<MigrationTask> found = store.get(taskId);
                    if (found.isEmpty()) {
                        respond(responseObserver, false, "unknown task: " + taskId);
                        return;
                    }
                    if (MigrationStateMachine.isTerminal(found.get().state())) {
                        respond(responseObserver, false,
                                "task " + taskId + " already terminal: " + found.get().state());
                        return;
                    }
                    store.transition(taskId, MigrationTaskState.CANCELLED);
                    respond(responseObserver, true, "task " + taskId + " cancelled");
                }
                default -> respond(responseObserver, false, "unsupported action: " + action);
            }
        } catch (MigrationException | IllegalStateTransitionException e) {
            respond(responseObserver, false, "control failed: " + e.getMessage());
        }
    }

    @Override
    public StreamObserver<ProgressReport> reportProgress(StreamObserver<ProgressSummary> responseObserver) {
        return new StreamObserver<>() {
            private int reports;

            @Override
            public void onNext(ProgressReport report) {
                reports++;
                try {
                    store.saveProgress(report.getTaskId(),
                            report.getCheckpointOffset(), report.getItemsDone(), null);
                } catch (RuntimeException e) {
                    LOG.warn("reportProgress save failed for task {}: {}",
                            report.getTaskId(), e.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("reportProgress error from client: {}", String.valueOf(t.getMessage()));
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(ProgressSummary.newBuilder()
                        .setOk(true)
                        .setMessage("accepted " + reports + " reports")
                        .build());
                responseObserver.onCompleted();
            }
        };
    }

    private static void respond(StreamObserver<TaskAck> responseObserver, boolean ok, String message) {
        responseObserver.onNext(TaskAck.newBuilder()
                .setOk(ok)
                .setMessage(message == null ? "" : message)
                .build());
        responseObserver.onCompleted();
    }
}
