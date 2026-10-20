package com.ww.runtime.migration;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.ClearReqP;
import com.ww.runtime.proto.CountAckP;
import com.ww.runtime.proto.CountReqP;
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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 迁移 gRPC 服务端：挂在<b>数据持有节点</b>上，暴露六类 RPC。
 *
 * <ul>
 *   <li><b>FetchPartition</b>（server 流）：按 {@code from_checkpoint} 调本地
 *       {@link PartitionDataAdapter#exportChunks} 分片推送，每片携带累计
 *       checkpoint_offset（条目偏移）与 checksum；调用方为
 *       {@link RemotePartitionAdapter}；</li>
 *   <li><b>TransferStream</b>（bidi 接收端）：逐片校验 checksum，不符回
 *       {@code ok=false}（ack_seq=seq）要求重发；通过则单片 import、累计断点、
 *       回 {@code ok=true}；fin 后回最终 ack 并持久化进度；</li>
 *   <li><b>CountPartition</b>：源端/目标端分区条目计数（SNAPSHOT/VERIFYING 对账）；</li>
 *   <li><b>ClearPartition</b>：迁移完成后清理本节点该分区数据（源端释放）；</li>
 *   <li><b>ControlTask</b>：action=cancel/cancelled 时置 store 状态 CANCELLED；</li>
 *   <li><b>ReportProgress</b>（client 流）：逐条落进度，返回汇总。</li>
 * </ul>
 *
 * <p>能力无关：各数据面 RPC 的 capability 参数经构造注入的组合适配器按能力分发。</p>
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
        Iterator<byte[]> pieces = null;
        try {
            // 惰性流式导出：磁盘/本地存储逐片产出 → 直接推入 gRPC 流（流控即背压），
            // 服务端堆内恒定单片量级，大分区不整体物化入堆
            String cursor = request.getFromCursor();
            pieces = localAdapter.exportChunks(
                    request.getCapability(), request.getPartitionId(),
                    request.getFromCheckpoint(), cursor.isEmpty() ? null : cursor);
            if (!pieces.hasNext()) {
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
                int seq = 0;
                byte[] current = pieces.next();
                while (pieces.hasNext()) {
                    cumulative += ChunkCodec.countEntries(current);
                    responseObserver.onNext(chunkMessage(seq++, false, current, cumulative));
                    current = pieces.next();
                }
                cumulative += ChunkCodec.countEntries(current);
                responseObserver.onNext(chunkMessage(seq, true, current, cumulative));
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            // 异常对象作末参（类名+堆栈）：仅打 getMessage() 会把 message 为 null 的
            // 异常（NPE 等）打成 "null"，根因丢失
            if (isCapabilityPrecondition(e)) {
                // 端点能力未就绪（滚动重启/启动收敛窗口）：瞬态前置条件失败——
                // 语义状态码 FAILED_PRECONDITION 供引擎侧按「稍后重试」降级留痕，
                // 不混入 INTERNAL 真错误
                LOG.info("fetchPartition deferred: capability={} partition={} — {}",
                        request.getCapability(), request.getPartitionId(), String.valueOf(e.getMessage()));
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("precondition: " + e.getMessage())
                        .asException());
                return;
            }
            LOG.warn("fetchPartition failed: capability={} partition={}",
                    request.getCapability(), request.getPartitionId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("fetch failed: " + e)
                    .asException());
        } finally {
            // T24：异常/客户端取消路径强制关闭导出迭代器，消灭文件句柄泄漏
            localAdapter.closeExportChunks(pieces);
        }
    }

    /** 能力未就绪类失败（启动/冻结窗口）判定：按异常链 message 识别。 */
    static boolean isCapabilityPrecondition(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && (m.contains("capability not started") || m.contains("frozen"))) {
                return true;
            }
        }
        return false;
    }

    private static DataChunkP chunkMessage(int seq, boolean fin, byte[] data, long checkpointOffset) {
        return DataChunkP.newBuilder()
                .setTaskId(0L)
                .setSeq(seq)
                .setFin(fin)
                .setData(ByteString.copyFrom(data))
                .setChecksum(ChunkCodec.sha256Hex(data))
                .setCheckpointOffset(checkpointOffset)
                .build();
    }

    @Override
    public StreamObserver<DataChunkP> transferStream(StreamObserver<ChunkAckP> responseObserver) {
        return new StreamObserver<>() {
            private long taskId = -1L;
            private long checkpoint;
            private long items;
            private boolean progressLoaded;
            /** 终态守卫：onError 关闭调用后，客户端的半关闭 onCompleted 不得再 close（否则 "call already closed" 噪声）。 */
            private boolean terminated;

            @Override
            public void onNext(DataChunkP chunk) {
                try {
                    handleChunk(chunk);
                } catch (RuntimeException e) {
                    // 与 fetchPartition/countPartition 同口径：端点能力处于启动/冻结窗口时
                    // 以 FAILED_PRECONDITION 终止流——客户端把 StatusRuntimeException 保留在
                    // cause 链中抛出，引擎 isPreconditionFailure 识别后按「稍后重试」降级；
                    // 其余异常为真错误，以 INTERNAL 终止（不再让异常逃逸成无语义的 UNKNOWN）。
                    terminated = true;
                    if (isCapabilityPrecondition(e)) {
                        LOG.info("transferStream deferred: task={} — {}",
                                chunk.getTaskId(), String.valueOf(e.getMessage()));
                        responseObserver.onError(Status.FAILED_PRECONDITION
                                .withDescription("precondition: " + e.getMessage())
                                .asException());
                        return;
                    }
                    LOG.warn("transferStream failed: task={}", chunk.getTaskId(), e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("import failed: " + e)
                            .asException());
                }
            }

            private void handleChunk(DataChunkP chunk) {
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
                    // 保留 epoch/sourceCount 与接收端可推导的游标：旧实现写 null 会
                    // 抹掉断点元数据（含键型游标），导致续跑退化到位置断点。
                    MigrationTask latest = store.get(taskId).orElse(task);
                    MigrationEngine.Meta meta = MigrationEngine.Meta.parse(latest.checkpointJson());
                    String cursor = localAdapter.cursorOf(task.capability(), data);
                    store.saveProgress(taskId, checkpoint, items,
                            MigrationEngine.Meta.toJson(meta.epoch(), meta.sourceCount(),
                                    cursor != null ? cursor : meta.cursor()));
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
                if (terminated) {
                    return; // 已以 onError 终态关闭（前置条件/INTERNAL），幂等忽略随后的半关闭
                }
                terminated = true;
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
    public void countPartition(CountReqP request, StreamObserver<CountAckP> responseObserver) {
        try {
            long count = localAdapter.itemCount(request.getCapability(), request.getPartitionId());
            responseObserver.onNext(CountAckP.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            if (isCapabilityPrecondition(e)) {
                LOG.info("countPartition deferred: capability={} partition={} — {}",
                        request.getCapability(), request.getPartitionId(), String.valueOf(e.getMessage()));
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("precondition: " + e.getMessage())
                        .asException());
                return;
            }
            LOG.warn("countPartition failed: capability={} partition={}",
                    request.getCapability(), request.getPartitionId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("count failed: " + e)
                    .asException());
        }
    }

    @Override
    public void clearPartition(ClearReqP request, StreamObserver<TaskAck> responseObserver) {
        String capability = request.getCapability();
        int partition = request.getPartitionId();
        try {
            boolean cleared = localAdapter.clearPartition(capability, partition, null);
            respond(responseObserver, cleared, cleared
                    ? "partition " + partition + " of " + capability + " cleared"
                    : "partition " + partition + " of " + capability + " not cleared"
                            + " (unsupported or partially failed)");
        } catch (RuntimeException e) {
            // 异常对象作末参（类名+堆栈）；ack message 同样携带类名回传引擎侧——
            // message 为 null 的异常（NPE 等）打 getMessage() 只会得到 "null"
            LOG.warn("clearPartition failed: capability={} partition={}",
                    capability, partition, e);
            respond(responseObserver, false, "clear failed: " + e);
        }
    }

    @Override
    public void freezePartition(com.ww.runtime.proto.FreezePartitionReqP request,
                                StreamObserver<TaskAck> responseObserver) {
        String capability = request.getCapability();
        int partition = request.getPartitionId();
        try {
            boolean ok = localAdapter.freezePartition(capability, partition,
                    request.getTtlMillis());
            respond(responseObserver, ok, ok
                    ? "partition " + partition + " of " + capability
                            + (request.getTtlMillis() <= 0 ? " unfrozen" : " frozen")
                    : "partition " + partition + " of " + capability + " freeze unsupported/failed");
        } catch (RuntimeException e) {
            LOG.warn("freezePartition failed: capability={} partition={} ttl={}",
                    capability, partition, request.getTtlMillis(), e);
            respond(responseObserver, false, "freeze failed: " + e);
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
                    LOG.warn("reportProgress save failed for task {}",
                            report.getTaskId(), e);
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
