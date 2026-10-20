package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.ClearReqP;
import com.ww.runtime.proto.CountAckP;
import com.ww.runtime.proto.CountReqP;
import com.ww.runtime.proto.DataChunkP;
import com.ww.runtime.proto.FetchReq;
import com.ww.runtime.proto.MigrationGrpc;
import com.ww.runtime.proto.TaskAck;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link PartitionDataAdapter} 的远端实现：经 gRPC {@code FetchPartition} 从远端
 * 数据持有节点拉取分片（源端读取路径），可选绑定 taskId 后经 {@code TransferStream}
 * 双向流向远端目标写入（接收端 ack 重发语义）。
 *
 * <p>计数经 {@code CountPartition} RPC（避免全量拉取数条目）；清理经
 * {@code ClearPartition} RPC。通道 {@code maxInboundMessageSize=8MB}（对齐
 * GrpcServerHost 既有约定，容纳默认 4MB 分片 + proto 头）。</p>
 */
public final class RemotePartitionAdapter implements PartitionDataAdapter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePartitionAdapter.class);

    /**
     * Fetch 为惰性流式消费：RPC 生命周期覆盖整个拉取-导入循环（含逐片限流睡眠），
     * 而非一次性全量拉取，故 deadline 需放宽至大分区量级（4MB 片 × 默认限流下
     * 数十 GB 分区亦在窗口内）。
     */
    private static final long FETCH_DEADLINE_SECONDS = 1800L;
    private static final long ROUND_TIMEOUT_SECONDS = 60L;
    private static final long RPC_DEADLINE_SECONDS = 30L;
    private static final int MAX_RESEND_ATTEMPTS = 3;
    /**
     * 重发退避基数（毫秒）：接收端要求重发后逐轮加倍等待（25ms、50ms）。
     * 端点能力启动窗口等瞬态故障下，避免毫秒级空转在同一窗口内打满 3 次重试预算。
     */
    private static final long RESEND_BACKOFF_BASE_MS = 25L;
    /** 与 GrpcServerHost 约定一致的 gRPC 单消息上限（4MB 分片 + 头部余量）。 */
    private static final int MAX_INBOUND_MESSAGE_SIZE = 8 * 1024 * 1024;

    /** 等待接缝：生产走 {@link Thread#sleep}，测试注入记录器以确定性验证退避节奏。 */
    @FunctionalInterface
    interface BackoffSleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final ManagedChannel channel;
    private final MigrationGrpc.MigrationBlockingStub blockingStub;
    private final MigrationGrpc.MigrationStub asyncStub;
    private final long boundTaskId;
    private final BackoffSleeper sleeper;

    /** 创建只读（拉取）适配器：importChunks 不可用。 */
    public RemotePartitionAdapter(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                .usePlaintext()
                .build(), 0L);
    }

    /** 创建绑定任务的适配器：importChunks 经 TransferStream 推送到接收端。 */
    public RemotePartitionAdapter(String host, int port, long taskId) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                .usePlaintext()
                .build(), taskId);
    }

    public RemotePartitionAdapter(ManagedChannel channel) {
        this(channel, 0L);
    }

    public RemotePartitionAdapter(ManagedChannel channel, long taskId) {
        this(channel, taskId, Thread::sleep);
    }

    RemotePartitionAdapter(ManagedChannel channel, long taskId, BackoffSleeper sleeper) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.blockingStub = MigrationGrpc.newBlockingStub(this.channel).withCompression("gzip");
        this.asyncStub = MigrationGrpc.newStub(this.channel);
        this.boundTaskId = taskId;
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    @Override
    public long itemCount(String capability, int partition) {
        return countPartition(capability, partition);
    }

    /**
     * 惰性流式拉取：每次 {@code next()} 才从网络取一片（校验后交付），
     * 客户端堆内恒定单片量级；gRPC 流控天然背压，源端按需产片。
     */
    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
        return exportChunks(capability, partition, fromCheckpoint, (String) null);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                         String cursor) {
        if (fromCheckpoint < 0) {
            throw new IllegalArgumentException("fromCheckpoint must be >= 0, got " + fromCheckpoint);
        }
        FetchReq request = FetchReq.newBuilder()
                .setCapability(capability)
                .setPartitionId(partition)
                .setFromCheckpoint(fromCheckpoint)
                .setFromCursor(cursor == null ? "" : cursor)
                .build();
        Iterator<DataChunkP> stream = blockingStub
                .withDeadlineAfter(FETCH_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .fetchPartition(request);
        return new Iterator<>() {
            private byte[] buffered;
            private boolean finished;

            @Override
            public boolean hasNext() {
                while (buffered == null && !finished) {
                    if (!stream.hasNext()) {
                        finished = true;
                        return false;
                    }
                    DataChunkP chunk = stream.next();
                    byte[] data = chunk.getData().toByteArray();
                    if (!ChunkCodec.verify(data, chunk.getChecksum())) {
                        throw new MigrationException("remote chunk checksum mismatch: capability="
                                + capability + " partition=" + partition + " seq=" + chunk.getSeq());
                    }
                    if (data.length > 0) {
                        buffered = data;
                    }
                    // 空数据片（无剩余数据的 fin 片）：跳过，由流终止判定
                }
                return buffered != null;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                byte[] data = buffered;
                buffered = null;
                return data;
            }
        };
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
        return pushChunks(chunks, null);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks,
                                NodeId target, long taskId, List<String> checksums) {
        return pushChunks(chunks, checksums);
    }

    /**
     * ack 重发推送。{@code checksums} 为调用方预计算的各分片 SHA-256（下标对应，
     * 引擎每片只哈希一次后传入）；为 null / 缺失时退化为逐片本地计算。
     */
    private boolean pushChunks(List<byte[]> chunks, List<String> checksums) {
        if (boundTaskId <= 0) {
            throw new MigrationException(
                    "remote import requires a task-bound adapter: construct with taskId (see Task 11)");
        }
        Objects.requireNonNull(chunks, "chunks");
        if (chunks.isEmpty()) {
            return true;
        }
        Map<Integer, byte[]> pending = new LinkedHashMap<>();
        for (int i = 0; i < chunks.size(); i++) {
            pending.put(i, chunks.get(i));
        }
        Map<Integer, Integer> attempts = new HashMap<>();
        Map<Integer, String> rejectReasons = new HashMap<>();
        int round = 0;
        while (!pending.isEmpty()) {
            List<Integer> failedSeqs = sendRound(pending, checksums, rejectReasons);
            if (failedSeqs.isEmpty()) {
                return true;
            }
            for (Integer seq : failedSeqs) {
                if (attempts.merge(seq, 1, Integer::sum) >= MAX_RESEND_ATTEMPTS) {
                    // 接收端拒绝原因随 ack 携带（如 "task X already terminal: COMPLETED" /
                    // "import failed, resend"）：放弃前必须打出——布尔返回会吞掉根因，
                    // 引擎侧只剩 "chunk import failed" 无从诊断。
                    LOG.warn("chunk import rejected by receiver: task={} seq={} attempts={} reason={}",
                            boundTaskId, seq, attempts.get(seq), rejectReasons.get(seq));
                    return false;
                }
            }
            // 仅保留接收端要求重发的分片（ok=false 的 ack_seq）
            pending.keySet().retainAll(failedSeqs);
            // 指数退避后再重发：端点能力启动/冻结窗口等瞬态故障下，立即重发只会在同一
            // 窗口内打满重试预算；第 1 轮后等待 base，第 2 轮后 2×base（至多 2 轮）。
            awaitResendBackoff(++round);
        }
        return true;
    }

    /** 重发轮次间指数退避（base×round）；中断时恢复中断标志并中止导入。 */
    private void awaitResendBackoff(int round) {
        long millis = RESEND_BACKOFF_BASE_MS * round;
        try {
            sleeper.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MigrationException("interrupted while backing off chunk resend", e);
        }
    }

    @Override
    public long verifyCount(String capability, int partition) {
        return countPartition(capability, partition);
    }

    /** 远端分区条目计数（CountPartition RPC）。 */
    public long countPartition(String capability, int partition) {
        CountAckP ack = blockingStub
                .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .countPartition(CountReqP.newBuilder()
                        .setCapability(capability)
                        .setPartitionId(partition)
                        .build());
        return ack.getCount();
    }

    /** 远端分区数据清理（ClearPartition RPC，迁移完成后释放源端空间）。 */
    public boolean clearPartition(String capability, int partition) {
        TaskAck ack = blockingStub
                .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .clearPartition(ClearReqP.newBuilder()
                        .setCapability(capability)
                        .setPartitionId(partition)
                        .build());
        if (!ack.getOk()) {
            // 失败/不支持原因随 ack message 携带（如 "clear failed: <异常类名>"）：
            // 引擎侧只见布尔 false，不打出则根因丢失
            LOG.warn("remote clearPartition not ok: capability={} partition={} reason={}",
                    capability, partition, ack.getMessage());
        }
        return ack.getOk();
    }

    /** 远端分区写冻结（FreezePartition RPC；ttlMillis<=0 立即解冻）。 */
    public boolean freezePartition(String capability, int partition, long ttlMillis) {
        TaskAck ack = blockingStub
                .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .freezePartition(com.ww.runtime.proto.FreezePartitionReqP.newBuilder()
                        .setCapability(capability)
                        .setPartitionId(partition)
                        .setTtlMillis(ttlMillis)
                        .build());
        if (!ack.getOk()) {
            LOG.warn("remote freezePartition not ok: capability={} partition={} ttl={} reason={}",
                    capability, partition, ttlMillis, ack.getMessage());
        }
        return ack.getOk();
    }

    /** 发送一轮全部 pending 分片，返回接收端要求重发（ok=false）的 seq 列表及其原因。 */
    private List<Integer> sendRound(Map<Integer, byte[]> pending, List<String> checksums,
                                    Map<Integer, String> rejectReasons) {
        CountDownLatch done = new CountDownLatch(1);
        List<Integer> failedSeqs = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<DataChunkP> request = asyncStub.transferStream(new StreamObserver<>() {
            @Override
            public void onNext(ChunkAckP ack) {
                if (!ack.getOk()) {
                    failedSeqs.add(ack.getAckSeq());
                    rejectReasons.put(ack.getAckSeq(), ack.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });
        int lastKey = -1;
        for (Integer key : pending.keySet()) {
            lastKey = key;
        }
        for (Map.Entry<Integer, byte[]> entry : pending.entrySet()) {
            byte[] data = entry.getValue();
            request.onNext(DataChunkP.newBuilder()
                    .setTaskId(boundTaskId)
                    .setSeq(entry.getKey())
                    .setFin(entry.getKey() == lastKey)
                    .setData(com.google.protobuf.ByteString.copyFrom(data))
                    .setChecksum(checksumFor(entry.getKey(), data, checksums))
                    .build());
        }
        request.onCompleted();
        try {
            if (!done.await(ROUND_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new MigrationException("timeout waiting transfer-stream acks");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MigrationException("interrupted while waiting transfer-stream acks", e);
        }
        Throwable t = error.get();
        if (t != null) {
            throw new MigrationException("transfer-stream failed: " + t.getMessage(), t);
        }
        return failedSeqs;
    }

    /** 优先使用调用方预计算的校验和（引擎每片只哈希一次）；缺失时本地补算。 */
    private static String checksumFor(int seq, byte[] data, List<String> checksums) {
        if (checksums != null && seq < checksums.size()) {
            String provided = checksums.get(seq);
            if (provided != null && !provided.isBlank()) {
                return provided;
            }
        }
        return ChunkCodec.sha256Hex(data);
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5L, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
