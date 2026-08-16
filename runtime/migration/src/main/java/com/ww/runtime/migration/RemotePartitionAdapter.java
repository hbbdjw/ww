package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationException;
import com.ww.runtime.proto.ChunkAckP;
import com.ww.runtime.proto.DataChunkP;
import com.ww.runtime.proto.FetchReq;
import com.ww.runtime.proto.MigrationGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

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
 * <p><b>本阶段定位：</b>拉取路径已可用（{@link MigrationServer} + 进程内
 * GrpcServerHost 的单测覆盖）；写入路径与真实跨节点编排的集成验证由 Task 11
 * 完成——本类当前仅要求编译通过与接口自洽。</p>
 */
public final class RemotePartitionAdapter implements PartitionDataAdapter, AutoCloseable {

    private static final long FETCH_DEADLINE_SECONDS = 60L;
    private static final long ROUND_TIMEOUT_SECONDS = 60L;
    private static final int MAX_RESEND_ATTEMPTS = 3;

    private final ManagedChannel channel;
    private final MigrationGrpc.MigrationBlockingStub blockingStub;
    private final MigrationGrpc.MigrationStub asyncStub;
    private final long boundTaskId;

    /** 创建只读（拉取）适配器：importChunks 不可用。 */
    public RemotePartitionAdapter(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build(), 0L);
    }

    /** 创建绑定任务的适配器：importChunks 经 TransferStream 推送到接收端。 */
    public RemotePartitionAdapter(String host, int port, long taskId) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build(), taskId);
    }

    public RemotePartitionAdapter(ManagedChannel channel) {
        this(channel, 0L);
    }

    public RemotePartitionAdapter(ManagedChannel channel, long taskId) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.blockingStub = MigrationGrpc.newBlockingStub(this.channel).withCompression("gzip");
        this.asyncStub = MigrationGrpc.newStub(this.channel);
        this.boundTaskId = taskId;
    }

    @Override
    public long itemCount(String capability, int partition) {
        return countEntries(exportChunks(capability, partition, 0L));
    }

    @Override
    public List<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
        if (fromCheckpoint < 0) {
            throw new IllegalArgumentException("fromCheckpoint must be >= 0, got " + fromCheckpoint);
        }
        FetchReq request = FetchReq.newBuilder()
                .setCapability(capability)
                .setPartitionId(partition)
                .setFromCheckpoint(fromCheckpoint)
                .build();
        List<byte[]> result = new java.util.ArrayList<>();
        Iterator<DataChunkP> stream = blockingStub
                .withDeadlineAfter(FETCH_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .fetchPartition(request);
        while (stream.hasNext()) {
            DataChunkP chunk = stream.next();
            byte[] data = chunk.getData().toByteArray();
            if (!ChunkCodec.verify(data, chunk.getChecksum())) {
                throw new MigrationException("remote chunk checksum mismatch: capability=" + capability
                        + " partition=" + partition + " seq=" + chunk.getSeq());
            }
            if (data.length > 0) {
                result.add(data);
            }
        }
        return result;
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
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
        while (!pending.isEmpty()) {
            List<Integer> failedSeqs = sendRound(pending);
            if (failedSeqs.isEmpty()) {
                return true;
            }
            for (Integer seq : failedSeqs) {
                if (attempts.merge(seq, 1, Integer::sum) >= MAX_RESEND_ATTEMPTS) {
                    return false;
                }
            }
            // 仅保留接收端要求重发的分片（ok=false 的 ack_seq）
            pending.keySet().retainAll(failedSeqs);
        }
        return true;
    }

    /** 发送一轮全部 pending 分片，返回接收端要求重发（ok=false）的 seq 列表。 */
    private List<Integer> sendRound(Map<Integer, byte[]> pending) {
        CountDownLatch done = new CountDownLatch(1);
        List<Integer> failedSeqs = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<DataChunkP> request = asyncStub.transferStream(new StreamObserver<>() {
            @Override
            public void onNext(ChunkAckP ack) {
                if (!ack.getOk()) {
                    failedSeqs.add(ack.getAckSeq());
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
                    .setChecksum(ChunkCodec.sha256Hex(data))
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

    @Override
    public long verifyCount(String capability, int partition) {
        return countEntries(exportChunks(capability, partition, 0L));
    }

    private static long countEntries(List<byte[]> chunks) {
        long total = 0L;
        for (byte[] chunk : chunks) {
            total += ChunkCodec.countEntries(chunk);
        }
        return total;
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
