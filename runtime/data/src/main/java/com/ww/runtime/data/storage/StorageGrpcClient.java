package com.ww.runtime.data.storage;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.proto.BucketReqP;
import com.ww.runtime.proto.CountObjectsAckP;
import com.ww.runtime.proto.CountObjectsReqP;
import com.ww.runtime.proto.DeleteAckP;
import com.ww.runtime.proto.ListReqP;
import com.ww.runtime.proto.ObjectMetaP;
import com.ww.runtime.proto.ObjectChunkP;
import com.ww.runtime.proto.ObjectReqP;
import com.ww.runtime.proto.StorageGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 存储数据面 gRPC 客户端：网关转发（Forward*）、副本复制（Replicate*）、
 * fan-out 列举（ListBucket）与桶同步（SyncBucket）。
 *
 * <p>节点地址经 {@link StoragePeerDirectory} 解析，按 {@code host:port} 缓存
 * {@link ManagedChannel}（plaintext 内网，与 GrpcNodeCommandChannel 同语义）。
 * 单次调用 deadline 默认 30s；流式（ForwardPut/ForwardGet/Replicate）默认 300s，
 * 均可构造覆盖。{@link #close()} 释放全部缓存通道。</p>
 */
public final class StorageGrpcClient implements StorageReplicationClient {

    public static final long DEFAULT_DEADLINE_MILLIS = 30_000L;
    /** 流式操作（Put/Get/Replicate）默认超时 1 小时，支持大文件传输 */
    public static final long DEFAULT_STREAM_DEADLINE_MILLIS = 3_600_000L;
    /** 流式分片大小（对齐 runtime.migration.chunk-size 默认 4MB）。 */
    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
    /** 单消息为分片数据 + proto 头字段（bucket/key/userMeta 等）预留的余量。 */
    private static final int MESSAGE_HEADROOM_BYTES = 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(StorageGrpcClient.class);

    private final StoragePeerDirectory directory;
    private final long deadlineMillis;
    private final long streamDeadlineMillis;
    private final int chunkSize;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public StorageGrpcClient(StoragePeerDirectory directory) {
        this(directory, DEFAULT_DEADLINE_MILLIS, DEFAULT_STREAM_DEADLINE_MILLIS, DEFAULT_CHUNK_SIZE);
    }

    public StorageGrpcClient(StoragePeerDirectory directory,
                             long deadlineMillis,
                             long streamDeadlineMillis,
                             int chunkSize) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (deadlineMillis <= 0 || streamDeadlineMillis <= 0) {
            throw new IllegalArgumentException("deadlines must be > 0");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0, got " + chunkSize);
        }
        this.deadlineMillis = deadlineMillis;
        this.streamDeadlineMillis = streamDeadlineMillis;
        this.chunkSize = chunkSize;
    }

    // ------------------------------------------------------------------
    // 网关转发（属主节点执行 S3 语义）
    // ------------------------------------------------------------------

    /**
     * 转发对象写到属主节点（ForwardPut，client 流）。
     *
     * @param length 内容长度提示，&lt;0 表示未知
     * @return 属主落盘后的元数据（含最终 version/etag）
     */
    public ObjectMetadata forwardPut(NodeId node, String bucket, String key, String contentType,
                                     Map<String, String> userMeta, InputStream data, long length) {
        return upload(node, StorageGrpc.StorageStub::forwardPut, bucket, key, contentType,
                length, "", 0L, 0L, userMeta, data);
    }

    /**
     * 转发对象读到属主节点（ForwardGet，server 流）：返回原始分片迭代器，
     * 调用方按 offset 顺序消费；对象不存在时首个响应前抛 {@link StatusRuntimeException}(NOT_FOUND)。
     */
    public Iterator<ObjectChunkP> forwardGet(NodeId node, String bucket, String key) {
        return forwardGet(node, bucket, key, 0L, 0L);
    }

    /**
     * 转发对象<b>区间</b>读到属主节点：属主从 {@code offset} 起最多回传 {@code length} 字节
     * （length=0 为直到对象末尾）。片上 offset 为对象内绝对偏移；旧版属主不识别字段时
     * 从 0 回放全量，由调用方按片上偏移快进兜底。
     */
    public Iterator<ObjectChunkP> forwardGet(NodeId node, String bucket, String key,
                                             long offset, long length) {
        StorageGrpc.StorageBlockingStub stub = stubFor(node, streamDeadlineMillis);
        return stub.forwardGet(ObjectReqP.newBuilder().setBucket(bucket).setKey(key)
                .setOffset(offset).setLength(length).build());
    }

    /**
     * 转发 HEAD：对象不存在返回 {@code empty()}。
     * <p>T4：仅 NOT_FOUND 归一为 empty；其余 gRPC 错误（不可用/内部错误等）原样上抛——
     * 旧行为把一切异常吞成 empty，对账/复制路径会把「节点不可用」误判为「对象缺失」触发全量补复制。
     */
    public Optional<ObjectMetadata> forwardHead(NodeId node, String bucket, String key) {
        try {
            StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
            ObjectMetaP meta = stub.forwardHead(
                    ObjectReqP.newBuilder().setBucket(bucket).setKey(key).build());
            return meta.getExists() ? Optional.of(fromProto(meta)) : Optional.empty();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            LOG.warn("forwardHead failed on {}: {}/{}: {}", node, bucket, key, e.getMessage());
            throw e;
        }
    }

    /**
     * 转发删除（属主端 fencing 删除）。
     *
     * <p>远端 fencing 拒绝经 {@code ok=false + stale-epoch} 标记还原为
     * {@link StaleEpochException}（网关重取属主重试收敛 / REST 层 409）；
     * 其余拒绝上抛 {@link RuntimeFrameworkException}，不静默吞掉。</p>
     *
     * @return 对象原本存在并已删除返回 true；不存在返回 false
     */
    public boolean forwardDelete(NodeId node, String bucket, String key) {
        StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
        DeleteAckP ack = stub.forwardDelete(
                ObjectReqP.newBuilder().setBucket(bucket).setKey(key).build());
        if (!ack.getOk()) {
            String message = ack.getMessage();
            if (message.startsWith("stale-epoch")) {
                throw new StaleEpochException(message);
            }
            throw new RuntimeFrameworkException("forwardDelete rejected on " + node + ": " + message);
        }
        return ack.getDeleted();
    }

    /**
     * fan-out 列举单页（ListBucket，server 流；服务端至多回传 maxKeys 条）。
     * <p>T19：失败上抛而非降级空列表——降级会让网关把「节点失败」误当「无数据」，
     * 调用方（网关）据异常记入 missingPeers 降级标记。
     */
    public List<ObjectMetadata> listBucket(NodeId node, String bucket,
                                           String prefix, String marker, int maxKeys) {
        List<ObjectMetadata> result = new ArrayList<>();
        StorageGrpc.StorageBlockingStub stub = stubFor(node, streamDeadlineMillis);
        Iterator<ObjectMetaP> stream = stub.listBucket(ListReqP.newBuilder()
                .setBucket(bucket)
                .setPrefix(nullToEmpty(prefix))
                .setMarker(nullToEmpty(marker))
                .setMaxKeys(maxKeys)
                .build());
        while (stream.hasNext()) {
            ObjectMetaP meta = stream.next();
            if (meta.getExists()) {
                result.add(fromProto(meta));
            }
        }
        return result;
    }

    /** T14：对账摘要列举——复用 ListBucket RPC 单页，转轻量摘要（key+version+etag）。 */
    @Override
    public List<ObjectSummary> remoteList(NodeId node, String bucket, String marker, int maxKeys) {
        List<ObjectMetadata> page = listBucket(node, bucket, null, marker, maxKeys);
        List<ObjectSummary> summaries = new ArrayList<>(page.size());
        for (ObjectMetadata meta : page) {
            summaries.add(new ObjectSummary(meta.getKey(), meta.getVersion(), meta.getEtag()));
        }
        return summaries;
    }

    /**
     * 对端节点对象计数（严格版）：失败<b>上抛</b>而非降级 0——供管理面每节点统计
     * 区分「节点不可达/能力未启动」与「0 对象」（降级版会把宕机误显示为空）。
     *
     * @param bucket 桶名；null/空 = 全部桶
     */
    public long countObjectsStrict(NodeId node, String bucket) {
        StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
        CountObjectsAckP ack = stub.countObjects(
                CountObjectsReqP.newBuilder().setBucket(nullToEmpty(bucket)).build());
        return ack.getCount();
    }

    /** 桶同步广播（SyncBucket：action=create/delete）。 */
    public boolean syncBucket(NodeId node, String bucket, String action, boolean recursive) {
        try {
            StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
            DeleteAckP ack = stub.syncBucket(BucketReqP.newBuilder()
                    .setBucket(bucket)
                    .setAction(action)
                    .setRecursive(recursive)
                    .build());
            return ack.getOk();
        } catch (StatusRuntimeException e) {
            LOG.warn("syncBucket failed on {} bucket={} action={}: {}",
                    node, bucket, action, e.getMessage());
            return false;
        }
    }

    /** T21 桶对账：列远端节点注册的桶名（异常原样上抛，由对账方按节点降级处理）。 */
    public List<String> listBucketNames(NodeId node) {
        StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
        List<String> names = new ArrayList<>();
        Iterator<com.ww.runtime.proto.BucketNameP> it = stub.listBucketNames(
                com.ww.runtime.proto.BucketNameP.getDefaultInstance());
        it.forEachRemaining(p -> names.add(p.getName()));
        return names;
    }

    // ------------------------------------------------------------------
    // 副本复制（StorageReplicationClient 端口实现）
    // ------------------------------------------------------------------

    @Override
    public boolean replicateObject(NodeId node, String bucket, ObjectMetadata meta, InputStream data) {
        Objects.requireNonNull(meta, "meta");
        try {
            upload(node, StorageGrpc.StorageStub::replicate, bucket, meta.getKey(),
                    meta.getContentType(), meta.getSize(), nullToEmpty(meta.getEtag()),
                    meta.getCreatedAt(), meta.getVersion(), meta.getUserMetadata(), data);
            return true;
        } catch (StatusRuntimeException | UncheckedIOException | IllegalStateException e) {
            LOG.warn("replicate failed on {} {}/{}: {}", node, bucket, meta.getKey(), e.getMessage());
            return false;
        }
    }

    @Override
    public boolean replicateDelete(NodeId node, String bucket, String key) {
        return replicateDelete(node, bucket, key, -1L);
    }

    @Override
    public boolean replicateDelete(NodeId node, String bucket, String key, long version) {
        try {
            StorageGrpc.StorageBlockingStub stub = stubFor(node, deadlineMillis);
            stub.replicateDelete(ObjectReqP.newBuilder()
                    .setBucket(bucket).setKey(key).setVersion(version).build());
            return true;
        } catch (StatusRuntimeException e) {
            LOG.warn("replicateDelete failed on {} {}/{}: {}", node, bucket, key, e.getMessage());
            return false;
        }
    }

    @Override
    public Optional<ObjectMetadata> remoteHead(NodeId node, String bucket, String key) {
        return forwardHead(node, bucket, key);
    }

    // ------------------------------------------------------------------
    // 通道与工具
    // ------------------------------------------------------------------

    /** 释放全部缓存通道（幂等）。 */
    @Override
    public void close() {
        channels.forEach((target, channel) -> channel.shutdown());
        channels.forEach((target, channel) -> {
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                    LOG.warn("grpc channel to {} did not terminate in time", target);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("interrupted while closing grpc channel to {}", target);
            }
        });
        channels.clear();
    }

    private ManagedChannel channelFor(NodeId node) {
        StoragePeerDirectory.PeerEndpoint endpoint = directory.endpoint(node)
                .orElseThrow(() -> new IllegalArgumentException("no storage endpoint for node " + node));
        return channels.computeIfAbsent(endpoint.target(),
                key -> ManagedChannelBuilder.forTarget(key)
                        .usePlaintext()
                        // 下行分片（ForwardGet/ListBucket server 流）与上行同构：chunkSize 数据 + proto 头，
                        // gRPC 默认 4MB 上限会拒收，须同步放宽（与服务端 GrpcServerHost 8MB 对齐）
                        .maxInboundMessageSize(chunkSize + MESSAGE_HEADROOM_BYTES)
                        .build());
    }

    private StorageGrpc.StorageBlockingStub stubFor(NodeId node, long deadline) {
        return StorageGrpc.newBlockingStub(channelFor(node))
                .withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
    }

    /** client 流 RPC 入口（blocking stub 不支持 client-streaming，须经 async stub）。 */
    @FunctionalInterface
    private interface StreamingRpc {
        StreamObserver<ObjectChunkP> invoke(StorageGrpc.StorageStub stub,
                                            StreamObserver<ObjectMetaP> response);
    }

    /**
     * client 流上传：分片经 async stub 推送，{@link CompletableFuture} 阻塞等待唯一应答；
     * RPC 已带 stream deadline，{@code future.get} 为本地看门狗兜底。
     */
    private ObjectMetadata upload(NodeId node, StreamingRpc rpc, String bucket, String key,
                                  String contentType, long size, String etag, long createdAt,
                                  long version, Map<String, String> userMeta, InputStream data) {
        CompletableFuture<ObjectMetaP> future = new CompletableFuture<>();
        StreamObserver<ObjectChunkP> requests = rpc.invoke(
                StorageGrpc.newStub(channelFor(node))
                        .withDeadlineAfter(streamDeadlineMillis, TimeUnit.MILLISECONDS),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ObjectMetaP meta) {
                        future.complete(meta);
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        // 应答已由 onNext 送达
                    }
                });
        try {
            Iterator<ObjectChunkP> chunks = chunkIterator(bucket, key, contentType, size,
                    etag, createdAt, version, userMeta, data);
            while (chunks.hasNext()) {
                requests.onNext(chunks.next());
            }
            requests.onCompleted();
            return fromProto(future.get(streamDeadlineMillis, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException("upload interrupted: " + bucket + "/" + key, e));
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            if (cause instanceof UncheckedIOException uio) {
                throw uio;
            }
            throw new UncheckedIOException(new IOException("upload failed: " + bucket + "/" + key, cause));
        }
    }

    /**
     * 惰性分片迭代器：每片携带全量头字段（服务端取首片头），
     * readNBytes 短读即 EOF，末片 fin=true（空对象也发一个空 fin 片）。
     */
    private Iterator<ObjectChunkP> chunkIterator(String bucket, String key, String contentType,
                                                 long size, String etag, long createdAt,
                                                 long version, Map<String, String> userMeta,
                                                 InputStream data) {
        return new Iterator<>() {
            private final byte[] buf = new byte[chunkSize];
            private long offset = 0L;
            private boolean done;

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public ObjectChunkP next() {
                int n;
                try {
                    n = data.readNBytes(buf, 0, buf.length);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                boolean fin = n < buf.length;
                if (fin) {
                    done = true;
                }
                ObjectChunkP.Builder b = ObjectChunkP.newBuilder()
                        .setBucket(bucket)
                        .setKey(key)
                        .setContentType(nullToEmpty(contentType))
                        .setSize(size)
                        .setEtag(etag)
                        .setCreatedAt(createdAt)
                        .setVersion(version)
                        .setOffset(offset)
                        .setFin(fin)
                        .setData(ByteString.copyFrom(buf, 0, n));
                if (userMeta != null && !userMeta.isEmpty()) {
                    b.putAllUserMeta(userMeta);
                }
                offset += n;
                return b.build();
            }
        };
    }

    private static ObjectMetadata fromProto(ObjectMetaP meta) {
        ObjectMetadata m = new ObjectMetadata(meta.getKey(), meta.getSize(),
                meta.getContentType(), emptyToNull(meta.getEtag()),
                meta.getCreatedAt(), meta.getVersion());
        if (!meta.getUserMetaMap().isEmpty()) {
            m.setUserMetadata(meta.getUserMetaMap());
        }
        return m;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
