package com.ww.runtime.data.storage;

import com.ww.runtime.data.storage.engine.ObjectMetadata;
import com.ww.runtime.data.storage.engine.ObjectStore;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 存储数据面 gRPC 服务端：挂在每个 STORAGE 节点上（RuntimeBootstrapper 于
 * host.start 前挂载，顺序约束与 MigrationServer 相同），暴露五类 RPC。
 *
 * <ul>
 *   <li><b>Forward*</b>（网关转发）：在本机属主身份下执行 S3 语义——
 *       ForwardPut 走 {@link StorageCapability#putObject}（含 fencing 与大小上限），
 *       ForwardGet/Head/Delete 直读本地引擎；</li>
 *   <li><b>Replicate*</b>（副本接收）：putReplica 落盘（version 防回退幂等）、
 *       副本删除，均不做 fencing（副本节点非属主）；</li>
 *   <li><b>ListBucket</b>（fan-out 列举）：单页有界回传（网关按「每节点一页」合并截断）；</li>
 *   <li><b>SyncBucket</b>（桶广播）：本地 BucketStore 落盘建桶/删桶（幂等）；</li>
 *   <li><b>CountObjects</b>（管理面计数）：全桶/指定桶对象数，能力未启动回错误态
 *       （严格客户端上抛，统计端点据此标记不可达）。</li>
 * </ul>
 *
 * <p><b>流式聚合</b>：ForwardPut/Replicate 为 client 流；接收端以有界阻塞队列
 * （{@link ChunkQueueInputStream}）衔接 gRPC 回调线程与虚拟线程落盘工作线程——
 * 队列满时 onNext 阻塞形成自然背压，全程不整对象入堆。</p>
 */
public final class StorageGrpcServer extends StorageGrpc.StorageImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(StorageGrpcServer.class);
    /** 聚合队列缓冲分片数（每片 ≤ chunkSize；满时 gRPC 回调阻塞形成背压）。 */
    private static final int INGEST_QUEUE_CAPACITY = 8;
    /** 聚合读阻塞超时：防止对端僵死导致工作线程永久挂起。 */
    private static final long INGEST_POLL_TIMEOUT_MS = 60_000L;

    private final StorageCapability capability;
    private final int chunkSize;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    /** T2 容错指标：落盘工作线程退出后被丢弃的冗余分片数 */
    private final java.util.concurrent.atomic.AtomicLong chunksDiscardedAfterAbort =
            new java.util.concurrent.atomic.AtomicLong();
    /** T2 容错指标：因入队超时判死的流数 */
    private final java.util.concurrent.atomic.AtomicLong streamsAbortedOnPushTimeout =
            new java.util.concurrent.atomic.AtomicLong();

    public StorageGrpcServer(StorageCapability capability, int chunkSize) {
        this.capability = Objects.requireNonNull(capability, "capability");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0, got " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    /** T2 容错指标：落盘工作线程退出后被丢弃的冗余分片累计数。 */
    public long chunksDiscardedAfterAbort() {
        return chunksDiscardedAfterAbort.get();
    }

    /** T2 容错指标：因入队超时判死的流累计数。 */
    public long streamsAbortedOnPushTimeout() {
        return streamsAbortedOnPushTimeout.get();
    }

    // ------------------------------------------------------------------
    // 网关转发（属主节点执行 S3 语义）
    // ------------------------------------------------------------------

    @Override
    public StreamObserver<ObjectChunkP> forwardPut(StreamObserver<ObjectMetaP> responseObserver) {
        return new IngestObserver(responseObserver, (header, input) ->
                capability.putObject(header.bucket, header.key, input, header.size,
                        header.contentType, header.userMeta));
    }

    @Override
    public void forwardGet(ObjectReqP request, StreamObserver<ObjectChunkP> responseObserver) {
        String bucket = request.getBucket();
        String key = request.getKey();
        try {
            ObjectStore.StoredObject obj = capability.getObject(bucket, key);
            if (obj == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("object not found: " + bucket + "/" + key)
                        .asException());
                return;
            }
            streamObject(bucket, obj, request.getOffset(), request.getLength(), responseObserver);
        } catch (RuntimeException e) {
            respondInternalError(responseObserver, "forwardGet", bucket + "/" + key, e);
        }
    }

    @Override
    public void forwardHead(ObjectReqP request, StreamObserver<ObjectMetaP> responseObserver) {
        ObjectMetadata meta = capability.head(request.getBucket(), request.getKey());
        if (meta == null) {
            responseObserver.onNext(ObjectMetaP.newBuilder().setExists(false).build());
        } else {
            responseObserver.onNext(toProto(request.getBucket(), meta));
        }
        responseObserver.onCompleted();
    }

    @Override
    public void forwardDelete(ObjectReqP request, StreamObserver<DeleteAckP> responseObserver) {
        try {
            boolean deleted = capability.deleteObject(request.getBucket(), request.getKey());
            ack(responseObserver, true, deleted, null);
        } catch (com.ww.runtime.common.StaleEpochException e) {
            // fencing 拒绝以 ok=false + stale-epoch 标记回传（网关还原 409）
            ack(responseObserver, false, false, "stale-epoch: " + e.getMessage());
        } catch (RuntimeException e) {
            LOG.warn("forwardDelete failed: {}/{}: {}", request.getBucket(), request.getKey(), e.getMessage());
            ack(responseObserver, false, false, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 副本接收（不做 fencing：副本节点非属主）
    // ------------------------------------------------------------------

    @Override
    public StreamObserver<ObjectChunkP> replicate(StreamObserver<ObjectMetaP> responseObserver) {
        return new IngestObserver(responseObserver, (header, input) ->
                capability.replicaPut(header.bucket, header.key, input, header.meta()));
    }

    @Override
    public void replicateDelete(ObjectReqP request, StreamObserver<DeleteAckP> responseObserver) {
        boolean deleted = capability.replicaDelete(request.getBucket(), request.getKey(),
                request.getVersion());
        ack(responseObserver, true, deleted, request.getVersion() > 0 ? null
                : "versionless delete (legacy)");
    }

    // ------------------------------------------------------------------
    // fan-out 列举 / 桶同步
    // ------------------------------------------------------------------

    @Override
    public void listBucket(ListReqP request, StreamObserver<ObjectMetaP> responseObserver) {
        String bucket = request.getBucket();
        String prefix = request.getPrefix().isEmpty() ? null : request.getPrefix();
        try {
            String marker = request.getMarker().isEmpty() ? null : request.getMarker();
            int maxKeys = request.getMaxKeys() <= 0 ? 1000 : request.getMaxKeys();
            // 单页有界回传：网关按「每节点一页」合并截断（节点端索引定位，成本与桶总量无关）
            ObjectStore.ListResult page = capability.list(bucket, prefix, marker, maxKeys);
            for (ObjectMetadata meta : page.getObjects()) {
                responseObserver.onNext(toProto(bucket, meta));
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            respondInternalError(responseObserver, "listBucket", bucket, e);
        }
    }

    @Override
    public void syncBucket(BucketReqP request, StreamObserver<DeleteAckP> responseObserver) {
        String bucket = request.getBucket();
        String action = request.getAction() == null ? "" : request.getAction().trim().toLowerCase();
        try {
            switch (action) {
                case "create" -> {
                    capability.createBucketLocal(bucket);
                    ack(responseObserver, true, false, null);
                }
                case "delete" -> {
                    capability.deleteBucketLocal(bucket, request.getRecursive());
                    ack(responseObserver, true, true, null);
                }
                default -> ack(responseObserver, false, false, "unsupported action: " + action);
            }
        } catch (RuntimeException e) {
            LOG.warn("syncBucket failed: {} action={}: {}", bucket, action, e.getMessage());
            ack(responseObserver, false, false, e.getMessage());
        }
    }

    @Override
    public void countObjects(CountObjectsReqP request, StreamObserver<CountObjectsAckP> responseObserver) {
        String bucket = request.getBucket();
        try {
            long count = bucket.isEmpty()
                    ? capability.totalObjectCount()
                    : capability.countObjects(bucket);
            responseObserver.onNext(CountObjectsAckP.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            respondInternalError(responseObserver, "countObjects", bucket, e);
        }
    }

    @Override
    public void listBucketNames(com.ww.runtime.proto.BucketNameP request,
                                StreamObserver<com.ww.runtime.proto.BucketNameP> responseObserver) {
        try {
            for (String name : capability.listBuckets()) {
                responseObserver.onNext(com.ww.runtime.proto.BucketNameP.newBuilder()
                        .setName(name).build());
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            respondInternalError(responseObserver, "listBucketNames", "", e);
        }
    }

    /** 释放落盘工作线程池（幂等；宿主停机时调用）。 */
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 内部：流式聚合与工具
    // ------------------------------------------------------------------

    /** 首片携带的头字段（后续片仅数据）。 */
    private record ChunkHeader(String bucket, String key, String contentType, long size,
                               String etag, long createdAt, long version,
                               Map<String, String> userMeta,
                               ObjectMetadata meta) {
    }

    /** 聚合落盘函数：fencing 主写或副本写。 */
    private interface IngestSink {
        ObjectMetadata ingest(ChunkHeader header, InputStream input) throws IOException;
    }

    /**
     * client 流聚合观察者：首片启动虚拟线程落盘（阻塞读 {@link ChunkQueueInputStream}），
     * 后续片限时入队（队满阻塞=背压）；对端异常毒化流。
     *
     * <p>生命周期防护（T2）：落盘工作线程可能提前结束（成功应答、磁盘满、大小超限、
     * 副本幂等跳过不消费流），此后：冗余分片静默丢弃（重放安全）、哨兵不再限时等待、
     * 应答经 {@code responded} CAS 互斥——确保 gRPC 回调线程任何情况下不被钉死。
     */
    private final class IngestObserver implements StreamObserver<ObjectChunkP> {

        private final StreamObserver<ObjectMetaP> response;
        private final IngestSink sink;
        /** 应答互斥：工作线程与回调线程竞争唯一应答机会（gRPC observer 非线程安全，不得双应答）。 */
        private final java.util.concurrent.atomic.AtomicBoolean responded =
                new java.util.concurrent.atomic.AtomicBoolean();
        /** 工作线程已退出或流已判死：后续冗余分片丢弃，哨兵不再投递。 */
        private volatile boolean aborted;
        private ChunkQueueInputStream input;
        private Future<?> worker;

        IngestObserver(StreamObserver<ObjectMetaP> response, IngestSink sink) {
            this.response = response;
            this.sink = sink;
        }

        @Override
        public void onNext(ObjectChunkP chunk) {
            if (input == null) {
                ChunkHeader header = new ChunkHeader(chunk.getBucket(), chunk.getKey(),
                        chunk.getContentType(), chunk.getSize(), chunk.getEtag(),
                        chunk.getCreatedAt(), chunk.getVersion(), chunk.getUserMetaMap(),
                        headerMeta(chunk));
                input = new ChunkQueueInputStream();
                worker = workers.submit(() -> {
                    try {
                        ObjectMetadata meta = sink.ingest(header, input);
                        if (responded.compareAndSet(false, true)) {
                            response.onNext(toProto(header.bucket, meta));
                            response.onCompleted();
                        }
                    } catch (Throwable t) {
                        LOG.warn("ingest failed: {}/{}: {}", header.bucket, header.key, t.getMessage());
                        if (responded.compareAndSet(false, true)) {
                            response.onError(asStatus("ingest", t));
                        }
                    } finally {
                        // 任何退出路径置位：回调侧据此丢弃冗余分片、跳过哨兵等待
                        aborted = true;
                        input.failNow(); // 非阻塞毒化：唤醒可能的读阻塞
                    }
                });
            }
            if (aborted) {
                // 工作线程已退出：成功已应答（副本幂等跳过场景，丢弃冗余分片=重放安全）
                // 或错误已上报，不得再向已完成的 response 应答
                if (chunksDiscardedAfterAbort.incrementAndGet() == 1) {
                    LOG.warn("ingest worker already exited, discarding subsequent chunks");
                }
                return;
            }
            boolean pushed;
            try {
                pushed = input.tryPush(chunk.getData().toByteArray(), INGEST_POLL_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                aborted = true;
                if (responded.compareAndSet(false, true)) {
                    response.onError(Status.CANCELLED
                            .withDescription("ingest interrupted").asException());
                }
                return;
            }
            if (!pushed) {
                // 限时入队失败 = 消费端已死（磁盘满/超限/幂等跳过）：判死本流，
                // 释放 gRPC 回调线程——此前 queue.put 无限阻塞会把整节点 RPC 钉死
                aborted = true;
                if (streamsAbortedOnPushTimeout.incrementAndGet() == 1) {
                    LOG.warn("ingest queue stalled, stream aborted (worker gone or consumer dead)");
                }
                if (responded.compareAndSet(false, true)) {
                    response.onError(Status.DEADLINE_EXCEEDED
                            .withDescription("ingest worker gone or queue stalled").asException());
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            LOG.warn("ingest stream error from client: {}", String.valueOf(t.getMessage()));
            if (input != null && !aborted) {
                input.fail(); // 毒化：工作线程读将抛 IOException，落盘中止
            }
        }

        @Override
        public void onCompleted() {
            if (input == null) {
                if (responded.compareAndSet(false, true)) {
                    response.onError(Status.INVALID_ARGUMENT
                            .withDescription("empty chunk stream")
                            .asException());
                }
                return;
            }
            if (!aborted) {
                input.close();
            }
            // aborted 时工作线程已退出（已应答或已报错）：跳过哨兵投递，
            // 否则限时 offer 会白等消费者永远腾不出的队列位
        }

        private ObjectMetadata headerMeta(ObjectChunkP chunk) {
            // 副本路径直接以主节点元数据落盘（version/etag/createdAt 由主预计算）
            ObjectMetadata meta = new ObjectMetadata(chunk.getKey(), chunk.getSize(), chunk.getContentType(),
                    chunk.getEtag().isEmpty() ? null : chunk.getEtag(),
                    chunk.getCreatedAt(), chunk.getVersion());
            // T7：补齐用户自定义元数据（发送端经 userMetaMap 携带，此前被丢弃）
            if (!chunk.getUserMetaMap().isEmpty()) {
                meta.setUserMetadata(chunk.getUserMetaMap());
            }
            return meta;
        }
    }

    /**
     * 有界阻塞队列衔接的 InputStream：push 阻塞背压；close=EOF；fail=毒化异常。
     *
     * <p>部分读（len &lt; 分片长）经 {@code current/pos} 游标留在本对象内消化，
     * 不回队——回队会排到后续分片与 EOF 哨兵之后，既乱序又提前 EOF（截断）。</p>
     */
    private static final class ChunkQueueInputStream extends InputStream {

        private static final byte[] EOF = new byte[0];
        private static final byte[] POISON = new byte[0];

        private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(INGEST_QUEUE_CAPACITY);

        /** 正在消费的分片与读游标（部分读后保留剩余，下次读继续）。 */
        private byte[] current;
        private int pos;
        /** EOF 哨兵已送达：后续 read 立即返回 -1，不再阻塞等待。 */
        private boolean eof;

        /**
         * 限时入队（背压）：队满等待消费者腾位；超时返回 false 表示消费端已死，
         * 由调用方判死本流——不得无限阻塞钉死 gRPC 回调线程。
         */
        boolean tryPush(byte[] data, long timeoutMs) throws InterruptedException {
            return queue.offer(data, timeoutMs, TimeUnit.MILLISECONDS);
        }

        /**
         * 非阻塞毒化：仅在工作线程退出后调用（此时无消费者，限时 offer 会空等）。
         * 清空滞留分片后投毒，确保任何残留读阻塞立即以异常解除。
         */
        void failNow() {
            queue.clear();
            queue.offer(POISON);
        }

        @Override
        public void close() {
            offerSentinel(EOF);
        }

        void fail() {
            offerSentinel(POISON);
        }

        /**
         * 哨兵限时阻塞入队：队满时等消费者腾位（非阻塞 offer 会被静默丢弃，
         * 读端将空等至超时）；消费者已死亡则超时放弃，不挂死 gRPC 回调线程。
         */
        private void offerSentinel(byte[] sentinel) {
            try {
                queue.offer(sentinel, INGEST_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (current == null || pos >= current.length) {
                if (eof) {
                    return -1;
                }
                byte[] head = poll();
                if (head == EOF) {
                    eof = true;
                    return -1;
                }
                if (head == POISON) {
                    throw new IOException("stream failed by remote peer");
                }
                current = head;
                pos = 0;
            }
            int n = Math.min(len, current.length - pos);
            System.arraycopy(current, pos, b, off, n);
            pos += n;
            return n;
        }

        private byte[] poll() throws IOException {
            try {
                byte[] head = queue.poll(INGEST_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (head == null) {
                    throw new IOException("chunk ingest timeout");
                }
                return head;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting chunk", e);
            }
        }
    }

    /**
     * 流式回传对象（可选区间 [offset, offset+length)；offset=0 且 length=0 为全量）。
     *
     * <p>先在本地读流上定位到 {@code offset}（本地文件为原生定位，加密流为读后丢弃），
     * 再按 {@code chunkSize} 分片回传；片上 {@code offset} 字段保持对象内绝对偏移。
     * offset 超出对象大小时不发任何片直接完成（网关映射为 404）。</p>
     */
    private void streamObject(String bucket, ObjectStore.StoredObject obj,
                              long offset, long length,
                              StreamObserver<ObjectChunkP> responseObserver) {
        try (InputStream in = capability.openRead(obj)) {
            long skipped = 0L;
            while (skipped < offset) {
                long s = in.skip(offset - skipped);
                if (s <= 0) {
                    break;
                }
                skipped += s;
            }
            if (skipped < offset) {
                responseObserver.onCompleted(); // 对象比 offset 短：空窗口
                return;
            }
            // T23：分片缓冲取自复用池（chunkOf 的 ByteString.copyFrom 已拷贝内容，
            // 缓冲跨片安全复用；流结束归还），并发转发读不再每流新分配 4MB 堆数组
            byte[] buf = borrowChunkBuffer();
            try {
                long pos = skipped;                 // 片在对象内的绝对偏移
                long remaining = length > 0 ? length : Long.MAX_VALUE;
                while (remaining > 0) {
                    int want = (int) Math.min(buf.length, remaining);
                    int n = in.readNBytes(buf, 0, want);
                    if (n <= 0) {
                        break;
                    }
                    boolean fin = n < want;          // 对象末尾或窗口末尾
                    responseObserver.onNext(chunkOf(bucket, obj.meta, buf, n, pos, fin));
                    pos += n;
                    remaining -= n;
                    if (fin) {
                        break;
                    }
                }
            } finally {
                returnChunkBuffer(buf);
            }
            responseObserver.onCompleted();
        } catch (IOException e) {
            respondInternalError(responseObserver, "forwardGet",
                    bucket + "/" + obj.meta.getKey(), e);
        }
    }

    /** T23：forwardGet 分片缓冲复用池（上限 16 块 ≈ chunkSize×16；空流归还超限时丢弃交由 GC）。 */
    private final java.util.concurrent.ConcurrentLinkedDeque<byte[]> chunkBufPool =
            new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final java.util.concurrent.atomic.AtomicInteger chunkBufCount =
            new java.util.concurrent.atomic.AtomicInteger();
    private static final int CHUNK_BUF_POOL_MAX = 16;

    private byte[] borrowChunkBuffer() {
        byte[] buf = chunkBufPool.pollFirst();
        if (buf != null) {
            chunkBufCount.decrementAndGet();
            return buf;
        }
        return new byte[chunkSize];
    }

    private void returnChunkBuffer(byte[] buf) {
        if (buf.length != chunkSize || chunkBufCount.get() >= CHUNK_BUF_POOL_MAX) {
            return;
        }
        chunkBufCount.incrementAndGet();
        chunkBufPool.offerLast(buf);
    }

    private ObjectChunkP chunkOf(String bucket, ObjectMetadata meta, byte[] buf, int n,
                                 long offset, boolean fin) {
        ObjectChunkP.Builder b = ObjectChunkP.newBuilder()
                .setBucket(bucket)
                .setKey(meta.getKey())
                .setContentType(meta.getContentType() == null ? "" : meta.getContentType())
                .setSize(meta.getSize())
                .setEtag(meta.getEtag() == null ? "" : meta.getEtag())
                .setCreatedAt(meta.getCreatedAt())
                .setVersion(meta.getVersion())
                .setOffset(offset)
                .setFin(fin)
                .setData(ByteString.copyFrom(buf, 0, n));
        if (!meta.getUserMetadata().isEmpty()) {
            b.putAllUserMeta(meta.getUserMetadata());
        }
        return b.build();
    }

    private static ObjectMetaP toProto(String bucket, ObjectMetadata meta) {
        ObjectMetaP.Builder b = ObjectMetaP.newBuilder()
                .setBucket(bucket)
                .setKey(meta.getKey())
                .setContentType(meta.getContentType() == null ? "" : meta.getContentType())
                .setSize(meta.getSize())
                .setEtag(meta.getEtag() == null ? "" : meta.getEtag())
                .setCreatedAt(meta.getCreatedAt())
                .setVersion(meta.getVersion())
                .setExists(true);
        if (!meta.getUserMetadata().isEmpty()) {
            b.putAllUserMeta(meta.getUserMetadata());
        }
        return b.build();
    }

    private static void ack(StreamObserver<DeleteAckP> responseObserver, boolean ok,
                            boolean deleted, String message) {
        responseObserver.onNext(DeleteAckP.newBuilder()
                .setOk(ok)
                .setDeleted(deleted)
                .setMessage(message == null ? "" : message)
                .build());
        responseObserver.onCompleted();
    }

    private static void respondInternalError(StreamObserver<?> responseObserver, String op,
                                             String target, Throwable e) {
        LOG.warn("{} failed: {}: {}", op, target, e.getMessage());
        responseObserver.onError(Status.INTERNAL
                .withDescription(op + " failed: " + e.getMessage())
                .asException());
    }

    /**
     * 落盘失败 → gRPC 状态映射（网关按标记还原 HTTP 语义）：
     * stale-epoch 标记 → ABORTED（HTTP 409）；frozen → UNAVAILABLE（HTTP 423）；
     * 其余 → INTERNAL。
     */
    private static Throwable asStatus(String op, Throwable t) {
        if (t instanceof com.ww.runtime.common.StaleEpochException) {
            return Status.ABORTED
                    .withDescription("stale-epoch: " + t.getMessage())
                    .asException();
        }
        if (t instanceof com.ww.runtime.common.RuntimeFrameworkException
                && "frozen".equals(t.getMessage())) {
            return Status.UNAVAILABLE
                    .withDescription("frozen")
                    .asException();
        }
        return Status.INTERNAL
                .withDescription(op + " failed: " + t.getMessage())
                .asException();
    }
}
