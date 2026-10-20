package com.ww.runtime.data.cache;

import com.google.protobuf.ByteString;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.data.GatewayErrors;
import com.ww.runtime.data.cache.engine.Codec;
import com.ww.runtime.proto.CacheCommandAckP;
import com.ww.runtime.proto.CacheCommandReqP;
import com.ww.runtime.proto.CacheCountAckP;
import com.ww.runtime.proto.CacheCountReqP;
import com.ww.runtime.proto.CacheDeleteAckP;
import com.ww.runtime.proto.CacheDeleteReqP;
import com.ww.runtime.proto.CacheGetReqP;
import com.ww.runtime.proto.CacheGrpc;
import com.ww.runtime.proto.CacheKeyEntryP;
import com.ww.runtime.proto.CacheListKeysReqP;
import com.ww.runtime.proto.CachePutAckP;
import com.ww.runtime.proto.CachePutIfAbsentAckP;
import com.ww.runtime.proto.CachePutReqP;
import com.ww.runtime.proto.CacheValueP;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * CACHE 数据面 gRPC 客户端：网关转发（ForwardPut/PutIfAbsent/Get/Delete）。
 *
 * <p>节点地址经 {@link CachePeerDirectory} 解析，按 {@code host:port} 缓存
 * {@link ManagedChannel}（plaintext 内网，与 StorageGrpcClient 同语义）。
 * 单次调用 deadline 默认 30s。{@link #close()} 释放全部缓存通道。
 */
public final class CacheGrpcClient {

    public static final long DEFAULT_DEADLINE_MILLIS = 30_000L;

    private static final Logger LOG = LoggerFactory.getLogger(CacheGrpcClient.class);

    private final CachePeerDirectory directory;
    private final long deadlineMillis;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public CacheGrpcClient(CachePeerDirectory directory) {
        this(directory, DEFAULT_DEADLINE_MILLIS);
    }

    public CacheGrpcClient(CachePeerDirectory directory, long deadlineMillis) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (deadlineMillis <= 0) {
            throw new IllegalArgumentException("deadlineMillis must be > 0");
        }
        this.deadlineMillis = deadlineMillis;
    }

    /** 转发写到属主节点（ForwardPut，属主端 fencing 生效）。 */
    public void forwardPut(NodeId node, String key, Object value, long ttlMillis) {
        CacheGrpc.CacheBlockingStub stub = stubFor(node);
        CachePutAckP ack = stub.forwardPut(CachePutReqP.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(Codec.encodeValue(value)))
                .setTtlMillis(ttlMillis)
                .build());
        if (!ack.getOk()) {
            throw new IllegalStateException("forwardPut rejected on " + node + ": " + ack.getMessage());
        }
    }

    /**
     * 转发“不存在才写”到属主节点（ForwardPutIfAbsent，属主端 fencing 生效）。
     *
     * @return {@code true} 表示本次写入；{@code false} 表示 key 已存在
     */
    public boolean forwardPutIfAbsent(NodeId node, String key, Object value, long ttlMillis) {
        CacheGrpc.CacheBlockingStub stub = stubFor(node);
        CachePutIfAbsentAckP ack = stub.forwardPutIfAbsent(CachePutReqP.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(Codec.encodeValue(value)))
                .setTtlMillis(ttlMillis)
                .build());
        if (!ack.getOk()) {
            throw new IllegalStateException(
                    "forwardPutIfAbsent rejected on " + node + ": " + ack.getMessage());
        }
        return ack.getApplied();
    }

    /** 转发读属主节点（ForwardGet）；key 不存在返回 {@code empty()}。 */
    public Optional<CacheValue> forwardGet(NodeId node, String key) {
        CacheGrpc.CacheBlockingStub stub = stubFor(node);
        CacheValueP resp = stub.forwardGet(CacheGetReqP.newBuilder().setKey(key).build());
        if (!resp.getExists()) {
            return Optional.empty();
        }
        return Optional.of(new CacheValue(Codec.decodeValue(resp.getValue().toByteArray()),
                resp.getTtlMillis()));
    }

    /** 转发删除属主节点（ForwardDelete，属主端 fencing 生效）。 */
    public boolean forwardDelete(NodeId node, String key) {
        CacheGrpc.CacheBlockingStub stub = stubFor(node);
        CacheDeleteAckP ack = stub.forwardDelete(CacheDeleteReqP.newBuilder().setKey(key).build());
        if (!ack.getOk()) {
            throw new IllegalStateException("forwardDelete rejected on " + node + ": " + ack.getMessage());
        }
        return ack.getDeleted();
    }

    // ------------------------------------------------------------------
    // 管理面：key 列举 / 前缀计数（fan-out 聚合用，失败降级）
    // ------------------------------------------------------------------

    /**
     * 列举对端节点持有的 key（ListKeys，server 流）。
     * 对端不可达/异常时仅日志并返回空列表（fan-out 部分失败降级，与 StorageGrpcClient.listBucket 同约定）。
     */
    /**
     * 统一命令转发：参数列表经 {@link Codec} 编码；语义错误以 {@link CacheCommandException}
     * 返回，路由/冻结错误仍走 {@link GatewayErrors} 还原为可重试异常。
     */
    public Object forwardCommand(NodeId node, CacheCommandRequest request) {
        CacheCommandReqP.Builder builder = CacheCommandReqP.newBuilder()
                .setCommand(request.command());
        if (!request.args().isEmpty()) {
            builder.setPayload(ByteString.copyFrom(Codec.encodeValue(request.args())));
        }
        try {
            CacheCommandAckP ack = stubFor(node).executeCommand(builder.build());
            if (!ack.getOk()) {
                throw new CacheCommandException(
                        ack.getCode().isEmpty() ? CacheCommandException.COMMAND_FAILED : ack.getCode(),
                        ack.getMessage());
            }
            return ack.getPayload().isEmpty() ? null : Codec.decodeValue(ack.getPayload().toByteArray());
        } catch (StatusRuntimeException e) {
            throw GatewayErrors.translate(e, "command", request.command());
        }
    }

    public List<CacheCapability.CacheKeyEntry> listKeys(NodeId node, String prefix, int limit) {
        List<CacheCapability.CacheKeyEntry> result = new ArrayList<>();
        try {
            CacheGrpc.CacheBlockingStub stub = stubFor(node);
            Iterator<CacheKeyEntryP> stream = stub.listKeys(CacheListKeysReqP.newBuilder()
                    .setPrefix(prefix == null ? "" : prefix)
                    .setLimit(limit)
                    .build());
            while (stream.hasNext()) {
                CacheKeyEntryP entry = stream.next();
                result.add(new CacheCapability.CacheKeyEntry(entry.getKey(), entry.getTtlMillis()));
            }
        } catch (StatusRuntimeException | IllegalArgumentException e) {
            LOG.warn("listKeys failed on {} prefix={}: {}", node, prefix, e.getMessage());
        }
        return result;
    }

    /**
     * 对端节点前缀计数（CountKeys）。失败降级返回 0 并日志。
     */
    public long countKeys(NodeId node, String prefix) {
        try {
            CacheGrpc.CacheBlockingStub stub = stubFor(node);
            CacheCountAckP ack = stub.countKeys(CacheCountReqP.newBuilder()
                    .setPrefix(prefix == null ? "" : prefix)
                    .build());
            return ack.getCount();
        } catch (StatusRuntimeException | IllegalArgumentException e) {
            LOG.warn("countKeys failed on {} prefix={}: {}", node, prefix, e.getMessage());
            return 0L;
        }
    }

    /**
     * 对端节点前缀计数（严格版）：失败<b>上抛</b>而非降级 0——供管理面每节点统计
     * 区分「节点不可达/能力未启动」与「0 条目」（降级版会把宕机误显示为空）。
     */
    public long countKeysStrict(NodeId node, String prefix) {
        CacheGrpc.CacheBlockingStub stub = stubFor(node);
        CacheCountAckP ack = stub.countKeys(CacheCountReqP.newBuilder()
                .setPrefix(prefix == null ? "" : prefix)
                .build());
        return ack.getCount();
    }

    /** 释放全部缓存通道（幂等）。 */
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

    /** 网关转发读应答：解码后的值 + 剩余 TTL（<=0 永不过期）。 */
    public record CacheValue(Object value, long ttlMillis) {
    }

    private CacheGrpc.CacheBlockingStub stubFor(NodeId node) {
        return CacheGrpc.newBlockingStub(channelFor(node))
                .withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
    }

    /** gRPC 消息上限（C27）：对齐集群 8MB 约定——默认 4MB 会拒绝本地允许的大 value 转发，
     *  造成"本地直写成功、远端转发失败"的拓扑漂移 */
    static final int MAX_MESSAGE_BYTES = 8 * 1024 * 1024;

    private ManagedChannel channelFor(NodeId node) {
        CachePeerDirectory.PeerEndpoint endpoint = directory.endpoint(node)
                .orElseThrow(() -> new IllegalArgumentException("no cache endpoint for node " + node));
        return channels.computeIfAbsent(endpoint.target(),
                key -> ManagedChannelBuilder.forTarget(key)
                        .usePlaintext()
                        .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                        .build());
    }
}
