package com.ww.runtime.data.cache;

import com.google.protobuf.ByteString;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * CACHE 数据面 gRPC 服务端：挂载于每个 CACHE 节点的 {@code GrpcServerHost}，
 * 暴露网关转发 ForwardPut/ForwardPutIfAbsent/ForwardGet/ForwardDelete（属主端执行 fencing 语义）。
 *
 * <p>值经 {@link Codec} 编解码为 bytes（与迁移通道同内网信任边界）。
 *
 * <h2>错误映射（与 StorageGrpcServer 同约定，网关按标记还原 HTTP 语义）</h2>
 * stale-epoch → ABORTED（HTTP 409）；frozen → UNAVAILABLE（HTTP 423）；其余 → INTERNAL。
 */
public final class CacheGrpcServer extends CacheGrpc.CacheImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(CacheGrpcServer.class);

    /** 单次列举上限（管理面防御）。 */
    private static final int MAX_LIST_LIMIT = 10_000;

    private final CacheCapability capability;
    private final CacheCommandExecutor commandExecutor;

    public CacheGrpcServer(CacheCapability capability) {
        this.capability = Objects.requireNonNull(capability, "capability");
        this.commandExecutor = new CacheCommandExecutor(capability);
    }

    @Override
    public void forwardPut(CachePutReqP request, StreamObserver<CachePutAckP> responseObserver) {
        String key = request.getKey();
        try {
            Object value = Codec.decodeValue(request.getValue().toByteArray());
            capability.put(key, value, request.getTtlMillis());
            responseObserver.onNext(CachePutAckP.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("forwardPut failed: {}: {}", key, e.getMessage());
            responseObserver.onError(asStatus("forwardPut", e));
        }
    }

    @Override
    public void forwardPutIfAbsent(CachePutReqP request,
                                   StreamObserver<CachePutIfAbsentAckP> responseObserver) {
        String key = request.getKey();
        try {
            Object value = Codec.decodeValue(request.getValue().toByteArray());
            boolean applied = capability.putIfAbsent(key, value, request.getTtlMillis());
            responseObserver.onNext(CachePutIfAbsentAckP.newBuilder()
                    .setOk(true)
                    .setApplied(applied)
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("forwardPutIfAbsent failed: {}: {}", key, e.getMessage());
            responseObserver.onError(asStatus("forwardPutIfAbsent", e));
        }
    }

    @Override
    public void forwardGet(CacheGetReqP request, StreamObserver<CacheValueP> responseObserver) {
        String key = request.getKey();
        try {
            // 读路由纠偏：cutover 后属主已非本节点 → not-owner 拒绝（ABORTED），
            // 网关重取属主重试一次收敛到新属主（调用端无感）。无放置（单机退化）仍本地服务。
            if (!capability.servesPartitionOf(key)) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("not-owner: partition of '" + key + "' owned elsewhere")
                        .asException());
                return;
            }
            Object value = capability.get(key);
            if (value == null) {
                responseObserver.onNext(CacheValueP.newBuilder().setExists(false).build());
            } else {
                long ttl = capability.remainingTtlMillis(key);
                // Long.MAX_VALUE 永不过期 → 归一为 <=0 语义
                long normalized = ttl == Long.MAX_VALUE ? 0L : ttl;
                responseObserver.onNext(CacheValueP.newBuilder()
                        .setExists(true)
                        .setValue(ByteString.copyFrom(Codec.encodeValue(value)))
                        .setTtlMillis(normalized)
                        .build());
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("forwardGet failed: {}: {}", key, e.getMessage());
            responseObserver.onError(asStatus("forwardGet", e));
        }
    }

    @Override
    public void forwardDelete(CacheDeleteReqP request, StreamObserver<CacheDeleteAckP> responseObserver) {
        String key = request.getKey();
        try {
            boolean deleted = capability.remove(key);
            responseObserver.onNext(CacheDeleteAckP.newBuilder()
                    .setOk(true)
                    .setDeleted(deleted)
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("forwardDelete failed: {}: {}", key, e.getMessage());
            responseObserver.onError(asStatus("forwardDelete", e));
        }
    }

    // ------------------------------------------------------------------
    // 统一命令通道
    // ------------------------------------------------------------------

    @Override
    public void executeCommand(CacheCommandReqP request,
                               StreamObserver<CacheCommandAckP> responseObserver) {
        try {
            List<Object> args = new ArrayList<>();
            if (!request.getPayload().isEmpty()) {
                Object decoded = Codec.decodeValue(request.getPayload().toByteArray());
                if (decoded instanceof List<?> list) {
                    args.addAll(list);
                } else if (decoded != null) {
                    responseObserver.onNext(CacheCommandAckP.newBuilder()
                            .setOk(false)
                            .setCode(CacheCommandException.INVALID_ARGUMENT)
                            .setMessage("command payload must be a List")
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
            }
            CacheCommandResponse response = commandExecutor.execute(
                    new CacheCommandRequest(request.getCommand(), args));
            if (!response.ok()) {
                responseObserver.onNext(CacheCommandAckP.newBuilder()
                        .setOk(false)
                        .setCode(response.code() == null ? CacheCommandException.COMMAND_FAILED : response.code())
                        .setMessage(response.message() == null ? "" : response.message())
                        .build());
                responseObserver.onCompleted();
                return;
            }
            byte[] payload = response.result() == null
                    ? new byte[0] : Codec.encodeValue(response.result());
            responseObserver.onNext(CacheCommandAckP.newBuilder()
                    .setOk(true)
                    .setPayload(ByteString.copyFrom(payload))
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("executeCommand failed: {}: {}", request.getCommand(), e.getMessage());
            responseObserver.onError(asStatus("executeCommand", e));
        }
    }

    // ------------------------------------------------------------------
    // 管理面：key 列举 / 前缀计数（不做 fencing，fan-out 聚合用）
    // ------------------------------------------------------------------

    /** 本节点 key 列举（server 流）：limit<=0 钳制为上限 10000，防御管理面大列举。
     * 能力未启动（节点被禁用、数据已迁出）返回空流——该节点贡献 0 条是正确语义
     * （网关 fan-out 聚合用，不视为错误）。 */
    @Override
    public void listKeys(CacheListKeysReqP request, StreamObserver<CacheKeyEntryP> responseObserver) {
        if (!capability.isStarted()) {
            responseObserver.onCompleted();
            return;
        }
        String prefix = request.getPrefix().isEmpty() ? null : request.getPrefix();
        int limit = request.getLimit() <= 0 ? MAX_LIST_LIMIT : Math.min(request.getLimit(), MAX_LIST_LIMIT);
        try {
            List<CacheCapability.CacheKeyEntry> entries = capability.listKeys(prefix, limit);
            for (CacheCapability.CacheKeyEntry entry : entries) {
                responseObserver.onNext(CacheKeyEntryP.newBuilder()
                        .setKey(entry.key())
                        .setTtlMillis(entry.ttlMillis())
                        .build());
            }
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("listKeys failed: prefix={}: {}", prefix, e.getMessage());
            responseObserver.onError(asStatus("listKeys", e));
        }
    }

    /** 本节点前缀计数（unary）；能力未启动返回 0（同 {@link #listKeys} 约定）。 */
    @Override
    public void countKeys(CacheCountReqP request, StreamObserver<CacheCountAckP> responseObserver) {
        if (!capability.isStarted()) {
            responseObserver.onNext(CacheCountAckP.newBuilder().setCount(0L).build());
            responseObserver.onCompleted();
            return;
        }
        String prefix = request.getPrefix().isEmpty() ? null : request.getPrefix();
        try {
            long count = capability.countKeys(prefix);
            responseObserver.onNext(CacheCountAckP.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.warn("countKeys failed: prefix={}: {}", prefix, e.getMessage());
            responseObserver.onError(asStatus("countKeys", e));
        }
    }

    /** 幂等关闭（与 StorageGrpcServer.close 对齐；本服务无独立线程池，为对称占位）。 */
    public void close() {
        // 一元 RPC 运行于 gRPC 线程，无自建线程池需释放
    }

    /**
     * 能力层异常 → gRPC 状态映射（与 StorageGrpcServer.asStatus 同约定）：
     * stale-epoch → ABORTED；frozen → UNAVAILABLE；其余 → INTERNAL。
     */
    private static Throwable asStatus(String op, Throwable t) {
        if (t instanceof StaleEpochException) {
            return Status.ABORTED
                    .withDescription("stale-epoch: " + t.getMessage())
                    .asException();
        }
        if (t instanceof RuntimeFrameworkException && "frozen".equals(t.getMessage())) {
            return Status.UNAVAILABLE
                    .withDescription("frozen")
                    .asException();
        }
        return Status.INTERNAL
                .withDescription(op + " failed: " + t.getMessage())
                .asException();
    }
}
