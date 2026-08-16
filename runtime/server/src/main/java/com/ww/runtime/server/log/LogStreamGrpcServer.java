package com.ww.runtime.server.log;

import com.ww.runtime.log.LogBus;
import com.ww.runtime.log.LogRecord;
import com.ww.runtime.proto.LogEventP;
import com.ww.runtime.proto.LogStreamGrpc;
import com.ww.runtime.proto.LogStreamReq;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 节点日志流 gRPC 服务端：对端节点（聚合 hub）经 server-streaming 订阅本节点日志。
 *
 * <p>只推<b>本节点</b>日志（LogBus 仅收本节点采集），不递归 fan-out，天然无环路。
 * 过滤参数随请求就地生效，减少转发量。客户端取消（Context CANCELLED）即退订。</p>
 */
public class LogStreamGrpcServer extends LogStreamGrpc.LogStreamImplBase {

    private static final Logger log = LoggerFactory.getLogger(LogStreamGrpcServer.class);

    private final LogBus logBus;

    public LogStreamGrpcServer(LogBus logBus) {
        this.logBus = Objects.requireNonNull(logBus, "logBus");
    }

    @Override
    public void stream(LogStreamReq request, StreamObserver<LogEventP> responseObserver) {
        LogBus.Subscription sub = logBus.subscribe(
                emptyToNull(request.getLevel()),
                emptyToNull(request.getNode()),
                emptyToNull(request.getKeyword()),
                record -> {
                    try {
                        responseObserver.onNext(toProto(record));
                    } catch (RuntimeException e) {
                        // 下游流已关闭：交由 CANCELLED 监听统一退订
                        log.debug("log stream onNext failed: {}", e.getMessage());
                    }
                },
                null);
        Context.current().addListener(cancelledContext -> {
            logBus.unsubscribe(sub);
            try {
                responseObserver.onCompleted();
            } catch (RuntimeException ignore) {
                // 流可能已被客户端侧关闭
            }
        }, Runnable::run);
    }

    private static LogEventP toProto(LogRecord r) {
        return LogEventP.newBuilder()
                .setLevel(r.level() == null ? "" : r.level())
                .setLoggerName(r.loggerName() == null ? "" : r.loggerName())
                .setThreadName(r.threadName() == null ? "" : r.threadName())
                .setMessage(r.message() == null ? "" : r.message())
                .setStackTrace(r.stackTrace() == null ? "" : r.stackTrace())
                .setNode(r.node() == null ? "" : r.node())
                .setTimestamp(r.timestamp())
                .build();
    }

    private static String emptyToNull(String value) {
        return (value == null || value.isEmpty()) ? null : value;
    }
}
