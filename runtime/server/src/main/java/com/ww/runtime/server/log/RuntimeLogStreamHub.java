package com.ww.runtime.server.log;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.proto.LogEventP;
import com.ww.runtime.proto.LogStreamGrpc;
import com.ww.runtime.proto.LogStreamReq;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 实时日志集群聚合 hub：前端订阅任意节点的 {@code /rt/log/stream} 后，由该节点作为 hub
 * 聚合本节点与所有对端节点的日志流。
 *
 * <p>本地日志走 {@link RuntimeLogStreamService} 现有订阅；对端日志经 gRPC
 * {@code LogStream.Stream}（对端 {@link LogStreamGrpcServer} 只推本节点日志、
 * 不递归 fan-out，天然无环路）拉取，逐帧转发到下游 emitter。上游断连按固定间隔重连，
 * 直至下游订阅结束。</p>
 *
 * <p>单机/无对端时无聚合，行为与纯本地订阅完全一致。</p>
 */
@Service
public class RuntimeLogStreamHub {

    private static final Logger log = LoggerFactory.getLogger(RuntimeLogStreamHub.class);

    /** 上游连接失败后的重连间隔（毫秒）。 */
    private static final long RECONNECT_INTERVAL_MS = 3000;

    private final RuntimeLogStreamService localService;
    private final NodeRegistry registry;
    private final NodeId self;
    private final ObjectMapper objectMapper;
    private final ExecutorService ioPool = Executors.newVirtualThreadPerTaskExecutor();

    public RuntimeLogStreamHub(RuntimeLogStreamService localService,
                               NodeRegistry registry,
                               NodeId self,
                               ObjectMapper objectMapper) {
        this.localService = localService;
        this.registry = registry;
        this.self = self;
        this.objectMapper = objectMapper;
    }

    /**
     * 建立聚合订阅：本地日志 + 对端节点日志。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空）
     */
    public SseEmitter subscribe(String level, String node, String keyword) {
        List<NodeInfo> peers = registry.listAlive().stream()
                .filter(n -> !n.id().equals(self))
                .toList();
        if (peers.isEmpty()) {
            return localService.subscribe(level, node, keyword, null);
        }
        AtomicBoolean closed = new AtomicBoolean(false);
        List<ManagedChannel> upstreams = new CopyOnWriteArrayList<>();
        SseEmitter emitter = localService.subscribe(level, node, keyword, () -> {
            closed.set(true);
            for (ManagedChannel channel : upstreams) {
                channel.shutdownNow();
            }
        });
        for (NodeInfo peer : peers) {
            ioPool.execute(() -> pumpPeer(peer, level, node, keyword, emitter, closed, upstreams));
        }
        return emitter;
    }

    @PreDestroy
    void stop() {
        ioPool.shutdownNow();
    }

    /**
     * 单个对端的转发循环：建 gRPC Stream 并逐帧转发；断连后按间隔重连，
     * 直至下游订阅关闭。
     */
    private void pumpPeer(NodeInfo peer, String level, String node, String keyword,
                          SseEmitter emitter, AtomicBoolean closed, List<ManagedChannel> upstreams) {
        while (!closed.get()) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(peer.host(), peer.grpcPort())
                    .usePlaintext()
                    .build();
            upstreams.add(channel);
            CountDownLatch finished = new CountDownLatch(1);
            try {
                LogStreamGrpc.newStub(channel).stream(buildRequest(level, node, keyword),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(LogEventP event) {
                                try {
                                    emitter.send(SseEmitter.event().data(toJson(event)));
                                } catch (Exception e) {
                                    // 下游连接已结束，终止该上游转发
                                    closed.set(true);
                                    finished.countDown();
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                log.debug("log stream from {} failed: {}", peer.id(), t.getMessage());
                                finished.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                finished.countDown();
                            }
                        });
                finished.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                log.debug("log stream to {} setup failed: {}", peer.id(), e.getMessage());
            } finally {
                upstreams.remove(channel);
                channel.shutdownNow();
            }
            if (closed.get()) {
                return;
            }
            try {
                Thread.sleep(RECONNECT_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static LogStreamReq buildRequest(String level, String node, String keyword) {
        return LogStreamReq.newBuilder()
                .setLevel(level == null ? "" : level)
                .setNode(node == null ? "" : node)
                .setKeyword(keyword == null ? "" : keyword)
                .build();
    }

    /** LogEventP → 与本地 LogRecord JSON 同构的 camelCase 帧。 */
    private String toJson(LogEventP event) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("level", event.getLevel());
        body.put("loggerName", event.getLoggerName());
        body.put("threadName", event.getThreadName());
        body.put("message", event.getMessage());
        body.put("stackTrace", event.getStackTrace().isEmpty() ? null : event.getStackTrace());
        body.put("node", event.getNode());
        body.put("timestamp", event.getTimestamp());
        try {
            return objectMapper.writeValueAsString(body);
        } catch (Exception e) {
            log.debug("log event serialize failed: {}", e.getMessage());
            return "{}";
        }
    }
}
