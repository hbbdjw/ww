package com.ww.runtime.server.log;

import com.ww.runtime.log.LogBus;
import com.ww.runtime.log.LogRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 实时日志 SSE 推送服务：把 {@link LogBus} 广播的日志经 SseEmitter 推给前端订阅者。
 *
 * <p>每个订阅绑定可选的 level/node/keyword 过滤条件（过滤在 {@link LogBus} 内完成）。
 * 连接断连（complete/timeout/error）自动退订。通过周期心跳（SSE 注释帧）保活，
 * 避免长时间无日志时连接被代理/负载均衡断开。</p>
 */
@Service
public class RuntimeLogStreamService {

    private static final Logger log = LoggerFactory.getLogger(RuntimeLogStreamService.class);

    private static final long HEARTBEAT_SECONDS = 30;

    private final LogBus logBus;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "log-sse-heartbeat");
        t.setDaemon(true);
        return t;
    });

    /** 活跃 SSE 订阅（emitter + 总线句柄），心跳时探活、断连清理。 */
    private final java.util.List<SseSubscription> emitters = new java.util.concurrent.CopyOnWriteArrayList<>();

    public RuntimeLogStreamService(LogBus logBus, ObjectMapper objectMapper) {
        this.logBus = logBus;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void startHeartbeat() {
        heartbeat.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        heartbeat.shutdownNow();
        for (SseSubscription s : emitters) {
            closeQuietly(s);
        }
        emitters.clear();
    }

    /**
     * 注册一个实时订阅。
     *
     * @param level   级别过滤（可空，全部级别）
     * @param node    节点过滤（可空，全部节点）
     * @param keyword 关键字过滤（可空，四字段匹配）
     * @param onClose 连接结束回调（可空）；供集群聚合 hub 关闭对端上游连接
     */
    public SseEmitter subscribe(String level, String node, String keyword, Runnable onClose) {
        SseEmitter emitter = new SseEmitter(0L);
        LogBus.Subscription busSub = logBus.subscribe(level, node, keyword,
                record -> sendRecord(emitter, record), null);
        SseSubscription sub = new SseSubscription(emitter, busSub, onClose);
        emitters.add(sub);
        Runnable cleanup = () -> closeQuietly(sub);
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());
        return emitter;
    }

    /** 订阅者数（供监控/调试）。 */
    public int subscriberCount() {
        return emitters.size();
    }

    private void sendRecord(SseEmitter emitter, LogRecord record) {
        try {
            String json = objectMapper.writeValueAsString(record);
            emitter.send(SseEmitter.event().data(json));
        } catch (Exception e) {
            // 客户端断连或发送失败：由 emitter 回调统一清理
            try {
                emitter.complete();
            } catch (Exception ignore) {
            }
        }
    }

    private void closeQuietly(SseSubscription sub) {
        emitters.remove(sub);
        logBus.unsubscribe(sub.busSub());
        Runnable onClose = sub.onClose();
        if (onClose != null) {
            try {
                onClose.run();
            } catch (Exception e) {
                log.debug("log sse close callback failed: {}", e.getMessage());
            }
        }
        try {
            sub.emitter().complete();
        } catch (Exception ignore) {
        }
    }

    private void sendHeartbeat() {
        for (SseSubscription sub : emitters) {
            try {
                sub.emitter().send(SseEmitter.event().comment("keep-alive"));
            } catch (Exception e) {
                closeQuietly(sub);
            }
        }
    }

    /** 单个订阅：SSE 连接 + 总线句柄 + 关闭回调。 */
    private record SseSubscription(SseEmitter emitter, LogBus.Subscription busSub, Runnable onClose) {
    }
}
