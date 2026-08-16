package com.ww.admin.service.log;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import tools.jackson.databind.ObjectMapper;

/**
 * 实时日志 SSE 推送服务：维护前端订阅者集合，把 {@link LogBatchWriter} 落库后的日志按订阅过滤条件广播。
 * <p>
 * 订阅者集合用 {@link CopyOnWriteArrayList}（订阅/退订低频、广播高频）；每个订阅绑定可选的
 * level / node / keyword 过滤条件，仅推送匹配的日志。连接断连（complete/timeout/error）自动移除订阅。
 * <p>
 * 通过周期心跳（SSE 注释帧）保活，避免长时间无日志时连接被代理/负载均衡断开。
 */
@Service
public class LogStreamService {

    private static final Logger log = LoggerFactory.getLogger(LogStreamService.class);

    private static final long HEARTBEAT_SECONDS = 30;

    private final ObjectMapper objectMapper;
    private final CopyOnWriteArrayList<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "log-sse-heartbeat");
        t.setDaemon(true);
        return t;
    });

    public LogStreamService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void startHeartbeat() {
        heartbeat.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        heartbeat.shutdownNow();
        for (Subscription s : subscriptions) {
            try {
                s.emitter().complete();
            } catch (Exception ignore) {
            }
        }
        subscriptions.clear();
    }

    /**
     * 注册一个实时订阅。
     *
     * @param level   级别过滤（可空，全部级别）
     * @param node    节点过滤（可空，全部节点）
     * @param keyword 关键字过滤（可空，匹配 message/loggerName）
     */
    public SseEmitter subscribe(String level, String node, String keyword) {
        return subscribe(level, node, keyword, null);
    }

    /**
     * 注册一个实时订阅，并在连接结束（complete/timeout/error）时触发关闭回调。
     *
     * @param onClose 连接结束回调（可空）；供集群聚合 hub 关闭对端上游连接
     */
    public SseEmitter subscribe(String level, String node, String keyword, Runnable onClose) {
        SseEmitter emitter = new SseEmitter(0L);
        Subscription sub = new Subscription(emitter, level, node, keyword);
        subscriptions.add(sub);
        Runnable cleanup = () -> {
            subscriptions.remove(sub);
            if (onClose != null) {
                try {
                    onClose.run();
                } catch (Exception e) {
                    log.debug("实时日志订阅关闭回调异常：{}", e.getMessage());
                }
            }
        };
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());
        return emitter;
    }

    /**
     * 批量广播日志：每条日志序列化一次，推送给所有匹配的订阅者。
     */
    public void publish(List<LogRecord> records) {
        if (records.isEmpty() || subscriptions.isEmpty()) {
            return;
        }
        for (LogRecord record : records) {
            String json = toJson(record);
            if (json == null) {
                continue;
            }
            for (Subscription sub : subscriptions) {
                if (!matches(sub, record)) {
                    continue;
                }
                try {
                    sub.emitter().send(SseEmitter.event().data(json));
                } catch (Exception e) {
                    // 客户端断连或发送失败：移除订阅
                    subscriptions.remove(sub);
                    try {
                        sub.emitter().complete();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
    }

    /** 订阅者数（供监控/调试）。 */
    public int subscriberCount() {
        return subscriptions.size();
    }

    private boolean matches(Subscription sub, LogRecord r) {
        if (sub.level() != null && !sub.level().isEmpty()
                && !sub.level().equalsIgnoreCase(r.level())) {
            return false;
        }
        if (sub.node() != null && !sub.node().isEmpty()
                && !sub.node().equals(r.node())) {
            return false;
        }
        if (sub.keyword() != null && !sub.keyword().isEmpty()) {
            String message = r.message() == null ? "" : r.message();
            String logger = r.loggerName() == null ? "" : r.loggerName();
            if (!message.contains(sub.keyword()) && !logger.contains(sub.keyword())) {
                return false;
            }
        }
        return true;
    }

    private String toJson(LogRecord record) {
        try {
            return objectMapper.writeValueAsString(record);
        } catch (Exception e) {
            log.warn("实时日志序列化失败：{}", e.getMessage());
            return null;
        }
    }

    private void sendHeartbeat() {
        for (Subscription sub : subscriptions) {
            try {
                sub.emitter().send(SseEmitter.event().comment("keep-alive"));
            } catch (Exception e) {
                subscriptions.remove(sub);
                try {
                    sub.emitter().complete();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /** 单个订阅：绑定 SSE 连接与其过滤条件。 */
    private record Subscription(SseEmitter emitter, String level, String node, String keyword) {
    }
}
