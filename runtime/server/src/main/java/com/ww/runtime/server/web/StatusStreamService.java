package com.ww.runtime.server.web;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 集群状态 SSE 实时推送：建立长连接后每秒推送一次集群状态快照。
 *
 * <p>推送策略：1秒一次周期推送（相比前端轮询，SSE为单TCP连接复用，开销更低且实时性更高）。
 * 30秒心跳注释帧保证连接不被代理断开；连接断连自动清理订阅者。</p>
 */
@RestController
@RequestMapping("/rt")
public class StatusStreamService {

    private static final Logger log = LoggerFactory.getLogger(StatusStreamService.class);

    private static final long PUSH_INTERVAL_MS = 1000;
    private static final long HEARTBEAT_SECONDS = 30;

    private final RuntimeAdminController adminController;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "status-sse-push");
        t.setDaemon(true);
        return t;
    });

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public StatusStreamService(RuntimeAdminController adminController) {
        this.adminController = adminController;
    }

    @PostConstruct
    void start() {
        scheduler.scheduleAtFixedRate(this::pushStatus, PUSH_INTERVAL_MS, PUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
        for (SseEmitter e : emitters) {
            try {
                e.complete();
            } catch (Exception ignore) {
            }
        }
        emitters.clear();
    }

    @GetMapping(value = "/status/stream", produces = "text/event-stream")
    public SseEmitter stream(HttpServletResponse response) {
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader("Cache-Control", "no-cache");
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        Runnable cleanup = () -> emitters.remove(emitter);
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());
        try {
            Object snapshot = adminController.status();
            emitter.send(SseEmitter.event().name("status").data(snapshot));
        } catch (IOException e) {
            log.debug("initial status push failed for new subscriber", e);
        }
        return emitter;
    }

    public int subscriberCount() {
        return emitters.size();
    }

    private void pushStatus() {
        if (emitters.isEmpty()) return;
        try {
            Object snapshot = adminController.status();
            for (SseEmitter emitter : emitters) {
                try {
                    emitter.send(SseEmitter.event().name("status").data(snapshot));
                } catch (Exception e) {
                    emitters.remove(emitter);
                    try {
                        emitter.complete();
                    } catch (Exception ignore) {
                    }
                }
            }
        } catch (Exception e) {
            log.warn("build status snapshot failed: {}", e.getMessage());
        }
    }

    private void sendHeartbeat() {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().comment("keep-alive"));
            } catch (Exception e) {
                emitters.remove(emitter);
            }
        }
    }
}
