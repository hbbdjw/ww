package com.ww.runtime.server.metrics;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 运维监控：接口负载 SSE 实时推送。
 *
 * <p>建立 {@code GET /rt/metrics/stream} 长连接后，每秒推送一帧全集群「上一封账秒」
 * 各节点接收到的 HTTP 请求数（req/s），前端每节点一条折线。</p>
 *
 * <h2>集群聚合模型</h2>
 * <ul>
 *   <li>本地计数直接读 {@link RequestMetricsCollector}（本节点秒桶）；</li>
 *   <li>对端计数经 HTTP 并行拉取 {@code GET /rt/metrics/request-rate?second=}<em>s</em>
 *       （虚拟线程 fan-out，单请求超时有界），以本 hub 的统一秒标签对齐，
 *       保证各节点折线在同一时间轴上可比（同 Prometheus pull 的服务端时间戳语义）；</li>
 *   <li>对端拉取复用订阅者的 Bearer token——会话存于集群 CACHE，同一不透明令牌
 *       在所有节点均可解析；SSE 存活期间鉴权过滤器的滑动续期使会话不会超时；</li>
 *   <li>对端不可达/超时/未上报 HTTP 端口时该秒 rps=null（前端折线缺口），不拖垮整帧；</li>
 *   <li>无订阅者时跳过全部采集，避免空闲集群的无谓轮询。</li>
 * </ul>
 *
 * <p>推送节奏对齐整秒边界（+50ms 余量），30 秒心跳注释帧防代理断连；
 * 断连 emitter 自动清理。</p>
 */
@RestController
@RequestMapping("/rt/metrics")
public class MetricsStreamController {

    private static final Logger log = LoggerFactory.getLogger(MetricsStreamController.class);

    private static final long PUSH_INTERVAL_MS = 1000;
    private static final long HEARTBEAT_SECONDS = 30;
    /** 对端单次指标拉取总预算（须小于推送周期，避免任务重叠堆积）。 */
    private static final long FETCH_TIMEOUT_MS = 800;
    private static final long CONNECT_TIMEOUT_MS = 300;

    /** 对端指标拉取共享 HTTP 客户端（HTTP/2 + 连接复用，生命周期与进程一致）。 */
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT_MS))
            .build();

    private final RequestMetricsCollector collector;
    private final NodeRegistry registry;
    private final NodeId self;
    private final ObjectMapper objectMapper;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-sse-push");
        t.setDaemon(true);
        return t;
    });

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /** 最近一个订阅者的 Authorization 头原值（含 "Bearer " 前缀），用作对端拉取凭证。 */
    private final AtomicReference<String> upstreamToken = new AtomicReference<>();

    public MetricsStreamController(RequestMetricsCollector collector,
                                   NodeRegistry registry,
                                   NodeId self,
                                   ObjectMapper objectMapper) {
        this.collector = collector;
        this.registry = registry;
        this.self = self;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void start() {
        long now = System.currentTimeMillis();
        // 对齐下一整秒后留 50ms 余量，确保读取的「上一秒」桶在各节点均已封账
        long initialDelay = PUSH_INTERVAL_MS - (now % PUSH_INTERVAL_MS) + 50;
        scheduler.scheduleAtFixedRate(this::publish, initialDelay, PUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.complete();
            } catch (Exception ignore) {
                // 停机清理，忽略已关闭的 emitter
            }
        }
        emitters.clear();
    }

    /**
     * 全集群请求速率 SSE 流：每秒一帧 {@code metrics} 事件。
     * 帧体：{@code {timestamp, second, intervalMs, nodes:[{nodeId,state,rps}]}}。
     */
    @GetMapping(value = "/stream", produces = "text/event-stream")
    public SseEmitter stream(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");

        String token = resolveBearer(request);
        if (token != null) {
            upstreamToken.set(token);
        }

        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        Runnable cleanup = () -> emitters.remove(emitter);
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());

        try {
            emitter.send(SseEmitter.event().name("metrics").data(buildSnapshot(collector.lastCompletedSecond())));
        } catch (IOException e) {
            log.debug("initial metrics push failed for new subscriber: {}", e.getMessage());
        }
        return emitter;
    }

    /**
     * 本节点秒级请求速率（供集群内对端 hub 拉取，同样在认证链路上，需 Bearer 令牌）。
     *
     * @param second 指定 epochSecond（hub 以统一秒标签对齐）；缺省取上一封账秒
     */
    @GetMapping("/request-rate")
    public Map<String, Object> requestRate(@RequestParam(required = false) Long second) {
        long sec = second != null ? second : collector.lastCompletedSecond();
        long count = collector.countForSecond(sec);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("nodeId", self.value());
        body.put("second", sec);
        body.put("rps", count == RequestMetricsCollector.NO_DATA ? null : count);
        body.put("gathered", count != RequestMetricsCollector.NO_DATA);
        return body;
    }

    /** 订阅者数量（可观测/测试用）。 */
    public int subscriberCount() {
        return emitters.size();
    }

    private void publish() {
        if (emitters.isEmpty()) {
            return;
        }
        long second = collector.lastCompletedSecond();
        Map<String, Object> snapshot;
        try {
            snapshot = buildSnapshot(second);
        } catch (Exception e) {
            log.warn("build metrics snapshot failed: {}", e.getMessage());
            return;
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("metrics").data(snapshot));
            } catch (Exception e) {
                emitters.remove(emitter);
                try {
                    emitter.complete();
                } catch (Exception ignore) {
                    // 对端已关闭，忽略
                }
            }
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

    /**
     * 构建一帧全集群速率快照：本地秒桶 + 对端并行拉取（虚拟线程 fan-out）。
     * 包私有以便单元测试直接断言组装结果。
     */
    Map<String, Object> buildSnapshot(long second) {
        List<NodeInfo> peers = registry.listAlive().stream()
                .filter(n -> !n.id().equals(self))
                .sorted(Comparator.comparing(n -> n.id().value()))
                .toList();

        String token = upstreamToken.get();
        List<NodeRate> rates = new ArrayList<>(peers.size() + 1);

        // 本节点始终在场（即使启动早期注册表里尚未出现 ACTIVE 自注册行）
        long localCount = collector.countForSecond(second);
        String selfState = registry.get(self).map(n -> n.state().name()).orElse("JOINING");
        rates.add(new NodeRate(self.value(), selfState,
                localCount == RequestMetricsCollector.NO_DATA ? null : localCount));

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<NodeRate>> futures = new ArrayList<>(peers.size());
            for (NodeInfo peer : peers) {
                futures.add(pool.submit(() -> fetchPeerRate(peer, second, token)));
            }
            for (Future<NodeRate> future : futures) {
                try {
                    rates.add(future.get(FETCH_TIMEOUT_MS + 200L, TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    // 单个对端超时已在 fetchPeerRate 内部降级；此处为防御性兜底
                    log.debug("collect peer rate failed: {}", e.toString());
                }
            }
        }
        rates.sort(Comparator.comparing(NodeRate::nodeId));

        List<Map<String, Object>> nodes = new ArrayList<>(rates.size());
        for (NodeRate rate : rates) {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("nodeId", rate.nodeId());
            node.put("state", rate.state());
            node.put("rps", rate.rps());
            nodes.add(node);
        }

        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("timestamp", (second + 1) * 1000L);
        snapshot.put("second", second);
        snapshot.put("intervalMs", PUSH_INTERVAL_MS);
        snapshot.put("nodes", nodes);
        return snapshot;
    }

    /** 拉取单个对端的秒桶计数；任何失败（不可达/超时/HTTP 非 200/无令牌/未上报端口）均降级为 rps=null。 */
    private NodeRate fetchPeerRate(NodeInfo peer, long second, String token) {
        NodeRate unavailable = new NodeRate(peer.id().value(), peer.state().name(), null);
        if (token == null || token.isBlank() || peer.httpPort() <= 0) {
            return unavailable;
        }
        try {
            URI uri = URI.create("http://" + peer.host() + ":" + peer.httpPort()
                    + "/rt/metrics/request-rate?second=" + second);
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofMillis(FETCH_TIMEOUT_MS))
                    .header(HttpHeaders.ACCEPT, "application/json")
                    .header(HttpHeaders.AUTHORIZATION, token)
                    .GET()
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                return unavailable;
            }
            JsonNode json = objectMapper.readTree(response.body());
            JsonNode rpsNode = json.get("rps");
            if (!json.path("gathered").asBoolean(false) || rpsNode == null || rpsNode.isNull()) {
                return unavailable;
            }
            return new NodeRate(peer.id().value(), peer.state().name(), rpsNode.asLong());
        } catch (Exception e) {
            // 对端瞬断是集群常态（滚动重启/故障注入），降级为缺口即可，不刷告警日志
            log.debug("fetch request-rate from {} failed: {}", peer.id(), e.toString());
            return unavailable;
        }
    }

    /** 从 SSE 初始请求解析 Bearer 令牌：优先 Authorization 头，GET 回退 access_token 查询参数。 */
    private static String resolveBearer(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header != null && !header.isBlank()) {
            return header.trim();
        }
        String queryToken = request.getParameter("access_token");
        if (queryToken != null && !queryToken.isBlank()) {
            return "Bearer " + queryToken.trim();
        }
        return null;
    }

    /** 单节点速率帧：rps=null 表示该秒无数据（对端不可达/桶缺失），前端表现为折线缺口。 */
    record NodeRate(String nodeId, String state, Long rps) {
    }
}
