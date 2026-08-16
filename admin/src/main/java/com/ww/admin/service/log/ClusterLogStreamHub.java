package com.ww.admin.service.log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ww.cache.ClusterMembership;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;

import jakarta.annotation.PreDestroy;

/**
 * 实时日志集群聚合 hub：前端订阅任意节点的 {@code /log/stream} 后，由该节点作为 hub
 * 聚合本节点与所有对端节点的日志流。
 * <p>
 * 本地日志走 {@link LogStreamService} 现有订阅；对端日志经
 * {@code /internal/log/stream}（{@code X-Cache-Token} 校验）建立上游 SSE 连接，
 * 将 {@code data:} 帧的 JSON 原样转发到下游 emitter。对端内部端点只做本地推送，
 * 不递归 fan-out，天然无环路。上游断连按固定间隔重连，直至下游订阅结束。
 * <p>
 * 单机/未启用集群时无对端节点，行为与纯本地订阅完全一致。
 */
@Service
public class ClusterLogStreamHub {

    private static final Logger log = LoggerFactory.getLogger(ClusterLogStreamHub.class);

    /** 节点间内部端点认证头（与 /internal/cache、/internal/storage 一致） */
    private static final String TOKEN_HEADER = "X-Cache-Token";
    /** 上游连接失败后的重连间隔（毫秒） */
    private static final long RECONNECT_INTERVAL_MS = 3000;
    /** 上游 TCP 连接超时（毫秒），读取本身不限时（靠对端 30s 心跳保活） */
    private static final int CONNECT_TIMEOUT_MS = 3000;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT_MS))
            .build();

    private final LogStreamService logStreamService;
    private final ExecutorService ioPool = Executors.newVirtualThreadPerTaskExecutor();

    @Autowired(required = false)
    private ClusterMembership membership;

    @Autowired(required = false)
    private ClusterProperties clusterProperties;

    @Autowired
    private CachePortProperties cachePortProperties;

    @Value("${server.port:8080}")
    private int serverPort;

    public ClusterLogStreamHub(LogStreamService logStreamService) {
        this.logStreamService = logStreamService;
    }

    /**
     * 建立聚合订阅：本地日志 + 对端节点日志。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空）
     */
    public SseEmitter subscribe(String level, String node, String keyword) {
        List<ClusterNode> peers = peerNodes();
        if (peers.isEmpty()) {
            return logStreamService.subscribe(level, node, keyword);
        }
        AtomicBoolean closed = new AtomicBoolean(false);
        List<InputStream> upstreams = new CopyOnWriteArrayList<>();
        SseEmitter emitter = logStreamService.subscribe(level, node, keyword, () -> {
            closed.set(true);
            for (InputStream in : upstreams) {
                closeQuietly(in);
            }
        });
        for (ClusterNode peer : peers) {
            ioPool.execute(() -> pumpPeer(peer, level, node, keyword, emitter, closed, upstreams));
        }
        return emitter;
    }

    @PreDestroy
    void stop() {
        ioPool.shutdownNow();
    }

    /**
     * 单个对端的转发循环：连接对端内部日志流并逐行转发 data 帧；断连后按间隔重连，
     * 直至下游订阅关闭。
     */
    private void pumpPeer(ClusterNode peer, String level, String node, String keyword,
                          SseEmitter emitter, AtomicBoolean closed, List<InputStream> upstreams) {
        String url = buildStreamUrl(peer, level, node, keyword);
        while (!closed.get()) {
            try {
                InputStream in = openStream(url);
                upstreams.add(in);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while (!closed.get() && (line = reader.readLine()) != null) {
                        if (!line.startsWith("data:")) {
                            continue; // 跳过注释帧（心跳）与空行
                        }
                        String payload = line.substring(5).trim();
                        if (payload.isEmpty()) {
                            continue;
                        }
                        try {
                            emitter.send(SseEmitter.event().data(payload));
                        } catch (Exception e) {
                            // 下游连接已结束，终止该上游转发
                            closed.set(true);
                            return;
                        }
                    }
                } finally {
                    upstreams.remove(in);
                }
                log.debug("实时日志上游 {} 断开", url);
            } catch (Exception e) {
                log.debug("实时日志上游 {} 连接/读取失败：{}", url, e.getMessage());
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

    /** 打开对端内部日志流；非 200 视为失败抛异常（走重连）。 */
    private InputStream openStream(String url) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "text/event-stream")
                .GET();
        String token = cachePortProperties.getAuthToken();
        if (token != null && !token.isEmpty()) {
            builder.header(TOKEN_HEADER, token);
        }
        HttpResponse<InputStream> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() != 200) {
            resp.body().close();
            throw new IllegalStateException("对端返回 " + resp.statusCode());
        }
        return resp.body();
    }

    /** 构造对端内部日志流 URL（过滤参数随请求下发，对端就地过滤减少转发量）。 */
    private String buildStreamUrl(ClusterNode peer, String level, String node, String keyword) {
        StringBuilder sb = new StringBuilder("http://")
                .append(peer.getHost()).append(':').append(adminHttpPort(peer))
                .append("/internal/log/stream");
        StringBuilder qs = new StringBuilder();
        appendParam(qs, "level", level);
        appendParam(qs, "node", node);
        appendParam(qs, "keyword", keyword);
        if (!qs.isEmpty()) {
            sb.append('?').append(qs);
        }
        return sb.toString();
    }

    private static void appendParam(StringBuilder qs, String name, String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        if (!qs.isEmpty()) {
            qs.append('&');
        }
        qs.append(name).append('=').append(URLEncoder.encode(value, StandardCharsets.UTF_8));
    }

    /** 对端节点列表：当前节点视图中排除 self 与 DEAD 节点 */
    private List<ClusterNode> peerNodes() {
        List<ClusterNode> result = new ArrayList<>();
        for (ClusterNode n : currentNodeList()) {
            if (n.isSelf() || n.getStatus() == ClusterNode.Status.DEAD) {
                continue;
            }
            result.add(n);
        }
        return result;
    }

    /** 当前节点列表：优先动态 membership，退化为静态配置（与 CacheAdminService 一致） */
    private List<ClusterNode> currentNodeList() {
        if (membership != null) {
            return membership.getAllNodes();
        }
        if (clusterProperties != null
                && clusterProperties.getNodes() != null
                && !clusterProperties.getNodes().isEmpty()) {
            return clusterProperties.getNodes();
        }
        return List.of();
    }

    /** 对端 admin HTTP 端口推导：优先显式 httpPort，否则 node.port + (server.port - cache.port) */
    private int adminHttpPort(ClusterNode n) {
        if (n.getHttpPort() > 0) {
            return n.getHttpPort();
        }
        int cachePort = cachePortProperties != null ? cachePortProperties.getPort() : 0;
        int offset = (cachePort > 0 && serverPort > 0) ? serverPort - cachePort : 0;
        int port = n.getPort() + offset;
        return port > 0 ? port : serverPort;
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (Exception ignore) {
        }
    }
}
