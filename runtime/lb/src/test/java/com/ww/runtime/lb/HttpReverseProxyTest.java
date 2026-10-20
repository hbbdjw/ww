package com.ww.runtime.lb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpReverseProxyTest {

    private HttpServer server;
    private String baseUrl;
    private final AtomicReference<HttpExchange> lastExchange = new AtomicReference<>();
    private final AtomicReference<byte[]> lastBody = new AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/echo", new EchoHandler());
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** 回显：method/path/query/关键头 + 原样 body；记录到 AtomicReference 供断言。 */
    class EchoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            lastExchange.set(exchange);
            byte[] body = exchange.getRequestBody().readAllBytes();
            lastBody.set(body);
            String resp = "method=" + exchange.getRequestMethod()
                    + "|uri=" + exchange.getRequestURI().getRawPath()
                    + "|query=" + exchange.getRequestURI().getRawQuery()
                    + "|auth=" + exchange.getRequestHeaders().getFirst("Authorization")
                    + "|meta=" + exchange.getRequestHeaders().getFirst("x-amz-meta-foo")
                    + "|range=" + exchange.getRequestHeaders().getFirst("Range")
                    + "|fwd=" + exchange.getRequestHeaders().getFirst("X-RT-LB-Forwarded")
                    + "|xff=" + exchange.getRequestHeaders().getFirst("X-Forwarded-For")
                    + "|host=" + exchange.getRequestHeaders().getFirst("Host")
                    + "|bodyLen=" + body.length;
            byte[] out = resp.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("X-Echo-Node", "node-2");
            exchange.sendResponseHeaders(200, out.length);
            try (var os = exchange.getResponseBody()) {
                os.write(out);
            }
        }
    }

    private static String readAll(InputStream in) throws IOException {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    @Test
    void forwardsGetWithQueryAndHeadersAndForwardedChain() throws IOException {
        HttpReverseProxy proxy = new HttpReverseProxy(Duration.ofSeconds(2));
        ForwardRequest req = new ForwardRequest("GET", baseUrl, "/echo?prefix=a&maxKeys=10",
                Map.of(
                        "Authorization", List.of("Bearer tok-123"),
                        "Range", List.of("bytes=0-99"),
                        "Connection", List.of("close"),
                        "Host", List.of("entry.example:8091")),
                "10.0.0.10", null);

        try (ForwardResponse resp = proxy.exchange(req, Duration.ofSeconds(10))) {
            assertThat(resp.statusCode()).isEqualTo(200);
            String text = readAll(resp.body());
            assertThat(text)
                    .contains("method=GET")
                    .contains("uri=/echo")
                    .contains("query=prefix=a&maxKeys=10")
                    .contains("auth=Bearer tok-123")
                    .contains("range=bytes=0-99")
                    .contains("fwd=1")
                    .contains("xff=10.0.0.10");
            // Host 被重写为目标，原始 Host 进入 X-Forwarded-Host（JDK 自行管理 Host 头）
            assertThat(text).doesNotContain("host=entry.example:8091");
            assertThat(firstHeader(resp, "X-Echo-Node")).isEqualTo("node-2");
        }
    }

    /** JDK 响应头名称大小写不保证，按名称大小写不敏感取首值。 */
    private static String firstHeader(ForwardResponse resp, String name) {
        return resp.headers().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(name))
                .map(e -> e.getValue().isEmpty() ? null : e.getValue().get(0))
                .findFirst().orElse(null);
    }

    @Test
    void streamsLargePutBodyWithMetadataHeader() throws IOException {
        HttpReverseProxy proxy = new HttpReverseProxy(Duration.ofSeconds(2));
        byte[] payload = new byte[2 * 1024 * 1024 + 7];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        ForwardRequest req = new ForwardRequest("PUT", baseUrl, "/echo/bucket/key?x=1",
                Map.of(
                        "Content-Type", List.of("application/octet-stream"),
                        "x-amz-meta-foo", List.of("bar"),
                        // hop-by-hop / 受限头，必须被吞掉而非导致失败
                        "Content-Length", List.of(String.valueOf(payload.length)),
                        "Transfer-Encoding", List.of("chunked")),
                "10.0.0.11", new ByteArrayInputStream(payload));

        try (ForwardResponse resp = proxy.exchange(req, Duration.ofSeconds(30))) {
            assertThat(resp.statusCode()).isEqualTo(200);
            String text = readAll(resp.body());
            assertThat(text).contains("method=PUT").contains("meta=bar").contains("bodyLen=" + payload.length);
        }
        assertThat(lastBody.get()).containsExactly(payload);
        // 元数据头透传
        assertThat(lastExchange.get().getRequestHeaders().getFirst("x-amz-meta-foo")).isEqualTo("bar");
    }

    @Test
    void connectFailureIsClassifiedRetryable() throws Exception {
        int closedPort;
        try (ServerSocket probe = new ServerSocket(0, 1, InetAddressUtil.LOCALHOST)) {
            closedPort = probe.getLocalPort();
        }
        HttpReverseProxy proxy = new HttpReverseProxy(Duration.ofMillis(500));
        ForwardRequest req = new ForwardRequest("GET",
                "http://127.0.0.1:" + closedPort, "/echo", Map.of(), "10.0.0.1", null);
        assertThatThrownBy(() -> proxy.exchange(req, Duration.ofSeconds(3)))
                .isInstanceOf(ProxyFailureException.class)
                .satisfies(e -> assertThat(((ProxyFailureException) e).retryable()).isTrue());
    }

    @Test
    void droppedResponseHeaderSetIncludesHopByHop() {
        assertThat(HttpReverseProxy.isDroppedResponseHeader("Connection")).isTrue();
        assertThat(HttpReverseProxy.isDroppedResponseHeader("transfer-encoding")).isTrue();
        assertThat(HttpReverseProxy.isDroppedResponseHeader("Content-Type")).isFalse();
    }

    /** 小工具持有回环地址，避免测试方法内声明受检异常包装问题。 */
    static final class InetAddressUtil {
        static final java.net.InetAddress LOCALHOST = java.net.InetAddress.getLoopbackAddress();
    }
}
