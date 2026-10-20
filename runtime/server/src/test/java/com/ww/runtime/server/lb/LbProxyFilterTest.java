package com.ww.runtime.server.lb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockFilterChain;

import com.ww.runtime.lb.HttpReverseProxy;
import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbLoadBalancer;
import com.ww.runtime.lb.LbNodeView;
import com.ww.runtime.lb.LbStrategy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LbProxyFilter} 直测（不起 Spring 容器）：MockHttpServletRequest 驱动，
 * 远端用 JDK HttpServer 伪后端验证转发/重试/防环/直通/503/502。
 */
class LbProxyFilterTest {

    private static final String SELF = "node-1";

    private HttpServer echoServer;
    private int echoPort;
    private final List<LbNodeView> nodes = new ArrayList<>();

    private LbProxyFilter filter;
    private LbLoadBalancer balancer;

    @BeforeEach
    void setUp() throws IOException {
        echoServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        echoServer.setExecutor(Executors.newFixedThreadPool(4));
        echoServer.createContext("/", new EchoHandler());
        echoServer.start();
        echoPort = echoServer.getAddress().getPort();

        balancer = new LbLoadBalancer(() -> nodes);
        filter = new LbProxyFilter(SELF, balancer, new HttpReverseProxy(Duration.ofSeconds(1)));
    }

    @AfterEach
    void tearDown() {
        if (echoServer != null) {
            echoServer.stop(0);
        }
    }

    /** 回显后端：把 method/uri/关键头写进响应体。 */
    static class EchoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = "method=" + exchange.getRequestMethod()
                    + "|uri=" + exchange.getRequestURI().getRawPath()
                    + "|query=" + exchange.getRequestURI().getRawQuery()
                    + "|auth=" + exchange.getRequestHeaders().getFirst("Authorization")
                    + "|range=" + exchange.getRequestHeaders().getFirst("Range")
                    + "|meta=" + exchange.getRequestHeaders().getFirst("x-amz-meta-foo")
                    + "|fwd=" + exchange.getRequestHeaders().getFirst("X-RT-LB-Forwarded");
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, out.length);
            try (var os = exchange.getResponseBody()) {
                os.write(out);
            }
        }
    }

    private LbNodeView node(String id, int port, boolean active) {
        return new LbNodeView(id, "127.0.0.1", port, 1.0, active);
    }

    private LbConfig config(boolean enabled, String entry, boolean selfServe) {
        return new LbConfig(enabled, entry, LbStrategy.ROUND_ROBIN, selfServe,
                List.of("/cache/**", "/storage/**"), Map.of(),
                3000, 1000, 1000, 30_000L, 0);
    }

    private MockHttpServletRequest request(String method, String uri) {
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.setMethod(method);
        req.setRequestURI(uri);
        req.setRemoteAddr("10.1.1.1");
        return req;
    }

    // ------------------------------------------------------------------
    // 直通场景（chain 被调用，不发起任何远端 HTTP）
    // ------------------------------------------------------------------

    @Test
    void disabledGoesStraightThroughChain() throws Exception {
        filter.acceptConfig(config(false, SELF, true));
        nodes.add(node(SELF, echoPort, true));
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/cache/k1"), new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isNotNull();
    }

    @Test
    void nonEntryNodeGoesThroughChain() throws Exception {
        filter.acceptConfig(config(true, "node-2", true)); // 本机不是入口
        nodes.add(node(SELF, echoPort, true));
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/cache/k1"), new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isNotNull();
    }

    @Test
    void controlPlanePathNeverProxied() throws Exception {
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node(SELF, echoPort, true));
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/rt/status"), new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isNotNull();
    }

    @Test
    void forwardedHeaderBreaksLoopAndGoesChain() throws Exception {
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node("node-2", echoPort, true));
        MockHttpServletRequest req = request("GET", "/cache/k1");
        req.addHeader("X-RT-LB-Forwarded", "1");
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(req, new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).as("被转发到达目标节点的请求不再二次代理").isNotNull();
    }

    @Test
    void selfSelectedGoesThroughChain() throws Exception {
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node(SELF, echoPort, true));
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/cache/k1"), new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isNotNull();
    }

    // ------------------------------------------------------------------
    // 远端转发
    // ------------------------------------------------------------------

    @Test
    void forwardsToRemoteNodeStreamingResponse() throws Exception {
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node("node-2", echoPort, true));
        MockHttpServletRequest req = request("GET", "/cache/k1");
        req.setQueryString("prefix=a");
        req.addHeader("Authorization", "Bearer tok");
        req.addHeader("Range", "bytes=0-9");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(req, resp, chain);

        assertThat(chain.getRequest()).isNull();
        assertThat(resp.getStatus()).isEqualTo(200);
        String text = resp.getContentAsString();
        assertThat(text)
                .contains("method=GET")
                .contains("uri=/cache/k1")
                .contains("query=prefix=a")
                .contains("auth=Bearer tok")
                .contains("range=bytes=0-9")
                .contains("fwd=1");
        assertThat(resp.getHeader("X-RT-LB-Target")).isEqualTo("node-2");
    }

    @Test
    void forwardsPutWithBodyAndMetadata() throws Exception {
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node("node-2", echoPort, true));
        MockHttpServletRequest req = request("PUT", "/storage/bucket/key");
        byte[] payload = "hello-lb-body".getBytes(StandardCharsets.UTF_8);
        req.setContent(payload);
        req.addHeader("Content-Type", "text/plain");
        req.addHeader("x-amz-meta-foo", "bar");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        filter.doFilter(req, resp, new MockFilterChain());

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(resp.getContentAsString())
                .contains("method=PUT")
                .contains("uri=/storage/bucket/key")
                .contains("meta=bar");
    }

    @Test
    void noActiveBackendReturns503() throws Exception {
        filter.acceptConfig(config(true, SELF, false)); // selfServe=false 且无其他候选
        nodes.add(node(SELF, echoPort, true));
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/cache/k1"), resp, chain);
        assertThat(chain.getRequest()).isNull();
        assertThat(resp.getStatus()).isEqualTo(503);
        assertThat(resp.getContentAsString()).contains("no ACTIVE backend");
    }

    @Test
    void getConnectionFailureRetriesOnceOnAnotherNode() throws Exception {
        int closedPort;
        try (ServerSocket probe = new ServerSocket(0, 1, new InetSocketAddress("127.0.0.1", 0).getAddress())) {
            closedPort = probe.getLocalPort();
        }
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node("node-2", closedPort, true)); // RR 首位，连接拒绝
        nodes.add(node("node-3", echoPort, true));   // 排除 node-2 后选中

        MockHttpServletResponse resp = new MockHttpServletResponse();
        filter.doFilter(request("GET", "/cache/k1"), resp, new MockFilterChain());

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(resp.getHeader("X-RT-LB-Target")).isEqualTo("node-3");
        assertThat(resp.getContentAsString()).contains("uri=/cache/k1");
    }

    @Test
    void putConnectionFailureIsNotRetriedAndReturns502() throws Exception {
        int closedPort;
        try (ServerSocket probe = new ServerSocket(0, 1, new InetSocketAddress("127.0.0.1", 0).getAddress())) {
            closedPort = probe.getLocalPort();
        }
        filter.acceptConfig(config(true, SELF, true));
        nodes.add(node("node-2", closedPort, true));

        MockHttpServletRequest req = request("PUT", "/cache/k1");
        req.setContent(new byte[] {1, 2, 3});
        MockHttpServletResponse resp = new MockHttpServletResponse();
        filter.doFilter(req, resp, new MockFilterChain());

        assertThat(resp.getStatus()).isEqualTo(502);
        assertThat(resp.getContentAsString()).contains("forward to backend failed");
    }

    @Test
    void selfServeFalseSkipsEntryAndForwards() throws Exception {
        filter.acceptConfig(config(true, SELF, false));
        nodes.add(node(SELF, echoPort, true));
        nodes.add(node("node-2", echoPort, true));
        MockHttpServletResponse resp = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request("GET", "/storage/b/o"), resp, chain);
        assertThat(chain.getRequest()).isNull();
        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(resp.getHeader("X-RT-LB-Target")).isEqualTo("node-2");
    }
}
