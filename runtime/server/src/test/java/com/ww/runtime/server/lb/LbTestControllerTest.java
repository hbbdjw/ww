package com.ww.runtime.server.lb;

import com.ww.runtime.common.NodeId;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LbTestController} 回显体单测：验证节点身份字段与 LB 转发标记语义。
 */
class LbTestControllerTest {

    private final LbTestController controller =
            new LbTestController(NodeId.of("node-2"), "10.0.0.2", 8082, 9102);

    @Test
    void echoOnEntryNodeSelfServedIsMarkedNotForwarded() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/cache/lb-echo");

        Map<String, Object> body = controller.echo(request);

        assertThat(body.get("nodeId")).isEqualTo("node-2");
        assertThat(body.get("host")).isEqualTo("10.0.0.2");
        assertThat(body.get("httpPort")).isEqualTo(8082);
        assertThat(body.get("grpcPort")).isEqualTo(9102);
        assertThat(body.get("forwarded")).isEqualTo(false);
        assertThat((Long) body.get("timestampMillis")).isPositive();
    }

    @Test
    void echoAfterLbForwardCarriesForwardedMarker() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/cache/lb-echo");
        // 反代到后端时由 HttpReverseProxy 写入的防环标记
        request.addHeader("X-RT-LB-Forwarded", "1");

        Map<String, Object> body = controller.echo(request);

        // 后端节点视角：该请求经 LB 转发到达
        assertThat(body.get("forwarded")).isEqualTo(true);
        assertThat(body.get("nodeId")).isEqualTo("node-2");
    }
}
