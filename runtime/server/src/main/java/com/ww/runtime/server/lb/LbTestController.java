package com.ww.runtime.server.lb;

import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.common.NodeId;

/**
 * 负载均衡压测专用回显端点：{@code GET /cache/lb-echo}。
 *
 * <p><b>为什么挂在 {@code /cache} 前缀下</b>：LB 反代默认代理前缀为
 * {@code /cache/**} 与 {@code /storage/**}（{@code LbConfig.defaultConfig()}），
 * 放在此前缀下无需任何额外配置即可经反代链路分发到各 ACTIVE 节点；
 * 精确路径优先于 {@code /cache/{key}} 映射（与 {@code /cache/keys} 同模式），
 * 不会被缓存数据面的 key 读取捕获。</p>
 *
 * <p>端点<b>不依赖任何能力组件</b>（CACHE/STORAGE 未启动也可压测纯转发链路），
 * 只回显处理请求的节点身份。压测工具按响应体 {@code nodeId} 统计请求分布：
 * 入口节点本机处理（selfServe 命中自己）时 {@code forwarded=false}；
 * 经反代落到后端节点时 {@code forwarded=true}。</p>
 *
 * <p><b>安全</b>：该端点已在 {@code SecurityConfig} 加入 GET 白名单（与
 * {@code /rt/health} 同类探针），响应仅含节点 id/主机/端口与时间戳，
 * 不含任何业务数据或集群敏感信息；仅限内网数据面压测使用。</p>
 */
@RestController
public class LbTestController {

    /** 防环标记头：与 LbProxyFilter/HttpReverseProxy 写入的头同名。 */
    private static final String FORWARDED_HEADER = "X-RT-LB-Forwarded";

    private final NodeId self;
    private final String host;
    private final int httpPort;
    private final int grpcPort;

    public LbTestController(NodeId self,
                            @Value("${runtime.node.host:localhost}") String host,
                            @Value("${server.port:8080}") int httpPort,
                            @Value("${runtime.node.grpc-port:9100}") int grpcPort) {
        this.self = self;
        this.host = host;
        this.httpPort = httpPort;
        this.grpcPort = grpcPort;
    }

    /** 回显实际处理节点身份与转发标记。 */
    @GetMapping("/cache/lb-echo")
    public Map<String, Object> echo(HttpServletRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("nodeId", self.value());
        body.put("host", host);
        body.put("httpPort", httpPort);
        body.put("grpcPort", grpcPort);
        body.put("forwarded", request.getHeader(FORWARDED_HEADER) != null);
        body.put("timestampMillis", System.currentTimeMillis());
        return body;
    }
}
