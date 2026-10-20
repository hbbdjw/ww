package com.ww.runtime.server.metrics;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 入站请求计数过滤器：节点每接收到一个 HTTP 请求即计入当前秒桶，
 * 供运维监控 Dashboard 展示各节点每秒请求数（req/s）。
 *
 * <p>统计口径为「节点接收到的全部入站请求」——管理面（{@code /rt}、{@code /api}）、
 * 数据面（{@code /cache}、{@code /storage}）、LB 压测回显与健康检查均计入，
 * 未认证/被拒绝的请求同样构成节点负载。</p>
 *
 * <p>{@code OncePerRequestFilter} 保证单次请求内（含 FORWARD）只计一次；
 * 关闭 ERROR dispatch 重复计数与 ASYNC 重入计数（SSE 长连接只在初始 REQUEST 计一次）。</p>
 */
@Component
@Order(0)
public class RequestMetricsFilter extends OncePerRequestFilter {

    private final RequestMetricsCollector collector;

    public RequestMetricsFilter(RequestMetricsCollector collector) {
        this.collector = collector;
    }

    @Override
    protected boolean shouldNotFilterErrorDispatch() {
        return true;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        collector.record();
        filterChain.doFilter(request, response);
    }
}
