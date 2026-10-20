package com.ww.runtime.server.lb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.PathContainer;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import com.ww.runtime.lb.ForwardRequest;
import com.ww.runtime.lb.ForwardResponse;
import com.ww.runtime.lb.HttpReverseProxy;
import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbNodeView;
import com.ww.runtime.lb.LbLoadBalancer;
import com.ww.runtime.lb.ProxyFailureException;

/**
 * 数据面请求反向代理过滤器（位于 springSecurityFilterChain 之后注册）。
 *
 * <p>仅当以下条件<b>同时</b>满足时代理，否则直通本机处理：</p>
 * <ol>
 *   <li>LB 已启用（enabled=true，上游无 LBS/Nginx 形态）；</li>
 *   <li>本节点是页面指定的入口节点；</li>
 *   <li>请求路径命中页面配置的前缀（默认 {@code /cache/**}、{@code /storage/**}）；</li>
 *   <li>请求未携带 {@code X-RT-LB-Forwarded}（防环：被转发到目标节点后不再二次代理）。</li>
 * </ol>
 *
 * <p>选中本机 → 直接走过滤链（零转发）；选中远端 → JDK HttpClient 流式转发，
 * 回写状态码/响应头/流式响应体。GET/HEAD 连接失败可换另一 ACTIVE 节点重试 1 次，
 * 写方法绝不重试；无健康候选 503；转发失败 502。过滤器未预期异常 fail-open
 * （回退本机处理），避免 LB 缺陷拖死整个数据面。</p>
 */
public final class LbProxyFilter extends OncePerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LbProxyFilter.class);

    /** 防环标记（与 runtime-lb HttpReverseProxy 写入的头同名）。 */
    private static final String FORWARDED_HEADER = "X-RT-LB-Forwarded";
    private static final String TARGET_HEADER = "X-RT-LB-Target";
    private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD");
    private static final int COPY_BUFFER_BYTES = 64 * 1024;

    private final String selfNodeId;
    private final LbLoadBalancer loadBalancer;
    private final HttpReverseProxy proxy;
    private final PathPatternParser patternParser = PathPatternParser.defaultInstance;
    private final AntPathMatcher fallbackMatcher = new AntPathMatcher();

    private volatile LbConfig config = LbConfig.defaultConfig();
    private volatile List<PathPattern> pathPatterns = List.of();

    public LbProxyFilter(String selfNodeId, LbLoadBalancer loadBalancer, HttpReverseProxy proxy) {
        this.selfNodeId = selfNodeId;
        this.loadBalancer = loadBalancer;
        this.proxy = proxy;
    }

    /**
     * 配置热更新入口（单一配置通道）：原子替换本过滤器的配置与编译后的路径模式，
     * 并同步发布给选择器。定时刷新与管理端点 PUT 后均走这里，避免两处配置状态漂移。
     */
    public void acceptConfig(LbConfig newConfig) {
        List<PathPattern> compiled = new ArrayList<>(newConfig.pathPrefixes().size());
        for (String prefix : newConfig.pathPrefixes()) {
            compiled.add(patternParser.parse(prefix));
        }
        this.pathPatterns = List.copyOf(compiled);
        this.config = newConfig;
        loadBalancer.updateConfig(newConfig);
    }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() {
        return true;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        LbConfig current = config;
        if (!shouldProxy(request, current)) {
            filterChain.doFilter(request, response);
            return;
        }
        try {
            Optional<LbNodeView> picked = loadBalancer.select(selfNodeId);
            if (picked.isEmpty()) {
                writeJsonError(response, HttpStatus.SERVICE_UNAVAILABLE,
                        "load balancer: no ACTIVE backend available");
                return;
            }
            LbNodeView target = picked.get();
            if (selfNodeId.equals(target.nodeId())) {
                filterChain.doFilter(request, response);
                return;
            }
            forwardWithRetry(request, response, current, target);
        } catch (RuntimeException unexpected) {
            // fail-open：LB 自身缺陷不应拖死数据面；响应已提交则无法回退，仅留日志
            LOG.error("lb proxy unexpected failure, falling back to local handling: {}",
                    unexpected.getMessage(), unexpected);
            if (!response.isCommitted()) {
                response.reset();
                filterChain.doFilter(request, response);
            }
        }
    }

    private boolean shouldProxy(HttpServletRequest request, LbConfig current) {
        if (!current.enabled() || !selfNodeId.equals(current.entryNodeId())) {
            return false;
        }
        if (request.getHeader(FORWARDED_HEADER) != null) {
            return false;
        }
        String path = pathWithinApplication(request);
        PathContainer pathContainer = PathContainer.parsePath(path);
        for (PathPattern pattern : pathPatterns) {
            if (pattern.matches(pathContainer)) {
                return true;
            }
            // PathPattern 不支持的旧式通配（极少见配置）退化为 Ant 匹配，保证不意外漏代理
            if (fallbackMatcher.match(pattern.getPatternString(), path)) {
                return true;
            }
        }
        return false;
    }

    /** GET/HEAD 连接类失败换另一节点重试一次；其余情况直接 502。 */
    private void forwardWithRetry(HttpServletRequest request,
                                  HttpServletResponse response,
                                  LbConfig current,
                                  LbNodeView first) throws IOException {
        try {
            forward(request, response, current, first);
            return;
        } catch (ProxyFailureException firstFailure) {
            boolean safe = SAFE_METHODS.contains(request.getMethod().toUpperCase(java.util.Locale.ROOT));
            if (safe && firstFailure.retryable()) {
                Optional<LbNodeView> second = loadBalancer.select(selfNodeId, first.nodeId());
                if (second.isPresent() && !selfNodeId.equals(second.get().nodeId())) {
                    try {
                        LOG.warn("forward to {} failed ({}), retrying once on {}",
                                first.nodeId(), firstFailure.getMessage(), second.get().nodeId());
                        forward(request, response, current, second.get());
                        return;
                    } catch (ProxyFailureException secondFailure) {
                        LOG.warn("retry forward to {} also failed: {}",
                                second.get().nodeId(), secondFailure.getMessage());
                    }
                }
            } else {
                LOG.warn("forward to {} failed (non-retryable): {}",
                        first.nodeId(), firstFailure.getMessage());
            }
            if (!response.isCommitted()) {
                response.reset();
                writeJsonError(response, HttpStatus.BAD_GATEWAY,
                        "load balancer: forward to backend failed");
            }
        }
    }

    private void forward(HttpServletRequest request,
                         HttpServletResponse response,
                         LbConfig current,
                         LbNodeView target) throws IOException {
        ForwardRequest outbound = buildForwardRequest(request, target);
        try (ForwardResponse result = proxy.exchange(
                outbound, Duration.ofMillis(current.requestTimeoutMs()))) {
            writeResponse(response, result, target.nodeId());
        }
    }

    private ForwardRequest buildForwardRequest(HttpServletRequest request, LbNodeView target)
            throws IOException {
        String uri = request.getRequestURI();
        String query = request.getQueryString();
        if (query != null && !query.isBlank()) {
            uri = uri + "?" + query;
        }
        Map<String, List<String>> headers = collectHeaders(request);
        boolean safe = SAFE_METHODS.contains(request.getMethod().toUpperCase(java.util.Locale.ROOT));
        InputStream body = safe ? null : request.getInputStream();
        return new ForwardRequest(request.getMethod(), target.baseUrl(), uri,
                headers, request.getRemoteAddr(), body);
    }

    private static Map<String, List<String>> collectHeaders(HttpServletRequest request) {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        List<String> names = Collections.list(request.getHeaderNames());
        for (String name : names) {
            List<String> values = Collections.list(request.getHeaders(name));
            if (!values.isEmpty()) {
                headers.put(name, values);
            }
        }
        return headers;
    }

    private static void writeResponse(HttpServletResponse response,
                                      ForwardResponse result,
                                      String targetNodeId) throws IOException {
        response.setStatus(result.statusCode());
        response.setHeader(TARGET_HEADER, targetNodeId);
        for (Map.Entry<String, List<String>> e : result.headers().entrySet()) {
            String name = e.getKey();
            if (HttpReverseProxy.isDroppedResponseHeader(name)) {
                continue;
            }
            List<String> values = e.getValue();
            if (values == null || values.isEmpty()) {
                continue;
            }
            response.setHeader(name, values.get(0));
            for (int i = 1; i < values.size(); i++) {
                response.addHeader(name, values.get(i));
            }
        }
        try (InputStream in = result.body(); OutputStream out = response.getOutputStream()) {
            byte[] buffer = new byte[COPY_BUFFER_BYTES];
            int n;
            while ((n = in.read(buffer)) >= 0) {
                if (n > 0) {
                    out.write(buffer, 0, n);
                }
            }
            out.flush();
        }
    }

    private static String pathWithinApplication(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (contextPath != null && !contextPath.isEmpty() && uri.startsWith(contextPath)) {
            uri = uri.substring(contextPath.length());
        }
        return uri.isBlank() ? "/" : uri;
    }

    private static void writeJsonError(HttpServletResponse response, HttpStatus status, String message)
            throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write("{\"message\":\"" + message + "\"}");
    }
}
