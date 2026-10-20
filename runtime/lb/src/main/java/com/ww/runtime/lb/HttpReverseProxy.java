package com.ww.runtime.lb;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 基于 JDK 内置 {@link HttpClient} 的流式反向代理（零三方依赖）。
 *
 * <p>职责：构造转发请求（头清洗、X-Forwarded-* 与防环标记）、流式发送、返回流式响应。
 * 不做重试决策（由上层过滤器按 HTTP 方法决定），不解析业务状态码。</p>
 *
 * <p>请求体经 {@code BodyPublishers.ofInputStream} 流式发送（不整体入堆，支撑大文件上传）；
 * 响应体使用 {@code BodyHandlers.ofInputStream()} 由调用方 64KB 分块拷贝。</p>
 */
public final class HttpReverseProxy {

    /** RFC 7230/2616 hop-by-hop 与由代理/客户端重算的头，一律不向前透传。 */
    private static final Set<String> DROP_REQUEST_HEADERS = Set.of(
            "host", "connection", "keep-alive", "proxy-connection", "transfer-encoding",
            "te", "trailer", "upgrade", "proxy-authenticate", "proxy-authorization",
            "content-length",
            // 转发链自己重新生成，避免外部伪造叠加
            "x-rt-lb-forwarded", "x-forwarded-for");

    /** 响应回写时剔除的 hop-by-hop 头。 */
    private static final Set<String> DROP_RESPONSE_HEADERS = Set.of(
            "connection", "keep-alive", "proxy-connection", "transfer-encoding",
            "te", "trailer", "upgrade", "proxy-authenticate", "proxy-authorization");

    private static final Set<String> BODYLESS_METHODS = Set.of("GET", "HEAD");

    private final HttpClient client;

    public HttpReverseProxy(Duration connectTimeout) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    /**
     * 同步执行转发；连接/超时类失败抛 {@link ProxyFailureException}(retryable=true)。
     *
     * @param requestTimeout 本次请求总超时（大文件/媒体流长耗时；配置热更后当次生效）
     */
    public ForwardResponse exchange(ForwardRequest request, Duration requestTimeout) {
        HttpRequest outbound = buildRequest(request, requestTimeout);
        HttpResponse<InputStream> response;
        try {
            response = client.send(outbound, HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            throw new ProxyFailureException("forward to " + outbound.uri() + " failed: " + e.getMessage(),
                    isConnectOrTimeout(e), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProxyFailureException("forward to " + outbound.uri() + " interrupted", false, e);
        }
        return new ForwardResponse(response.statusCode(), response.headers().map(), response.body());
    }

    private HttpRequest buildRequest(ForwardRequest request, Duration requestTimeout) {
        URI uri;
        try {
            String base = request.targetBaseUrl();
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            uri = URI.create(base + request.pathAndQuery());
        } catch (IllegalArgumentException e) {
            throw new ProxyFailureException("invalid forward target URI: "
                    + request.targetBaseUrl() + request.pathAndQuery(), false, e);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout == null ? Duration.ofMinutes(60) : requestTimeout);

        String method = request.method().toUpperCase(Locale.ROOT);
        boolean hasBody = request.body() != null && !BODYLESS_METHODS.contains(method);

        // 先复制清洗后的业务头
        String incomingHost = null;
        List<String> forwardedForChain = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : request.headers().entrySet()) {
            String name = e.getKey();
            if (name == null) {
                continue;
            }
            String lower = name.toLowerCase(Locale.ROOT);
            if (DROP_REQUEST_HEADERS.contains(lower)) {
                if ("host".equals(lower) && !e.getValue().isEmpty()) {
                    incomingHost = e.getValue().get(0);
                }
                if ("x-forwarded-for".equals(lower)) {
                    forwardedForChain.addAll(nonBlank(e.getValue()));
                }
                continue;
            }
            for (String value : e.getValue()) {
                if (value != null) {
                    builder.header(name, value);
                }
            }
        }

        // 转发链标准头
        forwardedForChain.add(request.remoteAddr() == null ? "" : request.remoteAddr());
        builder.header("X-Forwarded-For", String.join(", ", nonBlank(forwardedForChain)));
        builder.header("X-Forwarded-Proto", "http");
        if (incomingHost != null && !incomingHost.isBlank()) {
            builder.header("X-Forwarded-Host", incomingHost);
        }
        builder.header("X-RT-LB-Forwarded", "1");

        if (hasBody) {
            // Content-Length 已剔除：JDK 以 chunked 流式发送，Tomcat 正常接收
            InputStream body = request.body();
            builder.method(method, HttpRequest.BodyPublishers.ofInputStream(() -> body));
        } else if ("GET".equals(method)) {
            builder.GET();
        } else if ("HEAD".equals(method)) {
            builder.method("HEAD", HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        return builder.build();
    }

    /** 响应头是否属于 hop-by-hop（过滤器回写前调用）。 */
    public static boolean isDroppedResponseHeader(String name) {
        return name != null && DROP_RESPONSE_HEADERS.contains(name.toLowerCase(Locale.ROOT));
    }

    private static List<String> nonBlank(List<String> values) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                out.add(v.trim());
            }
        }
        return new ArrayList<>(out);
    }

    /** 连接拒绝/连接超时/请求超时视为可重试（仅 GET/HEAD 用）；其余 IO 异常不重试。 */
    private static boolean isConnectOrTimeout(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ConnectException
                    || c instanceof HttpConnectTimeoutException
                    || c instanceof HttpTimeoutException) {
                return true;
            }
        }
        return false;
    }
}
