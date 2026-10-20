package com.ww.runtime.lb;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 反向代理请求的协议无关描述（由 server 侧过滤器从 HttpServletRequest 映射，
 * 保证 runtime-lb 不依赖 servlet API）。
 *
 * @param method        HTTP 方法（GET/HEAD/PUT/POST/DELETE...）
 * @param targetBaseUrl 目标节点基地址，如 {@code http://rt-node2:8092}
 * @param pathAndQuery  原始请求 URI（以 / 开头，含 query string）
 * @param headers       已规整的请求头（可含同名多值）；hop-by-hop 头无需调用方剔除
 * @param remoteAddr    直连客户端地址（用于拼 X-Forwarded-For）
 * @param body          请求体输入流；无体（GET/HEAD）传 {@code null}
 */
public record ForwardRequest(
        String method,
        String targetBaseUrl,
        String pathAndQuery,
        Map<String, List<String>> headers,
        String remoteAddr,
        InputStream body) {

    public ForwardRequest {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(targetBaseUrl, "targetBaseUrl");
        Objects.requireNonNull(pathAndQuery, "pathAndQuery");
        if (!pathAndQuery.startsWith("/")) {
            throw new IllegalArgumentException("pathAndQuery must start with '/': " + pathAndQuery);
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
