package com.ww.admin.security;

import java.io.IOException;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.filter.AccessControlFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * 双模认证过滤器：同时支持 Session 与 Token 两种认证方式。
 * <ol>
 *   <li>若当前 Subject 已通过 Session 认证 → 直接放行。</li>
 *   <li>否则若请求头携带 {@code Authorization: Bearer <token>} → 以令牌登录，成功则放行。</li>
 *   <li>否则若为存储对象 GET/HEAD 下载请求且 URL 携带 {@code access_token} 参数
 *       → 以该令牌登录（媒体流式播放场景：浏览器原生 video/audio 标签无法附加自定义请求头）。</li>
 *   <li>否则返回 401 JSON。</li>
 * </ol>
 * 该过滤器是整个认证体系的统一入口，替代 Shiro 默认的 authc / form 过滤器。
 */
public class DualAuthFilter extends AccessControlFilter {

    private static final Logger log = LoggerFactory.getLogger(DualAuthFilter.class);

    private static final String JSON_401 = "{\"type\":\"error\",\"code\":\"401\",\"msg\":\"未登录或认证已过期\"}";

    @Override
    protected boolean isAccessAllowed(ServletRequest request, ServletResponse response, Object mappedValue) {
        Subject subject = getSubject(request, response);
        // 1) Session 认证：已登录直接放行
        if (subject.isAuthenticated()) {
            return true;
        }
        // 2) Token 认证：尝试用 Bearer 令牌登录
        HttpServletRequest httpReq = toHttp(request);
        String token = BearerToken.extract(httpReq);
        // 媒体流式播放场景：原生 <video>/<audio> 标签无法附加 Authorization 请求头，
        // 仅允许存储对象 GET/HEAD 下载端点通过 URL 参数携带令牌
        if (token == null && isObjectDownloadRequest(httpReq)) {
            token = httpReq.getParameter("access_token");
        }
        if (token != null) {
            try {
                subject.login(new BearerToken(token, request.getRemoteHost()));
                return true;
            } catch (AuthenticationException e) {
                log.debug("Bearer token 认证失败: {}", e.getMessage());
                // 落到 onAccessDenied 返回 401
            }
        }
        return false;
    }

    @Override
    protected boolean onAccessDenied(ServletRequest request, ServletResponse response) throws IOException {
        HttpServletResponse resp = toHttp(response);
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        resp.setContentType("application/json;charset=UTF-8");
        resp.getWriter().write(JSON_401);
        return false;
    }

    /** 是否为存储对象下载的 GET/HEAD 请求（唯一允许 URL 参数令牌认证的场景） */
    private boolean isObjectDownloadRequest(HttpServletRequest request) {
        String method = request.getMethod();
        if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
            return false;
        }
        String uri = request.getRequestURI();
        return uri != null && uri.startsWith("/storage/buckets/");
    }

    private HttpServletRequest toHttp(ServletRequest request) {
        return (HttpServletRequest) request;
    }

    private HttpServletResponse toHttp(ServletResponse response) {
        return (HttpServletResponse) response;
    }
}
