package com.ww.runtime.server.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Bearer token 认证过滤器：从 {@code Authorization: Bearer <token>} 取出不透明 token，
 * 经 {@link AuthSessionService} 解析会话并加载权限，构建 SecurityContext。
 *
 * <p>每次有效访问滑动续期会话；无 token / 会话过期 / 缓存异常均不设置认证
 * （fail-closed，最终由认证入口返回 401）。登录端点不经本过滤器。</p>
 *
 * <p>浏览器原生媒体元素（{@code <img>/<video>/<audio>/<iframe>}）无法附加自定义请求头，
 * 故 GET/HEAD 在缺失 Authorization 头时回退识别 {@code ?access_token=} 查询参数；
 * 写方法（PUT/POST/DELETE 等）不接受查询参数令牌，令牌只能经头发送，
 * 避免其经 URL/访问日志/Referer 扩大泄露面。</p>
 */
@Component
public class BearerTokenAuthenticationFilter extends OncePerRequestFilter {

    private static final String BEARER_PREFIX = "Bearer ";
    private static final String LOGIN_PATH = "/api/auth/login";
    /** 查询参数令牌名：仅 GET/HEAD 回退识别（媒体元素流式读取场景）。 */
    private static final String ACCESS_TOKEN_PARAM = "access_token";

    private final AuthSessionService sessions;

    public BearerTokenAuthenticationFilter(AuthSessionService sessions) {
        this.sessions = sessions;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return LOGIN_PATH.equals(request.getServletPath());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String token = extractToken(request);
        if (token != null) {
            authenticate(token, request);
        }
        try {
            filterChain.doFilter(request, response);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private void authenticate(String token, HttpServletRequest request) {
        try {
            Optional<AuthSessionService.SessionInfo> resolved = sessions.resolve(token);
            if (resolved.isEmpty()) {
                return;
            }
            AuthSessionService.SessionInfo session = resolved.get();
            sessions.touch(token, session);
            RbacStore.UserPermissions permissions = sessions.permissions(session.userId());
            List<GrantedAuthority> authorities = toAuthorities(permissions);
            UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                    AuthenticatedUser.from(session), token, authorities);
            authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
            SecurityContextHolder.getContext().setAuthentication(authentication);
        } catch (RuntimeException e) {
            // 会话/权限缓存不可用等任何异常：不设置认证，按未认证处理（fail-closed）
            logger.warn("bearer authentication failed: " + e.getMessage());
        }
    }

    private static List<GrantedAuthority> toAuthorities(RbacStore.UserPermissions permissions) {
        List<GrantedAuthority> authorities = new ArrayList<>();
        for (String roleKey : permissions.roleKeys()) {
            authorities.add(new SimpleGrantedAuthority("ROLE_" + roleKey));
        }
        for (String perm : permissions.perms()) {
            authorities.add(new SimpleGrantedAuthority(perm));
        }
        return List.copyOf(authorities);
    }

    private static String extractToken(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            String token = header.substring(BEARER_PREFIX.length()).trim();
            if (!token.isEmpty()) {
                return token;
            }
        }
        // Authorization 头缺失/无效时，仅幂等的 GET/HEAD 允许查询参数回退；
        // 此时请求无请求体，getParameter 不会干扰流式上传的 InputStream
        String method = request.getMethod();
        if (HttpMethod.GET.name().equals(method) || HttpMethod.HEAD.name().equals(method)) {
            String queryToken = request.getParameter(ACCESS_TOKEN_PARAM);
            if (queryToken != null) {
                queryToken = queryToken.trim();
                if (!queryToken.isEmpty()) {
                    return queryToken;
                }
            }
        }
        return null;
    }
}
