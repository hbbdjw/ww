package com.ww.admin.security;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.shiro.authc.AuthenticationToken;

/**
 * Bearer 令牌认证令牌。
 * <p>
 * 用于 Token 认证方式：客户端在请求头携带 {@code Authorization: Bearer <token>}，
 * 由 {@link DualAuthFilter} 提取后封装成本对象交由 {@link DbRealm} 校验。
 */
public class BearerToken implements AuthenticationToken {

    private final String token;
    private final String host;

    public BearerToken(String token, String host) {
        this.token = token;
        this.host = host;
    }

    public String getToken() {
        return token;
    }

    @Override
    public Object getPrincipal() {
        return token;
    }

    @Override
    public Object getCredentials() {
        return token;
    }

    public String getHost() {
        return host;
    }

    /**
     * 从请求头解析 Bearer 令牌。
     *
     * @param request HTTP 请求
     * @return 令牌字符串；不存在或格式不符时返回 null
     */
    public static String extract(HttpServletRequest request) {
        if (request == null) {
            return null;
        }
        String header = request.getHeader("Authorization");
        if (header == null || header.isBlank()) {
            return null;
        }
        header = header.trim();
        String prefix = "Bearer ";
        if (header.regionMatches(true, 0, prefix, 0, prefix.length())) {
            String token = header.substring(prefix.length()).trim();
            return token.isEmpty() ? null : token;
        }
        return null;
    }

}
