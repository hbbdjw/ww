package com.ww.runtime.server.web;

import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.server.security.AuthSessionService;
import com.ww.runtime.server.security.AuthenticatedUser;
import com.ww.runtime.server.security.LoginService;
import com.ww.runtime.server.security.RbacStore;

/**
 * 认证端点：登录 / 登出 / 当前登录用户信息。
 *
 * <p>登录成功返回不透明 token 与用户基础信息；后续请求以
 * {@code Authorization: Bearer <token>} 携带；登出立即删除会话。</p>
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private static final String BEARER_PREFIX = "Bearer ";

    private final LoginService loginService;
    private final AuthSessionService sessions;

    public AuthController(LoginService loginService, AuthSessionService sessions) {
        this.loginService = loginService;
        this.sessions = sessions;
    }

    /** 登录（白名单端点）。 */
    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody Map<String, Object> body) {
        String username = body == null ? null : (String) body.get("username");
        String password = body == null ? null : (String) body.get("password");
        LoginService.LoginResult result = loginService.login(username, password);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("token", result.token());
        response.put("user", toUserMap(result.user()));
        return response;
    }

    /** 登出：撤销当前 token。 */
    @PostMapping("/logout")
    public Map<String, Object> logout(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            sessions.revoke(header.substring(BEARER_PREFIX.length()).trim());
        }
        return Map.of("ok", true);
    }

    /** 当前登录用户信息：基础资料 + 角色 key + 全部权限标识。 */
    @GetMapping("/info")
    public Map<String, Object> info(@AuthenticationPrincipal AuthenticatedUser principal) {
        RbacStore.UserPermissions permissions = sessions.permissions(principal.userId());
        Map<String, Object> response = toUserMap(principal);
        response.put("roles", permissions.roleKeys());
        response.put("permissions", permissions.perms());
        return response;
    }

    private static Map<String, Object> toUserMap(AuthenticatedUser user) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("userId", user.userId());
        map.put("username", user.username());
        map.put("nickname", user.nickname());
        map.put("deptId", user.deptId());
        return map;
    }
}
