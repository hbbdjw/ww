package com.ww.admin.service;

import com.ww.admin.entity.SysUser;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.LockedAccountException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 认证服务：统一封装 Session 登录、Token 签发、登出与当前用户信息。
 */
@Service
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    private final UserService userService;
    private final AccessTokenService accessTokenService;

    @Value("${shiro.token.ttl-seconds:43200}")
    private long tokenTtlSeconds;

    public AuthService(UserService userService, AccessTokenService accessTokenService) {
        this.userService = userService;
        this.accessTokenService = accessTokenService;
    }

    /**
     * Session 登录：用户名 + 密码，成功后建立 Shiro 会话（Cookie）。
     *
     * @return 用户信息（脱敏，不含密码哈希等敏感字段）
     */
    public Map<String, Object> login(String username, String password) {
        Subject subject = SecurityUtils.getSubject();
        UsernamePasswordToken token = new UsernamePasswordToken(username, password);
        try {
            subject.login(token);
        } catch (UnknownAccountException e) {
            throw new AuthenticationException("账号不存在");
        } catch (IncorrectCredentialsException e) {
            throw new AuthenticationException("用户名或密码错误");
        } catch (LockedAccountException e) {
            throw new AuthenticationException("账号已禁用");
        } catch (AuthenticationException e) {
            throw new AuthenticationException("认证失败：" + e.getMessage());
        }
        return buildUserInfo(userService.findByUsername(username));
    }

    /**
     * Token 登录（前端 SPA 场景）：校验用户名密码后签发 Bearer 令牌。
     * <p>与 {@link #issueToken} 的区别：本方法用 Subject.login 完成密码校验，
     * 随后立即登出（不保留会话），仅返回令牌供前端通过 Authorization 头携带。
     *
     * @return 包含 accessToken / tokenType / expiresIn / username 的映射
     */
    @Transactional
    public Map<String, Object> loginWithToken(String username, String password, HttpServletRequest request) {
        SysUser user = userService.findByUsername(username);
        if (user == null) {
            throw new AuthenticationException("账号不存在");
        }
        if (user.getStatus() != null && user.getStatus() != 1) {
            throw new AuthenticationException("账号已禁用");
        }
        Subject subject = SecurityUtils.getSubject();
        UsernamePasswordToken upt = new UsernamePasswordToken(username, password);
        try {
            subject.login(upt);
        } catch (AuthenticationException e) {
            log.warn("前端登录失败: user={}, reason={}", username, e.getMessage());
            throw new AuthenticationException("用户名或密码错误");
        }
        try {
            subject.logout();
        } catch (Exception ignored) {
        }

        String clientIp = request != null ? request.getRemoteHost() : null;
        String rawToken = accessTokenService.issue(user, tokenTtlSeconds * 1000L, clientIp);
        log.info("前端登录签发令牌: user={}, ttl={}s, ip={}", username, tokenTtlSeconds, clientIp);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("accessToken", rawToken);
        result.put("tokenType", "Bearer");
        result.put("expiresIn", tokenTtlSeconds);
        result.put("username", username);
        return result;
    }

    /**
     * Token 登录：校验用户名密码后签发访问令牌（无状态，适用于 API/移动端）。
     *
     * @return 原始令牌字符串（仅返回一次）
     */
    @Transactional
    public Map<String, Object> issueToken(String username, String password, HttpServletRequest request) {
        SysUser user = userService.findByUsername(username);
        if (user == null) {
            throw new AuthenticationException("账号不存在");
        }
        if (user.getStatus() != null && user.getStatus() != 1) {
            throw new AuthenticationException("账号已禁用");
        }
        // 用一次 Subject.login 完成密码哈希校验，随后立即登出（Token 模式不保留会话）
        Subject subject = SecurityUtils.getSubject();
        UsernamePasswordToken upt = new UsernamePasswordToken(username, password);
        try {
            subject.login(upt);
        } catch (AuthenticationException e) {
            throw new AuthenticationException("用户名或密码错误");
        }
        // Token 模式不依赖会话，登出清掉刚建立的会话
        try {
            subject.logout();
        } catch (Exception ignored) {
            // 忽略登出异常
        }

        String clientIp = request != null ? request.getRemoteHost() : null;
        String rawToken = accessTokenService.issue(user, tokenTtlSeconds * 1000L, clientIp);
        log.info("为用户 {} 签发访问令牌, ttl={}s, ip={}", username, tokenTtlSeconds, clientIp);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("accessToken", rawToken);
        result.put("tokenType", "Bearer");
        result.put("expiresIn", tokenTtlSeconds);
        result.put("username", username);
        return result;
    }

    /**
     * 登出：若是 Token 请求则吊销令牌，并清除 Shiro 会话。
     */
    @Transactional
    public void logout(String bearerToken) {
        if (bearerToken != null && !bearerToken.isEmpty()) {
            accessTokenService.revoke(bearerToken);
        }
        try {
            SecurityUtils.getSubject().logout();
        } catch (Exception ignored) {
            // 忽略
        }
    }

    /**
     * 获取当前登录用户及其角色/权限信息。
     * <p>返回 vben-admin 前端期望的 UserInfo 格式：
     * {@code userId, username, realName, avatar, roles, permissions, homePath}。
     */
    public Map<String, Object> currentUser() {
        Object principal = SecurityUtils.getSubject().getPrincipal();
        if (principal == null) {
            return null;
        }
        String username = principal.toString();
        SysUser user = userService.findByUsername(username);
        if (user == null) {
            return null;
        }
        return buildUserInfo(user);
    }

    /**
     * 获取当前登录用户的权限码列表（供前端 getAccessCodesApi 使用）。
     */
    public java.util.List<String> getAccessCodes() {
        Object principal = SecurityUtils.getSubject().getPrincipal();
        if (principal == null) {
            return java.util.List.of();
        }
        SysUser user = userService.findByUsername(principal.toString());
        if (user == null || user.getRoles() == null) {
            return java.util.List.of();
        }
        return user.getRoles().stream()
                .filter(r -> r.getStatus() != null && r.getStatus() == 1)
                .flatMap(r -> r.getMenus() == null ? java.util.stream.Stream.empty() : r.getMenus().stream())
                .filter(m -> m.getStatus() != null && m.getStatus() == 1)
                .map(m -> m.getPerms())
                .filter(p -> p != null && !p.isEmpty())
                .distinct()
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * 保存当前登录用户的前端偏好设置（JSON 字符串）。
     * <p>前端 preferencesManager 在用户修改偏好后调用，将自定义偏好持久化到 sys_user.preferences 列。
     * 使用原生 SQL UPDATE 避免加载完整实体和乐观锁冲突。
     *
     * @param preferencesJson 偏好设置的 JSON 字符串
     */
    @Transactional
    public void savePreferences(String preferencesJson) {
        Object principal = SecurityUtils.getSubject().getPrincipal();
        if (principal == null) {
            throw new IllegalStateException("未登录");
        }
        SysUser user = userService.findByUsername(principal.toString());
        if (user == null) {
            throw new IllegalStateException("用户不存在");
        }
        userService.updatePreferences(user.getId(), preferencesJson);
        log.debug("用户偏好已保存: user={}", user.getUsername());
    }

    /**
     * 构建脱敏用户信息（不含 password 等敏感字段）。
     * <p>
     * 返回 vben-admin 前端期望的 UserInfo 格式：
     * <ul>
     *   <li>{@code userId} —— 用户 ID（vben 要求 string 类型）</li>
     *   <li>{@code username} —— 用户账号</li>
     *   <li>{@code realName} —— 用户昵称（vben 用 realName 展示）</li>
     *   <li>{@code avatar} —— 头像 URL</li>
     *   <li>{@code roles} —— 角色标识列表（role_key）</li>
     *   <li>{@code permissions} —— 权限码列表（menu.perms）</li>
     *   <li>{@code homePath} —— 登录后默认首页路径</li>
     * </ul>
     */
    private Map<String, Object> buildUserInfo(SysUser user) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("userId", String.valueOf(user.getId()));
        info.put("username", user.getUsername());
        info.put("realName", user.getNickname());
        info.put("avatar", "");
        if (user.getDept() != null) {
            info.put("deptId", user.getDept().getId());
            info.put("deptName", user.getDept().getDeptName());
        }
        Set<String> roles = user.getRoles() == null ? Set.of()
                : user.getRoles().stream()
                        .filter(r -> r.getStatus() != null && r.getStatus() == 1)
                        .map(r -> r.getRoleKey()).collect(Collectors.toSet());
        Set<String> perms = user.getRoles() == null ? Set.of()
                : user.getRoles().stream()
                        .filter(r -> r.getStatus() != null && r.getStatus() == 1)
                        .flatMap(r -> r.getMenus() == null ? java.util.stream.Stream.empty()
                                : r.getMenus().stream())
                        .filter(m -> m.getStatus() != null && m.getStatus() == 1)
                        .map(m -> m.getPerms())
                        .filter(p -> p != null && !p.isEmpty())
                        .collect(Collectors.toSet());
        info.put("roles", roles);
        info.put("permissions", perms);
        info.put("homePath", "/dashboard");
        info.put("preferences", user.getPreferences());
        return info;
    }
}
