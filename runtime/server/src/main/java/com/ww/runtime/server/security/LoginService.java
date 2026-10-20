package com.ww.runtime.server.security;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * 登录服务：校验用户名密码 → 更新登录时间 → 创建缓存会话 → 返回 token。
 *
 * <p>认证与授权分离：本服务只负责身份核验与会话签发，权限由过滤器在后续请求加载。
 * 用户不存在、密码错误统一返回相同的凭证错误，避免账号枚举；
 * 缓存层异常不吞（如能力未启动），由控制器映射为 503。</p>
 */
@Service
public final class LoginService {

    private final RbacStore store;
    private final PasswordEncoder passwordEncoder;
    private final AuthSessionService sessions;

    public LoginService(RbacStore store,
                        PasswordEncoder passwordEncoder,
                        AuthSessionService sessions) {
        this.store = store;
        this.passwordEncoder = passwordEncoder;
        this.sessions = sessions;
    }

    /** 执行登录，成功返回 token 与用户信息。 */
    public LoginResult login(String username, String password) {
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new BadCredentialsException("用户名或密码错误");
        }
        RbacStore.UserAccount user = store.findUserByUsername(username.trim())
                .orElseThrow(() -> new BadCredentialsException("用户名或密码错误"));
        if (user.status() != 1) {
            throw new DisabledException("账号已停用，请联系管理员");
        }
        if (!passwordEncoder.matches(password, user.password())) {
            throw new BadCredentialsException("用户名或密码错误");
        }

        long now = System.currentTimeMillis();
        store.updateLoginAt(user.userId(), now);
        AuthSessionService.SessionInfo session = new AuthSessionService.SessionInfo(
                user.userId(), user.username(), user.nickname(), user.deptId(), now);
        String token = sessions.createSession(session);
        return new LoginResult(token, AuthenticatedUser.from(session));
    }

    /** 登录结果。 */
    public record LoginResult(String token, AuthenticatedUser user) {
    }
}
