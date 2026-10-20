package com.ww.runtime.server.security;

/**
 * 认证主体：从会话还原的已认证用户身份（不含敏感凭证）。
 *
 * @param userId   用户 id
 * @param username 用户名
 * @param nickname 昵称
 * @param deptId   部门 id
 */
public record AuthenticatedUser(long userId,
                                String username,
                                String nickname,
                                long deptId) {

    public static AuthenticatedUser from(AuthSessionService.SessionInfo session) {
        return new AuthenticatedUser(session.userId(), session.username(),
                session.nickname(), session.deptId());
    }

    @Override
    public String toString() {
        return username;
    }
}
