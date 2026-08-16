package com.ww.admin.security;

import java.io.Serializable;

/**
 * 令牌快照：存入分布式缓存的轻量级令牌验证信息。
 * <p>
 * 避免将 Hibernate 实体（{@link com.ww.admin.entity.AccessToken} + {@link com.ww.admin.entity.SysUser}
 * 及其 EAGER 关联图）直接序列化到缓存，防止代理对象序列化失败。
 * <p>
 * 包含 Token 认证所需的全部字段：用户名（principal）、用户状态（用于禁用判断）、
 * 过期时间戳、是否已吊销。{@link DbRealm#authenticateByToken} 可直接基于此快照完成认证。
 */
public record TokenSnapshot(
        String tokenHash,
        Long userId,
        String username,
        Integer userStatus,
        long expiresAtMillis,
        boolean revoked
) implements Serializable {
}
