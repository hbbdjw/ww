package com.ww.runtime.server.security;

import com.ww.runtime.server.RuntimeProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 认证会话服务：会话与用户权限的创建、解析、滑动续期、撤销与缓存淘汰，
 * 全部状态存于集群 CACHE 能力（{@link AuthCachePort}），数据库仅为权限真源。
 *
 * <h2>凭证生命周期</h2>
 * <ul>
 *   <li>登录生成 32 字节随机不透明 token（URL-safe Base64，无含义不可猜测）；</li>
 *   <li>会话带 TTL，每次访问 {@link #touch} 滑动续期；超时由缓存自动驱逐；</li>
 *   <li>登出 {@link #revoke} 立即删除会话，凭证即刻失效；</li>
 *   <li>权限缓存独立 TTL，任何用户/角色/菜单变更路径同步淘汰。</li>
 * </ul>
 */
@Service
public final class AuthSessionService {

    private static final Logger LOG = LoggerFactory.getLogger(AuthSessionService.class);

    private static final String SESSION_PREFIX = "auth:session:";
    private static final String PERM_PREFIX = "auth:perm:user:";

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder TOKEN_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final AuthCachePort cache;
    private final RbacStore store;
    private final long sessionTtlMillis;
    private final long permTtlMillis;

    public AuthSessionService(AuthCachePort cache,
                              RbacStore store,
                              RuntimeProperties props) {
        this.cache = cache;
        this.store = store;
        this.sessionTtlMillis = props.security().sessionTimeoutMinutes() * 60_000L;
        this.permTtlMillis = props.security().permissionCacheMinutes() * 60_000L;
    }

    /** 登录成功后创建会话，返回不透明 token。 */
    public String createSession(SessionInfo session) {
        byte[] raw = new byte[32];
        RANDOM.nextBytes(raw);
        String token = TOKEN_ENCODER.encodeToString(raw);
        writeSession(token, session);
        return token;
    }

    /** 按 token 解析会话，不存在或已过期返回 empty。 */
    public Optional<SessionInfo> resolve(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        Object value = cache.get(SESSION_PREFIX + token);
        if (!(value instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        try {
            return Optional.of(SessionInfo.fromMap(map));
        } catch (RuntimeException e) {
            LOG.warn("corrupt session discarded: {}", e.getMessage());
            cache.remove(SESSION_PREFIX + token);
            return Optional.empty();
        }
    }

    /** 滑动续期：重写会话并刷新 TTL。 */
    public void touch(String token, SessionInfo session) {
        writeSession(token, session);
    }

    /** 登出：立即删除会话。 */
    public void revoke(String token) {
        if (token != null && !token.isBlank()) {
            cache.remove(SESSION_PREFIX + token);
        }
    }

    /** 加载用户权限：优先权限缓存，未命中则从数据库加载并回填。 */
    public RbacStore.UserPermissions permissions(long userId) {
        String key = PERM_PREFIX + userId;
        Object cached = cache.get(key);
        if (cached instanceof Map<?, ?> map) {
            try {
                return RbacStore.UserPermissions.fromMap(map);
            } catch (RuntimeException e) {
                LOG.warn("corrupt permission cache discarded for user {}: {}", userId, e.getMessage());
                cache.remove(key);
            }
        }
        RbacStore.UserPermissions loaded = store.loadPermissions(userId);
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("roleKeys", loaded.roleKeys());
        value.put("perms", loaded.perms());
        cache.put(key, value, permTtlMillis);
        return loaded;
    }

    /** 淘汰单个用户的权限缓存（用户/用户角色变更时）。 */
    public void evictPermissions(long userId) {
        cache.remove(PERM_PREFIX + userId);
    }

    /** 淘汰某角色下全部用户的权限缓存（角色/角色菜单变更时）。 */
    public void evictPermissionsForRole(long roleId) {
        for (long userId : store.findUserIdsByRole(roleId)) {
            cache.remove(PERM_PREFIX + userId);
        }
    }

    private void writeSession(String token, SessionInfo session) {
        Map<String, Object> value = session.toMap();
        value.put("expireAt", System.currentTimeMillis() + sessionTtlMillis);
        cache.put(SESSION_PREFIX + token, value, sessionTtlMillis);
    }

    /** 会话信息。 */
    public record SessionInfo(long userId,
                              String username,
                              String nickname,
                              long deptId,
                              long loginAt) {

        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("userId", userId);
            map.put("username", username);
            map.put("nickname", nickname);
            map.put("deptId", deptId);
            map.put("loginAt", loginAt);
            return map;
        }

        static SessionInfo fromMap(Map<?, ?> map) {
            return new SessionInfo(
                    ((Number) map.get("userId")).longValue(),
                    (String) map.get("username"),
                    (String) map.get("nickname"),
                    ((Number) map.get("deptId")).longValue(),
                    ((Number) map.get("loginAt")).longValue());
        }
    }
}
