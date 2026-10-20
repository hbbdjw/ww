package com.ww.runtime.server.security;

import com.ww.runtime.server.RuntimeProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AuthSessionService} 会话生命周期与权限缓存：创建/解析/续期/撤销、
 * 权限缓存命中与回填、按用户/角色淘汰、损坏条目丢弃。
 */
class AuthSessionServiceTest {

    private FakeAuthCache cache;
    private InMemoryRbacStore store;
    private AuthSessionService sessions;

    @BeforeEach
    void setUp() {
        cache = new FakeAuthCache();
        store = new InMemoryRbacStore();
        RuntimeProperties props = new RuntimeProperties(
                new RuntimeProperties.Node("node-1", "localhost", 0, 1.0, ""),
                "memory", 1000, 3, 6, 3.0, 8.0, 2, 200, 3, 100L, 0L,
                null, 4096, 1, 0.1, 30000, 30000, 12000L, 1, false, true,
                null, null, null,
                new RuntimeProperties.Security(30, 10), null, null);
        sessions = new AuthSessionService(cache, store, props);
    }

    @Test
    void sessionCreateResolveRevoke() {
        AuthSessionService.SessionInfo info = new AuthSessionService.SessionInfo(
                1, "alice", "爱丽丝", 0, 100L);
        String token1 = sessions.createSession(info);
        String token2 = sessions.createSession(info);
        assertThat(token1).isNotEqualTo(token2);

        assertThat(sessions.resolve(token1)).hasValueSatisfying(resolved -> {
            assertThat(resolved.userId()).isEqualTo(1);
            assertThat(resolved.username()).isEqualTo("alice");
        });
        assertThat(cache.ttlOf("auth:session:" + token1)).isEqualTo(30 * 60_000L);

        sessions.revoke(token1);
        assertThat(sessions.resolve(token1)).isEmpty();
        sessions.revoke(null);
    }

    @Test
    void touchRenewsTtl() {
        AuthSessionService.SessionInfo info = new AuthSessionService.SessionInfo(
                1, "alice", "爱丽丝", 0, 100L);
        String token = sessions.createSession(info);
        long ttlBefore = cache.ttlOf("auth:session:" + token);
        sessions.touch(token, info);
        assertThat(cache.ttlOf("auth:session:" + token)).isEqualTo(ttlBefore);
        assertThat(cache.stored.get("auth:session:" + token)).isInstanceOf(Map.class);
    }

    @Test
    void permissionCacheBackfillAndHit() {
        long userId = seedUserWithRole();
        String permKey = "auth:perm:user:" + userId;

        RbacStore.UserPermissions first = sessions.permissions(userId);
        assertThat(first.perms()).containsExactly("system:user:list");
        assertThat(cache.stored).containsKey(permKey);
        assertThat(cache.ttlOf(permKey)).isEqualTo(10 * 60_000L);

        // 给角色追加菜单：DB 已变，但缓存值保持（新鲜度由变更路径淘汰保证）
        long secondMenu = store.createMenu(new RbacStore.Menu(0, 0, "新增", "F",
                null, "system:user:add", null, 2, 1, 1));
        long roleId = store.findRoleIdsByUser(userId).get(0);
        store.bindRoleMenus(roleId, List.of(
                store.findMenuIdsByRole(roleId).get(0), secondMenu));

        RbacStore.UserPermissions cached = sessions.permissions(userId);
        assertThat(cached.perms()).containsExactly("system:user:list");
    }

    @Test
    void evictPermissionsByUser() {
        long userId = seedUserWithRole();
        sessions.permissions(userId);
        sessions.evictPermissions(userId);
        assertThat(cache.stored).doesNotContainKey("auth:perm:user:" + userId);
    }

    @Test
    void evictPermissionsForRoleRemovesAllMembers() {
        long roleId = store.createRole(new RbacStore.Role(0, "管理员", "admin", 0,
                1, 1, null, 1, 1));
        long menuId = store.createMenu(new RbacStore.Menu(0, 0, "用户管理", "C",
                null, "system:user:list", null, 1, 1, 1));
        store.bindRoleMenus(roleId, List.of(menuId));

        long user1 = store.createUser(account("alice"));
        long user2 = store.createUser(account("bob"));
        long user3 = store.createUser(account("carol"));
        store.bindUserRoles(user1, List.of(roleId));
        store.bindUserRoles(user2, List.of(roleId));
        // user3 无该角色

        sessions.permissions(user1);
        sessions.permissions(user2);
        sessions.permissions(user3);
        sessions.evictPermissionsForRole(roleId);

        assertThat(cache.stored).doesNotContainKey("auth:perm:user:" + user1);
        assertThat(cache.stored).doesNotContainKey("auth:perm:user:" + user2);
        assertThat(cache.stored).containsKey("auth:perm:user:" + user3);
    }

    @Test
    void corruptSessionIsDiscarded() {
        String token = sessions.createSession(new AuthSessionService.SessionInfo(
                1, "alice", "爱丽丝", 0, 100L));
        cache.stored.put("auth:session:" + token, Map.of("username", "alice"));
        assertThat(sessions.resolve(token)).isEmpty();
        assertThat(cache.stored).doesNotContainKey("auth:session:" + token);
    }

    // ------------------------------------------------------------------
    // 工厂
    // ------------------------------------------------------------------

    private long seedUserWithRole() {
        long userId = store.createUser(account("alice"));
        long roleId = store.createRole(new RbacStore.Role(0, "管理员", "admin", 0,
                1, 1, null, 1, 1));
        long menuId = store.createMenu(new RbacStore.Menu(0, 0, "用户管理", "C",
                null, "system:user:list", null, 1, 1, 1));
        store.bindRoleMenus(roleId, List.of(menuId));
        store.bindUserRoles(userId, List.of(roleId));
        return userId;
    }

    private static RbacStore.UserAccount account(String username) {
        return new RbacStore.UserAccount(0, 0, username, username, "encoded",
                null, null, 0, 1, null, null, 1, 1);
    }

    /** 内存缓存桩：记录 TTL 以便断言。 */
    private static final class FakeAuthCache implements AuthCachePort {

        private final Map<String, Object> stored = new HashMap<>();
        private final Map<String, Long> ttls = new HashMap<>();

        @Override
        public void put(String key, Object value, long ttlMillis) {
            stored.put(key, value);
            ttls.put(key, ttlMillis);
        }

        @Override
        public Object get(String key) {
            return stored.get(key);
        }

        @Override
        public void remove(String key) {
            stored.remove(key);
            ttls.remove(key);
        }

        long ttlOf(String key) {
            return ttls.get(key);
        }
    }
}
