package com.ww.runtime.server.web.system;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.server.security.AuthSessionService;
import com.ww.runtime.server.security.AuthenticatedUser;
import com.ww.runtime.server.security.RbacSeeder;
import com.ww.runtime.server.security.RbacStore;
import com.ww.runtime.server.security.RbacStore.Role;
import com.ww.runtime.server.security.RbacStore.UserAccount;

/**
 * 用户管理端点：分页查询、增删改、重置密码、分配角色。
 * 按钮级权限：{@code system:user:list/add/edit/remove/reset}。
 */
@RestController
@RequestMapping("/api/system/users")
public class UserManageController {

    private static final int MAX_PAGE_SIZE = 200;
    private static final String DEFAULT_NEW_PASSWORD = "123456";

    private final RbacStore store;
    private final PasswordEncoder passwordEncoder;
    private final AuthSessionService sessions;

    public UserManageController(RbacStore store,
                                PasswordEncoder passwordEncoder,
                                AuthSessionService sessions) {
        this.store = store;
        this.passwordEncoder = passwordEncoder;
        this.sessions = sessions;
    }

    /** 分页查询用户（含部门名与角色信息）。 */
    @GetMapping
    @PreAuthorize("hasAuthority('system:user:list')")
    public Map<String, Object> list(@RequestParam(required = false) String keyword,
                                    @RequestParam(required = false) Long deptId,
                                    @RequestParam(defaultValue = "1") int page,
                                    @RequestParam(defaultValue = "10") int size) {
        List<UserAccount> all = store.listUsers(keyword, deptId);
        int safePage = Math.max(1, page);
        int safeSize = Math.max(1, Math.min(size, MAX_PAGE_SIZE));
        int from = Math.min((safePage - 1) * safeSize, all.size());
        int to = Math.min(from + safeSize, all.size());
        List<Map<String, Object>> items = all.subList(from, to).stream()
                .map(this::toMap).toList();
        return Map.of("total", all.size(), "items", items);
    }

    /** 新建用户（用户名唯一；未给密码时使用默认密码 123456）。 */
    @PostMapping
    @PreAuthorize("hasAuthority('system:user:add')")
    public Map<String, Object> create(@RequestBody Map<String, Object> body) {
        String username = requireText(body, "username");
        if (store.findUserByUsername(username).isPresent()) {
            throw new IllegalArgumentException("用户名已存在：" + username);
        }
        String nickname = optionalText(body, "nickname", username);
        String rawPassword = optionalText(body, "password", DEFAULT_NEW_PASSWORD);
        long now = System.currentTimeMillis();

        UserAccount user = new UserAccount(0, longValue(body.get("deptId"), 0L),
                username, nickname, passwordEncoder.encode(rawPassword),
                optionalTextOrNull(body, "email"), optionalTextOrNull(body, "phone"),
                (int) longValue(body.get("gender"), 0L),
                (int) longValue(body.get("status"), 1L),
                null, optionalTextOrNull(body, "remark"), now, now);
        long userId = store.createUser(user);
        store.bindUserRoles(userId, idList(body.get("roleIds")));
        return Map.of("id", userId, "ok", true);
    }

    /** 修改用户基本信息与角色（不改密码；密码走重置接口）。 */
    @PutMapping("/{id}")
    @PreAuthorize("hasAuthority('system:user:edit')")
    public Map<String, Object> update(@PathVariable long id,
                                      @RequestBody Map<String, Object> body) {
        UserAccount existing = requireUser(id);
        String nickname = optionalText(body, "nickname", existing.nickname());
        long now = System.currentTimeMillis();
        UserAccount updated = new UserAccount(existing.userId(),
                longValue(body.get("deptId"), existing.deptId()),
                existing.username(), nickname, existing.password(),
                optionalTextOrNull(body, "email"), optionalTextOrNull(body, "phone"),
                (int) longValue(body.get("gender"), existing.gender()),
                (int) longValue(body.get("status"), existing.status()),
                existing.loginAt(), optionalTextOrNull(body, "remark"),
                existing.createdAt(), now);
        store.updateUser(updated);
        if (body.containsKey("roleIds")) {
            store.bindUserRoles(id, idList(body.get("roleIds")));
        }
        sessions.evictPermissions(id);
        return Map.of("id", id, "ok", true);
    }

    /** 删除用户（内置 admin 与当前登录用户不可删）。 */
    @DeleteMapping("/{id}")
    @PreAuthorize("hasAuthority('system:user:remove')")
    public Map<String, Object> delete(@PathVariable long id,
                                      @AuthenticationPrincipal AuthenticatedUser self) {
        UserAccount existing = requireUser(id);
        if (RbacSeeder.DEFAULT_USERNAME.equals(existing.username())) {
            throw new IllegalArgumentException("内置超级管理员账号不可删除");
        }
        if (self.userId() == id) {
            throw new IllegalArgumentException("不能删除当前登录用户");
        }
        store.deleteUser(id);
        sessions.evictPermissions(id);
        return Map.of("ok", true);
    }

    /** 重置密码。 */
    @PutMapping("/{id}/reset-password")
    @PreAuthorize("hasAuthority('system:user:reset')")
    public Map<String, Object> resetPassword(@PathVariable long id,
                                             @RequestBody Map<String, Object> body) {
        UserAccount existing = requireUser(id);
        String rawPassword = requireText(body, "password");
        long now = System.currentTimeMillis();
        UserAccount updated = new UserAccount(existing.userId(), existing.deptId(),
                existing.username(), existing.nickname(), passwordEncoder.encode(rawPassword),
                existing.email(), existing.phone(), existing.gender(), existing.status(),
                existing.loginAt(), existing.remark(), existing.createdAt(), now);
        store.updateUser(updated);
        return Map.of("ok", true);
    }

    // ------------------------------------------------------------------
    // 映射与工具
    // ------------------------------------------------------------------

    private Map<String, Object> toMap(UserAccount user) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("userId", user.userId());
        map.put("deptId", user.deptId());
        map.put("deptName", store.findDeptById(user.deptId()).map(RbacStore.Dept::deptName).orElse(null));
        map.put("username", user.username());
        map.put("nickname", user.nickname());
        map.put("email", user.email());
        map.put("phone", user.phone());
        map.put("gender", user.gender());
        map.put("status", user.status());
        map.put("loginAt", user.loginAt());
        map.put("remark", user.remark());

        List<Long> roleIds = store.findRoleIdsByUser(user.userId());
        List<String> roleNames = new ArrayList<>();
        for (long roleId : roleIds) {
            store.findRoleById(roleId).map(Role::roleName).ifPresent(roleNames::add);
        }
        map.put("roleIds", roleIds);
        map.put("roleNames", roleNames);
        return map;
    }

    private UserAccount requireUser(long id) {
        return store.findUserById(id)
                .orElseThrow(() -> new IllegalArgumentException("用户不存在：" + id));
    }

    private static String requireText(Map<String, Object> body, String field) {
        String value = optionalTextOrNull(body, field);
        if (value == null) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
        return value;
    }

    private static String optionalText(Map<String, Object> body, String field, String fallback) {
        String value = optionalTextOrNull(body, field);
        return value == null ? fallback : value;
    }

    private static String optionalTextOrNull(Map<String, Object> body, String field) {
        Object value = body == null ? null : body.get(field);
        if (!(value instanceof String s) || s.isBlank()) {
            return null;
        }
        return s.trim();
    }

    @SuppressWarnings("unchecked")
    private static List<Long> idList(Object value) {
        List<Long> result = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Number n) {
                    result.add(n.longValue());
                }
            }
        }
        return List.copyOf(result);
    }

    private static long longValue(Object value, long fallback) {
        return value instanceof Number n ? n.longValue() : fallback;
    }
}
