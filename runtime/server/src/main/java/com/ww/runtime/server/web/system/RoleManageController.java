package com.ww.runtime.server.web.system;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.server.security.AuthSessionService;
import com.ww.runtime.server.security.RbacSeeder;
import com.ww.runtime.server.security.RbacStore;
import com.ww.runtime.server.security.RbacStore.Menu;
import com.ww.runtime.server.security.RbacStore.Role;

/**
 * 角色管理端点：角色查询、增删改、菜单授权，以及菜单树查询。
 * 按钮级权限：{@code system:role:list/add/edit/remove}。
 */
@RestController
@RequestMapping("/api/system")
public class RoleManageController {

    private final RbacStore store;
    private final AuthSessionService sessions;

    public RoleManageController(RbacStore store, AuthSessionService sessions) {
        this.store = store;
        this.sessions = sessions;
    }

    /** 查询全部角色（含菜单 id 列表）。 */
    @GetMapping("/roles")
    @PreAuthorize("hasAuthority('system:role:list')")
    public Map<String, Object> list() {
        return Map.of("items", store.listRoles().stream().map(this::toMap).toList());
    }

    /** 新建角色（role_key 唯一）。 */
    @PostMapping("/roles")
    @PreAuthorize("hasAuthority('system:role:add')")
    public Map<String, Object> create(@RequestBody Map<String, Object> body) {
        String roleName = requireText(body, "roleName");
        String roleKey = requireText(body, "roleKey");
        if (store.findRoleByKey(roleKey).isPresent()) {
            throw new IllegalArgumentException("角色标识已存在：" + roleKey);
        }
        long now = System.currentTimeMillis();
        Role role = new Role(0, roleName, roleKey,
                (int) longValue(body.get("roleSort"), 0),
                (int) longValue(body.get("status"), 1L),
                (int) longValue(body.get("dataScope"), 1L),
                optionalTextOrNull(body, "remark"), now, now);
        long roleId = store.createRole(role);
        if (body.containsKey("menuIds")) {
            store.bindRoleMenus(roleId, idList(body.get("menuIds")));
        }
        return Map.of("id", roleId, "ok", true);
    }

    /** 修改角色基本信息（内置 super_admin 标识不可改）。 */
    @PutMapping("/roles/{id}")
    @PreAuthorize("hasAuthority('system:role:edit')")
    public Map<String, Object> update(@PathVariable long id,
                                      @RequestBody Map<String, Object> body) {
        Role existing = requireRole(id);
        String roleName = optionalText(body, "roleName", existing.roleName());
        long now = System.currentTimeMillis();
        Role updated = new Role(existing.roleId(), roleName, existing.roleKey(),
                (int) longValue(body.get("roleSort"), existing.roleSort()),
                (int) longValue(body.get("status"), existing.status()),
                (int) longValue(body.get("dataScope"), existing.dataScope()),
                optionalTextOrNull(body, "remark"), existing.createdAt(), now);
        store.updateRole(updated);
        sessions.evictPermissionsForRole(id);
        return Map.of("id", id, "ok", true);
    }

    /** 删除角色：内置超管角色与仍有用户绑定的角色不可删。 */
    @DeleteMapping("/roles/{id}")
    @PreAuthorize("hasAuthority('system:role:remove')")
    public Map<String, Object> delete(@PathVariable long id) {
        Role existing = requireRole(id);
        if (RbacSeeder.SUPER_ADMIN_KEY.equals(existing.roleKey())) {
            throw new IllegalArgumentException("内置超级管理员角色不可删除");
        }
        if (!store.findUserIdsByRole(id).isEmpty()) {
            throw new IllegalArgumentException("角色仍分配给用户，不允许删除");
        }
        store.deleteRole(id);
        return Map.of("ok", true);
    }

    /** 菜单树（供角色授权勾选，含目录/菜单/按钮全部节点）。 */
    @GetMapping("/menus")
    @PreAuthorize("hasAuthority('system:role:list')")
    public Map<String, Object> menuTree() {
        List<Menu> all = store.listMenus();
        return Map.of("items", buildTree(all, 0L));
    }

    /** 角色菜单授权（覆盖式）。 */
    @PutMapping("/roles/{id}/menus")
    @PreAuthorize("hasAuthority('system:role:edit')")
    public Map<String, Object> assignMenus(@PathVariable long id,
                                           @RequestBody Map<String, Object> body) {
        requireRole(id);
        store.bindRoleMenus(id, idList(body.get("menuIds")));
        sessions.evictPermissionsForRole(id);
        return Map.of("ok", true);
    }

    // ------------------------------------------------------------------
    // 映射与工具
    // ------------------------------------------------------------------

    private Role requireRole(long id) {
        return store.findRoleById(id)
                .orElseThrow(() -> new IllegalArgumentException("角色不存在：" + id));
    }

    private Map<String, Object> toMap(Role role) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("roleId", role.roleId());
        map.put("roleName", role.roleName());
        map.put("roleKey", role.roleKey());
        map.put("roleSort", role.roleSort());
        map.put("status", role.status());
        map.put("dataScope", role.dataScope());
        map.put("remark", role.remark());
        map.put("menuIds", store.findMenuIdsByRole(role.roleId()));
        return map;
    }

    private static List<Map<String, Object>> buildTree(List<Menu> all, long parentId) {
        List<Map<String, Object>> tree = new ArrayList<>();
        for (Menu menu : all) {
            if (menu.parentId() != parentId) {
                continue;
            }
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("menuId", menu.menuId());
            node.put("menuName", menu.menuName());
            node.put("menuType", menu.menuType());
            node.put("path", menu.path());
            node.put("perms", menu.perms());
            node.put("icon", menu.icon());
            node.put("orderNum", menu.orderNum());
            node.put("children", buildTree(all, menu.menuId()));
            tree.add(node);
        }
        return List.copyOf(tree);
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
