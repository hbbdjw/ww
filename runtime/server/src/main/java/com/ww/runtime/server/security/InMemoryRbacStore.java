package com.ww.runtime.server.security;

import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link RbacStore} 的进程内实现（{@code runtime.store=memory}）：
 * {@link ConcurrentHashMap} 持有实体，关系用 id 集合维护，覆盖式绑定。
 *
 * <p>id 由 {@link AtomicLong} 分配；写方法加锁以保证「分配 id → 放入表」原子。
 * 不触碰数据库，供单测与单机演示。</p>
 */
@Repository
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "runtime.store", havingValue = "memory")
public class InMemoryRbacStore implements RbacStore {

    private final Map<Long, Dept> depts = new ConcurrentHashMap<>();
    private final Map<Long, UserAccount> users = new ConcurrentHashMap<>();
    private final Map<Long, Role> roles = new ConcurrentHashMap<>();
    private final Map<Long, Menu> menus = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> userRoles = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> roleMenus = new ConcurrentHashMap<>();

    private final AtomicLong deptIds = new AtomicLong();
    private final AtomicLong userIds = new AtomicLong();
    private final AtomicLong roleIds = new AtomicLong();
    private final AtomicLong menuIds = new AtomicLong();

    // ------------------------------------------------------------------
    // 部门
    // ------------------------------------------------------------------

    @Override
    public synchronized long createDept(Dept dept) {
        long id = deptIds.incrementAndGet();
        depts.put(id, new Dept(id, dept.parentId(), dept.deptName(), dept.orderNum(),
                dept.leader(), dept.phone(), dept.status(), dept.createdAt(), dept.updatedAt()));
        return id;
    }

    @Override
    public void updateDept(Dept dept) {
        requireExisting(depts, dept.deptId(), "dept");
        depts.put(dept.deptId(), dept);
    }

    @Override
    public Optional<Dept> findDeptById(long deptId) {
        return Optional.ofNullable(depts.get(deptId));
    }

    @Override
    public List<Dept> listDepts() {
        return sorted(depts.values(), Comparator
                .comparingInt(Dept::orderNum)
                .thenComparing(Dept::deptId));
    }

    @Override
    public synchronized void deleteDept(long deptId) {
        depts.remove(deptId);
    }

    // ------------------------------------------------------------------
    // 用户
    // ------------------------------------------------------------------

    @Override
    public synchronized long createUser(UserAccount user) {
        long id = userIds.incrementAndGet();
        users.put(id, new UserAccount(id, user.deptId(), user.username(), user.nickname(),
                user.password(), user.email(), user.phone(), user.gender(), user.status(),
                user.loginAt(), user.remark(), user.createdAt(), user.updatedAt()));
        return id;
    }

    @Override
    public void updateUser(UserAccount user) {
        requireExisting(users, user.userId(), "user");
        users.put(user.userId(), user);
    }

    @Override
    public Optional<UserAccount> findUserById(long userId) {
        return Optional.ofNullable(users.get(userId));
    }

    @Override
    public Optional<UserAccount> findUserByUsername(String username) {
        if (username == null) {
            return Optional.empty();
        }
        return users.values().stream()
                .filter(u -> username.equals(u.username()))
                .findFirst();
    }

    @Override
    public List<UserAccount> listUsers(String keyword, Long deptId) {
        String kw = keyword == null ? null : keyword.trim().toLowerCase();
        List<UserAccount> result = new ArrayList<>();
        for (UserAccount user : users.values()) {
            if (deptId != null && user.deptId() != deptId) {
                continue;
            }
            if (kw != null && !kw.isEmpty()
                    && !user.username().toLowerCase().contains(kw)
                    && !user.nickname().toLowerCase().contains(kw)) {
                continue;
            }
            result.add(user);
        }
        result.sort(Comparator.comparing(UserAccount::userId));
        return List.copyOf(result);
    }

    @Override
    public synchronized void deleteUser(long userId) {
        users.remove(userId);
        userRoles.remove(userId);
    }

    @Override
    public void updateLoginAt(long userId, long loginAt) {
        UserAccount current = requireExisting(users, userId, "user");
        users.put(userId, new UserAccount(current.userId(), current.deptId(), current.username(),
                current.nickname(), current.password(), current.email(), current.phone(),
                current.gender(), current.status(), loginAt, current.remark(),
                current.createdAt(), current.updatedAt()));
    }

    // ------------------------------------------------------------------
    // 用户-角色
    // ------------------------------------------------------------------

    @Override
    public synchronized void bindUserRoles(long userId, List<Long> roleIds) {
        requireExisting(users, userId, "user");
        userRoles.put(userId, new LinkedHashSet<>(validIds(roleIds, roles, "role")));
    }

    @Override
    public List<Long> findRoleIdsByUser(long userId) {
        Set<Long> ids = userRoles.get(userId);
        return ids == null ? List.of() : List.copyOf(ids);
    }

    @Override
    public List<Long> findUserIdsByRole(long roleId) {
        List<Long> result = new ArrayList<>();
        for (Map.Entry<Long, Set<Long>> entry : userRoles.entrySet()) {
            if (entry.getValue().contains(roleId)) {
                result.add(entry.getKey());
            }
        }
        return List.copyOf(result);
    }

    // ------------------------------------------------------------------
    // 角色
    // ------------------------------------------------------------------

    @Override
    public synchronized long createRole(Role role) {
        long id = roleIds.incrementAndGet();
        roles.put(id, new Role(id, role.roleName(), role.roleKey(), role.roleSort(),
                role.status(), role.dataScope(), role.remark(), role.createdAt(), role.updatedAt()));
        return id;
    }

    @Override
    public void updateRole(Role role) {
        requireExisting(roles, role.roleId(), "role");
        roles.put(role.roleId(), role);
    }

    @Override
    public Optional<Role> findRoleById(long roleId) {
        return Optional.ofNullable(roles.get(roleId));
    }

    @Override
    public Optional<Role> findRoleByKey(String roleKey) {
        if (roleKey == null) {
            return Optional.empty();
        }
        return roles.values().stream()
                .filter(r -> roleKey.equals(r.roleKey()))
                .findFirst();
    }

    @Override
    public List<Role> listRoles() {
        return sorted(roles.values(), Comparator
                .comparingInt(Role::roleSort)
                .thenComparing(Role::roleId));
    }

    @Override
    public synchronized void deleteRole(long roleId) {
        roles.remove(roleId);
        roleMenus.remove(roleId);
        userRoles.values().forEach(bound -> bound.remove(roleId));
    }

    // ------------------------------------------------------------------
    // 角色-菜单
    // ------------------------------------------------------------------

    @Override
    public synchronized void bindRoleMenus(long roleId, List<Long> menuIds) {
        requireExisting(roles, roleId, "role");
        roleMenus.put(roleId, new LinkedHashSet<>(validIds(menuIds, menus, "menu")));
    }

    @Override
    public List<Long> findMenuIdsByRole(long roleId) {
        Set<Long> ids = roleMenus.get(roleId);
        return ids == null ? List.of() : List.copyOf(ids);
    }

    // ------------------------------------------------------------------
    // 菜单
    // ------------------------------------------------------------------

    @Override
    public synchronized long createMenu(Menu menu) {
        long id = menuIds.incrementAndGet();
        menus.put(id, new Menu(id, menu.parentId(), menu.menuName(), menu.menuType(),
                menu.path(), menu.perms(), menu.icon(), menu.orderNum(), menu.visible(), menu.status()));
        return id;
    }

    @Override
    public Optional<Menu> findMenuById(long menuId) {
        return Optional.ofNullable(menus.get(menuId));
    }

    @Override
    public List<Menu> listMenus() {
        return sorted(menus.values(), Comparator
                .comparingInt(Menu::orderNum)
                .thenComparing(Menu::menuId));
    }

    // ------------------------------------------------------------------
    // 权限聚合
    // ------------------------------------------------------------------

    @Override
    public UserPermissions loadPermissions(long userId) {
        Set<Long> boundRoles = userRoles.get(userId);
        if (boundRoles == null || boundRoles.isEmpty()) {
            return UserPermissions.empty();
        }
        List<String> roleKeys = new ArrayList<>();
        Set<String> perms = new LinkedHashSet<>();
        for (long roleId : boundRoles) {
            Role role = roles.get(roleId);
            if (role == null || role.status() != 1) {
                continue;
            }
            roleKeys.add(role.roleKey());
            Set<Long> boundMenus = roleMenus.get(roleId);
            if (boundMenus == null) {
                continue;
            }
            for (long menuId : boundMenus) {
                Menu menu = menus.get(menuId);
                if (menu != null && menu.status() == 1 && menu.perms() != null && !menu.perms().isBlank()) {
                    perms.add(menu.perms());
                }
            }
        }
        return new UserPermissions(List.copyOf(roleKeys), List.copyOf(perms));
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    private static <T> T requireExisting(Map<Long, T> table, long id, String label) {
        T existing = table.get(id);
        if (existing == null) {
            throw new IllegalArgumentException(label + " not found: " + id);
        }
        return existing;
    }

    private static <T> List<Long> validIds(List<Long> ids, Map<Long, T> table, String label) {
        List<Long> valid = new ArrayList<>();
        if (ids == null) {
            return valid;
        }
        for (Long id : ids) {
            if (id != null && table.containsKey(id)) {
                valid.add(id);
            } else if (id != null) {
                throw new IllegalArgumentException(label + " not found: " + id);
            }
        }
        return valid;
    }

    private static <T> List<T> sorted(java.util.Collection<T> values, Comparator<T> order) {
        List<T> list = new ArrayList<>(values);
        list.sort(order);
        return List.copyOf(list);
    }
}
