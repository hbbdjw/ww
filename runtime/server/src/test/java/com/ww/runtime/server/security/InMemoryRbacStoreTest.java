package com.ww.runtime.server.security;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link InMemoryRbacStore} 基础行为：CRUD、关系绑定、过滤查询与权限聚合。
 */
class InMemoryRbacStoreTest {

    private InMemoryRbacStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryRbacStore();
    }

    @Test
    void deptCrudAndOrdering() {
        long second = store.createDept(dept("研发部", 2));
        long first = store.createDept(dept("总公司", 0));

        assertThat(store.findDeptById(first)).isPresent();
        assertThat(store.listDepts()).extracting(RbacStore.Dept::deptName)
                .containsExactly("总公司", "研发部");

        store.updateDept(new RbacStore.Dept(second, 0L, "研发中心", 1,
                null, null, 1, 1, 2));
        assertThat(store.findDeptById(second)).map(RbacStore.Dept::deptName).hasValue("研发中心");

        store.deleteDept(second);
        assertThat(store.findDeptById(second)).isEmpty();
        assertThatThrownBy(() -> store.updateDept(new RbacStore.Dept(second, 0L, "x", 0,
                null, null, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void userCrudFilteringAndCascadeRoles() {
        long deptId = store.createDept(dept("研发部", 0));
        long user1 = store.createUser(user("alice", "爱丽丝", deptId));
        store.createUser(user("bob", "鲍勃", deptId));

        assertThat(store.findUserByUsername("alice")).isPresent();
        assertThat(store.findUserByUsername("ALICE")).isEmpty();
        assertThat(store.listUsers("ali", null)).extracting(u -> u.username()).containsExactly("alice");
        assertThat(store.listUsers("爱", null)).extracting(u -> u.username()).containsExactly("alice");
        assertThat(store.listUsers(null, deptId)).hasSize(2);
        assertThat(store.listUsers(null, 999L)).isEmpty();

        long roleId = store.createRole(role("管理员", "admin", 0));
        store.bindUserRoles(user1, List.of(roleId));
        assertThat(store.findRoleIdsByUser(user1)).containsExactly(roleId);

        // 删除用户后其角色关系一并清除
        store.deleteUser(user1);
        assertThat(store.findUserById(user1)).isEmpty();
        assertThat(store.findRoleIdsByUser(user1)).isEmpty();
    }

    @Test
    void roleAndMenuCrud() {
        long roleId = store.createRole(role("管理员", "admin", 0));
        long menuId = store.createMenu(menu("用户管理", "C", "system:user:list", 1));

        store.bindRoleMenus(roleId, List.of(menuId));
        assertThat(store.findMenuIdsByRole(roleId)).containsExactly(menuId);
        // 覆盖式绑定
        store.bindRoleMenus(roleId, List.of());
        assertThat(store.findMenuIdsByRole(roleId)).isEmpty();

        assertThat(store.listMenus()).extracting(RbacStore.Menu::menuName).containsExactly("用户管理");
        assertThat(store.findRoleByKey("admin")).isPresent();
    }

    @Test
    void bindValidatesReferences() {
        long userId = store.createUser(user("alice", "爱丽丝", 0));
        long roleId = store.createRole(role("管理员", "admin", 0));

        assertThatThrownBy(() -> store.bindUserRoles(userId, List.of(999L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.bindRoleMenus(roleId, List.of(999L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void permissionsAggregatedFromRolesAndMenus() {
        long userId = store.createUser(user("alice", "爱丽丝", 0));
        long roleId = store.createRole(role("管理员", "admin", 0));
        long menuList = store.createMenu(menu("用户管理", "C", "system:user:list", 1));
        long menuAdd = store.createMenu(menu("新增", "F", "system:user:add", 2));
        store.bindRoleMenus(roleId, List.of(menuList, menuAdd));
        store.bindUserRoles(userId, List.of(roleId));

        RbacStore.UserPermissions permissions = store.loadPermissions(userId);
        assertThat(permissions.roleKeys()).containsExactly("admin");
        assertThat(permissions.perms()).containsExactly("system:user:list", "system:user:add");

        // 停用角色后不再贡献其菜单权限
        RbacStore.Role disabledRole = new RbacStore.Role(roleId, "管理员", "admin", 0, 0, 1,
                null, 1, 1);
        store.updateRole(disabledRole);
        assertThat(store.loadPermissions(userId).perms()).isEmpty();
    }

    @Test
    void deletingRoleCleansBindings() {
        long userId = store.createUser(user("alice", "爱丽丝", 0));
        long roleId = store.createRole(role("管理员", "admin", 0));
        store.bindUserRoles(userId, List.of(roleId));

        store.deleteRole(roleId);
        assertThat(store.findRoleIdsByUser(userId)).isEmpty();
        assertThat(store.findUserIdsByRole(roleId)).isEmpty();
    }

    // ------------------------------------------------------------------
    // 工厂
    // ------------------------------------------------------------------

    private static RbacStore.Dept dept(String name, int order) {
        return new RbacStore.Dept(0, 0L, name, order, null, null, 1, 1, 1);
    }

    private static RbacStore.UserAccount user(String username, String nickname, long deptId) {
        return new RbacStore.UserAccount(0, deptId, username, nickname, "encoded",
                null, null, 0, 1, null, null, 1, 1);
    }

    private static RbacStore.Role role(String name, String key, int sort) {
        return new RbacStore.Role(0, name, key, sort, 1, 1, null, 1, 1);
    }

    private static RbacStore.Menu menu(String name, String type, String perms, int order) {
        return new RbacStore.Menu(0, 0L, name, type, null, perms, null, order, 1, 1);
    }
}
