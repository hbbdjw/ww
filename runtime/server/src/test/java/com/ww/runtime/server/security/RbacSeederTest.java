package com.ww.runtime.server.security;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RbacSeeder} 首次播种与幂等性：完整内容、密码加密、重复执行不覆盖。
 */
class RbacSeederTest {

    private InMemoryRbacStore store;
    private PasswordEncoder passwordEncoder;
    private RbacSeeder seeder;

    @BeforeEach
    void setUp() {
        store = new InMemoryRbacStore();
        passwordEncoder = new BCryptPasswordEncoder();
        seeder = new RbacSeeder(store, passwordEncoder);
    }

    @Test
    void firstSeedCreatesCompleteModel() {
        assertThat(seeder.seedIfEmpty()).isTrue();

        List<RbacStore.Dept> depts = store.listDepts();
        assertThat(depts).hasSize(1);
        assertThat(depts.get(0).deptName()).isEqualTo("总公司");

        List<RbacStore.Menu> menus = store.listMenus();
        long directoryCount = menus.stream().filter(m -> "M".equals(m.menuType())).count();
        long menuCount = menus.stream().filter(m -> "C".equals(m.menuType())).count();
        long buttonCount = menus.stream().filter(m -> "F".equals(m.menuType())).count();
        assertThat(directoryCount).isEqualTo(1);
        assertThat(menuCount).isEqualTo(8);
        assertThat(buttonCount).isEqualTo(10);

        RbacStore.Role superAdmin = store.findRoleByKey(RbacSeeder.SUPER_ADMIN_KEY).orElseThrow();
        assertThat(superAdmin.roleName()).isEqualTo("超级管理员");
        // 超管角色绑定全部菜单
        assertThat(store.findMenuIdsByRole(superAdmin.roleId())).hasSize((int) menus.size());

        RbacStore.UserAccount admin = store.findUserByUsername(RbacSeeder.DEFAULT_USERNAME).orElseThrow();
        assertThat(admin.nickname()).isEqualTo("超级管理员");
        assertThat(admin.deptId()).isEqualTo(depts.get(0).deptId());
        assertThat(passwordEncoder.matches(RbacSeeder.DEFAULT_PASSWORD, admin.password())).isTrue();
        assertThat(store.findRoleIdsByUser(admin.userId())).containsExactly(superAdmin.roleId());

        // 超管聚合得到全部 8 个 list 权限与 10 个按钮权限
        RbacStore.UserPermissions permissions = store.loadPermissions(admin.userId());
        assertThat(permissions.roleKeys()).containsExactly(RbacSeeder.SUPER_ADMIN_KEY);
        assertThat(permissions.perms()).hasSize(18);
        assertThat(permissions.perms())
                .contains("system:user:list", "system:user:add", "system:cluster:list",
                        "system:log:list", "system:monitor:list");
    }

    @Test
    void ensureLatestMenusBackfillsMissingMenuForExistingDeployment() {
        // 模拟老版本库：系统管理目录 + 一个老菜单 + 超级管理员角色，但尚无运维监控菜单
        long directoryId = store.createMenu(new RbacStore.Menu(0, 0L, "系统管理", "M",
                "/system", null, "mdi:cog-outline", 0, 1, 1));
        store.createMenu(new RbacStore.Menu(0, directoryId, "日志管理", "C",
                "/system/logs", "system:log:list", "mdi:text-box-multiple-outline", 7, 1, 1));
        long roleId = store.createRole(new RbacStore.Role(0, "超级管理员",
                RbacSeeder.SUPER_ADMIN_KEY, 0, 1, 1, null, 1L, 1L));
        store.bindRoleMenus(roleId, List.of(directoryId));

        assertThat(seeder.ensureLatestMenus()).isEqualTo(1);

        RbacStore.Menu monitor = store.listMenus().stream()
                .filter(m -> "/system/monitor".equals(m.path()))
                .findFirst()
                .orElseThrow();
        assertThat(monitor.menuType()).isEqualTo("C");
        assertThat(monitor.perms()).isEqualTo("system:monitor:list");
        assertThat(monitor.icon()).isEqualTo("mdi:chart-line");
        // 追加绑定给超级管理员，且不影响既有授权
        assertThat(store.findMenuIdsByRole(roleId)).containsExactly(directoryId, monitor.menuId());

        // 幂等：再次执行不重复插入
        assertThat(seeder.ensureLatestMenus()).isZero();
        long count = store.listMenus().stream()
                .filter(m -> "/system/monitor".equals(m.path()))
                .count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void ensureLatestMenusIsNoOpWhenMenuAlreadyPresent() {
        seeder.seedIfEmpty();
        int menuCount = store.listMenus().size();

        assertThat(seeder.ensureLatestMenus()).isZero();
        assertThat(store.listMenus()).hasSize(menuCount);
    }

    @Test
    void secondSeedIsIdempotentAndDoesNotOverwrite() {
        seeder.seedIfEmpty();
        int deptCount = store.listDepts().size();
        int menuCount = store.listMenus().size();
        int roleCount = store.listRoles().size();

        assertThat(seeder.seedIfEmpty()).isFalse();
        assertThat(store.listDepts()).hasSize(deptCount);
        assertThat(store.listMenus()).hasSize(menuCount);
        assertThat(store.listRoles()).hasSize(roleCount);
    }

    @Test
    void seedSkippedWhenAnyUserExists() {
        store.createUser(new RbacStore.UserAccount(0, 0, "someone", "某人", "encoded",
                null, null, 0, 1, null, null, 1, 1));

        assertThat(seeder.seedIfEmpty()).isFalse();
        assertThat(store.findUserByUsername("admin")).isEmpty();
        assertThat(store.listRoles()).isEmpty();
    }
}
