package com.ww.runtime.server.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * RBAC 幂等播种：仅在仓库中【不存在任何用户】时执行首次引导——
 * 根部门、系统管理目录与 8 个菜单（含按钮权限）、超级管理员角色、admin 用户。
 *
 * <p>默认账号 {@code admin / admin123}，密码以 {@link PasswordEncoder}
 * 加密后落库（不写明文）；重复执行直接跳过，不覆盖任何既有数据。</p>
 *
 * <p>老版本库的增量菜单由 {@link #ensureLatestMenus()} 在每次启动时幂等补齐
 * （缺失才插入并追加绑定给超级管理员），与首次播种互不干扰。</p>
 */
@Component
public final class RbacSeeder {

    private static final Logger LOG = LoggerFactory.getLogger(RbacSeeder.class);

    public static final String DEFAULT_USERNAME = "admin";
    public static final String DEFAULT_PASSWORD = "admin123";
    public static final String SUPER_ADMIN_KEY = "super_admin";

    private final RbacStore store;
    private final PasswordEncoder passwordEncoder;

    public RbacSeeder(RbacStore store, PasswordEncoder passwordEncoder) {
        this.store = store;
        this.passwordEncoder = passwordEncoder;
    }

    /**
     * 执行首次播种。
     *
     * @return true 表示本次完成播种；false 表示已有用户、跳过
     */
    public boolean seedIfEmpty() {
        if (!store.listUsers(null, null).isEmpty()) {
            LOG.info("RBAC seed skipped: users already exist");
            return false;
        }
        long now = System.currentTimeMillis();

        long rootDeptId = store.createDept(new RbacStore.Dept(0, 0L, "总公司", 0,
                null, null, 1, now, now));

        long directoryId = store.createMenu(new RbacStore.Menu(0, 0L, "系统管理", "M",
                "/system", null, "mdi:cog-outline", 0, 1, 1));

        List<Long> allMenuIds = new ArrayList<>();
        allMenuIds.add(directoryId);
        seedMenus(directoryId, allMenuIds);
        seedButtons(directoryId, allMenuIds);

        long roleId = store.createRole(new RbacStore.Role(0, "超级管理员",
                SUPER_ADMIN_KEY, 0, 1, 1, "系统内置超级管理员", now, now));
        store.bindRoleMenus(roleId, allMenuIds);

        String encoded = passwordEncoder.encode(DEFAULT_PASSWORD);
        long userId = store.createUser(new RbacStore.UserAccount(0, rootDeptId,
                DEFAULT_USERNAME, "超级管理员", encoded, null, null, 0, 1,
                null, "系统内置超级管理员", now, now));
        store.bindUserRoles(userId, List.of(roleId));

        LOG.info("RBAC seed completed: dept=总公司, role=super_admin, user={} (default password: {})",
                DEFAULT_USERNAME, DEFAULT_PASSWORD);
        return true;
    }

    /** 系统管理目录下的 8 个菜单。 */
    private void seedMenus(long directoryId, List<Long> allMenuIds) {
        record SeedMenu(String name, String path, String perms, String icon, int order) {
        }
        List<SeedMenu> menus = List.of(
                new SeedMenu("用户管理", "/system/users", "system:user:list", "mdi:account-multiple", 1),
                new SeedMenu("部门管理", "/system/depts", "system:dept:list", "mdi:sitemap-outline", 2),
                new SeedMenu("角色管理", "/system/roles", "system:role:list", "mdi:shield-account-outline", 3),
                new SeedMenu("集群管理", "/system/cluster", "system:cluster:list", "mdi:server-network", 4),
                new SeedMenu("缓存管理", "/system/cache", "system:cache:list", "mdi:cached", 5),
                new SeedMenu("存储管理", "/system/storage", "system:storage:list", "mdi:database", 6),
                new SeedMenu("日志管理", "/system/logs", "system:log:list", "mdi:text-box-multiple-outline", 7),
                new SeedMenu("运维监控", "/system/monitor", "system:monitor:list", "mdi:chart-line", 8)
        );
        for (SeedMenu menu : menus) {
            long id = store.createMenu(new RbacStore.Menu(0, directoryId, menu.name(), "C",
                    menu.path(), menu.perms(), menu.icon(), menu.order(), 1, 1));
            allMenuIds.add(id);
        }
    }

    /** 用户/部门/角色管理的按钮级权限。 */
    private void seedButtons(long directoryId, List<Long> allMenuIds) {
        List<String> perms = List.of(
                "system:user:add", "system:user:edit", "system:user:remove", "system:user:reset",
                "system:dept:add", "system:dept:edit", "system:dept:remove",
                "system:role:add", "system:role:edit", "system:role:remove"
        );
        int order = 0;
        for (String perm : perms) {
            long id = store.createMenu(new RbacStore.Menu(0, directoryId, perm, "F",
                    null, perm, null, 100 + order++, 1, 1));
            allMenuIds.add(id);
        }
    }

    /**
     * 增量菜单幂等补齐（老版本库升级）：每次启动执行，仅在菜单缺失时插入，
     * 并追加绑定给超级管理员角色；已存在则跳过，不覆盖任何既有授权。
     *
     * <p>新库由 {@link #seedIfEmpty()} 首次播种时已含全部菜单，本方法检查到
     * path 存在即跳过。补播种失败只告警不阻断启动，下个启动周期自然重试。</p>
     *
     * @return 本次实际新增的菜单数
     */
    public int ensureLatestMenus() {
        record IncrementalMenu(String name, String path, String perms, String icon, int order) {
        }
        // 版本迭代新增的页面菜单按时间顺序追加于此
        List<IncrementalMenu> wanted = List.of(
                new IncrementalMenu("运维监控", "/system/monitor",
                        "system:monitor:list", "mdi:chart-line", 8)
        );
        try {
            List<RbacStore.Menu> all = store.listMenus();
            Long directoryId = all.stream()
                    .filter(m -> "M".equals(m.menuType()) && "/system".equals(m.path()))
                    .map(RbacStore.Menu::menuId)
                    .findFirst()
                    .orElse(null);
            if (directoryId == null) {
                // 系统管理目录尚不存在（极端早期状态），等 seedIfEmpty 或下次启动
                return 0;
            }
            int added = 0;
            for (IncrementalMenu menu : wanted) {
                boolean exists = all.stream().anyMatch(m -> menu.path().equals(m.path()));
                if (exists) {
                    continue;
                }
                long menuId = store.createMenu(new RbacStore.Menu(0, directoryId, menu.name(), "C",
                        menu.path(), menu.perms(), menu.icon(), menu.order(), 1, 1));
                store.findRoleByKey(SUPER_ADMIN_KEY).ifPresent(role -> {
                    List<Long> ids = new ArrayList<>(store.findMenuIdsByRole(role.roleId()));
                    if (!ids.contains(menuId)) {
                        ids.add(menuId);
                        store.bindRoleMenus(role.roleId(), ids);
                    }
                });
                added++;
                LOG.info("RBAC incremental menu added: {} ({}, perms={})",
                        menu.name(), menu.path(), menu.perms());
            }
            return added;
        } catch (RuntimeException e) {
            LOG.warn("RBAC incremental menu ensure failed (will retry on next startup): {}", e.getMessage());
            return 0;
        }
    }
}
