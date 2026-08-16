package com.ww.admin.config;

import com.ww.admin.entity.SysDept;
import com.ww.admin.entity.SysMenu;
import com.ww.admin.entity.SysRole;
import com.ww.admin.entity.SysUser;
import com.ww.admin.service.DeptService;
import com.ww.admin.service.MenuService;
import com.ww.admin.service.RoleService;
import com.ww.admin.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

/**
 * 数据初始化器：首次启动时若数据库无用户，则引导创建默认部门、菜单、角色与用户。
 * <p>
 * 解决"首位管理员"引导问题：默认账号 admin/admin123，拥有全部权限（*:*）。
 * 请在首次登录后立即修改密码。
 */
@Component
public class DataInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataInitializer.class);

    private final UserService userService;
    private final RoleService roleService;
    private final MenuService menuService;
    private final DeptService deptService;

    public DataInitializer(UserService userService,
                           RoleService roleService,
                           MenuService menuService,
                           DeptService deptService) {
        this.userService = userService;
        this.roleService = roleService;
        this.menuService = menuService;
        this.deptService = deptService;
    }

    @Override
    @Transactional
    public void run(String... args) {
        if (userService.count() > 0) {
            return;
        }
        log.warn("数据库无用户，开始初始化默认管理员账号（admin/admin123），请尽快修改密码！");

        // 1. 创建根部门
        SysDept rootDept = new SysDept();
        rootDept.setParentId(0L);
        rootDept.setAncestors("0");
        rootDept.setDeptName("总公司");
        rootDept.setOrderNum(0);
        rootDept.setStatus(1);
        deptService.save(rootDept);

        // 2. 创建超级权限菜单（F=按钮/API 类型，perms=*:* 匹配所有权限）
        SysMenu allPerm = new SysMenu();
        allPerm.setMenuName("全部权限");
        allPerm.setParentId(0L);
        allPerm.setMenuType("F");
        allPerm.setPerms("*:*");
        allPerm.setStatus(1);
        menuService.save(allPerm);

        // 3. 创建管理员角色并授予超级权限（data_scope=1 全部数据）
        SysRole adminRole = new SysRole();
        adminRole.setRoleName("超级管理员");
        adminRole.setRoleKey("admin");
        adminRole.setDataScope("1");
        adminRole.setStatus(1);
        adminRole.setMenus(Set.of(allPerm));
        roleService.save(adminRole);

        // 4. 创建默认管理员用户
        SysUser admin = new SysUser();
        admin.setUsername("admin");
        admin.setNickname("管理员");
        admin.setPlainPassword("admin123");
        admin.setDept(rootDept);
        admin.setStatus(1);
        admin.setRoles(Set.of(adminRole));
        userService.save(admin);

        log.info("默认管理员初始化完成: admin / admin123, 角色: admin, 权限: *:*, 部门: 总公司");
    }
}
