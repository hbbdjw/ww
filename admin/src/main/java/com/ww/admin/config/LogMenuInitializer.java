package com.ww.admin.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.ww.admin.entity.SysMenu;
import com.ww.admin.service.MenuService;

/**
 * 日志管理权限菜单初始化器：启动时幂等登记 {@code log:*} 权限菜单（F=按钮/API 类型），
 * 供角色管理页面将其授权给非超级管理员角色。
 * <p>
 * 与 {@link DataInitializer}（仅首次引导）不同，本初始化器每次启动都会执行，
 * 但通过 {@code perms} 唯一性做幂等，已存在则不重复创建，保证已有数据库也能补登记。
 */
@Component
public class LogMenuInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(LogMenuInitializer.class);

    private static final String LOG_PERMS = "log:*";

    private final MenuService menuService;

    public LogMenuInitializer(MenuService menuService) {
        this.menuService = menuService;
    }

    @Override
    @Transactional
    public void run(String... args) {
        if (menuService.findByPerms(LOG_PERMS) != null) {
            return;
        }
        SysMenu menu = new SysMenu();
        menu.setMenuName("日志管理");
        menu.setParentId(0L);
        menu.setMenuType("F");
        menu.setPerms(LOG_PERMS);
        menu.setStatus(1);
        menuService.save(menu);
        log.info("已登记日志管理权限菜单：{}", LOG_PERMS);
    }
}
