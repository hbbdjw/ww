package com.ww.admin.controller;

import com.ww.admin.common.JsonResult;
import com.ww.admin.entity.SysMenu;
import com.ww.admin.service.MenuService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 菜单权限控制器：管理菜单/按钮定义（角色-菜单映射见 {@link RoleController}）。
 * 所有接口均需 {@code sys:menu:*} 权限。
 * <p>
 * SysMenu.perms 即 Shiro 权限标识，替代旧的 Permission.code。
 */
@RestController
@RequestMapping("/menu")
@RequiresPermissions("sys:menu:*")
public class MenuController {

    private final MenuService menuService;

    public MenuController(MenuService menuService) {
        this.menuService = menuService;
    }

    @GetMapping("/list")
    public JsonResult list() {
        List<SysMenu> menus = menuService.findAll();
        return JsonResult.success(menus);
    }

    @GetMapping("/{id:\\d+}")
    public JsonResult get(@PathVariable Long id) {
        return JsonResult.success(menuService.findById(id));
    }

    @PostMapping("/save")
    public JsonResult save(@RequestBody SysMenu menu) {
        menuService.save(menu);
        return JsonResult.success(menu.getId(), "保存成功");
    }

    @DeleteMapping("/{id:\\d+}")
    public JsonResult delete(@PathVariable Long id) {
        menuService.delete(id);
        return JsonResult.success("删除成功");
    }
}
