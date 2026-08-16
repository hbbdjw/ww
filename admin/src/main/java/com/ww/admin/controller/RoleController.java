package com.ww.admin.controller;

import com.ww.admin.common.JsonResult;
import com.ww.admin.entity.SysRole;
import com.ww.admin.service.RoleService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 角色控制器：管理角色及其菜单权限映射、部门数据范围映射（映射关系持久化在数据库）。
 * 所有接口均需 {@code sys:role:*} 权限。
 */
@RestController
@RequestMapping("/role")
@RequiresPermissions("sys:role:*")
public class RoleController {

    private final RoleService roleService;

    public RoleController(RoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping("/list")
    public JsonResult list() {
        List<SysRole> roles = roleService.findAll();
        return JsonResult.success(roles);
    }

    /**
     * 查询角色已分配的菜单ID列表
     */
    @GetMapping("/{id}/menuIds")
    public JsonResult getMenuIds(@PathVariable Long id) {
        return JsonResult.success(roleService.findMenuIds(id));
    }

    /**
     * 查询角色自定义数据范围的部门ID列表
     */
    @GetMapping("/{id}/deptIds")
    public JsonResult getDeptIds(@PathVariable Long id) {
        return JsonResult.success(roleService.findDeptIds(id));
    }

    @GetMapping("/{id:\\d+}")
    public JsonResult get(@PathVariable Long id) {
        return JsonResult.success(roleService.findById(id));
    }

    @PostMapping("/save")
    public JsonResult save(@RequestBody SysRole role) {
        roleService.save(role);
        return JsonResult.success(role.getId(), "保存成功");
    }

    @DeleteMapping("/{id:\\d+}")
    public JsonResult delete(@PathVariable Long id) {
        roleService.delete(id);
        return JsonResult.success("删除成功");
    }

    /**
     * 为角色分配菜单/按钮权限（全量覆盖）。
     * <p>请求体：{"roleId":1,"menuIds":[1,2,3]}
     */
    @PostMapping("/assignMenus")
    public JsonResult assignMenus(@RequestBody Map<String, Object> body) {
        Long roleId = ((Number) body.get("roleId")).longValue();
        @SuppressWarnings("unchecked")
        List<Number> ids = (List<Number>) body.get("menuIds");
        List<Long> menuIds = ids.stream().map(Number::longValue).toList();
        roleService.assignMenus(roleId, menuIds);
        return JsonResult.success("菜单权限分配成功");
    }

    /**
     * 为角色分配自定义数据范围部门（全量覆盖，仅 data_scope=2 时有意义）。
     * <p>请求体：{"roleId":1,"deptIds":[1,2]}
     */
    @PostMapping("/assignDepts")
    public JsonResult assignDepts(@RequestBody Map<String, Object> body) {
        Long roleId = ((Number) body.get("roleId")).longValue();
        @SuppressWarnings("unchecked")
        List<Number> ids = (List<Number>) body.get("deptIds");
        List<Long> deptIds = ids == null ? List.of() : ids.stream().map(Number::longValue).toList();
        roleService.assignDepts(roleId, deptIds);
        return JsonResult.success("数据范围部门分配成功");
    }
}
