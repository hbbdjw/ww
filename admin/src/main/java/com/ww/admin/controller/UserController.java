package com.ww.admin.controller;

import com.ww.admin.common.JsonResult;
import com.ww.admin.entity.SysRole;
import com.ww.admin.entity.SysUser;
import com.ww.admin.service.RoleService;
import com.ww.admin.service.UserService;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用户控制器：用户增删改查 + 角色分配。所有接口均需 {@code sys:user:*} 权限。
 * <p>列表/分页接口应用数据范围（data_scope）行级过滤，仅返回当前用户可见范围内的用户。
 */
@RestController
@RequestMapping("/user")
@RequiresPermissions("sys:user:*")
public class UserController {

    private final UserService userService;
    private final RoleService roleService;

    public UserController(UserService userService, RoleService roleService) {
        this.userService = userService;
        this.roleService = roleService;
    }

    /**
     * 分页查询用户（按当前用户数据范围过滤）。
     * <p>请求参数：pageNumber（默认1）、pageSize（默认10）、username（可选，模糊匹配）。
     */
    @GetMapping("/page")
    public JsonResult page(@RequestParam(defaultValue = "1") int pageNumber,
                           @RequestParam(defaultValue = "10") int pageSize,
                           @RequestParam(required = false) String username) {
        Pageable pageable = new Pageable();
        pageable.setPageNumber(pageNumber);
        pageable.setPageSize(pageSize);
        Page<SysUser> page = userService.findPageWithScope(pageable, username);
        return JsonResult.success(page);
    }

    @GetMapping("/{id:\\d+}")
    public JsonResult get(@PathVariable Long id) {
        return JsonResult.success(userService.findById(id));
    }

    @PostMapping("/save")
    public JsonResult save(@RequestBody SysUser user) {
        userService.save(user);
        return JsonResult.success(user.getId(), "保存成功");
    }

    @DeleteMapping("/{id:\\d+}")
    public JsonResult delete(@PathVariable Long id) {
        SysUser user = userService.findById(id);
        if (user != null) {
            userService.delete(user);
        }
        return JsonResult.success("删除成功");
    }

    /**
     * 重置用户密码。
     * <p>请求体：{"userId":1,"password":"newPassword123"}
     */
    @PostMapping("/resetPassword")
    public JsonResult resetPassword(@RequestBody Map<String, Object> body) {
        Long userId = ((Number) body.get("userId")).longValue();
        String password = (String) body.get("password");
        if (password == null || password.length() < 6) {
            return JsonResult.error("密码长度不能少于6位");
        }
        userService.resetPassword(userId, password);
        return JsonResult.success("密码重置成功");
    }

    /**
     * 为用户分配角色（全量覆盖）。
     * <p>请求体：{"userId":1,"roleIds":[1,2]}
     */
    @PostMapping("/assignRoles")
    public JsonResult assignRoles(@RequestBody Map<String, Object> body) {
        Long userId = ((Number) body.get("userId")).longValue();
        @SuppressWarnings("unchecked")
        List<Number> ids = (List<Number>) body.get("roleIds");
        SysUser user = userService.findById(userId);
        if (user == null) {
            return JsonResult.error("用户不存在");
        }
        List<SysRole> roles = roleService.findAll();
        Set<Long> targetIds = new HashSet<>();
        for (Number n : ids) {
            targetIds.add(n.longValue());
        }
        Set<SysRole> assigned = new HashSet<>();
        for (SysRole r : roles) {
            if (targetIds.contains(r.getId())) {
                assigned.add(r);
            }
        }
        user.setRoles(assigned);
        userService.save(user);
        return JsonResult.success("角色分配成功");
    }
}
