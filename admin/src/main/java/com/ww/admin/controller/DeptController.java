package com.ww.admin.controller;

import com.ww.admin.common.JsonResult;
import com.ww.admin.entity.SysDept;
import com.ww.admin.service.DeptService;
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
 * 部门控制器：管理部门树形结构。所有接口均需 {@code sys:dept:*} 权限。
 */
@RestController
@RequestMapping("/dept")
@RequiresPermissions("sys:dept:*")
public class DeptController {

    private final DeptService deptService;

    public DeptController(DeptService deptService) {
        this.deptService = deptService;
    }

    @GetMapping("/list")
    public JsonResult list() {
        List<SysDept> depts = deptService.findAll();
        return JsonResult.success(depts);
    }

    @GetMapping("/{id:\\d+}")
    public JsonResult get(@PathVariable Long id) {
        return JsonResult.success(deptService.findById(id));
    }

    @PostMapping("/save")
    public JsonResult save(@RequestBody SysDept dept) {
        deptService.save(dept);
        return JsonResult.success(dept.getId(), "保存成功");
    }

    @DeleteMapping("/{id:\\d+}")
    public JsonResult delete(@PathVariable Long id) {
        deptService.delete(id);
        return JsonResult.success("删除成功");
    }
}
