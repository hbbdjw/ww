package com.ww.runtime.server.web.system;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.server.security.RbacStore;
import com.ww.runtime.server.security.RbacStore.Dept;

/**
 * 部门管理端点：部门树查询、增删改。
 * 按钮级权限：{@code system:dept:list/add/edit/remove}。
 */
@RestController
@RequestMapping("/api/system/depts")
public class DeptManageController {

    private final RbacStore store;

    public DeptManageController(RbacStore store) {
        this.store = store;
    }

    /** 查询全部部门（返回平铺列表，顺序已按 orderNum 排好；前端按 parentId 组树）。 */
    @GetMapping
    @PreAuthorize("hasAuthority('system:dept:list')")
    public Map<String, Object> list() {
        return Map.of("items", store.listDepts().stream().map(DeptManageController::toMap).toList());
    }

    /** 新建部门。 */
    @PostMapping
    @PreAuthorize("hasAuthority('system:dept:add')")
    public Map<String, Object> create(@RequestBody Map<String, Object> body) {
        String deptName = requireText(body, "deptName");
        long now = System.currentTimeMillis();
        Dept dept = new Dept(0, longValue(body.get("parentId"), 0L), deptName,
                (int) longValue(body.get("orderNum"), 0),
                optionalTextOrNull(body, "leader"), optionalTextOrNull(body, "phone"),
                (int) longValue(body.get("status"), 1L), now, now);
        long deptId = store.createDept(dept);
        return Map.of("id", deptId, "ok", true);
    }

    /** 修改部门（禁止把自身设为上级）。 */
    @PutMapping("/{id}")
    @PreAuthorize("hasAuthority('system:dept:edit')")
    public Map<String, Object> update(@PathVariable long id,
                                      @RequestBody Map<String, Object> body) {
        Dept existing = requireDept(id);
        long parentId = longValue(body.get("parentId"), existing.parentId());
        if (parentId == id) {
            throw new IllegalArgumentException("上级部门不能选择自身");
        }
        String deptName = optionalText(body, "deptName", existing.deptName());
        long now = System.currentTimeMillis();
        Dept updated = new Dept(existing.deptId(), parentId, deptName,
                (int) longValue(body.get("orderNum"), existing.orderNum()),
                optionalTextOrNull(body, "leader"), optionalTextOrNull(body, "phone"),
                (int) longValue(body.get("status"), existing.status()),
                existing.createdAt(), now);
        store.updateDept(updated);
        return Map.of("id", id, "ok", true);
    }

    /** 删除部门：存在子部门或部门下仍有用户时拒绝。 */
    @DeleteMapping("/{id}")
    @PreAuthorize("hasAuthority('system:dept:remove')")
    public Map<String, Object> delete(@PathVariable long id) {
        requireDept(id);
        boolean hasChild = store.listDepts().stream()
                .anyMatch(dept -> dept.parentId() == id);
        if (hasChild) {
            throw new IllegalArgumentException("存在子部门，不允许删除");
        }
        boolean hasUser = store.listUsers(null, null).stream()
                .anyMatch(user -> user.deptId() == id);
        if (hasUser) {
            throw new IllegalArgumentException("部门下仍有用户，不允许删除");
        }
        store.deleteDept(id);
        return Map.of("ok", true);
    }

    // ------------------------------------------------------------------
    // 映射与工具
    // ------------------------------------------------------------------

    private Dept requireDept(long id) {
        return store.findDeptById(id)
                .orElseThrow(() -> new IllegalArgumentException("部门不存在：" + id));
    }

    private static Map<String, Object> toMap(Dept dept) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("deptId", dept.deptId());
        map.put("parentId", dept.parentId());
        map.put("deptName", dept.deptName());
        map.put("orderNum", dept.orderNum());
        map.put("leader", dept.leader());
        map.put("phone", dept.phone());
        map.put("status", dept.status());
        return map;
    }

    private static String requireText(Map<String, Object> body, String field) {
        String value = optionalTextOrNull(body, field);
        if (value == null) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
        return value;
    }

    private static String optionalText(Map<String, Object> body, String field, String fallback) {
        String value = optionalTextOrNull(body, field);
        return value == null ? fallback : value;
    }

    private static String optionalTextOrNull(Map<String, Object> body, String field) {
        Object value = body == null ? null : body.get(field);
        if (!(value instanceof String s) || s.isBlank()) {
            return null;
        }
        return s.trim();
    }

    private static long longValue(Object value, long fallback) {
        return value instanceof Number n ? n.longValue() : fallback;
    }
}
