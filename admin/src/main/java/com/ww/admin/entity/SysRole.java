package com.ww.admin.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.ww.common.entity.BaseEntity;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.Length;

import java.util.HashSet;
import java.util.Set;

/**
 * 角色表。
 * <p>
 * role_key 为 Shiro 角色标识（如 admin, manager），data_scope 控制数据范围（行级授权）：
 * <ul>
 *   <li>1: 全部数据</li>
 *   <li>2: 自定义数据（通过 sys_role_dept 关联）</li>
 *   <li>3: 本部门数据</li>
 *   <li>4: 本部门及以下数据</li>
 *   <li>5: 仅本人数据</li>
 * </ul>
 * 角色-菜单/按钮映射持久化在 sys_role_menu，角色-部门映射（自定义数据范围）持久化在 sys_role_dept。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_role")
@AttributeOverride(name = "id", column = @Column(name = "role_id"))
public class SysRole extends BaseEntity {

    /**
     * 角色名称
     */
    @NotEmpty
    @Length(max = 30)
    @Column(name = "role_name", nullable = false)
    private String roleName;

    /**
     * 角色权限字符串（Shiro 角色标识，如 admin, manager）
     */
    @NotEmpty
    @Length(max = 100)
    @Column(name = "role_key", nullable = false)
    private String roleKey;

    /**
     * 数据范围（1全部 2自定义 3本部门 4本部门及以下 5仅本人）
     */
    @Column(name = "data_scope", length = 1)
    private String dataScope = "1";

    /**
     * 状态（0禁用 1正常）
     */
    @Column(name = "status")
    private Integer status = 1;

    /**
     * 菜单/按钮集合（多对多，通过 sys_role_menu 关联）
     * 使用 @JsonIgnore 避免序列化时传输大量数据，通过 /role/{id}/menuIds 单独获取
     */
    @JsonIgnore
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
            name = "sys_role_menu",
            joinColumns = @JoinColumn(name = "role_id"),
            inverseJoinColumns = @JoinColumn(name = "menu_id")
    )
    private Set<SysMenu> menus = new HashSet<>();

    /**
     * 自定义数据范围关联的部门（多对多，通过 sys_role_dept 关联，仅 data_scope=2 时使用）
     * 使用 @JsonIgnore 避免懒加载集合在 Session 外被 Jackson 序列化导致 LazyInitializationException
     */
    @JsonIgnore
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "sys_role_dept",
            joinColumns = @JoinColumn(name = "role_id"),
            inverseJoinColumns = @JoinColumn(name = "dept_id")
    )
    private Set<SysDept> depts = new HashSet<>();

}
