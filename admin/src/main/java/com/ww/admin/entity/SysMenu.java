package com.ww.admin.entity;

import com.ww.common.entity.BaseEntity;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.Length;

/**
 * 菜单权限表。
 * <p>
 * menu_type 说明：M=目录，C=菜单，F=按钮/API。
 * <p>
 * perms 字段即 Shiro 权限标识（如 "sys:user:add"），替代旧的 ww_permission.code，
 * 该映射关系持久化在数据库（sys_menu + sys_role_menu），而非 ini 配置文件。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_menu")
@AttributeOverride(name = "id", column = @Column(name = "menu_id"))
public class SysMenu extends BaseEntity {

    /**
     * 菜单/按钮名称
     */
    @NotEmpty
    @Length(max = 50)
    @Column(name = "menu_name", nullable = false)
    private String menuName;

    /**
     * 父菜单ID（根菜单为 0）
     */
    @Column(name = "parent_id")
    private Long parentId;

    /**
     * 显示顺序
     */
    @Column(name = "order_num")
    private Integer orderNum;

    /**
     * 路由地址（仅目录/菜单有效）
     */
    @Column(name = "path", length = 200)
    private String path;

    /**
     * 组件路径（仅菜单有效）
     */
    @Column(name = "component", length = 255)
    private String component;

    /**
     * 类型：M目录 C菜单 F按钮/API
     */
    @NotEmpty
    @Column(name = "menu_type", nullable = false, length = 1)
    private String menuType;

    /**
     * 权限标识（Shiro 通配符格式，如 sys:user:add / sys:user:* / *:*）
     */
    @Column(name = "perms", length = 100)
    private String perms;

    /**
     * 状态（0禁用 1正常）
     */
    @Column(name = "status")
    private Integer status = 1;

}
