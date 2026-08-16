package com.ww.admin.entity;

import com.ww.common.entity.BaseEntity;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.Length;

import java.util.HashSet;
import java.util.Set;

/**
 * 用户表。
 * <p>
 * password 列存储 Shiro PasswordService 生成的哈希（SHA-256 + 随机盐，盐内嵌于哈希串），
 * {@link #plainPassword} 为请求体接收的明文密码（@Transient 不持久化），由 Service 层哈希后写入 password。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_user")
@AttributeOverride(name = "id", column = @Column(name = "user_id"))
public class SysUser extends BaseEntity {

    /**
     * 归属部门
     */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "dept_id")
    private SysDept dept;

    /**
     * 用户账号
     */
    @NotEmpty
    @Length(min = 4, max = 20)
    @Pattern.List({@Pattern(regexp = "^[0-9a-zA-Z_\\u4e00-\\u9fa5]+$"), @Pattern(regexp = "^.*[^\\d].*$")})
    @Column(name = "username", nullable = false, updatable = false, unique = true)
    private String username;

    /**
     * 用户昵称
     */
    @NotEmpty
    @Length(max = 30)
    @Column(name = "nickname", nullable = false)
    private String nickname;

    /**
     * 密码（存储 Shiro 哈希，非明文）
     */
    @Column(name = "password", nullable = false)
    private String password;

    /**
     * 状态（0禁用 1正常）
     */
    @Column(name = "status")
    private Integer status = 1;

    /**
     * 前端偏好设置（JSON 字符串）
     * <p>
     * 存储 vben-admin preferences 的用户自定义部分（主题、布局、侧边栏等），
     * 登录后由前端加载，修改后通过 PUT /auth/preferences 持久化。
     * null 表示用户未自定义，前端使用默认值。
     * <p>
     * 以纯 String 存储（text 列），不做 Hibernate JSON 类型映射：
     * 前端负责 JSON.stringify/parse，后端只做透传，避免引入 Jackson/JSONB 依赖。
     */
    @Column(name = "preferences", columnDefinition = "text")
    private String preferences;

    /**
     * 角色集合（多对多，通过 sys_user_role 关联）
     */
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
            name = "sys_user_role",
            joinColumns = @JoinColumn(name = "user_id"),
            inverseJoinColumns = @JoinColumn(name = "role_id")
    )
    private Set<SysRole> roles = new HashSet<>();

    /**
     * 临时字段-明文密码（不持久化，仅用于接收请求体中的密码输入）
     */
    @Transient
    private String plainPassword;

}
