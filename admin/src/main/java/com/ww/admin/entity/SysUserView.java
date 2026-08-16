package com.ww.admin.entity;

import com.blazebit.persistence.view.EntityView;
import com.blazebit.persistence.view.IdMapping;
import com.blazebit.persistence.view.Mapping;

import java.util.Date;

/**
 * EntityView — SysUser 的只读 DTO 投影（避免 N+1，编译期类型安全）。
 * <p>
 * 注意：SysUser 实体上部门关联字段名为 {@code dept}（SysDept 实体），不存在 {@code deptId} 属性，
 * 因此 {@link #getDeptId()} 需通过 {@code @Mapping("dept.id")} 显式导航至关联实体的主键。
 */
@EntityView(SysUser.class)
public interface SysUserView {

    @IdMapping
    Long getId();

    String getUsername();

    String getNickname();

    @Mapping("dept.id")
    Long getDeptId();

    Date getCreatedDate();
}
