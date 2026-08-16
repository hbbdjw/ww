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
 * 部门表（树形结构，通过 parent_id + ancestors 实现多级查询）。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_dept")
@AttributeOverride(name = "id", column = @Column(name = "dept_id"))
public class SysDept extends BaseEntity {

    /**
     * 父部门ID（根部门为 0）
     */
    @Column(name = "parent_id")
    private Long parentId;

    /**
     * 祖先列表（如 "0,100,101"，便于树形多级查询）
     */
    @Column(name = "ancestors", length = 500)
    private String ancestors;

    /**
     * 部门名称
     */
    @NotEmpty
    @Length(max = 50)
    @Column(name = "dept_name", nullable = false)
    private String deptName;

    /**
     * 显示顺序
     */
    @Column(name = "order_num")
    private Integer orderNum;

    /**
     * 状态（0禁用 1正常）
     */
    @Column(name = "status")
    private Integer status = 1;

}
