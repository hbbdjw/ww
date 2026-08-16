package com.ww.common.entity;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonView;
import com.ww.common.id.SnowflakeId;
import jakarta.persistence.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

/**
 * Entity - 基类
 */
@EntityListeners(AuditingEntityListener.class)
@MappedSuperclass
public abstract class BaseEntity implements Serializable {

    @Serial
    private static final long serialVersionUID = -67188388306700736L;

    /**
     * "ID"属性名称
     */
    public static final String ID_PROPERTY_NAME = "id";
    /**
     * "创建日期"属性名称
     */
    public static final String CREATED_DATE_PROPERTY_NAME = "createdDate";
    /**
     * "最后修改日期"属性名称
     */
    public static final String LAST_MODIFIED_DATE_PROPERTY_NAME = "lastModifiedDate";
    /**
     * "版本"属性名称
     */
    public static final String VERSION_PROPERTY_NAME = "version";

    /**
     * ID (Snowflake)
     */
    @JsonView(BaseView.class)
    @Id
    @SnowflakeId
    private Long id;
    /**
     * 创建日期
     */
    @JsonView(BaseView.class)
    @CreatedDate
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    @Column(nullable = false, updatable = false, name = "create_time")
    private Date createdDate;

    /**
     * 最后修改日期
     */
    @LastModifiedDate
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    @Column(nullable = false, name = "update_time")
    private Date lastModifiedDate;

    /**
     * 版本
     */
    @Version
    @Column(nullable = false, name = "version")
    private Long version;

    /**
     * 获取ID
     *
     * @return ID
     */
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /**
     * 获取创建日期
     *
     * @return 创建日期
     */
    public Date getCreatedDate() {
        return createdDate;
    }

    /**
     * 设置创建日期
     *
     * @param createdDate 创建日期
     */
    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    /**
     * 获取最后修改日期
     *
     * @return 最后修改日期
     */
    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    /**
     * 设置最后修改日期
     *
     * @param lastModifiedDate 最后修改日期
     */
    public void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    /**
     * 获取版本
     *
     * @return 版本
     */
    public Long getVersion() {
        return version;
    }

    /**
     * 设置版本
     *
     * @param version 版本
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * 判断是否为新建对象
     *
     * @return 是否为新建对象
     */
    @Transient
    public boolean isNew() {
        return getId() == null;
    }

    /**
     * 重写toString方法
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return String.format("Entity of type %s with id_generator: %s", getClass().getName(), getId());
    }

    /**
     * 重写equals方法
     *
     * @param obj 对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (!BaseEntity.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        BaseEntity other = (BaseEntity) obj;
        return getId() != null && getId().equals(other.getId());
    }

    /**
     * 重写hashCode方法
     *
     * @return HashCode
     */
    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode += getId() != null ? getId().hashCode() * 31 : 0;
        return hashCode;
    }

    /**
     * 基础视图
     */
    public interface BaseView {

    }

}