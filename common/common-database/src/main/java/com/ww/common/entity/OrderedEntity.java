package com.ww.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serial;

/**
 * 排序基类
 *
 * @author jiawei
 */
@MappedSuperclass
public abstract class OrderedEntity extends BaseEntity implements Comparable<OrderedEntity> {

    /**
     * "排序"属性名称
     */
    public static final String ORDER_PROPERTY_NAME = "order";
    @Serial
    private static final long serialVersionUID = 5995013015967525827L;

    /**
     * 排序
     */
    @Column(name = "orders")
    private Integer order;

    /**
     * 获取排序
     *
     * @return 排序
     */
    public Integer getOrder() {
        return order;
    }

    /**
     * 设置排序
     *
     * @param order 排序
     */
    public void setOrder(Integer order) {
        this.order = order;
    }

    /**
     * 实现compareTo方法
     *
     * @param orderEntity 排序对象
     * @return 比较结果
     */
    @Override
    public int compareTo(OrderedEntity orderEntity) {
        if (orderEntity == null) {
            return 1;
        }
        return new CompareToBuilder().append(getOrder(), orderEntity.getOrder()).append(getId(), orderEntity.getId()).toComparison();
    }

}