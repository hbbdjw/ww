package com.ww.common.query;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serial;
import java.io.Serializable;

/**
 * 查询排序
 *
 * @author jiawei
 */
public class Order implements Serializable {

    @Serial
    private static final long serialVersionUID = -3078342809727773232L;
    /**
     * 默认方向
     */
    private static final Direction DEFAULT_DIRECTION = Direction.desc;
    /**
     * 属性
     */
    private String property;
    /**
     * 方向
     */
    private Direction direction = DEFAULT_DIRECTION;

    /**
     * 构造方法
     */
    public Order() {
    }

    /**
     * 构造方法
     *
     * @param property  属性
     * @param direction 方向
     */
    public Order(String property, Direction direction) {
        this.property = property;
        this.direction = direction;
    }

    /**
     * 返回递增排序
     *
     * @param property 属性
     * @return 递增排序
     */
    public static Order asc(String property) {
        return new Order(property, Direction.asc);
    }

    /**
     * 返回递减排序
     *
     * @param property 属性
     * @return 递减排序
     */
    public static Order desc(String property) {
        return new Order(property, Direction.desc);
    }

    /**
     * 返回递增排序
     *
     * @param property 属性
     * @return 递增排序
     */
    public static Order str_asc(String property) {
        return new Order(property, Direction.str_asc);
    }

    /**
     * 返回递减排序
     *
     * @param property 属性
     * @return 递减排序
     */
    public static Order str_desc(String property) {
        return new Order(property, Direction.str_desc);
    }

    /**
     * 获取属性
     *
     * @return 属性
     */
    public String getProperty() {
        return property;
    }

    /**
     * 设置属性
     *
     * @param property 属性
     */
    public void setProperty(String property) {
        this.property = property;
    }

    /**
     * 获取方向
     *
     * @return 方向
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * 设置方向
     *
     * @param direction 方向
     */
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    /**
     * 重写equals方法
     *
     * @param obj 对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    /**
     * 重写hashCode方法
     *
     * @return HashCode
     */
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    /**
     * 方向
     */
    public enum Direction {

        /**
         * 递增
         */
        asc,

        /**
         * 递减
         */
        desc,

        /**
         * 字符串字段递增
         */
        str_asc,

        /**
         * 字符串字段递减
         */
        str_desc,
    }


}