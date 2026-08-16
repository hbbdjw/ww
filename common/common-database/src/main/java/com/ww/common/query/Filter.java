package com.ww.common.query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 查询筛选
 *
 * @author jiawei
 */
public class Filter implements Serializable {

    @Serial
    private static final long serialVersionUID = -8712382358441065075L;
    /**
     * 默认是否忽略大小写
     */
    private static final boolean DEFAULT_IGNORE_CASE = false;
    /**
     * 属性
     */
    private String property;
    /**
     * 运算符
     */
    private Operator operator;
    /**
     * 值
     */
    private Object value;

    /**
     * 是否忽略大小写
     */
    private Boolean ignoreCase = DEFAULT_IGNORE_CASE;

    /**
     * 子条件
     */
    private List<Filter> filters;

    /**
     * 或者
     *
     * @param filters 值
     * @return 或者筛选
     */
    public static Filter or(List<Filter> filters) {
        Filter filter = new Filter();
        filter.setOperator(Operator.or);
        filter.setFilters(filters);
        return filter;
    }

    /**
     * 或者
     *
     * @param filters 值
     * @return 或者筛选
     */
    public static Filter or(Filter... filters) {
        Filter filter = new Filter();
        filter.setOperator(Operator.or);
        filter.setFilters(Arrays.asList(filters));
        return filter;
    }

    /**
     * 返回等于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 等于筛选
     */
    public static Filter eq(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.eq);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回等于筛选
     *
     * @param property   属性
     * @param value      值
     * @param ignoreCase 忽略大小写
     * @return 等于筛选
     */
    public static Filter eq(String property, Object value, boolean ignoreCase) {
        Filter filter = new Filter();
        filter.setOperator(Operator.eq);
        filter.setProperty(property);
        filter.setIgnoreCase(ignoreCase);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回不等于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 不等于筛选
     */
    public static Filter ne(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.ne);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回不等于筛选
     *
     * @param property   属性
     * @param value      值
     * @param ignoreCase 忽略大小写
     * @return 不等于筛选
     */
    public static Filter ne(String property, Object value, boolean ignoreCase) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.ne);
        filter.setIgnoreCase(ignoreCase);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回大于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 大于筛选
     */
    public static Filter gt(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.gt);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回小于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 小于筛选
     */
    public static Filter lt(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.lt);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回大于等于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 大于等于筛选
     */
    public static Filter ge(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.ge);
        filter.setValue(value);
        return filter;
    }

    /**
     * lessThan
     *
     * @param property 属性
     * @param value    值
     * @return lessThan
     */
    public static Filter lessThan(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.lessThan);
        filter.setValue(value);
        return filter;
    }

    /**
     * lessThanOrEqualTo
     *
     * @param property 属性
     * @param value    值
     * @return lessThan
     */
    public static Filter lessThanOrEqualTo(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.lessThanOrEqualTo);
        filter.setValue(value);
        return filter;
    }

    /**
     * greaterThan
     *
     * @param property 属性
     * @param value    值
     * @return greaterThan
     */
    public static Filter greaterThan(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.greaterThan);
        filter.setValue(value);
        return filter;
    }

    /**
     * greaterThanOrEqualTo
     *
     * @param property 属性
     * @param value    值
     * @return lessThan
     */
    public static Filter greaterThanOrEqualTo(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.greaterThanOrEqualTo);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回小于等于筛选
     *
     * @param property 属性
     * @param value    值
     * @return 小于等于筛选
     */
    public static Filter le(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.le);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回相似筛选
     *
     * @param property 属性
     * @param value    值
     * @return 相似筛选
     */
    public static Filter like(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.like);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回包含筛选
     *
     * @param property 属性
     * @param value    值
     * @return 包含筛选
     */
    public static Filter in(String property, Object value) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.in);
        filter.setValue(value);
        return filter;
    }

    /**
     * 返回为Null筛选
     *
     * @param property 属性
     * @return 为Null筛选
     */
    public static Filter isNull(String property) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.isNull);
        return filter;
    }

    /**
     * 返回不为Null筛选
     *
     * @param property 属性
     * @return 不为Null筛选
     */
    public static Filter isNotNull(String property) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.isNotNull);
        return filter;
    }

    /**
     * 返回groupBy
     *
     * @param property 属性
     * @return groupBy
     */
    public static Filter groupBy(String property) {
        Filter filter = new Filter();
        filter.setProperty(property);
        filter.setOperator(Operator.groupBy);
        return filter;
    }

    /**
     * 返回忽略大小写筛选
     *
     * @return 忽略大小写筛选
     */
    public Filter ignoreCase() {
        this.ignoreCase = true;
        return this;
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
     * 获取运算符
     *
     * @return 运算符
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * 设置运算符
     *
     * @param operator 运算符
     */
    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    /**
     * 获取值
     *
     * @return 值
     */
    public Object getValue() {
        return value;
    }

    /**
     * 设置值
     *
     * @param value 值
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * 获取是否忽略大小写
     *
     * @return 是否忽略大小写
     */
    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    /**
     * 设置是否忽略大小写
     *
     * @param ignoreCase 是否忽略大小写
     */
    public void setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
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

}