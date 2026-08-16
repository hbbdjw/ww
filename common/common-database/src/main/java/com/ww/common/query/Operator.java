package com.ww.common.query;

/**
 * 运算符
 */
public enum Operator {
    /**
     * 或者
     */
    or,

    /**
     * 等于
     */
    eq,

    /**
     * 不等于
     */
    ne,

    /**
     * 大于
     */
    gt,

    /**
     * 小于
     */
    lt,

    /**
     * 大于等于
     */
    ge,

    /**
     * 小于等于
     */
    le,

    /**
     * 相似
     */
    like,

    /**
     * 包含
     */
    in,

    /**
     * 为Null
     */
    isNull,

    /**
     * 不为Null
     */
    isNotNull,
    /**
     * lessThan
     */
    lessThan,
    /**
     * lessThan
     */
    greaterThan,
    /**
     * lessThanOrEqualTo
     */
    lessThanOrEqualTo,
    /**
     * greaterThanOrEqualTo
     */
    greaterThanOrEqualTo,

    groupBy
}