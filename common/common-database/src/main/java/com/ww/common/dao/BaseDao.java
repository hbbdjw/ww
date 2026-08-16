package com.ww.common.dao;


import com.ww.common.query.Filter;
import com.ww.common.query.Order;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import com.ww.common.entity.BaseEntity;
import jakarta.persistence.LockModeType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Dao - 基类
 *
 * @author jiawei
 */
public interface BaseDao {

    // ==================== exists ====================

    /**
     * 判断是否存在
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @return 是否存在
     */
    <E extends BaseEntity> boolean exists(Class<E> entityClass, String attributeName, Object attributeValue);

    /**
     * 判断是否存在
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @param ignoreCase     忽略大小写
     * @return 是否存在
     */
    <E extends BaseEntity> boolean exists(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase);

    // ==================== unique ====================

    /**
     * 判断是否唯一
     *
     * @param entityClass    实体类
     * @param id             ID
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @return 是否唯一
     */
    <E extends BaseEntity> boolean unique(Class<E> entityClass, Serializable id, String attributeName, Object attributeValue);

    /**
     * 判断是否唯一
     *
     * @param entityClass    实体类
     * @param id             ID
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @param ignoreCase     忽略大小写
     * @return 是否唯一
     */
    <E extends BaseEntity> boolean unique(Class<E> entityClass, Serializable id, String attributeName, String attributeValue, boolean ignoreCase);

    // ==================== find ====================

    /**
     * 查找实体对象
     *
     * @param entityClass 实体类
     * @param id          ID
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, Serializable id);

    /**
     * 查找实体对象
     *
     * @param entityClass  实体类
     * @param id           ID
     * @param lockModeType 锁定方式
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, Serializable id, LockModeType lockModeType);

    /**
     * 查找实体对象
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, Object attributeValue);

    /**
     * 查找实体对象
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @param ignoreCase     忽略大小写
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase);

    /**
     * 查找实体对象
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @param lockModeType   锁定方式
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, Object attributeValue, LockModeType lockModeType);

    /**
     * 查找实体对象
     *
     * @param entityClass    实体类
     * @param attributeName  属性名称
     * @param attributeValue 属性值
     * @param ignoreCase     忽略大小写
     * @param lockModeType   锁定方式
     * @return 实体对象，若不存在则返回null
     */
    <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase, LockModeType lockModeType);

    // ==================== findList ====================

    /**
     * 查找实体对象集合
     *
     * @param entityClass 实体类
     * @param first       起始记录
     * @param count       数量
     * @param filters     筛选
     * @param orders      排序
     * @return 实体对象集合
     */
    <E extends BaseEntity> List<E> findList(Class<E> entityClass, Integer first, Integer count, List<Filter> filters, List<Order> orders);

    // ==================== findPage ====================

    /**
     * 查找实体对象分页
     *
     * @param entityClass 实体类
     * @param pageable    分页信息
     * @return 实体对象分页
     */
    <E extends BaseEntity> Page<E> findPage(Class<E> entityClass, Pageable pageable);

    // ==================== count ====================

    /**
     * 查询实体对象数量
     *
     * @param entityClass 实体类
     * @param filters     筛选
     * @return 实体对象数量
     */
    <E extends BaseEntity> long count(Class<E> entityClass, Filter... filters);

    /**
     * 查询实体对象数量
     *
     * @param entityClass 实体类
     * @param filters     筛选
     * @return 实体对象数量
     */
    <E extends BaseEntity> long count(Class<E> entityClass, List<Filter> filters);

    // ==================== sum ====================

    /**
     * 字段求和
     *
     * @param entityClass 实体类
     * @param filters     筛选
     * @param sum         要求和的字段
     * @return 求和结果
     */
    <E extends BaseEntity> BigDecimal sum(Class<E> entityClass, List<Filter> filters, String sum);

    // ==================== persist / merge / remove ====================

    /**
     * 持久化实体对象
     *
     * @param entity 实体对象
     */
    void persist(BaseEntity entity);

    /**
     * 合并实体对象
     *
     * @param entity 实体对象
     * @return 实体对象
     */
    <E extends BaseEntity> E merge(E entity);

    /**
     * 移除实体对象
     *
     * @param entity 实体对象
     */
    void remove(BaseEntity entity);

    /**
     * 批量持久化
     *
     * @param entities  实体对象集合
     * @param batchSize 每批数量
     */
    <E extends BaseEntity> void batchPersist(List<E> entities, int batchSize);

    // ==================== 原生 SQL 查询 ====================

    /**
     * 原生 SQL 查询实体列表
     *
     * @param sql         原生 SQL（使用命名参数 :name）
     * @param resultClass 实体类
     * @param params      命名参数
     * @return 实体列表
     */
    <E extends BaseEntity> List<E> findListBySQL(String sql, Class<E> resultClass, Map<String, Object> params);

    /**
     * 原生 SQL 查询实体列表（无参数）
     *
     * @param sql         原生 SQL
     * @param resultClass 实体类
     * @return 实体列表
     */
    <E extends BaseEntity> List<E> findListBySQL(String sql, Class<E> resultClass);

    /**
     * 原生 SQL 查询单个实体
     *
     * @param sql         原生 SQL（使用命名参数 :name）
     * @param resultClass 实体类
     * @param params      命名参数
     * @return 实体对象，不存在返回 null
     */
    <E extends BaseEntity> E findBySQL(String sql, Class<E> resultClass, Map<String, Object> params);

    /**
     * 原生 SQL 分页查询实体
     *
     * @param sql         原生 SQL（使用命名参数 :name）
     * @param resultClass 实体类
     * @param params      命名参数
     * @param pageable    分页信息
     * @return 分页结果
     */
    <E extends BaseEntity> Page<E> findPageBySQL(String sql, Class<E> resultClass, Map<String, Object> params, Pageable pageable);

    /**
     * 原生 SQL 查询标量列表（返回 Object[]）
     *
     * @param sql    原生 SQL（使用命名参数 :name）
     * @param params 命名参数
     * @return 标量结果列表
     */
    List<Object[]> findScalarListBySQL(String sql, Map<String, Object> params);

    /**
     * 原生 SQL 计数查询
     *
     * @param sql    原生 SQL（使用命名参数 :name），应返回单行单列数值
     * @param params 命名参数
     * @return 计数
     */
    long countBySQL(String sql, Map<String, Object> params);

    /**
     * 原生 SQL 更新/删除（执行 INSERT/UPDATE/DELETE）
     *
     * @param sql    原生 SQL（使用命名参数 :name）
     * @param params 命名参数
     * @return 受影响行数
     */
    int updateBySQL(String sql, Map<String, Object> params);

    // ==================== refresh ====================

    /**
     * 刷新实体对象
     *
     * @param entity 实体对象
     */
    void refresh(BaseEntity entity);

    /**
     * 刷新实体对象
     *
     * @param entity       实体对象
     * @param lockModeType 锁定方式
     */
    void refresh(BaseEntity entity, LockModeType lockModeType);

    // ==================== 实体状态 ====================

    /**
     * 获取实体对象ID
     *
     * @param entity 实体对象
     * @return 实体对象ID
     */
    Serializable getIdentifier(BaseEntity entity);

    /**
     * 判断实体对象是否已加载
     *
     * @param entity 实体对象
     * @return 实体对象是否已加载
     */
    boolean isLoaded(BaseEntity entity);

    /**
     * 判断实体对象属性是否已加载
     *
     * @param entity        实体对象
     * @param attributeName 属性名称
     * @return 实体对象属性是否已加载
     */
    boolean isLoaded(BaseEntity entity, String attributeName);

    /**
     * 判断是否为托管状态
     *
     * @param entity 实体对象
     * @return 是否为托管状态
     */
    boolean isManaged(BaseEntity entity);

    /**
     * 设置为游离状态
     *
     * @param entity 实体对象
     */
    void detach(BaseEntity entity);

    /**
     * 获取锁定方式
     *
     * @param entity 实体对象
     * @return 锁定方式
     */
    LockModeType getLockMode(BaseEntity entity);

    /**
     * 锁定实体对象
     *
     * @param entity       实体对象
     * @param lockModeType 锁定方式
     */
    void lock(BaseEntity entity, LockModeType lockModeType);

    // ==================== EntityManager ====================

    /**
     * 清除缓存
     */
    void clear();

    /**
     * 同步数据
     */
    void flush();

}
