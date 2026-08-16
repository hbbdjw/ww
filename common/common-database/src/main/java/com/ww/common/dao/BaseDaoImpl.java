package com.ww.common.dao;

import com.ww.common.query.Filter;
import com.ww.common.query.Operator;
import com.ww.common.query.Order;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import com.ww.common.entity.BaseEntity;
import com.ww.common.entity.OrderedEntity;
import com.blazebit.persistence.CriteriaBuilderFactory;
import com.blazebit.persistence.FullQueryBuilder;
import com.blazebit.persistence.PagedList;
import com.blazebit.persistence.view.EntityViewManager;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.*;
import jakarta.persistence.criteria.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * Dao - 基类实现
 * <p>同时提供三套查询能力：
 * <ul>
 *     <li>基于 JPA Criteria API + {@link com.ww.common.query.Filter}/{@link com.ww.common.query.Pageable} 的动态查询（原有，向后兼容）</li>
 *     <li>基于 QueryDSL 的类型安全查询入口（{@link #queryFactory()} / {@link #findPage(JPAQuery, Pageable)}）</li>
 *     <li>基于 Blaze-Persistence 的高级查询入口（{@link #cbf()} / {@link #evm()} / {@link #findPage(FullQueryBuilder, Pageable)}）— CTE / 窗口函数 / EntityView DTO 投影</li>
 * </ul>
 * Service 层可按需选用，三者并存互不干扰。
 */
public class BaseDaoImpl implements BaseDao {

    /**
     * 属性分隔符
     */
    private static final String ATTRIBUTE_SEPARATOR = ".";

    /**
     * 别名前缀
     */
    private static final String ALIAS_PREFIX = "mmGeneratedAlias";

    /**
     * 别名数
     */
    private static volatile long aliasCount = 0L;

    @PersistenceContext
    protected EntityManager entityManager;

    /**
     * QueryDSL 查询工厂（类型安全动态查询入口）
     * 与现有 Criteria API / Filter 查询并存，供 Service 层按需选用。
     */
    protected JPAQueryFactory queryFactory;

    @PostConstruct
    protected void initQueryFactory() {
        this.queryFactory = new JPAQueryFactory(entityManager);
    }

    /**
     * Blaze-Persistence CriteriaBuilderFactory（高级查询入口：CTE / 窗口函数 / keyset 分页）
     */
    @Autowired
    protected CriteriaBuilderFactory criteriaBuilderFactory;

    /**
     * Blaze-Persistence EntityViewManager（EntityView DTO 投影 / 可写视图）
     */
    @Autowired
    protected EntityViewManager entityViewManager;

    // ==================== exists ====================

    @Override
    public <E extends BaseEntity> boolean exists(Class<E> entityClass, String attributeName, Object attributeValue) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        Root<E> root = cq.from(entityClass);
        cq.select(cb.count(root));
        cq.where(cb.equal(root.get(attributeName), attributeValue));
        return entityManager.createQuery(cq).getSingleResult() > 0;
    }

    @Override
    public <E extends BaseEntity> boolean exists(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        Root<E> root = cq.from(entityClass);
        cq.select(cb.count(root));
        if (ignoreCase) {
            cq.where(cb.equal(cb.lower(root.<String>get(attributeName)), StringUtils.lowerCase(attributeValue)));
        } else {
            cq.where(cb.equal(root.get(attributeName), attributeValue));
        }
        return entityManager.createQuery(cq).getSingleResult() > 0;
    }

    // ==================== unique ====================

    @Override
    public <E extends BaseEntity> boolean unique(Class<E> entityClass, Serializable id, String attributeName, Object attributeValue) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        if (id != null) {
            CriteriaBuilder cb = entityManager.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<E> root = cq.from(entityClass);
            cq.select(cb.count(root));
            cq.where(cb.and(cb.equal(root.get(attributeName), attributeValue), cb.notEqual(root.get(BaseEntity.ID_PROPERTY_NAME), id)));
            return entityManager.createQuery(cq).getSingleResult() <= 0;
        } else {
            return !exists(entityClass, attributeName, attributeValue);
        }
    }

    @Override
    public <E extends BaseEntity> boolean unique(Class<E> entityClass, Serializable id, String attributeName, String attributeValue, boolean ignoreCase) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        if (id != null) {
            CriteriaBuilder cb = entityManager.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<E> root = cq.from(entityClass);
            cq.select(cb.count(root));
            if (ignoreCase) {
                cq.where(cb.and(cb.equal(cb.lower(root.<String>get(attributeName)), StringUtils.lowerCase(attributeValue)), cb.notEqual(root.get(BaseEntity.ID_PROPERTY_NAME), id)));
            } else {
                cq.where(cb.and(cb.equal(root.get(attributeName), attributeValue), cb.notEqual(root.get(BaseEntity.ID_PROPERTY_NAME), id)));
            }
            return entityManager.createQuery(cq).getSingleResult() <= 0;
        } else {
            return !exists(entityClass, attributeName, attributeValue);
        }
    }

    // ==================== find ====================

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, Serializable id) {
        if (id == null) {
            return null;
        }
        return entityManager.find(entityClass, id);
    }

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, Serializable id, LockModeType lockModeType) {
        if (id == null) {
            return null;
        }
        if (lockModeType != null) {
            return entityManager.find(entityClass, id, lockModeType);
        } else {
            return entityManager.find(entityClass, id);
        }
    }

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, Object attributeValue) {
        return find(entityClass, attributeName, attributeValue, (LockModeType) null);
    }

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase) {
        return find(entityClass, attributeName, attributeValue, ignoreCase, null);
    }

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, Object attributeValue, LockModeType lockModeType) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        Root<E> root = cq.from(entityClass);
        cq.select(root);
        cq.where(cb.equal(root.get(attributeName), attributeValue));
        TypedQuery<E> query = entityManager.createQuery(cq);
        if (lockModeType != null) {
            query.setLockMode(lockModeType);
        }
        try {
            return query.getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    @Override
    public <E extends BaseEntity> E find(Class<E> entityClass, String attributeName, String attributeValue, boolean ignoreCase, LockModeType lockModeType) {
        Assert.hasText(attributeName, "attributeName must not be empty");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        Root<E> root = cq.from(entityClass);
        cq.select(root);
        if (ignoreCase) {
            cq.where(cb.equal(cb.lower(root.<String>get(attributeName)), StringUtils.lowerCase(attributeValue)));
        } else {
            cq.where(cb.equal(root.get(attributeName), attributeValue));
        }
        TypedQuery<E> query = entityManager.createQuery(cq);
        if (lockModeType != null) {
            query.setLockMode(lockModeType);
        }
        try {
            return query.getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    // ==================== findList ====================

    @Override
    public <E extends BaseEntity> List<E> findList(Class<E> entityClass, Integer first, Integer count, List<Filter> filters, List<Order> orders) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        cq.select(cq.from(entityClass));
        return findList(cq, first, count, filters, orders);
    }

    // ==================== findPage ====================

    @Override
    public <E extends BaseEntity> Page<E> findPage(Class<E> entityClass, Pageable pageable) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        cq.select(cq.from(entityClass));
        return findPage(entityClass, cq, pageable);
    }

    // ==================== count ====================

    @Override
    public <E extends BaseEntity> long count(Class<E> entityClass, Filter... filters) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        cq.select(cq.from(entityClass));
        return count(entityClass, cq, filters != null ? Arrays.asList(filters) : null);
    }

    @Override
    public <E extends BaseEntity> long count(Class<E> entityClass, List<Filter> filters) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        cq.select(cq.from(entityClass));
        return count(entityClass, cq, filters);
    }

    // ==================== sum ====================

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends BaseEntity> BigDecimal sum(Class<E> entityClass, List<Filter> filters, String sum) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<E> cq = cb.createQuery(entityClass);
        Root<E> root = cq.from(entityClass);

        Predicate restrictions = cq.getRestriction() != null ? cq.getRestriction() : cb.conjunction();
        if (CollectionUtils.isNotEmpty(filters)) {
            restrictions = cb.and(restrictions, toPredicate(root, filters));
        }
        cq.where(restrictions);

        CriteriaQuery<BigDecimal> sumCq = cb.createQuery(BigDecimal.class);
        copyCriteriaWithoutSelectionAndOrder(cq, sumCq);
        Path objectPath = root.get(sum);
        sumCq.select(cb.sum(objectPath));
        List<BigDecimal> resultList = entityManager.createQuery(sumCq).setMaxResults(1).getResultList();
        return CollectionUtils.isNotEmpty(resultList) ? resultList.getFirst() : null;
    }

    // ==================== persist / merge / remove ====================

    @Override
    public void persist(BaseEntity entity) {
        Assert.notNull(entity, "entity must not be null");
        entityManager.persist(entity);
    }

    @Override
    public <E extends BaseEntity> E merge(E entity) {
        Assert.notNull(entity, "entity must not be null");
        return (E) entityManager.merge(entity);
    }

    @Override
    public void remove(BaseEntity entity) {
        if (entity != null) {
            entityManager.remove(entity);
        }
    }

    @Override
    public <E extends BaseEntity> void batchPersist(List<E> entities, int batchSize) {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }
        if (batchSize <= 0) {
            batchSize = 50;
        }
        int size = entities.size();
        for (int i = 0; i < size; i++) {
            entityManager.persist(entities.get(i));
            if ((i + 1) % batchSize == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
    }

    // ==================== refresh ====================

    @Override
    public void refresh(BaseEntity entity) {
        if (entity != null) {
            entityManager.refresh(entity);
        }
    }

    @Override
    public void refresh(BaseEntity entity, LockModeType lockModeType) {
        if (entity != null) {
            if (lockModeType != null) {
                entityManager.refresh(entity, lockModeType);
            } else {
                entityManager.refresh(entity);
            }
        }
    }

    // ==================== 实体状态 ====================

    @Override
    public Serializable getIdentifier(BaseEntity entity) {
        Assert.notNull(entity, "entity must not be null");
        return (Serializable) entityManager.getEntityManagerFactory().getPersistenceUnitUtil().getIdentifier(entity);
    }

    @Override
    public boolean isLoaded(BaseEntity entity) {
        Assert.notNull(entity, "entity must not be null");
        return entityManager.getEntityManagerFactory().getPersistenceUnitUtil().isLoaded(entity);
    }

    @Override
    public boolean isLoaded(BaseEntity entity, String attributeName) {
        Assert.notNull(entity, "entity must not be null");
        Assert.hasText(attributeName, "attributeName must not be empty");
        return entityManager.getEntityManagerFactory().getPersistenceUnitUtil().isLoaded(entity, attributeName);
    }

    @Override
    public boolean isManaged(BaseEntity entity) {
        Assert.notNull(entity, "entity must not be null");
        return entityManager.contains(entity);
    }

    @Override
    public void detach(BaseEntity entity) {
        if (entity != null) {
            entityManager.detach(entity);
        }
    }

    @Override
    public LockModeType getLockMode(BaseEntity entity) {
        Assert.notNull(entity, "entity must not be null");
        return entityManager.getLockMode(entity);
    }

    @Override
    public void lock(BaseEntity entity, LockModeType lockModeType) {
        if (entity != null && lockModeType != null) {
            entityManager.lock(entity, lockModeType);
        }
    }

    // ==================== EntityManager ====================

    @Override
    public void clear() {
        entityManager.clear();
    }

    @Override
    public void flush() {
        entityManager.flush();
    }

    // ==================== protected helper methods ====================

    /**
     * 查找实体对象集合
     *
     * @param criteriaQuery  查询条件
     * @param first          起始记录
     * @param count          数量
     * @param filters        筛选
     * @param orders         排序
     * @param lockModeType   锁定方式
     * @return 实体对象集合
     */
    protected <T extends BaseEntity> List<T> findList(CriteriaQuery<T> criteriaQuery, Integer first, Integer count, List<Filter> filters, List<Order> orders, LockModeType lockModeType) {
        Assert.notNull(criteriaQuery, "criteriaQuery must not be null");
        Assert.notNull(criteriaQuery.getSelection(), "criteriaQuery.getSelection() must not be null");
        Assert.notEmpty(criteriaQuery.getRoots(), "criteriaQuery.getRoots() must not be empty");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        Root<T> root = findRoot(criteriaQuery);

        Predicate restrictions = criteriaQuery.getRestriction() != null ? criteriaQuery.getRestriction() : cb.conjunction();
        boolean hasGroupBy = false;
        if (CollectionUtils.isNotEmpty(filters)) {
            restrictions = cb.and(restrictions, toPredicate(root, filters));
            for (Filter filter : filters) {
                if (!hasGroupBy) {
                    if (Operator.groupBy.equals(filter.getOperator())) {
                        criteriaQuery.groupBy(root.get(filter.getProperty().toString()));
                        hasGroupBy = true;
                    }
                }
            }
        }
        criteriaQuery.where(restrictions);
        List<jakarta.persistence.criteria.Order> orderList = new ArrayList<>();
        orderList.addAll(criteriaQuery.getOrderList());
        orderList.addAll(toOrders(root, orders));
        if (CollectionUtils.isEmpty(orderList)) {
            Class<?> entityClass = root.getJavaType();
            if (OrderedEntity.class.isAssignableFrom(entityClass)) {
                orderList.add(cb.asc(getPath(root, OrderedEntity.ORDER_PROPERTY_NAME)));
            } else {
                orderList.add(cb.desc(getPath(root, BaseEntity.LAST_MODIFIED_DATE_PROPERTY_NAME)));
            }
        }
        criteriaQuery.orderBy(orderList);

        TypedQuery<T> query = entityManager.createQuery(criteriaQuery);
        if (first != null) {
            query.setFirstResult(first);
        }
        if (count != null) {
            query.setMaxResults(count);
        }
        if (lockModeType != null) {
            query.setLockMode(lockModeType);
        }
        return query.getResultList();
    }

    /**
     * 查找实体对象集合
     *
     * @param criteriaQuery 查询条件
     * @param first         起始记录
     * @param count         数量
     * @param filters       筛选
     * @param orders        排序
     * @return 实体对象集合
     */
    protected <T extends BaseEntity> List<T> findList(CriteriaQuery<T> criteriaQuery, Integer first, Integer count, List<Filter> filters, List<Order> orders) {
        return findList(criteriaQuery, first, count, filters, orders, null);
    }

    /**
     * 查找实体对象集合
     *
     * @param criteriaQuery 查询条件
     * @param first         起始记录
     * @param count         数量
     * @return 实体对象集合
     */
    protected <T extends BaseEntity> List<T> findList(CriteriaQuery<T> criteriaQuery, Integer first, Integer count) {
        return findList(criteriaQuery, first, count, null, null, null);
    }

    /**
     * 查找实体对象集合
     *
     * @param criteriaQuery 查询条件
     * @return 实体对象集合
     */
    protected <T extends BaseEntity> List<T> findList(CriteriaQuery<T> criteriaQuery) {
        return findList(criteriaQuery, null, null, null, null, null);
    }

    /**
     * 查找实体对象分页
     *
     * @param entityClass  实体类
     * @param criteriaQuery 查询条件
     * @param pageable      分页信息
     * @param lockModeType  锁定方式
     * @return 实体对象分页
     */
    protected <T extends BaseEntity> Page<T> findPage(Class<T> entityClass, CriteriaQuery<T> criteriaQuery, Pageable pageable, LockModeType lockModeType) {
        Assert.notNull(criteriaQuery, "criteriaQuery must not be null");
        Assert.notNull(criteriaQuery.getSelection(), "criteriaQuery.getSelection() must not be null");
        Assert.notEmpty(criteriaQuery.getRoots(), "criteriaQuery.getRoots() must not be empty");

        if (pageable == null) {
            pageable = new Pageable();
        }

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        Root<T> root = findRoot(criteriaQuery);

        Predicate restrictions = criteriaQuery.getRestriction() != null ? criteriaQuery.getRestriction() : cb.conjunction();
        List<Filter> filterList = pageable.getFilters();
        if (CollectionUtils.isNotEmpty(filterList)) {
            restrictions = cb.and(restrictions, toPredicate(root, filterList));
        }
        String searchProperty = pageable.getSearchProperty();
        String searchValue = pageable.getSearchValue();
        if (StringUtils.isNotEmpty(searchProperty) && StringUtils.isNotEmpty(searchValue)) {
            Path<String> searchPath = getPath(root, searchProperty);
            if (searchPath != null) {
                restrictions = cb.and(restrictions, cb.like(searchPath, "%" + searchValue + "%"));
            }
        }
        criteriaQuery.where(restrictions);

        // count before orderBy to avoid Hibernate SQM mutation interference
        long total = count(entityClass, criteriaQuery, null);

        List<jakarta.persistence.criteria.Order> orderList = new ArrayList<>();
        orderList.addAll(criteriaQuery.getOrderList());
        orderList.addAll(toOrders(root, pageable.getOrders()));
        String orderProperty = pageable.getOrderProperty();
        Order.Direction orderDirection = pageable.getOrderDirection();
        if (StringUtils.isNotEmpty(orderProperty) && orderDirection != null) {
            Path<?> orderPath = getPath(root, orderProperty);
            if (orderPath != null) {
                switch (orderDirection) {
                    case asc:
                        orderList.add(cb.asc(orderPath));
                        break;
                    case desc:
                        orderList.add(cb.desc(orderPath));
                        break;
                    case str_asc:
                        orderList.add(cb.asc(root.get(orderProperty).as(Integer.class)));
                        break;
                    case str_desc:
                        orderList.add(cb.desc(root.get(orderProperty).as(Integer.class)));
                        break;
                    default:
                        break;
                }
            }
        }
        if (CollectionUtils.isEmpty(orderList)) {
            if (OrderedEntity.class.isAssignableFrom(entityClass)) {
                orderList.add(cb.asc(getPath(root, OrderedEntity.ORDER_PROPERTY_NAME)));
            } else {
                orderList.add(cb.desc(getPath(root, BaseEntity.LAST_MODIFIED_DATE_PROPERTY_NAME)));
            }
        }
        TypedQuery<T> query = entityManager.createQuery(criteriaQuery.distinct(true).orderBy(orderList));
        query.setFirstResult((pageable.getPageNumber() - 1) * pageable.getPageSize());
        query.setMaxResults(pageable.getPageSize());
        if (lockModeType != null) {
            query.setLockMode(lockModeType);
        }
        Page<T> tPage = new Page<>(query.getResultList(), total, pageable);
        if (CollectionUtils.isNotEmpty(pageable.getSums())) {
            List<String> sums = pageable.getSums();
            Map<String, Object> sumMap = new HashMap<>();
            for (String s : sums) {
                if (StringUtils.isNotBlank(s)) {
                    BigDecimal sumVal = sum(entityClass, pageable.getFilters(), s);
                    if (sumVal != null) {
                        sumMap.put(s, sumVal.stripTrailingZeros().toPlainString());
                    }
                }
            }
            tPage.setTotalRow(sumMap);
        }
        return tPage;
    }

    /**
     * 查找实体对象分页
     *
     * @param entityClass   实体类
     * @param criteriaQuery 查询条件
     * @param pageable      分页信息
     * @return 实体对象分页
     */
    protected <T extends BaseEntity> Page<T> findPage(Class<T> entityClass, CriteriaQuery<T> criteriaQuery, Pageable pageable) {
        return findPage(entityClass, criteriaQuery, pageable, null);
    }

    /**
     * 查询实体对象数量
     *
     * @param entityClass   实体类
     * @param criteriaQuery 查询条件
     * @param filters       筛选
     * @return 实体对象数量
     */
    protected <T extends BaseEntity> Long count(Class<T> entityClass, CriteriaQuery<T> criteriaQuery, List<Filter> filters) {
        Assert.notNull(criteriaQuery, "[criteriaQuery] cannot be null!");
        Assert.notNull(criteriaQuery.getSelection(), "[criteriaQuery.getSelection()] cannot be null!");
        Assert.notEmpty(criteriaQuery.getRoots(), "[criteriaQuery.getRoots()] cannot be empty!");

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        Root<T> root = findRoot(criteriaQuery);
        Predicate restrictions = criteriaQuery.getRestriction() != null ? criteriaQuery.getRestriction() : cb.conjunction();
        if (CollectionUtils.isNotEmpty(filters)) {
            restrictions = cb.and(restrictions, toPredicate(root, filters));
        }
        criteriaQuery.where(restrictions);

        CriteriaQuery<Long> countCq = cb.createQuery(Long.class);
        copyCriteriaWithoutSelectionAndOrder(criteriaQuery, countCq);

        Root<?> countRoot = findRoot(countCq);
        if (criteriaQuery.isDistinct()) {
            countCq.select(cb.countDistinct(countRoot));
        } else {
            countCq.select(cb.count(countRoot));
        }
        return entityManager.createQuery(countCq).getSingleResult();
    }

    /**
     * 查询实体对象数量
     *
     * @param criteriaQuery 查询条件
     * @return 实体对象数量
     */
    protected <T extends BaseEntity> Long count(CriteriaQuery<T> criteriaQuery) {
        Root<T> root = findRoot(criteriaQuery);
        return count((Class<T>) root.getJavaType(), criteriaQuery, null);
    }

    /**
     * 查找Root
     *
     * @param criteriaQuery 查询条件
     * @return Root
     */
    @SuppressWarnings("unchecked")
    protected <T extends BaseEntity> Root<T> findRoot(CriteriaQuery<?> criteriaQuery) {
        Assert.notNull(criteriaQuery, "criteriaQuery must not be null");
        return (Root<T>) criteriaQuery.getRoots().iterator().next();
    }

    /**
     * 获取Path
     *
     * @param path          Path
     * @param attributeName 属性名称
     * @return Path
     */
    @SuppressWarnings("unchecked")
    private <X> Path<X> getPath(Path<?> path, String attributeName) {
        if (path == null || StringUtils.isEmpty(attributeName)) {
            return (Path<X>) path;
        }
        return getPath(path.get(StringUtils.substringBefore(attributeName, ATTRIBUTE_SEPARATOR)), StringUtils.substringAfter(attributeName, ATTRIBUTE_SEPARATOR));
    }

    /**
     * 获取或创建别名
     *
     * @param selection Selection
     * @return 别名
     */
    private synchronized String getOrCreateAlias(Selection<?> selection) {
        Assert.notNull(selection, "selection must not be null");

        String alias = selection.getAlias();
        if (alias == null) {
            if (aliasCount > 1000) {
                aliasCount = 0;
            }
            alias = ALIAS_PREFIX + aliasCount++;
            selection.alias(alias);
        }
        return alias;
    }

    /**
     * 拷贝Join
     *
     * @param from 源
     * @param to   目标
     */
    private void copyJoins(From<?, ?> from, From<?, ?> to) {
        Assert.notNull(from, "from must not be null");
        Assert.notNull(to, "to must not be null");

        for (Join<?, ?> fromJoin : from.getJoins()) {
            Join<?, ?> toJoin = to.join(fromJoin.getAttribute().getName(), fromJoin.getJoinType());
            toJoin.alias(getOrCreateAlias(fromJoin));
            copyJoins(fromJoin, toJoin);
        }
    }

    /**
     * 拷贝Fetch
     *
     * @param from 源
     * @param to   目标
     */
    private void copyFetches(From<?, ?> from, From<?, ?> to) {
        Assert.notNull(from, "from must not be null");
        Assert.notNull(to, "to must not be null");

        for (Fetch<?, ?> fromFetch : from.getFetches()) {
            Fetch<?, ?> toFetch = to.fetch(fromFetch.getAttribute().getName());
            copyFetches(fromFetch, toFetch);
        }
    }

    /**
     * 拷贝Fetch
     *
     * @param from 源
     * @param to   目标
     */
    private void copyFetches(Fetch<?, ?> from, Fetch<?, ?> to) {
        Assert.notNull(from, "from must not be null");
        Assert.notNull(to, "to must not be null");

        for (Fetch<?, ?> fromFetch : from.getFetches()) {
            Fetch<?, ?> toFetch = to.fetch(fromFetch.getAttribute().getName());
            copyFetches(fromFetch, toFetch);
        }
    }

    /**
     * 拷贝查询条件(不包含Selection、Order)
     *
     * @param from 源
     * @param to   目标
     */
    public void copyCriteriaWithoutSelectionAndOrder(CriteriaQuery<?> from, CriteriaQuery<?> to) {
        Assert.notNull(from, "[CriteriaQuery<?>] cannot be null!");
        Assert.notNull(to, "[CriteriaQuery<?>] cannot be null!");

        for (Root<?> root : from.getRoots()) {
            Root<?> dest = to.from(root.getJavaType());
            dest.alias(getOrCreateAlias(root));
            copyJoins(root, dest);
            copyFetches(root, dest);
        }

        to.groupBy(from.getGroupList());
        to.distinct(from.isDistinct());

        if (from.getGroupRestriction() != null) {
            to.having(from.getGroupRestriction());
        }

        if (from.getRestriction() != null) {
            to.where(from.getRestriction());
        }
    }

    /**
     * 转换为Predicate
     *
     * @param root    Root
     * @param filters 筛选
     * @return Predicate
     */
    @SuppressWarnings("unchecked")
    private Predicate toPredicate(Root<?> root, List<Filter> filters) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        Predicate restrictions = cb.conjunction();
        if (root == null || CollectionUtils.isEmpty(filters)) {
            return restrictions;
        }
        for (Filter filter : filters) {
            if (filter == null) {
                continue;
            }
            String property = filter.getProperty();
            Operator operator = filter.getOperator();
            Object value = filter.getValue();
            Boolean ignoreCase = filter.getIgnoreCase();
            Path<?> path = getPath(root, property);
            if (path == null) {
                continue;
            }
            if (StringUtils.isBlank(property) && !Operator.or.equals(operator)) {
                continue;
            }
            if (StringUtils.contains(property, ".id_generator") || StringUtils.equals(property, "id")) {
                if (value instanceof String) {
                    if (StringUtils.isBlank((String) value)) {
                        value = null;
                    } else {
                        value = Long.valueOf(String.valueOf(value));
                    }
                }
            }
            if (value instanceof String) {
                if (StringUtils.isBlank((String) value)) {
                    value = null;
                }
                if (StringUtils.contains((String) value, "true")) {
                    value = true;
                } else if (StringUtils.contains((String) value, "false")) {
                    value = false;
                }
            }
            switch (operator) {
                case or:
                    List<Filter> filterList = filter.getFilters();
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        Predicate[] predicates = new Predicate[filterList.size()];
                        for (int i = 0; i < filterList.size(); i++) {
                            List<Filter> filters1 = new ArrayList<>();
                            filters1.add(filterList.get(i));
                            predicates[i] = toPredicate(root, filters1);
                        }
                        restrictions = cb.and(restrictions, cb.or(predicates));
                    }
                    break;
                case eq:
                    if (value != null) {
                        if (BooleanUtils.isTrue(ignoreCase) && String.class.isAssignableFrom(path.getJavaType()) && value instanceof String) {
                            restrictions = cb.and(restrictions, cb.equal(cb.lower((Path<String>) path), ((String) value).toLowerCase()));
                        } else {
                            restrictions = cb.and(restrictions, cb.equal(path, value));
                        }
                    } else {
                        restrictions = cb.and(restrictions, path.isNull());
                    }
                    break;
                case ne:
                    if (value != null) {
                        if (BooleanUtils.isTrue(ignoreCase) && String.class.isAssignableFrom(path.getJavaType()) && value instanceof String) {
                            restrictions = cb.and(restrictions, cb.notEqual(cb.lower((Path<String>) path), ((String) value).toLowerCase()));
                        } else {
                            restrictions = cb.and(restrictions, cb.notEqual(path, value));
                        }
                    } else {
                        restrictions = cb.and(restrictions, path.isNotNull());
                    }
                    break;
                case gt:
                    if (Number.class.isAssignableFrom(path.getJavaType()) && value instanceof Number) {
                        restrictions = cb.and(restrictions, cb.gt((Path<Number>) path, (Number) value));
                    }
                    break;
                case lt:
                    if (Number.class.isAssignableFrom(path.getJavaType()) && value instanceof Number) {
                        restrictions = cb.and(restrictions, cb.lt((Path<Number>) path, (Number) value));
                    }
                    break;
                case ge:
                    if (Number.class.isAssignableFrom(path.getJavaType()) && value instanceof Number) {
                        restrictions = cb.and(restrictions, cb.ge((Path<Number>) path, (Number) value));
                    }
                    break;
                case le:
                    if (Number.class.isAssignableFrom(path.getJavaType()) && value instanceof Number) {
                        restrictions = cb.and(restrictions, cb.le((Path<Number>) path, (Number) value));
                    }
                    break;
                case lessThan:
                    if (Date.class.isAssignableFrom(path.getJavaType()) && value instanceof Date) {
                        restrictions = cb.and(restrictions, cb.lessThan((Path<Date>) path, (Date) value));
                    }
                    break;
                case lessThanOrEqualTo:
                    if (Date.class.isAssignableFrom(path.getJavaType()) && value instanceof Date) {
                        restrictions = cb.and(restrictions, cb.lessThanOrEqualTo((Path<Date>) path, (Date) value));
                    }
                    break;
                case greaterThan:
                    if (Date.class.isAssignableFrom(path.getJavaType()) && value instanceof Date) {
                        restrictions = cb.and(restrictions, cb.greaterThan((Path<Date>) path, (Date) value));
                    }
                    break;
                case greaterThanOrEqualTo:
                    if (Date.class.isAssignableFrom(path.getJavaType()) && value instanceof Date) {
                        restrictions = cb.and(restrictions, cb.greaterThanOrEqualTo((Path<Date>) path, (Date) value));
                    }
                    break;
                case like:
                    if (String.class.isAssignableFrom(path.getJavaType()) && value instanceof String) {
                        if (BooleanUtils.isTrue(ignoreCase)) {
                            restrictions = cb.and(restrictions, cb.like(cb.lower((Path<String>) path), ((String) value).toLowerCase()));
                        } else {
                            restrictions = cb.and(restrictions, cb.like((Path<String>) path, (String) value));
                        }
                    }
                    break;
                case in:
                    restrictions = cb.and(restrictions, path.in(value));
                    break;
                case isNull:
                    restrictions = cb.and(restrictions, path.isNull());
                    break;
                case isNotNull:
                    restrictions = cb.and(restrictions, path.isNotNull());
                    break;
                default:
                    break;
            }
        }
        return restrictions;
    }

    /**
     * 转换为Order
     *
     * @param root   Root
     * @param orders 排序
     * @return Order
     */
    private List<jakarta.persistence.criteria.Order> toOrders(Root<?> root, List<Order> orders) {
        List<jakarta.persistence.criteria.Order> orderList = new ArrayList<>();
        if (root == null || CollectionUtils.isEmpty(orders)) {
            return orderList;
        }
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        for (Order order : orders) {
            if (order == null) {
                continue;
            }
            String property = order.getProperty();
            Order.Direction direction = order.getDirection();
            Path<?> path = getPath(root, property);
            if (path == null || direction == null) {
                continue;
            }
            switch (direction) {
                case asc:
                    orderList.add(cb.asc(path));
                    break;
                case desc:
                    orderList.add(cb.desc(path));
                    break;
                case str_asc:
                    orderList.add(cb.asc(root.get(property).as(Integer.class)));
                    break;
                case str_desc:
                    orderList.add(cb.desc(root.get(property).as(Integer.class)));
                    break;
                default:
                    break;
            }
        }
        return orderList;
    }

    public int updateBySQL(String sql, String[] p) {
        Query query = entityManager.createQuery(sql);
        return query.executeUpdate();
    }

    // ==================== 原生 SQL 查询 ====================

    @Override
    @SuppressWarnings("unchecked")
    public <E extends BaseEntity> List<E> findListBySQL(String sql, Class<E> resultClass, Map<String, Object> params) {
        Query query = entityManager.createNativeQuery(sql, resultClass);
        setParameters(query, params);
        return query.getResultList();
    }

    @Override
    public <E extends BaseEntity> List<E> findListBySQL(String sql, Class<E> resultClass) {
        return findListBySQL(sql, resultClass, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends BaseEntity> E findBySQL(String sql, Class<E> resultClass, Map<String, Object> params) {
        Query query = entityManager.createNativeQuery(sql, resultClass);
        setParameters(query, params);
        try {
            return (E) query.getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends BaseEntity> Page<E> findPageBySQL(String sql, Class<E> resultClass, Map<String, Object> params, Pageable pageable) {
        if (pageable == null) {
            pageable = new Pageable();
        }
        // 计数：包装子查询
        String countSQL = "SELECT COUNT(*) FROM (" + sql + ") AS _cnt";
        Query countQuery = entityManager.createNativeQuery(countSQL);
        setParameters(countQuery, params);
        long total = ((Number) countQuery.getSingleResult()).longValue();
        // 数据
        Query dataQuery = entityManager.createNativeQuery(sql, resultClass);
        setParameters(dataQuery, params);
        dataQuery.setFirstResult((pageable.getPageNumber() - 1) * pageable.getPageSize());
        dataQuery.setMaxResults(pageable.getPageSize());
        List<E> list = dataQuery.getResultList();
        return new Page<>(list, total, pageable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object[]> findScalarListBySQL(String sql, Map<String, Object> params) {
        Query query = entityManager.createNativeQuery(sql);
        setParameters(query, params);
        return query.getResultList();
    }

    @Override
    public long countBySQL(String sql, Map<String, Object> params) {
        Query query = entityManager.createNativeQuery(sql);
        setParameters(query, params);
        return ((Number) query.getSingleResult()).longValue();
    }

    @Override
    public int updateBySQL(String sql, Map<String, Object> params) {
        Query query = entityManager.createNativeQuery(sql);
        setParameters(query, params);
        return query.executeUpdate();
    }

    /**
     * 设置命名参数到查询
     */
    private void setParameters(Query query, Map<String, Object> params) {
        if (params != null) {
            params.forEach(query::setParameter);
        }
    }

    // ==================== QueryDSL 类型安全查询（与现有查询并存） ====================

    /**
     * 获取 QueryDSL 查询工厂，用于构建类型安全的 JPAQuery。
     * <p>示例：
     * <pre>{@code
     * queryFactory().selectFrom(QUser.user)
     *     .where(QUser.user.username.eq("admin"))
     *     .fetchOne();
     * }</pre>
     *
     * @return JPAQueryFactory
     */
    public JPAQueryFactory queryFactory() {
        return queryFactory;
    }

    /**
     * 基于 QueryDSL 的分页查询，与现有 Page / Pageable 结果封装结合。
     * Service 层可用 Q 类构建类型安全的 WHERE / ORDER BY，再交由本方法完成分页与计数。
     *
     * @param contentQuery 已配置 select / where / orderBy 的内容查询（不要提前 offset/limit）
     * @param pageable     分页参数（pageNumber / pageSize 生效；filters/orders 由调用方用 Q 类自行处理）
     * @return 分页结果
     */
    public <E extends BaseEntity> Page<E> findPage(JPAQuery<E> contentQuery, Pageable pageable) {
        Assert.notNull(contentQuery, "contentQuery must not be null");
        if (pageable == null) {
            pageable = new Pageable();
        }
        long total = contentQuery.fetchCount();
        List<E> list = contentQuery
                .offset((long) (pageable.getPageNumber() - 1) * pageable.getPageSize())
                .limit(pageable.getPageSize())
                .fetch();
        return new Page<>(list, total, pageable);
    }

    /**
     * 基于 QueryDSL 的计数查询（语法糖，等价于 query.fetchCount()）。
     */
    protected <E extends BaseEntity> long count(JPAQuery<E> query) {
        Assert.notNull(query, "query must not be null");
        return query.fetchCount();
    }

    // ==================== Blaze-Persistence 查询（与现有查询并存） ====================

    /**
     * 获取 Blaze-Persistence CriteriaBuilderFactory，用于构建高级查询
     * （CTE / 窗口函数 / keyset 分页 / 批量 upsert 等）。
     */
    public CriteriaBuilderFactory cbf() {
        return criteriaBuilderFactory;
    }

    /**
     * 获取 EntityViewManager，用于 EntityView DTO 投影查询与可写视图。
     */
    public EntityViewManager evm() {
        return entityViewManager;
    }

    /**
     * 基于 Blaze-Persistence 的分页查询，与现有 Page / Pageable 结果封装结合。
     * Blaze 一次调用完成 count + 内容查询（PagedList），性能优于分离查询。
     *
     * @param query    已配置 select/from/where/orderBy 的 FullQueryBuilder（不要提前 page()）
     * @param pageable 分页参数
     * @return 分页结果
     */
    public <E extends BaseEntity> Page<E> findPage(FullQueryBuilder<E, ?> query, Pageable pageable) {
        Assert.notNull(query, "query must not be null");
        if (pageable == null) {
            pageable = new Pageable();
        }
        PagedList<E> paged = query
                .page((pageable.getPageNumber() - 1) * pageable.getPageSize(), pageable.getPageSize())
                .getResultList();
        return new Page<>(paged, paged.getTotalSize(), pageable);
    }
}
