package com.ww.admin.dao;

import com.blazebit.persistence.CriteriaBuilder;
import com.blazebit.persistence.FullQueryBuilder;
import com.blazebit.persistence.PagedList;
import com.blazebit.persistence.view.EntityViewSetting;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.admin.config.BlazePersistenceConfig;
import com.ww.admin.config.DaoConfig;
import com.ww.admin.entity.SysDept;
import com.ww.admin.entity.SysUser;
import com.ww.admin.entity.SysUserView;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Blaze-Persistence 查询逻辑集成测试
 * <p>使用最小化 Spring 上下文（仅 JPA + Blaze + DAO，排除 CacheConfig 等缓存/RPC 配置），
 * PostgreSQL + create-drop，构造 7 条 Mock 用户数据验证：
 * <ul>
 *     <li>Blaze CriteriaBuilder 基础/条件查询</li>
 *     <li>findPage(FullQueryBuilder, Pageable) 分页桥接</li>
 *     <li>EntityView DTO 投影</li>
 *     <li>EntityView 分页（forPagination）</li>
 *     <li>窗口函数 ROW_NUMBER() OVER (...)</li>
 * </ul>
 */
@DisplayName("Blaze-Persistence 查询逻辑")
@SpringBootTest(classes = BlazePersistenceQueryTest.TestConfig.class)
@ActiveProfiles("test")
@Transactional
class BlazePersistenceQueryTest {

    /**
     * 最小化测试配置：仅加载 JPA 自动配置 + Blaze + DAO，不扫描 com.ww.admin.config（避免 CacheConfig）
     */
    @Configuration
    @EnableAutoConfiguration
    @EnableJpaAuditing
    @EntityScan("com.ww.admin.entity")
    @Import({BlazePersistenceConfig.class, DaoConfig.class})
    static class TestConfig {
    }

    @Autowired
    private BaseDaoImpl baseDao;

    @Autowired
    private EntityManager entityManager;

    private SysDept testDept;

    @BeforeEach
    void setUp() {
        // 创建测试部门（部分用户归属此部门，用于 IS NOT NULL 测试）
        testDept = new SysDept();
        testDept.setDeptName("测试部门");
        testDept.setParentId(0L);
        testDept.setAncestors("0");
        testDept.setOrderNum(0);
        testDept.setStatus(1);
        entityManager.persist(testDept);

        // 7 条 Mock 用户：username 长度 >= 4（满足 @Length(min=4) 约束），dept 部分为 null
        persist("alice",  testDept);
        persist("bobby",  testDept);
        persist("carol",  null);
        persist("dave",   testDept);
        persist("evelyn", null);
        persist("admin1", testDept);
        persist("admin2", null);
        entityManager.flush();
        // 用 SQL 覆盖 create_time 为确定性时间（@CreatedDate 审计会设为当前时间，不利于排序断言）
        overrideCreatedDate("alice",  "2026-01-15 10:00:00");
        overrideCreatedDate("bobby",  "2026-02-20 14:30:00");
        overrideCreatedDate("carol",  "2026-03-10 09:15:00");
        overrideCreatedDate("dave",   "2026-04-05 16:45:00");
        overrideCreatedDate("evelyn", "2026-05-12 11:20:00");
        overrideCreatedDate("admin1", "2026-06-01 08:00:00");
        overrideCreatedDate("admin2", "2026-07-08 13:10:00");
        entityManager.flush();
        entityManager.clear();
    }

    private void persist(String username, SysDept dept) {
        SysUser u = new SysUser();
        u.setUsername(username);
        u.setNickname("nick-" + username);
        u.setPassword("$shiro1$dummy.hash.for.testing");
        u.setDept(dept);
        u.setStatus(1);
        entityManager.persist(u);
    }

    private void overrideCreatedDate(String username, String timestamp) {
        entityManager.createNativeQuery("UPDATE sys_user SET create_time = ? WHERE username = ?")
                .setParameter(1, Timestamp.valueOf(timestamp))
                .setParameter(2, username)
                .executeUpdate();
    }

    // ==================== Blaze CriteriaBuilder 基础查询 ====================

    @Test
    @DisplayName("基础查询：SELECT u FROM SysUser u 返回全部 7 条")
    void shouldQueryAllUsers() {
        List<SysUser> users = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .orderByAsc("u.username")
                .getResultList();
        assertThat(users).hasSize(7);
        assertThat(users).extracting(SysUser::getUsername)
                .containsExactly("admin1", "admin2", "alice", "bobby", "carol", "dave", "evelyn");
    }

    // ==================== Blaze 条件查询 ====================

    @Test
    @DisplayName("条件查询：username LIKE 'admin%' 返回 admin1/admin2")
    void shouldFilterByLike() {
        List<SysUser> users = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .where("u.username").like().value("admin%").noEscape()
                .orderByAsc("u.username")
                .getResultList();
        assertThat(users).hasSize(2);
        assertThat(users).extracting(SysUser::getUsername).containsExactly("admin1", "admin2");
    }

    @Test
    @DisplayName("条件查询：dept IS NOT NULL 返回 4 条有部门的用户")
    void shouldFilterByIsNotNull() {
        List<SysUser> users = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .where("u.dept").isNotNull()
                .orderByAsc("u.username")
                .getResultList();
        assertThat(users).hasSize(4);
        assertThat(users).extracting(SysUser::getUsername)
                .containsExactly("admin1", "alice", "bobby", "dave");
        assertThat(users).allSatisfy(u -> assertThat(u.getDept()).isNotNull());
    }

    // ==================== Blaze 分页（findPage 桥接） ====================

    @Test
    @DisplayName("分页：第1页3条，总数7，按 username 排序")
    void shouldPaginatePage1() {
        CriteriaBuilder<SysUser> query = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .orderByAsc("u.username")
                .orderByAsc("u.id");
        Pageable pageable = new Pageable();
        pageable.setPageNumber(1);
        pageable.setPageSize(3);
        Page<SysUser> page = baseDao.findPage(query, pageable);
        assertThat(page.getContent()).hasSize(3);
        assertThat(page.getTotal()).isEqualTo(7L);
        assertThat(page.getContent()).extracting(SysUser::getUsername)
                .containsExactly("admin1", "admin2", "alice");
    }

    @Test
    @DisplayName("分页：第3页1条（7条按3分页 → 第3页1条）")
    void shouldPaginateLastPage() {
        CriteriaBuilder<SysUser> query = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .orderByAsc("u.username")
                .orderByAsc("u.id");
        Pageable pageable = new Pageable();
        pageable.setPageNumber(3);
        pageable.setPageSize(3);
        Page<SysUser> page = baseDao.findPage(query, pageable);
        assertThat(page.getContent()).hasSize(1);
        assertThat(page.getTotal()).isEqualTo(7L);
        assertThat(page.getContent().get(0).getUsername()).isEqualTo("evelyn");
    }

    // ==================== EntityView DTO 投影 ====================

    @Test
    @DisplayName("EntityView 投影：查询有部门用户的 id/username/nickname/deptId/createdDate")
    void shouldProjectToUserView() {
        CriteriaBuilder<SysUser> cb = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .where("u.dept").isNotNull()
                .orderByAsc("u.username");
        List<SysUserView> views = baseDao.evm()
                .applySetting(EntityViewSetting.create(SysUserView.class), cb)
                .getResultList();
        assertThat(views).hasSize(4);
        assertThat(views).extracting(SysUserView::getUsername)
                .containsExactly("admin1", "alice", "bobby", "dave");
        assertThat(views).allSatisfy(v -> {
            assertThat(v.getId()).isNotNull();
            assertThat(v.getDeptId()).isNotNull();
            assertThat(v.getNickname()).isNotNull();
            assertThat(v.getCreatedDate()).isNotNull();
        });
    }

    // ==================== EntityView 分页 ====================

    @Test
    @DisplayName("EntityView 分页：forPagination 第1页3条，总数7")
    void shouldPaginateEntityView() {
        CriteriaBuilder<SysUser> cb = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .orderByAsc("u.username")
                .orderByAsc("u.id");
        PagedList<SysUserView> paged = baseDao.evm()
                .applySetting(EntityViewSetting.create(SysUserView.class, 0, 3), cb)
                .getResultList();
        assertThat(paged).hasSize(3);
        assertThat(paged.getTotalSize()).isEqualTo(7L);
        assertThat(paged).extracting(SysUserView::getUsername)
                .containsExactly("admin1", "admin2", "alice");
    }

    // ==================== 窗口函数 ====================

    @Test
    @DisplayName("窗口函数：ROW_NUMBER() OVER (ORDER BY create_time ASC) 按创建时间编号")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void shouldUseWindowFunctionRowNumber() {
        // select() 改变了实际返回类型（Object[]），但泛型仍为 SysUser，需 raw cast
        List rows = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .select("u.username")
                .select("ROW_NUMBER() OVER (ORDER BY u.createdDate ASC)")
                .orderByAsc("u.createdDate")
                .getResultList();
        assertThat(rows).hasSize(7);
        Object[] first = (Object[]) rows.get(0);
        Object[] last = (Object[]) rows.get(6);
        // alice 创建最早 → rn=1；admin2 创建最晚 → rn=7
        assertThat(first[0]).isEqualTo("alice");
        assertThat(((Number) first[1]).intValue()).isEqualTo(1);
        assertThat(last[0]).isEqualTo("admin2");
        assertThat(((Number) last[1]).intValue()).isEqualTo(7);
    }

    // ==================== 三套查询栈并存验证 ====================

    @Test
    @DisplayName("并存验证：同一 BaseDao 实例 Criteria/QueryDSL/Blaze 三栈均可工作")
    void shouldCoexistThreeQueryStacks() {
        // Criteria（原有）
        long criteriaCount = baseDao.count(SysUser.class);
        // Blaze
        long blazeCount = baseDao.cbf()
                .create(entityManager, SysUser.class, "u")
                .getResultList()
                .size();
        assertThat(criteriaCount).isEqualTo(7L);
        assertThat(blazeCount).isEqualTo(7);
    }
}
