package com.ww.admin.service;

import com.querydsl.core.types.Predicate;
import com.querydsl.jpa.impl.JPAQuery;
import com.ww.admin.entity.QSysUser;
import com.ww.admin.entity.SysDept;
import com.ww.admin.entity.SysUser;
import com.ww.admin.security.AuthzCacheService;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import org.apache.shiro.authc.credential.PasswordService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 用户服务：管理用户增删改查，密码哈希由 Shiro PasswordService 处理。
 * <p>
 * 用户角色/状态变更时通过 {@link AuthzCacheService} 主动失效授权缓存，
 * 满足 read-after-write 一致性。
 */
@Service
public class UserService {

    private final BaseDaoImpl baseDao;
    private final PasswordService passwordService;
    private final DataScopeService dataScopeService;
    private final AuthzCacheService authzCacheService;
    private final DeptService deptService;

    public UserService(BaseDaoImpl baseDao, PasswordService passwordService,
                       DataScopeService dataScopeService, AuthzCacheService authzCacheService,
                       DeptService deptService) {
        this.baseDao = baseDao;
        this.passwordService = passwordService;
        this.dataScopeService = dataScopeService;
        this.authzCacheService = authzCacheService;
        this.deptService = deptService;
    }

    public Page<SysUser> findPage(Pageable pageable) {
        return baseDao.findPage(SysUser.class, pageable);
    }

    /**
     * 按当前登录用户的数据范围分页查询用户（行级权限过滤）。
     * <p>data_scope=1（全部数据）时不附加过滤；其它范围按角色 data_scope 并集过滤。
     */
    public Page<SysUser> findPageWithScope(Pageable pageable, String usernameLike) {
        QSysUser u = QSysUser.sysUser;
        JPAQuery<SysUser> q = baseDao.queryFactory()
                .selectFrom(u)
                .orderBy(u.createdDate.desc());
        if (usernameLike != null && !usernameLike.isEmpty()) {
            q.where(u.username.contains(usernameLike));
        }
        // 附加数据范围过滤
        dataScopeService.buildUserPredicate().ifPresent(p -> q.where((Predicate) p));
        return baseDao.findPage(q.getType(), pageable);
    }

    public SysUser findById(Long id) {
        return baseDao.find(SysUser.class, id);
    }

    public long count() {
        return baseDao.count(SysUser.class);
    }

    /**
     * 保存用户：若携带明文密码（plainPassword），则使用 Shiro PasswordService 哈希后写入 password。
     * 更新时若未传密码则保留原密码；更新时保留原有角色（角色通过 assignRoles 单独管理）。
     * <p>dept 字段通过 getReference 获取代理引用，避免级联持久化问题。
     * <p>更新现有用户时失效其授权缓存（角色/状态可能已变更）。
     */
    @Transactional
    public void save(SysUser user) {
        boolean isNew = user.isNew();

        // 处理部门引用：通过 DeptService 加载实体，避免级联持久化问题
        if (user.getDept() != null && user.getDept().getId() != null) {
            SysDept dept = deptService.findById(user.getDept().getId());
            user.setDept(dept);
        } else {
            user.setDept(null);
        }

        if (isNew) {
            // 新增用户
            if (user.getPlainPassword() != null && !user.getPlainPassword().isEmpty()) {
                user.setPassword(passwordService.encryptPassword(user.getPlainPassword()));
                user.setPlainPassword(null);
            }
            if (user.getStatus() == null) {
                user.setStatus(1);
            }
            baseDao.persist(user);
        } else {
            // 更新用户：加载现有记录，只更新允许修改的字段，保留密码和角色
            SysUser existing = baseDao.find(SysUser.class, user.getId());
            if (existing == null) {
                throw new IllegalArgumentException("用户不存在");
            }
            existing.setNickname(user.getNickname());
            existing.setDept(user.getDept());
            existing.setStatus(user.getStatus() != null ? user.getStatus() : existing.getStatus());

            // 仅在提供了新密码时更新密码
            if (user.getPlainPassword() != null && !user.getPlainPassword().isEmpty()) {
                existing.setPassword(passwordService.encryptPassword(user.getPlainPassword()));
            }
            // 角色不在此处更新，通过 assignRoles 接口单独管理

            baseDao.merge(existing);
            // 失效授权缓存
            authzCacheService.invalidate(existing.getUsername());
        }
    }

    @Transactional
    public void delete(SysUser user) {
        baseDao.remove(user);
    }

    /**
     * 重置用户密码。
     * @param userId 用户ID
     * @param newPassword 新明文密码
     */
    @Transactional
    public void resetPassword(Long userId, String newPassword) {
        SysUser user = baseDao.find(SysUser.class, userId);
        if (user == null) {
            throw new IllegalArgumentException("用户不存在");
        }
        user.setPassword(passwordService.encryptPassword(newPassword));
        baseDao.merge(user);
        authzCacheService.invalidate(user.getUsername());
    }

    /**
     * 更新用户的前端偏好设置（JSON 字符串）。
     * <p>使用原生 SQL UPDATE（createNativeQuery），避免加载完整实体和乐观锁冲突，
     * 适合高频、单字段更新场景。
     * <p>注意：必须调用 {@code updateBySQL(String, Map)} 重载，{@code updateBySQL(String, String[])}
     * 实际走的是 {@code createQuery}（HQL），会把表名当作实体名解析报错。
     */
    @Transactional
    public void updatePreferences(Long userId, String preferencesJson) {
        java.util.Map<String, Object> params = new java.util.HashMap<>(2);
        params.put("preferences", preferencesJson);
        params.put("userId", userId);
        baseDao.updateBySQL(
                "UPDATE sys_user SET preferences = :preferences WHERE user_id = :userId",
                params
        );
    }

    public SysUser findByUsername(String username) {
        return baseDao.queryFactory()
                .selectFrom(QSysUser.sysUser)
                .where(QSysUser.sysUser.username.eq(username))
                .fetchOne();
    }

    public List<SysUser> search(String usernameLike, Boolean hasDept) {
        QSysUser u = QSysUser.sysUser;
        JPAQuery<SysUser> q = baseDao.queryFactory().selectFrom(u);
        if (usernameLike != null && !usernameLike.isEmpty()) {
            q.where(u.username.contains(usernameLike));
        }
        if (Boolean.TRUE.equals(hasDept)) {
            q.where(u.dept.isNotNull());
        }
        return q.fetch();
    }

    public Page<SysUser> findPageByQueryDsl(Pageable pageable, String usernameLike) {
        QSysUser u = QSysUser.sysUser;
        JPAQuery<SysUser> q = baseDao.queryFactory()
                .selectFrom(u)
                .orderBy(u.createdDate.desc());
        if (usernameLike != null && !usernameLike.isEmpty()) {
            q.where(u.username.contains(usernameLike));
        }
        return baseDao.findPage(q.getType(), pageable);
    }
}
