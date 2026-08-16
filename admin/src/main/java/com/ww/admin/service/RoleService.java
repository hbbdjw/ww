package com.ww.admin.service;

import com.ww.admin.entity.QSysDept;
import com.ww.admin.entity.QSysMenu;
import com.ww.admin.entity.QSysRole;
import com.ww.admin.entity.QSysUser;
import com.ww.admin.entity.SysDept;
import com.ww.admin.entity.SysMenu;
import com.ww.admin.entity.SysRole;
import com.ww.admin.security.AuthzCacheService;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 角色服务：管理角色及其菜单权限映射、部门数据范围映射（全部持久化在数据库）。
 * <p>
 * 权限变更（assignMenus、delete）时通过 {@link AuthzCacheService} 主动失效受影响用户的授权缓存，
 * 满足 read-after-write 一致性。
 */
@Service
public class RoleService {

    private final BaseDaoImpl baseDao;
    private final AuthzCacheService authzCacheService;

    public RoleService(BaseDaoImpl baseDao, AuthzCacheService authzCacheService) {
        this.baseDao = baseDao;
        this.authzCacheService = authzCacheService;
    }

    public SysRole findById(Long id) {
        return baseDao.find(SysRole.class, id);
    }

    public SysRole findByRoleKey(String roleKey) {
        return baseDao.queryFactory()
                .selectFrom(QSysRole.sysRole)
                .where(QSysRole.sysRole.roleKey.eq(roleKey))
                .fetchOne();
    }

    public List<SysRole> findAll() {
        return baseDao.queryFactory()
                .selectFrom(QSysRole.sysRole)
                .orderBy(QSysRole.sysRole.createdDate.asc())
                .fetch();
    }

    /**
     * 查询角色已分配的菜单ID列表
     */
    public List<Long> findMenuIds(Long roleId) {
        QSysMenu menu = QSysMenu.sysMenu;
        QSysRole role = QSysRole.sysRole;
        return baseDao.queryFactory()
                .select(menu.id)
                .from(role)
                .join(role.menus, menu)
                .where(role.id.eq(roleId))
                .fetch();
    }

    /**
     * 查询角色自定义数据范围的部门ID列表
     */
    public List<Long> findDeptIds(Long roleId) {
        QSysDept dept = QSysDept.sysDept;
        QSysRole role = QSysRole.sysRole;
        return baseDao.queryFactory()
                .select(dept.id)
                .from(role)
                .leftJoin(role.depts, dept)
                .where(role.id.eq(roleId))
                .fetch();
    }

    public Page<SysRole> findPage(Pageable pageable) {
        return baseDao.findPage(SysRole.class, pageable);
    }

    @Transactional
    public void save(SysRole role) {
        if (role.isNew()) {
            baseDao.persist(role);
        } else {
            baseDao.merge(role);
        }
    }

    @Transactional
    public void delete(Long id) {
        // 删除前先失效该角色下所有用户的授权缓存
        invalidateUsersByRole(id);
        SysRole role = baseDao.find(SysRole.class, id);
        if (role != null) {
            baseDao.remove(role);
        }
    }

    /**
     * 为角色分配菜单/按钮权限（全量覆盖）。
     * <p>权限变更后失效该角色下所有用户的授权缓存。
     */
    @Transactional
    public void assignMenus(Long roleId, List<Long> menuIds) {
        SysRole role = baseDao.find(SysRole.class, roleId);
        if (role == null) {
            throw new IllegalArgumentException("角色不存在: " + roleId);
        }
        List<SysMenu> menus = baseDao.queryFactory()
                .selectFrom(QSysMenu.sysMenu)
                .where(QSysMenu.sysMenu.id.in(menuIds))
                .fetch();
        role.setMenus(new java.util.HashSet<>(menus));
        baseDao.merge(role);
        // 失效该角色下所有用户的授权缓存
        invalidateUsersByRole(roleId);
    }

    /**
     * 为角色分配自定义数据范围部门（全量覆盖，仅 data_scope=2 时有意义）。
     */
    @Transactional
    public void assignDepts(Long roleId, List<Long> deptIds) {
        SysRole role = baseDao.find(SysRole.class, roleId);
        if (role == null) {
            throw new IllegalArgumentException("角色不存在: " + roleId);
        }
        if (deptIds == null || deptIds.isEmpty()) {
            role.setDepts(new java.util.HashSet<>());
        } else {
            List<SysDept> depts = baseDao.queryFactory()
                    .selectFrom(com.ww.admin.entity.QSysDept.sysDept)
                    .where(com.ww.admin.entity.QSysDept.sysDept.id.in(deptIds))
                    .fetch();
            role.setDepts(new java.util.HashSet<>(depts));
        }
        baseDao.merge(role);
    }

    /**
     * 失效指定角色下所有用户的授权缓存。
     * <p>查询 sys_user_role 关联表找出该角色的全部用户名，逐个移除 {@code shiro:authz:<username>}。
     */
    private void invalidateUsersByRole(Long roleId) {
        List<String> usernames = baseDao.queryFactory()
                .select(QSysUser.sysUser.username)
                .from(QSysUser.sysUser)
                .where(QSysUser.sysUser.roles.any().id.eq(roleId))
                .fetch();
        authzCacheService.invalidateAll(usernames);
    }
}
