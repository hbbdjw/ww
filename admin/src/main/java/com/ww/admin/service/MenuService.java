package com.ww.admin.service;

import com.ww.admin.entity.QSysMenu;
import com.ww.admin.entity.SysMenu;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 菜单权限服务：管理菜单/按钮定义（角色-菜单映射见 {@link RoleService}）。
 * <p>
 * SysMenu.perms 即 Shiro 权限标识，替代旧的 Permission.code。
 */
@Service
public class MenuService {

    private final BaseDaoImpl baseDao;

    public MenuService(BaseDaoImpl baseDao) {
        this.baseDao = baseDao;
    }

    public SysMenu findById(Long id) {
        return baseDao.find(SysMenu.class, id);
    }

    /**
     * 按 perms 查询菜单（幂等检查用）。取第一条，避免数据库存在重复 perms 时抛
     * {@code NonUniqueResultException} 导致启动失败；perms 唯一性由业务层保证。
     */
    public SysMenu findByPerms(String perms) {
        return baseDao.queryFactory()
                .selectFrom(QSysMenu.sysMenu)
                .where(QSysMenu.sysMenu.perms.eq(perms))
                .fetchFirst();
    }

    public List<SysMenu> findAll() {
        return baseDao.queryFactory()
                .selectFrom(QSysMenu.sysMenu)
                .orderBy(QSysMenu.sysMenu.orderNum.asc().nullsLast())
                .fetch();
    }

    public Page<SysMenu> findPage(Pageable pageable) {
        return baseDao.findPage(SysMenu.class, pageable);
    }

    @Transactional
    public void save(SysMenu menu) {
        if (menu.isNew()) {
            baseDao.persist(menu);
        } else {
            baseDao.merge(menu);
        }
    }

    @Transactional
    public void delete(Long id) {
        SysMenu menu = baseDao.find(SysMenu.class, id);
        if (menu != null) {
            baseDao.remove(menu);
        }
    }
}
