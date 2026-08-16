package com.ww.admin.service;

import com.ww.admin.entity.QSysDept;
import com.ww.admin.entity.SysDept;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 部门服务：管理部门树形结构，支持祖先列表查询（用于数据范围"本部门及以下"）。
 */
@Service
public class DeptService {

    private final BaseDaoImpl baseDao;

    public DeptService(BaseDaoImpl baseDao) {
        this.baseDao = baseDao;
    }

    public SysDept findById(Long id) {
        return baseDao.find(SysDept.class, id);
    }

    public List<SysDept> findAll() {
        return baseDao.queryFactory()
                .selectFrom(QSysDept.sysDept)
                .orderBy(QSysDept.sysDept.orderNum.asc().nullsLast())
                .fetch();
    }

    public Page<SysDept> findPage(Pageable pageable) {
        return baseDao.findPage(SysDept.class, pageable);
    }

    @Transactional
    public void save(SysDept dept) {
        if (dept.getStatus() == null) {
            dept.setStatus(1);
        }
        if (dept.isNew()) {
            baseDao.persist(dept);
        } else {
            baseDao.merge(dept);
        }
    }

    @Transactional
    public void delete(Long id) {
        SysDept dept = baseDao.find(SysDept.class, id);
        if (dept != null) {
            baseDao.remove(dept);
        }
    }

    /**
     * 查询指定部门的所有后代部门（基于 ancestors 字段，用于数据范围"本部门及以下"）。
     */
    public List<SysDept> findDescendants(Long deptId) {
        return baseDao.queryFactory()
                .selectFrom(QSysDept.sysDept)
                .where(QSysDept.sysDept.ancestors.contains(String.valueOf(deptId)))
                .fetch();
    }
}
