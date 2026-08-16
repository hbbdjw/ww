package com.ww.admin.service;

import com.ww.admin.entity.QSysUser;
import com.ww.admin.entity.SysDept;
import com.ww.admin.entity.SysRole;
import com.ww.admin.entity.SysUser;
import com.ww.common.dao.BaseDaoImpl;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import org.apache.shiro.SecurityUtils;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Set;

/**
 * 数据范围服务：根据当前登录用户角色的 {@code data_scope} 构建行级过滤谓词。
 * <p>
 * 数据范围取用户所有启用角色中"最宽"的一个（并集语义）：
 * <ul>
 *   <li>1 全部数据 —— 不附加过滤（返回 {@link Optional#empty()}）。</li>
 *   <li>2 自定义数据 —— {@code user.dept.id IN role.depts}。</li>
 *   <li>3 本部门数据 —— {@code user.dept.id = 当前用户部门ID}。</li>
 *   <li>4 本部门及以下 —— 当前部门ID 或其后代部门（通过 {@code ancestors} 包含当前部门ID）。</li>
 *   <li>5 仅本人 —— {@code user.id = 当前用户ID}。</li>
 * </ul>
 * 若任一角色为"全部数据"，则整体不附加过滤。无认证用户或无启用角色时返回"拒绝全部"谓词。
 * <p>
 * 直接依赖 {@link BaseDaoImpl} 而非 {@code UserService}，以避免与 UserService 之间的循环依赖。
 */
@Service
public class DataScopeService {

    /** 拒绝全部的哨兵谓词（id = -1，保证匹配不到任何行）。 */
    private static final BooleanExpression DENY_ALL = QSysUser.sysUser.id.eq(-1L);

    private final BaseDaoImpl baseDao;

    public DataScopeService(BaseDaoImpl baseDao) {
        this.baseDao = baseDao;
    }

    /**
     * 构建针对 SysUser 查询的数据范围谓词。
     *
     * @return {@link Optional#empty()} 表示不附加过滤（全部数据权限）；
     *         否则返回需要 AND 到查询上的谓词。
     */
    public Optional<Predicate> buildUserPredicate() {
        Object principal = SecurityUtils.getSubject().getPrincipal();
        if (principal == null) {
            return Optional.of(DENY_ALL);
        }
        SysUser current = baseDao.queryFactory()
                .selectFrom(QSysUser.sysUser)
                .where(QSysUser.sysUser.username.eq(principal.toString()))
                .fetchOne();
        if (current == null) {
            return Optional.of(DENY_ALL);
        }
        Set<SysRole> roles = current.getRoles();
        if (roles == null || roles.isEmpty()) {
            return Optional.of(DENY_ALL);
        }

        BooleanExpression combined = null;
        for (SysRole role : roles) {
            if (role.getStatus() == null || role.getStatus() != 1) {
                continue;
            }
            BooleanExpression rolePredicate = predicateForRole(role, current);
            if (rolePredicate == null) {
                // 任一角色为"全部数据" → 整体不附加过滤
                return Optional.empty();
            }
            combined = combined == null ? rolePredicate : combined.or(rolePredicate);
        }
        return Optional.of(combined == null ? DENY_ALL : combined);
    }

    /**
     * 计算单个角色的数据范围谓词。
     *
     * @return {@code null} 表示该角色为"全部数据"（无需过滤）；
     *         否则返回对应谓词。
     */
    private BooleanExpression predicateForRole(SysRole role, SysUser current) {
        String scope = role.getDataScope();
        if (scope == null || scope.isEmpty()) {
            scope = "1";
        }
        QSysUser u = QSysUser.sysUser;
        switch (scope) {
            case "1":
                return null;
            case "2":
                Set<SysDept> depts = role.getDepts();
                if (depts == null || depts.isEmpty()) {
                    return DENY_ALL;
                }
                return u.dept.id.in(depts.stream().map(SysDept::getId).toList());
            case "3":
                if (current.getDept() == null) {
                    return u.id.eq(current.getId());
                }
                return u.dept.id.eq(current.getDept().getId());
            case "4":
                if (current.getDept() == null) {
                    return u.id.eq(current.getId());
                }
                Long deptId = current.getDept().getId();
                // 本部门 或 后代部门（ancestors 包含当前部门ID）
                return u.dept.id.eq(deptId)
                        .or(u.dept.ancestors.contains(String.valueOf(deptId)));
            case "5":
                return u.id.eq(current.getId());
            default:
                return DENY_ALL;
        }
    }
}
