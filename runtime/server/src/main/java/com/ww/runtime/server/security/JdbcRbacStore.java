package com.ww.runtime.server.security;

import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

/**
 * {@link RbacStore} 的 PostgreSQL 实现：委托身份域与授权域两个 JDBC 仓库，
 * 自身仅做端口适配（{@code runtime.store=jdbc}，默认模式）。
 *
 * <p>使用前须先 {@link RbacSchema#bootstrap} 建表。</p>
 */
@Repository
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "runtime.store", havingValue = "jdbc", matchIfMissing = true)
public class JdbcRbacStore implements RbacStore {

    private final JdbcIdentityRepository identity;
    private final JdbcAuthorizationRepository authorization;

    public JdbcRbacStore(DataSource dataSource) {
        this.identity = new JdbcIdentityRepository(dataSource);
        this.authorization = new JdbcAuthorizationRepository(dataSource);
    }

    @Override
    public long createDept(Dept dept) {
        return identity.createDept(dept);
    }

    @Override
    public void updateDept(Dept dept) {
        identity.updateDept(dept);
    }

    @Override
    public Optional<Dept> findDeptById(long deptId) {
        return identity.findDeptById(deptId);
    }

    @Override
    public List<Dept> listDepts() {
        return identity.listDepts();
    }

    @Override
    public void deleteDept(long deptId) {
        identity.deleteDept(deptId);
    }

    @Override
    public long createUser(UserAccount user) {
        return identity.createUser(user);
    }

    @Override
    public void updateUser(UserAccount user) {
        identity.updateUser(user);
    }

    @Override
    public Optional<UserAccount> findUserById(long userId) {
        return identity.findUserById(userId);
    }

    @Override
    public Optional<UserAccount> findUserByUsername(String username) {
        return identity.findUserByUsername(username);
    }

    @Override
    public List<UserAccount> listUsers(String keyword, Long deptId) {
        return identity.listUsers(keyword, deptId);
    }

    @Override
    public void deleteUser(long userId) {
        identity.deleteUser(userId);
    }

    @Override
    public void updateLoginAt(long userId, long loginAt) {
        identity.updateLoginAt(userId, loginAt);
    }

    @Override
    public void bindUserRoles(long userId, List<Long> roleIds) {
        identity.bindUserRoles(userId, roleIds);
    }

    @Override
    public List<Long> findRoleIdsByUser(long userId) {
        return identity.findRoleIdsByUser(userId);
    }

    @Override
    public List<Long> findUserIdsByRole(long roleId) {
        return identity.findUserIdsByRole(roleId);
    }

    @Override
    public long createRole(Role role) {
        return authorization.createRole(role);
    }

    @Override
    public void updateRole(Role role) {
        authorization.updateRole(role);
    }

    @Override
    public Optional<Role> findRoleById(long roleId) {
        return authorization.findRoleById(roleId);
    }

    @Override
    public Optional<Role> findRoleByKey(String roleKey) {
        return authorization.findRoleByKey(roleKey);
    }

    @Override
    public List<Role> listRoles() {
        return authorization.listRoles();
    }

    @Override
    public void deleteRole(long roleId) {
        authorization.deleteRole(roleId);
    }

    @Override
    public void bindRoleMenus(long roleId, List<Long> menuIds) {
        authorization.bindRoleMenus(roleId, menuIds);
    }

    @Override
    public List<Long> findMenuIdsByRole(long roleId) {
        return authorization.findMenuIdsByRole(roleId);
    }

    @Override
    public long createMenu(Menu menu) {
        return authorization.createMenu(menu);
    }

    @Override
    public Optional<Menu> findMenuById(long menuId) {
        return authorization.findMenuById(menuId);
    }

    @Override
    public List<Menu> listMenus() {
        return authorization.listMenus();
    }

    @Override
    public UserPermissions loadPermissions(long userId) {
        return authorization.loadPermissions(userId);
    }
}
