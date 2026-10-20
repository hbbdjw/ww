package com.ww.runtime.server.security;

import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * RBAC 授权域 JDBC 仓库：角色、菜单、角色-菜单关系与用户权限聚合（plain JDBC）。
 *
 * <p>关系绑定为覆盖式（事务内先删后插）；{@link #loadPermissions} 以一条
 * 连接查询经 用户→角色→角色菜单→菜单 聚合权限标识，按角色与菜单状态过滤。
 * SQLException 一律包装为 {@link RuntimeFrameworkException}。</p>
 */
final class JdbcAuthorizationRepository {

    private static final String INSERT_ROLE =
            "INSERT INTO sys_role (role_name, role_key, role_sort, status, data_scope, remark, created_at, updated_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_ROLE =
            "UPDATE sys_role SET role_name = ?, role_sort = ?, status = ?, data_scope = ?,"
                    + " remark = ?, updated_at = ? WHERE role_id = ?";

    private static final String SELECT_ROLE =
            "SELECT role_id, role_name, role_key, role_sort, status, data_scope, remark, created_at, updated_at"
                    + " FROM sys_role WHERE role_id = ?";

    private static final String SELECT_ROLE_BY_KEY =
            "SELECT role_id, role_name, role_key, role_sort, status, data_scope, remark, created_at, updated_at"
                    + " FROM sys_role WHERE role_key = ?";

    private static final String LIST_ROLES =
            "SELECT role_id, role_name, role_key, role_sort, status, data_scope, remark, created_at, updated_at"
                    + " FROM sys_role ORDER BY role_sort, role_id";

    private static final String DELETE_ROLE = "DELETE FROM sys_role WHERE role_id = ?";

    private static final String DELETE_ROLE_MENUS =
            "DELETE FROM sys_role_menu WHERE role_id = ?";

    private static final String INSERT_ROLE_MENU =
            "INSERT INTO sys_role_menu (role_id, menu_id) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private static final String SELECT_MENU_IDS_BY_ROLE =
            "SELECT menu_id FROM sys_role_menu WHERE role_id = ?";

    private static final String INSERT_MENU =
            "INSERT INTO sys_menu (parent_id, menu_name, menu_type, path, perms, icon, order_num, visible, status)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_MENU =
            "SELECT menu_id, parent_id, menu_name, menu_type, path, perms, icon, order_num, visible, status"
                    + " FROM sys_menu WHERE menu_id = ?";

    private static final String LIST_MENUS =
            "SELECT menu_id, parent_id, menu_name, menu_type, path, perms, icon, order_num, visible, status"
                    + " FROM sys_menu ORDER BY order_num, menu_id";

    private static final String SELECT_PERMISSIONS =
            "SELECT r.role_key, m.perms FROM sys_user_role ur"
                    + " JOIN sys_role r ON r.role_id = ur.role_id AND r.status = 1"
                    + " LEFT JOIN sys_role_menu rm ON rm.role_id = r.role_id"
                    + " LEFT JOIN sys_menu m ON m.menu_id = rm.menu_id AND m.status = 1"
                    + " WHERE ur.user_id = ?";

    private final DataSource dataSource;

    JdbcAuthorizationRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // ------------------------------------------------------------------
    // 角色
    // ------------------------------------------------------------------

    long createRole(RbacStore.Role role) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ROLE, Statement.RETURN_GENERATED_KEYS)) {
            fillRole(ps, role);
            ps.executeUpdate();
            return generatedId(ps);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to create role: " + role.roleKey(), e);
        }
    }

    void updateRole(RbacStore.Role role) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_ROLE)) {
            ps.setString(1, role.roleName());
            ps.setInt(2, role.roleSort());
            ps.setInt(3, role.status());
            ps.setInt(4, role.dataScope());
            ps.setString(5, emptyToNull(role.remark()));
            ps.setLong(6, role.updatedAt());
            ps.setLong(7, role.roleId());
            requireUpdated(ps, "role", role.roleId());
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to update role: " + role.roleId(), e);
        }
    }

    Optional<RbacStore.Role> findRoleById(long roleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ROLE)) {
            ps.setLong(1, roleId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRole(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load role: " + roleId, e);
        }
    }

    Optional<RbacStore.Role> findRoleByKey(String roleKey) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ROLE_BY_KEY)) {
            ps.setString(1, roleKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRole(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load role by key: " + roleKey, e);
        }
    }

    List<RbacStore.Role> listRoles() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_ROLES);
             ResultSet rs = ps.executeQuery()) {
            List<RbacStore.Role> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapRole(rs));
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list roles", e);
        }
    }

    void deleteRole(long roleId) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement delMenus = conn.prepareStatement(DELETE_ROLE_MENUS);
                 PreparedStatement delRole = conn.prepareStatement(DELETE_ROLE)) {
                delMenus.setLong(1, roleId);
                delMenus.executeUpdate();
                delRole.setLong(1, roleId);
                delRole.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to delete role: " + roleId, e);
        }
    }

    // ------------------------------------------------------------------
    // 角色-菜单
    // ------------------------------------------------------------------

    void bindRoleMenus(long roleId, List<Long> menuIds) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement del = conn.prepareStatement(DELETE_ROLE_MENUS);
                 PreparedStatement insert = conn.prepareStatement(INSERT_ROLE_MENU)) {
                del.setLong(1, roleId);
                del.executeUpdate();
                for (long menuId : distinct(menuIds)) {
                    insert.setLong(1, roleId);
                    insert.setLong(2, menuId);
                    insert.addBatch();
                }
                insert.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to bind role menus: " + roleId, e);
        }
    }

    List<Long> findMenuIdsByRole(long roleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_MENU_IDS_BY_ROLE)) {
            ps.setLong(1, roleId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
                return List.copyOf(result);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list menu ids for role: " + roleId, e);
        }
    }

    // ------------------------------------------------------------------
    // 菜单
    // ------------------------------------------------------------------

    long createMenu(RbacStore.Menu menu) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_MENU, Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, menu.parentId());
            ps.setString(2, menu.menuName());
            ps.setString(3, menu.menuType());
            ps.setString(4, emptyToNull(menu.path()));
            ps.setString(5, emptyToNull(menu.perms()));
            ps.setString(6, emptyToNull(menu.icon()));
            ps.setInt(7, menu.orderNum());
            ps.setInt(8, menu.visible());
            ps.setInt(9, menu.status());
            ps.executeUpdate();
            return generatedId(ps);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to create menu: " + menu.menuName(), e);
        }
    }

    Optional<RbacStore.Menu> findMenuById(long menuId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_MENU)) {
            ps.setLong(1, menuId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapMenu(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load menu: " + menuId, e);
        }
    }

    List<RbacStore.Menu> listMenus() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_MENUS);
             ResultSet rs = ps.executeQuery()) {
            List<RbacStore.Menu> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapMenu(rs));
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list menus", e);
        }
    }

    // ------------------------------------------------------------------
    // 权限聚合
    // ------------------------------------------------------------------

    RbacStore.UserPermissions loadPermissions(long userId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PERMISSIONS)) {
            ps.setLong(1, userId);
            Set<String> roleKeys = new LinkedHashSet<>();
            Set<String> perms = new LinkedHashSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    roleKeys.add(rs.getString("role_key"));
                    String perm = rs.getString("perms");
                    if (perm != null && !perm.isBlank()) {
                        perms.add(perm);
                    }
                }
            }
            return new RbacStore.UserPermissions(List.copyOf(roleKeys), List.copyOf(perms));
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load permissions for user: " + userId, e);
        }
    }

    // ------------------------------------------------------------------
    // 映射
    // ------------------------------------------------------------------

    private static void fillRole(PreparedStatement ps, RbacStore.Role role) throws SQLException {
        ps.setString(1, role.roleName());
        ps.setString(2, role.roleKey());
        ps.setInt(3, role.roleSort());
        ps.setInt(4, role.status());
        ps.setInt(5, role.dataScope());
        ps.setString(6, emptyToNull(role.remark()));
        ps.setLong(7, role.createdAt());
        ps.setLong(8, role.updatedAt());
    }

    private static RbacStore.Role mapRole(ResultSet rs) throws SQLException {
        return new RbacStore.Role(
                rs.getLong("role_id"),
                rs.getString("role_name"),
                rs.getString("role_key"),
                rs.getInt("role_sort"),
                rs.getInt("status"),
                rs.getInt("data_scope"),
                rs.getString("remark"),
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }

    private static RbacStore.Menu mapMenu(ResultSet rs) throws SQLException {
        return new RbacStore.Menu(
                rs.getLong("menu_id"),
                rs.getLong("parent_id"),
                rs.getString("menu_name"),
                rs.getString("menu_type"),
                rs.getString("path"),
                rs.getString("perms"),
                rs.getString("icon"),
                rs.getInt("order_num"),
                rs.getInt("visible"),
                rs.getInt("status"));
    }

    private static long generatedId(PreparedStatement ps) throws SQLException {
        try (ResultSet keys = ps.getGeneratedKeys()) {
            if (!keys.next()) {
                throw new RuntimeFrameworkException("insert succeeded but no generated key returned");
            }
            return keys.getLong(1);
        }
    }

    private static void requireUpdated(PreparedStatement ps, String label, long id) throws SQLException {
        if (ps.executeUpdate() == 0) {
            throw new IllegalArgumentException(label + " not found: " + id);
        }
    }

    private static List<Long> distinct(List<Long> ids) {
        List<Long> result = new ArrayList<>();
        if (ids != null) {
            for (Long id : ids) {
                if (id != null && !result.contains(id)) {
                    result.add(id);
                }
            }
        }
        return result;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
