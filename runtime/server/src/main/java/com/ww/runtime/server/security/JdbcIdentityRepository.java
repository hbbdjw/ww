package com.ww.runtime.server.security;

import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * RBAC 身份域 JDBC 仓库：部门、用户、用户-角色关系（plain JDBC）。
 *
 * <p>写路径使用 {@code INSERT ... ON CONFLICT DO UPDATE} 语义与参数化语句；
 * 关系绑定为覆盖式（事务内先删后插）。SQLException 一律包装为
 * {@link RuntimeFrameworkException}，不吞异常。</p>
 */
final class JdbcIdentityRepository {

    private static final String INSERT_DEPT =
            "INSERT INTO sys_dept (parent_id, dept_name, order_num, leader, phone, status, created_at, updated_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_DEPT =
            "UPDATE sys_dept SET parent_id = ?, dept_name = ?, order_num = ?, leader = ?,"
                    + " phone = ?, status = ?, updated_at = ? WHERE dept_id = ?";

    private static final String SELECT_DEPT =
            "SELECT dept_id, parent_id, dept_name, order_num, leader, phone, status, created_at, updated_at"
                    + " FROM sys_dept WHERE dept_id = ?";

    private static final String LIST_DEPTS =
            "SELECT dept_id, parent_id, dept_name, order_num, leader, phone, status, created_at, updated_at"
                    + " FROM sys_dept ORDER BY order_num, dept_id";

    private static final String DELETE_DEPT = "DELETE FROM sys_dept WHERE dept_id = ?";

    private static final String INSERT_USER =
            "INSERT INTO sys_user (dept_id, username, nickname, password, email, phone, gender,"
                    + " status, login_at, remark, created_at, updated_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_USER =
            "UPDATE sys_user SET dept_id = ?, nickname = ?, password = ?, email = ?, phone = ?,"
                    + " gender = ?, status = ?, login_at = ?, remark = ?, updated_at = ? WHERE user_id = ?";

    private static final String SELECT_USER =
            "SELECT user_id, dept_id, username, nickname, password, email, phone, gender, status,"
                    + " login_at, remark, created_at, updated_at FROM sys_user WHERE user_id = ?";

    private static final String SELECT_USER_BY_USERNAME =
            "SELECT user_id, dept_id, username, nickname, password, email, phone, gender, status,"
                    + " login_at, remark, created_at, updated_at FROM sys_user WHERE username = ?";

    private static final String LIST_USERS =
            "SELECT user_id, dept_id, username, nickname, password, email, phone, gender, status,"
                    + " login_at, remark, created_at, updated_at FROM sys_user"
                    + " WHERE (? IS NULL OR dept_id = ?)"
                    + " AND (? IS NULL OR lower(username) LIKE ? OR lower(nickname) LIKE ?)"
                    + " ORDER BY user_id";

    private static final String DELETE_USER = "DELETE FROM sys_user WHERE user_id = ?";

    private static final String UPDATE_LOGIN_AT =
            "UPDATE sys_user SET login_at = ? WHERE user_id = ?";

    private static final String DELETE_USER_ROLES =
            "DELETE FROM sys_user_role WHERE user_id = ?";

    private static final String INSERT_USER_ROLE =
            "INSERT INTO sys_user_role (user_id, role_id) VALUES (?, ?) ON CONFLICT DO NOTHING";

    private static final String SELECT_ROLE_IDS_BY_USER =
            "SELECT role_id FROM sys_user_role WHERE user_id = ?";

    private static final String SELECT_USER_IDS_BY_ROLE =
            "SELECT user_id FROM sys_user_role WHERE role_id = ?";

    private final DataSource dataSource;

    JdbcIdentityRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // ------------------------------------------------------------------
    // 部门
    // ------------------------------------------------------------------

    long createDept(RbacStore.Dept dept) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_DEPT, Statement.RETURN_GENERATED_KEYS)) {
            fillDept(ps, dept);
            ps.executeUpdate();
            return generatedId(ps);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to create dept: " + dept.deptName(), e);
        }
    }

    void updateDept(RbacStore.Dept dept) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_DEPT)) {
            fillDept(ps, dept);
            ps.setLong(9, dept.deptId());
            requireUpdated(ps, "dept", dept.deptId());
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to update dept: " + dept.deptId(), e);
        }
    }

    Optional<RbacStore.Dept> findDeptById(long deptId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_DEPT)) {
            ps.setLong(1, deptId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapDept(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load dept: " + deptId, e);
        }
    }

    List<RbacStore.Dept> listDepts() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_DEPTS);
             ResultSet rs = ps.executeQuery()) {
            List<RbacStore.Dept> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapDept(rs));
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list depts", e);
        }
    }

    void deleteDept(long deptId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_DEPT)) {
            ps.setLong(1, deptId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to delete dept: " + deptId, e);
        }
    }

    // ------------------------------------------------------------------
    // 用户
    // ------------------------------------------------------------------

    long createUser(RbacStore.UserAccount user) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_USER, Statement.RETURN_GENERATED_KEYS)) {
            fillUser(ps, user);
            ps.executeUpdate();
            return generatedId(ps);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to create user: " + user.username(), e);
        }
    }

    void updateUser(RbacStore.UserAccount user) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_USER)) {
            ps.setLong(1, user.deptId());
            ps.setString(2, user.nickname());
            ps.setString(3, user.password());
            ps.setString(4, emptyToNull(user.email()));
            ps.setString(5, emptyToNull(user.phone()));
            ps.setInt(6, user.gender());
            ps.setInt(7, user.status());
            if (user.loginAt() == null) {
                ps.setNull(8, java.sql.Types.BIGINT);
            } else {
                ps.setLong(8, user.loginAt());
            }
            ps.setString(9, emptyToNull(user.remark()));
            ps.setLong(10, user.updatedAt());
            ps.setLong(11, user.userId());
            requireUpdated(ps, "user", user.userId());
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to update user: " + user.userId(), e);
        }
    }

    Optional<RbacStore.UserAccount> findUserById(long userId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_USER)) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapUser(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load user: " + userId, e);
        }
    }

    Optional<RbacStore.UserAccount> findUserByUsername(String username) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_USER_BY_USERNAME)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapUser(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load user by username: " + username, e);
        }
    }

    List<RbacStore.UserAccount> listUsers(String keyword, Long deptId) {
        String kw = keyword == null || keyword.trim().isEmpty() ? null : "%" + keyword.trim().toLowerCase() + "%";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_USERS)) {
            if (deptId == null) {
                ps.setNull(1, java.sql.Types.BIGINT);
                ps.setNull(2, java.sql.Types.BIGINT);
            } else {
                ps.setLong(1, deptId);
                ps.setLong(2, deptId);
            }
            ps.setString(3, kw);
            if (kw == null) {
                ps.setNull(4, java.sql.Types.VARCHAR);
                ps.setNull(5, java.sql.Types.VARCHAR);
            } else {
                ps.setString(4, kw);
                ps.setString(5, kw);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<RbacStore.UserAccount> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapUser(rs));
                }
                return List.copyOf(result);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list users", e);
        }
    }

    void deleteUser(long userId) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement delRel = conn.prepareStatement(DELETE_USER_ROLES);
                 PreparedStatement delUser = conn.prepareStatement(DELETE_USER)) {
                delRel.setLong(1, userId);
                delRel.executeUpdate();
                delUser.setLong(1, userId);
                delUser.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to delete user: " + userId, e);
        }
    }

    void updateLoginAt(long userId, long loginAt) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_LOGIN_AT)) {
            ps.setLong(1, loginAt);
            ps.setLong(2, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to update login_at: " + userId, e);
        }
    }

    // ------------------------------------------------------------------
    // 用户-角色
    // ------------------------------------------------------------------

    void bindUserRoles(long userId, List<Long> roleIds) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement del = conn.prepareStatement(DELETE_USER_ROLES);
                 PreparedStatement insert = conn.prepareStatement(INSERT_USER_ROLE)) {
                del.setLong(1, userId);
                del.executeUpdate();
                for (long roleId : distinct(roleIds)) {
                    insert.setLong(1, userId);
                    insert.setLong(2, roleId);
                    insert.addBatch();
                }
                insert.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to bind user roles: " + userId, e);
        }
    }

    List<Long> findRoleIdsByUser(long userId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ROLE_IDS_BY_USER)) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
                return List.copyOf(result);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list role ids for user: " + userId, e);
        }
    }

    List<Long> findUserIdsByRole(long roleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_USER_IDS_BY_ROLE)) {
            ps.setLong(1, roleId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
                return List.copyOf(result);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list user ids for role: " + roleId, e);
        }
    }

    // ------------------------------------------------------------------
    // 映射
    // ------------------------------------------------------------------

    private static void fillDept(PreparedStatement ps, RbacStore.Dept dept) throws SQLException {
        ps.setLong(1, dept.parentId());
        ps.setString(2, dept.deptName());
        ps.setInt(3, dept.orderNum());
        ps.setString(4, emptyToNull(dept.leader()));
        ps.setString(5, emptyToNull(dept.phone()));
        ps.setInt(6, dept.status());
        ps.setLong(7, dept.createdAt());
        ps.setLong(8, dept.updatedAt());
    }

    private static void fillUser(PreparedStatement ps, RbacStore.UserAccount user) throws SQLException {
        ps.setLong(1, user.deptId());
        ps.setString(2, user.username());
        ps.setString(3, user.nickname());
        ps.setString(4, user.password());
        ps.setString(5, emptyToNull(user.email()));
        ps.setString(6, emptyToNull(user.phone()));
        ps.setInt(7, user.gender());
        ps.setInt(8, user.status());
        if (user.loginAt() == null) {
            ps.setNull(9, java.sql.Types.BIGINT);
        } else {
            ps.setLong(9, user.loginAt());
        }
        ps.setString(10, emptyToNull(user.remark()));
        ps.setLong(11, user.createdAt());
        ps.setLong(12, user.updatedAt());
    }

    private static RbacStore.Dept mapDept(ResultSet rs) throws SQLException {
        return new RbacStore.Dept(
                rs.getLong("dept_id"),
                rs.getLong("parent_id"),
                rs.getString("dept_name"),
                rs.getInt("order_num"),
                rs.getString("leader"),
                rs.getString("phone"),
                rs.getInt("status"),
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }

    private static RbacStore.UserAccount mapUser(ResultSet rs) throws SQLException {
        long loginAt = rs.getLong("login_at");
        return new RbacStore.UserAccount(
                rs.getLong("user_id"),
                rs.getLong("dept_id"),
                rs.getString("username"),
                rs.getString("nickname"),
                rs.getString("password"),
                rs.getString("email"),
                rs.getString("phone"),
                rs.getInt("gender"),
                rs.getInt("status"),
                rs.wasNull() ? null : loginAt,
                rs.getString("remark"),
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
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
