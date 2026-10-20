package com.ww.runtime.server.security;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

import com.ww.runtime.common.RuntimeFrameworkException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JdbcRbacStore 无真实数据库环境下的基础行为：SQLException 一律包装为
 * {@link RuntimeFrameworkException}（真实建表/读写路径依赖 PostgreSQL，由集成测试覆盖）。
 */
class JdbcRbacStoreTest {

    private final JdbcRbacStore store = new JdbcRbacStore(new FailingDataSource());

    @Test
    void identitySqlFailuresAreWrapped() {
        assertThatThrownBy(() -> store.findUserByUsername("admin"))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("username");

        assertThatThrownBy(() -> store.listUsers(null, null))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("list users");

        assertThatThrownBy(() -> store.createUser(sampleUser()))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("admin");

        assertThatThrownBy(() -> store.listDepts())
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("list depts");
    }

    @Test
    void authorizationSqlFailuresAreWrapped() {
        assertThatThrownBy(() -> store.findRoleByKey("super_admin"))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("key");

        assertThatThrownBy(() -> store.listRoles())
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("list roles");

        assertThatThrownBy(() -> store.loadPermissions(1L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("permissions");

        assertThatThrownBy(() -> store.createRole(sampleRole()))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("super_admin");
    }

    @Test
    void bootstrapWrapsSqlFailure() {
        assertThatThrownBy(() -> RbacSchema.bootstrap(new FailingDataSource()))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("bootstrap");
    }

    /** 互斥播种在无库环境下同样包装 SQLException（并发首启的串行化入口）。 */
    @Test
    void seedExclusivelyWrapsSqlFailure() {
        assertThatThrownBy(() -> RbacSchema.seedExclusively(new FailingDataSource(), () -> {
            throw new AssertionError("播种动作不应在取锁失败时执行");
        }))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("seedExclusively");
    }

    private static RbacStore.UserAccount sampleUser() {
        return new RbacStore.UserAccount(0, 0, "admin", "管理员", "encoded",
                null, null, 0, 1, null, null, 1, 1);
    }

    private static RbacStore.Role sampleRole() {
        return new RbacStore.Role(0, "超级管理员", "super_admin", 0, 1, 1, null, 1, 1);
    }

    /** 所有连接获取均失败的 DataSource 桩。 */
    private static final class FailingDataSource implements DataSource {

        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("no database in unit test");
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("no database in unit test");
        }

        @Override
        public PrintWriter getLogWriter() {
            return new PrintWriter(System.err);
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getLogger("test");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
