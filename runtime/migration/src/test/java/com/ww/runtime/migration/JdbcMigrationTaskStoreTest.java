package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JdbcMigrationTaskStore 无真实数据库环境下的基础行为：SQLException 一律包装为
 * MigrationException（真实建表/读写路径依赖 PostgreSQL，由集成测试覆盖）。
 */
class JdbcMigrationTaskStoreTest {

    private final JdbcMigrationTaskStore store =
            new JdbcMigrationTaskStore(new FailingDataSource(), new TestClock());

    @Test
    void sqlFailuresAreWrappedAsMigrationException() {
        NodeId node = NodeId.of("node-1");

        assertThatThrownBy(() -> store.create("mock", 0, node, NodeId.of("node-2")))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("create");

        assertThatThrownBy(() -> store.get(1L))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("get");

        assertThatThrownBy(store::listActive)
                .isInstanceOf(MigrationException.class);
        assertThatThrownBy(store::listAll)
                .isInstanceOf(MigrationException.class);

        // get 失败时 transition 会在读取阶段即抛 MigrationException
        assertThatThrownBy(() -> store.transition(1L, MigrationTaskState.PREPARING))
                .isInstanceOf(MigrationException.class);

        assertThatThrownBy(() -> store.saveProgress(1L, 1L, 1L, null))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("saveProgress");
    }

    @Test
    void bootstrapWrapsSqlFailure() {
        assertThatThrownBy(() -> MigrationSchema.bootstrap(new FailingDataSource()))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("bootstrap");
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
            return null;
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
            throw new SQLFeatureNotSupportedException("not supported");
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
