package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JdbcCapabilityStateStore 无真实数据库环境下的基础行为：
 * SQLException 一律包装为 RuntimeFrameworkException（真实建表/读写路径依赖
 * PostgreSQL，由集成测试覆盖）。
 */
class JdbcCapabilityStateStoreTest {

    @Test
    void sqlFailuresAreWrappedAsFrameworkException() {
        JdbcCapabilityStateStore store = new JdbcCapabilityStateStore(new FailingDataSource());
        NodeId node = NodeId.of("node-1");

        assertThatThrownBy(() -> store.get(node, CapabilityType.MOCK))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessageContaining("node-1");
        assertThatThrownBy(() -> store.setDesired(node, CapabilityType.MOCK, CapabilityState.ACTIVE))
                .isInstanceOf(RuntimeFrameworkException.class);
        assertThatThrownBy(() -> store.setActual(node, CapabilityType.MOCK, CapabilityState.ACTIVE))
                .isInstanceOf(RuntimeFrameworkException.class);
        assertThatThrownBy(store::list)
                .isInstanceOf(RuntimeFrameworkException.class);
    }

    @Test
    void bootstrapWrapsSqlFailure() {
        assertThatThrownBy(() -> CapabilitySchema.bootstrap(new FailingDataSource()))
                .isInstanceOf(RuntimeFrameworkException.class)
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
