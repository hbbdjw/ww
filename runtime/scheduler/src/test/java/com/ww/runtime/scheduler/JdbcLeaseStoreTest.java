package com.ww.runtime.scheduler;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link JdbcLeaseStore} 语句编排测试（JDK Proxy 手写 RecordingDataSource，
 * 脚本化 affected-row 并记录 SQL 执行顺序）。
 *
 * <p>真实 PostgreSQL 的并发抢占行为由 {@code RuntimePostgresSmokeIT} 覆盖。</p>
 */
@DisplayName("JdbcLeaseStore 语句编排（S20）")
class JdbcLeaseStoreTest {

    private final TestClock clock = new TestClock(1_000L);

    /** 记录型 JDBC 假件：按脚本返回 executeUpdate 影响行数，记录语句类型顺序。 */
    private static final class RecordingDataSource {

        final List<String> executed = new ArrayList<>();
        final Deque<Integer> updateResults = new ArrayDeque<>();

        DataSource proxy() {
            Connection connection = (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class}, connectionHandler());
            return (DataSource) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                    (p, m, a) -> {
                        if ("getConnection".equals(m.getName())) {
                            return connection;
                        }
                        throw new UnsupportedOperationException(m.getName());
                    });
        }

        private InvocationHandler connectionHandler() {
            return (proxy, method, args) -> {
                switch (method.getName()) {
                    case "prepareStatement":
                        String sql = (String) args[0];
                        executed.add(sql.trim().startsWith("UPDATE") ? "UPDATE" : "INSERT");
                        return Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{PreparedStatement.class}, statementHandler());
                    case "close":
                    case "unwrap":
                        return null;
                    case "isWrapperFor":
                        return false;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            };
        }

        private InvocationHandler statementHandler() {
            return (proxy, method, args) -> {
                switch (method.getName()) {
                    case "executeUpdate":
                        return updateResults.isEmpty() ? 0 : updateResults.poll();
                    case "setString":
                    case "setLong":
                    case "close":
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            };
        }
    }

    private JdbcLeaseStore store(RecordingDataSource ds) {
        return new JdbcLeaseStore(ds.proxy(), clock);
    }

    @Test
    @DisplayName("常规续约：单条 UPDATE 命中即返回，无冗余 INSERT")
    void renewHitsUpdateSingleRoundTrip() {
        RecordingDataSource ds = new RecordingDataSource();
        ds.updateResults.add(1);

        assertThat(store(ds).acquireOrRenew("node-a", 10_000)).isTrue();
        assertThat(ds.executed).containsExactly("UPDATE");
    }

    @Test
    @DisplayName("行不存在：UPDATE 未命中 → INSERT 成功")
    void missingRowFallsBackToInsert() {
        RecordingDataSource ds = new RecordingDataSource();
        ds.updateResults.add(0); // UPDATE 未命中
        ds.updateResults.add(1); // INSERT 成功

        assertThat(store(ds).acquireOrRenew("node-a", 10_000)).isTrue();
        assertThat(ds.executed).containsExactly("UPDATE", "INSERT");
    }

    @Test
    @DisplayName("插入冲突后再 UPDATE：覆盖两语句间隙租约恰好过期")
    void insertConflictRetriesUpdate() {
        RecordingDataSource ds = new RecordingDataSource();
        ds.updateResults.add(0); // UPDATE 未命中
        ds.updateResults.add(0); // INSERT 冲突（行已存在）
        ds.updateResults.add(1); // 再 UPDATE 命中（间隙过期）

        assertThat(store(ds).acquireOrRenew("node-a", 10_000)).isTrue();
        assertThat(ds.executed).containsExactly("UPDATE", "INSERT", "UPDATE");
    }

    @Test
    @DisplayName("租约被他人持有：三步全部未命中返回 false")
    void heldByOtherReturnsFalse() {
        RecordingDataSource ds = new RecordingDataSource();
        ds.updateResults.add(0);
        ds.updateResults.add(0);
        ds.updateResults.add(0);

        assertThat(store(ds).acquireOrRenew("node-a", 10_000)).isFalse();
        assertThat(ds.executed).containsExactly("UPDATE", "INSERT", "UPDATE");
    }

    @Test
    @DisplayName("SQL 异常包装为 RuntimeFrameworkException")
    void sqlFailureWrapped() {
        DataSource failing = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (p, m, a) -> {
                    if ("getConnection".equals(m.getName())) {
                        throw new SQLException("connection refused");
                    }
                    throw new UnsupportedOperationException(m.getName());
                });

        assertThatThrownBy(() -> new JdbcLeaseStore(failing, clock).acquireOrRenew("node-a", 10_000))
                .isInstanceOf(RuntimeFrameworkException.class);
    }
}
