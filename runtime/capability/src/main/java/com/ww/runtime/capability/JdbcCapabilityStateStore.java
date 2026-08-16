package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@link CapabilityStateStore} 的 PostgreSQL 实现（plain JDBC，无连接池假设）。
 *
 * <p>写路径使用 {@code INSERT ... ON CONFLICT (node_id, capability) DO UPDATE}
 * 幂等 upsert：首次插入时另一侧状态取列默认值 'DISABLED'，
 * 之后每次只更新目标列与 updated_at。
 *
 * <p>使用前须先 {@link CapabilitySchema#bootstrap(DataSource)} 建表。
 */
public final class JdbcCapabilityStateStore implements CapabilityStateStore {

    private static final String SELECT_ONE =
            "SELECT node_id, capability, desired, actual, updated_at"
                    + " FROM rt_capability_state WHERE node_id = ? AND capability = ?";

    private static final String SELECT_ALL =
            "SELECT node_id, capability, desired, actual, updated_at"
                    + " FROM rt_capability_state ORDER BY node_id, capability";

    private static final String UPSERT_DESIRED =
            "INSERT INTO rt_capability_state (node_id, capability, desired, actual, updated_at)"
                    + " VALUES (?, ?, ?, 'DISABLED', ?)"
                    + " ON CONFLICT (node_id, capability)"
                    + " DO UPDATE SET desired = EXCLUDED.desired, updated_at = EXCLUDED.updated_at";

    private static final String UPSERT_ACTUAL =
            "INSERT INTO rt_capability_state (node_id, capability, desired, actual, updated_at)"
                    + " VALUES (?, ?, 'DISABLED', ?, ?)"
                    + " ON CONFLICT (node_id, capability)"
                    + " DO UPDATE SET actual = EXCLUDED.actual, updated_at = EXCLUDED.updated_at";

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcCapabilityStateStore(DataSource dataSource) {
        this(dataSource, SystemClock.instance());
    }

    public JdbcCapabilityStateStore(DataSource dataSource, Clock clock) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<CapabilityRecord> get(NodeId node, CapabilityType type) {
        require(node, type);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_ONE)) {
            statement.setString(1, node.value());
            statement.setString(2, type.name());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to load capability state: " + node + "/" + type, e);
        }
    }

    @Override
    public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        require(node, type, desired);
        upsert(UPSERT_DESIRED, node, type, desired);
    }

    @Override
    public void setActual(NodeId node, CapabilityType type, CapabilityState actual) {
        require(node, type, actual);
        upsert(UPSERT_ACTUAL, node, type, actual);
    }

    @Override
    public List<CapabilityRecord> list() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL);
             ResultSet rs = statement.executeQuery()) {
            List<CapabilityRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(map(rs));
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list capability states", e);
        }
    }

    private void upsert(String sql, NodeId node, CapabilityType type, CapabilityState state) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, node.value());
            statement.setString(2, type.name());
            statement.setString(3, state.name());
            statement.setLong(4, clock.nowMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "failed to persist capability state: " + node + "/" + type + " -> " + state, e);
        }
    }

    private static CapabilityRecord map(ResultSet rs) throws SQLException {
        return new CapabilityRecord(
                NodeId.of(rs.getString("node_id")),
                CapabilityType.parse(rs.getString("capability")),
                CapabilityState.valueOf(rs.getString("desired")),
                CapabilityState.valueOf(rs.getString("actual")),
                rs.getLong("updated_at"));
    }

    private static void require(NodeId node, CapabilityType type) {
        java.util.Objects.requireNonNull(node, "node");
        java.util.Objects.requireNonNull(type, "type");
    }

    private static void require(NodeId node, CapabilityType type, CapabilityState state) {
        require(node, type);
        java.util.Objects.requireNonNull(state, "state");
    }
}
