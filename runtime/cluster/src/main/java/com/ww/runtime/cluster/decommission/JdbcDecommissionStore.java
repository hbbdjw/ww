package com.ww.runtime.cluster.decommission;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link DecommissionStore} 的 PostgreSQL 实现。
 */
public final class JdbcDecommissionStore implements DecommissionStore {

    private static final String SELECT_ONE =
            "SELECT node_id, state, force_flag, detail, started_at, updated_at, completed_at"
                    + " FROM rt_decommission WHERE node_id = ?";

    private static final String SELECT_ACTIVE =
            "SELECT node_id, state, force_flag, detail, started_at, updated_at, completed_at"
                    + " FROM rt_decommission WHERE state IN ('DRAINING', 'REMOVING')";

    private static final String UPSERT =
            "INSERT INTO rt_decommission (node_id, state, force_flag, detail, started_at, updated_at, completed_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?)"
                    + " ON CONFLICT (node_id) DO UPDATE SET"
                    + " state = EXCLUDED.state, force_flag = EXCLUDED.force_flag, detail = EXCLUDED.detail,"
                    + " started_at = EXCLUDED.started_at, updated_at = EXCLUDED.updated_at,"
                    + " completed_at = EXCLUDED.completed_at";

    private static final String UPDATE_STATE =
            "UPDATE rt_decommission SET state = ?, detail = ?, updated_at = ?"
                    + " WHERE node_id = ?";

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcDecommissionStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<DecommissionRecord> get(NodeId node) {
        Objects.requireNonNull(node, "node");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ONE)) {
            ps.setString(1, node.value());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to get decommission record: " + node, e);
        }
    }

    @Override
    public List<DecommissionRecord> listActive() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ACTIVE);
             ResultSet rs = ps.executeQuery()) {
            List<DecommissionRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(map(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to list active decommission records", e);
        }
    }

    @Override
    public void upsert(DecommissionRecord record) {
        Objects.requireNonNull(record, "record");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, record.node().value());
            ps.setString(2, record.state().name());
            ps.setBoolean(3, record.force());
            ps.setString(4, record.detail());
            ps.setLong(5, record.startedAtMillis());
            ps.setLong(6, record.updatedAtMillis());
            if (record.completedAtMillis() != null) {
                ps.setLong(7, record.completedAtMillis());
            } else {
                ps.setNull(7, java.sql.Types.BIGINT);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to upsert decommission record: " + record.node(), e);
        }
    }

    @Override
    public void setState(NodeId node, DecommissionState state, String detail) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(state, "state");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATE)) {
            ps.setString(1, state.name());
            ps.setString(2, detail);
            ps.setLong(3, clock.nowMillis());
            ps.setString(4, node.value());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to update decommission state: " + node, e);
        }
    }

    private static DecommissionRecord map(ResultSet rs) throws SQLException {
        long completedAtRaw = rs.getLong("completed_at");
        Long completedAt = rs.wasNull() ? null : completedAtRaw;
        return new DecommissionRecord(
                NodeId.of(rs.getString("node_id")),
                DecommissionState.valueOf(rs.getString("state")),
                rs.getBoolean("force_flag"),
                rs.getString("detail"),
                rs.getLong("started_at"),
                rs.getLong("updated_at"),
                completedAt);
    }
}
