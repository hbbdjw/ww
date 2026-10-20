package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 基于 JDBC 的 {@link DegradedPartitionStore}（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>建表 DDL 见 {@code sql/rt_degraded.sql}，经 {@link DegradedPartitionSchema#bootstrap(DataSource)}
 * 幂等初始化；{@link #mark} 用 {@code INSERT ... ON CONFLICT DO UPDATE} 实现幂等 upsert。</p>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅验证语义自洽；
 * 真实 PG 行为（upsert 并发、schema 幂等）由集成测试覆盖。</p>
 */
public final class JdbcDegradedPartitionStore implements DegradedPartitionStore {

    private static final String INSERT_SQL = """
            INSERT INTO rt_degraded_partition (capability, partition_id, node_id, since_ms, reason)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (capability, partition_id) DO UPDATE
            SET node_id = EXCLUDED.node_id, since_ms = EXCLUDED.since_ms, reason = EXCLUDED.reason
            """;

    private static final String DELETE_SQL =
            "DELETE FROM rt_degraded_partition WHERE capability = ? AND partition_id = ?";

    private static final String SELECT_PARTITIONS_SQL =
            "SELECT partition_id FROM rt_degraded_partition WHERE capability = ? ORDER BY partition_id";

    private static final String SELECT_LIST_SQL = """
            SELECT capability, partition_id, node_id, since_ms, reason
            FROM rt_degraded_partition WHERE capability = ? ORDER BY partition_id
            """;

    private static final String SELECT_ALL_SQL = """
            SELECT capability, partition_id, node_id, since_ms, reason
            FROM rt_degraded_partition ORDER BY capability, partition_id
            """;

    private static final String SELECT_EXISTS_SQL = """
            SELECT 1 FROM rt_degraded_partition WHERE capability = ? AND partition_id = ?
            """;

    private final DataSource dataSource;

    public JdbcDegradedPartitionStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void mark(String capability, int partition, NodeId owner, String reason) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            ps.setString(3, owner.value());
            ps.setLong(4, System.currentTimeMillis());
            ps.setString(5, reason);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcDegradedPartitionStore.mark failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void clear(String capability, int partition) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcDegradedPartitionStore.clear failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Set<Integer> partitionsOf(String capability) {
        List<Integer> partitions = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PARTITIONS_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getInt(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcDegradedPartitionStore.partitionsOf failed: "
                    + e.getMessage(), e);
        }
        return Set.copyOf(partitions);
    }

    @Override
    public List<DegradedPartition> list(String capability) {
        return queryList(SELECT_LIST_SQL, capability);
    }

    @Override
    public List<DegradedPartition> listAll() {
        return queryList(SELECT_ALL_SQL, null);
    }

    @Override
    public boolean isDegraded(String capability, int partition) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_EXISTS_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcDegradedPartitionStore.isDegraded failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Map<Integer, NodeId> ownersOf(String capability) {
        Map<Integer, NodeId> out = new LinkedHashMap<>();
        for (DegradedPartition record : list(capability)) {
            out.put(record.partition(), record.owner());
        }
        return out;
    }

    private List<DegradedPartition> queryList(String sql, String capability) {
        List<DegradedPartition> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (capability != null) {
                ps.setString(1, capability);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new DegradedPartition(
                            rs.getString("capability"),
                            rs.getInt("partition_id"),
                            NodeId.of(rs.getString("node_id")),
                            rs.getLong("since_ms"),
                            rs.getString("reason")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcDegradedPartitionStore query failed: "
                    + e.getMessage(), e);
        }
        return List.copyOf(out);
    }
}
