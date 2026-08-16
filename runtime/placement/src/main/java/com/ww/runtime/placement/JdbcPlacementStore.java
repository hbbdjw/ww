package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 基于 JDBC 的 {@link PlacementStore} 实现（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>不引入连接池依赖：{@link DataSource} 由宿主注入。建表 DDL 见
 * {@code src/main/resources/sql/rt_placement.sql}，可通过
 * {@link PlacementSchema#bootstrap(DataSource)} 幂等初始化。</p>
 *
 * <p>语义要点：</p>
 * <ul>
 *   <li>replaceAll：单事务内「DELETE 该 capability 全部行 + 批量 INSERT」，
 *       提交前对读者不可见，失败整体回滚（原子整体替换）。</li>
 *   <li>cutover：单条 UPDATE 的条件即 CAS——
 *       {@code WHERE capability=? AND partition_id=? AND epoch=? AND node_id<>?}，
 *       命中 0 行即竞争失败，返回 false。</li>
 * </ul>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅保证该实现编译通过与语义自洽；
 * 针对真实 PostgreSQL 的行为（事务原子性、cutover 并发竞争、Schema 幂等建表等）
 * 由后续基于 Testcontainers/嵌入式 PG 的集成测试覆盖。</p>
 */
public final class JdbcPlacementStore implements PlacementStore {

    private static final String DELETE_SQL =
            "DELETE FROM rt_partition_placement WHERE capability = ?";

    private static final String INSERT_SQL = """
            INSERT INTO rt_partition_placement (capability, partition_id, node_id, role, epoch, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_COLUMNS = "partition_id, node_id, role, epoch";

    private static final String SELECT_ALL_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM rt_partition_placement WHERE capability = ? ORDER BY partition_id";

    private static final String SELECT_ONE_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM rt_partition_placement WHERE capability = ? AND partition_id = ?";

    private static final String CUTOVER_SQL = """
            UPDATE rt_partition_placement
            SET node_id = ?, epoch = epoch + 1, updated_at = ?
            WHERE capability = ? AND partition_id = ? AND epoch = ? AND node_id <> ?
            """;

    private final DataSource dataSource;

    public JdbcPlacementStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void replaceAll(String capability, List<Placement> placements) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(placements, "placements");
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement del = conn.prepareStatement(DELETE_SQL)) {
                    del.setString(1, capability);
                    del.executeUpdate();
                }
                try (PreparedStatement ins = conn.prepareStatement(INSERT_SQL)) {
                    long now = System.currentTimeMillis();
                    for (Placement placement : placements) {
                        Objects.requireNonNull(placement, "placement");
                        ins.setString(1, capability);
                        ins.setInt(2, placement.partition());
                        ins.setString(3, placement.node().value());
                        ins.setString(4, placement.role().name());
                        ins.setLong(5, placement.epoch());
                        ins.setLong(6, now);
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.replaceAll failed for capability " + capability, e);
        }
    }

    @Override
    public List<Placement> list(String capability) {
        Objects.requireNonNull(capability, "capability");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                List<Placement> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.list failed for capability " + capability, e);
        }
    }

    @Override
    public Optional<Placement> get(String capability, int partition) {
        Objects.requireNonNull(capability, "capability");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ONE_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.get failed for capability " + capability + " partition " + partition, e);
        }
    }

    @Override
    public Map<Integer, NodeId> primaries(String capability) {
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        for (Placement placement : list(capability)) {
            if (placement.isPrimary()) {
                primaries.put(placement.partition(), placement.node());
            }
        }
        return primaries;
    }

    @Override
    public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CUTOVER_SQL)) {
            ps.setString(1, newNode.value());
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, capability);
            ps.setInt(4, partition);
            ps.setLong(5, expectedEpoch);
            ps.setString(6, newNode.value());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.cutover failed for capability " + capability
                            + " partition " + partition, e);
        }
    }

    private static Placement mapRow(ResultSet rs) throws SQLException {
        return new Placement(
                rs.getInt("partition_id"),
                NodeId.of(rs.getString("node_id")),
                PlacementRole.valueOf(rs.getString("role")),
                rs.getLong("epoch"));
    }
}
