package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 基于 JDBC 的 {@link NodeRegistry} 实现（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>不引入连接池依赖：{@link DataSource} 由宿主注入（如 HikariCP / 容器数据源）。
 * 建表 DDL 见 {@code src/main/resources/sql/rt_cluster.sql}，可通过
 * {@link ClusterSchema#bootstrap(DataSource)} 幂等初始化。</p>
 *
 * <p>语义要点：</p>
 * <ul>
 *   <li>register：{@code INSERT ... ON CONFLICT (node_id) DO UPDATE} upsert，
 *       强制 state='JOINING'、state_epoch=0；registered_at 取数据库时钟（EXTRACT(EPOCH FROM NOW())）。</li>
 *   <li>heartbeat：按 node_id 更新资源列与 heartbeat_at/resources_at（取快照的 updatedAtMillis），
 *       未知节点（0 行命中）抛 IllegalArgumentException。</li>
 *   <li>markState：先经 {@link NodeStateMachine#check} 校验，再以
 *       {@code WHERE state = ? AND state_epoch = ?} 乐观锁更新；命中 0 行说明被并发修改，重读重试。</li>
 *   <li>tombstone：{@code WHERE state <> 'DEAD'} 条件更新，幂等且保留行。</li>
 * </ul>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅保证该实现编译通过与语义自洽；
 * 针对真实 PostgreSQL 的行为（upsert 冲突合并、乐观锁并发竞争、tombstone 幂等、
 * ClusterSchema 幂等建表等）由后续基于 Testcontainers/嵌入式 PG 的集成测试模块覆盖。</p>
 */
public final class JdbcNodeRegistry implements NodeRegistry {

    /** markState 乐观锁冲突时的最大重试次数。 */
    private static final int MAX_MARK_STATE_ATTEMPTS = 8;

    private static final String SELECT_COLUMNS =
            "node_id, host, grpc_port, state, state_epoch, weight, "
                    + "cpu_percent, mem_total_bytes, mem_used_bytes, disk_total_bytes, disk_used_bytes, "
                    + "resources_at, registered_at, heartbeat_at";

    private static final String UPSERT_SQL = """
            INSERT INTO rt_node (node_id, host, grpc_port, state, state_epoch, weight,
                                 cpu_percent, mem_total_bytes, mem_used_bytes, disk_total_bytes, disk_used_bytes,
                                 resources_at, registered_at, heartbeat_at)
            VALUES (?, ?, ?, 'JOINING', 0, ?, ?, ?, ?, ?, ?, ?,
                    (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, ?)
            ON CONFLICT (node_id) DO UPDATE SET
                host = EXCLUDED.host,
                grpc_port = EXCLUDED.grpc_port,
                state = 'JOINING',
                state_epoch = 0,
                weight = EXCLUDED.weight,
                cpu_percent = EXCLUDED.cpu_percent,
                mem_total_bytes = EXCLUDED.mem_total_bytes,
                mem_used_bytes = EXCLUDED.mem_used_bytes,
                disk_total_bytes = EXCLUDED.disk_total_bytes,
                disk_used_bytes = EXCLUDED.disk_used_bytes,
                resources_at = EXCLUDED.resources_at,
                registered_at = (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
                heartbeat_at = EXCLUDED.heartbeat_at
            """;

    private static final String HEARTBEAT_SQL = """
            UPDATE rt_node
            SET cpu_percent = ?, mem_total_bytes = ?, mem_used_bytes = ?,
                disk_total_bytes = ?, disk_used_bytes = ?, resources_at = ?, heartbeat_at = ?
            WHERE node_id = ?
            """;

    private static final String MARK_STATE_SQL = """
            UPDATE rt_node
            SET state = ?, state_epoch = state_epoch + 1
            WHERE node_id = ? AND state = ? AND state_epoch = ?
            """;

    private static final String TOMBSTONE_SQL = """
            UPDATE rt_node
            SET state = 'DEAD', state_epoch = state_epoch + 1
            WHERE node_id = ? AND state <> 'DEAD'
            """;

    private static final String SELECT_BY_ID_SQL = "SELECT " + SELECT_COLUMNS + " FROM rt_node WHERE node_id = ?";

    private static final String SELECT_ALL_SQL = "SELECT " + SELECT_COLUMNS + " FROM rt_node ORDER BY node_id";

    private final DataSource dataSource;

    public JdbcNodeRegistry(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void register(NodeInfo info) {
        Objects.requireNonNull(info, "info");
        ResourceSnapshot snapshot = info.resources();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, info.id().value());
            ps.setString(2, info.host());
            ps.setInt(3, info.grpcPort());
            ps.setDouble(4, info.weight());
            ps.setDouble(5, snapshot.cpuPercent());
            ps.setLong(6, snapshot.memTotalBytes());
            ps.setLong(7, snapshot.memUsedBytes());
            ps.setLong(8, snapshot.diskTotalBytes());
            ps.setLong(9, snapshot.diskUsedBytes());
            ps.setLong(10, snapshot.updatedAtMillis());
            ps.setLong(11, snapshot.updatedAtMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.register failed for " + info.id(), e);
        }
    }

    @Override
    public void heartbeat(NodeId id, ResourceSnapshot snapshot) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(snapshot, "snapshot");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(HEARTBEAT_SQL)) {
            ps.setDouble(1, snapshot.cpuPercent());
            ps.setLong(2, snapshot.memTotalBytes());
            ps.setLong(3, snapshot.memUsedBytes());
            ps.setLong(4, snapshot.diskTotalBytes());
            ps.setLong(5, snapshot.diskUsedBytes());
            ps.setLong(6, snapshot.updatedAtMillis());
            ps.setLong(7, snapshot.updatedAtMillis());
            ps.setString(8, id.value());
            if (ps.executeUpdate() == 0) {
                throw unknownNode(id);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.heartbeat failed for " + id, e);
        }
    }

    @Override
    public void markState(NodeId id, NodeState newState) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(newState, "newState");
        for (int attempt = 0; attempt < MAX_MARK_STATE_ATTEMPTS; attempt++) {
            NodeInfo current = get(id).orElseThrow(() -> unknownNode(id));
            NodeStateMachine.check(current.state(), newState);
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(MARK_STATE_SQL)) {
                ps.setString(1, newState.name());
                ps.setString(2, id.value());
                ps.setString(3, current.state().name());
                ps.setLong(4, current.stateEpoch());
                if (ps.executeUpdate() == 1) {
                    return;
                }
            } catch (SQLException e) {
                throw new RuntimeFrameworkException("JdbcNodeRegistry.markState failed for " + id, e);
            }
            // 0 行命中：状态/纪元被并发修改，重读后重试
        }
        throw new RuntimeFrameworkException("JdbcNodeRegistry.markState(" + id + " -> " + newState
                + ") lost optimistic-update race " + MAX_MARK_STATE_ATTEMPTS + " times");
    }

    @Override
    public Optional<NodeInfo> get(NodeId id) {
        Objects.requireNonNull(id, "id");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID_SQL)) {
            ps.setString(1, id.value());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.get failed for " + id, e);
        }
    }

    @Override
    public List<NodeInfo> listAlive() {
        return listAll().stream()
                .filter(info -> info.state() != NodeState.DEAD && info.state() != NodeState.LEAVING)
                .toList();
    }

    @Override
    public List<NodeInfo> listAll() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            List<NodeInfo> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapRow(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.listAll failed", e);
        }
    }

    @Override
    public void tombstone(NodeId id) {
        Objects.requireNonNull(id, "id");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(TOMBSTONE_SQL)) {
            ps.setString(1, id.value());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.tombstone failed for " + id, e);
        }
    }

    private static NodeInfo mapRow(ResultSet rs) throws SQLException {
        Double cpuPercent = (Double) rs.getObject("cpu_percent");
        long updatedAt = rs.getLong("resources_at");
        if (rs.wasNull()) {
            updatedAt = rs.getLong("registered_at");
        }
        ResourceSnapshot snapshot = ResourceSnapshot.of(
                cpuPercent == null ? 0.0 : cpuPercent,
                max0(rs.getLong("mem_total_bytes"), rs),
                max0(rs.getLong("mem_used_bytes"), rs),
                max0(rs.getLong("disk_total_bytes"), rs),
                max0(rs.getLong("disk_used_bytes"), rs),
                Math.max(0L, updatedAt));
        return NodeInfo.of(
                NodeId.of(rs.getString("node_id")),
                rs.getString("host"),
                rs.getInt("grpc_port"),
                NodeState.valueOf(rs.getString("state")),
                rs.getLong("state_epoch"),
                rs.getDouble("weight"),
                snapshot);
    }

    private static long max0(long value, ResultSet rs) throws SQLException {
        return rs.wasNull() ? 0L : value;
    }

    private static IllegalArgumentException unknownNode(NodeId id) {
        return new IllegalArgumentException("unknown node: " + id);
    }
}
