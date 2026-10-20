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
import java.util.Set;

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
 *   <li>markState（S9 单条 UPDATE）：合法性下推 {@code WHERE state<>target AND state IN(合法前驱)}，
 *       命中 1 行即完成；0 行回读归类——同态保持非法语义抛
 *       {@code IllegalStateTransitionException}，合法前驱被并发抢先则重试。</li>
 *   <li>tombstone：{@code WHERE state <> 'DEAD'} 条件更新，幂等且保留行。</li>
 *   <li>sweepStaleJoining（§3.12③）：数据库时钟判定，超时 JOINING 孤儿置 DEAD。</li>
 * </ul>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅保证该实现编译通过与语义自洽；
 * 针对真实 PostgreSQL 的行为（upsert 冲突合并、乐观锁并发竞争、tombstone 幂等、
 * ClusterSchema 幂等建表等）由后续基于 Testcontainers/嵌入式 PG 的集成测试模块覆盖。</p>
 */
public final class JdbcNodeRegistry implements NodeRegistry {

    /** markState 乐观锁冲突时的最大重试次数。 */
    private static final int MAX_MARK_STATE_ATTEMPTS = 8;

    /** S9：目标态 → 预构建 markState SQL 缓存（枚举有限集，无失效问题）。 */
    private static final java.util.EnumMap<NodeState, String> MARK_STATE_SQL_BY_TARGET =
            new java.util.EnumMap<>(NodeState.class);

    private static final String SELECT_COLUMNS =
            "node_id, host, grpc_port, http_port, state, state_epoch, weight, zone, "
                    + "cpu_percent, mem_total_bytes, mem_used_bytes, disk_total_bytes, disk_used_bytes, "
                    + "resources_at, registered_at, heartbeat_at";

    private static final String UPSERT_SQL = """
            INSERT INTO rt_node (node_id, host, grpc_port, http_port, state, state_epoch, weight,
                                 cpu_percent, mem_total_bytes, mem_used_bytes, disk_total_bytes, disk_used_bytes,
                                 resources_at, registered_at, heartbeat_at, zone)
            VALUES (?, ?, ?, ?, 'JOINING', 0, ?, ?, ?, ?, ?, ?, ?,
                    (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, ?, ?)
            ON CONFLICT (node_id) DO UPDATE SET
                host = EXCLUDED.host,
                grpc_port = EXCLUDED.grpc_port,
                http_port = EXCLUDED.http_port,
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
                heartbeat_at = EXCLUDED.heartbeat_at,
                zone = EXCLUDED.zone
            """;

    private static final String HEARTBEAT_SQL = """
            UPDATE rt_node
            SET cpu_percent = ?, mem_total_bytes = ?, mem_used_bytes = ?,
                disk_total_bytes = ?, disk_used_bytes = ?, resources_at = ?, heartbeat_at = ?
            WHERE node_id = ?
            """;

    /**
     * S9：markState 合法跃迁的单条条件 UPDATE——用「目标态合法前驱集合 + state&lt;&gt;target」
     * 把合法性校验下推进 WHERE，常规路径单次往返（旧实现先 SELECT 再更新，
     * 状态风暴时 DB 读放大 2N）；仅 0 行命中时回读一次做错误归类/重试。
     */
    private static final String MARK_STATE_SQL_TEMPLATE = """
            UPDATE rt_node
            SET state = ?, state_epoch = state_epoch + 1
            WHERE node_id = ? AND state <> ? AND state IN (%s)
            """;

    private static final String TOMBSTONE_SQL = """
            UPDATE rt_node
            SET state = 'DEAD', state_epoch = state_epoch + 1
            WHERE node_id = ? AND state <> 'DEAD'
            """;

    private static final String SELECT_BY_ID_SQL = "SELECT " + SELECT_COLUMNS + " FROM rt_node WHERE node_id = ?";

    private static final String SELECT_ALL_SQL = "SELECT " + SELECT_COLUMNS + " FROM rt_node ORDER BY node_id";

    private static final String SELECT_ALIVE_SQL = "SELECT " + SELECT_COLUMNS
            + " FROM rt_node WHERE state NOT IN ('DEAD', 'LEAVING') ORDER BY node_id";

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
            ps.setInt(4, info.httpPort());
            ps.setDouble(5, info.weight());
            ps.setDouble(6, snapshot.cpuPercent());
            ps.setLong(7, snapshot.memTotalBytes());
            ps.setLong(8, snapshot.memUsedBytes());
            ps.setLong(9, snapshot.diskTotalBytes());
            ps.setLong(10, snapshot.diskUsedBytes());
            ps.setLong(11, snapshot.updatedAtMillis());
            ps.setLong(12, snapshot.updatedAtMillis());
            ps.setString(13, info.zone());
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
        String sql = MARK_STATE_SQL_BY_TARGET.computeIfAbsent(newState, JdbcNodeRegistry::buildMarkStateSql);
        for (int attempt = 0; attempt < MAX_MARK_STATE_ATTEMPTS; attempt++) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, newState.name());
                ps.setString(2, id.value());
                ps.setString(3, newState.name());
                if (ps.executeUpdate() == 1) {
                    return;
                }
            } catch (SQLException e) {
                throw new RuntimeFrameworkException("JdbcNodeRegistry.markState failed for " + id, e);
            }
            // S9：0 行命中——回读一次归类：同态（保持状态机同态非法语义）/非法前驱/并发竞态
            NodeInfo current = get(id).orElseThrow(() -> unknownNode(id));
            if (current.state() == newState) {
                throw new com.ww.runtime.common.IllegalStateTransitionException(current.state(), newState);
            }
            NodeStateMachine.check(current.state(), newState); // 非法即抛；合法=并发抢先，重试
        }
        throw new RuntimeFrameworkException("JdbcNodeRegistry.markState(" + id + " -> " + newState
                + ") lost update race " + MAX_MARK_STATE_ATTEMPTS + " times");
    }

    /** S9：按目标态生成合法前驱条件 SQL（枚举名内联，无注入面）。 */
    private static String buildMarkStateSql(NodeState target) {
        Set<NodeState> preds = NodeStateMachine.legalPredecessors(target);
        String placeholders = preds.stream()
                .map(state -> "'" + state.name() + "'")
                .collect(java.util.stream.Collectors.joining(", "));
        return MARK_STATE_SQL_TEMPLATE.formatted(placeholders);
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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALIVE_SQL);
             ResultSet rs = ps.executeQuery()) {
            List<NodeInfo> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapRow(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeRegistry.listAlive failed", e);
        }
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
                rs.getInt("http_port"),
                NodeState.valueOf(rs.getString("state")),
                rs.getLong("state_epoch"),
                rs.getDouble("weight"),
                snapshot,
                rs.getString("zone"));
    }

    private static long max0(long value, ResultSet rs) throws SQLException {
        return rs.wasNull() ? 0L : value;
    }

    private static IllegalArgumentException unknownNode(NodeId id) {
        return new IllegalArgumentException("unknown node: " + id);
    }
}
