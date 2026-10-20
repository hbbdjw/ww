package com.ww.runtime.migration;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.SystemClock;

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
 * 基于 JDBC 的 {@link MigrationTaskStore} 实现（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>不引入连接池依赖：{@link DataSource} 由宿主注入。建表 DDL 见
 * {@code src/main/resources/sql/rt_migration.sql}，可通过
 * {@link MigrationSchema#bootstrap(DataSource)} 幂等初始化。</p>
 *
 * <p>语义要点：</p>
 * <ul>
 *   <li>create：{@code INSERT ... RETURNING task_id} 取回 BIGSERIAL 自增主键；</li>
 *   <li>transition：单条 {@code UPDATE ... WHERE task_id=? AND state=?} 的条件即 CAS，
 *       命中 0 行（并发修改）抛 {@link MigrationException}；跃迁合法性先经状态机校验；</li>
 *   <li>saveProgress：{@code checkpoint_json = COALESCE(?, checkpoint_json)}——传 null 保留原值。</li>
 * </ul>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅验证异常包装语义；
 * 真实数据库行为由集成测试覆盖。</p>
 */
public final class JdbcMigrationTaskStore implements MigrationTaskStore {

    private static final String INSERT_SQL = """
            INSERT INTO rt_migration_task (capability, partition_id, from_node, to_node, state, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'PENDING', ?, ?)
            RETURNING task_id
            """;

    private static final String SELECT_COLUMNS =
            "task_id, capability, partition_id, from_node, to_node, state, checkpoint_offset, items_done, checkpoint_json, updated_at";

    private static final String SELECT_ONE_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM rt_migration_task WHERE task_id = ?";

    private static final String SELECT_ACTIVE_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM rt_migration_task"
                    + " WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED') ORDER BY task_id";

    private static final String SELECT_ALL_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM rt_migration_task ORDER BY task_id";

    private static final String TRANSITION_SQL = """
            UPDATE rt_migration_task
            SET state = ?, updated_at = ?
            WHERE task_id = ? AND state = ?
            """;

    private static final String SAVE_PROGRESS_SQL = """
            UPDATE rt_migration_task
            SET checkpoint_offset = ?, items_done = ?, checkpoint_json = COALESCE(?, checkpoint_json), updated_at = ?
            WHERE task_id = ?
            """;

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcMigrationTaskStore(DataSource dataSource) {
        this(dataSource, SystemClock.instance());
    }

    public JdbcMigrationTaskStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public long create(String capability, int partition, NodeId from, NodeId to) {
        if (capability == null || capability.isBlank()) {
            throw new IllegalArgumentException("capability must not be null or blank");
        }
        Objects.requireNonNull(to, "to");
        long now = clock.nowMillis();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            ps.setString(3, from == null ? null : from.value());
            ps.setString(4, to.value());
            ps.setLong(5, now);
            ps.setLong(6, now);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new MigrationException("create failed: no generated task_id returned");
                }
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new MigrationException("JdbcMigrationTaskStore.create failed for capability "
                    + capability + " partition " + partition, e);
        }
    }

    @Override
    public Optional<MigrationTask> get(long taskId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ONE_SQL)) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new MigrationException("JdbcMigrationTaskStore.get failed for task " + taskId, e);
        }
    }

    @Override
    public List<MigrationTask> listActive() {
        return queryList(SELECT_ACTIVE_SQL);
    }

    @Override
    public List<MigrationTask> listAll() {
        return queryList(SELECT_ALL_SQL);
    }

    @Override
    public boolean transition(long taskId, MigrationTaskState newState) {
        Objects.requireNonNull(newState, "newState");
        MigrationTask current = get(taskId).orElseThrow(() ->
                new MigrationException("migration task not found: " + taskId));
        MigrationStateMachine.check(current.state(), newState);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(TRANSITION_SQL)) {
            ps.setString(1, newState.name());
            ps.setLong(2, clock.nowMillis());
            ps.setLong(3, taskId);
            ps.setString(4, current.state().name());
            if (ps.executeUpdate() == 0) {
                throw new MigrationException(
                        "transition CAS failed for task " + taskId + ": state changed concurrently");
            }
            return true;
        } catch (SQLException e) {
            throw new MigrationException(
                    "JdbcMigrationTaskStore.transition failed for task " + taskId, e);
        }
    }

    @Override
    public void saveProgress(long taskId, long checkpointOffset, long itemsDone, String checkpointJson) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SAVE_PROGRESS_SQL)) {
            ps.setLong(1, checkpointOffset);
            ps.setLong(2, itemsDone);
            ps.setString(3, checkpointJson);
            ps.setLong(4, clock.nowMillis());
            ps.setLong(5, taskId);
            if (ps.executeUpdate() == 0) {
                throw new MigrationException("migration task not found: " + taskId);
            }
        } catch (SQLException e) {
            throw new MigrationException(
                    "JdbcMigrationTaskStore.saveProgress failed for task " + taskId, e);
        }
    }

    private List<MigrationTask> queryList(String sql) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<MigrationTask> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapRow(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new MigrationException("JdbcMigrationTaskStore query failed: " + e.getMessage(), e);
        }
    }

    private static MigrationTask mapRow(ResultSet rs) throws SQLException {
        String fromNode = rs.getString("from_node");
        return new MigrationTask(
                rs.getLong("task_id"),
                rs.getString("capability"),
                rs.getInt("partition_id"),
                fromNode == null ? null : NodeId.of(fromNode),
                NodeId.of(rs.getString("to_node")),
                MigrationTaskState.valueOf(rs.getString("state")),
                rs.getLong("checkpoint_offset"),
                rs.getLong("items_done"),
                rs.getString("checkpoint_json"),
                rs.getLong("updated_at"));
    }
}
