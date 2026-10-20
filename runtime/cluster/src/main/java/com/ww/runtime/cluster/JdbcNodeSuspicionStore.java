package com.ww.runtime.cluster;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 JDBC 的 {@link NodeSuspicionStore} 实现（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>表 {@code rt_node_suspicion}（复合主键 suspect+observer）DDL 见
 * {@code src/main/resources/sql/rt_cluster.sql}，随 {@link ClusterSchema#bootstrap}
 * 幂等建表。</p>
 *
 * <p>时间戳语义与 {@link JdbcLeaseStore}（S14）一致：生产默认 {@link SystemClock}
 * 走数据库时钟（投票时刻/TTL 截断均以 {@code EXTRACT(EPOCH FROM NOW())} 为准，
 * 应用时钟偏移不影响仲裁）；注入测试时钟时走本地时钟分支。</p>
 *
 * <p>本模块单测无 PostgreSQL 环境，仅保证编译通过与语义自洽；真实 PostgreSQL
 * 行为（upsert 覆盖、GROUP BY 计票、TTL 截断删除）由集成测试覆盖。</p>
 */
public final class JdbcNodeSuspicionStore implements NodeSuspicionStore {

    private static final String VOTE_SQL = """
            INSERT INTO rt_node_suspicion (suspect_node_id, observer_node_id, phi, updated_at_ms)
            VALUES (?, ?, ?, %s)
            ON CONFLICT (suspect_node_id, observer_node_id) DO UPDATE SET
                phi = EXCLUDED.phi,
                updated_at_ms = EXCLUDED.updated_at_ms
            """;

    private static final String RECALL_SQL = """
            DELETE FROM rt_node_suspicion
            WHERE suspect_node_id = ? AND observer_node_id = ?
            """;

    private static final String FRESH_COUNT_SQL = """
            SELECT COUNT(*) FROM rt_node_suspicion
            WHERE suspect_node_id = ? AND updated_at_ms >= %s
            """;

    private static final String FRESH_COUNTS_SQL = """
            SELECT suspect_node_id, COUNT(*) AS cnt FROM rt_node_suspicion
            WHERE updated_at_ms >= %s
            GROUP BY suspect_node_id
            """;

    private static final String CLEAR_SQL =
            "DELETE FROM rt_node_suspicion WHERE suspect_node_id = ?";

    private static final String PRUNE_SQL =
            "DELETE FROM rt_node_suspicion WHERE updated_at_ms < %s";

    private static final String DB_NOW = "(EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT";

    private final DataSource dataSource;
    private final Clock clock;
    private final boolean dbClock;

    public JdbcNodeSuspicionStore(DataSource dataSource) {
        this(dataSource, SystemClock.instance());
    }

    public JdbcNodeSuspicionStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.dbClock = clock instanceof SystemClock;
    }

    @Override
    public void vote(NodeId suspect, NodeId observer, double phi) {
        Objects.requireNonNull(suspect, "suspect");
        Objects.requireNonNull(observer, "observer");
        String sql = VOTE_SQL.formatted(dbClock ? DB_NOW : "?");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, suspect.value());
            ps.setString(2, observer.value());
            ps.setDouble(3, phi);
            if (!dbClock) {
                ps.setLong(4, clock.nowMillis());
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcNodeSuspicionStore.vote failed for " + suspect + " by " + observer, e);
        }
    }

    @Override
    public void recall(NodeId suspect, NodeId observer) {
        Objects.requireNonNull(suspect, "suspect");
        Objects.requireNonNull(observer, "observer");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(RECALL_SQL)) {
            ps.setString(1, suspect.value());
            ps.setString(2, observer.value());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcNodeSuspicionStore.recall failed for " + suspect + " by " + observer, e);
        }
    }

    @Override
    public int freshVoteCount(NodeId suspect, long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        Objects.requireNonNull(suspect, "suspect");
        String sql = FRESH_COUNT_SQL.formatted(dbClock ? DB_NOW + " - ?" : "?");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, suspect.value());
            ps.setLong(2, ttlMillis);
            if (!dbClock) {
                ps.setLong(2, now - ttlMillis);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcNodeSuspicionStore.freshVoteCount failed for " + suspect, e);
        }
    }

    @Override
    public Map<NodeId, Integer> freshVoteCounts(long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        String sql = FRESH_COUNTS_SQL.formatted(dbClock ? DB_NOW + " - ?" : "?");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, dbClock ? ttlMillis : now - ttlMillis);
            Map<NodeId, Integer> result = new LinkedHashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(NodeId.of(rs.getString(1)), rs.getInt(2));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeSuspicionStore.freshVoteCounts failed", e);
        }
    }

    @Override
    public void clear(NodeId suspect) {
        Objects.requireNonNull(suspect, "suspect");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CLEAR_SQL)) {
            ps.setString(1, suspect.value());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcNodeSuspicionStore.clear failed for " + suspect, e);
        }
    }

    @Override
    public void pruneExpired(long now, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be >= 0, got " + ttlMillis);
        }
        String sql = PRUNE_SQL.formatted(dbClock ? DB_NOW + " - ?" : "?");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, dbClock ? ttlMillis : now - ttlMillis);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcNodeSuspicionStore.pruneExpired failed", e);
        }
    }
}
