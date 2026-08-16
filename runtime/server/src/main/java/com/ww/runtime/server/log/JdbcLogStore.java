package com.ww.runtime.server.log;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.log.LogRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 日志仓库（PostgreSQL 方言，plain SQL，无 ORM）：批量写入 + 分页查询。
 *
 * <p>建表 DDL 见 {@code sql/rt_log.sql}，经 {@link LogSchema#bootstrap(DataSource)} 幂等初始化。
 * keyword 过滤与实时总线同语义：忽略大小写匹配 message/logger_name/node/stack_trace 四字段。</p>
 */
public final class JdbcLogStore {

    private static final String INSERT_SQL = """
            INSERT INTO rt_log (level, logger_name, thread_name, message, stack_trace, node, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_COLUMNS =
            "id, level, logger_name, thread_name, message, stack_trace, node, created_at";

    private final DataSource dataSource;

    public JdbcLogStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** 批量写入（单连接 + addBatch/executeBatch）。 */
    public void batchInsert(List<LogRecord> records) {
        Objects.requireNonNull(records, "records");
        if (records.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            for (LogRecord r : records) {
                ps.setString(1, r.level());
                ps.setString(2, r.loggerName());
                ps.setString(3, r.threadName());
                ps.setString(4, r.message());
                ps.setString(5, r.stackTrace());
                ps.setString(6, r.node());
                ps.setLong(7, r.timestamp());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("rt_log batch insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * 分页查询（按 created_at DESC + id DESC 稳定排序）。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空，四字段 ILIKE OR）
     * @param offset  偏移（>=0）
     * @param limit   页大小（>=1）
     */
    public List<LogRecord> page(String level, String node, String keyword, int offset, int limit) {
        StringBuilder sql = new StringBuilder("SELECT ").append(SELECT_COLUMNS)
                .append(" FROM rt_log");
        List<Object> params = appendWhere(sql, level, node, keyword);
        sql.append(" ORDER BY created_at DESC, id DESC OFFSET ? LIMIT ?");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : params) {
                ps.setObject(idx++, param);
            }
            ps.setInt(idx++, Math.max(0, offset));
            ps.setInt(idx, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                List<LogRecord> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new LogRecord(
                            rs.getString("level"),
                            rs.getString("logger_name"),
                            rs.getString("thread_name"),
                            rs.getString("message"),
                            rs.getString("stack_trace"),
                            rs.getString("node"),
                            rs.getLong("created_at")));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("rt_log page query failed: " + e.getMessage(), e);
        }
    }

    /** 条件计数（与 {@link #page} 同口径）。 */
    public long count(String level, String node, String keyword) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM rt_log");
        List<Object> params = appendWhere(sql, level, node, keyword);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : params) {
                ps.setObject(idx++, param);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("rt_log count query failed: " + e.getMessage(), e);
        }
    }

    /** 组装 WHERE 子句并返回参数列表（ILIKE 用 lower() 归一，避免依赖 pg_trgm）。 */
    private static List<Object> appendWhere(StringBuilder sql, String level, String node, String keyword) {
        List<Object> params = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
        if (level != null && !level.isBlank()) {
            conditions.add("LOWER(level) = ?");
            params.add(level.trim().toLowerCase(Locale.ROOT));
        }
        if (node != null && !node.isBlank()) {
            conditions.add("node = ?");
            params.add(node.trim());
        }
        if (keyword != null && !keyword.isBlank()) {
            String like = "%" + keyword.trim().toLowerCase(Locale.ROOT) + "%";
            conditions.add("(LOWER(message) LIKE ? OR LOWER(logger_name) LIKE ?"
                    + " OR LOWER(node) LIKE ? OR LOWER(stack_trace) LIKE ?)");
            params.add(like);
            params.add(like);
            params.add(like);
            params.add(like);
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        return params;
    }
}
