package com.ww.runtime.placement;

import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 分区隔离注册表 schema 引导：从 classpath 读取 {@code sql/rt_degraded.sql}，
 * 去除行注释后按分号拆分逐条执行（PostgreSQL 方言）。
 *
 * <p>DDL 使用 {@code CREATE TABLE IF NOT EXISTS}，bootstrap 幂等，可在节点启动路径
 * 安全重复调用；会话级 advisory lock（4001,9）互斥跨节点并发建表（编号续接
 * PlacementSchema 的 4001,3 家族，避开 LbSchema 的 4007,1）。</p>
 */
public final class DegradedPartitionSchema {

    private static final String SQL_RESOURCE = "sql/rt_degraded.sql";

    private DegradedPartitionSchema() {
    }

    /** 幂等执行 classpath 下的 sql/rt_degraded.sql。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_advisory_lock(4001, 9)");
            try {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 9)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("DegradedPartitionSchema.bootstrap failed: "
                    + e.getMessage(), e);
        }
    }

    private static String readSql() {
        try (InputStream in = DegradedPartitionSchema.class.getClassLoader()
                .getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new RuntimeFrameworkException("sql resource not found on classpath: " + SQL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read " + SQL_RESOURCE, e);
        }
    }

    /** 去除 -- 行注释后按分号拆分，忽略空白片段（本 DDL 无字符串内分号）。 */
    static List<String> splitStatements(String sql) {
        StringBuilder noComments = new StringBuilder(sql.length());
        for (String line : sql.split("\\R")) {
            int commentIdx = line.indexOf("--");
            noComments.append(commentIdx >= 0 ? line.substring(0, commentIdx) : line).append('\n');
        }
        List<String> statements = new ArrayList<>();
        for (String statement : noComments.toString().split(";")) {
            if (!statement.isBlank()) {
                statements.add(statement.trim());
            }
        }
        return statements;
    }
}
