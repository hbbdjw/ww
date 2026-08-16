package com.ww.runtime.server.log;

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
 * 日志模块数据库 schema 引导：从 classpath 读取 {@code sql/rt_log.sql}，
 * 按分号拆分后逐条执行。
 *
 * <p>DDL 全部使用 {@code CREATE TABLE/INDEX IF NOT EXISTS}，bootstrap 幂等。
 * advisory lock key 用 {@code (4001, 6)}，与 cluster(1)/capability(2)/placement(3)/
 * scheduler(4)/migration(5) 错开。</p>
 */
public final class LogSchema {

    private static final String SQL_RESOURCE = "sql/rt_log.sql";

    private LogSchema() {
    }

    /** 幂等执行 classpath 下的 sql/rt_log.sql。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 跨节点并发 bootstrap 串行化（与 ClusterSchema 同一竞态防护口径）
            stmt.execute("SELECT pg_advisory_lock(4001, 6)");
            try {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 6)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("LogSchema.bootstrap failed: " + e.getMessage(), e);
        }
    }

    private static String readSql() {
        try (InputStream in = LogSchema.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new RuntimeFrameworkException("sql resource not found on classpath: " + SQL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read " + SQL_RESOURCE, e);
        }
    }

    /** 去除 {@code --} 行注释后按分号拆分，忽略空白片段。 */
    private static List<String> splitStatements(String sql) {
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
