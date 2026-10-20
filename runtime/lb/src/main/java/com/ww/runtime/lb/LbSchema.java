package com.ww.runtime.lb;

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
 * LB 模块数据库 schema 引导：从 classpath 读取 {@code sql/rt_lb.sql}，
 * 去除行注释后按分号拆分逐条执行（PostgreSQL 方言）。
 *
 * <p>DDL 使用 {@code CREATE TABLE IF NOT EXISTS}，bootstrap 幂等，可在节点启动路径
 * 安全重复调用；会话级 advisory lock（4007,1）互斥跨节点并发建表（编号避开
 * ClusterSchema 的 4001,1）。</p>
 */
public final class LbSchema {

    private static final String SQL_RESOURCE = "sql/rt_lb.sql";

    private LbSchema() {
    }

    /** 幂等执行 classpath 下的 sql/rt_lb.sql。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_advisory_lock(4007, 1)");
            try {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4007, 1)");
            }
        } catch (SQLException e) {
            throw new LbException("LbSchema.bootstrap failed: " + e.getMessage(), e);
        }
    }

    private static String readSql() {
        try (InputStream in = LbSchema.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new LbException("sql resource not found on classpath: " + SQL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new LbException("failed to read " + SQL_RESOURCE, e);
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
