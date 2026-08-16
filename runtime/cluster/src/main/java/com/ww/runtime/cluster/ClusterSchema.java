package com.ww.runtime.cluster;

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
 * 集群模块数据库 schema 引导：从 classpath 读取 {@code sql/rt_cluster.sql}，
 * 按分号拆分后逐条执行。
 *
 * <p>DDL 全部使用 {@code CREATE TABLE IF NOT EXISTS}，因此 bootstrap 是幂等的，
 * 可在节点启动路径上安全重复调用（PostgreSQL 方言）。</p>
 */
public final class ClusterSchema {

    private static final String SQL_RESOURCE = "sql/rt_cluster.sql";

    private ClusterSchema() {
    }

    /** 幂等执行 classpath 下的 sql/rt_cluster.sql。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 跨节点并发 bootstrap 串行化：CREATE TABLE IF NOT EXISTS 在 PG 下存在
            // 两个事务同时通过存在性检查、随后在 pg_type 插入上撞唯一键的竞态
            // （3 节点场景暴露），用会话级 advisory lock 互斥（连接关闭兜底释放）。
            stmt.execute("SELECT pg_advisory_lock(4001, 1)");
            try {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 1)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("ClusterSchema.bootstrap failed: " + e.getMessage(), e);
        }
    }

    private static String readSql() {
        try (InputStream in = ClusterSchema.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new RuntimeFrameworkException("sql resource not found on classpath: " + SQL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read " + SQL_RESOURCE, e);
        }
    }

    /**
     * 去除 {@code --} 行注释后按分号拆分，忽略空白片段。
     * 本 DDL 不含字符串字面量中的分号，简单拆分即可满足。
     */
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
