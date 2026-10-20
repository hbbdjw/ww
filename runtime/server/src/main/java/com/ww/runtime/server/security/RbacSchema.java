package com.ww.runtime.server.security;

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
 * RBAC 权限体系 schema 引导：从 classpath 读取 {@code sql/sys_rbac.sql}，
 * 按分号拆分后逐条执行。
 *
 * <p>DDL 全部使用 {@code CREATE TABLE IF NOT EXISTS}，bootstrap 幂等。
 * advisory lock key 用 {@code (4001, 7)}，与 cluster(1)/capability(2)/placement(3)/
 * scheduler(4)/migration(5)/log(6) 错开。</p>
 */
public final class RbacSchema {

    private static final String SQL_RESOURCE = "sql/sys_rbac.sql";

    private RbacSchema() {
    }

    /** 幂等执行 classpath 下的 sql/sys_rbac.sql。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 跨节点并发 bootstrap 串行化（与 LogSchema 同一竞态防护口径）
            stmt.execute("SELECT pg_advisory_lock(4001, 7)");
            try {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 7)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("RbacSchema.bootstrap failed: " + e.getMessage(), e);
        }
    }

    /**
     * 在跨节点互斥锁内执行 RBAC 播种。
     *
     * <p>必要性：多节点同时首启时，各节点独立「判空 → 插入」会并发写入 admin 用户与
     * 超管角色，撞上 {@code uk_sys_user_username} / {@code uk_sys_role_key} 唯一约束，
     * 使其中一个节点的 bootstrap 抛异常并启动失败。锁内执行后，后到节点在播种内部的
     * 二次判空即发现已有用户并跳过。</p>
     *
     * <p>advisory lock key 用 {@code (4001, 8)}，与 DDL 引导的 {@code (4001, 7)} 错开
     * （二者虽在同一节点内顺序执行不会互等，但分键可避免任何嵌套获取的歧义）。</p>
     *
     * @param seeder 播种动作（内部须自行判空，保证锁内二次检查生效）
     */
    public static void seedExclusively(DataSource dataSource, Runnable seeder) {
        Objects.requireNonNull(dataSource, "dataSource");
        Objects.requireNonNull(seeder, "seeder");
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_advisory_lock(4001, 8)");
            try {
                seeder.run();
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 8)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("RbacSchema.seedExclusively failed: " + e.getMessage(), e);
        }
    }

    private static String readSql() {
        try (InputStream in = RbacSchema.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
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
