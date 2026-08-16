package com.ww.runtime.capability;

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

/**
 * 能力模块数据库 schema 引导：执行 classpath 下 {@code sql/rt_capability.sql}
 * 的 DDL（CREATE TABLE IF NOT EXISTS，幂等，可重复执行）。
 *
 * <p>用法：{@code CapabilitySchema.bootstrap(dataSource);}（通常在宿主启动时执行一次）。
 */
public final class CapabilitySchema {

    private static final String DDL_RESOURCE = "/sql/rt_capability.sql";

    private CapabilitySchema() {
    }

    /**
     * 在给定数据源上执行能力状态表 DDL（幂等）。
     *
     * @throws RuntimeFrameworkException DDL 资源缺失或执行失败
     */
    public static void bootstrap(DataSource dataSource) {
        String ddl = readDdl();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // 跨节点并发 bootstrap 串行化：CREATE TABLE IF NOT EXISTS 在 PG 下存在
            // 并发竞态（pg_type 唯一键冲突），用会话级 advisory lock 互斥。
            statement.execute("SELECT pg_advisory_lock(4001, 2)");
            try {
                for (String sql : splitStatements(ddl)) {
                    statement.execute(sql);
                }
            } finally {
                statement.execute("SELECT pg_advisory_unlock(4001, 2)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to bootstrap rt_capability schema", e);
        }
    }

    /**
     * 去除 {@code --} 行注释后按分号拆分，忽略空白片段（与 ClusterSchema 同口径）。
     * 修复说明：原先直接按分号拆分并跳过以 "--" 开头的片段——而 rt_capability.sql
     * 首行即行注释，唯一语句片段整体被跳过，真实 PG 上建不出 rt_capability_state
     * （Testcontainers 冒烟 IT 暴露）。
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

    private static String readDdl() {
        try (InputStream in = CapabilitySchema.class.getResourceAsStream(DDL_RESOURCE)) {
            if (in == null) {
                throw new RuntimeFrameworkException("ddl resource not found: " + DDL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read ddl resource: " + DDL_RESOURCE, e);
        }
    }
}
