package com.ww.runtime.scheduler;

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
import java.util.stream.Collectors;

/**
 * 调度器数据库 schema 引导：执行 classpath 下 {@code sql/rt_scheduler.sql}
 * 的 DDL（CREATE TABLE IF NOT EXISTS，幂等，可重复执行）。
 *
 * <p>用法：{@code SchedulerSchema.bootstrap(dataSource);}（通常在宿主启动时执行一次，
 * 随后即可构造 {@link JdbcLeaseStore}）。</p>
 */
public final class SchedulerSchema {

    private static final String DDL_RESOURCE = "/sql/rt_scheduler.sql";

    private SchedulerSchema() {
    }

    /**
     * 在给定数据源上执行调度器租约表 DDL（幂等）。
     *
     * @throws RuntimeFrameworkException DDL 资源缺失或执行失败
     */
    public static void bootstrap(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        List<String> statements = splitStatements(readDdl());
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // 跨节点并发 bootstrap 串行化：CREATE TABLE IF NOT EXISTS 在 PG 下存在
            // 并发竞态（pg_type 唯一键冲突），用会话级 advisory lock 互斥。
            statement.execute("SELECT pg_advisory_lock(4001, 4)");
            try {
                for (String sql : statements) {
                    statement.execute(sql);
                }
            } finally {
                statement.execute("SELECT pg_advisory_unlock(4001, 4)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to bootstrap rt_scheduler schema", e);
        }
    }

    /**
     * 按 ';' 切分 DDL 并剔除整行注释（-- 开头的行），避免带注释头的语句块被跳过。
     */
    private static List<String> splitStatements(String ddl) {
        List<String> statements = new ArrayList<>();
        for (String chunk : ddl.split(";")) {
            String stripped = chunk.lines()
                    .filter(line -> !line.strip().startsWith("--"))
                    .collect(Collectors.joining("\n"))
                    .strip();
            if (!stripped.isEmpty()) {
                statements.add(stripped);
            }
        }
        return statements;
    }

    private static String readDdl() {
        try (InputStream in = SchedulerSchema.class.getResourceAsStream(DDL_RESOURCE)) {
            if (in == null) {
                throw new RuntimeFrameworkException("ddl resource not found: " + DDL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeFrameworkException("failed to read ddl resource: " + DDL_RESOURCE, e);
        }
    }
}
