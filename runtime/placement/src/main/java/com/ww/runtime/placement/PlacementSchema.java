package com.ww.runtime.placement;

import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 放置模块数据库 schema 引导：从 classpath 读取 {@code sql/rt_placement.sql}，
 * 按分号拆分后逐条执行。
 *
 * <p>DDL 全部使用 {@code CREATE TABLE IF NOT EXISTS}，因此 bootstrap 是幂等的，
 * 可在节点启动路径上安全重复调用（PostgreSQL 方言）。</p>
 *
 * <p><b>v2 主键迁移：</b>放置表主键从 {@code (capability, partition_id)} 扩展为
 * {@code (capability, partition_id, node_id)}（每分区多行：[0]=PRIMARY 其余 REPLICA）。
 * bootstrap 检测旧主键表并 DROP 重建——放置为派生状态（可由 desired+alive 全量
 * 重算），不做数据搬迁；重建期间该能力短暂无 owner，写入被 fencing 拒绝
 * （安全侧失败），leader 调度器下周期自动 replaceAll 重新引导。</p>
 */
public final class PlacementSchema {

    private static final String SQL_RESOURCE = "sql/rt_placement.sql";

    private static final String TABLE = "rt_partition_placement";

    private PlacementSchema() {
    }

    /** 幂等执行 classpath 下的 sql/rt_placement.sql（含旧主键表检测重建）。 */
    public static void bootstrap(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        List<String> statements = splitStatements(readSql());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 跨节点并发 bootstrap 串行化：CREATE TABLE IF NOT EXISTS 在 PG 下存在
            // 并发竞态（pg_type 唯一键冲突），用会话级 advisory lock 互斥。
            stmt.execute("SELECT pg_advisory_lock(4001, 3)");
            try {
                if (isLegacyTable(stmt)) {
                    stmt.execute("DROP TABLE " + TABLE);
                }
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            } finally {
                stmt.execute("SELECT pg_advisory_unlock(4001, 3)");
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("PlacementSchema.bootstrap failed: " + e.getMessage(), e);
        }
    }

    /**
     * 检测旧主键表：查询 pg_index 中该表唯一/主键索引的列集，
     * 不含 node_id 列即为旧表（需 DROP 重建）。
     */
    private static boolean isLegacyTable(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = '%s'
                  AND (i.indisprimary OR i.indisunique)
                """.formatted(TABLE))) {
            boolean hasNodeId = false;
            boolean hasPartitionId = false;
            while (rs.next()) {
                if ("node_id".equals(rs.getString(1))) {
                    hasNodeId = true;
                }
                if ("partition_id".equals(rs.getString(1))) {
                    hasPartitionId = true;
                }
            }
            // 表不存在（无任何索引列）→ 全新部署，非旧表
            return hasPartitionId && !hasNodeId;
        }
    }

    private static String readSql() {
        try (InputStream in = PlacementSchema.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
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
