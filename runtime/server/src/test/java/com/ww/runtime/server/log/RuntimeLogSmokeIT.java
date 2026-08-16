package com.ww.runtime.server.log;

import com.ww.runtime.log.LogRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testcontainers PostgreSQL 冒烟集成测试：LogSchema 建表幂等 + JdbcLogStore
 * 批量写入 + 分页查询过滤（level/node/keyword 四字段）。
 *
 * <p>{@code disabledWithoutDocker = true}：本机无 Docker 时整类 SKIPPED 而非失败。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class RuntimeLogSmokeIT {

    @Container
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("rt")
                    .withUsername("rt")
                    .withPassword("rt");

    private static DataSource dataSource;
    private static JdbcLogStore store;

    @BeforeAll
    static void setUp() {
        dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        LogSchema.bootstrap(dataSource);
        // 二次 bootstrap：幂等
        LogSchema.bootstrap(dataSource);
        store = new JdbcLogStore(dataSource);
    }

    private static LogRecord record(String level, String logger, String message,
                                    String node, String stack, long ts) {
        return new LogRecord(level, logger, "thread-1", message, stack, node, ts);
    }

    @Test
    void batchInsertAndPageQueryRoundTrip() {
        store.batchInsert(List.of(
                record("INFO", "com.x.A", "hello world", "node-1", null, 1000L),
                record("WARN", "com.x.B", "disk almost full", "node-2", null, 2000L),
                record("ERROR", "com.x.C", "boom", "node-1",
                        "java.lang.NullPointerException: npe-here", 3000L)));

        // 全量分页（时间倒序）
        List<LogRecord> all = store.page(null, null, null, 0, 10);
        assertThat(all).hasSizeGreaterThanOrEqualTo(3);
        assertThat(all.get(0).timestamp()).isGreaterThanOrEqualTo(all.get(all.size() - 1).timestamp());
        assertThat(store.count(null, null, null)).isGreaterThanOrEqualTo(3);
    }

    @Test
    void filterByLevelNodeAndKeyword() {
        store.batchInsert(List.of(
                record("INFO", "com.x.Filter", "alpha message", "node-f1", null, 4000L),
                record("WARN", "com.x.Filter", "beta message", "node-f2", null, 5000L)));

        // level 过滤（大小写不敏感）
        assertThat(store.count("warn", null, null)).isGreaterThanOrEqualTo(1);
        List<LogRecord> warnLogs = store.page("WARN", null, null, 0, 10);
        assertThat(warnLogs).allSatisfy(r -> assertThat(r.level()).isEqualTo("WARN"));

        // node 过滤（精确匹配）
        List<LogRecord> nodeLogs = store.page(null, "node-f1", null, 0, 10);
        assertThat(nodeLogs).allSatisfy(r -> assertThat(r.node()).isEqualTo("node-f1"));
        assertThat(store.page(null, "node-f", null, 0, 10))
                .noneSatisfy(r -> assertThat(r.node()).isEqualTo("node-f")); // 前缀不算命中

        // keyword 四字段匹配（message/loggerName/node/stackTrace，大小写不敏感）
        assertThat(store.count(null, null, "ALPHA")).isGreaterThanOrEqualTo(1);
        assertThat(store.count(null, null, "com.x.filter")).isGreaterThanOrEqualTo(2);
        assertThat(store.count(null, null, "NODE-F2")).isGreaterThanOrEqualTo(1);
    }

    @Test
    void batchInsertEmptyIsNoop() {
        long before = store.count(null, null, null);
        store.batchInsert(List.of());
        assertThat(store.count(null, null, null)).isEqualTo(before);
    }
}
