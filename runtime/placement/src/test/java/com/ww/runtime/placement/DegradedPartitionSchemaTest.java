package com.ww.runtime.placement;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** DegradedPartitionSchema：DDL 资源打包形态 + 注释/分号拆分口径。 */
class DegradedPartitionSchemaTest {

    @Test
    void ddlResourceIsPackagedWithExpectedShape() throws IOException {
        try (InputStream in = DegradedPartitionSchema.class.getResourceAsStream("/sql/rt_degraded.sql")) {
            assertThat(in).as("ddl resource must be packaged").isNotNull();
            String ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS rt_degraded_partition");
            assertThat(ddl).contains("capability VARCHAR(16) NOT NULL");
            assertThat(ddl).contains("partition_id INT NOT NULL");
            assertThat(ddl).contains("node_id VARCHAR(64) NOT NULL");
            assertThat(ddl).contains("since_ms BIGINT NOT NULL");
            assertThat(ddl).contains("PRIMARY KEY (capability, partition_id)");
        }
    }

    @Test
    void splitStatementsStripsCommentsAndSplitsOnSemicolons() {
        String sql = """
                -- 注释行
                CREATE TABLE t (a INT); -- 行尾注释
                CREATE INDEX i ON t (a);
                """;
        List<String> statements = DegradedPartitionSchema.splitStatements(sql);
        assertThat(statements).containsExactly("CREATE TABLE t (a INT)", "CREATE INDEX i ON t (a)");
    }
}
