package com.ww.runtime.scheduler;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchedulerSchemaTest {

    @Test
    void ddlResourceIsPackagedWithExpectedShape() throws IOException {
        try (InputStream in = SchedulerSchema.class.getResourceAsStream("/sql/rt_scheduler.sql")) {
            assertThat(in).as("ddl resource must be packaged").isNotNull();
            String ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS rt_scheduler_lease");
            assertThat(ddl).contains("id            INT PRIMARY KEY DEFAULT 1 CHECK (id = 1)");
            assertThat(ddl).contains("owner         VARCHAR(64) NOT NULL");
            assertThat(ddl).contains("expires_at_ms BIGINT      NOT NULL DEFAULT 0");
        }
    }

    @Test
    void bootstrapRejectsNullDataSource() {
        assertThatThrownBy(() -> SchedulerSchema.bootstrap(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
