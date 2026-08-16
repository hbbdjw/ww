package com.ww.runtime.capability;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CapabilitySchemaTest {

    @Test
    void ddlResourceIsPackagedWithExpectedShape() throws IOException {
        try (InputStream in = CapabilitySchema.class.getResourceAsStream("/sql/rt_capability.sql")) {
            assertThat(in).as("ddl resource must be packaged").isNotNull();
            String ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS rt_capability_state");
            assertThat(ddl).contains("node_id    VARCHAR(64)");
            assertThat(ddl).contains("capability VARCHAR(16)");
            assertThat(ddl).contains("desired    VARCHAR(16)  NOT NULL DEFAULT 'DISABLED'");
            assertThat(ddl).contains("actual     VARCHAR(16)  NOT NULL DEFAULT 'DISABLED'");
            assertThat(ddl).contains("updated_at BIGINT       NOT NULL");
            assertThat(ddl).contains("PRIMARY KEY (node_id, capability)");
        }
    }
}
