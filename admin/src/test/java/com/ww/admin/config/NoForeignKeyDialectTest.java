package com.ww.admin.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.hibernate.mapping.ForeignKey;
import org.hibernate.tool.schema.spi.Exporter;
import org.junit.jupiter.api.Test;

/**
 * {@link NoForeignKeyPostgreSQLDialect} 回归测试：
 * 验证外键 exporter 不产生任何 DDL，从而在 ddl-auto 建表时禁止创建外键。
 */
class NoForeignKeyDialectTest {

    @Test
    void foreignKeyExporterProducesNoCommands() {
        NoForeignKeyPostgreSQLDialect dialect = new NoForeignKeyPostgreSQLDialect();
        Exporter<ForeignKey> exporter = dialect.getForeignKeyExporter();

        // 该实现直接返回 NO_COMMANDS，不读取参数，故 Metadata / SqlStringGenerationContext 传 null 即可
        ForeignKey fk = new ForeignKey();
        assertThat(exporter.getSqlCreateStrings(fk, null, null)).isEmpty();
        assertThat(exporter.getSqlDropStrings(fk, null, null)).isEmpty();
    }
}
