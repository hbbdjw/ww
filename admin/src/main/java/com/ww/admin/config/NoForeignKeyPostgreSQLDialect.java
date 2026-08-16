package com.ww.admin.config;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.tool.schema.spi.Exporter;

/**
 * 禁用外键约束生成的 PostgreSQL 方言。
 *
 * <p>Hibernate 的 {@code ddl-auto}（{@code create}/{@code update}）在生成/迁移表结构时，
 * 会通过 {@link org.hibernate.dialect.Dialect#getForeignKeyExporter()} 生成外键约束的 DDL。
 * 本方言将其重写为“不生成任何命令”，从而在自动建表时不再创建外键，
 * 仅保留关联列与索引（如需索引仍需单独配置）。
 *
 * <p>通过 {@code spring.jpa.properties.hibernate.dialect} 指定本类即可全局生效。
 */
public class NoForeignKeyPostgreSQLDialect extends PostgreSQLDialect {

    @Override
    public Exporter<ForeignKey> getForeignKeyExporter() {
        return new Exporter<>() {
            @Override
            public String[] getSqlCreateStrings(ForeignKey exportable, Metadata metadata, SqlStringGenerationContext context) {
                return NO_COMMANDS;
            }

            @Override
            public String[] getSqlDropStrings(ForeignKey exportable, Metadata metadata, SqlStringGenerationContext context) {
                return NO_COMMANDS;
            }
        };
    }
}
