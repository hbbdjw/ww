-- runtime-migration 任务表（PostgreSQL）
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 MigrationSchema.bootstrap 幂等执行。

CREATE TABLE IF NOT EXISTS rt_migration_task (
    task_id         BIGSERIAL PRIMARY KEY,
    capability      VARCHAR(16) NOT NULL,
    partition_id    INT NOT NULL,
    from_node       VARCHAR(64),
    to_node         VARCHAR(64) NOT NULL,
    state           VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    checkpoint_offset BIGINT NOT NULL DEFAULT 0,
    items_done      BIGINT NOT NULL DEFAULT 0,
    checkpoint_json TEXT,
    created_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL
);
