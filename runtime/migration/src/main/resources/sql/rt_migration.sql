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

-- 活动任务查询（listActive：state NOT IN 终态）走 state 索引，避免全表扫描；
-- 任务复用/归属查找按 (capability, partition_id, to_node) 组合索引。
CREATE INDEX IF NOT EXISTS idx_migration_task_state ON rt_migration_task (state);
CREATE INDEX IF NOT EXISTS idx_migration_task_cap_part ON rt_migration_task (capability, partition_id, to_node);
