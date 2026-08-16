-- runtime-placement 模块 DDL（PostgreSQL）
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 PlacementSchema.bootstrap 幂等执行。

CREATE TABLE IF NOT EXISTS rt_partition_placement (
    capability   VARCHAR(16) NOT NULL,
    partition_id INT NOT NULL,
    node_id      VARCHAR(64) NOT NULL,
    role         VARCHAR(8) NOT NULL,
    epoch        BIGINT NOT NULL DEFAULT 0,
    updated_at   BIGINT,
    PRIMARY KEY (capability, partition_id)
);
