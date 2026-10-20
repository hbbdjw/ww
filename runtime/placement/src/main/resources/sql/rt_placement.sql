-- runtime-placement 模块 DDL（PostgreSQL）
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 PlacementSchema.bootstrap 幂等执行。
-- v2：主键扩展为 (capability, partition_id, node_id)——每分区多行放置
-- （[0]=PRIMARY，其余 REPLICA）；旧 PK (capability, partition_id) 表由
-- PlacementSchema.bootstrap 检测后自动重建（放置为派生状态，调度器重新引导）。

CREATE TABLE IF NOT EXISTS rt_partition_placement (
    capability     VARCHAR(16) NOT NULL,
    partition_id   INT NOT NULL,
    node_id        VARCHAR(64) NOT NULL,
    role           VARCHAR(8) NOT NULL,
    epoch          BIGINT NOT NULL DEFAULT 0,
    lease_until_ms BIGINT NOT NULL DEFAULT 0,
    valid_after_ms BIGINT NOT NULL DEFAULT 0,
    updated_at     BIGINT,
    PRIMARY KEY (capability, partition_id, node_id)
);

-- 滚动升级：既有表补列（放置为派生状态，旧行租约从 0 开始，属主启动后续约）
ALTER TABLE rt_partition_placement
    ADD COLUMN IF NOT EXISTS lease_until_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE rt_partition_placement
    ADD COLUMN IF NOT EXISTS valid_after_ms BIGINT NOT NULL DEFAULT 0;

-- 放置修订号：数据面本地视图轮询该值决定是否重载（租约续约不推进修订号）。
CREATE TABLE IF NOT EXISTS rt_placement_revision (
    capability VARCHAR(16) PRIMARY KEY,
    revision   BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT
);
