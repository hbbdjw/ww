-- runtime-placement 模块 DDL（PostgreSQL）
-- 分区隔离注册表：no live replica 分区治愈宽限期赌输后的 DEGRADED 状态。
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 DegradedPartitionSchema.bootstrap 幂等执行。
--
-- 语义：放置表（rt_partition_placement）保持指向死亡属主（数据留盘押注重启），
-- 本表仅记录「数据面拒绝服务」的隔离标记；属主回归由调度器清除本表行自动解除，
-- 运营 force-adopt（接受丢失）时 cutover 放置后同样清除。

CREATE TABLE IF NOT EXISTS rt_degraded_partition (
    capability VARCHAR(16) NOT NULL,
    partition_id INT NOT NULL,
    node_id VARCHAR(64) NOT NULL,
    since_ms BIGINT NOT NULL,
    reason VARCHAR(256) NOT NULL,
    PRIMARY KEY (capability, partition_id)
);
