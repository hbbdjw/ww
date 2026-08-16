-- runtime-scheduler 租约表（PostgreSQL）
-- 单行表：id 恒为 1，CHECK 约束阻止出现第二行；CREATE TABLE IF NOT EXISTS 可由
-- SchedulerSchema.bootstrap 幂等执行。
CREATE TABLE IF NOT EXISTS rt_scheduler_lease (
    id            INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    owner         VARCHAR(64) NOT NULL,
    expires_at_ms BIGINT      NOT NULL DEFAULT 0
);
