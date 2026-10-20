-- runtime-lb 模块 DDL（PostgreSQL）
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 LbSchema.bootstrap 幂等执行。

-- 负载均衡集群级配置：恒为单行（id=1），cfg_json 为 LbConfig 整体 JSON，
-- version 为乐观锁版本（每次成功更新 +1）。
CREATE TABLE IF NOT EXISTS rt_lb_config (
    id         INT PRIMARY KEY CHECK (id = 1),
    cfg_json   TEXT NOT NULL,
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL
);
