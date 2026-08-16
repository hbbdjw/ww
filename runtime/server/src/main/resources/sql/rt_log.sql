-- runtime 日志表：节点 logback 事件实时采集，由宿主批量写入器批量落库。
-- 全部 CREATE TABLE / INDEX IF NOT EXISTS，幂等（PostgreSQL 方言）。
CREATE TABLE IF NOT EXISTS rt_log (
    id          BIGSERIAL PRIMARY KEY,
    level       VARCHAR(16),
    logger_name VARCHAR(255),
    thread_name VARCHAR(128),
    message     TEXT,
    stack_trace TEXT,
    node        VARCHAR(64),
    created_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rt_log_created_at ON rt_log (created_at);
CREATE INDEX IF NOT EXISTS idx_rt_log_node ON rt_log (node);
CREATE INDEX IF NOT EXISTS idx_rt_log_level ON rt_log (level);
