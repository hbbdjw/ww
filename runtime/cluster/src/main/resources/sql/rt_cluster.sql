-- runtime-cluster 模块 DDL（PostgreSQL）
-- 全部使用 CREATE TABLE IF NOT EXISTS，可由 ClusterSchema.bootstrap 幂等执行。

CREATE TABLE IF NOT EXISTS rt_node (
    node_id         VARCHAR(64) PRIMARY KEY,
    host            VARCHAR(64) NOT NULL,
    grpc_port       INT NOT NULL,
    state           VARCHAR(16) NOT NULL,
    state_epoch     BIGINT NOT NULL DEFAULT 0,
    weight          DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    cpu_percent     DOUBLE PRECISION,
    mem_total_bytes BIGINT,
    mem_used_bytes  BIGINT,
    disk_total_bytes BIGINT,
    disk_used_bytes BIGINT,
    resources_at    BIGINT,
    registered_at   BIGINT NOT NULL,
    heartbeat_at    BIGINT
);

CREATE TABLE IF NOT EXISTS rt_cluster_config (
    cfg_key   VARCHAR(128) PRIMARY KEY,
    cfg_value TEXT,
    version   BIGINT NOT NULL DEFAULT 0
);
