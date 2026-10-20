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

CREATE TABLE IF NOT EXISTS rt_decommission (
    node_id      VARCHAR(64) PRIMARY KEY,
    state        VARCHAR(16) NOT NULL,
    force_flag   BOOLEAN NOT NULL DEFAULT FALSE,
    detail       TEXT,
    started_at   BIGINT NOT NULL,
    updated_at   BIGINT NOT NULL,
    completed_at BIGINT
);

-- LB 反代寻址：节点 REST 数据面端口（节点启动按 server.port 上报；0=未知，不参与 LB 分发）。
-- IF NOT EXISTS 加列，幂等兼容旧库。
ALTER TABLE rt_node ADD COLUMN IF NOT EXISTS http_port INT NOT NULL DEFAULT 0;

-- P1-9 故障域（机架/可用区）：副本放置反亲和；空串 = 未配置。
ALTER TABLE rt_node ADD COLUMN IF NOT EXISTS zone VARCHAR(64) NOT NULL DEFAULT '';

-- Phi Accrual 多级故障确认：观察者怀疑票（PFAIL→FAIL 多数仲裁凭据）。
-- (suspect, observer) 复合主键：同一观察者重复投票只刷新 phi/updated_at，不叠加票数。
-- leader 按 TTL 内不同 observer 数量计票，达法定人数才把 suspect 提升为 DEAD。
CREATE TABLE IF NOT EXISTS rt_node_suspicion (
    suspect_node_id  VARCHAR(64) NOT NULL,
    observer_node_id VARCHAR(64) NOT NULL,
    phi              DOUBLE PRECISION NOT NULL,
    updated_at_ms    BIGINT NOT NULL,
    PRIMARY KEY (suspect_node_id, observer_node_id)
);
