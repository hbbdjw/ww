-- runtime-capability 状态表（PostgreSQL）
CREATE TABLE IF NOT EXISTS rt_capability_state (
    node_id    VARCHAR(64)  NOT NULL,
    capability VARCHAR(16)  NOT NULL,
    desired    VARCHAR(16)  NOT NULL DEFAULT 'DISABLED',
    actual     VARCHAR(16)  NOT NULL DEFAULT 'DISABLED',
    updated_at BIGINT       NOT NULL,
    PRIMARY KEY (node_id, capability)
);
