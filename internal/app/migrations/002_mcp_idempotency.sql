CREATE TABLE mcp_idempotency (
    action TEXT NOT NULL,
    project_id BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    build_id BIGINT NOT NULL DEFAULT 0,
    created_unix BIGINT NOT NULL,
    PRIMARY KEY (action, project_id, idempotency_key)
);
