-- One row per project: the node allowed to write its event store, until `expires_at_ms`.
-- `generation` grows every time the holder changes and is what storage fences on.
-- (Numbered 0100 so it cannot collide with migrations on other open branches.)
CREATE TABLE IF NOT EXISTS writer_leases (
    project_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);
