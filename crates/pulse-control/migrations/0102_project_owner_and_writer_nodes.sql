-- Project ownership mapping for W3 multi-node routing and writer-lease gating.
-- Each project belongs to a single storage/writer node (`node_name`); ownership
-- changes only via explicit placement or operator reassignment.
CREATE TABLE IF NOT EXISTS project_owner (
    project_id TEXT PRIMARY KEY,
    node_name TEXT NOT NULL,
    address TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_project_owner_node
    ON project_owner (node_name);

-- Live/healthy writer-capable storage nodes registered with stable `node_name`
-- and HTTP `address` so least-loaded placement can assign new projects.
CREATE TABLE IF NOT EXISTS writer_nodes (
    node_name TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    role TEXT NOT NULL,
    last_heartbeat_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_writer_nodes_heartbeat
    ON writer_nodes (role, last_heartbeat_ms);
