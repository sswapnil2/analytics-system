-- Cluster-wide shared secret for internal node-to-node forwarding (`/internal/v1/forward/*`).
-- Single-row table (`id = 1`) auto-provisioned on first boot when `PULSE_INTERNAL_TOKEN` is unset.
CREATE TABLE IF NOT EXISTS cluster_internal_auth (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    token TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
