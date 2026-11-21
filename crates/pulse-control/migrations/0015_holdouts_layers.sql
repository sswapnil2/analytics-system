-- Holdouts (a share of users kept off every flag attached to it, to measure the flags' combined
-- effect) and layers (flags that split one pool of users so no one is in two of their
-- experiments). `bps` is the holdout's share; layers leave it 0.
CREATE TABLE IF NOT EXISTS traffic_groups (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    bps BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_traffic_groups_project ON traffic_groups (project_id, kind);

-- A flag's holdout and layer range, as JSON ({"holdout": {...}, "layer": {...}}). NULL = neither.
ALTER TABLE feature_flags ADD COLUMN placement_json TEXT;
