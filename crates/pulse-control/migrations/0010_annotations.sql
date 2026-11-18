-- Dated notes on a project's timeline ("v2.3 released", "pricing page changed"), shown on
-- report charts so a change in a metric can be tied to what happened.
CREATE TABLE IF NOT EXISTS annotations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    ts_ms BIGINT NOT NULL,
    text TEXT NOT NULL,
    created_by TEXT,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_annotations_project_ts ON annotations (project_id, ts_ms);
