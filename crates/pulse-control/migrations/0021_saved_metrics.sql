-- Experiment metrics defined once (e.g. "KYC failure") and reused as guardrails or secondaries.
CREATE TABLE IF NOT EXISTS saved_metrics (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT '',
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS saved_metrics_project ON saved_metrics (project_id);
