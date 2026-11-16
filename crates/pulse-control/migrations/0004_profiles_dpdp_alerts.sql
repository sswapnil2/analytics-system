CREATE TABLE IF NOT EXISTS user_profiles (
    project_id TEXT NOT NULL,
    distinct_id TEXT NOT NULL,
    canonical_uid BIGINT NOT NULL DEFAULT 0,
    props_json TEXT NOT NULL DEFAULT '{}',
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, distinct_id)
);

CREATE TABLE IF NOT EXISTS group_profiles (
    project_id TEXT NOT NULL,
    group_type TEXT NOT NULL,
    group_id TEXT NOT NULL,
    props_json TEXT NOT NULL DEFAULT '{}',
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, group_type, group_id)
);

CREATE TABLE IF NOT EXISTS deletion_audit_log (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    distinct_id_masked TEXT NOT NULL,
    canonical_uid BIGINT NOT NULL,
    events_purged BIGINT NOT NULL DEFAULT 0,
    replays_purged BIGINT NOT NULL DEFAULT 0,
    requested_by TEXT NOT NULL,
    deleted_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS erased_uids (
    project_id TEXT NOT NULL,
    uid BIGINT NOT NULL,
    PRIMARY KEY (project_id, uid)
);

CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    event_name TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    webhook_url TEXT,
    last_triggered_ms BIGINT,
    created_at_ms BIGINT NOT NULL
);
