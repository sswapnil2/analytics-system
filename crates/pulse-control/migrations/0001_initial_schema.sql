CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    project_id TEXT NOT NULL,
    label TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lexicon_events (
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    owner TEXT NOT NULL DEFAULT 'product',
    volume_24h BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE IF NOT EXISTS lexicon_properties (
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    prop_scope TEXT NOT NULL DEFAULT 'event',
    inferred_types TEXT NOT NULL DEFAULT 'string',
    status TEXT NOT NULL DEFAULT 'active',
    pii_masked INTEGER NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE IF NOT EXISTS custom_events (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (project_id, name)
);

CREATE TABLE IF NOT EXISTS custom_properties (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    sql_expr TEXT NOT NULL,
    referenced_props_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (project_id, name)
);

CREATE TABLE IF NOT EXISTS lookup_tables (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    join_prop TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (project_id, name)
);

CREATE TABLE IF NOT EXISTS cohorts (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_reports (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    report_kind TEXT NOT NULL,
    query_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dashboards (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    report_ids_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS feature_flags (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    flag_key TEXT NOT NULL,
    description TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    rollout_pct DOUBLE PRECISION NOT NULL DEFAULT 100.0,
    targeting_cohort_id TEXT,
    variants_json TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (project_id, flag_key)
);

CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    flag_id TEXT NOT NULL,
    flag_key TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    start_ts_ms BIGINT NOT NULL,
    end_ts_ms BIGINT NOT NULL,
    primary_metric_json TEXT NOT NULL,
    guardrail_metrics_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
