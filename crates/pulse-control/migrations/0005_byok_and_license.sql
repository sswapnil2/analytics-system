CREATE TABLE IF NOT EXISTS system_license (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    license_key TEXT NOT NULL,
    customer_org TEXT NOT NULL,
    edition TEXT NOT NULL,
    features_json TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    activated_at_ms BIGINT NOT NULL,
    max_seen_ts_ms BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS system_keyring (
    key_id TEXT PRIMARY KEY,
    salt_hex TEXT NOT NULL,
    sealed_key_hex TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_audit_log (
    id TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS byok_credentials (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    provider TEXT NOT NULL,
    label TEXT NOT NULL,
    key_prefix_masked TEXT NOT NULL,
    sealed_secret_hex TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'active',
    last_verified_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE(project_id, category)
);
