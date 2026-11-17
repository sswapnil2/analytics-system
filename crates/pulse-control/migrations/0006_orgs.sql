-- Accounts belong to an organisation and see only that organisation's projects.
CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

INSERT INTO orgs (id, name, created_at_ms) VALUES ('org_default', 'Default organisation', 0)
ON CONFLICT DO NOTHING;

-- Accounts created before organisations existed keep the access they had: the default org.
ALTER TABLE users ADD COLUMN org_id TEXT NOT NULL DEFAULT 'org_default';
