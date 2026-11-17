-- Roles within an organisation: viewer < member < admin < owner.
-- Accounts that existed before roles keep the full access they had (admin); the earliest account
-- in each organisation becomes its owner.
ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin';
UPDATE users SET role = 'owner'
WHERE created_at_ms = (SELECT MIN(u2.created_at_ms) FROM users u2 WHERE u2.org_id = users.org_id);

-- Active-sessions list on the Profile page.
ALTER TABLE user_sessions ADD COLUMN user_agent TEXT;
ALTER TABLE user_sessions ADD COLUMN last_seen_ms BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS org_invites (
    id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    org_id TEXT NOT NULL,
    email TEXT NOT NULL,
    role TEXT NOT NULL,
    invited_by TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    accepted_at_ms BIGINT
);

CREATE TABLE IF NOT EXISTS password_resets (
    token_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    used_at_ms BIGINT
);

-- A password sign-up proves its email before it joins or creates an organisation. Until then the
-- account sits in the placeholder organisation `org_pending`, which has no projects, and gets no session.
CREATE TABLE IF NOT EXISTS email_verifications (
    token_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    -- Set when verifying creates a new organisation with this name; NULL joins the default one.
    org_name TEXT,
    expires_at_ms BIGINT NOT NULL,
    used_at_ms BIGINT
);
