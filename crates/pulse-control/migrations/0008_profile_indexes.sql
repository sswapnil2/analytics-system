-- Profile lookups by canonical user (identity merges, per-user rebuilds) and incremental
-- reloads by update time. Both run on every engage and every catalog refresh.
CREATE INDEX IF NOT EXISTS idx_user_profiles_canonical ON user_profiles (project_id, canonical_uid);
CREATE INDEX IF NOT EXISTS idx_user_profiles_updated ON user_profiles (project_id, updated_at_ms);
