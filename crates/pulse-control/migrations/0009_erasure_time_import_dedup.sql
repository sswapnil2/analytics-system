-- Erasure tombstones carry the erasure time: an erased id hides only events at or before it, so
-- a person who signs up again with the same id is counted from then on. Rows written before this
-- migration keep the old meaning (hidden for good).
ALTER TABLE erased_uids ADD COLUMN erased_at_ms BIGINT NOT NULL DEFAULT 9223372036854775807;

-- Event ids already taken in by `/import`, so re-running the same backfill adds nothing. The
-- in-memory dedup window only covers about 30 minutes.
CREATE TABLE IF NOT EXISTS import_seen (
    project_id TEXT NOT NULL,
    event_uuid TEXT NOT NULL,
    seen_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, event_uuid)
);
CREATE INDEX IF NOT EXISTS idx_import_seen_at ON import_seen (seen_at_ms);
