-- Track whether an agent erasure tombstone has been physically purged across all
-- tiers (L0, agent_wal, local hot segments, S3 segments, published manifest, and pending GC).
-- Tombstones remain active for query/compaction enforcement and cannot be pruned from SQL
-- until `physically_purged_at_ms IS NOT NULL`.
ALTER TABLE agent_erasure_tombstones ADD COLUMN physically_purged_at_ms BIGINT;
CREATE INDEX IF NOT EXISTS idx_agent_erasure_unpurged
    ON agent_erasure_tombstones (physically_purged_at_ms, expires_at_ms);
