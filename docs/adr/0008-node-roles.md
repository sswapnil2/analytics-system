# ADR-0008: Scale out by node role over shared storage

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Ingest and queries compete for CPU and memory, so a heavy query should not slow down ingest.

## Decision
One binary can run as `--role all | ingest | writer | query | compactor` (`crates/pulse-node/src/lib.rs`).
- `ingest` (and `all`) nodes own storage for projects assigned to them in `project_owner` and execute local WAL/L0/segment writes, S3 uploads, and `/internal/v1/forward/*` requests.
- `writer` nodes are stateless ingest/routing frontends: they buffer async `/v1/ingest`, `/v1/agent/traces`, and `/v1/otel/v1/traces` into local `writer_log/` (`PWLF` v2) and forward idempotently (`PWFE`/`PWFA`) to each project's owner, and forward sync profile/replay calls and `/v1/projects/:id/*` queries/admin requests via `MetaOwnerDirectory` (`30 s` cache with immediate refresh on `503 not_owner`, `307`, or dead-owner transport failure).
- `query` nodes open storage and identity **read-only**, refresh from S3 `published/` + `agent_published/`, and forward storage-writing mutations to `project_owner`.
- `compactor` nodes only compact.

All roles share Postgres (`DATABASE_URL`, with `PULSE_VAULT_KEK` across nodes); storage-owning, `query`, and `compactor` nodes share the object store (`PULSE_OBJECT_STORE_URL`).

## Trade-offs
- **+** Stateless `writer`, storage-owning `ingest`, and `query` tiers scale and fail independently without separate service codebases, while `project_owner` routing keeps SlateDB/L0 single-writer per project.
- **−** Some state remains in memory per node: rate limits, event dedup windows, login lockout, and OAuth state (agent forwarded batches deduplicate idempotently on `(writer_id, offset)` via `writer_offsets` across `T_expire = 7d`).
- **−** Under `PULSE_HOT_STORE=local`, cross-machine ownership failover for an existing project requires `PULSE_HOT_STORE=object`, a shared volume, or explicit `pulse-node restore --from-published`.

## Future considerations
Keep multi-node integration coverage (`crates/pulse-node/tests/m6a_ownership.rs`, `m6b_writer_log_and_forwarder.rs`, `m6c_multi_node_w3.rs`) updated as placement and rebalancing evolve.
