# ADR-0008: Scale out by node role over shared storage

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Ingest and queries compete for CPU and memory, so a heavy query should not slow down ingest.

## Decision
One binary can run as `--role all | ingest | query | compactor` (`crates/pulse-node/src/lib.rs`).
- Ingest nodes write.
- Query nodes open storage and identity **read-only** and refresh them.
- The compactor only compacts.

All roles share an object store and a Postgres metadata DB.

## Trade-offs
- **+** Ingest and query scale and fail independently; no separate service codebases.
- **−** Some state is in memory per node: rate limits, dedup windows, login lockout, OAuth state.
- **−** This documentation has not verified that running several ingest nodes against one project is safe.

## Future considerations
Write down the supported multi-node topology and its guarantees.
