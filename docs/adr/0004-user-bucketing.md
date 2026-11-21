# ADR-0004: Route events to buckets by a stable device id

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
Funnels, retention and flows are per-user computations over ordered events.

## Decision
Every event goes to bucket `xxh3_64(routing_seed) % num_buckets`. The default is 16 buckets (`crates/pulse-storage/src/engine.rs`). `routing_seed` is an immutable device install id that the SDKs send. Segments are per `(day, bucket)`, and rows are sorted by user and time within a segment.

## Inferred rationale
All of one device's events share a bucket, so per-user work does not cross buckets and can run per bucket in parallel. Sampling whole buckets is *user-consistent*: a sampled funnel still sees each sampled user's full journey (`crates/pulse-exec/src/scanner.rs`).

## Trade-offs
- **+** Parallel ingest and query per bucket; whole buckets can be pruned when sampling.
- **−** A user active on several devices spans several buckets. The identity graph tracks this with a cross-bucket bitmap (`crates/pulse-identity/src/lib.rs`).
- **−** Changing `PULSE_NUM_BUCKETS` after data exists would re-route users. No migration path is documented.
