# ADR-0003: Acknowledge ingest only after a durable WAL write

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Context
SDKs retry until they get a success response, so every success must mean the events are safe.

## Decision
`IngestService` writes the identity graph first. It then appends each bucket's rows to a CRC-checked WAL object and inserts them into the in-memory L0. It returns `200` only after all of that has succeeded (`crates/pulse-ingest/src/service.rs`, `crates/pulse-storage/src/wal.rs`). On restart the WAL is replayed into L0, and the dedup windows are re-seeded from it. Code comments call this "RPO = 0".

## Inferred rationale
Combined with client event ids and server dedup, this gives *effectively-once* counting under retries. Writing identity first means an event never refers to an id the graph doesn't have.

## Trade-offs
- **+** No acknowledged event is lost on a crash; retries are safe.
- **−** Ingest latency includes a durable write (an fsync locally, a PUT on S3).
- **−** Dedup is windowed (15–30 min). A retry after the window is counted again.

## Consequences
Backpressure is explicit: when L0 is full, ingest answers `429 Retry-After: 1` instead of buffering without limit.
