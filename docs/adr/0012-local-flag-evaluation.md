# ADR-0012: Deterministic flag evaluation, shared by SDKs and server

> **Status:** Accepted, as implemented on `main`. **Evidence level:** the *decision* is verified in code. The repo holds no written design discussion, so the *rationale* below is **inferred** from the code, comments and tests, and is marked as such. Alternatives are listed only where the repo itself shows them.

## Decision
SDKs download a flag bundle (`GET /v1/flags/:ingest_key`, with ETag caching) and evaluate flags locally:
- The rollout slot is `xxh3_64("{flag_key}:{distinct_id}") % 10000`.
- The variant slot uses the prefix `var:`.
- Cohort targets ship as member hashes, not raw ids.

The same functions live in `crates/pulse-wire/src/bucketing.rs`. Cross-SDK test vectors in `sdks/bucketing_vectors.json` keep every implementation in agreement.

## Inferred rationale
A flag check makes no network call. The experiment analysis on the server can recompute exactly who was assigned which variant.

## Trade-offs
- **+** Zero-latency flag checks that work offline; client and server agree on assignment.
- **−** Assignment depends on the current distinct id, so `identify()` can change a user's variant.
- **−** The bundle is public to anyone holding the ingest key, which is why cohort members are hashed.
