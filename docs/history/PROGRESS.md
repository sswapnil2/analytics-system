# Pulse Analytics — Autonomous End-to-End Execution Progress

## Milestone 1: Workspace Foundation, `pulse-wire`, and `pulse-identity`
- [x] Initialize Cargo workspace (`Cargo.toml`) with shared dependencies & release profile (`lto = "thin"`, `codegen-units = 1`)
- [x] Implement `crates/pulse-wire` *(verified: P0-05, P0-06, P1-14, P2-03, P3-01)*:
  - [x] `WireEvent` & `PropValue` (`Null`, `Bool`, `Int`, `Float`, `Str`) + full-payload `canonical_content_uuid`
  - [x] `WireBatch` columnar binary framing (`PLSB` magic, bounded 64 MiB Zstd + dictionary framing, 64 MiB string materialization cap, JSON fallback)
  - [x] `ReplayChunk` & `WireframeOp` binary framing (`PLSR` magic, bounded allocations, default-deny fixed-length `"████"` PII masking)
  - [x] Shared `xxh3_64` bucketing module (`bucketing.rs`) verified against cross-SDK `sdks/bucketing_vectors.json`
- [x] Implement `crates/pulse-identity` *(verified: P1-08, P1-09, P1-10, P1-11, P3-04, Section 6)*:
  - [x] `ProjectIdentityGraph` backed by `slatedb` + `object_store` + `DashMap` with compacted `meta/checkpoint`, self-healing reopen, and loud replay validation
  - [x] Zero-rewrite retroactive `alias(anon_id, user_id, anon_bucket, user_bucket)` refusing identified-to-identified merges (`409 Conflict`)
  - [x] `cross_bucket_users` `RoaringBitmap` tracking & lock-free `IdentitySnapshot`
- [x] **Acceptance Gate 1**:
  - [x] `WireBatch` 10,000 polymorphic event round-trip & >= 65% compression vs JSON
  - [x] Hostile wire input bounds (`crates/pulse-wire/tests/hostile_input.rs`)
  - [x] `ProjectIdentityGraph` 250,000 IDs + 50,000 retroactive merges + SlateDB failure/restart safety (`crates/pulse-identity/tests/identity_durability_and_safety.rs`)

## Milestone 2: `pulse-storage` & `pulse-ingest` (WAL, Dedup, Vortex Tier A/B/C, MVCC & Compactor)
- [x] Implement `crates/pulse-storage` *(verified: P1-02..P1-07, P1-12, P1-15, P1-16, P2-01, P2-09l, P3-03, P3-04, Section 5 & 6)*:
  - [x] Partitioning (`xxh3_64(routing_seed) % NUM_BUCKETS`)
  - [x] `BucketWal` `PWL2` CRC32-checked WAL writer (covering `epoch || seq || len || payload` + `DurableLocalObjectStore` `fsync` + corrupt-frame quarantine + epoch fencing) (RPO = 0)
  - [x] `MemBucket` L0 hot RAM buffer with ordered per-bucket `bucket_append_locks`
  - [x] Immutable Vortex-backed Tier A/B/C segment writer & reader preserving explicit `PropValue::Null` (`p_{pk}_null` + `TAG_NULL`) & typed `InvertedValueTag` keys
  - [x] Per-segment inverted `RoaringBitmap` indexes (Tier B with `bool_indexed` / `str_indexed_complete` / `has_null`) + 64-bit Bloom filter (Tier C) + `SessionIndex` 64-bit event Bloom footer
  - [x] Strict read-after-write MVCC `Manifest` snapshot isolation (atomic `Arc<[Arc<StoredRow>]>` L0 + reader pin under `manifest.read()` + atomic `remove_if` unpin) & per-partition locked `Compactor`
  - [x] `ReplayStore` immutable per-chunk objects (`replays/{project}/{session}/{seq:010}.plsr`, `PutMode::Create`) & byte-range reader
- [x] Implement `crates/pulse-ingest` *(verified: P0-07, P1-01, P1-10, P1-12, P1-13, P3-01, P3-03, P3-05)*:
  - [x] `POST /v1/ingest`, `POST /v1/identify`, `POST /v1/replay/chunk` (header-only key, durable WAL append before dedup commit, durable identity flush before inline L1 flush)
  - [x] QUIC (`quinn`) UDP listener disabled by default & pre-body header-authenticated + stream-bounded when enabled
  - [x] Lexicon governance enforcement (drop events, mask PII on both events & replay labels, dictionary/governance cardinality caps)
  - [x] Bounded rotating 15-minute deterministic deduplication ring & overflow-safe `i128` clock-skew normalizer
- [x] **Acceptance Gate 2**:
  - [x] Concurrent failure-injected WAL + L0 + compaction + crash recovery (`crates/pulse-ingest/tests/p1_durability_and_concurrency.rs`, RPO = 0, 0 duplicates)

## Milestone 3: `pulse-exec` (Insights, Funnels, Retention, Flows, Cohorts, DataFusion Formulas & Lookups)
- [x] Implement `pulse-exec` *(verified: P0-08, P0-09, P2-01, P2-09a..l, P3-02, P3-03, P3-05, Section 6)*:
  - [x] Byte-bounded `foyer` / `moka` segment caches (evicting Tier A, inverted & prop caches by byte weight)
  - [x] Global pre-filter & cycle-safe `BehavioralCohort` `RoaringBitmap` evaluator (`validate_cohort_acyclic` + `MAX_COHORT_DEPTH`)
  - [x] `InsightsEngine` (signed `DDSketch`, end-anchored WAU/MAU, calendar IANA TZ bucketing, depth/length-bounded formula parser, bucket & breakdown `"$other"` caps)
  - [x] `FunnelEngine` (validated step & exclusion bounds <= 16, `u32` chunk refs, lock-free per-shard breakdown interner & `xxh3_64` hold-tuple hashing)
  - [x] `RetentionEngine` (`n_day`, `unbounded`, `frequency_stickiness` with calendar IANA TZ bucketing)
  - [x] `FlowsEngine` (honoring `top_n_per_step` `"$Other"` folding & bounded event scan)
  - [x] Custom Events, case-preserving DataFusion Custom Properties, qualified `table.col` pre-indexed Lookup Tables (physical props win), `search_sessions` `limit` pushdown, and per-query deadline (`ExecError::QueryTimeout`)
- [x] **Acceptance Gate 3**:
  - [x] Independent zero-shared-code randomized differential oracle (`proptest`) covering filters, cohorts, `first_time_ever`, signed percentiles, `Neq`, and window boundaries (`crates/pulse-exec/tests/proptest_oracle.rs`, `crates/pulse-exec/tests/limits.rs`)
  - [x] 1,000,000 event 4-step Funnel benchmark < 50ms in `--release`

## Milestone 4: `pulse-control`, `pulse-sdk-core`, `sdks/ts-web`, and `pulse-node`
- [x] Implement `crates/pulse-control` *(verified: P0-01..P0-04, P2-02, P2-09f, P2-10, P3-05, Section 5 `sqlx::migrate!`, Section 6)*:
  - [x] Versioned `sqlx::migrate!` schema & deduplicated DB layer with SHA-256 hashed `pk_`/`sk_` keys & tenant-scoped queries/upserts
  - [x] Frequentist Z-test + Bayesian Experimentation engine with Chi-square SRM check, Holm-Bonferroni multi-variant correction, Fieller/Delta variance-propagated lift CI, sub-24h window support & peeking notice
  - [x] Deterministic ETag-cached Edge Flag Bundle endpoint (`GET /v1/flags/:project_api_key`) with materialized `cohort_members` targeting
  - [x] Principal-scoped MCP JSON-RPC 2.0 server (`POST /v1/mcp`) & RFC 9110 `Range` (`206` / `416`) replay handler
- [x] Implement `crates/pulse-sdk-core` & `sdks/bindings-gen` *(verified: P1-08, P1-13, P2-03, P2-06, P2-07, P2-08, P3-01, P3-05)*:
  - [x] Thread-safe `PulseClient` (`async_runtime = "tokio"` + shared background Tokio runtime for UniFFI, append-only CRC32-verified 10 MiB binary disk WAL off caller thread, mutex-serialized ID-tracked flush, `400/413/422` drop / `5xx` backoff, `reset()`, transmit-time `client_sent_ts_ms`, default-deny `"████"` masking, rage-tap clear & pre-rotation replay seal)
  - [x] UniFFI Kotlin & Swift binding generator CLI & regenerated Kotlin/Swift bindings
- [x] Implement `sdks/ts-web` *(verified: P2-03, P2-04, P2-05, P3-01, P3-05)*:
  - [x] Zero-dependency TypeScript SDK (shared `xxh3_64` bucketing, `localStorage` persistence, `reset()`, failure requeue + backoff, unconditional `visibilitychange` `keepalive`, default-deny DOM/title/contenteditable `"████"` masking, listener cleanup)
- [x] Implement `crates/pulse-node` *(verified: P0-01, P0-02, P0-07, P3-05, Section 6)*:
  - [x] Secure defaults (`127.0.0.1` bind, random first-boot `pk_`/`sk_` keys printed once, `--seed` opt-in, `--quic` opt-in, restricted admin CORS, `SIGTERM` graceful shutdown)
- [x] **Acceptance Gate 4** (`crates/pulse-control/tests/security_and_tenancy.rs`, `crates/pulse-control/tests/flags_and_experiments.rs`, `pulse-node` `gate4`)

## Milestone 5: `web/` Full Analytics Studio, Production Build & End-to-End Verification
- [x] Scaffold `web/` (Vite + React + TypeScript + Tailwind dark-mode `#090d16` studio)
- [x] Update Studio & `scripts/verify_e2e.sh` for bearer admin auth, opt-in `--seed`, RFC 9110 `206` status-line check, and removal of production demo routes *(verified: P0-01, P0-02, P0-04, P3-01, P3-05, Section 6)*
- [x] **Acceptance Gate 5**:
  - [x] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --release && npm --prefix web run typecheck`
  - [x] `scripts/verify_e2e.sh` against live `pulse-node` (9/9 gates passed)

---

## First-Principles Review Remediation Checklist
- [x] **P0**: P0-01, P0-02, P0-03, P0-04, P0-05, P0-06, P0-07, P0-08, P0-09
- [x] **P1**: P1-01, P1-02, P1-03, P1-04, P1-05, P1-06, P1-07, P1-08, P1-09, P1-10, P1-11, P1-12, P1-13, P1-14, P1-15, P1-16
- [x] **P2**: P2-01, P2-02, P2-03, P2-04, P2-05, P2-06, P2-07, P2-08, P2-09 (a..l), P2-10
- [x] **P3**: P3-01, P3-02, P3-03, P3-04, P3-05
- [x] **Section 5 & 6**: Independent `proptest` oracle, `hostile_input.rs`, `limits.rs`, `sqlx::migrate!`, dead-code & demo cleanup
