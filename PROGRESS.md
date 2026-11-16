# Pulse Analytics — Autonomous End-to-End Execution Progress

## Milestone 1: Workspace Foundation, `pulse-wire`, and `pulse-identity`
- [x] Initialize Cargo workspace (`Cargo.toml`) with shared dependencies & release profile (`lto = "thin"`, `codegen-units = 1`)
- [ ] Implement `crates/pulse-wire` *(open: P0-05, P0-06, P1-14, P3-01)*:
  - [x] `WireEvent` & `PropValue` (`Null`, `Bool`, `Int`, `Float`, `Str`)
  - [ ] `WireBatch` columnar binary framing (`PLSB` magic, bounded Zstd + optional pre-shared dictionary framing, JSON fallback)
  - [ ] `ReplayChunk` & `WireframeOp` binary framing (`PLSR` magic, bounded allocations, default-deny PII masking)
- [ ] Implement `crates/pulse-identity` *(open: P1-08, P1-09, P1-10, P1-11, P3-04)*:
  - [ ] `ProjectIdentityGraph` backed by `slatedb` + `object_store` + `DashMap` with checkpointing & durable ordering
  - [ ] Zero-rewrite retroactive `alias(anon_id, user_id, anon_bucket, user_bucket)` refusing identified-to-identified merges
  - [x] `cross_bucket_users` `RoaringBitmap` tracking & lock-free `IdentitySnapshot`
- [ ] **Acceptance Gate 1** *(open: P0-05, P0-06, P1-08..P1-11)*:
  - [x] `WireBatch` 10,000 polymorphic event round-trip & >= 65% compression vs JSON
  - [ ] Hostile wire input bounds (`crates/pulse-wire/tests/hostile_input.rs`)
  - [ ] `ProjectIdentityGraph` 250,000 IDs + 50,000 retroactive merges + SlateDB failure/restart safety

## Milestone 2: `pulse-storage` & `pulse-ingest` (WAL, Dedup, Vortex Tier A/B/C, MVCC & Compactor)
- [ ] Implement `crates/pulse-storage` *(open: P1-02..P1-07, P1-12, P1-15, P1-16)*:
  - [x] Partitioning (`xxh3_64(routing_seed) % NUM_BUCKETS`)
  - [ ] `BucketWal` CRC32-checked WAL writer (covering header + fsync on local WAL + corrupt-frame quarantine + epoch fencing) (RPO = 0)
  - [ ] `MemBucket` L0 hot RAM buffer with ordered per-bucket seq/append lock
  - [ ] Immutable Vortex-backed Tier A/B/C segment writer & reader preserving `Null` props & typed inverted-index keys
  - [ ] Per-segment inverted `RoaringBitmap` indexes (Tier B) + 64-bit Bloom filter (Tier C) + `SessionIndex` 64-bit event Bloom footer
  - [ ] Strict read-after-write MVCC `Manifest` snapshot isolation (atomic L0 + pin lock) & locked `Compactor`
  - [ ] `ReplayStore` immutable per-chunk objects (`replays/{project}/{session}/{seq:010}.plsr`) & byte-range reader
- [ ] Implement `crates/pulse-ingest` *(open: P0-07, P1-01, P1-10, P1-13, P3-01, P3-05)*:
  - [ ] `POST /v1/ingest`, `POST /v1/identify`, `POST /v1/replay/chunk` (header-only key, post-append dedup commit)
  - [ ] QUIC (`quinn`) UDP listener disabled by default & resource-bounded when enabled
  - [ ] Lexicon governance enforcement (drop events, mask PII on both events & replay labels, key-cardinality cap)
  - [ ] Bounded rotating 15-minute deterministic deduplication ring & safe clock-skew normalizer
- [ ] **Acceptance Gate 2** *(open: P1-01..P1-07, P1-12..P1-16)*:
  - [ ] Concurrent failure-injected WAL + L0 + compaction + crash recovery (RPO = 0, 0 duplicates)

## Milestone 3: `pulse-exec` (Insights, Funnels, Retention, Flows, Cohorts, DataFusion Formulas & Lookups)
- [ ] Implement `pulse-exec` *(open: P0-08, P0-09, P2-01, P2-09a..l, P3-02, P3-03)*:
  - [ ] Bounded `foyer` / `moka` segment caches (evicting Tier A, inverted & prop caches)
  - [ ] Global pre-filter & cycle-safe `BehavioralCohort` `RoaringBitmap` evaluator
  - [ ] `InsightsEngine` (signed `DDSketch`, end-anchored WAU/MAU, calendar TZ bucketing, strict formula parser, resource caps)
  - [ ] `FunnelEngine` (validated step bounds <= 16, `u32` chunk refs, sharded drop-off collector)
  - [x] `RetentionEngine` (`n_day`, `unbounded`, `frequency_stickiness`)
  - [ ] `FlowsEngine` (honoring `top_n_per_step` & bounded event scan)
  - [ ] Custom Events, case-preserving DataFusion Custom Properties, and qualified `table.col` pre-indexed Lookup Tables
- [ ] **Acceptance Gate 3** *(open: Section 5 independent `proptest` oracle, `limits.rs`)*:
  - [ ] Independent zero-shared-code randomized differential oracle (`proptest`) covering filters, cohorts, `first_time_ever`, percentiles, `Neq`, window boundaries
  - [x] 1,000,000 event 4-step Funnel benchmark < 50ms in `--release`

## Milestone 4: `pulse-control`, `pulse-sdk-core`, `sdks/ts-web`, and `pulse-node`
- [ ] Implement `crates/pulse-control` *(open: P0-01..P0-04, P2-02, P2-10, Section 5 `sqlx::migrate!`, Section 6)*:
  - [ ] Versioned `sqlx::migrate!` schema & deduplicated DB layer with SHA-256 hashed `pk_`/`sk_` keys & tenant-scoped queries/upserts
  - [ ] Frequentist Z-test + Bayesian Experimentation engine with SRM check, Bonferroni/Holm multi-variant correction, variance-propagated lift CI & peeking notice
  - [ ] Deterministic ETag-cached Edge Flag Bundle endpoint (`GET /v1/flags/:project_api_key`) with cohort targeting
  - [ ] Principal-scoped MCP JSON-RPC 2.0 server (`POST /v1/mcp`)
- [ ] Implement `crates/pulse-sdk-core` & `sdks/bindings-gen` *(open: P1-08, P1-13, P2-03, P2-06, P2-07, P2-08, P3-01, P3-05)*:
  - [ ] Thread-safe `PulseClient` (owned Tokio runtime for UniFFI, append-only CRC-verified 10 MB disk WAL off caller thread, mutex-serialized ID-tracked flush, 4xx drop / 5xx backoff, `reset()`, transmit-time `sent_ts`)
  - [x] UniFFI Kotlin & Swift binding generator CLI
- [ ] Implement `sdks/ts-web` *(open: P2-03, P2-04, P2-05, P3-01, P3-05)*:
  - [ ] Zero-dependency TypeScript SDK (shared `xxh3_64` bucketing, `localStorage` persistence, failure requeue + backoff, default-deny DOM text masking, listener cleanup)
- [ ] Implement `crates/pulse-node` *(open: P0-01, P0-02, P0-07, P3-05, Section 6)*:
  - [ ] Secure defaults (`127.0.0.1` bind, random first-boot `pk_`/`sk_` keys, `--seed` opt-in, `--quic` opt-in, restricted admin CORS, SIGTERM graceful shutdown)
- [ ] **Acceptance Gate 4** *(open: P0-01..P0-04, P2-02..P2-10)*

## Milestone 5: `web/` Full Analytics Studio, Production Build & End-to-End Verification
- [x] Scaffold `web/` (Vite + React + TypeScript + Tailwind dark-mode `#090d16` studio)
- [ ] Update Studio & `scripts/verify_e2e.sh` for bearer admin auth, opt-in `--seed`, and removal of production demo routes *(open: P0-01, P0-02, P3-05, Section 6)*
- [ ] **Acceptance Gate 5**:
  - [ ] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --release && npm --prefix web run typecheck`
  - [ ] `scripts/verify_e2e.sh` against live `pulse-node`

---

## First-Principles Review Remediation Checklist
- [x] **P0**: P0-01, P0-02, P0-03, P0-04, P0-05, P0-06, P0-07, P0-08, P0-09
- [ ] **P1**: P1-01, P1-02, P1-03, P1-04, P1-05, P1-06, P1-07, P1-08, P1-09, P1-10, P1-11, P1-12, P1-13, P1-14, P1-15, P1-16
- [ ] **P2**: P2-01, P2-02, P2-03, P2-04, P2-05, P2-06, P2-07, P2-08, P2-09 (a..l), P2-10
- [ ] **P3**: P3-01, P3-02, P3-03, P3-04, P3-05
- [ ] **Section 5 & 6**: Independent `proptest` oracle, `hostile_input.rs`, `limits.rs`, `sqlx::migrate!`, dead-code & demo cleanup
