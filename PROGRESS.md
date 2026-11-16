# Pulse Analytics — Autonomous End-to-End Execution Progress

## Milestone 1: Workspace Foundation, `pulse-wire`, and `pulse-identity`
- [x] Initialize Cargo workspace (`Cargo.toml`) with shared dependencies & release profile (`lto = "thin"`, `codegen-units = 1`)
- [x] Implement `crates/pulse-wire`:
  - [x] `WireEvent` & `PropValue` (`Null`, `Bool`, `Int`, `Float`, `Str`)
  - [x] `WireBatch` columnar binary framing (`PLSB` magic, Zstd + optional pre-shared dictionary framing, JSON fallback)
  - [x] `ReplayChunk` & `WireframeOp` binary framing (`PLSR` magic, `FullSnapshot`, `NodeUpsert`, `NodeRemove`, `PointerInteraction`, `NetworkBreadcrumb`)
- [x] Implement `crates/pulse-identity`:
  - [x] `ProjectIdentityGraph` backed by `slatedb` + `object_store` + `DashMap`
  - [x] Zero-rewrite retroactive `alias(anon_id, user_id, anon_bucket, user_bucket)` with Union-Find path compression
  - [x] `cross_bucket_users` `RoaringBitmap` tracking & < 5ms lock-free `IdentitySnapshot`
- [x] **Acceptance Gate 1**:
  - [x] `WireBatch` 10,000 polymorphic event round-trip & >= 65% compression vs JSON
  - [x] `ProjectIdentityGraph` 250,000 IDs + 50,000 retroactive merges + SlateDB restart recovery + < 5ms snapshot
  - [x] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p pulse-wire -p pulse-identity --release`

## Milestone 2: `pulse-storage` & `pulse-ingest` (WAL, Dedup, Vortex Tier A/B/C, MVCC & Compactor)
- [x] Implement `crates/pulse-storage`:
  - [x] Partitioning (`xxh3_64(routing_seed) % NUM_BUCKETS`)
  - [x] `BucketWal` CRC32-checked WAL writer & startup recovery replayer (RPO = 0)
  - [x] `MemBucket` L0 hot RAM buffer with row-count / age / manual flush
  - [x] Immutable Vortex-backed Tier A/B/C segment writer & reader sorted by `(uid_internal, session_seq, event_ts_ms)`
  - [x] Per-segment inverted `RoaringBitmap` indexes (Tier B) + 64-bit Bloom filter (Tier C) + `SessionIndex` 64-bit event Bloom footer
  - [x] MVCC `Manifest` snapshot isolation & background late-data `Compactor` (>= 4 overlapping segments)
  - [x] `ReplayStore` append & byte-range reader (`.plsr`)
- [x] Implement `crates/pulse-ingest`:
  - [x] `POST /v1/ingest` (binary `application/x-pulse-frame` + `application/json`), `POST /v1/identify`, `POST /v1/replay/chunk`
  - [x] QUIC (`quinn`) UDP listener sharing ingestion service handler
  - [x] Lexicon governance enforcement (drop events, mask PII, async discovery)
  - [x] Rotating 15-minute deterministic deduplication ring & clock-skew normalizer
- [x] **Acceptance Gate 2**:
  - [x] 500,000 events with 20% duplicates, 15% out-of-order late timestamps, polymorphic `amount` (`Int`/`Float`/`Str`)
  - [x] Abrupt restart WAL replay recovery test (100% restored, 0 duplicates)
  - [x] L0 flush + compaction + `SessionIndex` skipping > 90% of non-matching sessions
  - [x] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p pulse-storage -p pulse-ingest --release`

## Milestone 3: `pulse-exec` (Insights, Funnels, Retention, Flows, Cohorts, DataFusion Formulas & Lookups)
- [x] Implement `crates/pulse-exec`:
  - [x] `foyer` hybrid memory + disk cache integration for segment reads
  - [x] Global pre-filter & recursive `BehavioralCohort` `RoaringBitmap` evaluator
  - [x] `InsightsEngine` (Uniques, Totals, Sessions, DAU/WAU/MAU, Sum/Avg/Min/Max/p50/p90/p99 via `DDSketch`, breakdowns, previous period, formulas)
  - [x] `FunnelEngine` (strict/loose/any, conversion window, exclusions, hold-constant, first-time-ever, cross-bucket stitch, TTC `DDSketch`, dropped sample sessions)
  - [x] `RetentionEngine` (`n_day`, `unbounded`, `frequency_stickiness`)
  - [x] `FlowsEngine` (forward +N / reverse -N steps, consecutive collapse, Sankey nodes/links)
  - [x] Custom Events, DataFusion expression Custom Properties, and Lookup Table dictionary joins
- [x] **Acceptance Gate 3**:
  - [x] Differential correctness tests matching brute-force reference oracle 100% identically
  - [x] 1,000,000 event 4-step Funnel benchmark < 50ms in `--release` (measured 4.17ms warm / 14.63ms cold)
  - [x] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p pulse-exec --release`

## Milestone 4: `pulse-control`, `pulse-sdk-core`, `sdks/ts-web`, and `pulse-node`
- [x] Implement `crates/pulse-control`:
  - [x] SQLite/Postgres `sqlx` schema & migrations (Orgs, Projects, API Keys, Lexicon, Custom Events/Props, Lookup Tables, Cohorts, Saved Reports, Dashboards, Feature Flags, Experiments)
  - [x] Frequentist Z-test + Bayesian Monte Carlo Beta-Binomial Experimentation engine
  - [x] Deterministic ETag-cached Edge Flag Bundle endpoint (`GET /v1/flags/:project_api_key`)
  - [x] Built-in MCP JSON-RPC 2.0 server (`POST /v1/mcp` with 5 tools)
- [x] Implement `crates/pulse-sdk-core` & `sdks/bindings-gen`:
  - [x] Thread-safe `PulseClient` (ring buffer, bounded 10 MB disk WAL, 30-min session rotation, Rage Tap & Dead Tap detector, `████` PII masking)
  - [x] UniFFI Kotlin & Swift binding generator CLI
- [x] Implement `sdks/ts-web`:
  - [x] Zero-dependency TypeScript SDK + DOM wireframe walker + PII regex/attribute masker + rage-click listener
- [x] Implement `crates/pulse-node`:
  - [x] Unified single-binary Control + Data Plane + QUIC + Static UI server
  - [x] Realistic 100,000+ event Indian Fintech & Quick-Commerce deterministic auto-seeder
- [x] **Acceptance Gate 4**:
  - [x] Full `pulse-node` ephemeral integration test (SDK batches, wireframe diffs, `identify`, Lexicon drop, A/B lift, MCP tools)
  - [x] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --release`

## Milestone 5: `web/` Full Analytics Studio, Production Build & End-to-End Verification
- [x] Scaffold `web/` (Vite + React + TypeScript + Tailwind dark-mode `#090d16` studio)
- [x] Build Global Topbar + all 8 Workspaces:
  - [x] Insights
  - [x] Funnels (with clickable Drop-Off -> Wireframe Replay drawer)
  - [x] Flows (interactive SVG Sankey diagram)
  - [x] Retention (color-graded Cohort Heatmap Triangle)
  - [x] Sessions & Interactive Phone Wireframe Player
  - [x] Cohorts & Lookup Tables
  - [x] Experiments & Feature Flags
  - [x] Lexicon & Ask Pulse (MCP Playground)
- [x] Wire `pulse-node` static asset serving (`web/dist`) & create `scripts/verify_e2e.sh`
- [x] **Acceptance Gate 5**:
  - [x] `npm --prefix web run typecheck && npm --prefix web run build`
  - [x] `cargo test --workspace --release`
  - [x] `scripts/verify_e2e.sh` against live `pulse-node`
