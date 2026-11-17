# Pulse Analytics

Single-binary product analytics, session wireframe replay, feature flags, and experimentation engine built in Rust. Zero legacy big-data middleware (no ClickHouse, Kafka, ZooKeeper, or external Redis).

## Architecture at a Glance

- **Entity-centric columnar storage (`pulse-storage`):** WAL durability + immutable Vortex L1 segments partitioned by `project_id / yyyymmdd / bucket_00..15`, physically sorted by `(uid_internal, session_seq, event_ts_ms)`. Background L0 -> L1 flushing uses `spawn_blocking` segment serialization, CAS manifest commits (`commit_manifest_cas`), and HTTP `429 Too Many Requests` (`Retry-After: 1`) under L0 backpressure.
- **Exact unsampled user math & isolated compute (`pulse-exec`, `pulse-identity`):** Dense `u32` user ID interning with SlateDB-backed Union-Find identity merging (`anon_id -> user_id`) supporting group-committed flushes and read-only `DbReader` refresh mode, plus `RoaringBitmap` set operations and Rayon parallel scans isolated from Tokio I/O via `block_in_place` (`run_rayon_compute`) for Funnels, Retention, Insights, Flows, and Behavioral Cohorts.
- **Privacy-by-design wireframe replay (`pulse-wire`):** DOM bounding-box snapshots (`PLSR` byte-range streams) with client-side `"████"` text/input masking, rage-tap / dead-tap detection, and network breadcrumbs.
- **Unified Control Plane & AI MCP (`pulse-control`):** Cookie/session auth + bearer API tokens, Lexicon PII governance, deterministic `xxh3_64` edge feature flags, A/B experimentation with SRM check + Holm-Bonferroni correction, and JSON-RPC 2.0 MCP tools.

## Documentation

- [Quickstart](docs/quickstart.md): build, run, admin token and ingest keys
- [Web SDK](docs/web-sdk.md) and [mobile core (beta)](docs/mobile-core.md)
- [REST API](docs/rest-api.md) and [MCP for agents](docs/mcp.md) (Claude, Cursor, OpenAI Codex and Responses API, Gemini CLI, Antigravity)
- [Claude Code plugin](integrations/claude-code-plugin/README.md): `/plugin marketplace add sswapnil2/analytics-system`, then `/plugin install pulse@pulse`

## 60-Second Quickstart

### 1. Build the Studio UI & Rust Node

```bash
npm --prefix web ci && npm --prefix web run build
cargo build --release --bin pulse-node
```

### 2. Boot with the Pre-Seeded Indian Fintech Dataset

```bash
PORT=8080 PULSE_DEFAULT_PROJECT=proj_fintech_in ./target/release/pulse-node --seed --no-quic
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) in your browser:
- On first launch with a seeded database, sign in with the generated `sk_root_...` admin token printed in startup logs, or create the first workspace account from `/#/signup`.
- Empty projects display an in-app **Send your first event** quickstart panel (also accessible anytime from the terminal icon in the sidebar footer).

## High-Availability & Node Roles

By default, `pulse-node` runs in single-binary `--role all` mode. For horizontal scaling and fault isolation, run dedicated roles via `--role` (or `PULSE_NODE_ROLE`) against a shared data directory (`--data-dir` / `PULSE_DATA_DIR`), or use `PULSE_OBJECT_STORE_URL` with `DATABASE_URL` (`postgres://...`) for multi-host storage and control metadata:

- `all` *(default)*: Full ingest, query/Studio API, L0 -> L1 flushing, and background compaction in one process.
- `ingest`: Serves `/v1/ingest` (HTTP + optional QUIC), `/healthz`, and public flag evaluation; runs background L0 -> L1 flushes and identity graph commits.
- `query`: Serves admin/analytics queries, Studio UI, public flags, and `/healthz`; opens storage and SlateDB identity graphs in read-only (`DbReader`) refresh mode without blocking ingest.
- `compactor`: Refreshes manifests and runs background L1 segment compaction (`--role compactor`, exposing `/healthz`).

```bash
# Example multi-role cluster sharing object storage & control metadata (single-host / shared filesystem)
./target/release/pulse-node --role ingest    --port 8081 --data-dir /shared/pulse_data
./target/release/pulse-node --role query     --port 8080 --data-dir /shared/pulse_data --no-quic
./target/release/pulse-node --role compactor --port 8082 --data-dir /shared/pulse_data --no-quic
```

## Ingest Events (Pulse or Segment JSON)

`/v1/ingest` accepts single event objects, Segment `{"batch": [...]}` payloads, top-level arrays `[...]`, or Pulse `{"events": [...]}` batches authenticated with `x-pulse-key`:

```bash
curl -X POST "http://127.0.0.1:8080/v1/ingest" \
  -H "Content-Type: application/json" \
  -H "x-pulse-key: pk_live_..." \
  -d '{
    "event": "KYC_PAN_Submitted",
    "userId": "usr_1042",
    "properties": { "city": "Bengaluru", "plan": "pro" }
  }'
```

## Run the Workspace Tests & E2E Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --release
npm --prefix sdks/ts-web run typecheck && npm --prefix sdks/ts-web test
npm --prefix web run typecheck && npm --prefix web run build
./scripts/verify_e2e.sh
./scripts/bench_scale.sh all   # runs 100k & 1m SLO profiles (cold/warm scans + concurrent load)
```

### Measured Release Benchmark (`gate3` & `bench_scale.sh 1m`)

- `1,000,000` stored events across `25,000` users, 4-step `LooseOrder` funnel: **`44.49 ms`** cold scan, **`7.98 ms`** warm cache scan (single-node M-series macOS release build).
- Concurrent 1M scale SLOs (`./scripts/bench_scale.sh 1m`): `ingest_throughput` **`226,585 ev/s`**, `ingest_under_load` **`7.24 ms`** p50 / **`42.65 ms`** p99, `query_under_ingest` **`149.43 ms`** p50 / **`237.40 ms`** p99.
