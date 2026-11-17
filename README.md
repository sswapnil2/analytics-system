# Pulse Analytics

Single-binary product analytics, session wireframe replay, feature flags, and experimentation engine built in Rust. Zero legacy big-data middleware (no ClickHouse, Kafka, ZooKeeper, or external Redis).

## Architecture at a Glance

- **Entity-centric columnar storage (`pulse-storage`):** WAL durability + immutable Vortex L1 segments partitioned by `project_id / yyyymmdd / bucket_00..15`, physically sorted by `(uid_internal, session_seq, event_ts_ms)`.
- **Exact unsampled user math (`pulse-exec`, `pulse-identity`):** Dense `u32` user ID interning with SlateDB-backed Union-Find identity merging (`anon_id -> user_id`) and `RoaringBitmap` set operations for Funnels, Retention, Insights, Flows, and Behavioral Cohorts.
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

## Ingest Events (Pulse, Segment, or Mixpanel JSON)

`/v1/ingest` accepts single event objects, Segment `{"batch": [...]}` payloads, Mixpanel top-level arrays `[...]`, or Pulse `{"events": [...]}` batches authenticated with `x-pulse-key`:

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
```

### Measured Release Benchmark (`gate3`)

- `1,000,000` stored events across `25,000` users, 4-step `LooseOrder` funnel: **`44.49 ms`** cold scan, **`7.98 ms`** warm cache scan (single-node M-series macOS release build).
