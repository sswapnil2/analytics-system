# Pulse Analytics

Single-binary product analytics, session wireframe replay, feature flags, and experimentation engine built in Rust. Zero legacy big-data middleware (no ClickHouse, Kafka, ZooKeeper, or external Redis).

## How it works

- **Storage (`pulse-storage`):** events land in a write-ahead log, then flush to compressed columnar segments on local disk or any S3-compatible bucket, sorted by user and time so per-user questions are fast.
- **Queries (`pulse-exec`, `pulse-identity`):** funnels, retention, trends, journeys and cohorts are computed exactly, with no sampling. Anonymous visitors are merged into users when they log in, and earlier events follow them.
- **Session replay (`pulse-wire`):** the SDK records page layout (element boxes and masked text), not pixels or HTML. Text is masked on the device. Rage taps, dead taps, errors and network calls are on the timeline.
- **Control plane (`pulse-control`):** Studio accounts and API keys, an event dictionary with PII controls, feature flags, A/B tests with sample-ratio and multiple-comparison checks, and an MCP server for AI agents.

## Documentation

Browse it at **[sswapnil2.github.io/analytics-system/docs](https://sswapnil2.github.io/analytics-system/docs/)**, or read the Markdown in [`docs/`](docs/README.md):

- [Concepts](docs/concepts.md), [architecture](docs/architecture.md), [how Pulse works](docs/how-it-works.md), [data model](docs/data-model.md) and [design decisions](docs/adr/README.md)
- [Security & privacy](docs/security-privacy.md), [configuration](docs/configuration.md), [troubleshooting](docs/troubleshooting.md) and [limitations](docs/limitations.md)
- [Quickstart](docs/quickstart.md): build, run, admin token and ingest keys
- [Web SDK](docs/web-sdk.md) and [mobile core (beta)](docs/mobile-core.md)
- [REST API](docs/rest-api.md) and [MCP for agents](docs/mcp.md) (Claude, Cursor, OpenAI Codex and Responses API, Gemini CLI, Antigravity)
- [Changelog](CHANGELOG.md), [contributing](CONTRIBUTING.md) and [security policy](SECURITY.md)
- [Claude Code plugin](integrations/claude-code-plugin/README.md): `/plugin marketplace add sswapnil2/analytics-system`, then `/plugin install pulse@pulse`

## What you can install today

| Platform | Status |
|---|---|
| Server (`pulse-node` + Studio) | Docker image `ghcr.io/sswapnil2/pulse` and Linux x86_64/aarch64 binaries on every [release](https://github.com/sswapnil2/analytics-system/releases/latest); build from source elsewhere |
| Web SDK | Packed `.tgz` on every release (`npm install <asset url>`, see [Web SDK](docs/web-sdk.md)); not on the npm registry yet |
| Backend / server-side events | Plain HTTP: `POST /v1/ingest` (Pulse or Segment JSON). No server SDKs yet |
| iOS and Android | **No installable SDK yet.** A Rust core with Swift/Kotlin bindings exists for early adopters who build it themselves ([mobile core, beta](docs/mobile-core.md)) |

## 60-Second Quickstart

### 1. Run a node

With Docker:

```bash
export PULSE_ADMIN_TOKEN="sk_root_$(openssl rand -hex 24)"   # keep this: it's your setup code
docker run -p 8080:8080 -v pulse_data:/data -e PULSE_ADMIN_TOKEN ghcr.io/sswapnil2/pulse:latest
```

Or build from source:

```bash
npm --prefix web ci && npm --prefix web run build
cargo build --release --bin pulse-node
PORT=8080 ./target/release/pulse-node --no-quic
```

### 2. Open the Studio

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) and create the first account at `/#/signup`. It asks for a setup code: the `sk_root_…` root admin token in `pulse_data/root_admin_token` (or the `PULSE_ADMIN_TOKEN` you set), so nobody else can claim a fresh node. The Studio then walks you through sending your first event. Anyone who signs up after that gets their own organisation and can't see yours. See [accounts and organisations](docs/README.md#accounts-and-organisations).

To explore with demo data instead, add `--seed`. It loads a sample fintech app (`PULSE_DEFAULT_PROJECT=proj_fintech_in` names its project).

### Open source

Everything is Apache-2.0, with no licence key and no event or seat limits: analytics, replay, flags, cohorts, privacy tools, the MCP server, the AI Copilot, warehouse export, the audit log and webhook alerts.

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
npm --prefix web run typecheck && npm --prefix web test && npm --prefix web run build
./scripts/verify_e2e.sh
./scripts/bench_scale.sh all   # runs 100k & 1m SLO profiles (cold/warm scans + concurrent load)
```

### Measured release benchmark (`bench_scale.sh 1m`)

- `1,000,000` stored events across `25,000` users, 4-step `LooseOrder` funnel: **`44.49 ms`** cold scan, **`7.98 ms`** warm cache scan (single-node M-series macOS release build).
- Concurrent 1M scale SLOs (`./scripts/bench_scale.sh 1m`): `ingest_throughput` **`226,585 ev/s`**, `ingest_under_load` **`7.24 ms`** p50 / **`42.65 ms`** p99, `query_under_ingest` **`149.43 ms`** p50 / **`237.40 ms`** p99.
