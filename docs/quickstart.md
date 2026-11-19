# Quickstart

Run a Pulse node locally, get credentials, and send a first event.

## 1. Install

**Docker** (Linux x86_64 and arm64 images):

```bash
docker run -p 8080:8080 -v pulse_data:/data \
  -e PULSE_ADMIN_TOKEN="sk_root_$(openssl rand -hex 24)" \
  ghcr.io/sswapnil2/pulse:latest --seed
```

Inside the container the node listens on `0.0.0.0:8080` and keeps everything in `/data`. Drop
`--seed` for an empty node. To build a Docker image locally from source instead, run
`docker build -f Dockerfile.source -t pulse:local .`. Then skip to step 3.

**Release tarball** (Linux x86_64 or aarch64, glibc 2.35+: Ubuntu 22.04+, Debian 12+): download
`pulse-linux-<arch>.tar.gz` from the [latest release](https://github.com/sswapnil2/analytics-system/releases/latest),
check it against `SHA256SUMS`, unpack it, and run `./pulse/pulse-node --web-dir pulse/web/dist`.
The tarball also contains a systemd unit, a Caddyfile and an env template from `deploy/`.

**From source** (macOS, or any other platform). Prerequisites: the Rust toolchain pinned in `rust-toolchain.toml` (rustup picks it up) and Node.js for the Studio UI.

```bash
npm --prefix web ci && npm --prefix web run build   # Studio UI -> web/dist (optional, API works without it)
cargo build --release -p pulse-node                 # -> target/release/pulse-node
```

The release build uses thin LTO with one codegen unit, so the first build is slow (about 15 minutes
on an M-series Mac). For trying things out, `cargo build -p pulse-node` (debug) is much faster to compile.

## 2. Run

```bash
export PULSE_ADMIN_TOKEN="sk_root_$(openssl rand -hex 24)"   # keep this; see below
./target/release/pulse-node --port 8080 --data-dir ./pulse_data --seed
```

`--seed` loads a synthetic demo dataset (an Indian fintech app) into project `proj_fintech_in` and makes
that the default project. Leave it off for an empty node, whose default project is `proj_default`.

Open <http://127.0.0.1:8080> for the Studio. Sign in with the admin token, or create the first account at `/#/signup`.

### Flags and environment variables

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--host <ip>` | `PULSE_HOST` | `127.0.0.1` | Bind address |
| `--port <n>` | `PORT` | `8080` | HTTP port (API, Studio and MCP share it) |
| `--data-dir <path>` | `PULSE_DATA_DIR` | `./pulse_data` | All storage and metadata |
| `--web-dir <path>` | `PULSE_WEB_DIST_DIR` | `./web/dist` | Built Studio files |
| `--project-id <id>` | `PULSE_DEFAULT_PROJECT` | `proj_default` (`proj_fintech_in` with `--seed`) | Project used when a request names none |
| `--seed` / `--no-seed` | `PULSE_SEED=1` | off | Load the demo dataset into an empty default project |
| `--role <role>` | `PULSE_NODE_ROLE` | `all` | `all`, `ingest` (ingest routes + flushing), `query` (admin, query and MCP routes) or `compactor` |
| `--quic-addr <ip:port>` / `--no-quic` | `PULSE_QUIC_ADDR` | off | Optional QUIC/UDP ingest listener |
| | `PULSE_ADMIN_TOKEN` | random each start | Root admin bearer token |
| | `PULSE_CORS_ORIGINS` | the node's own origin + `:5173` | Comma-separated browser origins allowed on admin/query routes |
| | `PULSE_PUBLIC_URL` | `http://localhost:<PORT>` | Public origin used for Google OAuth callbacks, invite/reset links, and `Secure` session cookies |
| | `PULSE_ALLOW_NEW_ORGS` | `true` | Set to `false` on public nodes to stop strangers creating organisations |
| | `PULSE_ALLOWED_EMAIL_DOMAINS` | unset | Comma-separated domains that can auto-join the default organisation |
| | `PULSE_GOOGLE_CLIENT_ID`, `PULSE_GOOGLE_CLIENT_SECRET` | unset | Optional Google OAuth credentials for Studio sign-in |
| | `DATABASE_URL` | SQLite in `PULSE_DATA_DIR` | Metadata database URL (`postgres://...` for multi-node deployments) |
| | `PULSE_OBJECT_STORE_URL` | local `PULSE_DATA_DIR/object_store` | Shared object store (`s3://bucket`, `file://...`, or `memory://`) |
| | `AWS_REGION`, `AWS_ENDPOINT` | unset | S3-compatible region and custom endpoint (e.g. Cloudflare R2, MinIO) |
| | `PULSE_VAULT_KEK` | `.pulse_cluster.kek` next to SQLite | 64-hex key that seals stored secrets. **Required with Postgres or several nodes**; keep it outside the data backup |
| | `PULSE_ALLOW_EPHEMERAL_KEK` | off | `1`/`true` allows sealing secrets without a durable KEK (dev/testing only) |
| | `PULSE_BYOK_MASTER_KEY` | unset | Optional static master key for encrypted object-store BYOK |
| | `PULSE_ENCRYPTION_STRICT` | off | `1` refuses to read unencrypted objects once an encryption key is loaded. Only for stores encrypted from day one |
| | `PULSE_METRICS_TOKEN` | unset | Optional bearer token protecting `GET /metrics` |
| | `PULSE_SHUTDOWN_DRAIN_MS` | `0` | Delay (ms) after `/readyz` turns 503 before closing the HTTP listener |
| | `PULSE_NUM_BUCKETS` | built-in default | Storage buckets per day partition (1–256) |
| | `PULSE_L0_MAX_ROWS_PER_BUCKET` | built-in default | L0 in-memory row cap per bucket before backpressure/flush |
| | `PULSE_INGEST_EVENTS_PER_SEC`, `PULSE_INGEST_BURST_SECS` | `20000`, `5.0` | Per-project ingest token-bucket rate limit (`<=0` disables) |
| | `PULSE_QUERY_TIMEOUT_MS`, `PULSE_QUERY_MAX_ROWS` | built-in defaults | Per-query deadline (ms) and maximum scanned rows |
| | `PULSE_QUERY_MAX_CONCURRENT`, `PULSE_QUERY_MAX_CONCURRENT_PER_PROJECT` | `cores/2`, `75%` of global | Global and per-project concurrent query limits |
| | `PULSE_ALLOW_PRIVATE_ENDPOINTS` | off | `1` allows outbound webhooks/LLM/warehouse calls to private/loopback IPs (dev/testing only) |
| | `RUST_LOG` | `info,slatedb=warn` | Log filter |

Webhook requests carry `X-Pulse-Timestamp` (Unix seconds) and `X-Pulse-Signature-256: sha256=<hex>`,
the HMAC-SHA256 of `"{timestamp}.{body}"` under your `signing_secret` (at least 16 characters).
Recompute it, compare in constant time, and reject timestamps more than a few minutes old.

## 3. Get the admin token

- If you set `PULSE_ADMIN_TOKEN`, that is the root token.
- If you did not, the node mints a new `sk_root_…` token **on every start** and writes it to
  `root_admin_token` in the data directory, readable only by your user. The log says where. It is
  never printed, so it doesn't end up in shipped or shared logs:

  ```bash
  cat pulse_data/root_admin_token
  ```

  The previous token stops working after a restart (and Studio sessions signed in with it end), so
  set `PULSE_ADMIN_TOKEN` for anything longer-lived than a quick test.

Check it works:

```bash
curl -s http://127.0.0.1:8080/v1/status -H "Authorization: Bearer $PULSE_ADMIN_TOKEN"
```

## 4. Get an ingest key

On first boot of a data directory the node creates an ingest key for the default project and prints it once:

```
Generated the public ingest key for client SDKs: pk_live_…
```

Keys are stored hashed, so a missed log line cannot be recovered. Create a new one in the Studio
(Settings → API keys), or with the API (the response's `plaintext_key` is shown only this once):

```bash
curl -s -X POST http://127.0.0.1:8080/v1/projects/proj_fintech_in/api-keys \
  -H "Authorization: Bearer $PULSE_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"label": "web app", "role": "ingest_and_flags"}'
```

Use `"role": "admin"` instead to get an `sk_live_…` project admin key, which is the right credential
to hand to an AI agent: it can query and administer that one project and nothing else.

## 5. Send an event

```bash
curl -X POST http://127.0.0.1:8080/v1/ingest \
  -H "Content-Type: application/json" -H "x-pulse-key: pk_live_…" \
  -d '{"event": "Signed_Up", "userId": "usr_1", "properties": {"plan": "pro"}}'
```

`/v1/ingest` also accepts Segment `{"batch": [...]}`, top-level arrays and Pulse
`{"events": [...]}` batches. New events are queryable straight away; `POST /v1/admin/flush` forces
in-memory rows to disk segments.

Next: instrument an app with the [web SDK](web-sdk.md), or connect an agent over [MCP](mcp.md).
