# Quickstart

Run a Pulse node locally, get credentials, and send a first event.

## 1. Build

Prerequisites: the Rust toolchain pinned in `rust-toolchain.toml` (rustup picks it up) and Node.js for the Studio UI.

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
| | `PULSE_NUM_BUCKETS` | built-in default | Storage buckets per day partition (1–256) |
| | `RUST_LOG` | `info,slatedb=warn` | Log filter |
| | `PULSE_VAULT_KEK` | `.pulse_cluster.kek` next to SQLite | 64-hex key that seals stored secrets. **Required with Postgres or several nodes**; keep it outside the data backup |
| | `PULSE_ENCRYPTION_STRICT` | off | `1` refuses to read unencrypted objects once an encryption key is loaded. Only for stores encrypted from day one |

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
