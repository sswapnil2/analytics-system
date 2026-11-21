# Configuration

`pulse-node` reads command-line flags and environment variables. A flag overrides the matching env var. An unknown flag is an error. Every value below was checked against the code (`grep env::var` across `crates/`), and the source file is given for each one.

## Process and network

| Flag | Env var | Default | Meaning | Source |
|---|---|---|---|---|
| `--host <ip>` | `PULSE_HOST` | `127.0.0.1` (`0.0.0.0` in the Docker image) | HTTP bind address | `pulse-node/src/lib.rs` |
| `--port <n>` | `PORT` | `8080` | HTTP port. The API, Studio, MCP, health and metrics all share it | `pulse-node/src/lib.rs` |
| `--data-dir <path>` | `PULSE_DATA_DIR` | `./pulse_data` | All local state | `pulse-node/src/lib.rs` |
| `--web-dir <path>` | `PULSE_WEB_DIST_DIR` | `./web/dist` | Built Studio files. Without them the API still works | `pulse-node/src/lib.rs` |
| `--project-id <id>` | `PULSE_DEFAULT_PROJECT` | `proj_default` (`proj_fintech_in` with `--seed`) | Project used when a request names none | `pulse-node/src/lib.rs` |
| `--seed` / `--no-seed` | `PULSE_SEED=1` | off | Load the synthetic demo dataset into an empty default project | `pulse-node/src/main.rs` |
| `--role <role>` | `PULSE_NODE_ROLE` | `all` | `all`, `ingest`, `query` or `compactor`. See [node roles](architecture.md#node-roles-and-deployment) | `pulse-node/src/lib.rs` |
| `--quic-addr <ip:port>` / `--no-quic` | `PULSE_QUIC_ADDR` | off | Optional QUIC/UDP ingest listener, with a self-signed certificate | `pulse-ingest/src/quic.rs` |
| | `PULSE_SHUTDOWN_DRAIN_MS` | `0` | Wait this long after `/readyz` turns `503` before closing the listener | `pulse-node/src/lib.rs` |
| | `RUST_LOG` | `info,slatedb=warn` | Log filter | `pulse-node/src/main.rs` |

## Accounts and security

| Env var | Default | Meaning |
|---|---|---|
| `PULSE_ADMIN_TOKEN` | A new random `sk_root_…` on **every start**, written to `<data_dir>/root_admin_token` | Root admin token. Set it for anything longer-lived than a test |
| `PULSE_PUBLIC_URL` | `http://localhost:<PORT>` | The public origin: Google OAuth callback, invite and reset links, `Secure` cookies, CSRF origin check |
| `PULSE_CORS_ORIGINS` | `http://127.0.0.1:<PORT>`, `http://localhost:<PORT>` and the same on `:5173` | Comma-separated browser origins allowed on admin routes. Ingest routes allow any origin |
| `PULSE_ALLOW_NEW_ORGS` | `true` | `false` stops strangers creating organisations at sign-up |
| `PULSE_ALLOWED_EMAIL_DOMAINS` | unset | Comma-separated domains whose sign-ups join the default organisation |
| `PULSE_GOOGLE_CLIENT_ID`, `PULSE_GOOGLE_CLIENT_SECRET` | unset | Google sign-in, enabled only when both are set |
| `PULSE_VAULT_KEK` | `.pulse_cluster.kek` next to SQLite | 64-hex key that seals stored secrets. **Required with Postgres or several nodes**. Keep it outside the data backup |
| `PULSE_ALLOW_EPHEMERAL_KEK` | off | `1` allows sealing secrets with no durable KEK. Dev and testing only |
| `PULSE_BYOK_MASTER_KEY` | unset (no encryption) | Encrypts all stored event data. Use 64 hex chars, or a passphrase of 16+ characters (Argon2id with a fixed salt). An invalid value stops the node |
| `PULSE_ENCRYPTION_STRICT` | off | `1` refuses to read unencrypted objects. Only for stores encrypted from day one |
| `PULSE_METRICS_TOKEN` | unset (open) | Bearer token required on `GET /metrics` |
| `PULSE_ALLOW_PRIVATE_ENDPOINTS` | off | `1` lets webhooks, LLM, warehouse and S3 calls reach private or loopback IPs. Dev and testing only |

## Storage

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `sqlite://<data_dir>/pulse_meta.db` | Metadata DB. Use `postgres://…` for multi-node |
| `PULSE_OBJECT_STORE_URL` | `<data_dir>/object_store` (fsynced local files) | `s3://bucket`, `file:///path` or `memory://` |
| `AWS_REGION`, `AWS_ENDPOINT` (+ standard `AWS_*` credentials) | region `ap-south-1` | S3-compatible region and endpoint (R2, MinIO, …) |
| `PULSE_NUM_BUCKETS` | `16` (1–256) | Storage buckets per day partition. Choose before you ingest real data |
| `PULSE_L0_MAX_ROWS_PER_BUCKET` | `1,000,000` (4 × the 250k flush threshold) | In-memory rows per bucket before ingest returns `429` |

## Ingest

| Env var | Default | Meaning |
|---|---|---|
| `PULSE_INGEST_EVENTS_PER_SEC` | `20000` | Per-project token-bucket rate, per node. `<= 0` disables it |
| `PULSE_INGEST_BURST_SECS` | `5` | Burst size, in seconds of rate |

## Queries

| Env var | Default | Meaning | Source |
|---|---|---|---|
| `PULSE_QUERY_TIMEOUT_MS` | `30000` | Per-query deadline | `pulse-exec/src/engine.rs` |
| `PULSE_QUERY_MAX_ROWS` | `250000000` | Maximum rows one query may scan | `pulse-exec/src/engine.rs` |
| `PULSE_QUERY_MAX_MEMORY_MB` | `1024` | Maximum memory one query may hold | `pulse-exec/src/engine.rs` |
| `PULSE_QUERY_MAX_CONCURRENT` | CPU cores / 2 (min 1) | Concurrent queries per node | `pulse-exec/src/budget.rs` |
| `PULSE_QUERY_MAX_CONCURRENT_PER_PROJECT` | 75 % of the node limit | Concurrent queries per project | `pulse-exec/src/budget.rs` |
| `PULSE_QUERY_PARALLEL_SEGMENTS` | CPU cores, clamped to 2–16 | Segments fetched and decoded in parallel per query | `pulse-exec/src/scanner.rs` |
| `PULSE_AUTO_SAMPLE_MIN_ROWS` | `10000000` | Below this many estimated rows, `auto` sampling stays exact | `pulse-exec/src/scanner.rs` |
| `PULSE_CACHE_MEMORY_MB` | `128` | In-memory segment cache. The disk cache is a fixed 512 MiB at `<data_dir>/foyer_cache` | `pulse-node/src/lib.rs` |

## Files in the data directory

| Path | What it is |
|---|---|
| `object_store/` | Event data, identity graph, profiles, replays (unless `PULSE_OBJECT_STORE_URL` is set) |
| `pulse_meta.db` | SQLite metadata (unless `DATABASE_URL` is set) |
| `root_admin_token` | Generated root token, mode `0600`. Rewritten on every start unless `PULSE_ADMIN_TOKEN` is set |
| `.pulse_cluster.kek` | Generated key-encryption key, if `PULSE_VAULT_KEK` is unset |
| `outbox` | Email-verification and password-reset links. Pulse has no mailer |
| `foyer_cache/` | Query disk cache. Safe to delete while the node is stopped; `deploy/backup.sh` skips it |

## Webhook signatures

Webhook requests carry `X-Pulse-Timestamp` (Unix seconds) and `X-Pulse-Signature-256: sha256=<hex>`,
the HMAC-SHA256 of `"{timestamp}.{body}"` under your `signing_secret` (at least 16 characters).
Recompute it, compare in constant time, and reject timestamps more than a few minutes old.

For a production template, see `deploy/pulse.env.example` and [Self-hosting in the cloud](self-hosting-cloud.md).
