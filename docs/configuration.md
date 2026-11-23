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
| | `PULSE_WRITER_LEASE_TTL_SECS` | `15` (min `3`) | How long a project's writer lease lasts without renewal: how long another ingest node, or this node restarted after a crash, must wait before it can write the projects a dead holder had. Running a standby is not supported yet ([limitations](limitations.md#operational-limitations)). Renewed every third of it; node clocks must agree to within a third of it. See [limitations](limitations.md#operational-limitations) | `pulse-control/src/db.rs` |
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
| `DATABASE_URL` | `sqlite://<data_dir>/pulse_meta.db` | Metadata DB. Use `postgres://…` for multi-node or recoverable `PULSE_HOT_STORE=local` deployments. On `PULSE_HOT_STORE=local` with SQLite, `pulse-node` logs a startup warning and `pulse-node restore --from-published` refuses to run |
| `PULSE_OBJECT_STORE_URL` | `<data_dir>/object_store` (fsynced local files) | `s3://bucket`, `file:///path` or `memory://` |
| `PULSE_HOT_STORE` | `local` | Where the hot tier lives: event and agent-trace write-ahead logs (`wal/`, `wal_corrupt/`, `wal_dead/`, `agent_wal/`, `agent_wal_corrupt/`), local L0-flushed segments (`hot/segments/`), local authoritative manifests and upload intent records (`manifests/`), and the SlateDB identity graph and profile stores (`identity/`, `profiles/`). `local`: fsynced files in `<data_dir>/wal` and `<data_dir>/hot`, so acknowledging a request or committing an L0 flush writes no WAL, hot-segment, or manifest objects to S3. `object`: keeps all hot prefixes in the object store. The chosen mode is recorded in `<data_dir>/hot_store_mode` and switching modes on an initialized data directory (or starting a writable node in `local` mode without its local `identity/`, `profiles/`, and `manifests/` stores, or with an uninitialized `<data_dir>` when `published/` exists) is refused at startup (`--role query` skips the wiped-directory refusal and restores published snapshots into `<data_dir>/query_hot`). Write-open checks S3 `published/` (`publish_seq`, `writer_generation`, `source_local_epoch`) against the local manifest to fence deposed or stale writers (with step-6 self-crash recovery), replays any local `upload_intent.json` to remove orphaned S3 upload artifacts, and sweeps unreferenced local `hot/segments/` files. Recovering a wiped `<data_dir>` from S3 requires `pulse-node restore --from-published [--data-dir <path>] [--project-id <id>]` with an external PostgreSQL `DATABASE_URL` (SQLite is refused). Use `object` (or put `<data_dir>` on a volume every ingest machine can mount) if another machine may take over a project's lease. See [limitations](limitations.md#operational-limitations) |
| `PULSE_UPLOAD_INTERVAL_SECS` | `300` | Seconds between 7-step local-to-S3 upload and `published/` manifest publish cycles (`0` = upload and publish eagerly on every flush while keeping `manifests/` local-authoritative and count-based compaction active; when `> 0`, count-based compaction on `hot/segments/` is disabled because each upload merges frozen local segments per `(date, bucket)` up to `PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES` = `64 MiB`). Closed dates (`date < today_utc`) upload on `PULSE_CLOSED_DATE_UPLOAD_SECS` (`3600` s) or immediately when their local bytes exceed `PULSE_CLOSED_DATE_MAX_BYTES` (`4 MiB`). Superseded S3 segments and S3 snapshot blobs are retained for 2 publish intervals after a superseding `PublishedManifest` lands (and while pinned locally) before deletion (`pulse-storage/src/engine.rs`) |
| `PULSE_COLD_MERGE_IDLE_SECS` / `PULSE_COLD_MERGE_INTERVAL_SECS` / `PULSE_COLD_MERGE_MAX_INPUT_BYTES` | `3600` / `300` / `67108864` (64 MiB) | Cold S3 `(date, bucket)` merge policy on owning writable nodes (`all` and `ingest`): merges closed-date (`date < today_utc`) multi-segment S3 partitions with no pending `hot/segments/` once idle for `PULSE_COLD_MERGE_IDLE_SECS` (`3600` s since last upload to that partition), sequentially in `<= PULSE_COLD_MERGE_MAX_INPUT_BYTES` (`64 MiB`, falling back to `PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES`) batches, checked every `PULSE_COLD_MERGE_INTERVAL_SECS` (`0` disables background cold merge). Superseded S3 segments obey the 2-publish-interval T2 deletion grace, and idle projects execute 0 S3 calls (`pulse-storage/src/engine.rs`) |
| `PULSE_HOT_SEGMENTS_PROJECT_MAX_MB` / `PULSE_HOT_SEGMENTS_MAX_MB` | `512` / `2048` | `1x` bounds on local `hot/segments/` bytes awaiting S3 upload, per project and per node. Crossing `1x` wakes the upload cycle early (with failure cooldown if S3 is unreachable); reaching `2x` (`1024` / `4096` MiB) returns `429` (`hot_segment_backpressure`) on event ingest |
| `AWS_REGION`, `AWS_ENDPOINT` (+ standard `AWS_*` credentials) | region `ap-south-1` | S3-compatible region and endpoint (R2, MinIO, …) |
| `PULSE_NUM_BUCKETS` | `16` (1–256) | Storage buckets per day partition. Choose before you ingest real data |
| `PULSE_L0_PROJECT_MAX_MB` / `PULSE_L0_MAX_MB` | `512` / `1024` | Estimated bytes of in-memory rows, per project and per node, before ingest returns `429` (`l0_backpressure`). A bucket is flushed when it reaches the project limit ÷ (2 × buckets) |
| `PULSE_LOG_LAG_PROJECT_MAX_MB` / `PULSE_LOG_LAG_MAX_MB` | `256` / `512` | Estimated bytes of WAL-logged events not yet applied by the per-bucket processor, per project and per node, before ingest returns `429` (`log_backpressure`) |
| `PULSE_L0_MAX_ROWS_PER_BUCKET` | `1,000,000` (4 × the 250k flush threshold) | In-memory rows per bucket before ingest returns `429` |
| `PULSE_FLUSH_CONCURRENCY` | `4` | Maximum concurrent background L0-to-segment flushes across the node |

## Ingest

| Env var | Default | Meaning |
|---|---|---|
| `PULSE_WRITER_FLUSH_MS` | `20` | Max delay (ms) before the W1 writer memory buffer flushes queued `/v1/ingest` events to the WAL |
| `PULSE_WRITER_FLUSH_BYTES` | `1048576` (1 MiB) | Byte threshold at which the W1 writer memory buffer flushes immediately to the WAL |
| `PULSE_INGEST_BUFFER_PROJECT_MAX_MB` / `PULSE_INGEST_BUFFER_MAX_MB` | `64` / `256` | Per-project and node-wide bounds on queued W1 writer buffer bytes before `/v1/ingest` returns `429` (`ingest_buffer_backpressure`) |
| `PULSE_INGEST_EVENTS_PER_SEC` | `20000` | Per-project token-bucket rate, per node. `<= 0` disables it |
| `PULSE_INGEST_MAX_INFLIGHT` | `256` | Concurrent ingest requests per node (events, replay and agent traces). Beyond it the node answers `503` with `Retry-After: 1`, which the SDKs retry. `0` removes the cap |
| `PULSE_INGEST_BURST_SECS` | `5` | Burst size, in seconds of rate |
| `PULSE_AGENT_L0_MAX_MB` | `512` | Unflushed agent-trace bytes the node may hold in memory across all projects. Above it, agent ingest answers `429` (`l0_backpressure`) and every shard is flushed at once. The figure is an estimate; measured process memory is about twice the limit in use plus roughly 250 MB for flush buffers and caches |
| `PULSE_AGENT_L0_PROJECT_MAX_MB` | `256` | The same limit for one project, so a single tenant cannot use the whole node budget |

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
| `object_store/` | Flushed event segments, `published/` manifests and snapshots, and replays (unless `PULSE_OBJECT_STORE_URL` is set; with `PULSE_HOT_STORE=object`, also holds WALs, manifests, identity graph and profiles) |
| `wal/` | Write-ahead logs, dead-letter records and corrupt-WAL quarantine for events and agent traces (with `PULSE_HOT_STORE=local`, the default). Must be on a persistent volume. Empty after a clean shutdown |
| `hot/` | Local SlateDB stores (`identity/` and `profiles/`), local-tier `hot/segments/`, authoritative `manifests/`, and `manifests/*/upload_intent.json` when `PULSE_HOT_STORE=local` (the default). Must be on a persistent volume |
| `query_hot/` | Ephemeral local Candidate B snapshot copies (`identity/` and `profiles/{project_id}/pub_{seq}`) restored by `--role query` nodes from S3 `published/` |
| `hot_store_mode` | Persistent mode marker (`local` or `object`) written at initialization to prevent silent mode switches or volume-loss UID reuse |
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
