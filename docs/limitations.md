# Limitations

These are the things Pulse does not do, or does with known limits. This list comes from reading the code on `main`. If something here has been fixed, please open a pull request to update this page.

## Not supported

- **OpenTelemetry beyond agents.** The only OTLP receiver is `POST /v1/otel/v1/traces`, for LLM and agent spans ([AI agent observability](agent-observability-architecture.md)). No OTLP metrics or logs, and general service traces are not analysed.
- **Server SDKs.** None for Python, Java, Go, Ruby or Node for product events. Use plain [HTTP](integrations.md#server-side-events-over-http). (Agent tracing has its own SDKs in `sdks/ts-agent` and `sdks/py-agent`.)
- **Packaged mobile SDKs.** The iOS and Android core is beta and has to be built yourself ([mobile core](mobile-core.md)). React Native exists in `sdks/react-native` but is undocumented, untested in CI and not released.
- **Sign-in and identity.** No SSO/SAML, SCIM or MFA. No OAuth on the MCP endpoint, so OAuth-only MCP clients such as ChatGPT connectors cannot connect.
- **Email.** Pulse has no mailer. Verification and reset links are written to a local `outbox` file; invite links are returned to the admin who created them.
- **Not on registries.** The Web SDK is not on npm yet. Install it from the release `.tgz`.

## Data limitations

- **No time-based retention.** Data is kept until you erase a user or delete the storage. There is no TTL setting.
- **Erasure uses tombstones and rows can stay in S3.** Erased users are hidden at once across queries, profile reads, and catalogs (`erased_uids` hides rows with timestamp at or before the erasure time). On local disk or when segments are compacted, rows are physically dropped on the next rewrite of that day and bucket. However, in tiered S3 storage segments are rewritten at most once, so erased rows can stay in S3 segments indefinitely behind the tombstone. External `distinct_id` strings also remain in the identity graph. Exports and backups are not touched.
- **Schema caps.** At most 8,192 event names and 16,384 property keys per project. A single batch may add at most 128 new event names or 256 new property keys; going over rejects the whole batch.
- **Size limits.** Event and property names: 255 characters. String values are truncated to 8 KiB. Request body: 8 MiB.
- **Dead-lettered events.** An event that cannot be turned into a row after its write-ahead-log record is durable (the project's dictionary is full) is written to `wal_dead/<project>/bucket_NNN/seq_<n>_<fingerprint>.plsb` and skipped, instead of failing the request and every later open of the project. There is no tool yet to list or replay these files; each is a one-event `PLSB` batch that can be re-sent to `/v1/ingest` after the cause is fixed.
- **Agent trace size.** One request may carry at most 512 spans per trace. Across requests, a trace accepts 10,000 span records or about 64 MiB (estimated in-memory size), whichever comes first; later spans are dropped, reported in the response's `dropped_spans` and counted in `pulse_agent_spans_dropped_trace_limit_total`. A span sent as start, update and final counts three times. The counts are kept in memory per node and reset on restart.
- **Dedup windows.** `/v1/ingest` deduplicates within 15–30 minutes only. A retry after that is counted again (`/import` remembers ids for 30 days).
- **Changing bucket count.** Do not change `PULSE_NUM_BUCKETS` on a data directory that already has data. Events route by `hash % num_buckets`, so changing it would split one user's history across buckets. This is inferred from the routing code; migration is not documented.
- **Two organisation tables.** `orgs` and `organizations` both exist in the metadata DB. This matters only if you query the DB directly.

## Query limitations

| Limit | Value |
|---|---|
| Query timeout | 30 s (`PULSE_QUERY_TIMEOUT_MS`) |
| Rows scanned per query | 250 M (`PULSE_QUERY_MAX_ROWS`) |
| Memory per query | 1 GiB (`PULSE_QUERY_MAX_MEMORY_MB`) |
| Time buckets per insights query | 10,000 |
| Breakdown groups (insights, funnels) | 250 |
| Retention cohorts | 10,000 |
| Flows: events scanned / per user | 2,000,000 / 4,096 |
| Nested cohort depth | 32 |
| Event export | 50,000 events per call |

- **Sampling.** Queries over 10 M estimated rows are sampled by default (`auto`). Sampled results are disclosed and can be forced exact.
- **HLL estimates.** Long-range unfiltered uniques use HyperLogLog sketches with about 3 % error.
- **No SQL.** The query API is structured JSON (insights, funnel, retention, flows). Custom properties use SQL expressions internally, but you cannot run arbitrary SQL over events.

## Experiments (on `main`)

- **Peeking.** Significance is a fixed-horizon two-proportion z-test with Holm-Bonferroni correction. Repeatedly checking a running experiment inflates false positives. Sequential testing is in review in separate pull requests.
- **Metrics.** Experiment metrics are conversion rates only. Count, sum and ratio metrics are not on `main`.
- **Variant stability.** Assignment hashes the current distinct id, so `identify()` can change a user's variant.

## Operational limitations

- **One ingest node per project.** The single-node `all` role is the tested path. Several `query` nodes over one object store are supported. Only one ingest-capable node (`ingest` or `all`) may write a project: a second one overwrites the first's write-ahead log and loses acknowledged events, and nothing stops it from starting yet ([scalability review](history/SCALABILITY_REVIEW.md), R-01). There is no automatic write failover.
- **Per-node state.** The ingest rate limit, login lockout, dedup windows and OAuth state are kept in memory per node. With several nodes, limits are per node, and an OAuth flow must return to the node that started it.
- **The hot tier (WAL, identity, and profiles) is on local disk by default.** With `PULSE_HOT_STORE=local`, acknowledged events and traces are fsynced to `<data_dir>/wal` and the SlateDB identity graph (`identity/`) and profile store (`profiles/`) are fsynced to `<data_dir>/hot` (including multipart uploads), so request acknowledgement makes zero calls to the segment object store. A process crash loses nothing: the WAL is replayed and local SlateDB state recovers on restart. Losing the `<data_dir>` volume causes an outage rather than silent UID reuse: `<data_dir>/hot_store_mode` records the initialized mode, and the node refuses to start if the mode mismatches `PULSE_HOT_STORE`, if opposite-tier hot objects exist, or if `local` mode's `<data_dir>/hot/identity` or `<data_dir>/hot/profiles` store is missing or wiped while `published/` or other project state exists. To recover a wiped `<data_dir>` from the latest S3 `published/` snapshot, run `pulse-node restore --from-published --data-dir <path>` explicitly — this requires an external PostgreSQL `DATABASE_URL` (SQLite `pulse_meta.db` is refused by `restore --from-published` because accounts, projects, and `erased_uids` tombstones on SQLite are lost with the volume, and `PULSE_HOT_STORE=local` logs a startup warning when running on SQLite) and loses any events, profile updates, identity merges, and erasures after the printed publish timestamp. Put `<data_dir>` on a persistent volume, not on an instance's ephemeral disk. A full disk or unwritable `wal/` or `hot/` directory answers `503` and acknowledges nothing. Running more than one ingest-capable machine for a project (a node that may take over the lease) requires `PULSE_HOT_STORE=object` or a `<data_dir>` on a shared volume: otherwise the machine taking over cannot see the hot state the previous one left. Switching an initialized data directory between `local` and `object` is refused at startup. A `query`-role node on another machine only sees object-store data unless it shares `<data_dir>`.
- **A second ingest node is refused safely; running a standby is not supported yet.** Each project's event store has one writer at a time, chosen by a lease in the metadata database. A second `ingest` or `all` node on the same storage answers `503` (`code: "not_writer"`) for that project instead of overwriting the first node's events, until the holder shuts down cleanly or its lease expires (`PULSE_WRITER_LEASE_TTL_SECS`, default 15 s). A node opens a project's identity and user-profile stores for writing only while it holds that project's lease; a node without it reads them read-only, so it cannot take them from the writer. On such a node `/v1/engage` and `/v1/identify` answer `503 not_writer`, and a user erasure fails with `500`; send them to the writer. Other consequences of the lease: after a crash, the restarted node waits out its old lease before it accepts events for the projects it held; when the metadata database is unreachable for longer than two thirds of the TTL, ingest stops until it is back; a node that does not hold a project's lease cannot query it either (queries belong on `query` nodes); node clocks must agree to within a third of the TTL. The lease covers events, identity, user profiles and the WAL. Agent traces are written under a per-process writer id and session-replay chunks as create-only objects, so neither conflicts with another node.
- **TLS.** No built-in TLS for HTTP. Use a reverse proxy. QUIC uses a self-signed certificate.
- **Encryption.** Event data is encrypted at rest only if you set `PULSE_BYOK_MASTER_KEY`. A passphrase is stretched with a fixed salt. Whether rotated keys survive a restart is not verified.
- **Google sign-in.** The id_token signature is not verified; Pulse relies on the TLS exchange with Google's token endpoint.
- **Performance numbers.** The figures in the README come from one machine and synthetic data. No production-hardware benchmarks are published.

## Platform support

- **Release binaries.** Linux x86_64 and aarch64 with glibc 2.35+ (Ubuntu 22.04+, Debian 12+), plus a multi-arch Docker image. No macOS or Windows binaries; build from source there.
- **Releases lag `main`.** These docs describe `main`. A feature merged after the latest release (listed under *Unreleased* in the [changelog](../CHANGELOG.md)) needs a source build until the next release.
- **Rust toolchain.** Pinned to 1.88 (`rust-toolchain.toml`).
