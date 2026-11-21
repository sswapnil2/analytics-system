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
- **Erasure is delayed on disk.** Erased users are hidden at once, but rows are physically removed only when each affected day and bucket is next compacted. Exports and backups are not touched.
- **Schema caps.** At most 8,192 event names and 16,384 property keys per project. A single batch may add at most 128 new event names or 256 new property keys; going over rejects the whole batch.
- **Size limits.** Event and property names: 255 characters. String values are truncated to 8 KiB. Request body: 8 MiB.
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

- **One tested topology.** The single-node `all` role is the tested path. Ingest, query and compactor roles exist, but this documentation does not verify running several ingest nodes against the same project.
- **Per-node state.** The ingest rate limit, login lockout, dedup windows and OAuth state are kept in memory per node. With several nodes, limits are per node, and an OAuth flow must return to the node that started it.
- **Writer lease (active-standby ingest).** Each project's event store has one writer at a time, chosen by a lease in the metadata database. A second `ingest` or `all` node answers `503` (`code: "not_writer"`) for that project until the holder shuts down cleanly (takeover is immediate) or its lease expires (`PULSE_WRITER_LEASE_TTL_SECS`, default 15 s). Consequences: after a crash, the restarted node also waits out its old lease before it accepts events for the projects it held; when the metadata database is unreachable for longer than two thirds of the TTL, ingest stops until it is back; queries for a project fail on a standby `all` node that does not hold its lease (run standbys as `ingest`, and queries on `query` nodes); node clocks must agree to within a third of the TTL. The lease covers events, identity and the WAL. Session-replay chunks, user profiles and agent traces are not fenced by it.
- **TLS.** No built-in TLS for HTTP. Use a reverse proxy. QUIC uses a self-signed certificate.
- **Encryption.** Event data is encrypted at rest only if you set `PULSE_BYOK_MASTER_KEY`. A passphrase is stretched with a fixed salt. Whether rotated keys survive a restart is not verified.
- **Google sign-in.** The id_token signature is not verified; Pulse relies on the TLS exchange with Google's token endpoint.
- **Performance numbers.** The figures in the README come from one machine and synthetic data. No production-hardware benchmarks are published.

## Platform support

- **Release binaries.** Linux x86_64 and aarch64 with glibc 2.35+ (Ubuntu 22.04+, Debian 12+), plus a multi-arch Docker image. No macOS or Windows binaries; build from source there.
- **Releases lag `main`.** These docs describe `main`. A feature merged after the latest release (listed under *Unreleased* in the [changelog](../CHANGELOG.md)) needs a source build until the next release.
- **Rust toolchain.** Pinned to 1.88 (`rust-toolchain.toml`).
