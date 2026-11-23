# Docs status & roadmap

This page records how these docs were checked, what is still unverified, and what to write next. Update it whenever you verify or change a page.

*Last verification: 2026-11-21, against `main` at `95a13e5`.*

## How the docs were built

Every architecture page was written from the source on `main`, not from older docs or the marketing site. Claims name the file that implements them, so you can check them. Where the code and the existing docs disagreed, the code won, and the docs were corrected (see below).

## What was verified, and how

**Against a running node.** A fresh debug build of `pulse-node` (empty data directory) was run, and these were called as documented:

| Check | Result |
|---|---|
| `/healthz`, `/readyz` | `ok`, `ready` |
| Ingest key printed on first boot; `POST /v1/projects/:id/api-keys` | Returns `plaintext_key` once |
| Quickstart `curl` to `/v1/ingest` | `{"status":"ok","accepted":1,…}` |
| Server-side example in [Integrations](integrations.md#server-side-events-over-http), sent twice with the same `messageId` | Both `/v1/ingest` responses return `accepted: 1, deduped: 0` (W1 async ack); `pulse_ingest_events_deduped_total` increments by `1` after the 20 ms WAL flush |
| Batch with one invalid event | `200` with `rejected: [{index: 1, error: "missing event name…"}]` |
| Unknown ingest key / ingest key on an admin route / 9 MiB body | `401` / `403` / `413` |
| Insights query from the Integrations page; `sampling` set to `"exact"`, `"auto"`, `{"rate":0.25}` | Returns series and a `sampling` object |
| `/v1/identify` merging two identified users | `409` with the documented message |
| `/v1/engage`, `/v1/identify`, `/v1/groups`, `/v1/replay/chunk` (W2 sync durability) | `/v1/identify` and `/v1/engage` flush identity before writing profiles, return `canonical_uid` (`canonical_uids` on batches), read back immediately on the next request, survive `SIGKILL`, and return `503` on `ProfileStore` / `hot/profiles/` unavailability |
| `DELETE /v1/projects/:id/users/:distinct_id`, then `GET …/deletions` | Erasure recorded with `events_purged` |
| `/metrics` | `pulse_ingest_events_*`, `pulse_writer_*`, `pulse_wal_bytes`, `pulse_ingest_apply_*`, `pulse_l0_rows`, `pulse_ready` present |
| MCP `tools/list` | The 9 tools listed in [MCP](mcp.md) |
| `GET /v1/flags/:key` | `200` with a weak `ETag` |

**The website.** Every page in the navigation and every ADR was loaded in a browser:

- All 9 Mermaid diagrams render.
- Every link between docs pages, and every heading anchor, resolves. One broken anchor was found and fixed.
- Every link from `docs/` to elsewhere in the repo points at a file that exists.
- Search works, and deep links to headings work.
- The layout works at phone width.

**Corrections made to existing pages:**

- `rest-api.md`: the ingest body limit is 8 MiB (`413` above that), not 64 MB. 64 MiB is the decoded-batch limit.
- `web-sdk.md`: flag assignment hashes `flag_key` + the current distinct id, not the routing seed. This is why `identify()` can change a variant.
- `web-sdk.md`: `$current_url` and `$referrer` are sent **without** the query string. The old text said the query string was included.
- `quickstart.md`: the long env-var table moved to [Configuration](configuration.md), which now lists every variable the code reads.

## What remains unverified

These statements come from reading code; they were not exercised:

- **Multi-node.** Running several ingest nodes against one project. The docs deliberately make no safety claim.
- **S3 and Postgres.** The `PULSE_OBJECT_STORE_URL=s3://…` and `DATABASE_URL=postgres://…` paths were not run.
- **Encryption.** `PULSE_BYOK_MASTER_KEY` encryption, key rotation, and whether rotated keys survive a restart.
- **Google sign-in, Copilot, webhooks, warehouse export.** None of these was exercised (they need external credentials).
- **Session replay.** The end-to-end replay path and the Studio player.
- **Mobile.** The mobile core and the iOS and Android examples (the Android example is marked unverified upstream). React Native was not reviewed beyond its source.
- **Web SDK snippets.** These were checked against the SDK's exported API and its CI tests, not run in a fresh app.
- **Release binaries.** For v0.1.2, all four assets were downloaded through the `releases/latest/download` URLs in the [Quickstart](quickstart.md#1-install): both Linux tarballs, the SDK `.tgz` and `SHA256SUMS`. Every checksum matched, the tarball layout matched the docs, and the binary contains the query-sampling code. The binaries were not run here (Linux binaries, macOS machine); release CI smoke-tests both architectures with `/healthz`. The Docker image was published by CI but not pulled here.
- **Performance.** No performance numbers were reproduced. The docs quote none beyond pointing to the SLO test.

## Known documentation gaps

- **React Native.** No page; it should be documented or labelled experimental in its package.
- **Full API reference.** The REST API page lists routes, but it is hand-written. There is no OpenAPI spec to generate request and response schemas from.
- **Query bodies.** Only a few query body shapes are spelled out; the full schema lives in `crates/pulse-exec/src/ast.rs`.
- **Studio guide.** No page per workspace (Trends, Funnels and so on).
- **Upgrades.** No upgrade or migration guide between releases.
- **Backup and restore drill.** `deploy/backup.sh` exists, but there is no restore procedure.

## Roadmap

1. **Generate an OpenAPI spec** from the Axum routes, and render the API reference from it, so the docs can't drift from the routes.
2. **Extend the docs check in CI.** `scripts/check_docs_links.py` (the `docs-links` job) already fails on broken links, anchors and nav entries. Next: render the Mermaid diagrams, and run the quickstart `curl`s against a fresh node.
3. **Document React Native**, or mark it experimental, and add it to CI.
4. **Write a backup-and-restore runbook**, and test it.
5. **Write a multi-node deployment guide** once a topology is tested (see [ADR-0008](adr/0008-node-roles.md)).
6. **Write a Studio tour:** one short page per workspace, with screenshots.
7. **Write the suggested ADRs** in [Design decisions](adr/README.md#suggested-future-adrs).
8. **Keep the sequential-testing and experiment-metrics docs in step** as those pull requests merge, and update [Limitations](limitations.md#experiments-on-main).
