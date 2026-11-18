# Architecture notes

Traced from the source on `feat/product-improvements` (= `origin/main` @ `66a0a07` + the review docs). Every claim below was read from the code path, not inferred from folder names.

## Important: the review ran against a stale `main`

The PM review (`product-review/`) built and tested `a709476`. That was the local `main` checkout. `origin/main` was 29 commits ahead and already contained:

- #11: the Studio filter panel.
- #12: retention in days/weeks/months, the cohort builder, experiment creation, and setup.
- #13: query deadlines and budgets.
- #14: Rust CI with fmt, clippy and tests.

All work here starts from `origin/main`, and every finding was re-checked against it (see [validated-findings.md](validated-findings.md)).

## Process layout

There is one binary, `pulse-node` (`crates/pulse-node/src/lib.rs::start_pulse_node`). Its `--role` decides what it runs:

| Role | Serves | Background loop (`lib.rs` ~318–360) |
|---|---|---|
| `all` (default) | ingest, query, admin, Studio, MCP | `storage.spawn_background_maintenance` (flush and compact) |
| `ingest` | `/v1/ingest`, `/v1/identify`, `/v1/engage` | every 5 s: flush identities and L0 buckets, then `reload_caches_from_db` (keys, lexicon, catalog) |
| `query` | query, admin, MCP | every 5 s: `refresh_all_manifests` only. **It never reloads keys, profiles or erasures (B-11).** |
| `compactor` | – | every 5 s: refresh manifests, compact |

## Crates and how they talk

```
SDKs (sdks/ts-web, pulse-sdk-core → UniFFI Swift/Kotlin)
   │  HTTP JSON / binary wire (pulse-wire)
   ▼
pulse-ingest::IngestService ── GovernanceProvider (trait) ──► pulse-control::ControlPlaneService
   │  dedup ring (15–30 min, in memory)                          (API keys, lexicon, engage → user_profiles)
   │  identity interning (pulse-identity, durable alias graph)
   ▼
pulse-storage::StorageEngine (WAL → L0 MemBucket → L1 segments, local disk or S3, manifest epochs)
   ▲
pulse-exec::QueryEngine (insights / funnel / retention / flows / cohorts / sessions)
   │  VirtualCatalog per project: tz, custom events/props, lookup tables, saved cohorts,
   │  user profiles (HashMap<canonical_uid, props>), erased uids
   ▲
pulse-control (axum REST: /v1/projects/:id/..., auth, MCP at /mcp, Studio static files)
   ▲
web/ (React + Vite Studio, hash router, `web/src/api/client.ts`)
```

- **Event write path:** `IngestService::ingest_for_project` (`pulse-ingest/src/service.rs:203`) does these steps in order: lexicon rules → bucket routing → dedup pre-check → schema cardinality limits → **clock-skew normalisation** → interning the uid → durable identity flush → WAL + L0 append under the bucket lock → dedup commit → discovery.
- **Decoding:** `WireBatch::decode_json` (`pulse-wire/src/batch.rs:138`) uses the `WireEvent` deserializer (`pulse-wire/src/event.rs:279`). It accepts Pulse, Segment and generic field names. A missing timestamp becomes `now()`, a missing `session_id` becomes `sess_{distinct_id}`, and a missing id becomes a content hash.
- **Profiles:**
  - Writes: `/v1/engage` and `/v1/identify` call `GovernanceProvider::apply_engage`, which is implemented in `pulse-control/src/api.rs` (~986). It reads the row by `distinct_id`, applies `$set`/`$unset`/… and upserts `user_profiles`, then pushes into `VirtualCatalog::upsert_user_profile(canonical_uid, props)`.
  - Reads: on load, `refresh_project_virtual_catalog` hydrates the catalog from `list_user_profiles(project, 1000)`.
  - Queries: `user.<key>` resolves through `VirtualCatalog::resolve_user_prop_column`, which maps the event's raw uid to its canonical uid and then to the profile.
- **Identity:** `pulse-identity` keeps a durable alias graph. A query takes an `IdentitySnapshot`, so a merge applies retroactively to old events.
- **Filters:** a `PropPredicate` (`pulse-exec/src/ast.rs:220`) is evaluated row by row with numeric/string coalescing (`values_equal_coalesced`). For `Eq`, there is also an optional pre-mask from the Tier-B inverted index (`scanner.rs::precompute_segment_row_mask`). The mask must be a superset of the row result, because the row check still runs after it.
- **Time:** `TimeInterval::align_floor_tz` (`ast.rs:116`) aligns days, weeks and months in the project's IANA zone. `VirtualCatalog::set_timezone` ignores names it can't parse, which leaves the zone as UTC.
- **MCP:** `pulse-control/src/mcp.rs`. It speaks JSON-RPC over HTTP POST and is bound to the project of the presenting key. The 5 tools map loosely onto the engine query structs.
- **Auth:** there are root, user-session and project-admin principals. Ingest keys (`sk_live_…`, role `ingest_and_flags`) are only valid on ingest/flags routes. Revoked keys drop out of `api_key_cache` on the next `reload_caches_from_db`.

## Tests that exist

- `pulse-exec/tests`: a proptest oracle against a reference implementation, limits, and scale SLOs.
- `pulse-ingest/tests`: durability and concurrency.
- `pulse-control/tests`: auth, BYOK and licence.
- `pulse-node/src/lib.rs`: full-node gate tests (SDK → ingest → MCP) and a split ingest/query test.
- CI (`.github/workflows/rust.yml`): fmt, clippy `-D warnings`, `cargo test --workspace`.
- The web app has no test runner (`web/package.json` only has build/dev scripts).
