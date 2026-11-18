# Implementation log

Branch `feat/product-improvements`, based on `origin/main` @ `66a0a07`. IDs refer to [implementation-plan.md](implementation-plan.md).

## E-1 User profiles correct at any scale (B-1, B-2, B-3, N-1)

- **Root cause:**
  - The catalog was hydrated from the 1,000 newest profiles.
  - Rows loaded newest-first, so `extend` let the oldest values win.
  - The live write replaced the canonical user's whole map with one id's props.
  - `identify` never re-keyed the anonymous row.
  - Hydration dropped `total_revenue`.
- **Change:**
  - `refresh_user_profiles` loads every profile oldest-first, then merges per canonical uid (newest key wins; revenue summed). After the first load it only reloads users whose rows changed (`updated_at_ms >= last_sync − 60 s`).
  - `rebuild_user_profile` recomputes one user from all of their rows. Live engage, identify and the delta reload all use it.
  - A new `GovernanceProvider::on_identify` hook re-keys the anon and user rows to the canonical uid.
  - Search and pagination run in SQL (`LOWER(...) LIKE ? ESCAPE`), with `?offset=`.
  - The revenue summary is a SQL aggregate.
  - Cohort export uses all merged profiles.
  - Profile write failures are logged instead of discarded.
- **Files:**
  - `pulse-control/src/{api.rs,db.rs,copilot.rs}`
  - `pulse-control/migrations/0008_profile_indexes.sql` (indexes on `(project_id, canonical_uid)` and `(project_id, updated_at_ms)`)
  - `pulse-ingest/src/{governance.rs,service.rs}`
- **Tests:** `pulse-node/tests/correctness.rs`. With 1,200 profiles, `user.tier = gold` is 100 both live and after a restart (it was 0 after a restart before the fix). The breakdown has no `(none)` bucket. A later `$set` wins. An anonymous `$set` followed by identify shows on the user. `search=p0005` finds the user.

## E-2 Query nodes reload control state (B-11)

- **Change:** the query-role background loop calls `reload_caches_from_db` every 5 s. That reloads API keys (so revocation applies), erasures, cohorts, the time zone, and the profile delta. Reload failures are logged.
- **Files:** `pulse-node/src/lib.rs`.
- **Tests:** the `ha_02` split-role test now writes `$set` through the ingest node and polls the query node until `user.plan = gold` resolves. It **fails without the change** (verified by disabling the reload line).

## E-3 Import keeps historical timestamps (B-4)

- **Change:** `/v1/projects/:id/import` clears `client_sent_ts_ms` before ingest, so the mobile skew correction never applies to backfills. Live `/v1/ingest` is unchanged.
- **Tests:** `correctness.rs`: an event with `timestamp` and `sentAt` both on Nov 2 is found on Nov 2.

## E-4 Partial-success batches, Segment call types, size limits (I-8, I-11, I-12, I-14)

- **Change:**
  - `WireBatch::decode_json` keeps the valid events and reports the others as `rejected: [{index, error}]`. It returns a 400 only when nothing is valid. NDJSON and CSV import do the same per line or row.
  - `IngestResponse.rejected` is a new field, omitted when empty, so existing clients see the same JSON.
  - Segment `page` → `$pageview`, `screen` → `$screen`, keeping `name` as a property.
  - `identify`, `alias` and `group` are refused per item with the endpoint to use.
  - Event and property names are capped at 255 characters; string values are truncated to 8 KiB.
- **Files:** `pulse-wire/src/{batch.rs,event.rs,lib.rs}`, `pulse-ingest/src/service.rs`, `pulse-control/src/api.rs`.
- **Tests:** `correctness.rs`: a 5-item batch with a missing name, a Segment identify, a Segment page and a 300-character name stores 2 events and rejects indexes `[1, 2, 4]`. The page shows up as `$pageview`.

## E-5 Equality coercion consistent (B-5)

- **Change:** `precompute_segment_row_mask` skips the exact-string inverted index when the target string parses as a number. The coercing row predicate then decides, as it already did for in-memory rows.
- **Files:** `pulse-exec/src/scanner.rs`.
- **Tests:** `correctness.rs`: `amount eq 499`, `eq "499"` and `eq "499.0"` all give 30, live and from disk segments. The last one was 28 before the fix.

## E-6 Time zone validation (B-9)

- **Change:** project creation rejects names that aren't IANA zones, with an example in the message.
- **Files:** `pulse-control/src/api.rs`, `pulse-control/Cargo.toml` (workspace `chrono-tz`, no new crate).
- **Tests:** `correctness.rs`: `Mars/Olympus` → 400; `Asia/Kolkata` is accepted.

## E-7 Hour buckets in the project time zone (B-10)

- **Change:** `TimeInterval::align_floor_tz` floors hours on the local clock, using the UTC offset at that instant.
- **Files:** `pulse-exec/src/ast.rs`.
- **Tests:**
  - Unit test `hour_buckets_follow_the_local_clock`: IST floors to `:30` UTC, and New York stays contiguous across the Nov 1 DST change.
  - `correctness.rs`: the TZ probe lands in the 19:30 UTC hour bucket. It was 20:00 before the fix.

## E-8 MCP that an agent can rely on (B-8, M-1, M-4, M-6, N-2)

- **Change:**
  - Unknown event names return `isError` with the closest real name ("Did you mean …"). Known names are the union of stored events, the Lexicon and custom events; `$any_event` is allowed.
  - An unknown `metric`, `order_mode` or `mode` is an error.
  - Descriptions are rewritten for the model.
  - `start_ts_ms`/`end_ts_ms` and `compare_previous_period` are advertised in `inputSchema`.
  - `days` is computed in the project's time zone.
  - Text output is compact JSON.
- **Files:** `pulse-control/src/mcp.rs`.
- **Tests:** `correctness.rs`: `query_insights` with `"Payment Complete"` returns `isError: true` and the message names `Payment Completed`.

## E-9 Correctness release gate (P0-6)

- `crates/pulse-node/tests/correctness.rs` boots a full node on a temp directory. It creates an IST project and ingests the review's e-commerce fixture (rebuilt in Rust) and 1,200 profiles. It checks:
  - funnels: 7d, 14d, split by plan;
  - dedup by id and by content;
  - the anonymous merge;
  - null semantics;
  - coercion;
  - retention D1/D7;
  - IST day and hour buckets;
  - user properties;
  - search;
  - import timestamps.

  It then restarts the node and checks everything again.
- It runs as part of `cargo test --workspace`, so the existing CI (`rust.yml`) gates on it. It takes about 2 s.
- **Proof that it catches the bugs:** run with soft asserts against the original code, it reports 17 failures across B-1, B-2, B-3, B-4, B-5, B-8, B-9, B-10, I-8 and I-11/12. With this branch, everything passes.

## E-10 Filter and split by user properties (P0-2 remainder)

- **Change:** `GET /schema-extensions` returns `user_properties` (catalog profile keys). The Studio adds them as `user.<key>` to the shared property list, which feeds the filter panel and every "Split by" (Funnels, Trends, Retention, Journeys). Value suggestions already work, because they use a breakdown query.
- **Files:** `pulse-exec/src/catalog.rs`, `pulse-control/src/{api.rs,models.rs}`, `web/src/{App.tsx,api/client.ts}`.

## E-11 Trends granularity (P1-2)

- **Change:** a Group by selector (Hour/Day/Week/Month) in Trends, saved with the report and restored. Bucket labels, the tooltip and the table caption follow the interval.
- **Files:** `web/src/workspaces/InsightsWorkspace.tsx`.

## E-12 Docs truthfulness (P1-8)

- **Changes:**
  - `quickstart.md`: the key screen exists.
  - `web-sdk.md`: the queue is in memory; SPA pageviews.
  - `rest-api.md`: a new "Ingest semantics" section (partial success, Segment types, limits, dedup and retry rules, import timestamps, sessions), plus users search and pagination and `user_properties`.
  - `mcp.md`: errors, ranges and comparison.
- Removed `FEATURE_GAP.md`. It was never on `origin/main` and the review itself calls it mostly stale.

## E-13 SDK SPA pageviews (P0-4 part)

- **Change:** with pageview autocapture on, `history.pushState`/`replaceState` are wrapped and emit one `$pageview` per new path + query. `shutdown()` restores the originals.
- **Files:** `sdks/ts-web/src/index.ts`.
- **Tests:** a new SDK test: pushState to a new path → +1, same-URL replaceState → +0, query change → +1, and history is restored on shutdown.
