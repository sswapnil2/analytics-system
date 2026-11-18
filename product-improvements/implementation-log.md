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

## E-13b SDK production defaults (P0-4 remainder)

- **Change:** the queue persists to `localStorage` (capped, restored on load, in-flight events kept until acknowledged). `window.onerror` and `unhandledrejection` are captured (`captureErrors`, default on). Failed `fetch`/XHR calls are recorded as breadcrumbs with origin + path only (no query string, no bodies). Replay snapshots are taken automatically on DOM change (throttled to 1 s). `debug: true` logs what the SDK does.
- **Files:** `sdks/ts-web/src/index.ts`, `docs/web-sdk.md`.
- **Tests:** `sdks/ts-web/test/sdk.test.mjs` (8 tests): the queue survives a "reload", errors and failed requests are captured, and query strings are stripped.

## E-13c Segment `identify` and `alias` inside batches

- **Change:** batch items of type `identify` and `alias` are applied after the events are stored. `identify` with both ids merges them and applies `traits`; with one id it sets the traits; `alias` merges `previousId`. The response reports `profiles_updated`.
- **Files:** `pulse-wire/src/batch.rs`, `pulse-ingest/src/service.rs`.
- **Tests:** `correctness.rs`: a Segment `identify` in a batch links `seg_anon` to `seg_user`, and an event the anonymous id sends counts for a user with `segment_plan = pro`.

## E-14 Durable dedup for imports (I-18)

- **Change:** `/import` claims every event id in a metastore table (`import_seen`, migration 0009) before ingesting. Only ids not seen before go through. On an ingest failure the claims are released, so a retry isn't counted as a duplicate. Ids are kept for 30 days. Claiming first also makes two concurrent imports of the same file add each event once.
- **Files:** `pulse-control/src/{api.rs,db.rs}`, `migrations/0009_erasure_time_import_dedup.sql`.
- **Tests:** `correctness.rs`: re-importing the same file after a restart (which clears the in-memory window) gives `accepted: 0, deduped: 1`, and the count stays 1.

## E-15 Time-scoped erasure, dropped at compaction (B-12)

- **Change:**
  - Tombstones carry `erased_at_ms` (migration 0009). Rows erased before the migration keep the old meaning (hidden for good).
  - Tombstones moved from the query catalog to the storage layer (`ErasedUids`), so queries and the compactor use one set. The control plane sets them without opening a project's storage.
  - Scanners hide an event only when its raw or canonical id was erased at or after the event's time, in L0 and L1 alike. Session search uses the session start.
  - Compaction drops erased rows from the merged segment, so they leave disk. Compactor nodes now reload control state to get the tombstones.
- **Files:** `pulse-storage/src/{erased.rs,engine.rs,lib.rs}`, `pulse-exec/src/{scanner.rs,engine.rs,catalog.rs}`, `pulse-control/src/{api.rs,db.rs}`, `pulse-node/src/lib.rs`.
- **Tests:**
  - `erased.rs` unit test.
  - `pulse-ingest` `compaction_drops_erased_rows_and_keeps_later_ones` reads the compacted segment back: the erased row is gone and the post-erasure row is kept. It **fails** with the compaction filter removed (verified).
  - `correctness.rs`: erase `er1`, then `er1` sends a new event. The total is 2 (control user + re-signup), live and after a restart. Without erasure it would be 3; with the old permanent tombstone, 1.

## E-16 MCP tools for follow-up questions (P1-7)

- **Change:** four new tools:
  - `query_flows`: next or previous events per step.
  - `get_user`: merged profile, revenue, recent events.
  - `list_property_values`: top values with counts, so an agent filters on values that exist.
  - `list_cohorts`: definitions and sizes.

  Event names are validated with "Did you mean" like the other tools.
- **Files:** `pulse-control/src/mcp.rs`, `docs/mcp.md`.
- **Tests:** `correctness.rs`:
  - `list_property_values(user.tier, Login)` is exactly basic 1,099, gold 100, platinum 1.
  - `get_user(p0700)` shows the trait set while anonymous.
  - `query_flows` after signup includes Product Viewed.

## E-17 Report state in the URL, landing and starter dashboard (P1-1)

- **Change:**
  - Every report page keeps its query in the URL (`#/funnels?q=<base64url>&range=`) with `replaceState`, and restores it on load. A link or a reload opens the same report.
  - Sign-in lands on Dashboards.
  - The demo seed creates an "Overview" dashboard: onboarding funnel, daily active users, day-N retention and a note.
- **Files:** `web/src/dashboards/SaveToDashboard.tsx` (shared `useSavedReport`), the four report workspaces, `web/src/App.tsx`, `pulse-node/src/seeder.rs`.

## E-18 Funnel builder: per-step conditions and an honest empty state

- **Change:**
  - Each step has "+ where" (property is / is not / contains a value), sent as `FunnelStepSpec.filters` and restored from saved reports and URLs.
  - With fewer than two steps the page says "Pick the steps of a journey you care about" instead of "No events in this project yet" (that message now only shows when the project really has no events).
- **Files:** `web/src/workspaces/FunnelsWorkspace.tsx`.

## E-19 Cohort as a Trends split

- **Change:** "Break down by" in Trends lists saved cohorts ("In a cohort or not"), sent as `$cohort:<id>`. Results are labelled "In cohort" / "Not in cohort". Only the Trends engine supports cohort breakdowns, so the option is only offered there.
- **Files:** `web/src/workspaces/InsightsWorkspace.tsx`.

## E-20 Users page (P1-4)

- **Change:** a top-level Users page:
  - SQL search with Previous/Next pages.
  - Columns picked from the project's own most common properties (no hardcoded `plan`/`utm_source`).
  - Merged profile properties (all of a user's ids; the detail endpoint now merges them too).
  - A readable timeline grouped by day, with event properties shown as text.
  - "Play session" inline for recorded sessions.
  - Erase with an explicit confirmation, the erasure log, and CSV export of every matching profile.

  The old JSON panel inside Cohorts is removed.
- **Files:** `web/src/workspaces/UsersWorkspace.tsx` (new), `web/src/App.tsx`, `web/src/workspaces/CohortsLookupsWorkspace.tsx`, `web/src/api/client.ts`, `pulse-control/src/api.rs`.

## E-21 Full drop-off list (P1-3)

- **Change:** the funnel engine records, per step, the users who stopped right before it, in a compressed bitmap (one insert per non-converting user). `POST /v1/projects/:id/query/funnel/dropped?step=N` returns them as CSV or JSON. It needs the admin role, like cohort export. The drop-off drawer has "Download all N" for admins.
- **Files:** `pulse-exec/src/{funnel.rs,ast.rs,engine.rs}`, `pulse-control/src/{api.rs,api/accounts.rs}`, `web/src/workspaces/FunnelsWorkspace.tsx`.
- **Tests:**
  - `correctness.rs`: dropped before step 3 of the 100/80/50/30/20 funnel is exactly 30 distinct users.
  - The route-guard unit test asserts admin.

## E-22 Project creation and key rotation (P1-6)

- **Change:** Settings has a Projects panel for admins: the list, and "Create project" with a time-zone picker (the browser's IANA zones, its own first). The new project's ingest key is shown once, and the app switches to the project. API keys have "Rotate": a new key with the same label and role, then the old one revoked, with a confirmation that says the old key stops at once.
- **Files:** `web/src/account/SettingsPage.tsx`, `web/src/App.tsx`, `web/src/api/client.ts`.

## E-23 Release impact: annotations and range comparison (P1-5)

- **Change:**
  - Annotations: a table (migration 0010) and `GET/POST/DELETE /v1/projects/:id/annotations`.
  - Trends draws them as dashed markers with the text on hover, and lists them under the chart with an add form (members and above).
  - Funnels and Retention have "Compare to": previous period, or specific dates (for example the weeks before a release).
    - Funnels show the compared conversion in the headline ("+x pts now") and "was y%" per step.
    - Retention draws the compared curve dashed in the second-series colour, with both values in the tooltip.
  - The choice is saved with the report and the URL.
- **Files:** `pulse-control/src/{api.rs,db.rs,models.rs}`, `migrations/0010_annotations.sql`, `web/src/components/Annotations.tsx` (new), `web/src/dashboards/ReportCard.tsx` (`CompareControl`), the Trends, Funnels and Retention workspaces.
- **Tests:** `correctness.rs`: create, list by range (in and out), delete.

## E-24 Site claims (P1-8)

- **Change:** `site/index.html` no longer claims group-property splits (not supported) or retention cell drill-down (not built). It now describes range comparison, which ships.

## E-25 Fixes from the Studio pass

- **Change:**
  - `App.tsx`: the first-events reload effect moved above the early returns. Signing out and back in, or a session expiring, blanked the app.
  - `GET /v1/projects/:id/users/:distinct_id?uid=N` looks the user up by canonical uid. The drop-off drawer passes it, because query results only carry masked ids.
  - Funnels: an empty comparison period gets one line instead of "was 0%" on each step.
  - Settings tables: their scroll wrapper is `relative`, so the screen-reader header no longer widens the page on phones.
- **Tests:** `correctness.rs`: user detail by `?uid=` returns the same user and events (fails without the change). The rest were checked in the Studio (see test-results).

## E-26 Product re-review blockers

The product leader re-reviewed `main` after PR 17 (`product-review/re-review.md`, NOT SATISFIED) with three blockers.

- **P1-1 links lose the project.** Every Studio URL now carries `p=<project>`, and the last project is remembered in the browser (`pulse_project`). Dashboard "Share" links include it. Picking a project by hand drops the old project's report from the URL. A link to a project the account can't open falls back to the default project and says so.
  - Files: `web/src/App.tsx`, `web/src/lib/projectUrl.ts` (new), `web/src/dashboards/DashboardsWorkspace.tsx`.
  - Tests: `web/src/lib/projectUrl.test.ts` (`npm --prefix web test`).
- **P1-3 saved drop-off cohort ≠ drop-off list.** New cohort clause `{"kind": "users", "canonical_uids": […]}` (a fixed list, re-resolved through the identity graph on use). `POST /v1/projects/:id/cohorts/from-funnel-dropped?step=N` saves the same bitmap the CSV uses, so step filters, window and order all apply. The drawer uses it. MCP `list_cohorts` summarises a fixed list instead of returning every uid.
  - Files: `pulse-exec/src/{ast.rs,scanner.rs,engine.rs}`, `pulse-control/src/{api.rs,mcp.rs}`, `web/src/workspaces/DropOffInsights.tsx`, `web/src/api/client.ts`.
  - Tests: `correctness.rs` adds a step-filtered funnel, and asserts that the saved cohort's size equals the drop-off count.
- **P0-5 NDJSON import rejected.** `/import` reads the body line by line when the content type says NDJSON/JSON Lines/plain text, or when a `{` body isn't a single JSON value.
  - Files: `pulse-control/src/api.rs`.
  - Tests: `correctness.rs` imports the same NDJSON as `application/x-ndjson` (2 accepted), then `application/json` and `text/plain` (2 deduped each). It fails without the change.
- **Checked in the Studio** (release build, two projects): reload, bare URL, Share link in a clean browser, an unknown project, a pasted link to another project, and saving a cohort from a step-filtered funnel (90 = "Download all 90").

## E-27 Pending items after the re-review

- **Flaky `p1_02` WAL test.** This was a bug in how the test models a crash, not data loss. Dropping the engine left a background L0 flush running, and that flush committed the manifest and purged the WAL while the second engine was recovering. Phase 1 now runs on its own Tokio runtime, which `shutdown_background()` kills mid-write, the way a real crash would. Production code is unchanged.
  - Result: 30 of 30 passes (it failed 24 of 30 before the fix).
- **Times on the project's clock.** These now render in the project's time zone instead of the browser's: the drop-off drawer and its "Left …" list, the Users timeline, session lists, Trends and Retention day buckets, annotations, and every `formatTimestamp`. Timelines name the zone ("12:00 GMT+9"). Before this, a Tokyo project viewed from India labelled each day bucket with the previous day.
  - Files: `web/src/lib/format.ts` (`setProjectTimeZone`, `formatDate`, `formatDateTime`, `formatTime`), `App.tsx`, the Funnels, Users, Sessions, Trends and Retention workspaces, `DropOffInsights.tsx` and `Annotations.tsx`.
  - Left on the browser clock on purpose: the custom date-range picker, and account metadata dates such as dashboard creation.
  - Tests: `web/src/lib/format.test.ts`.
- **Change a project's time zone or name.** New route `PATCH /v1/projects/:id` (Admin). It validates the zone and refreshes the query catalog. In Settings → Projects, each project has a time-zone picker, with a confirm before the change.
  - Tests: `correctness.rs` (bad zone `400`, rename and move, then back); `required_role` unit test.
- **First event confirmation.** When a project goes from 0 events to some, the Studio says "First event received" instead of the setup panel just disappearing.
- **Seed hygiene.** The demo project is named "Demo: consumer lending app", unless the first owner names it at signup. The seed no longer writes events with future timestamps: it slides the whole synthetic timeline back by the overrun (a few hours), so the volume (100k+) and the gaps between each user's events are unchanged.
- **Still open (feature work, not fixes):**
  - Funnel conversion windows beyond 1h/1d/7d/14d.
  - Exclusion steps at any position.
  - Cohort split in Funnels and Retention.
  - Environments.
  - MCP filter schemas.
