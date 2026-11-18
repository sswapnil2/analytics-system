# Validated findings

Each review finding was re-checked against `origin/main` @ `66a0a07` by reading the execution path, and reproduced in a test where marked. Verdicts:

- **CONFIRMED:** the bug is real on current `main`.
- **ALREADY FIXED:** fixed on `origin/main` after the review's build (`a709476`).
- **PARTIAL:** partly addressed.
- **CHALLENGED:** the finding is real, but the proposed fix is wrong or not worth its cost; the reasoning is given.

## Broken (data correctness)

| ID | Finding | Verdict | Root cause (file) |
|---|---|---|---|
| B-1 | `user.*` filters and breakdowns use only 1,000 profiles after a restart | CONFIRMED | `refresh_project_virtual_catalog` hydrates from `list_user_profiles(project, 1000)` (`pulse-control/src/api.rs:797`). The DB helper also clamps to 1,000 (`db.rs:1715`). |
| B-2 | Profile search misses users | CONFIRMED | `handle_list_user_profiles` loads 1,000 rows, then filters in Rust (`api.rs:3227`). Cohort CSV export (`:3554`), the revenue summary (`:3633`) and Copilot (`copilot.rs:396`) have the same cap, so cohort exports lose properties for most members and revenue totals are wrong above 1,000 users. |
| B-3 | Older profiles overwrite newer ones; the anonymous profile isn't merged on identify | CONFIRMED | Hydration iterates `ORDER BY updated_at_ms DESC` and `extend`s, so the oldest row wins per key. Live `upsert_user_profile` `insert`s one `distinct_id`'s props over the canonical user's whole map. `identify` never re-keys the anon row, which keeps its pre-merge `canonical_uid`, so `resolve_user_prop_column` can't find it. |
| N-1 (new) | `user.total_revenue` changes meaning after a restart | CONFIRMED | The live path adds `total_revenue` to the catalog (`api.rs:1037`); hydration doesn't. |
| B-4 | Import restamps history when `sentAt` is present | CONFIRMED | `/import` goes through `ingest_for_project`, which applies `normalize_event_timestamp` (mobile skew correction) to every event (`service.rs:281`). |
| B-5 | `eq 499` / `eq "499"` / `eq "499.0"` disagree | CONFIRMED | The Tier-B string index does exact-string lookups (`scanner.rs::precompute_segment_row_mask`), while the row predicate coerces numerically (`ast.rs::values_equal_coalesced`). The mask must be a superset of the row result and isn't for numeric-looking strings. |
| B-6 | `sessions` metric on events without `session_id` | CONFIRMED, **fix deferred (documented)** | `event.rs:345` defaults to `sess_{distinct_id}`. A real fix is server-side sessionisation (30 min of inactivity). That changes a metric definition across stored data, so it needs its own design. Documented the current semantics instead. Both SDKs send session ids. |
| B-7 | Retries without an id or timestamp double-count | CONFIRMED, **CHALLENGED** | With no id or timestamp, the server can't tell a retry from a second real event. Hashing without the timestamp would merge genuine repeats (two `Login`s in 15 min) and under-count, which is worse. The Web SDK already sets a stable `event_uuid` and timestamp per event. Fix: document that server-side callers must send `insert_id`/`messageId` or `timestamp`, and say so in the API response docs. |
| B-8 | MCP returns 0 for unknown event names | CONFIRMED | `mcp.rs::invoke_tool` never checks names. An unknown `metric` silently becomes uniques. |
| B-9 | An invalid time zone is accepted, then silently treated as UTC | CONFIRMED | `create_project_in_org` stores any string (`api.rs:1562`). `VirtualCatalog::set_timezone` ignores parse failures. |
| B-10 | Hourly buckets ignore the project time zone | CONFIRMED | `align_floor_tz` floors hours in UTC (`ast.rs:117`). That's wrong for zones with a half-hour offset (IST +5:30, NPT +5:45, ACST +9:30). |
| B-11 | Query nodes never reload profiles, keys or erasures | CONFIRMED | Query role loop only calls `refresh_all_manifests` (`pulse-node/src/lib.rs`). Erased users stay visible and revoked keys stay valid on query nodes until restart. |
| B-12 | Erased ids are tombstoned forever | CONFIRMED, **deferred** | Needs time-scoped tombstones plus a physical rewrite during compaction. That's a storage-format change; see remaining-gaps. |
| I-8 | One bad event fails the whole batch | CONFIRMED | `decode_json` uses `collect::<Result<Vec<_>>>()?`. |
| I-11/12 | Segment `identify`/`page` | CONFIRMED | `type` is ignored. `identify` has no event name, so it fails the batch; `page` becomes an event named after its `name`. |
| I-14 | No limits on name or value size | CONFIRMED | Nothing in the `WireEvent` deserializer checks sizes. |
| I-18 | Dedup window is in memory, 15–30 min | CONFIRMED, **deferred** | A durable dedup index for at least 7 days is a storage feature. Recovered WAL fingerprints are already re-seeded on restart. |

## UI, SDK and docs (checked on the current tree)

| Finding | Verdict | Evidence |
|---|---|---|
| No filter bar on reports | **PARTIAL** | `FilterPanel` (property + "in cohort") is used by Funnels, Trends, Retention and Journeys (#11). **Missing:** `user.*` properties in the list (`App.tsx:249-255`) and per-step funnel filters (`FunnelsWorkspace.tsx:101`). |
| Split by user properties or cohort | CONFIRMED | Split lists use event properties only. |
| Cohorts are a dead end | **ALREADY FIXED** | Cohorts feed the filter panel; the builder supports did/didn't/property with all/any (#12). |
| Report state not in the URL | CONFIRMED | The hash router only stores the tab (`App.tsx:116`). |
| First screen is a 0% auto funnel | PARTIAL | Funnel defaults now follow first-occurrence order (#11). Sign-in still lands on Funnels, and the seed has no dashboard. |
| Trends daily only | CONFIRMED | `interval: "day"` is hard-coded (`InsightsWorkspace.tsx:168`). |
| "Last 7 days" header hard-coded | **ALREADY FIXED** | Computed (`InsightsWorkspace.tsx:709`). |
| Retention day-only, ≤13 periods | **ALREADY FIXED** | Day/week/month units and 7/14/30, 4/8/12 and 3/6/12 periods (#12). |
| Drop-off: 15 samples, no cohort or export | PARTIAL | "Save as cohort" exists (#11); samples are still 15 and there's no export. |
| Users page | CONFIRMED | Lives inside Cohorts and shows raw JSON with 20 events. |
| Project creation UI / key revoke | PARTIAL | Revoke exists in API and UI (`SettingsPage.tsx:330`). There's no project-creation UI and no rotate. |
| Experiments empty state, cohort copy | **ALREADY FIXED** | Experiment creation exists (#6/#12). |
| SDK `pushState` | CONFIRMED | Only `popstate` (`sdks/ts-web/src/index.ts:402`). |
| SDK queue persisted (docs claim) | CONFIRMED | The queue is in memory (`index.ts:267`); `docs/web-sdk.md:3` is wrong. |
| SDK auto error, network and replay capture; debug mode | CONFIRMED | None of them exist. |
| npm package publishable | PARTIAL | `sdks/ts-web/package.json` has no `exports`/`files`; nothing publishes it. Publishing needs the owner's npm credentials. |
| Docker image / binaries | CONFIRMED | No Dockerfile, no release workflow. |
| Rust CI | **ALREADY FIXED** | `.github/workflows/rust.yml` (#14). |
| `docs/quickstart.md` "no screen for keys" | CONFIRMED | Line 80. |
| `FEATURE_GAP.md` stale | CONFIRMED (and **introduced by the review PR itself**) | It was never on `origin/main`; commit `07696ff` adds it. It should not be merged as-is. |

## MCP

| Finding | Verdict |
|---|---|
| M-1: jargon descriptions | CONFIRMED (`mcp.rs:96-147`) |
| M-3: typo returns 0 | CONFIRMED (= B-8) |
| M-4: invalid metric becomes uniques | CONFIRMED |
| M-6: no absolute range or comparison in the schema | CONFIRMED: `start_ts_ms`/`end_ts_ms` work but aren't advertised. Relative days are also floored to the UTC day, not the project day (new, N-2). |
| M-2: about 10 KB pretty-printed output | CONFIRMED, low priority. `structuredContent` is the machine channel; the text is pretty JSON. Switching to compact JSON is a one-line change. |
