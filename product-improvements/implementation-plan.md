# Implementation plan

Scope for this pass: **all P0 data-correctness and integration findings that are fixable without a storage-format change**, the cheapest high-value P1 items, and a correctness regression suite that runs in the existing CI. Items marked *later* are tracked in [remaining-gaps.md](remaining-gaps.md) with the reason.

Complexity: S < 1 day · M 1–3 days · L > 3 days (one engineer).

## P0: critical

| ID | Original finding | Root cause | User impact | Proposed solution | Components | Size | Depends on | Acceptance criteria | Regression risk |
|---|---|---|---|---|---|---|---|---|---|
| E-1 | B-1, B-2, B-3, N-1 (review P0-1) | 1,000-row hydration; DESC + `extend` ordering; live upsert replaces the merged map; anon row never re-keyed; `total_revenue` missing on hydration | Wrong segment numbers above 1,000 users; users can't be found; anonymous traits lost | (1) Hydrate **all** profiles `ORDER BY updated_at_ms ASC`, merging per canonical uid so the newest key wins, and include `total_revenue`. (2) Live writes rebuild that canonical user's merged map from all of its rows, so `$unset` and multi-id merges stay right. (3) On identify, re-key the anon and user rows to the canonical uid and rebuild. (4) Periodic refresh loads only rows `updated_at_ms >= last_sync` and rebuilds those users. (5) Search and paginate in SQL. (6) Cohort export and revenue read all rows, with revenue as a SQL aggregate | control (db, api), exec (catalog) | M | – | 1,200 profiles: `user.tier=gold` = 100 before and after a restart; breakdown has no `(none)`; search finds any user; anon `$set` + identify shows on the user | Memory: all profiles in RAM (the same model the live path already grows to). An extra indexed query per engage |
| E-2 | B-11, P0-7 (partial) | The query-role loop doesn't reload control state | Erased users visible; revoked keys valid on query nodes | The query role runs `reload_caches_from_db` every 5 s (cheap now thanks to the E-1 delta) | node | S | E-1 | An erasure or key revoke is visible on a query node within one tick | More DB reads from query nodes (bounded by the delta) |
| E-3 | B-4 | Skew correction applied to backfills | Migrations land on "now" | `/import` clears `client_sent_ts_ms` before ingest: imports are backfills by definition | control | S | – | Imported event with `sentAt` keeps its original timestamp | None. Live `/v1/ingest` is unchanged |
| E-4 | I-8, I-11, I-12, I-14 | Batch decode is all-or-nothing; Segment `type` ignored; no size limits | One bad event or a Segment `identify` drops the whole batch; huge payloads accepted | Partial success: valid events are stored and invalid ones reported as `rejected: [{index, error}]`; 400 only if nothing is valid. Segment `page` → `$pageview`, `screen` → `$screen` (with `name` kept as a property). `identify`/`alias`/`group` rejected per index with a pointer to `/v1/identify`. Event name ≤ 255 chars, property key ≤ 255, string value ≤ 8 KiB | wire, ingest, control | M | – | Batch of 3 with 1 bad → 2 stored + 1 rejected index; Segment batch with identify + track stores the track | API response gains a field (additive). Clients that relied on a 400 for partial garbage now get 200 + `rejected` |
| E-5 | B-5 | Exact-string index mask vs coercing row filter | The same filter gives different numbers | Skip the string index for numeric-looking targets so the row path decides | exec | S | – | `eq 499`, `eq "499"` and `eq "499.0"` give equal counts on flushed segments | Slightly slower for numeric-string equality on large segments |
| E-6 | B-9 | No time zone validation | Day boundaries silently UTC | Validate IANA names on create (400 with a message) | control | S | – | `Mars/Olympus` → 400; `Asia/Kolkata` OK | None |
| E-7 | B-10 | Hours floored in UTC | IST/NPT hourly charts shifted by 30/45 min | Floor hours in local time using the offset at that instant | exec | S | – | IST hour buckets start at `:00` local | DST: hours are still contiguous; covered by a test |
| E-8 | B-8, M-1, M-4, M-6, N-2 | No name validation; jargon; hidden params | Agents report false zeros | Unknown event → `isError` with "did you mean"; unknown metric → error; plain-language descriptions; advertise `start_ts_ms`/`end_ts_ms` and `compare_previous_period`; project-tz day floor; compact text output | control (mcp) | S | – | Typo → error naming the closest event; `tools/list` documents absolute ranges | Agents that passed garbage now get errors (intended) |
| E-9 | P0-6 | Nothing guards correctness | Silent regressions | A Rust integration test in `pulse-node` that boots a full node, ingests the review's e-commerce fixture plus 1,200 profiles, restarts, and checks the expected answers. It runs in the existing CI `cargo test` | node tests | M | E-1…E-7 | CI fails on any mismatch | Test time (~seconds) |

## P1: core experience (this pass)

| ID | Finding | Solution | Size | Acceptance |
|---|---|---|---|---|
| E-10 | Filters/splits lack `user.*` (P0-2 remainder) | Add `user.<key>` options from a new `GET /v1/projects/:id/user-properties` (catalog keys) to the filter panel and split lists | S | A funnel can be filtered and split by `user.plan` in the UI |
| E-11 | Trends daily only (P1-2) | Hour/Day/Week/Month selector in Trends (the engine already supports it) | S | Weekly trend renders |
| E-12 | Docs truthfulness (P1-8) | Fix `quickstart.md:80`, `web-sdk.md:3`; drop `FEATURE_GAP.md` from this branch; document `sessions`, retry dedup and partial-success semantics in `rest-api.md` | S | Docs match behaviour |
| E-13 | SDK SPA pageviews (P0-4 part) | Patch `history.pushState`/`replaceState` in `sdks/ts-web` when autocapture pageviews are on | S | `pushState` emits `$pageview` |

## Later (not in this pass, with reasons in remaining-gaps)

URL state for reports, Users page, release-impact workflow, Docker/npm publishing, a persisted SDK queue, auto error/network capture, durable dedup, time-scoped erasure tombstones, server-side sessionisation, mobile packages, alerts UI.

## Order

E-1 → E-2 → E-3 → E-4 → E-5 → E-6 → E-7 → E-8 → E-9 (test suite covering all of them) → E-10 → E-11 → E-12 → E-13 → full regression.
