# Test results

Review date: 2026-11-18. Build: `main` @ `a709476`, release build of `pulse-node` (14m 50s compile on an M-series Mac).

Everything below is labelled with how it was established:

- **VERIFIED (runtime)**: reproduced against a running node during this review.
- **VERIFIED (UI)**: observed in the Studio in a browser.
- **CODE**: read in the source but not exercised at runtime. File references are given.
- **NOT TESTED**: could not be exercised, with the reason.

## Environment

| Node | Port | Data | Purpose |
|---|---|---|---|
| `review-fresh` | 18101 | empty, then the e-commerce fixture | Integration and correctness tests |
| `review-seeded` | 18100 | `--seed` (Indian fintech demo, 8,000 users, ~105k events, 14 days) | PM workflows in the Studio |

Both run from `.claude/launch.json` with a fixed `PULSE_ADMIN_TOKEN` and a scratch data directory. No production code was modified.

## Test dataset

`product-review/fixtures/gen_ecommerce.py` produces a deterministic 386-event e-commerce dataset with hand-computed answers in `fixtures/expected.json`. `fixtures/verify_queries.py` re-runs every check. The design:

| Cohort | What they do | Why |
|---|---|---|
| u000–u099 | `User Signed Up`, spread over Nov 1–10, 10:00 UTC | funnel entry, 100 users |
| u000–u079 / u049 / u029 / u019 | `Product Viewed` → `Product Added to Cart` → `Checkout Started` → `Payment Completed` within 20 min | expected funnel 100 / 80 / 50 / 30 / 20 |
| u020–u024 | pay 8 days after signing up | inside a 14-day window, outside a 7-day one |
| u090–u094 | pay with no view, cart or checkout | must not inflate later steps |
| u000–u009 | `Payment Completed` sent twice with the same `messageId` | dedup by id |
| u010–u014 | `Product Viewed` sent twice, identical, no id | dedup by content hash |
| anon_060–069 | `Product Viewed` as anonymous, then `/v1/identify` → u060–069 | anonymous-to-user merge |
| even / odd users | `plan` = premium / free | segment comparison |
| Checkout users | `coupon` = "SAVE10" (10), explicit `null` (5), missing (15) | null and missing handling |
| u000–u002 | `amount` sent as the string `"499"`; everyone else sends `499` | type coercion |
| u000–u039 / u000–u019 | `App Opened` on day 1 / day 7 | expected D1 = 40%, D7 = 20% |
| 10 users | `TZ Probe` at 20:00 UTC Nov 12 (= 01:30 IST Nov 13) | project time zone (`Asia/Kolkata`) |
| u000 | event dated 2027-06-01 | future timestamps |

## Results: analytics correctness (VERIFIED, runtime)

Output: `evidence/verify-queries-output.txt`. **10/10 checks pass**, and they still pass after a node restart and after a flush to disk segments.

| Check | Expected | Got | |
|---|---|---|---|
| Loose funnel, 7-day window | 100/80/50/30/20 | 100/80/50/30/20 | PASS |
| Last step, 14-day window | 25 | 25 | PASS |
| Funnel split by `plan` | 50/40/25/15/10 each | same | PASS |
| `Payment Completed` totals after dedup | 30 | 30 | PASS |
| `Product Viewed` totals after content-hash dedup | 90 | 90 | PASS |
| `Product Viewed` uniques (anonymous views merged) | 80 | 80 | PASS (merge is retroactive) |
| `coupon != SAVE10` counts missing and null | 20 | 20 | PASS (documented semantics) |
| Retention cohort size / D1 / D7 | 100 / 40 / 20 | 100 / 40 / 20 | PASS |
| `sum(amount)`, mixed int and string | 14,970 | 14,970 | PASS |
| `TZ Probe` bucketed on the IST day | Nov 13 IST | bucket 1794508200000 (= Nov 13 00:00 IST) | PASS |
| Retention filtered by a "paid" cohort vs "never paid" (API only) | payers 30 users, D1 25, D7 20; non-payers 70, D1 15, D7 0 | identical | PASS |
| Strict-order funnel | 100/80/50/30/20 | same | PASS |
| Time to convert (2-step) | 25 converters | `converted_count: 25` | PASS |

**Conclusion: the core counting engine (funnels, retention, uniques, identity merge) is correct on known data.** That is the most important thing a skeptical analyst wants to hear, and it holds.

## Results: ingestion and integration (VERIFIED, runtime)

| # | Test | Result |
|---|---|---|
| I-1 | Send the full 386-event batch | `accepted 371, deduped 15` (10 by `messageId` + 5 by content). Correct. |
| I-2 | Re-send the whole batch (simulated retry) | `accepted 0, deduped 386`. Correct. |
| I-3 | `/v1/identify` anonymous → user | 200, returns `canonical_uid` (an internal integer that leaks into the public API) |
| I-4 | Identify an anonymous id already merged into u060 as u061 (shared device, second login) | **409** `cannot alias an already-identified user root into a different user`. Shared devices cannot log in a second user. |
| I-5 | Event without a name | 400, clear message |
| I-6 | Event without a user id | 400, clear message |
| I-7 | `properties` is a string | 400, clear message |
| I-8 | Batch of 3 with 1 bad event | **400 for the whole batch.** The 2 good events are lost. Message names the index. |
| I-9 | Invalid JSON | 400, clear |
| I-10 | Wrong ingest key | 401, clear |
| I-11 | Segment batch with `type: identify` + `type: track` | **400 for the whole batch** (`missing event name`). Segment `identify` is not understood, and the track event in the same batch is dropped. |
| I-12 | Segment `type: page` with `name: "Home"` | 200, accepted as an event named after the `name` field (CODE: event.rs:288). Not a `$pageview`. |
| I-13 | The same event with no id and no timestamp, sent twice | **Both accepted.** The server stamps `now()`, which changes the content hash, so client retries double-count. |
| I-14 | 300-character event name + 1 MB property value | **Accepted.** There is no length limit on JSON ingest. |
| I-15 | Ingest key used on an admin route | 403, clear |
| I-16 | Create a project with time zone `Mars/Olympus` | **Accepted and stored.** CODE says queries then silently fall back to UTC (api.rs:1453). |
| I-17 | Admin import of a historical event with original `timestamp` Nov 2 and `sentAt` Nov 2 +1 s | **Stored with Nov 18 (import time).** The same event without `sentAt` kept Nov 2. Clock-skew correction is applied to backfills. |
| I-18 | Re-import the same `messageId` right away | deduped. CODE: only within a 15–30 minute in-memory window (dedup.rs:6), so a re-import an hour later duplicates. |

## Results: user properties (VERIFIED, runtime)

1,200 profiles via `/v1/engage`: `tier = gold` for p0000–p0099 (written first), `basic` for the rest. One `Login` event each.

| Step | `user.tier = gold` uniques (expect 100) | Breakdown by `user.tier` |
|---|---|---|
| Right after ingest (in-memory) | **100** | not run |
| After a node restart | **0** | `(none)`: 200, `basic`: 1000 |

`GET /users?search=p0005` returns `[]` although p0005 exists.

Cause (CODE): the query catalog loads only the 1,000 most recently updated profiles (`db.rs:1225`, `api.rs:732`), and profile search filters only that set (`api.rs:3017`). **Any project with more than 1,000 users gets silently wrong user-property filters and breakdowns after a restart.** This is the most serious bug found.

## Results: other query behaviour (VERIFIED, runtime)

| # | Test | Result |
|---|---|---|
| Q-1 | `amount eq 499` vs `eq "499"` vs `eq "499.0"` on the same 30 events | 30 / 30 / **28**. Equality coercion disagrees between code paths (CODE: scanner.rs:549 exact-string index vs coercing row filter). |
| Q-2 | `sessions` metric for `App Opened` (60 events, 40 users, no `session_id` sent) | **40.** Events without a session id default to `sess_{user}` (event.rs:345), so a server-side user has one lifelong session. |
| Q-3 | `coupon is_set` (10 "SAVE10", 5 explicit null) | 10. Explicit null is treated as not set. Defensible, but undocumented. |
| Q-4 | Event dated 2027-06-01 | Accepted and queryable. There is no future-timestamp guard. |

## Results: MCP (VERIFIED, runtime)

| # | Test | Result |
|---|---|---|
| M-1 | `initialize`, `tools/list` with an `sk_live_` project key | 5 read-only tools. Descriptions use engine jargon ("exact unsampled u32 + RoaringBitmap", "64-bit SessionIndex Bloom pruning"). |
| M-2 | `query_funnel` | Correct counts. The response is about 10 KB of pretty-printed JSON with raw epoch-ms timestamps for a 3-step funnel. |
| M-3 | `query_insights` with the typo `"Payment Complete"` | **Returns 0, with `isError` unset.** An agent will confidently report "no payments". |
| M-4 | `metric: "revenue"` (invalid) | Silently falls back to uniques. |
| M-5 | Project key asking for another project | Refused (`key is not authorized for the requested project`). Scoping works. |
| M-6 | "Compare this week with last week" | Not possible with the advertised schema: only `days` (relative to now). `start_ts_ms` / `end_ts_ms` are accepted but undocumented in `inputSchema` (CODE: mcp.rs). |

## Results: Studio (VERIFIED, UI, seeded node)

| # | Workflow | Result |
|---|---|---|
| U-1 | Sign in with the admin token | Works. Sign-in screen copy is clear. |
| U-2 | First screen after sign-in | The Funnels page with an auto-built funnel of the first 4 catalog events: **"0.0% of users who did App Open went on to Browse Credit Card"**. A bad first impression. |
| U-3 | Build App Open → OTP → KYC → Mandate → First Disbursal | Done in about 6 interactions. Headline "25.9% … within 7 days", biggest drop called out, time to convert shown. Ran in 15 ms. **Good.** |
| U-4 | Add a property filter (e.g. platform = android) | **Impossible.** No filter control exists in Funnels, Trends, Retention or Journeys. |
| U-5 | Split by a user property | Not offered: the "Split by" list shows event properties only. |
| U-6 | See who dropped | Drawer "3,133 users didn't reach UPI Autopay Mandate Approved". Lists 15 sample sessions, masked phone numbers, wireframe replay with a "What happened" list (504s, rage taps, dead taps). **Excellent**, but only a sample, with no export and no "save as cohort". |
| U-7 | Share the funnel I built | The URL stays `#/funnels`, so the state is not shareable until saved to a dashboard. |
| U-8 | Retention | Day granularity only, 7/10/13 days. No D30, no weekly or monthly. |
| U-9 | Dashboards on the seeded demo | "No dashboards yet". The landing page hero and the video show a populated dashboard. |
| U-10 | Empty project first run | "Send your first event" panel with a curl snippet and "Generate ingest key". No SDK snippet. No "waiting for your first event… received" confirmation. Lands on `proj_default`. |

## NOT TESTED, and why

- **Web SDK in a real browser app.** It isn't on npm, and building a host app was out of scope for a review. Findings on autocapture, pushState, the in-memory queue and replay snapshots are CODE plus the SDK's own docs.
- **Mobile.** There is no shippable iOS or Android package (docs/mobile-core.md:3, :56-60).
- **Split-role cluster (ingest/query/compactor) and Postgres/S3.** These need extra infrastructure. CODE review flags stale profile and erasure state on query nodes (pulse-node/src/lib.rs:326-328).
- **The dedup window expiring after 15–30 minutes.** Not waited out; CODE only.
- **Commercial add-ons** (Copilot, warehouse export, audit log, webhook alerts). These need a licence key the reviewer does not have.
- **Performance at 50k–500k MAU.** Seeded queries ran in 13–23 ms on about 105k events. The repo benchmark (1M events) was not re-run.
- **Google sign-in, email signup.** Token sign-in only.
