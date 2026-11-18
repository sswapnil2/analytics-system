VERDICT: NOT SATISFIED

# Pulse re-review (18 Nov 2026)

**Reviewer:** the product lead who wrote the original review in this folder.
**Build reviewed:** `feat/product-improvements` checkout, content identical to `main` @ `81ad43e`.
**Out of scope:** P0-3 (npm, Docker, release binaries), at the owner's request. It doesn't count against this verdict.

## How I tested

I built `pulse-node` (release) and `web/dist`, then ran two nodes with `--no-quic` and fresh data dirs:

- **:18121** started with no seed. I acted as a new non-fintech SaaS customer:
  - signed up with the setup code;
  - created "Acme SaaS" in `America/New_York`;
  - sent 391 events I generated myself (120 current users, 50 previous-period users) with known answers;
  - set 120 profiles;
  - restarted the node once mid-way.
- **:18122** started with `--seed` (the demo).

I checked the API with scripts. I checked the Studio in the built-in browser at phone width (415 px):

- Dashboards landing;
- the first-run panel;
- the funnel builder with "+ where", compare, the drop-off drawer and save as cohort;
- Trends with a cohort split and annotations;
- Retention with compare;
- the Users list and detail;
- Settings (projects with a time-zone picker, Rotate);
- sign out and sign in again.

I didn't edit any source files.

The engine work is excellent. Every number I sent came back exact:

- the funnel was 120/84/48/24;
- a step filter gave 120/36/36/24;
- the previous period was 50/25/0/0;
- `user.company_size` split 24/96, both live and after a restart;
- a filter plus that split gave 12/48;
- the cohort split was 24/96;
- the full drop-off list at step 1 was exactly 36 users;
- erasure, then a re-signup, gave 120 → 119 → 120 with the old events still hidden;
- JSON import run twice gave `accepted 0, deduped 5`, including after a restart;
- the MCP typo now errors with "Did you mean 'Plan_Upgraded'?".

Most of my P0 and P1 list is closed. Three defects remain, each in a core journey from my review. Each one either gives a PM silently wrong data or breaks a documented path.

## Blocking items

### 1. P1-1: report and dashboard links lose the project, so reloading or sharing shows the wrong project's numbers

- **What I did:**
  1. On :18121 (projects "Default Project" and "Acme SaaS"), I switched to Acme SaaS.
  2. I built the funnel Signed_Up → Project_Created (where template is kanban) → Invite_Sent → Plan_Upgraded over 7D. The Studio showed 120 → 36 → 36 → 24 (20.0%), which is correct.
  3. I reloaded the page. The URL `#/funnels?q=…&range=7` was unchanged.
  4. I also created a dashboard in Acme SaaS and opened `#/dashboards/<id>` in a fresh load.
- **What I saw:**
  - After the reload, the project switcher was back on "Default Project". The same report ran there with no warning: "0.0% of users… Signed Up 1". The network log shows the queries going to `/v1/projects/proj_default/query/funnel`.
  - The dashboard link showed "Couldn't open this dashboard: dashboard '…' not found".
  - The URL holds the query but not the project. The selection isn't kept in storage either, so `App.tsx` falls back to `status.active_project_id`.
  - Every install has at least two projects, because the default project always exists. So a pasted link or a reload of any report outside the default project shows the wrong data, and "Share" on any non-default dashboard is a dead link.
- **What would satisfy me:**
  - Report and dashboard URLs carry the project (for example `#/p/<project_id>/funnels?q=…`, or `&project=`).
  - Opening such a link selects that project, or says the user can't access it.
  - The last-used project survives a reload.
  - A test covers it: reload on a non-default project gives the same numbers, and a pasted dashboard link opens.

### 2. P1-3: "Save these users as a cohort" saves a different set of users than the drawer shows

- **What I did:**
  1. On the step-filtered funnel above, I opened "See who dropped and why" at step 1. The drawer read "84 users didn't reach Project Created" and "Download all 84".
  2. I clicked "Save these users as a cohort".
- **What I saw:**
  - The cohort saved as "Did Signed Up, not Project Created" with **37 users** (checked with MCP `list_cohorts`).
  - It is rebuilt as "performed A AND not performed B in range". That drops the step's `where` condition, and in general also the conversion window and order mode.
  - The CSV list from `/query/funnel/dropped` is exact. The cohort isn't, and it then feeds every filter and split, so a PM silently analyses or messages the wrong users.
  - My original acceptance criterion was "the saved cohort size equals the dropped count".
- **What would satisfy me:**
  - The saved cohort holds exactly the dropped users: a static cohort from the dropped bitmap, or a definition that carries the step filters, window and order.
  - A test asserts the cohort size equals `dropped_users` for a funnel with a step filter.

### 3. P0-5: NDJSON import, the format Segment and Mixpanel exports use, is rejected

- **What I did:** I POSTed 5 NDJSON lines to `/v1/projects/:id/import` with `Content-Type: application/x-ndjson`, then again with `text/plain`.
- **What I saw:**
  - Both returned `400 "json payload error: trailing characters at line 2 column 1"`.
  - The same body with `Content-Type: text/csv` was accepted.
  - The JSON-array form works, keeps the original timestamps (stored on 29 Oct although `sentAt` was now) and dedups durably.
  - In `handle_import` (`crates/pulse-control/src/api.rs`, around line 3640), a body that starts with `{` goes to the single-JSON decoder unless the content type contains `csv`. So the NDJSON branch can't be reached for real NDJSON.
  - `docs/rest-api.md` promises "JSON, NDJSON or CSV". The migration path from my Stage B fails on the first try with a misleading error.
- **What would satisfy me:**
  - NDJSON is detected by content type (`ndjson` or `jsonl`), or by more than one top-level object.
  - A `correctness.rs` case imports NDJSON twice: the first run is accepted and the second is fully deduped.

## Status of the original findings

### P0

| ID | Status | Evidence |
|---|---|---|
| P0-1 User properties at any scale | **Resolved** | `user.company_size` split exact live and after a restart; filter plus split exact; `?search=` matches ids and any property across all profiles; offset paging works; Segment `identify` traits show on the user (`seat_count`). The 100k-profile case is covered only by the 1,200-profile `correctness.rs`. |
| P0-2 Filter bar and cohorts in reports | **Resolved** | Filter panel with properties and "Users in cohort"; per-step "+ where" (120/36/36/24); `user.*` in every Split by; cohort split in Trends (24/96). Cohort split in Funnels and Retention isn't offered (non-blocking). |
| P0-3 Installable artifacts | Excluded | Out of scope. Note: the first-run panel tells users to `npm install @pulse-analytics/ts-web`, which isn't published yet. |
| P0-4 Web SDK defaults | **Resolved (unit-test evidence)** | `npm --prefix sdks/ts-web test`: 8/8 pass, covering pushState/replaceState pageviews, a persisted queue across reload, error and fetch capture without query strings, and snapshots. Not run in a real SPA. |
| P0-5 Ingest and import integrity | **Partially** | Done: partial batches (`rejected: [{index:1,…}]`, the rest stored); Segment `page` → `$pageview`; `identify` in a batch; invalid time zone refused with a helpful 400; import keeps history; durable import dedup across restart. **Open:** NDJSON import (blocker 3). Also no `PATCH /projects/:id` to fix a project's time zone (non-blocking). |
| P0-6 Correctness release gate | **Resolved** (not re-run by me) | `crates/pulse-node/tests/correctness.rs` is in the workspace tests. My black-box results agree with it on every overlapping check. |
| P0-7 Erasure on every node | **Resolved (single node verified)** | Erasing u003 changed Signed_Up from 120 to 119 and Project_Created from 84 to 83. A later re-signup is counted (120) while the old events stay hidden, including after a restart and in imported history before the erasure time. The erasure log is shown in Users. Split-role mode is covered only by `ha_02`. |

### P1

| ID | Status | Evidence |
|---|---|---|
| P1-1 URL state, Dashboards landing, starter board | **Partially (blocker 1)** | The query in the URL survives a reload and a project switch; sign-in lands on Dashboards; the demo has the "Overview" board (funnel 25.9%, DAU, D7 36%). But links lose the project. Funnels with no saved report still auto-pick steps; on the demo it opens "Quick Grocery Browse → …" (non-blocking). |
| P1-2 Granularity, longer retention | **Resolved** | Trends: Hour (170 buckets), Day, Week, Month. Retention: Day/Week/Month with 7/14/30 periods. Weekly retention API OK. |
| P1-3 Drop-off users → cohort and export | **Partially (blocker 2)** | "Download all 84" and `/query/funnel/dropped` are exact (36 of 36). The drawer shows a per-user timeline for users without a recording, including masked demo ids. The saved cohort is wrong under step filters. Retention cell drill-down isn't built, and the site claim was removed (acceptable). |
| P1-4 Users page | **Resolved** | A top-level Users page with project-specific columns, search, Previous/Next, readable properties, a day-grouped timeline, "Erase this user" and the erasure log. |
| P1-5 Release impact | **Resolved** | Annotation "v2.3 released" created, listed by range, and drawn on Trends. Funnel compare: "0.0% of 50 users (+20.0 pts now)" with "was 50 / was 0.0%" per step. Retention compare draws a second dashed curve with its range. I didn't check experiment creation. |
| P1-6 Projects, environments, keys | **Resolved, except environments** | Settings has Projects with an IANA time-zone picker (aliases such as `Asia/Calcutta` are accepted) and Rotate on each key. A revoked key gets an immediate 401 and the new key works. An environment tag isn't built (non-blocking). |
| P1-7 Reliable MCP | **Resolved, mostly** | 9 tools. A typo gives `isError` with "Did you mean"; a bad metric errors; `start_ts_ms`/`end_ts_ms` and `compare_previous_period` are advertised; `get_user`, `list_property_values` (`user.company_size`), `list_cohorts` and `query_flows` return correct data; output is compact. Property filters still aren't in the tool schemas, only `breakdown_by` (non-blocking). |
| P1-8 Docs and site truthfulness | **Partially** | The quickstart, web-sdk queue and Ingest semantics are fixed, and FEATURE_GAP was removed. Still untrue: NDJSON import (blocker 3) and the npm install line in the first-run panel (tied to the excluded P0-3). |

### Notable P2/P3 and usability items

These are non-blocking.

- **Not fixed:**
  - Funnel exclusion is still fixed "between steps 2 and 3" (U-09).
  - Conversion windows are still only 1h/1d/7d/14d (U-19).
- **Partially fixed:**
  - The first-run panel now has Web, Android, iOS and HTTP snippets, and it disappears within 5 s of the first event.
  - There's still no "first event received, build your first funnel" confirmation (U-18).
- **Polish:**
  - The drawer and Users timelines show times in the browser's zone, not the project's.
  - The demo project is still named "Default Project" (F-22).
  - The seed writes events up to about 5 h in the future, so the drawer showed "Left 19 Nov 00:04" on 18 Nov.
- **Fixed in the last pass:**
  - Sign out and sign in again lands on Dashboards.
  - No sideways scroll at phone width on the pages I opened.

## What would flip the verdict

Fix the three blocking items above, each with its test. Everything else on my P0/P1 list is closed or reduced to non-blocking follow-ups. On those three fixes, I'd move Pulse to a one-team pilot.
