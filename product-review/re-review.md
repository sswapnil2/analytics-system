VERDICT: SATISFIED

# Pulse re-review, round 2 (18 Nov 2026)

**Reviewer:** the product lead who wrote the original review in this folder.

**Build reviewed:** `main` @ `2a2387b` (PR 22). Out of scope: P0-3 (npm, Docker, release binaries), at the owner's request.

## How I tested

I used the release `pulse-node` from this tree and a fresh `web/dist`. I ran two nodes with `--no-quic` and new data dirs:

- **:18121**, with no seed: a new SaaS customer. Same 391 known-answer events, 120 profiles, one restart.
- **:18122**, with `--seed`: the demo.

I checked the Studio in the built-in browser. I drove it with page scripts where the pane was hidden, then read the rendered page. I checked the API with scripts. I didn't edit any source files.

## Blocking items

None. All three blockers from round 1 are fixed, and the fixes hold up under the cases around them.

### 1. P1-1: project lost on reload or shared link. Resolved.

- **Pasted report link:**
  - I opened `#/funnels?q=…&range=7&p=<acme>` with the remembered project cleared, so it behaved like a different browser.
  - It opened Acme SaaS and showed the correct report: 120 → 36 → 36 → 24, 20.0%.
  - Reloading gave the same result.
- **Dashboard link:**
  - `#/dashboards/<acme dashboard>?p=<acme>` opens the dashboard after a reload.
  - The Share popover's link includes `?p=`.
- **Links without `p=`:**
  - `#/trends` opened the remembered project (`pulse_project`).
  - The Studio added `p=` to the URL.
- **Switching project by hand:**
  - It drops the old `q=` and sets `p=` to the new project.
  - No stale report runs on the wrong project.
- **Project you can't open:** `p=proj_doesnotexist` falls back to the default project with "That project isn't available to you, so Pulse opened your default project."

### 2. P1-3: saved drop-off cohort didn't match the drop-off count. Resolved.

- **Studio:**
  - On the step-filtered funnel, the drawer read "84 users didn't reach Project Created".
  - "Save these users as a cohort" saved a fixed list with `users: 84` (checked with MCP `list_cohorts`).
  - In round 1 the same flow saved 37.
- **Cohort matches the download, user for user:**
  - Through the API, the member lists of `from-funnel-dropped?step=1` and `?step=3` equal the `/query/funnel/dropped` CSV exactly (84 of 84, 12 of 12).
- **Cohort counts match in every report:**
  - As a filter it gives 84 in Trends, 84 → 47 in a Funnel and 84 starting users in Retention.
  - As a split it gives 84 in cohort and 36 not in cohort.
  - All of these were unchanged after a restart.
- **Demo:**
  - Android step filter: 1,642 dropped, 1,642 in the cohort, and 1,642 uniques when it's used as a filter.
  - Out-of-range steps (0, 9) get a clear 400.
- **Cohorts page:** it lists fixed-list cohorts with their description and Export CSV.

### 3. P0-5: NDJSON import rejected. Resolved.

- **Accepted formats:** NDJSON is accepted as `application/x-ndjson`, `text/plain` and `application/jsonl`.
- **Repeat imports:** a repeat of the same ids is fully deduped, across formats and after a restart.
- **Partial success:** NDJSON with a missing name and a non-JSON line stores the valid lines and reports `rejected` with the line numbers.
- **Other bodies:** a single JSON object, a JSON array, a Segment `{batch}` and CSV still import.
- **Timestamps:** a backfill with `sentAt` = now stays on 29 Oct.

## Regression check

I re-ran my round-1 SaaS journey. Every result matched round 1:

| Check | Result |
|---|---|
| Funnel | 120/84/48/24 |
| Step filter | 120/36/36/24 |
| Previous period | 50/25/0/0 |
| `user.*` split, live and after restart | 24/96 |
| `user.*` filter plus split | 12/48 |
| Cohort split | 24/96 |
| Full drop-off CSV | 36 |
| Key rotation | old key 401 |
| Erase, then re-signup | 120 → 119 → 120 |
| Invalid time zone | 400 |
| MCP "Did you mean" | errors as expected |
| Demo Overview dashboard | present |

## Status of the original findings

### Resolved

- **P0-1** user properties at scale.
- **P0-2** filter bar and cohorts.
- **P0-4** web SDK defaults, on unit-test evidence.
- **P0-5** ingest and import integrity.
- **P0-6** correctness gate.
- **P0-7** erasure, verified on a single node.
- **P1-1** URL state, landing and starter dashboard.
- **P1-2** time granularity and longer retention.
- **P1-3** drop-off users to a cohort and to export.
- **P1-4** Users page.
- **P1-5** annotations and compare-to.
- **P1-6** projects and keys, without the environment tag.
- **P1-7** MCP.

### Partially resolved

- **P1-8** docs. The first-run panel still shows `npm install @pulse-analytics/ts-web`. That package belongs to the excluded P0-3, and it can be published once artifacts ship.

### Excluded

- **P0-3** installable artifacts.

### Follow-ups from round 1

These are still non-blocking:

- Funnel exclusion is fixed "between steps 2 and 3".
- Conversion windows are coarse.
- Funnels with no saved report still pick their own steps.
- There's no "first event received" confirmation.
- Timelines show times in the browser's time zone, not the project's.
- The demo project is named "Default Project".
- The seed writes events a few hours in the future.
- There's no cohort split in Funnels or Retention.
- There are no environments.
- MCP doesn't advertise filters.
- There's no `PATCH` to change a project's time zone.

I'm ready to run a one-team pilot.
