# Prioritized roadmap

**Principle:** finish the product a PM touches before widening it. The engine already supports most of what's needed; the roadmap is mostly about exposing it, fixing trust, and packaging.

**Personas:** **PM** (product manager), **AN** (analyst), **ENG** (integrating engineer), **OPS** (platform/self-host operator), **LEAD** (leadership), **AGENT** (AI agent user).

**Complexity:** S < 1 week · M 1–3 weeks · L 3–6 weeks · XL > 6 weeks (one engineer).

---

## P0: Adoption blockers

### P0-1 Make user properties correct at any scale

- **Problem:** After a restart, user-property filters and breakdowns use only 1,000 profiles. Verified: gold users read 0 instead of 100. On load, older profiles overwrite newer ones, and profile search misses most users.
- **Persona:** AN, PM.
- **Impact:** Silent wrong numbers end evaluations and destroy trust after adoption.
- **Solution:** Make the profile store the source of truth for queries:
  - load all profiles, or look them up by canonical id in a columnar or KV side table;
  - resolve conflicts by `updated_at`, latest wins;
  - merge the anonymous profile into the user's on identify;
  - search and paginate in the database.
- **Acceptance criteria:**
  - A CI test with 100k profiles gives an identical `user.*` filter and breakdown result before and after a restart, and on a query-role node.
  - `GET /users?search=` finds any user, paginated.
  - An anonymous `$set` followed by identify shows on the user.
- **Complexity:** L. **Dependencies:** none. **Priority:** P0.

### P0-2 Filter bar and cohort use on every report

- **Problem:** No report in the UI can be filtered or segmented by user property or cohort. This fails about 4 of the 10 core PM questions.
- **Persona:** PM, AN.
- **Impact:** This is the gap between "demo" and "tool". It's the first thing compared against Mixpanel.
- **Solution:**
  - A shared `FilterBar` (property · operator · value, AND/OR, "in cohort / not in cohort") on Funnels, Trends, Retention and Journeys;
  - per-step "+ where" on funnels;
  - user properties and cohort membership in "Split by";
  - the full cohort builder (not performed, property match, AND/OR).
- **Acceptance criteria:**
  - A PM can build "Android, premium users' onboarding funnel, split by acquisition source" with no API call.
  - The filter round-trips through a saved report and the URL.
  - Results match the equivalent API query in an automated test.
- **Complexity:** L. **Dependencies:** P0-1 for user properties. **Priority:** P0.

### P0-3 Ship installable artifacts

- **Problem:** No npm package, no CDN snippet, no Docker image, no release binary, no Rust CI. A 15-minute source build is the only path.
- **Persona:** ENG, OPS.
- **Impact:** Most evaluators stop here.
- **Solution:**
  - publish `@pulse-analytics/web` (ESM and IIFE) to npm and a CDN;
  - a multi-arch Docker image plus a `docker run` quickstart and a compose file with MinIO;
  - GitHub Releases with binaries;
  - Rust CI (fmt, clippy, tests, the correctness suite from P0-6).
- **Acceptance criteria:**
  - `docker run -p 8080:8080 ghcr.io/…/pulse --seed` works on a clean machine in under 2 minutes.
  - `npm i @pulse-analytics/web` and a 5-line snippet send events.
  - Every tag produces signed artifacts.
- **Complexity:** M. **Dependencies:** none. **Priority:** P0.

### P0-4 Make the web SDK production-grade by default

- **Problem:**
  - SPA `pushState` navigations are not tracked.
  - The queue isn't persisted, so events are lost on reload or offline.
  - Replay needs manual snapshot calls.
  - Errors and network failures need manual calls.
  - There's no debug mode.
- **Persona:** ENG, PM.
- **Impact:** Production data is incomplete, and the "drop → failing request" story only works in the demo.
- **Solution:**
  - patch `history.pushState` and `replaceState`;
  - keep a localStorage or IndexedDB queue with flush on load;
  - snapshot automatically on mutation or route change, throttled;
  - capture `window.onerror`, `unhandledrejection`, and failed `fetch`/XHR (URL without query, status, duration) with opt-out;
  - add `debug: true` console logging.
- **Acceptance criteria:**
  - In a React Router demo app with zero manual calls: pageviews per route, replays recorded, and a 500 response appearing on the replay timeline.
  - Events queued offline arrive after reconnecting and reloading.
- **Complexity:** M. **Dependencies:** P0-3. **Priority:** P0.

### P0-5 Fix ingest and import data-integrity bugs

- **Problem:**
  - Import restamps history when `sentAt` is present.
  - Segment `identify`/`page` calls fail the whole batch.
  - One bad event rejects every event in the batch.
  - Retries without a timestamp duplicate.
  - The dedup window is 15–30 minutes and in memory.
  - Invalid time zones are accepted.
  - There are no size limits on JSON names or properties.
- **Persona:** ENG, AN.
- **Impact:** Migrations corrupt data and real-world clients double-count.
- **Solution:**
  - skip skew correction on `/import`, or when `sentAt` is far in the past;
  - handle Segment `identify` → engage + alias, `page`/`screen` → `$pageview`/`$screen`, `alias`, `group`;
  - partial success: return per-index errors, accept the valid events;
  - durable insert-id dedup for at least 7 days, at minimum on import;
  - validate time zones (IANA) and add `PATCH /projects/:id`;
  - cap names (255) and property values (e.g. 8 KB) with clear 422s.
- **Acceptance criteria:**
  - A Segment export replays with original timestamps, preserving identities and traits.
  - A 1,000-event batch with 3 bad events stores 997 and reports the 3 indexes.
  - Re-importing a file a day later adds 0 rows.
- **Complexity:** M. **Dependencies:** P0-1 for traits. **Priority:** P0.

### P0-6 Correctness test suite as a release gate

- **Problem:** The engine is correct today, but nothing proves it stays correct, and several verified bugs slipped through.
- **Persona:** AN, LEAD.
- **Impact:** Trust is the product.
- **Solution:** Promote `product-review/fixtures/gen_ecommerce.py` and `verify_queries.py` into a CI suite. Add:
  - 100k profiles;
  - restart, flush and compaction between ingest and query;
  - split-role mode;
  - IST and DST time zones;
  - the coercion cases;
  - cross-surface checks (Trends totals = export count = funnel step 1 uniques).
- **Acceptance criteria:**
  - CI fails on any mismatch.
  - B-1 to B-10 in the gap analysis are each covered by a test.
- **Complexity:** M. **Dependencies:** P0-3 (CI). **Priority:** P0.

### P0-7 Erasure must hold on every node

- **Problem:** Query-role nodes never reload erasures or profiles. Erased ids are tombstoned forever.
- **Persona:** OPS, legal.
- **Impact:** A DPDP/GDPR violation in the very scenario Pulse markets.
- **Solution:**
  - query nodes refresh the catalog, or subscribe to changes;
  - erasure physically rewrites L1 segments during compaction;
  - tombstones are scoped by time.
- **Acceptance criteria:**
  - In a 3-role cluster, an erased user disappears from query results within 10 seconds.
  - After compaction, the raw rows are gone.
  - The same id re-registering after erasure is counted again.
- **Complexity:** M. **Dependencies:** none. **Priority:** P0 (P1 if split-role mode is marked experimental).

---

## P1: Critical product gaps

### P1-1 Report state in the URL; land on Dashboards with a starter board

- **Persona:** PM, LEAD.
- **Problem:** Unsaved reports can't be shared, and the first screen is a 0% funnel.
- **Impact:** First impression and everyday collaboration.
- **Solution:**
  - serialize every query into the URL;
  - the default route is Dashboards;
  - the seed creates an "Onboarding health" board like the one on the site;
  - an empty funnel shows a builder prompt.
- **Acceptance criteria:**
  - Reloading reproduces the exact report.
  - A pasted link opens it for another signed-in user.
  - No report ever opens showing auto-chosen steps.
- **Complexity:** S. **Dependencies:** none.

### P1-2 Time granularity and longer retention

- **Persona:** PM, LEAD.
- **Problem:** Trends are daily only; retention is daily and at most 13 periods.
- **Impact:** Weekly and monthly retention and D30 are board metrics.
- **Solution:**
  - an Hour/Day/Week/Month selector in Trends and Retention, with up to 12 periods for weeks and months;
  - a previous-period overlay line;
  - fix the "Last 7 days" header;
  - a matching interval on dashboard cards.
- **Acceptance criteria:**
  - D30 and month-3 retention are visible in the UI.
  - Dashboard and Trends render a report identically.
- **Complexity:** S. **Dependencies:** none.

### P1-3 Drop-off users → cohort and export

- **Persona:** PM, AN.
- **Problem:** Only 15 sample sessions are shown out of thousands.
- **Solution:**
  - "View all users", "Save as cohort" and "Export CSV" from any funnel step;
  - "click a retention cell to see the users".
- **Acceptance criteria:**
  - The saved cohort size equals the dropped count.
  - The cohort is usable in the P0-2 filter bar.
- **Complexity:** M. **Dependencies:** P0-2.

### P1-4 Users page

- **Persona:** PM, support.
- **Problem:** Profiles are buried under Cohorts, with raw JSON and a 20-event cap.
- **Solution:** A top-level "Users" page with search, properties, a paginated readable timeline, linked replays, and erasure behind an admin menu.
- **Acceptance criteria:** Find any user by id or email property in under 2 seconds on 100k users. The timeline is paginated.
- **Complexity:** M. **Dependencies:** P0-1.

### P1-5 Release-impact workflow

- **Persona:** PM, LEAD.
- **Problem:** "Did the new onboarding help?" has no answer path.
- **Solution:**
  - chart annotations (UI plus API, created from CI);
  - an `app_version` default property in the SDKs;
  - "compare range A vs range B" on Funnels and Retention;
  - an experiment-creation UI (flag → variants → goal), with the dead empty-state copy removed until it ships.
- **Acceptance criteria:**
  - A PM can show funnel conversion for 14 days before vs 14 days after an annotated release, with the delta.
  - An experiment can be created end to end in the UI.
- **Complexity:** M. **Dependencies:** P0-2.

### P1-6 Projects, environments and keys

- **Persona:** ENG, OPS.
- **Problem:** No project creation in the UI. No environment notion. Keys can't be revoked.
- **Solution:**
  - "+ New project" with a time zone picker;
  - an `environment` tag (dev/staging/prod) per project with a coloured badge in the UI;
  - copy the Lexicon from staging to production;
  - key revoke, rotate and last-used;
  - a separate server-side ingest secret.
- **Acceptance criteria:**
  - Staging and production are set up with no curl.
  - A revoked key gets 401 within 5 seconds on every node.
- **Complexity:** M. **Dependencies:** none.

### P1-7 MCP that an agent can rely on

- **Persona:** AGENT, PM.
- **Problem:**
  - A typo returns 0 silently.
  - There's no period comparison or filter in the advertised schema.
  - Descriptions are jargon.
  - Responses are about 10 KB.
- **Solution:**
  - error on unknown events and properties, with "did you mean";
  - advertise `start`/`end` (ISO dates), `compare_to`, `filters` and `breakdown_by`;
  - add `query_flows`, `get_user`, `list_property_values` and `list_cohorts` tools;
  - return compact summaries (ISO dates, rounded percentages) with an optional `raw: true`;
  - descriptions written for the model ("Use this to find where users drop off…").
- **Acceptance criteria:** In a scripted eval, an agent answers "why did onboarding conversion drop this week vs last week, and for which platform?" correctly on the seeded dataset in at least 4 of 5 runs, citing replays.
- **Complexity:** M. **Dependencies:** none.

### P1-8 Docs and site truthfulness pass

- **Persona:** ENG, LEAD.
- **Problem:** Stale `FEATURE_GAP.md`, contradictory quickstarts and benchmarks, and over-claims on the site.
- **Solution:**
  - delete or regenerate `FEATURE_GAP.md`;
  - fix quickstart.md:77 and web-sdk.md:4;
  - one benchmark source of truth;
  - remove site claims that the UI doesn't support (user-property splits, user-property rollouts, retention cell drill-down) until they ship;
  - add a "Known limitations" page.
- **Acceptance criteria:** Every claim on the landing page maps to a verifiable UI path or doc.
- **Complexity:** S. **Dependencies:** none.

---

## P2: Competitive improvements

| ID | Item | Persona | Acceptance criteria | Size | Depends on |
|---|---|---|---|---|---|
| P2-1 | Alerts UI (threshold and % change on saved reports) and Slack/email delivery in the community edition. Gate scale, not basic alerts. | PM, LEAD | Alert fires within 10 minutes of a crossing; a test notification button works | M | P1-1 |
| P2-2 | Scheduled dashboard digests (email/Slack) | LEAD | Weekly digest with snapshot PNGs is delivered | M | P2-1 |
| P2-3 | Mobile SDKs: packaged Swift (SPM) and Kotlin (Maven), then React Native | ENG | A sample app on each platform sends events, identifies and records replay | XL | P0-3 |
| P2-4 | Server SDKs (Node, Python) with batching and insert ids | ENG | Published packages; idempotent retries | M | P0-5 |
| P2-5 | Lifecycle and stickiness reports in the UI | PM | New/returning/resurrected/dormant per week | M | P1-2 |
| P2-6 | Paginated raw export (cursor) and scheduled S3/Parquet export in the community edition | AN, OPS | Export 100M rows via cursor without timeouts | M | none |
| P2-7 | Live event debugger (last N events per key or user, with properties) | ENG | New event visible within 2 seconds of ingest | S | none |
| P2-8 | Schema governance: planned-events allowlist, type enforcement, new-event alerts | AN | An unplanned event is flagged or rejected by policy | M | P1-6 |
| P2-9 | Funnel correlation ("what converters did differently") | PM | Top 10 events and properties by lift with significance | L | P0-2 |
| P2-10 | Shared-device identity (allow a second login: re-anchor anonymous id on `reset`), user-to-user merge | ENG | Two users on one device stay distinct; no 409 | M | P0-1 |
| P2-11 | Docker/Helm production guide: backup and restore, upgrade, sizing at 500k MAU | OPS | Documented restore drill passes | M | P0-3 |

## P3: Nice to have

| ID | Item | Why it can wait |
|---|---|---|
| P3-1 | Heatmaps | Needs autocapture maturity first |
| P3-2 | In-Studio natural-language Copilot | MCP plus external agents covers this; fix P1-7 first |
| P3-3 | SQL query surface | Export plus a warehouse covers analysts short term |
| P3-4 | Web vitals and performance events | Not core to product decisions |
| P3-5 | Visual (no-code) event tagging | Governance risk; autocapture covers discovery |
| P3-6 | SSO/SAML and audit UI polish | Needed for enterprise, not for the first 10 customers |
| P3-7 | QUIC ingest, more storage tiers, further benchmark work | No customer is asking; HTTP ingest is already fast enough |

## Sequencing (next 90 days)

- **Days 1–30:** P0-1, P0-3, P0-5, P0-6, P1-1, P1-8. Start P0-2.
- **Days 31–60:** Finish P0-2. P0-4, P0-7, P1-2, P1-3, P1-7.
- **Days 61–90:** P1-4, P1-5, P1-6, P2-1, P2-7. Recruit 2 design partners and run them for 30 days.

**Deliberately not building now:** more commercial add-ons, Copilot, heatmaps, SQL, QUIC, storage tiers. These don't move any of the 10 PM questions until the filter bar and data trust are in place.
