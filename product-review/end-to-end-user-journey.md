# End-to-end user journey

This follows one company from discovering Pulse to using it daily. Friction points are numbered **F-n** and referenced from the usability audit and the roadmap. Labels: VERIFIED = done in this review; CODE = read in source; ASSUMPTION = reviewer judgement.

---

## Stage A: Discovery (site: sswapnil2.github.io/analytics-system)

**What works**
- The positioning is clear within one screen: "Product analytics you actually own… open source, privacy-first… no event caps, no per-seat pricing."
- The capabilities are well illustrated: dashboards, funnels, retention, replay, flags, MCP, architecture, and an honest benchmark section ("not a comparison with other products").
- The cost calculator is concrete and believable.
- The FAQ answers the licensing question directly.

**Friction**
- **F-1 No hosted demo.** "Explore the product" scrolls to a video. A PM can't click around without compiling Rust for 15 minutes. The established analytics tools all offer a sandbox or a free cloud tier.
- **F-2 "Deploy Pulse" leads to a build-from-source doc.** There are no releases, Docker image or one-click deploy (VERIFIED: `gh release list` is empty; CODE: no Dockerfile or Helm chart).
- **F-3 The site overstates the product:**
  - "Split any chart by event, user or group properties" and "Targeted rollouts… rules on user properties": the Studio offers no user-property split and no filter at all (VERIFIED, UI).
  - "Click any cell to see the users behind it" in retention: not found in the Studio.
  - "Shared boards": the seeded demo has no dashboards (VERIFIED).
- **F-4 The numbers disagree.**
  - The README says 226,585 ev/s and a 44 ms cold funnel.
  - The site says 249,044 ev/s and a 145 ms cold funnel.
  - The site quickstart says the root token is "written to pulse_data/root_admin_token"; `docs/quickstart.md` says it is printed once and changes on every restart.
- **F-5 Trust signals are thin:** 0 GitHub stars, no releases, no changelog, no named users or security policy, and no CI badge (the only workflow is GitHub Pages).
- **F-6 Target customer.** The demo, copy and examples are Indian fintech (PAN, UPI, DPDP). That's a strong niche signal, but a US e-commerce team has to translate everything.

**Would I trust it with company analytics after Stage A?** I'd be interested but not convinced. I'd need a demo and a deployable artifact before spending engineering time.

**Why choose Pulse over the established tools?** Simpler self-hosting than the open-source suites (no ClickHouse or Kafka), and data sovereignty plus replay masking that hosted SaaS tools can't offer. Those are the only two reasons, and both are real.

**Missing before I'd try it:** a hosted demo, a Docker one-liner, a supported-platforms matrix (web, iOS, Android, server SDKs), a data-retention and backup story, and a "known limitations" page.

---

## Stage B: Installation and integration

**Steps taken (VERIFIED):**
1. `npm --prefix web ci && npm --prefix web run build`, then `cargo build --release -p pulse-node`: **14m 50s**.
2. Start with a fixed `PULSE_ADMIN_TOKEN`. The node prints a one-time `pk_live_` ingest key.
3. `POST /v1/projects {"name":"Shop Prod","timezone":"Asia/Kolkata"}` returns a project plus an ingest key in one call. Nice.
4. Send the e-commerce events: `User Signed Up` → `Product Viewed` → `Product Added to Cart` → `Checkout Started` → `Payment Completed`.
5. `/v1/identify` for anonymous browsing. Query via REST.

**Realistic time to integrate (ASSUMPTION, from the evidence):**

| Task | Pulse | Typical SaaS competitor |
|---|---|---|
| Run the server | 0.5–1 day (Rust toolchain, build, process supervision, TLS, no image) | 0 |
| Web SDK | 1–2 days: vendor the SDK from the repo, wire SPA route tracking by hand, call replay snapshots by hand, add error and network hooks | 1–2 hours |
| Server-side events | 0.5 day with raw HTTP (no server SDK) | 1–2 hours |
| Mobile | **Blocked:** no iOS or Android package; stale UniFFI bindings (docs/mobile-core.md:56-60) | 1–2 hours each |
| Migration backfill from another analytics tool | Risky: `sentAt` time shift (F-12); `identify` records in exports break batches (F-11) | Supported importers |

**Friction**
- **F-7 SDK not packaged.** "The SDK is not published to npm yet" (docs/web-sdk.md:9). ESM only, no CDN snippet, and `dist/` isn't committed.
- **F-8 SPA pageviews are missed.** Autocapture listens to `popstate` only, so `pushState` navigation (React Router, Next.js) isn't tracked (docs/web-sdk.md:83).
- **F-9 Replay is manual.**
  - Nothing records until you call `captureDomWireframeSnapshot()` after every screen change.
  - Console errors and network failures need manual `recordConsoleError` and `recordNetworkBreadcrumb` calls (docs/web-sdk.md:79-82).
  - The seeded demo's beautiful "Request failed (504)" timeline depends on engineers instrumenting it themselves.
- **F-10 The SDK queue isn't persisted, despite the docs.** Events queued at page close or offline are lost (CODE: ts-web/src/index.ts:267-268 vs docs/web-sdk.md:4).
- **F-11 Segment compatibility is shallow.** A Segment batch with an `identify` call returns 400 and loses the `track` events in the same batch (VERIFIED). `context` and `traits` are ignored (CODE).
- **F-12 Backfill timestamps shift.** An import with an original `sentAt` was stored at import time instead of the event date (VERIFIED).
- **F-13 One bad event rejects the whole batch** (VERIFIED). A schema mistake in one event drops its neighbours, and the SDK drops a batch on 400 (CODE: index.ts:1163).
- **F-14 Duplicate prevention is short-lived.** Ids are deduplicated for only 15–30 minutes, in memory (CODE: dedup.rs:6). Retries without an id or timestamp double-count (VERIFIED).
- **F-15 No environments.** Staging and production have to be separate projects. There is no environment flag, no promote-schema flow, and **no UI to create a project** (CODE). We had to use the API.
- **F-16 Keys can't be revoked or rotated.** There is no DELETE/PATCH key route (CODE: api.rs:1073-1076). A leaked ingest key is permanent.
- **F-17 Weak validation.** A 300-character event name and a 1 MB property were accepted. A bogus time zone was accepted and silently treated as UTC (VERIFIED).
- **F-18 Validating arrival.** Ingest responses report `accepted/deduped/dropped` (good). The Studio sidebar shows "events stored" and "No events in the last few seconds". There is no live event debugger showing the last N raw events with properties.
- **F-19 Schema changes.** The Lexicon can hide, drop or mask PII. There's no type enforcement, no "planned events" allowlist, and no alert on new or unknown events (CODE: governance.rs:18-38).

**Answers to the Stage B questions**

| Question | Answer |
|---|---|
| Understandable without reading source? | REST ingest: yes. SDK and replay: partially. Mobile: no. |
| Can I validate events are arriving? | Yes, crudely: the ingest response counts, the sidebar counter, `/v1/status`. No live debugger. |
| Malformed events? | Clear 400 messages, but the whole batch is rejected. |
| Duplicates? | Send `messageId`. Protected for 15–30 minutes only. |
| Schema changes? | Manually in the Lexicon. No enforcement. |
| Staging vs production? | Separate projects, created via the API. |
| Ingestion fails? | 429 with `Retry-After` under backpressure. The SDK retries with backoff, but the queue is in memory only. |

---

## Stage C: First-time user experience (PM)

**Seeded node (VERIFIED):** sign in with the admin token, and you land on Funnels.

- **F-20** The landing report is an auto-generated funnel of the first four catalog events. It reads **"0.0% of users who did App Open went on to Browse Credit Card within 7 days."** The first thing a new PM sees is a broken-looking number.
- **F-21** The landing page is not Dashboards, and there are no dashboards in the demo.
- **F-22** The project selector shows "Default Project" for the seeded fintech data (id `proj_fintech_in`). That's confusing naming.

**Empty node (VERIFIED):** the "Send your first event" panel offers a curl snippet and "Generate ingest key".

- **F-23** There's no JS or SDK snippet in the panel. Nothing confirms "we received your first event, now build a funnel". There's no sample-data toggle for an empty project, and no checklist of next steps.
- **F-24** Admin areas use jargon:
  - "Save {n} buffered events now"
  - "BYOK AI Provider Key"
  - "BYOC Object Storage"
  - "Named Memory Bucket (memory://bucket)"
  - "Erase User (DPDP)"
  - a raw "uid"
  - (CODE: App.tsx:261, ByokCommercialPanel.tsx, UserProfilesDpdpPanel.tsx:257)
- **F-25** User profiles, which a PM needs for "show me this user's timeline", sit inside **Cohorts** under the title "User Profiles & DPDP Erasure". The timeline shows 20 events as raw JSON (CODE).

**Can a PM who doesn't understand the architecture get value independently?** On the seeded demo, yes, for unsegmented funnels and drop-off replays. On their own data, not without an engineer: there's no project creation, the SDK needs vendoring, and any segmented question needs the API.

---

## Stage D: Core analytics workflows

| # | Workflow | UI | API | Notes |
|---|---|---|---|---|
| 1 | Onboarding funnel (signup → profile → KYC → first txn) | **Partial** | Yes | Built in about 6 clicks (VERIFIED). Window choices are 1h/1d/7d/14d. **No filters**; split by event property only; drop-off shows 15 sample sessions, not the full list; can save to a dashboard. |
| 2 | Retention D1/D7/D30, weekly/monthly, by cohort | **Partial** | Yes | D1/D7 correct (VERIFIED). UI is day-only with at most 13 periods, so no D30, weekly or monthly. "Do first-transaction users return more?" works only via API `global_filter.cohort` (VERIFIED: payers D7 20/30 vs non-payers 0/70). |
| 3 | Feature adoption (Instant Transfers) | **Partial** | Partial | "How many used it" and "how often" via Trends uniques/totals and stickiness retention. **"Discovered it"** needs exposure or impression events you instrument yourself. **"Which segments"** works for one event-property breakdown only. **"Does it improve retention"** is API only. |
| 4 | Why did a user abandon checkout? | **Yes** (strongest) | Yes | Drop-off drawer → replay → "What happened" (VERIFIED on the seed). Replays can't be searched by user id or date range (CODE). The profile timeline is buried. Correlation works only if errors and network calls are instrumented by hand. |
| 5 | Compare segments (new vs returning, free vs premium, source, onboarded or not) | **No** | Partial | One event-property split per report; no user-property split in the UI; cohorts can't be applied; user properties break above 1,000 users (VERIFIED). New vs returning has no built-in definition. |
| 6 | Before/after a release | **Weak** | Partial | Trends' "compare with previous period" shows only a % number. No annotations, no custom A-vs-B date ranges, no `app_version` filter (no filters at all). Experiments give a cleaner answer, but they can't be created in the UI (CODE). |

**The ten questions from the brief, answerable by a PM in the UI without engineering help:**

| # | Question | Verdict |
|---|---|---|
| 1 | Where do users drop off in onboarding? | **Yes** |
| 2 | Which features drive long-term retention? | No: no cohort comparison, no D30+ |
| 3 | What % complete a purchase? | **Yes** (funnel or Trends formula) |
| 4 | Which segments convert best? | Partial: one event property at a time |
| 5 | What changed after a release? | Weak: previous-period % only |
| 6 | Which acquisition cohorts retain better? | No: no filter or split in retention |
| 7 | What do users do right before abandoning? | **Yes**: replay plus reverse Journeys |
| 8 | Is the change improving activation? | Weak: no annotations or date comparison; experiments can't be created in the UI |
| 9 | Which features do our most valuable customers use? | No: needs user properties and cohort filters |
| 10 | Can I trust the numbers? | Events yes (verified); user properties no (above 1,000 users) |

**Score: 3 yes, 3 partial, 4 no.**

---

## Stage E: Data trust

See [test-results.md](test-results.md). In short:

- **Trustworthy (VERIFIED):** funnel steps and windows, order modes, breakdowns, retention periods, uniques after identity merge, dedup within the window, missing and null semantics, half-hour time zones, aggregation over mixed numeric types.
- **Not trustworthy (VERIFIED):**
  - user-property filters and breakdowns above 1,000 profiles;
  - backfilled timestamps when `sentAt` is present;
  - inconsistent equality coercion (`499` vs `"499.0"`);
  - the `sessions` metric for server events;
  - retries without timestamps;
  - MCP returning 0 for unknown events.
- **Not trustworthy (CODE, not run):**
  - hourly buckets ignore the project time zone;
  - split-role query nodes never reload profiles or erasures;
  - dedup reseeding after a crash misses late events;
  - erased user ids stay tombstoned forever, so a returning user is invisible.
- **Cross-surface consistency:** the dashboard cards switch to weekly above 30 days while the Trends page stays daily, so the same saved report looks different in two places. The Trends table is headed "Last 7 days" whatever the range (CODE: InsightsWorkspace.tsx:669, ReportCard.tsx:200).

---

## Stage F: Daily PM experience

A morning check would go: open Dashboards (empty until someone builds them), look at a few cards, and open Funnels to look for drops.

| Need | Status |
|---|---|
| Active users | Trends: DAU/WAU/MAU measures exist |
| What changed yesterday | Preset "Yesterday" exists; no automatic "biggest movers" |
| Declining metrics, deteriorating funnels | **No** alerts in the UI. A `/alerts` API exists, but webhook alerts need a commercial licence (site FAQ). No anomaly detection. |
| Saved reports | Yes, via "Save to dashboard". Journeys can't be saved. |
| Customization | Dashboard cards, text cards, one date range per dashboard; no dashboard filters |
| Sharing | Copy link for signed-in users; no permissions or public link |
| Export | CSV from Trends and cohorts; raw export capped at 50k rows |
| Scheduled reports, annotations, comments | **None** |
| Moving between related insights | Funnel → replay is excellent; nothing else links (e.g. funnel step → Trends, cohort → report) |

**Verdict:** an occasional query tool with one excellent investigation path, not yet a daily decision tool.

---

## Stage G: AI and MCP

Scenario: "Why did onboarding conversion decrease this week?"

| Step | Can the agent do it? | Evidence |
|---|---|---|
| 1. Discover tools | Yes: 5 tools | VERIFIED `tools/list` |
| 2. Understand events and properties | Partially. `get_lexicon_schema` lists them with 24h volume. No property values, no user properties. | VERIFIED |
| 3. Query the right funnel | Yes, if it guesses the step names | VERIFIED |
| 4. Compare time periods | **No** with the advertised schema: `days` is relative to now only, and `compare_previous_period` is hard-coded false | CODE: mcp.rs:316; VERIFIED schema |
| 5. Identify affected segments | One `breakdown_by` per call; no filters in the advertised schema | VERIFIED |
| 6. Find relevant sessions | Yes: drop-off samples plus `inspect_frustrated_sessions` | VERIFIED |
| 7. Explain with evidence | Possible, but risky: a misspelled event returns **0 with no error**, so an agent can fabricate a "conversion collapsed" story | VERIFIED |

- **Security:** project-scoped `sk_live_` keys were refused for other projects (VERIFIED). All tools are read-only. Good.
- **Weaknesses:**
  - jargon tool descriptions ("RoaringBitmap", "Bloom pruning");
  - about 10 KB responses with raw epoch timestamps;
  - no OAuth;
  - no flows, profile or cohort tools.

**Verdict:** a real, safe integration, not just marketing, but too thin to answer a "why did X change" question unaided. It's a good foundation.
