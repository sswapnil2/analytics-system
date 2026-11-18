# Product gap analysis

Status key:

- **Missing:** not built anywhere.
- **Backend-only:** the API or engine supports it, but there is no UI.
- **Partial:** built, but with limits that matter.
- **Broken:** built, but produces wrong results (verified).
- **Mock/placeholder:** the UI implies something exists that doesn't.

> Note: the repo's own `FEATURE_GAP.md` (dated 2026-11-17) is **mostly stale**. It lists profiles, `$set`, autocapture, UTMs, opt-out, deletion, revenue, dashboards UI, batch import, groups, super properties, replay controls, alerts, cohort export and raw export as missing. All of these now exist in some form. It should be deleted or regenerated, because anyone evaluating the repo will read it and under-rate the product.

## 1. Broken (verified wrong results)

| # | Gap | Evidence | Impact |
|---|---|---|---|
| B-1 | User-property filters and breakdowns use only the 1,000 most recently updated profiles after a restart | `user.tier=gold` returned 100 before the restart and **0 after**; breakdown showed `(none)` 200 (test-results). db.rs:1225, api.rs:732 | Any customer above 1,000 users gets wrong segment numbers silently |
| B-2 | Profile search misses users outside the latest 1,000 | `search=p0005` returned `[]` | Support and PM can't look up users |
| B-3 | Profile conflict ordering: older profiles overwrite newer ones on load; an anonymous profile isn't merged on identify | CODE: api.rs:743-746, catalog.rs:606 | Wrong current user properties |
| B-4 | Import applies clock-skew correction, so a backfill with `sentAt` is restamped to "now" | Nov 2 event stored as Nov 18 | Corrupts migrations from Segment, Mixpanel or Amplitude |
| B-5 | Equality coercion differs between code paths | `amount eq 499` → 30, `eq "499.0"` → 28 | The same filter gives different answers |
| B-6 | `sessions` metric for events without `session_id` | 40 sessions for 60 events on separate days | Session counts meaningless for server events |
| B-7 | Retries without id or timestamp double-count | Two identical posts were both accepted | Inflated counts from naive server integrations |
| B-8 | MCP returns 0 for unknown event names with no error | `"Payment Complete"` → 0 | Agents produce confident false conclusions |
| B-9 | Invalid project time zone accepted, then silently treated as UTC | `Mars/Olympus` stored | Day boundaries wrong with no warning |
| B-10 | Hourly buckets ignore the project time zone | CODE: ast.rs:117-118 | IST (+5:30) hourly charts misaligned |
| B-11 | Query nodes in split-role mode never reload profiles, keys or erasures | CODE: pulse-node/src/lib.rs:326-328 | Stale data; **erased users stay visible on query nodes** (compliance risk) |
| B-12 | Erased ids are tombstoned forever | CODE: scanner.rs:279-318 | A user who erases and re-signs up with the same id is invisible |

## 2. Backend-only (engine ready, no UI)

| Capability | Engine/API | UI |
|---|---|---|
| Global property filters (eq, neq, gt/lt, contains, in, is_set) | ast.rs:208-223, 318 | None |
| Per-step funnel filters, "first time ever" | ast.rs:530-535 | Hard-coded off |
| Cohort as a filter or breakdown | `GlobalFilter.cohort`; insights cohort breakdown | None |
| Cohort algebra (not performed, property match, AND/OR, reference) | ast.rs:287-313 | "Performed ≥ N" only |
| Hour/week/month intervals | ast.rs:66-71 | Day only (Trends, Retention) |
| Two-dimension breakdowns | `breakdown_by: Vec` | One only |
| Min/max and arbitrary percentiles | ast.rs:343-346 | Not offered |
| Journeys `top_n_per_step`, filters | ast.rs:715-717 | Not sent |
| Experiment creation | Experiment tables and results routes | List only (empty state implies creation) |
| Alerts | `/v1/projects/:id/alerts` | No UI; delivery is licence-gated |
| Revenue summary | `/v1/projects/:id/revenue` | No UI |
| Project creation | `POST /v1/projects` | None |
| Replay search by time range and dead taps | `SessionSearchQuery` | Not exposed |

## 3. Partial

| Capability | What exists | What's missing |
|---|---|---|
| Web SDK | Full API (track, identify, profiles, groups, consent, flags, revenue, autocapture, replay) | npm/CDN distribution; `pushState` pageviews; a persisted queue (docs claim one); automatic error and network capture; automatic replay snapshots; a debug mode |
| Mobile | Rust core with UniFFI | Packaged Swift and Kotlin SDKs (bindings stale); React Native or Flutter |
| Server ingest | `/v1/ingest`, `/import` (JSON, NDJSON, CSV) | Server SDKs; a server-only secret key distinct from browser keys; per-event errors instead of failing the whole batch |
| Segment compatibility | Field aliases | `type: identify/page/screen/alias/group`, `context`, `traits` |
| Dedup | Id and content hash | Only a 15–30 minute in-memory window; no durable insert-id dedup; per-bucket scope |
| Identity | Retroactive anonymous → user merge | User-to-user merge, a shared-device second login (409 today), unmerge |
| Funnel drop-off | 15 sample sessions with replay | A full user list, save as cohort, export |
| Retention | Correct engine | Week and month in the UI; D30+; cohort comparison; "click a cell to see users" (claimed on the site) |
| Dashboards | Boards, date range, TV mode, copy link | Dashboard filters, permissions, public links, Journeys cards |
| Data export | NDJSON/JSON/CSV | Hard cap of 50,000 rows with no cursor; properties as a JSON blob in CSV |
| Lexicon governance | Hide, drop, PII-mask | Type enforcement; planned-event allowlist; new-event alerts |
| API keys | Create (UI + API), two roles | Revoke, rotate, expiry, last-used |
| Replay | Wireframe, masking, rage and dead taps | Pixel fidelity, automatic console and network capture, user and date search |
| MCP | 5 read-only tools, scoped | Comparison periods, filters in the schema, flows/profiles/cohorts tools, errors for unknown names, compact output, OAuth |
| Self-hosting | Single binary, SQLite/Postgres, disk/S3, roles | Docker image, Helm, releases, CI, backup and restore, an upgrade guide |

## 4. Missing

- **Environments:** dev/staging/prod as a first-class concept, and schema promotion between them.
- **Annotations** (release markers), **scheduled reports**, **comments**, **report permissions**.
- **Lifecycle** (new/returning/resurrected/dormant) and a **stickiness** report as UI reports. "New vs returning" has no built-in definition.
- **Release-impact analysis:** compare custom date ranges side by side; `app_version` default property and filter.
- **Correlation analysis** (what distinguishes converters from non-converters).
- **SQL / ad-hoc query** surface.
- **Live event debugger.**
- **SSO/SAML**, an org-level membership UI, per-project roles in the UI (the API has roles).
- **A hosted demo** and **a published package or binary**.

## 5. Mock or placeholder

| Item | Where |
|---|---|
| "Experiments appear here once one is attached to a feature flag below": nothing in the UI attaches one | ExperimentsFlagsWorkspace.tsx:150-153 |
| "Save the group to reuse it in other reports": cohorts can't be reused | CohortsLookupsWorkspace.tsx:304-305 |
| Site: "Split any chart by event, user or group properties"; "Targeted rollouts… rules on user properties"; "Click any cell to see the users behind it" | Landing page sections 02, 03, 05 |
| Site hero dashboard "Onboarding health · SHARED" | Not present in the seeded demo |
| Trends table "Last 7 days" header | Hard-coded, whatever the range (InsightsWorkspace.tsx:669) |

## 6. Documentation gaps and contradictions

| Doc | Says | Reality |
|---|---|---|
| docs/quickstart.md:77 | "the Studio has no screen for keys yet" | The key screen exists (Events and privacy → API keys) |
| docs/web-sdk.md:4 | SDK "persists its queue" | The queue is in memory only |
| Site quickstart | Root token "written to pulse_data/root_admin_token" | docs say it's printed once and regenerated each start |
| README vs site | 226,585 ev/s, 44 ms cold funnel | 249,044 ev/s, 145 ms cold funnel |
| docs/rest-api.md | | Omits commercial and system routes, dedup window semantics, rate limits, key revocation (absent) |
| docs/mcp.md | Documents advanced query bodies | The tool `inputSchema` doesn't, so agents can't discover them |
| FEATURE_GAP.md | 18 gaps | About 13 are now closed |
