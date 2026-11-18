# Pulse vs Mixpanel and Amplitude: feature gaps

Date: 2026-11-17. Branch: `ui-production-pass`.

## How this was built

1. **SDK features: reverse-engineered.** These come from the published npm packages of each vendor's open-source browser SDK:
   - `mixpanel-browser` 2.86.0;
   - `@amplitude/analytics-browser` 2.49.2 and `analytics-core` 2.62.1;
   - `plugin-autocapture-browser` 1.31.2;
   - `session-replay-browser` 1.51.8;
   - `experiment-js-client` 1.24.0.

   The packages were read statically and never executed. The SDK is the contract with the vendor's server, so its routes, payload operators and config keys show what the backend must support.
2. **Server and UI features: not visible in an SDK.** They come from product knowledge of the two products and are tagged *(docs)*. Not re-verified against live docs in this pass.
3. **Pulse surface: read from this repo.** Sources:
   - `sdks/ts-web/src/index.ts`
   - `crates/pulse-sdk-core`
   - every `"/v1/..."` route string in `crates/`
   - `crates/pulse-exec/src/ast.rs`
   - `web/src/workspaces/`

Tags: **RE** = seen in vendor SDK code (path given) · **docs** = product knowledge · **Pulse** = what this repo has.

## What Pulse already matches (don't rebuild)

Pulse is further along on query semantics than its UI suggests. `crates/pulse-exec/src/ast.rs` already has:

- **Trends:** uniques, totals, sessions, DAU/WAU/MAU, and sum/avg/min/max/percentile of a property. It also supports multi-series, breakdowns, compare to previous period and formulas (`ast.rs:281-330`).
- **Funnels:** strict, loose and any-order steps; first-time-ever steps; per-step filters; exclusions between steps; conversion window; hold property constant; breakdown; time-to-convert histogram (`ast.rs:361-425`).
- **Behavioral cohorts:** performed, not performed, property match, cohort reference, and AND/OR (`ast.rs:239-266`).
- **Property filters:** eq, neq, gt/gte/lt/lte, contains, in, is set (`ast.rs:160-169`).
- **Also present:** retention, flows, session replay with rage taps, feature flags with experiments, lexicon, lookup tables, custom events and properties, project roles, project timezone, and an MCP endpoint.

The gaps below are what's left.

---

## Must-have: blocks a team from switching

### 1. User profiles and profile operations
- **Mixpanel (RE):** People API in `mixpanel-browser/.../mixpanel.cjs.js`:
  - operations: `set`, `set_once`, `unset`, `increment`, `append`, `union`, `remove`, `track_charge`, `clear_charges`, `delete_user`;
  - posts to route `engage/` (`DEFAULT_API_ROUTES`, line 31111).
- **Amplitude (RE):** `Identify` operators `$set`, `$setOnce`, `$add`, `$append`, `$prepend`, `$unset`, `$preInsert`, `$postInsert`, `$remove`, `$clearAll` (`analytics-core/lib/esm`, `IdentifyOperation`).
- **Pulse:** `identify(userId)` only merges IDs (`sdks/ts-web/src/index.ts:346`). There is no profile store, no user properties, and no `$set` handling anywhere in ingest or identity.
- **Why it matters:** "users whose plan = gold" or "KYC status = pending" is impossible today. Breakdowns and cohorts can only use event properties. Every competitor query builder assumes user properties exist.
- **Build:**
  - a profile table keyed by canonical uid, with the ten operators above applied in arrival order;
  - a `/v1/engage` (or `/v1/identify` with ops) ingest path;
  - SDK methods `setUserProperties` / `setOnce` / `increment` / `unset`;
  - user-property filters and breakdowns in `PropPredicate`;
  - a user profile page showing properties plus that user's event stream and replays.

### 2. Autocapture and default properties
- **Mixpanel (RE):** `DEFAULT_CONFIG` (line 31123):
  - autocapture options: `click`, `dead_click`, `rage_click`, `scroll`, `submit`, `pageview`, `input`, `capture_text_content`, `allow_selectors` / `block_selectors`;
  - automatic `$browser`, `$os`, `$device`, `$current_url`, `$referrer`, `$initial_referrer`, `$referring_domain`, `$screen_height`, `$search_engine`.
- **Amplitude (RE):**
  - `analytics-browser/lib/esm/config/joined-config.d.ts` has autocapture keys `attribution`, `pageViews`, `sessions`, `formInteractions`, `fileDownloads`, `elementInteractions`, `frustrationInteractions`, `webVitals`, `networkTracking`, `pageUrlEnrichment`;
  - the autocapture plugin emits `[Amplitude] Element Clicked`, `Element Changed`, `Rage Click`, `Dead Click`, `Error Click`, `Thrashed Cursor`, `Page Scrolled`, `Element Exposed`, `Network Request`, `Main Thread Block`, `Screen Viewed`.
- **Pulse:** detects rage taps and installs one click listener (`index.ts:554`). It has no page views, no session start/end events, and no device or browser context.
- **Why it matters:** without page views and device context, a new customer sees an empty product until engineers hand-instrument every screen.
- **Build:**
  - a `$pageview` on load and on history change, plus session start/end;
  - a default property set (browser, OS, device, screen, URL, referrer, locale);
  - click/submit/change autocapture with element text, selector and href, behind allow and block selectors;
  - dead clicks and error clicks next to the existing rage taps.

### 3. Marketing attribution
- **Mixpanel (RE):** `track_marketing` captures `utm_source/medium/campaign/content/term`, `gclid`, `fbclid`, `ttclid`, `li_fat_id`, `msclkid`.
- **Amplitude (RE):** an `attribution` autocapture block, plus `isUserPropertyAttributionEnabled` and `isEventPropertyAttributionEnabled` in `browser-client.js`.
- **Pulse:** no `utm_` or referrer handling in the SDK.
- **Why it matters:** "which campaign brought users who completed KYC" is the first question a growth PM asks.
- **Build:** parse UTMs and click IDs on landing, store first-touch and last-touch as user properties (needs gap 1), and attach them to events.

### 4. Consent, opt-out and data deletion
- **Mixpanel (RE):**
  - consent methods: `opt_in_tracking`, `opt_out_tracking`, `has_opted_in/out`, `clear_opt_in_out_tracking`;
  - config: `consent_mode`, `opt_out_tracking_by_default`, `property_blacklist`;
  - People `delete_user`.
- **Amplitude (RE):** `setOptOut` in the core client.
- **Pulse:** has no opt-out API in the SDK and no user-deletion endpoint.
- **Why it matters:** Indian fintech is subject to the DPDP Act 2023. Consent withdrawal and erasure on request are legal requirements, not features. Pulse's privacy story (masking) is strong. Its consent and erasure story is missing.
- **Build:**
  - SDK `optOut()` / `optIn()`, persisted, with opt-out stopping both events and replay;
  - an `opt_out_by_default` config;
  - `DELETE /v1/projects/:id/users/:distinct_id`, which removes events, profile and replays, with an audit record;
  - a deletion-request log in the UI.

### 5. Revenue
- **Mixpanel (RE):** `people.track_charge` / `clear_charges`.
- **Amplitude (RE):** a `Revenue` class with `setProductId`, `setPrice`, `setQuantity`, `setRevenueType`, `setCurrency`, `setReceipt`, `setReceiptSig`, `setEventProperties`; core client `revenue()`.
- **Pulse:** no revenue concept.
- **Why it matters:** fintech products live on GMV, ARPU and LTV.
- **Build:** a typed revenue event (amount, currency, product, type), a revenue/ARPU measure in Trends, and an LTV-by-cohort chart.

### 6. Dashboards in the UI
- **Pulse:** `/v1/projects/:id/dashboards` and `/saved-reports` routes exist. There is no workspace that uses them (`web/src/workspaces/` has none).
- **Mixpanel and Amplitude (docs):** boards of saved reports with text cards and a shared date-range filter.
- **Why it matters:** every report currently disappears on reload. Nobody can share "the onboarding dashboard".
- **Build:**
  - "Save report" on every workspace;
  - a Dashboards page with a grid of saved reports and one global date range and filter;
  - share by link inside the org.

### 7. Server-side ingest API and batch import
- **Amplitude (RE):** `api2.amplitude.com/2/httpapi`, `/batch`, and EU variants `api.eu.amplitude.com/...` (URL strings in `analytics-browser`).
- **Mixpanel (RE):** `track/`, `engage/`, `groups/` routes. *(docs)* Both vendors also offer historical import endpoints and server SDKs (Python, Node, Java, Go).
- **Pulse:** `/v1/ingest` is shaped around its own SDK packets. There is no documented server-to-server or backfill API.
- **Why it matters:** the events that matter most in fintech come from the backend: payment success, KYC verdict, mandate approval. Moving off a competitor needs a history backfill.
- **Build:**
  - a documented JSON batch endpoint with insert-id dedup and past timestamps;
  - a CSV/NDJSON import job;
  - one server SDK (Node or Python first).

---

## Should-have: expected within a quarter of adoption

### 8. Group (account) analytics
- **Mixpanel (RE):** `add_group`, `set_group`, `get_group`, `remove_group`, `track_with_groups`, Group `set/set_once/union/remove/unset`, and route `groups/`.
- **Amplitude (RE):** `setGroup`, `groupIdentify`.
- **Pulse:** none.
- **Why it matters:** B2B fintech counts merchants and businesses, not just users.
- **Build:** a group key on events, a group profile store, and a "count unique groups" measure in every report.

### 9. Super properties and enrichment hooks
- **Mixpanel (RE):** `register`, `register_once`, `unregister`, `add_hook` / `remove_hook`, `hooks` config.
- **Amplitude (RE):** a plugin pipeline, `add` / `remove` / `plugin` in `core-client`, with enrichment and destination plugin types.
- **Pulse:** none; every property has to be passed on every `track` call.
- **Build:** `register(props)` persisted per device, and one `beforeSend(event) => event | null` hook. Skip a full plugin system until someone asks.

### 10. Timed events
- **Mixpanel (RE):** `time_event(name)` attaches `$duration` to the next track of that event.
- **Build:** about 10 lines in the SDK. Very useful for "time on KYC form".

### 11. Session replay controls
- **Mixpanel (RE):**
  - controls: `start/stop/pause/resume_session_recording`, `get_session_replay_url`;
  - config: `record_sessions_percent`, `recording_event_triggers`, `record_mask_inputs`, `record_block_selector`, `record_console`, `record_network`, `record_canvas`, `record_heatmap_data`.
- **Amplitude (RE):** `session-replay-browser` config: `sampleRate`, `privacyConfig` (`blockSelector`, `maskSelector`, `unmaskSelector`, `defaultMaskLevel`), `targetingConfig`, `ugcFilterRules`, `interactionConfig`, `performanceConfig`, `captureEnabled`, `useWebWorker`.
- **Pulse:** a wireframe snapshot (`captureDomWireframeSnapshot`, `index.ts:417`) plus PII text masking. No sampling, no start/stop, no trigger-based recording, no console or network capture.
- **Why it matters:** recording 100% of sessions doesn't scale on cost. "Record only sessions that hit an error" is the high-value mode.
- **Build:**
  - a sample rate;
  - record-on-event triggers;
  - mask, block and unmask selectors;
  - start/stop API;
  - console errors and failed network requests as timeline markers (the replay "What happened" list is ready for them).

### 12. Heatmaps
- **Mixpanel (RE):** `record_heatmap_data`, `is_recording_heatmap_data`.
- **Amplitude (docs):** click and scroll heatmaps built from autocapture.
- **Pulse:** has the click and rage data but no heatmap view.
- **Build:** after gap 2, a click/scroll overlay per URL or screen.

### 13. Experiments: exposure tracking and remote config
- **Amplitude (RE):** `ExperimentClient` exposes `fetch`, `variant`, `all`, `exposure`, `start`, `stop`, `setUser`, `getEvaluationTraces`, `setTracksAssignment`, against `/sdk/v2/flags` and `/sdk/v2/vardata`.
- **Mixpanel (RE):** flags API `get_variant(_sync)`, `get_variant_value(_sync)`, `is_enabled(_sync)`, `load_flags`, `update_context`; route `flags/`; plus `visual_experiments` and `remote_settings_mode` config.
- **Pulse:** `getVariant`, `refreshFlags`, edge evaluation.
- **Missing pieces:**
  - an explicit exposure event, so experiment analysis counts only users who actually saw the variant (today it counts everyone assigned, which dilutes results);
  - variant payloads (JSON values, i.e. remote config);
  - synchronous `isEnabled`;
  - targeting by user properties (needs gap 1).

### 14. Alerts and scheduled reports
- **Mixpanel and Amplitude (docs):**
  - alerts when a metric crosses a threshold or moves anomalously;
  - scheduled email or Slack digests of a dashboard.
- **Pulse:** none.
- **Build:** a threshold alert on a saved Trends report, checked on an interval, delivered by webhook. Slack and email come after.

### 15. Cohort export and sync
- **Mixpanel and Amplitude (docs):** cohorts sync to ad, CRM and messaging tools on a schedule, or export as CSV.
- **Pulse:** cohorts exist but can't leave the product.
- **Build:**
  - CSV download of a cohort's users (masked by default, raw with permission);
  - a webhook on cohort membership change.

### 16. Data export and warehouse
- **Mixpanel and Amplitude (docs):**
  - raw event export API;
  - scheduled export to S3/GCS/BigQuery/Snowflake;
  - Amplitude also offers warehouse-native mode.
- **Pulse:** none; data is locked in the Vortex segments.
- **Why it matters:** finance and risk teams will want the raw data in their own warehouse. Without it, procurement often says no.
- **Build:** an NDJSON/Parquet export API by time range, then scheduled export to S3.

### 17. Remaining report types
All *(docs)*.

| Report | What it answers | Pulse status |
|---|---|---|
| Stickiness | how many days per week users do X | Partly covered by retention `frequency_stickiness`; no dedicated report |
| Impact / causal | did launching X change Y | Missing |
| Lifecycle (new/current/resurrected/dormant) | is growth real | Missing |
| Event segmentation by user property | — | Blocked on gap 1 |
| SQL or custom query | — | Missing; MCP partly covers it |
| Annotations on charts | "release 4.2 shipped here" | Missing |
| Funnel "explore users who dropped" to cohort | — | Partial: drawer lists sessions, but you can't save them as a cohort |

### 18. Org, access and audit
*(docs)*
- **SSO/SAML:** missing. Pulse uses bearer tokens.
- **Audit log:** missing.
- **Per-project role permissions:** Pulse has roles in `pulse-control/src/api.rs`, but they aren't exposed in the UI.
- **Data residency, EU vs IN:** missing. Amplitude has EU endpoints (RE: `api.eu.amplitude.com`). For India, an IN region matters for RBI data-localization expectations.

---

## Later: differentiators, not table stakes

- **Natural-language queries:** Amplitude "Ask Amplitude", Mixpanel Spark *(docs)*. Pulse's MCP server is a head start: put a question box in the UI that drives it.
- **Data governance:** event approval workflow, schema enforcement on ingest, and property type drift alerts *(docs)*. Pulse's lexicon is the base for this.
- **Web Vitals and performance:**
  - *(RE)* Amplitude `webVitals` autocapture;
  - *(RE)* the `[Amplitude] Main Thread Block` event family.
- **Visual (no-code) event definition:** *(RE)* Amplitude `visual-tagging-selector` CDN script and Mixpanel `visual_experiments`.
- **Diagnostics:** *(RE)* Amplitude `diagnostics.prod.*.amplitude.com/v1/capture`; Mixpanel `report_error`. SDKs report their own errors so the vendor sees broken installs.
- **Cross-subdomain identity:** *(RE)* Mixpanel `cross_subdomain_cookie`, plus cookie vs localStorage persistence choice.

---

## Suggested order

Each step unblocks the next:

1. **User profiles (gap 1).** This unblocks attribution, revenue LTV, group analytics, flag targeting and user-property breakdowns.
2. **Autocapture, default properties and page views (gap 2), plus attribution (gap 3).** A new project then shows data on day one.
3. **Consent and deletion (gap 4).** This is a legal requirement for the target market; ship before the first paying customer.
4. **Dashboards UI (gap 6).** The backend routes already exist, so this is mostly frontend.
5. **Server ingest and import (gap 7).** Lets customers send backend events and migrate in.
6. **Then:** revenue, exposure events, replay sampling and triggers, alerts, export.

## Gaps in this analysis

- **Server-side features (*docs* tags) were not re-checked against current vendor docs in this pass.** Pricing-tier gating isn't covered.
- **Mobile SDK parity (iOS/Android/React Native/Flutter) was not reverse-engineered.** Only the browser SDKs were. Pulse's UniFFI mobile core has the same gaps as its web SDK: `pulse-sdk-core` exposes `track`, `identify`, `reset`, `flush`, `evaluate_flag`, `record_touch` and `record_wireframe_snapshot` only.
- **Amplitude's minified CDN loaders were not read.** Only the ESM builds were.
