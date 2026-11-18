# Usability audit (Studio + integration surfaces)

**Severity scale**

| Severity | Meaning |
|---|---|
| **S1** | Blocks a core workflow or produces wrong answers |
| **S2** | Forces a workaround or an engineer |
| **S3** | Friction or confusion |
| **S4** | Polish |

Source references are in `web/src` unless they start with `crates/` or `docs/`. VERIFIED means observed in the running Studio during this review; everything else was read in the code.

Each issue gives the same five things:

1. **What:** what is wrong.
2. **Where:** where it occurs.
3. **Why it matters.**
4. **Expected:** what the experience should look like.
5. **Fix:** how to improve it.

---

### U-01 No filters in any report (S1)

- **What:** No property filter, global filter, per-step filter or cohort filter exists in the UI.
- **Where:** Funnels, Trends, Retention and Journeys (VERIFIED in Funnels). The code hard-codes `filters: []` (InsightsWorkspace.tsx:161, FunnelsWorkspace.tsx:87). `global_filter` is never sent.
- **Why it matters:** Most product questions are segmented: platform, plan, country, app version.
- **Expected:** A "Filter" row under the report header with *property · operator · value* chips and AND/OR, plus "Users in cohort ▾". Funnel steps each get "+ where".
- **Fix:** One shared `FilterBar` component that emits `GlobalFilter`, reused on all four workspaces. The engine already supports it.

### U-02 Cohorts are a dead end (S1)

- **What:** You can build and save a cohort, but you can't apply it anywhere. The empty-state copy promises otherwise: "save the group to reuse it in other reports" (CohortsLookupsWorkspace.tsx:304).
- **Where:** Cohorts.
- **Why it matters:** "Users who completed onboarding vs not" is unanswerable. The UI also breaks its own promise.
- **Expected:** Cohorts appear in the filter bar (U-01) and in "Split by → cohort membership".
- **Fix:** Wire `global_filter.cohort` and the cohort breakdown into the UI. Also expose `not_performed`, property match and AND/OR in the builder: today it only does "performed ≥ N" (:143-175).

### U-03 Split by offers no user properties (S1)

- **What:** "Split by" and "Same value on every step" list event properties only.
- **Where:** Funnels and Trends (VERIFIED).
- **Why it matters:** Plan, tier and acquisition source live on the user.
- **Expected:** A grouped picker with "Event properties" and "User properties" sections, each with a search box.
- **Fix:** Merge profile keys into the picker as `user.*`. This depends on fixing the profile catalog cap (see the gap analysis).

### U-04 First screen shows a meaningless 0% funnel (S2)

- **What:** After sign-in, the user lands on an auto-built funnel of the first four catalog events: "0.0% of users who did App Open went on to Browse Credit Card".
- **Where:** Funnels default view (VERIFIED).
- **Why it matters:** This is the first impression. A PM will assume the data or the product is broken.
- **Expected:** Land on Dashboards, or on an empty funnel builder that says "Pick the first step of a journey you care about." Suggest the most frequent events.
- **Fix:** Remove the auto-steps and make Dashboards the default route (App.tsx:92). Seed a starter dashboard.

### U-05 Report state is not in the URL (S2)

- **What:** Refreshing the page loses the report. You can't paste a link to a report you haven't saved.
- **Where:** All workspaces (VERIFIED: the URL stays `#/funnels`).
- **Why it matters:** PMs share links in Slack all day.
- **Expected:** Every change updates a URL query, as established analytics tools do.
- **Fix:** Serialize the query into the hash with a compact encoding.

### U-06 Retention granularity is locked to days, at most 13 (S2)

- **What:** Retention is day-only with 7/10/13 periods. D30, weekly and monthly retention are impossible.
- **Where:** RetentionWorkspace.tsx:52, 168-173.
- **Why it matters:** Monthly retention is the board-level metric.
- **Expected:** A unit selector (Day/Week/Month) and periods up to 12 weeks or 12 months.
- **Fix:** Expose `unit` and raise `num_periods`. The engine already supports up to 60 periods.

### U-07 Trends are daily-only, and the comparison is number-only (S2)

- **What:** The interval is hard-coded to day (:156). "Compare with previous period" shows only a % in Number view; the line chart doesn't overlay the previous period. The table header is hard-coded "Last 7 days" (:669).
- **Where:** Trends.
- **Why it matters:** The header mislabels data. Release comparisons need overlays.
- **Expected:** An Hour/Day/Week/Month toggle, a dashed previous-period line, and a header that reflects the selected range.
- **Fix:** Small frontend change.

### U-08 Drop-off drawer has samples only (S2)

- **What:** "3,133 users didn't reach…" lists 15 sessions. There's no full list, no export and no "save as cohort".
- **Where:** Funnels drawer (VERIFIED).
- **Why it matters:** PMs want to message or analyse the dropped users, not just watch five of them.
- **Expected:** "View all 3,133 users", "Save as cohort" and "Export CSV".
- **Fix:** Add a `dropped_at_step` cohort kind on the backend and two buttons in the drawer.

### U-09 Funnel exclusion is fixed between steps 2 and 3 (S2)

- **What:** The label reads "Exclude users who did (between steps 2 and 3)", and that span can't be changed.
- **Where:** FunnelsWorkspace.tsx:91, 267.
- **Why it matters:** Most exclusions apply between the first and last step.
- **Expected:** Choose an event, then "between step [x] and [y]".
- **Fix:** Two selects.

### U-10 No experiment creation (S2)

- **What:** Experiments can only be listed. The empty state reads "Experiments appear here once one is attached to a feature flag below", but nothing in the UI attaches one.
- **Where:** ExperimentsFlagsWorkspace.tsx:150-153.
- **Why it matters:** This is promised on the site and dead in the product.
- **Expected:** A "New experiment" flow: flag, variants, goal metric, guardrails.
- **Fix:** Add an experiment create API client call and a form. Until then, hide the section.

### U-11 No project creation or key revocation in the UI (S2)

- **What:** The project switcher can't create a project. API keys can be created but not revoked, in the UI or the API.
- **Where:** Sidebar; Events and privacy → API keys.
- **Why it matters:** Staging and production setup needs an engineer and curl. A leaked key can't be killed.
- **Expected:** "+ New project" in the switcher. A per-key "Revoke" with confirmation.
- **Fix:** Add a revoke route and the UI for both.

### U-12 User profiles are buried and raw (S2)

- **What:** Profiles live under Cohorts as "User Profiles & DPDP Erasure". The timeline shows 20 events as raw `JSON.stringify` output and a "uid" label.
- **Where:** UserProfilesDpdpPanel.tsx:133, 257, 384-393.
- **Why it matters:** "Look up this customer" is a top-five PM and support task.
- **Expected:** A top-level "Users" nav item. A profile page with properties, a paginated timeline with readable rows, and linked replays.
- **Fix:** Promote it to its own page and reuse the replay player.

### U-13 Session replays can't be searched by user or date (S3)

- **What:** Replays filter only by event, rage tap and error. There's no user search and no date range, and results are capped at 30.
- **Where:** SessionsReplayWorkspace.tsx:59-64.
- **Why it matters:** Support asks "show me what user X did yesterday."
- **Expected:** User id search, a date range, a dead-tap filter, and pagination.
- **Fix:** The API already accepts `time_range`; add a user filter to it.

### U-14 Jargon in navigation and admin pages (S3)

- **What:** The UI uses jargon:
  - "Enterprise and security"
  - "BYOK AI Provider Key"
  - "BYOC Object Storage & Warehouse Export"
  - "Named Memory Bucket (memory://bucket)"
  - "Cluster Encryption at Rest (AES-256-GCM)"
  - "Save {n} buffered events now"
  - "Erase User (DPDP)"
- **Where:** ByokCommercialPanel.tsx, App.tsx:261, 467.
- **Why it matters:** PMs and analysts are the main users. Storage internals belong in docs or an admin area.
- **Expected:** "Settings → AI provider", "Data export", "Delete user data". Hide the flush button from non-admins.
- **Fix:** Copy pass, and move operator controls behind an "Admin" section.

### U-15 Inconsistent names (S3)

- **What:** The same things go by several names:
  - "Trends" in the nav vs "Insights" in the API and MCP.
  - "Journeys" vs "flows".
  - "Events and privacy" vs "Lexicon".
  - "Studio" in the docs, but never in the UI.
- **Where:** Navigation, API, MCP and docs.
- **Why it matters:** Docs, MCP and UI don't line up, so users can't map between them.
- **Expected:** One term per concept.
- **Fix:** Pick the UI terms and alias the API and MCP names to them.

### U-16 Saved report looks different on a dashboard (S3)

- **What:** A dashboard card switches to weekly above 30 days, while Trends stays daily.
- **Where:** ReportCard.tsx:200.
- **Why it matters:** The same report shows two different shapes.
- **Expected:** Identical rendering, or a visible interval label on both.
- **Fix:** Store the interval in the saved report.

### U-17 Journeys can't be saved or exported (S3)

- **What:** There's no Save to dashboard on Journeys.
- **Where:** FlowsWorkspace.tsx.
- **Why it matters:** "What do users do before abandoning?" is a report PMs revisit.
- **Expected:** Journeys behave like every other report.
- **Fix:** Add the `flows` kind to `KIND_LABEL` and the card.

### U-18 Empty project onboarding is thin (S3)

- **What:** The first-run panel offers a curl snippet only. There's no SDK snippet, no live "first event received" state, and no next-step checklist.
- **Where:** App.tsx:302-318 (VERIFIED).
- **Why it matters:** This is where a self-serve trial succeeds or dies.
- **Expected:** Tabs for Web SDK, Server, curl and AI agent. A live listener that turns green on the first event. Then "Build your first funnel".
- **Fix:** Poll `/v1/status` and add the snippets.

### U-19 Conversion window choices are coarse (S4)

- **What:** Only 1h, 1d, 7d and 14d are offered.
- **Where:** FunnelsWorkspace.tsx:38-43.
- **Why it matters:** Lending and B2B funnels need 30, 60 or 90 days.
- **Expected:** A number plus unit input.
- **Fix:** Trivial.

### U-20 Experiment CI bar clamps at ±30% (S4)

- **What:** Large effects render as capped.
- **Where:** ExperimentsFlagsWorkspace.tsx:194-196.
- **Why it matters:** It visually misstates the effect size.
- **Expected:** A scale that fits the data.
- **Fix:** Use a dynamic axis.

### U-21 Leftover demo hints (S4)

- **What:** Placeholders like `amount * 0.18` and `CASE WHEN amount > 5000 THEN 'Prime'…` appear in custom properties.
- **Where:** CohortsLookupsWorkspace.tsx:410, 433.
- **Why it matters:** Fintech-specific hints confuse other users.
- **Expected:** Neutral examples.
- **Fix:** Copy change.

---

## What is good and should be kept

- **Report copy.** "25.9% of users who did App Open went on to First Disbursal Success within 7 days" plus the "biggest drop" callout. This beats the incumbents.
- **Order-mode labels in plain English** ("In order, other events allowed between") instead of "loose/strict".
- **The drop-off → replay drawer** with its "What happened" list.
- **Empty and error states** that are specific: "No one did the first step {range}" and "The funnel didn't run: {error}".
- **Speed.** Funnels re-run in about 15 ms, and loading states are skeletons rather than spinners.
