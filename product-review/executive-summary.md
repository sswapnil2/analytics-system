# Pulse: executive summary

> **Snapshot of `a709476` (18 Nov 2026).** Most findings below are fixed. See [README.md](README.md) for current status.

**Reviewer stance:** Principal PM evaluating Pulse for a company with 50k–500k MAU, several product teams, web and mobile, and hundreds of events.
**Basis:** the public site, every doc in `docs/`, a source read of all crates and the Studio, a release build run locally, a hand-built e-commerce dataset with known answers, and real workflows in the Studio and over MCP. Evidence is in [test-results.md](test-results.md).

## Verdict

**Not adoptable today for the company described. Worth piloting in 2–3 months if the P0 list ships.**

Pulse has a correct, fast analytics engine and one genuinely differentiated workflow: funnel drop → the masked replay of who dropped → what broke. Around that core, the product a PM touches is about a third finished:

- No report has a property filter.
- Cohorts can't be used in reports.
- User-property analytics silently break above 1,000 users.
- There's no installable SDK package, no Docker image, and no published binary.

The engine is ahead of the product. That is a fixable problem, and a much better one than the reverse.

**Overall maturity: early beta, roughly a strong v0.3.** The backend semantics are about where the established hosted tools were around 2016. The PM-facing surface is behind that.

## What Pulse does exceptionally well

1. **The counting engine is correct.** On a known 386-event dataset with traps (duplicates, out-of-order events, late conversions, anonymous-then-identified users, nulls, mixed types, a half-hour time zone), every funnel, retention and uniques check matched the hand count: 10/10, stable across a restart and a flush. Identity merges apply retroactively. Few young analytics tools pass this.
2. **Drop-off → replay → root cause in two clicks.** On the seeded demo, "See who dropped and why" opened 15 masked sessions. Each had a wireframe replay and a "What happened" list: `POST /api/v2/kyc/nsdl-pan-verify` 504 ×2, a 4-tap rage tap, a dead tap. That answers "why did they abandon?" faster than hosted tools that need a separate replay product and context switching. Open-source suites have the parts but not this tight linkage by default.
3. **Privacy by architecture.** Replay masks on the device, records layout rather than pixels, and the data never leaves your infrastructure. Combined with erasure, an audit log and opt-out APIs, this is a real story for regulated industries (fintech, health, India's DPDP Act).
4. **Operational simplicity, as designed.** One Rust binary, local disk or S3, no ClickHouse/Kafka/ZooKeeper. Queries on about 105k events ran in 13–23 ms locally. The cost model is believable.
5. **Plain-language reports.** The funnel headline "25.9% of users who did App Open went on to First Disbursal Success within 7 days. The biggest drop is between KYC PAN Submitted and UPI Autopay Mandate Approved" is better copy than any of the incumbents' defaults.

## Biggest weaknesses

1. **The Studio can't filter.** No property filter, global filter, per-step filter or cohort filter in Funnels, Trends, Retention or Journeys, though the engine supports all of them (`pulse-exec/src/ast.rs`). "Android users only", "premium vs free" and "users who completed onboarding" are unanswerable in the UI. That fails roughly half of the ten questions in the brief.
2. **User-property analytics are silently wrong above 1,000 users.** Verified: after a restart, `user.tier = gold` returned 0 instead of 100, and profile search can't find most users. The target customer has 50k–500k users. A data-trust bug of this kind ends evaluations.
3. **Integration is a build-from-source project.**
   - The web SDK isn't on npm, there's no CDN snippet, and no iOS or Android package exists.
   - There's no Docker image, no published binary, and no Rust CI.
   - The release build takes 15 minutes.
   - Segment `identify` calls reject the whole batch, and historical import shifts timestamps to "now" when `sentAt` is present.
4. **Thin daily-use loop.**
   - Retention is day-only and caps at 13 days.
   - Trends are daily only.
   - Unsaved reports can't be shared by URL.
   - There are no annotations, no alert UI (webhook alerts need a commercial licence) and no scheduled reports.
   - Experiments can't be created in the UI.
5. **Credibility gaps in the docs and marketing.**
   - `FEATURE_GAP.md` is a day old and already mostly wrong.
   - The quickstart says there's no key UI (there is one).
   - The SDK doc says the queue is persisted (it isn't).
   - The site promises "split any chart by user or group properties" and "targeted rollouts on user properties", which the UI doesn't deliver.
   - Benchmark numbers differ between the README and the site.

## Adoption readiness

| Area | Ready? |
|---|---|
| Engineering can integrate a web app in a day | No: no package, SPA pageviews missed, replay needs manual snapshot calls |
| PM can self-serve the 10 core questions | No: about 4 of 10 answerable in the UI (see the journey doc) |
| Analyst can trust the numbers | Mostly for events; **no** for user properties above 1,000 users |
| Leadership can get a weekly view | No: dashboards exist but no alerts, digests or annotations |
| Platform team can run it in production | Not yet: no image, upgrade guide, backups or CI; split-role mode has stale-state risks |

## Strongest differentiators

1. Correct, exact, unsampled counting in a single self-hosted binary.
2. Funnel → replay → error linkage with on-device masking.
3. Data sovereignty with no per-event or per-seat pricing.
4. MCP built into the server, scoped per project. It's a thin layer today.

## Biggest competitive threats

- **Open-source analytics suites** already offer self-hosting, open source, analytics + replay + flags + experiments + SQL, and an AI assistant. Pulse's answer has to be "simpler to operate and more private." That needs a Docker image and a one-command deploy to be true in practice.
- **Hosted analytics tools'** free tiers and self-serve UX. A PM will compare filter and segment UX in the first ten minutes, and Pulse loses that comparison today.
- **Warehouse-native tools.** Teams that already have Snowflake or BigQuery may skip a new data store entirely. Pulse's raw export is capped at 50,000 rows with no cursor.

## Scores (1–10)

| Dimension | Score | Evidence |
|---|---|---|
| Ease of integration | **3** | No npm/CDN/mobile packages; build from source; Segment `identify` breaks batches; import timestamp bug. Ingest API errors are clear, which helps. |
| Time to first insight | **4** | With `--seed`, a funnel in under 5 minutes once built (15-minute compile first). With your own data: curl only, no event-received confirmation, and the default funnel shows 0%. |
| PM usability | **4** | Clear copy and fast funnels, but no filters, no cohort use, no URL state, and jargon in admin areas. |
| Event analytics (Trends) | **4** | Multi-series, formulas, percentiles, compare and CSV. Daily only, no filters, one breakdown, previous period shown only as a number. |
| Funnel analytics | **6** | Correct, three order modes, windows, hold-constant, split, time to convert, drop-off replays. No filters; exclusion fixed to steps 2–3; only 15 drop-off samples. |
| Retention analytics | **4** | Correct (D1/D7 verified), three modes. UI is day-only, ≤13 days, no cohort comparison. |
| User segmentation | **2** | One breakdown at a time; cohorts are a dead end in the UI; user properties broken above 1,000 users. |
| Session replay | **6** | Excellent linkage and masking on the demo. In a real web app, errors and network calls are manual, snapshots must be called by hand, and pushState navigations are missed. Not a pixel replay. |
| Data trust | **5** | Core counting verified correct. Six reproducible defects: profile cap, import time shift, equality coercion, sessions metric, no-timestamp retry dupes, a short dedup window. |
| Reporting and collaboration | **3** | Dashboards with a date range, TV mode and copy-link. No alerts UI, annotations, scheduling, comments or permissions. |
| Developer experience | **4** | Good REST docs and clear 4xx errors. No packages, no server SDKs, no key revocation, no CI, stale docs. |
| AI / MCP | **4** | Works, scoped, read-only. Five tools, jargon descriptions, silent zero on typos, no period comparison, no profile or flows tools. |
| Self-hosting | **5** | One binary and clean config. No Docker, Helm, published binaries, backup or upgrade guide. |
| **Overall adoption readiness** | **3.5** | Strong engine; product layer and packaging not ready for a multi-team company. |

No dimension scores 8 or above. The only candidate was funnel correctness, and the missing filters cap the funnel workflow.

## Final assessment

**1. Would I recommend Pulse to my company today?**
No. Our PMs couldn't segment a funnel by platform or plan without an engineer writing JSON against the API. Our user-property reports would be silently wrong at our scale. Our mobile teams have nothing to install. I would recommend a one-team pilot on the web app *after* the 30-day plan below. The engine and the replay linkage are worth that bet.

**2. The three biggest reasons I'd reject it**
1. No filtering or segmentation in the UI. Most product questions are segmented questions.
2. Silent data-trust bugs: the 1,000-profile cap, the import time shift, and MCP returning 0 for unknown events.
3. No packaged SDKs (npm, iOS, Android) or deployable artifact (Docker/binary).

**3. The three strongest reasons I'd adopt it**
1. The counting is correct and exact, and fast on modest hardware.
2. Drop-off → masked replay → failing request is the best "why" workflow I've seen out of the box.
3. Full data ownership with predictable infrastructure-only cost, which suits regulated markets.

**4. The single biggest product improvement needed**
A filter and segment bar on every report (event properties, user properties and saved cohorts), backed by a fixed, unbounded profile store. It turns four half-answers into answers.

**5. What I would build in the next 30 days**
1. Fix the 1,000-profile cap and the profile conflict ordering.
2. Add a global filter bar (property and cohort) to Funnels, Trends and Retention.
3. Put query state in the URL.
4. Add week and month granularity, with D30 and longer, to retention and trends.
5. Publish `@pulse/web` to npm with SPA pageviews and automatic error and network capture.
6. Ship a Docker image and a GitHub release binary, with Rust CI.
7. Make import ignore `sentAt`, and accept or skip Segment `identify`/`page`/`alias` instead of failing the batch.
8. MCP: return an error for unknown events, add `compare_previous_period` and absolute ranges, and drop the jargon.

**6. What I would deliberately NOT build**
- More commercial add-ons (BYOK Copilot, warehouse export polish, encryption rotation UI).
- QUIC ingest, more storage tiers, more benchmark work.
- Heatmaps, surveys, web vitals or a visual event tagger.
- A natural-language Copilot inside the Studio before the MCP tools are reliable.

None of these matter until a PM can filter a funnel. Every one of them makes the surface wider while the core remains unfinished.

**7. Pulse's strongest competitive positioning**
"Self-hosted, privacy-first product analytics for regulated apps. Find the drop-off, watch the masked session, see the failing call, in one binary you run." Lead with fintech and health teams under data-localization rules who can't send replay data to a US SaaS. Don't position against the hosted tools on analytical breadth.

**8. What must happen before Pulse is ready for paying customers**
- All P0 items in [prioritized-roadmap.md](prioritized-roadmap.md) are closed.
- A published, versioned SDK and server artifact exist, with a changelog and an upgrade path.
- A data-correctness test suite like `fixtures/verify_queries.py` runs in CI against every release, including user properties at 100k profiles and after a restart.
- At least one design partner has run it in production for 30 days on real traffic.
- Docs match the product: delete or rewrite `FEATURE_GAP.md`, and fix the quickstart, SDK and site claims.
