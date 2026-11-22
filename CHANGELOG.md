# Changelog

All notable changes to Pulse are listed here. Versions follow [Semantic Versioning](https://semver.org/); until 1.0, minor versions may include breaking changes.

## [Unreleased]

### Added
- Developer documentation site at `/docs` (architecture, internals, ADRs, configuration, troubleshooting, limitations), with `llms.txt` and `llms-full.txt` for AI agents.
- The Experiments page has tabs for experiments, feature flags, and holdouts and layers. Each variant's verdict sits beside its name ("Significantly worse" in red), stopped and draft experiments say so instead of showing live-test copy, and funnel drops are labelled "A → B" with a hint when a step gets no one.
- The start review names A-to-B and time metrics in full, decisions recorded with an old user id read "a teammate", and the experiment list says "Stopped · shipped …" to match the experiment page.
- Saved metrics: name a guardrail or secondary metric once (say, "KYC failure" as a KYC event where status isn't success) and add it to any experiment from a list.
- Shipping can roll the winner out in steps: 25%, then 50%, then everyone, every 1 to 7 days. A hand change to the flag stops the schedule.
- Create the experiment's flag from the experiment form (made off, 50/50, turned on when the experiment starts). Flags in use by another experiment are marked and can't be picked, and a taken name is flagged as soon as you leave the field.
- Pausing an experiment now turns its flag off, so everyone gets the default until you resume; you're asked to confirm and can note why, and users get their same variant back on resume.
- A flag used by a running or paused experiment shows which experiment, and its on/off switch, audience, holdout and rollout are locked until the experiment is stopped or shipped (pausing turns the flag off).
- The experiment page shows Pulse's recommendation. Ship is no longer the main button when a guardrail has no data or got worse, the split is off, or the sample is short; concluding then needs each of those acknowledged, including a guardrail that got worse.
- Sequential testing for experiments: results stay valid however often you check them, so you can stop a test early. It is on by default for new experiments.
- Experiments can set their significance level and planned sample size, and the sample-size estimate accounts for sequential testing and extra variants.
- Guardrails show lift and significance, say which way is bad, and flag variants that made them significantly worse.
- The Studio warns when an experiment's traffic split doesn't match its flag weights.
- Experiment metrics can count events per user, add up a property per user (revenue), or divide one total by another (spend per visit), not just measure conversion.
- CUPED: experiments can use each user's behaviour before the test to cut noise, so results get significant sooner. The Studio shows how much noise it removed.
- Experiment results can be split by a property (country, plan, …) and show lift over time.
- Experiment results show the setup at a glance (metric, guardrails, flag split, audience, dates), progress toward the planned sample, the absolute difference from control, a traffic-split check, and a clear "no data, check tracking" state for metrics nobody has sent yet.

- Holdouts: keep a share of users off a set of flags, even after shipping, and measure what those flags added up to against them.
- Layers: experiments in the same layer never share a user, so tests on the same page don't affect each other. Each experiment gets its own slice of the layer's users.
- Experiments can be saved as drafts and started later, after a review of what starting will do (flag turned on, share of users reached, end date).
- Concluding an experiment records the decision (ship, keep control or inconclusive), the reason and who made it, and applies it to the flag in the same step.
- Results open with a plain verdict that checks the lift against the one you planned for, says when it's too early, and calls out guardrails with no data. Shipping with such a guardrail needs an explicit acknowledgement, and guardrails with auto-stop say it can't fire without data.
- The sample-size estimate counts only users the experiment can reach: rollout, layer slice and holdout.
- Experiments can have several guardrails and any number of secondary metrics. Secondary metrics show next to the results but never decide the outcome.
- Two new metric types: step-through (the share of users who go from one event to the next) and time between two events.
- An experiments list with search, a status filter, progress, decisions and owners. Experiment names are now unique within a project.
- Flags can be created with your own variants: names, splits, more than two arms, and an optional JSON payload each.
- Segments hide experiment plumbing, ids and personal-data properties, show no lift for segments with too few users, and name the "no value" segment.
- Experiments send a webhook (and an audit row) once when a result turns significant, the planned sample is reached, a guardrail gets worse, or a guardrail has no data.
- Sum and ratio metrics can carry a unit (₹, $, items), shown on every value and difference.
- Segment pickers say that event properties come from the exposure event, warn when most users have no value, and offer the profile property instead.
- Full layers can't be picked, and asking for more of a layer than is free is caught before saving.
- Copied summaries start with a status line (day, planned sample, traffic split, blind guardrails) and a recommendation. Fixed-horizon tests read "too early" until their planned sample, and day counts match everywhere.
- Experiments can target an audience: keep the flag's, pick a cohort, or build one inline ("hasn't done", "has done", "property is") with a live count of matching users that feeds the duration estimate.
- Experiment metrics can count only events where a property matches, such as a KYC check with `status = failed`.
- Every primary metric says whether higher or lower is better (time defaults to lower), and results, verdicts and Ship buttons follow it.
- Experiments record the problem they address and an owner; "Test a fix" fills in the problem.
- The start dialog recaps the metrics, guardrails, variants and audience, and warns when the test is too short to detect the target.
- Each experiment shows whether its milestones are sent to the project webhook, and can turn them off.
- Shipping before the planned sample, with no users, or with a guardrail that has no data needs a tick for each caveat, and the accepted caveats are saved with the decision.
- Experiments can run for any number of days up to 120, and the sample-size warning offers to use the length the plan needs.
- Guardrails with no data say why: the event never fired since the test started, or it fired but not from users in the test.
- Every A/B experiment asks for the smallest lift worth shipping, and the verdict says when no target was set.
- "Test a fix" on a funnel drop-off opens a new experiment measuring that step.
- Experiment results can be shared: each experiment has its own link, and "Copy summary" gives a plain-text write-up of the setup, results and decision.
- Paused experiments keep their guardrail checks and auto-stop running, since users still get their variants, and the Studio says so.
- Holdouts remember the metric they were last measured on, say plainly when there are no conversions yet, and explain that their counts are users assigned, not experiment exposures.

### Changed
- A flag can back only one running or paused experiment at a time; a second one is refused with 409 until the first is stopped.
- New flags from the Studio start off. Starting an experiment turns its flag on, and the Studio asks before you toggle, retarget or re-roll a flag a live experiment uses.
- Users who saw more than one variant are left out of experiment results and counted separately.
- Guardrails can stop an experiment on their own: if a treatment makes one significantly worse, everyone goes back to control, and the Studio shows why it stopped.
- The web and mobile SDKs re-check feature flags every 5 minutes (configurable), so rollbacks reach apps that are already open.
- Flag and experiment changes are recorded in the audit log.
- Experiments can be paused and resumed from the Studio.
- Bandits: a new experiment type that moves traffic to the best variant once a day, shows each variant's chance to be best and the cost of shipping it, and charts how traffic moved.

## [0.1.2] - 2026-11-21

### Added
- Query sampling: exact while a query is under 10 M estimated rows, then deterministic user-bucket sampling, always disclosed in the result (`sampling`) and forceable with `"sampling": "exact"`. Covers Insights, Funnels, Retention and Flows.
- Zero-I/O HyperLogLog sketches in segment footers for fast long-range Uniques, DAU, WAU and MAU.
- Edit and delete saved cohorts.
- Studio: chart export, funnel step picker, per-card intervals, save reports in place, shared trend chart with hover sync, property picker, and UX passes on Journeys, Cohorts, Users, Experiments, Events and Security.
- React Native wrapper with a native recorder (`sdks/react-native`, experimental, not released).

### Changed
- Scale work for 10 M–100 M events: streaming query engine, SlateDB profile store, compressed and batched manifests, identity graph GC.
- Web SDK: fetch timeout (`fetchTimeoutMs`), incremental wireframe diffs, session dedup.
- Open-source readiness: privacy fixes, CI, docs; internal review codes removed from comments and test names; process logs moved to `docs/history/`; crate manifests carry descriptions, repository and homepage.

### Fixed
- Mobile core no longer loses queued events when the app is killed.

## [0.1.1] - 2026-11-19

### Added
- The web SDK is packed and attached to every GitHub release (`npm install <asset url>`), and its checksum is in `SHA256SUMS`.
- Security headers (CSP and others), HttpOnly session cookies and CSRF protection for the Studio.

### Changed
- README lists what you can install today, per platform.
- The Studio's storage settings offer only real object stores (S3, Cloudflare R2, MinIO).

## [0.1.0] - 2026-11-19

First public release.

- Single `pulse-node` binary with the Studio: events, funnels, retention, trends, journeys, cohorts, session wireframe replay, feature flags, experiments, privacy tools and an MCP endpoint.
- Docker image `ghcr.io/sswapnil2/pulse` and Linux x86_64/aarch64 binaries.
- Everything is Apache-2.0 with no licence key.

[Unreleased]: https://github.com/sswapnil2/analytics-system/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/sswapnil2/analytics-system/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/sswapnil2/analytics-system/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/sswapnil2/analytics-system/releases/tag/v0.1.0
