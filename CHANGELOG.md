# Changelog

All notable changes to Pulse are listed here. Versions follow [Semantic Versioning](https://semver.org/); until 1.0, minor versions may include breaking changes.

## [Unreleased]

### Added
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

### Changed
- Internal review codes removed from comments and test names; process logs moved to `docs/history/`.
- Crate manifests now carry descriptions, repository and homepage.

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

[Unreleased]: https://github.com/sswapnil2/analytics-system/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/sswapnil2/analytics-system/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/sswapnil2/analytics-system/releases/tag/v0.1.0
