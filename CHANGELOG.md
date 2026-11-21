# Changelog

All notable changes to Pulse are listed here. Versions follow [Semantic Versioning](https://semver.org/); until 1.0, minor versions may include breaking changes.

## [Unreleased]

### Added
- Developer documentation site at `/docs` (architecture, internals, ADRs, configuration, troubleshooting, limitations), with `llms.txt` and `llms-full.txt` for AI agents.

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
