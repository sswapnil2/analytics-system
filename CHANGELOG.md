# Changelog

All notable changes to Pulse are listed here. Versions follow [Semantic Versioning](https://semver.org/); until 1.0, minor versions may include breaking changes.

## [Unreleased]

### Added
- Sequential testing for experiments: results stay valid however often you check them, so you can stop a test early. It is on by default for new experiments.
- Experiments can set their significance level and planned sample size, and the sample-size estimate accounts for sequential testing and extra variants.
- Guardrails show lift and significance, say which way is bad, and flag variants that made them significantly worse.
- The Studio warns when an experiment's traffic split doesn't match its flag weights.

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
