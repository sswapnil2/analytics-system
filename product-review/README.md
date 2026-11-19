# Product review (snapshot)

These documents are a **point-in-time review of `main` @ `a709476` (18 Nov 2026)**. They describe the product as it was then, not as it is now. Most findings have since been fixed. Read them as the record of why the fixes were made, not as a list of open problems.

| Document | What it is |
|---|---|
| [executive-summary.md](executive-summary.md) | Original verdict and scores (pre-fix) |
| [re-review.md](re-review.md) | Round-2 re-review of `2a2387b`: verdict SATISFIED |
| [test-results.md](test-results.md), [fixtures/](fixtures/) | Evidence and the known-answer dataset |
| Other files | Journey, usability, competitive, gap analysis and roadmap, all pre-fix |

## Status of the original findings (checked on `main` @ `047a233`, 19 Nov 2026)

| Finding | Status |
|---|---|
| User-property filters wrong above 1,000 profiles, including after a restart | Fixed |
| Profile search missed older users | Fixed |
| Historical import restamped to "now" when `sentAt` was present | Fixed |
| Segment `identify` failed the whole batch | Fixed |
| One bad event rejected the whole batch | Fixed: valid events are stored, bad ones reported per index |
| `amount = 499` vs `"499.0"` gave different answers | Fixed |
| Invalid project time zone accepted | Fixed: 400 |
| MCP returned 0 for an unknown event | Fixed: flagged as an error |
| No property, cohort or user-property filters in the Studio | Fixed |
| Retention day-only, at most 13 periods | Fixed: week and month available |
| No Docker image, no release binaries | Fixed: `ghcr.io/sswapnil2/pulse` and GitHub Releases |
| Commercial licence key gating some features | Removed: everything is Apache-2.0 with no key |
| Web SDK not installable | Partly fixed: packed `.tgz` on each release; not on the npm registry yet |
| No iOS or Android SDK | Open |
| Event sent twice with no id and no timestamp is stored twice | By design: send `messageId` to deduplicate retries |

Re-check everything with `fixtures/regressions.sh` (run against a fresh node, then again with `RESTARTED=1` after restarting it) and `fixtures/verify_queries.py`.
