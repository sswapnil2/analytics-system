# Remaining gaps

These accepted findings are **not** done. Each one has the reason and the proposed next step. Ordered by priority.

## Excluded by the owner

| Finding | Status | Next step |
|---|---|---|
| **P0-3 Installable artifacts** (npm, Docker, release binaries) | Out of scope for this pass, by the owner's decision. Publishing needs the owner's registry credentials. | 1. Add `exports`/`files`/`publishConfig` to `sdks/ts-web/package.json` and an IIFE build. 2. Add a multi-stage Dockerfile (Rust builder → distroless, plus `web/dist`). 3. Add a tag-triggered release workflow. Then the owner configures `NPM_TOKEN` and GHCR permissions. |

## Still open

| Finding | Why not now | Next step |
|---|---|---|
| **Durable dedup for live `/v1/ingest`** (I-18 remainder) | `/import` now has a 30-day durable index. Live ingest still dedups in memory for 15–30 minutes, because a metastore round trip per event would cap ingest throughput. SDK retries land well inside that window. | A per-bucket fingerprint table in SlateDB with a TTL, checked in the append path. |
| **Erased rows in partitions that are never compacted** (B-12 remainder) | Erased events are hidden from every query at once. They leave disk when their day's partition is compacted (at 4+ segments), so a day with a single segment keeps the bytes. | An erasure job that rewrites only the partitions holding the erased uids (the segment footers' uid ranges narrow the search). |
| **Retention cell drill-down** | No engine API returns the users behind one retention cell. The site no longer claims it. | Same approach as the funnel drop-off list: collect per-cell bitmaps on request. |
| **Cohort split in Funnels and Retention** | Only the Trends engine evaluates `$cohort:` breakdowns, so only Trends offers it. | Evaluate the cohort bitmap once, then use it as the breakdown key in `funnel.rs` and `retention.rs`. |
| **Annotations on Funnels/Retention charts** | Annotations show on Trends. Funnels and Retention have no time axis to draw them on. Their "Compare to" covers before/after a release. | Show the range's annotations as a list above those reports. |
| **Group-property splits** | Not in the engine. The site claim was removed. | Join group profiles the way `user.` properties are joined. |

## Deliberately not changed

- **B-6 `sessions` without `session_id`.** Server-side sessionisation would change a stored metric's meaning. It's documented in `docs/rest-api.md` (Ingest semantics) instead.
- **B-7 retries with no id and no timestamp.** Hashing without the timestamp would merge genuine repeat events. The rule that server callers must send an id or a timestamp is documented.
- **P2/P3** (alerts UI, digests, mobile packages, server SDKs, lifecycle, correlation, SQL, Copilot): not started; they come after the open items above.

## Known limitations of what shipped

- **Profiles in RAM.** All profiles are held in memory per project (one map entry per user). That's fine into the low millions. Beyond about 10M users per project, move to an on-disk KV keyed by canonical uid (`ponytail:` note in `refresh_user_profiles`).
- **Erasure tombstones in RAM.** Each node holds every erased uid with its time (one map entry each). That's fine for any realistic erasure volume.
- **Import dedup window.** 30 days, one global constant (`IMPORT_DEDUP_RETENTION_MS`). Re-importing a file more than 30 days later adds it again.
- **Erased users' profiles on other nodes.** When a user is erased, other nodes keep the in-memory profile entry until their next reload (5 s on query nodes) or restart. Their events are already hidden, so no numbers change.
- **Profiles written before this change.** An anonymous profile stored before upgrading still carries its pre-merge uid until the next identify for that pair. Re-running identify, or a one-off re-key script, fixes the history.
- **Lord Howe Island.** Hour buckets assume DST shifts are whole hours; Lord Howe (+10:30/+11) is the one exception.
- **Postgres.** The new SQL (`ON CONFLICT … RETURNING`, `ALTER TABLE … ADD COLUMN … DEFAULT`) is standard and runs on both backends, but the test suite runs on SQLite only.
