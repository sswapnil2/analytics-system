# Remaining gaps

These accepted findings are **not** done in this pass. Each one has the reason and the proposed next step. Ordered by priority.

## P0 still open

| Finding | Why not now | Next step |
|---|---|---|
| **P0-3 Installable artifacts** (npm, Docker, release binaries) | Publishing to npm or GHCR needs the owner's registry credentials, and a release workflow is an outward-facing decision. | 1. Add `exports`/`files`/`publishConfig` to `sdks/ts-web/package.json` and an IIFE build. 2. Add a multi-stage Dockerfile (Rust builder → distroless, plus `web/dist`). 3. Add a tag-triggered release workflow. Then the owner configures `NPM_TOKEN` and GHCR permissions. |
| **P0-4 SDK production defaults** (part done: SPA pageviews) | A persisted queue, automatic error and network capture, and automatic replay snapshots are each a real SDK feature with privacy implications (network capture must strip query strings and bodies). | 1. Persist the queue to `localStorage` with a size cap. 2. Hook `window.onerror`/`unhandledrejection`. 3. Capture failed `fetch`/XHR (origin + path, status, duration; opt-out). 4. Throttle snapshots on route change. 5. Add `debug: true`. |
| **B-12 Erasure tombstones forever** | Needs time-scoped tombstones (an id erased at T hides only events before T) and a physical rewrite of L1 segments during compaction. That's a storage-format change. | Design note first: store `(uid, erased_at_ms)`; scanners filter `ts < erased_at`; the compactor drops those rows. |
| **I-18 Durable dedup ≥ 7 days** | In-memory rings are bounded at 524k fingerprints per bucket. A durable index is a storage feature (a fingerprint table in SlateDB with a TTL). | Do it first for `/import`, where re-imports are most likely. |
| **Segment `identify` traits in batches** | They're now rejected per item with a clear pointer, and no longer fail the batch. Applying them inline means calling identify/engage from inside the ingest batch path. | Route `identify` items to `identify_for_project` + `apply_engage` after the events are stored. |

## P1 still open

| Finding | Notes |
|---|---|
| Report state in the URL (P1-1) | The hash router only stores the tab. Serialize each workspace's query into `#/funnels?q=<base64 json>` using the same shape `useSavedReport` already restores. |
| Default landing and starter dashboard (P1-1) | Sign-in still lands on Funnels; the seeder creates no dashboard. |
| Per-step funnel filters ("+ where") | The engine supports `FunnelStepSpec.filters`; the UI always sends `[]`. |
| Cohort as a split dimension | The engine recognizes `$cohort:` breakdowns; it isn't in the split lists yet. |
| Drop-off export and full user list (P1-3) | "Save as cohort" exists; cohort export then gives the full list. A direct CSV button is not built. |
| Users page (P1-4) | Search and pagination now work in the API (`?search=`, `?offset=`). The page itself is still inside Cohorts with raw JSON. |
| Release-impact workflow (P1-5) | Annotations and range-vs-range comparison. |
| Project creation UI and key rotation (P1-6) | The API exists (`POST /v1/projects`, and it now validates the time zone); there's no UI. Revoke exists; rotate = create + revoke. |
| MCP extra tools (P1-7) | `query_flows`, `get_user`, `list_property_values`, `list_cohorts`. Validation, plain descriptions, absolute ranges and comparison are done. |
| Site claims (P1-8) | The site still says user-property splits and retention-cell drill-down. User-property splits now work, but drill-down doesn't. Review `site/` before the next deploy. |

## Deliberately not changed

- **B-6 `sessions` without `session_id`.** Server-side sessionisation would change a stored metric's meaning. It's documented in `docs/rest-api.md` (Ingest semantics) instead.
- **B-7 retries with no id and no timestamp.** Hashing without the timestamp would merge genuine repeat events. The rule that server callers must send an id or a timestamp is now documented.
- **P2/P3** (alerts UI, digests, mobile packages, server SDKs, lifecycle, correlation, SQL, Copilot): none of them should start before the P0/P1 items above.

## Known limitations of what shipped

- **Profiles in RAM.** All profiles are held in memory per project (one map entry per user). That's fine into the low millions. Beyond about 10M users per project, move to an on-disk KV keyed by canonical uid (`ponytail:` note in `refresh_user_profiles`).
- **Erased users' profiles on other nodes.** When a user is erased, other nodes keep the profile map entry until they restart. Their events are already excluded through the erased-uid bitmap, so no numbers change.
- **Profiles written before this change.** An anonymous profile stored before upgrading still carries its pre-merge uid until the next identify for that pair. Re-running identify, or a one-off re-key script, fixes the history.
- **Lord Howe Island.** Hour buckets assume DST shifts are whole hours; Lord Howe (+10:30/+11) is the one exception.
