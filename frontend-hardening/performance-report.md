# Pulse Frontend Performance Report

**Date:** 2026-11-19

---

## 1. Bundle Size & Code Splitting (Measured)

Route-level code splitting via `React.lazy` + `Suspense` (plus decoupling `RoleBadge` out of `SettingsPage.tsx` into `ui.tsx`) split the monolithic JS bundle into a lightweight shell entry chunk plus on-demand workspace chunks:

| Metric | Baseline (Before) | Hardened (After) | Improvement |
|---|---:|---:|---:|
| Initial Entry JS (`index-*.js`, raw) | **442.09 kB** | **217.30 kB** | **-224.79 kB (-50.8%)** |
| Initial Entry JS (`index-*.js`, gzip) | **125.65 kB** | **66.83 kB** | **-58.82 kB (-46.8%)** |
| CSS (`index-*.css`, gzip) | 7.74 kB | 7.75 kB | +0.01 kB (negligible) |
| Production Build Time (`tsc && vite build`) | 5.13 s | 4.58 s | -0.55 s (-10.7%) |
| Lazy Workspace Chunks | 0 | 13 route chunks (5.3–30.2 kB raw each) | Loaded on demand |

### Per-Route On-Demand Chunk Sizes (After)
- `FunnelsWorkspace`: 30.22 kB raw / 9.69 kB gzip
- `InsightsWorkspace`: 21.75 kB raw / 7.96 kB gzip
- `ExperimentsFlagsWorkspace`: 21.30 kB raw / 6.90 kB gzip
- `DashboardsWorkspace`: 21.25 kB raw / 6.96 kB gzip (`+ ReportCard` 14.66 kB / 5.11 kB gzip)
- `ByokCommercialPanel`: 20.43 kB raw / 6.26 kB gzip
- `LexiconMcpWorkspace`: 15.06 kB raw / 4.98 kB gzip
- `SettingsPage`: 13.39 kB raw / 4.19 kB gzip
- `RetentionWorkspace`: 13.21 kB raw / 4.90 kB gzip
- `UsersWorkspace`: 12.94 kB raw / 4.49 kB gzip
- `CohortsLookupsWorkspace`: 12.79 kB raw / 4.31 kB gzip
- `WireframePlayer`: 11.43 kB raw / 4.26 kB gzip
- `FlowsWorkspace`: 7.51 kB raw / 3.37 kB gzip
- `ProfilePage`: 6.31 kB raw / 2.43 kB gzip
- `SessionsReplayWorkspace`: 5.30 kB raw / 2.36 kB gzip

---

## 2. Runtime, Network & Rendering Bottlenecks Fixed

1. **Eliminated Infinite API Request Loop (`CohortsLookupsWorkspace.tsx`):**
   - *Before:* `cohortDefinition()` called `Date.now()` during render, producing a new `defKey` on every render when any condition had `days > 0`, firing continuous non-stop `POST /cohorts/preview` requests.
   - *After:* Stable key derived solely from `(conds, match)` + 200 ms debounce + `AbortController`. Fires exactly 1 request per edit.
2. **Eliminated Redundant 5-Second Root Re-renders (`App.tsx`):**
   - *Before:* `loadStatus()` unconditionally called `setStatus(s)` every 5 seconds and passed a fresh `access` object literal to `AccessContext.Provider`, re-rendering the entire app and every `useAccess()` consumer every 5 seconds.
   - *After:* `setStatus` is skipped when the serialized status snapshot is unchanged, and `access` is memoized with `useMemo`.
3. **Eliminated Slider PATCH Storm & Initial Double-Fetch (`ExperimentsFlagsWorkspace.tsx`):**
   - *Before:* Dragging the rollout slider fired up to 100 unthrottled `PATCH /flags/:id` requests; initial mount fetched `getFlags`, `getExperiments`, and `getExperimentResults` twice because `loadAll` mutated its own `selectedExpId` dependency.
   - *After:* Rollout slider updates local draft state immediately and commits a single debounced `PATCH` (250 ms); initial mount fetches once, and switching experiments fetches only `getExperimentResults`.
4. **Debounced Trends Formula Queries & Added Request Cancellation (`InsightsWorkspace.tsx`, `FunnelsWorkspace.tsx`, `UsersWorkspace.tsx`, `SessionsReplayWorkspace.tsx`, `ReportCard.tsx`, `WireframePlayer.tsx`):**
   - Formula input is debounced (350 ms) and in-flight analytics/user/replay queries are cancelled via `AbortController` on superseded inputs or unmount.
5. **Cached Filter Metadata (`FilterPanel.tsx`):**
   - Added 60-second in-memory TTL cache for `useCohortNames` and `topValues`, eliminating repeated 30-day breakdown queries on workspace switches and filter edits.
6. **Bounded Users CSV Export (`UsersWorkspace.tsx`):**
   - Capped pagination loop at 50,000 users to prevent browser tab OOM on very large user bases.
