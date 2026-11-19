# Pulse Frontend Hardening Remediation Plan

Work is organized into controlled phases prioritized by real user & security impact (P0 → P1 → P2 → P3).

---

## Phase 2: Critical Security & Reliability (P0)

1. **P0-SEC-1 — Encode `projectId` in all API URLs (`src/api/client.ts`)**
   - Add `encodeURIComponent(projectId)` to `listUsers`, `getUserDetail`, `deleteUserDpdp`, `listDeletionAudit`, `exportCohortCsv`, `importEvents`, and `exportRawEvents`.
   - Add helper `projectPath(projectId, suffix)` to prevent future regressions.
2. **P0-SEC-2 — Mitigate CSV Formula Injection (`src/lib/csv.ts`, `UsersWorkspace.tsx`, `InsightsWorkspace.tsx`)**
   - Create a small, tested `formatCsvCell` / `buildCsv` utility that neutralizes leading `=`, `+`, `-`, `@`, `\t`, `\r` (OWASP CSV injection protection) and quotes cells properly.
   - Replace ad-hoc CSV builders and raw `a.click()` downloads with `buildCsv` + `downloadTextFile`.
3. **P0-SEC-3 — Auth session consistency & global 401 handling (`src/api/client.ts`, `src/App.tsx`, `src/auth/AuthScreens.tsx`)**
   - Add an unauthorized callback registry (`onUnauthorized`) invoked by `fetchJson`/`fetchText` on HTTP 401 so any workspace immediately triggers clean sign-out when a session expires or is revoked.
   - Listen to `window` `storage` events in `App.tsx` so signing out in one tab immediately signs out other open tabs.
   - Pass `onPasswordReset` (or invoke `onUnauthorized`) from `ResetPasswordScreen` so resetting a password while signed in immediately clears `App`'s `signedIn` state.
4. **P0-REL-1 — Add React `ErrorBoundary` (`src/components/ErrorBoundary.tsx`, `src/App.tsx`, `src/main.tsx`)**
   - Wrap the root app and the active workspace area in an accessible `ErrorBoundary` with retry/reset controls so a single workspace crash never white-screens Pulse.
5. **P0-REL-2 — Harden untrusted URL & JSON parsing (`src/workspaces/InsightsWorkspace.tsx`, `src/dashboards/DashboardsWorkspace.tsx`, `src/dashboards/ReportCard.tsx`, `src/dashboards/SaveToDashboard.tsx`, `src/api/client.ts`)**
   - Validate `display_currency` against `CURRENCIES` allowlist before formatting with `Intl`/`toLocaleString`.
   - Wrap `decodeURIComponent` in `dashboardIdFromHash()` in `try/catch`.
   - Guard `rangeFor(sel)` against `NaN` / non-positive day counts (default to 14 days).
   - Cap `decodeQuery` payload size (32 KB) and handle non-JSON 2xx responses cleanly in `fetchJson`.
6. **P1-SEC-4 & P1-SEC-5 — Quickstart key isolation & snippet escaping (`src/App.tsx`)**
   - Clear `mintedIngestKey` whenever `switchProject` or `signOut` runs.
   - Properly escape interpolated `origin`, `keyText`, `sampleEvent`, and `sampleProp` in JS/Kotlin/Swift/bash quickstart snippets.
   - Add `<meta name="referrer" content="strict-origin-when-cross-origin" />` to `web/index.html`.
7. **P2-SEC-6 — Harden MCP tool argument handling (`src/workspaces/LexiconMcpWorkspace.tsx`)**
   - Ensure parsed JSON arguments are a plain object (reject arrays/primitives) and enforce `project_id: projectId` *after* spreading `parsedArgs`.
   - Skip `api.listApiKeys` call when `!canAdmin`.

---

## Phase 3: Performance Engineering (P1)

1. **P1-PERF-1 — Fix infinite API request loop in `CohortsLookupsWorkspace.tsx`**
   - Decouple `defKey` from `Date.now()` by deriving the stability key from `(conds, match)` rather than timestamped `definition`, and compute `Date.now()` only inside `runPreview` / `saveCohort` with debounce + `AbortController`.
2. **P1-PERF-2 — Route-level code splitting (`src/App.tsx`, `src/components/ui.tsx`)**
   - Extract `RoleBadge` out of `SettingsPage.tsx` into `src/components/ui.tsx` so `ProfilePage` does not pull in `SettingsPage`.
   - Convert all workspaces, account pages, and `ByokCommercialPanel` in `App.tsx` to `React.lazy` with `<Suspense>` fallback.
   - Measure before/after bundle sizes.
3. **P1-PERF-3 — Eliminate redundant API calls & polling re-renders**
   - `App.tsx`: Memoize `AccessContext` value with `useMemo`; avoid `setStatus` state updates when the polled `NodeStatus` JSON is identical to the previous snapshot; add request sequence/abort guard to `loadCatalog`.
   - `ExperimentsFlagsWorkspace.tsx`: Debounce rollout slider `PATCH` requests (local draft state + debounced commit) and fix double-fetch on mount caused by `selectedExpId` dependency.
   - `InsightsWorkspace.tsx`: Debounce formula input (350 ms) before querying.
   - `FilterPanel.tsx`: Add in-memory TTL cache (60s) for `topValues` and `getCohorts` per project to avoid duplicate breakdown queries across tab switches.
   - `UsersWorkspace.tsx`: Cap CSV export at 50,000 rows (with clear notice if truncated) and add request sequence guard to `open(distinctId)`.
   - `SessionsReplayWorkspace.tsx`: Add request sequence guard to `runSearch`.
   - `api/client.ts`: Support optional `signal?: AbortSignal` on query methods.

---

## Phase 4: Architecture, Code Quality & Reliability (P2)

1. Fix `setState`-inside-updater anti-patterns in `App.tsx` (line 215) and `WireframePlayer.tsx` (line 187).
2. Clean up `setTimeout` timers on unmount / re-trigger in `App.tsx`, `SettingsPage.tsx`, and `LexiconMcpWorkspace.tsx`.
3. Guard `WireframePlayer.tsx` scale calculation against `fitW <= 12`.
4. Fix `InsightsWorkspace.tsx` series label generation after deleting earlier series (pick first unused letter A–Z).
5. Preserve `window.location.pathname` in `SettingsPage.tsx` invite/reset links and `encodeURIComponent(token)` in `AuthScreens.tsx`.
6. Fix `DashboardsWorkspace.tsx` `onLoaded` callback stability (`useCallback`) and `ReportCard.tsx` `AbortController` support.

---

## Phase 5: UI/UX & Accessibility (P2/P3)

1. Show a clear, accessible access-denied state when a non-admin visits `#/security` directly (instead of a blank main pane).
2. Replace index keys (`key={i}`) in `FilterPanel.tsx` with stable row IDs.
3. Ensure `SignInScreen` syncs `reason` prop updates when a session expires while mounted.
4. Keep `AuthCallback` default redirect aligned with `#/dashboards`.

---

## Phase 6: Testing & Verification

1. Expand `node --test` unit test suite to cover:
   - `csv.ts` (formula injection neutralization, quoting, newlines)
   - `api/client.ts` URL encoding & `roleAtLeast`/`outranks`
   - `lib/reportQuery.ts` / `rangeFor`, `decodeQuery`, `validateCurrency`
   - `lib/format.ts` (`displayUserId`, `formatDuration`, `errorMessage`, `isNotFound`)
   - `lib/projectUrl.ts`
2. Verify `npm run typecheck`, `npm test`, and `npm run build` (compare before/after bundle chunks).
3. Start the live Pulse server + frontend and verify critical workflows in the real application where possible.
4. Complete all remaining deliverables (`implementation-log.md`, `security-findings.md`, `performance-report.md`, `test-results.md`, `remaining-risks.md`).
