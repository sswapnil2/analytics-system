# Pulse Frontend Hardening Implementation Log

**Date:** 2026-11-19  
**Scope:** `web/` (Pulse Studio frontend)

---

## Summary of Changes

### New Files
1. `web/src/lib/csv.ts` — RFC 4180 CSV serializer (`formatCsvCell`, `buildCsv`) with OWASP CSV/Formula Injection protection (prefixes leading `=`, `+`, `-`, `@`, `\t`, `\r` with `'`).
2. `web/src/components/ErrorBoundary.tsx` — Reusable React `ErrorBoundary` with accessible `role="alert"` fallback, "Try again" recovery, full-page reload option, and automatic reset via `resetKey`.
3. `web/src/lib/csv.test.ts` — Regression tests for CSV formula-injection neutralization and RFC 4180 quoting.
4. `web/src/lib/security.test.ts` — Unit & regression tests for `projectPath` URL encoding, `roleAtLeast`/`outranks` RBAC helpers, `isAbortError`, `displayUserId` PII masking, `errorMessage`, `isNotFound`, and numeric/duration formatters.

### Modified Files
1. `web/index.html` — Added `<meta name="referrer" content="strict-origin-when-cross-origin" />`.
2. `web/src/main.tsx` — Wrapped `<App />` in `<ErrorBoundary fullPage>`.
3. `web/src/api/client.ts` —
   - Added `projectPath(projectId, suffix)` using `encodeURIComponent(projectId)` and applied it to all 7 previously unencoded endpoints (`listUsers`, `getUserDetail`, `deleteUserDpdp`, `listDeletionAudit`, `exportCohortCsv`, `importEvents`, `exportRawEvents`).
   - Added `setUnauthorizedHandler` so any 401 on a protected endpoint immediately triggers session expiration across all workspaces (excluding credential-submission paths like `/v1/auth/login`).
   - Added `isAbortError(err)` helper and optional `signal?: AbortSignal` parameter on query and catalog methods.
   - Wrapped `JSON.parse` in `fetchJson` with a clean error message for unexpected non-JSON 2xx responses.
   - Exported `ADMIN_TOKEN_STORAGE_KEY` for cross-tab storage synchronization.
4. `web/src/App.tsx` —
   - Converted all 11 workspaces, `ProfilePage`, `SettingsPage`, and `ByokCommercialPanel` to `React.lazy` with `<Suspense>` fallback.
   - Wrapped active workspace rendering in `<ErrorBoundary resetKey={`${activeTab}:${activePid}`}>`.
   - Wired `setUnauthorizedHandler(sessionEnded)` and cross-tab `storage` listener for `ADMIN_TOKEN_STORAGE_KEY`.
   - Wired `ResetPasswordScreen` `onSignedOut` callback to immediately clear signed-in shell state.
   - Cleared `mintedIngestKey` on `switchProject` and `signOut` to prevent cross-project key leakage.
   - Escaped quickstart code snippets via `JSON.stringify` and `shellSingleQuote`.
   - Fixed `setState`-inside-updater anti-pattern in `hashchange` listener using `projectIdRef`.
   - Memoized `AccessContext` value with `useMemo` and deduplicated 5-second `loadStatus` polls via serialized snapshot comparison.
   - Added `AbortController` to `loadCatalog` and cleanup for all `setTimeout` timers.
   - Added explicit "Admin access required" empty state when non-admins visit `#/security`.
5. `web/src/auth/AuthScreens.tsx` — Synced `SignInScreen` `reason` prop updates; aligned `AuthCallback` redirect to `#/dashboards`; URL-encoded invite token in sign-in link; added `onSignedOut` callback to `ResetPasswordScreen`.
6. `web/src/components/ui.tsx` — Extracted shared `RoleBadge` & `ROLE_HELP` (decoupling `ProfilePage` from `SettingsPage` for clean code splitting); replaced hardcoded `id="report-query"` in `ReportLayout` with `useId()`.
7. `web/src/account/ProfilePage.tsx` — Updated `RoleBadge` import to `../components/ui`.
8. `web/src/account/SettingsPage.tsx` — Imported `RoleBadge`/`ROLE_HELP` from `../components/ui`; cleaned up `OneTimeValue` copy timer on unmount; preserved `window.location.pathname` and URL-encoded tokens in invite/reset links.
9. `web/src/workspaces/CohortsLookupsWorkspace.tsx` — Fixed **P1 infinite API loop** by deriving `defStableKey` from `(conds, match)` instead of `Date.now()`-stamped definitions; added 200 ms debounce and `AbortController` to `runPreview`.
10. `web/src/workspaces/UsersWorkspace.tsx` — Replaced ad-hoc CSV export with `buildCsv` + `downloadTextFile`; capped CSV export at 50,000 users with truncation notice; added `AbortController` to `load` and `open`.
11. `web/src/workspaces/InsightsWorkspace.tsx` — Validated `display_currency` against `CURRENCIES` allowlist (preventing `RangeError` crash); debounced `formula` input (350 ms) with `AbortController`; replaced ad-hoc CSV export with `buildCsv` + `downloadTextFile`; fixed duplicate series label generation (`nextSeriesLabel`).
12. `web/src/workspaces/ExperimentsFlagsWorkspace.tsx` — Eliminated double-fetch on initial mount (`selectedExpIdRef` + targeted `handleSelectExperiment`); debounced feature-flag rollout slider `PATCH` calls (250 ms) with local draft state and unmount cleanup.
13. `web/src/workspaces/FunnelsWorkspace.tsx` — Added `AbortController` to `runFunnel`; replaced raw `a.click()` with `downloadTextFile`.
14. `web/src/workspaces/SessionsReplayWorkspace.tsx` — Added `AbortController` to `runSearch`.
15. `web/src/workspaces/LexiconMcpWorkspace.tsx` — Skipped `listApiKeys` call for non-admins; validated MCP JSON arguments are a plain object and enforced `project_id: projectId` after spreading; cleaned up copy timer on unmount.
16. `web/src/components/FilterPanel.tsx` — Added 60-second TTL cache for `useCohortNames` and `topValues`; added stable `id` on `FilterRow` to replace index keys.
17. `web/src/components/WireframePlayer.tsx` — Moved `setPlaying(false)` out of `setCursorMs` updater; guarded `scale` calculation against `fitW <= 12`; passed `AbortController` signal to `getReplay`.
18. `web/src/dashboards/DashboardsWorkspace.tsx` — Wrapped `decodeURIComponent` in `dashboardIdFromHash` in `try/catch`; memoized `handleCardLoaded` with `useCallback`.
19. `web/src/dashboards/ReportCard.tsx` — Guarded `rangeFor` against `NaN` / non-positive day values; added `AbortController` to `ReportCardBody`.
20. `web/src/dashboards/SaveToDashboard.tsx` — Added 32 KB size cap on `decodeQuery` to guard against oversized tampered URL hashes.
