# Pulse Frontend Engineering Audit Report

**Date:** 2026-11-19  
**Scope:** `web/` (Pulse Studio React + TypeScript + Vite SPA, 32 source files, 13,413 LOC)  
**Methodology:** Full line-by-line inspection of all 32 source files, build/bundle analysis, dependency vulnerability audit, typecheck, test suite audit, and backend static-serving inspection.

---

## Executive Summary

Pulse Studio (`web/`) has a clean, disciplined foundation: zero `dangerouslySetInnerHTML`, zero `as any` / `: any` / `@ts-ignore`, zero `console.*` statements, a token-driven Tailwind design system with light/dark support and `prefers-reduced-motion` handling, and good baseline accessibility practices (skip links, `aria-sort`, focus trap in `DropOffDrawer`, `role="status"`/`role="alert"`).

However, the audit uncovered **several P0/P1 production issues** across security, reliability, performance, and state management that must be fixed before serious production deployment:

| Severity | Count | Key Findings |
|---|---:|---|
| **P0 – Critical** | 5 | Unencoded `projectId` in 7 API URLs (path traversal from `#/?p=`); CSV Formula Injection (DDE) in User & Trends CSV exports; crash-inducing unvalidated `display_currency` from URL `?q=` (`RangeError`) and `decodeURIComponent` (`URIError`); no React `ErrorBoundary` (single error white-screens entire app); broken auth state sync after password reset & missing global 401/cross-tab sign-out. |
| **P1 – High** | 7 | **Infinite API request loop** in `CohortsLookupsWorkspace` when any condition has `days > 0` (`Date.now()` in render drives `useEffect`); monolithic **442.09 kB** single JS bundle (no route/workspace code-splitting); rollout slider in `ExperimentsFlagsWorkspace` fires unthrottled `PATCH` requests on every drag tick + double-fetch on mount; 5s `loadStatus` poll re-renders entire `App` tree & recreates `AccessContext` object; uncancelled/racy API requests across workspaces and unbounded loop in Users CSV export; `mintedIngestKey` leaks across project switches; quickstart code snippets lack escaping for event/property names. |
| **P2 – Medium** | 6 | `setState` called inside state updaters (`App.tsx` hashchange, `WireframePlayer.tsx` interval); uncleaned `setTimeout` timers; `FilterPanel` `useTopValues` uncached + `key={i}` row identity; `WireframePlayer` negative scale when container width is 0; unauthorized `#/security` route renders blank screen; `LexiconMcpWorkspace` fires 403 `listApiKeys` for non-admins & allows `project_id` override in MCP JSON args; `vite`/`esbuild` dev-server CVEs. |
| **P3 – Low** | 4 | Minimal test coverage (only 6 unit tests in `src/lib/`); `SettingsPage` invite/reset links drop `window.location.pathname`; `AuthCallback` redirects to `#/funnels` instead of `#/dashboards`; `InsightsWorkspace` series label collision after removing earlier series. |

---

## Dimension-by-Dimension Assessment

### 1. Security
- **Strengths:** No `dangerouslySetInnerHTML`, `innerHTML`, `eval`, or unsafe HTML/Markdown rendering. OAuth callback code is stripped from URL history via `replaceState`. Invite `next=` redirect is regex-validated (`/^invite\/[0-9a-f]+$/`). PII user IDs are masked via `displayUserId`.
- **Weaknesses:**
  - **P0-SEC-1:** 7 methods in `src/api/client.ts` (`listUsers`, `getUserDetail`, `deleteUserDpdp`, `listDeletionAudit`, `exportCohortCsv`, `importEvents`, `exportRawEvents`) interpolate `${projectId}` directly into URL paths without `encodeURIComponent`. Since `projectId` is hydrated from untrusted `#/...?p=` hash parameters, crafted values can traverse API paths.
  - **P0-SEC-2:** `UsersWorkspace.tsx` and `InsightsWorkspace.tsx` build CSVs without neutralizing formula-triggering prefixes (`=`, `+`, `-`, `@`, `\t`, `\r`), exposing analysts opening exported CSVs in Excel/Sheets to CSV/DDE Formula Injection.
  - **P0-SEC-3:** `App.tsx` stores `ps_` session tokens in `localStorage` (backend architectural requirement to migrate to `HttpOnly; Secure; SameSite=Lax` cookies). On the frontend, 401 responses from workspaces other than `loadStatus`/`loadCatalog` do not trigger sign-out, cross-tab logout (`storage` event) is not synchronized, and `ResetPasswordScreen` clears the token without updating `App`'s `signedIn` state.
  - **P1-SEC-4:** `App.tsx` retains `mintedIngestKey` in state when switching projects, displaying Project A's plaintext ingest key inside Project B's quickstart snippet.
  - **P1-SEC-5:** `App.tsx` quickstart snippets interpolate `sampleEvent` and `sampleProp` directly into JS/Kotlin/Swift/bash `curl` snippets without escaping quotes or shell metacharacters.
  - **P2-SEC-6:** `LexiconMcpWorkspace.tsx` spreads user-supplied JSON `...parsedArgs` *after* `project_id: projectId`, allowing override of `project_id` and non-object JSON payloads.
  - **P2-SEC-7:** `web/index.html` lacks `<meta name="referrer" content="strict-origin-when-cross-origin" />`; backend `pulse-node` sets no `CSP`, `X-Frame-Options`, `X-Content-Type-Options`, or `Permissions-Policy` headers (documented as backend requirement).
  - **P2-SEC-8:** `npm audit` reports 2 dev-dependency vulnerabilities (`esbuild <=0.24.2`, `vite <=6.4.3`, high severity on Windows `launch-editor` + moderate dev-server CORS bypass). Production bundle is unaffected, but dev environments are exposed.

### 2. Performance
- **P1-PERF-1 (Critical Bug):** `CohortsLookupsWorkspace.tsx` computes `cohortDefinition()` using `Date.now()` during render, then serializes it to `defKey` which triggers `useEffect -> runPreview() -> setPreviewLoading()`. Whenever any condition has `days > 0`, every render advances `Date.now()`, causing an **infinite `/cohorts/preview` API request loop**.
- **P1-PERF-2:** All 13 workspaces, account pages, and auth screens are eagerly imported in `App.tsx`, producing a single **442.09 kB** (`125.65 kB` gzip) JS chunk. Route-level `React.lazy` splitting will substantially reduce initial JS parse/download cost.
- **P1-PERF-3:** `ExperimentsFlagsWorkspace.tsx` calls `api.patchFlag` on every `onChange` event of the rollout `<input type="range">`, firing dozens of concurrent PATCH requests during a single slider drag, and double-fetches `loadAll()` on initial mount due to `selectedExpId` being both a dependency and mutated inside `loadAll`.
- **P1-PERF-4:** `App.tsx` polls `/v1/status` every 5 seconds and unconditionally calls `setStatus(s)` with a new object reference, while passing an unmemoized `access = { canEdit, canAdmin }` object to `AccessContext.Provider`. This re-renders the entire application shell and every `useAccess()` consumer every 5 seconds even when nothing changed.
- **P1-PERF-5:** `InsightsWorkspace.tsx` triggers `queryInsights` on every keystroke in the `Formula` text input (no debounce, no request cancellation).
- **P1-PERF-6:** `FilterPanel.tsx` `useTopValues` issues uncached 30-day breakdown queries on every mount/row change; `UsersWorkspace.tsx` `exportCsv` has an unbounded pagination loop (`for (let off = 0; ; off += 500)`) with no safety cap.

### 3. Reliability & Error Handling
- **P0-REL-1:** Zero React `ErrorBoundary` components. Any render error crashes the entire app to a blank screen.
- **P0-REL-2:** Unvalidated URL parameters can crash the app:
  - `InsightsWorkspace.tsx`: `display_currency` from `#/trends?q=` is passed directly to `toLocaleString({ style: "currency", currency })`, throwing `RangeError` on invalid currency codes.
  - `DashboardsWorkspace.tsx`: `decodeURIComponent(m[1])` throws `URIError` on malformed percent-encoding in `#/dashboards/%E0%A4%A`.
  - `ReportCard.tsx`: `rangeFor(sel)` produces `NaN` timestamps on non-numeric `sel`.
- **P1-REL-3:** Missing `AbortController` / request sequence guards in `App.tsx` (`loadCatalog`), `UsersWorkspace.tsx` (`open`), `SessionsReplayWorkspace.tsx` (`runSearch`), and `ReportCardBody` (`run`) allow stale responses to overwrite newer selections.
- **P2-REL-4:** Calling `setState` inside another `setState` updater in `App.tsx` (line 215) and `WireframePlayer.tsx` (line 187).
- **P2-REL-5:** Uncleaned `window.setTimeout` timers in `App.tsx`, `SettingsPage.tsx`, and `LexiconMcpWorkspace.tsx`.
- **P2-REL-6:** `fetchJson` in `src/api/client.ts` throws raw `SyntaxError` if a proxy/gateway returns HTML or malformed JSON with a 2xx status.

### 4. Architecture, Code Quality & Accessibility
- **Architecture:** Feature-oriented layout (`auth/`, `account/`, `dashboards/`, `workspaces/`, `components/`, `api/`, `lib/`) is sound and should be preserved. Small targeted improvements: extract shared `ErrorBoundary`, `csv.ts` helper, and `RoleBadge` (to decouple `ProfilePage` from `SettingsPage` for clean code splitting).
- **UI/UX & Accessibility:**
  - Direct navigation to `#/security` by non-admins renders an empty main area instead of an access-denied message.
  - `WireframePlayer.tsx` can compute negative frame dimensions (`(fitW - 12) / viewport.w`) when `fitW <= 12`.
  - `FilterPanel.tsx` uses array index `key={i}` on removable filter rows.
  - `InsightsWorkspace.tsx` can generate duplicate series labels (`B`, `B`) after deleting an earlier series and adding a new one.
  - `UsersWorkspace.tsx` and `InsightsWorkspace.tsx` trigger `a.click()` without attaching the anchor to the DOM (breaks in some Firefox configs; `downloadTextFile` already handles this properly).
- **Testing:** Only 6 unit tests exist (`format.test.ts`, `projectUrl.test.ts`). Zero tests for API URL encoding, CSV injection mitigation, date range parsing, saved report decoding, filter serialization, cohort definition logic, or component error boundaries.
