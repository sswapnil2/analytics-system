# Pulse Frontend & Backend Security Findings

**Date:** 2026-11-19 (Updated after External Hard Review + Backend Security Hardening)

---

## 1. Fixed Frontend Security Issues

| ID | Severity | Finding | Fix Implemented & Verified |
|---|---|---|---|
| **P0-SEC-1** | Critical | **API Path Traversal via unencoded `projectId`**: 7 methods in `src/api/client.ts` interpolated `${projectId}` raw into URLs. | Added `projectPath(projectId, suffix)` using `encodeURIComponent(projectId)` across all 7 methods. Verified via `src/lib/security.test.ts`. |
| **P0-SEC-2** | Critical | **CSV / Formula Injection (DDE)** in client-generated CSVs (`UsersWorkspace.tsx`, `InsightsWorkspace.tsx`). | Created `src/lib/csv.ts` (`formatCsvCell`, `buildCsv`) neutralizing leading `=`, `+`, `-`, `@`, `\t`, `\r`, `\n`, and leading spaces before formula prefixes. Verified via `src/lib/csv.test.ts`. |
| **P0-SEC-3** | Critical | **Session Token Exposure & Auth Desync**: `ps_` tokens were persisted in JS-readable `localStorage`; 401s across workspaces and cross-tab sign-outs were not synchronized; `ResetPasswordScreen` left `App` signed in. | Migrated `web/src/api/client.ts` to `credentials: "same-origin"` using the server-set `HttpOnly` `pulse_session` cookie (no `ps_` token is ever stored in `localStorage`; only a non-secret boolean hint `pulse_has_session` is kept for boot/cross-tab sync). Added idempotent global 401 handler (active only while signed in), cross-tab `storage` sync (including `localStorage.clear()` & account switches), and `ResetPasswordScreen` `onSignedOut`. |
| **P0-SEC-4** | High | **RBAC UI Fail-Open**: `App.tsx` previously treated `user === null` as admin while `/v1/auth/me` was loading or on non-404 errors. | Changed `App.tsx` to fail closed to `viewer` until `/v1/auth/me` resolves; only explicit admin-token sessions (`sk_...` in `sessionStorage`) bypass `user` role checks. |
| **P1-SEC-5** | High | **Cross-Account Cache & Key Leakage**: `mintedIngestKey` persisted across project switches; `FilterPanel` module caches were never cleared on sign-out/account switch. | Cleared `mintedIngestKey` and called `clearFilterCaches()` on `switchProject`, `signOut`, `handleSignedIn`, and cross-tab account switches. |
| **P1-SEC-6** | High | **Quickstart Snippet / Shell Escaping**: Unescaped interpolation in JS/Kotlin/Swift/bash `curl` snippets (including Kotlin `$` interpolation). | Used `JSON.stringify`, Kotlin `\$` escaping, and POSIX `shellSingleQuote`. |
| **P2-SEC-7** | Medium | **MCP Tool Argument Override & BYOK Secret Retention**: `LexiconMcpWorkspace.tsx` allowed `parsedArgs` to overwrite `project_id`; `ByokCommercialPanel` retained `draftSigningSecret`/`draftStorageAccessKey`. | Enforced `project_id: projectId` after plain-object validation; cleared BYOK secret drafts after save and added `autoComplete="off"`. |

---

## 2. Implemented Backend Security Improvements (`crates/`)

1. **HTTP Security Headers Middleware (`crates/pulse-node/src/lib.rs`):**
   - Attached `security_headers_middleware` to both `admin_router` and `spa_router` (leaving public SDK routes `/v1/ingest`, `/v1/identify`, `/v1/engage`, `/v1/flags/:key` untouched with permissive CORS):
     - `X-Content-Type-Options: nosniff`
     - `X-Frame-Options: DENY`
     - `Referrer-Policy: strict-origin-when-cross-origin`
     - `Permissions-Policy: camera=(), microphone=(), geolocation=()`
     - `Content-Security-Policy: default-src 'self'; script-src 'self' 'sha256-CHRePUA+2hu8+7Fc1MQN0Ox3SoQuGEleoBo20ZdmAyU='; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'`
     - `Strict-Transport-Security: max-age=31536000; includeSubDomains` (when served over HTTPS or `X-Forwarded-Proto: https`)
2. **`HttpOnly; SameSite=Lax` Session Cookies (`crates/pulse-control/src/api/auth.rs`, `accounts.rs`, `api.rs`):**
   - Issued `Set-Cookie: pulse_session=<token>; HttpOnly; SameSite=Lax; Path=/; Max-Age=2592000` (with `; Secure` on HTTPS / `X-Forwarded-Proto: https` / `Forwarded: proto=https`) on `/v1/auth/signup`, `/v1/auth/login`, `/v1/auth/exchange`, `/v1/auth/verify-email`, and `/v1/auth/invites/accept`.
   - Revoked session and cleared cookie (`Max-Age=0`) on `/v1/auth/logout`.
   - Accepted `pulse_session` cookie in `require_admin_auth` (strictly restricted to `ps_` user sessions; root admin keys / project keys are rejected in cookies) while keeping `Authorization: Bearer` 100% backwards-compatible for CLI/SDK/API clients.
3. **CSRF Protection for Cookie-Authenticated Mutations (`crates/pulse-control/src/api.rs`):**
   - Enforced `verify_cookie_csrf` on `POST`, `PUT`, `PATCH`, `DELETE` requests authenticated via cookie: requires `Sec-Fetch-Site: same-origin` (with no conflicting `Origin`) or matching `Origin` header, rejecting cross-site or headerless mutations with `403 Forbidden`.

---

## 3. Remaining Backend Recommendations

1. **Server-Generated CSV Exports:** Three backend endpoints (`api.exportCohortCsv`, `api.exportFunnelDropped`, `api.exportRawEvents?format=csv`) generate CSV server-side and bypass `web/src/lib/csv.ts`; add identical formula-prefix neutralization on those Rust CSV writers.
2. **Optional CSP Script-Hash Elimination:** Extracting the 6-line inline theme script from `web/index.html` into a static `/theme-init.js` file would allow removing the SHA-256 hash from `STUDIO_CSP` (`script-src 'self'`).
3. **Dev Dependency Advisories (`vite <=6.4.3` / `esbuild <=0.24.2`):** Dev-only advisories (not bundled in `web/dist`); plan a major upgrade to `vite@8.x` in a dedicated PR.
