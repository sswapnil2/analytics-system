# Pulse Remaining Risks & Follow-Up Recommendations

**Date:** 2026-11-19 (Post Hard Review & Backend Security Implementation)

---

## 1. Remaining Security Follow-Ups

1. **Server-Generated CSV Formula Injection (`P2`):**
   - Client-generated CSVs (`UsersWorkspace`, `InsightsWorkspace`) are protected via `web/src/lib/csv.ts`, but 3 server-generated CSV endpoints (`exportCohortCsv`, `exportFunnelDropped`, `exportRawEvents?format=csv`) emit CSV from Rust. Apply the same formula-prefix neutralization (`^[ \t\r\n]*[=+\-@]`) in the Rust CSV writers.
2. **Optional Token Field Removal in Browser Login JSON (`P3`):**
   - `web/` now uses the `HttpOnly` `pulse_session` cookie exclusively and no longer stores `ps_` tokens in `localStorage`. The backend still returns `"token"` in login JSON for CLI/API backwards compatibility; once non-browser clients use dedicated endpoints or accept cookies, `"token"` can be omitted for browser logins.
3. **Inline Theme Script CSP Hash Maintenance (`P3`):**
   - `STUDIO_CSP` in `crates/pulse-node/src/lib.rs` pins the exact SHA-256 hash of the 6-line theme initializer in `web/index.html`. Any edit to that inline script requires updating the hash (or extracting it to a non-immutable static `/theme-init.js` file).

---

## 2. Dependency & Tooling Gaps

4. **Dev-Server Advisories (`vite <=6.4.3` / `esbuild <=0.24.2`) (`P2`):**
   - Dev-only advisories (not bundled in `web/dist`). Upgrade to `vite@8.x` in a dedicated PR.
5. **No Frontend Linter or Browser E2E Suite (`P2`):**
   - Add ESLint (`eslint-plugin-react-hooks`, `eslint-plugin-jsx-a11y`) and Playwright E2E tests against `pulse-node` in CI.
   - Add unit tests for `setUnauthorizedHandler`, `decodeQuery`, and `rangeFor` helpers.

---

## 3. Scalability & UX

6. **Large Lexicon Tables (`P3`):**
   - `LexiconMcpWorkspace` renders all events/properties without search or virtualization; add search filtering and virtualization for projects with 1,000+ distinct events/properties.
7. **`React.lazy` Chunk-Load Retry (`P3`):**
   - Clicking "Try again" in `ErrorBoundary` after a network chunk-load failure cannot reset a rejected `React.lazy` promise (users must click "Reload page", which is already provided).
8. **Live Web Vitals Measurement (`P3`):**
   - Measure LCP, INP, and CLS on a staging deployment with real production datasets.
