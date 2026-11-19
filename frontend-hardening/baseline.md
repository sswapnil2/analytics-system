# Pulse Frontend Baseline Metrics

**Captured:** 2026-11-19 (prior to any code modifications)  
**Environment:** macOS, Node.js (`node --test`), Vite `5.4.21`, TypeScript `5.6.3`, React `18.3.1`, Tailwind CSS `3.4.15`.

---

## 1. Codebase Footprint

- **Source files (`web/src/`):** 32 files (30 `.ts`/`.tsx`, 1 `.css`, 2 `.test.ts` included in count)
- **Total Lines of Code:** 13,413 lines
- **Largest files:**
  - `src/api/client.ts` — 1,144 lines
  - `src/workspaces/FunnelsWorkspace.tsx` — 939 lines
  - `src/App.tsx` — 905 lines
  - `src/dashboards/DashboardsWorkspace.tsx` — 884 lines
  - `src/auth/AuthScreens.tsx` — 877 lines
  - `src/components/ByokCommercialPanel.tsx` — 861 lines
  - `src/workspaces/InsightsWorkspace.tsx` — 830 lines
  - `src/workspaces/ExperimentsFlagsWorkspace.tsx` — 792 lines

---

## 2. Build & Bundle Size Baseline (`npm run build`)

- **Build command:** `tsc --noEmit && vite build`
- **Build time:** `5.13s` (1,603 modules transformed)
- **Production assets (`web/dist/`):**

| Asset | Raw Size | Gzip Size |
|---|---:|---:|
| `dist/index.html` | 0.71 kB | 0.42 kB |
| `dist/assets/index-BOHgfDFu.css` | 34.95 kB | 7.74 kB |
| `dist/assets/index-B3fTd-O9.js` | **442.09 kB** | **125.65 kB** |
| `dist/assets/instrument-sans-latin-wght-normal-*.woff2` | 30.09 kB | — |
| `dist/assets/instrument-sans-latin-ext-wght-normal-*.woff2` | 11.14 kB | — |

- **Code splitting:** None (1 monolithic JS entry chunk containing all 13 workspaces, auth flows, account pages, and replay player).

---

## 3. Quality & Security Tooling Baseline

| Check | Status | Details |
|---|---|---|
| `npm run typecheck` (`tsc --noEmit`) | PASS | 0 errors (`strict: true`, `noUnusedLocals: true`, `noUnusedParameters: true`) |
| `npm test` (`node --test src/lib/*.test.ts`) | PASS | **6 tests**, 6 passed, 0 failed (`184 ms`). Only covers `format.ts` and `projectUrl.ts`. |
| ESLint / Linter | NOT CONFIGURED | No eslint config or lint script in `package.json`. |
| `npm audit` | **2 vulnerabilities** | 1 moderate (`esbuild <=0.24.2` GHSA-67mh-4wv8-2f99), 1 high (`vite <=6.4.3` via `launch-editor` & dev-server path traversal advisories). Both are `devDependencies`. |
| Static security grep | PASS | 0 `dangerouslySetInnerHTML`, 0 `innerHTML`, 0 `eval`/`new Function`, 0 `as any`/`: any`, 0 `@ts-ignore`, 0 `console.*`. |

---

## 4. Runtime & Network Behavior Baseline

- **Initial route load:** Downloads and parses all 442.09 kB JS upfront regardless of whether user is on `#/signin` or `#/dashboards`.
- **Background polling:** `App.tsx` polls `GET /v1/status` every 5,000 ms while visible and unconditionally triggers a full root `App` + `AccessContext` re-render on every poll response.
- **Cohorts preview (`CohortsLookupsWorkspace`):** When any condition has `days > 0`, `Date.now()` inside render invalidates `defKey` on every render, causing a continuous unbounded loop of `POST /v1/projects/:id/cohorts/preview` requests.
- **Feature flag rollout slider (`ExperimentsFlagsWorkspace`):** Fires one `PATCH /v1/projects/:id/flags/:flagId` per slider `onChange` tick without debouncing.
- **Experiments initial load:** Double-fetches `getFlags`, `getExperiments`, and `getExperimentResults` on initial mount because `loadAll` mutates its own dependency `selectedExpId`.
- **Trends formula input (`InsightsWorkspace`):** Fires `POST /query/insights` on every keystroke with no debounce.
