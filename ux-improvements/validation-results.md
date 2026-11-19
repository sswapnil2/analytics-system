# Validation results

## Commands
| Command | Result |
|---|---|
| `npm --prefix web test` | 19 tests, 19 pass (4 new in `chart.test.ts`); rerun after the second pass: 19 pass |
| `npm --prefix web run build` (typecheck + Vite) | pass (both passes) |
| Demo-name grep on new files | no hits |

No backend code changed, so no Rust tests were run. There is no frontend linter configured in `web/`.

## In the browser
Nodes: the release `pulse-node` (backend identical to this branch) serving this branch's `web/dist`. Demo node seeded; fresh node with a SaaS schema (2,380 events over 14 days).

| Checked | Node / theme / size | Result |
|---|---|---|
| Trends: line chart, hover ring, tooltip with change % and full date | demo, dark + light, 1440 | ✅ |
| Chart menu without breakdown: stacked/pie disabled with reason | demo | ✅ |
| Breakdown by city: all kinds enabled; legend names are segments | demo | ✅ |
| Stacked column: band under pointer focused, others dimmed, tooltip bolds it | demo | ✅ |
| Pie: donut, total 8,000; hiding Mumbai via legend → 6,280 and shares recomputed | demo | ✅ |
| Switching kinds makes no `/insights` request (resource timings cleared, then switched twice) | demo | ✅ only `/v1/status` polls |
| Keyboard: menu opens with focus on first item; ArrowDown ×2 + Enter picks Stacked area | demo | ✅ |
| Kind survives reload (URL state) | demo | ✅ |
| Save to dashboard with Stacked area → card draws stacked area | demo | ✅ |
| Dashboard card hover tooltip; card series named by event | demo | ✅ |
| Dashboard card menu (refactored `Menu`) opens with 5 items, focus on first | demo | ✅ |
| Sidebar collapse to icon rail and back; charts refill wider cards | demo | ✅ |
| Funnel drop-off drawer opens, replay plays | demo | ✅ |
| 375px: chart readable, no horizontal page scroll on Trends and Dashboard | demo, light | ✅ (was 399px wide before the fix) |
| SaaS schema: Trends defaults to Page Viewed, 291 users; country breakdown enables stacked/pie | fresh | ✅ |
| Two Unique-users metrics: stacked/pie disabled ("don't add up") | fresh | ✅ |


### Second pass (P3 fixes), 20 Nov 2026
Fresh node started on an **empty** data dir; demo checks on a separate seeded node (`ux-demo-2`, port 18133).

| Checked | Node / theme / size | Result |
|---|---|---|
| Every page before any event: Dashboards, Funnels, Trends, Journeys, Retention, Replays, Users, Cohorts, Experiments, Events and privacy, Security | fresh, empty | ✅ each explains the next step |
| SaaS schema sent to `/v1/ingest` (2,380 events, `Page_Viewed`, `Signed_Up`, `Project_Created`, `Invite_Sent`; `plan`, `country`) | fresh | ✅ 200; pickers list only `country`, `plan` |
| Breakdown picker by keyboard: ArrowDown opens, type "cou", Enter; focus returns to the button; recents stored | fresh, light, 1440 | ✅ segments 80+74+73+64 = 291 = total |
| Picker by mouse: Recent group, pick "plan", Escape, outside click; opens upward near the bottom of the window | fresh, light, 1440 | ✅ pro 105 / free 96 / team 90 = 291 |
| Picker when `localStorage.setItem` throws | fresh | ✅ choice applied, no error |
| Picker at 375: full width, popover inside the screen, "No properties match" message | fresh, dark, 375 | ✅ |
| Row hover → series focused (others 0.18); legend hover and plot hover → row highlighted | fresh, light, 1440 | ✅ |
| Keyboard: Tab from Export CSV through sort headers into rows; focused row focuses its series, ring visible | fresh, light, 1440 | ✅ |
| Group by beside chart type; hidden for Pie and Number, back for Column/Stacked area/Line | fresh, 1440 | ✅ |
| Five chart-type switches: `/insights` requests 2 → 2 | fresh | ✅ |
| Group by week: URL query keys unchanged (`interval` = "week"), survives reload | fresh | ✅ |
| Cohort breakdown through picker (ArrowUp wraps to last option) | demo | ✅ In cohort 2,507 / Not 5,493 |
| Save Column + week + cohort split to dashboard → card draws columns; reopening the report restores week, Column, cohort | demo, light | ✅ |
| Dashboard card legend for a cohort split | demo | ❌ showed `in_cohort` → fixed → ✅ "In cohort / Not in cohort" |
| Funnels Split by picker (keyboard search "pla" + Enter) | fresh | ✅ |
| Funnels segment bars use `--chart-1…4`; histogram tooltip, right edge stays inside the panel, `shadow-pop` | fresh, dark, 1440 | ✅ |
| Retention Split by picker; shared Recent list from Funnels | fresh | ✅ |
| Retention curve `--chart-1` (lime in dark), compare curve `--chart-2` dashed, tooltip text and legend match | fresh dark; demo light (7D, previous period) | ✅ |
| No horizontal page scroll at 375: Trends, Funnels, Retention, Dashboards, Replays | demo, dark + light | ✅ 375 = 375 |
| Regression: drop-off drawer opens, replay plays, experiment results table | demo | ✅ |
| Demo-name grep on changed files | — | no hits |

## Not checked
- Reduced motion was not emulated in the browser; it relies on the existing global CSS rule plus `motion-safe:` prefixes.
- Safari/Firefox: `d` path tweening is Chromium-only by design; not opened in other browsers.
- Screen reader pass not done (roles/labels were checked in the accessibility tree).
- Journeys, Cohorts, Experiments, Users: opened on both nodes (empty and seeded) but not changed or exercised in depth.
- Hover sync and the picker on a touch device: there is no hover on touch; rows are still tappable to focus. Not tried on a real phone.
- Some interaction steps were driven by in-page script clicks (chart-type menu sweep, one Group by change); the rest used real clicks and keys.

Note: screenshots in the browser pane sometimes catch a 140–240 ms animation mid-way (faded menus or charts). Computed styles read immediately after showed opacity 1 and final geometry.
