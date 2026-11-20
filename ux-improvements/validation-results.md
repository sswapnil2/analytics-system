# Validation results

## Commands
| Command | Result |
|---|---|
| `npm --prefix web test` | 19 tests, 19 pass (4 new in `chart.test.ts`); rerun after the second pass: 19 pass |
| `npm --prefix web run build` (typecheck + Vite) | pass (both passes) |
| Demo-name grep on new files | no hits |

Rust: the cohort API branch's session ran `cargo test -p pulse-control` and the workspace check; here the merged tree built in release. `scripts/verify_e2e.sh` was not run. There is no frontend linter configured in `web/`.

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

### Third pass (Journeys, Cohorts, segmentation, Users), 20 Nov 2026
Fresh node with the SaaS events plus 120 profiles (`plan`, `country`, `company`) sent to `/v1/engage`; demo node `ux-demo-2`.

| Checked | Node / theme / size | Result |
|---|---|---|
| Journeys step table sums: step 1 = 108 + 107 + 55 + 21 = 291 | fresh, light, 1440 | ✅ |
| Journeys node / path hover: only connected items lit (3 of 21 paths), tooltip text and share correct, next to the pointer | fresh, light, 1440 | ✅ (was clipped, then far from pointer → fixed) |
| Tab through step rows lights the node and its neighbours | fresh | ✅ |
| Key event picker by keyboard (Enter, "invite", Enter) | fresh | ✅ "After Invite Sent" |
| Filter panel picker: profile properties grouped; value suggestions pro/team/free and BR/IN/DE/US | fresh | ✅ |
| Cohort "Did Signed Up ≥1" = 300 (300 users in the sent data) | fresh | ✅ |
| Cohort event picker in a row | fresh, 1440 | ❌ squeezed to "P…" → fixed (`basis-36`, row wraps) → ✅ |
| + "Has property" profile plan = pro → 36 (36 profiles with plan pro) | fresh | ✅ |
| Save cohort: listed with description and saved date; offered in Trends "Users in cohort" (291 → 36), then filter country = DE → 6 | fresh | ✅ |
| Users: open u80 → profile in view and focused; Back to the list focuses the row; clearing the search restores the full page (35 → 50 rows) | fresh, light, 1440 | ✅ |
| Users at 375: first open did not scroll or focus | fresh, dark, 375 | ❌ → fixed (effect after commit, rerun after load) → ✅ top 16px, focused |
| Users: no Revenue column when all revenue is 0; shown on demo | fresh / demo | ✅ |
| No horizontal page scroll at 375: Trends, Journeys, Cohorts, Users | fresh, dark | ✅ 375 = 375 |
| Picker popover at 375 stays on screen (49–299 px) | fresh, dark | ✅ |
| Journeys step table at 375 | fresh, dark | ❌ cramped → fixed → ✅ |
| Regression: demo Journeys (8,000 per step), Cohorts preview, drop-off drawer, replay plays | demo, light, 1440 | ✅ |
| `npm --prefix web test` 21/21, build, demo-name grep on changed files | — | ✅ |

### Fourth pass (owner pass), 20 Nov 2026
| Check | Node | Result |
|---|---|---|
| Open saved report → "Saved as App Open"; Column→Line shows Unsaved + Save; back to Column clears it | demo, light, 1440 | ✅ |
| Line + Save sends PUT; stored query has no time range; reload opens as Line, clean | demo | ✅ |
| Funnel, retention, daily-active saved reports open clean | demo | ✅ |
| Experiments: metric picker starts "Choose…"; OTP Verified estimate "about 297 users per variant"; App Open shows the 100% message; rollout 50→51 shows "Saved" (reset to 50) | demo | ✅ |
| Events and privacy: search "city" filters both tables; hide → note → keep restored; Drop with confirm cancelled stays active | demo | ✅ |
| Security: licence message for the audit log (no licence key on the node) | demo | ✅ |
| No horizontal scroll at 375: Security, Events, Experiments | demo, dark | ❌ Security 407 px, Experiments 381 px → fixed → ✅ 375 |
| Funnel step where: keyboard pick "city" → suggestions Bengaluru, Mumbai, Delhi_NCR, … | demo | ✅ |
| Funnel step where at 375 | demo | ❌ page 612 px, "is" clipped, property pre-filled → fixed → ✅ 375, starts "Choose…" |
| Dashboard on 14D: daily card asks for `day`, weekly saved card asks for `week` and draws weekly buckets | demo | ✅ |
| Trends export: SVG renders standalone (title, range, axes, lines, legend); PNG 1492×712, non-blank | demo, light | ✅ |
| Cohort CSV export: `…-users.csv`, 85 KB, header `canonical_uid,distinct_id,plan,…` | demo | ✅ |
| Lookup upload | demo | ❌ join key pre-filled, typed key appended → fixed → ✅ "Matched on city. Adds tier (2 rows)"; re-upload replaced the bad table |
| `npm test` 22/22, `npm run build`, demo-name grep and secret grep on the diff | — | ✅ |
| Cohort edit/delete, on a node built from this branch with the merged API: Edit loads "Did UPI Autopay Mandate Approved 1+ times, ever"; rename + count 2 → Save sends PUT; same id, created date kept, "Updated …" | demo, dark, 1024 | ✅ |
| After saving an edit the builder still held the cohort under "Save cohort" (a second click would copy it) | demo | ❌ → clears after update → ✅ |
| Saved cohort row in the narrow panel: name wrapped one word per line | demo | ❌ → buttons wrap under the text → ✅ |
| Delete while a saved report uses it: confirm shown, then "…still used by report 'Autopay users trend'…", cohort kept | demo | ✅ |
| Report removed → Delete → list empty, error cleared; Trends no longer offers it | demo | ✅ |
| New cohort → Edit → Cancel clears the builder; Trends offers the new cohort | demo | ✅ |
| Cohorts at 375 | demo, dark | ✅ 375 |
| `npm test` 24/24, build; `cargo build -p pulse-node --release` with the merge | — | ✅ |

## Not checked
- Reduced motion was not emulated in the browser; it relies on the existing global CSS rule plus `motion-safe:` prefixes.
- Safari/Firefox: `d` path tweening is Chromium-only by design; not opened in other browsers.
- Screen reader pass not done (roles/labels were checked in the accessibility tree).
- Experiments, Events and privacy, Security: opened on both nodes but not exercised in depth.
- Chart export in dark mode was not opened as a file (the panel colour is read from the theme, so it exports dark).
- Fourth-pass checks ran on the demo node only; the fresh node was not revisited.
- Hover sync and the picker on a touch device: there is no hover on touch; rows are still tappable to focus. Not tried on a real phone.
- Some interaction steps were driven by in-page script clicks (chart-type menu sweep, one Group by change); the rest used real clicks and keys.

Note: screenshots in the browser pane sometimes catch a 140–240 ms animation mid-way (faded menus or charts). Computed styles read immediately after showed opacity 1 and final geometry.
