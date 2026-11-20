# Implementation log

All changes are in `web/`. No new dependencies; no backend changes on this branch (cohort edit/delete endpoints were built separately, see the fourth pass).

| File | Change |
|---|---|
| `src/lib/chart.ts` (new) | Chart kinds list, `isAdditive`, `chartKindBlocker`, `niceTicks`, `pctChange`, `stackBases`, `donutSlice`. |
| `src/lib/chart.test.ts` (new) | Tests for ticks, stacking/pie rules, percent change, stack bases. |
| `src/components/TrendChart.tsx` (new) | `TrendChart` (all time-series kinds + pie, hover focus, tooltip, legend toggling, annotations, previous-period ghost line, compact mode, measured width) and `ChartKindPicker`. |
| `src/components/Menu.tsx` (new) | The dashboard's keyboard-accessible menu, moved here and extended: `menuitemradio` items, icons, groups with dividers, disabled hints, left/right alignment. |
| `src/workspaces/InsightsWorkspace.tsx` | Uses `TrendChart` + `ChartKindPicker`; chart kind saved in the query and restored from saved reports/links; hidden series state, reset when the query shape changes; segment naming fix; 8-colour palette. ~230 lines of bespoke SVG removed. |
| `src/dashboards/ReportCard.tsx` | Trend cards use `TrendChart` (compact) with the saved kind, hover tooltips, legend toggles, event names; date-range popover uses motion/elevation tokens. |
| `src/dashboards/DashboardsWorkspace.tsx` | Imports the shared `Menu`; popover tokens. |
| `src/dashboards/SaveToDashboard.tsx` | Popover tokens. |
| `src/workspaces/FunnelsWorkspace.tsx` | Drop-off drawer slides in, backdrop fades. |
| `src/App.tsx` | Collapsible sidebar (icon rail, titles on icons, persisted). |
| `src/components/ui.tsx` | `PageHeader` actions may shrink (phone overflow). |
| `src/index.css` | `--chart-1…8` tokens for light and dark; `.chart-geom` transitions. |
| `tailwind.config.js` | Easing, keyframes, animations, `shadow-pop`. |

### Second pass (P3 fixes)
| File | Change |
|---|---|
| `src/components/TrendChart.tsx` | Focused series can be controlled (`focus` / `onFocus`); chart hover, legend hover and pie hover all set it. Uncontrolled on dashboard cards. |
| `src/workspaces/InsightsWorkspace.tsx` | Focus state lifted here and shared with the "By segment" table: hovering or keyboard-focusing a row focuses its series; hovering a series highlights its row. Colour swatch per row (faded when hidden by the legend). A stale or hidden focus key is ignored so the chart never dims everything. "Group by" moved into the chart header beside the chart type and hidden for Pie and Number; query and URL format unchanged. "Break down by" uses the new picker (properties + cohorts). |
| `src/components/PropertyPicker.tsx` (new) | Searchable single-choice picker: search box (`role=combobox`, `aria-activedescendant`), grouped `listbox`, Arrow/PageUp/PageDown/Enter/Escape/Tab, recents (5 per project, `localStorage` in try/catch, shared by all reports), opens upward when there's more room above. Lists only what it is given; no API calls. |
| `src/workspaces/FunnelsWorkspace.tsx` | "Split by" uses the picker. Segment colours use `--chart-1…4` (the third is the same amber `--warn` used before). Time-to-complete bars use `--chart-2` and get a hover tooltip (`shadow-pop`, `animate-pop`) instead of the native `title`; the bin list is in the chart's `aria-label`. Drop-off hatch unchanged. |
| `src/workspaces/RetentionWorkspace.tsx` | "Split by" uses the picker. Curve, compare curve, legend and tooltip text use `--chart-1` / `--chart-2`; tooltip uses `shadow-pop` + `animate-pop`. Heatmap unchanged. |
| `src/lib/format.ts` | `segmentName` (in_cohort → "In cohort"), shared by Trends and dashboard cards. |
| `src/dashboards/ReportCard.tsx` | Bug fix: cohort-split trend cards showed raw `in_cohort` / `not_in_cohort` in the legend and headline. |

### Third pass (Journeys, Cohorts, segmentation, Users)
| File | Change |
|---|---|
| `src/workspaces/FlowsWorkspace.tsx` | Journeys: pointing at a step or path lights only what it connects to (others dim) with a tooltip by the pointer ("Signed Up · 108 users · 37% of step 1"; "Page Viewed → Signed Up · 108 users · 37% of Page Viewed"); the tooltip sits outside the scroll box so it isn't clipped. New "Step by step" table under the diagram (step, swatch, event, users, share of step, "most common"); rows are focusable and light their node. Key event uses the picker (search, recents; no "none"). Phone: "most common" is screen-reader only and the header reads "Share" so the table fits 375px. |
| `src/workspaces/CohortsLookupsWorkspace.tsx` | Condition rows use the dense picker for Event (search events, recent events) and Property (event and profile properties grouped); property values suggest real top values. Empty preview explains what to pick. Saved cohorts show their saved date. Lookup "Match on property" suggests known properties. |
| `src/components/FilterPanel.tsx` | Filter property uses the dense picker with profile properties grouped; `useTopValues` exported for Cohorts. |
| `src/components/PropertyPicker.tsx` | `dense` mode (screen-reader label, short button, `basis-36` so it wraps in a row instead of shrinking to "P…"), `placeholder`, required mode (no "none"), wider popover on narrow buttons. |
| `src/lib/format.ts` | `propertyOptions`: `user.*` properties listed as "User profile" after "Event properties", without the prefix. Used by every picker. Tests added. |
| `src/workspaces/UsersWorkspace.tsx` | Opening a user scrolls the profile into view and moves focus to it (from an effect, so it also works on the first open, and again after loading so it reaches the top). "Back to the list" returns focus to that row. Revenue column and lifetime revenue hidden when every user has 0. Clearing the search shows everyone again. Search box fills the width on phones. |
| `src/workspaces/{Insights,Funnels,Retention}Workspace.tsx` | Pickers use `propertyOptions`. |

### Fourth pass (owner pass: remaining gaps)
| File | Change |
|---|---|
| `src/api/client.ts`, `src/dashboards/SaveToDashboard.tsx` | Save in place. The API already had `PUT /saved-reports/:id`; the earlier "needs backend" note was wrong. An opened report shows "Saved as {name}" or "Unsaved changes" + Save (PUT, then the baseline resets). The other button becomes "Save a copy". Viewers see only the badge. The baseline ignores the time range, so changing the range alone isn't "unsaved". |
| `src/workspaces/{Insights,Funnels,Retention,Flows}Workspace.tsx` | Wired to the saved state. |
| `src/workspaces/ExperimentsFlagsWorkspace.tsx` | Conversion and guardrail metrics use the searchable picker, no default (the busiest event usually can't move). Sample estimate explains 100% and 0% base rates instead of showing nonsense. Rollout change shows "Saved" for 2.5 s. "Who" select no longer widens the page at 375. |
| `src/workspaces/LexiconMcpWorkspace.tsx` | Events and privacy: one search filters both tables, with a no-match row. Saving shows what happened in words. "Drop at ingest" asks first, naming the 24 h volume and saying it can't be undone. |
| `src/components/SecurityPanel.tsx` | Plain-language encryption copy; turning encryption off asks first. Labels on unlabeled inputs. The audit log shows the licence message on 402 instead of a misleading empty state; removed the unsupported "Cryptographically tracked" claim. Copilot preset buttons wrap at 375. |
| `src/workspaces/FunnelsWorkspace.tsx` | Step "where" uses the dense picker and suggests real values for that step's event. It starts on "Choose…" instead of the first property alphabetically. Step rows wrap so a where clause fits at 375 (was 612 px wide). Operator and value controls are the picker's height (the "is" text was clipped). |
| `src/lib/exportChart.ts` (new), `src/workspaces/InsightsWorkspace.tsx` | Trends "Download chart" → PNG (2×) or SVG. The on-screen SVG with computed colours inlined, plus title, date range and legend. Disabled with a reason when there's no data or the kind is Number. |
| `src/index.css` | Dark `--chart-3` is now golden yellow (248 200 20); the old amber was hard to tell apart from the drop-off orange. Light is unchanged. |
| `src/lib/chart.ts`, `src/dashboards/ReportCard.tsx` | Dashboard cards use the report's saved interval (`cardInterval`), and coarsen it only when it would make too many buckets (hour > 7 days, day > 90 days). Axis and tooltip labels follow the bucket size. Test added. |
| `src/workspaces/CohortsLookupsWorkspace.tsx` | Lookup "Match on property" is the picker and starts empty. It used to pre-fill the first property, so typing a key appended to it (found when re-testing the upload). |

Backend (separate branch, not on this one): `claude/cohort-update-delete-api-4c4cc9`, commit `e8c16ab`. It adds `PUT` and `DELETE /v1/projects/:id/cohorts/:cohort_id`; delete returns 409 naming whatever still uses the cohort. The Cohorts edit/delete UI waits for that branch to be merged.

## Decisions
- **Hover sync, not row checkboxes.** Rows and series highlight each other; hiding stays on the legend so there is one place to toggle.
- **Legend toggles instead of table checkboxes.** Mixpanel uses table checkboxes; Pulse's table is below the fold, so the legend is where the user's eyes are.
- **Stacking rules are strict.** Unique users of two different events overlap, so stacking them would sum the same people twice; averages, percentiles, rolling actives and formulas don't add up. Those keep Line/Area/Column/Number. Unique users split by one property, and totals/sessions/sums, can stack and be shown as a pie.
- **An invalid saved kind falls back to Line** with a one-line note, and returns automatically when the query allows it again.
- **No Table kind.** Pulse always shows the segment table under the chart; a Table kind would duplicate it.
- **No chart library.** The existing hand-written SVG approach was extended; a library would have added weight for the same result.
