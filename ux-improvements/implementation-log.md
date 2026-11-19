# Implementation log

All changes are in `web/`. No new dependencies; no backend changes.

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

## Decisions
- **Hover sync, not row checkboxes.** Rows and series highlight each other; hiding stays on the legend so there is one place to toggle.
- **Legend toggles instead of table checkboxes.** Mixpanel uses table checkboxes; Pulse's table is below the fold, so the legend is where the user's eyes are.
- **Stacking rules are strict.** Unique users of two different events overlap, so stacking them would sum the same people twice; averages, percentiles, rolling actives and formulas don't add up. Those keep Line/Area/Column/Number. Unique users split by one property, and totals/sessions/sums, can stack and be shown as a pie.
- **An invalid saved kind falls back to Line** with a one-line note, and returns automatically when the query allows it again.
- **No Table kind.** Pulse always shows the segment table under the chart; a Table kind would duplicate it.
- **No chart library.** The existing hand-written SVG approach was extended; a library would have added weight for the same result.
