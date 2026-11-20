# Comparison with Pulse

| # | Observed in Mixpanel (where) | Pulse before | Pulse should | Worth it? | Status |
|---|---|---|---|---|---|
| 1 | Grouped chart-type menu, invalid kinds greyed (Insights chart header) | Line/Bar/Number toggle on Trends only | Menu with Line, Area, Stacked area, Column, Stacked column, Pie, Number; disabled with reason | Yes, P1 | Done |
| 2 | Switching kinds redraws without a new query (Insights) | Same for the 3 kinds | Same for all kinds | Yes | Done, verified 0 extra requests |
| 3 | Hover focuses nearest series, others dim (Insights, breakdown) | All series full strength | Focus + dim, legend hover too | Yes, P1 | Done |
| 4 | Tooltip: full date, value, change vs previous point; ring on point | Values only, native `<title>` on dots | Add change %, ring, "so far" | Yes, P1 | Done |
| 5 | Series visibility via table checkboxes (Insights table) | None | Legend toggles (table is below the fold in Pulse) | Yes, P1 | Done (legend) |
| 6 | Donut pie with total (Pie kind) | No pie | Donut, only for parts of a whole | Yes | Done |
| 7 | Round y ticks (all charts) | Quarter ticks like 1,250 | 1/2/2.5/5 steps | Yes, P2 | Done |
| 8 | 8+ categorical colours | 5, repeating, two greys | 8 tokens per theme | Yes, P1 | Done |
| 9 | Saved report keeps its chart kind on the board (Recents thumbnails show the kind) | Dashboard cards always drew a line | Store `chart_type`, honour on cards | Yes, P0 | Done |
| 10 | Cards are live charts | Card trend had no axes or hover | Same chart component, compact | Yes, P1 | Done |
| 11 | Collapsible icon rail (sidebar footer) | Fixed 240px | Collapse, remember | Yes, P2 | Done |
| 12 | Fast ease-out popover motion (measured 120–200 ms) | Popovers appeared instantly | `animate-pop` 140 ms | Yes, P2 | Done |
| 13 | Drawer curve 240 ms | Drop-off drawer appeared instantly | Slide-in 240 ms | Yes, P2 | Done |
| 14 | Shared property picker with recents and details pane | Native selects / own filter search | Shared searchable picker | Later, P2 | Not done |
| 15 | Saved/unsaved state on reports | No in-place editing | Needs update-report API | P3 | Not done |
| 16 | Table row ↔ chart hover sync | None | Sync highlight | P3 | Not done |
| 17 | Report-type switcher keeping the query | None | Needs shared query model | P3 | Not done |
| 18 | Interval control beside chart type, hidden for Pie | Interval in query rail | Move next to chart menu | P3 | Not done |

Found while auditing Pulse (not from Mixpanel):
- Trend tooltip and legend printed the engine's `overall` key as if it were a segment. **Fixed** (segment shown only when a breakdown is set; table says "All users").
- On a 375px screen the date-preset bar pushed the page 24px wider than the viewport. **Fixed** (`PageHeader` actions can shrink).
- Dashboard trend cards labelled series by letter ("A"). **Fixed** (event name from the saved query).
