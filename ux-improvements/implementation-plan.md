# Implementation plan

Ordered by value; everything marked ✅ shipped in this branch.

## P0
- ✅ Persist the trend chart kind in the saved query (`chart_type`, display only, like `display_currency`) and honour it on dashboard cards. Old reports saved with `"bar"` map to Column.

## P1
- ✅ One reusable `TrendChart` (Line, Area, Stacked area, Column, Stacked column, Pie) used by Trends and dashboard cards.
- ✅ `ChartKindPicker`: grouped menu with icons; kinds that can't show the result are disabled with the reason. Validity rules in `lib/chart.ts` (`isAdditive`, `chartKindBlocker`), unit-tested.
- ✅ No refetch on kind switch (kind is display state).
- ✅ Hover focus (nearest line, stacked band or column), dim others; ring on the hovered point; change vs previous point; "so far" on the partial bucket.
- ✅ Legend items toggle series (buttons, `aria-pressed`), hover focuses; at least one series stays visible; pie recomputes total and shares.
- ✅ 8 categorical colour tokens per theme.
- ✅ Fix `overall` showing as a segment.
- ✅ Measure the chart's real width so labels stay 11px and plots fill cards.

## P2
- ✅ Round y ticks (`niceTicks`).
- ✅ Motion tokens and keyframes; popover/menu pop, chart fade, chart geometry tween, drawer slide-in, sidebar width.
- ✅ Collapsible icon-rail sidebar, remembered in `localStorage`.
- ✅ Shared `Menu` component (moved from the dashboard) with radio items, icons, groups and hints.
- ✅ `shadow-pop` elevation token.
- ✅ Phone overflow from `PageHeader` actions.

## P3 (next)
- Shared property picker with search, recents and a details pane.
- Saved/unsaved indicator once reports can be edited in place.
- Hover sync between chart and segment table.
- Move Funnels/Retention mini-charts onto shared primitives where it fits.
