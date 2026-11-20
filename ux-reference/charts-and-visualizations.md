# Charts and visualizations

## 1. One chart-type menu, grouped, with impossible kinds disabled
**Observed (Insights, top-right of chart):** A dropdown showing the current kind ("Metric ▾", "Line ▾"). Items in groups split by dividers: Line, Stacked Line, Column, Stacked Column | Bar, Stacked Bar | Pie, Table, Metric | Funnel Steps, Retention Curve. The last two were greyed out for an Insights query.
**Why it matters:** One place to change the picture; the user isn't offered something that can't work.
**Pulse today (before):** A three-way toggle Line / Bar / Number on Trends only; dashboards always drew a line.
**For Pulse:** A grouped menu offering Line, Area, Stacked area, Column, Stacked column, Pie, Number; disabled kinds show why. **Implemented.**

## 2. Switching reuses the data
**Observed:** Switching Line → Stacked Column → Pie redrew instantly; the table below changed shape (per-day columns → a single "Value" column for Pie).
**For Pulse:** Chart kind is display state only; no query re-runs. **Implemented and checked:** after clearing resource timings, switching kinds made zero `/insights` requests (only the periodic `/v1/status` poll).

## 3. Tooltip: one series, full date, change from previous point
**Observed (Line, hover):** Tooltip with a coloured bar, series name ("Uniques of Bill Paid"), segment ("Hyderabad") when broken down, the full date ("Sat Nov 14, 2026"), the value with its unit ("6 users"), and a coloured chip with change from the previous point ("−25% from previous day"). The hovered point becomes a hollow ring. A `+` appears under the x-axis at the hovered date to add an annotation.
**Pulse today (before):** Tooltip listed every series' value with no change; points used native `<title>` tooltips; no point emphasis.
**For Pulse:** Ring on the hovered point, change vs previous point per series, "so far" on the partial bucket. **Implemented.** Annotation `+` on the axis: Pulse already has an annotation form under the chart. **Not done.**

## 4. Hover focuses one series
**Observed (breakdown by city, hover a line):** The nearest line goes bold, every other line fades to a dim tint, the tooltip shows only that series, and its row in the table below highlights.
**Why it matters:** With 6+ series a line chart is spaghetti; focus makes it readable without hiding anything.
**For Pulse:** Focus the nearest line (or the stacked band / column under the pointer), dim the rest, bold that row in the tooltip. Hovering a legend item focuses too. **Implemented** (table row sync **not done**).

## 5. Series visibility from the table, not the legend
**Observed:** Clicking a legend swatch opened a colour-theme popover, not a toggle. Visibility was controlled by checkboxes on each table row; unchecking Mumbai removed it from the chart and the legend and switched the segment mode from "Dynamic Segments" to "Manual Segments". Pie's centre total then excluded Mumbai.
**For Pulse:** Pulse's table sits below the fold; toggling from the legend is more discoverable there. Legend items are buttons (`aria-pressed`), the last visible series can't be hidden, and pie shares and total recompute. **Implemented (legend toggle).**

## 6. Pie is a donut with the total in the middle
**Observed:** Donut, legend above, centre shows the total of visible slices.
**For Pulse:** Donut with total; hovering a slice shows its value and share in the centre; legend shows value · share %. Only offered when the series are parts of one whole. **Implemented.**

## 7. Partial periods and axes
**Observed:** The last point (today) is drawn dotted. Y ticks at round values (0, 20, 40, 60).
**Pulse today:** Already dotted the last segment. Y ticks were quarters of a rounded max (1,250 / 2,500 / 3,750).
**For Pulse:** Ticks on 1/2/2.5/5 × 10ⁿ steps. **Implemented** (`niceTicks`).

## 8. Colours
**Observed:** Eight-plus distinct categorical colours for breakdown segments.
**Pulse today (before):** Five colours, two of them greys, repeating after five series.
**For Pulse:** Eight categorical tokens per theme, never orange (Pulse uses it for drop-offs) or red (deletes). **Implemented.**

## 9. Not observed
Zoom/brush, chart export menu, Bar (horizontal) behaviour, Table kind in depth, the Chart tab contents (it rendered empty for Pie).
