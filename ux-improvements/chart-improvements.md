# Chart improvements

## Kinds and when they're offered
| Kind | Offered when | Draws |
|---|---|---|
| Line | always | one line per series; last bucket dashed (partial) |
| Area | always | line + 12% fill |
| Stacked area | ≥ 2 series and the values add up | bands stacked from 0 |
| Column | always | grouped columns per bucket |
| Stacked column | ≥ 2 series and the values add up | stacked columns |
| Pie | ≥ 2 series and the values add up | donut of whole-period values, total in the centre |
| Number | always | big whole-period figure per series, change vs previous period |

"Values add up" = no formula shown, and either every metric is Total events / Sessions / Sum of property, or there's a single Unique users metric with a breakdown. Disabled kinds show "Needs a breakdown or a second metric" or "These values don't add up to a whole".

## Interaction
- Hover: vertical guide, the nearest line / stacked band / column is focused and the rest dim to 18%; ring on the focused point, dots on the others.
- Tooltip: full date (weekday for daily), "(so far)" on today's bucket, each visible series with value and change from the previous point (green/orange chip), focused series bold; at most 8 rows.
- Legend: each item is a button. Click hides/shows (strike-through when hidden); the last visible series can't be hidden. Hover/focus a legend item focuses that series. Pie legend shows value · share.
- Annotations still draw as dashed markers.
- Chart switching never re-queries.

## Presentation
- Width measured with `ResizeObserver`; SVG drawn in real pixels, so 11px labels at every size and plots that fill the card.
- Y ticks on 1/2/2.5/5 × 10ⁿ steps; x labels spaced by available width (~one per 90px).
- 8 categorical colours per theme (`--chart-1…8`), never orange or red.
- Breakdown legends name the segment ("Mumbai"), not "App Open (A) Mumbai", when there's one metric.

## Dashboards
Trend cards use the same component in compact mode, honour the saved kind, keep the previous-period dashed line (single series), and gained hover tooltips and (for multi-series) legend toggles.
