# Remaining gaps

Fixed in the fourth pass: save in place with a saved/unsaved badge, chart export (PNG/SVG), picker and value suggestions in funnel step "where", the Experiments, Events and privacy and Security pass, dark `--chart-3`, dashboard cards using the saved interval, the lookup join key default, and cohort edit/delete (API merged from its own branch). Earlier passes are in the implementation log.

| Gap | Why not now | Size |
|---|---|---|
| Editing fixed-list cohorts (funnel drop-offs) or cohorts built from cohorts | The builder can't show them, so they have Delete but no Edit; renaming them would need a name-only form | S |
| Report-type switcher keeping the query | Each report has its own query shape. Only events and the global filter carry over cleanly; a half-kept query would surprise more than help | L |
| Property categories and a details pane in the picker | Needs per-property type/description data the API doesn't serve | M |
| Funnels and Retention charts on `TrendChart` | Bespoke visuals (drop-off hatch, heatmap); they share tokens only | M |
| Journeys diagram scrolls sideways inside its box on a phone | Sankey needs width; the step table has the same numbers | S |
| Row checkboxes in the segment table | Legend toggles cover visibility | S |
| Drag-to-rearrange / free resize of dashboard cards | Move up/down and half/full width work by keyboard | M |
| Horizontal bar kind | Column covers time series | S |
| Audit log | Needs `PULSE_LICENSE_KEY`; the page says so | — |
