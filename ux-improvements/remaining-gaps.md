# Remaining gaps

Fixed in the fourth pass: save in place with a saved/unsaved badge, chart export (PNG/SVG), picker and value suggestions in funnel step "where", the Experiments, Events and privacy and Security pass, dark `--chart-3`, dashboard cards using the saved interval, and the lookup join key default. Earlier passes are in the implementation log.

| Gap | Why not now | Size |
|---|---|---|
| Edit or delete a saved cohort | API built on branch `claude/cohort-update-delete-api-4c4cc9` (`e8c16ab`), not merged here. UI follows the merge | S (UI) |
| Report-type switcher keeping the query | Each report has its own query shape. Only events and the global filter carry over cleanly; a half-kept query would surprise more than help | L |
| Property categories and a details pane in the picker | Needs per-property type/description data the API doesn't serve | M |
| Funnels and Retention charts on `TrendChart` | Bespoke visuals (drop-off hatch, heatmap); they share tokens only | M |
| Journeys diagram scrolls sideways inside its box on a phone | Sankey needs width; the step table has the same numbers | S |
| Row checkboxes in the segment table | Legend toggles cover visibility | S |
| Drag-to-rearrange / free resize of dashboard cards | Move up/down and half/full width work by keyboard | M |
| Horizontal bar kind | Column covers time series | S |
| Audit log | Needs `PULSE_LICENSE_KEY`; the page says so | — |
