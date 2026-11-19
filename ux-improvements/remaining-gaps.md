# Remaining gaps

Done in the second pass (see implementation-log): shared searchable property picker with recents (Trends breakdown, Funnels and Retention split), chart ↔ segment-table hover sync with swatches, interval beside the chart type, Funnels/Retention tooltips and series colours on tokens, fresh-node empty-state pass.

| Gap | Why not now | Size |
|---|---|---|
| Saved/unsaved indicator and "Save" in place | Needs an update-saved-report API; Pulse saves copies | M (backend + UI) |
| Report-type switcher keeping the query | Query shapes differ per report; needs a shared query model | L |
| Chart export (PNG/SVG) | Out of scope for frontend-only pass as specified; CSV export exists | S |
| Property picker in filters (FilterPanel) and step `where` clauses; categories and a details pane | Picker only lists names; types/descriptions would need Lexicon data per property | M |
| Funnels and Retention charts on `TrendChart` itself | Bespoke visuals with their own semantics (hatch drop-offs, heatmap); only tokens were shared | M |
| Journeys, Cohorts, Users, Experiments: no UX pass | Not in this task | M |
| Dark `--chart-3` (amber 230 178 80) is close to dark drop-off orange (244 162 97) | Palette change touches every chart; Funnels already used this amber before | S |
| Dashboard cards pick interval from the range (day/week), not from the saved report | Existing card behaviour; changing it alters every dashboard | S |
| Row checkboxes in the segment table | Legend toggling covers visibility | S |
| Drag-to-rearrange / free resize of dashboard cards | Move up/down + half/full width already work by keyboard | M |
| Horizontal bar kind | Column covers time series | S |
| Live Mixpanel board observation | The board never finished loading on the reference account | — |
