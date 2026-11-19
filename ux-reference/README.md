# Mixpanel reference notes

Notes from using Mixpanel in a browser on 20 Nov 2026, signed in to a small project with real events (`Bill Paid`, `App Open`, `OTP Verified`, properties `city`, `platform`, `amount`). Everything here was clicked through, not read about. Report state was changed only in the editor; nothing was saved in Mixpanel.

**What was observed:** Home, the sidebar (expanded and collapsed), the Create New menu, one Insights report (chart type menu, Line, Stacked Column, Pie, Metric; hover tooltips; breakdown by `city`; the property picker; the filter value picker; table row checkboxes; legend click; the Chart tab), computed CSS transitions on the report page.

**What was not observed:** a populated board (the board page stayed on a spinner for over 15 seconds and never rendered cards), Funnels, Flows and Retention reports, modals, sharing, drag-to-rearrange. Nothing in these notes describes those.

Each file separates **Observed** (what Mixpanel did) from **For Pulse** (our recommendation). No Mixpanel assets, code, copy or branding were copied.

| File | Covers |
|---|---|
| [navigation.md](navigation.md) | Sidebar, collapse, create menu, context |
| [analytics-exploration.md](analytics-exploration.md) | Query panel, pickers, filters, breakdowns |
| [charts-and-visualizations.md](charts-and-visualizations.md) | Chart types, switching, tooltips, legends, table |
| [motion-and-animations.md](motion-and-animations.md) | Measured durations and easing |
| [dashboard-experience.md](dashboard-experience.md) | What could and couldn't be seen of boards |
| [interaction-patterns.md](interaction-patterns.md) | Cross-cutting behaviour |
| [design-system-observations.md](design-system-observations.md) | Type, colour, density |
| [comparison-with-pulse.md](comparison-with-pulse.md) | Finding by finding, with a verdict |
