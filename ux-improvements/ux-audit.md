# UX audit of the Pulse Studio

Audited on the seeded demo node (consumer-lending schema) and a fresh node fed a SaaS schema (`Page_Viewed`, `Signed_Up`, `Project_Created`, `Invite_Sent`, with `plan`, `country`), at 1440×900 and 375×812, dark and light.

## Architecture (as found)
- React 18 + TypeScript + Vite + Tailwind 3; no router (hash routes in `App.tsx`), no state library, no chart library. Charts are hand-written SVG per workspace.
- Design tokens: RGB CSS variables in `index.css`, mapped in `tailwind.config.js`; light (stone/teal) and dark (near-black/lime). Shared primitives in `components/ui.tsx` (`Panel`, `PageHeader`, `ReportLayout`, `btn`, `field`, skeletons, `useSort`).
- Data: `api/client.ts` fetch wrappers; each workspace owns its query state, aborts stale requests and keeps the newest result. Report state round-trips through the URL (`useSavedReport`).

## What's already good
- Query-left/results-right layout with a sticky rail that collapses to a summary on narrow screens.
- Honest empty and error states with next steps; skeletons while loading; stale-request handling.
- Strong semantic colour rules (orange = lost users, red = delete only).
- Keyboard-usable dashboard menus; reduced-motion respected globally.

## Problems found (by severity)
**P0**
- Saved trend reports lost their chart type: a report saved as Bar or Number always drew as a line on dashboards (the type wasn't stored).

**P1**
- Trends offered only Line/Bar/Number. Breakdowns (the most common way to split a metric) had no stacked or share view.
- Only five series colours, two of them greys; a 6-way breakdown repeated colours.
- Tooltip listed values only; no change from the previous point; no point emphasis; data dots used native `title` tooltips.
- With many series the line chart was unreadable: no focus or way to hide a series.
- Tooltip and legend printed the engine's internal `overall` key as if it were a segment name.
- Dashboard trend cards: no axes, no hover, series labelled "A".
- The chart rendered into a fixed 900-wide viewBox, so on narrow cards labels scaled down to ~4px.

**P2**
- Y-axis ticks at quarters of a rounded max (1,250 / 2,500 / 3,750).
- Popovers, menus and the funnel drop-off drawer appeared with no transition.
- Sidebar fixed at 240px on desktop.
- On phones the page header's date presets made the page 24px wider than the screen.
- Four copies of the same arbitrary popover shadow.

**P3** (second pass fixed: picker, hover sync, interval placement, Funnels/Retention tokens; third pass: Journeys hover and step table, Cohorts/filter pickers, Users profile focus; still open: saved/unsaved state, bespoke Journeys chart)
- No shared searchable property picker; no recents.
- No saved/unsaved state for reports; Save to dashboard always makes a copy.
- Segment table and chart don't share hover.
- Interval control lives in the query rail rather than next to the chart type.
- Retention, Funnels and Journeys charts are bespoke and were not moved onto the new chart component.
