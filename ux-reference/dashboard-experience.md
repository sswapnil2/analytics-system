# Dashboard experience

## Observed
- **Board header:** breadcrumb `/ Untitled`, large board title, and Subscribe, Share (disabled for this account), copy-link and an overflow menu on the right.
- **Loading:** a spinner in the body. On this account the board never rendered its two reports in over 15 seconds, so card layout, card menus, resizing and rearranging **were not observed**.
- **Home "Your Recents":** cards with a small thumbnail of each report's chart, its name and type icon, plus a "See All" card. Skeleton tiles shown while they load.
- **Home "Your Product":** dashed "Add Report" placeholders, an empty state that is itself the call to action.

## Pulse today
Dashboards already have: half/full width cards, move up/down, remove, text notes, TV mode, share link, refresh, a date range for all cards, skeletons.

## For Pulse
- Cards should be as interactive as the report: **implemented** — trend cards now use the same chart component with hover tooltips, legend toggling, round y ticks and the saved chart kind.
- Cards drew a line regardless of how the report was saved: **fixed** — the chosen kind is stored in the saved query (`chart_type`, display only) and honoured on cards.
- Drag-to-rearrange and free resizing: not observed in Mixpanel here, and Pulse's menu-based Move up/down already works with a keyboard. **Not implemented.**
