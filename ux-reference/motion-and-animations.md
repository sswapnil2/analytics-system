# Motion and animations

Measured by reading computed `transition-*` values from every element on the Insights page while the chart menu was open.

## Observed
| Element family | Properties | Duration | Easing |
|---|---|---|---|
| Menu/popover content | filter, opacity, transform | 120–200 ms | `cubic-bezier(0.23, 1, 0.32, 1)` (strong ease-out) |
| Panels, segmented controls | width, height, border-radius, gap | 240 ms | `cubic-bezier(0.32, 0.72, 0, 1)` (drawer curve) |
| Small toggles/checkboxes | width, height, background | 60–140 ms | ease |
| Hover backgrounds | background-color | 120–250 ms | ease |
| One springy element | opacity 150 ms, transform 350 ms | — | `cubic-bezier(0.34, 1.56, 0.64, 1)` (overshoot) |

Also observed: grey skeleton tiles on Home while reports load; a spinner on the board page; series dimming on hover was immediate-feeling (well under 200 ms); chart kind switches redrew without a visible tween.

Not observed: modal transitions, sidebar collapse timing (it looked instant, not measured).

## What it means
Fast (≤ 240 ms), decelerating curves, motion only on things that appear/disappear or change size. No decorative motion.

## For Pulse
Adopted the two curves as tokens (`ease-out`, `ease-drawer`) and three keyframes:
- `animate-pop` 140 ms: popovers and menus (date range, Save to dashboard, dashboard menus, chart menu).
- `animate-fade-in` 200 ms: chart body on kind change, number view, funnel drawer backdrop.
- `animate-slide-in-right` 240 ms: funnel drop-off drawer.
- `.chart-geom`: line, area and bar geometry eases to new values (320 ms) when the range or data changes; opacity 150 ms for focus dimming.
- Sidebar width 200 ms ease-out.

All behind `motion-safe:` or covered by the existing global `prefers-reduced-motion` rule.
