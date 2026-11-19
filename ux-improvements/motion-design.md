# Motion design

Principle: motion only for things that appear, leave, or change value; fast and decelerating; nothing loops except loading indicators.

## Tokens (`tailwind.config.js`)
| Token | Value | Use |
|---|---|---|
| `ease-out` | `cubic-bezier(0.23, 1, 0.32, 1)` | anything entering, hover changes |
| `ease-drawer` | `cubic-bezier(0.32, 0.72, 0, 1)` | panels and drawers |
| `animate-pop` | 140 ms, opacity 0→1, translateY −4px, scale .98 | menus, popovers |
| `animate-fade-in` | 200 ms opacity | chart body on kind change, number view, drawer backdrop |
| `animate-slide-in-right` | 240 ms, translateX 24px + fade | funnel drop-off drawer |
| `.chart-geom` (CSS) | `d`, `y`, `height` 320 ms ease-out; opacity 150 ms | chart geometry tweening on new data; focus dimming |

## Where it's applied
- Chart-type menu and dashboard menus (shared `Menu`, transform origin on the aligned corner), date-range Custom popover, Save to dashboard, the dashboard's other popover: `motion-safe:animate-pop`.
- Funnel drop-off drawer and backdrop.
- Sidebar collapse: width 200 ms ease-out.
- Charts: kind change remounts the plot with a fade; new data tweens line/area paths and bar heights (Chromium animates `d`; other browsers jump, which is fine); legend and series focus fade in 150 ms; the results area dims to 60% while a refresh is in flight.

## Reduced motion
All keyframes are behind `motion-safe:`; the existing global `prefers-reduced-motion` rule shortens every transition and animation to 0.01 ms, which also covers `.chart-geom`.

## Not added
No spring/overshoot, no staggered list entrances, no page transitions: they would slow daily use.
