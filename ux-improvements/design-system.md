# Design system changes

Pulse's existing token system (`index.css` + `tailwind.config.js`) was kept; additions only.

## Colour
`--chart-1` … `--chart-8`, defined for light and for dark (both the `data-theme="dark"` block and the `prefers-color-scheme` block).

| # | Light (stone/teal) | Dark (zinc/lime) |
|---|---|---|
| 1 | teal 11 110 95 (= brand) | lime 200 240 74 (= brand) |
| 2 | indigo 69 83 201 | indigo 110 123 255 |
| 3 | amber 176 122 10 | amber 230 178 80 |
| 4 | magenta 176 58 140 | pink 236 112 182 |
| 5 | sky 22 128 178 | sky 86 182 230 |
| 6 | olive 104 128 24 | teal 64 200 170 |
| 7 | violet 124 82 204 | violet 168 140 255 |
| 8 | slate 94 104 98 | zinc 161 161 170 |

Rule kept: no orange (drop-offs) or red (deletes) in series colours.

## Elevation
`shadow-pop` = `0 12px 32px rgb(0 0 0 / 0.16)` for popovers and menus (replaced four arbitrary copies).

## Motion
See [motion-design.md](motion-design.md).

## Components
- `components/Menu.tsx`: one menu for the app. Items: `label`, `onSelect | href`, `danger`, `disabled`, `checked` (renders `menuitemradio`), `icon`, `hint`, `group`. Arrow keys, Home/End, Escape (refocus trigger), Tab closes; outside click closes.
- `components/TrendChart.tsx`: `TrendChart` and `ChartKindPicker`.
- `lib/chart.ts`: pure chart rules and geometry.
