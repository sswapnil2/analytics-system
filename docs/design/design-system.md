# Pulse Studio design system

The rules the Studio (`web/`) is built with. Tokens live in
[`web/src/index.css`](../../web/src/index.css) and
[`web/tailwind.config.js`](../../web/tailwind.config.js); shared components in
[`web/src/components/ui.tsx`](../../web/src/components/ui.tsx). If a rule here and the code
disagree, fix whichever is wrong in the same change.

For the reasoning behind these rules see [ux-audit.md](ux-audit.md), section E.

## Rules of thumb

- Use a token class (`bg-panel`, `text-muted`, `border-line`). Never a Tailwind palette
  colour (`amber-300`, `zinc-800`, `white`) and never a hex value in a component.
- Use a named size from the type scale. Never `text-[13px]`.
- Reach for a component in `ui.tsx` before writing markup. Add to `ui.tsx` when the same
  thing appears in a second workspace.
- State that can be shared goes in the URL hash.
- Status is never colour alone: pair it with a word or an icon.

## Colour

Semantic tokens, defined for light and dark. Tailwind classes take the token name
(`bg-canvas`, `text-ink-2`, `border-line-strong`) and accept alpha (`bg-brand/15`).

| Token | Use |
|---|---|
| `canvas` | Page background |
| `panel` | Cards, panels, menus |
| `sunken` | Code blocks, tracks, chips, skeletons |
| `field` | Inputs and selects |
| `hover` | Hover fill on rows and ghost buttons |
| `line`, `line-strong` | 1px borders; `line-strong` for controls and floating surfaces |
| `ink`, `ink-2` | Primary and secondary text |
| `muted`, `faint` | Labels and captions; both pass AA on `canvas` and `sunken` |
| `brand`, `brand-hover`, `brand-soft`, `on-brand` | Primary action, selection, converted users |
| `danger`, `danger-soft` | Lost users and failures: failed runs, failed steps, failed checks. Nothing else is orange |
| `destructive`, `destructive-soft` | Delete and remove actions only |
| `warn`, `warn-soft` | Needs attention but not failed: timeouts, cancellations, loops, sampling |
| `ok`, `ok-soft` | Succeeded, passed |
| `info`, `info-soft` | Second series, neutral highlights |
| `--chart-1` … `--chart-8` | Categorical series in order. Never orange or red |

Dark is near-black `#09090B` with a lime accent and dark text on lime. Check every new
tinted surface in both themes.

"Lost" or "failed" as a quantity (drop-off bars, failed runs in a chart) is drawn with
the hatch classes `hatch-drop` and `hatch-drop-strong`, so it reads without colour.

## Type

Instrument Sans for everything; the system monospace for code, ids and payloads.
Numbers that can change use `tabular` (tables get it automatically).

| Class | Size | Use |
|---|---|---|
| `text-2xs` | 11px | Chips, axis labels, key hints |
| `text-xs` | 12px | Captions, table headers, meta |
| `text-compact` | 13px | Segmented controls, form labels |
| `text-sm` | 14px | Body, table cells, buttons. The default |
| `text-md` | 15px | Panel titles, inspector titles |
| `text-xl` | 20px | KPI values |
| `text-[1.5rem]` | 24px | Page title (`PageHeader` only) |

Weights: 400 body, 500 for emphasis and controls, 600 for titles and values. Uppercase
only on 11px chips, with `tracking-wide`.

## Space, shape, elevation

- Spacing is Tailwind's 4px scale. Panels pad 20px (`p-5`); stacked panels sit 20px apart
  (`space-y-5`); controls in a row sit 8px apart (`gap-2`).
- Rows are 36–40px (`py-2`/`py-2.5` at 14px). Controls are 36px tall; compact ones 24–28px.
- Radius: 4px (`rounded`) for badges, chips and bars; 8px (`rounded-lg`) for buttons,
  inputs and callouts; 12px (`rounded-xl`) for panels, menus and dialogs; `rounded-full`
  only for avatars and status dots. `rounded-md` is for controls nested inside an 8px one.
- Borders do the structural work. The one shadow, `shadow-pop`, is for things that float:
  menus, the command palette, drawers.
- Content column is `max-w-[1200px]`. Report pages use `ReportLayout`: results left, a
  20rem query rail right, collapsing to a summary row under 1280px.
- Breakpoints are Tailwind's: `sm` 640, `md` 768 (sidebar appears), `lg` 1024 (split
  views), `xl` 1280 (query rail).
- Layers: sticky table headers `z-10`, menus and popovers `z-40`, dialogs, drawers and the
  palette `z-50`.

## Motion

| Token | Value | Use |
|---|---|---|
| `animate-pop` | 140ms | Menus, palette |
| `animate-fade-in` | 200ms | Scrims |
| `animate-slide-in-right` | 240ms | Drawers |
| `ease-out` | `cubic-bezier(0.23, 1, 0.32, 1)` | Everything that appears or changes |
| `.chart-geom` | 320ms | Chart geometry easing to new values |

Animate things that appear, move or change value. Do not fade whole pages or tab panels:
content must be readable on the first frame. Prefix with `motion-safe:`; the global
reduced-motion rule is the backstop.

## Focus and keyboard

- `:focus-visible` draws a 2px brand outline globally. Do not remove it; inside a clipped
  container use `focus-visible:ring-2 ring-inset ring-brand` instead.
- Every icon-only button has an `aria-label` and a `title`.
- Shortcuts: `⌘K`/`Ctrl K` command palette everywhere. In an agent run: `J`/`K` or
  `↑`/`↓` step, `←`/`→` collapse and expand, `[` `]` previous and next run, `Esc` close.
  Single-letter shortcuts must ignore keystrokes in inputs and while a dialog is open
  (see `typing()` in the agent workspace).
- `Esc` always closes the innermost thing.

## Components

In `components/ui.tsx` unless noted.

| Component | Use |
|---|---|
| `PageHeader` | Page title, one-line description, actions on the right |
| `Panel` | A titled section. `meta` for a quiet note beside the title, `actions` on the right |
| `btn.primary` / `btn.secondary` / `btn.ghost` | One primary per view. Ghost for icon buttons and tertiary actions |
| `field`, `fieldLabel` | Inputs, selects, textareas and their labels |
| `Tabs` | Views of one page. Arrow keys move, one tab stop. `underline` for page level, `pill` inside a panel |
| `Segmented` | Two to six mutually exclusive choices, such as a time range |
| `Menu` (`Menu.tsx`) | Dropdown actions and single-choice menus |
| `CommandPalette` (`CommandPalette.tsx`) | The `⌘K` list. Add commands in `App.tsx` |
| `Kbd` | A key hint |
| `EmptyState` | Nothing here yet: say why and what to do next |
| `ErrorNotice` | A failed load, with retry |
| `Skeleton`, `ChartSkeleton`, `ListSkeleton`, `Spinner` | Loading. Skeleton when the shape is known, spinner otherwise |
| `SortHeader` + `useSort` | Sortable table columns |
| `ReportLayout` | Results plus query rail |
| `SamplingMeta`, `RanIn` | Sampling badge and run time on query panels |
| `RoleBadge` | Member role |
| `TrendChart`, `ChartKindPicker` (`TrendChart.tsx`) | Time-series charts. Only offers kinds the data supports |
| `RangePresets` (`dashboards/ReportCard.tsx`) | Date range with custom from/to |

Badges are `rounded px-1.5 py-0.5 text-xs font-medium` on a `-soft` fill with the
matching text colour.

Tables: 12px muted header, 14px cells, `divide-y divide-line`, numbers right-aligned,
`hover:bg-hover` on clickable rows, the first cell holds the real `<button>` or link so
the row is keyboard reachable. Wide tables scroll inside their panel, never the page.

## Charts

- Every chart has a value axis or printed values, a label per bucket where they fit, a
  hover readout and a keyboard path to the same readout.
- Gridlines are `line`; axis text is `text-2xs text-faint`.
- One series uses `brand`. More use `--chart-n` in order.
- The incomplete last bucket is dashed or marked "so far".
- Offer a chart kind only when the data supports it (`chartKindBlocker` in `lib/chart.ts`).

## Icons

`lucide-react`, 16px (`h-4 w-4`) in buttons and rows, 14px in dense rows, stroke as
shipped. Decorative icons are `aria-hidden`; an icon that carries meaning gets
`role="img"` and an `aria-label`.
