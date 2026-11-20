# Design system observations

Observed in Mixpanel's dark theme; recorded for principles, not to copy.

- **Density:** 13–14px UI text, 32–36px control height, generous whitespace around the chart, tight in the query panel.
- **Hierarchy:** the result dominates; controls are low-contrast outlined buttons; the only filled button is Save (and Create New).
- **Colour:** one accent for primary actions and selection; a separate many-colour categorical palette for data. Positive change green, negative red, as small chips.
- **Type:** big bold numbers for single metrics (the "99 users" Metric view), small muted axis labels, tabular figures in tables.
- **Surfaces:** flat panels separated by 1px borders; popovers lifted with a soft shadow; no gradients in the work area.
- **Icons:** every menu item and sidebar entry has a line icon, same stroke weight.

## Pulse
Pulse already has a disciplined token system (stone/teal light, near-black/lime dark, semantic orange for drop-offs, red only for deletes). Changes made to fit it rather than Mixpanel:
- 8 categorical chart tokens per theme (`--chart-1…8`), avoiding orange and red.
- Elevation token `shadow-pop` replacing four copies of an arbitrary shadow.
- Motion tokens (`ease-out`, `ease-drawer`, `animate-pop`, `animate-fade-in`, `animate-slide-in-right`).
- Icons on menu items (chart kinds).
