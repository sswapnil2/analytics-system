# Interaction patterns

1. **Every menu is the same menu.** Observed: Create New, chart type, interval and Compare all open the same style of popover (rounded panel, item icons, dividers between groups, selected item highlighted). For Pulse: the dashboard's accessible menu was moved to `components/Menu.tsx` and reused for the chart-type picker, gaining radio items, icons, groups and disabled hints. **Implemented.**
2. **Disabled, not hidden.** Observed: Funnel Steps / Retention Curve shown greyed in the Insights chart menu. For Pulse: same, plus a one-line reason under the item. **Implemented.**
3. **Hover means focus.** Observed: hovering a series dims the others and highlights its table row. **Implemented** for chart and legend.
4. **Changes are visible at once, saving is separate.** Observed: every query edit redraws immediately and flips the header button to "Save". Pulse already re-runs on change; a saved/unsaved indicator is deferred.
5. **Escape and outside click close popovers.** Observed in all Mixpanel popovers. Pulse's menus and popovers already did; kept.
6. **Recents first.** Observed in the property picker. Not implemented (see remaining gaps).
7. **Incomplete input is kept, not discarded.** Observed: an unfinished filter row stays. Pulse already does this.
8. **The table is part of the chart.** Observed: table rows have the series colours and checkboxes, and hover sync. Pulse's segment table is separate; sync is a remaining gap.
