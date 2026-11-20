# Analytics exploration

## 1. Query on the right, result on the left
**Observed (Insights):** Right panel with tabs Query / Chart / Annotations and four report-type icons (Insights, Funnels, Flows, Retention) to switch the same query between report types. Below: Metrics (A, B…), Filter, Breakdown, each with a `+`.
**Pulse today:** Same layout (`ReportLayout`): results left, sticky query rail right, collapsing to a one-line summary below 1280px. Good.
**For Pulse:** Keep. Switching report type with the query kept is attractive but the four Pulse report pages have different query shapes. **Deferred.**

## 2. A metric reads as a sentence
**Observed:** Metric A shows the event ("Bill Paid"), an "Add Event" slot, then inline dropdowns `# User ▾ Uniques ▾`. Its auto-name ("Uniques of Bill Paid") sits on the header row.
**Pulse today:** Stacked labelled `<select>`s: Event, Measure, Property. Clear but tall.
**For Pulse:** A compact one-line metric row would save height. **Worth doing later (P2);** not done here to keep native, accessible selects.

## 3. One property picker everywhere
**Observed (Breakdown +, Filter +):** The same large popover: search field, category list on the left (All, Event, Computed, Cohorts, Experiments), results in the middle with "Recents" first, and a details pane on the right describing the hovered property (type, description, "Recently used by you", event count).
**Why it matters:** One learned control for every property choice; recents make repeat work fast.
**Pulse today:** Native `<select>` for breakdown; the filter panel has its own property search.
**For Pulse:** A shared searchable picker with recents would help schemas with many properties. **P2, not done** (larger change; native selects are accessible today).

## 4. Filter values from data
**Observed (Filter → city):** After picking the property, a second popover lists the actual values seen (Bengaluru, Delhi, Hyderabad…, plus "(empty string)") as checkboxes with "Select all defined values" and "Select all in list (7)", a search box, and an "Add" button. Operator defaults to "is". An incomplete filter stays in the panel as "city is Select value…" rather than vanishing.
**Pulse today:** `FilterPanel` already suggests values from data and supports cohorts.
**For Pulse:** Keep the draft row visible when the value is still empty. Pulse already does this. **No change.**

## 5. Date range and interval live on the chart
**Observed:** Above the chart: Custom / Today / Yesterday / 7D / 30D / 3M / 6M / 12M, then Exclude, Compare, interval (Day ▾) and chart type (Line ▾). The interval control only appears for time-series kinds; switching to Pie removed it.
**Pulse today:** Date presets in the page header; interval ("Group by") in the query rail.
**For Pulse:** Moving interval next to the chart type is a small win; **deferred** because the saved-report URL format keys off the current layout and the gain is small.

## 6. Progressive disclosure
**Observed:** Empty sections are just a heading and `+`. Options appear only after a choice (interval only for time charts; "Top 12" and "Dynamic Segments" only after a breakdown; the value list only after a property).
**For Pulse:** Same principle applied to the chart menu: kinds that can't show the current result are listed but disabled with the reason. **Implemented.**
