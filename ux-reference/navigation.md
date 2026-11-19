# Navigation

## 1. Flat sidebar, content listed under it
**Observed (Home, sidebar):** Top to bottom: project switcher ("All Project Data"), a primary "Create New" button, Search (⌘K), Home, Agent, Data (with a chevron to a submenu), Session Replay, Experiments, Feature Flags, then a collapsible "Your content" section listing the user's boards. A footer row holds apps, help, settings, notifications and a collapse control.
**Why it matters:** The analysis types are not in the sidebar at all; they live behind Create New. The sidebar is for places and saved things, the create menu for new work.
**Pulse today:** Grouped sidebar (Analyze / Investigate / Audience and shipping) listing each report type. Clear for a product of Pulse's size.
**For Pulse:** Keep the grouped list; Pulse has no "create" concept separate from opening a report page, so moving report types into a menu would add a click for no gain. **Not worth implementing.**

## 2. Collapse to an icon rail
**Observed (sidebar footer, `|←` icon):** One click shrinks the sidebar to a ~40px rail of icons; the report gains the width. The same icon (now `→|`) expands it. State held across navigation within the session.
**Why it matters:** Reports are wide. A 240px sidebar costs a fifth of a 1280px screen on every page.
**Pulse today:** Fixed 240px sidebar on desktop, no collapse.
**For Pulse:** Add a collapse control that keeps icons, hides labels, and remembers the choice. **Implemented.**

## 3. Create menu grouped by intent
**Observed (Create New):** A popover with sections "Report" (Insights, Funnels, Flows, Retention), "Analysis" (Board, Heatmap, Playlist), "Experimentation" (Experiment, Feature Flag), each item with a coloured icon.
**For Pulse:** The dashboard "Add" menu already does this for cards. **No change.**

## 4. Report context in a breadcrumb
**Observed (report header):** `/ Untitled / Bill Paid Event Uniques` breadcrumb (board, then report), "+ Add Description", and Close / Save as New / Save on the right. Save turns from a disabled "Saved ✓" to a filled "Save" the moment the query changes.
**Why it matters:** The user always knows whether what they see is saved.
**Pulse today:** Report pages are always "unsaved explorations"; Save to dashboard creates a copy.
**For Pulse:** A dirty/saved indicator needs an "edit saved report in place" API that Pulse doesn't have. **Deferred (see remaining-gaps).**
