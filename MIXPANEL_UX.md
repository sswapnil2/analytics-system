# Mixpanel UX walkthrough → what Pulse takes from it

I sent 2,229 synthetic events (about 600 `demo_user_NNNN` users, fintech onboarding) to the Mixpanel project. Then I built a funnel, a trend, a retention report, a flow and a board through their UI on 17 Nov 2026.

## What Mixpanel does

### Report pages (Insights, Funnels, Flows, Retention)
- **Layout.** The chart and its results table fill the centre. A query panel sits on the right, about 320px wide, with three tabs (Query, Chart, Annotations) and a row of icons for switching the report type.
- **Top bar above the chart.** Date presets (Custom, Today, Yesterday, 7D, 30D, 3M, 6M, 12M), Exclude and Compare. Granularity (Day) and chart type are at the right.
- **Results update on every change.** There's no Run button. Save stays disabled until something changes.
- **Titles name themselves**, for example "App Open → OTP Verified Conversion Rate (Funnel)", and you can rename them. The breadcrumb reads "Board / Report" when the report is opened from a board.
- **Query panel.**
  - Funnels: steps are joined with "then", with a window in days, an Advanced menu, and "% User · Conversion Rate · All Steps".
  - Insights: metrics are labelled A, B, …; "# User · Uniques"; Add Metric.
  - Every report has Filter (+) and Breakdown (+) sections.
- **Event picker.**
  - A search box with categories on the left (Events, Funnels, Retention, Formulas, Cohorts, Custom).
  - Recently used items first.
  - A preview pane showing the description, how many reports use the event, and recent replays.
  - Typing a name that doesn't exist and pressing Enter creates a placeholder event, which silently returns no data. Pulse should not copy this.
- **Funnel with a breakdown.** Bars are grouped by segment, and the share that dropped is ghosted above each bar. The table has per-segment rows, median time to convert per step, a significance column, Top N and search.
- **Insights.** Chart types: Line, Stacked Line, Column, Stacked Column, Bar, Stacked Bar, Pie, Table and Metric (one big number). The incomplete current period is drawn dotted. The table shows the average, then one column per period.
- **Retention.** The default view is a curve of the weighted average by day. Below it is a heatmap table whose average row opens into one row per start date.
- **Flows.** A Sankey with columns A, A+1, A+2 and + buttons to add steps before or after. Drop-offs are grey nodes, and each node shows its % and count. There are also Hide Events and row-count controls.

### Boards
- **Creating a board** is one click. It starts as "Untitled", saves itself, has an inline "+ Add description…", and offers templates when empty.
- **Board toolbar.** The same date presets plus **Default**, which keeps each card's own range. Also Exclude and a "+" to add filters to the whole board.
- **Adding content.** "+ Add content" can pick an existing report, create a new Insights, Funnels, Flows or Retention report inside the board (the editor opens with Close and Save), or add Text or Media.
- **Layout is row-based.** Each row has a drag handle and a "+" on each edge to drop a card into that row, and cards in a row share the width equally. Text cards have no border and are edited in place.
- **Card.** A type icon, the title (which links to the report), and a subtitle: "2-step Funnel · Last 30 days". Hovering shows a drag handle and a ⋮ menu with Delete, Rename, Duplicate, Chart type, Move to top or bottom, Copy URL, Move to another board, Linked copy, and Refresh ("Data from < 1 min ago").
- **Board header.**
  - Subscribe: email or Slack; daily, weekly or monthly; a time and time zone; Send test.
  - Owner avatar, comments, Share, Copy link and Favourite.
  - ⋯ menu: Rename, Duplicate, Move to, Pin, Export, **TV mode** and Refresh.
- **Share dialog.** People with roles, project-wide access (Editor or Viewer), a **Make Board Public** switch, and Copy URL.

## What Pulse takes, and what it skips

| Mixpanel pattern | Pulse decision |
|---|---|
| Query panel on the right, chart in the centre | **Adopt.** A sticky query panel on the right of report pages from 1024px up; on phones the query sits above the chart. |
| Date preset bar, results update on every change | **Adopt** (done): 7D, 14D, 30D, 90D. Today, Yesterday and Custom come later. |
| Self-naming report titles | **Adopt.** The page title describes the query; Save to dashboard pre-fills that name. |
| Retention curve above the table | **Adopt.** An average-retention curve above the start-date grid. |
| Metric (big number) chart type | **Adopt** for Trends. |
| Card subtitle and ⋮ menu, data freshness, refresh | **Adopt** on dashboard cards. |
| TV mode | **Adopt.** Full screen, no sidebar, refreshes every 5 minutes. |
| Create a report from inside a board | **Adopt, lightly.** "Add" links to the report page with this dashboard already chosen in Save to dashboard. |
| Board-wide filters, Subscribe (email or Slack), public links | **Later.** These need back-end work: filter injection, a mail and Slack sender, unauthenticated read tokens. |
| Drag-and-drop rows of up to 4 | **Skip for now.** Half or full width plus move up and down covers it. |
| Creating events from free text in the picker | **Skip.** Pulse only lists events it has seen, which avoids silent empty reports. |
| Upgrade nags, MCP promo panel | **Skip.** |

## Where Pulse already leads
- A **recording one click from every drop-off**. In Mixpanel, Recent Replay was empty in the event picker and there's no replay link on funnel steps.
- The **biggest drop is called out in words**, with "See who dropped and why". Mixpanel leaves the reading to the user.
- The **orange hatched drop segment** is easier to spot than Mixpanel's 20% ghost bar.
