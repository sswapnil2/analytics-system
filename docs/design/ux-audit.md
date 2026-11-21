# Pulse Studio UX audit

Audited 22 November 2026 against the seeded demo project (`pulse-node --seed`), in the
built-in browser at 1440×900, 800×600 and 375×812, in light and dark. Every page in the
sidebar was opened. The AI agents section was exercised end to end (overview, run list,
filters, a failed run, step expansion, evaluations, datasets, experiments).

What was **not** exercised: creating dashboards, editing reports, playing a full session
recording, the sign-up and invite flows, and a project with no data. Findings about those
areas come from reading the code and are marked "(code)".

The design system that came out of this audit is in [design-system.md](design-system.md).
The trace view redesign is in [trace-view-decision.md](trace-view-decision.md).

## A. Executive summary

**Maturity.** Pulse is further along than most products at this stage. It already has
semantic colour tokens for light and dark, shared button, field, panel, empty, error and
skeleton primitives, a consistent report layout (results left, query on a sticky rail),
URL state for every report, a skip link, visible focus rings and reduced-motion handling.
The product analytics half (funnels, trends, journeys, retention, dashboards) is coherent
and reads as one product.

**Where it falls short.**

1. **Agent debugging is a list with an accordion.** Opening a run inserts a panel above
   the run list. Nothing is selected, so a failed run shows its error message but the
   failed step's input and output are one click away, and opening a second step closes the
   first. The waterfall has no time axis. There is no keyboard path through a run.
2. **No way to move without the mouse.** There is no command palette, no shortcuts and no
   search across pages. Tabs do not respond to arrow keys.
3. **The type scale drifted.** 18 distinct font sizes are in use, a third of them written
   as one-off values (`text-[0.9375rem]`, `text-[11px]`, `text-[0.6875rem]`).
4. **The same idea is drawn differently in different places.** Time range is a segmented
   control on four pages and a dropdown on AI agents. A failed agent run is amber (`warn`)
   while the token file reserves orange (`danger`) for failures. The sampling badge uses
   classes that do not exist in the theme.
5. **Charts on the agent overview are below the standard of the rest.** "Runs per day"
   has no value axis, labels only the first and last day, hides failures and cannot be
   reached by keyboard.

**Verdict.** A solid 7/10 product with a 5/10 agent debugger. The fastest route to an
8+ is not a redesign: it is finishing the system that already exists and rebuilding the
one screen engineers will live in.

## B. Scorecard

| Category | Score | Observations |
|---|---:|---|
| Visual design | 7 | Calm, restrained palette; one accent; good dark theme. Page titles at 30px are oversized for a dense tool. |
| Navigation | 6 | Clear sidebar with collapse. No command palette or shortcuts; browser tab always reads "Pulse"; five unlabelled icon buttons in the footer with Sign out beside Theme. |
| Information architecture | 6 | "Audience and shipping" holds Users, Cohorts, Experiments, Events and privacy, and Security: five unrelated things under one label. |
| Interaction design | 6 | Good URL state and drill-downs from KPI tiles. Tabs ignore arrow keys; run detail is an accordion; no keyboard movement in lists. |
| Data visualization | 7 | Trend, funnel, journey and retention charts are strong (kind switching, legend toggling, partial-day dashing). Agent overview bar chart is bare. |
| AI trace UX | 5 | Error message is surfaced, chat payloads render as a conversation. But no selection, no axis, no per-step cost at a glance, no keyboard, no previous/next run. |
| Dashboard UX | 7 | Cards carry their own menu, date range and TV mode. Not re-tested for editing in this audit. |
| Accessibility | 7 | Skip link, focus ring, `aria-sort`, `role="alert"`, reduced motion. Gaps: tabs keyboard model, charts not keyboard reachable, failure shown by colour alone in the step list. |
| Responsive design | 7 | Intentional: nav becomes a strip, the query rail collapses to a summary, tables scroll in a box. Run filters take four rows on a phone. |
| Design consistency | 6 | Tokens are respected almost everywhere (8 off-token classes in 17k lines), but the type scale and the time-range control are not. |
| Developer experience | 7 | Copyable quickstart per platform, deep links everywhere, MCP tool runner. No copy affordance on step payloads. |
| Overall product usability | 7 | Analytics is ready for daily use. Agent debugging works but is slow. |

## C. Findings

Severity: P0 critical, P1 major usability, P2 meaningful design, P3 polish.
Complexity: S under an hour, M a few hours, L a day or more.

| ID | Where | Sev | Observed | Why it matters | Fix | Cx | Status |
|---|---|---|---|---|---|---|---|
| T1 | AI agents › run | P1 | Opening a failed run selects nothing. The banner says `ToolError: lookup_account failed` but the failing step's input and output need a click on the right row. | "What went wrong" is the first question on every failed run. | Select the first failed step on open; make the banner jump to it. | M | Done |
| T2 | AI agents › run | P1 | Steps are an accordion: one open at a time, detail pushes later steps down. | Comparing a tool call with the LLM step that used it means opening and closing rows. | Split view: step tree on the left, inspector on the right. | M | Done |
| T3 | AI agents › run | P1 | Waterfall has no time axis or gridlines. | Bars cannot be read as offsets; "when did it start" is unanswerable. | Tick row with five labels and matching gridlines. | S | Done |
| T4 | AI agents › run | P1 | No keyboard movement between steps or runs. | Engineers triage dozens of runs in a sitting. | ↑/↓ and j/k between steps, ←/→ collapse and expand, `[` `]` previous and next run, Esc closes. | M | Done |
| T5 | AI agents › run | P2 | Failed steps differ from passing ones by colour only. | Fails WCAG 1.4.1. | Add an icon and "Failed" text alternative. | S | Done |
| T6 | AI agents › run | P2 | Tokens and cost are only in the expanded detail. | The expensive step should be visible while scanning. | Show tokens on LLM rows; show share of run time in the inspector. | S | Done |
| T7 | AI agents › run | P2 | Nested steps cannot be collapsed. | Multi-agent runs become a wall. | Collapse per parent, with a hidden-step count. | S | Done |
| T8 | AI agents › run | P2 | Evaluations are one flat list under the tree. | A step's verdict is far from the step. | Show a step's evaluations in its inspector; keep run-level ones below. | S | Done |
| T9 | AI agents › run | P2 | No copy button on payloads. | Reproducing a tool call means selecting text in a scrolling box. | Copy button per payload. | S | Done |
| T10 | AI agents | P2 | Failed runs, failed steps and failed checks are amber (`warn`); the token file says orange (`danger`) is for failures. | Two colours for one meaning, and amber also means "timed out" and "looped". | Failed uses `danger`; timed out, cancelled and looped stay `warn`. | S | Done |
| T11 | AI agents › run | P3 | Hundreds of steps render as hundreds of rows. | Slow and unreadable on very large runs. | Collapsing helps; windowing the list is the real fix. | L | Open |
| N1 | Shell | P1 | No command palette, no shortcuts. | Twelve destinations and several projects with no fast path. | ⌘K palette: pages, agent views, projects, theme, quickstart. | M | Done |
| N2 | Shell | P2 | `document.title` is always "Pulse". | Tabs and history entries are indistinguishable; screen readers get no page change. | "Trends · Pulse". | S | Done |
| N3 | Shell | P2 | "Audience and shipping" groups five unrelated pages. | First-time users cannot predict where Security or Events live. | Audience: Users, Cohorts, Experiments. Data: Events and privacy, Security and integrations. | S | Done |
| N4 | Shell | P2 | Changing page keeps the previous scroll position. | You land mid-page. | Scroll to top on page change. | S | Done |
| N5 | Shell | P3 | Footer has five icon-only buttons; Sign out sits between Theme and Collapse. | Mis-click risk. | Move account actions into a menu on the name. | M | Open |
| C1 | Agents overview | P1 | "Runs per day" has no value axis, two date labels, no failures, hover-only detail. | The page's main chart cannot answer "how many" or "how many failed". | Axis ticks and gridlines, a label per day, failed runs hatched on top, a legend, keyboard focus per day. Clicking a day to open its runs needs a date filter on the run list (see C3). | M | Done |
| C2 | Report panels | P1 | The sampling selector uses `bg-surface` and `border-accent`, which are not in the theme, and amber-300 text. In dark it renders as a light grey box; in light the "sample" badge is pale yellow on white. | Broken control on every report. | Use `field`, `warn` and `warn-soft` tokens. | S | Done |
| C3 | AI agents | P2 | Time range is a dropdown here and a segmented control on Trends, Funnels, Journeys, Retention. No custom range. | One concept, two controls. | Segmented control. Custom range needs an end bound on the trace API. | S | Done (presets) |
| C4 | Trends | P3 | No drag-to-zoom on the time axis. | Standard in Datadog and Grafana. | Brush that sets a custom range. | M | Open |
| D1 | Global | P2 | 18 font sizes, 8 as arbitrary values. | Hierarchy is decided per component. | Named scale: `2xs` 11, `xs` 12, `compact` 13, `sm` 14, `md` 15, then titles. | S | Done |
| D2 | Global | P2 | Page titles are 30px with a 15px description. | Costs 40px of height on every page of a dense tool. | 24px titles, 14px description. | S | Done |
| D3 | AI agents | P2 | Tabs are `role="tab"` buttons with no arrow-key model and no `tabpanel`. | Announced as tabs, behaves as buttons. | Shared `Tabs` with roving focus. | S | Done |
| D4 | Global | P3 | Radii: `rounded` 45, `rounded-md` 13, `rounded-lg` 67, `rounded-xl` 19, with no stated rule. | Drift will continue. | Rule documented: 4 badges, 8 controls, 12 panels. | S | Documented |
| D5 | Agents tables | P3 | The local `Table` has no sticky header or sort; `SortHeader` exists and is used elsewhere. | Two table idioms. | Adopt `SortHeader` in agent tables. | M | Open |
| R1 | AI agents (phone) | P3 | Run filters stack to four rows before the first run. | The list is below the fold. | Collapse filters behind a "Filters (n)" button under 640px. | S | Open |
| R2 | Shell (phone) | P3 | The nav strip does not scroll the current page into view. | On AI agents the strip shows Dashboards, Funnels, Trends. | `scrollIntoView` on the active item. | S | Done |

## D. Competitive analysis

Access: no accounts were created. Only Grafana's public playground was reachable as a
live product; everything else comes from each product's own documentation and changelog.
The table says which.

### What was read

| Product | Source | Patterns noted |
|---|---|---|
| Langfuse | Docs and changelog (the public demo now redirects to sign-in) | Trace has Tree, Timeline and Graph views. Row click opens a side peek; `J`/`K` move between rows, `Esc` closes. Facet sidebar with counts plus a typed query bar; saved views; ⌘K. Timeline fits ~600 spans on one screen by shrinking rows. |
| LangSmith | Docs | Row opens a side panel with Trajectory and Details tabs. Rendering varies by run type; repeated tool calls collapse into one grouped row. In-run find with next/previous. Highlight a value to filter on it. |
| Datadog | Docs | Flame graph, span list, waterfall and map behind tabs; selected span in a resizable bottom panel. Errors checkbox highlights failed spans. Very large traces open as a preview of entry, error and slow spans with numbered pills for the rest. Charts shade the incomplete last bucket; compare-time overlay; ⌘K quick nav. |
| Grafana | Live shell at play.grafana.org (panels did not paint) and docs | Collapsible grouped sidebar, breadcrumbs, ⌘K, `?` for shortcuts, time range in the URL, drag to zoom and double-click to reset, legend toggles series. Trace view has a minimap and "critical path only". |
| Mixpanel | Docs | Query builder beside the chart, sortable table under it, "Compare to past", annotations on dates, show/hide metric. Boards on a 12-column grid. |
| Linear | Docs | ⌘K, `/` search, `G` then a letter to go places, `?` shortcut list, `Space` to peek. Filters are editable tokens and live in the URL. |
| Stripe | Docs | Sidebar of primary objects plus pinned and recent. Global search with typed operators; `?` lists shortcuts. |
| Vercel | Docs and changelog | Moved from top tabs to a collapsible sidebar; ⌘K; drag a period on a chart then "Zoom in"; row click drills into a detail view. |
| Figma | Not researched | Its panel model is for a canvas editor; nothing here maps to it closely enough to borrow. |

Row heights, type scales and motion timings are not documented by any of these products, so
no claims are made about them.

### Conventions shared by three or more

1. ⌘K palette for navigation and actions (Langfuse, Datadog, Grafana, Linear, Vercel).
2. View state in the URL (Langfuse, Linear, Stripe, Grafana).
3. A row opens a peek or panel with the detail beside the list (Langfuse, LangSmith, Linear, Datadog).
4. `?` shows the shortcut list (Grafana, Linear, Stripe).
5. Drag on a chart to zoom, with an explicit reset (Grafana, Vercel, Langfuse, Datadog).
6. Large traces are handled by collapsing and grouping, not paging (Datadog, LangSmith, Langfuse).
7. Named saved views in the table header (Langfuse, LangSmith, Linear).
8. A typed `key:value` query bar beside click filters (Langfuse, Datadog, Stripe).

### Where Pulse stands

**Already level or ahead.** URL state on every report and on the agent run list. The
incomplete last bucket is drawn dashed, as Datadog does. Compare with the previous period,
annotations and legend toggling match Mixpanel. Chat payloads render as a conversation
rather than JSON. One click from an agent run to the user's screen recording is something
none of the trace tools above offer, because none of them hold the product analytics.

**Behind.** No ⌘K (convention 1). The run detail was an accordion rather than a selected
step beside the tree (3). No keyboard movement between runs or steps. No drag to zoom (5).
No saved views (7) or typed query bar (8). No alternative trace views such as a graph.

**Adopted in this pass.** ⌘K, selected-step inspector beside the tree, `J`/`K` and arrow
movement, `[` `]` between runs, `Esc` to close, collapsing subtrees, `?` listing shortcuts
inside the palette footer.

**Deliberately not copied.**
- Datadog's four trace visualisations and tabbed bottom panel. Pulse runs are agent
  runs with tens of steps, not distributed traces with thousands of spans across services;
  one tree with a waterfall answers the question.
- A facet sidebar on the run list. Five filters fit in one row; a sidebar would cost the
  width the table needs.
- Grafana-style free-form panel editing on dashboards. Pulse cards are saved reports; the
  report builder is where a chart is configured.
- A graph view of agent steps. Worth doing when multi-agent runs are common in real data,
  not before.

**Pulse's own ground.** The link between an agent run and what the user saw and did:
session replay, the user's profile, funnels that include agent outcomes. The design should
keep making that link one click, in both directions.


## E. Design direction

**Philosophy: quiet until it matters.** Pulse's screens are read far more than they are
operated. The interface stays neutral so the only saturated things on screen are the
user's data and the one thing that went wrong.

1. **One accent, three signals.** Teal (lime in dark) means "act here" or "converted".
   Orange means lost users and failures and nothing else. Red is for delete. Amber is for
   "look at this" (timeouts, sampling, loops). No new hues without removing one.
2. **Structure with lines, not boxes.** A panel is a 1px border on a white surface. No
   shadows except on things that float (menus, the palette, drawers). No gradients.
3. **Density by default, air on purpose.** 14px body, 36px rows, 20px panel padding.
   Whitespace separates groups; it does not pad single items.
4. **Numbers are typography.** Tabular figures everywhere a number can change. Units are
   quieter than values. Right-align anything that is compared down a column.
5. **Every view is a link.** Tab, filters, time range, open run and selected step live in
   the URL. If it cannot be pasted to a colleague it is not done.
6. **Keyboard is a first-class path.** ⌘K reaches every page; lists move with arrows;
   Esc always backs out one level.
7. **Motion explains, never decorates.** 140–240ms, decelerating, only on things that
   appear, move or change value. Everything honours reduced motion.
8. **Charts answer one question each.** An axis, a unit, a hover readout and a keyboard
   path are the minimum. A chart only offers kinds its data supports (already enforced by
   `chartKindBlocker`).
9. **Not colour alone.** Status always carries a word or an icon.

Pulse's own identity: the stone canvas with teal in light and near-black with lime in
dark, hatch for "lost", and Instrument Sans. Keep those; they are already distinctive and
none of the compared products look like it.

## Roadmap

| Order | Item | Why now |
|---|---|---|
| 1 | Window the step list for very large runs (T11) | The split view makes large runs usable up to a few hundred steps; beyond that it needs windowing. |
| 2 | Custom date range on AI agents (C3) | Needs `end_ts_ms` on trace and analytics queries, then the shared range control drops in. |
| 3 | Account menu in the sidebar footer (N5) | Removes the mis-click risk and frees a row. |
| 4 | Sortable, sticky headers in agent tables (D5) | Reuses `SortHeader`; replaces the "Sort" dropdown. |
| 5 | Compare two runs side by side | The step tree and inspector are now components; a diff view is two of them. |
| 6 | Drag to zoom on trend charts (C4) | Brings charts level with Grafana and Datadog. |
| 7 | Saved views for run filters | Filters are already in the URL; this is naming and storing them. |
| 8 | Filter sheet on phones (R1) | Small, finishes the phone layout for triage. |
