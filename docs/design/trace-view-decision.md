# Design decision: the agent run view

November 2026. Code: `TraceView`, `SpanRow`, `SpanInspector` in
[`AgentObservabilityWorkspace.tsx`](../../web/src/workspaces/AgentObservabilityWorkspace.tsx).
Both layouts were drawn first in Claude Design ("Pulse trace view exploration", a private
canvas in the author's account) using the seeded run `tr_demo_004`.

## The problem

A run opened as a panel above the run list, with its steps as an accordion.

- A failed run showed the error message, but the failing step's input and output were a
  click away, and you had to find the right row first.
- Only one step could be open, and opening it pushed the rest of the tree down, so
  comparing a tool call with the model call that used its result meant toggling.
- The waterfall had no axis: bars showed proportion but not time.
- Tokens and cost were only inside an opened step.
- Nothing could be done from the keyboard.
- A failed step differed from a passing one only by colour.

## What was built

A split view inside the same panel.

- **Left, the step tree.** One row per step: collapse control, type chip, name, a failed
  icon, tokens on model calls, a waterfall bar on a shared axis with quarter gridlines and
  tick labels, and the duration.
- **Right, the inspector** for the selected step: what kind of step it is, when it started
  and what share of the run it took, the error, the facts that apply (model, tokens in and
  out, cost, tool, retries), input and output with copy buttons, and the evaluations that
  scored that step.
- **On open, the deepest failed step is selected.** Parents of a failed step are marked
  failed too, so the deepest one is the cause. The run's error banner jumps back to it.
- **Keyboard.** `J`/`K` or arrows move between steps, left and right collapse and expand,
  `[` and `]` move to the previous and next run in the list, `Esc` closes.
- **The selected step is in the URL** (`&step=`), so a link opens on the same step.
- Run-level evaluations sit under the split; the write actions (run evaluators, add to
  dataset, review) moved below the evidence they act on.
- Under 1024px the inspector drops below the tree; under 768px the bar moves under the
  step name.

## Alternatives considered

**B. Side-peek drawer over the run list.** The pattern Langfuse, LangSmith and Linear use
for opening a row. It keeps the list visible, but a drawer of about 720px has to hold the
tree, the waterfall and the payloads in one column. Tool arguments and model messages are
the widest content on the page and they would get the least room. It also adds an overlay
with its own focus trap and scroll. Worth revisiting as a quick look from the Evaluations
tab, where the list matters more than the payload.

**C. A full page per run.** The most room, but it loses previous/next triage unless the
list state is carried across, and the back button becomes the only way out.

**D. Keep the accordion, add an axis and auto-open the failed step.** The smallest change,
but it keeps the one-open-at-a-time limit and the layout shift.

**E. Flame graph or node graph.** What Datadog and Langfuse offer for large distributed
traces and multi-agent graphs. Agent runs in Pulse today are tens of steps deep in one
process; a tree answers the question. Revisit when multi-agent runs are common.

## Why the split view

It answers "what went wrong" with no click, keeps the tree stable while the detail
changes, gives payloads a fixed readable column, and needs no overlay. It reuses the
existing panel, URL state and data loading, so nothing about the API changed.

## Accessibility

- The tree is `role="tree"` with `treeitem` rows carrying `aria-level`, `aria-selected`
  and `aria-expanded`; one tab stop, arrows move within it.
- Failure is an icon with a text alternative as well as colour. A step that passed but
  failed an evaluation gets its own icon.
- The inspector is an `aria-live="polite"` region, so moving through steps announces the
  new step; a failed step's error is `role="alert"`.
- Single-letter shortcuts are ignored while typing in a field or while a dialog is open.
- Failed text is `danger` on `danger-soft`, 4.7:1 in light.

## Implementation notes

- `visible` and `kids` are derived in one pass over the depth-ordered rows from
  `orderSpanTree`; collapsing is a set of span ids.
- The axis row and every step row share `STEP_GRID`, so ticks and bars line up.
- `Payload` renders chat-shaped values as a conversation with a raw toggle.

## Trade-offs and limits

- Opening a run still pushes the run list down the page; `[` and `]` make that cheaper,
  but the list is not visible beside the run.
- Every step is rendered. Collapsing makes a few hundred steps workable; thousands need a
  windowed list, which is the next step.
- The waterfall cannot be zoomed, so very short steps in a long run are 1% slivers.
- "Deepest failed step" is a heuristic. A run that fails at the top level with no failed
  child selects the top step.
