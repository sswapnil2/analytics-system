---
name: analyze-funnel
description: Find where users drop off in a conversion funnel in Pulse analytics, which segment drops most, and what the frustrated sessions at that step show. Use when asked why conversion fell, where users drop off, or to analyze onboarding, signup, checkout or KYC funnels in Pulse.
---

# Analyze a funnel drop-off with Pulse

Uses the `pulse` MCP server's tools. All of them are scoped to one project (the token's project, or
`PULSE_PROJECT`); never pass a different `project_id`.

1. **Learn the event names.** Call `get_lexicon_schema`. Pick the funnel steps from real event names;
   never guess names. If the user named steps loosely ("signup", "paid"), map them to lexicon events
   and say which you chose. Skip events whose status is `dropped`.
2. **Run the funnel.** Call `query_funnel` with `steps` (2 or more event names, in order) and `days`
   (default 14). Default `order_mode` is `loose_order`; use `strict_order` only if the user asks for
   no other events between steps. The conversion window is 7 days.
3. **Find the worst step.** In the result, compare `entered_users` across `overall_steps` and report
   step-to-step conversion as percentages. The biggest relative drop is the step to dig into.
4. **Segment it.** Re-run `query_funnel` with `breakdown_by` set to one property that exists in the
   lexicon (for example `platform`, `city` or `payment_method` in the demo data). Report the segments whose
   conversion at the worst step differs most from the overall rate, with user counts so small
   segments aren't over-read.
5. **Look at real sessions.** The funnel result's `dropped_sample_sessions` lists users who dropped
   and whether they have a replay. Also call `inspect_frustrated_sessions` with `event_name` set to
   the step before the drop, first with `only_rage_tap: true`, then with `only_errors: true`.
6. **Check it's not a one-off.** Optionally call `query_insights` (`metric: "uniques"`) on the drop
   step's event to see whether the drop is recent or steady.

Report: the funnel table, the worst step, the segment that explains it, and up to five session ids
(with rage-tap / error counts) worth watching in the Pulse Studio replay view. State the date range.
Say plainly when the data cannot support a conclusion (for example, fewer than ~50 users at a step).
