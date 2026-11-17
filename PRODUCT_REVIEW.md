# Pulse Analytics: End-to-End Product & Engineering Review

**Date:** 2026-11-17
**Build reviewed:** `a4c6384`, after the 8 remediation commits made against `REVIEW.md`.

**How the review was done**
1. Built `pulse-node` (release) and `web/`.
2. Ran `cargo test --workspace --release`.
3. Booted with `--seed --no-quic` (108,244 events).
4. Used all 8 workspaces in a browser at 800px and 1440px widths.
5. Sent one real event from ingest to query with the generated keys.

---

## Bottom line

The engine is genuinely impressive. The product isn't one yet.

Today Pulse is a strong engineering demo of a fast analytics engine, wrapped in a UI that reads like an architecture diagram. Its one standout feature is going from a funnel drop-off to that user's session replay, and that feature fails on the path a buyer would actually click.

---

## Product view

### What works

- **Session replay is the product.** The phone wireframe shows masked PAN fields, rage-tap markers and network breadcrumbs (`POST 504 /api/v2/kyc/nsdl…`). It tells a story a PM understands in five seconds: "users rage-tapped because the KYC gateway timed out."
- **The analytics basics are all there, and they're fast.**

  | Query | Warm time |
  |---|---|
  | 5-step funnel | 18 ms |
  | Retention triangle | 15 ms |
  | Sankey flows | 136 ms |

  Breakdowns, the experiment readout and the flag rollout all work.
- **The India fintech focus is a real wedge.** PAN, UPI Autopay, NSDL and Video KYC are concepts that mainstream product-analytics tools don't model.

### What loses a buyer in the first 10 minutes

| # | Issue | Evidence |
|---|---|---|
| PR-1 | **The headline feature breaks when clicked.** "Watch Replay" on a funnel drop-off fails silently. | `GET /v1/projects/proj_fintech_in/replay/sess_onboard_06153?offset_kb=0` → **404**, and the drawer stays blank. Only 32 of about 8,000 seeded sessions have a replay, yet every row offers the button and there's no "no recording" state. |
| PR-2 | **The UI talks to engineers, not PMs.** | Labels such as "Zero-Middleware Disaggregated S3 + Ephemeral NVMe Engine", "Retroactive Union-Find + Cross-Bucket Stitch", "64-bit SessionIndex Pruned 45,489 / 45,519", "Unique Users (RoaringBitmap)" and "65263 µs RoaringBitmap scan". |
| PR-3 | **The privacy story contradicts itself on screen.** | Raw phone numbers (`+919845106153`) appear as user IDs in the funnel drop-off list, the sessions list, and MCP output (which is what an LLM reads). For a fintech buyer this is disqualifying. |
| PR-4 | **The demo data is visibly synthetic.** | City counts are 1,333/1,334, perfectly round-robin. Conversions alternate between 0 and ~55 on alternating days, so the Insights chart is a sawtooth. The experiment splits 2,484 vs 2,484 with control at exactly 50.00%. Time-to-convert has p50 = p90 = p99 = 240.5 s. |
| PR-5 | **The experiment screen tells a contradictory story.** | The flag is at 100% rollout while the experiment is still "running" with a control group. It shows "P(beat control) 100.0%"; it should show ">99.9%". |
| PR-6 | **"Ask Pulse" doesn't ask anything.** | It's a raw JSON-RPC console with preset buttons. Either add natural language or rename it to what it is (the MCP endpoint for AI tools). |
| PR-7 | **There's no onboarding.** | There's no README. First boot shows a blank page for about 26 s while seeding. Login is a password box asking for an `sk_root_…` token that is only printed in the server logs. A failed login gives no feedback. Before login the app polls `/v1/status` → 401 repeatedly, and its first query goes to `proj_default` → 404 (an uncaught promise in the console). |
| PR-8 | **The ingest payload is unintuitive.** | A Segment-style `{"event","properties"}` payload is rejected with `data did not match any variant of untagged enum JsonBatchEnvelope`. |
| PR-9 | **Responsive gaps.** | The replay player disappears below about 1,000 px wide. |
| PR-10 | **System events pollute Flows.** | `$experiment_started` dominates step 2 of the Sankey diagram. |

### The positioning question to answer first

Which of these is Pulse?
1. "Self-hosted product analytics for Indian fintech."
2. "Session-replay-led debugging of KYC and onboarding drop-off."

The second is sharper and more defensible, and it's where the product is strongest today. Eight equally weighted workspaces suggest the choice hasn't been made yet.

---

## Engineering view

### Credit where due

The remediation was real and fast:
- Admin routes return **401** without a token, and a public `pk_` key gets **403**.
- Keys are hashed, randomly generated and shown once.
- It binds to localhost by default, and seeding is opt-in.
- Read-after-write works: a probe event was queryable immediately (`overall_value: 1.0`).

The architecture is coherent: a WAL feeding an L0 memory buffer, then columnar segments, an MVCC manifest, and identity stitching at query time. Stitching at query time instead of rewriting history is a genuinely good decision.

### Concerns

| # | Issue | Evidence | Ask |
|---|---|---|---|
| EN-1 | **The test suite is not green.** | `cargo test --workspace --release` fails `gate4_full_node_sdk_offline_wireframe_lexicon_ab_lift_and_mcp` at `crates/pulse-node/src/lib.rs:484` on the assertion `replay_text.contains("████")`. It fails deterministically. This is the only test that covers SDK → replay → masking, and it matches the replay 404 seen in the UI. The commit messages still claim the replay and E2E gates pass. | Treat "done" claims as unverified until this passes. Get the suite green before any new work. |
| EN-2 | **The commits are too big to review.** | The +12,309/−4,260 line remediation landed in 8 commits, each covering several findings (e.g. `fix(P2-01,P2-09a…P3-05)`). This is how a regression like EN-1 slips through. | One finding per commit from here on. |
| EN-3 | **Cold queries are much slower than warm ones.** | The first funnel took 1,735 ms; the warm run took 18 ms on 108k rows. The "4 ms on 1M events" headline is a warm-cache number. | Publish cold and warm numbers. Set a cold-path budget. |
| EN-4 | **The API exposes Rust and serde internals.** | `"interval":"Hour"` is rejected (it must be lowercase). `"metric":"total_events"` is rejected because metrics must be tagged objects like `{"type":"totals"}`. Error messages are raw serde output. | Accept the obvious input forms. Add a Segment-compatible ingest shape. Return human-readable errors. |
| EN-5 | **Too much surface area for the team.** | QUIC, UniFFI mobile bindings, MCP, DataFusion custom properties, lookup tables, Bayesian and frequentist statistics, and a custom columnar store. Each one is a maintenance and security liability. | Freeze features. Invest in a realistic dataset, a soak test under concurrent ingest with crash injection, and a cold-path performance budget. |

---

## Next two weeks, in priority order

1. Make Gate 4 pass. Make "Watch Replay" work, or show "no recording" honestly (PR-1, EN-1).
2. Mask user identifiers by default everywhere in the UI and MCP (PR-3).
3. Write a README and a "send your first event" path. Accept a Segment-style JSON payload (PR-7, PR-8, EN-4).
4. Replace engine jargon in the UI with outcome language. Move internals into a debug drawer (PR-2).
5. Make the demo data realistic: add noise, weekly seasonality and uneven splits (PR-4).
