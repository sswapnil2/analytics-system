# Test results

Branch `feat/product-improvements` (origin/main `66a0a07` + this work). macOS, Apple silicon, Rust toolchain from `rust-toolchain.toml`.

## Automated

| Command | Result |
|---|---|
| `cargo fmt --all --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass (0 warnings) |
| `cargo test --workspace --locked` | **pass**: all suites; 0 failures |
| `cargo test -p pulse-node --test correctness` | pass in about 3 s (release gate, see below) |
| `cargo test -p pulse-node --lib ha_02` | pass. **Fails** with the query-node reload removed (verified) |
| `cargo test -p pulse-node --lib gate4` | pass (seeded demo, full node; now asserts the 9 MCP tool names) |
| `cargo test -p pulse-exec --lib hour_buckets` | pass (IST and DST hour floors) |
| `cargo test -p pulse-ingest --lib compaction_drops` | pass. **Fails** with the compaction erasure filter removed (verified) |
| `cargo test -p pulse-storage --lib erased` | pass |
| `npm --prefix sdks/ts-web test` | 8/8 pass |
| `cargo build -p pulse-node --release --locked` | pass |
| `npm --prefix web run build` (tsc + Vite) | pass |

## Correctness gate: before and after

`crates/pulse-node/tests/correctness.rs` was run with soft asserts against the **original** `origin/main` code, then against this branch.

| Check | Review ID | origin/main | This branch |
|---|---|---|---|
| Invalid time zone refused | B-9 | 200 (accepted) | 400 |
| Batch with bad events: valid ones stored | I-8 | 400, all lost | 2 stored, `rejected: [1, 2, 4]` |
| Segment `page` → `$pageview` | I-12 | 0 | 1 |
| MCP typo is an error | B-8 | silent 0 | `isError`, "Did you mean 'Payment Completed'?" |
| Funnel 100/80/50/30/20, 14d, by plan | – | pass | pass |
| Dedup by id and content; anon merge; neq semantics | – | pass | pass |
| `amount eq "499.0"` | B-5 | 28 | 30 |
| Retention 100 / D1 40 / D7 20 | – | pass | pass |
| IST day bucket | – | pass | pass |
| IST hour bucket starts at local :00 | B-10 | 20:00 UTC | 19:30 UTC |
| `user.tier = gold` after restart (1,200 profiles) | B-1 | **0** | 100 |
| Breakdown by `user.tier` after restart | B-1 | `(none)` 201, basic 998 | basic 1,099, gold 100, platinum 1 |
| Anonymous `$set` shown after identify | B-3 | 0 | 1 |
| Later `$set` wins | B-3 | pass | pass |
| `search=p0005` | B-2 | `[]` | found |
| Import with past `sentAt` keeps its date | B-4 | 0 on Nov 2 | 1 on Nov 2 |
| Erased id that signs up again is counted; old events stay hidden | B-12 | 1 (re-signup hidden) | 2 |
| Same import re-run after a restart | I-18 | accepted again | `accepted: 0, deduped: 1` |
| Full drop-off list before step 3 | P1-3 | no endpoint | exactly 30 users |
| MCP `list_property_values`, `get_user`, `query_flows`, `list_cohorts` | P1-7 | not available | exact values |
| Annotations create / list by range / delete | P1-5 | not available | pass |

The rows below the B-4 line were added in the second pass; their "before" column is what the old code does, not a soft-assert run.

Every check is run twice: on the live node, and again after a restart (reading from disk segments).

## Manual: release node and Studio

Release `pulse-node` from this tree with a fresh data dir and no seed. A non-demo SaaS project (`America/New_York`) received 855 events (`Signed_Up`, `Page_Viewed`, `Project_Created`, `Invite_Sent`, `Plan_Upgraded`; `plan`, `country`) and 300 profiles (`company_size`: 60 × "50+", 240 × "1-49"; `role`: pm/eng alternating).

| Check | Result |
|---|---|
| Empty default project | First-run panel with SDK/HTTP snippets and "Generate ingest key"; no silent zeros |
| `schema-extensions.user_properties` | `company_size`, `role`, `total_revenue` |
| Trends, Page Viewed, 14D | 300 unique users (matches the data sent) |
| Trends, Group by = Week | Buckets labelled by Monday (2, 9, 16 Nov); table caption "last 3 weeks" |
| Split by `user.company_size` | 50+ = 60, 1-49 = 240 (exact) |
| Filter `user.role is pm` + that split | 50+ = 30, 1-49 = 120 (exact) |
| Light and dark theme | New controls readable in both |
| MCP `query_funnel` with `Plan_Upgrade` | `isError`: "Did you mean 'Plan_Upgraded'?" |
| MCP funnel split by `user.company_size` | Breakdowns per company size returned |

**Not checked manually:** the seeded demo node (its pages weren't touched by this diff, and the full-node `gate4` test, which runs on the seeded demo, passed in `cargo test`); split-role mode in a browser (covered by `ha_02`); the Postgres metadata backend (the SQL is portable: `LOWER`, `LIKE … ESCAPE`, `CASE`, but it wasn't run against Postgres); the web SDK in a real SPA (covered by the unit test only).
