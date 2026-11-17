---
name: verify
description: Verify a Pulse change before calling it done or opening a PR. Builds and tests what changed, runs a node, and checks the Studio against an empty project, a non-demo event schema and the seeded demo, in light and dark. Use after any change to crates/, web/, sdks/ or site/, and before merging or making the repo public.
---

# Verify a Pulse change

Prove the change works in the running product, not only that it compiles. Report what you checked, what you did not, and anything that failed, with the output.

## 1. Build and test what changed

Pick the rows that match the diff (`git diff --stat origin/main`).

| Changed | Run |
|---|---|
| `web/` | `npm --prefix web run build` (typecheck + Vite build) |
| `crates/<name>/` | `cargo test -p <name>`; `cargo check --workspace --tests` if a shared type (`pulse-exec/src/ast.rs`, `pulse-wire`) changed |
| `sdks/ts-web/` | `npm --prefix sdks/ts-web test` |
| HTTP API, ingest, query paths | `scripts/verify_e2e.sh` (boots a seeded release node if none is running) |

- `cargo build -p pulse-node --release` takes about 15 minutes. Run it in the background and do the web checks meanwhile.
- Known pre-existing failure: `cargo test -p pulse-node` gate4 expects at least 8000 users at Funnel Step 0 and the seeder now makes about 7183. Say so if it fails that way; any other failure is yours.

## 2. Run two nodes

Use the release binary from the tree you changed (`target/release/pulse-node`), with `PULSE_WEB_DIST_DIR` pointing at that tree's `web/dist`.

- **Fresh:** no `--seed`, empty `PULSE_DATA_DIR`, for example port 18091. This is what a new customer sees.
- **Demo:** `--seed --no-quic`, port 18090.

Each node prints a one-time root admin token and a public ingest key (`pk_live_…`) in its startup log. To sign the Studio in, set `localStorage.pulse_admin_bearer_token` to the root admin token and reload. Never paste these tokens into chat, commits or PRs.

## 3. Check the Studio with data that isn't the demo

The demo seed is a fintech app (`KYC_PAN_Submitted`, `First_Disbursal_Success`, `city`, …). Code that only works on those names is broken for every real customer.

1. On the fresh node, before sending anything, visit every page: Dashboards, Funnels, Trends, Journeys, Retention, Session replays, Cohorts, Experiments, Events and privacy, Enterprise and security. Each must explain what to do next. A blank panel or a report that silently shows zeros is a failure.
2. Send a different schema to `POST /v1/ingest` with the `x-pulse-key` header, for example a SaaS app: `Page_Viewed`, `Signed_Up`, `Project_Created`, `Invite_Sent`, `Plan_Upgraded`, with `plan` and `country`, spread over the last two weeks.
3. Revisit every page. Defaults and pickers must show that project's events and properties, and the numbers must match what you sent.
4. Grep the built app for demo names:
   `grep -rnE 'KYC|UPI|NSDL|Disbursal|App_Open|Quick_|fintech|payment_method' web/src crates/*/src --include='*.ts*' --include='*.rs' | grep -v seeder`
   Every hit needs a reason, such as a test fixture or the seeder.
5. Exercise the feature you changed end to end in the UI with real clicks: create it, see its result, open it again.

## 4. Check the seeded demo

On the demo node, open every page and confirm nothing regressed:
- Funnels: a drop-off drawer opens.
- Session replays: a recording plays.
- Experiments: results show users per variant.
- A saved report opens from a dashboard.

## 5. Look at it in both themes

Toggle the theme (or emulate `prefers-color-scheme: dark`) and screenshot the pages you touched. The dark palette matches the homepage: near-black `#09090B`, zinc text, lime accent `#C8F04A` with dark text on lime, indigo second series, amber drop-offs, red only for delete actions. Check that text on tinted cells (retention heatmap, badges) is readable.

## 6. Report

End with a short report:
- **Ran:** the commands and their result (pass, or the failing test and its error).
- **Checked in the Studio:** the pages and flows, the node (fresh or demo) and the theme.
- **Not checked:** anything skipped and why.

Don't call the change verified when a step was skipped. Name the step instead.
