# REST API

All routes are served by `pulse-node` on one port (default `8080`). Request and response bodies are
JSON unless noted. Errors return a non-2xx status with a message body.

Source of truth: `crates/pulse-ingest/src/http.rs` (ingest), `build_admin_control_router` in
`crates/pulse-control/src/api.rs` (admin/query), and `crates/pulse-control/src/api/auth.rs` (sign-in).

## Authentication

| Route group | Header | Accepted credentials |
|---|---|---|
| Ingest (`/v1/ingest`, `/v1/identify`, `/v1/engage`, `/v1/groups`, `/v1/replay/chunk`) | `x-pulse-key: pk_…` or `Authorization: Bearer pk_…` | Ingest key (`ingest_and_flags` role). The key selects the project. |
| Flag bundle (`/v1/flags/:project_api_key`) | key in the path | Ingest key |
| Admin, query, replay, MCP (everything else under `/v1`) | `Authorization: Bearer <token>` or `x-pulse-admin-key: <token>` | Root token (`sk_root_…` / `PULSE_ADMIN_TOKEN`), project admin key (`sk_live_…`), or Studio session (`ps_…`) |
| Sign-in (`/v1/auth/config`, `signup`, `login`, `exchange`, `google/*`) | none | |

Ingest keys are rejected on admin routes (`403`). A project admin key is bound to its project: asking
for another project returns `403`. Root tokens can address any project; a Studio session can address its organisation's projects, as its role allows (see below).

**Choosing the project.** Routes with `:id` in the path use that project. Routes without one
use, in order: the requested project (`?project_id=` on `/v1/status`, `GET /v1/sessions` and
`/v1/replay/:session_id`; `"project_id"` in the body of `/v1/query`; the `x-pulse-project` header on
`/v1/mcp`; `POST /v1/sessions` takes none); else the key's own project for project admin keys; else
the node's default project.

Browser calls to admin routes are limited to `PULSE_CORS_ORIGINS`; ingest and flag routes allow any origin.

### Users, organisations and roles

Every Studio account belongs to one organisation and has a role in it. Each role can do everything
the roles before it can:

| Role | Can |
|---|---|
| `viewer` | Read everything in the organisation's projects and run queries (`GET`, `query/*`, `/v1/query`, sessions, MCP) |
| `member` | Create, edit and delete reports, dashboards, cohorts, alerts, flags, experiments and lexicon entries |
| `admin` | Manage API keys, members, invites, BYOK, import/export, user-profile deletion and new projects |
| `owner` | Rename the organisation, and grant, change or remove the owner role |

The check runs once for every route, before the handler, and is deny-by-default. Writes a viewer
attempts get `403` with the role they would need. An admin can grant, change, remove or issue reset
links only for roles below their own; owners can do this for anyone. Nobody can change their own
role, and an organisation always keeps at least one owner.

**How the root token relates to accounts.** The root token (`PULSE_ADMIN_TOKEN`, or
`root_admin_token` in the data directory) is the node operator's key. It is not an account. It acts
as an owner of every organisation, and it has the only access to cluster settings (encryption keys,
audit log). Organisation-level routes (`/v1/org/*`) called with it act on the default
organisation. The first account on a node needs the root token as its setup code, and becomes the
owner of the default organisation and its existing data. After that the root token can stay in a
vault. A project admin key (`sk_live_…`) acts as an admin of its own project. Ingest keys (`pk_…`)
are unchanged.

Accounts that existed before roles became `admin`, except the earliest one in each organisation,
which became `owner`. Someone who signs up and creates a new organisation owns it. Someone who joins
the default organisation through an allowed email domain becomes a `member`.

**Email verification.** A password sign-up must prove its address before it gets a session. This
applies both to joining the default organisation by email domain and to creating a new
organisation. The domain case needs it because otherwise anyone could type `anything@yourcompany.com`
and read the company's data. New organisations need it too, so an unproven account never owns
anything. Until then the account sits in a placeholder organisation (`org_pending`) with no projects,
and sign-in answers `403`. The organisation is joined or created only when the link is used, after
the account row exists, so a sign-up that loses a race on its email never leaves an empty
organisation behind. Some accounts need no link because they've already proven themselves:
- the first account, which gives the setup code
- Google sign-ins (Google has verified the email)
- accounts created from an invite link

An unverified account can't block anyone. Signing up again, accepting an invite for that address, or
signing in with Google for it replaces the unverified account.

**Invites to existing accounts.** An account belongs to one organisation. Someone who already has an
account accepts an invite while signed in (`POST /v1/auth/join-org`), which moves the account and its
sessions to the inviting organisation with the invited role. The last owner of an organisation that
still has other members must make someone else an owner first.

Pulse has no mailer. Invite links are returned only to the admin who created them. Email-verification and
password-reset links are appended to the owner-only `outbox` file in the data directory (never the log).

## Health

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Returns `ok`. No auth. |

## Ingest (ingest key)

| Method | Path | Body |
|---|---|---|
| POST | `/v1/ingest` | One event, Pulse `{"events": [...]}`, Segment `{"batch": [...]}`, or a top-level array. Up to 64 MB. |
| POST | `/v1/identify` | `{"anon_id", "user_id", "user_properties"?, "operations"?}`: merges an anonymous id into a user |
| POST | `/v1/engage` | User-profile operations (`$set` and friends), one object, an array, or `{"profiles": [...]}` / `{"batch": [...]}` |
| POST | `/v1/groups` | Group-profile operations |
| POST | `/v1/replay/chunk` | Encoded wireframe replay chunk (sent by the SDKs) |
| GET | `/v1/flags/:project_api_key` | Feature-flag bundle for client-side evaluation; supports `If-None-Match` |

```bash
curl -X POST http://127.0.0.1:8080/v1/ingest -H "x-pulse-key: pk_live_…" -H "Content-Type: application/json" \
  -d '{"event": "KYC_PAN_Submitted", "userId": "usr_1042", "properties": {"city": "Bengaluru"}}'
```

### Ingest semantics

- **Partial success.** In a JSON batch, invalid events are skipped and the rest are stored. The response lists each refused one as `"rejected": [{"index": 1, "error": "missing event name: …"}]`. The request is a 400 only when no event in it is valid. `/import` behaves the same per NDJSON line or CSV row.
- **Segment calls.** `type: "track"` works as-is. `type: "page"` is stored as `$pageview` and `type: "screen"` as `$screen`; their `name` becomes the `name` property. In a `/v1/ingest` batch, `identify` with both `userId` and `anonymousId` links the two ids (as `/v1/identify` does) and sets `traits` on the user. `identify` with one id just sets its traits, and `alias` links `previousId` to `userId`. The response counts these in `profiles_updated`. `group` calls are rejected per item (Segment doesn't say the group type); send them to `/v1/groups`.
- **Limits.** Event and property names can be at most 255 characters; a longer one rejects that event. String property values are truncated to 8 KiB.
- **Retries and duplicates.** Each event is deduplicated on `event_uuid` / `messageId` / `insert_id`, or, if it has none, on a hash of its content including the timestamp. The dedup window is 15–30 minutes and is re-seeded from the WAL on restart. **Server-side callers must send an id or a `timestamp`.** An event with neither is stamped with the arrival time, so the server can't tell a retry from a second real event, and both are counted. The Web and mobile SDKs always send both. `/v1/projects/:id/import` also remembers every event id it has taken in for 30 days, across restarts, so running the same backfill twice adds nothing the second time (the repeats are counted in `deduped`).
- **Timestamps.** `/v1/ingest` corrects device clock skew using `sentAt` / `client_sent_ts_ms` when it differs from server time by more than 5 seconds. `/v1/projects/:id/import` never applies that correction, because a backfill's `sentAt` is in the past by design.
- **Sessions.** An event without `session_id` belongs to the session `sess_<distinct_id>`. So for server-side events, the `sessions` metric counts users, not visits. Send `session_id` to count real sessions; the SDKs do.

## Admin (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/status` | Row counts, segments and identity stats for a project (`?project_id=`) |
| POST | `/v1/admin/flush` | Flush in-memory rows and compact segments for all projects. Root token or Studio session only. |
| GET, POST | `/v1/projects` | List projects / create one (`{"name", "timezone"?}`; root or session only, id is generated) |
| PATCH | `/v1/projects/:id` | Rename a project and/or change its time zone: `{"name"?, "timezone"?}` (IANA name; `400` otherwise). Admin. Reports bucket days and hours on the new clock at once. |
| GET, POST | `/v1/projects/:id/api-keys` | List keys / create one: `{"label", "role": "ingest_and_flags" \| "admin"}`. The response's `plaintext_key` is shown once. |
| DELETE | `/v1/projects/:id/api-keys/:key_id` | Revoke a key. It stops working at once on this node and within seconds on other nodes. |
| POST | `/v1/auth/logout` | End a `ps_` session |
| GET | `/v1/system/encryption` | Cluster encryption status (root token only) |
| POST | `/v1/system/encryption/rotate` | Rotate the cluster encryption key (root token only) |
| GET | `/v1/audit-logs` | Paginated security and admin audit log (root token only) |
| GET, PUT | `/v1/projects/:id/byok` | Inspect or save project BYOK credentials (LLM, S3 warehouse, webhooks) |
| DELETE | `/v1/projects/:id/byok/:category` | Delete a stored BYOK credential category |
| POST | `/v1/projects/:id/byok-verify` | Test a stored BYOK configuration |
| POST | `/v1/projects/:id/export/warehouse` | Trigger an on-demand Parquet export to a configured BYOK S3 bucket |
| POST | `/v1/projects/:id/ai/copilot` | Run an AI Copilot analytics query using the project's BYOK LLM |

## Operations & health (unauthenticated unless noted)

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Process liveness probe (`200 OK`) |
| GET | `/readyz` | Readiness probe (checks drain state, metadata DB, and object store; returns `503` when not ready) |
| GET | `/metrics` | Prometheus text exposition; requires `Authorization: Bearer $PULSE_METRICS_TOKEN` when `PULSE_METRICS_TOKEN` is set |

## Queries (bearer token)

| Method | Path | Body |
|---|---|---|
| POST | `/v1/query` | `{"kind": "insights" \| "funnel" \| "retention" \| "flows", "project_id"?, "query": {...}}` |
| POST | `/v1/projects/:id/query/insights` | Insights query |
| POST | `/v1/projects/:id/query/funnel` | Funnel query |
| POST | `/v1/projects/:id/query/funnel/dropped?step=N` | Same body as a funnel query. Every user who reached step `N-1` but not step `N` (0-based index of the step not reached), as CSV (`distinct_id` per line) or `?format=json`. Exactly the users the funnel counts as dropped. Admin role. |
| POST | `/v1/projects/:id/query/retention` | Retention query |
| POST | `/v1/projects/:id/query/flows` | Flows (path) query |

Insights example (time range in epoch milliseconds):

```json
{
  "time_range": { "start_ts_ms": 1762300800000, "end_ts_ms": 1763510400000 },
  "interval": "day",
  "series": [{ "label": "A", "event_name": "App_Open", "metric": { "type": "uniques" } }],
  "breakdown_by": ["city"]
}
```

`metric` also accepts a plain string such as `"uniques"`.

Funnel queries require `time_range`, `steps` (`[{"event_name": "..."}]`) and `conversion_window_ms`
(`0` means 7 days); optional: `order_mode` (`loose_order` / `strict_order` / `any_order`),
`exclusions`, `hold_constant_props`, `breakdown_by` (one property name), `global_filter`. Retention
requires `time_range`, `start_event`, `return_event` and `num_periods`; `mode` (`n_day` /
`unbounded` / `frequency_stickiness`) and `unit` default to `n_day` and `day`.
The query structs live in `crates/pulse-exec/src/ast.rs`. For quick questions the [MCP tools](mcp.md)
take simpler arguments.

## Sessions and replay (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET, POST | `/v1/sessions` | Search sessions. GET params: `project_id`, `start_ts_ms`, `end_ts_ms`, `event` (or `event_name`), `rage_tap` (or `only_rage_tap`), `dead_tap`, `errors` (or `only_errors`), `limit`. POST takes a JSON search body. |
| GET, POST | `/v1/projects/:id/sessions` | Same, for a given project |
| GET | `/v1/replay/:session_id` | Replay stream for a session; supports `Range` |
| GET | `/v1/projects/:id/replay/:session_id` | Same, for a given project |

## Lexicon, cohorts, data management (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/projects/:id/lexicon` | Event and property definitions |
| PUT | `/v1/projects/:id/lexicon/events/:name` | `{"status": "active" \| "hidden" \| "dropped", "display_name"?, "description"?, "owner"?}` |
| PUT | `/v1/projects/:id/lexicon/properties/:name` | `{"pii_masked": bool, "status"?: "active" \| "dropped", "prop_scope"?, "inferred_types"?}` |
| GET, POST | `/v1/projects/:id/cohorts` | List / create behavioural cohorts |
| POST | `/v1/projects/:id/cohorts/preview` | Size a cohort definition without saving it |
| POST | `/v1/projects/:id/cohorts/from-funnel-dropped?step=N` | Save exactly the users the drop-off list shows for step `N` (body: `{name, description?, query}` with the funnel query) as a fixed cohort (`{"kind": "users", "canonical_uids": […]}`) |
| GET | `/v1/projects/:id/cohorts/:cohort_id/export` | Export cohort members (CSV, or `?format=json`) |
| GET | `/v1/projects/:id/users` | User profiles, newest first. `?search=` matches any part of the distinct id or of any property (case-insensitive, across all profiles); `?limit=` (default 100, max 500) and `?offset=` paginate |
| GET, DELETE | `/v1/projects/:id/users/:distinct_id` | Profile detail (properties merged across all of the user's ids; `?uid=N` looks the user up by canonical uid, for the masked ids that query results return) / erase a user (recorded in the deletion log). Erasure hides and, at the next compaction of each day's segments, deletes everything the user did up to the moment of erasure. If the same id sends events later (they signed up again), those new events are counted. |
| GET | `/v1/projects/:id/deletions` | Erasure audit log |
| POST | `/v1/projects/:id/import` | Batch import events: a JSON array or `{"events": […]}`, NDJSON (one event per line, as analytics tools usually export; detected from the body or `application/x-ndjson`), or CSV with a header row (`text/csv`). Timestamps are kept as given; see *Ingest semantics* |
| GET | `/v1/projects/:id/export` | Export events: `?format=ndjson` (default) `\|json\|csv`, `start_ts_ms` + `end_ts_ms`, `limit` (default 5000, max 50000) |
| GET | `/v1/projects/:id/revenue` | Revenue summary from user profiles' `total_revenue` (built from `$track_charge` profile operations), plus a 14-day daily series of `$revenue` events (sum of `amount`, unique payers). Pass `?event=Order_Paid` to chart your own purchase event instead |
| GET, POST | `/v1/projects/:id/alerts` | List / create metric alerts: `{"name", "event_name", "operator": "gt" \| "gte" \| "lt" \| "lte", "threshold", "webhook_url"?}`. The node checks every 5 minutes against the event's count over the last 24 hours, and fires at most once per 24 hours while the condition holds. It POSTs `pulse.alert.triggered` to `webhook_url` (or the project's webhook credential URL), signed with that credential's secret in `X-Pulse-Signature-256`. Delivery needs a webhook credential; without one the trigger is only recorded in `last_triggered_ms`. |
| DELETE | `/v1/projects/:id/alerts/:alert_id` | Delete an alert |
| GET | `/v1/projects/:id/schema-extensions` | Lookup tables, custom events, custom properties, and `user_properties` (profile keys, queryable as `user.<key>`) |
| GET, POST | `/v1/projects/:id/lookup-tables` | List / upload lookup tables |
| POST | `/v1/projects/:id/custom-events` | Define a custom (composite) event |
| POST | `/v1/projects/:id/custom-properties` | Define a computed property |

## Reports and dashboards (bearer token)

| Method | Path |
|---|---|
| GET, POST | `/v1/projects/:id/annotations`: list with `?start_ts_ms=&end_ts_ms=`, create with `{"ts_ms", "text"}` (1–280 characters). Shown as markers on Trends charts. |
| DELETE | `/v1/projects/:id/annotations/:ann_id` |
| GET, POST | `/v1/projects/:id/saved-reports` |
| GET, PUT, DELETE | `/v1/projects/:id/saved-reports/:rep_id` |
| GET, POST | `/v1/projects/:id/dashboards` |
| GET, PUT, DELETE | `/v1/projects/:id/dashboards/:dash_id` |

## Feature flags and experiments (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET, POST | `/v1/projects/:id/flags` | List / create or update a flag |
| PATCH | `/v1/projects/:id/flags/:flag_id` | Change a flag (enable, rollout, variants). `holdout_id` attaches a holdout (`""` detaches); `layer_id: ""` leaves the flag's layer (joining one goes through an experiment's `layer`). Turning on a flag whose layer slice another live flag now uses returns 409. While a running or paused experiment uses the flag, changes to anything but `description` return 409: pause the experiment to turn the flag off, or stop or ship it |
| GET, POST | `/v1/projects/:id/holdouts` | List / create holdouts: `{ name, holdout_pct }` (above 0, at most 50). Users in a holdout get no variant from any attached flag. The list includes each holdout's attached `flags` |
| DELETE | `/v1/projects/:id/holdouts/:holdout_id` | Delete a holdout no flag uses (409 otherwise) |
| POST | `/v1/projects/:id/holdouts/:holdout_id/results` | `{ metric, start_ts_ms?, end_ts_ms? }` (defaults: since the holdout was created, until now). Users who reached an attached flag (`exposed`) against held-back users (`holdout`, the control), from `$holdout_assigned`; same shape as experiment results, always sequential. The metric's event is saved as the holdout's `metric_event`, which the list returns |
| GET, POST | `/v1/projects/:id/layers` | List / create layers: `{ name }`. Experiments in one layer never share a user. The list includes each flag's slice (`start_bps`, `end_bps`) and `free_bps` |
| DELETE | `/v1/projects/:id/layers/:layer_id` | Delete a layer no flag uses (409 otherwise) |
| GET, POST | `/v1/projects/:id/experiments` | List / create experiments on a flag. `status` is `running` (the default; turns the flag on) or `draft` (saved, not started; holds no flag or layer). Names are unique per project, ignoring case (409). `secondary_metrics` (same shape as guardrails) are reported in the results' `secondary` and never decide or stop anything; guardrails and secondaries together are capped at 20. `created_by` records the creator's email. Optional `problem` (≤ 2,000 characters) and `owner` (≤ 200; empty means the creator) describe it, and `notify` (default `true`) controls whether its milestones go to the project webhook; all three can be changed later with PATCH. Optional `analysis: { sequential, alpha, planned_users_per_variant, cuped, cuped_pre_window_ms, target_lift_pct }` (defaults: fixed horizon, `0.05`, 10,000, CUPED off, 14 days). Each metric takes `metric_type` (`conversion`, the default; `count` of events per user; `sum` of `numeric_prop` per user; `ratio` of the metric over `denominator: { event_name, numeric_prop? }`; `step_through`, the share of users who did `denominator.event_name` and then `event_name`; `time_between`, the mean seconds from the first to the second among users who did both), optional `filters` on the metric event, an optional display `unit` (≤ 12 characters, such as `₹` or `items`), an optional `direction` (`decrease_is_bad`, the default, or `increase_is_bad`; on the primary metric it says which way is better), and for guardrails `auto_stop` (needs `analysis.sequential`). The legacy `continuous` type reads as `sum`. `kind: "bandit"` (default `"ab"`) with optional `bandit: { min_weight_bps, burn_in_users }` (defaults 100 and 100) makes a bandit: the flag must exist with at least two variants, and guardrails aren't allowed. `layer: { layer_id, traffic_pct }` gives the flag the first free slice of that share of the layer's users (409 when none is big enough; slices of turned-off flags are free). A flag backs one running or paused experiment at a time: creating a second (or resuming a stopped one) on the same flag returns 409 until the other is stopped |
| PATCH | `/v1/projects/:id/experiments/:exp_id` | Start, stop, pause or resume (`status`, `end_ts_ms`), or change `problem`, `owner` and `notify`. Pausing turns the flag off, so everyone gets the default and no one new is exposed; an optional `reason` (≤ 500 characters) is kept as `pause_reason`. Resuming turns the flag back on (users get the same variant, since bucketing is deterministic; 409 if its layer slice was taken meanwhile) and clears `stop_reason` and `pause_reason`. Moving a `draft` to `running` restarts its clock at now (same length, at least a day) and turns its flag on; a draft can't be paused. Concluding can carry `decision: { outcome: "ship" \| "keep_control" \| "inconclusive", variant, reason, flag_action: "rollout" \| "rollback" \| "keep", acknowledged }`: `rollout` gives everyone `variant` (100% rollout, out of its layer), `rollback` turns the flag off, `keep` leaves it. `acknowledged` lists the caveats the decider accepted (at most 10, 500 characters each), such as an unfinished sample or a guardrail with no data. The decision is stored on the experiment with `decided_by` and `decided_at_ms` |
| GET | `/v1/projects/:id/experiments/:exp_id/results` | Lift with absolute and relative intervals, p-values (Holm-Bonferroni corrected; always valid when `sequential`), Bayesian probability, SRM check, and `regressed_variants` per guardrail. Also `metric_type`, `cuped_applied`, `variance_reduction_pct`, `multi_variant_users` (left out of the results), and `timeseries` (cumulative lift by exposure day). `?segment=<prop>` adds `segments` (up to 50 values plus `$other`; exploratory, not corrected across segments). For bandits, `variants` and `guardrails` are empty and `bandit` has each arm's day-stratified mean and interval, `prob_best`, `expected_loss` and current `weight_bps`, plus `weight_history`, `best_variant`, `burn_in_complete` and `ready_to_ship` |

**Guardrail auto-stop.** Every 5 minutes the node checks running sequential experiments with an `auto_stop` guardrail. When a treatment makes one significantly worse, the flag's weights move to 10000 on control (the flag is turned off if it has no control variant), the experiment is concluded with a `stop_reason`, an `experiment.auto_stopped` row goes to the audit log, and `pulse.experiment.auto_stopped` is POSTed to the project's webhook credential URL, signed like alerts. SDKs pick the change up on their next flag refresh (`flagsTtlMs` / `flags_ttl_ms`, 5 minutes by default). Flag and experiment changes made through the API are audited too (`flag.saved`, `flag.updated`, `experiment.created`, `experiment.updated`).

**Experiment milestones.** On the same timer, each running A/B experiment announces these once, as an `experiment.milestone` audit row and a signed `pulse.experiment.milestone` webhook (`{event, project_id, experiment: {id, name, flag_key}, milestone, message, timestamp_ms}`):

| `milestone` | When |
|---|---|
| `planned_sample_reached` | Every variant has `analysis.planned_users_per_variant` users. |
| `significant:<variant>` | A treatment moved the primary metric significantly, with no traffic-split mismatch. |
| `guardrail_harmed:<name>` | A treatment made a guardrail significantly worse (auto-stop or not). |
| `guardrail_no_data:<name>` | 1,000+ users are in the test and a guardrail has no events from any of them. |

Milestones stay in the audit log when an experiment has `notify: false`; only the webhook is skipped. Significance and harm are only announced once results are safe to read: the experiment is sequential, or it has reached its planned sample. Paused experiments are checked too: users already in them can still produce guardrail events, so guardrails, auto-stop and milestones keep running. Only a bandit's daily traffic moves stop while it is paused.

**Bandits.** On the same timer, once a day, in the first hour after midnight in the project's timezone, each running bandit that is past burn-in gets new flag weights from Thompson sampling: every arm keeps `min_weight_bps` and the rest is shared by each arm's chance to be best. The change is recorded in the weight history and audited as `experiment.bandit_reweighted` (actor `system:bandit`). Arm estimates are stratified by exposure day, so traffic moving between arms doesn't bias them, and the SRM check compares users against the weights in effect on each day. `ready_to_ship` is true when the leader's expected loss is under 1% of its value.

The edge flag bundle (`GET /v1/flags/:api_key`) is reused for up to 30 seconds while no flag changes, so polling SDKs don't re-run targeting cohorts on every request.

## MCP (bearer token)

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/mcp` | MCP over JSON-RPC 2.0. See [MCP for agents](mcp.md). |

## Sign-in (no auth)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/auth/config` | Which sign-in methods are enabled |
| POST | `/v1/auth/signup`, `/v1/auth/login` | Email/password; returns a `ps_` session, or for a sign-up that must verify its email, `{verification_required: true}` |
| POST | `/v1/auth/exchange` | Exchange a sign-in code for a session |
| GET | `/v1/auth/google/start`, `/v1/auth/google/callback` | Google sign-in, when configured |
| POST | `/v1/auth/verify-email` | `{token}`: finishes a sign-up that answered `verification_required`; returns a session |
| GET | `/v1/auth/invites/:token` | Email, role and organisation of a pending invite, and `has_account` (then sign in and use `join-org`) |
| POST | `/v1/auth/invites/accept` | `{token, password, name?}`: creates the account with the invite's role and returns a session |
| POST | `/v1/auth/password-reset/request` | `{email}`: always `204`. If the account exists, the link goes to the `outbox` file; at most one request per email every 5 minutes |
| POST | `/v1/auth/password-reset` | `{token, new_password}`: one use; signs out every session of the account |

## Account and organisation (Studio session)

| Method | Path | Role | Notes |
|---|---|---|---|
| GET, PATCH | `/v1/auth/me` | any | Profile (`role` included); PATCH `{name}` |
| POST | `/v1/auth/password` | any | `{current_password, new_password}`: signs out your other sessions |
| GET | `/v1/auth/sessions` | any | Your live sessions (`id` is a hash prefix, `current` marks this one) |
| DELETE | `/v1/auth/sessions/:sid` | any | Sign out one session |
| POST | `/v1/auth/sessions/revoke-others` | any | Sign out every other session |
| POST | `/v1/auth/join-org` | any | `{token}`: accept an invite for your own email and move to that organisation |
| GET | `/v1/org` | viewer | Organisation name and your role |
| PATCH | `/v1/org` | owner | `{name}` |
| GET | `/v1/org/members` | viewer | Members and roles |
| PATCH, DELETE | `/v1/org/members/:user_id` | admin | `{role}`, or remove (deletes the account) |
| POST | `/v1/org/members/:user_id/password-reset` | admin | Returns a one-hour `reset_url` |
| GET, POST | `/v1/org/invites` | admin | `{email, role}`: returns `invite_url` once (7 days, one use) |
| DELETE | `/v1/org/invites/:invite_id` | admin | Revoke a pending invite |

These events are written to the audit log: sign-ins and failed sign-ins, password changes and resets,
role changes, member removals, invites created, accepted and revoked, organisation renames, and API
keys created and revoked.
