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
for another project returns `403`. Root tokens and Studio sessions can address any project.

**Choosing the project.** Routes with `:id` in the path use that project. Routes without one
use, in order: the requested project (`?project_id=` on `/v1/status`, `GET /v1/sessions` and
`/v1/replay/:session_id`; `"project_id"` in the body of `/v1/query`; the `x-pulse-project` header on
`/v1/mcp`; `POST /v1/sessions` takes none); else the key's own project for project admin keys; else
the node's default project.

Browser calls to admin routes are limited to `PULSE_CORS_ORIGINS`; ingest and flag routes allow any origin.

## Health

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Returns `ok`. No auth. |

## Ingest (ingest key)

| Method | Path | Body |
|---|---|---|
| POST | `/v1/ingest` | One event, Pulse `{"events": [...]}`, Segment `{"batch": [...]}`, or a top-level array (Mixpanel style). Up to 64 MB. |
| POST | `/v1/identify` | `{"anon_id", "user_id", "user_properties"?, "operations"?}`: merges an anonymous id into a user |
| POST | `/v1/engage` | User-profile operations (`$set` and friends), one object, an array, or `{"profiles": [...]}` / `{"batch": [...]}` |
| POST | `/v1/groups` | Group-profile operations |
| POST | `/v1/replay/chunk` | Encoded wireframe replay chunk (sent by the SDKs) |
| GET | `/v1/flags/:project_api_key` | Feature-flag bundle for client-side evaluation; supports `If-None-Match` |

```bash
curl -X POST http://127.0.0.1:8080/v1/ingest -H "x-pulse-key: pk_live_…" -H "Content-Type: application/json" \
  -d '{"event": "KYC_PAN_Submitted", "userId": "usr_1042", "properties": {"city": "Bengaluru"}}'
```

## Admin (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/status` | Row counts, segments and identity stats for a project (`?project_id=`) |
| POST | `/v1/admin/flush` | Flush in-memory rows and compact segments for all projects. Root token or Studio session only. |
| GET, POST | `/v1/projects` | List projects / create one (`{"name", "timezone"?}`; root or session only, id is generated) |
| GET, POST | `/v1/projects/:id/api-keys` | List keys / create one: `{"label", "role": "ingest_and_flags" \| "admin"}`. The response's `plaintext_key` is shown once. |
| GET | `/v1/auth/me` | The signed-in user (Studio `ps_` sessions only; other tokens get `404`) |
| POST | `/v1/auth/logout` | End a `ps_` session |

## Queries (bearer token)

| Method | Path | Body |
|---|---|---|
| POST | `/v1/query` | `{"kind": "insights" \| "funnel" \| "retention" \| "flows", "project_id"?, "query": {...}}` |
| POST | `/v1/projects/:id/query/insights` | Insights query |
| POST | `/v1/projects/:id/query/funnel` | Funnel query |
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
| GET | `/v1/projects/:id/cohorts/:cohort_id/export` | Export cohort members (CSV, or `?format=json`) |
| GET | `/v1/projects/:id/users` | User profiles (`?search=`, `?limit=` default 100, max 500) |
| GET, DELETE | `/v1/projects/:id/users/:distinct_id` | Profile detail / erase a user (recorded in the deletion log) |
| GET | `/v1/projects/:id/deletions` | Erasure audit log |
| POST | `/v1/projects/:id/import` | Batch import events (JSON, NDJSON or CSV body) |
| GET | `/v1/projects/:id/export` | Export events: `?format=ndjson` (default) `\|json\|csv`, `start_ts_ms` + `end_ts_ms`, `limit` (default 5000, max 50000) |
| GET | `/v1/projects/:id/revenue` | Revenue summary from user profiles' `total_revenue` (built from `$track_charge` profile operations) |
| GET, POST | `/v1/projects/:id/alerts` | List / create metric alerts |
| DELETE | `/v1/projects/:id/alerts/:alert_id` | Delete an alert |
| GET | `/v1/projects/:id/schema-extensions` | Lookup tables, custom events and custom properties |
| GET, POST | `/v1/projects/:id/lookup-tables` | List / upload lookup tables |
| POST | `/v1/projects/:id/custom-events` | Define a custom (composite) event |
| POST | `/v1/projects/:id/custom-properties` | Define a computed property |

## Reports and dashboards (bearer token)

| Method | Path |
|---|---|
| GET, POST | `/v1/projects/:id/saved-reports` |
| GET, PUT, DELETE | `/v1/projects/:id/saved-reports/:rep_id` |
| GET, POST | `/v1/projects/:id/dashboards` |
| GET, PUT, DELETE | `/v1/projects/:id/dashboards/:dash_id` |

## Feature flags and experiments (bearer token)

| Method | Path | Notes |
|---|---|---|
| GET, POST | `/v1/projects/:id/flags` | List / create or update a flag |
| PATCH | `/v1/projects/:id/flags/:flag_id` | Change a flag (enable, rollout, variants) |
| GET, POST | `/v1/projects/:id/experiments` | List / create experiments on a flag |
| GET | `/v1/projects/:id/experiments/:exp_id/results` | Lift, p-values (Holm-Bonferroni corrected), Bayesian probability, SRM check |

## MCP (bearer token)

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/mcp` | MCP over JSON-RPC 2.0. See [MCP for agents](mcp.md). |

## Sign-in (no auth)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/auth/config` | Which sign-in methods are enabled |
| POST | `/v1/auth/signup`, `/v1/auth/login` | Email/password; returns a `ps_` session |
| POST | `/v1/auth/exchange` | Exchange a sign-in code for a session |
| GET | `/v1/auth/google/start`, `/v1/auth/google/callback` | Google sign-in, when configured |
