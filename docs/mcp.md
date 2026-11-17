# MCP for agents

Every Pulse node serves a [Model Context Protocol](https://modelcontextprotocol.io) server at
`POST /v1/mcp` on its normal port. Agents such as Claude Code, Claude Desktop and Cursor can use it to
read the event schema and run funnel, retention, insights and session queries in plain language.

## Transport and protocol

- **Transport:** Streamable HTTP, JSON responses only (no SSE stream; `GET /v1/mcp` returns `405`).
  Stateless: no session id is issued.
- **Protocol versions:** `2025-06-18`, `2025-03-26` and `2024-11-05`. The server echoes the client's
  requested version if it is one of these, otherwise answers with `2025-06-18`.
- **Methods:** `initialize`, `ping`, `tools/list`, `tools/call`. Notifications (such as
  `notifications/initialized`) are accepted with `202`.
- **Capabilities:** tools only (no resources, prompts or sampling).
- **Errors:** an unknown tool or method is a JSON-RPC error; a tool that fails (bad arguments, query
  error) returns a normal result with `isError: true` and the message as text.

## Auth and project scope

Send a bearer token on every request: `Authorization: Bearer <token>`. Missing or wrong tokens get `401`.

- **Recommended for agents:** a project admin key (`sk_live_…`), which can only see one project.
  Create one with `POST /v1/projects/<id>/api-keys` and `{"label": "agent", "role": "admin"}` (see the [quickstart](quickstart.md#4-get-an-ingest-key)).
- A root token (`sk_root_…` / `PULSE_ADMIN_TOKEN`) also works. It queries the node's default project
  unless you add an `x-pulse-project: <project_id>` header.
- `pk_` ingest keys are refused (`403`).

Every tool runs against that one project. A `project_id` argument that names a different project is rejected.

Tokens minted at startup change on every restart unless `PULSE_ADMIN_TOKEN` is set, so give agents a
`sk_live_` key or a fixed `PULSE_ADMIN_TOKEN`.

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `get_lexicon_schema` | none | Event and property definitions (names, status, descriptions, PII flags) |
| `query_insights` | `event_name` (required), `metric`: `uniques` (default) / `totals` / `sessions` / `dau` / `wau` / `mau`, `days` (default 14), `breakdown_by` (property name) | Daily series, optionally broken down |
| `query_funnel` | `steps` (required, 2+ event names in order), `order_mode`: `loose_order` (default) / `strict_order` / `any_order`, `days` (default 14), `breakdown_by` | Per-step `entered_users` and conversion, breakdowns, sample sessions of users who dropped. 7-day conversion window. |
| `query_retention` | `start_event`, `return_event` (both required), `mode`: `n_day` (default) / `unbounded` / `frequency_stickiness`, `num_periods` (default 7), `days` (default 14) | Daily cohort retention matrix |
| `inspect_frustrated_sessions` | `event_name`, `only_rage_tap` (default `true`), `only_dead_tap` (default `false`), `only_errors` (default `false`), `limit` (default 10) | Sessions with rage taps, dead taps or errors, and whether each has a replay |

All query tools also accept `start_ts_ms` and `end_ts_ms` (epoch ms) in place of `days` (1–365).
For full control, `query_insights` accepts a complete Insights query body (with `series`) and
`query_funnel` accepts step objects (`[{"event_name": ...}]` plus `time_range`, `conversion_window_ms`),
the same shapes as the [REST query routes](rest-api.md#queries-bearer-token).

Results come back both as pretty-printed JSON text and as `structuredContent`.

## Connect a client

Replace the URL and token with yours. Use your node's host and port; the examples use `http://127.0.0.1:8080`.

### Claude Code

```bash
claude mcp add --transport http pulse http://127.0.0.1:8080/v1/mcp \
  --header "Authorization: Bearer sk_live_…"
```

Add `--scope project` to write it to the repo's `.mcp.json`, or `--header "x-pulse-project: proj_…"`
to pick a project with a root token. Check with `claude mcp list` (it should say `✔ Connected`).

Or install the [Pulse plugin](../integrations/claude-code-plugin/README.md), which adds the server
plus funnel-analysis and SDK-instrumentation skills.

### `.mcp.json` (Claude Code project config)

Commit this and keep the token in each developer's environment:

```json
{
  "mcpServers": {
    "pulse": {
      "type": "http",
      "url": "${PULSE_URL:-http://127.0.0.1:8080}/v1/mcp",
      "headers": { "Authorization": "Bearer ${PULSE_TOKEN}" }
    }
  }
}
```

### Cursor

`~/.cursor/mcp.json` (or `.cursor/mcp.json` in a project):

```json
{
  "mcpServers": {
    "pulse": {
      "url": "http://127.0.0.1:8080/v1/mcp",
      "headers": { "Authorization": "Bearer sk_live_…" }
    }
  }
}
```

### Claude Desktop

Claude Desktop's config file starts local (stdio) servers, so bridge to the HTTP endpoint with
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote) (needs Node.js). In `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pulse": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://127.0.0.1:8080/v1/mcp", "--allow-http",
               "--header", "Authorization:${PULSE_AUTH}"],
      "env": { "PULSE_AUTH": "Bearer sk_live_…" }
    }
  }
}
```

`--allow-http` is only needed for a plain-HTTP node that isn't on localhost.

## Try it with curl

```bash
curl -s -X POST http://127.0.0.1:8080/v1/mcp \
  -H "Authorization: Bearer $PULSE_TOKEN" -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query_funnel","arguments":{"steps":["App_Open","KYC_PAN_Submitted","First_Disbursal_Success"],"days":14}}}'
```

## Troubleshooting

- **`401` / the client offers an OAuth sign-in:** the token is missing or wrong. Pulse has no OAuth;
  if the node restarted without `PULSE_ADMIN_TOKEN`, its root token changed.
- **`403`:** a `pk_` key was used, or a project admin key asked for another project via `x-pulse-project`.
- **Empty results:** check the project (`x-pulse-project`) and the date range. The `--seed` demo data
  lives in `proj_fintech_in`.
- **Browser-based clients:** admin routes, including `/v1/mcp`, only allow the origins in `PULSE_CORS_ORIGINS`.
