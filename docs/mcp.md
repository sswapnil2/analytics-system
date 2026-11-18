# MCP for agents

Every Pulse node serves a [Model Context Protocol](https://modelcontextprotocol.io) server at
`POST /v1/mcp` on its normal port. Any agent that speaks MCP over Streamable HTTP and can send an
`Authorization` header can use it to read the event schema and run funnel, retention, insights and
session queries in plain language: Claude Code, Claude Desktop, Cursor, OpenAI Codex, the OpenAI
Responses API, Gemini CLI and Google Antigravity are covered below.

## Transport and protocol

- **Transport:** Streamable HTTP, JSON responses only (no SSE stream; `GET /v1/mcp` returns `405`).
  Stateless: no session id is issued.
- **Protocol versions:** `2025-11-25`, `2025-06-18`, `2025-03-26` and `2024-11-05`. The server echoes
  the client's requested version if it is one of these, otherwise answers with `2025-11-25`.
- **Methods:** `initialize`, `ping`, `tools/list`, `tools/call`. Notifications (such as
  `notifications/initialized`) are accepted with `202`.
- **Node role:** served by nodes running with `--role all` (the default) or `--role query`.
- **Capabilities:** tools only (no resources, prompts or sampling).
- **Auth:** a static bearer token. Pulse has no OAuth, so clients that only accept OAuth-protected
  remote servers (for example ChatGPT connectors) cannot connect directly.
- **Errors:** an unknown tool or method is a JSON-RPC error; a tool that fails (bad arguments, query
  error) returns a normal result with `isError: true` and the message as text. An event name the
  project has never seen is such an error, with the closest real name ("unknown event 'Payment
  Complete'. Did you mean 'Payment Completed'?"), so an agent never reads a typo as "zero". The
  same goes for an unknown `metric`, `order_mode` or `mode`.

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

All query tools also accept `start_ts_ms` and `end_ts_ms` (epoch ms) in place of `days` (1–365); both
are advertised in each tool's `inputSchema`. `days` counts whole days ending today in the project's
time zone. `query_insights` also takes `compare_previous_period: true` (week-over-week and similar).
Results come back as compact JSON text plus the same object in `structuredContent`.
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

### OpenAI Codex (CLI and IDE extension)

Codex reads `~/.codex/config.toml`. Put the token in an environment variable rather than the file:

```toml
[mcp_servers.pulse]
url = "http://127.0.0.1:8080/v1/mcp"
bearer_token_env_var = "PULSE_TOKEN"
# with a root token, choose the project from another env var:
# env_http_headers = { "x-pulse-project" = "PULSE_PROJECT" }
```

Then `export PULSE_TOKEN=sk_live_…` before starting `codex`, and check with `/mcp` inside Codex.

### OpenAI Responses API / Agents SDK

The Responses API calls MCP servers from OpenAI's side, so the node must be reachable at a public
HTTPS URL (for example behind a reverse proxy). Pass the token as a header on each request:

```json
{
  "type": "mcp",
  "server_label": "pulse",
  "server_url": "https://pulse.example.com/v1/mcp",
  "headers": { "Authorization": "Bearer sk_live_…" },
  "require_approval": "never"
}
```

Use a project admin key here, and a dedicated one, since the token leaves your network with each request.

### Gemini CLI

```bash
gemini mcp add --transport http --header "Authorization: Bearer sk_live_…" pulse http://127.0.0.1:8080/v1/mcp
```

or in `~/.gemini/settings.json` (or `.gemini/settings.json` in a project). Use `httpUrl` (Streamable
HTTP), not `url` (which Gemini CLI treats as SSE):

```json
{
  "mcpServers": {
    "pulse": {
      "httpUrl": "http://127.0.0.1:8080/v1/mcp",
      "headers": { "Authorization": "Bearer sk_live_…" }
    }
  }
}
```

Check with `/mcp` inside Gemini CLI.

### Google Antigravity (`agy` CLI and IDE)

```bash
agy mcp add --header "Authorization: Bearer sk_live_…" pulse http://127.0.0.1:8080/v1/mcp
```

This writes a `serverUrl` entry to `~/.gemini/config/mcp_config.json`; the IDE reads the same file
(or `.agents/mcp_config.json` in a workspace). Write the token literally: Antigravity does not expand
`${VAR}` placeholders in this file. In headless mode (`agy -p`) MCP calls need an allow rule, for
example `{"permissions": {"allow": ["mcp(pulse/*)"]}}` in a file passed with `--settings`.

`agy plugin install <path>/integrations/claude-code-plugin` also installs the Pulse skills into
Antigravity, but its MCP entry relies on `${PULSE_URL}` / `${PULSE_TOKEN}`, which Antigravity leaves
unexpanded, so add the server with `agy mcp add` as above.

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
- **A cloud-hosted agent can't connect:** `127.0.0.1` is only reachable from your machine. Hosted
  agents (the Responses API, web-based assistants) need the node behind a public HTTPS URL.
- **Browser-based clients:** admin routes, including `/v1/mcp`, only allow the origins in `PULSE_CORS_ORIGINS`.
