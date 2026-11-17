# Pulse documentation

Pulse is a self-hosted product analytics system: one Rust binary (`pulse-node`) serves event ingest,
the query engine, session replay, feature flags, experiments, the Studio web UI and an MCP endpoint
for AI agents.

| Guide | What it covers |
|---|---|
| [Quickstart](quickstart.md) | Build and run `pulse-node`, get the admin token and an ingest key, send a first event |
| [Web SDK](web-sdk.md) | `sdks/ts-web`: tracking, identify, user properties, flags, replay |
| [Mobile core (beta)](mobile-core.md) | `crates/pulse-sdk-core`: the Rust core with UniFFI Swift/Kotlin bindings |
| [REST API](rest-api.md) | Every `/v1/...` route, grouped by auth type |
| [MCP for agents](mcp.md) | `POST /v1/mcp`: tools, protocol support, Claude Code / Claude Desktop / Cursor config |
| [Claude Code plugin](../integrations/claude-code-plugin/README.md) | Installable plugin that wires up the MCP server and adds analysis skills |

## Credentials at a glance

| Credential | Looks like | Where it comes from | Use it for |
|---|---|---|---|
| Root admin token | `sk_root_…` (or whatever you set in `PULSE_ADMIN_TOKEN`) | Printed in the startup log, or set by you | Everything: all projects, creating projects and keys, `/v1/mcp` |
| Project admin key | `sk_live_…` | `POST /v1/projects/:id/api-keys` with `"role": "admin"` | Admin, query and MCP routes for **one** project |
| Ingest key | `pk_live_…` | Startup log on first boot, or `POST /v1/projects/:id/api-keys` (default role) | SDKs and `/v1/ingest`-style routes; safe to ship in a browser |
| Studio session | `ps_…` | `POST /v1/auth/login` / `signup` (the Studio does this for you) | Same access as the root token for a signed-in user |
