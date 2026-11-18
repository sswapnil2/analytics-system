# Pulse documentation

Pulse is a self-hosted product analytics system: one Rust binary (`pulse-node`) serves event ingest,
the query engine, session replay, feature flags, experiments, the Studio web UI and an MCP endpoint
for AI agents.

| Guide | What it covers |
|---|---|
| [Set up with an agent](agent-setup.md) | One prompt for Claude Code, Codex, Cursor or Gemini CLI that installs Pulse, instruments your app and connects MCP |
| [Quickstart](quickstart.md) | Build and run `pulse-node`, get the admin token and an ingest key, send a first event |
| [Self-hosting in the cloud](self-hosting-cloud.md) | Cheapest hosts, domain, systemd + Caddy HTTPS, firewall, storage and backups |
| [Web SDK](web-sdk.md) | `sdks/ts-web`: tracking, identify, user properties, flags, replay |
| [Mobile core (beta)](mobile-core.md) | `crates/pulse-sdk-core`: the Rust core with UniFFI Swift/Kotlin bindings |
| [REST API](rest-api.md) | The open-source `/v1/...` routes, grouped by auth type |
| [MCP for agents](mcp.md) | `POST /v1/mcp`: tools, protocol support, and config for Claude Code, Claude Desktop, Cursor, OpenAI Codex / Responses API, Gemini CLI and Antigravity |
| [Claude Code plugin](../integrations/claude-code-plugin/README.md) | Installable plugin that wires up the MCP server and adds analysis skills |

## Credentials at a glance

| Credential | Looks like | Where it comes from | Use it for |
|---|---|---|---|
| Root admin token | `sk_root_…` (or whatever you set in `PULSE_ADMIN_TOKEN`) | Written to `root_admin_token` in the data directory, or set by you | Everything: all projects, creating projects and keys, `/v1/mcp` |
| Project admin key | `sk_live_…` | `POST /v1/projects/:id/api-keys` with `"role": "admin"` | Admin, query and MCP routes for **one** project |
| Ingest key | `pk_live_…` | Startup log on first boot, or `POST /v1/projects/:id/api-keys` (default role) | SDKs and `/v1/ingest`-style routes; safe to ship in a browser |
| Studio session | `ps_…` | `POST /v1/auth/login` / `signup` (the Studio does this for you) | The projects in the user's organisation only. Not encryption keys, the licence or the audit log |

## Accounts and organisations

Every account belongs to one organisation and sees only that organisation's projects.

- **First account:** sign-up asks for the root admin token as a setup code. That account owns the default organisation and the data already on the node.
- **Later accounts:** sign-up asks for an organisation name and creates a new, separate organisation with its own empty project and ingest key.
- **Teammates:** set `PULSE_ALLOWED_EMAIL_DOMAINS=yourco.com`. People at that domain who leave the organisation name blank join the default organisation.
- **Closed node:** set `PULSE_ALLOW_NEW_ORGS=false` to stop strangers creating organisations. Then only allowed domains can sign up.
- **Google sign-in:** a new Google account gets a workspace named after its owner.

There are no invites yet, so the allowed-domain setting is how people join an existing organisation. The root admin token still sees every organisation.
