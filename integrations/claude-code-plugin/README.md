# Pulse plugin for Claude Code

Connects Claude Code to a running Pulse node's MCP server (`/v1/mcp`) and adds two skills:

- **analyze-funnel**: find the step where users drop off, the segment that explains it, and the frustrated sessions to watch.
- **instrument-web-app**: add the Pulse web SDK to a browser app with sensible events, identify, and privacy-safe replay.

## Install

```text
/plugin marketplace add sswapnil2/analytics-system
/plugin install pulse@pulse
```

From a local clone, use the clone's path instead of `sswapnil2/analytics-system`, or load it for one
session with `claude --plugin-dir integrations/claude-code-plugin`.

## Configure

The plugin's MCP server reads three environment variables when Claude Code starts:

| Variable | Default | Meaning |
|---|---|---|
| `PULSE_URL` | `http://127.0.0.1:8080` | Base URL of your node (no trailing slash, no `/v1/mcp`) |
| `PULSE_TOKEN` | none (required) | A project admin key (`sk_live_…`, recommended) or the root token |
| `PULSE_PROJECT` | node default | Project id to query when using a root token |

```bash
export PULSE_URL=http://127.0.0.1:8080
export PULSE_TOKEN=sk_live_…
claude
```

Run `/mcp` in Claude Code to check that `plugin:pulse:pulse` is connected. See [docs/mcp.md](../../docs/mcp.md)
for the tools, how to create a key, and troubleshooting.

Using another agent? The MCP server itself works with any Streamable HTTP client; see
[docs/mcp.md](../../docs/mcp.md#connect-a-client) for OpenAI Codex, Gemini CLI and Antigravity config.
