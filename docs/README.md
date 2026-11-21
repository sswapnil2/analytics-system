# Pulse documentation

Pulse is open-source, self-hosted **product analytics**. One Rust binary (`pulse-node`) provides:

- event ingest
- an exact query engine for trends, funnels, retention and journeys
- privacy-masked session replay
- feature flags and A/B experiments
- the Studio web UI
- an MCP endpoint, so AI agents can query your analytics

It runs on a laptop with one data directory, or against S3 and Postgres.

## What problem it solves

Product analytics usually means one of two things: sending your users' behaviour to a SaaS vendor, or running a stack of ClickHouse, Kafka and Redis yourself. Pulse gives you the questions product teams actually ask, without either:

- "where do users drop between KYC and first payment?"
- "did week-1 retention fall after the release?"
- "show me the sessions of people who rage-tapped checkout"

The data stays on your infrastructure. Concretely:

- **One process to run.** Back it up by copying one directory ([why](adr/0001-single-binary.md)).
- **Exact numbers by default.** Sampling kicks in only above 10 M rows and is always disclosed ([how](how-it-works.md#4-how-queries-work)).
- **Privacy on the device.** Replay text is masked before it leaves the browser. Lexicon rules can drop or mask properties before storage ([details](security-privacy.md)).
- **Agent-ready.** Claude, Cursor, Codex or Gemini can answer analytics questions over [MCP](mcp.md).

**What it is not:** Pulse is not an APM: it does not take OpenTelemetry metrics, logs or general service traces. It does trace LLM and agent runs, including OTLP GenAI spans ([AI agent observability](agent-observability-architecture.md)). See [Limitations](limitations.md).

## Start here

| If you want to… | Read |
|---|---|
| Download Pulse (Linux binaries, Docker image) | [Quickstart → Install](quickstart.md#1-install) |
| Understand the vocabulary | [Concepts](concepts.md) |
| Run Pulse and send a first event in 5 minutes | [Quickstart](quickstart.md) |
| Let a coding agent set everything up | [Set up with an AI agent](agent-setup.md) |
| Instrument a web app | [Web SDK](web-sdk.md) |
| Send events from a backend | [Integrations → server-side](integrations.md#server-side-events-over-http) |
| See every supported integration | [Integrations](integrations.md) |
| Look up an endpoint | [REST API](rest-api.md) |
| Trace LLM and AI agent runs | [AI agent observability](agent-observability-architecture.md) |
| Look up a setting | [Configuration](configuration.md) |
| Understand how it works inside | [System architecture](architecture.md) → [How Pulse works](how-it-works.md) → [Data model](data-model.md) |
| Evaluate security and data handling | [Security & privacy](security-privacy.md) |
| Know why it's built this way | [Design decisions (ADRs)](adr/README.md) |
| Run it in production | [Self-hosting in the cloud](self-hosting-cloud.md) |
| Fix something | [Troubleshooting](troubleshooting.md) |
| Know what's missing | [Limitations](limitations.md) |
| Contribute | [Contributing](contributing.md) |

**Giving these docs to an AI agent?** Point it at [`llms.txt`](https://sswapnil2.github.io/analytics-system/llms.txt), an index of every page as plain Markdown, or at [`llms-full.txt`](https://sswapnil2.github.io/analytics-system/llms-full.txt), all pages in one file. On the website, every page also has *View as Markdown* and *Copy as Markdown*.

## Credentials at a glance

| Credential | Looks like | Where it comes from | Use it for |
|---|---|---|---|
| Root admin token | `sk_root_…` (or whatever you set in `PULSE_ADMIN_TOKEN`) | Written to `root_admin_token` in the data directory, or set by you | Everything: all projects, creating projects and keys, `/v1/mcp` |
| Project admin key | `sk_live_…` | `POST /v1/projects/:id/api-keys` with `"role": "admin"` | Admin, query and MCP routes for **one** project |
| Ingest key | `pk_live_…` | Startup log on first boot, or `POST /v1/projects/:id/api-keys` (default role) | SDKs and `/v1/ingest`-style routes; safe to ship in a browser |
| Studio session | `ps_…` | `POST /v1/auth/login` / `signup` (the Studio does this for you) | The projects in the user's organisation only. Not encryption keys or the audit log |

## Accounts and organisations

Every account belongs to one organisation and sees only that organisation's projects.

- **First account:** sign-up asks for the root admin token as a setup code. That account owns the default organisation and the data already on the node.
- **Later accounts:** sign-up asks for an organisation name and creates a new, separate organisation with its own empty project and ingest key.
- **Teammates (invites or allowed domains):** organisation admins can create single-use invite links in the Studio (or via `POST /v1/org/invites`), or you can set `PULSE_ALLOWED_EMAIL_DOMAINS=yourco.com` so teammates at that domain who leave the organisation name blank join the default organisation.
- **Closed node:** set `PULSE_ALLOW_NEW_ORGS=false` to stop strangers creating organisations. Then only invited users or allowed domains can sign up.
- **Google sign-in:** configure `PULSE_GOOGLE_CLIENT_ID`, `PULSE_GOOGLE_CLIENT_SECRET`, and `PULSE_PUBLIC_URL`. A new Google account gets a workspace named after its owner unless `PULSE_ALLOW_NEW_ORGS=false` is set.

The root admin token sees every organisation.

## Other guides

- [Mobile core (beta)](mobile-core.md): the Rust core with UniFFI Swift/Kotlin bindings
- [Claude Code plugin](../integrations/claude-code-plugin/README.md): installs the MCP server and adds analysis skills
