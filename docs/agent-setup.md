# Set up Pulse with a coding agent

Paste the prompt below into Claude Code, OpenAI Codex, Cursor, Gemini CLI or any coding agent that can
run shell commands. Run the agent from your app's repository: it clones and runs Pulse next to your app,
adds the web SDK, and connects itself to Pulse over MCP so you can ask it questions about your data.

It needs Git, the Rust toolchain (via rustup) and Node.js. The first release build takes about 15 minutes;
the prompt uses a debug build, which compiles much faster.

## The prompt

```text
Set up Pulse (open-source product analytics, https://github.com/sswapnil2/analytics-system) for this project.

1. Clone https://github.com/sswapnil2/analytics-system into ../pulse (skip if it exists).
   In that folder run:
     npm --prefix web ci && npm --prefix web run build
     cargo build -p pulse-node
2. Create a root admin token with `openssl rand -hex 24`, prefixed with `sk_root_`.
   Save it as PULSE_ADMIN_TOKEN in ../pulse/.env (create the file, never commit it).
   Start the node in the background from ../pulse:
     PULSE_ADMIN_TOKEN=<token> ./target/debug/pulse-node --port 8080 --data-dir ./pulse_data
   Wait until `curl -s http://127.0.0.1:8080/v1/status -H "Authorization: Bearer <token>"` succeeds.
3. Create two keys for the default project `proj_default` with
   POST http://127.0.0.1:8080/v1/projects/proj_default/api-keys (Bearer <token>, JSON body):
     {"label": "web app", "role": "ingest_and_flags"}  -> pk_live_… ingest key for the app
     {"label": "agent",   "role": "admin"}             -> sk_live_… project admin key for MCP
   Each `plaintext_key` is shown only once; keep both in ../pulse/.env.
4. If this project is a browser app, follow ../pulse/docs/web-sdk.md: install ../pulse/sdks/ts-web,
   initialise PulseWebClient with endpointUrl http://127.0.0.1:8080 and the pk_live_ key read from
   an environment variable, call identify() after login, and track() the 5–10 events that matter most
   for this product (sign-up, activation, the core action, purchase). Show me the event list first.
5. Connect yourself to Pulse over MCP using ../pulse/docs/mcp.md. URL http://127.0.0.1:8080/v1/mcp,
   bearer token = the sk_live_ admin key. In Claude Code you can instead install the plugin:
   /plugin marketplace add sswapnil2/analytics-system  then  /plugin install pulse@pulse
6. Send one test event to /v1/ingest, then confirm it through MCP, and tell me the Studio URL
   (http://127.0.0.1:8080) and where the tokens are saved.
```

## What you get

- A Pulse node on <http://127.0.0.1:8080> with the Studio. Sign in with the root token from `../pulse/.env`.
- Your app sending events and, if you allow it, privacy-safe session replay.
- An agent that can query funnels, retention and replays for you over MCP. Try
  "Where do users drop off between sign-up and first purchase this week?"

## Notes

- **Keys:** the agent gets a project admin key (`sk_live_…`), which only reaches that one project.
  Keep the root token for yourself.
- **Demo data:** add `--seed` to the start command to load a demo dataset into `proj_fintech_in` instead.
- **Production:** for anything beyond a laptop, follow the [quickstart](quickstart.md) and set
  `PULSE_VAULT_KEK` before storing secrets.
