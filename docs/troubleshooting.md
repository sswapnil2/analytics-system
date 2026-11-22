# Troubleshooting

Each entry gives the **symptom**, its likely **cause**, how to **diagnose** it, and the **fix**. Problems connecting an MCP client are covered in [MCP → Troubleshooting](mcp.md#troubleshooting).

Two tools help with almost everything below:

- `new PulseWebClient({ ..., debug: true })` logs every tracked event and every send result in the browser console, including events the server rejected.
- `GET /v1/status?project_id=<id>` with an admin token returns row counts, segments and identity stats for a project.

## Ingest errors

| Status | Meaning | What to do |
|---|---|---|
| `200` with `rejected: [...]` | Part of the batch was invalid; the rest was stored | Read each `rejected[i].error` (for example `missing event name`) |
| `400` | No event in the request was valid, the body isn't valid JSON or `PLSB`, or an id is invalid | Fix the payload. The SDK drops such batches |
| `401` | Missing or unknown ingest key | Send `x-pulse-key: pk_live_…` from *this* node and project |
| `409` | `identify` tried to merge two ids that are both already identified users | Expected protection. Don't call `identify(B)` while still identified as `A`; call `reset()` on logout |
| `413` | Request body over 8 MiB | Send smaller batches |
| `422` | Schema cap hit: more than 128 new event names or 256 new property keys in one batch, or the project is at 8,192 event names or 16,384 property keys | You are probably putting ids or timestamps into event or property *names*. Put them in property *values* |
| `503` with `code: "not_writer"` | Another node holds this project's writer lease, this node restarted after a crash and its old lease has not expired yet, or it cannot reach the metadata database | Retry after `Retry-After` (the SDKs do). Clears within `PULSE_WRITER_LEASE_TTL_SECS` (15 s) of the other writer stopping. Route ingest to one node per project |
| `429` | The response `code` says which: `rate_limited` (default 20,000 events/s per project per node), `l0_backpressure` (ingest is ahead of flushing) or `log_backpressure` (ingest is ahead of the processor that applies logged events) | Retry after `Retry-After`. The SDKs do this. Raise `PULSE_INGEST_EVENTS_PER_SEC` if the load is legitimate; watch `pulse_l0_bytes` and `pulse_ingest_log_lag_bytes` in `/metrics` for the other two. On agent trace ingest, `l0_backpressure` means unflushed traces reached `PULSE_AGENT_L0_MAX_MB` or `PULSE_AGENT_L0_PROJECT_MAX_MB`; it clears when the flush catches up |
| `pulse_ingest_dead_lettered_events_total` grows | Events could not be stored, usually because the project reached 8,192 event names or 16,384 property keys | Find what is generating new names (ids or timestamps in event or property names). The events are under `wal_dead/<project>/` in the object store |
| `5xx` | Storage write failed | Check the node log and `/readyz`. Events in that request were **not** acknowledged; retry them |

## Events don't show up

**Symptom:** you call `track()` but nothing appears in Trends.

| Possible cause | Diagnose | Fix |
|---|---|---|
| The SDK hasn't sent yet | `debug: true`; `pulse.getQueuedEventCount()` | Sends happen every 5 s or at 20 events. Call `await pulse.flush()` to send now |
| Wrong `endpointUrl` or a blocked request | Browser Network tab: look for `POST /v1/ingest` | Point `endpointUrl` at the node's public URL. Ingest allows any origin, so a CORS error here usually means the URL is wrong, or a proxy in front is rejecting the request |
| Wrong key, or a `sk_` key in the SDK | The response is `401` or `403` | Use a `pk_live_` key for the project you are looking at |
| Looking at a different project | The Studio's project picker vs. the key's project | Switch project. A key always writes to its own project |
| The user opted out | `pulse.hasOptedOut()` | `optIn()`, or check `optOutByDefault` |
| `beforeSend` returned `null`, or the event is `dropped` in the lexicon | `debug: true` shows the drop; check Events & privacy | Fix the hook or the lexicon status |
| The time range excludes the event | Device clock far off, or a backfill via `/import` | `/v1/ingest` corrects skew over 5 s using `sentAt`. For `/import`, check the timestamps you sent |
| Counted as a duplicate | The response shows `deduped > 0` | Send a distinct `messageId` per real event. Same id or same content within 15–30 min counts as a retry |

Events are queryable as soon as ingest returns `200`. You don't need to wait for a flush.

## Numbers look wrong

| Symptom | Cause | Fix |
|---|---|---|
| A user counted twice (before and after login) | `identify()` never ran, or the server uses a different user id from the browser | Call `identify(userId)` on login, with the same id your backend sends |
| "Sessions" ≈ "users" for backend events | Server events without `session_id` share one session per user | Send `session_id` |
| A result labelled *sampled* | The query covers more than 10 M estimated rows, so `auto` sampling engaged | Read `sampling.relative_error_pct`, or rerun with `"sampling": "exact"` (slower) |
| Uniques differ slightly between two long-range charts | One used the footer HyperLogLog fast path (`method: "hll_sketch"`, about 3 % error) | Add any filter or breakdown, or pass `"sampling": "exact"` |
| A flag variant changed after login | Assignment hashes the current distinct id, which `identify()` changes | Identify before the first flag check |
| Server events attached to the wrong user | A Segment `identify` in a batch with both `userId` and `anonymousId` links those two ids | Send `anonymousId` only when you mean to link it |

## Query errors

| Error | Cause | Fix |
|---|---|---|
| `QueryTimeout(ms)` | The query ran past `PULSE_QUERY_TIMEOUT_MS` (30 s) | Narrow the range, remove breakdowns, or raise the timeout |
| `QueryTooLarge` | More than `PULSE_QUERY_MAX_ROWS` (250 M) rows to scan | Narrow the range, or allow `auto` sampling |
| `QueryMemoryExceeded` | A breakdown with very many values, or a big flows query | Break down by a lower-cardinality property; raise `PULSE_QUERY_MAX_MEMORY_MB` |
| `Overloaded` | Too many concurrent queries for the node or the project | Retry. Raise `PULSE_QUERY_MAX_CONCURRENT` if the CPU allows |
| MCP: "unknown event 'X'. Did you mean 'Y'?" | The event name doesn't exist in the project | Use the suggested name. `get_lexicon_schema` lists them all |

## Authentication problems

| Symptom | Cause | Fix |
|---|---|---|
| The admin token stopped working after a restart | No `PULSE_ADMIN_TOKEN` set, so a new token is generated on every start | `cat <data_dir>/root_admin_token`, and set `PULSE_ADMIN_TOKEN` permanently |
| The ingest key from first boot is lost | Keys are stored hashed and shown once | Create a new one in Settings → API keys, or with `POST /v1/projects/:id/api-keys` |
| `403` on an admin route | An ingest key was used, the role is too low, or a `sk_live_` key named another project | Use an admin credential for the right project. The error names the role you need |
| `404` for a project that exists | The project belongs to a different organisation from your account | Expected: other organisations' projects are hidden |
| Sign-up says "verification required" | Password sign-ups must verify their email, and Pulse has no mailer | Open the link the node wrote to `<data_dir>/outbox` |
| `429` on login | 10 failed attempts for this email within 15 minutes | Wait, or reset the password |
| `403` on Studio writes behind a proxy | The CSRF check: `Origin` doesn't match `PULSE_PUBLIC_URL` | Set `PULSE_PUBLIC_URL` to the exact public origin (scheme, host, port) |
| Browser CORS error calling admin routes from another origin | Admin routes allow only `PULSE_CORS_ORIGINS` | Add your origin there |
| Google sign-in button missing | Only one of the two Google variables is set | Set both `PULSE_GOOGLE_CLIENT_ID` and `PULSE_GOOGLE_CLIENT_SECRET`, plus `PULSE_PUBLIC_URL` |

## Replay problems

| Symptom | Cause | Fix |
|---|---|---|
| No replay for a session | `enableWireframeReplay` off, `replaySampleRate` < 1, or `replayTriggerEvents` not met | Check the config; `pulse.isRecordingSession()` |
| Everything shows `████` | Text is masked by default | Add `data-pulse-unmask` or `unmaskSelectors` for safe text. Inputs stay masked regardless |
| The replay looks sparse | Snapshots cover at most 120 elements and are taken at most once a second | Expected for wireframe replay; it is not pixel-perfect |

## Node and configuration problems

| Symptom | Cause | Fix |
|---|---|---|
| The node won't start: "unknown flag" | A typo in a flag | `pulse-node --help` |
| The node won't start with Postgres | No `PULSE_VAULT_KEK` | Set a 64-hex `PULSE_VAULT_KEK` (the same on every node) |
| The node panics on start with an encryption error | `PULSE_BYOK_MASTER_KEY` is neither 64 hex characters nor a passphrase of 16+ characters | Fix the value |
| The Studio shows a placeholder page | `PULSE_WEB_DIST_DIR` has no `index.html` | `npm --prefix web ci && npm --prefix web run build`, or pass `--web-dir` |
| `/readyz` returns `503` | The node is draining, or the metadata DB or object store is unreachable | The JSON body lists the failing check |
| `pulse_maintenance_errors_total` keeps rising | Background flushes or compactions are failing | Check the logs for the `kind`; usually storage permissions or space |
| The first release build takes ~15 min | Thin LTO with one codegen unit | Use `cargo build -p pulse-node` (debug) while you develop |

Still stuck? Open an issue with the Pulse commit, how you ran the node, and the smallest request that reproduces the problem (see [Contributing](contributing.md)).
