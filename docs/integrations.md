# Integrations

This is every way to get data into Pulse or out of it, and how well each one is supported. Only the items marked **Supported** are tested in CI and shipped in releases.

## Sending data

| Integration | Status | Guide |
|---|---|---|
| Browser (any framework: React, Vue, plain JS) | **Supported.** `@pulse-analytics/ts-web`, tested in CI, packed on every release | [Web SDK](web-sdk.md) |
| Server-side / backend (any language) | **Supported** over plain HTTP JSON. No server SDK packages | [Below](#server-side-events-over-http) |
| Segment-format payloads | **Supported.** `track`, `page`, `screen`, `identify`, `alias` in `/v1/ingest`; `group` goes to `/v1/groups` | [REST API → ingest semantics](rest-api.md#ingest-semantics) |
| Backfill from another tool (JSON, NDJSON, CSV) | **Supported.** `POST /v1/projects/:id/import`, deduplicated for 30 days | [REST API](rest-api.md#lexicon-cohorts-data-management-bearer-token) |
| iOS / Android (native) | **Beta.** A Rust core with UniFFI Swift/Kotlin bindings that you build yourself. No packaged SDK. The Android demo is marked *not yet verified* | [Mobile core](mobile-core.md) |
| React Native | **Experimental, undocumented.** `sdks/react-native` wraps the Web SDK with native recorder modules. It is not in CI or releases | `sdks/react-native/README.md` |
| OpenTelemetry (OTLP traces, metrics, logs) | **Not supported** | — |
| Python, Java, Go, Ruby SDKs | **Not available.** Use HTTP | — |
| LLM / agent tracing | **Supported.** `POST /v1/agent/traces`, OTLP GenAI spans at `/v1/otel/v1/traces`, or the `sdks/ts-agent` and `sdks/py-agent` SDKs | [AI agent observability](agent-observability-architecture.md) |

## Reading data

| Integration | Status | Guide |
|---|---|---|
| REST query API | **Supported** | [REST API](rest-api.md#queries-bearer-token) |
| MCP: Claude Code, Claude Desktop, Cursor, OpenAI Codex, OpenAI Responses API, Gemini CLI, Google Antigravity | **Supported** over Streamable HTTP with a bearer token. No OAuth, so OAuth-only clients cannot connect | [MCP for agents](mcp.md) |
| Claude Code plugin | **Supported.** MCP config plus two skills (`analyze-funnel`, `instrument-web-app`) | [Plugin README](../integrations/claude-code-plugin/README.md) |
| Event export (NDJSON, JSON, CSV) | **Supported.** `GET /v1/projects/:id/export`, up to 50,000 events per call | [REST API](rest-api.md#lexicon-cohorts-data-management-bearer-token) |
| Warehouse export (Parquet to your S3 bucket) | **Supported**, on demand: `POST /v1/projects/:id/export/warehouse` with a `storage` BYOK credential | [REST API](rest-api.md#admin-bearer-token) |
| Alert webhooks | **Supported.** HMAC-signed `pulse.alert.triggered` | [Configuration → webhook signatures](configuration.md#webhook-signatures) |
| Prometheus | **Supported.** `GET /metrics` for the node's own health | [Architecture](architecture.md#observability-of-pulse-itself) |
| AI Copilot (natural-language questions in the Studio) | **Supported** with your own LLM key: Anthropic, OpenAI, Gemini or a custom endpoint | [Security → outbound](security-privacy.md#outbound-connections) |

## Server-side events over HTTP

There is no server SDK to install. A server sends events with one HTTP call from any language.

**Requirements:** a running node and an ingest key (`pk_live_…`). See the [Quickstart](quickstart.md).

**Send events.** Batch up to a few thousand events per request. The body may be at most 8 MiB.

```bash
curl -X POST http://127.0.0.1:8080/v1/ingest \
  -H "Content-Type: application/json" -H "x-pulse-key: $PULSE_INGEST_KEY" \
  -d '{"events": [
        {"event": "Order_Paid", "userId": "usr_1042", "messageId": "ord_981-paid",
         "timestamp": "2026-11-21T10:15:00Z", "properties": {"amount": 49, "currency": "USD"}}
      ]}'
```

**Expected result:**

```json
{"status":"ok","accepted":1,"deduped":0,"dropped":0,"server_ts_ms":1795256100000}
```

Rules for server callers:

- **Always send `messageId` (or `event_uuid` / `insert_id`) or a `timestamp`.** A retry is only recognised as a duplicate if it carries the same id or the same content and timestamp. With neither, a retried event is counted twice.
- **Send `session_id` if you care about sessions.** Without it, all of a user's server events fall into one session.
- **Treat `429` as "slow down".** Wait for `Retry-After`, then retry the same body; the ids make the retry safe. Treat `400` and `422` (on sync endpoints like `/import`) as permanent errors for that batch. Treat `5xx` as retryable.
- **Note on `/v1/ingest` response counts.** Under W1 async acknowledgement, `accepted` counts valid events queued into the writer buffer and `deduped` in the HTTP response is `0` (exact duplicate counts are exposed on `GET /metrics` as `pulse_ingest_events_deduped_total`).
- **Use the same `userId` as your frontend's `identify()`.** Then server and browser events belong to the same person.

**Verify.** Check the response's `accepted` count first. Then run a query, or open Trends in the Studio and pick the event:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/projects/$PROJECT/query/insights \
  -H "Authorization: Bearer $PULSE_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"time_range":{"start_ts_ms":1795219200000,"end_ts_ms":1795305600000},"interval":"day",
       "series":[{"label":"A","event_name":"Order_Paid","metric":"totals"}]}'
```

**Limitations:**

- No client library, so batching and retries are your code's job.
- Profile operations go to `/v1/engage` (synchronous; returns `canonical_uid` / `canonical_uids` and propagates `409`/`503`/`500`), identity merges to `/v1/identify` (synchronous; returns `canonical_uid`), and group operations to `/v1/groups` (synchronous; propagates `409`/`503`/`500`).

## Requesting an integration

Open an issue on [GitHub](https://github.com/sswapnil2/analytics-system/issues) describing the platform and how you would use it.
