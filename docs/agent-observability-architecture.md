# AI agent observability: architecture

How `pulse-node` records, stores, evaluates and serves LLM agent runs. Code lives in
`crates/pulse-agent` (model, wire protocol, store, evaluators), `crates/pulse-control/src/api_agent.rs`
(HTTP routes), `agent_eval_worker.rs` (async evaluations) and `web/src/workspaces/AgentObservabilityWorkspace.tsx`
(Studio, `#/agents`).

## Data model

A **trace** is one agent run. It holds identity and versions (`agent_name`, `agent_version`,
`prompt_version`, `environment`, `app_release`), context (`session_id`, `user_id`), outcome (`status`
`ok | running | error | timeout | cancelled`, `error_type`, `error_message`, `failure_source`), payloads,
and rollups recomputed as spans arrive (tokens, `cost_nanos`, span/LLM/tool counts, `primary_model`,
`has_loop_warning`, eval score).

A **span** is one step inside a run, linked by `parent_span_id` (and `linked_span_ids` for DAG edges,
`parent_trace_id` for sub-agent runs). `span_type` is `agent | llm | tool | retrieval | embedding |
guardrail | http | db | workflow | custom`. LLM spans carry model, tokens and cost; tool spans carry
`tool_name` and retries. Spans can stream: `span_phase` `start` → `update` → `final`, merged by
per-field CRDT ranks so out-of-order updates converge.

Everything is scoped by `project_id`.

## Ingest

| Route | Body | Auth |
|---|---|---|
| `POST /v1/agent/traces` | `application/json` (`{"traces": [...], "spans": [...]}`) or `application/x-pulse-trace` (PLST binary) | `x-pulse-key: pk_…` or `Authorization: Bearer pk_…` |
| `POST /v1/otel/v1/traces` | OTLP `ExportTraceServiceRequest`, JSON or protobuf | same |

OTLP spans are mapped from OpenTelemetry GenAI attributes (`gen_ai.*`) and OpenInference / Traceloop
fallbacks (`llm.model_name`, `llm.token_count.*`, `tool.name`, `input.value`, `output.value`).

### PLST wire format (`application/x-pulse-trace`)

A 20-byte little-endian header: `magic "PLST" | ver=1 | flags | dict_id | uncomp_len | comp_len | crc32`,
then a raw or Zstd body with five columnar sections: string table, content-addressed blob table
(`xxh3_64` + exact-byte dedup), trace rows, span rows, span events. Flags: `FLAG_RAW_ZSTD` (0x01),
`FLAG_DICT_ZSTD` (0x02, the built-in agent dictionary, `AGENT_DICT_ID = 2`), `FLAG_PARTIAL` / `FLAG_FINAL`
(0x04 / 0x08) for streamed runs. The SDKs in `sdks/ts-agent` and `sdks/py-agent` send PLST by default and
fall back to JSON.

Ingest does privacy redaction and pricing, appends to the WAL, and returns. It makes no SQL writes.

## Storage

Traces and spans are not stored in SQL. There are no `agent_traces` or `agent_spans` tables (removed in
favour of the store below). The store uses the node's object store (`PULSE_DATA_DIR` locally, or
`PULSE_OBJECT_STORE_URL`), encrypted at rest like event segments.

| Layer | Object path | What it is |
|---|---|---|
| **PTWL** write-ahead log | `agent_wal/{project}/{writer}/seq_{n}.ptwl` | Group-committed frames (10 ms default) with a 32-byte header `"PTWL" \| ver \| flags \| seq \| ts \| len \| crc32` around a PLST payload. Replayed on start; a torn tail frame is skipped. |
| **L0** memtable | memory | Sharded by trace; merges partial spans. Flushed when a shard is 4 s old or full. |
| **PLSS** segments | `agent_segments/{project}/{date}/shard_{nn}/…plss` | Immutable, trace-clustered columnar files. Tags: 0x01 strings, 0x02 trace summaries, 0x03 span metrics, 0x04 span tree + events, 0x05 64 KiB-aligned payload packs, 0x06 split-block Bloom filters + zone maps (pruning), 0x07 session rollups. |
| Manifest | `agent_manifests/{project}/epoch_{n}.json` | MVCC list of live segments, swapped by compare-and-set. Replaced segments wait in `pending_gc` for a 15-minute grace period so running reads never 404. |

Queries merge unflushed L0 with pruned PLSS segments, so a run is readable as soon as ingest returns.
Background compaction merges small segments. Run summaries leave out large fields (`input`, `output`,
`metadata`, `error_message`) to stay small; trace detail reads them from the payload packs.

Node roles: `all` and `ingest` write the store (WAL, L0 flush every 5 s, compaction and GC every minute);
`query` and `compactor` open it read-only. The eval worker runs on every role except `compactor`.

### SQL (`MetaStore`, migration `0011_agent_observability.sql`)

SQL holds configuration and small derived data only:

- `agent_evaluations` and `agent_eval_rollups` (per trace, span and session averages, used for score filters)
- `agent_evaluator_configs`, `agent_datasets`, `agent_dataset_items`, `agent_experiments`, `agent_experiment_runs`
- `agent_alert_rules`, `agent_alert_events`, `agent_privacy_policies`, `agent_model_pricing`
- `agent_erasure_tombstones` (trace, session or user erasure, applied at read time and purged on compaction)

## Evaluations

Enabled evaluators run on new runs from a bounded queue (`EVAL_QUEUE_CAPACITY = 2048`). Ingest uses a
non-blocking `try_send`; when the queue is full the job is dropped and counted in
`pulse_agent_eval_queue_dropped_total`, so a slow judge never stalls ingest. At most 8 LLM-judge calls
run at once, on the project's BYOK LLM key. Built-in checks: `no_errors`, `no_loop`, `output_not_empty`,
`latency_budget`, `cost_budget`, `token_budget`, `regex_match`, `contains_keywords`, `json_schema_keys`,
`tool_sequence`, `exact_match`, `pii_leakage`, `llm_judge`. Results can also be posted directly (human
review) or triggered for one run (`POST .../traces/:trace_id/evaluate`).

## Privacy and limits

The per-project privacy policy controls what is captured (inputs, outputs, tool arguments and results,
prompts, `user_id`), redacts PII and keys (emails, card numbers, `sk-…`/`pk_…`/`ghp_…`, and a key
blocklist such as `password`, `authorization`, `token`) to `"[REDACTED]"`, sets sampling, and sets
`retention_days` (default 30, pruned by trace date). Erasure by trace, session or user writes a tombstone
that hides data immediately.

| Limit | Value |
|---|---|
| Ingest body | 8 MiB (`413`) |
| PLST decompressed size, and materialized blobs | 64 MiB (`413`) |
| Spans per trace | 512 |
| Payload per field (input, output, attributes) | 64 KiB, UTF-8-safe truncation |
| Eval queue | 2,048 jobs |

## Access

Reads need the `viewer` role; creating evaluators, datasets, experiments, alerts and reviews needs
`member`; privacy policy, pricing rules, trace deletion and erasure need `admin`. Routes are listed in
[REST API](rest-api.md#ai-agent-observability), and four MCP tools (`list_agent_traces`,
`get_agent_trace`, `query_agent_analytics`, `list_agent_evaluations`) are in [MCP for agents](mcp.md).

Prometheus counters (no per-tenant labels): `pulse_agent_traces_ingested_total`,
`pulse_agent_spans_ingested_total`, `pulse_agent_blobs_deduped_total`, `pulse_agent_evals_executed_total`,
`pulse_agent_eval_queue_dropped_total`.
