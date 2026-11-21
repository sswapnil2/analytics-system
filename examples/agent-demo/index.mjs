import assert from "node:assert/strict";
import http from "node:http";
import process from "node:process";
import {
  AGENT_DICT_ID,
  decodePlstBatch,
  DEFAULT_REDACT_KEYS,
  FLAG_DICT_ZSTD,
  PLST_HEADER_LEN,
  PLST_MAGIC,
  PLST_VERSION,
  PulseAgentClient,
  REDACTED_MARKER,
  supportsZstd,
  traceGuardrail,
  traceRag,
  wrapOpenAI,
} from "../../sdks/ts-agent/dist/index.js";

function printHelp() {
  console.log(`Pulse AI Agent Observability Demo (Node ESM)

Usage:
  node examples/agent-demo/index.mjs [options]

Options:
  --endpoint <url>     Pulse base URL or /v1/agent/traces endpoint
                       (precedence: --endpoint > PULSE_ENDPOINT > PULSE_BASE_URL > http://127.0.0.1:18190)
  --api-key <key>      Pulse ingest API key
                       (precedence: --api-key > PULSE_API_KEY > pk_live_REPLACE_ME)
  --project-id <id>    Optional Pulse project ID
                       (precedence: --project-id > PULSE_PROJECT_ID > "")
  --self-test          Run offline loopback verification with transient 429/503 retries and PLST assertions
  -h, --help           Show this help message
`);
}

function firstNonEmpty(...values) {
  for (const val of values) {
    if (typeof val === "string" && val.trim().length > 0) {
      return val.trim();
    }
  }
  return undefined;
}

function parseArgs(argv) {
  let endpointArg;
  let apiKeyArg;
  let projectIdArg;
  let selfTest = false;
  let help = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--self-test") {
      selfTest = true;
    } else if (arg === "--help" || arg === "-h") {
      help = true;
    } else if (arg === "--endpoint") {
      if (i + 1 >= argv.length) {
        throw new Error("Missing value for --endpoint");
      }
      endpointArg = argv[++i];
    } else if (arg.startsWith("--endpoint=")) {
      endpointArg = arg.slice("--endpoint=".length);
    } else if (arg === "--api-key") {
      if (i + 1 >= argv.length) {
        throw new Error("Missing value for --api-key");
      }
      apiKeyArg = argv[++i];
    } else if (arg.startsWith("--api-key=")) {
      apiKeyArg = arg.slice("--api-key=".length);
    } else if (arg === "--project-id") {
      if (i + 1 >= argv.length) {
        throw new Error("Missing value for --project-id");
      }
      projectIdArg = argv[++i];
    } else if (arg.startsWith("--project-id=")) {
      projectIdArg = arg.slice("--project-id=".length);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  const endpoint =
    firstNonEmpty(endpointArg, process.env.PULSE_ENDPOINT, process.env.PULSE_BASE_URL) ??
    "http://127.0.0.1:18190";

  const envOrFlagApiKey = firstNonEmpty(apiKeyArg, process.env.PULSE_API_KEY);
  const apiKey = envOrFlagApiKey ?? (selfTest ? "pk_live_selftest_demo" : "pk_live_REPLACE_ME");

  const projectId = projectIdArg ?? process.env.PULSE_PROJECT_ID ?? "";

  return { endpoint, apiKey, projectId, selfTest, help };
}

function createMockOpenAI() {
  return {
    chat: {
      completions: {
        async create(params) {
          async function* streamChunks() {
            yield {
              id: "chatcmpl-demo-01",
              model: params.model,
              choices: [
                {
                  index: 0,
                  delta: { role: "assistant", content: "Your order ORD-901 is eligible " },
                  finish_reason: null,
                },
              ],
            };
            yield {
              id: "chatcmpl-demo-01",
              model: params.model,
              choices: [
                {
                  index: 0,
                  delta: { content: "for a full refund within 5 business days." },
                  finish_reason: "stop",
                },
              ],
              usage: {
                prompt_tokens: 145,
                completion_tokens: 28,
                total_tokens: 173,
                prompt_tokens_details: { cached_tokens: 24 },
              },
            };
          }
          return streamChunks();
        },
      },
    },
  };
}

async function runAgentScenario({ endpoint, apiKey, projectId, selfTest }) {
  const client = new PulseAgentClient({
    endpoint,
    apiKey,
    projectId,
    agentName: "support-copilot",
    agentVersion: "1.2.0",
    environment: "staging",
    promptVersion: "prompt-v3",
    modelConfigVersion: "gpt4o-mini-2026",
    toolConfigVersion: "tools-v2",
    appRelease: "agent-demo-0.1.0",
    transport: "auto",
    compression: "auto",
    flushBatchSize: 64,
    flushIntervalMs: 0,
    maxRetries: selfTest ? 4 : 3,
    retryBaseMs: selfTest ? 15 : 250,
    retryMaxMs: selfTest ? 60 : 30000,
    redactPii: true,
    redactKeys: [...DEFAULT_REDACT_KEYS, "customer_secret"],
    onError: (err, ctx) => {
      console.error(
        `[pulse-agent-demo] transport error (status=${ctx?.status ?? "network"}, retryable=${ctx?.retryable}): ${err.message}`,
      );
    },
  });

  const sessionId = `sess_ts_demo_${Date.now()}`;
  let turn1TraceId = "";
  let turn2TraceId = "";
  let retrievalSpanId = "";

  await client.withSession(
    {
      sessionId,
      userId: "user_demo_42",
      workflowId: "wf_customer_support",
      agentName: "support-copilot",
      agentVersion: "1.2.0",
      environment: "staging",
      metadata: { channel: "web_chat" },
    },
    async (sess) => {
      // Turn 1: Guardrail + RAG retrieval + linked LLM + wrapOpenAI stream + tool error/recovery
      await sess.withTrace(
        {
          input: {
            question: "Check refund eligibility for alice@example.com on order ORD-901",
            customer_secret: "cs_top_secret_987",
            api_key: "sk-proj-demo1234567890abcdef",
          },
          tags: ["demo", "rag", "support"],
          metadata: { turn: "support.turn_1", turnNumber: 1 },
        },
        async (tr1) => {
          turn1TraceId = tr1.traceId;

          await traceGuardrail(
            client,
            {
              guardrailName: "input_pii_and_policy",
              input: {
                text: "User alice@example.com (SSN 123-45-6789, card 4111-1111-1111-1111) asked for refund.",
              },
            },
            async () => ({
              passed: true,
              action: "redact_and_allow",
              reason: "PII masked client-side before ingest",
            }),
          );

          const docs = await traceRag(
            client,
            {
              query: "refund eligibility and turnaround policy",
              topK: 3,
              indexName: "kb_policies",
            },
            async (ragSpan) => {
              retrievalSpanId = ragSpan.spanId;
              return [
                {
                  id: "policy-refund-01",
                  score: 0.95,
                  content: "Digital & physical orders qualify for a full refund within 30 days.",
                  metadata: { source: "policies/refunds.md" },
                },
                {
                  id: "policy-sla-02",
                  score: 0.88,
                  content: "Approved refunds are processed within 5 business days.",
                  metadata: { source: "policies/sla.md" },
                },
              ];
            },
          );

          let ragGenSpanId = "";
          await tr1.withGeneration(
            {
              name: "rag.grounded.planner",
              provider: "openai",
              model: "gpt-4o-mini",
              linkedSpanIds: [retrievalSpanId],
              temperature: 0.2,
              input: {
                messages: [
                  { role: "system", content: "Use policy docs to plan order lookup." },
                  { role: "user", content: "Check order ORD-901 for alice@example.com." },
                ],
                docIds: docs.map((d) => d.id),
              },
            },
            async (genSpan) => {
              ragGenSpanId = genSpan.spanId;
              genSpan.end({
                output: { next_action: "lookup_order", order_id: "ORD-901" },
                inputTokens: 185,
                outputTokens: 32,
                cachedTokens: 40,
                finishReason: "tool_calls",
              });
            },
          );

          // Tool call #1: simulates transient failure caught by agent
          try {
            await tr1.withTool(
              {
                name: "tool.order_lookup",
                toolName: "order_api_v1",
                toolVersion: "1.0.0",
                toolRetryCount: 0,
                input: {
                  orderId: "ORD-901",
                  customer_email: "alice@example.com",
                  customer_secret: "cs_tool_secret_456",
                },
              },
              async () => {
                throw new Error("Upstream order API timeout after 250ms");
              },
            );
          } catch {
            // Caught and recovered via fallback cache tool below
          }

          // Tool call #2: recovery succeeds
          const orderData = await tr1.withTool(
            {
              name: "tool.order_lookup_cache",
              toolName: "order_cache_v2",
              toolVersion: "2.1.0",
              toolRetryCount: 1,
              input: { orderId: "ORD-901", fallback: true },
            },
            async (toolSpan) => {
              const res = {
                orderId: "ORD-901",
                status: "delivered",
                eligibleForRefund: true,
                totalUsd: 49.0,
              };
              toolSpan.end({
                output: res,
                attributes: { "tool.recovered_from": "order_api_v1" },
              });
              return res;
            },
          );

          // Streaming OpenAI adapter inside sess.withTrace (emits first_token + rolls up onto tr1)
          const openai = wrapOpenAI(createMockOpenAI(), client, {
            spanName: "openai.chat.stream",
          });
          const stream = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            stream: true,
            temperature: 0.3,
            messages: [
              { role: "system", content: "Synthesize customer response." },
              { role: "user", content: `Order lookup result: ${JSON.stringify(orderData)}` },
            ],
          });
          let streamedText = "";
          for await (const chunk of stream) {
            const delta = chunk.choices?.[0]?.delta?.content;
            if (typeof delta === "string") {
              streamedText += delta;
            }
          }

          await traceGuardrail(
            client,
            {
              guardrailName: "output_safety_check",
              linkedSpanIds: [retrievalSpanId, ragGenSpanId],
              input: { response_preview: streamedText },
            },
            async () => ({ passed: true, action: "allow" }),
          );

          tr1.end({
            output: { answer: streamedText, order: orderData },
          });
        },
      );

      // Turn 2: Follow-up child trace linked via parentTraceId + explicit span_phase streaming
      await sess.withTrace(
        {
          parentTraceId: turn1TraceId,
          input: { followUp: "Summarize the refund SLA concisely." },
          tags: ["demo", "follow_up"],
          metadata: { turn: "support.turn_2", turnNumber: 2 },
        },
        async (tr2) => {
          turn2TraceId = tr2.traceId;

          const streamSpan = tr2.generation({
            name: "anthropic.stream.summary",
            provider: "anthropic",
            model: "claude-sonnet-4",
            linkedSpanIds: [retrievalSpanId],
            input: { prompt: "Summarize refund SLA in one sentence." },
          });
          streamSpan.start({
            attributes: { "stream.mode": "explicit_phase" },
          });
          streamSpan.update({
            output: { partial: "Refunds arrive within 5 " },
            attributes: { "stream.chunk": 1 },
          });
          streamSpan.end({
            output: { content: "Refunds arrive within 5 business days to your original payment method." },
            inputTokens: 110,
            outputTokens: 24,
            finishReason: "end_turn",
          });

          tr2.end({
            output: {
              summary: "Refunds arrive within 5 business days to your original payment method.",
            },
          });
        },
      );
    },
  );

  await client.flush();
  await client.shutdown();

  return {
    stats: client.getStats(),
    sessionId,
    turn1TraceId,
    turn2TraceId,
    retrievalSpanId,
  };
}

async function runSelfTest(config) {
  const receivedRequests = [];
  const decodedBatches = [];
  let attemptCount = 0;

  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const rawBody = Buffer.concat(chunks);
    attemptCount += 1;

    receivedRequests.push({
      method: req.method,
      url: req.url,
      headers: req.headers,
      rawBody,
    });

    if (req.method !== "POST" || req.url !== "/v1/agent/traces") {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not_found" }));
      return;
    }

    assert.equal(req.headers["x-pulse-key"], config.apiKey, "x-pulse-key header mismatch");
    assert.equal(
      req.headers["content-type"],
      "application/x-pulse-trace",
      "Expected PLST binary content-type",
    );

    const frame = new Uint8Array(rawBody.buffer, rawBody.byteOffset, rawBody.byteLength);
    assert.ok(frame.byteLength >= PLST_HEADER_LEN, "PLST frame shorter than 20B header");
    assert.deepEqual(frame.slice(0, 4), PLST_MAGIC, "Invalid PLST magic bytes");
    assert.equal(frame[4], PLST_VERSION, "Invalid PLST version");
    const dv = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    if (supportsZstd()) {
      assert.ok((frame[5] & FLAG_DICT_ZSTD) !== 0, "Expected FLAG_DICT_ZSTD when zstd is supported");
      assert.equal(dv.getUint16(6, true), AGENT_DICT_ID, "Expected AGENT_DICT_ID === 2");
    }

    const decoded = decodePlstBatch(frame);
    decodedBatches.push(decoded);

    if (attemptCount === 1) {
      res.writeHead(429, { "retry-after": "0" });
      res.end();
      return;
    }
    if (attemptCount === 2) {
      res.writeHead(503);
      res.end();
      return;
    }

    res.writeHead(202, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        accepted_traces: decoded.traces.length,
        accepted_spans: decoded.spans.length,
        truncated_payloads: 0,
        redacted_fields: 0,
        dropped_spans: 0,
        warnings: [],
      }),
    );
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  assert.ok(addr && typeof addr === "object", "Expected loopback server address");
  const mockEndpoint = `http://127.0.0.1:${addr.port}`;

  try {
    const result = await runAgentScenario({
      endpoint: mockEndpoint,
      apiKey: config.apiKey,
      projectId: config.projectId,
      selfTest: true,
    });

    const { stats, sessionId, turn1TraceId, turn2TraceId, retrievalSpanId } = result;
    assert.ok(attemptCount >= 3, `Expected >= 3 HTTP attempts (429 + 503 + 202), got ${attemptCount}`);
    assert.ok(stats.retriedBatches >= 2, `Expected retriedBatches >= 2, got ${stats.retriedBatches}`);
    assert.equal(stats.droppedBatches, 0, `Expected droppedBatches === 0, got ${stats.droppedBatches}`);
    assert.ok(stats.sentTraces >= 2, `Expected sentTraces >= 2, got ${stats.sentTraces}`);
    assert.ok(stats.redactedFields > 0, `Expected redactedFields > 0, got ${stats.redactedFields}`);

    // Use accepted batches (attempt 3+) for content assertions (earlier frames contain same batch before retry)
    const acceptedFrames = decodedBatches.slice(2);
    const allTraces = acceptedFrames.flatMap((b) => b.traces);
    const allSpans = acceptedFrames.flatMap((b) => b.spans);

    const tr1 = allTraces.find((t) => t.trace_id === turn1TraceId);
    const tr2 = allTraces.find((t) => t.trace_id === turn2TraceId);
    assert.ok(tr1, "Turn 1 trace missing in decoded PLST batches");
    assert.ok(tr2, "Turn 2 trace missing in decoded PLST batches");
    assert.equal(tr1.agent_name, "support-copilot", "Turn 1 agent_name mismatch");
    assert.equal(tr2.agent_name, "support-copilot", "Turn 2 agent_name mismatch");
    assert.equal(tr1.session_id, sessionId, "Turn 1 session_id mismatch");
    assert.equal(tr2.session_id, sessionId, "Turn 2 session_id mismatch");
    assert.equal(tr2.parent_trace_id, turn1TraceId, "Turn 2 parent_trace_id linkage mismatch");
    assert.ok(
      tr1.total_cost_nanos + tr2.total_cost_nanos > 0,
      "Expected positive total_cost_nanos across traces",
    );

    const spanTypes = new Set(allSpans.map((s) => s.span_type));
    for (const expectedType of ["retrieval", "llm", "tool", "guardrail"]) {
      assert.ok(spanTypes.has(expectedType), `Missing span_type=${expectedType}`);
    }

    const linkedLlmSpan = allSpans.find(
      (s) => s.span_type === "llm" && s.linked_span_ids.includes(retrievalSpanId),
    );
    assert.ok(linkedLlmSpan, "Expected LLM span linked to retrievalSpanId");

    const openAiStreamSpan = allSpans.find(
      (s) => s.name === "openai.chat.stream" && s.span_phase === "final",
    );
    assert.ok(openAiStreamSpan, "Expected final openai.chat.stream span");
    assert.ok(
      openAiStreamSpan.events.some((e) => e.name === "first_token"),
      "Expected first_token event on wrapOpenAI streaming span",
    );

    const explicitStreamPhases = new Set(
      allSpans.filter((s) => s.name === "anthropic.stream.summary").map((s) => s.span_phase),
    );
    for (const phase of ["start", "update", "final"]) {
      assert.ok(
        explicitStreamPhases.has(phase),
        `Missing span_phase=${phase} for anthropic.stream.summary`,
      );
    }

    const toolSpans = allSpans.filter((s) => s.span_type === "tool");
    assert.ok(toolSpans.some((s) => s.status === "error"), "Expected at least one error tool span");
    assert.ok(toolSpans.some((s) => s.status === "ok"), "Expected at least one ok tool span");

    const serializedPayloads = JSON.stringify({ traces: allTraces, spans: allSpans });
    assert.ok(
      serializedPayloads.includes(REDACTED_MARKER),
      "Expected REDACTED_MARKER in decoded payloads",
    );
    for (const secret of [
      "alice@example.com",
      "sk-proj-demo1234567890abcdef",
      "123-45-6789",
      "4111-1111-1111-1111",
      "cs_top_secret_987",
      "cs_tool_secret_456",
    ]) {
      assert.ok(
        !serializedPayloads.includes(secret),
        `Raw secret leaked in decoded PLST payload: ${secret}`,
      );
    }

    console.log(
      JSON.stringify(
        {
          mode: "self-test",
          ok: true,
          zstd: supportsZstd(),
          httpAttempts: attemptCount,
          stats,
        },
        null,
        2,
      ),
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

async function main() {
  let config;
  try {
    config = parseArgs(process.argv.slice(2));
  } catch (err) {
    console.error(err instanceof Error ? err.message : String(err));
    printHelp();
    process.exitCode = 2;
    return;
  }

  if (config.help) {
    printHelp();
    return;
  }

  if (config.selfTest) {
    await runSelfTest(config);
    return;
  }

  const result = await runAgentScenario(config);
  console.log(
    JSON.stringify(
      {
        mode: "live",
        endpoint: config.endpoint,
        sessionId: result.sessionId,
        turn1TraceId: result.turn1TraceId,
        turn2TraceId: result.turn2TraceId,
        stats: result.stats,
      },
      null,
      2,
    ),
  );
  if (result.stats.droppedBatches > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
