import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  capPayload,
  computeSpanDepths,
  isLuhnValidCard,
  MAX_PAYLOAD_BYTES,
  MAX_SPANS_PER_TRACE,
  REDACTED_MARKER,
  redactValue,
  resolvePrivacyPolicy,
  sanitizeBatch,
  toJsonValue,
} from "../dist/index.js";

function makeSpan(id, traceId, parentId = null, extra = {}) {
  return {
    span_id: id,
    trace_id: traceId,
    project_id: "p1",
    parent_span_id: parentId,
    linked_span_ids: [],
    span_phase: "final",
    depth: 0,
    name: id,
    span_type: "llm",
    status: "ok",
    start_ts_ms: 1000,
    end_ts_ms: 1100,
    duration_ms: 100,
    provider: "openai",
    model: "gpt-4o",
    model_id: null,
    model_version: null,
    prompt_template_id: null,
    prompt_template_version: null,
    input_tokens: 10,
    output_tokens: 5,
    cached_tokens: 0,
    total_tokens: 15,
    cost_nanos: 0,
    cost_usd: 0,
    temperature: null,
    finish_reason: null,
    model_params: null,
    tool_name: null,
    tool_version: null,
    tool_retry_count: 0,
    input: null,
    output: null,
    attributes: null,
    events: [],
    metadata: null,
    error_type: null,
    error_message: null,
    error_stack: null,
    ...extra,
  };
}

describe("Privacy, Redaction, Truncation & Graph Governance", () => {
  it("redacts API keys, Bearer tokens, emails, SSNs, Luhn cards, and sensitive keys", () => {
    const policy = resolvePrivacyPolicy({
      redactPatterns: [/CUSTOM-[0-9]{4}/g],
    });
    const stats = { redactedFields: 0, truncatedPayloads: 0, droppedSpans: 0 };

    // Construct sensitive test strings dynamically so secret scanners aren't tripped
    const openaiKey = ["sk", "proj", "1234567890abcdef1234"].join("-");
    const anthropicKey = ["sk", "ant", "api03", "abcdefghijklmnop1234"].join("-");
    const awsKey = ["AKIA", "IOSFODNN7EXAMPLE"].join("");
    const luhnCard = ["4111", "1111", "1111", "1111"].join("-");
    const nonLuhnNumber = "4111-1111-1111-1112";

    assert.equal(isLuhnValidCard(luhnCard), true);
    assert.equal(isLuhnValidCard(nonLuhnNumber), false);

    const payload = toJsonValue({
      prompt: `Contact alice@example.com, SSN 123-45-6789, OpenAI ${openaiKey}, Anthropic ${anthropicKey}, AWS ${awsKey}, token Bearer abcdef1234567890, code CUSTOM-4321`,
      card: `Pay with ${luhnCard} not ${nonLuhnNumber}`,
      Authorization: "should-be-completely-replaced",
      nested: {
        user_api_key: "secret-value",
        safe: "ok",
      },
    });

    const cleaned = redactValue(payload, policy, stats);
    assert.equal(cleaned.Authorization, REDACTED_MARKER);
    assert.equal(cleaned.nested.user_api_key, REDACTED_MARKER);
    assert.equal(cleaned.nested.safe, "ok");
    assert.equal(cleaned.prompt.includes("alice@example.com"), false);
    assert.equal(cleaned.prompt.includes("123-45-6789"), false);
    assert.equal(cleaned.prompt.includes(openaiKey), false);
    assert.equal(cleaned.prompt.includes(anthropicKey), false);
    assert.equal(cleaned.prompt.includes(awsKey), false);
    assert.equal(cleaned.prompt.includes("abcdef1234567890"), false);
    assert.equal(cleaned.prompt.includes("CUSTOM-4321"), false);
    assert.equal(cleaned.card.includes(luhnCard), false);
    assert.equal(cleaned.card.includes(nonLuhnNumber), true);
    assert.ok(stats.redactedFields >= 8);
  });

  it("enforces capture toggles per SpanType and excludeAttributes", () => {
    const policy = resolvePrivacyPolicy({
      capturePrompts: false,
      captureToolResults: false,
      captureUserId: false,
      excludeAttributes: ["SecretHeader"],
    });

    const batch = {
      traces: [
        {
          trace_id: "t1",
          project_id: "p1",
          agent_name: "a",
          agent_version: "1",
          prompt_version: null,
          model_config_version: null,
          tool_config_version: null,
          app_release: null,
          environment: "production",
          session_id: null,
          user_id: "user_secret",
          workflow_id: null,
          parent_trace_id: null,
          status: "ok",
          start_ts_ms: 1000,
          end_ts_ms: 1100,
          duration_ms: 100,
          input: "trace input",
          output: "trace output",
          error_type: null,
          error_message: null,
          failure_source: null,
          metadata: null,
          tags: [],
          span_count: 2,
          llm_call_count: 1,
          tool_call_count: 1,
          error_span_count: 0,
          input_tokens: 0,
          output_tokens: 0,
          cached_tokens: 0,
          total_tokens: 0,
          total_cost_nanos: 0,
          total_cost_usd: 0,
          primary_model: null,
          primary_provider: null,
          avg_eval_score: null,
          eval_pass_rate: null,
          has_loop_warning: false,
        },
      ],
      spans: [
        makeSpan("s_llm", "t1", null, {
          span_type: "llm",
          input: "secret prompt",
          output: "visible response",
          attributes: { secretheader: "x", keep: "y" },
        }),
        makeSpan("s_tool", "t1", "s_llm", {
          span_type: "tool",
          input: { query: "weather" },
          output: { raw: "hidden result" },
        }),
      ],
    };

    sanitizeBatch(batch, policy);
    assert.equal(batch.traces[0].user_id, null);
    assert.equal(batch.traces[0].input, "trace input");
    assert.equal(batch.spans[0].input, null);
    assert.equal(batch.spans[0].output, "visible response");
    assert.deepEqual(batch.spans[0].attributes, { keep: "y" });
    assert.deepEqual(batch.spans[1].input, { query: "weather" });
    assert.equal(batch.spans[1].output, null);
  });

  it("caps payloads at 64 KiB safely across multi-byte UTF-8 and quotes", () => {
    const stats = { redactedFields: 0, truncatedPayloads: 0, droppedSpans: 0 };
    const big = "🚀\"\\".repeat(30_000);
    const capped = capPayload(big, MAX_PAYLOAD_BYTES, stats);
    assert.equal(stats.truncatedPayloads, 1);
    assert.equal(capped._truncated, true);
    assert.ok(capped._original_bytes > MAX_PAYLOAD_BYTES);
    const encodedBytes = new TextEncoder().encode(JSON.stringify(capped)).byteLength;
    assert.ok(encodedBytes <= MAX_PAYLOAD_BYTES);
    // Ensure preview does not contain broken UTF-16 lone surrogates
    assert.equal(encodeURIComponent(capped._preview).length > 0, true);
  });

  it("breaks cycles in computeSpanDepths without orphaning ancestors and caps at 512 spans", () => {
    // C -> A <-> B (root C, child A, child B, cycle A -> B)
    const spans = [
      makeSpan("C", "t1", null),
      makeSpan("A", "t1", "C"),
      makeSpan("B", "t1", "A"),
    ];
    // Close cycle C's parent? No: C -> A <-> B means A.parent = B and B.parent = A, while A has ancestor C?
    // Wait: in single-parent tree C -> A <-> B means C's parent is null, A.parent = B, B.parent = A, or D -> A -> B -> A.
    const cycleSpans = [
      makeSpan("C", "t1", null),
      makeSpan("A", "t1", "B"),
      makeSpan("B", "t1", "A"),
      makeSpan("D", "t1", "C"),
    ];
    computeSpanDepths(cycleSpans);
    // Neither C nor D is orphaned; cycle between A and B is broken cleanly
    assert.equal(cycleSpans[0].depth, 0);
    assert.equal(cycleSpans[3].depth, 1);
    assert.ok(cycleSpans[1].parent_span_id === null || cycleSpans[2].parent_span_id === null);

    const manySpans = Array.from({ length: 520 }, (_, i) =>
      makeSpan(`sp_${i}`, "t1", null, {
        linked_span_ids: ["sp_orphan", `sp_${i}`, "sp_orphan", "  "],
      }),
    );
    const batch = { traces: [], spans: manySpans };
    const stats = sanitizeBatch(batch, resolvePrivacyPolicy());
    assert.equal(batch.spans.length, MAX_SPANS_PER_TRACE);
    assert.equal(stats.droppedSpans, 8);
    assert.deepEqual(batch.spans[0].linked_span_ids, ["sp_orphan"]);
  });
});
