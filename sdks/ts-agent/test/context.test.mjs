import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { ContextManager, PulseAgentClient } from "../dist/index.js";

function makeCaptureFetch(captured) {
  return async (url, init) => {
    captured.push({ url, init });
    return new Response(JSON.stringify({ accepted_traces: 1, accepted_spans: 1, warnings: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
}

describe("Context Propagation & Streaming Span Phases", () => {
  it("isolates concurrent async branches with AsyncLocalStorage", async () => {
    const captured = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:9999",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      fetch: makeCaptureFetch(captured),
    });

    await Promise.all([
      client.withSession({ sessionId: "sess_A", userId: "user_A" }, async () => {
        await client.withTrace({ traceId: "tr_A", agentName: "agentA" }, async (tr) => {
          await new Promise((r) => setTimeout(r, 15));
          await tr.withSpan({ spanId: "sp_A1", name: "rootA" }, async () => {
            await new Promise((r) => setTimeout(r, 15));
            const child = client.tool({ spanId: "sp_A2", name: "toolA", toolName: "calc" });
            child.end({ output: 1 });
          });
        });
      }),
      client.withSession({ sessionId: "sess_B", userId: "user_B" }, async () => {
        await client.withTrace({ traceId: "tr_B", agentName: "agentB" }, async (tr) => {
          await new Promise((r) => setTimeout(r, 5));
          await tr.withSpan({ spanId: "sp_B1", name: "rootB" }, async () => {
            await new Promise((r) => setTimeout(r, 25));
            const child = client.tool({ spanId: "sp_B2", name: "toolB", toolName: "search" });
            child.end({ output: 2 });
          });
        });
      }),
    ]);

    await client.shutdown();
    const batches = captured.map((c) => JSON.parse(c.init.body));
    const traces = batches.flatMap((b) => b.traces);
    const spans = batches.flatMap((b) => b.spans);

    const trA = traces.find((t) => t.trace_id === "tr_A");
    const trB = traces.find((t) => t.trace_id === "tr_B");
    assert.equal(trA.session_id, "sess_A");
    assert.equal(trB.session_id, "sess_B");

    const spA2 = spans.find((s) => s.span_id === "sp_A2");
    const spB2 = spans.find((s) => s.span_id === "sp_B2");
    assert.equal(spA2.trace_id, "tr_A");
    assert.equal(spA2.parent_span_id, "sp_A1");
    assert.equal(spB2.trace_id, "tr_B");
    assert.equal(spB2.parent_span_id, "sp_B1");
  });

  it("supports stack fallback ContextManager for non-Node/Edge runtimes", async () => {
    const fallbackCtx = new ContextManager({ forceFallback: true });
    assert.equal(fallbackCtx.usingAsyncLocalStorage, false);

    const client = new PulseAgentClient(
      {
        endpoint: "http://localhost:9999",
        apiKey: "pk_test",
        usePlst: false,
        flushIntervalMs: 0,
        fetch: async () => new Response("{}", { status: 200 }),
      },
      fallbackCtx,
    );

    await assert.rejects(async () => {
      await client.withSession({ sessionId: "sess_fb" }, async () => {
        assert.equal(client.getContext().sessionId, "sess_fb");
        await client.withTrace({ traceId: "tr_fb" }, async () => {
          assert.equal(client.getContext().traceId, "tr_fb");
          throw new Error("boom");
        });
      });
    }, /boom/);

    // Stack must be cleanly unwound after async throw
    assert.deepEqual(client.getContext(), {});
    await client.shutdown();
  });

  it("emits span_phase start -> update -> final and rolls up trace loop warning", async () => {
    const captured = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:9999",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      fetch: makeCaptureFetch(captured),
    });

    const tr = client.trace({ traceId: "tr_stream", agentName: "stream-bot" });
    const sp = tr.generation({ spanId: "sp_stream", name: "stream.llm", model: "gpt-4o", provider: "openai" });
    sp.start({ input: "hi" });
    sp.update({ output: "hel" });
    sp.end({ output: "hello", inputTokens: 100, outputTokens: 50 });
    sp.end({ output: "ignored double end" });

    for (let i = 0; i < 4; i++) {
      tr.tool({ name: "loop_tool", toolName: "same_tool" }).end({ output: i });
    }
    tr.end({ output: "done" });

    await client.shutdown();
    const batches = captured.map((c) => JSON.parse(c.init.body));
    const spans = batches.flatMap((b) => b.spans).filter((s) => s.span_id === "sp_stream");
    assert.deepEqual(
      spans.map((s) => s.span_phase),
      ["start", "update", "final"],
    );
    assert.equal(spans[2].output, "hello");
    assert.ok(spans[2].cost_nanos > 0);

    const trace = batches.flatMap((b) => b.traces)[0];
    assert.equal(trace.has_loop_warning, true);
    assert.equal(trace.llm_call_count, 1);
    assert.equal(trace.tool_call_count, 4);
  });

  it("clears parentSpanId across nested withTrace boundaries while preserving parentTraceId", async () => {
    const captured = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:9999",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      fetch: makeCaptureFetch(captured),
    });

    await client.withTrace({ traceId: "tr_outer", agentName: "outer" }, async (outerTr) => {
      await outerTr.withSpan({ spanId: "sp_outer", name: "outer.step" }, async () => {
        await client.withTrace({ traceId: "tr_inner", agentName: "inner" }, async () => {
          const rootInner = client.span({ spanId: "sp_inner_root", name: "inner.root" });
          rootInner.end({ output: "ok" });
        });
        const afterInner = client.span({ spanId: "sp_outer_child", name: "outer.child" });
        afterInner.end({ output: "back" });
      });
    });

    await client.shutdown();
    const batches = captured.map((c) => JSON.parse(c.init.body));
    const traces = batches.flatMap((b) => b.traces);
    const spans = batches.flatMap((b) => b.spans);

    const trInner = traces.find((t) => t.trace_id === "tr_inner");
    const spInnerRoot = spans.find((s) => s.span_id === "sp_inner_root");
    const spOuterChild = spans.find((s) => s.span_id === "sp_outer_child");

    assert.equal(trInner.parent_trace_id, "tr_outer");
    assert.equal(spInnerRoot.trace_id, "tr_inner");
    assert.equal(spInnerRoot.parent_span_id, null);
    assert.equal(spOuterChild.trace_id, "tr_outer");
    assert.equal(spOuterChild.parent_span_id, "sp_outer");
  });

  it("rolls up context-created spans inside client.withTrace onto the AgentTrace", async () => {
    const captured = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:9999",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      fetch: makeCaptureFetch(captured),
    });

    await client.withTrace({ traceId: "tr_ctx_rollup", agentName: "ctx-bot" }, async () => {
      await client.withGeneration(
        { name: "ctx.llm", provider: "openai", model: "gpt-4o" },
        async (sp) => {
          sp.end({ inputTokens: 100, outputTokens: 50, output: "hi" });
        },
      );
      await client.withTool({ name: "ctx.tool", toolName: "search" }, async (sp) => {
        sp.end({ output: ["r1"] });
      });
    });

    await client.shutdown();
    const trace = captured
      .map((c) => JSON.parse(c.init.body))
      .flatMap((b) => b.traces)
      .find((t) => t.trace_id === "tr_ctx_rollup");

    assert.ok(trace);
    assert.equal(trace.span_count, 2);
    assert.equal(trace.llm_call_count, 1);
    assert.equal(trace.tool_call_count, 1);
    assert.equal(trace.input_tokens, 100);
    assert.equal(trace.output_tokens, 50);
    assert.equal(trace.total_tokens, 150);
    assert.ok(trace.total_cost_nanos > 0);
    assert.equal(trace.primary_model, "gpt-4o");
    assert.equal(trace.primary_provider, "openai");
  });
});
