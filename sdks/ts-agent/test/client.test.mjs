import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  decodePlstBatch,
  PulseAgentClient,
  RingBuffer,
} from "../dist/index.js";

describe("RingBuffer, Retry, Backpressure, Sampling & Transport", () => {
  it("enforces drop_oldest and drop_newest policies in RingBuffer", () => {
    const rbOld = new RingBuffer(2, "drop_oldest");
    assert.equal(rbOld.push(1).dropped, undefined);
    assert.equal(rbOld.push(2).dropped, undefined);
    assert.equal(rbOld.push(3).dropped, 1);
    assert.deepEqual(rbOld.drain(5), [2, 3]);

    const rbNew = new RingBuffer(2, "drop_newest");
    rbNew.push("a");
    rbNew.push("b");
    const res = rbNew.push("c");
    assert.equal(res.accepted, false);
    assert.equal(res.dropped, "c");
    assert.deepEqual(rbNew.drain(5), ["a", "b"]);
  });

  it("sends PLST binary wire frames decodable by decodePlstBatch", async () => {
    const requests = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_live_123",
      usePlst: true,
      flushIntervalMs: 0,
      fetch: async (url, init) => {
        requests.push({ url, init });
        return new Response(
          JSON.stringify({
            accepted_traces: 1,
            accepted_spans: 1,
            truncated_payloads: 0,
            redacted_fields: 0,
            dropped_spans: 0,
            warnings: [],
          }),
          { status: 200 },
        );
      },
    });

    const sess = client.session({ sessionId: "sess_plst" });
    const tr = sess.trace({ traceId: "tr_plst", agentName: "qa-agent", input: { q: "hi" } });
    const gen = tr.generation({ name: "gen", model: "gpt-4o", provider: "openai" });
    gen.end({ output: "ok", inputTokens: 10, outputTokens: 20 });
    tr.end({ output: "done" });

    await client.shutdown();
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, "http://localhost:8080/v1/agent/traces");
    assert.equal(requests[0].init.headers["x-pulse-key"], "pk_live_123");
    assert.equal(requests[0].init.headers["Content-Type"], "application/x-pulse-trace");

    const decoded = decodePlstBatch(requests[0].init.body);
    assert.equal(decoded.traces.length, 1);
    assert.equal(decoded.spans.length, 1);
    assert.equal(decoded.traces[0].session_id, "sess_plst");
    assert.equal(decoded.spans[0].model, "gpt-4o");
  });

  it("retries 429 (with Retry-After: 0) and 503 then succeeds", async () => {
    let calls = 0;
    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      flushIntervalMs: 0,
      retryBaseMs: 5,
      retryMaxMs: 15,
      maxRetries: 3,
      fetch: async () => {
        calls += 1;
        if (calls === 1) {
          return new Response("rate limited", {
            status: 429,
            headers: { "Retry-After": "0" },
          });
        }
        if (calls === 2) {
          return new Response("unavailable", { status: 503 });
        }
        return new Response("{}", { status: 200 });
      },
    });

    client.trace({ agentName: "retry-bot" }).end();
    await client.flush();

    assert.equal(calls, 3);
    const stats = client.getStats();
    assert.equal(stats.retriedBatches, 2);
    assert.equal(stats.sentBatches, 1);
    assert.equal(stats.droppedBatches, 0);
    await client.shutdown();
  });

  it("drops permanent 4xx errors without retrying and invokes onError", async () => {
    for (const status of [400, 401, 413, 422]) {
      let calls = 0;
      const errors = [];
      const client = new PulseAgentClient({
        endpoint: "http://localhost:8080",
        apiKey: "pk_bad",
        flushIntervalMs: 0,
        maxRetries: 3,
        onError: (err, ctx) => errors.push({ err, ctx }),
        fetch: async () => {
          calls += 1;
          return new Response("bad request", { status });
        },
      });

      client.trace({ agentName: "drop-bot" }).end();
      await client.flush();

      assert.equal(calls, 1);
      assert.equal(errors.length, 1);
      assert.equal(errors[0].ctx.status, status);
      assert.equal(errors[0].ctx.retryable, false);
      assert.equal(client.getStats().droppedBatches, 1);
      assert.equal(client.getStats().droppedTraces, 1);
      await client.shutdown();
    }
  });

  it("handles network errors gracefully and emits 0 HTTP calls when sampleRate=0", async () => {
    let netCalls = 0;
    const netClient = new PulseAgentClient({
      endpoint: "http://127.0.0.1:1",
      apiKey: "pk_test",
      flushIntervalMs: 0,
      maxRetries: 1,
      retryBaseMs: 2,
      fetch: async () => {
        netCalls += 1;
        throw new TypeError("fetch failed");
      },
    });
    netClient.trace({ agentName: "offline-bot" }).end();
    await netClient.shutdown();
    assert.equal(netCalls, 2);
    assert.equal(netClient.getStats().droppedBatches, 1);

    let zeroCalls = 0;
    const zeroClient = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      sampleRate: 0,
      flushIntervalMs: 0,
      fetch: async () => {
        zeroCalls += 1;
        return new Response("{}", { status: 200 });
      },
    });
    const tr = zeroClient.trace({ agentName: "sampled-out" });
    tr.generation({ name: "gen" }).end();
    tr.end();
    await zeroClient.shutdown();
    assert.equal(zeroCalls, 0);
    assert.equal(zeroClient.getStats().sampledOutTraces, 1);
    assert.equal(zeroClient.getStats().sampledOutSpans, 1);
  });

  it("splits oversized batches to stay below maxBatchBytes", async () => {
    const bodies = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      maxBatchBytes: 2048,
      fetch: async (_url, init) => {
        bodies.push(init.body);
        return new Response("{}", { status: 200 });
      },
    });

    for (let i = 0; i < 10; i++) {
      client.trace({
        traceId: `tr_${i}`,
        agentName: "chunk-bot",
        input: "x".repeat(400),
      }).end();
    }
    await client.shutdown();
    assert.ok(bodies.length > 1);
    for (const b of bodies) {
      assert.ok(new TextEncoder().encode(b).byteLength <= 2048);
    }
    assert.equal(client.getStats().sentTraces, 10);
  });

  it("sanitizes once across maxBatchBytes splits without inflating redactedFields or hook calls", async () => {
    const sentBatches = [];
    let hookInputCalls = 0;
    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      flushBatchSize: 20,
      maxBatchBytes: 1024,
      redact: (value, ctx) => {
        if (ctx.field === "trace.input") {
          hookInputCalls += 1;
        }
        return value;
      },
      fetch: async (_url, init) => {
        sentBatches.push(JSON.parse(init.body));
        return new Response("{}", { status: 200 });
      },
    });

    for (let i = 0; i < 6; i++) {
      client
        .trace({
          traceId: `tr_redact_${i}`,
          agentName: "redact-bot",
          input: { api_key: `secret_${i}`, pad: "x".repeat(300) },
        })
        .end();
    }

    await client.shutdown();
    assert.ok(sentBatches.length > 1);
    const stats = client.getStats();
    assert.equal(stats.sentTraces, 6);
    assert.equal(stats.redactedFields, 6);
    assert.equal(hookInputCalls, 6);
  });

  it("enforces MAX_SPANS_PER_TRACE across maxBatchBytes splits", async () => {
    const sentSpans = [];
    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      flushBatchSize: 600,
      maxBatchBytes: 4096,
      fetch: async (_url, init) => {
        const body = JSON.parse(init.body);
        sentSpans.push(...body.spans);
        return new Response("{}", { status: 200 });
      },
    });

    for (let i = 0; i < 520; i++) {
      client
        .span({
          spanId: `sp_${i}`,
          traceId: "tr_cap_512",
          name: "step",
          input: { idx: i, pad: "p".repeat(120) },
        })
        .end();
    }

    await client.shutdown();
    const stats = client.getStats();
    assert.equal(sentSpans.length, 512);
    assert.equal(stats.sentSpans, 512);
    assert.equal(stats.droppedSpans, 8);
  });

  it("serializes concurrent flush() calls without racing", async () => {
    let activeFetches = 0;
    let maxConcurrentFetches = 0;
    const sentTraceIds = [];
    let releaseFirstFetch;
    const firstFetchGate = new Promise((resolve) => {
      releaseFirstFetch = resolve;
    });
    let fetchCount = 0;

    const client = new PulseAgentClient({
      endpoint: "http://localhost:8080",
      apiKey: "pk_test",
      usePlst: false,
      flushIntervalMs: 0,
      fetch: async (_url, init) => {
        fetchCount += 1;
        activeFetches += 1;
        maxConcurrentFetches = Math.max(maxConcurrentFetches, activeFetches);
        try {
          if (fetchCount === 1) {
            await firstFetchGate;
          }
          const body = JSON.parse(init.body);
          for (const tr of body.traces) {
            sentTraceIds.push(tr.trace_id);
          }
          return new Response("{}", { status: 200 });
        } finally {
          activeFetches -= 1;
        }
      },
    });

    client.trace({ traceId: "tr_c1", agentName: "c" }).end();
    const f1 = client.flush();
    // Wait a tick so fetch #1 is actively awaiting firstFetchGate.
    await new Promise((r) => setTimeout(r, 10));

    // Enqueue another trace while f1 is in flight and trigger two concurrent flushes.
    client.trace({ traceId: "tr_c2", agentName: "c" }).end();
    const f2 = client.flush();
    const f3 = client.flush();

    releaseFirstFetch();
    await Promise.all([f1, f2, f3]);

    assert.equal(maxConcurrentFetches, 1);
    assert.deepEqual(sentTraceIds, ["tr_c1", "tr_c2"]);
    await client.shutdown();
  });
});
