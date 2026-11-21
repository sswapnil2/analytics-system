import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  createLangChainCallbackHandler,
  PulseAgentClient,
  traceEmbedding,
  traceGuardrail,
  traceRag,
  wrapAnthropic,
  wrapGemini,
  wrapOpenAI,
  wrapRetriever,
} from "../dist/index.js";

function createTestClient() {
  const batches = [];
  const client = new PulseAgentClient({
    endpoint: "http://localhost:8080",
    apiKey: "pk_test",
    usePlst: false,
    flushIntervalMs: 0,
    fetch: async (_url, init) => {
      batches.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    },
  });
  return { client, batches };
}

describe("Zero-Dependency Adapters", () => {
  it("wraps OpenAI sync, streaming (with TTFT first_token), and embeddings", async () => {
    const { client, batches } = createTestClient();
    const fakeOpenAI = {
      chat: {
        completions: {
          async create(params) {
            if (params.stream) {
              return {
                async *[Symbol.asyncIterator]() {
                  yield { model: "gpt-4o", choices: [{ delta: { content: "Hel" } }] };
                  yield {
                    model: "gpt-4o",
                    choices: [{ delta: { content: "lo" }, finish_reason: "stop" }],
                    usage: {
                      prompt_tokens: 100,
                      completion_tokens: 20,
                      prompt_tokens_details: { cached_tokens: 40 },
                    },
                  };
                },
              };
            }
            return {
              model: "gpt-4o-mini",
              choices: [{ finish_reason: "stop", message: { content: "Hi" } }],
              usage: { prompt_tokens: 50, completion_tokens: 10 },
            };
          },
        },
      },
      embeddings: {
        async create() {
          return { model: "text-embedding-3-small", usage: { prompt_tokens: 12 } };
        },
      },
    };

    const openai = wrapOpenAI(fakeOpenAI, client);

    await client.withTrace({ traceId: "tr_oai", agentName: "oai-agent" }, async () => {
      const syncRes = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: "hi" }],
      });
      assert.equal(syncRes.model, "gpt-4o-mini");

      const stream = await openai.chat.completions.create({
        model: "gpt-4o",
        stream: true,
        messages: [{ role: "user", content: "stream" }],
      });
      let full = "";
      for await (const chunk of stream) {
        full += chunk.choices?.[0]?.delta?.content ?? "";
      }
      assert.equal(full, "Hello");

      await openai.embeddings.create({ model: "text-embedding-3-small", input: "doc" });
    });

    await client.shutdown();
    const spans = batches.flatMap((b) => b.spans);
    const streamFinal = spans.find(
      (s) => s.model === "gpt-4o" && s.span_phase === "final",
    );
    assert.ok(streamFinal);
    assert.equal(streamFinal.input_tokens, 100);
    assert.equal(streamFinal.output_tokens, 20);
    assert.equal(streamFinal.cached_tokens, 40);
    assert.equal(streamFinal.finish_reason, "stop");
    assert.deepEqual(streamFinal.output, { content: "Hello" });
    assert.equal(streamFinal.events.some((e) => e.name === "first_token"), true);
    assert.ok(streamFinal.cost_nanos > 0);

    const embSpan = spans.find((s) => s.span_type === "embedding");
    assert.ok(embSpan);
    assert.equal(embSpan.input_tokens, 12);

    const oaiTrace = batches.flatMap((b) => b.traces).find((t) => t.trace_id === "tr_oai");
    assert.ok(oaiTrace);
    assert.equal(oaiTrace.span_count, 3);
    assert.equal(oaiTrace.llm_call_count, 2);
    assert.equal(oaiTrace.input_tokens, 162);
    assert.equal(oaiTrace.output_tokens, 30);
    assert.equal(oaiTrace.cached_tokens, 40);
    assert.equal(oaiTrace.primary_model, "gpt-4o-mini");
    assert.equal(oaiTrace.primary_provider, "openai");
  });

  it("wraps Anthropic messages.create and messages.stream", async () => {
    const { client, batches } = createTestClient();
    const fakeAnthropic = {
      messages: {
        async create(params) {
          return {
            model: params.model,
            stop_reason: "end_turn",
            usage: { input_tokens: 80, output_tokens: 30, cache_read_input_tokens: 10 },
            content: [{ type: "text", text: "Claude reply" }],
          };
        },
        stream() {
          const events = [
            {
              type: "message_start",
              message: {
                model: "claude-sonnet-4",
                usage: { input_tokens: 60, cache_read_input_tokens: 20 },
              },
            },
            { type: "content_block_delta", delta: { text: "Streamed" } },
            {
              type: "message_delta",
              delta: { stop_reason: "end_turn" },
              usage: { output_tokens: 15 },
            },
          ];
          const streamObj = {
            aborted: false,
            listeners: [],
            on(event, fn) {
              this.listeners.push({ event, fn });
              return this;
            },
            abort() {
              this.aborted = true;
            },
            async finalMessage() {
              return {
                model: "claude-sonnet-4",
                stop_reason: "end_turn",
                usage: { input_tokens: 60, output_tokens: 15, cache_read_input_tokens: 20 },
                content: [{ type: "text", text: "Streamed" }],
              };
            },
            async finalText() {
              return "Streamed";
            },
            async *[Symbol.asyncIterator]() {
              for (const ev of events) {
                yield ev;
              }
            },
          };
          return streamObj;
        },
      },
    };

    const anthropic = wrapAnthropic(fakeAnthropic, client);
    await client.withTrace({ traceId: "tr_ant", agentName: "ant-agent" }, async () => {
      await anthropic.messages.create({
        model: "claude-sonnet-4",
        messages: [{ role: "user", content: "hello" }],
      });
      const stream1 = anthropic.messages.stream({
        model: "claude-sonnet-4",
        messages: [{ role: "user", content: "stream" }],
      });
      assert.equal(stream1.on("text", () => {}), stream1);
      for await (const _ev of stream1) {
        // consume
      }
      const stream2 = anthropic.messages.stream({
        model: "claude-sonnet-4",
        messages: [{ role: "user", content: "finalMessage-only" }],
      });
      const finalMsg = await stream2.finalMessage();
      assert.equal(finalMsg.stop_reason, "end_turn");
      assert.equal(await stream2.finalText(), "Streamed");
      stream2.abort();
      assert.equal(stream2.aborted, true);
    });

    await client.shutdown();
    const spans = batches.flatMap((b) => b.spans).filter((s) => s.span_phase === "final");
    assert.equal(spans.length, 3);
    assert.equal(spans[0].provider, "anthropic");
    assert.equal(spans[0].cached_tokens, 10);
    assert.equal(spans[1].events.some((e) => e.name === "first_token"), true);
    assert.deepEqual(spans[1].output, { content: "Streamed" });
    assert.equal(spans[2].input_tokens, 60);
    assert.equal(spans[2].output_tokens, 15);
    assert.equal(spans[2].cached_tokens, 20);
    assert.equal(spans[2].finish_reason, "end_turn");
    assert.deepEqual(spans[2].output, { content: "Streamed" });
  });

  it("wraps Gemini (@google/generative-ai and @google/genai shapes) and preserves partial chunk usage", async () => {
    const { client, batches } = createTestClient();
    const fakeGenAI = {
      getGenerativeModel({ model }) {
        return {
          model,
          async generateContent() {
            return {
              response: {
                text: () => "Gemini answer",
                candidates: [{ finishReason: "STOP" }],
                usageMetadata: {
                  promptTokenCount: 40,
                  candidatesTokenCount: 15,
                  cachedContentTokenCount: 5,
                  totalTokenCount: 55,
                },
              },
            };
          },
          async generateContentStream() {
            return {
              stream: {
                async *[Symbol.asyncIterator]() {
                  // Chunk 1 has cachedContentTokenCount + finishReason; Chunk 2 omits both.
                  yield {
                    text: () => "Part1 ",
                    candidates: [{ finishReason: "STOP" }],
                    usageMetadata: {
                      promptTokenCount: 100,
                      cachedContentTokenCount: 30,
                      candidatesTokenCount: 5,
                      totalTokenCount: 105,
                    },
                  };
                  yield {
                    text: () => "Part2",
                    usageMetadata: {
                      promptTokenCount: 100,
                      candidatesTokenCount: 12,
                    },
                  };
                },
              },
            };
          },
        };
      },
      models: {
        async embedContent() {
          return {
            usageMetadata: { promptTokenCount: 9 },
            embedding: { values: [0.1, 0.2] },
          };
        },
      },
    };

    const gemini = wrapGemini(fakeGenAI, client);
    await client.withTrace({ traceId: "tr_gem", agentName: "gem-agent" }, async () => {
      const m = gemini.getGenerativeModel({ model: "gemini-2.5-flash" });
      await m.generateContent("hi");
      const sRes = await m.generateContentStream("stream");
      for await (const _chunk of sRes.stream) {
        // consume
      }
      await gemini.models.embedContent({ model: "text-embedding-004", contents: "embed" });
    });

    await client.shutdown();
    const spans = batches.flatMap((b) => b.spans).filter((s) => s.span_phase === "final");
    assert.equal(spans.length, 3);
    assert.equal(spans[0].provider, "google");
    assert.equal(spans[0].input_tokens, 40);
    assert.equal(spans[1].events.some((e) => e.name === "first_token"), true);
    assert.deepEqual(spans[1].output, { content: "Part1 Part2" });
    assert.equal(spans[1].input_tokens, 100);
    assert.equal(spans[1].output_tokens, 12);
    assert.equal(spans[1].cached_tokens, 30);
    assert.equal(spans[1].total_tokens, 105);
    assert.equal(spans[1].finish_reason, "STOP");
    assert.equal(spans[2].span_type, "embedding");
  });

  it("tracks LangChain runId/parentRunId hierarchy and RAG/Guardrail helpers", async () => {
    const { client, batches } = createTestClient();
    const handler = createLangChainCallbackHandler(client);

    await client.withTrace({ traceId: "tr_lc", agentName: "lc-agent" }, async () => {
      handler.handleChainStart({ name: "AgentChain" }, { q: "test" }, "run_chain");
      handler.handleRetrieverStart({ name: "VectorStore" }, "test", "run_ret", "run_chain");
      handler.handleRetrieverEnd([{ pageContent: "doc1" }], "run_ret");
      handler.handleLLMStart({ name: "ChatOpenAI" }, ["prompt"], "run_llm", "run_chain", {
        invocation_params: { model_name: "gpt-4o" },
      });
      handler.handleLLMNewToken("hi", 0, "run_llm");
      handler.handleLLMEnd({ llmOutput: { tokenUsage: { promptTokens: 20, completionTokens: 5 } } }, "run_llm");
      handler.handleChainEnd({ answer: "done" }, "run_chain");

      const retriever = wrapRetriever(
        {
          async retrieve(q) {
            return [{ id: "d1", score: 0.91, content: `result for ${q}` }];
          },
        },
        client,
      );
      await retriever.retrieve("hello");
      await traceRag(client, { query: "q2", topK: 3 }, async () => [{ id: "d2", content: "c2" }]);
      await traceEmbedding(client, { model: "text-embedding-3-small", provider: "openai", input: "abc" }, async () => [0.1]);
      await traceGuardrail(
        client,
        { guardrailName: "pii_check", linkedSpanIds: ["run_llm"], input: "safe" },
        async () => ({ passed: true, action: "allow" }),
      );
    });

    await client.shutdown();
    const spans = batches.flatMap((b) => b.spans);
    const llmSpan = spans.find((s) => s.span_id === "run_llm");
    assert.equal(llmSpan.parent_span_id, "run_chain");
    assert.equal(llmSpan.events.some((e) => e.name === "first_token"), true);
    const guardSpan = spans.find((s) => s.span_type === "guardrail");
    assert.deepEqual(guardSpan.linked_span_ids, ["run_llm"]);
    assert.equal(guardSpan.attributes["guardrail.passed"], true);
  });
});
