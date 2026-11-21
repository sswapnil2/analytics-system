import type { PulseAgentClient, SpanHandle } from "../client.js";
import { isAsyncIterable, type AdapterOptions } from "./shared.js";

export interface OpenAICompletionsLike {
  create(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
}

export interface OpenAIClientLike {
  chat?: {
    completions?: OpenAICompletionsLike;
  };
  responses?: {
    create(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
  };
  embeddings?: {
    create(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
  };
}

interface OpenAIUsage {
  prompt_tokens?: number;
  completion_tokens?: number;
  total_tokens?: number;
  input_tokens?: number;
  output_tokens?: number;
  prompt_tokens_details?: { cached_tokens?: number };
  input_tokens_details?: { cached_tokens?: number };
}

function extractOpenAIUsage(usage: OpenAIUsage | undefined): {
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  totalTokens?: number;
} {
  if (!usage || typeof usage !== "object") return {};
  const inputTokens = usage.prompt_tokens ?? usage.input_tokens;
  const outputTokens = usage.completion_tokens ?? usage.output_tokens;
  const cachedTokens =
    usage.prompt_tokens_details?.cached_tokens ??
    usage.input_tokens_details?.cached_tokens;
  return {
    inputTokens: typeof inputTokens === "number" ? inputTokens : undefined,
    outputTokens: typeof outputTokens === "number" ? outputTokens : undefined,
    cachedTokens: typeof cachedTokens === "number" ? cachedTokens : undefined,
    totalTokens: typeof usage.total_tokens === "number" ? usage.total_tokens : undefined,
  };
}

function wrapOpenAIStream(
  stream: AsyncIterable<Record<string, unknown>>,
  span: SpanHandle,
  startTs: number,
): AsyncIterable<Record<string, unknown>> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<Record<string, unknown>> {
      const inner = stream[Symbol.asyncIterator]();
      let sawFirstToken = false;
      let textOut = "";
      let finishReason: string | undefined;
      let model: string | undefined;
      let usage: OpenAIUsage | undefined;
      let finalized = false;

      const finalize = (err?: unknown): void => {
        if (finalized) return;
        finalized = true;
        const u = extractOpenAIUsage(usage);
        span.end({
          model,
          finishReason,
          output: textOut.length > 0 ? { content: textOut } : undefined,
          ...u,
          error: err,
        });
      };

      return {
        async next(): Promise<IteratorResult<Record<string, unknown>>> {
          try {
            const res = await inner.next();
            if (res.done) {
              finalize();
              return res;
            }
            const chunk = res.value;
            if (chunk && typeof chunk === "object") {
              if (typeof chunk.model === "string") {
                model = chunk.model;
              }
              if (chunk.usage && typeof chunk.usage === "object") {
                usage = chunk.usage as OpenAIUsage;
              }
              const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
              const first = choices[0] as Record<string, unknown> | undefined;
              if (first) {
                if (typeof first.finish_reason === "string") {
                  finishReason = first.finish_reason;
                }
                const delta = first.delta as Record<string, unknown> | undefined;
                const content = delta?.content;
                if (typeof content === "string" && content.length > 0) {
                  if (!sawFirstToken) {
                    sawFirstToken = true;
                    const now = Date.now();
                    span.addEvent("first_token", { ttft_ms: Math.max(0, now - startTs) }, now);
                  }
                  textOut += content;
                } else if (!sawFirstToken && Array.isArray(delta?.tool_calls) && delta.tool_calls.length > 0) {
                  sawFirstToken = true;
                  const now = Date.now();
                  span.addEvent("first_token", { ttft_ms: Math.max(0, now - startTs) }, now);
                }
              } else if (chunk.type === "response.output_text.delta" && typeof chunk.delta === "string") {
                if (!sawFirstToken) {
                  sawFirstToken = true;
                  const now = Date.now();
                  span.addEvent("first_token", { ttft_ms: Math.max(0, now - startTs) }, now);
                }
                textOut += chunk.delta;
              } else if (chunk.type === "response.completed" && chunk.response && typeof chunk.response === "object") {
                const resp = chunk.response as Record<string, unknown>;
                if (resp.usage && typeof resp.usage === "object") {
                  usage = resp.usage as OpenAIUsage;
                }
                if (typeof resp.model === "string") {
                  model = resp.model;
                }
              }
            }
            return res;
          } catch (err) {
            finalize(err);
            throw err;
          }
        },
        async return(value?: unknown): Promise<IteratorResult<Record<string, unknown>>> {
          finalize();
          if (typeof inner.return === "function") {
            return inner.return(value);
          }
          return { value: undefined as unknown as Record<string, unknown>, done: true };
        },
        async throw(err?: unknown): Promise<IteratorResult<Record<string, unknown>>> {
          finalize(err);
          if (typeof inner.throw === "function") {
            return inner.throw(err);
          }
          throw err;
        },
      };
    },
  };
}

export function wrapOpenAI<T extends OpenAIClientLike>(
  openaiClient: T,
  pulseClient: PulseAgentClient,
  options: AdapterOptions = {},
): T {
  const wrapCreate = (
    targetObj: object,
    origCreate: (params: Record<string, unknown>, ...rest: unknown[]) => Promise<unknown>,
    spanType: "llm" | "embedding",
    defaultName: string,
  ) => {
    return async (params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown> => {
      const startTs = Date.now();
      const model = typeof params?.model === "string" ? params.model : undefined;
      const temperature =
        typeof params?.temperature === "number" ? params.temperature : undefined;
      const span = pulseClient.span({
        name: options.spanName ?? defaultName,
        spanType,
        provider: "openai",
        model,
        temperature,
        startTsMs: startTs,
        input: params?.messages ?? params?.input ?? params,
        modelParams: {
          temperature: params?.temperature,
          max_tokens: params?.max_tokens ?? params?.max_output_tokens,
          top_p: params?.top_p,
        },
        attributes: {
          "gen_ai.system": "openai",
          ...(model ? { "gen_ai.request.model": model } : {}),
          ...options.attributes,
        },
        metadata: options.metadata,
        emitStart: options.emitStart ?? Boolean(params?.stream),
      });

      try {
        const res = await origCreate.call(targetObj, params, ...rest);
        if (isAsyncIterable<Record<string, unknown>>(res)) {
          return wrapOpenAIStream(res, span, startTs);
        }
        const obj = res as Record<string, unknown> | null | undefined;
        const usage = extractOpenAIUsage(obj?.usage as OpenAIUsage | undefined);
        const choices = Array.isArray(obj?.choices) ? obj.choices : [];
        const firstChoice = choices[0] as Record<string, unknown> | undefined;
        const finishReason =
          typeof firstChoice?.finish_reason === "string"
            ? firstChoice.finish_reason
            : undefined;
        const respModel = typeof obj?.model === "string" ? obj.model : model;
        span.end({
          model: respModel,
          finishReason,
          output: obj ?? res,
          ...usage,
        });
        return res;
      } catch (err) {
        span.end({ error: err });
        throw err;
      }
    };
  };

  return new Proxy(openaiClient, {
    get(target, prop, receiver) {
      if (prop === "chat" && target.chat?.completions) {
        const chatTarget = target.chat;
        return new Proxy(chatTarget, {
          get(cTarget, cProp, cRecv) {
            if (cProp === "completions" && cTarget.completions) {
              const compTarget = cTarget.completions;
              return new Proxy(compTarget, {
                get(mTarget, mProp, mRecv) {
                  if (mProp === "create" && typeof mTarget.create === "function") {
                    return wrapCreate(
                      mTarget,
                      mTarget.create,
                      "llm",
                      "openai.chat.completions",
                    );
                  }
                  const val = Reflect.get(mTarget, mProp, mRecv);
                  return typeof val === "function" ? val.bind(mTarget) : val;
                },
              });
            }
            const val = Reflect.get(cTarget, cProp, cRecv);
            return typeof val === "function" ? val.bind(cTarget) : val;
          },
        });
      }
      if (prop === "responses" && target.responses) {
        const respTarget = target.responses;
        return new Proxy(respTarget, {
          get(rTarget, rProp, rRecv) {
            if (rProp === "create" && typeof rTarget.create === "function") {
              return wrapCreate(rTarget, rTarget.create, "llm", "openai.responses");
            }
            const val = Reflect.get(rTarget, rProp, rRecv);
            return typeof val === "function" ? val.bind(rTarget) : val;
          },
        });
      }
      if (prop === "embeddings" && target.embeddings) {
        const embTarget = target.embeddings;
        return new Proxy(embTarget, {
          get(eTarget, eProp, eRecv) {
            if (eProp === "create" && typeof eTarget.create === "function") {
              return wrapCreate(eTarget, eTarget.create, "embedding", "openai.embeddings");
            }
            const val = Reflect.get(eTarget, eProp, eRecv);
            return typeof val === "function" ? val.bind(eTarget) : val;
          },
        });
      }
      const val = Reflect.get(target, prop, receiver);
      return typeof val === "function" ? val.bind(target) : val;
    },
  });
}
