import type { PulseAgentClient, SpanHandle } from "../client.js";
import { isAsyncIterable, type AdapterOptions } from "./shared.js";

export interface AnthropicMessagesLike {
  create(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
  stream?(params: Record<string, unknown>, ...rest: unknown[]): AsyncIterable<unknown>;
}

export interface AnthropicClientLike {
  messages?: AnthropicMessagesLike;
}

interface AnthropicUsage {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_input_tokens?: number;
}

function wrapAnthropicStream<S extends AsyncIterable<Record<string, unknown>>>(
  stream: S,
  span: SpanHandle,
  startTs: number,
  fallbackModel?: string,
): S {
  let sawFirstToken = false;
  let textOut = "";
  let model = fallbackModel;
  let stopReason: string | undefined;
  let inputTokens = 0;
  let outputTokens = 0;
  let cachedTokens = 0;
  let finalized = false;

  const finalize = (err?: unknown): void => {
    if (finalized) return;
    finalized = true;
    span.end({
      model,
      finishReason: stopReason,
      inputTokens: inputTokens > 0 ? inputTokens : undefined,
      outputTokens: outputTokens > 0 ? outputTokens : undefined,
      cachedTokens: cachedTokens > 0 ? cachedTokens : undefined,
      output: textOut.length > 0 ? { content: textOut } : undefined,
      error: err,
    });
  };

  const handleEvent = (ev: Record<string, unknown> | null | undefined): void => {
    if (!ev || typeof ev !== "object") return;
    if (ev.type === "message_start" && ev.message && typeof ev.message === "object") {
      const msg = ev.message as Record<string, unknown>;
      if (typeof msg.model === "string") model = msg.model;
      const u = msg.usage as AnthropicUsage | undefined;
      if (u) {
        if (typeof u.input_tokens === "number") inputTokens = u.input_tokens;
        if (typeof u.output_tokens === "number") outputTokens = u.output_tokens;
        if (typeof u.cache_read_input_tokens === "number") {
          cachedTokens = u.cache_read_input_tokens;
        }
      }
    } else if (ev.type === "content_block_delta" && ev.delta && typeof ev.delta === "object") {
      const d = ev.delta as Record<string, unknown>;
      const chunkText =
        typeof d.text === "string"
          ? d.text
          : typeof d.partial_json === "string"
            ? d.partial_json
            : "";
      if (chunkText.length > 0) {
        if (!sawFirstToken) {
          sawFirstToken = true;
          const now = Date.now();
          span.addEvent("first_token", { ttft_ms: Math.max(0, now - startTs) }, now);
        }
        textOut += chunkText;
      }
    } else if (ev.type === "message_delta") {
      const d = ev.delta as Record<string, unknown> | undefined;
      if (typeof d?.stop_reason === "string") stopReason = d.stop_reason;
      const u = ev.usage as AnthropicUsage | undefined;
      if (u && typeof u.output_tokens === "number") {
        outputTokens = u.output_tokens;
      }
    }
  };

  const proxy: S = new Proxy(stream, {
    get(target, prop) {
      if (prop === Symbol.asyncIterator) {
        return (): AsyncIterator<Record<string, unknown>> => {
          const inner = target[Symbol.asyncIterator]();
          return {
            async next(): Promise<IteratorResult<Record<string, unknown>>> {
              try {
                const res = await inner.next();
                if (res.done) {
                  finalize();
                  return res;
                }
                handleEvent(res.value);
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
        };
      }

      if (
        prop === "finalMessage" &&
        typeof (target as unknown as Record<string, unknown>).finalMessage === "function"
      ) {
        const orig = (target as unknown as { finalMessage: (...a: unknown[]) => Promise<unknown> })
          .finalMessage;
        return async (...args: unknown[]): Promise<unknown> => {
          try {
            const msg = await orig.apply(target, args);
            if (!finalized && msg && typeof msg === "object") {
              const obj = msg as Record<string, unknown>;
              if (typeof obj.model === "string") model = obj.model;
              if (typeof obj.stop_reason === "string") stopReason = obj.stop_reason;
              const u = obj.usage as AnthropicUsage | undefined;
              if (u) {
                if (typeof u.input_tokens === "number") inputTokens = u.input_tokens;
                if (typeof u.output_tokens === "number") outputTokens = u.output_tokens;
                if (typeof u.cache_read_input_tokens === "number") {
                  cachedTokens = u.cache_read_input_tokens;
                }
              }
              if (!textOut && Array.isArray(obj.content)) {
                let txt = "";
                for (const block of obj.content) {
                  if (
                    block &&
                    typeof block === "object" &&
                    typeof (block as Record<string, unknown>).text === "string"
                  ) {
                    txt += (block as Record<string, unknown>).text as string;
                  }
                }
                if (txt.length > 0) textOut = txt;
              }
            }
            finalize();
            return msg;
          } catch (err) {
            finalize(err);
            throw err;
          }
        };
      }

      if (
        prop === "finalText" &&
        typeof (target as unknown as Record<string, unknown>).finalText === "function"
      ) {
        const orig = (target as unknown as { finalText: (...a: unknown[]) => Promise<unknown> })
          .finalText;
        return async (...args: unknown[]): Promise<unknown> => {
          try {
            const txt = await orig.apply(target, args);
            if (!finalized && typeof txt === "string" && !textOut) {
              textOut = txt;
            }
            finalize();
            return txt;
          } catch (err) {
            finalize(err);
            throw err;
          }
        };
      }

      const val = Reflect.get(target, prop, target);
      if (typeof val === "function") {
        return (...args: unknown[]): unknown => {
          const res = (val as (...a: unknown[]) => unknown).apply(target, args);
          return res === target ? proxy : res;
        };
      }
      return val;
    },
  });

  return proxy;
}

export function wrapAnthropic<T extends AnthropicClientLike>(
  anthropicClient: T,
  pulseClient: PulseAgentClient,
  options: AdapterOptions = {},
): T {
  const createSpan = (params: Record<string, unknown>, startTs: number): SpanHandle => {
    const model = typeof params?.model === "string" ? params.model : undefined;
    return pulseClient.span({
      name: options.spanName ?? "anthropic.messages",
      spanType: "llm",
      provider: "anthropic",
      model,
      temperature: typeof params?.temperature === "number" ? params.temperature : undefined,
      startTsMs: startTs,
      input: {
        ...(params?.system !== undefined ? { system: params.system } : {}),
        messages: params?.messages,
      },
      modelParams: {
        max_tokens: params?.max_tokens,
        temperature: params?.temperature,
        top_p: params?.top_p,
      },
      attributes: {
        "gen_ai.system": "anthropic",
        ...(model ? { "gen_ai.request.model": model } : {}),
        ...options.attributes,
      },
      metadata: options.metadata,
      emitStart: options.emitStart ?? Boolean(params?.stream),
    });
  };

  return new Proxy(anthropicClient, {
    get(target, prop, receiver) {
      if (prop === "messages" && target.messages) {
        const msgTarget = target.messages;
        return new Proxy(msgTarget, {
          get(mTarget, mProp, mRecv) {
            if (mProp === "create" && typeof mTarget.create === "function") {
              return async (params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown> => {
                const startTs = Date.now();
                const model = typeof params?.model === "string" ? params.model : undefined;
                const span = createSpan(params, startTs);
                try {
                  const res = await mTarget.create.call(mTarget, params, ...rest);
                  if (isAsyncIterable<Record<string, unknown>>(res)) {
                    return wrapAnthropicStream(res, span, startTs, model);
                  }
                  const obj = res as Record<string, unknown> | null | undefined;
                  const usage = obj?.usage as AnthropicUsage | undefined;
                  span.end({
                    model: typeof obj?.model === "string" ? obj.model : model,
                    finishReason:
                      typeof obj?.stop_reason === "string" ? obj.stop_reason : undefined,
                    inputTokens: usage?.input_tokens,
                    outputTokens: usage?.output_tokens,
                    cachedTokens: usage?.cache_read_input_tokens,
                    output: obj ?? res,
                  });
                  return res;
                } catch (err) {
                  span.end({ error: err });
                  throw err;
                }
              };
            }
            if (mProp === "stream" && typeof mTarget.stream === "function") {
              return (params: Record<string, unknown>, ...rest: unknown[]): unknown => {
                const startTs = Date.now();
                const model = typeof params?.model === "string" ? params.model : undefined;
                const span = createSpan({ ...params, stream: true }, startTs);
                try {
                  const streamObj = mTarget.stream!.call(mTarget, params, ...rest);
                  if (isAsyncIterable<Record<string, unknown>>(streamObj)) {
                    return wrapAnthropicStream(streamObj, span, startTs, model);
                  }
                  return streamObj;
                } catch (err) {
                  span.end({ error: err });
                  throw err;
                }
              };
            }
            const val = Reflect.get(mTarget, mProp, mRecv);
            return typeof val === "function" ? val.bind(mTarget) : val;
          },
        });
      }
      const val = Reflect.get(target, prop, receiver);
      return typeof val === "function" ? val.bind(target) : val;
    },
  });
}
