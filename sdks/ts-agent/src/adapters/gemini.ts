import type { PulseAgentClient, SpanHandle } from "../client.js";
import { isAsyncIterable, type AdapterOptions } from "./shared.js";

interface GeminiUsageMetadata {
  promptTokenCount?: number;
  candidatesTokenCount?: number;
  cachedContentTokenCount?: number;
  totalTokenCount?: number;
}

export interface GeminiClientLike {
  // @google/generative-ai
  getGenerativeModel?(params: Record<string, unknown>, ...rest: unknown[]): unknown;
  // @google/genai
  models?: {
    generateContent?(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
    generateContentStream?(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
    embedContent?(params: Record<string, unknown>, ...rest: unknown[]): Promise<unknown>;
  };
}

function extractGeminiText(res: unknown): string {
  if (!res || typeof res !== "object") return "";
  const obj = res as Record<string, unknown>;
  if (typeof obj.text === "string") return obj.text;
  if (typeof obj.text === "function") {
    try {
      const val = (obj.text as () => unknown)();
      if (typeof val === "string") return val;
    } catch {
      // Ignore if text() throws on tool-only responses.
    }
  }
  const respObj =
    obj.response && typeof obj.response === "object"
      ? (obj.response as Record<string, unknown>)
      : obj;
  const candidates = Array.isArray(respObj.candidates) ? respObj.candidates : [];
  const first = candidates[0] as Record<string, unknown> | undefined;
  const content = first?.content as Record<string, unknown> | undefined;
  const parts = Array.isArray(content?.parts) ? content.parts : [];
  let text = "";
  for (const p of parts) {
    if (p && typeof p === "object" && typeof (p as Record<string, unknown>).text === "string") {
      text += (p as Record<string, unknown>).text as string;
    }
  }
  return text;
}

function extractGeminiUsage(res: unknown): {
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  totalTokens?: number;
  finishReason?: string;
} {
  if (!res || typeof res !== "object") return {};
  const obj = res as Record<string, unknown>;
  const root =
    obj.response && typeof obj.response === "object"
      ? (obj.response as Record<string, unknown>)
      : obj;
  const u = root.usageMetadata as GeminiUsageMetadata | undefined;
  const candidates = Array.isArray(root.candidates) ? root.candidates : [];
  const first = candidates[0] as Record<string, unknown> | undefined;
  const finishReason =
    typeof first?.finishReason === "string" ? first.finishReason : undefined;
  return {
    inputTokens: typeof u?.promptTokenCount === "number" ? u.promptTokenCount : undefined,
    outputTokens: typeof u?.candidatesTokenCount === "number" ? u.candidatesTokenCount : undefined,
    cachedTokens:
      typeof u?.cachedContentTokenCount === "number" ? u.cachedContentTokenCount : undefined,
    totalTokens: typeof u?.totalTokenCount === "number" ? u.totalTokenCount : undefined,
    finishReason,
  };
}

function wrapGeminiStream(
  stream: AsyncIterable<unknown>,
  span: SpanHandle,
  startTs: number,
): AsyncIterable<unknown> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<unknown> {
      const inner = stream[Symbol.asyncIterator]();
      let sawFirstToken = false;
      let textOut = "";
      let lastUsage: ReturnType<typeof extractGeminiUsage> = {};
      let finalized = false;

      const finalize = (err?: unknown): void => {
        if (finalized) return;
        finalized = true;
        span.end({
          ...lastUsage,
          output: textOut.length > 0 ? { content: textOut } : undefined,
          error: err,
        });
      };

      return {
        async next(): Promise<IteratorResult<unknown>> {
          try {
            const res = await inner.next();
            if (res.done) {
              finalize();
              return res;
            }
            const chunkText = extractGeminiText(res.value);
            if (chunkText.length > 0) {
              if (!sawFirstToken) {
                sawFirstToken = true;
                const now = Date.now();
                span.addEvent("first_token", { ttft_ms: Math.max(0, now - startTs) }, now);
              }
              textOut += chunkText;
            }
            const u = extractGeminiUsage(res.value);
            if (u.inputTokens !== undefined) {
              lastUsage.inputTokens = u.inputTokens;
            }
            if (u.outputTokens !== undefined) {
              lastUsage.outputTokens = u.outputTokens;
            }
            if (u.cachedTokens !== undefined) {
              lastUsage.cachedTokens = u.cachedTokens;
            }
            if (u.totalTokens !== undefined) {
              lastUsage.totalTokens = u.totalTokens;
            }
            if (u.finishReason !== undefined) {
              lastUsage.finishReason = u.finishReason;
            }
            return res;
          } catch (err) {
            finalize(err);
            throw err;
          }
        },
        async return(value?: unknown): Promise<IteratorResult<unknown>> {
          finalize();
          if (typeof inner.return === "function") {
            return inner.return(value);
          }
          return { value: undefined, done: true };
        },
        async throw(err?: unknown): Promise<IteratorResult<unknown>> {
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

export function wrapGemini<T extends GeminiClientLike>(
  geminiClient: T,
  pulseClient: PulseAgentClient,
  options: AdapterOptions = {},
): T {
  const wrapModelMethods = (
    modelObj: Record<string, unknown>,
    defaultModelName?: string,
  ): Record<string, unknown> => {
    return new Proxy(modelObj, {
      get(target, prop, receiver) {
        if (prop === "generateContent" && typeof target.generateContent === "function") {
          const orig = target.generateContent as (...args: unknown[]) => Promise<unknown>;
          return async (req: unknown, ...rest: unknown[]): Promise<unknown> => {
            const reqObj = req && typeof req === "object" ? (req as Record<string, unknown>) : undefined;
            const model =
              (typeof reqObj?.model === "string" ? reqObj.model : undefined) ??
              defaultModelName;
            const span = pulseClient.span({
              name: options.spanName ?? "gemini.generateContent",
              spanType: "llm",
              provider: "google",
              model,
              input: reqObj?.contents ?? req,
              attributes: {
                "gen_ai.system": "google",
                ...(model ? { "gen_ai.request.model": model } : {}),
                ...options.attributes,
              },
              metadata: options.metadata,
              emitStart: options.emitStart,
            });
            try {
              const res = await orig.call(target, req, ...rest);
              const u = extractGeminiUsage(res);
              span.end({
                ...u,
                output: res,
              });
              return res;
            } catch (err) {
              span.end({ error: err });
              throw err;
            }
          };
        }

        if (
          prop === "generateContentStream" &&
          typeof target.generateContentStream === "function"
        ) {
          const orig = target.generateContentStream as (...args: unknown[]) => Promise<unknown>;
          return async (req: unknown, ...rest: unknown[]): Promise<unknown> => {
            const startTs = Date.now();
            const reqObj = req && typeof req === "object" ? (req as Record<string, unknown>) : undefined;
            const model =
              (typeof reqObj?.model === "string" ? reqObj.model : undefined) ??
              defaultModelName;
            const span = pulseClient.span({
              name: options.spanName ?? "gemini.generateContentStream",
              spanType: "llm",
              provider: "google",
              model,
              startTsMs: startTs,
              input: reqObj?.contents ?? req,
              attributes: {
                "gen_ai.system": "google",
                ...(model ? { "gen_ai.request.model": model } : {}),
                ...options.attributes,
              },
              metadata: options.metadata,
              emitStart: options.emitStart ?? true,
            });
            try {
              const res = await orig.call(target, req, ...rest);
              if (isAsyncIterable(res)) {
                return wrapGeminiStream(res, span, startTs);
              }
              const resObj = res as Record<string, unknown> | null | undefined;
              if (resObj && isAsyncIterable(resObj.stream)) {
                const wrappedStream = wrapGeminiStream(resObj.stream, span, startTs);
                return { ...resObj, stream: wrappedStream };
              }
              const u = extractGeminiUsage(res);
              span.end({ ...u, output: res });
              return res;
            } catch (err) {
              span.end({ error: err });
              throw err;
            }
          };
        }

        if (prop === "embedContent" && typeof target.embedContent === "function") {
          const orig = target.embedContent as (...args: unknown[]) => Promise<unknown>;
          return async (req: unknown, ...rest: unknown[]): Promise<unknown> => {
            const reqObj = req && typeof req === "object" ? (req as Record<string, unknown>) : undefined;
            const model =
              (typeof reqObj?.model === "string" ? reqObj.model : undefined) ??
              defaultModelName;
            const span = pulseClient.span({
              name: options.spanName ?? "gemini.embedContent",
              spanType: "embedding",
              provider: "google",
              model,
              input: reqObj?.contents ?? reqObj?.content ?? req,
              attributes: {
                "gen_ai.system": "google",
                ...(model ? { "gen_ai.request.model": model } : {}),
                ...options.attributes,
              },
              metadata: options.metadata,
            });
            try {
              const res = await orig.call(target, req, ...rest);
              const u = extractGeminiUsage(res);
              span.end({ ...u, output: res });
              return res;
            } catch (err) {
              span.end({ error: err });
              throw err;
            }
          };
        }

        const val = Reflect.get(target, prop, receiver);
        return typeof val === "function" ? val.bind(target) : val;
      },
    });
  };

  return new Proxy(geminiClient, {
    get(target, prop, receiver) {
      if (prop === "getGenerativeModel" && typeof target.getGenerativeModel === "function") {
        return (modelParams: Record<string, unknown>, ...rest: unknown[]): unknown => {
          const modelObj = target.getGenerativeModel!(modelParams, ...rest);
          if (modelObj && typeof modelObj === "object") {
            const modelName =
              typeof modelParams?.model === "string" ? modelParams.model : undefined;
            return wrapModelMethods(modelObj as Record<string, unknown>, modelName);
          }
          return modelObj;
        };
      }
      if (prop === "models" && target.models) {
        return wrapModelMethods(target.models as Record<string, unknown>);
      }
      const val = Reflect.get(target, prop, receiver);
      return typeof val === "function" ? val.bind(target) : val;
    },
  });
}
