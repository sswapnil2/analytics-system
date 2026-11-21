import type { PulseAgentClient, SpanHandle } from "../client.js";
import type { AdapterOptions } from "./shared.js";

export interface RagDocument {
  id?: string;
  score?: number;
  content?: string;
  metadata?: Record<string, unknown>;
}

export interface RagTraceOptions extends AdapterOptions {
  query: string;
  topK?: number;
  indexName?: string;
}

export interface EmbeddingTraceOptions extends AdapterOptions {
  model?: string;
  provider?: string;
  input: unknown;
}

export interface GuardrailTraceOptions extends AdapterOptions {
  guardrailName: string;
  linkedSpanIds?: string[];
  input?: unknown;
}

export async function traceRag<T extends RagDocument[] | { documents?: RagDocument[] }>(
  pulseClient: PulseAgentClient,
  options: RagTraceOptions,
  fn: (span: SpanHandle) => Promise<T> | T,
): Promise<T> {
  return pulseClient.withRetrieval(
    {
      name: options.spanName ?? "rag.retrieve",
      input: {
        query: options.query,
        ...(options.topK !== undefined ? { topK: options.topK } : {}),
        ...(options.indexName !== undefined ? { indexName: options.indexName } : {}),
      },
      attributes: {
        "rag.query": options.query,
        ...(options.topK !== undefined ? { "rag.top_k": options.topK } : {}),
        ...(options.indexName !== undefined ? { "rag.index": options.indexName } : {}),
        ...options.attributes,
      },
      metadata: options.metadata,
      emitStart: options.emitStart,
    },
    async (span) => {
      const res = await fn(span);
      const docs = Array.isArray(res) ? res : res?.documents ?? [];
      span.end({
        output: {
          documentsCount: docs.length,
          documents: docs.map((d) => ({
            id: d.id,
            score: d.score,
            content: typeof d.content === "string" ? d.content.slice(0, 1024) : d.content,
            metadata: d.metadata,
          })),
        },
        attributes: {
          "rag.documents_count": docs.length,
        },
      });
      return res;
    },
  );
}

export interface RetrieverLike {
  retrieve?(query: string, ...rest: unknown[]): Promise<RagDocument[]>;
  getRelevantDocuments?(query: string, ...rest: unknown[]): Promise<RagDocument[]>;
}

export function wrapRetriever<T extends RetrieverLike>(
  retriever: T,
  pulseClient: PulseAgentClient,
  options: AdapterOptions = {},
): T {
  return new Proxy(retriever, {
    get(target, prop, receiver) {
      if (
        (prop === "retrieve" || prop === "getRelevantDocuments") &&
        typeof target[prop] === "function"
      ) {
        const orig = target[prop] as (query: string, ...rest: unknown[]) => Promise<RagDocument[]>;
        return (query: string, ...rest: unknown[]) =>
          traceRag(
            pulseClient,
            {
              ...options,
              spanName: options.spanName ?? `retriever.${String(prop)}`,
              query,
            },
            () => orig.call(target, query, ...rest),
          );
      }
      const val = Reflect.get(target, prop, receiver);
      return typeof val === "function" ? val.bind(target) : val;
    },
  });
}

export async function traceEmbedding<T>(
  pulseClient: PulseAgentClient,
  options: EmbeddingTraceOptions,
  fn: (span: SpanHandle) => Promise<T> | T,
): Promise<T> {
  return pulseClient.withEmbedding(
    {
      name: options.spanName ?? "embedding",
      provider: options.provider,
      model: options.model,
      input: options.input,
      attributes: options.attributes,
      metadata: options.metadata,
      emitStart: options.emitStart,
    },
    async (span) => {
      const res = await fn(span);
      span.end({ output: res });
      return res;
    },
  );
}

export interface GuardrailEvaluationResult {
  passed: boolean;
  action?: string;
  reason?: string;
  [key: string]: unknown;
}

export async function traceGuardrail<T extends GuardrailEvaluationResult | boolean>(
  pulseClient: PulseAgentClient,
  options: GuardrailTraceOptions,
  fn: (span: SpanHandle) => Promise<T> | T,
): Promise<T> {
  return pulseClient.withGuardrail(
    {
      name: options.spanName ?? options.guardrailName,
      linkedSpanIds: options.linkedSpanIds,
      input: options.input,
      attributes: {
        "pulse.guardrail.name": options.guardrailName,
        ...options.attributes,
      },
      metadata: options.metadata,
      emitStart: options.emitStart,
    },
    async (span) => {
      const res = await fn(span);
      const passed = typeof res === "boolean" ? res : Boolean(res.passed);
      const action =
        typeof res === "object" && res !== null && typeof res.action === "string"
          ? res.action
          : passed
            ? "allow"
            : "block";
      span.end({
        output: res,
        attributes: {
          "guardrail.name": options.guardrailName,
          "guardrail.passed": passed,
          "guardrail.action": action,
        },
      });
      return res;
    },
  );
}
