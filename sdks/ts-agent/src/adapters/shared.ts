import type { PulseAgentClient } from "../client.js";

export interface AdapterOptions {
  spanName?: string;
  attributes?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  emitStart?: boolean;
}

export function isAsyncIterable<T = unknown>(val: unknown): val is AsyncIterable<T> {
  return (
    val !== null &&
    typeof val === "object" &&
    typeof (val as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
  );
}

export function isPromiseLike<T = unknown>(val: unknown): val is PromiseLike<T> {
  return (
    val !== null &&
    (typeof val === "object" || typeof val === "function") &&
    typeof (val as { then?: unknown }).then === "function"
  );
}

export type PulseClientSource = PulseAgentClient;
