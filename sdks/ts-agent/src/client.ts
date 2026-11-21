import { ContextManager } from "./context.js";
import { encodePlstBatch, supportsZstd } from "./plst.js";
import { costNanosToUsd, estimateCostNanos, usdToCostNanos } from "./pricing.js";
import {
  resolvePrivacyPolicy,
  sanitizeBatch,
  toJsonValue,
  type ResolvedPrivacyPolicy,
} from "./privacy.js";
import { RingBuffer } from "./ring_buffer.js";
import { clampSampleRate, shouldSample } from "./sampling.js";
import type {
  AgentSpan,
  AgentTrace,
  ClientStats,
  FailureSource,
  FetchLike,
  ModelPricingRule,
  OverflowPolicy,
  PlstCompressionMode,
  PlstFallbackMode,
  PulseAgentConfig,
  PulseContext,
  SamplingMode,
  SessionOptions,
  SpanEndOptions,
  SpanEvent,
  SpanOptions,
  SpanPhase,
  SpanType,
  SpanUpdateOptions,
  TraceEndOptions,
  TraceIngestBatch,
  TraceIngestResponse,
  TraceOptions,
  TraceStatus,
  TraceUpdateOptions,
  TransportMode,
} from "./types.js";

const DEFAULT_MAX_QUEUE_SIZE = 2048;
const DEFAULT_FLUSH_BATCH_SIZE = 64;
const DEFAULT_MAX_BATCH_BYTES = 2 * 1024 * 1024;
const HARD_MAX_BODY_BYTES = 8 * 1024 * 1024 - 1024;
const DEFAULT_FLUSH_INTERVAL_MS = 5000;
const DEFAULT_FETCH_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_BASE_MS = 250;
const DEFAULT_RETRY_MAX_MS = 30_000;
const MAX_RETRY_AFTER_MS = 10 * 60_000;

interface TimerLike {
  unref?: () => void;
}

type QueueItem =
  | { kind: "trace"; data: AgentTrace }
  | { kind: "span"; data: AgentSpan };

interface EncodedPayload {
  body: Uint8Array | string;
  byteLength: number;
  contentType: string;
}

function generateId(prefix: string): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string; getRandomValues?: (arr: Uint8Array) => Uint8Array } }).crypto;
  if (c && typeof c.randomUUID === "function") {
    return `${prefix}_${c.randomUUID().replace(/-/g, "")}`;
  }
  const bytes = new Uint8Array(16);
  if (c && typeof c.getRandomValues === "function") {
    c.getRandomValues(bytes);
  } else {
    for (let i = 0; i < 16; i++) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
  }
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  let hex = "";
  for (let i = 0; i < 16; i++) {
    hex += bytes[i]!.toString(16).padStart(2, "0");
  }
  return `${prefix}_${hex}`;
}

function errorToDetails(err: unknown): {
  errorType: string;
  errorMessage: string;
  errorStack: string | null;
} {
  if (err instanceof Error) {
    return {
      errorType: err.name || "Error",
      errorMessage: err.message || String(err),
      errorStack: err.stack ?? null,
    };
  }
  return {
    errorType: "Error",
    errorMessage: typeof err === "string" ? err : JSON.stringify(err),
    errorStack: null,
  };
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

function parseRetryAfterMs(headerValue: string | null): number | null {
  if (!headerValue) return null;
  const trimmed = headerValue.trim();
  if (trimmed.length === 0) return null;
  const num = Number(trimmed);
  if (Number.isFinite(num) && num >= 0) {
    return Math.min(MAX_RETRY_AFTER_MS, Math.round(num * 1000));
  }
  const dateMs = Date.parse(trimmed);
  if (Number.isFinite(dateMs)) {
    const delta = Math.max(0, dateMs - Date.now());
    return Math.min(MAX_RETRY_AFTER_MS, delta);
  }
  return null;
}

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function cloneTrace(t: AgentTrace): AgentTrace {
  return {
    ...t,
    tags: [...t.tags],
  };
}

function cloneSpan(s: AgentSpan): AgentSpan {
  return {
    ...s,
    linked_span_ids: [...s.linked_span_ids],
    events: s.events.map((ev) => ({ ...ev })),
  };
}

export class SessionHandle {
  readonly sessionId: string;
  readonly userId: string | undefined;
  readonly environment: string | undefined;
  readonly agentName: string | undefined;
  readonly agentVersion: string | undefined;
  readonly promptVersion: string | undefined;
  readonly modelConfigVersion: string | undefined;
  readonly toolConfigVersion: string | undefined;
  readonly appRelease: string | undefined;
  readonly workflowId: string | undefined;
  readonly sampled: boolean | undefined;

  private readonly client: PulseAgentClient;

  constructor(client: PulseAgentClient, options: SessionOptions = {}) {
    this.client = client;
    this.sessionId = options.sessionId ?? options.id ?? generateId("sess");
    this.userId = options.userId;
    this.environment = options.environment;
    this.agentName = options.agentName;
    this.agentVersion = options.agentVersion;
    this.promptVersion = options.promptVersion;
    this.modelConfigVersion = options.modelConfigVersion;
    this.toolConfigVersion = options.toolConfigVersion;
    this.appRelease = options.appRelease;
    this.workflowId = options.workflowId;
    this.sampled = options.sampled;
  }

  toContextPatch(): Partial<PulseContext> {
    return {
      sessionId: this.sessionId,
      userId: this.userId,
      environment: this.environment,
      agentName: this.agentName,
      agentVersion: this.agentVersion,
      promptVersion: this.promptVersion,
      modelConfigVersion: this.modelConfigVersion,
      toolConfigVersion: this.toolConfigVersion,
      appRelease: this.appRelease,
      workflowId: this.workflowId,
      sampled: this.sampled,
    };
  }

  trace(options: TraceOptions = {}): TraceHandle {
    return this.client.trace({
      ...this.toContextPatch(),
      ...options,
      sessionId: options.sessionId ?? this.sessionId,
    });
  }

  withTrace<R>(
    optionsOrFn: TraceOptions | ((trace: TraceHandle) => R),
    maybeFn?: (trace: TraceHandle) => R,
  ): R {
    const opts = typeof optionsOrFn === "function" ? {} : optionsOrFn;
    const fn = typeof optionsOrFn === "function" ? optionsOrFn : maybeFn!;
    return this.client.withTrace(
      {
        ...this.toContextPatch(),
        ...opts,
        sessionId: opts.sessionId ?? this.sessionId,
      },
      fn,
    );
  }

  end(): void {
    // Session lifecycle is derived from its traces; no-op hook for symmetry.
  }
}

export class SpanHandle {
  readonly spanId: string;
  readonly traceId: string;
  readonly parentSpanId: string | null;
  readonly spanType: SpanType;
  readonly sampled: boolean;

  private readonly client: PulseAgentClient;
  private readonly traceHandle: TraceHandle | null;
  private readonly state: AgentSpan;
  private ended = false;
  private explicitCostProvided = false;

  constructor(
    client: PulseAgentClient,
    traceHandle: TraceHandle | null,
    state: AgentSpan,
    sampled: boolean,
    explicitCostProvided: boolean,
  ) {
    this.client = client;
    this.traceHandle = traceHandle;
    this.state = state;
    this.spanId = state.span_id;
    this.traceId = state.trace_id;
    this.parentSpanId = state.parent_span_id;
    this.spanType = state.span_type;
    this.sampled = sampled;
    this.explicitCostProvided = explicitCostProvided;
  }

  addEvent(name: string, attributes?: unknown, timestampMs?: number): this {
    if (!this.sampled || this.ended) return this;
    const ev: SpanEvent = {
      name,
      timestamp_ms: timestampMs ?? Date.now(),
      attributes: toJsonValue(attributes),
    };
    this.state.events.push(ev);
    return this;
  }

  start(patch: SpanUpdateOptions = {}): this {
    if (!this.sampled || this.ended) return this;
    this.applyPatch(patch);
    this.state.span_phase = "start";
    if (patch.status === undefined) {
      this.state.status = "running";
    }
    this.client._enqueueSpan(cloneSpan(this.state), this.sampled);
    return this;
  }

  update(patch: SpanUpdateOptions = {}): this {
    if (!this.sampled || this.ended) return this;
    this.applyPatch(patch);
    this.state.span_phase = "update";
    if (patch.status === undefined) {
      this.state.status = "running";
    }
    this.client._enqueueSpan(cloneSpan(this.state), this.sampled);
    return this;
  }

  end(options: SpanEndOptions = {}): this {
    if (this.ended) return this;
    this.ended = true;
    if (!this.sampled) return this;

    this.applyPatch(options);
    const endTs = options.endTsMs ?? Date.now();
    this.state.end_ts_ms = endTs;
    this.state.duration_ms =
      options.durationMs ?? Math.max(0, endTs - this.state.start_ts_ms);
    this.state.span_phase = "final";
    if (options.status !== undefined) {
      this.state.status = options.status;
    } else if (options.error !== undefined || this.state.error_message !== null) {
      this.state.status = "error";
    } else {
      this.state.status = "ok";
    }

    if (
      !this.explicitCostProvided &&
      this.state.cost_nanos === 0 &&
      this.state.cost_usd === 0 &&
      this.state.model !== null
    ) {
      const est = estimateCostNanos(
        this.state.provider,
        this.state.model,
        this.state.input_tokens,
        this.state.output_tokens,
        this.state.cached_tokens,
        this.client._getPricingRules(),
      );
      if (est > 0) {
        this.state.cost_nanos = est;
        this.state.cost_usd = costNanosToUsd(est);
      }
    } else if (this.state.cost_nanos === 0 && this.state.cost_usd > 0) {
      this.state.cost_nanos = usdToCostNanos(this.state.cost_usd);
    } else if (this.state.cost_nanos > 0) {
      this.state.cost_usd = costNanosToUsd(this.state.cost_nanos);
    }

    if (this.state.total_tokens === 0 && (this.state.input_tokens > 0 || this.state.output_tokens > 0)) {
      this.state.total_tokens = this.state.input_tokens + this.state.output_tokens;
    }

    const snapshot = cloneSpan(this.state);
    this.traceHandle?._recordSpan(snapshot);
    this.client._enqueueSpan(snapshot, this.sampled);
    return this;
  }

  private applyPatch(patch: SpanUpdateOptions): void {
    if (patch.status !== undefined) this.state.status = patch.status;
    if (patch.linkedSpanIds !== undefined) {
      this.state.linked_span_ids = [...patch.linkedSpanIds];
    }
    if (patch.provider !== undefined) this.state.provider = patch.provider;
    if (patch.model !== undefined) this.state.model = patch.model;
    if (patch.modelId !== undefined) this.state.model_id = patch.modelId;
    if (patch.modelVersion !== undefined) this.state.model_version = patch.modelVersion;
    if (patch.inputTokens !== undefined) this.state.input_tokens = Math.max(0, Math.trunc(patch.inputTokens));
    if (patch.outputTokens !== undefined) this.state.output_tokens = Math.max(0, Math.trunc(patch.outputTokens));
    if (patch.cachedTokens !== undefined) this.state.cached_tokens = Math.max(0, Math.trunc(patch.cachedTokens));
    if (patch.totalTokens !== undefined) this.state.total_tokens = Math.max(0, Math.trunc(patch.totalTokens));
    if (patch.costNanos !== undefined) {
      this.state.cost_nanos = Math.max(0, Math.trunc(patch.costNanos));
      this.state.cost_usd = costNanosToUsd(this.state.cost_nanos);
      this.explicitCostProvided = true;
    }
    if (patch.costUsd !== undefined) {
      this.state.cost_usd = patch.costUsd;
      if (patch.costNanos === undefined) {
        this.state.cost_nanos = usdToCostNanos(patch.costUsd);
      }
      this.explicitCostProvided = true;
    }
    if (patch.temperature !== undefined) this.state.temperature = patch.temperature;
    if (patch.finishReason !== undefined) this.state.finish_reason = patch.finishReason;
    if (patch.modelParams !== undefined) this.state.model_params = toJsonValue(patch.modelParams);
    if (patch.toolName !== undefined) this.state.tool_name = patch.toolName;
    if (patch.toolVersion !== undefined) this.state.tool_version = patch.toolVersion;
    if (patch.toolRetryCount !== undefined) this.state.tool_retry_count = Math.trunc(patch.toolRetryCount);
    if (patch.input !== undefined) this.state.input = toJsonValue(patch.input);
    if (patch.output !== undefined) this.state.output = toJsonValue(patch.output);
    if (patch.attributes !== undefined) {
      const existing =
        this.state.attributes !== null &&
        typeof this.state.attributes === "object" &&
        !Array.isArray(this.state.attributes)
          ? this.state.attributes
          : {};
      this.state.attributes = toJsonValue({ ...existing, ...patch.attributes });
    }
    if (patch.metadata !== undefined) this.state.metadata = toJsonValue(patch.metadata);
    if (patch.errorType !== undefined) this.state.error_type = patch.errorType;
    if (patch.errorMessage !== undefined) this.state.error_message = patch.errorMessage;
    if (patch.errorStack !== undefined) this.state.error_stack = patch.errorStack;
    if (patch.error !== undefined) {
      const details = errorToDetails(patch.error);
      this.state.error_type = patch.errorType ?? details.errorType;
      this.state.error_message = patch.errorMessage ?? details.errorMessage;
      this.state.error_stack = patch.errorStack ?? details.errorStack;
      if (patch.status === undefined) {
        this.state.status = "error";
      }
    }
  }
}

export class TraceHandle {
  readonly traceId: string;
  readonly sessionId: string | null;
  readonly sampled: boolean;

  private readonly client: PulseAgentClient;
  private readonly state: AgentTrace;
  private readonly recordedSpans = new Map<string, AgentSpan>();
  private ended = false;

  constructor(client: PulseAgentClient, state: AgentTrace, sampled: boolean) {
    this.client = client;
    this.state = state;
    this.traceId = state.trace_id;
    this.sessionId = state.session_id;
    this.sampled = sampled;
  }

  _recordSpan(span: AgentSpan): void {
    this.recordedSpans.set(span.span_id, span);
  }

  toContextPatch(): Partial<PulseContext> {
    const cur = this.client.getContext();
    return {
      sessionId: this.state.session_id ?? cur.sessionId,
      traceId: this.state.trace_id,
      parentTraceId:
        cur.traceId && cur.traceId !== this.state.trace_id
          ? cur.traceId
          : this.state.parent_trace_id ?? cur.parentTraceId,
      workflowId: this.state.workflow_id ?? cur.workflowId,
      userId: this.state.user_id ?? cur.userId,
      environment: this.state.environment,
      agentName: this.state.agent_name,
      agentVersion: this.state.agent_version,
      promptVersion: this.state.prompt_version ?? cur.promptVersion,
      modelConfigVersion: this.state.model_config_version ?? cur.modelConfigVersion,
      toolConfigVersion: this.state.tool_config_version ?? cur.toolConfigVersion,
      appRelease: this.state.app_release ?? cur.appRelease,
      parentSpanId: null,
      sampled: this.sampled,
    };
  }

  span(options: SpanOptions): SpanHandle {
    return this.client._createSpan(options, this);
  }

  generation(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "llm" });
  }

  tool(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "tool" });
  }

  guardrail(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "guardrail" });
  }

  retrieval(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "retrieval" });
  }

  embedding(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "embedding" });
  }

  withSpan<R>(options: SpanOptions, fn: (span: SpanHandle) => R): R {
    const sp = this.span(options);
    return this.client._runWithSpan(sp, this, fn);
  }

  withGeneration<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "llm" }, fn);
  }

  withTool<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "tool" }, fn);
  }

  withGuardrail<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "guardrail" }, fn);
  }

  withRetrieval<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "retrieval" }, fn);
  }

  withEmbedding<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "embedding" }, fn);
  }

  update(patch: TraceUpdateOptions = {}): this {
    if (!this.sampled || this.ended) return this;
    this.applyPatch(patch);
    if (patch.status === undefined) {
      this.state.status = "running";
    }
    this.client._enqueueTrace(cloneTrace(this.state), this.sampled);
    return this;
  }

  end(options: TraceEndOptions = {}): this {
    if (this.ended) return this;
    this.ended = true;
    this.client._unregisterActiveTrace(this.traceId, this);
    if (!this.sampled) return this;

    this.applyPatch(options);
    const endTs = options.endTsMs ?? Date.now();
    this.state.end_ts_ms = endTs;
    this.state.duration_ms =
      options.durationMs ?? Math.max(0, endTs - this.state.start_ts_ms);

    if (options.status !== undefined) {
      this.state.status = options.status;
    } else if (options.error !== undefined || this.state.error_message !== null) {
      this.state.status = "error";
    } else {
      this.state.status = "ok";
    }

    const spans = Array.from(this.recordedSpans.values());
    if (spans.length > 0) {
      let llmCalls = 0;
      let toolCalls = 0;
      let errSpans = 0;
      let inTok = 0;
      let outTok = 0;
      let cacheTok = 0;
      let totTok = 0;
      let costNanos = 0;
      let primaryModel: string | null = this.state.primary_model;
      let primaryProvider: string | null = this.state.primary_provider;

      const toolCounts = new Map<string, number>();
      let prevTool: string | null = null;
      let consecTool = 0;
      let loopDetected = false;

      spans.sort((a, b) => a.start_ts_ms - b.start_ts_ms);
      for (const sp of spans) {
        if (sp.span_type === "llm") {
          llmCalls += 1;
          if (primaryModel === null && sp.model !== null) {
            primaryModel = sp.model;
          }
          if (primaryProvider === null && sp.provider !== null) {
            primaryProvider = sp.provider;
          }
        }
        if (sp.span_type === "tool") {
          toolCalls += 1;
          const tName = sp.tool_name ?? sp.name;
          const count = (toolCounts.get(tName) ?? 0) + 1;
          toolCounts.set(tName, count);
          if (prevTool === tName) {
            consecTool += 1;
          } else {
            prevTool = tName;
            consecTool = 1;
          }
          if (consecTool >= 4 || count >= 6) {
            loopDetected = true;
          }
        }
        if (sp.status === "error" || sp.status === "timeout") {
          errSpans += 1;
        }
        inTok += sp.input_tokens;
        outTok += sp.output_tokens;
        cacheTok += sp.cached_tokens;
        totTok += sp.total_tokens;
        costNanos += sp.cost_nanos;
      }
      if (llmCalls >= 12) {
        loopDetected = true;
      }

      this.state.span_count = spans.length;
      this.state.llm_call_count = llmCalls;
      this.state.tool_call_count = toolCalls;
      this.state.error_span_count = errSpans;
      this.state.input_tokens = options.inputTokens ?? inTok;
      this.state.output_tokens = options.outputTokens ?? outTok;
      this.state.cached_tokens = options.cachedTokens ?? cacheTok;
      this.state.total_tokens = options.totalTokens ?? totTok;
      this.state.primary_model = options.primaryModel ?? primaryModel;
      this.state.primary_provider = options.primaryProvider ?? primaryProvider;
      if (options.hasLoopWarning === undefined && loopDetected) {
        this.state.has_loop_warning = true;
      }
      if (options.totalCostNanos === undefined && options.totalCostUsd === undefined && costNanos > 0) {
        this.state.total_cost_nanos = costNanos;
        this.state.total_cost_usd = costNanosToUsd(costNanos);
      }
    }

    if (options.inputTokens !== undefined) this.state.input_tokens = Math.max(0, Math.trunc(options.inputTokens));
    if (options.outputTokens !== undefined) this.state.output_tokens = Math.max(0, Math.trunc(options.outputTokens));
    if (options.cachedTokens !== undefined) this.state.cached_tokens = Math.max(0, Math.trunc(options.cachedTokens));
    if (options.totalTokens !== undefined) this.state.total_tokens = Math.max(0, Math.trunc(options.totalTokens));
    if (options.totalCostNanos !== undefined) {
      this.state.total_cost_nanos = Math.max(0, Math.trunc(options.totalCostNanos));
      this.state.total_cost_usd = costNanosToUsd(this.state.total_cost_nanos);
    } else if (options.totalCostUsd !== undefined) {
      this.state.total_cost_usd = options.totalCostUsd;
      this.state.total_cost_nanos = usdToCostNanos(options.totalCostUsd);
    }

    this.client._enqueueTrace(cloneTrace(this.state), this.sampled);
    return this;
  }

  private applyPatch(patch: TraceUpdateOptions): void {
    if (patch.status !== undefined) this.state.status = patch.status;
    if (patch.output !== undefined) this.state.output = toJsonValue(patch.output);
    if (patch.metadata !== undefined) this.state.metadata = toJsonValue(patch.metadata);
    if (patch.tags !== undefined) this.state.tags = [...patch.tags];
    if (patch.errorType !== undefined) this.state.error_type = patch.errorType;
    if (patch.errorMessage !== undefined) this.state.error_message = patch.errorMessage;
    if (patch.failureSource !== undefined) this.state.failure_source = patch.failureSource;
    if (patch.primaryModel !== undefined) this.state.primary_model = patch.primaryModel;
    if (patch.primaryProvider !== undefined) this.state.primary_provider = patch.primaryProvider;
    if (patch.avgEvalScore !== undefined) this.state.avg_eval_score = patch.avgEvalScore;
    if (patch.evalPassRate !== undefined) this.state.eval_pass_rate = patch.evalPassRate;
    if (patch.hasLoopWarning !== undefined) this.state.has_loop_warning = patch.hasLoopWarning;
    if (patch.error !== undefined) {
      const details = errorToDetails(patch.error);
      this.state.error_type = patch.errorType ?? details.errorType;
      this.state.error_message = patch.errorMessage ?? details.errorMessage;
      if (patch.status === undefined) {
        this.state.status = "error";
      }
      if (patch.failureSource === undefined && this.state.failure_source === null) {
        this.state.failure_source = "application";
      }
    }
  }
}

export class PulseAgentClient {
  private readonly ingestUrl: string;
  private readonly apiKey: string;
  private readonly projectId: string;
  private readonly defaultAgentName: string;
  private readonly defaultAgentVersion: string;
  private readonly defaultEnvironment: string;
  private readonly defaultPromptVersion?: string;
  private readonly defaultModelConfigVersion?: string;
  private readonly defaultToolConfigVersion?: string;
  private readonly defaultAppRelease?: string;

  private readonly transport: TransportMode;
  private readonly compression: PlstCompressionMode;
  private readonly plstFallback: PlstFallbackMode;

  private readonly queue: RingBuffer<QueueItem>;
  private readonly flushBatchSize: number;
  private readonly maxBatchBytes: number;
  private readonly fetchTimeoutMs: number;
  private readonly maxRetries: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;
  private readonly throwOnFlushError: boolean;

  private readonly sampleRate: number;
  private readonly samplingMode: SamplingMode;
  private readonly privacyPolicy: ResolvedPrivacyPolicy;
  private readonly pricingRules: readonly ModelPricingRule[];
  private readonly fetchImpl: FetchLike;
  private readonly onError?: PulseAgentConfig["onError"];
  private readonly contextManager: ContextManager;

  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private inFlightFlush: Promise<void> | null = null;
  private closed = false;
  private readonly activeTraces = new Map<string, TraceHandle>();

  private readonly stats: Omit<ClientStats, "queuedCount"> = {
    enqueuedTraces: 0,
    enqueuedSpans: 0,
    sentBatches: 0,
    sentTraces: 0,
    sentSpans: 0,
    retriedBatches: 0,
    droppedBatches: 0,
    droppedTraces: 0,
    droppedSpans: 0,
    droppedEvents: 0,
    redactedFields: 0,
    truncatedPayloads: 0,
    sampledOutTraces: 0,
    sampledOutSpans: 0,
  };

  constructor(config: PulseAgentConfig, contextManager?: ContextManager) {
    const rawEndpoint = (
      config.endpoint ??
      config.baseUrl ??
      config.endpointUrl ??
      ""
    )
      .trim()
      .replace(/\/+$/, "");

    this.ingestUrl = rawEndpoint
      ? rawEndpoint.endsWith("/v1/agent/traces")
        ? rawEndpoint
        : `${rawEndpoint}/v1/agent/traces`
      : "/v1/agent/traces";

    this.apiKey = config.apiKey;
    this.projectId = config.projectId ?? "";
    this.defaultAgentName = config.agentName ?? "agent";
    this.defaultAgentVersion = config.agentVersion ?? "unversioned";
    this.defaultEnvironment = config.environment ?? "production";
    this.defaultPromptVersion = config.promptVersion;
    this.defaultModelConfigVersion = config.modelConfigVersion;
    this.defaultToolConfigVersion = config.toolConfigVersion;
    this.defaultAppRelease = config.appRelease;

    const explicitBinary = config.usePlst ?? config.useBinary;
    if (config.transport !== undefined) {
      this.transport = config.transport;
    } else if (explicitBinary === false) {
      this.transport = "json";
    } else if (explicitBinary === true) {
      this.transport = "plst";
    } else {
      this.transport = "auto";
    }

    this.compression = config.compression ?? "auto";
    this.plstFallback =
      config.plstFallback ?? (this.transport === "plst" ? "uncompressed" : "json");

    const maxQueueSize = Math.max(1, config.maxQueueSize ?? DEFAULT_MAX_QUEUE_SIZE);
    const overflowPolicy: OverflowPolicy = config.overflowPolicy ?? "drop_oldest";
    this.queue = new RingBuffer<QueueItem>(maxQueueSize, overflowPolicy);

    this.flushBatchSize = Math.max(1, config.flushBatchSize ?? DEFAULT_FLUSH_BATCH_SIZE);
    this.maxBatchBytes = Math.min(
      HARD_MAX_BODY_BYTES,
      Math.max(1024, config.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES),
    );
    this.fetchTimeoutMs = Math.max(100, config.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS);
    this.maxRetries = Math.max(0, config.maxRetries ?? DEFAULT_MAX_RETRIES);
    this.retryBaseMs = Math.max(0, config.retryBaseMs ?? DEFAULT_RETRY_BASE_MS);
    this.retryMaxMs = Math.max(this.retryBaseMs, config.retryMaxMs ?? DEFAULT_RETRY_MAX_MS);
    this.throwOnFlushError = config.throwOnFlushError ?? false;

    this.sampleRate = clampSampleRate(config.sampleRate);
    this.samplingMode = config.samplingMode ?? "deterministic";
    this.privacyPolicy = resolvePrivacyPolicy(config);
    this.pricingRules = config.pricingRules ?? [];

    const defaultFetch =
      typeof globalThis.fetch === "function"
        ? globalThis.fetch.bind(globalThis)
        : undefined;
    this.fetchImpl =
      config.fetch ??
      defaultFetch ??
      (() => Promise.reject(new Error("No fetch implementation available")));

    this.onError = config.onError;
    this.contextManager = contextManager ?? new ContextManager();

    const intervalMs = config.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    if (intervalMs > 0) {
      this.flushTimer = setInterval(() => {
        void this.flush().catch(() => {});
      }, intervalMs);
      const timerObj = this.flushTimer as unknown as TimerLike;
      if (typeof timerObj.unref === "function") {
        timerObj.unref();
      }
    }
  }

  getContext(): PulseContext {
    return this.contextManager.getContext();
  }

  _getPricingRules(): readonly ModelPricingRule[] {
    return this.pricingRules;
  }

  session(options: SessionOptions = {}): SessionHandle {
    return new SessionHandle(this, options);
  }

  withSession<R>(
    optionsOrFn: SessionOptions | ((session: SessionHandle) => R),
    maybeFn?: (session: SessionHandle) => R,
  ): R {
    const opts = typeof optionsOrFn === "function" ? {} : optionsOrFn;
    const fn = typeof optionsOrFn === "function" ? optionsOrFn : maybeFn!;
    const sess = this.session(opts);
    return this.contextManager.runWithContext(sess.toContextPatch(), () => fn(sess));
  }

  trace(options: TraceOptions = {}): TraceHandle {
    const ctx = this.contextManager.getContext();
    const traceId = options.traceId ?? options.id ?? generateId("tr");
    const sessionId = options.sessionId ?? ctx.sessionId ?? null;
    const parentTraceId =
      options.parentTraceId ??
      (ctx.traceId && ctx.traceId !== traceId ? ctx.traceId : ctx.parentTraceId) ??
      null;

    const sampled = shouldSample({
      traceId,
      sessionId,
      sampleRate: options.sampleRate ?? this.sampleRate,
      samplingMode: this.samplingMode,
      explicitSampled: options.sampled ?? ctx.sampled,
    });

    const startTs = options.startTsMs ?? Date.now();
    const traceState: AgentTrace = {
      trace_id: traceId,
      project_id: options.projectId ?? this.projectId,
      agent_name: options.agentName ?? ctx.agentName ?? this.defaultAgentName,
      agent_version: options.agentVersion ?? ctx.agentVersion ?? this.defaultAgentVersion,
      prompt_version: options.promptVersion ?? ctx.promptVersion ?? this.defaultPromptVersion ?? null,
      model_config_version:
        options.modelConfigVersion ??
        ctx.modelConfigVersion ??
        this.defaultModelConfigVersion ??
        null,
      tool_config_version:
        options.toolConfigVersion ??
        ctx.toolConfigVersion ??
        this.defaultToolConfigVersion ??
        null,
      app_release: options.appRelease ?? ctx.appRelease ?? this.defaultAppRelease ?? null,
      environment: options.environment ?? ctx.environment ?? this.defaultEnvironment,
      session_id: sessionId,
      user_id: options.userId ?? ctx.userId ?? null,
      workflow_id: options.workflowId ?? ctx.workflowId ?? null,
      parent_trace_id: parentTraceId,
      status: options.status ?? "running",
      start_ts_ms: startTs,
      end_ts_ms: null,
      duration_ms: 0,
      input: toJsonValue(options.input),
      output: null,
      error_type: null,
      error_message: null,
      failure_source: null,
      metadata: toJsonValue(options.metadata),
      tags: options.tags ? [...options.tags] : [],
      span_count: 0,
      llm_call_count: 0,
      tool_call_count: 0,
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
    };

    if (!sampled) {
      this.stats.sampledOutTraces += 1;
    } else if (options.emitStart) {
      this._enqueueTrace(cloneTrace(traceState), true);
    }

    const handle = new TraceHandle(this, traceState, sampled);
    this.activeTraces.set(traceId, handle);
    return handle;
  }

  _unregisterActiveTrace(traceId: string, handle: TraceHandle): void {
    if (this.activeTraces.get(traceId) === handle) {
      this.activeTraces.delete(traceId);
    }
  }

  withTrace<R>(
    optionsOrFn: TraceOptions | ((trace: TraceHandle) => R),
    maybeFn?: (trace: TraceHandle) => R,
  ): R {
    const opts = typeof optionsOrFn === "function" ? {} : optionsOrFn;
    const fn = typeof optionsOrFn === "function" ? optionsOrFn : maybeFn!;
    const tr = this.trace(opts);
    let finalized = false;
    const finalize = (err?: unknown): void => {
      if (!finalized) {
        finalized = true;
        tr.end(err !== undefined ? { error: err } : {});
      }
    };

    try {
      const res = this.contextManager.runWithContext(tr.toContextPatch(), () => fn(tr));
      if (
        res !== null &&
        (typeof res === "object" || typeof res === "function") &&
        typeof (res as { then?: unknown }).then === "function"
      ) {
        return Promise.resolve(res).then(
          (val) => {
            finalize();
            return val;
          },
          (err) => {
            finalize(err);
            throw err;
          },
        ) as unknown as R;
      }
      finalize();
      return res;
    } catch (err) {
      finalize(err);
      throw err;
    }
  }

  span(options: SpanOptions): SpanHandle {
    return this._createSpan(options, null);
  }

  generation(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "llm" });
  }

  tool(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "tool" });
  }

  guardrail(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "guardrail" });
  }

  retrieval(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "retrieval" });
  }

  embedding(options: Omit<SpanOptions, "spanType">): SpanHandle {
    return this.span({ ...options, spanType: "embedding" });
  }

  withSpan<R>(options: SpanOptions, fn: (span: SpanHandle) => R): R {
    const sp = this.span(options);
    return this._runWithSpan(sp, null, fn);
  }

  withGeneration<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "llm" }, fn);
  }

  withTool<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "tool" }, fn);
  }

  withGuardrail<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "guardrail" }, fn);
  }

  withRetrieval<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "retrieval" }, fn);
  }

  withEmbedding<R>(
    options: Omit<SpanOptions, "spanType">,
    fn: (span: SpanHandle) => R,
  ): R {
    return this.withSpan({ ...options, spanType: "embedding" }, fn);
  }

  _createSpan(options: SpanOptions, traceHandle: TraceHandle | null): SpanHandle {
    const ctx = this.contextManager.getContext();
    const spanId = options.spanId ?? options.id ?? generateId("sp");
    const traceId = options.traceId ?? traceHandle?.traceId ?? ctx.traceId ?? generateId("tr");
    const effectiveTraceHandle = traceHandle ?? this.activeTraces.get(traceId) ?? null;
    const canInheritParentSpan =
      ctx.parentSpanId !== undefined &&
      ctx.parentSpanId !== null &&
      (ctx.traceId === undefined || traceId === ctx.traceId);
    const parentSpanId =
      options.parentSpanId !== undefined
        ? options.parentSpanId
        : canInheritParentSpan
          ? ctx.parentSpanId!
          : null;

    const sampled =
      effectiveTraceHandle?.sampled ??
      shouldSample({
        traceId,
        sessionId: ctx.sessionId,
        sampleRate: this.sampleRate,
        samplingMode: this.samplingMode,
        explicitSampled: ctx.sampled,
      });

    const explicitCost =
      options.costNanos !== undefined || options.costUsd !== undefined;
    const costNanos =
      options.costNanos !== undefined
        ? Math.max(0, Math.trunc(options.costNanos))
        : options.costUsd !== undefined
          ? usdToCostNanos(options.costUsd)
          : 0;
    const costUsd =
      options.costUsd !== undefined ? options.costUsd : costNanosToUsd(costNanos);

    const startTs = options.startTsMs ?? Date.now();
    const events: SpanEvent[] = (options.events ?? []).map((ev) => ({
      name: ev.name,
      timestamp_ms: ev.timestampMs ?? startTs,
      attributes: toJsonValue(ev.attributes),
    }));

    const spanState: AgentSpan = {
      span_id: spanId,
      trace_id: traceId,
      project_id: options.projectId ?? this.projectId,
      parent_span_id: parentSpanId,
      linked_span_ids: options.linkedSpanIds ? [...options.linkedSpanIds] : [],
      span_phase: options.spanPhase ?? (options.emitStart ? "start" : "final"),
      depth: options.depth ?? 0,
      name: options.name,
      span_type: options.spanType ?? "custom",
      status: options.status ?? (options.emitStart ? "running" : "ok"),
      start_ts_ms: startTs,
      end_ts_ms: null,
      duration_ms: 0,
      provider: options.provider ?? null,
      model: options.model ?? null,
      model_id: options.modelId ?? null,
      model_version: options.modelVersion ?? null,
      prompt_template_id: options.promptTemplateId ?? null,
      prompt_template_version: options.promptTemplateVersion ?? null,
      input_tokens: Math.max(0, Math.trunc(options.inputTokens ?? 0)),
      output_tokens: Math.max(0, Math.trunc(options.outputTokens ?? 0)),
      cached_tokens: Math.max(0, Math.trunc(options.cachedTokens ?? 0)),
      total_tokens: Math.max(0, Math.trunc(options.totalTokens ?? 0)),
      cost_nanos: costNanos,
      cost_usd: costUsd,
      temperature: options.temperature ?? null,
      finish_reason: options.finishReason ?? null,
      model_params: toJsonValue(options.modelParams),
      tool_name: options.toolName ?? null,
      tool_version: options.toolVersion ?? null,
      tool_retry_count: Math.trunc(options.toolRetryCount ?? 0),
      input: toJsonValue(options.input),
      output: toJsonValue(options.output),
      attributes: toJsonValue(options.attributes),
      events,
      metadata: toJsonValue(options.metadata),
      error_type: null,
      error_message: null,
      error_stack: null,
    };

    const handle = new SpanHandle(
      this,
      effectiveTraceHandle,
      spanState,
      sampled,
      explicitCost,
    );

    if (!sampled) {
      this.stats.sampledOutSpans += 1;
    } else if (options.emitStart) {
      this._enqueueSpan(cloneSpan(spanState), true);
    }

    return handle;
  }

  _runWithSpan<R>(
    span: SpanHandle,
    traceHandle: TraceHandle | null,
    fn: (span: SpanHandle) => R,
  ): R {
    const patch: Partial<PulseContext> = {
      traceId: traceHandle?.traceId ?? span.traceId,
      parentSpanId: span.spanId,
      sampled: span.sampled,
    };

    let finalized = false;
    const finalize = (err?: unknown): void => {
      if (!finalized) {
        finalized = true;
        span.end(err !== undefined ? { error: err } : {});
      }
    };

    try {
      const res = this.contextManager.runWithContext(patch, () => fn(span));
      if (
        res !== null &&
        (typeof res === "object" || typeof res === "function") &&
        typeof (res as { then?: unknown }).then === "function"
      ) {
        return Promise.resolve(res).then(
          (val) => {
            finalize();
            return val;
          },
          (err) => {
            finalize(err);
            throw err;
          },
        ) as unknown as R;
      }
      finalize();
      return res;
    } catch (err) {
      finalize(err);
      throw err;
    }
  }

  ingestBatch(batch: TraceIngestBatch): void {
    if (this.closed) return;
    for (const tr of batch.traces) {
      this._enqueueTrace(cloneTrace(tr), true);
    }
    for (const sp of batch.spans) {
      this._enqueueSpan(cloneSpan(sp), true);
    }
  }

  _enqueueTrace(trace: AgentTrace, sampled: boolean): void {
    if (this.closed || !sampled) return;
    this.stats.enqueuedTraces += 1;
    this.pushToQueue({ kind: "trace", data: trace });
  }

  _enqueueSpan(span: AgentSpan, sampled: boolean): void {
    if (this.closed || !sampled) return;
    this.stats.enqueuedSpans += 1;
    this.pushToQueue({ kind: "span", data: span });
  }

  private pushToQueue(item: QueueItem): void {
    const res = this.queue.push(item);
    if (res.dropped !== undefined) {
      if (res.dropped.kind === "trace") {
        this.stats.droppedTraces += 1;
      } else {
        this.stats.droppedSpans += 1;
        this.stats.droppedEvents += res.dropped.data.events.length;
      }
    }
  }

  getStats(): ClientStats {
    return {
      ...this.stats,
      queuedCount: this.queue.length,
    };
  }

  getMetrics(): ClientStats {
    return this.getStats();
  }

  async flush(): Promise<void> {
    while (this.inFlightFlush !== null) {
      await this.inFlightFlush;
    }
    if (this.queue.length === 0) {
      return;
    }
    const work = this.drainAndSendAll();
    this.inFlightFlush = work;
    try {
      await work;
    } finally {
      if (this.inFlightFlush === work) {
        this.inFlightFlush = null;
      }
    }
  }

  async shutdown(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      if (this.flushTimer !== null) {
        clearInterval(this.flushTimer);
        this.flushTimer = null;
      }
    }
    await this.flush();
  }

  private async drainAndSendAll(): Promise<void> {
    while (this.queue.length > 0) {
      const items = this.queue.drain(this.flushBatchSize);
      if (items.length === 0) break;
      const batch = this.buildSanitizedBatch(items);
      if (batch.traces.length === 0 && batch.spans.length === 0) {
        continue;
      }
      await this.sendChunkedBatch(batch);
    }
  }

  private async sendChunkedBatch(batch: TraceIngestBatch): Promise<void> {
    const totalItems = batch.traces.length + batch.spans.length;
    if (totalItems === 0) return;

    const encoded = this.encodeBatch(batch);

    if (encoded.byteLength <= this.maxBatchBytes) {
      await this.sendWithRetries(encoded, batch);
      return;
    }

    if (totalItems <= 1) {
      if (encoded.byteLength <= HARD_MAX_BODY_BYTES) {
        await this.sendWithRetries(encoded, batch);
        return;
      }
      this.recordDroppedBatch(batch);
      this.notifyError(
        new Error(`Single item batch (${encoded.byteLength} bytes) exceeds 8 MiB limit`),
        totalItems,
        undefined,
        false,
      );
      return;
    }

    const traceCount = batch.traces.length;
    const mid = Math.floor(totalItems / 2);
    const left: TraceIngestBatch = {
      traces: batch.traces.slice(0, Math.min(traceCount, mid)),
      spans: batch.spans.slice(0, Math.max(0, mid - traceCount)),
    };
    const right: TraceIngestBatch = {
      traces: batch.traces.slice(Math.min(traceCount, mid)),
      spans: batch.spans.slice(Math.max(0, mid - traceCount)),
    };
    await this.sendChunkedBatch(left);
    await this.sendChunkedBatch(right);
  }

  private buildSanitizedBatch(items: readonly QueueItem[]): TraceIngestBatch {
    const traces: AgentTrace[] = [];
    const spans: AgentSpan[] = [];
    for (const item of items) {
      if (item.kind === "trace") {
        traces.push(item.data);
      } else {
        spans.push(item.data);
      }
    }
    const batch: TraceIngestBatch = { traces, spans };
    const sanStats = sanitizeBatch(batch, this.privacyPolicy);
    this.stats.redactedFields += sanStats.redactedFields;
    this.stats.truncatedPayloads += sanStats.truncatedPayloads;
    this.stats.droppedSpans += sanStats.droppedSpans;
    return batch;
  }

  private encodeBatch(batch: TraceIngestBatch): EncodedPayload {
    const zstdOk = supportsZstd();
    let useJson = false;
    let compressionMode: PlstCompressionMode = this.compression;

    if (this.transport === "json") {
      useJson = true;
    } else if (this.transport === "auto") {
      if (!zstdOk && this.plstFallback === "json" && this.compression === "auto") {
        useJson = true;
      }
    }

    if (!useJson) {
      if (
        (compressionMode === "dict" || compressionMode === "raw") &&
        !zstdOk
      ) {
        if (this.plstFallback === "json") {
          useJson = true;
        } else {
          compressionMode = "none";
        }
      }
    }

    if (useJson) {
      const jsonStr = JSON.stringify(batch);
      const byteLength = new TextEncoder().encode(jsonStr).length;
      return {
        body: jsonStr,
        byteLength,
        contentType: "application/json",
      };
    }

    const frame = encodePlstBatch(batch, { compression: compressionMode });
    return {
      body: frame,
      byteLength: frame.byteLength,
      contentType: "application/x-pulse-trace",
    };
  }

  private async sendWithRetries(
    encoded: EncodedPayload,
    batch: TraceIngestBatch,
  ): Promise<void> {
    let lastErr: Error | null = null;
    let lastStatus: number | undefined;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const res = await this.fetchWithTimeout(encoded);
        if (res.ok) {
          // Consume optional JSON body safely without failing if empty.
          try {
            const text = await res.text();
            if (text.length > 0) {
              JSON.parse(text) as TraceIngestResponse;
            }
          } catch {
            // Ignore response body parse errors on 2xx.
          }
          this.stats.sentBatches += 1;
          this.stats.sentTraces += batch.traces.length;
          this.stats.sentSpans += batch.spans.length;
          return;
        }

        lastStatus = res.status;
        if (!isRetryableStatus(res.status)) {
          const bodyText = await res.text().catch(() => "");
          const err = new Error(
            `Pulse ingest rejected batch with HTTP ${res.status}: ${bodyText.slice(0, 256)}`,
          );
          this.recordDroppedBatch(batch);
          this.notifyError(err, batch.traces.length + batch.spans.length, res.status, false);
          if (this.throwOnFlushError) throw err;
          return;
        }

        lastErr = new Error(`Pulse ingest transient HTTP ${res.status}`);
        if (attempt < this.maxRetries) {
          this.stats.retriedBatches += 1;
          const retryAfterMs = parseRetryAfterMs(res.headers.get("retry-after"));
          const cap = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** attempt);
          const jitter = Math.random() * cap;
          const delayMs =
            retryAfterMs !== null ? Math.max(retryAfterMs, jitter) : jitter;
          await sleep(delayMs);
        }
      } catch (err) {
        if (this.throwOnFlushError && lastStatus !== undefined && !isRetryableStatus(lastStatus)) {
          throw err;
        }
        lastErr = err instanceof Error ? err : new Error(String(err));
        if (attempt < this.maxRetries) {
          this.stats.retriedBatches += 1;
          const cap = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** attempt);
          await sleep(Math.random() * cap);
        }
      }
    }

    this.recordDroppedBatch(batch);
    const finalErr = lastErr ?? new Error("Pulse ingest failed after retries");
    this.notifyError(
      finalErr,
      batch.traces.length + batch.spans.length,
      lastStatus,
      true,
    );
    if (this.throwOnFlushError) {
      throw finalErr;
    }
  }

  private async fetchWithTimeout(encoded: EncodedPayload): Promise<Response> {
    const controller =
      typeof AbortController !== "undefined" ? new AbortController() : null;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller?.abort();
        reject(new Error(`Pulse ingest request timed out after ${this.fetchTimeoutMs}ms`));
      }, this.fetchTimeoutMs);
    });

    const headers: Record<string, string> = {
      "Content-Type": encoded.contentType,
      "x-pulse-key": this.apiKey,
    };

    const bodyInit =
      typeof encoded.body === "string"
        ? encoded.body
        : (encoded.body as unknown as BodyInit);

    const fetchPromise = this.fetchImpl(this.ingestUrl, {
      method: "POST",
      headers,
      body: bodyInit,
      signal: controller?.signal,
    });

    try {
      return await Promise.race([fetchPromise, timeoutPromise]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    }
  }

  private recordDroppedBatch(batch: TraceIngestBatch): void {
    this.stats.droppedBatches += 1;
    this.stats.droppedTraces += batch.traces.length;
    this.stats.droppedSpans += batch.spans.length;
    for (const sp of batch.spans) {
      this.stats.droppedEvents += sp.events.length;
    }
  }

  private notifyError(
    error: Error,
    batchSize: number,
    status: number | undefined,
    retryable: boolean,
  ): void {
    if (this.onError) {
      try {
        this.onError(error, { batchSize, status, retryable });
      } catch {
        // Never allow user error handler to crash SDK flush.
      }
    }
  }
}
