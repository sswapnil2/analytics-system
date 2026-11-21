export type TraceStatus = "ok" | "running" | "error" | "timeout" | "cancelled";

export type SpanPhase = "start" | "update" | "final";

export type SpanType =
  | "agent"
  | "llm"
  | "tool"
  | "retrieval"
  | "embedding"
  | "guardrail"
  | "http"
  | "db"
  | "workflow"
  | "custom";

export type FailureSource =
  | "model"
  | "tool"
  | "application"
  | "prompt"
  | "external"
  | "unknown";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonArray;
export interface JsonObject {
  [key: string]: JsonValue;
}
export type JsonArray = JsonValue[];

export interface SpanEvent {
  name: string;
  timestamp_ms: number;
  attributes: JsonValue;
}

export interface AgentSpan {
  span_id: string;
  trace_id: string;
  project_id: string;
  parent_span_id: string | null;
  linked_span_ids: string[];
  span_phase: SpanPhase;
  depth: number;
  name: string;
  span_type: SpanType;
  status: TraceStatus;
  start_ts_ms: number;
  end_ts_ms: number | null;
  duration_ms: number;

  provider: string | null;
  model: string | null;
  model_id: string | null;
  model_version: string | null;
  prompt_template_id: string | null;
  prompt_template_version: string | null;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  total_tokens: number;
  cost_nanos: number;
  cost_usd: number;
  temperature: number | null;
  finish_reason: string | null;
  model_params: JsonValue;

  tool_name: string | null;
  tool_version: string | null;
  tool_retry_count: number;

  input: JsonValue;
  output: JsonValue;
  attributes: JsonValue;
  events: SpanEvent[];
  metadata: JsonValue;
  error_type: string | null;
  error_message: string | null;
  error_stack: string | null;
}

export interface AgentTrace {
  trace_id: string;
  project_id: string;
  agent_name: string;
  agent_version: string;
  prompt_version: string | null;
  model_config_version: string | null;
  tool_config_version: string | null;
  app_release: string | null;
  environment: string;
  session_id: string | null;
  user_id: string | null;
  workflow_id: string | null;
  parent_trace_id: string | null;
  status: TraceStatus;
  start_ts_ms: number;
  end_ts_ms: number | null;
  duration_ms: number;
  input: JsonValue;
  output: JsonValue;
  error_type: string | null;
  error_message: string | null;
  failure_source: FailureSource | null;
  metadata: JsonValue;
  tags: string[];

  span_count: number;
  llm_call_count: number;
  tool_call_count: number;
  error_span_count: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  total_tokens: number;
  total_cost_nanos: number;
  total_cost_usd: number;
  primary_model: string | null;
  primary_provider: string | null;
  avg_eval_score: number | null;
  eval_pass_rate: number | null;
  has_loop_warning: boolean;
}

export interface TraceIngestBatch {
  traces: AgentTrace[];
  spans: AgentSpan[];
}

export interface TraceIngestResponse {
  accepted_traces: number;
  accepted_spans: number;
  truncated_payloads: number;
  redacted_fields: number;
  dropped_spans: number;
  warnings: string[];
}

export type PlstCompressionMode = "auto" | "dict" | "raw" | "none";
export type TransportMode = "auto" | "plst" | "json";
export type PlstFallbackMode = "uncompressed" | "json";
export type OverflowPolicy = "drop_oldest" | "drop_newest";
export type SamplingMode = "deterministic" | "probabilistic";

export interface ModelPricingRule {
  provider: string;
  model_pattern: string;
  input_per_1m_usd: number;
  output_per_1m_usd: number;
  cached_input_per_1m_usd: number;
}

export type RedactHook = (
  value: unknown,
  context: { field: string; spanType?: SpanType },
) => unknown;

export interface PrivacyConfig {
  captureInputs?: boolean;
  captureOutputs?: boolean;
  capturePrompts?: boolean;
  captureResponses?: boolean;
  captureToolArgs?: boolean;
  captureToolResults?: boolean;
  captureUserId?: boolean;
  redactPii?: boolean;
  redactKeys?: string[];
  excludeAttributes?: string[];
  redactPatterns?: RegExp[];
  redact?: RedactHook;
}

export interface SamplingConfig {
  sampleRate?: number;
  samplingMode?: SamplingMode;
}

export type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<Response>;

export interface PulseAgentConfig extends PrivacyConfig, SamplingConfig {
  endpoint?: string;
  baseUrl?: string;
  endpointUrl?: string;
  apiKey: string;
  projectId?: string;
  agentName?: string;
  agentVersion?: string;
  environment?: string;
  promptVersion?: string;
  modelConfigVersion?: string;
  toolConfigVersion?: string;
  appRelease?: string;

  usePlst?: boolean;
  useBinary?: boolean;
  transport?: TransportMode;
  compression?: PlstCompressionMode;
  plstFallback?: PlstFallbackMode;

  maxQueueSize?: number;
  overflowPolicy?: OverflowPolicy;
  flushBatchSize?: number;
  maxBatchBytes?: number;
  flushIntervalMs?: number;
  fetchTimeoutMs?: number;
  maxRetries?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
  throwOnFlushError?: boolean;

  pricingRules?: ModelPricingRule[];
  fetch?: FetchLike;
  onError?: (error: Error, context?: { batchSize: number; status?: number; retryable: boolean }) => void;
}

export interface PulseContext {
  sessionId?: string;
  traceId?: string;
  parentTraceId?: string;
  parentSpanId?: string | null;
  workflowId?: string;
  userId?: string;
  environment?: string;
  agentName?: string;
  agentVersion?: string;
  promptVersion?: string;
  modelConfigVersion?: string;
  toolConfigVersion?: string;
  appRelease?: string;
  sampled?: boolean;
}

export interface SessionOptions {
  sessionId?: string;
  id?: string;
  userId?: string;
  environment?: string;
  agentName?: string;
  agentVersion?: string;
  promptVersion?: string;
  modelConfigVersion?: string;
  toolConfigVersion?: string;
  appRelease?: string;
  workflowId?: string;
  sampled?: boolean;
  metadata?: unknown;
}

export interface TraceOptions {
  traceId?: string;
  id?: string;
  projectId?: string;
  sessionId?: string;
  userId?: string;
  workflowId?: string;
  parentTraceId?: string;
  agentName?: string;
  agentVersion?: string;
  promptVersion?: string;
  modelConfigVersion?: string;
  toolConfigVersion?: string;
  appRelease?: string;
  environment?: string;
  status?: TraceStatus;
  startTsMs?: number;
  input?: unknown;
  metadata?: unknown;
  tags?: string[];
  sampled?: boolean;
  sampleRate?: number;
  emitStart?: boolean;
}

export interface TraceUpdateOptions {
  status?: TraceStatus;
  output?: unknown;
  metadata?: unknown;
  tags?: string[];
  errorType?: string;
  errorMessage?: string;
  failureSource?: FailureSource;
  error?: unknown;
  primaryModel?: string;
  primaryProvider?: string;
  avgEvalScore?: number | null;
  evalPassRate?: number | null;
  hasLoopWarning?: boolean;
}

export interface TraceEndOptions extends TraceUpdateOptions {
  endTsMs?: number;
  durationMs?: number;
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  totalTokens?: number;
  totalCostNanos?: number;
  totalCostUsd?: number;
}

export interface SpanOptions {
  spanId?: string;
  id?: string;
  traceId?: string;
  projectId?: string;
  parentSpanId?: string | null;
  linkedSpanIds?: string[];
  spanPhase?: SpanPhase;
  depth?: number;
  name: string;
  spanType?: SpanType;
  status?: TraceStatus;
  startTsMs?: number;
  provider?: string;
  model?: string;
  modelId?: string;
  modelVersion?: string;
  promptTemplateId?: string;
  promptTemplateVersion?: string;
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  totalTokens?: number;
  costNanos?: number;
  costUsd?: number;
  temperature?: number | null;
  finishReason?: string;
  modelParams?: unknown;
  toolName?: string;
  toolVersion?: string;
  toolRetryCount?: number;
  input?: unknown;
  output?: unknown;
  attributes?: Record<string, unknown>;
  metadata?: unknown;
  events?: Array<{ name: string; timestampMs?: number; attributes?: unknown }>;
  emitStart?: boolean;
}

export interface SpanUpdateOptions {
  status?: TraceStatus;
  linkedSpanIds?: string[];
  provider?: string;
  model?: string;
  modelId?: string;
  modelVersion?: string;
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  totalTokens?: number;
  costNanos?: number;
  costUsd?: number;
  temperature?: number | null;
  finishReason?: string;
  modelParams?: unknown;
  toolName?: string;
  toolVersion?: string;
  toolRetryCount?: number;
  input?: unknown;
  output?: unknown;
  attributes?: Record<string, unknown>;
  metadata?: unknown;
  errorType?: string;
  errorMessage?: string;
  errorStack?: string;
  error?: unknown;
}

export interface SpanEndOptions extends SpanUpdateOptions {
  endTsMs?: number;
  durationMs?: number;
}

export interface ClientStats {
  enqueuedTraces: number;
  enqueuedSpans: number;
  sentBatches: number;
  sentTraces: number;
  sentSpans: number;
  retriedBatches: number;
  droppedBatches: number;
  droppedTraces: number;
  droppedSpans: number;
  droppedEvents: number;
  redactedFields: number;
  truncatedPayloads: number;
  sampledOutTraces: number;
  sampledOutSpans: number;
  queuedCount: number;
}
