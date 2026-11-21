import { costNanosToUsd, syncSpanCost, syncTraceCost } from "./pricing.js";
import type {
  AgentSpan,
  AgentTrace,
  JsonObject,
  JsonValue,
  PrivacyConfig,
  RedactHook,
  SpanType,
  TraceIngestBatch,
} from "./types.js";

export const REDACTED_MARKER = "[REDACTED]";
export const MAX_SPANS_PER_TRACE = 512;
export const MAX_SPAN_DEPTH = 32;
export const MAX_LINKED_SPANS_PER_SPAN = 64;
export const MAX_PAYLOAD_BYTES = 64 * 1024;
export const MAX_ATTRIBUTES_PER_SPAN = 64;
export const MAX_EVENTS_PER_SPAN = 64;
export const MAX_TAGS_PER_TRACE = 32;
const MAX_SPAN_ID_BYTES = 256;

export const DEFAULT_REDACT_KEYS: readonly string[] = [
  "password",
  "secret",
  "token",
  "api_key",
  "authorization",
  "cookie",
  "ssn",
  "credit_card",
];

const EMAIL_RE = /\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b/gi;
const API_KEY_RE =
  /\b(?:sk-[A-Za-z0-9_-]{16,}|sk_live_[A-Za-z0-9_-]{12,}|sk_root_[A-Za-z0-9_-]{12,}|pk_live_[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{20,}|AIza[0-9A-Za-z_-]{30,}|xox[baprs]-[0-9A-Za-z-]{10,}|(?:AKIA|ASIA)[0-9A-Z]{16})\b/g;
const BEARER_RE = /\bBearer\s+[A-Za-z0-9._~+/=-]{12,}\b/gi;
const SSN_RE = /\b\d{3}-\d{2}-\d{4}\b/g;
const CARD_CANDIDATE_RE = /\b(?:\d[ -]*?){13,19}\b/g;

const textEncoder = new TextEncoder();

export interface SanitizeStats {
  redactedFields: number;
  truncatedPayloads: number;
  droppedSpans: number;
}

export interface ResolvedPrivacyPolicy {
  captureInputs: boolean;
  captureOutputs: boolean;
  capturePrompts: boolean;
  captureResponses: boolean;
  captureToolArgs: boolean;
  captureToolResults: boolean;
  captureUserId: boolean;
  redactPii: boolean;
  redactKeysLower: string[];
  excludeAttributesLower: Set<string>;
  redactPatterns: RegExp[];
  redactHook?: RedactHook;
}

export function resolvePrivacyPolicy(config: PrivacyConfig = {}): ResolvedPrivacyPolicy {
  const keys = config.redactKeys ?? [...DEFAULT_REDACT_KEYS];
  return {
    captureInputs: config.captureInputs ?? true,
    captureOutputs: config.captureOutputs ?? true,
    capturePrompts: config.capturePrompts ?? true,
    captureResponses: config.captureResponses ?? true,
    captureToolArgs: config.captureToolArgs ?? true,
    captureToolResults: config.captureToolResults ?? true,
    captureUserId: config.captureUserId ?? true,
    redactPii: config.redactPii ?? true,
    redactKeysLower: keys
      .map((k) => k.trim().toLowerCase())
      .filter((k) => k.length > 0),
    excludeAttributesLower: new Set(
      (config.excludeAttributes ?? [])
        .map((k) => k.trim().toLowerCase())
        .filter((k) => k.length > 0),
    ),
    redactPatterns: (config.redactPatterns ?? []).map((re) => {
      const flags = re.flags.includes("g") ? re.flags : `${re.flags}g`;
      return new RegExp(re.source, flags);
    }),
    redactHook: config.redact,
  };
}

export function isLuhnValidCard(candidate: string): boolean {
  const digits: number[] = [];
  for (let i = 0; i < candidate.length; i++) {
    const ch = candidate.charCodeAt(i);
    if (ch >= 48 && ch <= 57) {
      digits.push(ch - 48);
    }
  }
  if (digits.length < 13 || digits.length > 19) {
    return false;
  }
  let sum = 0;
  let alt = false;
  for (let i = digits.length - 1; i >= 0; i--) {
    let n = digits[i]!;
    if (alt) {
      n *= 2;
      if (n > 9) n -= 9;
    }
    sum += n;
    alt = !alt;
  }
  return sum % 10 === 0;
}

export function redactPiiString(
  input: string,
  stats: SanitizeStats,
  customPatterns: readonly RegExp[] = [],
): string {
  let out = input;

  for (const re of [API_KEY_RE, BEARER_RE, EMAIL_RE, SSN_RE]) {
    re.lastIndex = 0;
    if (re.test(out)) {
      stats.redactedFields += 1;
      re.lastIndex = 0;
      out = out.replace(re, (m) => (re === BEARER_RE ? `Bearer ${REDACTED_MARKER}` : REDACTED_MARKER));
    }
  }

  CARD_CANDIDATE_RE.lastIndex = 0;
  if (CARD_CANDIDATE_RE.test(out)) {
    CARD_CANDIDATE_RE.lastIndex = 0;
    let cardRedacted = false;
    out = out.replace(CARD_CANDIDATE_RE, (match) => {
      if (isLuhnValidCard(match)) {
        cardRedacted = true;
        return REDACTED_MARKER;
      }
      return match;
    });
    if (cardRedacted) {
      stats.redactedFields += 1;
    }
  }

  for (const pat of customPatterns) {
    pat.lastIndex = 0;
    if (pat.test(out)) {
      stats.redactedFields += 1;
      pat.lastIndex = 0;
      out = out.replace(pat, REDACTED_MARKER);
    }
  }

  return out;
}

export function toJsonValue(
  value: unknown,
  seen: WeakSet<object> = new WeakSet(),
  depth = 0,
): JsonValue {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "symbol" || typeof value === "function") {
    return String(value);
  }
  if (value instanceof Date) {
    return Number.isFinite(value.getTime()) ? value.toISOString() : null;
  }
  if (value instanceof Error) {
    const errObj: JsonObject = {
      name: value.name,
      message: value.message,
    };
    if (value.stack !== undefined) {
      errObj.stack = value.stack;
    }
    return errObj;
  }
  if (typeof value === "object") {
    if (seen.has(value) || depth >= 32) {
      return "[Circular]";
    }
    seen.add(value);
    try {
      if (Array.isArray(value)) {
        return value.map((item) => toJsonValue(item, seen, depth + 1));
      }
      if (value instanceof Set) {
        return Array.from(value).map((item) => toJsonValue(item, seen, depth + 1));
      }
      if (value instanceof Map) {
        const obj: JsonObject = {};
        for (const [k, v] of value.entries()) {
          obj[String(k)] = toJsonValue(v, seen, depth + 1);
        }
        return obj;
      }
      const obj: JsonObject = {};
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
        if (v === undefined) continue;
        obj[k] = toJsonValue(v, seen, depth + 1);
      }
      return obj;
    } finally {
      seen.delete(value);
    }
  }
  return null;
}

export function redactValue(
  val: JsonValue,
  policy: ResolvedPrivacyPolicy,
  stats: SanitizeStats,
): JsonValue {
  if (val === null || typeof val === "number" || typeof val === "boolean") {
    return val;
  }
  if (typeof val === "string") {
    return policy.redactPii
      ? redactPiiString(val, stats, policy.redactPatterns)
      : applyCustomPatternsOnly(val, stats, policy.redactPatterns);
  }
  if (Array.isArray(val)) {
    return val.map((item) => redactValue(item, policy, stats));
  }
  const out: JsonObject = {};
  for (const [k, v] of Object.entries(val)) {
    const kl = k.toLowerCase();
    if (policy.redactKeysLower.some((rk) => kl.includes(rk))) {
      out[k] = REDACTED_MARKER;
      if (v !== REDACTED_MARKER) {
        stats.redactedFields += 1;
      }
    } else {
      out[k] = redactValue(v, policy, stats);
    }
  }
  return out;
}

function applyCustomPatternsOnly(
  input: string,
  stats: SanitizeStats,
  customPatterns: readonly RegExp[],
): string {
  if (customPatterns.length === 0) return input;
  let out = input;
  for (const pat of customPatterns) {
    pat.lastIndex = 0;
    if (pat.test(out)) {
      stats.redactedFields += 1;
      pat.lastIndex = 0;
      out = out.replace(pat, REDACTED_MARKER);
    }
  }
  return out;
}

function utf8ByteLength(str: string): number {
  return textEncoder.encode(str).length;
}

function floorUtf16Boundary(str: string, idx: number): number {
  let cut = Math.max(0, Math.min(idx, str.length));
  if (cut > 0 && cut < str.length) {
    const code = str.charCodeAt(cut - 1);
    if (code >= 0xd800 && code <= 0xdbff) {
      cut -= 1;
    }
  }
  return cut;
}

function sliceByUtf8Bytes(str: string, maxBytes: number): string {
  if (maxBytes <= 0 || str.length === 0) return "";
  let lo = 0;
  let hi = str.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    const safeMid = floorUtf16Boundary(str, mid);
    if (safeMid === lo) {
      break;
    }
    if (utf8ByteLength(str.slice(0, safeMid)) <= maxBytes) {
      lo = safeMid;
    } else {
      hi = safeMid - 1;
    }
  }
  return str.slice(0, floorUtf16Boundary(str, lo));
}

export function capPayload(
  val: JsonValue,
  maxBytes: number,
  stats: SanitizeStats,
): JsonValue {
  if (val === null) return null;
  const serialized = JSON.stringify(val);
  const originalBytes = utf8ByteLength(serialized);
  if (originalBytes <= maxBytes) {
    return val;
  }
  stats.truncatedPayloads += 1;

  if (maxBytes < 64) {
    const minimal: JsonObject = { _truncated: true };
    if (utf8ByteLength(JSON.stringify(minimal)) <= maxBytes) {
      return minimal;
    }
    return null;
  }

  let cutBytes = Math.max(0, maxBytes - 96);
  let preview = sliceByUtf8Bytes(serialized, cutBytes);
  while (true) {
    const candidate: JsonObject = {
      _truncated: true,
      _original_bytes: originalBytes,
      _preview: preview,
    };
    const encodedLen = utf8ByteLength(JSON.stringify(candidate));
    if (encodedLen <= maxBytes) {
      return candidate;
    }
    if (preview.length === 0) {
      break;
    }
    const excess = encodedLen - maxBytes;
    const nextMaxUtf8 = Math.max(
      0,
      utf8ByteLength(preview) - Math.max(1, Math.min(excess, Math.ceil(utf8ByteLength(preview) / 2))),
    );
    const nextPreview = sliceByUtf8Bytes(preview, nextMaxUtf8);
    if (nextPreview.length >= preview.length) {
      preview = preview.slice(0, floorUtf16Boundary(preview, preview.length - 1));
    } else {
      preview = nextPreview;
    }
  }

  const fallback: JsonObject = {
    _truncated: true,
    _original_bytes: originalBytes,
  };
  if (utf8ByteLength(JSON.stringify(fallback)) <= maxBytes) {
    return fallback;
  }
  return { _truncated: true };
}

function sanitizeLinkedSpanIds(spanId: string, linked: readonly string[]): string[] {
  const cleaned: string[] = [];
  const seen = new Set<string>();
  for (const raw of linked) {
    const trimmed = raw.trim();
    if (!trimmed || trimmed === spanId) continue;
    const bounded = sliceByUtf8Bytes(trimmed, MAX_SPAN_ID_BYTES);
    if (bounded && !seen.has(bounded)) {
      seen.add(bounded);
      cleaned.push(bounded);
      if (cleaned.length >= MAX_LINKED_SPANS_PER_SPAN) break;
    }
  }
  return cleaned;
}

function applyFieldHook(
  raw: JsonValue,
  field: string,
  spanType: SpanType | undefined,
  policy: ResolvedPrivacyPolicy,
): JsonValue {
  if (!policy.redactHook) return raw;
  return toJsonValue(policy.redactHook(raw, { field, spanType }));
}

export function sanitizeTrace(
  trace: AgentTrace,
  policy: ResolvedPrivacyPolicy,
): SanitizeStats {
  const stats: SanitizeStats = {
    redactedFields: 0,
    truncatedPayloads: 0,
    droppedSpans: 0,
  };

  if (!policy.captureUserId) {
    if (trace.user_id !== null) {
      stats.redactedFields += 1;
    }
    trace.user_id = null;
  }

  if (!policy.captureInputs) {
    if (trace.input !== null) stats.redactedFields += 1;
    trace.input = null;
  } else {
    let v = applyFieldHook(toJsonValue(trace.input), "trace.input", undefined, policy);
    v = redactValue(v, policy, stats);
    trace.input = capPayload(v, MAX_PAYLOAD_BYTES, stats);
  }

  if (!policy.captureOutputs) {
    if (trace.output !== null) stats.redactedFields += 1;
    trace.output = null;
  } else {
    let v = applyFieldHook(toJsonValue(trace.output), "trace.output", undefined, policy);
    v = redactValue(v, policy, stats);
    trace.output = capPayload(v, MAX_PAYLOAD_BYTES, stats);
  }

  let meta = applyFieldHook(toJsonValue(trace.metadata), "trace.metadata", undefined, policy);
  meta = redactValue(meta, policy, stats);
  trace.metadata = capPayload(meta, MAX_PAYLOAD_BYTES, stats);

  if (trace.error_message !== null) {
    trace.error_message = policy.redactPii
      ? redactPiiString(trace.error_message, stats, policy.redactPatterns)
      : applyCustomPatternsOnly(trace.error_message, stats, policy.redactPatterns);
  }

  if (trace.tags.length > MAX_TAGS_PER_TRACE) {
    trace.tags = trace.tags.slice(0, MAX_TAGS_PER_TRACE);
  }

  trace.input_tokens = Math.max(0, Math.trunc(trace.input_tokens));
  trace.output_tokens = Math.max(0, Math.trunc(trace.output_tokens));
  trace.cached_tokens = Math.max(0, Math.trunc(trace.cached_tokens));
  trace.total_tokens = Math.max(0, Math.trunc(trace.total_tokens));
  syncTraceCost(trace);
  return stats;
}

export function sanitizeSpan(
  span: AgentSpan,
  policy: ResolvedPrivacyPolicy,
): SanitizeStats {
  const stats: SanitizeStats = {
    redactedFields: 0,
    truncatedPayloads: 0,
    droppedSpans: 0,
  };

  const allowInput =
    span.span_type === "llm"
      ? policy.capturePrompts && policy.captureInputs
      : span.span_type === "tool"
        ? policy.captureToolArgs && policy.captureInputs
        : policy.captureInputs;

  const allowOutput =
    span.span_type === "llm"
      ? policy.captureResponses && policy.captureOutputs
      : span.span_type === "tool"
        ? policy.captureToolResults && policy.captureOutputs
        : policy.captureOutputs;

  if (!allowInput) {
    if (span.input !== null) stats.redactedFields += 1;
    span.input = null;
  } else {
    let v = applyFieldHook(toJsonValue(span.input), "span.input", span.span_type, policy);
    v = redactValue(v, policy, stats);
    span.input = capPayload(v, MAX_PAYLOAD_BYTES, stats);
  }

  if (!allowOutput) {
    if (span.output !== null) stats.redactedFields += 1;
    span.output = null;
  } else {
    let v = applyFieldHook(toJsonValue(span.output), "span.output", span.span_type, policy);
    v = redactValue(v, policy, stats);
    span.output = capPayload(v, MAX_PAYLOAD_BYTES, stats);
  }

  let attrs = toJsonValue(span.attributes);
  if (attrs !== null && typeof attrs === "object" && !Array.isArray(attrs)) {
    const filtered: JsonObject = {};
    let count = 0;
    for (const [k, v] of Object.entries(attrs)) {
      if (policy.excludeAttributesLower.has(k.toLowerCase())) {
        continue;
      }
      filtered[k] = v;
      count += 1;
      if (count >= MAX_ATTRIBUTES_PER_SPAN) break;
    }
    attrs = filtered;
  }
  attrs = applyFieldHook(attrs, "span.attributes", span.span_type, policy);
  attrs = redactValue(attrs, policy, stats);
  span.attributes = capPayload(attrs, MAX_PAYLOAD_BYTES, stats);

  let meta = applyFieldHook(toJsonValue(span.metadata), "span.metadata", span.span_type, policy);
  meta = redactValue(meta, policy, stats);
  span.metadata = capPayload(meta, MAX_PAYLOAD_BYTES, stats);

  if (span.events.length > MAX_EVENTS_PER_SPAN) {
    span.events = span.events.slice(0, MAX_EVENTS_PER_SPAN);
  }
  for (const ev of span.events) {
    let evAttrs = applyFieldHook(
      toJsonValue(ev.attributes),
      "span.event.attributes",
      span.span_type,
      policy,
    );
    evAttrs = redactValue(evAttrs, policy, stats);
    ev.attributes = capPayload(evAttrs, MAX_PAYLOAD_BYTES, stats);
  }

  if (span.error_message !== null) {
    span.error_message = policy.redactPii
      ? redactPiiString(span.error_message, stats, policy.redactPatterns)
      : applyCustomPatternsOnly(span.error_message, stats, policy.redactPatterns);
  }
  if (span.error_stack !== null) {
    span.error_stack = policy.redactPii
      ? redactPiiString(span.error_stack, stats, policy.redactPatterns)
      : applyCustomPatternsOnly(span.error_stack, stats, policy.redactPatterns);
  }

  span.linked_span_ids = sanitizeLinkedSpanIds(span.span_id, span.linked_span_ids);
  span.input_tokens = Math.max(0, Math.trunc(span.input_tokens));
  span.output_tokens = Math.max(0, Math.trunc(span.output_tokens));
  span.cached_tokens = Math.max(0, Math.trunc(span.cached_tokens));
  span.total_tokens = Math.max(0, Math.trunc(span.total_tokens));
  syncSpanCost(span);
  return stats;
}

function computeSpanDepthsForIndices(spans: AgentSpan[], indices: readonly number[]): void {
  const parentMap = new Map<string, string | null>();
  for (const idx of indices) {
    const sp = spans[idx]!;
    parentMap.set(sp.span_id, sp.parent_span_id);
  }

  const broken = new Set<string>();
  for (const idx of indices) {
    const startId = spans[idx]!.span_id;
    const seen = new Set<string>([startId]);
    let prevId = startId;
    let cur = parentMap.get(prevId) ?? null;
    while (cur !== null) {
      if (seen.has(cur)) {
        parentMap.set(prevId, null);
        broken.add(prevId);
        break;
      }
      seen.add(cur);
      prevId = cur;
      cur = parentMap.get(cur) ?? null;
    }
  }

  for (const idx of indices) {
    const sp = spans[idx]!;
    if (broken.has(sp.span_id)) {
      sp.parent_span_id = null;
    }
    let depth = 0;
    let cur = parentMap.get(sp.span_id) ?? null;
    let steps = 0;
    while (cur !== null) {
      depth += 1;
      steps += 1;
      if (depth >= MAX_SPAN_DEPTH || steps > indices.length) {
        break;
      }
      cur = parentMap.get(cur) ?? null;
    }
    sp.depth = Math.min(depth, MAX_SPAN_DEPTH);
  }
}

export function computeSpanDepths(spans: AgentSpan[]): void {
  const indices = spans.map((_, idx) => idx);
  computeSpanDepthsForIndices(spans, indices);
}

export function sanitizeBatch(
  batch: TraceIngestBatch,
  policy: ResolvedPrivacyPolicy,
): SanitizeStats {
  const stats: SanitizeStats = {
    redactedFields: 0,
    truncatedPayloads: 0,
    droppedSpans: 0,
  };

  const countsPerTrace = new Map<string, number>();
  const retainedSpans: AgentSpan[] = [];
  for (const span of batch.spans) {
    const count = countsPerTrace.get(span.trace_id) ?? 0;
    if (count >= MAX_SPANS_PER_TRACE) {
      stats.droppedSpans += 1;
      continue;
    }
    countsPerTrace.set(span.trace_id, count + 1);
    const s = sanitizeSpan(span, policy);
    stats.redactedFields += s.redactedFields;
    stats.truncatedPayloads += s.truncatedPayloads;
    stats.droppedSpans += s.droppedSpans;
    retainedSpans.push(span);
  }
  batch.spans = retainedSpans;

  const indicesByTrace = new Map<string, number[]>();
  for (let i = 0; i < batch.spans.length; i++) {
    const tid = batch.spans[i]!.trace_id;
    const list = indicesByTrace.get(tid);
    if (list !== undefined) {
      list.push(i);
    } else {
      indicesByTrace.set(tid, [i]);
    }
  }

  const costByTrace = new Map<string, number>();
  for (const [tid, indices] of indicesByTrace.entries()) {
    computeSpanDepthsForIndices(batch.spans, indices);
    let sumNanos = 0;
    for (const idx of indices) {
      sumNanos += Math.max(0, batch.spans[idx]!.cost_nanos);
    }
    costByTrace.set(tid, sumNanos);
  }

  for (const trace of batch.traces) {
    const s = sanitizeTrace(trace, policy);
    stats.redactedFields += s.redactedFields;
    stats.truncatedPayloads += s.truncatedPayloads;
    stats.droppedSpans += s.droppedSpans;
    const spanCostSum = costByTrace.get(trace.trace_id);
    if (spanCostSum !== undefined && spanCostSum > 0) {
      trace.total_cost_nanos = spanCostSum;
      trace.total_cost_usd = costNanosToUsd(spanCostSum);
    }
  }

  return stats;
}
