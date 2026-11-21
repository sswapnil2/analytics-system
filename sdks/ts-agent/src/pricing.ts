import type { AgentSpan, AgentTrace, ModelPricingRule } from "./types.js";

export const NANOS_PER_USD = 1_000_000_000;
export const NANOS_PER_USD_BIGINT = 1_000_000_000n;
const I64_MAX_BIGINT = 9_223_372_036_854_775_807n;
const I64_MAX_FLOAT = 9_223_372_036_854_775_807;

export interface BuiltinPricing {
  readonly provider: string;
  readonly model_prefix: string;
  readonly input_per_1m_usd: number;
  readonly output_per_1m_usd: number;
  readonly cached_input_per_1m_usd: number;
}

/**
 * Built-in reference pricing table (USD per 1M tokens), matching `crates/pulse-agent/src/pricing.rs`.
 */
export const BUILTIN_PRICING: readonly BuiltinPricing[] = [
  // OpenAI
  {
    provider: "openai",
    model_prefix: "gpt-4.1-mini",
    input_per_1m_usd: 0.4,
    output_per_1m_usd: 1.6,
    cached_input_per_1m_usd: 0.1,
  },
  {
    provider: "openai",
    model_prefix: "gpt-4.1-nano",
    input_per_1m_usd: 0.1,
    output_per_1m_usd: 0.4,
    cached_input_per_1m_usd: 0.025,
  },
  {
    provider: "openai",
    model_prefix: "gpt-4.1",
    input_per_1m_usd: 2.0,
    output_per_1m_usd: 8.0,
    cached_input_per_1m_usd: 0.5,
  },
  {
    provider: "openai",
    model_prefix: "gpt-4o-mini",
    input_per_1m_usd: 0.15,
    output_per_1m_usd: 0.6,
    cached_input_per_1m_usd: 0.075,
  },
  {
    provider: "openai",
    model_prefix: "gpt-4o",
    input_per_1m_usd: 2.5,
    output_per_1m_usd: 10.0,
    cached_input_per_1m_usd: 1.25,
  },
  {
    provider: "openai",
    model_prefix: "o3-mini",
    input_per_1m_usd: 1.1,
    output_per_1m_usd: 4.4,
    cached_input_per_1m_usd: 0.55,
  },
  {
    provider: "openai",
    model_prefix: "o3",
    input_per_1m_usd: 2.0,
    output_per_1m_usd: 8.0,
    cached_input_per_1m_usd: 0.5,
  },
  {
    provider: "openai",
    model_prefix: "o4-mini",
    input_per_1m_usd: 1.1,
    output_per_1m_usd: 4.4,
    cached_input_per_1m_usd: 0.275,
  },
  {
    provider: "openai",
    model_prefix: "text-embedding-3-large",
    input_per_1m_usd: 0.13,
    output_per_1m_usd: 0.0,
    cached_input_per_1m_usd: 0.13,
  },
  {
    provider: "openai",
    model_prefix: "text-embedding-3-small",
    input_per_1m_usd: 0.02,
    output_per_1m_usd: 0.0,
    cached_input_per_1m_usd: 0.02,
  },
  // Anthropic
  {
    provider: "anthropic",
    model_prefix: "claude-opus-4",
    input_per_1m_usd: 15.0,
    output_per_1m_usd: 75.0,
    cached_input_per_1m_usd: 1.5,
  },
  {
    provider: "anthropic",
    model_prefix: "claude-sonnet-4",
    input_per_1m_usd: 3.0,
    output_per_1m_usd: 15.0,
    cached_input_per_1m_usd: 0.3,
  },
  {
    provider: "anthropic",
    model_prefix: "claude-3-7-sonnet",
    input_per_1m_usd: 3.0,
    output_per_1m_usd: 15.0,
    cached_input_per_1m_usd: 0.3,
  },
  {
    provider: "anthropic",
    model_prefix: "claude-3-5-sonnet",
    input_per_1m_usd: 3.0,
    output_per_1m_usd: 15.0,
    cached_input_per_1m_usd: 0.3,
  },
  {
    provider: "anthropic",
    model_prefix: "claude-3-5-haiku",
    input_per_1m_usd: 0.8,
    output_per_1m_usd: 4.0,
    cached_input_per_1m_usd: 0.08,
  },
  // Google Gemini
  {
    provider: "google",
    model_prefix: "gemini-2.5-pro",
    input_per_1m_usd: 1.25,
    output_per_1m_usd: 10.0,
    cached_input_per_1m_usd: 0.31,
  },
  {
    provider: "google",
    model_prefix: "gemini-2.5-flash-lite",
    input_per_1m_usd: 0.1,
    output_per_1m_usd: 0.4,
    cached_input_per_1m_usd: 0.025,
  },
  {
    provider: "google",
    model_prefix: "gemini-2.5-flash",
    input_per_1m_usd: 0.3,
    output_per_1m_usd: 2.5,
    cached_input_per_1m_usd: 0.075,
  },
  {
    provider: "google",
    model_prefix: "gemini-2.0-flash",
    input_per_1m_usd: 0.1,
    output_per_1m_usd: 0.4,
    cached_input_per_1m_usd: 0.025,
  },
];

export function costNanosToUsd(nanos: number | bigint): number {
  return Number(nanos) / NANOS_PER_USD;
}

export function usdToCostNanosBigInt(usd: number): bigint {
  if (!Number.isFinite(usd)) {
    return usd > 0 ? I64_MAX_BIGINT : 0n;
  }
  if (usd <= 0) {
    return 0n;
  }
  const scaled = Math.round(usd * NANOS_PER_USD);
  if (scaled >= I64_MAX_FLOAT) {
    return I64_MAX_BIGINT;
  }
  return BigInt(scaled);
}

export function usdToCostNanos(usd: number): number {
  return Number(usdToCostNanosBigInt(usd));
}

function clampTokenBigInt(val: number): bigint {
  if (!Number.isFinite(val) || val <= 0) {
    return 0n;
  }
  return BigInt(Math.trunc(val));
}

function clampI64BigInt(val: bigint): bigint {
  if (val <= 0n) return 0n;
  if (val >= I64_MAX_BIGINT) return I64_MAX_BIGINT;
  return val;
}

function calcBuiltinTokenCostNanos(
  inputTokens: number,
  outputTokens: number,
  cachedTokens: number,
  inputRatePer1m: number,
  outputRatePer1m: number,
  cachedRatePer1m: number,
): bigint {
  const cached = clampTokenBigInt(cachedTokens);
  const rawInput = clampTokenBigInt(inputTokens);
  const uncachedInput = rawInput > cached ? rawInput - cached : 0n;
  const out = clampTokenBigInt(outputTokens);

  const inNanosPerTok = BigInt(Math.round(inputRatePer1m * 1_000));
  const cachedNanosPerTok = BigInt(Math.round(cachedRatePer1m * 1_000));
  const outNanosPerTok = BigInt(Math.round(outputRatePer1m * 1_000));

  const total =
    uncachedInput * inNanosPerTok +
    cached * cachedNanosPerTok +
    out * outNanosPerTok;

  return clampI64BigInt(total);
}

function calcCustomTokenCostNanos(
  inputTokens: number,
  outputTokens: number,
  cachedTokens: number,
  inputRatePer1m: number,
  outputRatePer1m: number,
  cachedRatePer1m: number,
): bigint {
  const cached = Math.max(0, Number.isFinite(cachedTokens) ? Math.trunc(cachedTokens) : 0);
  const rawInput = Math.max(0, Number.isFinite(inputTokens) ? Math.trunc(inputTokens) : 0);
  const uncachedInput = Math.max(0, rawInput - cached);
  const out = Math.max(0, Number.isFinite(outputTokens) ? Math.trunc(outputTokens) : 0);

  const usd =
    (uncachedInput * (Number.isFinite(inputRatePer1m) && inputRatePer1m > 0 ? inputRatePer1m : 0) +
      cached * (Number.isFinite(cachedRatePer1m) && cachedRatePer1m > 0 ? cachedRatePer1m : 0) +
      out * (Number.isFinite(outputRatePer1m) && outputRatePer1m > 0 ? outputRatePer1m : 0)) /
    1_000_000;

  return usdToCostNanosBigInt(usd);
}

export function estimateCostNanos(
  provider: string | null | undefined,
  model: string | null | undefined,
  inputTokens: number,
  outputTokens: number,
  cachedTokens: number = 0,
  customRules: readonly ModelPricingRule[] = [],
): number {
  return Number(
    estimateCostNanosBigInt(
      provider,
      model,
      inputTokens,
      outputTokens,
      cachedTokens,
      customRules,
    ),
  );
}

export function estimateCostNanosBigInt(
  provider: string | null | undefined,
  model: string | null | undefined,
  inputTokens: number,
  outputTokens: number,
  cachedTokens: number = 0,
  customRules: readonly ModelPricingRule[] = [],
): bigint {
  const rawModel = model?.trim() ?? "";
  if (rawModel.length === 0) {
    return 0n;
  }
  const modelLc = rawModel.toLowerCase();
  const providerLc = provider?.trim().toLowerCase() ?? "";

  for (const rule of customRules) {
    const pat = rule.model_pattern.trim().toLowerCase();
    if (pat.length === 0) continue;
    const ruleProv = rule.provider.trim().toLowerCase();
    const provOk =
      providerLc.length === 0 || ruleProv.length === 0 || ruleProv === providerLc;
    if (provOk && (modelLc === pat || modelLc.startsWith(pat))) {
      return calcCustomTokenCostNanos(
        inputTokens,
        outputTokens,
        cachedTokens,
        rule.input_per_1m_usd,
        rule.output_per_1m_usd,
        rule.cached_input_per_1m_usd,
      );
    }
  }

  let best: BuiltinPricing | null = null;
  for (const entry of BUILTIN_PRICING) {
    if (modelLc.startsWith(entry.model_prefix)) {
      const provOk =
        providerLc.length === 0 ||
        providerLc === entry.provider ||
        providerLc.includes(entry.provider) ||
        (entry.provider === "google" && providerLc === "gemini");
      if (provOk && (best === null || entry.model_prefix.length > best.model_prefix.length)) {
        best = entry;
      }
    }
  }

  if (best === null) {
    return 0n;
  }

  return calcBuiltinTokenCostNanos(
    inputTokens,
    outputTokens,
    cachedTokens,
    best.input_per_1m_usd,
    best.output_per_1m_usd,
    best.cached_input_per_1m_usd,
  );
}

export function estimateCostUsd(
  provider: string | null | undefined,
  model: string | null | undefined,
  inputTokens: number,
  outputTokens: number,
  cachedTokens: number = 0,
  customRules: readonly ModelPricingRule[] = [],
): number {
  return costNanosToUsd(
    estimateCostNanosBigInt(
      provider,
      model,
      inputTokens,
      outputTokens,
      cachedTokens,
      customRules,
    ),
  );
}

export function syncSpanCost(span: AgentSpan): void {
  if (!Number.isFinite(span.cost_nanos) || span.cost_nanos < 0) {
    span.cost_nanos = 0;
  } else {
    span.cost_nanos = Math.trunc(span.cost_nanos);
  }
  if (span.cost_nanos === 0 && Number.isFinite(span.cost_usd) && span.cost_usd > 0) {
    span.cost_nanos = usdToCostNanos(span.cost_usd);
  }
  if (span.cost_nanos > 0 || !Number.isFinite(span.cost_usd) || span.cost_usd < 0) {
    span.cost_usd = costNanosToUsd(span.cost_nanos);
  }
}

export function syncTraceCost(trace: AgentTrace): void {
  if (!Number.isFinite(trace.total_cost_nanos) || trace.total_cost_nanos < 0) {
    trace.total_cost_nanos = 0;
  } else {
    trace.total_cost_nanos = Math.trunc(trace.total_cost_nanos);
  }
  if (
    trace.total_cost_nanos === 0 &&
    Number.isFinite(trace.total_cost_usd) &&
    trace.total_cost_usd > 0
  ) {
    trace.total_cost_nanos = usdToCostNanos(trace.total_cost_usd);
  }
  if (
    trace.total_cost_nanos > 0 ||
    !Number.isFinite(trace.total_cost_usd) ||
    trace.total_cost_usd < 0
  ) {
    trace.total_cost_usd = costNanosToUsd(trace.total_cost_nanos);
  }
}
