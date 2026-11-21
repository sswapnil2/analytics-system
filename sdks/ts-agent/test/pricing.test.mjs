import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  costNanosToUsd,
  estimateCostNanos,
  estimateCostNanosBigInt,
  estimateCostUsd,
  syncSpanCost,
  syncTraceCost,
  usdToCostNanos,
} from "../dist/index.js";

describe("Pricing & Nano-Dollar Arithmetic", () => {
  it("computes exact BigInt nano-dollar cost for gpt-4o with cached tokens", () => {
    // gpt-4o: input=2.50/1M (2500 nanos/tok), output=10.00/1M (10000 nanos/tok), cached=1.25/1M (1250 nanos/tok)
    // 1000 input (200 cached => 800 uncached) + 500 output:
    // 800 * 2500 + 200 * 1250 + 500 * 10000 = 2,000,000 + 250,000 + 5,000,000 = 7,250,000 nanos
    const nanos = estimateCostNanosBigInt("openai", "gpt-4o-2024-11-20", 1000, 500, 200);
    assert.equal(nanos, 7_250_000n);
    assert.equal(estimateCostNanos("openai", "gpt-4o", 1000, 500, 200), 7_250_000);
    assert.equal(estimateCostUsd("openai", "gpt-4o", 1000, 500, 200), 0.00725);
  });

  it("picks longest matching model prefix (gpt-4o-mini over gpt-4o)", () => {
    // gpt-4o-mini: 0.15 / 0.60 / 0.075 => 1000 uncached * 150 + 1000 output * 600 = 750,000
    assert.equal(estimateCostNanos("openai", "gpt-4o-mini", 1000, 1000, 0), 750_000);
    // claude-sonnet-4: 3.00 / 15.00 / 0.30
    assert.equal(estimateCostNanos("anthropic", "claude-sonnet-4-20250514", 1000, 200, 500), 4_650_000);
  });

  it("matches both google and gemini provider aliases", () => {
    const a = estimateCostNanos("google", "gemini-2.5-pro", 1000, 100, 0);
    const b = estimateCostNanos("gemini", "gemini-2.5-pro", 1000, 100, 0);
    assert.equal(a, b);
    assert.equal(a, 2_250_000);
  });

  it("applies custom pricing rules before builtin table", () => {
    const custom = [
      {
        provider: "openai",
        model_pattern: "gpt-4o",
        input_per_1m_usd: 1.0,
        output_per_1m_usd: 2.0,
        cached_input_per_1m_usd: 0.5,
      },
    ];
    assert.equal(estimateCostNanos("openai", "gpt-4o", 1000, 500, 0, custom), 2_000_000);
  });

  it("clamps cached > input and handles usdToCostNanos edge cases", () => {
    // cached (1500) > input (1000) => uncached=0, cached=1500 * 1250 = 1,875,000
    assert.equal(estimateCostNanos("openai", "gpt-4o", 1000, 0, 1500), 1_875_000);
    assert.equal(estimateCostNanos("openai", "unknown-model-xyz", 1000, 1000, 0), 0);
    assert.equal(usdToCostNanos(Number.NaN), 0);
    assert.equal(usdToCostNanos(-5), 0);
    assert.equal(usdToCostNanos(Number.NEGATIVE_INFINITY), 0);
    assert.equal(usdToCostNanos(0.001234567), 1_234_567);
    assert.equal(costNanosToUsd(1_234_567), 0.001234567);
  });

  it("reconciles span and trace costs with cost_nanos authoritative", () => {
    const span = {
      cost_nanos: 5_000_000,
      cost_usd: 99.0,
    };
    syncSpanCost(span);
    assert.equal(span.cost_nanos, 5_000_000);
    assert.equal(span.cost_usd, 0.005);

    const trace = {
      total_cost_nanos: 0,
      total_cost_usd: 0.25,
    };
    syncTraceCost(trace);
    assert.equal(trace.total_cost_nanos, 250_000_000);
    assert.equal(trace.total_cost_usd, 0.25);
  });
});
