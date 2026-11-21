import type { SamplingMode } from "./types.js";
import { xxh3_64 } from "./xxh3.js";

export interface SampleDecisionInput {
  traceId: string;
  sessionId?: string | null;
  sampleRate: number;
  samplingMode: SamplingMode;
  explicitSampled?: boolean;
}

export function clampSampleRate(rate: number | undefined): number {
  if (rate === undefined || !Number.isFinite(rate)) {
    return 1.0;
  }
  if (rate <= 0) return 0.0;
  if (rate >= 1) return 1.0;
  return rate;
}

export function shouldSample(input: SampleDecisionInput): boolean {
  if (input.explicitSampled !== undefined) {
    return input.explicitSampled;
  }
  const rate = clampSampleRate(input.sampleRate);
  if (rate <= 0) {
    return false;
  }
  if (rate >= 1) {
    return true;
  }
  if (input.samplingMode === "probabilistic") {
    return Math.random() < rate;
  }
  const key = input.sessionId ?? input.traceId;
  const threshold = Math.round(rate * 10_000);
  const bucket = Number(xxh3_64(key) % 10_000n);
  return bucket < threshold;
}
