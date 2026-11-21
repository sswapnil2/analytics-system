"""Token pricing and nano-dollar ($10^-9 USD) reconciliation matching crates/pulse-agent/src/pricing.rs."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Optional, Sequence

from .model import AgentSpan, AgentTrace, ModelPricingRule

NANOS_PER_USD = 1_000_000_000
_I64_MAX = (1 << 63) - 1


@dataclass(frozen=True)
class BuiltinPricing:
    provider: str
    model_prefix: str
    input_per_1m_usd: float
    output_per_1m_usd: float
    cached_input_per_1m_usd: float


BUILTIN_PRICING: tuple[BuiltinPricing, ...] = (
    # OpenAI
    BuiltinPricing("openai", "gpt-4.1-mini", 0.40, 1.60, 0.10),
    BuiltinPricing("openai", "gpt-4.1-nano", 0.10, 0.40, 0.025),
    BuiltinPricing("openai", "gpt-4.1", 2.00, 8.00, 0.50),
    BuiltinPricing("openai", "gpt-4o-mini", 0.15, 0.60, 0.075),
    BuiltinPricing("openai", "gpt-4o", 2.50, 10.00, 1.25),
    BuiltinPricing("openai", "o3-mini", 1.10, 4.40, 0.55),
    BuiltinPricing("openai", "o3", 2.00, 8.00, 0.50),
    BuiltinPricing("openai", "o4-mini", 1.10, 4.40, 0.275),
    BuiltinPricing("openai", "text-embedding-3-large", 0.13, 0.0, 0.13),
    BuiltinPricing("openai", "text-embedding-3-small", 0.02, 0.0, 0.02),
    # Anthropic
    BuiltinPricing("anthropic", "claude-opus-4", 15.00, 75.00, 1.50),
    BuiltinPricing("anthropic", "claude-sonnet-4", 3.00, 15.00, 0.30),
    BuiltinPricing("anthropic", "claude-3-7-sonnet", 3.00, 15.00, 0.30),
    BuiltinPricing("anthropic", "claude-3-5-sonnet", 3.00, 15.00, 0.30),
    BuiltinPricing("anthropic", "claude-3-5-haiku", 0.80, 4.00, 0.08),
    # Google Gemini
    BuiltinPricing("google", "gemini-2.5-pro", 1.25, 10.00, 0.31),
    BuiltinPricing("google", "gemini-2.5-flash-lite", 0.10, 0.40, 0.025),
    BuiltinPricing("google", "gemini-2.5-flash", 0.30, 2.50, 0.075),
    BuiltinPricing("google", "gemini-2.0-flash", 0.10, 0.40, 0.025),
)


def _rust_round_half_away_from_zero(val: float) -> float:
    """Matches Rust f64::round() (half away from zero) for finite numbers."""
    if not math.isfinite(val):
        return val
    if val >= float(_I64_MAX) or val <= float(-_I64_MAX):
        return math.trunc(val)
    return float(Decimal.from_float(val).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def cost_nanos_to_usd(nanos: int) -> float:
    return float(nanos) / float(NANOS_PER_USD)


def usd_to_cost_nanos(usd: float) -> int:
    if not math.isfinite(usd):
        if math.isinf(usd) and usd > 0.0:
            return _I64_MAX
        return 0
    if usd <= 0.0:
        return 0
    scaled = _rust_round_half_away_from_zero(usd * float(NANOS_PER_USD))
    if scaled >= float(_I64_MAX):
        return _I64_MAX
    return int(scaled)


def sync_span_cost(span: AgentSpan) -> None:
    if span.cost_nanos < 0:
        span.cost_nanos = 0
    if span.cost_nanos == 0 and math.isfinite(span.cost_usd) and span.cost_usd > 0.0:
        span.cost_nanos = usd_to_cost_nanos(span.cost_usd)
    if span.cost_nanos > 0 or not math.isfinite(span.cost_usd) or span.cost_usd < 0.0:
        span.cost_usd = cost_nanos_to_usd(span.cost_nanos)


def sync_trace_cost(trace: AgentTrace) -> None:
    if trace.total_cost_nanos < 0:
        trace.total_cost_nanos = 0
    if (
        trace.total_cost_nanos == 0
        and math.isfinite(trace.total_cost_usd)
        and trace.total_cost_usd > 0.0
    ):
        trace.total_cost_nanos = usd_to_cost_nanos(trace.total_cost_usd)
    if (
        trace.total_cost_nanos > 0
        or not math.isfinite(trace.total_cost_usd)
        or trace.total_cost_usd < 0.0
    ):
        trace.total_cost_usd = cost_nanos_to_usd(trace.total_cost_nanos)


def _calc_builtin_token_cost_nanos(
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    input_rate_per_1m: float,
    output_rate_per_1m: float,
    cached_rate_per_1m: float,
) -> int:
    cached = max(0, int(cached_tokens))
    uncached_input = max(0, max(0, int(input_tokens)) - cached)
    out = max(0, int(output_tokens))

    in_nanos_per_tok = int(_rust_round_half_away_from_zero(input_rate_per_1m * 1_000.0))
    cached_nanos_per_tok = int(_rust_round_half_away_from_zero(cached_rate_per_1m * 1_000.0))
    out_nanos_per_tok = int(_rust_round_half_away_from_zero(output_rate_per_1m * 1_000.0))

    total = (
        uncached_input * in_nanos_per_tok
        + cached * cached_nanos_per_tok
        + out * out_nanos_per_tok
    )
    return max(0, min(_I64_MAX, total))


def _calc_custom_token_cost_nanos(
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    input_rate_per_1m: float,
    output_rate_per_1m: float,
    cached_rate_per_1m: float,
) -> int:
    cached = max(0, int(cached_tokens))
    uncached_input = max(0, max(0, int(input_tokens)) - cached)
    out = max(0, int(output_tokens))

    def sanitize(r: float) -> float:
        return r if math.isfinite(r) and r > 0.0 else 0.0

    in_r = sanitize(input_rate_per_1m)
    out_r = sanitize(output_rate_per_1m)
    cached_r = sanitize(cached_rate_per_1m)

    nanos_f64 = ((uncached_input * in_r) + (cached * cached_r) + (out * out_r)) * 1_000.0
    if not math.isfinite(nanos_f64):
        if math.isinf(nanos_f64) and nanos_f64 > 0.0:
            return _I64_MAX
        return 0
    rounded = _rust_round_half_away_from_zero(nanos_f64)
    if rounded <= 0.0:
        return 0
    if rounded >= float(_I64_MAX):
        return _I64_MAX
    return int(rounded)


def estimate_cost_nanos(
    provider: Optional[str],
    model: Optional[str],
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
    custom_rules: Sequence[ModelPricingRule] = (),
) -> int:
    if not model:
        return 0
    raw_model = model.strip()
    if not raw_model:
        return 0
    model_lc = raw_model.lower()
    provider_lc = provider.strip().lower() if provider is not None else None

    for rule in custom_rules:
        pat = rule.model_pattern.strip().lower()
        if not pat:
            continue
        prov_ok = (
            provider_lc is None
            or provider_lc == ""
            or rule.provider == ""
            or rule.provider.lower() == provider_lc
        )
        if prov_ok and (model_lc == pat or model_lc.startswith(pat)):
            return _calc_custom_token_cost_nanos(
                input_tokens,
                output_tokens,
                cached_tokens,
                rule.input_per_1m_usd,
                rule.output_per_1m_usd,
                rule.cached_input_per_1m_usd,
            )

    best: Optional[BuiltinPricing] = None
    for entry in BUILTIN_PRICING:
        if model_lc.startswith(entry.model_prefix):
            prov_ok = (
                provider_lc is None
                or provider_lc == ""
                or provider_lc == entry.provider
                or entry.provider in provider_lc
                or (entry.provider == "google" and provider_lc == "gemini")
            )
            if prov_ok and (best is None or len(entry.model_prefix) > len(best.model_prefix)):
                best = entry

    if best is None:
        return 0
    return _calc_builtin_token_cost_nanos(
        input_tokens,
        output_tokens,
        cached_tokens,
        best.input_per_1m_usd,
        best.output_per_1m_usd,
        best.cached_input_per_1m_usd,
    )


def estimate_cost_usd(
    provider: Optional[str],
    model: Optional[str],
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
    custom_rules: Sequence[ModelPricingRule] = (),
) -> float:
    return cost_nanos_to_usd(
        estimate_cost_nanos(
            provider, model, input_tokens, output_tokens, cached_tokens, custom_rules
        )
    )
