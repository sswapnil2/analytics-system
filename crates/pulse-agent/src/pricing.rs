use crate::model::{AgentSpan, AgentTrace, ModelPricingRule};

const NANOS_PER_USD_F64: f64 = crate::NANOS_PER_USD as f64;

#[derive(Debug, Clone, Copy)]
pub struct BuiltinPricing {
    pub provider: &'static str,
    pub model_prefix: &'static str,
    pub input_per_1m_usd: f64,
    pub output_per_1m_usd: f64,
    pub cached_input_per_1m_usd: f64,
}

/// Built-in reference pricing table (USD per 1M tokens).
/// Projects can override or extend any entry via `agent_model_pricing`.
pub const BUILTIN_PRICING: &[BuiltinPricing] = &[
    // OpenAI
    BuiltinPricing {
        provider: "openai",
        model_prefix: "gpt-4.1-mini",
        input_per_1m_usd: 0.40,
        output_per_1m_usd: 1.60,
        cached_input_per_1m_usd: 0.10,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "gpt-4.1-nano",
        input_per_1m_usd: 0.10,
        output_per_1m_usd: 0.40,
        cached_input_per_1m_usd: 0.025,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "gpt-4.1",
        input_per_1m_usd: 2.00,
        output_per_1m_usd: 8.00,
        cached_input_per_1m_usd: 0.50,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "gpt-4o-mini",
        input_per_1m_usd: 0.15,
        output_per_1m_usd: 0.60,
        cached_input_per_1m_usd: 0.075,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "gpt-4o",
        input_per_1m_usd: 2.50,
        output_per_1m_usd: 10.00,
        cached_input_per_1m_usd: 1.25,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "o3-mini",
        input_per_1m_usd: 1.10,
        output_per_1m_usd: 4.40,
        cached_input_per_1m_usd: 0.55,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "o3",
        input_per_1m_usd: 2.00,
        output_per_1m_usd: 8.00,
        cached_input_per_1m_usd: 0.50,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "o4-mini",
        input_per_1m_usd: 1.10,
        output_per_1m_usd: 4.40,
        cached_input_per_1m_usd: 0.275,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "text-embedding-3-large",
        input_per_1m_usd: 0.13,
        output_per_1m_usd: 0.0,
        cached_input_per_1m_usd: 0.13,
    },
    BuiltinPricing {
        provider: "openai",
        model_prefix: "text-embedding-3-small",
        input_per_1m_usd: 0.02,
        output_per_1m_usd: 0.0,
        cached_input_per_1m_usd: 0.02,
    },
    // Anthropic
    BuiltinPricing {
        provider: "anthropic",
        model_prefix: "claude-opus-4",
        input_per_1m_usd: 15.00,
        output_per_1m_usd: 75.00,
        cached_input_per_1m_usd: 1.50,
    },
    BuiltinPricing {
        provider: "anthropic",
        model_prefix: "claude-sonnet-4",
        input_per_1m_usd: 3.00,
        output_per_1m_usd: 15.00,
        cached_input_per_1m_usd: 0.30,
    },
    BuiltinPricing {
        provider: "anthropic",
        model_prefix: "claude-3-7-sonnet",
        input_per_1m_usd: 3.00,
        output_per_1m_usd: 15.00,
        cached_input_per_1m_usd: 0.30,
    },
    BuiltinPricing {
        provider: "anthropic",
        model_prefix: "claude-3-5-sonnet",
        input_per_1m_usd: 3.00,
        output_per_1m_usd: 15.00,
        cached_input_per_1m_usd: 0.30,
    },
    BuiltinPricing {
        provider: "anthropic",
        model_prefix: "claude-3-5-haiku",
        input_per_1m_usd: 0.80,
        output_per_1m_usd: 4.00,
        cached_input_per_1m_usd: 0.08,
    },
    // Google Gemini
    BuiltinPricing {
        provider: "google",
        model_prefix: "gemini-2.5-pro",
        input_per_1m_usd: 1.25,
        output_per_1m_usd: 10.00,
        cached_input_per_1m_usd: 0.31,
    },
    BuiltinPricing {
        provider: "google",
        model_prefix: "gemini-2.5-flash-lite",
        input_per_1m_usd: 0.10,
        output_per_1m_usd: 0.40,
        cached_input_per_1m_usd: 0.025,
    },
    BuiltinPricing {
        provider: "google",
        model_prefix: "gemini-2.5-flash",
        input_per_1m_usd: 0.30,
        output_per_1m_usd: 2.50,
        cached_input_per_1m_usd: 0.075,
    },
    BuiltinPricing {
        provider: "google",
        model_prefix: "gemini-2.0-flash",
        input_per_1m_usd: 0.10,
        output_per_1m_usd: 0.40,
        cached_input_per_1m_usd: 0.025,
    },
];

/// Converts integer nano-dollars ($10^{-9}$ USD) to USD `f64`.
#[inline]
pub fn cost_nanos_to_usd(nanos: i64) -> f64 {
    nanos as f64 / NANOS_PER_USD_F64
}

/// Converts USD `f64` to non-negative saturating nano-dollars ($10^{-9}$ USD).
/// `NaN`, negative values, and `-inf` map to `0`; `+inf` and overflow map to `i64::MAX`.
#[inline]
pub fn usd_to_cost_nanos(usd: f64) -> i64 {
    if !usd.is_finite() {
        return if usd.is_sign_positive() && usd.is_infinite() {
            i64::MAX
        } else {
            0
        };
    }
    if usd <= 0.0 {
        return 0;
    }
    let scaled = (usd * NANOS_PER_USD_F64).round();
    if scaled >= i64::MAX as f64 {
        i64::MAX
    } else {
        scaled as i64
    }
}

/// Synchronizes `span.cost_nanos` and `span.cost_usd` with `cost_nanos` authoritative when non-zero.
pub fn sync_span_cost(span: &mut AgentSpan) {
    if span.cost_nanos < 0 {
        span.cost_nanos = 0;
    }
    if span.cost_nanos == 0 && span.cost_usd.is_finite() && span.cost_usd > 0.0 {
        span.cost_nanos = usd_to_cost_nanos(span.cost_usd);
    }
    if span.cost_nanos > 0 || !span.cost_usd.is_finite() || span.cost_usd < 0.0 {
        span.cost_usd = cost_nanos_to_usd(span.cost_nanos);
    }
}

/// Synchronizes `trace.total_cost_nanos` and `trace.total_cost_usd` with `total_cost_nanos` authoritative when non-zero.
pub fn sync_trace_cost(trace: &mut AgentTrace) {
    if trace.total_cost_nanos < 0 {
        trace.total_cost_nanos = 0;
    }
    if trace.total_cost_nanos == 0 && trace.total_cost_usd.is_finite() && trace.total_cost_usd > 0.0
    {
        trace.total_cost_nanos = usd_to_cost_nanos(trace.total_cost_usd);
    }
    if trace.total_cost_nanos > 0 || !trace.total_cost_usd.is_finite() || trace.total_cost_usd < 0.0
    {
        trace.total_cost_usd = cost_nanos_to_usd(trace.total_cost_nanos);
    }
}

/// Computes integer nano-dollar cost ($10^{-9}$ USD) from token counts using project overrides first,
/// then exact `i128` integer arithmetic over built-in pricing.
/// `input_tokens` may include `cached_tokens`; billable uncached input tokens are clamped to `>= 0`.
pub fn estimate_cost_nanos(
    provider: Option<&str>,
    model: Option<&str>,
    input_tokens: i64,
    output_tokens: i64,
    cached_tokens: i64,
    custom_rules: &[ModelPricingRule],
) -> i64 {
    let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) else {
        return 0;
    };
    let model_lc = raw_model.to_ascii_lowercase();
    let provider_lc = provider.map(|p| p.trim().to_ascii_lowercase());

    // 1. Project-defined overrides (exact or prefix match)
    for rule in custom_rules {
        let pat = rule.model_pattern.trim().to_ascii_lowercase();
        if pat.is_empty() {
            continue;
        }
        let prov_ok = provider_lc.as_deref().is_none_or(|p| {
            p.is_empty() || rule.provider.is_empty() || rule.provider.eq_ignore_ascii_case(p)
        });
        if prov_ok && (model_lc == pat || model_lc.starts_with(&pat)) {
            return calc_custom_token_cost_nanos(
                input_tokens,
                output_tokens,
                cached_tokens,
                rule.input_per_1m_usd,
                rule.output_per_1m_usd,
                rule.cached_input_per_1m_usd,
            );
        }
    }

    // 2. Built-in reference table (longest matching prefix wins)
    let mut best: Option<&BuiltinPricing> = None;
    for entry in BUILTIN_PRICING {
        if model_lc.starts_with(entry.model_prefix) {
            let prov_ok = provider_lc.as_deref().is_none_or(|p| {
                p.is_empty()
                    || p == entry.provider
                    || p.contains(entry.provider)
                    || (entry.provider == "google" && p == "gemini")
            });
            if prov_ok && best.is_none_or(|b| entry.model_prefix.len() > b.model_prefix.len()) {
                best = Some(entry);
            }
        }
    }

    if let Some(p) = best {
        calc_builtin_token_cost_nanos(
            input_tokens,
            output_tokens,
            cached_tokens,
            p.input_per_1m_usd,
            p.output_per_1m_usd,
            p.cached_input_per_1m_usd,
        )
    } else {
        0
    }
}

/// Computes USD cost from token counts by delegating directly to `estimate_cost_nanos`.
pub fn estimate_cost_usd(
    provider: Option<&str>,
    model: Option<&str>,
    input_tokens: i64,
    output_tokens: i64,
    cached_tokens: i64,
    custom_rules: &[ModelPricingRule],
) -> f64 {
    cost_nanos_to_usd(estimate_cost_nanos(
        provider,
        model,
        input_tokens,
        output_tokens,
        cached_tokens,
        custom_rules,
    ))
}

/// Exact `i128` arithmetic for built-in rates (since 1 USD / 1M tokens = 1,000 nanos / token,
/// and all built-in rates have <= 3 decimal places).
fn calc_builtin_token_cost_nanos(
    input_tokens: i64,
    output_tokens: i64,
    cached_tokens: i64,
    input_rate_per_1m: f64,
    output_rate_per_1m: f64,
    cached_rate_per_1m: f64,
) -> i64 {
    let cached = cached_tokens.max(0);
    let uncached_input = input_tokens.max(0).saturating_sub(cached).max(0);
    let out = output_tokens.max(0);

    let in_nanos_per_tok = (input_rate_per_1m * 1_000.0).round() as i128;
    let cached_nanos_per_tok = (cached_rate_per_1m * 1_000.0).round() as i128;
    let out_nanos_per_tok = (output_rate_per_1m * 1_000.0).round() as i128;

    let total: i128 = (uncached_input as i128)
        .saturating_mul(in_nanos_per_tok)
        .saturating_add((cached as i128).saturating_mul(cached_nanos_per_tok))
        .saturating_add((out as i128).saturating_mul(out_nanos_per_tok));

    total.clamp(0, i64::MAX as i128) as i64
}

/// Saturating float-to-nanos calculation for arbitrary custom `ModelPricingRule` rates.
fn calc_custom_token_cost_nanos(
    input_tokens: i64,
    output_tokens: i64,
    cached_tokens: i64,
    input_rate_per_1m: f64,
    output_rate_per_1m: f64,
    cached_rate_per_1m: f64,
) -> i64 {
    let cached = cached_tokens.max(0);
    let uncached_input = input_tokens.max(0).saturating_sub(cached).max(0);
    let out = output_tokens.max(0);

    let sanitize = |r: f64| if r.is_finite() && r > 0.0 { r } else { 0.0 };
    let in_r = sanitize(input_rate_per_1m);
    let out_r = sanitize(output_rate_per_1m);
    let cached_r = sanitize(cached_rate_per_1m);

    let nanos_f64 =
        ((uncached_input as f64 * in_r) + (cached as f64 * cached_r) + (out as f64 * out_r))
            * 1_000.0;
    if !nanos_f64.is_finite() {
        return if nanos_f64.is_sign_positive() && nanos_f64.is_infinite() {
            i64::MAX
        } else {
            0
        };
    }
    let rounded = nanos_f64.round();
    if rounded <= 0.0 {
        0
    } else if rounded >= i64::MAX as f64 {
        i64::MAX
    } else {
        rounded as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_cost_with_cached_tokens_and_overrides() {
        let nanos = estimate_cost_nanos(
            Some("openai"),
            Some("gpt-4o-mini-2024-07-18"),
            10_000,
            2_000,
            4_000,
            &[],
        );
        // 6k * 150 + 4k * 75 + 2k * 600 = 900_000 + 300_000 + 1_200_000 = 2_400_000 nanos ($0.0024)
        assert_eq!(nanos, 2_400_000);
        let cost = estimate_cost_usd(
            Some("openai"),
            Some("gpt-4o-mini-2024-07-18"),
            10_000,
            2_000,
            4_000,
            &[],
        );
        assert!((cost - 0.0024).abs() < 1e-12, "got {cost}");

        let custom = vec![ModelPricingRule {
            id: "r1".into(),
            project_id: "p1".into(),
            provider: "openai".into(),
            model_pattern: "gpt-4o-mini".into(),
            input_per_1m_usd: 1.0,
            output_per_1m_usd: 2.0,
            cached_input_per_1m_usd: 0.5,
            updated_at_ms: 0,
        }];
        let override_nanos = estimate_cost_nanos(
            Some("openai"),
            Some("gpt-4o-mini"),
            1_000_000,
            500_000,
            0,
            &custom,
        );
        assert_eq!(override_nanos, 2_000_000_000);
        let override_cost = estimate_cost_usd(
            Some("openai"),
            Some("gpt-4o-mini"),
            1_000_000,
            500_000,
            0,
            &custom,
        );
        assert!((override_cost - 2.0).abs() < 1e-12, "got {override_cost}");
    }

    #[test]
    fn custom_rule_allows_empty_provider_filter() {
        let custom = vec![ModelPricingRule {
            id: "r2".into(),
            project_id: "p1".into(),
            provider: "custom-gw".into(),
            model_pattern: "my-model".into(),
            input_per_1m_usd: 2.0,
            output_per_1m_usd: 4.0,
            cached_input_per_1m_usd: 1.0,
            updated_at_ms: 0,
        }];
        let nanos = estimate_cost_nanos(Some(""), Some("my-model"), 1_000, 500, 0, &custom);
        assert_eq!(nanos, 4_000_000);
    }

    #[test]
    fn usd_to_cost_nanos_handles_edges_and_saturation() {
        assert_eq!(usd_to_cost_nanos(f64::NAN), 0);
        assert_eq!(usd_to_cost_nanos(f64::NEG_INFINITY), 0);
        assert_eq!(usd_to_cost_nanos(-1.25), 0);
        assert_eq!(usd_to_cost_nanos(0.0), 0);
        assert_eq!(usd_to_cost_nanos(f64::INFINITY), i64::MAX);
        assert_eq!(usd_to_cost_nanos(1e20), i64::MAX);
        assert_eq!(usd_to_cost_nanos(0.000_000_001), 1);
    }

    #[test]
    fn builtin_nanos_zero_drift_over_10k_spans() {
        let mut sum_nanos = 0i64;
        for _ in 0..10_000 {
            sum_nanos = sum_nanos.saturating_add(estimate_cost_nanos(
                Some("google"),
                Some("gemini-2.5-flash"),
                1,
                1,
                1,
                &[],
            ));
        }
        // 1 cached * 75 + 1 output * 2500 = 2575 nanos/span * 10,000 = 25,750,000 nanos
        assert_eq!(sum_nanos, 25_750_000);
        assert!((cost_nanos_to_usd(sum_nanos) - 0.02575).abs() < 1e-12);
    }

    #[test]
    fn cached_tokens_exceeding_input_clamps_uncached_to_zero() {
        // Built-in: gpt-4o-mini (in=150, out=600, cached=75 nanos/token)
        // input=2, output=1, cached=5 => uncached=0 => 5*75 + 1*600 = 975
        assert_eq!(
            estimate_cost_nanos(Some("openai"), Some("gpt-4o-mini"), 2, 1, 5, &[]),
            975
        );
        // input=0, output=10, cached=100 => uncached=0 => 100*75 + 10*600 = 13_500
        assert_eq!(
            estimate_cost_nanos(Some("openai"), Some("gpt-4o-mini"), 0, 10, 100, &[]),
            13_500
        );

        // Custom rule: 1.0 / 2.0 / 0.5 per 1M => 1000 / 2000 / 500 nanos/token
        // input=2, output=1, cached=5 => uncached=0 => 5*500 + 1*2000 = 4_500
        let custom = vec![ModelPricingRule {
            id: "r3".into(),
            project_id: "p1".into(),
            provider: "openai".into(),
            model_pattern: "gpt-4o-mini".into(),
            input_per_1m_usd: 1.0,
            output_per_1m_usd: 2.0,
            cached_input_per_1m_usd: 0.5,
            updated_at_ms: 0,
        }];
        assert_eq!(
            estimate_cost_nanos(Some("openai"), Some("gpt-4o-mini"), 2, 1, 5, &custom),
            4_500
        );
        assert_eq!(
            estimate_cost_nanos(Some("openai"), Some("gpt-4o-mini"), -5, 10, 100, &custom),
            70_000
        );
    }
}
