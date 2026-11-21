import json
import math
from pathlib import Path
import sys
import unittest

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import _bootstrap  # noqa: F401
from pulse_agent.model import (
    AgentSpan,
    AgentTrace,
    FailureSource,
    ModelPricingRule,
    SpanEvent,
    SpanPhase,
    SpanType,
    TraceIngestBatch,
    TraceStatus,
)
from pulse_agent.pricing import (
    cost_nanos_to_usd,
    estimate_cost_nanos,
    estimate_cost_usd,
    sync_span_cost,
    usd_to_cost_nanos,
)
from pulse_agent.privacy import (
    MAX_LINKED_SPANS_PER_SPAN,
    MAX_PAYLOAD_BYTES,
    MAX_SPAN_DEPTH,
    MAX_SPANS_PER_TRACE,
    REDACTED_MARKER,
    PrivacyConfig,
    SanitizeStats,
    cap_payload,
    compute_span_depths,
    detect_pii_patterns,
    redact_pii_str,
    redact_value,
    sanitize_batch,
    sanitize_span,
    sanitize_trace,
)


class TestModelPricingPrivacy(unittest.TestCase):
    def test_enum_wire_mappings(self) -> None:
        self.assertEqual(TraceStatus.OK.to_u8(), 0)
        self.assertEqual(TraceStatus.CANCELLED.to_u8(), 4)
        self.assertEqual(SpanPhase.START.to_u8(), 0)
        self.assertEqual(SpanPhase.FINAL.to_u8(), 2)
        self.assertEqual(SpanType.GUARDRAIL.to_u8(), 5)
        self.assertEqual(SpanType.parse("safety"), SpanType.GUARDRAIL)
        self.assertEqual(FailureSource.MODEL.to_u8(), 1)
        self.assertEqual(FailureSource.UNKNOWN.to_u8(), 6)

    def test_pricing_matches_rust_cases(self) -> None:
        nanos = estimate_cost_nanos("openai", "gpt-4o-mini-2024-07-18", 10_000, 2_000, 4_000)
        self.assertEqual(nanos, 2_400_000)
        self.assertAlmostEqual(
            estimate_cost_usd("openai", "gpt-4o-mini-2024-07-18", 10_000, 2_000, 4_000),
            0.0024,
            places=12,
        )
        self.assertEqual(estimate_cost_nanos("gemini", "gemini-2.5-flash", 1, 1, 1), 2_575)
        self.assertEqual(estimate_cost_nanos("openai", "gpt-4o-mini", 2, 1, 5), 975)

        custom = [
            ModelPricingRule(
                model_pattern="gpt-4o-mini",
                provider="openai",
                input_per_1m_usd=1.0,
                output_per_1m_usd=2.0,
                cached_input_per_1m_usd=0.5,
            )
        ]
        self.assertEqual(
            estimate_cost_nanos("openai", "gpt-4o-mini", 1_000_000, 500_000, 0, custom),
            2_000_000_000,
        )

        self.assertEqual(usd_to_cost_nanos(float("nan")), 0)
        self.assertEqual(usd_to_cost_nanos(-1.5), 0)
        self.assertEqual(usd_to_cost_nanos(float("inf")), (1 << 63) - 1)
        self.assertEqual(usd_to_cost_nanos(0.000_000_001), 1)
        self.assertEqual(usd_to_cost_nanos(0.000_000_0025), 3)  # half away from zero

        sp = AgentSpan(
            span_id="s1",
            trace_id="t1",
            name="llm",
            start_ts_ms=1000,
            cost_nanos=0,
            cost_usd=0.005,
        )
        sync_span_cost(sp)
        self.assertEqual(sp.cost_nanos, 5_000_000)
        self.assertAlmostEqual(sp.cost_usd, cost_nanos_to_usd(5_000_000))

    def test_pii_key_redaction_and_luhn(self) -> None:
        stats = SanitizeStats()
        cleaned = redact_value(
            {
                "User_Password_Hash": "hunter2",
                "X-Api_Key-Header": "xyz",
                "prompt": "Contact alice@example.com with sk-proj-secret123 or Bearer tok_12345678 and ssn 123-45-6789",
                "valid_card": "4111-1111-1111-1111",
                "invalid_card": "4111-1111-1111-1112",
                "short_digits": "123456789012",
            },
            redact_pii=True,
            redact_keys_lower=["password", "api_key"],
            stats=stats,
        )
        self.assertEqual(cleaned["User_Password_Hash"], REDACTED_MARKER)
        self.assertEqual(cleaned["X-Api_Key-Header"], REDACTED_MARKER)
        self.assertNotIn("alice@example.com", cleaned["prompt"])
        self.assertNotIn("sk-proj-secret123", cleaned["prompt"])
        self.assertNotIn("tok_12345678", cleaned["prompt"])
        self.assertNotIn("123-45-6789", cleaned["prompt"])
        self.assertEqual(cleaned["valid_card"], REDACTED_MARKER)
        self.assertEqual(cleaned["invalid_card"], "4111-1111-1111-1112")
        self.assertEqual(cleaned["short_digits"], "123456789012")
        self.assertGreaterEqual(stats.redacted_fields, 4)
        self.assertIn("email", detect_pii_patterns("a@b.com"))
        self.assertNotIn("credit_card", detect_pii_patterns("4111-1111-1111-1112"))

    def test_truncation_quote_heavy_and_utf8(self) -> None:
        stats = SanitizeStats()
        big = {"text": "A" * (MAX_PAYLOAD_BYTES + 10_000)}
        capped = cap_payload(big, MAX_PAYLOAD_BYTES, stats)
        self.assertTrue(capped["_truncated"])
        self.assertEqual(stats.truncated_payloads, 1)
        encoded = json.dumps(capped, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.assertLessEqual(len(encoded), MAX_PAYLOAD_BYTES)

        qstats = SanitizeStats()
        qpayload = {"text": '"\\🚀' * 500}
        qcapped = cap_payload(qpayload, 256, qstats)
        self.assertTrue(qcapped["_truncated"])
        qencoded = json.dumps(qcapped, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.assertLessEqual(len(qencoded), 256)

    def test_capture_toggles_exclude_attrs_cycles_and_bounds(self) -> None:
        policy = PrivacyConfig(
            capture_prompts=False,
            capture_responses=True,
            capture_user_id=False,
            exclude_attributes=("internal_debug",),
        )
        sp = AgentSpan(
            span_id="s1",
            trace_id="t1",
            name="llm",
            span_type=SpanType.LLM,
            start_ts_ms=1000,
            input={"prompt": "secret"},
            output={"text": "ok"},
            attributes={"INTERNAL_DEBUG": "drop", "keep": 1},
        )
        sanitized_sp, _ = sanitize_span(sp, policy)
        self.assertIsNone(sanitized_sp.input)
        self.assertEqual(sanitized_sp.output, {"text": "ok"})
        self.assertEqual(sanitized_sp.attributes, {"keep": 1})

        # Cycle breaking: C -> A <-> B
        spans = [
            AgentSpan(span_id="C", trace_id="t1", parent_span_id="A", name="C", start_ts_ms=1),
            AgentSpan(span_id="A", trace_id="t1", parent_span_id="B", name="A", start_ts_ms=2),
            AgentSpan(span_id="B", trace_id="t1", parent_span_id="A", name="B", start_ts_ms=3),
        ]
        compute_span_depths(spans)
        self.assertEqual(spans[0].parent_span_id, "A")
        self.assertEqual(spans[1].parent_span_id, "B")
        self.assertIsNone(spans[2].parent_span_id)
        self.assertEqual([s.depth for s in spans], [2, 1, 0])

        # Span cap + linked span dedup/cap + cost rollup
        many_spans = []
        for i in range(520):
            s = AgentSpan(
                span_id=f"sp_{i}",
                trace_id="tr_cap",
                name="step",
                start_ts_ms=1000 + i,
                cost_nanos=10_000,
            )
            if i == 0:
                s.linked_span_ids = [f"lnk_{k}" for k in range(100)] + ["sp_0", "  ", "lnk_1"]
            many_spans.append(s)

        batch = TraceIngestBatch(
            traces=[AgentTrace(trace_id="tr_cap", agent_name="a", start_ts_ms=1000, user_id="u1")],
            spans=many_spans,
        )
        sanitized_batch, bstats = sanitize_batch(batch, policy)
        self.assertEqual(len(sanitized_batch.spans), MAX_SPANS_PER_TRACE)
        self.assertEqual(bstats.dropped_spans, 8)
        self.assertIsNone(sanitized_batch.traces[0].user_id)
        self.assertEqual(len(sanitized_batch.spans[0].linked_span_ids), MAX_LINKED_SPANS_PER_SPAN)
        self.assertNotIn("sp_0", sanitized_batch.spans[0].linked_span_ids)
        self.assertEqual(sanitized_batch.traces[0].total_cost_nanos, MAX_SPANS_PER_TRACE * 10_000)

    def test_custom_object_and_nan_redaction_and_dict_preservation(self) -> None:
        class _SecretReprObj:
            def __str__(self) -> str:
                return "SecretObj(api_key=sk-1234567890abcdef, email=leak@example.com)"

        policy = PrivacyConfig(redact_pii=True)
        sp = AgentSpan(
            span_id="s_obj",
            trace_id="t_obj",
            name="fn",
            start_ts_ms=1000,
            input={"args": {"obj": _SecretReprObj(), "raw": b"token Bearer tok_abcdefgh"}},
            output={"score": float("nan"), "inf": float("inf"), "ok": True, "tags": {"a"}},
        )
        san_sp, _ = sanitize_span(sp, policy)
        self.assertIsInstance(san_sp.input, dict)
        self.assertNotIn("sk-1234567890abcdef", san_sp.input["args"]["obj"])
        self.assertNotIn("leak@example.com", san_sp.input["args"]["obj"])
        self.assertNotIn("tok_abcdefgh", san_sp.input["args"]["raw"])
        self.assertIsInstance(san_sp.output, dict)
        self.assertIsNone(san_sp.output["score"])
        self.assertIsNone(san_sp.output["inf"])
        self.assertTrue(san_sp.output["ok"])
        self.assertEqual(san_sp.output["tags"], ["a"])

        tr = AgentTrace(
            trace_id="t_obj",
            agent_name="a",
            start_ts_ms=1000,
            input=_SecretReprObj(),
            output={"val": float("-inf"), "ok": True},
        )
        san_tr, _ = sanitize_trace(tr, policy)
        self.assertNotIn("sk-1234567890abcdef", str(san_tr.input))
        self.assertEqual(san_tr.output, {"val": None, "ok": True})

        capped_nan = cap_payload({"score": float("nan"), "ok": True})
        self.assertEqual(capped_nan, {"score": None, "ok": True})


if __name__ == "__main__":
    unittest.main()
