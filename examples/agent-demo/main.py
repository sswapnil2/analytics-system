"""Zero-dependency Python AI Agent Observability demo using pulse_agent."""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import struct
import sys
import threading
import time
from typing import Any

try:
    import pulse_agent  # noqa: F401
except ImportError:
    _sdk_root = Path(__file__).resolve().parents[2] / "sdks" / "py-agent"
    if _sdk_root.is_dir() and str(_sdk_root) not in sys.path:
        sys.path.insert(0, str(_sdk_root))

from pulse_agent import (  # noqa: E402
    AGENT_DICT_ID,
    FLAG_DICT_ZSTD,
    PLST_HEADER_LEN,
    PLST_MAGIC,
    PLST_VERSION,
    PulseAgentClient,
    REDACTED_MARKER,
    SpanPhase,
    SpanType,
    TraceStatus,
    decode_plst_batch,
    has_zstd,
    tool,
    wrap_openai,
    wrap_retriever,
)


def _first_non_empty(*values: str | None) -> str | None:
    for val in values:
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _normalize_endpoint(url: str) -> str:
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith("/v1/agent/traces"):
        cleaned = cleaned[: -len("/v1/agent/traces")].rstrip("/")
    return cleaned


def _resolve_config(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pulse AI Agent Observability Demo (Python)",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Pulse base URL or /v1/agent/traces endpoint (precedence: --endpoint > PULSE_ENDPOINT > PULSE_BASE_URL > http://127.0.0.1:18190)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Pulse ingest API key (precedence: --api-key > PULSE_API_KEY > pk_live_REPLACE_ME)",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="Optional Pulse project ID (precedence: --project-id > PULSE_PROJECT_ID > '')",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline loopback verification with transient 429/503 retries and PLST assertions",
    )
    args = parser.parse_args(argv)

    raw_endpoint = (
        _first_non_empty(
            args.endpoint,
            os.environ.get("PULSE_ENDPOINT"),
            os.environ.get("PULSE_BASE_URL"),
        )
        or "http://127.0.0.1:18190"
    )
    endpoint = _normalize_endpoint(raw_endpoint)
    raw_api_key = _first_non_empty(args.api_key, os.environ.get("PULSE_API_KEY"))
    if raw_api_key:
        api_key = raw_api_key
    elif args.self_test:
        api_key = "pk_live_selftest_demo"
    else:
        api_key = "pk_live_REPLACE_ME"

    project_id = (
        args.project_id
        if args.project_id is not None
        else os.environ.get("PULSE_PROJECT_ID", "")
    )
    args.endpoint = endpoint
    args.api_key = api_key
    args.project_id = project_id
    return args


class _LocalPolicyRetriever:
    def retrieve(self, query: str) -> list[dict[str, Any]]:
        return [
            {
                "id": "policy-refund-01",
                "score": 0.95,
                "content": "Digital & physical orders qualify for a full refund within 30 days.",
                "metadata": {"source": "policies/refunds.md", "query": query},
            },
            {
                "id": "policy-sla-02",
                "score": 0.88,
                "content": "Approved refunds are processed within 5 business days.",
                "metadata": {"source": "policies/sla.md"},
            },
        ]


class _MockOpenAICompletions:
    def create(self, *, model: str, messages: list[dict[str, Any]], stream: bool = False, **kwargs: Any) -> Any:
        if not stream:
            return {
                "id": "chatcmpl-demo-01",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Order ORD-901 is eligible for a refund."},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 145, "completion_tokens": 28, "total_tokens": 173},
            }

        def _gen():
            yield {
                "id": "chatcmpl-demo-01",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "content": "Your order ORD-901 is eligible "},
                        "finish_reason": None,
                    }
                ],
            }
            yield {
                "id": "chatcmpl-demo-01",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": "for a full refund within 5 business days."},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 145,
                    "completion_tokens": 28,
                    "total_tokens": 173,
                    "prompt_tokens_details": {"cached_tokens": 24},
                },
            }

        return _gen()


class _MockOpenAIChat:
    def __init__(self) -> None:
        self.completions = _MockOpenAICompletions()


class _MockOpenAIClient:
    def __init__(self) -> None:
        self.chat = _MockOpenAIChat()


def run_agent_scenario(
    *,
    endpoint: str,
    api_key: str,
    project_id: str,
    self_test: bool,
) -> dict[str, Any]:
    client = PulseAgentClient(
        api_key=api_key,
        endpoint=endpoint,
        project_id=project_id,
        environment="staging",
        agent_name="support-copilot",
        agent_version="1.2.0",
        prompt_version="prompt-v3",
        model_config_version="gpt4o-mini-2026",
        tool_config_version="tools-v2",
        app_release="agent-demo-0.1.0",
        use_plst=True,
        compression="auto",
        flush_interval_ms=0,
        flush_batch_size=64,
        max_retries=4 if self_test else 3,
        retry_base_ms=15 if self_test else 200,
        redact_pii=True,
        redact_keys=["customer_secret"],
        on_error=lambda exc: print(
            f"[pulse-agent-demo-py] transport warning: {exc}",
            file=sys.stderr,
        ),
    )

    session_id = f"sess_py_demo_{int(time.time() * 1000)}"
    turn1_trace_id = ""
    turn2_trace_id = ""
    retrieval_span_id = ""

    @tool(
        name="tool.order_lookup_cache",
        tool_name="order_cache_v2",
        tool_version="2.1.0",
        tool_retry_count=1,
    )
    def lookup_order_cache(order_id: str) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "status": "delivered",
            "eligible_for_refund": True,
            "total_usd": 49.0,
        }

    with client.session(
        session_id=session_id,
        user_id="user_demo_42",
        workflow_id="wf_customer_support",
    ) as sess:
        # Turn 1: Guardrails, RAG retrieval, linked LLM generation, wrap_openai stream, failing tool + recovery
        with sess.trace(
            "support.turn_1",
            agent_name="support-copilot",
            input={
                "question": "Check refund eligibility for alice@example.com on order ORD-901",
                "customer_secret": "cs_top_secret_987",
                "api_key": "sk-proj-demo1234567890abcdef",
            },
            tags=["demo", "rag", "support"],
            metadata={"turn": "support.turn_1", "turn_number": 1},
        ) as tr1:
            turn1_trace_id = tr1.trace_id

            with tr1.guardrail(
                "guardrail.input_pii_and_policy",
                input={
                    "text": "User alice@example.com (SSN 123-45-6789, card 4111-1111-1111-1111) asked for refund."
                },
            ) as gd_in:
                gd_in.set_attributes({"guardrail.name": "input_pii_and_policy", "guardrail.passed": True})
                gd_in.set_output({"passed": True, "action": "redact_and_allow"})

            retriever = wrap_retriever(_LocalPolicyRetriever(), tr1, name="rag.retrieve")
            docs = retriever.retrieve("refund eligibility and turnaround policy")
            retrieval_span_id = tr1.last_retrieval_span_id or ""

            with tr1.generation(
                "rag.grounded.planner",
                provider="openai",
                model="gpt-4o-mini",
                linked_span_ids=[retrieval_span_id],
                temperature=0.2,
                input={
                    "messages": [
                        {"role": "system", "content": "Use policy docs to plan order lookup."},
                        {"role": "user", "content": "Check order ORD-901 for alice@example.com."},
                    ],
                    "doc_ids": [d["id"] for d in docs],
                },
            ) as gen_rag:
                gen_rag.set_usage(input_tokens=185, output_tokens=32, cached_tokens=40)
                gen_rag.set_model("gpt-4o-mini", provider="openai", finish_reason="tool_calls")
                gen_rag.set_output({"next_action": "lookup_order", "order_id": "ORD-901"})
                rag_gen_span_id = gen_rag.span_id

            try:
                with tr1.tool(
                    "tool.order_lookup",
                    tool_name="order_api_v1",
                    tool_version="1.0.0",
                    tool_retry_count=0,
                    input={
                        "order_id": "ORD-901",
                        "customer_email": "alice@example.com",
                        "customer_secret": "cs_tool_secret_456",
                    },
                ):
                    raise TimeoutError("Upstream order API timeout after 250ms")
            except TimeoutError:
                pass

            order_data = lookup_order_cache("ORD-901")

            openai_client = wrap_openai(
                _MockOpenAIClient(),
                tr1,
                name="openai.chat.stream",
                capture_stream=True,
            )
            stream = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                stream=True,
                messages=[
                    {"role": "system", "content": "Synthesize customer response."},
                    {"role": "user", "content": f"Order lookup result: {json.dumps(order_data)}"},
                ],
            )
            streamed_parts: list[str] = []
            for chunk in stream:
                choices = chunk.get("choices") or []
                if choices:
                    content = choices[0].get("delta", {}).get("content")
                    if isinstance(content, str):
                        streamed_parts.append(content)
            streamed_text = "".join(streamed_parts)

            with tr1.guardrail(
                "guardrail.output_safety_check",
                linked_span_ids=[retrieval_span_id, rag_gen_span_id],
                input={"response_preview": streamed_text},
            ) as gd_out:
                gd_out.set_attributes({"guardrail.name": "output_safety_check", "guardrail.passed": True})
                gd_out.set_output({"passed": True, "action": "allow"})

            tr1.set_output({"answer": streamed_text, "order": order_data})

        # Turn 2: Follow-up child trace with parent_trace_id and explicit SpanPhase START -> UPDATE -> FINAL
        with sess.trace(
            "support.turn_2",
            agent_name="support-copilot",
            parent_trace_id=turn1_trace_id,
            input={"follow_up": "Summarize the refund SLA concisely."},
            tags=["demo", "follow_up"],
            metadata={"turn": "support.turn_2", "turn_number": 2},
        ) as tr2:
            turn2_trace_id = tr2.trace_id
            with tr2.generation(
                "anthropic.stream.summary",
                provider="anthropic",
                model="claude-sonnet-4",
                linked_span_ids=[retrieval_span_id],
                emit_on_start=True,
                input={"prompt": "Summarize refund SLA in one sentence."},
                attributes={"stream.mode": "explicit_phase"},
            ) as gen2:
                gen2.add_event("first_token", {"ttft_ms": 24})
                gen2.update(
                    output={"partial": "Refunds arrive within 5 "},
                    attributes={"stream.chunk": 1},
                    emit=True,
                )
                gen2.set_usage(input_tokens=110, output_tokens=24)
                gen2.set_model("claude-sonnet-4", provider="anthropic", finish_reason="end_turn")
                gen2.set_output(
                    {"content": "Refunds arrive within 5 business days to your original payment method."}
                )

            tr2.set_output(
                {"summary": "Refunds arrive within 5 business days to your original payment method."}
            )

    flushed_ok = client.flush(timeout_s=5.0)
    stats = client.stats()
    client.shutdown(timeout_s=5.0)
    return {
        "flushed_ok": flushed_ok,
        "stats": stats,
        "session_id": session_id,
        "turn1_trace_id": turn1_trace_id,
        "turn2_trace_id": turn2_trace_id,
        "retrieval_span_id": retrieval_span_id,
    }


def run_self_test(args: argparse.Namespace) -> None:
    lock = threading.Lock()
    attempts: list[dict[str, Any]] = []
    decoded_batches: list[Any] = []
    errors: list[str] = []

    class _MockHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *msg_args: Any) -> None:  # noqa: A002
            return

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length)

            if self.path != "/v1/agent/traces":
                body = b'{"error":"not_found"}'
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            try:
                if self.headers.get("x-pulse-key") != args.api_key:
                    raise AssertionError(
                        f"x-pulse-key mismatch: {self.headers.get('x-pulse-key')!r}"
                    )
                if self.headers.get("content-type") != "application/x-pulse-trace":
                    raise AssertionError(
                        f"Expected application/x-pulse-trace, got {self.headers.get('content-type')!r}"
                    )
                if len(raw_body) < PLST_HEADER_LEN:
                    raise AssertionError("PLST frame shorter than 20 bytes")

                magic, ver, flags, dict_id, _uncomp_len, _comp_len, _crc = struct.unpack_from(
                    "<4sBBHIII", raw_body, 0
                )
                if magic != PLST_MAGIC or ver != PLST_VERSION:
                    raise AssertionError(f"Invalid PLST header: magic={magic!r}, ver={ver}")
                if has_zstd():
                    if not (flags & FLAG_DICT_ZSTD) or dict_id != AGENT_DICT_ID:
                        raise AssertionError(
                            f"Expected FLAG_DICT_ZSTD and AGENT_DICT_ID=2, got flags={flags}, dict_id={dict_id}"
                        )
                else:
                    if dict_id != 0:
                        raise AssertionError(f"Expected dict_id=0 without zstd, got {dict_id}")

                decoded = decode_plst_batch(raw_body)
            except Exception as exc:  # pragma: no cover
                with lock:
                    errors.append(str(exc))
                err_body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err_body)))
                self.end_headers()
                self.wfile.write(err_body)
                return

            with lock:
                attempts.append({"path": self.path, "size": len(raw_body)})
                attempt_no = len(attempts)
                decoded_batches.append((attempt_no, decoded))

            if attempt_no == 1:
                self.send_response(429)
                self.send_header("Retry-After", "0.02")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if attempt_no == 2:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            resp_body = json.dumps(
                {
                    "accepted_traces": len(decoded.traces),
                    "accepted_spans": len(decoded.spans),
                    "truncated_payloads": 0,
                    "redacted_fields": 0,
                    "dropped_spans": 0,
                    "warnings": [],
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _MockHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        assert _normalize_endpoint(f"http://127.0.0.1:{port}/v1/agent/traces/") == f"http://127.0.0.1:{port}"
        result = run_agent_scenario(
            endpoint=_normalize_endpoint(f"http://127.0.0.1:{port}/v1/agent/traces"),
            api_key=args.api_key,
            project_id=args.project_id,
            self_test=True,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)

    assert not errors, f"Mock server validation errors: {errors}"
    assert result["flushed_ok"] is True, "Expected client.flush() to return True"
    stats = result["stats"]
    assert len(attempts) >= 3, f"Expected >= 3 HTTP attempts, got {len(attempts)}"
    assert stats["retries"] >= 2, f"Expected retries >= 2, got {stats['retries']}"
    assert stats["http_errors"] >= 2, f"Expected http_errors >= 2, got {stats['http_errors']}"
    assert stats["dropped_permanent"] == 0, f"Unexpected dropped_permanent: {stats['dropped_permanent']}"
    assert (
        stats["dropped_retry_exhausted"] == 0
    ), f"Unexpected dropped_retry_exhausted: {stats['dropped_retry_exhausted']}"
    assert stats["flushed_traces"] >= 2, f"Expected flushed_traces >= 2, got {stats['flushed_traces']}"
    assert stats["redacted_fields"] > 0, f"Expected redacted_fields > 0, got {stats['redacted_fields']}"

    accepted_batches = [batch for no, batch in decoded_batches if no >= 3]
    all_traces = [tr for b in accepted_batches for tr in b.traces]
    all_spans = [sp for b in accepted_batches for sp in b.spans]

    tr1 = next((t for t in all_traces if t.trace_id == result["turn1_trace_id"]), None)
    tr2 = next((t for t in all_traces if t.trace_id == result["turn2_trace_id"]), None)
    assert tr1 is not None, "Turn 1 trace missing in decoded PLST batches"
    assert tr2 is not None, "Turn 2 trace missing in decoded PLST batches"
    assert tr1.agent_name == "support-copilot", f"Turn 1 agent_name mismatch: {tr1.agent_name!r}"
    assert tr2.agent_name == "support-copilot", f"Turn 2 agent_name mismatch: {tr2.agent_name!r}"
    assert tr1.session_id == result["session_id"], "Turn 1 session_id mismatch"
    assert tr2.session_id == result["session_id"], "Turn 2 session_id mismatch"
    assert tr2.parent_trace_id == tr1.trace_id, "Turn 2 parent_trace_id linkage mismatch"
    assert (tr1.total_cost_nanos + tr2.total_cost_nanos) > 0, "Expected total_cost_nanos > 0"

    span_types = {sp.span_type for sp in all_spans}
    for expected in (SpanType.RETRIEVAL, SpanType.LLM, SpanType.TOOL, SpanType.GUARDRAIL):
        assert expected in span_types, f"Missing span_type={expected}"

    retrieval_id = result["retrieval_span_id"]
    assert retrieval_id, "Expected non-empty retrieval_span_id"
    assert any(
        sp.span_type == SpanType.LLM and retrieval_id in sp.linked_span_ids
        for sp in all_spans
    ), "Expected LLM span linked to retrieval_span_id"

    for stream_name in ("openai.chat.stream", "anthropic.stream.summary"):
        phases = {sp.span_phase for sp in all_spans if sp.name == stream_name}
        for phase in (SpanPhase.START, SpanPhase.UPDATE, SpanPhase.FINAL):
            assert phase in phases, f"Missing phase {phase} for {stream_name}, saw {phases}"

    tool_spans = [sp for sp in all_spans if sp.span_type == SpanType.TOOL]
    assert any(
        sp.status == TraceStatus.ERROR and sp.error_type == "TimeoutError" for sp in tool_spans
    ), "Expected error tool span with TimeoutError"
    assert any(sp.status == TraceStatus.OK for sp in tool_spans), "Expected ok tool span"

    serialized = json.dumps(
        {
            "traces": [t.to_dict() for t in all_traces],
            "spans": [s.to_dict() for s in all_spans],
        }
    )
    assert REDACTED_MARKER in serialized, "Expected REDACTED_MARKER in decoded payloads"
    for secret in (
        "alice@example.com",
        "sk-proj-demo1234567890abcdef",
        "123-45-6789",
        "4111-1111-1111-1111",
        "cs_top_secret_987",
        "cs_tool_secret_456",
    ):
        assert secret not in serialized, f"Raw secret leaked in decoded PLST payload: {secret}"

    print(
        json.dumps(
            {
                "mode": "self-test",
                "ok": True,
                "zstd": has_zstd(),
                "http_attempts": len(attempts),
                "stats": stats,
            },
            indent=2,
        )
    )


def main(argv: list[str] | None = None) -> int:
    args = _resolve_config(sys.argv[1:] if argv is None else argv)
    if args.self_test:
        run_self_test(args)
        return 0

    result = run_agent_scenario(
        endpoint=args.endpoint,
        api_key=args.api_key,
        project_id=args.project_id,
        self_test=False,
    )
    print(
        json.dumps(
            {
                "mode": "live",
                "endpoint": args.endpoint,
                "session_id": result["session_id"],
                "turn1_trace_id": result["turn1_trace_id"],
                "turn2_trace_id": result["turn2_trace_id"],
                "stats": result["stats"],
            },
            indent=2,
        )
    )
    stats = result["stats"]
    if not result["flushed_ok"] or stats["dropped_permanent"] > 0 or stats["dropped_retry_exhausted"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
