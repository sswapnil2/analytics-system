import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest import mock

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import _bootstrap  # noqa: F401
from pulse_agent import (
    AgentSpan,
    ProtocolError,
    PulseAgentClient,
    SpanPhase,
    SpanType,
    TraceIngestBatch,
    TraceStatus,
    decode_plst_batch,
    get_current_session_id,
    get_current_span,
    get_current_trace,
    guardrail,
    observe,
    tool,
)
from pulse_agent.privacy import MAX_SPAN_DEPTH, MAX_SPANS_PER_TRACE
from pulse_agent.transport import MAX_HTTP_BODY_BYTES, HttpTransport, parse_retry_after


class _RecordingServer:
    def __init__(self, statuses=None, retry_after=None):
        self.statuses = list(statuses) if statuses is not None else [200]
        self.retry_after = retry_after
        self.requests = []
        self._lock = threading.Lock()
        parent = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length)
                with parent._lock:
                    idx = len(parent.requests)
                    parent.requests.append(
                        {
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "body": body,
                        }
                    )
                    code = parent.statuses[min(idx, len(parent.statuses) - 1)]
                if code == 200:
                    resp = b'{"accepted_traces":1,"accepted_spans":1,"truncated_payloads":0,"redacted_fields":0,"dropped_spans":0,"warnings":[]}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(resp)))
                    self.end_headers()
                    self.wfile.write(resp)
                else:
                    self.send_response(code)
                    if parent.retry_after is not None and code == 429:
                        self.send_header("Retry-After", parent.retry_after)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

        self._srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._srv.server_address[1]}"
        self._thread = threading.Thread(target=self._srv.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._srv.shutdown()
        self._srv.server_close()


class TestClientTransportContext(unittest.TestCase):
    def test_plst_http_roundtrip_and_json_fallback(self) -> None:
        with _RecordingServer([200]) as srv:
            client = PulseAgentClient(
                api_key="pk_test_123",
                endpoint=srv.url,
                use_plst=True,
                flush_interval_ms=0,
            )
            with client.session(session_id="sess_py", user_id="u_py", workflow_id="wf_1"):
                with client.trace(
                    trace_id="tr_py_1",
                    agent_name="demo-agent",
                    input={"email": "alice@example.com"},
                ) as tr:
                    with tr.generation(
                        name="llm.call",
                        model="gpt-4o-mini",
                        provider="openai",
                        input_tokens=100,
                        output_tokens=50,
                        emit_on_start=True,
                    ) as gen:
                        gen.update(output={"partial": "hel"}, emit=True)
                        gen.set_output({"text": "hello"})
                    with tr.guardrail(name="safety") as gd:
                        gd.set_output({"safe": True})
                    tr.set_output({"done": True})
            self.assertTrue(client.flush())
            client.shutdown()

            self.assertEqual(len(srv.requests), 1)
            req = srv.requests[0]
            self.assertEqual(req["path"], "/v1/agent/traces")
            self.assertEqual(req["headers"].get("x-pulse-key"), "pk_test_123")
            self.assertEqual(req["headers"].get("content-type"), "application/x-pulse-trace")
            decoded = decode_plst_batch(req["body"])
            self.assertEqual(len(decoded.traces), 1)
            self.assertEqual(decoded.traces[0].session_id, "sess_py")
            self.assertEqual(decoded.traces[0].input, {"email": "[REDACTED]"})
            self.assertEqual(len(decoded.spans), 4)  # start, update, final (gen) + final (guardrail)
            phases = [s.span_phase for s in decoded.spans]
            self.assertEqual(phases, [SpanPhase.START, SpanPhase.UPDATE, SpanPhase.FINAL, SpanPhase.FINAL])
            self.assertEqual(decoded.spans[2].cost_nanos, 45_000)

        with _RecordingServer([200]) as srv_json:
            c_json = PulseAgentClient(
                api_key="pk_json",
                endpoint=srv_json.url,
                use_binary=False,
                flush_interval_ms=0,
            )
            with c_json.trace(trace_id="tr_json", agent_name="json-agent"):
                pass
            c_json.shutdown()
            self.assertEqual(len(srv_json.requests), 1)
            self.assertEqual(srv_json.requests[0]["headers"].get("content-type"), "application/json")
            payload = json.loads(srv_json.requests[0]["body"].decode("utf-8"))
            self.assertEqual(payload["traces"][0]["trace_id"], "tr_json")

    def test_retry_and_permanent_4xx_drop(self) -> None:
        with _RecordingServer([429, 503, 200], retry_after="0") as srv:
            c = PulseAgentClient(
                api_key="pk_r",
                endpoint=srv.url,
                use_plst=False,
                flush_interval_ms=0,
                max_retries=3,
                retry_base_ms=10,
            )
            with c.trace(trace_id="tr_retry", agent_name="retry"):
                pass
            self.assertTrue(c.flush())
            st = c.stats()
            c.shutdown()
            self.assertEqual(len(srv.requests), 3)
            self.assertEqual(st["retries"], 2)
            self.assertEqual(st["flushed_traces"], 1)

        for code in (400, 401, 403, 404, 413, 422):
            with _RecordingServer([code, 200]) as srv_drop:
                c = PulseAgentClient(
                    api_key="pk_d",
                    endpoint=srv_drop.url,
                    use_plst=False,
                    flush_interval_ms=0,
                    max_retries=3,
                    retry_base_ms=10,
                )
                with c.trace(trace_id=f"tr_{code}", agent_name="drop"):
                    pass
                self.assertFalse(c.flush())
                st = c.stats()
                c.shutdown()
                self.assertEqual(len(srv_drop.requests), 1, f"code={code} should not retry")
                self.assertEqual(st["dropped_permanent"], 1)

    def test_retry_after_http_date_parsing(self) -> None:
        self.assertEqual(parse_retry_after("5"), 5.0)
        self.assertEqual(parse_retry_after("0"), 0.0)
        future = datetime.now(timezone.utc) + timedelta(seconds=10)
        parsed = parse_retry_after(format_datetime(future))
        self.assertGreater(parsed, 5.0)
        self.assertLessEqual(parsed, 11.0)

    def test_backpressure_and_oversized_split(self) -> None:
        c = PulseAgentClient(
            api_key="pk_bp",
            endpoint="http://127.0.0.1:9",
            max_queue_size=4,
            flush_interval_ms=0,
            max_retries=0,
            request_timeout_ms=100,
        )
        for i in range(10):
            with c.trace(trace_id=f"tr_bp_{i}", agent_name="bp"):
                pass
        st = c.stats()
        self.assertEqual(st["queued"], 4)
        self.assertEqual(st["dropped_queue_full"], 6)
        self.assertEqual(c.dropped_count, 6)
        c.shutdown()

        # Verify multi-item batch exceeding 8 MiB splits into multiple HTTP requests
        with _RecordingServer([200]) as srv:
            c_big = PulseAgentClient(
                api_key="pk_big",
                endpoint=srv.url,
                use_plst=False,
                flush_interval_ms=0,
                flush_batch_size=200,
            )
            # Each trace ~55 KiB JSON, 170 traces > 8 MiB -> auto-splits into >=2 requests
            big_text = "B" * 55_000
            for i in range(170):
                c_big.record_trace(
                    {
                        "trace_id": f"tr_big_{i}",
                        "agent_name": "big",
                        "start_ts_ms": 1730000000000 + i,
                        "input": big_text,
                    }
                )
            self.assertTrue(c_big.flush())
            c_big.shutdown()
            self.assertGreaterEqual(len(srv.requests), 2)
            for req in srv.requests:
                self.assertLessEqual(len(req["body"]), MAX_HTTP_BODY_BYTES)

    def test_sampling_rates(self) -> None:
        with _RecordingServer([200]) as srv:
            c0 = PulseAgentClient(
                api_key="pk_s", endpoint=srv.url, sample_rate=0.0, flush_interval_ms=0
            )
            with c0.trace(trace_id="tr_s0", agent_name="s"):
                with c0.generation(name="g"):
                    pass
            c0.shutdown()
            self.assertEqual(len(srv.requests), 0)

            c_half = PulseAgentClient(
                api_key="pk_s", endpoint=srv.url, sample_rate=0.5, flush_interval_ms=0
            )
            for i in range(40):
                with c_half.trace(trace_id=f"tr_sample_{i}", agent_name="s"):
                    pass
            st = c_half.stats()
            c_half.shutdown()
            self.assertGreater(st["enqueued_traces"], 0)
            self.assertGreater(st["sampled_out"], 0)

    def test_async_context_and_decorators(self) -> None:
        with _RecordingServer([200]) as srv:
            client = PulseAgentClient(
                api_key="pk_dec", endpoint=srv.url, use_plst=False, flush_interval_ms=0
            )

            @client.observe(name="workflow.run")
            async def run_workflow(x: int) -> dict:
                self.assertIsNotNone(get_current_trace())
                self.assertEqual(get_current_session_id(), "sess_async")
                return await run_tool(x)

            @tool(name="calc.double")
            async def run_tool(v: int) -> int:
                sp = get_current_span()
                self.assertIsNotNone(sp)
                self.assertEqual(sp.span_type, SpanType.TOOL)
                return v * 2

            @guardrail(name="check.positive")
            def check_sync(v: int) -> bool:
                return v > 0

            async def main() -> None:
                async with client.session(session_id="sess_async"):
                    res = await run_workflow(21)
                    self.assertEqual(res, 42)
                    with client.trace(trace_id="tr_sync_g", agent_name="g"):
                        self.assertTrue(check_sync(1))

            asyncio.run(main())
            client.shutdown()
            self.assertEqual(len(srv.requests), 1)
            body = json.loads(srv.requests[0]["body"].decode("utf-8"))
            self.assertEqual(len(body["traces"]), 2)
            self.assertEqual({s["span_type"] for s in body["spans"]}, {"tool", "guardrail"})

    def test_record_span_sampling_and_runtime_caps(self) -> None:
        c0 = PulseAgentClient(api_key="pk_s", endpoint="http://127.0.0.1:9", sample_rate=0.0, flush_interval_ms=0)
        c0.record_span(AgentSpan(span_id="sp_drop", trace_id="tr_drop", name="x", start_ts_ms=1000))
        st0 = c0.stats()
        self.assertEqual(st0["queued"], 0)
        self.assertEqual(st0["enqueued_spans"], 0)
        self.assertEqual(st0["sampled_out"], 1)
        c0.shutdown()

        # Depth clamped at MAX_SPAN_DEPTH (32) and >512 spans per trace dropped at runtime
        c = PulseAgentClient(
            api_key="pk_cap",
            endpoint="http://127.0.0.1:9",
            max_queue_size=2000,
            flush_interval_ms=0,
        )
        with c.trace(trace_id="tr_deep", agent_name="deep") as tr:
            stack = []
            for i in range(45):
                cm = tr.span(f"s_{i}")
                cm.__enter__()
                stack.append(cm)
            while stack:
                cm = stack.pop()
                cm.__exit__(None, None, None)

        batch_deep = c._drain_chunk(1000)
        self.assertEqual(max(s.depth for s in batch_deep.spans), MAX_SPAN_DEPTH)

        with c.trace(trace_id="tr_many", agent_name="many") as tr:
            for i in range(600):
                with tr.span(f"step_{i}"):
                    pass
        st_many = c.stats()
        self.assertEqual(st_many["dropped_spans"], 600 - MAX_SPANS_PER_TRACE)
        self.assertEqual(c.dropped_count, 600 - MAX_SPANS_PER_TRACE)
        drained = c._drain_chunk(2000)
        self.assertEqual(len(drained.spans), MAX_SPANS_PER_TRACE)
        c.shutdown()

    def test_shutdown_bounded_timeout_and_closed_guard(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class _SlowHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or "0")
                _ = self.rfile.read(length)
                entered.set()
                release.wait(timeout=2.0)
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _SlowHandler)
        srv_thread = threading.Thread(target=srv.serve_forever, daemon=True)
        srv_thread.start()
        try:
            url = f"http://127.0.0.1:{srv.server_address[1]}"
            c = PulseAgentClient(
                api_key="pk_slow",
                endpoint=url,
                use_plst=False,
                flush_interval_ms=10,
                request_timeout_ms=2000,
                max_retries=2,
                retry_base_ms=500,
            )
            with c.trace(trace_id="tr_slow", agent_name="slow"):
                pass
            self.assertTrue(entered.wait(timeout=2.0))
            t0 = time.monotonic()
            c.shutdown(timeout_s=0.15)
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 0.6)
            # Post-shutdown enqueue must be ignored
            before = c.stats()["enqueued_traces"]
            c.record_trace({"trace_id": "tr_after", "agent_name": "after", "start_ts_ms": 1000})
            self.assertEqual(c.stats()["enqueued_traces"], before)
        finally:
            release.set()
            srv.shutdown()
            srv.server_close()

    def test_transport_on_error_and_protocol_split(self) -> None:
        errors: list[Exception] = []
        with _RecordingServer([503, 503, 503]) as srv:
            tr = HttpTransport(
                srv.url,
                "pk_err",
                use_plst=False,
                max_retries=1,
                retry_base_ms=1,
                on_error=errors.append,
            )
            res = tr.send_batch(
                TraceIngestBatch(
                    spans=[AgentSpan(span_id="s1", trace_id="t1", name="x", start_ts_ms=1000)]
                )
            )
            self.assertFalse(res.ok)
            self.assertEqual(res.dropped_retry_exhausted, 1)
            self.assertEqual(len(errors), 1)

        # Split batch when encode_batch raises ProtocolError (e.g. >64 MiB) for >1 items
        with _RecordingServer([200]) as srv_ok:
            tr2 = HttpTransport(srv_ok.url, "pk_sp", use_plst=True, max_retries=0)
            orig_encode = tr2.encode_batch

            def _mock_encode(b: TraceIngestBatch):
                if len(b.spans) > 1:
                    raise ProtocolError("uncompressed PLST payload exceeds 64 MiB limit")
                return orig_encode(b)

            with mock.patch.object(tr2, "encode_batch", side_effect=_mock_encode):
                res2 = tr2.send_batch(
                    TraceIngestBatch(
                        spans=[
                            AgentSpan(span_id="s1", trace_id="t1", name="a", start_ts_ms=1000),
                            AgentSpan(span_id="s2", trace_id="t1", name="b", start_ts_ms=1001),
                        ]
                    )
                )
            self.assertTrue(res2.ok)
            self.assertEqual(res2.spans_sent, 2)
            self.assertEqual(len(srv_ok.requests), 2)


if __name__ == "__main__":
    unittest.main()
