"""Pure-stdlib loopback HTTP mocks for BYOK LLM-as-judge and HMAC-SHA256 webhooks."""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def compute_webhook_signature(signing_secret: str, timestamp_hdr: str, raw_body: bytes) -> str:
    """Compute Pulse's `sha256=<hex>` HMAC signature over `f'{ts}.' + raw_body`."""
    payload = timestamp_hdr.encode("ascii") + b"." + raw_body
    digest = hmac.new(signing_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_webhook_signature(
    signing_secret: str,
    timestamp_hdr: str,
    raw_body: bytes,
    signature_hdr: str,
    *,
    max_skew_secs: int | None = 300,
    now_secs: int | None = None,
) -> bool:
    """Verify Pulse `X-Pulse-Timestamp` and `X-Pulse-Signature-256` HMAC-SHA256 headers."""
    ts_norm = (timestamp_hdr or "").strip()
    sig_norm = (signature_hdr or "").strip().lower()
    if not signing_secret or not ts_norm or not sig_norm:
        return False
    if not sig_norm.startswith("sha256="):
        return False
    try:
        ts_val = int(ts_norm)
    except ValueError:
        return False
    if max_skew_secs is not None:
        current = int(time.time()) if now_secs is None else now_secs
        if abs(current - ts_val) > max_skew_secs:
            return False
    expected = compute_webhook_signature(signing_secret, ts_norm, raw_body)
    return hmac.compare_digest(expected, sig_norm)


class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class MockLlmServer:
    """Loopback OpenAI-compatible `/v1/chat/completions` mock server."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        status_code: int = 200,
        response_content: str = '{"score": 0.9, "passed": true, "reasoning": "Mock LLM judge pass"}',
        latency_s: float = 0.0,
    ) -> None:
        self.host = host
        self._bind_port = port
        self.status_code = status_code
        self.response_content = response_content
        self.raw_body_override: bytes | None = None
        self.latency_s = latency_s
        self.custom_responder: Callable[[dict[str, Any]], tuple[int, dict[str, Any] | bytes]] | None = None

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._requests: list[dict[str, Any]] = []
        self._server: _QuietThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("MockLlmServer is not started")
        return int(self._server.server_address[1])

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/v1/chat/completions"

    @property
    def requests(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._requests)

    def clear(self) -> None:
        with self._lock:
            self._requests.clear()

    def wait_for_count(self, count: int, timeout: float = 10.0) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self._requests) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timed out waiting for {count} MockLlmServer requests (got {len(self._requests)})"
                    )
                self._cond.wait(timeout=remaining)
            return list(self._requests)

    def start(self) -> MockLlmServer:
        if self._server is not None:
            return self
        parent = self

        class _LlmHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b""
                parsed: Any = None
                try:
                    parsed = json.loads(raw.decode("utf-8")) if raw else None
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed = None

                record = {
                    "method": "POST",
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": raw,
                    "json": parsed,
                    "ts_ms": int(time.time() * 1000),
                }
                with parent._cond:
                    parent._requests.append(record)
                    parent._cond.notify_all()
                    status = parent.status_code
                    content = parent.response_content
                    raw_override = parent.raw_body_override
                    delay = parent.latency_s
                    responder = parent.custom_responder

                if delay > 0:
                    time.sleep(delay)

                if responder is not None:
                    status, custom_payload = responder(record)
                    if isinstance(custom_payload, (bytes, bytearray)):
                        out_bytes = bytes(custom_payload)
                    else:
                        out_bytes = json.dumps(custom_payload).encode("utf-8")
                elif raw_override is not None:
                    out_bytes = raw_override
                else:
                    model_name = "gpt-4o-mini"
                    if isinstance(parsed, dict) and isinstance(parsed.get("model"), str):
                        model_name = parsed["model"]
                    body_obj = {
                        "id": "chatcmpl-mock",
                        "object": "chat.completion",
                        "model": model_name,
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": content},
                                "finish_reason": "stop",
                            }
                        ],
                    }
                    out_bytes = json.dumps(body_obj).encode("utf-8")

                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out_bytes)))
                self.end_headers()
                self.wfile.write(out_bytes)

        self._server = _QuietThreadingHTTPServer((self.host, self._bind_port), _LlmHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="MockLlmServer",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def __enter__(self) -> MockLlmServer:
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()


class MockWebhookServer:
    """Loopback webhook receiver verifying Pulse HMAC-SHA256 alert signatures."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        status_code: int = 200,
        signing_secret: str | None = None,
        max_skew_secs: int | None = 300,
        latency_s: float = 0.0,
    ) -> None:
        self.host = host
        self._bind_port = port
        self.status_code = status_code
        self.signing_secret = signing_secret
        self.max_skew_secs = max_skew_secs
        self.latency_s = latency_s

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._requests: list[dict[str, Any]] = []
        self._server: _QuietThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("MockWebhookServer is not started")
        return int(self._server.server_address[1])

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/hook"

    @property
    def requests(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._requests)

    def clear(self) -> None:
        with self._lock:
            self._requests.clear()

    def wait_for_count(self, count: int, timeout: float = 10.0) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self._requests) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timed out waiting for {count} MockWebhookServer requests (got {len(self._requests)})"
                    )
                self._cond.wait(timeout=remaining)
            return list(self._requests)

    def start(self) -> MockWebhookServer:
        if self._server is not None:
            return self
        parent = self

        class _HookHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b""
                headers_lower = {k.lower(): v for k, v in self.headers.items()}
                ts_hdr = headers_lower.get("x-pulse-timestamp", "")
                sig_hdr = headers_lower.get("x-pulse-signature-256", "")
                parsed: Any = None
                try:
                    parsed = json.loads(raw.decode("utf-8")) if raw else None
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed = None

                with parent._lock:
                    secret = parent.signing_secret
                    skew = parent.max_skew_secs
                    status = parent.status_code
                    delay = parent.latency_s

                sig_valid: bool | None = None
                if secret is not None:
                    sig_valid = verify_webhook_signature(
                        secret,
                        ts_hdr,
                        raw,
                        sig_hdr,
                        max_skew_secs=skew,
                    )

                record = {
                    "method": "POST",
                    "path": self.path,
                    "headers": headers_lower,
                    "raw_body": raw,
                    "json": parsed,
                    "timestamp_hdr": ts_hdr,
                    "signature_hdr": sig_hdr,
                    "signature_valid": sig_valid,
                    "ts_ms": int(time.time() * 1000),
                }
                with parent._cond:
                    parent._requests.append(record)
                    parent._cond.notify_all()

                if delay > 0:
                    time.sleep(delay)

                out = b'{"received":true}'
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

        self._server = _QuietThreadingHTTPServer((self.host, self._bind_port), _HookHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="MockWebhookServer",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def __enter__(self) -> MockWebhookServer:
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()
