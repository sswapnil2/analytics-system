"""HTTP transport for POST /v1/agent/traces with PLST binary, JSON fallback, 8 MiB splitting, and jittered retries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import http.client
import json
import random
import threading
import time
from typing import Any, Callable, Optional
import urllib.error
import urllib.request

from .model import TraceIngestBatch, TraceIngestResponse
from .protocol import CompressionMode, ProtocolError, encode_plst_batch

MAX_HTTP_BODY_BYTES = 8 * 1024 * 1024  # 8 MiB Axum DefaultBodyLimit
MAX_RETRY_AFTER_S = 600.0
CONTENT_TYPE_PLST = "application/x-pulse-trace"
CONTENT_TYPE_JSON = "application/json"

_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


def parse_retry_after(header_value: Optional[str]) -> float:
    if not header_value:
        return 0.0
    raw = str(header_value).strip()
    if not raw:
        return 0.0
    try:
        secs = float(raw)
        if secs != secs:  # NaN
            return 0.0
        return max(0.0, min(MAX_RETRY_AFTER_S, secs))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, min(MAX_RETRY_AFTER_S, delta))
    except (TypeError, ValueError, OverflowError):
        return 0.0


@dataclass
class TransportSendResult:
    ok: bool = True
    batches_sent: int = 0
    traces_sent: int = 0
    spans_sent: int = 0
    retries: int = 0
    http_errors: int = 0
    dropped_oversized: int = 0
    dropped_permanent: int = 0
    dropped_retry_exhausted: int = 0
    responses: list[TraceIngestResponse] = None  # type: ignore[assignment]
    last_error: Optional[str] = None

    def __post_init__(self) -> None:
        if self.responses is None:
            self.responses = []

    def merge(self, other: "TransportSendResult") -> None:
        self.ok = self.ok and other.ok
        self.batches_sent += other.batches_sent
        self.traces_sent += other.traces_sent
        self.spans_sent += other.spans_sent
        self.retries += other.retries
        self.http_errors += other.http_errors
        self.dropped_oversized += other.dropped_oversized
        self.dropped_permanent += other.dropped_permanent
        self.dropped_retry_exhausted += other.dropped_retry_exhausted
        self.responses.extend(other.responses)
        if other.last_error:
            self.last_error = other.last_error


class HttpTransport:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        *,
        use_plst: bool = True,
        compression: CompressionMode = "auto",
        request_timeout_ms: int = 10_000,
        max_retries: int = 3,
        retry_base_ms: int = 200,
        on_error: Optional[Callable[[Exception], None]] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.ingest_url = f"{self.endpoint}/v1/agent/traces"
        self.api_key = api_key
        self.use_plst = use_plst
        self.compression: CompressionMode = compression
        self.request_timeout_ms = max(0, int(request_timeout_ms))
        self.max_retries = max(0, int(max_retries))
        self.retry_base_ms = max(1, int(retry_base_ms))
        self.on_error = on_error
        self.stop_event = stop_event

    def encode_batch(self, batch: TraceIngestBatch) -> tuple[bytes, str]:
        if self.use_plst:
            return (
                encode_plst_batch(batch, compression=self.compression),
                CONTENT_TYPE_PLST,
            )
        payload = json.dumps(
            batch.to_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        return payload, CONTENT_TYPE_JSON

    def send_batch(
        self,
        batch: TraceIngestBatch,
        *,
        deadline: Optional[float] = None,
        abort_event: Optional[threading.Event] = None,
    ) -> TransportSendResult:
        if not batch.traces and not batch.spans:
            return TransportSendResult(ok=True)
        return self._send_with_split(batch, deadline=deadline, abort_event=abort_event)

    def _send_with_split(
        self,
        batch: TraceIngestBatch,
        *,
        deadline: Optional[float] = None,
        abort_event: Optional[threading.Event] = None,
    ) -> TransportSendResult:
        total_items = len(batch.traces) + len(batch.spans)
        try:
            body, content_type = self.encode_batch(batch)
        except ProtocolError as exc:
            if total_items > 1:
                left, right = _split_batch(batch)
                res = TransportSendResult(ok=True)
                if left.traces or left.spans:
                    res.merge(self._send_with_split(left, deadline=deadline, abort_event=abort_event))
                if right.traces or right.spans:
                    res.merge(
                        self._send_with_split(right, deadline=deadline, abort_event=abort_event)
                    )
                return res
            self._notify_error(exc)
            return TransportSendResult(
                ok=False,
                dropped_permanent=total_items,
                last_error=str(exc),
            )
        except Exception as exc:
            self._notify_error(exc)
            return TransportSendResult(
                ok=False,
                dropped_permanent=total_items,
                last_error=str(exc),
            )

        if len(body) > MAX_HTTP_BODY_BYTES:
            if total_items <= 1:
                err_msg = f"single item encoded size {len(body)} exceeds 8 MiB limit"
                self._notify_error(ValueError(err_msg))
                return TransportSendResult(
                    ok=False,
                    dropped_oversized=total_items,
                    last_error=err_msg,
                )
            left, right = _split_batch(batch)
            res = TransportSendResult(ok=True)
            if left.traces or left.spans:
                res.merge(self._send_with_split(left, deadline=deadline, abort_event=abort_event))
            if right.traces or right.spans:
                res.merge(self._send_with_split(right, deadline=deadline, abort_event=abort_event))
            return res

        return self._send_payload_with_retry(
            body,
            content_type,
            len(batch.traces),
            len(batch.spans),
            deadline=deadline,
            abort_event=abort_event,
        )

    def _send_payload_with_retry(
        self,
        body: bytes,
        content_type: str,
        trace_count: int,
        span_count: int,
        *,
        deadline: Optional[float] = None,
        abort_event: Optional[threading.Event] = None,
    ) -> TransportSendResult:
        result = TransportSendResult(ok=False)
        base_timeout_s = self.request_timeout_ms / 1000.0 if self.request_timeout_ms > 0 else None
        wait_event = abort_event if abort_event is not None else self.stop_event
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            if wait_event is not None and wait_event.is_set():
                result.last_error = result.last_error or "aborted"
                break
            timeout_s = base_timeout_s
            if deadline is not None:
                rem = deadline - time.monotonic()
                if rem <= 0.0:
                    result.last_error = result.last_error or "flush deadline exceeded"
                    break
                timeout_s = rem if timeout_s is None else min(timeout_s, rem)

            headers = {
                "Content-Type": content_type,
                "x-pulse-key": self.api_key,
                "User-Agent": "pulse-agent-py/0.1.0",
            }
            req = urllib.request.Request(
                self.ingest_url, data=body, headers=headers, method="POST"
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                    status = getattr(resp, "status", 200)
                    raw_resp = resp.read()
                if 200 <= status < 300:
                    result.ok = True
                    result.batches_sent += 1
                    result.traces_sent += trace_count
                    result.spans_sent += span_count
                    if raw_resp:
                        try:
                            parsed = json.loads(raw_resp.decode("utf-8"))
                            if isinstance(parsed, dict):
                                result.responses.append(TraceIngestResponse.from_dict(parsed))
                        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                            pass
                    return result
                # Unexpected non-2xx without raising HTTPError
                result.http_errors += 1
                err_msg = f"HTTP {status}"
                result.last_error = err_msg
                last_exc = RuntimeError(err_msg)
                if status not in _RETRYABLE_STATUS_CODES and not (500 <= status <= 599):
                    result.dropped_permanent += trace_count + span_count
                    self._notify_error(last_exc)
                    return result
                retry_after_s = 0.0
            except urllib.error.HTTPError as exc:
                result.http_errors += 1
                status = exc.code
                retry_after_hdr = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    exc.read()
                except Exception:
                    pass
                try:
                    exc.close()
                except Exception:
                    pass
                result.last_error = f"HTTP {status}"
                last_exc = exc
                if status not in _RETRYABLE_STATUS_CODES and not (500 <= status <= 599):
                    result.dropped_permanent += trace_count + span_count
                    self._notify_error(exc)
                    return result
                retry_after_s = parse_retry_after(retry_after_hdr)
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                result.http_errors += 1
                result.last_error = str(exc)
                last_exc = exc
                retry_after_s = 0.0
            except Exception as exc:
                result.http_errors += 1
                result.dropped_permanent += trace_count + span_count
                result.last_error = str(exc)
                self._notify_error(exc)
                return result

            if attempt < self.max_retries:
                cap_s = min(60.0, (self.retry_base_ms / 1000.0) * (2**attempt))
                jitter_s = random.uniform(0.0, cap_s)
                delay_s = max(retry_after_s, jitter_s)
                if deadline is not None:
                    rem = deadline - time.monotonic()
                    if rem <= 0.0:
                        break
                    delay_s = min(delay_s, rem)
                if wait_event is not None and wait_event.is_set():
                    break
                result.retries += 1
                if delay_s > 0.0:
                    if wait_event is not None:
                        if wait_event.wait(delay_s):
                            break
                    else:
                        time.sleep(delay_s)

        result.dropped_retry_exhausted += trace_count + span_count
        if last_exc is not None:
            self._notify_error(last_exc)
        return result

    def _notify_error(self, exc: Exception) -> None:
        if self.on_error is not None:
            try:
                self.on_error(exc)
            except Exception:
                pass


def _split_batch(batch: TraceIngestBatch) -> tuple[TraceIngestBatch, TraceIngestBatch]:
    t_mid = len(batch.traces) // 2
    s_mid = len(batch.spans) // 2
    if len(batch.traces) == 1 and len(batch.spans) >= 1:
        # Put single trace in left, spans in right (or split spans if trace is empty)
        return (
            TraceIngestBatch(traces=list(batch.traces), spans=[]),
            TraceIngestBatch(traces=[], spans=list(batch.spans)),
        )
    if len(batch.spans) == 1 and len(batch.traces) >= 1:
        return (
            TraceIngestBatch(traces=list(batch.traces), spans=[]),
            TraceIngestBatch(traces=[], spans=list(batch.spans)),
        )
    return (
        TraceIngestBatch(traces=batch.traces[:t_mid], spans=batch.spans[:s_mid]),
        TraceIngestBatch(traces=batch.traces[t_mid:], spans=batch.spans[s_mid:]),
    )
