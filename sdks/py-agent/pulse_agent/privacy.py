"""Client-side PII/secret redaction, payload truncation, and structural bounds matching crates/pulse-agent/src/privacy.rs."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
import json
import math
import re
from typing import Any, Optional, Sequence

from .model import AgentSpan, AgentTrace, SpanType, TraceIngestBatch
from .pricing import cost_nanos_to_usd, sync_span_cost, sync_trace_cost

REDACTED_MARKER = "[REDACTED]"
MAX_SPANS_PER_TRACE = 512
MAX_SPAN_DEPTH = 32
MAX_LINKED_SPANS_PER_SPAN = 64
MAX_SPAN_ID_BYTES = 256
MAX_PAYLOAD_BYTES = 64 * 1024  # 65536 bytes
MAX_ATTRIBUTES_PER_SPAN = 64
MAX_EVENTS_PER_SPAN = 64
MAX_TAGS_PER_TRACE = 32

DEFAULT_REDACT_KEYS: tuple[str, ...] = (
    "password",
    "secret",
    "token",
    "api_key",
    "authorization",
    "cookie",
    "ssn",
    "credit_card",
)

_EMAIL_RE = re.compile(r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b", re.IGNORECASE)
_API_KEY_RE = re.compile(
    r"\b(?:sk-[A-Za-z0-9_-]{8,}|sk_live_[A-Za-z0-9_-]{8,}|sk_root_[A-Za-z0-9_-]{8,}|"
    r"pk_(?:live_)?[A-Za-z0-9_-]{8,}|ghp_[A-Za-z0-9]{16,}|AIza[0-9A-Za-z_-]{20,}|"
    r"xox[baprs]-[0-9A-Za-z-]{10,})\b"
)
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._\-+/=]{8,}", re.IGNORECASE)
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CREDIT_CARD_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")


@dataclass
class PrivacyConfig:
    capture_inputs: bool = True
    capture_outputs: bool = True
    capture_tool_args: bool = True
    capture_tool_results: bool = True
    capture_prompts: bool = True
    capture_responses: bool = True
    capture_user_id: bool = True
    redact_pii: bool = True
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS
    exclude_attributes: tuple[str, ...] = ()

    @classmethod
    def default_for_project(cls) -> "PrivacyConfig":
        return cls(capture_user_id=False)

    @classmethod
    def with_custom_redact_keys(
        cls,
        extra_keys: Optional[Sequence[str]] = None,
        *,
        merge_defaults: bool = True,
        **kwargs: Any,
    ) -> "PrivacyConfig":
        keys: list[str] = list(DEFAULT_REDACT_KEYS) if merge_defaults else []
        seen = {k.lower() for k in keys}
        if extra_keys:
            for k in extra_keys:
                kl = str(k).strip().lower()
                if kl and kl not in seen:
                    seen.add(kl)
                    keys.append(kl)
        return cls(redact_keys=tuple(keys), **kwargs)


@dataclass
class SanitizeStats:
    redacted_fields: int = 0
    truncated_payloads: int = 0
    dropped_spans: int = 0

    def merge(self, other: "SanitizeStats") -> None:
        self.redacted_fields += other.redacted_fields
        self.truncated_payloads += other.truncated_payloads
        self.dropped_spans += other.dropped_spans


def _luhn_valid(candidate: str) -> bool:
    digits = [ord(ch) - 48 for ch in candidate if "0" <= ch <= "9"]
    if len(digits) < 13 or len(digits) > 19:
        return False
    total = 0
    alt = False
    for d in reversed(digits):
        n = d
        if alt:
            n *= 2
            if n > 9:
                n -= 9
        total += n
        alt = not alt
    return total % 10 == 0


def looks_like_luhn_card(text: str) -> bool:
    for m in _CREDIT_CARD_RE.finditer(text):
        if _luhn_valid(m.group(0)):
            return True
    return False


def detect_pii_patterns(text: str) -> list[str]:
    hits: list[str] = []
    if _EMAIL_RE.search(text):
        hits.append("email")
    if _API_KEY_RE.search(text) or _BEARER_RE.search(text):
        hits.append("api_key_or_secret")
    if _SSN_RE.search(text):
        hits.append("ssn")
    if looks_like_luhn_card(text):
        hits.append("credit_card")
    return hits


def redact_pii_str(text: str, stats: Optional[SanitizeStats] = None) -> str:
    out = text
    for pat in (_BEARER_RE, _API_KEY_RE, _EMAIL_RE, _SSN_RE):
        if pat.search(out):
            if stats is not None:
                stats.redacted_fields += 1
            out = pat.sub(REDACTED_MARKER, out)

    if _CREDIT_CARD_RE.search(out):
        replaced = 0

        def _cc_sub(m: re.Match[str]) -> str:
            nonlocal replaced
            if _luhn_valid(m.group(0)):
                replaced += 1
                return REDACTED_MARKER
            return m.group(0)

        candidate = _CREDIT_CARD_RE.sub(_cc_sub, out)
        if replaced > 0:
            if stats is not None:
                stats.redacted_fields += 1
            out = candidate
    return out


def _safe_str(val: Any) -> str:
    try:
        return str(val)
    except Exception:
        try:
            return repr(val)
        except Exception:
            return "<unserializable>"


def _normalize_json_value(val: Any, _seen: Optional[set[int]] = None) -> Any:
    if val is None or isinstance(val, bool):
        return val
    if isinstance(val, int):
        return int(val) if type(val) is not int else val
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val) if type(val) is not float else val
    if isinstance(val, str):
        return str(val) if type(val) is not str else val
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val).decode("utf-8", errors="replace")

    seen = _seen if _seen is not None else set()
    if isinstance(val, (list, tuple, set, frozenset)):
        oid = id(val)
        if oid in seen:
            return ["<cycle>"]
        seen.add(oid)
        try:
            return [_normalize_json_value(item, seen) for item in val]
        finally:
            seen.discard(oid)
    if isinstance(val, Mapping):
        oid = id(val)
        if oid in seen:
            return {"_cycle": True}
        seen.add(oid)
        try:
            return {str(k): _normalize_json_value(v, seen) for k, v in val.items()}
        finally:
            seen.discard(oid)
    return _safe_str(val)


def _redact_value_inner(
    val: Any,
    redact_pii: bool,
    redact_keys_lower: Sequence[str],
    stats: Optional[SanitizeStats],
    seen: set[int],
) -> Any:
    if val is None or isinstance(val, bool):
        return val
    if isinstance(val, int):
        return int(val) if type(val) is not int else val
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val) if type(val) is not float else val
    if isinstance(val, str):
        s = str(val) if type(val) is not str else val
        return redact_pii_str(s, stats) if redact_pii else s
    if isinstance(val, (bytes, bytearray, memoryview)):
        s = bytes(val).decode("utf-8", errors="replace")
        return redact_pii_str(s, stats) if redact_pii else s
    if isinstance(val, (list, tuple, set, frozenset)):
        oid = id(val)
        if oid in seen:
            return ["<cycle>"]
        seen.add(oid)
        try:
            return [
                _redact_value_inner(item, redact_pii, redact_keys_lower, stats, seen)
                for item in val
            ]
        finally:
            seen.discard(oid)
    if isinstance(val, Mapping):
        oid = id(val)
        if oid in seen:
            return {"_cycle": True}
        seen.add(oid)
        try:
            cleaned: dict[str, Any] = {}
            for k, v in val.items():
                ks = str(k)
                kl = ks.lower()
                if any(rk in kl for rk in redact_keys_lower):
                    cleaned[ks] = REDACTED_MARKER
                    if stats is not None:
                        stats.redacted_fields += 1
                else:
                    cleaned[ks] = _redact_value_inner(
                        v, redact_pii, redact_keys_lower, stats, seen
                    )
            return cleaned
        finally:
            seen.discard(oid)
    s = _safe_str(val)
    return redact_pii_str(s, stats) if redact_pii else s


def redact_value(
    val: Any,
    redact_pii: bool,
    redact_keys_lower: Sequence[str],
    stats: Optional[SanitizeStats] = None,
) -> Any:
    return _redact_value_inner(val, redact_pii, redact_keys_lower, stats, set())


def _compact_json_bytes(val: Any) -> bytes:
    return json.dumps(val, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )


def _utf8_prefix(raw: bytes, max_len: int) -> str:
    if max_len <= 0:
        return ""
    cut = min(max_len, len(raw))
    while cut > 0:
        try:
            return raw[:cut].decode("utf-8")
        except UnicodeDecodeError:
            cut -= 1
    return ""


def cap_payload(
    val: Any,
    max_bytes: int = MAX_PAYLOAD_BYTES,
    stats: Optional[SanitizeStats] = None,
) -> Any:
    if val is None:
        return None
    val = _normalize_json_value(val)
    try:
        raw_bytes = _compact_json_bytes(val)
    except (TypeError, ValueError):
        val = _safe_str(val)
        raw_bytes = _compact_json_bytes(val)
    orig_len = len(raw_bytes)
    if orig_len <= max_bytes:
        return val

    if stats is not None:
        stats.truncated_payloads += 1

    if max_bytes < 64:
        minimal = {"_truncated": True}
        if len(_compact_json_bytes(minimal)) <= max_bytes:
            return minimal
        return None

    lo = 0
    hi = min(orig_len, max(0, max_bytes - 48))
    best_candidate: Optional[dict[str, Any]] = None

    while lo <= hi:
        mid = (lo + hi) // 2
        preview = _utf8_prefix(raw_bytes, mid)
        candidate = {
            "_truncated": True,
            "_original_bytes": orig_len,
            "_preview": preview,
        }
        encoded_len = len(_compact_json_bytes(candidate))
        if encoded_len <= max_bytes:
            best_candidate = candidate
            lo = mid + 1
        else:
            hi = mid - 1

    if best_candidate is not None:
        return best_candidate

    fallback = {"_truncated": True, "_original_bytes": orig_len}
    if len(_compact_json_bytes(fallback)) <= max_bytes:
        return fallback
    return {"_truncated": True}


def _sanitize_linked_span_ids(span_id: str, linked: Sequence[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in linked:
        trimmed = str(raw).strip()
        if not trimmed or trimmed == span_id:
            continue
        raw_b = trimmed.encode("utf-8")
        bounded = _utf8_prefix(raw_b, MAX_SPAN_ID_BYTES)
        if bounded and bounded not in seen:
            seen.add(bounded)
            cleaned.append(bounded)
            if len(cleaned) >= MAX_LINKED_SPANS_PER_SPAN:
                break
    return cleaned


def sanitize_trace(
    trace: AgentTrace,
    policy: PrivacyConfig,
    *,
    inplace: bool = False,
) -> tuple[AgentTrace, SanitizeStats]:
    tr = trace if inplace else deepcopy(trace)
    stats = SanitizeStats()
    redact_keys = [k.strip().lower() for k in policy.redact_keys if k and k.strip()]

    if not policy.capture_user_id:
        if tr.user_id is not None:
            stats.redacted_fields += 1
        tr.user_id = None

    if not policy.capture_inputs:
        if tr.input is not None:
            stats.redacted_fields += 1
        tr.input = None
    else:
        tr.input = redact_value(tr.input, policy.redact_pii, redact_keys, stats)
        tr.input = cap_payload(tr.input, MAX_PAYLOAD_BYTES, stats)

    if not policy.capture_outputs:
        if tr.output is not None:
            stats.redacted_fields += 1
        tr.output = None
    else:
        tr.output = redact_value(tr.output, policy.redact_pii, redact_keys, stats)
        tr.output = cap_payload(tr.output, MAX_PAYLOAD_BYTES, stats)

    tr.metadata = redact_value(tr.metadata, policy.redact_pii, redact_keys, stats)
    tr.metadata = cap_payload(tr.metadata, MAX_PAYLOAD_BYTES, stats)

    if tr.error_message is not None and policy.redact_pii:
        tr.error_message = redact_pii_str(tr.error_message, stats)

    if len(tr.tags) > MAX_TAGS_PER_TRACE:
        tr.tags = list(tr.tags[:MAX_TAGS_PER_TRACE])

    tr.input_tokens = max(0, int(tr.input_tokens))
    tr.output_tokens = max(0, int(tr.output_tokens))
    tr.cached_tokens = max(0, int(tr.cached_tokens))
    tr.total_tokens = max(0, int(tr.total_tokens))
    sync_trace_cost(tr)
    return tr, stats


def sanitize_span(
    span: AgentSpan,
    policy: PrivacyConfig,
    *,
    inplace: bool = False,
) -> tuple[AgentSpan, SanitizeStats]:
    sp = span if inplace else deepcopy(span)
    stats = SanitizeStats()
    redact_keys = [k.strip().lower() for k in policy.redact_keys if k and k.strip()]
    excluded_attrs = {k.strip().lower() for k in policy.exclude_attributes if k and k.strip()}

    if sp.span_type == SpanType.LLM:
        allow_input = policy.capture_prompts and policy.capture_inputs
        allow_output = policy.capture_responses and policy.capture_outputs
    elif sp.span_type == SpanType.TOOL:
        allow_input = policy.capture_tool_args and policy.capture_inputs
        allow_output = policy.capture_tool_results and policy.capture_outputs
    else:
        allow_input = policy.capture_inputs
        allow_output = policy.capture_outputs

    if not allow_input:
        if sp.input is not None:
            stats.redacted_fields += 1
        sp.input = None
    else:
        sp.input = redact_value(sp.input, policy.redact_pii, redact_keys, stats)
        sp.input = cap_payload(sp.input, MAX_PAYLOAD_BYTES, stats)

    if not allow_output:
        if sp.output is not None:
            stats.redacted_fields += 1
        sp.output = None
    else:
        sp.output = redact_value(sp.output, policy.redact_pii, redact_keys, stats)
        sp.output = cap_payload(sp.output, MAX_PAYLOAD_BYTES, stats)

    if isinstance(sp.attributes, Mapping):
        attrs: dict[str, Any] = {}
        for k, v in sp.attributes.items():
            ks = str(k)
            if excluded_attrs and ks.lower() in excluded_attrs:
                continue
            attrs[ks] = v
            if len(attrs) >= MAX_ATTRIBUTES_PER_SPAN:
                break
        sp.attributes = attrs
    sp.attributes = redact_value(sp.attributes, policy.redact_pii, redact_keys, stats)
    sp.attributes = cap_payload(sp.attributes, MAX_PAYLOAD_BYTES, stats)

    sp.metadata = redact_value(sp.metadata, policy.redact_pii, redact_keys, stats)
    sp.metadata = cap_payload(sp.metadata, MAX_PAYLOAD_BYTES, stats)

    if len(sp.events) > MAX_EVENTS_PER_SPAN:
        sp.events = list(sp.events[:MAX_EVENTS_PER_SPAN])
    for ev in sp.events:
        ev.attributes = redact_value(ev.attributes, policy.redact_pii, redact_keys, stats)
        ev.attributes = cap_payload(ev.attributes, MAX_PAYLOAD_BYTES, stats)

    if sp.error_message is not None and policy.redact_pii:
        sp.error_message = redact_pii_str(sp.error_message, stats)
    if sp.error_stack is not None and policy.redact_pii:
        sp.error_stack = redact_pii_str(sp.error_stack, stats)

    sp.linked_span_ids = _sanitize_linked_span_ids(sp.span_id, sp.linked_span_ids)
    sp.input_tokens = max(0, int(sp.input_tokens))
    sp.output_tokens = max(0, int(sp.output_tokens))
    sp.cached_tokens = max(0, int(sp.cached_tokens))
    sp.total_tokens = max(0, int(sp.total_tokens))
    sync_span_cost(sp)
    return sp, stats


def compute_span_depths(spans: Sequence[AgentSpan], indices: Optional[Sequence[int]] = None) -> None:
    idxs = list(range(len(spans))) if indices is None else list(indices)
    parent_map: dict[str, Optional[str]] = {}
    for idx in idxs:
        sp = spans[idx]
        parent_map[sp.span_id] = sp.parent_span_id

    broken: set[str] = set()
    for idx in idxs:
        start_id = spans[idx].span_id
        seen = {start_id}
        prev_id = start_id
        cur = parent_map.get(prev_id)
        while cur is not None:
            if cur in seen:
                parent_map[prev_id] = None
                broken.add(prev_id)
                break
            seen.add(cur)
            prev_id = cur
            cur = parent_map.get(cur)

    for idx in idxs:
        span = spans[idx]
        if span.span_id in broken:
            span.parent_span_id = None
        depth = 0
        steps = 0
        cur = parent_map.get(span.span_id)
        while cur is not None:
            depth += 1
            steps += 1
            if depth >= MAX_SPAN_DEPTH or steps > len(idxs):
                break
            cur = parent_map.get(cur)
        span.depth = min(depth, MAX_SPAN_DEPTH)


def sanitize_batch(
    batch: TraceIngestBatch,
    policy: PrivacyConfig,
    *,
    inplace: bool = False,
) -> tuple[TraceIngestBatch, SanitizeStats]:
    out = batch if inplace else deepcopy(batch)
    stats = SanitizeStats()

    counts_per_trace: dict[str, int] = {}
    retained_spans: list[AgentSpan] = []
    for sp in out.spans:
        cnt = counts_per_trace.get(sp.trace_id, 0)
        if cnt >= MAX_SPANS_PER_TRACE:
            stats.dropped_spans += 1
            continue
        counts_per_trace[sp.trace_id] = cnt + 1
        sanitized_sp, sp_stats = sanitize_span(sp, policy, inplace=True)
        stats.merge(sp_stats)
        retained_spans.append(sanitized_sp)
    out.spans = retained_spans

    indices_by_trace: dict[str, list[int]] = {}
    for idx, sp in enumerate(out.spans):
        indices_by_trace.setdefault(sp.trace_id, []).append(idx)

    cost_by_trace: dict[str, int] = {}
    for tid, idxs in indices_by_trace.items():
        compute_span_depths(out.spans, idxs)
        cost_by_trace[tid] = sum(max(0, out.spans[i].cost_nanos) for i in idxs)

    sanitized_traces: list[AgentTrace] = []
    for tr in out.traces:
        sanitized_tr, tr_stats = sanitize_trace(tr, policy, inplace=True)
        stats.merge(tr_stats)
        span_cost_sum = cost_by_trace.get(sanitized_tr.trace_id, 0)
        if span_cost_sum > 0:
            sanitized_tr.total_cost_nanos = span_cost_sum
            sanitized_tr.total_cost_usd = cost_nanos_to_usd(span_cost_sum)
        sanitized_traces.append(sanitized_tr)
    out.traces = sanitized_traces
    return out, stats
