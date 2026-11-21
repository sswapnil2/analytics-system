"""PLST v1 binary protocol encoder/decoder matching crates/pulse-agent/src/protocol.rs byte-for-byte."""

from __future__ import annotations

import json
import struct
from typing import Any, Literal, Optional, Sequence
import zlib

from ._xxh3 import xxh3_64
from .model import (
    AgentSpan,
    AgentTrace,
    FailureSource,
    SpanEvent,
    SpanPhase,
    SpanType,
    TraceIngestBatch,
    TraceStatus,
)
from .pricing import cost_nanos_to_usd, usd_to_cost_nanos

try:
    from compression import zstd as _zstd  # Python 3.14+ stdlib
except ImportError:  # pragma: no cover - Python < 3.14 fallback
    _zstd = None

PLST_MAGIC = b"PLST"
PLST_VERSION = 1
AGENT_DICT_ID = 2
MAX_PLST_UNCOMP_BYTES = 64 * 1024 * 1024

FLAG_RAW_ZSTD = 0x01
FLAG_DICT_ZSTD = 0x02
FLAG_PARTIAL = 0x04
FLAG_FINAL = 0x08
KNOWN_FLAGS_MASK = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | FLAG_PARTIAL | FLAG_FINAL

PLST_HEADER_LEN = 20
MAX_PLST_TRACES = 100_000
MAX_PLST_SPANS = 500_000
MAX_PLST_EVENTS = 2_000_000
MAX_PLST_STRINGS = 2_000_000
MAX_PLST_BLOBS = 500_000
MAX_PLST_SINGLE_STRING_BYTES = 1024 * 1024
MAX_PLST_MATERIALIZED_BYTES = 64 * 1024 * 1024
NONE_IDX = 0xFFFFFFFF

BLOB_ENC_JSON_UTF8 = 0
BLOB_ENC_UTF8_TEXT = 1

AGENT_ZSTD_DICT: bytes = (
    b'{"messages":[{"role":"system","content":\x00'
    b'{"role":"user","content":\x00'
    b'{"role":"assistant","content":\x00'
    b'{"role":"tool","tool_call_id":\x00'
    b'"tool_calls":[{"id":\x00'
    b'"type":"function","function":{"name":\x00'
    b'"arguments":\x00'
    b'"choices":[{"index":0,"message":\x00'
    b'"finish_reason":"stop"\x00'
    b'"finish_reason":"tool_calls"\x00'
    b'"usage":{"prompt_tokens":\x00'
    b'"completion_tokens":\x00'
    b'"total_tokens":\x00'
    b"gen_ai.system\x00gen_ai.request.model\x00gen_ai.response.model\x00"
    b"gen_ai.usage.input_tokens\x00gen_ai.usage.output_tokens\x00"
    b"gen_ai.operation.name\x00gen_ai.tool.name\x00gen_ai.response.finish_reasons\x00"
    b"llm.model_name\x00llm.token_count.prompt\x00llm.token_count.completion\x00"
    b"input.value\x00output.value\x00service.name\x00deployment.environment\x00"
    b"openai\x00anthropic\x00google\x00bedrock\x00azure\x00"
    b"gpt-4o\x00gpt-4o-mini\x00gpt-4.1\x00gpt-4.1-mini\x00o3-mini\x00"
    b"claude-sonnet-4\x00claude-3-5-sonnet\x00claude-3-5-haiku\x00"
    b"gemini-2.5-pro\x00gemini-2.5-flash\x00"
    b"production\x00staging\x00development\x00unversioned\x00"
    b"agent\x00llm\x00tool\x00retrieval\x00embedding\x00guardrail\x00workflow\x00"
    b"start\x00update\x00final\x00ok\x00error\x00timeout\x00cancelled\x00"
    b"[REDACTED]\x00_truncated\x00_preview\x00_original_bytes\x00"
)

CompressionMode = Literal["auto", "dict", "raw", "none"]

_HEADER_STRUCT = struct.Struct("<4sBBHIII")
_U32_STRUCT = struct.Struct("<I")

_DICT_CACHE: dict[int, Any] = {}


class ProtocolError(Exception):
    """Raised when PLST encoding or decoding fails validation."""


def has_zstd() -> bool:
    return _zstd is not None


def _get_zstd_dict(zstd_mod: Any) -> Any:
    key = id(zstd_mod)
    cached = _DICT_CACHE.get(key)
    if cached is None:
        cached = zstd_mod.ZstdDict(AGENT_ZSTD_DICT, is_raw=True)
        _DICT_CACHE[key] = cached
    return cached


def _wrap_i64(val: int) -> int:
    return ((val + (1 << 63)) & 0xFFFFFFFFFFFFFFFF) - (1 << 63)


def _f64_to_bits(val: float) -> int:
    return struct.unpack("<Q", struct.pack("<d", float(val)))[0]


def _bits_to_f64(bits: int) -> float:
    return struct.unpack("<d", struct.pack("<Q", bits & 0xFFFFFFFFFFFFFFFF))[0]


def _pack_u16(items: Sequence[int]) -> bytes:
    if not items:
        return b""
    return struct.pack(f"<{len(items)}H", *items)


def _pack_u32(items: Sequence[int]) -> bytes:
    if not items:
        return b""
    return struct.pack(f"<{len(items)}I", *items)


def _pack_i32(items: Sequence[int]) -> bytes:
    if not items:
        return b""
    return struct.pack(f"<{len(items)}i", *items)


def _pack_i64(items: Sequence[int]) -> bytes:
    if not items:
        return b""
    return struct.pack(f"<{len(items)}q", *items)


def _pack_u64(items: Sequence[int]) -> bytes:
    if not items:
        return b""
    return struct.pack(f"<{len(items)}Q", *items)


class _StringTableBuilder:
    def __init__(self) -> None:
        self.strings: list[bytes] = []
        self._map: dict[str, int] = {}
        self.blob_bytes = 0

    def intern(self, s: str) -> int:
        existing = self._map.get(s)
        if existing is not None:
            return existing
        encoded = s.encode("utf-8")
        if len(encoded) > MAX_PLST_SINGLE_STRING_BYTES:
            raise ProtocolError("single string exceeds MAX_PLST_SINGLE_STRING_BYTES")
        if len(self.strings) >= MAX_PLST_STRINGS:
            raise ProtocolError("string table exceeds MAX_PLST_STRINGS")
        next_bytes = self.blob_bytes + len(encoded)
        if next_bytes > 0xFFFFFFFF:
            raise ProtocolError("string table exceeds u32 offset range")
        idx = len(self.strings)
        if idx == NONE_IDX:
            raise ProtocolError("string index collided with NONE_IDX")
        self.strings.append(encoded)
        self._map[s] = idx
        self.blob_bytes = next_bytes
        return idx

    def intern_opt(self, s: Optional[str]) -> int:
        if s is None:
            return NONE_IDX
        return self.intern(s)

    def write_to(self, out: bytearray) -> None:
        out.extend(_U32_STRUCT.pack(len(self.strings)))
        offsets = [0]
        acc = 0
        for raw in self.strings:
            acc += len(raw)
            offsets.append(acc)
        out.extend(_pack_u32(offsets))
        for raw in self.strings:
            out.extend(raw)


class _BlobTableBuilder:
    def __init__(self) -> None:
        self.hashes: list[int] = []
        self.encodings: bytearray = bytearray()
        self.ranges: list[tuple[int, int]] = []
        self.data: bytearray = bytearray()
        self._buckets: dict[tuple[int, int, int], list[int]] = {}

    def intern_value(self, val: Any) -> int:
        if val is None:
            return NONE_IDX
        if isinstance(val, str):
            encoding = BLOB_ENC_UTF8_TEXT
            raw = val.encode("utf-8")
        else:
            encoding = BLOB_ENC_JSON_UTF8
            try:
                raw = json.dumps(
                    val,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                    sort_keys=True,
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ProtocolError(f"JSON serialization failed for blob: {exc}") from exc

        byte_len = len(raw)
        if byte_len > 0xFFFFFFFF:
            raise ProtocolError("blob byte length exceeds u32")
        h = xxh3_64(raw)
        key = (h, encoding, byte_len)
        candidates = self._buckets.get(key)
        if candidates is not None:
            for cid in candidates:
                start, end = self.ranges[cid]
                if self.data[start:end] == raw:
                    return cid

        if len(self.hashes) >= MAX_PLST_BLOBS:
            raise ProtocolError("blob table exceeds MAX_PLST_BLOBS")
        start = len(self.data)
        end = start + byte_len
        if end > 0xFFFFFFFF:
            raise ProtocolError("blob table exceeds u32 offset range")
        bid = len(self.hashes)
        if bid == NONE_IDX:
            raise ProtocolError("blob index collided with NONE_IDX")
        self.hashes.append(h)
        self.encodings.append(encoding)
        self.ranges.append((start, end))
        self.data.extend(raw)
        self._buckets.setdefault(key, []).append(bid)
        return bid

    def write_to(self, out: bytearray) -> None:
        out.extend(_U32_STRUCT.pack(len(self.hashes)))
        out.extend(_pack_u64(self.hashes))
        out.extend(self.encodings)
        offsets = [0]
        acc = 0
        for start, end in self.ranges:
            acc += end - start
            offsets.append(acc)
        out.extend(_pack_u32(offsets))
        out.extend(self.data)


def encode_columnar_payload(batch: TraceIngestBatch) -> bytes:
    strings = _StringTableBuilder()
    blobs = _BlobTableBuilder()

    trace_count = len(batch.traces)
    span_count = len(batch.spans)

    trace_idx_by_id: dict[str, int] = {}
    for idx, tr in enumerate(batch.traces):
        if tr.trace_id not in trace_idx_by_id:
            trace_idx_by_id[tr.trace_id] = idx

    # Section 3 (Traces)
    tr_trace_ids: list[int] = []
    tr_project_ids: list[int] = []
    tr_session_ids: list[int] = []
    tr_user_ids: list[int] = []
    tr_workflow_ids: list[int] = []
    tr_parent_trace_ids: list[int] = []
    tr_agent_names: list[int] = []
    tr_agent_versions: list[int] = []
    tr_prompt_versions: list[int] = []
    tr_model_cfg_versions: list[int] = []
    tr_tool_cfg_versions: list[int] = []
    tr_app_releases: list[int] = []
    tr_environments: list[int] = []

    tr_statuses = bytearray()
    tr_failure_sources = bytearray()
    tr_start_deltas: list[int] = []
    tr_has_end = bytearray()
    tr_end_offsets: list[int] = []
    tr_durations: list[int] = []

    tr_span_counts: list[int] = []
    tr_llm_counts: list[int] = []
    tr_tool_counts: list[int] = []
    tr_err_span_counts: list[int] = []
    tr_in_tokens: list[int] = []
    tr_out_tokens: list[int] = []
    tr_cached_tokens: list[int] = []
    tr_total_tokens: list[int] = []
    tr_cost_nanos: list[int] = []

    tr_primary_models: list[int] = []
    tr_primary_providers: list[int] = []
    tr_has_avg_eval = bytearray()
    tr_avg_eval_bits: list[int] = []
    tr_has_pass_rate = bytearray()
    tr_pass_rate_bits: list[int] = []
    tr_has_loop = bytearray()
    tr_error_types: list[int] = []
    tr_error_messages: list[int] = []

    tr_tag_counts: list[int] = []
    tr_tag_str_ids: list[int] = []
    tr_input_blobs: list[int] = []
    tr_output_blobs: list[int] = []
    tr_metadata_blobs: list[int] = []

    prev_tr_start = 0
    for t in batch.traces:
        tr_trace_ids.append(strings.intern(t.trace_id))
        tr_project_ids.append(strings.intern(t.project_id))
        tr_session_ids.append(strings.intern_opt(t.session_id))
        tr_user_ids.append(strings.intern_opt(t.user_id))
        tr_workflow_ids.append(strings.intern_opt(t.workflow_id))
        tr_parent_trace_ids.append(strings.intern_opt(t.parent_trace_id))
        tr_agent_names.append(strings.intern(t.agent_name))
        tr_agent_versions.append(strings.intern(t.agent_version))
        tr_prompt_versions.append(strings.intern_opt(t.prompt_version))
        tr_model_cfg_versions.append(strings.intern_opt(t.model_config_version))
        tr_tool_cfg_versions.append(strings.intern_opt(t.tool_config_version))
        tr_app_releases.append(strings.intern_opt(t.app_release))
        tr_environments.append(strings.intern(t.environment))

        tr_statuses.append(TraceStatus.parse(t.status).to_u8())
        tr_failure_sources.append(
            0 if t.failure_source is None else FailureSource.parse(t.failure_source).to_u8()
        )
        tr_start_deltas.append(_wrap_i64(int(t.start_ts_ms) - prev_tr_start))
        prev_tr_start = int(t.start_ts_ms)
        if t.end_ts_ms is not None:
            tr_has_end.append(1)
            tr_end_offsets.append(_wrap_i64(int(t.end_ts_ms) - int(t.start_ts_ms)))
        else:
            tr_has_end.append(0)
            tr_end_offsets.append(0)
        tr_durations.append(int(t.duration_ms))

        tr_span_counts.append(int(t.span_count))
        tr_llm_counts.append(int(t.llm_call_count))
        tr_tool_counts.append(int(t.tool_call_count))
        tr_err_span_counts.append(int(t.error_span_count))
        tr_in_tokens.append(int(t.input_tokens))
        tr_out_tokens.append(int(t.output_tokens))
        tr_cached_tokens.append(int(t.cached_tokens))
        tr_total_tokens.append(int(t.total_tokens))
        c_nanos = (
            int(t.total_cost_nanos)
            if int(t.total_cost_nanos) != 0
            else usd_to_cost_nanos(t.total_cost_usd)
        )
        tr_cost_nanos.append(c_nanos)

        tr_primary_models.append(strings.intern_opt(t.primary_model))
        tr_primary_providers.append(strings.intern_opt(t.primary_provider))
        if t.avg_eval_score is not None:
            tr_has_avg_eval.append(1)
            tr_avg_eval_bits.append(_f64_to_bits(t.avg_eval_score))
        else:
            tr_has_avg_eval.append(0)
            tr_avg_eval_bits.append(0)
        if t.eval_pass_rate is not None:
            tr_has_pass_rate.append(1)
            tr_pass_rate_bits.append(_f64_to_bits(t.eval_pass_rate))
        else:
            tr_has_pass_rate.append(0)
            tr_pass_rate_bits.append(0)
        tr_has_loop.append(1 if t.has_loop_warning else 0)
        tr_error_types.append(strings.intern_opt(t.error_type))
        tr_error_messages.append(strings.intern_opt(t.error_message))

        if len(t.tags) > 0xFFFF:
            raise ProtocolError("trace tag count exceeds u16::MAX")
        tr_tag_counts.append(len(t.tags))
        for tag in t.tags:
            tr_tag_str_ids.append(strings.intern(tag))
        tr_input_blobs.append(blobs.intern_value(t.input))
        tr_output_blobs.append(blobs.intern_value(t.output))
        tr_metadata_blobs.append(blobs.intern_value(t.metadata))

    # Section 4 (Spans) & Section 5 (Events)
    sp_trace_indices: list[int] = []
    sp_trace_ids: list[int] = []
    sp_project_ids: list[int] = []
    sp_span_ids: list[int] = []
    sp_parent_span_ids: list[int] = []
    sp_linked_counts: list[int] = []
    sp_linked_str_ids: list[int] = []
    sp_depths: list[int] = []

    sp_names: list[int] = []
    sp_types = bytearray()
    sp_phases = bytearray()
    sp_statuses = bytearray()
    sp_start_deltas: list[int] = []
    sp_has_end = bytearray()
    sp_end_offsets: list[int] = []
    sp_durations: list[int] = []

    sp_providers: list[int] = []
    sp_models: list[int] = []
    sp_model_ids: list[int] = []
    sp_model_versions: list[int] = []
    sp_prompt_tpl_ids: list[int] = []
    sp_prompt_tpl_versions: list[int] = []
    sp_in_tokens: list[int] = []
    sp_out_tokens: list[int] = []
    sp_cached_tokens: list[int] = []
    sp_total_tokens: list[int] = []
    sp_cost_nanos: list[int] = []
    sp_has_temp = bytearray()
    sp_temp_bits: list[int] = []
    sp_finish_reasons: list[int] = []
    sp_model_params_blobs: list[int] = []
    sp_tool_names: list[int] = []
    sp_tool_versions: list[int] = []
    sp_tool_retries: list[int] = []

    sp_error_types: list[int] = []
    sp_error_messages: list[int] = []
    sp_error_stacks: list[int] = []
    sp_input_blobs: list[int] = []
    sp_output_blobs: list[int] = []
    sp_attr_blobs: list[int] = []
    sp_meta_blobs: list[int] = []

    ev_span_indices: list[int] = []
    ev_ts_deltas: list[int] = []
    ev_names: list[int] = []
    ev_attr_blobs: list[int] = []
    prev_ev_ts = 0

    prev_sp_start = 0
    for s_idx, s in enumerate(batch.spans):
        sp_trace_indices.append(trace_idx_by_id.get(s.trace_id, NONE_IDX))
        sp_trace_ids.append(strings.intern(s.trace_id))
        sp_project_ids.append(strings.intern(s.project_id))
        sp_span_ids.append(strings.intern(s.span_id))
        sp_parent_span_ids.append(strings.intern_opt(s.parent_span_id))

        if len(s.linked_span_ids) > 0xFFFF:
            raise ProtocolError("span linked_span_ids count exceeds u16::MAX")
        sp_linked_counts.append(len(s.linked_span_ids))
        for lid in s.linked_span_ids:
            sp_linked_str_ids.append(strings.intern(lid))
        sp_depths.append(int(s.depth))

        sp_names.append(strings.intern(s.name))
        sp_types.append(SpanType.parse(s.span_type).to_u8())
        sp_phases.append(SpanPhase.parse(s.span_phase).to_u8())
        sp_statuses.append(TraceStatus.parse(s.status).to_u8())
        sp_start_deltas.append(_wrap_i64(int(s.start_ts_ms) - prev_sp_start))
        prev_sp_start = int(s.start_ts_ms)
        if s.end_ts_ms is not None:
            sp_has_end.append(1)
            sp_end_offsets.append(_wrap_i64(int(s.end_ts_ms) - int(s.start_ts_ms)))
        else:
            sp_has_end.append(0)
            sp_end_offsets.append(0)
        sp_durations.append(int(s.duration_ms))

        sp_providers.append(strings.intern_opt(s.provider))
        sp_models.append(strings.intern_opt(s.model))
        sp_model_ids.append(strings.intern_opt(s.model_id))
        sp_model_versions.append(strings.intern_opt(s.model_version))
        sp_prompt_tpl_ids.append(strings.intern_opt(s.prompt_template_id))
        sp_prompt_tpl_versions.append(strings.intern_opt(s.prompt_template_version))
        sp_in_tokens.append(int(s.input_tokens))
        sp_out_tokens.append(int(s.output_tokens))
        sp_cached_tokens.append(int(s.cached_tokens))
        sp_total_tokens.append(int(s.total_tokens))
        s_nanos = int(s.cost_nanos) if int(s.cost_nanos) != 0 else usd_to_cost_nanos(s.cost_usd)
        sp_cost_nanos.append(s_nanos)

        if s.temperature is not None:
            sp_has_temp.append(1)
            sp_temp_bits.append(_f64_to_bits(s.temperature))
        else:
            sp_has_temp.append(0)
            sp_temp_bits.append(0)
        sp_finish_reasons.append(strings.intern_opt(s.finish_reason))
        sp_model_params_blobs.append(blobs.intern_value(s.model_params))
        sp_tool_names.append(strings.intern_opt(s.tool_name))
        sp_tool_versions.append(strings.intern_opt(s.tool_version))
        sp_tool_retries.append(int(s.tool_retry_count))

        sp_error_types.append(strings.intern_opt(s.error_type))
        sp_error_messages.append(strings.intern_opt(s.error_message))
        sp_error_stacks.append(strings.intern_opt(s.error_stack))
        sp_input_blobs.append(blobs.intern_value(s.input))
        sp_output_blobs.append(blobs.intern_value(s.output))
        sp_attr_blobs.append(blobs.intern_value(s.attributes))
        sp_meta_blobs.append(blobs.intern_value(s.metadata))

        for ev in s.events:
            if len(ev_span_indices) >= MAX_PLST_EVENTS:
                raise ProtocolError("span events exceed MAX_PLST_EVENTS")
            ev_span_indices.append(s_idx)
            ev_ts_deltas.append(_wrap_i64(int(ev.timestamp_ms) - prev_ev_ts))
            prev_ev_ts = int(ev.timestamp_ms)
            ev_names.append(strings.intern(ev.name))
            ev_attr_blobs.append(blobs.intern_value(ev.attributes))

    out = bytearray()
    # Section 1: String Table
    strings.write_to(out)
    # Section 2: Blob Table
    blobs.write_to(out)

    # Section 3: Trace Rows
    out.extend(_U32_STRUCT.pack(trace_count))
    out.extend(_pack_u32(tr_trace_ids))
    out.extend(_pack_u32(tr_project_ids))
    out.extend(_pack_u32(tr_session_ids))
    out.extend(_pack_u32(tr_user_ids))
    out.extend(_pack_u32(tr_workflow_ids))
    out.extend(_pack_u32(tr_parent_trace_ids))
    out.extend(_pack_u32(tr_agent_names))
    out.extend(_pack_u32(tr_agent_versions))
    out.extend(_pack_u32(tr_prompt_versions))
    out.extend(_pack_u32(tr_model_cfg_versions))
    out.extend(_pack_u32(tr_tool_cfg_versions))
    out.extend(_pack_u32(tr_app_releases))
    out.extend(_pack_u32(tr_environments))

    out.extend(tr_statuses)
    out.extend(tr_failure_sources)
    out.extend(_pack_i64(tr_start_deltas))
    out.extend(tr_has_end)
    out.extend(_pack_i64(tr_end_offsets))
    out.extend(_pack_i64(tr_durations))

    out.extend(_pack_i32(tr_span_counts))
    out.extend(_pack_i32(tr_llm_counts))
    out.extend(_pack_i32(tr_tool_counts))
    out.extend(_pack_i32(tr_err_span_counts))
    out.extend(_pack_i64(tr_in_tokens))
    out.extend(_pack_i64(tr_out_tokens))
    out.extend(_pack_i64(tr_cached_tokens))
    out.extend(_pack_i64(tr_total_tokens))
    out.extend(_pack_i64(tr_cost_nanos))

    out.extend(_pack_u32(tr_primary_models))
    out.extend(_pack_u32(tr_primary_providers))
    out.extend(tr_has_avg_eval)
    out.extend(_pack_u64(tr_avg_eval_bits))
    out.extend(tr_has_pass_rate)
    out.extend(_pack_u64(tr_pass_rate_bits))
    out.extend(tr_has_loop)
    out.extend(_pack_u32(tr_error_types))
    out.extend(_pack_u32(tr_error_messages))

    out.extend(_pack_u16(tr_tag_counts))
    out.extend(_U32_STRUCT.pack(len(tr_tag_str_ids)))
    out.extend(_pack_u32(tr_tag_str_ids))
    out.extend(_pack_u32(tr_input_blobs))
    out.extend(_pack_u32(tr_output_blobs))
    out.extend(_pack_u32(tr_metadata_blobs))

    # Section 4: Span Rows
    out.extend(_U32_STRUCT.pack(span_count))
    out.extend(_pack_u32(sp_trace_indices))
    out.extend(_pack_u32(sp_trace_ids))
    out.extend(_pack_u32(sp_project_ids))
    out.extend(_pack_u32(sp_span_ids))
    out.extend(_pack_u32(sp_parent_span_ids))
    out.extend(_pack_u16(sp_linked_counts))
    out.extend(_U32_STRUCT.pack(len(sp_linked_str_ids)))
    out.extend(_pack_u32(sp_linked_str_ids))
    out.extend(_pack_i32(sp_depths))

    out.extend(_pack_u32(sp_names))
    out.extend(sp_types)
    out.extend(sp_phases)
    out.extend(sp_statuses)
    out.extend(_pack_i64(sp_start_deltas))
    out.extend(sp_has_end)
    out.extend(_pack_i64(sp_end_offsets))
    out.extend(_pack_i64(sp_durations))

    out.extend(_pack_u32(sp_providers))
    out.extend(_pack_u32(sp_models))
    out.extend(_pack_u32(sp_model_ids))
    out.extend(_pack_u32(sp_model_versions))
    out.extend(_pack_u32(sp_prompt_tpl_ids))
    out.extend(_pack_u32(sp_prompt_tpl_versions))
    out.extend(_pack_i64(sp_in_tokens))
    out.extend(_pack_i64(sp_out_tokens))
    out.extend(_pack_i64(sp_cached_tokens))
    out.extend(_pack_i64(sp_total_tokens))
    out.extend(_pack_i64(sp_cost_nanos))
    out.extend(sp_has_temp)
    out.extend(_pack_u64(sp_temp_bits))
    out.extend(_pack_u32(sp_finish_reasons))
    out.extend(_pack_u32(sp_model_params_blobs))
    out.extend(_pack_u32(sp_tool_names))
    out.extend(_pack_u32(sp_tool_versions))
    out.extend(_pack_i32(sp_tool_retries))

    out.extend(_pack_u32(sp_error_types))
    out.extend(_pack_u32(sp_error_messages))
    out.extend(_pack_u32(sp_error_stacks))
    out.extend(_pack_u32(sp_input_blobs))
    out.extend(_pack_u32(sp_output_blobs))
    out.extend(_pack_u32(sp_attr_blobs))
    out.extend(_pack_u32(sp_meta_blobs))

    # Section 5: Columnar Span Events
    out.extend(_U32_STRUCT.pack(len(ev_span_indices)))
    out.extend(_pack_u32(ev_span_indices))
    out.extend(_pack_i64(ev_ts_deltas))
    out.extend(_pack_u32(ev_names))
    out.extend(_pack_u32(ev_attr_blobs))

    return bytes(out)


def encode_plst_batch(
    batch: TraceIngestBatch,
    *,
    compression: CompressionMode = "auto",
    use_dict: Optional[bool] = None,
) -> bytes:
    if len(batch.traces) > MAX_PLST_TRACES:
        raise ProtocolError("trace count exceeds MAX_PLST_TRACES")
    if len(batch.spans) > MAX_PLST_SPANS:
        raise ProtocolError("span count exceeds MAX_PLST_SPANS")

    mode: CompressionMode = compression
    if use_dict is True:
        mode = "dict"
    elif use_dict is False:
        mode = "raw"

    if mode == "auto":
        mode = "dict" if has_zstd() else "none"

    uncomp = encode_columnar_payload(batch)
    if len(uncomp) > MAX_PLST_UNCOMP_BYTES:
        raise ProtocolError("uncompressed PLST payload exceeds 64 MiB limit")

    if mode == "none":
        comp_flag = 0
        dict_id = 0
        body = uncomp
    elif mode == "raw":
        if _zstd is None:
            raise ProtocolError("compression.zstd is unavailable for raw Zstd mode")
        comp_flag = FLAG_RAW_ZSTD
        dict_id = 0
        body = _zstd.compress(uncomp, level=3)
    elif mode == "dict":
        if _zstd is None:
            raise ProtocolError("compression.zstd is unavailable for dict Zstd mode")
        comp_flag = FLAG_DICT_ZSTD
        dict_id = AGENT_DICT_ID
        zd = _get_zstd_dict(_zstd)
        body = _zstd.compress(uncomp, level=3, zstd_dict=zd)
    else:
        raise ProtocolError(f"unsupported compression mode: {mode!r}")

    if len(body) > 0xFFFFFFFF:
        raise ProtocolError("compressed payload length exceeds u32")

    has_partial = any(TraceStatus.parse(t.status) == TraceStatus.RUNNING for t in batch.traces) or any(
        SpanPhase.parse(s.span_phase) != SpanPhase.FINAL for s in batch.spans
    )
    has_final = any(TraceStatus.parse(t.status) != TraceStatus.RUNNING for t in batch.traces) or any(
        SpanPhase.parse(s.span_phase) == SpanPhase.FINAL for s in batch.spans
    )
    if not has_partial and not has_final:
        lifecycle_flags = FLAG_FINAL
    else:
        lifecycle_flags = (FLAG_PARTIAL if has_partial else 0) | (FLAG_FINAL if has_final else 0)

    flags = comp_flag | lifecycle_flags
    crc = zlib.crc32(body) & 0xFFFFFFFF
    header = _HEADER_STRUCT.pack(
        PLST_MAGIC,
        PLST_VERSION,
        flags,
        dict_id,
        len(uncomp),
        len(body),
        crc,
    )
    return header + body


class _ByteCursor:
    def __init__(self, buf: bytes) -> None:
        self.view = memoryview(buf)
        self.pos = 0

    def remaining(self) -> int:
        return max(0, len(self.view) - self.pos)

    def read_bytes(self, length: int, ctx: str) -> bytes:
        end = self.pos + length
        if end > len(self.view):
            raise ProtocolError(f"truncated PLST frame at {ctx}")
        out = self.view[self.pos : end].tobytes()
        self.pos = end
        return out

    def read_u8(self, ctx: str) -> int:
        if self.pos + 1 > len(self.view):
            raise ProtocolError(f"truncated PLST frame at {ctx}")
        val = self.view[self.pos]
        self.pos += 1
        return int(val)

    def read_u32(self, ctx: str) -> int:
        if self.pos + 4 > len(self.view):
            raise ProtocolError(f"truncated PLST frame at {ctx}")
        val = _U32_STRUCT.unpack_from(self.view, self.pos)[0]
        self.pos += 4
        return int(val)

    def read_u8_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count, ctx)
        return list(raw)

    def read_u16_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count * 2, ctx)
        return list(struct.unpack(f"<{count}H", raw)) if count else []

    def read_u32_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count * 4, ctx)
        return list(struct.unpack(f"<{count}I", raw)) if count else []

    def read_i32_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count * 4, ctx)
        return list(struct.unpack(f"<{count}i", raw)) if count else []

    def read_i64_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count * 8, ctx)
        return list(struct.unpack(f"<{count}q", raw)) if count else []

    def read_u64_list(self, count: int, ctx: str) -> list[int]:
        raw = self.read_bytes(count * 8, ctx)
        return list(struct.unpack(f"<{count}Q", raw)) if count else []


class _StringTableReader:
    def __init__(self, strings: list[str]) -> None:
        self.strings = strings
        self.materialized_bytes = 0

    @classmethod
    def read(cls, cursor: _ByteCursor) -> "_StringTableReader":
        count = cursor.read_u32("string_table.count")
        if count > MAX_PLST_STRINGS:
            raise ProtocolError("string table count exceeds MAX_PLST_STRINGS")
        offsets = cursor.read_u32_list(count + 1, "string_table.offsets")
        if offsets[0] != 0:
            raise ProtocolError("string table offsets[0] must be 0")
        total_bytes = offsets[-1]
        blob = cursor.read_bytes(total_bytes, "string_table.data")
        strings: list[str] = []
        for i in range(count):
            start = offsets[i]
            end = offsets[i + 1]
            if end < start or end > total_bytes:
                raise ProtocolError("non-monotonic string table offsets")
            if end - start > MAX_PLST_SINGLE_STRING_BYTES:
                raise ProtocolError("single string exceeds MAX_PLST_SINGLE_STRING_BYTES")
            try:
                strings.append(blob[start:end].decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise ProtocolError(f"invalid UTF-8 in string table: {exc}") from exc
        return cls(strings)

    def get_req(self, idx: int) -> str:
        if idx == NONE_IDX or idx >= len(self.strings):
            raise ProtocolError(f"invalid string table index: {idx}")
        s = self.strings[idx]
        self.materialized_bytes += len(s.encode("utf-8"))
        if self.materialized_bytes > MAX_PLST_MATERIALIZED_BYTES:
            raise ProtocolError("materialized bytes exceed 64 MiB anti-amplification cap")
        return s

    def get_opt(self, idx: int) -> Optional[str]:
        if idx == NONE_IDX:
            return None
        return self.get_req(idx)


class _BlobTableReader:
    def __init__(self, data: bytes, encodings: list[int], ranges: list[tuple[int, int]]) -> None:
        self.data = data
        self.encodings = encodings
        self.ranges = ranges
        self._cache: list[tuple[bool, Any]] = [(False, None)] * len(ranges)

    @classmethod
    def read(cls, cursor: _ByteCursor) -> "_BlobTableReader":
        count = cursor.read_u32("blob_table.count")
        if count > MAX_PLST_BLOBS:
            raise ProtocolError("blob count exceeds MAX_PLST_BLOBS")
        hashes = cursor.read_u64_list(count, "blob_table.xxh3_64")
        encodings = cursor.read_u8_list(count, "blob_table.encodings")
        for enc in encodings:
            if enc not in (BLOB_ENC_JSON_UTF8, BLOB_ENC_UTF8_TEXT):
                raise ProtocolError(f"invalid blob_encoding enum tag: {enc}")
        offsets = cursor.read_u32_list(count + 1, "blob_table.offsets")
        if offsets[0] != 0:
            raise ProtocolError("blob table offsets[0] must be 0")
        total_bytes = offsets[-1]
        data = cursor.read_bytes(total_bytes, "blob_table.data")
        ranges: list[tuple[int, int]] = []
        mv = memoryview(data)
        for i in range(count):
            start = offsets[i]
            end = offsets[i + 1]
            if end < start or end > total_bytes:
                raise ProtocolError("non-monotonic blob table offsets")
            actual = xxh3_64(mv[start:end])
            expected = hashes[i]
            if actual != expected:
                raise ProtocolError(
                    f"PLST blob {i} xxh3_64 mismatch: expected 0x{expected:016x}, got 0x{actual:016x}"
                )
            ranges.append((start, end))
        return cls(data, encodings, ranges)

    def get_value(self, idx: int, shared_materialized: list[int]) -> Any:
        if idx == NONE_IDX:
            return None
        if idx >= len(self.ranges):
            raise ProtocolError(f"invalid blob table index: {idx}")
        start, end = self.ranges[idx]
        byte_len = end - start
        shared_materialized[0] += byte_len
        if shared_materialized[0] > MAX_PLST_MATERIALIZED_BYTES:
            raise ProtocolError("materialized blob bytes exceed 64 MiB anti-amplification cap")
        cached_flag, cached_val = self._cache[idx]
        if cached_flag:
            return cached_val
        raw = self.data[start:end]
        enc = self.encodings[idx]
        if enc == BLOB_ENC_UTF8_TEXT:
            try:
                val: Any = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProtocolError(f"invalid UTF-8 in blob: {exc}") from exc
        elif enc == BLOB_ENC_JSON_UTF8:
            try:
                val = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ProtocolError(f"invalid JSON in blob: {exc}") from exc
        else:
            raise ProtocolError(f"invalid blob_encoding enum tag: {enc}")
        self._cache[idx] = (True, val)
        return val


def decode_columnar_payload(payload: bytes) -> TraceIngestBatch:
    cursor = _ByteCursor(payload)
    strings = _StringTableReader.read(cursor)
    blobs = _BlobTableReader.read(cursor)
    shared_materialized = [0]

    trace_count = cursor.read_u32("traces.count")
    if trace_count > MAX_PLST_TRACES:
        raise ProtocolError("trace count exceeds MAX_PLST_TRACES")

    tr_trace_ids = cursor.read_u32_list(trace_count, "traces.trace_ids")
    tr_project_ids = cursor.read_u32_list(trace_count, "traces.project_ids")
    tr_session_ids = cursor.read_u32_list(trace_count, "traces.session_ids")
    tr_user_ids = cursor.read_u32_list(trace_count, "traces.user_ids")
    tr_workflow_ids = cursor.read_u32_list(trace_count, "traces.workflow_ids")
    tr_parent_trace_ids = cursor.read_u32_list(trace_count, "traces.parent_trace_ids")
    tr_agent_names = cursor.read_u32_list(trace_count, "traces.agent_names")
    tr_agent_versions = cursor.read_u32_list(trace_count, "traces.agent_versions")
    tr_prompt_versions = cursor.read_u32_list(trace_count, "traces.prompt_versions")
    tr_model_cfg_versions = cursor.read_u32_list(trace_count, "traces.model_config_versions")
    tr_tool_cfg_versions = cursor.read_u32_list(trace_count, "traces.tool_config_versions")
    tr_app_releases = cursor.read_u32_list(trace_count, "traces.app_releases")
    tr_environments = cursor.read_u32_list(trace_count, "traces.environments")

    tr_statuses = cursor.read_u8_list(trace_count, "traces.statuses")
    tr_failure_sources = cursor.read_u8_list(trace_count, "traces.failure_sources")
    tr_start_deltas = cursor.read_i64_list(trace_count, "traces.start_ts_deltas")
    tr_has_end = cursor.read_u8_list(trace_count, "traces.has_end_ts")
    tr_end_offsets = cursor.read_i64_list(trace_count, "traces.end_ts_offsets")
    tr_durations = cursor.read_i64_list(trace_count, "traces.duration_ms")

    tr_span_counts = cursor.read_i32_list(trace_count, "traces.span_counts")
    tr_llm_counts = cursor.read_i32_list(trace_count, "traces.llm_call_counts")
    tr_tool_counts = cursor.read_i32_list(trace_count, "traces.tool_call_counts")
    tr_err_span_counts = cursor.read_i32_list(trace_count, "traces.error_span_counts")
    tr_in_tokens = cursor.read_i64_list(trace_count, "traces.input_tokens")
    tr_out_tokens = cursor.read_i64_list(trace_count, "traces.output_tokens")
    tr_cached_tokens = cursor.read_i64_list(trace_count, "traces.cached_tokens")
    tr_total_tokens = cursor.read_i64_list(trace_count, "traces.total_tokens")
    tr_cost_nanos = cursor.read_i64_list(trace_count, "traces.total_cost_nanos")

    tr_primary_models = cursor.read_u32_list(trace_count, "traces.primary_models")
    tr_primary_providers = cursor.read_u32_list(trace_count, "traces.primary_providers")
    tr_has_avg_eval = cursor.read_u8_list(trace_count, "traces.has_avg_eval")
    tr_avg_eval_bits = cursor.read_u64_list(trace_count, "traces.avg_eval_bits")
    tr_has_pass_rate = cursor.read_u8_list(trace_count, "traces.has_pass_rate")
    tr_pass_rate_bits = cursor.read_u64_list(trace_count, "traces.pass_rate_bits")
    tr_has_loop = cursor.read_u8_list(trace_count, "traces.has_loop_warning")
    tr_error_types = cursor.read_u32_list(trace_count, "traces.error_types")
    tr_error_messages = cursor.read_u32_list(trace_count, "traces.error_messages")

    tr_tag_counts = cursor.read_u16_list(trace_count, "traces.tag_counts")
    total_tags = cursor.read_u32("traces.total_tags")
    if sum(tr_tag_counts) != total_tags:
        raise ProtocolError("traces total_tags length mismatch")
    tr_tag_str_ids = cursor.read_u32_list(total_tags, "traces.tag_str_ids")
    tr_input_blobs = cursor.read_u32_list(trace_count, "traces.input_blobs")
    tr_output_blobs = cursor.read_u32_list(trace_count, "traces.output_blobs")
    tr_metadata_blobs = cursor.read_u32_list(trace_count, "traces.metadata_blobs")

    traces: list[AgentTrace] = []
    prev_tr_start = 0
    tag_cursor = 0
    for i in range(trace_count):
        start_ts_ms = _wrap_i64(prev_tr_start + tr_start_deltas[i])
        prev_tr_start = start_ts_ms
        if tr_has_end[i] == 0:
            end_ts_ms = None
        elif tr_has_end[i] == 1:
            end_ts_ms = _wrap_i64(start_ts_ms + tr_end_offsets[i])
        else:
            raise ProtocolError(f"invalid has_end_ts tag: {tr_has_end[i]}")
        try:
            status = TraceStatus.from_u8(tr_statuses[i])
        except ValueError as exc:
            raise ProtocolError(str(exc)) from exc
        if tr_failure_sources[i] == 0:
            failure_source = None
        else:
            try:
                failure_source = FailureSource.from_u8(tr_failure_sources[i])
            except ValueError as exc:
                raise ProtocolError(str(exc)) from exc

        if tr_has_avg_eval[i] == 0:
            avg_eval = None
        elif tr_has_avg_eval[i] == 1:
            avg_eval = _bits_to_f64(tr_avg_eval_bits[i])
        else:
            raise ProtocolError(f"invalid has_avg_eval_score tag: {tr_has_avg_eval[i]}")

        if tr_has_pass_rate[i] == 0:
            pass_rate = None
        elif tr_has_pass_rate[i] == 1:
            pass_rate = _bits_to_f64(tr_pass_rate_bits[i])
        else:
            raise ProtocolError(f"invalid has_eval_pass_rate tag: {tr_has_pass_rate[i]}")

        if tr_has_loop[i] == 0:
            has_loop = False
        elif tr_has_loop[i] == 1:
            has_loop = True
        else:
            raise ProtocolError(f"invalid has_loop_warning tag: {tr_has_loop[i]}")

        tc = tr_tag_counts[i]
        tags = [strings.get_req(sid) for sid in tr_tag_str_ids[tag_cursor : tag_cursor + tc]]
        tag_cursor += tc

        cost_nanos = tr_cost_nanos[i]
        traces.append(
            AgentTrace(
                trace_id=strings.get_req(tr_trace_ids[i]),
                project_id=strings.get_req(tr_project_ids[i]),
                agent_name=strings.get_req(tr_agent_names[i]),
                agent_version=strings.get_req(tr_agent_versions[i]),
                prompt_version=strings.get_opt(tr_prompt_versions[i]),
                model_config_version=strings.get_opt(tr_model_cfg_versions[i]),
                tool_config_version=strings.get_opt(tr_tool_cfg_versions[i]),
                app_release=strings.get_opt(tr_app_releases[i]),
                environment=strings.get_req(tr_environments[i]),
                session_id=strings.get_opt(tr_session_ids[i]),
                user_id=strings.get_opt(tr_user_ids[i]),
                workflow_id=strings.get_opt(tr_workflow_ids[i]),
                parent_trace_id=strings.get_opt(tr_parent_trace_ids[i]),
                status=status,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                duration_ms=tr_durations[i],
                input=blobs.get_value(tr_input_blobs[i], shared_materialized),
                output=blobs.get_value(tr_output_blobs[i], shared_materialized),
                error_type=strings.get_opt(tr_error_types[i]),
                error_message=strings.get_opt(tr_error_messages[i]),
                failure_source=failure_source,
                metadata=blobs.get_value(tr_metadata_blobs[i], shared_materialized),
                tags=tags,
                span_count=tr_span_counts[i],
                llm_call_count=tr_llm_counts[i],
                tool_call_count=tr_tool_counts[i],
                error_span_count=tr_err_span_counts[i],
                input_tokens=tr_in_tokens[i],
                output_tokens=tr_out_tokens[i],
                cached_tokens=tr_cached_tokens[i],
                total_tokens=tr_total_tokens[i],
                total_cost_nanos=cost_nanos,
                total_cost_usd=cost_nanos_to_usd(cost_nanos),
                primary_model=strings.get_opt(tr_primary_models[i]),
                primary_provider=strings.get_opt(tr_primary_providers[i]),
                avg_eval_score=avg_eval,
                eval_pass_rate=pass_rate,
                has_loop_warning=has_loop,
            )
        )

    span_count = cursor.read_u32("spans.count")
    if span_count > MAX_PLST_SPANS:
        raise ProtocolError("span count exceeds MAX_PLST_SPANS")

    sp_trace_indices = cursor.read_u32_list(span_count, "spans.trace_indices")
    sp_trace_ids = cursor.read_u32_list(span_count, "spans.trace_id_str_ids")
    sp_project_ids = cursor.read_u32_list(span_count, "spans.project_id_str_ids")
    sp_span_ids = cursor.read_u32_list(span_count, "spans.span_id_str_ids")
    sp_parent_span_ids = cursor.read_u32_list(span_count, "spans.parent_span_id_str_ids")
    sp_linked_counts = cursor.read_u16_list(span_count, "spans.linked_counts")
    total_linked = cursor.read_u32("spans.total_linked")
    if sum(sp_linked_counts) != total_linked:
        raise ProtocolError("spans total_linked length mismatch")
    sp_linked_str_ids = cursor.read_u32_list(total_linked, "spans.linked_span_str_ids")
    sp_depths = cursor.read_i32_list(span_count, "spans.depths")

    sp_names = cursor.read_u32_list(span_count, "spans.names")
    sp_types = cursor.read_u8_list(span_count, "spans.span_types")
    sp_phases = cursor.read_u8_list(span_count, "spans.span_phases")
    sp_statuses = cursor.read_u8_list(span_count, "spans.statuses")
    sp_start_deltas = cursor.read_i64_list(span_count, "spans.start_ts_deltas")
    sp_has_end = cursor.read_u8_list(span_count, "spans.has_end_ts")
    sp_end_offsets = cursor.read_i64_list(span_count, "spans.end_ts_offsets")
    sp_durations = cursor.read_i64_list(span_count, "spans.duration_ms")

    sp_providers = cursor.read_u32_list(span_count, "spans.providers")
    sp_models = cursor.read_u32_list(span_count, "spans.models")
    sp_model_ids = cursor.read_u32_list(span_count, "spans.model_ids")
    sp_model_versions = cursor.read_u32_list(span_count, "spans.model_versions")
    sp_prompt_tpl_ids = cursor.read_u32_list(span_count, "spans.prompt_template_ids")
    sp_prompt_tpl_versions = cursor.read_u32_list(span_count, "spans.prompt_template_versions")
    sp_in_tokens = cursor.read_i64_list(span_count, "spans.input_tokens")
    sp_out_tokens = cursor.read_i64_list(span_count, "spans.output_tokens")
    sp_cached_tokens = cursor.read_i64_list(span_count, "spans.cached_tokens")
    sp_total_tokens = cursor.read_i64_list(span_count, "spans.total_tokens")
    sp_cost_nanos = cursor.read_i64_list(span_count, "spans.cost_nanos")
    sp_has_temp = cursor.read_u8_list(span_count, "spans.has_temperature")
    sp_temp_bits = cursor.read_u64_list(span_count, "spans.temperature_bits")
    sp_finish_reasons = cursor.read_u32_list(span_count, "spans.finish_reasons")
    sp_model_params_blobs = cursor.read_u32_list(span_count, "spans.model_params_blobs")
    sp_tool_names = cursor.read_u32_list(span_count, "spans.tool_names")
    sp_tool_versions = cursor.read_u32_list(span_count, "spans.tool_versions")
    sp_tool_retries = cursor.read_i32_list(span_count, "spans.tool_retry_counts")

    sp_error_types = cursor.read_u32_list(span_count, "spans.error_types")
    sp_error_messages = cursor.read_u32_list(span_count, "spans.error_messages")
    sp_error_stacks = cursor.read_u32_list(span_count, "spans.error_stacks")
    sp_input_blobs = cursor.read_u32_list(span_count, "spans.input_blobs")
    sp_output_blobs = cursor.read_u32_list(span_count, "spans.output_blobs")
    sp_attr_blobs = cursor.read_u32_list(span_count, "spans.attributes_blobs")
    sp_meta_blobs = cursor.read_u32_list(span_count, "spans.metadata_blobs")

    spans: list[AgentSpan] = []
    prev_sp_start = 0
    linked_cursor = 0
    for i in range(span_count):
        trace_id = strings.get_req(sp_trace_ids[i])
        tr_idx = sp_trace_indices[i]
        if tr_idx != NONE_IDX:
            if tr_idx >= len(traces) or traces[tr_idx].trace_id != trace_id:
                raise ProtocolError(f"invalid trace index: {tr_idx}")

        start_ts_ms = _wrap_i64(prev_sp_start + sp_start_deltas[i])
        prev_sp_start = start_ts_ms
        if sp_has_end[i] == 0:
            end_ts_ms = None
        elif sp_has_end[i] == 1:
            end_ts_ms = _wrap_i64(start_ts_ms + sp_end_offsets[i])
        else:
            raise ProtocolError(f"invalid span_has_end_ts tag: {sp_has_end[i]}")
        try:
            span_type = SpanType.from_u8(sp_types[i])
            span_phase = SpanPhase.from_u8(sp_phases[i])
            status = TraceStatus.from_u8(sp_statuses[i])
        except ValueError as exc:
            raise ProtocolError(str(exc)) from exc

        if sp_has_temp[i] == 0:
            temperature = None
        elif sp_has_temp[i] == 1:
            temperature = _bits_to_f64(sp_temp_bits[i])
        else:
            raise ProtocolError(f"invalid has_temperature tag: {sp_has_temp[i]}")

        lc = sp_linked_counts[i]
        linked_span_ids = [
            strings.get_req(sid) for sid in sp_linked_str_ids[linked_cursor : linked_cursor + lc]
        ]
        linked_cursor += lc

        cost_nanos = sp_cost_nanos[i]
        spans.append(
            AgentSpan(
                span_id=strings.get_req(sp_span_ids[i]),
                trace_id=trace_id,
                project_id=strings.get_req(sp_project_ids[i]),
                parent_span_id=strings.get_opt(sp_parent_span_ids[i]),
                linked_span_ids=linked_span_ids,
                span_phase=span_phase,
                depth=sp_depths[i],
                name=strings.get_req(sp_names[i]),
                span_type=span_type,
                status=status,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                duration_ms=sp_durations[i],
                provider=strings.get_opt(sp_providers[i]),
                model=strings.get_opt(sp_models[i]),
                model_id=strings.get_opt(sp_model_ids[i]),
                model_version=strings.get_opt(sp_model_versions[i]),
                prompt_template_id=strings.get_opt(sp_prompt_tpl_ids[i]),
                prompt_template_version=strings.get_opt(sp_prompt_tpl_versions[i]),
                input_tokens=sp_in_tokens[i],
                output_tokens=sp_out_tokens[i],
                cached_tokens=sp_cached_tokens[i],
                total_tokens=sp_total_tokens[i],
                cost_nanos=cost_nanos,
                cost_usd=cost_nanos_to_usd(cost_nanos),
                temperature=temperature,
                finish_reason=strings.get_opt(sp_finish_reasons[i]),
                model_params=blobs.get_value(sp_model_params_blobs[i], shared_materialized),
                tool_name=strings.get_opt(sp_tool_names[i]),
                tool_version=strings.get_opt(sp_tool_versions[i]),
                tool_retry_count=sp_tool_retries[i],
                input=blobs.get_value(sp_input_blobs[i], shared_materialized),
                output=blobs.get_value(sp_output_blobs[i], shared_materialized),
                attributes=blobs.get_value(sp_attr_blobs[i], shared_materialized),
                events=[],
                metadata=blobs.get_value(sp_meta_blobs[i], shared_materialized),
                error_type=strings.get_opt(sp_error_types[i]),
                error_message=strings.get_opt(sp_error_messages[i]),
                error_stack=strings.get_opt(sp_error_stacks[i]),
            )
        )

    event_count = cursor.read_u32("events.count")
    if event_count > MAX_PLST_EVENTS:
        raise ProtocolError("event count exceeds MAX_PLST_EVENTS")
    ev_span_indices = cursor.read_u32_list(event_count, "events.span_indices")
    ev_ts_deltas = cursor.read_i64_list(event_count, "events.timestamp_deltas")
    ev_names = cursor.read_u32_list(event_count, "events.names")
    ev_attr_blobs = cursor.read_u32_list(event_count, "events.attributes_blobs")

    prev_ev_ts = 0
    for i in range(event_count):
        s_idx = ev_span_indices[i]
        if s_idx >= len(spans):
            raise ProtocolError(f"invalid span index: {s_idx}")
        ts = _wrap_i64(prev_ev_ts + ev_ts_deltas[i])
        prev_ev_ts = ts
        spans[s_idx].events.append(
            SpanEvent(
                name=strings.get_req(ev_names[i]),
                timestamp_ms=ts,
                attributes=blobs.get_value(ev_attr_blobs[i], shared_materialized),
            )
        )

    if cursor.remaining() != 0:
        raise ProtocolError(f"unexpected trailing bytes in PLST payload: {cursor.remaining()}")

    return TraceIngestBatch(traces=traces, spans=spans)


def decode_plst_batch(data: bytes) -> TraceIngestBatch:
    if len(data) < PLST_HEADER_LEN:
        raise ProtocolError("truncated PLST 20B header")
    magic, version, flags, dict_id, uncomp_len, comp_len, expected_crc = _HEADER_STRUCT.unpack_from(
        data, 0
    )
    if magic != PLST_MAGIC:
        raise ProtocolError(f"invalid PLST magic: {magic!r}")
    if version != PLST_VERSION:
        raise ProtocolError(f"unsupported PLST version: {version}")
    if (flags & ~KNOWN_FLAGS_MASK) != 0:
        raise ProtocolError(f"unsupported PLST flags: 0x{flags:02x}")
    comp_bits = flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD)
    if comp_bits == (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD):
        raise ProtocolError(f"unsupported PLST flags: 0x{flags:02x}")
    if flags & FLAG_DICT_ZSTD:
        if dict_id != AGENT_DICT_ID:
            raise ProtocolError(f"unknown PLST dictionary id: {dict_id}")
    elif dict_id != 0:
        raise ProtocolError("dict_id must be 0 when FLAG_DICT_ZSTD is unset")

    if uncomp_len > MAX_PLST_UNCOMP_BYTES:
        raise ProtocolError("header uncomp_len exceeds 64 MiB limit")

    actual_body_len = len(data) - PLST_HEADER_LEN
    if actual_body_len < comp_len:
        raise ProtocolError("truncated PLST body")
    if actual_body_len > comp_len:
        raise ProtocolError(f"unexpected trailing bytes in PLST frame: {actual_body_len - comp_len}")
    if comp_bits == 0 and comp_len != uncomp_len:
        raise ProtocolError(f"PLST length mismatch: expected {uncomp_len}, got {comp_len}")

    body = data[PLST_HEADER_LEN:]
    actual_crc = zlib.crc32(body) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        raise ProtocolError(
            f"PLST compressed CRC32 mismatch: expected 0x{expected_crc:08x}, got 0x{actual_crc:08x}"
        )

    if comp_bits == 0:
        payload = body
    elif comp_bits in (FLAG_RAW_ZSTD, FLAG_DICT_ZSTD):
        if _zstd is None:
            raise ProtocolError("compression.zstd unavailable to decompress PLST frame")
        try:
            if comp_bits == FLAG_DICT_ZSTD:
                zd = _get_zstd_dict(_zstd)
                decompressor = _zstd.ZstdDecompressor(zstd_dict=zd)
            else:
                decompressor = _zstd.ZstdDecompressor()
            payload = decompressor.decompress(body, max_length=MAX_PLST_UNCOMP_BYTES)
        except (_zstd.ZstdError, ValueError) as exc:
            raise ProtocolError(f"PLST Zstd decompression failed: {exc}") from exc
        if not decompressor.eof or bool(decompressor.unused_data) or len(payload) > MAX_PLST_UNCOMP_BYTES:
            raise ProtocolError("PLST decompressed payload exceeds 64 MiB limit or is incomplete")
        if len(payload) != uncomp_len:
            raise ProtocolError(f"PLST length mismatch: expected {uncomp_len}, got {len(payload)}")
    else:  # pragma: no cover
        raise ProtocolError(f"unsupported compression bits: 0x{comp_bits:02x}")

    return decode_columnar_payload(payload)
