"""Shared duck-typing helpers for zero-dependency SDK adapters."""

from __future__ import annotations

from typing import Any, Optional

from .. import context as _ctx
from ..client import PulseAgentClient, SpanContext, TraceContext
from ..model import SpanType


def _get_field(obj: Any, *names: str, default: Any = None) -> Any:
    if obj is None:
        return default
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _open_adapter_span(
    trace_or_client: Optional[TraceContext | PulseAgentClient],
    *,
    name: str,
    span_type: SpanType,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    input_data: Any = None,
    emit_on_start: bool = False,
    linked_span_ids: Optional[list[str]] = None,
) -> Optional[SpanContext]:
    if isinstance(trace_or_client, TraceContext):
        return trace_or_client.span(
            name=name,
            span_type=span_type,
            provider=provider,
            model=model,
            input=input_data,
            emit_on_start=emit_on_start,
            linked_span_ids=linked_span_ids,
        )
    if isinstance(trace_or_client, PulseAgentClient):
        return trace_or_client.span(
            name=name,
            span_type=span_type,
            provider=provider,
            model=model,
            input=input_data,
            emit_on_start=emit_on_start,
            linked_span_ids=linked_span_ids,
        )
    cur_trace = _ctx.get_current_trace()
    if cur_trace is not None:
        return cur_trace.span(
            name=name,
            span_type=span_type,
            provider=provider,
            model=model,
            input=input_data,
            emit_on_start=emit_on_start,
            linked_span_ids=linked_span_ids,
        )
    cur_client = _ctx.get_current_client()
    if cur_client is not None:
        return cur_client.span(
            name=name,
            span_type=span_type,
            provider=provider,
            model=model,
            input=input_data,
            emit_on_start=emit_on_start,
            linked_span_ids=linked_span_ids,
        )
    return None


def _summarize_value(val: Any) -> Any:
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, dict):
        return {str(k): _summarize_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_summarize_value(item) for item in val]
    if hasattr(val, "model_dump") and callable(val.model_dump):
        try:
            return val.model_dump()
        except Exception:
            pass
    if hasattr(val, "to_dict") and callable(val.to_dict):
        try:
            return val.to_dict()
        except Exception:
            pass
    return repr(val)
