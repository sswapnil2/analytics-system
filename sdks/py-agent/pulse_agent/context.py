"""ContextVar-based propagation for sessions, traces, spans, and clients across sync, asyncio, and threads."""

from __future__ import annotations

import contextvars
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")

_active_client: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar(
    "pulse_agent_active_client", default=None
)
_active_trace: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar(
    "pulse_agent_active_trace", default=None
)
_active_span_stack: contextvars.ContextVar[tuple[Any, ...]] = contextvars.ContextVar(
    "pulse_agent_active_span_stack", default=()
)
_active_session_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pulse_agent_active_session_id", default=None
)
_active_user_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pulse_agent_active_user_id", default=None
)
_active_workflow_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pulse_agent_active_workflow_id", default=None
)
_active_linked_span_ids: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "pulse_agent_active_linked_span_ids", default=()
)


def get_current_client() -> Optional[Any]:
    return _active_client.get()


def get_current_trace() -> Optional[Any]:
    return _active_trace.get()


def get_current_span() -> Optional[Any]:
    stack = _active_span_stack.get()
    return stack[-1] if stack else None


def get_current_span_stack() -> tuple[Any, ...]:
    return _active_span_stack.get()


def get_current_session_id() -> Optional[str]:
    trace = _active_trace.get()
    if trace is not None and getattr(trace, "session_id", None) is not None:
        return trace.session_id
    return _active_session_id.get()


def get_current_user_id() -> Optional[str]:
    trace = _active_trace.get()
    if trace is not None and getattr(trace, "user_id", None) is not None:
        return trace.user_id
    return _active_user_id.get()


def get_current_workflow_id() -> Optional[str]:
    trace = _active_trace.get()
    if trace is not None and getattr(trace, "workflow_id", None) is not None:
        return trace.workflow_id
    return _active_workflow_id.get()


def get_current_linked_span_ids() -> tuple[str, ...]:
    return _active_linked_span_ids.get()


def copy_context() -> contextvars.Context:
    return contextvars.copy_context()


def run_in_context(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    ctx = contextvars.copy_context()
    return ctx.run(fn, *args, **kwargs)
