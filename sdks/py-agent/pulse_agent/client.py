"""PulseAgentClient runtime, context managers, streaming span_phase, ring-buffer batching, and decorators."""

from __future__ import annotations

from collections import OrderedDict, deque
import contextlib
import functools
import inspect
import random
import threading
import time
import traceback
from typing import Any, Callable, Iterator, Optional, Sequence, TypeVar

from ._xxh3 import xxh3_64
from . import context as _ctx
from .model import (
    AgentSpan,
    AgentTrace,
    FailureSource,
    ModelPricingRule,
    SpanEvent,
    SpanPhase,
    SpanType,
    TraceIngestBatch,
    TraceStatus,
    new_id,
    now_ms,
)
from .pricing import cost_nanos_to_usd, estimate_cost_nanos, sync_span_cost, sync_trace_cost
from .privacy import (
    MAX_SPAN_DEPTH,
    MAX_SPANS_PER_TRACE,
    PrivacyConfig,
    SanitizeStats,
    sanitize_span,
    sanitize_trace,
)
from .protocol import CompressionMode
from .transport import HttpTransport

F = TypeVar("F", bound=Callable[..., Any])


class SessionContext:
    """Sync & async context manager propagating session_id, user_id, and workflow_id."""

    def __init__(
        self,
        client: "PulseAgentClient",
        *,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> None:
        self.client = client
        self.session_id = session_id or new_id("sess")
        self.user_id = user_id
        self.workflow_id = workflow_id
        self._tokens: list[tuple[Any, Any]] = []

    def __enter__(self) -> "SessionContext":
        self._tokens.append((_ctx._active_client, _ctx._active_client.set(self.client)))
        self._tokens.append((_ctx._active_session_id, _ctx._active_session_id.set(self.session_id)))
        if self.user_id is not None:
            self._tokens.append((_ctx._active_user_id, _ctx._active_user_id.set(self.user_id)))
        if self.workflow_id is not None:
            self._tokens.append(
                (_ctx._active_workflow_id, _ctx._active_workflow_id.set(self.workflow_id))
            )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        while self._tokens:
            var, tok = self._tokens.pop()
            try:
                var.reset(tok)
            except ValueError:
                pass

    async def __aenter__(self) -> "SessionContext":
        return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)

    def trace(self, *args: Any, **kwargs: Any) -> "TraceContext":
        kwargs.setdefault("session_id", self.session_id)
        if self.user_id is not None:
            kwargs.setdefault("user_id", self.user_id)
        if self.workflow_id is not None:
            kwargs.setdefault("workflow_id", self.workflow_id)
        return self.client.trace(*args, **kwargs)


class SpanContext:
    """Sync & async context manager representing an AgentSpan with streaming span_phase support."""

    def __init__(
        self,
        client: "PulseAgentClient",
        *,
        trace_id: str,
        span_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        linked_span_ids: Optional[Sequence[str]] = None,
        name: str = "span",
        span_type: SpanType | str = SpanType.CUSTOM,
        depth: int = 0,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        model_id: Optional[str] = None,
        model_version: Optional[str] = None,
        prompt_template_id: Optional[str] = None,
        prompt_template_version: Optional[str] = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: int = 0,
        cost_nanos: int = 0,
        cost_usd: float = 0.0,
        temperature: Optional[float] = None,
        finish_reason: Optional[str] = None,
        model_params: Any = None,
        tool_name: Optional[str] = None,
        tool_version: Optional[str] = None,
        tool_retry_count: int = 0,
        input: Any = None,
        output: Any = None,
        attributes: Optional[dict[str, Any]] = None,
        metadata: Any = None,
        emit_on_start: bool = False,
        trace_ctx: Optional["TraceContext"] = None,
        sampled: bool = True,
    ) -> None:
        self.client = client
        self.trace_id = trace_id
        self.span_id = span_id or new_id("sp")
        self.parent_span_id = parent_span_id
        self.linked_span_ids: list[str] = list(linked_span_ids) if linked_span_ids else []
        self.name = name
        self.span_type = SpanType.parse(span_type)
        self.depth = max(0, min(MAX_SPAN_DEPTH, int(depth)))
        self.status = TraceStatus.OK
        self.start_ts_ms = now_ms()
        self.end_ts_ms: Optional[int] = None
        self.duration_ms = 0

        self.provider = provider
        self.model = model
        self.model_id = model_id
        self.model_version = model_version
        self.prompt_template_id = prompt_template_id
        self.prompt_template_version = prompt_template_version
        self.input_tokens = max(0, int(input_tokens))
        self.output_tokens = max(0, int(output_tokens))
        self.cached_tokens = max(0, int(cached_tokens))
        self.total_tokens = max(0, int(total_tokens))
        self.cost_nanos = int(cost_nanos)
        self.cost_usd = float(cost_usd)
        self.temperature = temperature
        self.finish_reason = finish_reason
        self.model_params = model_params
        self.tool_name = tool_name
        self.tool_version = tool_version
        self.tool_retry_count = int(tool_retry_count)

        self.input = input
        self.output = output
        self.attributes: dict[str, Any] = dict(attributes) if attributes else {}
        self.events: list[SpanEvent] = []
        self.metadata = metadata
        self.error_type: Optional[str] = None
        self.error_message: Optional[str] = None
        self.error_stack: Optional[str] = None

        self.emit_on_start = emit_on_start
        self.trace_ctx = trace_ctx
        self.sampled = sampled
        self._ended = False
        self._start_emitted = False
        self._tokens: list[tuple[Any, Any]] = []

    def _build_model(self, *, phase: SpanPhase, status: TraceStatus, end_ts: Optional[int], duration_ms: int) -> AgentSpan:
        in_tok = max(0, int(self.input_tokens))
        out_tok = max(0, int(self.output_tokens))
        cached_tok = max(0, int(self.cached_tokens))
        tot_tok = max(0, int(self.total_tokens))
        if tot_tok == 0 and (in_tok > 0 or out_tok > 0):
            tot_tok = in_tok + out_tok

        c_nanos = int(self.cost_nanos)
        c_usd = float(self.cost_usd)
        if c_nanos == 0 and c_usd == 0.0 and self.model and (in_tok > 0 or out_tok > 0 or cached_tok > 0):
            c_nanos = estimate_cost_nanos(
                self.provider,
                self.model,
                in_tok,
                out_tok,
                cached_tok,
                self.client.custom_pricing,
            )
            c_usd = cost_nanos_to_usd(c_nanos)

        sp = AgentSpan(
            span_id=self.span_id,
            trace_id=self.trace_id,
            project_id=self.client.project_id,
            parent_span_id=self.parent_span_id,
            linked_span_ids=list(self.linked_span_ids),
            span_phase=phase,
            depth=self.depth,
            name=self.name,
            span_type=self.span_type,
            status=status,
            start_ts_ms=self.start_ts_ms,
            end_ts_ms=end_ts,
            duration_ms=duration_ms,
            provider=self.provider,
            model=self.model,
            model_id=self.model_id,
            model_version=self.model_version,
            prompt_template_id=self.prompt_template_id,
            prompt_template_version=self.prompt_template_version,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cached_tokens=cached_tok,
            total_tokens=tot_tok,
            cost_nanos=c_nanos,
            cost_usd=c_usd,
            temperature=self.temperature,
            finish_reason=self.finish_reason,
            model_params=self.model_params,
            tool_name=self.tool_name,
            tool_version=self.tool_version,
            tool_retry_count=self.tool_retry_count,
            input=self.input,
            output=self.output,
            attributes=dict(self.attributes) if self.attributes else None,
            events=list(self.events),
            metadata=self.metadata,
            error_type=self.error_type,
            error_message=self.error_message,
            error_stack=self.error_stack,
        )
        sync_span_cost(sp)
        return sp

    def enter_context(self) -> None:
        stack = _ctx._active_span_stack.get()
        self._tokens.append((_ctx._active_span_stack, _ctx._active_span_stack.set(stack + (self,))))
        self._tokens.append((_ctx._active_client, _ctx._active_client.set(self.client)))

    def detach_context(self) -> None:
        while self._tokens:
            var, tok = self._tokens.pop()
            try:
                var.reset(tok)
            except ValueError:
                pass

    def emit_start(self) -> None:
        if self.emit_on_start and self.sampled and not self._start_emitted and not self._ended:
            self._start_emitted = True
            self.client._enqueue_span(
                self._build_model(
                    phase=SpanPhase.START,
                    status=TraceStatus.RUNNING,
                    end_ts=None,
                    duration_ms=0,
                )
            )

    @contextlib.contextmanager
    def scoped_context(self) -> Iterator["SpanContext"]:
        self.enter_context()
        try:
            yield self
        finally:
            self.detach_context()

    def __enter__(self) -> "SpanContext":
        self.enter_context()
        self.emit_start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_val is not None and not self._ended:
                self.set_error(exc_val, exc_tb=exc_tb)
            self.end()
        finally:
            self.detach_context()

    async def __aenter__(self) -> "SpanContext":
        return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)

    def set_input(self, value: Any) -> "SpanContext":
        self.input = value
        return self

    def set_output(self, value: Any) -> "SpanContext":
        self.output = value
        return self

    def set_metadata(self, value: Any) -> "SpanContext":
        self.metadata = value
        return self

    def set_attribute(self, key: str, value: Any) -> "SpanContext":
        self.attributes[str(key)] = value
        return self

    def set_attributes(self, attrs: dict[str, Any]) -> "SpanContext":
        for k, v in attrs.items():
            self.attributes[str(k)] = v
        return self

    def link_span(self, span_id: str) -> "SpanContext":
        sid = str(span_id).strip()
        if sid and sid != self.span_id and sid not in self.linked_span_ids:
            self.linked_span_ids.append(sid)
        return self

    def add_event(
        self,
        name: str,
        attributes: Any = None,
        timestamp_ms: Optional[int] = None,
    ) -> "SpanContext":
        self.events.append(
            SpanEvent(
                name=str(name),
                timestamp_ms=int(timestamp_ms) if timestamp_ms is not None else now_ms(),
                attributes=attributes,
            )
        )
        return self

    def set_usage(
        self,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: Optional[int] = None,
        cost_nanos: Optional[int] = None,
        cost_usd: Optional[float] = None,
    ) -> "SpanContext":
        self.input_tokens = max(0, int(input_tokens))
        self.output_tokens = max(0, int(output_tokens))
        self.cached_tokens = max(0, int(cached_tokens))
        if total_tokens is not None:
            self.total_tokens = max(0, int(total_tokens))
        else:
            self.total_tokens = self.input_tokens + self.output_tokens
        if cost_nanos is not None:
            self.cost_nanos = max(0, int(cost_nanos))
            self.cost_usd = cost_nanos_to_usd(self.cost_nanos)
        elif cost_usd is not None:
            self.cost_usd = float(cost_usd)
        return self

    def set_model(
        self,
        model: Optional[str],
        *,
        provider: Optional[str] = None,
        model_id: Optional[str] = None,
        model_version: Optional[str] = None,
        temperature: Optional[float] = None,
        finish_reason: Optional[str] = None,
        model_params: Any = None,
    ) -> "SpanContext":
        if model is not None:
            self.model = model
        if provider is not None:
            self.provider = provider
        if model_id is not None:
            self.model_id = model_id
        if model_version is not None:
            self.model_version = model_version
        if temperature is not None:
            self.temperature = temperature
        if finish_reason is not None:
            self.finish_reason = finish_reason
        if model_params is not None:
            self.model_params = model_params
        return self

    def set_error(
        self,
        error: BaseException | str,
        *,
        error_type: Optional[str] = None,
        exc_tb: Any = None,
        status: TraceStatus = TraceStatus.ERROR,
    ) -> "SpanContext":
        self.status = status
        if isinstance(error, BaseException):
            self.error_type = error_type or type(error).__name__
            self.error_message = str(error)
            if exc_tb is not None:
                self.error_stack = "".join(traceback.format_exception(type(error), error, exc_tb))
            elif error.__traceback__ is not None:
                self.error_stack = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )
        else:
            self.error_type = error_type or "Error"
            self.error_message = str(error)
        return self

    def update(
        self,
        *,
        output: Any = None,
        attributes: Optional[dict[str, Any]] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        cached_tokens: Optional[int] = None,
        total_tokens: Optional[int] = None,
        finish_reason: Optional[str] = None,
        emit: bool = True,
    ) -> "SpanContext":
        if output is not None:
            self.output = output
        if attributes:
            self.set_attributes(attributes)
        if input_tokens is not None:
            self.input_tokens = max(0, int(input_tokens))
        if output_tokens is not None:
            self.output_tokens = max(0, int(output_tokens))
        if cached_tokens is not None:
            self.cached_tokens = max(0, int(cached_tokens))
        if total_tokens is not None:
            self.total_tokens = max(0, int(total_tokens))
        if finish_reason is not None:
            self.finish_reason = finish_reason
        if emit and self.sampled and not self._ended:
            elapsed = max(0, now_ms() - self.start_ts_ms)
            self.client._enqueue_span(
                self._build_model(
                    phase=SpanPhase.UPDATE,
                    status=TraceStatus.RUNNING,
                    end_ts=None,
                    duration_ms=elapsed,
                )
            )
        return self

    def end(
        self,
        *,
        output: Any = None,
        status: Optional[TraceStatus | str] = None,
        end_ts_ms: Optional[int] = None,
    ) -> Optional[AgentSpan]:
        if self._ended:
            return None
        self._ended = True
        if output is not None:
            self.output = output
        if status is not None:
            self.status = TraceStatus.parse(status)
        end_ms = int(end_ts_ms) if end_ts_ms is not None else now_ms()
        self.end_ts_ms = end_ms
        self.duration_ms = max(0, end_ms - self.start_ts_ms)
        model = self._build_model(
            phase=SpanPhase.FINAL,
            status=self.status,
            end_ts=end_ms,
            duration_ms=self.duration_ms,
        )
        if self.trace_ctx is not None:
            self.trace_ctx._record_finished_span(model)
        if self.sampled:
            self.client._enqueue_span(model)
        return model

    # Nested child helpers
    def span(self, name: str = "span", **kwargs: Any) -> "SpanContext":
        kwargs.setdefault("trace_id", self.trace_id)
        kwargs.setdefault("parent_span_id", self.span_id)
        kwargs.setdefault("depth", min(MAX_SPAN_DEPTH, self.depth + 1))
        kwargs.setdefault("trace_ctx", self.trace_ctx)
        kwargs.setdefault("sampled", self.sampled)
        return self.client.span(name=name, **kwargs)

    def generation(self, name: str = "llm.generation", **kwargs: Any) -> "SpanContext":
        kwargs["span_type"] = SpanType.LLM
        return self.span(name=name, **kwargs)

    def tool(self, name: str = "tool.call", **kwargs: Any) -> "SpanContext":
        kwargs["span_type"] = SpanType.TOOL
        return self.span(name=name, **kwargs)

    def guardrail(self, name: str = "guardrail.check", **kwargs: Any) -> "SpanContext":
        kwargs["span_type"] = SpanType.GUARDRAIL
        return self.span(name=name, **kwargs)

    def retrieval(self, name: str = "retrieval.search", **kwargs: Any) -> "SpanContext":
        kwargs["span_type"] = SpanType.RETRIEVAL
        return self.span(name=name, **kwargs)


class TraceContext:
    """Sync & async context manager representing an AgentTrace."""

    def __init__(
        self,
        client: "PulseAgentClient",
        *,
        trace_id: Optional[str] = None,
        agent_name: Optional[str] = None,
        name: Optional[str] = None,
        agent_version: Optional[str] = None,
        prompt_version: Optional[str] = None,
        model_config_version: Optional[str] = None,
        tool_config_version: Optional[str] = None,
        app_release: Optional[str] = None,
        environment: Optional[str] = None,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        parent_trace_id: Optional[str] = None,
        input: Any = None,
        output: Any = None,
        metadata: Any = None,
        tags: Optional[Sequence[str]] = None,
        emit_on_start: bool = False,
        sampled: Optional[bool] = None,
    ) -> None:
        self.client = client
        self.trace_id = trace_id or new_id("tr")
        self.agent_name = agent_name or name or client.agent_name
        self.agent_version = agent_version or client.agent_version
        self.prompt_version = prompt_version if prompt_version is not None else client.prompt_version
        self.model_config_version = (
            model_config_version
            if model_config_version is not None
            else client.model_config_version
        )
        self.tool_config_version = (
            tool_config_version
            if tool_config_version is not None
            else client.tool_config_version
        )
        self.app_release = app_release if app_release is not None else client.app_release
        self.environment = environment or client.environment
        self.session_id = session_id if session_id is not None else _ctx._active_session_id.get()
        self.user_id = user_id if user_id is not None else _ctx._active_user_id.get()
        self.workflow_id = workflow_id if workflow_id is not None else _ctx._active_workflow_id.get()
        self.parent_trace_id = parent_trace_id
        if self.parent_trace_id is None:
            cur_tr = _ctx.get_current_trace()
            if cur_tr is not None:
                self.parent_trace_id = cur_tr.trace_id

        self.status = TraceStatus.OK
        self.start_ts_ms = now_ms()
        self.end_ts_ms: Optional[int] = None
        self.duration_ms = 0
        self.input = input
        self.output = output
        self.error_type: Optional[str] = None
        self.error_message: Optional[str] = None
        self.failure_source: Optional[FailureSource] = None
        self.metadata = dict(metadata) if isinstance(metadata, dict) else metadata
        if name and agent_name and name != agent_name and isinstance(self.metadata, dict):
            self.metadata.setdefault("trace_name", name)
        self.tags: list[str] = list(tags) if tags else []

        self.span_count = 0
        self.llm_call_count = 0
        self.tool_call_count = 0
        self.error_span_count = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.cached_tokens = 0
        self.total_tokens = 0
        self.total_cost_nanos = 0
        self.total_cost_usd = 0.0
        self.primary_model: Optional[str] = None
        self.primary_provider: Optional[str] = None
        self.has_loop_warning = False
        self.last_retrieval_span_id: Optional[str] = None

        self.emit_on_start = emit_on_start
        self.sampled = client._decide_sample(self.trace_id) if sampled is None else sampled
        self._ended = False
        self._lock = threading.Lock()
        self._tokens: list[tuple[Any, Any]] = []

    def _record_finished_span(self, span: AgentSpan) -> None:
        with self._lock:
            self.span_count += 1
            if span.span_type == SpanType.LLM:
                self.llm_call_count += 1
                if self.primary_model is None and span.model:
                    self.primary_model = span.model
                if self.primary_provider is None and span.provider:
                    self.primary_provider = span.provider
            elif span.span_type == SpanType.TOOL:
                self.tool_call_count += 1
            elif span.span_type == SpanType.RETRIEVAL:
                self.last_retrieval_span_id = span.span_id
            if span.status in (TraceStatus.ERROR, TraceStatus.TIMEOUT):
                self.error_span_count += 1
            self.input_tokens += max(0, span.input_tokens)
            self.output_tokens += max(0, span.output_tokens)
            self.cached_tokens += max(0, span.cached_tokens)
            self.total_tokens += max(0, span.total_tokens)
            self.total_cost_nanos += max(0, span.cost_nanos)
            self.total_cost_usd = cost_nanos_to_usd(self.total_cost_nanos)

    def _build_model(self, *, status: TraceStatus, end_ts: Optional[int], duration_ms: int) -> AgentTrace:
        with self._lock:
            tr = AgentTrace(
                trace_id=self.trace_id,
                project_id=self.client.project_id,
                agent_name=self.agent_name,
                agent_version=self.agent_version,
                prompt_version=self.prompt_version,
                model_config_version=self.model_config_version,
                tool_config_version=self.tool_config_version,
                app_release=self.app_release,
                environment=self.environment,
                session_id=self.session_id,
                user_id=self.user_id,
                workflow_id=self.workflow_id,
                parent_trace_id=self.parent_trace_id,
                status=status,
                start_ts_ms=self.start_ts_ms,
                end_ts_ms=end_ts,
                duration_ms=duration_ms,
                input=self.input,
                output=self.output,
                error_type=self.error_type,
                error_message=self.error_message,
                failure_source=self.failure_source,
                metadata=self.metadata,
                tags=list(self.tags),
                span_count=self.span_count,
                llm_call_count=self.llm_call_count,
                tool_call_count=self.tool_call_count,
                error_span_count=self.error_span_count,
                input_tokens=self.input_tokens,
                output_tokens=self.output_tokens,
                cached_tokens=self.cached_tokens,
                total_tokens=self.total_tokens,
                total_cost_nanos=self.total_cost_nanos,
                total_cost_usd=cost_nanos_to_usd(self.total_cost_nanos),
                primary_model=self.primary_model,
                primary_provider=self.primary_provider,
                has_loop_warning=self.has_loop_warning,
            )
            sync_trace_cost(tr)
            return tr

    def __enter__(self) -> "TraceContext":
        self._tokens.append((_ctx._active_client, _ctx._active_client.set(self.client)))
        self._tokens.append((_ctx._active_trace, _ctx._active_trace.set(self)))
        if self.session_id is not None:
            self._tokens.append(
                (_ctx._active_session_id, _ctx._active_session_id.set(self.session_id))
            )
        if self.user_id is not None:
            self._tokens.append((_ctx._active_user_id, _ctx._active_user_id.set(self.user_id)))
        if self.workflow_id is not None:
            self._tokens.append(
                (_ctx._active_workflow_id, _ctx._active_workflow_id.set(self.workflow_id))
            )
        if self.emit_on_start and self.sampled:
            self.client._enqueue_trace(
                self._build_model(status=TraceStatus.RUNNING, end_ts=None, duration_ms=0)
            )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_val is not None and not self._ended:
            self.set_error(exc_val)
        self.end()
        while self._tokens:
            var, tok = self._tokens.pop()
            try:
                var.reset(tok)
            except ValueError:
                pass

    async def __aenter__(self) -> "TraceContext":
        return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)

    def set_input(self, value: Any) -> "TraceContext":
        self.input = value
        return self

    def set_output(self, value: Any) -> "TraceContext":
        self.output = value
        return self

    def set_metadata(self, value: Any) -> "TraceContext":
        self.metadata = value
        return self

    def add_tag(self, tag: str) -> "TraceContext":
        if tag:
            with self._lock:
                if tag not in self.tags:
                    self.tags.append(str(tag))
        return self

    def set_error(
        self,
        error: BaseException | str,
        *,
        error_type: Optional[str] = None,
        failure_source: Optional[FailureSource | str] = None,
        status: TraceStatus = TraceStatus.ERROR,
    ) -> "TraceContext":
        self.status = status
        if isinstance(error, BaseException):
            self.error_type = error_type or type(error).__name__
            self.error_message = str(error)
        else:
            self.error_type = error_type or "Error"
            self.error_message = str(error)
        if failure_source is not None:
            self.failure_source = FailureSource.parse(failure_source)
        return self

    def update(self, *, output: Any = None, metadata: Any = None, emit: bool = False) -> "TraceContext":
        if output is not None:
            self.output = output
        if metadata is not None:
            self.metadata = metadata
        if emit and self.sampled and not self._ended:
            self.client._enqueue_trace(
                self._build_model(
                    status=TraceStatus.RUNNING,
                    end_ts=None,
                    duration_ms=max(0, now_ms() - self.start_ts_ms),
                )
            )
        return self

    def end(
        self,
        *,
        output: Any = None,
        status: Optional[TraceStatus | str] = None,
        end_ts_ms: Optional[int] = None,
    ) -> Optional[AgentTrace]:
        if self._ended:
            return None
        self._ended = True
        if output is not None:
            self.output = output
        if status is not None:
            self.status = TraceStatus.parse(status)
        end_ms = int(end_ts_ms) if end_ts_ms is not None else now_ms()
        self.end_ts_ms = end_ms
        self.duration_ms = max(0, end_ms - self.start_ts_ms)
        model = self._build_model(status=self.status, end_ts=end_ms, duration_ms=self.duration_ms)
        if self.sampled:
            self.client._enqueue_trace(model)
        return model

    def span(self, name: str = "span", **kwargs: Any) -> SpanContext:
        kwargs.setdefault("trace_id", self.trace_id)
        kwargs.setdefault("trace_ctx", self)
        kwargs.setdefault("sampled", self.sampled)
        cur_sp = _ctx.get_current_span()
        if cur_sp is not None and cur_sp.trace_id == self.trace_id:
            kwargs.setdefault("parent_span_id", cur_sp.span_id)
            kwargs.setdefault("depth", cur_sp.depth + 1)
        if kwargs.get("linked_span_ids") is None and self.last_retrieval_span_id:
            st = SpanType.parse(kwargs.get("span_type", SpanType.CUSTOM))
            if st == SpanType.LLM:
                kwargs["linked_span_ids"] = [self.last_retrieval_span_id]
        return self.client.span(name=name, **kwargs)

    def generation(self, name: str = "llm.generation", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.LLM
        return self.span(name=name, **kwargs)

    def tool(self, name: str = "tool.call", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.TOOL
        return self.span(name=name, **kwargs)

    def guardrail(self, name: str = "guardrail.check", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.GUARDRAIL
        return self.span(name=name, **kwargs)

    def retrieval(self, name: str = "retrieval.search", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.RETRIEVAL
        return self.span(name=name, **kwargs)


class PulseAgentClient:
    """Thread-safe, async-friendly zero-dependency observability client for Pulse AI Agent Tracing."""

    def __init__(
        self,
        api_key: str = "",
        endpoint: str = "http://localhost:8080",
        *,
        project_id: str = "",
        environment: str = "production",
        agent_name: str = "default-agent",
        agent_version: str = "unversioned",
        prompt_version: Optional[str] = None,
        model_config_version: Optional[str] = None,
        tool_config_version: Optional[str] = None,
        app_release: Optional[str] = None,
        use_plst: bool = True,
        use_binary: Optional[bool] = None,
        compression: CompressionMode = "auto",
        sample_rate: float = 1.0,
        deterministic_sampling: bool = True,
        max_queue_size: int = 2000,
        flush_batch_size: int = 50,
        flush_interval_ms: int = 5000,
        request_timeout_ms: int = 10_000,
        max_retries: int = 3,
        retry_base_ms: int = 200,
        privacy: Optional[PrivacyConfig] = None,
        capture_inputs: bool = True,
        capture_outputs: bool = True,
        capture_prompts: bool = True,
        capture_responses: bool = True,
        capture_tool_args: bool = True,
        capture_tool_results: bool = True,
        capture_user_id: bool = True,
        redact_pii: bool = True,
        redact_keys: Optional[Sequence[str]] = None,
        exclude_attributes: Optional[Sequence[str]] = None,
        custom_pricing: Optional[Sequence[ModelPricingRule]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs: Any,
    ) -> None:
        if not api_key and "key" in kwargs:
            api_key = str(kwargs["key"])
        for alias in ("base_url", "host", "url"):
            if alias in kwargs and kwargs[alias]:
                endpoint = str(kwargs[alias])
                break
        if "flush_interval_s" in kwargs and kwargs["flush_interval_s"] is not None:
            flush_interval_ms = int(float(kwargs["flush_interval_s"]) * 1000)
        for q_alias in ("queue_size", "capacity"):
            if q_alias in kwargs and kwargs[q_alias] is not None:
                max_queue_size = int(kwargs[q_alias])

        self.api_key = api_key
        self.endpoint = endpoint.rstrip("/")
        self.project_id = project_id
        self.environment = environment
        self.agent_name = agent_name
        self.agent_version = agent_version
        self.prompt_version = prompt_version
        self.model_config_version = model_config_version
        self.tool_config_version = tool_config_version
        self.app_release = app_release

        effective_plst = bool(use_binary) if use_binary is not None else bool(use_plst)
        self.use_plst = effective_plst
        self.compression: CompressionMode = compression
        self.sample_rate = max(0.0, min(1.0, float(sample_rate)))
        self.deterministic_sampling = bool(deterministic_sampling)
        self.max_queue_size = max(1, int(max_queue_size))
        self.flush_batch_size = max(1, int(flush_batch_size))
        self.flush_interval_ms = max(0, int(flush_interval_ms))
        self.custom_pricing: tuple[ModelPricingRule, ...] = (
            tuple(custom_pricing) if custom_pricing else ()
        )

        if privacy is not None:
            self.privacy = privacy
        else:
            self.privacy = PrivacyConfig.with_custom_redact_keys(
                redact_keys,
                merge_defaults=True,
                capture_inputs=capture_inputs,
                capture_outputs=capture_outputs,
                capture_prompts=capture_prompts,
                capture_responses=capture_responses,
                capture_tool_args=capture_tool_args,
                capture_tool_results=capture_tool_results,
                capture_user_id=capture_user_id,
                redact_pii=redact_pii,
                exclude_attributes=tuple(exclude_attributes) if exclude_attributes else (),
            )

        self.transport = HttpTransport(
            self.endpoint,
            self.api_key,
            use_plst=effective_plst,
            compression=compression,
            request_timeout_ms=request_timeout_ms,
            max_retries=max_retries,
            retry_base_ms=retry_base_ms,
            on_error=on_error,
        )

        self._queue: deque[tuple[str, Any]] = deque()
        self._trace_span_counts: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._closed = False
        self._stats: dict[str, int] = {
            "enqueued_traces": 0,
            "enqueued_spans": 0,
            "flushed_batches": 0,
            "flushed_traces": 0,
            "flushed_spans": 0,
            "dropped_traces": 0,
            "dropped_spans": 0,
            "dropped_queue_full": 0,
            "dropped_oversized": 0,
            "dropped_permanent": 0,
            "dropped_retry_exhausted": 0,
            "sampled_out": 0,
            "retries": 0,
            "http_errors": 0,
            "redacted_fields": 0,
            "truncated_payloads": 0,
        }

        self._worker: Optional[threading.Thread] = None
        if self.flush_interval_ms > 0:
            self._worker = threading.Thread(
                target=self._background_loop,
                name="pulse-agent-flusher",
                daemon=True,
            )
            self._worker.start()

    def __enter__(self) -> "PulseAgentClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown()

    def _decide_sample(self, trace_id: str) -> bool:
        if self.sample_rate <= 0.0:
            with self._lock:
                self._stats["sampled_out"] += 1
            return False
        if self.sample_rate >= 1.0:
            return True
        if self.deterministic_sampling:
            slot = xxh3_64(trace_id.encode("utf-8")) % 10_000
            keep = slot < int(self.sample_rate * 10_000)
        else:
            keep = random.random() < self.sample_rate
        if not keep:
            with self._lock:
                self._stats["sampled_out"] += 1
        return keep

    def _enqueue_item(self, kind: str, item: Any, st: SanitizeStats) -> None:
        with self._lock:
            if self._closed:
                return
            if kind == "span":
                tid = item.trace_id
                cnt = self._trace_span_counts.get(tid, 0)
                if cnt >= MAX_SPANS_PER_TRACE:
                    self._stats["dropped_spans"] += 1
                    return
                self._trace_span_counts[tid] = cnt + 1
                self._trace_span_counts.move_to_end(tid)
                if len(self._trace_span_counts) > 16_384:
                    self._trace_span_counts.popitem(last=False)
            self._stats["redacted_fields"] += st.redacted_fields
            self._stats["truncated_payloads"] += st.truncated_payloads
            if len(self._queue) >= self.max_queue_size:
                evicted_kind, _ = self._queue.popleft()
                self._stats["dropped_queue_full"] += 1
                if evicted_kind == "trace":
                    self._stats["dropped_traces"] += 1
                else:
                    self._stats["dropped_spans"] += 1
            self._queue.append((kind, item))
            if kind == "trace":
                self._stats["enqueued_traces"] += 1
            else:
                self._stats["enqueued_spans"] += 1

    def _enqueue_trace(self, trace: AgentTrace) -> None:
        sanitized, st = sanitize_trace(trace, self.privacy, inplace=True)
        self._enqueue_item("trace", sanitized, st)

    def _enqueue_span(self, span: AgentSpan) -> None:
        sanitized, st = sanitize_span(span, self.privacy, inplace=True)
        self._enqueue_item("span", sanitized, st)

    def record_trace(self, trace: AgentTrace | dict[str, Any]) -> None:
        tr = trace if isinstance(trace, AgentTrace) else AgentTrace.from_dict(trace)
        if not tr.project_id and self.project_id:
            tr.project_id = self.project_id
        if not self._decide_sample(tr.trace_id):
            return
        self._enqueue_trace(tr)

    def record_span(self, span: AgentSpan | dict[str, Any]) -> None:
        sp = span if isinstance(span, AgentSpan) else AgentSpan.from_dict(span)
        if not sp.project_id and self.project_id:
            sp.project_id = self.project_id
        if not self._decide_sample(sp.trace_id):
            return
        sp.depth = max(0, min(MAX_SPAN_DEPTH, int(sp.depth)))
        self._enqueue_span(sp)

    def record_batch(
        self,
        *,
        traces: Sequence[AgentTrace | dict[str, Any]] = (),
        spans: Sequence[AgentSpan | dict[str, Any]] = (),
    ) -> None:
        for t in traces:
            self.record_trace(t)
        for s in spans:
            self.record_span(s)

    def stats(self) -> dict[str, int]:
        with self._lock:
            snap = dict(self._stats)
            snap["queued"] = len(self._queue)
            return snap

    @property
    def dropped_count(self) -> int:
        with self._lock:
            return self._stats["dropped_traces"] + self._stats["dropped_spans"]

    def _drain_chunk(self, max_items: int) -> TraceIngestBatch:
        traces: list[AgentTrace] = []
        spans: list[AgentSpan] = []
        with self._lock:
            count = 0
            while self._queue and count < max_items:
                kind, item = self._queue.popleft()
                if kind == "trace":
                    traces.append(item)
                else:
                    spans.append(item)
                count += 1
        return TraceIngestBatch(traces=traces, spans=spans)

    def _flush_impl(
        self,
        *,
        timeout_s: Optional[float] = None,
        deadline: Optional[float] = None,
        abort_event: Optional[threading.Event] = None,
    ) -> bool:
        if deadline is None and timeout_s is not None:
            deadline = time.monotonic() + max(0.0, float(timeout_s))
        if deadline is not None:
            rem = max(0.0, deadline - time.monotonic())
            acquired = self._flush_lock.acquire(timeout=rem)
        else:
            acquired = self._flush_lock.acquire()
        if not acquired:
            return False

        try:
            all_ok = True
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    with self._lock:
                        has_more = bool(self._queue)
                    if has_more:
                        all_ok = False
                    break
                batch = self._drain_chunk(self.flush_batch_size)
                if not batch.traces and not batch.spans:
                    break
                res = self.transport.send_batch(
                    batch, deadline=deadline, abort_event=abort_event
                )
                with self._lock:
                    self._stats["flushed_batches"] += res.batches_sent
                    self._stats["flushed_traces"] += res.traces_sent
                    self._stats["flushed_spans"] += res.spans_sent
                    self._stats["retries"] += res.retries
                    self._stats["http_errors"] += res.http_errors
                    self._stats["dropped_oversized"] += res.dropped_oversized
                    self._stats["dropped_permanent"] += res.dropped_permanent
                    self._stats["dropped_retry_exhausted"] += res.dropped_retry_exhausted
                if not res.ok:
                    all_ok = False
            return all_ok
        finally:
            self._flush_lock.release()

    def flush(self, timeout_s: Optional[float] = None) -> bool:
        return self._flush_impl(timeout_s=timeout_s)

    def _background_loop(self) -> None:
        interval_s = self.flush_interval_ms / 1000.0
        while not self._stop_event.wait(interval_s):
            try:
                self._flush_impl(abort_event=self._stop_event)
            except Exception:
                pass

    def shutdown(self, timeout_s: float = 5.0) -> None:
        with self._lock:
            self._closed = True
        self._stop_event.set()
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        if self._worker is not None and self._worker.is_alive():
            rem = max(0.0, deadline - time.monotonic())
            self._worker.join(timeout=rem)
        rem = max(0.0, deadline - time.monotonic())
        self._flush_impl(timeout_s=rem, deadline=deadline, abort_event=None)

    def close(self, timeout_s: float = 5.0) -> None:
        self.shutdown(timeout_s=timeout_s)

    # Context managers
    def session(
        self,
        *,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> SessionContext:
        return SessionContext(
            self, session_id=session_id, user_id=user_id, workflow_id=workflow_id
        )

    def trace(
        self,
        name: Optional[str] = None,
        *,
        agent_name: Optional[str] = None,
        trace_id: Optional[str] = None,
        agent_version: Optional[str] = None,
        prompt_version: Optional[str] = None,
        model_config_version: Optional[str] = None,
        tool_config_version: Optional[str] = None,
        app_release: Optional[str] = None,
        environment: Optional[str] = None,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        parent_trace_id: Optional[str] = None,
        input: Any = None,
        output: Any = None,
        metadata: Any = None,
        tags: Optional[Sequence[str]] = None,
        emit_on_start: bool = False,
    ) -> TraceContext:
        return TraceContext(
            self,
            trace_id=trace_id,
            agent_name=agent_name,
            name=name,
            agent_version=agent_version,
            prompt_version=prompt_version,
            model_config_version=model_config_version,
            tool_config_version=tool_config_version,
            app_release=app_release,
            environment=environment,
            session_id=session_id,
            user_id=user_id,
            workflow_id=workflow_id,
            parent_trace_id=parent_trace_id,
            input=input,
            output=output,
            metadata=metadata,
            tags=tags,
            emit_on_start=emit_on_start,
        )

    def span(
        self,
        name: str = "span",
        *,
        span_type: SpanType | str = SpanType.CUSTOM,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        linked_span_ids: Optional[Sequence[str]] = None,
        depth: Optional[int] = None,
        trace_ctx: Optional[TraceContext] = None,
        sampled: Optional[bool] = None,
        **kwargs: Any,
    ) -> SpanContext:
        cur_trace = trace_ctx if trace_ctx is not None else _ctx.get_current_trace()
        cur_span = _ctx.get_current_span()
        resolved_trace_id = trace_id or (cur_trace.trace_id if cur_trace is not None else new_id("tr"))
        if parent_span_id is None and cur_span is not None and cur_span.trace_id == resolved_trace_id:
            parent_span_id = cur_span.span_id
        if depth is None:
            if cur_span is not None and cur_span.trace_id == resolved_trace_id:
                depth = min(MAX_SPAN_DEPTH, cur_span.depth + 1)
            else:
                depth = 0
        else:
            depth = max(0, min(MAX_SPAN_DEPTH, int(depth)))
        resolved_sampled = (
            sampled
            if sampled is not None
            else (cur_trace.sampled if cur_trace is not None else self._decide_sample(resolved_trace_id))
        )
        if linked_span_ids is None:
            ctx_links = _ctx.get_current_linked_span_ids()
            if ctx_links:
                linked_span_ids = list(ctx_links)
            elif (
                cur_trace is not None
                and cur_trace.last_retrieval_span_id
                and SpanType.parse(span_type) == SpanType.LLM
            ):
                linked_span_ids = [cur_trace.last_retrieval_span_id]

        return SpanContext(
            self,
            trace_id=resolved_trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            linked_span_ids=linked_span_ids,
            name=name,
            span_type=span_type,
            depth=depth,
            trace_ctx=cur_trace,
            sampled=resolved_sampled,
            **kwargs,
        )

    def generation(self, name: str = "llm.generation", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.LLM
        return self.span(name=name, **kwargs)

    def tool(self, name: str = "tool.call", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.TOOL
        return self.span(name=name, **kwargs)

    def guardrail(self, name: str = "guardrail.check", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.GUARDRAIL
        return self.span(name=name, **kwargs)

    def retrieval(self, name: str = "retrieval.search", **kwargs: Any) -> SpanContext:
        kwargs["span_type"] = SpanType.RETRIEVAL
        return self.span(name=name, **kwargs)

    def observe(
        self,
        fn: Optional[F] = None,
        *,
        name: Optional[str] = None,
        span_type: Optional[SpanType | str] = None,
        as_trace: Optional[bool] = None,
        capture_args: bool = True,
        capture_result: bool = True,
        **span_or_trace_kwargs: Any,
    ) -> Any:
        return _make_observe_decorator(
            client=self,
            fn=fn,
            name=name,
            span_type=span_type,
            as_trace=as_trace,
            capture_args=capture_args,
            capture_result=capture_result,
            extra_kwargs=span_or_trace_kwargs,
        )


PulseAgent = PulseAgentClient
AgentClient = PulseAgentClient
PulseClient = PulseAgentClient


def _capture_call_input(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    try:
        sig = inspect.signature(fn)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        return {"args": dict(bound.arguments)}
    except Exception:
        return {"args": [repr(a) for a in args], "kwargs": dict(kwargs)}


def _make_observe_decorator(
    *,
    client: Optional[PulseAgentClient],
    fn: Optional[Callable[..., Any]],
    name: Optional[str],
    span_type: Optional[SpanType | str],
    as_trace: Optional[bool],
    capture_args: bool,
    capture_result: bool,
    extra_kwargs: dict[str, Any],
) -> Any:
    def decorator(target: Callable[..., Any]) -> Callable[..., Any]:
        op_name = name or target.__name__

        def _resolve_client() -> Optional[PulseAgentClient]:
            return client or _ctx.get_current_client()

        def _open_ctx(active_client: PulseAgentClient, call_input: Any) -> Any:
            cur_trace = _ctx.get_current_trace()
            use_trace = as_trace if as_trace is not None else (cur_trace is None)
            if use_trace:
                tr_kw = dict(extra_kwargs)
                if capture_args and "input" not in tr_kw:
                    tr_kw["input"] = call_input
                return active_client.trace(name=op_name, **tr_kw)
            sp_kw = dict(extra_kwargs)
            if span_type is not None:
                sp_kw["span_type"] = span_type
            if capture_args and "input" not in sp_kw:
                sp_kw["input"] = call_input
            return active_client.span(name=op_name, **sp_kw)

        if inspect.iscoroutinefunction(target):

            @functools.wraps(target)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                cl = _resolve_client()
                if cl is None:
                    return await target(*args, **kwargs)
                call_in = _capture_call_input(target, args, kwargs) if capture_args else None
                async with _open_ctx(cl, call_in) as ctx_obj:
                    res = await target(*args, **kwargs)
                    if capture_result:
                        ctx_obj.set_output(res)
                    return res

            return async_wrapper

        @functools.wraps(target)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            cl = _resolve_client()
            if cl is None:
                return target(*args, **kwargs)
            call_in = _capture_call_input(target, args, kwargs) if capture_args else None
            with _open_ctx(cl, call_in) as ctx_obj:
                res = target(*args, **kwargs)
                if capture_result:
                    ctx_obj.set_output(res)
                return res

        return sync_wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


def observe(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    span_type: Optional[SpanType | str] = None,
    as_trace: Optional[bool] = None,
    capture_args: bool = True,
    capture_result: bool = True,
    **kwargs: Any,
) -> Any:
    return _make_observe_decorator(
        client=client,
        fn=fn,
        name=name,
        span_type=span_type,
        as_trace=as_trace,
        capture_args=capture_args,
        capture_result=capture_result,
        extra_kwargs=kwargs,
    )


def trace(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    return _make_observe_decorator(
        client=client,
        fn=fn,
        name=name,
        span_type=None,
        as_trace=True,
        capture_args=True,
        capture_result=True,
        extra_kwargs=kwargs,
    )


def span(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    span_type: SpanType | str = SpanType.CUSTOM,
    **kwargs: Any,
) -> Any:
    return _make_observe_decorator(
        client=client,
        fn=fn,
        name=name,
        span_type=span_type,
        as_trace=False,
        capture_args=True,
        capture_result=True,
        extra_kwargs=kwargs,
    )


def generation(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    return span(fn, client=client, name=name, span_type=SpanType.LLM, **kwargs)


def tool(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    return span(fn, client=client, name=name, span_type=SpanType.TOOL, **kwargs)


def guardrail(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    return span(fn, client=client, name=name, span_type=SpanType.GUARDRAIL, **kwargs)


def retrieval(
    fn: Optional[F] = None,
    *,
    client: Optional[PulseAgentClient] = None,
    name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    return span(fn, client=client, name=name, span_type=SpanType.RETRIEVAL, **kwargs)
