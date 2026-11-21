"""Data models matching crates/pulse-agent/src/model.rs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math
import time
from typing import Any, Optional
import uuid


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id(prefix: str = "") -> str:
    raw = f"{int(time.time() * 1000):012x}{uuid.uuid4().hex[:16]}"
    return f"{prefix}_{raw}" if prefix else raw


def _sanitize_json_value(val: Any) -> Any:
    if val is None or isinstance(val, (bool, int, str)):
        return val
    if isinstance(val, float):
        return val if math.isfinite(val) else None
    if isinstance(val, dict):
        return {str(k): _sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_sanitize_json_value(item) for item in val]
    if hasattr(val, "to_dict") and callable(val.to_dict):
        return _sanitize_json_value(val.to_dict())
    return str(val)


class TraceStatus(str, Enum):
    OK = "ok"
    RUNNING = "running"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

    def to_u8(self) -> int:
        return _TRACE_STATUS_TO_U8[self]

    @classmethod
    def from_u8(cls, tag: int) -> "TraceStatus":
        if tag not in _U8_TO_TRACE_STATUS:
            raise ValueError(f"invalid trace_status enum tag: {tag}")
        return _U8_TO_TRACE_STATUS[tag]

    @classmethod
    def parse(cls, value: UnionStatus) -> "TraceStatus":
        if isinstance(value, cls):
            return value
        s = str(value).strip().lower()
        if s in ("running", "in_progress", "pending"):
            return cls.RUNNING
        if s in ("error", "failed", "failure"):
            return cls.ERROR
        if s in ("timeout", "timed_out"):
            return cls.TIMEOUT
        if s in ("cancelled", "canceled"):
            return cls.CANCELLED
        return cls.OK


UnionStatus = TraceStatus | str

_TRACE_STATUS_TO_U8 = {
    TraceStatus.OK: 0,
    TraceStatus.RUNNING: 1,
    TraceStatus.ERROR: 2,
    TraceStatus.TIMEOUT: 3,
    TraceStatus.CANCELLED: 4,
}
_U8_TO_TRACE_STATUS = {v: k for k, v in _TRACE_STATUS_TO_U8.items()}


class SpanPhase(str, Enum):
    START = "start"
    UPDATE = "update"
    FINAL = "final"

    def to_u8(self) -> int:
        return _SPAN_PHASE_TO_U8[self]

    @classmethod
    def from_u8(cls, tag: int) -> "SpanPhase":
        if tag not in _U8_TO_SPAN_PHASE:
            raise ValueError(f"invalid span_phase enum tag: {tag}")
        return _U8_TO_SPAN_PHASE[tag]

    @classmethod
    def parse(cls, value: "SpanPhase | str") -> "SpanPhase":
        if isinstance(value, cls):
            return value
        s = str(value).strip().lower()
        if s == "start":
            return cls.START
        if s == "update":
            return cls.UPDATE
        return cls.FINAL


_SPAN_PHASE_TO_U8 = {
    SpanPhase.START: 0,
    SpanPhase.UPDATE: 1,
    SpanPhase.FINAL: 2,
}
_U8_TO_SPAN_PHASE = {v: k for k, v in _SPAN_PHASE_TO_U8.items()}


class SpanType(str, Enum):
    AGENT = "agent"
    LLM = "llm"
    TOOL = "tool"
    RETRIEVAL = "retrieval"
    EMBEDDING = "embedding"
    GUARDRAIL = "guardrail"
    HTTP = "http"
    DB = "db"
    WORKFLOW = "workflow"
    CUSTOM = "custom"

    def to_u8(self) -> int:
        return _SPAN_TYPE_TO_U8[self]

    @classmethod
    def from_u8(cls, tag: int) -> "SpanType":
        if tag not in _U8_TO_SPAN_TYPE:
            raise ValueError(f"invalid span_type enum tag: {tag}")
        return _U8_TO_SPAN_TYPE[tag]

    @classmethod
    def parse(cls, value: "SpanType | str") -> "SpanType":
        if isinstance(value, cls):
            return value
        s = str(value).strip().lower()
        if s in ("agent", "chain"):
            return cls.AGENT
        if s in ("llm", "generation", "chat", "completion"):
            return cls.LLM
        if s in ("tool", "function", "tool_call"):
            return cls.TOOL
        if s in ("retrieval", "retriever", "rag"):
            return cls.RETRIEVAL
        if s in ("embedding", "embeddings"):
            return cls.EMBEDDING
        if s in ("guardrail", "safety", "moderation"):
            return cls.GUARDRAIL
        if s in ("http", "network"):
            return cls.HTTP
        if s in ("db", "database", "sql"):
            return cls.DB
        if s in ("workflow", "step", "graph"):
            return cls.WORKFLOW
        return cls.CUSTOM


_SPAN_TYPE_TO_U8 = {
    SpanType.AGENT: 0,
    SpanType.LLM: 1,
    SpanType.TOOL: 2,
    SpanType.RETRIEVAL: 3,
    SpanType.EMBEDDING: 4,
    SpanType.GUARDRAIL: 5,
    SpanType.HTTP: 6,
    SpanType.DB: 7,
    SpanType.WORKFLOW: 8,
    SpanType.CUSTOM: 9,
}
_U8_TO_SPAN_TYPE = {v: k for k, v in _SPAN_TYPE_TO_U8.items()}


class FailureSource(str, Enum):
    MODEL = "model"
    TOOL = "tool"
    APPLICATION = "application"
    PROMPT = "prompt"
    EXTERNAL = "external"
    UNKNOWN = "unknown"

    def to_u8(self) -> int:
        return _FAILURE_SOURCE_TO_U8[self]

    @classmethod
    def from_u8(cls, tag: int) -> "FailureSource":
        if tag not in _U8_TO_FAILURE_SOURCE:
            raise ValueError(f"invalid failure_source enum tag: {tag}")
        return _U8_TO_FAILURE_SOURCE[tag]

    @classmethod
    def parse(cls, value: "FailureSource | str") -> "FailureSource":
        if isinstance(value, cls):
            return value
        s = str(value).strip().lower()
        if s in ("model", "llm"):
            return cls.MODEL
        if s == "tool":
            return cls.TOOL
        if s in ("application", "app", "code"):
            return cls.APPLICATION
        if s == "prompt":
            return cls.PROMPT
        if s in ("external", "http", "network", "db"):
            return cls.EXTERNAL
        return cls.UNKNOWN


_FAILURE_SOURCE_TO_U8 = {
    FailureSource.MODEL: 1,
    FailureSource.TOOL: 2,
    FailureSource.APPLICATION: 3,
    FailureSource.PROMPT: 4,
    FailureSource.EXTERNAL: 5,
    FailureSource.UNKNOWN: 6,
}
_U8_TO_FAILURE_SOURCE = {v: k for k, v in _FAILURE_SOURCE_TO_U8.items()}


@dataclass
class SpanEvent:
    name: str
    timestamp_ms: int
    attributes: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "timestamp_ms": int(self.timestamp_ms),
            "attributes": _sanitize_json_value(self.attributes),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SpanEvent":
        return cls(
            name=str(data["name"]),
            timestamp_ms=int(data["timestamp_ms"]),
            attributes=data.get("attributes", None),
        )


@dataclass
class AgentSpan:
    span_id: str
    trace_id: str
    name: str
    start_ts_ms: int
    project_id: str = ""
    parent_span_id: Optional[str] = None
    linked_span_ids: list[str] = field(default_factory=list)
    span_phase: SpanPhase = SpanPhase.FINAL
    depth: int = 0
    span_type: SpanType = SpanType.CUSTOM
    status: TraceStatus = TraceStatus.OK
    end_ts_ms: Optional[int] = None
    duration_ms: int = 0

    # Generation fields
    provider: Optional[str] = None
    model: Optional[str] = None
    model_id: Optional[str] = None
    model_version: Optional[str] = None
    prompt_template_id: Optional[str] = None
    prompt_template_version: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    cost_nanos: int = 0
    cost_usd: float = 0.0
    temperature: Optional[float] = None
    finish_reason: Optional[str] = None
    model_params: Any = None

    # Tool fields
    tool_name: Optional[str] = None
    tool_version: Optional[str] = None
    tool_retry_count: int = 0

    # Payloads & diagnostics
    input: Any = None
    output: Any = None
    attributes: Any = None
    events: list[SpanEvent] = field(default_factory=list)
    metadata: Any = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_stack: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        temp = self.temperature
        if temp is not None and not math.isfinite(temp):
            temp = None
        cost_usd = float(self.cost_usd) if math.isfinite(self.cost_usd) else 0.0
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "project_id": self.project_id,
            "parent_span_id": self.parent_span_id,
            "linked_span_ids": list(self.linked_span_ids),
            "span_phase": SpanPhase.parse(self.span_phase).value,
            "depth": int(self.depth),
            "name": self.name,
            "span_type": SpanType.parse(self.span_type).value,
            "status": TraceStatus.parse(self.status).value,
            "start_ts_ms": int(self.start_ts_ms),
            "end_ts_ms": int(self.end_ts_ms) if self.end_ts_ms is not None else None,
            "duration_ms": int(self.duration_ms),
            "provider": self.provider,
            "model": self.model,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "prompt_template_id": self.prompt_template_id,
            "prompt_template_version": self.prompt_template_version,
            "input_tokens": max(0, int(self.input_tokens)),
            "output_tokens": max(0, int(self.output_tokens)),
            "cached_tokens": max(0, int(self.cached_tokens)),
            "total_tokens": max(0, int(self.total_tokens)),
            "cost_nanos": int(self.cost_nanos),
            "cost_usd": cost_usd,
            "temperature": temp,
            "finish_reason": self.finish_reason,
            "model_params": _sanitize_json_value(self.model_params),
            "tool_name": self.tool_name,
            "tool_version": self.tool_version,
            "tool_retry_count": int(self.tool_retry_count),
            "input": _sanitize_json_value(self.input),
            "output": _sanitize_json_value(self.output),
            "attributes": _sanitize_json_value(self.attributes),
            "events": [ev.to_dict() for ev in self.events],
            "metadata": _sanitize_json_value(self.metadata),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "error_stack": self.error_stack,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentSpan":
        events = [
            ev if isinstance(ev, SpanEvent) else SpanEvent.from_dict(ev)
            for ev in data.get("events", [])
        ]
        return cls(
            span_id=str(data["span_id"]),
            trace_id=str(data["trace_id"]),
            project_id=str(data.get("project_id", "")),
            parent_span_id=data.get("parent_span_id"),
            linked_span_ids=list(data.get("linked_span_ids", [])),
            span_phase=SpanPhase.parse(data.get("span_phase", SpanPhase.FINAL)),
            depth=int(data.get("depth", 0)),
            name=str(data["name"]),
            span_type=SpanType.parse(data.get("span_type", SpanType.CUSTOM)),
            status=TraceStatus.parse(data.get("status", TraceStatus.OK)),
            start_ts_ms=int(data["start_ts_ms"]),
            end_ts_ms=int(data["end_ts_ms"]) if data.get("end_ts_ms") is not None else None,
            duration_ms=int(data.get("duration_ms", 0)),
            provider=data.get("provider"),
            model=data.get("model"),
            model_id=data.get("model_id"),
            model_version=data.get("model_version"),
            prompt_template_id=data.get("prompt_template_id"),
            prompt_template_version=data.get("prompt_template_version"),
            input_tokens=int(data.get("input_tokens", 0)),
            output_tokens=int(data.get("output_tokens", 0)),
            cached_tokens=int(data.get("cached_tokens", 0)),
            total_tokens=int(data.get("total_tokens", 0)),
            cost_nanos=int(data.get("cost_nanos", 0)),
            cost_usd=float(data.get("cost_usd", 0.0)),
            temperature=float(data["temperature"]) if data.get("temperature") is not None else None,
            finish_reason=data.get("finish_reason"),
            model_params=data.get("model_params", None),
            tool_name=data.get("tool_name"),
            tool_version=data.get("tool_version"),
            tool_retry_count=int(data.get("tool_retry_count", 0)),
            input=data.get("input", None),
            output=data.get("output", None),
            attributes=data.get("attributes", None),
            events=events,
            metadata=data.get("metadata", None),
            error_type=data.get("error_type"),
            error_message=data.get("error_message"),
            error_stack=data.get("error_stack"),
        )


@dataclass
class AgentTrace:
    trace_id: str
    agent_name: str
    start_ts_ms: int
    project_id: str = ""
    agent_version: str = "unversioned"
    prompt_version: Optional[str] = None
    model_config_version: Optional[str] = None
    tool_config_version: Optional[str] = None
    app_release: Optional[str] = None
    environment: str = "production"
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    workflow_id: Optional[str] = None
    parent_trace_id: Optional[str] = None
    status: TraceStatus = TraceStatus.OK
    end_ts_ms: Optional[int] = None
    duration_ms: int = 0
    input: Any = None
    output: Any = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    failure_source: Optional[FailureSource] = None
    metadata: Any = None
    tags: list[str] = field(default_factory=list)

    # Rolled-up summary metrics
    span_count: int = 0
    llm_call_count: int = 0
    tool_call_count: int = 0
    error_span_count: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    total_cost_nanos: int = 0
    total_cost_usd: float = 0.0
    primary_model: Optional[str] = None
    primary_provider: Optional[str] = None
    avg_eval_score: Optional[float] = None
    eval_pass_rate: Optional[float] = None
    has_loop_warning: bool = False

    def to_dict(self) -> dict[str, Any]:
        avg_eval = self.avg_eval_score
        if avg_eval is not None and not math.isfinite(avg_eval):
            avg_eval = None
        pass_rate = self.eval_pass_rate
        if pass_rate is not None and not math.isfinite(pass_rate):
            pass_rate = None
        cost_usd = float(self.total_cost_usd) if math.isfinite(self.total_cost_usd) else 0.0
        return {
            "trace_id": self.trace_id,
            "project_id": self.project_id,
            "agent_name": self.agent_name,
            "agent_version": self.agent_version,
            "prompt_version": self.prompt_version,
            "model_config_version": self.model_config_version,
            "tool_config_version": self.tool_config_version,
            "app_release": self.app_release,
            "environment": self.environment,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "workflow_id": self.workflow_id,
            "parent_trace_id": self.parent_trace_id,
            "status": TraceStatus.parse(self.status).value,
            "start_ts_ms": int(self.start_ts_ms),
            "end_ts_ms": int(self.end_ts_ms) if self.end_ts_ms is not None else None,
            "duration_ms": int(self.duration_ms),
            "input": _sanitize_json_value(self.input),
            "output": _sanitize_json_value(self.output),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "failure_source": FailureSource.parse(self.failure_source).value if self.failure_source is not None else None,
            "metadata": _sanitize_json_value(self.metadata),
            "tags": list(self.tags),
            "span_count": int(self.span_count),
            "llm_call_count": int(self.llm_call_count),
            "tool_call_count": int(self.tool_call_count),
            "error_span_count": int(self.error_span_count),
            "input_tokens": max(0, int(self.input_tokens)),
            "output_tokens": max(0, int(self.output_tokens)),
            "cached_tokens": max(0, int(self.cached_tokens)),
            "total_tokens": max(0, int(self.total_tokens)),
            "total_cost_nanos": int(self.total_cost_nanos),
            "total_cost_usd": cost_usd,
            "primary_model": self.primary_model,
            "primary_provider": self.primary_provider,
            "avg_eval_score": avg_eval,
            "eval_pass_rate": pass_rate,
            "has_loop_warning": bool(self.has_loop_warning),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentTrace":
        fs = data.get("failure_source")
        return cls(
            trace_id=str(data["trace_id"]),
            project_id=str(data.get("project_id", "")),
            agent_name=str(data["agent_name"]),
            agent_version=str(data.get("agent_version", "unversioned")),
            prompt_version=data.get("prompt_version"),
            model_config_version=data.get("model_config_version"),
            tool_config_version=data.get("tool_config_version"),
            app_release=data.get("app_release"),
            environment=str(data.get("environment", "production")),
            session_id=data.get("session_id"),
            user_id=data.get("user_id"),
            workflow_id=data.get("workflow_id"),
            parent_trace_id=data.get("parent_trace_id"),
            status=TraceStatus.parse(data.get("status", TraceStatus.OK)),
            start_ts_ms=int(data["start_ts_ms"]),
            end_ts_ms=int(data["end_ts_ms"]) if data.get("end_ts_ms") is not None else None,
            duration_ms=int(data.get("duration_ms", 0)),
            input=data.get("input", None),
            output=data.get("output", None),
            error_type=data.get("error_type"),
            error_message=data.get("error_message"),
            failure_source=FailureSource.parse(fs) if fs is not None else None,
            metadata=data.get("metadata", None),
            tags=list(data.get("tags", [])),
            span_count=int(data.get("span_count", 0)),
            llm_call_count=int(data.get("llm_call_count", 0)),
            tool_call_count=int(data.get("tool_call_count", 0)),
            error_span_count=int(data.get("error_span_count", 0)),
            input_tokens=int(data.get("input_tokens", 0)),
            output_tokens=int(data.get("output_tokens", 0)),
            cached_tokens=int(data.get("cached_tokens", 0)),
            total_tokens=int(data.get("total_tokens", 0)),
            total_cost_nanos=int(data.get("total_cost_nanos", 0)),
            total_cost_usd=float(data.get("total_cost_usd", 0.0)),
            primary_model=data.get("primary_model"),
            primary_provider=data.get("primary_provider"),
            avg_eval_score=float(data["avg_eval_score"]) if data.get("avg_eval_score") is not None else None,
            eval_pass_rate=float(data["eval_pass_rate"]) if data.get("eval_pass_rate") is not None else None,
            has_loop_warning=bool(data.get("has_loop_warning", False)),
        )


@dataclass
class TraceIngestBatch:
    traces: list[AgentTrace] = field(default_factory=list)
    spans: list[AgentSpan] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "traces": [t.to_dict() for t in self.traces],
            "spans": [s.to_dict() for s in self.spans],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TraceIngestBatch":
        traces = [
            t if isinstance(t, AgentTrace) else AgentTrace.from_dict(t)
            for t in data.get("traces", [])
        ]
        spans = [
            s if isinstance(s, AgentSpan) else AgentSpan.from_dict(s)
            for s in data.get("spans", [])
        ]
        return cls(traces=traces, spans=spans)


@dataclass
class TraceIngestResponse:
    accepted_traces: int = 0
    accepted_spans: int = 0
    truncated_payloads: int = 0
    redacted_fields: int = 0
    dropped_spans: int = 0
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TraceIngestResponse":
        return cls(
            accepted_traces=int(data.get("accepted_traces", 0)),
            accepted_spans=int(data.get("accepted_spans", 0)),
            truncated_payloads=int(data.get("truncated_payloads", 0)),
            redacted_fields=int(data.get("redacted_fields", 0)),
            dropped_spans=int(data.get("dropped_spans", 0)),
            warnings=list(data.get("warnings", [])),
        )


@dataclass(frozen=True)
class ModelPricingRule:
    model_pattern: str
    input_per_1m_usd: float
    output_per_1m_usd: float
    cached_input_per_1m_usd: float = 0.0
    provider: str = ""
    id: str = ""
    project_id: str = ""
    updated_at_ms: int = 0
