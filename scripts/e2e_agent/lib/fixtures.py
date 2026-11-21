"""Deterministic payload and fixture generators for Pulse AI Agent Observability E2E tests."""

from __future__ import annotations

import hashlib
import time
from typing import Any

ALL_SPAN_TYPES: tuple[str, ...] = (
    "agent",
    "llm",
    "tool",
    "retrieval",
    "embedding",
    "guardrail",
    "http",
    "db",
    "workflow",
    "custom",
)
ALL_SPAN_PHASES: tuple[str, ...] = ("start", "update", "final")
ALL_DETERMINISTIC_EVAL_KINDS: tuple[str, ...] = (
    "no_errors",
    "no_loop",
    "latency_budget",
    "cost_budget",
    "token_budget",
    "output_not_empty",
    "exact_match",
    "regex_match",
    "contains_keywords",
    "json_schema_keys",
    "tool_sequence",
    "pii_leakage",
)
ALL_EVAL_KINDS: tuple[str, ...] = (*ALL_DETERMINISTIC_EVAL_KINDS, "llm_judge")
ALL_EVAL_TARGET_TYPES: tuple[str, ...] = ("session", "trace", "span")
ALL_ALERT_METRICS: tuple[str, ...] = (
    "failure_rate",
    "p95_latency_ms",
    "cost_usd",
    "eval_score_drop",
    "tool_failure_rate",
    "loop_rate",
    "avg_tokens",
)

# Midnight UTC yesterday: fixed within a day, and inside the server's 30-day agent retention,
# which prunes older traces on node start (a fixed 2024 date was erased on every restart).
DEFAULT_BASE_TS_MS: int = (int(time.time()) // 86_400 - 1) * 86_400_000


def deterministic_hex_id(seed: str, nbytes: int) -> str:
    """Return a deterministic lowercase hex string of `nbytes * 2` characters."""
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    while len(digest) < nbytes:
        digest += hashlib.sha256(digest).digest()
    return digest[:nbytes].hex()


def make_event(
    name: str = "token.chunk",
    *,
    timestamp_ms: int = DEFAULT_BASE_TS_MS + 10,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic SpanEvent payload."""
    return {
        "name": name,
        "timestamp_ms": timestamp_ms,
        "attributes": dict(attributes) if attributes is not None else {"index": 0},
    }


def make_span(
    span_id: str,
    trace_id: str,
    *,
    project_id: str = "proj_e2e",
    parent_span_id: str | None = None,
    linked_span_ids: list[str] | None = None,
    depth: int = 0,
    name: str = "llm.completion",
    span_type: str = "llm",
    span_phase: str = "final",
    status: str = "ok",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
    duration_ms: int = 120,
    provider: str | None = "openai",
    model: str | None = "gpt-4o-mini",
    input_tokens: int = 120,
    output_tokens: int = 45,
    cached_tokens: int = 20,
    cost_nanos: int | None = None,
    cost_usd: float | None = None,
    temperature: float | None = 0.2,
    finish_reason: str | None = "stop",
    tool_name: str | None = None,
    tool_version: str | None = None,
    tool_retry_count: int = 0,
    input_payload: Any = None,
    output_payload: Any = None,
    attributes: dict[str, Any] | None = None,
    events: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    error_stack: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic AgentSpan dict compatible with Pulse JSON & PLST codecs."""
    if span_type not in ALL_SPAN_TYPES:
        raise ValueError(f"Unsupported span_type: {span_type!r}")
    if span_phase not in ALL_SPAN_PHASES:
        raise ValueError(f"Unsupported span_phase: {span_phase!r}")

    if cost_nanos is None and cost_usd is None:
        resolved_nanos = 2_500_000 if span_type == "llm" else 0
        resolved_usd = resolved_nanos / 1_000_000_000
    elif cost_nanos is not None and cost_usd is None:
        resolved_nanos = int(cost_nanos)
        resolved_usd = resolved_nanos / 1_000_000_000
    elif cost_usd is not None and cost_nanos is None:
        resolved_usd = float(cost_usd)
        resolved_nanos = round(resolved_usd * 1_000_000_000)
    else:
        resolved_nanos = int(cost_nanos)  # type: ignore[arg-type]
        resolved_usd = float(cost_usd)  # type: ignore[arg-type]

    if span_type == "tool" and tool_name is None:
        tool_name = "search_docs"
        tool_version = tool_version or "1.0.0"

    if span_type != "llm" and model == "gpt-4o-mini":
        provider = None
        model = None
        input_tokens = 0
        output_tokens = 0
        cached_tokens = 0
        temperature = None
        finish_reason = None

    total_tokens = input_tokens + output_tokens
    return {
        "span_id": span_id,
        "trace_id": trace_id,
        "project_id": project_id,
        "parent_span_id": parent_span_id,
        "linked_span_ids": list(linked_span_ids or []),
        "span_phase": span_phase,
        "depth": depth,
        "name": name,
        "span_type": span_type,
        "status": status,
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": start_ts_ms + duration_ms,
        "duration_ms": duration_ms,
        "provider": provider,
        "model": model,
        "model_id": model,
        "model_version": "2024-07-18" if model else None,
        "prompt_template_id": "tpl_support" if model else None,
        "prompt_template_version": "v1" if model else None,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cached_tokens": cached_tokens,
        "total_tokens": total_tokens,
        "cost_nanos": resolved_nanos,
        "cost_usd": resolved_usd,
        "temperature": temperature,
        "finish_reason": finish_reason,
        "model_params": {"max_tokens": 512} if model else {},
        "tool_name": tool_name,
        "tool_version": tool_version,
        "tool_retry_count": tool_retry_count,
        "input": input_payload if input_payload is not None else {"query": f"input:{name}"},
        "output": output_payload if output_payload is not None else {"result": f"ok:{name}"},
        "attributes": dict(attributes) if attributes is not None else {"span.kind": span_type},
        "events": list(events) if events is not None else [],
        "metadata": dict(metadata) if metadata is not None else {"e2e": True},
        "error_type": error_type,
        "error_message": error_message,
        "error_stack": error_stack,
    }


def make_trace(
    trace_id: str,
    *,
    project_id: str = "proj_e2e",
    agent_name: str = "support-agent",
    agent_version: str = "1.0.0",
    prompt_version: str | None = "p_v1",
    model_config_version: str | None = "mc_v1",
    tool_config_version: str | None = "tc_v1",
    app_release: str | None = "2026.11.1",
    environment: str = "production",
    session_id: str | None = "sess_e2e_1",
    user_id: str | None = "user_e2e_1",
    workflow_id: str | None = "wf_support",
    parent_trace_id: str | None = None,
    status: str = "ok",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
    duration_ms: int = 350,
    input_payload: Any = None,
    output_payload: Any = None,
    error_type: str | None = None,
    error_message: str | None = None,
    failure_source: str | None = None,
    metadata: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    spans: list[dict[str, Any]] | None = None,
    has_loop_warning: bool = False,
) -> dict[str, Any]:
    """Build a deterministic AgentTrace dict with optional rollup calculation from spans."""
    span_list = spans or []
    span_count = len(span_list)
    llm_call_count = sum(1 for s in span_list if s.get("span_type") == "llm")
    tool_call_count = sum(1 for s in span_list if s.get("span_type") == "tool")
    error_span_count = sum(1 for s in span_list if s.get("status") == "error")
    in_tok = sum(int(s.get("input_tokens", 0)) for s in span_list)
    out_tok = sum(int(s.get("output_tokens", 0)) for s in span_list)
    cached_tok = sum(int(s.get("cached_tokens", 0)) for s in span_list)
    tot_tok = sum(int(s.get("total_tokens", 0)) for s in span_list)
    tot_nanos = sum(int(s.get("cost_nanos", 0)) for s in span_list)
    tot_usd = tot_nanos / 1_000_000_000
    primary_llm = next((s for s in span_list if s.get("span_type") == "llm"), None)

    return {
        "trace_id": trace_id,
        "project_id": project_id,
        "agent_name": agent_name,
        "agent_version": agent_version,
        "prompt_version": prompt_version,
        "model_config_version": model_config_version,
        "tool_config_version": tool_config_version,
        "app_release": app_release,
        "environment": environment,
        "session_id": session_id,
        "user_id": user_id,
        "workflow_id": workflow_id,
        "parent_trace_id": parent_trace_id,
        "status": status,
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": start_ts_ms + duration_ms,
        "duration_ms": duration_ms,
        "input": input_payload if input_payload is not None else {"message": "Where is my order?"},
        "output": output_payload if output_payload is not None else {"reply": "Order #1042 is out for delivery."},
        "error_type": error_type,
        "error_message": error_message,
        "failure_source": failure_source,
        "metadata": dict(metadata) if metadata is not None else {"channel": "web"},
        "tags": list(tags) if tags is not None else ["e2e", "support"],
        "span_count": span_count,
        "llm_call_count": llm_call_count,
        "tool_call_count": tool_call_count,
        "error_span_count": error_span_count,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached_tok,
        "total_tokens": tot_tok,
        "total_cost_nanos": tot_nanos,
        "total_cost_usd": tot_usd,
        "primary_model": primary_llm.get("model") if primary_llm else "gpt-4o-mini",
        "primary_provider": primary_llm.get("provider") if primary_llm else "openai",
        "has_loop_warning": has_loop_warning,
    }


def make_batch(
    *,
    seed: str = "default",
    project_id: str = "proj_e2e",
    session_id: str | None = None,
    user_id: str | None = None,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a simple valid single-trace, multi-span TraceIngestBatch."""
    tr_id = f"tr_{seed}"
    sid = session_id or f"sess_{seed}"
    uid = user_id or f"user_{seed}"
    root_sp = make_span(
        f"sp_{seed}_root",
        tr_id,
        project_id=project_id,
        depth=0,
        name="agent.run",
        span_type="agent",
        start_ts_ms=start_ts_ms,
        duration_ms=250,
    )
    llm_sp = make_span(
        f"sp_{seed}_llm",
        tr_id,
        project_id=project_id,
        parent_span_id=root_sp["span_id"],
        depth=1,
        name="llm.respond",
        span_type="llm",
        start_ts_ms=start_ts_ms + 10,
        duration_ms=150,
        events=[make_event("first_token", timestamp_ms=start_ts_ms + 35)],
    )
    tool_sp = make_span(
        f"sp_{seed}_tool",
        tr_id,
        project_id=project_id,
        parent_span_id=root_sp["span_id"],
        linked_span_ids=[llm_sp["span_id"]],
        depth=1,
        name="tool.order_lookup",
        span_type="tool",
        tool_name="order_lookup",
        start_ts_ms=start_ts_ms + 165,
        duration_ms=60,
    )
    spans = [root_sp, llm_sp, tool_sp]
    trace = make_trace(
        tr_id,
        project_id=project_id,
        session_id=sid,
        user_id=uid,
        start_ts_ms=start_ts_ms,
        duration_ms=250,
        spans=spans,
    )
    return {"traces": [trace], "spans": spans}


def make_hierarchy_batch(
    *,
    seed: str = "hier",
    project_id: str = "proj_e2e",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a 5-level hierarchy batch covering parent_trace_id, DAG links, guardrail, all 10 span types, and events."""
    session_id = f"sess_{seed}"
    user_id = f"user_{seed}"
    workflow_id = f"wf_{seed}"
    parent_tr_id = f"tr_{seed}_parent"
    child_tr_id = f"tr_{seed}_child"

    parent_spans: list[dict[str, Any]] = []
    root_id = f"sp_{seed}_0_agent"
    for idx, stype in enumerate(ALL_SPAN_TYPES):
        sp_id = f"sp_{seed}_{idx}_{stype}"
        parent_id = None if idx == 0 else root_id
        linked = [f"sp_{seed}_{idx - 1}_{ALL_SPAN_TYPES[idx - 1]}"] if idx >= 2 else []
        phase = "final"
        if stype == "guardrail":
            phase = "final"
        sp = make_span(
            sp_id,
            parent_tr_id,
            project_id=project_id,
            parent_span_id=parent_id,
            linked_span_ids=linked,
            depth=0 if idx == 0 else 1,
            name=f"step.{stype}",
            span_type=stype,
            span_phase=phase,
            start_ts_ms=start_ts_ms + idx * 20,
            duration_ms=15,
            cost_nanos=1_250_000 if stype == "llm" else (250_000 if stype == "embedding" else 0),
            events=[
                make_event(
                    f"ev.{stype}",
                    timestamp_ms=start_ts_ms + idx * 20 + 5,
                    attributes={"span_type": stype, "seq": idx},
                )
            ],
        )
        parent_spans.append(sp)

    child_root = make_span(
        f"sp_{seed}_child_root",
        child_tr_id,
        project_id=project_id,
        depth=0,
        name="subagent.delegate",
        span_type="workflow",
        span_phase="final",
        linked_span_ids=[parent_spans[0]["span_id"]],
        start_ts_ms=start_ts_ms + 220,
        duration_ms=90,
        events=[make_event("delegated", timestamp_ms=start_ts_ms + 225, attributes={"from": parent_tr_id})],
    )

    parent_trace = make_trace(
        parent_tr_id,
        project_id=project_id,
        session_id=session_id,
        user_id=user_id,
        workflow_id=workflow_id,
        parent_trace_id=None,
        start_ts_ms=start_ts_ms,
        duration_ms=220,
        spans=parent_spans,
    )
    child_trace = make_trace(
        child_tr_id,
        project_id=project_id,
        agent_name="billing-subagent",
        session_id=session_id,
        user_id=user_id,
        workflow_id=workflow_id,
        parent_trace_id=parent_tr_id,
        start_ts_ms=start_ts_ms + 220,
        duration_ms=90,
        spans=[child_root],
    )
    return {"traces": [parent_trace, child_trace], "spans": [*parent_spans, child_root]}


def make_streaming_phase_batch(
    *,
    seed: str = "stream",
    project_id: str = "proj_e2e",
    reverse_order: bool = False,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build start -> update -> final streaming span phase updates (optionally out-of-order)."""
    tr_id = f"tr_{seed}"
    sp_id = f"sp_{seed}_stream"
    phases = [
        make_span(
            sp_id,
            tr_id,
            project_id=project_id,
            name="llm.stream",
            span_type="llm",
            span_phase="start",
            status="running",
            start_ts_ms=start_ts_ms,
            duration_ms=10,
            output_tokens=0,
            cost_nanos=0,
            output_payload={"partial": ""},
        ),
        make_span(
            sp_id,
            tr_id,
            project_id=project_id,
            name="llm.stream",
            span_type="llm",
            span_phase="update",
            status="running",
            start_ts_ms=start_ts_ms,
            duration_ms=60,
            output_tokens=20,
            cost_nanos=400_000,
            output_payload={"partial": "Working on it..."},
        ),
        make_span(
            sp_id,
            tr_id,
            project_id=project_id,
            name="llm.stream",
            span_type="llm",
            span_phase="final",
            status="ok",
            start_ts_ms=start_ts_ms,
            duration_ms=120,
            output_tokens=50,
            cost_nanos=1_000_000,
            output_payload={"text": "Completed streaming response."},
        ),
    ]
    if reverse_order:
        phases = list(reversed(phases))
    trace = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        start_ts_ms=start_ts_ms,
        duration_ms=120,
        spans=[phases[0] if not reverse_order else phases[-1]],
    )
    return {"traces": [trace], "spans": phases}


def make_pii_batch(
    *,
    seed: str = "pii",
    project_id: str = "proj_e2e",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a batch containing email, OpenAI-style key, SSN, and Luhn-valid test card."""
    tr_id = f"tr_{seed}"
    pii_in = {
        "prompt": "Customer alice.smith@example.com SSN 123-45-6789 card 4111111111111111",
        "api_key": "sk-proj-abc1234567890secrettokenxyz",
        "password": "super-secret-password-999",
    }
    pii_out = {
        "reply": "Confirmed for alice.smith@example.com using token sk-proj-abc1234567890secrettokenxyz"
    }
    sp = make_span(
        f"sp_{seed}_1",
        tr_id,
        project_id=project_id,
        name="llm.pii",
        span_type="llm",
        input_payload=pii_in,
        output_payload=pii_out,
        start_ts_ms=start_ts_ms,
    )
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        user_id=f"user_{seed}",
        input_payload=pii_in,
        output_payload=pii_out,
        start_ts_ms=start_ts_ms,
        spans=[sp],
    )
    return {"traces": [tr], "spans": [sp]}


def make_oversized_payload_batch(
    *,
    seed: str = "oversized",
    project_id: str = "proj_e2e",
    payload_bytes: int = 80 * 1024,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a batch with >64 KiB payloads to test truncation & storage limits."""
    tr_id = f"tr_{seed}"
    big_text = "X" * payload_bytes
    sp = make_span(
        f"sp_{seed}_1",
        tr_id,
        project_id=project_id,
        name="llm.big_context",
        span_type="llm",
        input_payload={"prompt": big_text},
        output_payload={"completion": "Y" * payload_bytes},
        start_ts_ms=start_ts_ms,
    )
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        input_payload={"prompt": big_text},
        output_payload={"completion": "Y" * payload_bytes},
        start_ts_ms=start_ts_ms,
        spans=[sp],
    )
    return {"traces": [tr], "spans": [sp]}


def make_cyclic_spans_batch(
    *,
    seed: str = "cycle",
    project_id: str = "proj_e2e",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a trace whose spans contain a parent_span_id cycle (A -> B -> A)."""
    tr_id = f"tr_{seed}"
    sp_a_id = f"sp_{seed}_a"
    sp_b_id = f"sp_{seed}_b"
    sp_a = make_span(
        sp_a_id,
        tr_id,
        project_id=project_id,
        parent_span_id=sp_b_id,
        depth=0,
        name="cycle.a",
        span_type="agent",
        start_ts_ms=start_ts_ms,
    )
    sp_b = make_span(
        sp_b_id,
        tr_id,
        project_id=project_id,
        parent_span_id=sp_a_id,
        depth=1,
        name="cycle.b",
        span_type="tool",
        start_ts_ms=start_ts_ms + 10,
    )
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        start_ts_ms=start_ts_ms,
        spans=[sp_a, sp_b],
    )
    return {"traces": [tr], "spans": [sp_a, sp_b]}


def make_deep_chain_batch(
    *,
    seed: str = "deep",
    project_id: str = "proj_e2e",
    depth_count: int = 40,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a linear span chain deeper than MAX_SPAN_DEPTH (32)."""
    tr_id = f"tr_{seed}"
    spans: list[dict[str, Any]] = []
    prev_id: str | None = None
    for i in range(depth_count):
        sp_id = f"sp_{seed}_{i:03d}"
        spans.append(
            make_span(
                sp_id,
                tr_id,
                project_id=project_id,
                parent_span_id=prev_id,
                depth=i,
                name=f"chain.{i}",
                span_type="agent" if i == 0 else "custom",
                start_ts_ms=start_ts_ms + i,
                duration_ms=5,
            )
        )
        prev_id = sp_id
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        start_ts_ms=start_ts_ms,
        duration_ms=depth_count + 5,
        spans=spans,
    )
    return {"traces": [tr], "spans": spans}


def make_many_spans_batch(
    *,
    seed: str = "manyspans",
    project_id: str = "proj_e2e",
    span_count: int = 550,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a trace exceeding MAX_SPANS_PER_TRACE (512)."""
    tr_id = f"tr_{seed}"
    root_id = f"sp_{seed}_0000"
    spans = [
        make_span(
            root_id,
            tr_id,
            project_id=project_id,
            depth=0,
            name="root",
            span_type="agent",
            start_ts_ms=start_ts_ms,
            duration_ms=span_count + 10,
        )
    ]
    for i in range(1, span_count):
        spans.append(
            make_span(
                f"sp_{seed}_{i:04d}",
                tr_id,
                project_id=project_id,
                parent_span_id=root_id,
                depth=1,
                name=f"leaf.{i}",
                span_type="custom",
                start_ts_ms=start_ts_ms + i,
                duration_ms=1,
            )
        )
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        start_ts_ms=start_ts_ms,
        duration_ms=span_count + 10,
        spans=spans,
    )
    return {"traces": [tr], "spans": spans}


def make_tool_retry_loop_batch(
    *,
    seed: str = "loop",
    project_id: str = "proj_e2e",
    retry_count: int = 6,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a trace exhibiting a repeated tool-retry loop."""
    tr_id = f"tr_{seed}"
    root_id = f"sp_{seed}_root"
    spans = [
        make_span(
            root_id,
            tr_id,
            project_id=project_id,
            depth=0,
            name="agent.loop",
            span_type="agent",
            status="error",
            start_ts_ms=start_ts_ms,
            duration_ms=retry_count * 50,
            error_type="LoopDetected",
            error_message="Tool retry budget exhausted",
        )
    ]
    for i in range(retry_count):
        spans.append(
            make_span(
                f"sp_{seed}_tool_{i}",
                tr_id,
                project_id=project_id,
                parent_span_id=root_id,
                depth=1,
                name="tool.fetch_invoice",
                span_type="tool",
                status="error",
                tool_name="fetch_invoice",
                tool_version="1.2.0",
                tool_retry_count=i,
                start_ts_ms=start_ts_ms + i * 40,
                duration_ms=35,
                error_type="Upstream503",
                error_message="Invoice gateway unavailable",
            )
        )
    tr = make_trace(
        tr_id,
        project_id=project_id,
        session_id=f"sess_{seed}",
        status="error",
        failure_source="tool",
        error_type="LoopDetected",
        error_message="Tool retry budget exhausted",
        start_ts_ms=start_ts_ms,
        duration_ms=retry_count * 50,
        spans=spans,
        has_loop_warning=True,
    )
    return {"traces": [tr], "spans": spans}


def make_repeated_prompt_batch(
    *,
    seed: str = "dedup",
    project_id: str = "proj_e2e",
    repeats: int = 8,
    prompt_bytes: int = 4096,
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> dict[str, Any]:
    """Build a multi-turn batch sharing identical large system prompts for PLST blob dedup testing."""
    shared_prompt = {
        "system": ("You are a strict Pulse Financial Support assistant. " * (prompt_bytes // 54 + 1))[:prompt_bytes],
        "policies": ["never_leak_secrets", "verify_identity", "cite_sources"],
    }
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for i in range(repeats):
        tr_id = f"tr_{seed}_{i:02d}"
        sp = make_span(
            f"sp_{seed}_{i:02d}",
            tr_id,
            project_id=project_id,
            name="llm.turn",
            span_type="llm",
            input_payload=shared_prompt,
            output_payload={"answer": f"Turn {i} resolved"},
            start_ts_ms=start_ts_ms + i * 100,
            duration_ms=80,
        )
        spans.append(sp)
        traces.append(
            make_trace(
                tr_id,
                project_id=project_id,
                session_id=f"sess_{seed}",
                input_payload=shared_prompt,
                output_payload={"answer": f"Turn {i} resolved"},
                start_ts_ms=start_ts_ms + i * 100,
                duration_ms=80,
                spans=[sp],
            )
        )
    return {"traces": traces, "spans": spans}


def make_otlp_export_request(
    *,
    seed: str = "otlp",
    agent_name: str = "otlp-agent",
    agent_version: str = "1.2.0",
    environment: str = "production",
    session_id: str = "sess_otlp_1",
    user_id: str = "user_otlp_1",
    start_ts_ms: int = DEFAULT_BASE_TS_MS,
    include_error_span: bool = False,
) -> dict[str, Any]:
    """Build a valid OTLP ExportTraceServiceRequest (32-hex traceId, 16-hex spanId, ns strings)."""
    trace_id = deterministic_hex_id(f"trace:{seed}", 16)
    root_span_id = deterministic_hex_id(f"span:{seed}:root", 8)
    llm_span_id = deterministic_hex_id(f"span:{seed}:llm", 8)
    guard_span_id = deterministic_hex_id(f"span:{seed}:guard", 8)

    def _kv_str(k: str, v: str) -> dict[str, Any]:
        return {"key": k, "value": {"stringValue": v}}

    def _kv_int(k: str, v: int) -> dict[str, Any]:
        return {"key": k, "value": {"intValue": str(v)}}

    def _kv_dbl(k: str, v: float) -> dict[str, Any]:
        return {"key": k, "value": {"doubleValue": v}}

    spans = [
        {
            "traceId": trace_id,
            "spanId": root_span_id,
            "name": "agent.handle_request",
            "kind": 1,
            "startTimeUnixNano": str(start_ts_ms * 1_000_000),
            "endTimeUnixNano": str((start_ts_ms + 300) * 1_000_000),
            "attributes": [
                _kv_str("pulse.span_type", "agent"),
                _kv_str("session.id", session_id),
                _kv_str("enduser.id", user_id),
                _kv_str("pulse.workflow_id", f"wf_{seed}"),
                _kv_str("input.value", '{"question":"refund status"}'),
                _kv_str("output.value", '{"status":"approved"}'),
            ],
            "events": [
                {
                    "timeUnixNano": str((start_ts_ms + 15) * 1_000_000),
                    "name": "session.attached",
                    "attributes": [_kv_str("session.id", session_id)],
                }
            ],
            "status": {"code": 1},
        },
        {
            "traceId": trace_id,
            "spanId": llm_span_id,
            "parentSpanId": root_span_id,
            "name": "chat gpt-4o-mini",
            "kind": 3,
            "startTimeUnixNano": str((start_ts_ms + 20) * 1_000_000),
            "endTimeUnixNano": str((start_ts_ms + 210) * 1_000_000),
            "attributes": [
                _kv_str("gen_ai.operation.name", "chat"),
                _kv_str("gen_ai.system", "openai"),
                _kv_str("gen_ai.request.model", "gpt-4o-mini"),
                _kv_str("gen_ai.response.model", "gpt-4o-mini"),
                _kv_int("gen_ai.usage.input_tokens", 180),
                _kv_int("gen_ai.usage.output_tokens", 64),
                _kv_dbl("pulse.cost_usd", 0.0018),
                _kv_dbl("gen_ai.request.temperature", 0.1),
                _kv_str("gen_ai.prompt", "Summarize refund #991"),
                _kv_str("gen_ai.completion", "Refund #991 has been processed."),
            ],
            "links": [{"traceId": trace_id, "spanId": root_span_id}],
            "status": {"code": 2 if include_error_span else 1, "message": "rate_limit" if include_error_span else ""},
        },
        {
            "traceId": trace_id,
            "spanId": guard_span_id,
            "parentSpanId": root_span_id,
            "name": "guardrail.pii_check",
            "kind": 1,
            "startTimeUnixNano": str((start_ts_ms + 220) * 1_000_000),
            "endTimeUnixNano": str((start_ts_ms + 260) * 1_000_000),
            "attributes": [
                _kv_str("pulse.span_type", "guardrail"),
                _kv_str("pulse.span_phase", "final"),
            ],
            "status": {"code": 1},
        },
    ]

    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        _kv_str("pulse.agent.name", agent_name),
                        _kv_str("pulse.agent.version", agent_version),
                        _kv_str("deployment.environment.name", environment),
                        _kv_str("pulse.prompt.version", "p_otlp_v1"),
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "pulse-e2e-otlp", "version": "1.0.0"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }


def make_privacy_policy(
    *,
    project_id: str = "proj_e2e",
    capture_inputs: bool = True,
    capture_outputs: bool = True,
    capture_user_id: bool = True,
    redact_pii: bool = True,
    redact_keys: list[str] | None = None,
    exclude_attributes: list[str] | None = None,
    retention_days: int = 30,
    sample_rate: float = 1.0,
) -> dict[str, Any]:
    """Build an AgentPrivacyPolicy payload."""
    return {
        "project_id": project_id,
        "capture_inputs": capture_inputs,
        "capture_outputs": capture_outputs,
        "capture_tool_args": True,
        "capture_tool_results": True,
        "capture_prompts": True,
        "capture_responses": True,
        "capture_user_id": capture_user_id,
        "redact_pii": redact_pii,
        "redact_keys": list(redact_keys)
        if redact_keys is not None
        else ["password", "secret", "token", "api_key", "authorization", "ssn", "credit_card"],
        "exclude_attributes": list(exclude_attributes or []),
        "retention_days": retention_days,
        "sample_rate": sample_rate,
    }


def make_pricing_rule(
    *,
    provider: str = "openai",
    model_pattern: str = "custom-e2e-*",
    input_per_1m_usd: float = 1.50,
    output_per_1m_usd: float = 6.00,
    cached_input_per_1m_usd: float = 0.75,
) -> dict[str, Any]:
    """Build a ModelPricingRule creation/update payload."""
    return {
        "provider": provider,
        "model_pattern": model_pattern,
        "input_per_1m_usd": input_per_1m_usd,
        "output_per_1m_usd": output_per_1m_usd,
        "cached_input_per_1m_usd": cached_input_per_1m_usd,
    }


def make_evaluator_config(
    *,
    name: str = "eval_no_errors",
    kind: str = "no_errors",
    target_type: str = "trace",
    evaluator_type: str | None = None,
    enabled: bool = True,
    agent_name: str | None = None,
    environment: str | None = None,
    sample_rate: float = 1.0,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an EvaluatorConfig creation payload supporting all 12 deterministic kinds + llm_judge."""
    if kind not in ALL_EVAL_KINDS:
        raise ValueError(f"Unsupported evaluator kind: {kind!r}")
    if target_type not in ALL_EVAL_TARGET_TYPES:
        raise ValueError(f"Unsupported eval target_type: {target_type!r}")

    default_configs: dict[str, dict[str, Any]] = {
        "no_errors": {},
        "no_loop": {},
        "latency_budget": {"max_ms": 2000},
        "cost_budget": {"max_usd": 0.25},
        "token_budget": {"max_tokens": 4096},
        "output_not_empty": {},
        "exact_match": {"expected": "Order #1042 is out for delivery."},
        "regex_match": {"pattern": r"Order #\d+"},
        "contains_keywords": {"keywords": ["order", "delivery"], "require_all": True},
        "json_schema_keys": {"required_keys": ["reply"]},
        "tool_sequence": {"expected_tools": ["order_lookup"]},
        "pii_leakage": {},
        "llm_judge": {
            "rubric": "Score helpfulness and accuracy from 0.0 to 1.0 as JSON {score, passed, reasoning}.",
            "model": "gpt-4o-mini",
            "pass_threshold": 0.7,
        },
    }
    resolved_type = evaluator_type or ("llm_judge" if kind == "llm_judge" else "deterministic")
    return {
        "name": name,
        "description": f"E2E evaluator for {kind}",
        "target_type": target_type,
        "evaluator_type": resolved_type,
        "kind": kind,
        "version": "1.0.0",
        "enabled": enabled,
        "agent_name": agent_name,
        "environment": environment,
        "sample_rate": sample_rate,
        "config": dict(config) if config is not None else default_configs[kind],
    }


def make_manual_evaluation(
    *,
    target_type: str = "trace",
    session_id: str = "sess_e2e_1",
    trace_id: str | None = "tr_e2e_1",
    span_id: str | None = None,
    evaluator_id: str = "eval_manual",
    evaluator_name: str = "manual_review",
    score: float = 0.95,
    passed: bool = True,
    reasoning: str = "Verified accurate response",
) -> dict[str, Any]:
    """Build a multi-level AgentEvaluation payload (session | trace | span)."""
    if target_type not in ALL_EVAL_TARGET_TYPES:
        raise ValueError(f"Unsupported target_type: {target_type!r}")
    return {
        "target_type": target_type,
        "session_id": session_id,
        "trace_id": None if target_type == "session" else trace_id,
        "span_id": span_id if target_type == "span" else None,
        "evaluator_id": evaluator_id,
        "evaluator_name": evaluator_name,
        "evaluator_type": "custom",
        "evaluator_version": "1.0.0",
        "score": score,
        "score_type": "numeric",
        "passed": passed,
        "reasoning": reasoning,
        "metadata": {"source": "e2e"},
    }


def make_dataset(
    *,
    name: str = "e2e-golden-dataset",
    description: str = "E2E regression dataset",
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Build an AgentDataset creation payload."""
    return {
        "name": name,
        "description": description,
        "tags": list(tags) if tags is not None else ["e2e", "regression"],
    }


def make_dataset_item(
    *,
    name: str = "item-1",
    input_payload: Any = None,
    expected_output: Any = None,
    expected_behavior: Any = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Build an AgentDatasetItem payload."""
    return {
        "name": name,
        "input": input_payload if input_payload is not None else {"message": "Track order 1042"},
        "expected_output": expected_output if expected_output is not None else {"reply": "Order #1042 is out for delivery."},
        "expected_behavior": expected_behavior if expected_behavior is not None else {"expected_tools": ["order_lookup"]},
        "metadata": {"e2e": True},
        "tags": list(tags) if tags is not None else ["golden"],
    }


def make_experiment(
    *,
    dataset_id: str,
    name: str = "e2e-exp-baseline",
    dataset_version: int = 1,
    agent_name: str = "support-agent",
    agent_version: str = "1.0.0",
    prompt_version: str = "p_v1",
    model: str = "gpt-4o-mini",
    evaluator_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Build an AgentExperiment creation payload."""
    return {
        "name": name,
        "description": "E2E experiment",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "agent_name": agent_name,
        "agent_version": agent_version,
        "prompt_version": prompt_version,
        "model": model,
        "evaluator_ids": list(evaluator_ids or []),
    }


def make_experiment_run(
    *,
    dataset_item_id: str,
    trace_id: str | None = None,
    status: str = "completed",
    actual_output: Any = None,
    latency_ms: int = 120,
    cost_usd: float = 0.002,
    total_tokens: int = 200,
    avg_score: float = 0.9,
    passed: bool = True,
) -> dict[str, Any]:
    """Build an AgentExperimentRun submission payload."""
    return {
        "dataset_item_id": dataset_item_id,
        "trace_id": trace_id,
        "status": status,
        "actual_output": actual_output if actual_output is not None else {"reply": "Order #1042 is out for delivery."},
        "latency_ms": latency_ms,
        "cost_usd": cost_usd,
        "total_tokens": total_tokens,
        "avg_score": avg_score,
        "passed": passed,
    }


def make_alert_rule(
    *,
    name: str = "e2e-high-failure-rate",
    metric: str = "failure_rate",
    operator: str = "gt",
    threshold: float = 0.1,
    window_minutes: int = 15,
    cooldown_minutes: int = 5,
    enabled: bool = True,
    agent_name: str | None = None,
    environment: str | None = None,
    webhook_url: str | None = None,
) -> dict[str, Any]:
    """Build an AgentAlertRule creation payload for any of the 7 supported metrics."""
    if metric not in ALL_ALERT_METRICS:
        raise ValueError(f"Unsupported alert metric: {metric!r}")
    return {
        "name": name,
        "agent_name": agent_name,
        "environment": environment,
        "metric": metric,
        "operator": operator,
        "threshold": threshold,
        "window_minutes": window_minutes,
        "cooldown_minutes": cooldown_minutes,
        "enabled": enabled,
        "webhook_url": webhook_url,
    }
