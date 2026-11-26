"""Milestone gating decorators, deterministic fixtures, and polling helpers."""

from __future__ import annotations

import functools
import json
import os
import time
import unittest
import uuid
from collections.abc import Callable
from compression import zstd
from typing import Any, TypeVar

T = TypeVar("T")

MILESTONES: tuple[str, ...] = ("T0a", "T0b", "T1", "T2", "T3", "W1", "W2", "T4", "W3")
VALID_FEATURES: tuple[str, ...] = tuple(f"F{i:02d}" for i in range(1, 32))
VALID_TIERS: tuple[str | int, ...] = ("self", 1, 2, 3, 4, 5)

_UNSET = object()
_ACTIVE_MODE: str | None = None
_ACTIVE_MAX_MILESTONE: Any = _UNSET


def set_active_gating(*, mode: str | None = None, max_milestone: Any = _UNSET) -> None:
    """Configure in-process milestone gating (mirrors E2E_TIERED_MODE / E2E_TIERED_MAX_MILESTONE)."""
    global _ACTIVE_MODE, _ACTIVE_MAX_MILESTONE
    if mode is not None and mode not in ("full", "baseline"):
        raise ValueError(f"Invalid mode {mode!r}; expected 'full' or 'baseline'")
    if max_milestone is not _UNSET and max_milestone is not None and max_milestone != "" and max_milestone not in MILESTONES:
        raise ValueError(f"Invalid max_milestone {max_milestone!r}; expected one of {MILESTONES}")
    _ACTIVE_MODE = mode
    if max_milestone is _UNSET:
        _ACTIVE_MAX_MILESTONE = _UNSET
    elif max_milestone == "":
        _ACTIVE_MAX_MILESTONE = None
    else:
        _ACTIVE_MAX_MILESTONE = max_milestone


def get_active_gating() -> tuple[str, str | None]:
    """Return `(mode, max_milestone)` resolved from active config or environment."""
    mode = _ACTIVE_MODE or os.environ.get("E2E_TIERED_MODE", "full").strip().lower() or "full"
    if mode not in ("full", "baseline"):
        mode = "full"
    if _ACTIVE_MAX_MILESTONE is _UNSET:
        env_m = os.environ.get("E2E_TIERED_MAX_MILESTONE", "").strip()
        max_m: str | None = env_m if env_m else None
    else:
        max_m = _ACTIVE_MAX_MILESTONE
    if mode == "baseline" and max_m is None:
        max_m = "T0a"
    return mode, max_m


def milestone_index(milestone: str) -> int:
    """Return 0-based ordering index for milestone `T0a..W3`."""
    try:
        return MILESTONES.index(milestone)
    except ValueError as exc:
        raise ValueError(f"Unknown milestone {milestone!r}; expected one of {MILESTONES}") from exc


def is_milestone_enabled(
    milestone: str,
    *,
    mode: str | None = None,
    max_milestone: str | None = None,
) -> bool:
    """Return True if `milestone` should run under the active or given gating settings."""
    idx = milestone_index(milestone)
    eff_mode, eff_max = get_active_gating()
    if mode is not None:
        eff_mode = mode
    if max_milestone is not None:
        eff_max = max_milestone or None
    if eff_mode == "baseline" and eff_max is None:
        eff_max = "T0a"
    if eff_max is None:
        return True
    return idx <= milestone_index(eff_max)


def _check_milestone_or_skip(milestone: str) -> None:
    eff_mode, eff_max = get_active_gating()
    if not is_milestone_enabled(milestone, mode=eff_mode, max_milestone=eff_max):
        raise unittest.SkipTest(
            f"Milestone {milestone} > max_milestone {eff_max} (mode={eff_mode})"
        )


def tier(value: int | str) -> Callable[[Any], Any]:
    """Tag a TestCase class or test method with tier `1|2|3|4|5|'self'`."""
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    if isinstance(value, bool) or value not in VALID_TIERS:
        raise ValueError(f"Invalid tier {value!r}; expected one of {VALID_TIERS}")

    def decorator(obj: Any) -> Any:
        setattr(obj, "_e2e_tier", value)
        return obj

    return decorator


def feature(*features: str) -> Callable[[Any], Any]:
    """Tag a TestCase class or test method with one or more features `F01..F31`."""
    if not features:
        raise ValueError("@feature requires at least one feature code (e.g. 'F01')")
    normalized: list[str] = []
    for feat in features:
        f = str(feat).strip().upper()
        if f not in VALID_FEATURES:
            raise ValueError(f"Invalid feature {feat!r}; expected F01..F31")
        if f not in normalized:
            normalized.append(f)

    def decorator(obj: Any) -> Any:
        existing = tuple(getattr(obj, "_e2e_features", ()))
        merged = tuple(dict.fromkeys([*existing, *normalized]))
        setattr(obj, "_e2e_features", merged)
        setattr(obj, "_e2e_feature", merged[0])
        return obj

    return decorator


def require_milestone(milestone: str) -> Callable[[Any], Any]:
    """Tag a TestCase class or method with a minimum milestone `T0a..W3`."""
    milestone_index(milestone)

    def decorator(obj: Any) -> Any:
        if isinstance(obj, type):
            setattr(obj, "_e2e_milestone", milestone)
            orig_setup = getattr(obj, "setUp", None)

            @functools.wraps(orig_setup)
            def wrapped_setup(self: Any, *args: Any, **kwargs: Any) -> Any:
                method = getattr(self, self._testMethodName, None)
                method_ms = getattr(method, "_e2e_milestone", None)
                effective_ms = method_ms or milestone
                _check_milestone_or_skip(effective_ms)
                if orig_setup is not None:
                    return orig_setup(self, *args, **kwargs)
                return None

            setattr(obj, "setUp", wrapped_setup)
            return obj

        setattr(obj, "_e2e_milestone", milestone)

        @functools.wraps(obj)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            _check_milestone_or_skip(milestone)
            return obj(*args, **kwargs)

        setattr(wrapper, "_e2e_milestone", milestone)
        return wrapper

    return decorator


def get_test_metadata(test_case: unittest.TestCase) -> dict[str, Any]:
    """Extract `(tier, features, milestone)` from a `unittest.TestCase` instance."""
    cls = type(test_case)
    method = getattr(test_case, test_case._testMethodName, None)

    test_tier = getattr(method, "_e2e_tier", None)
    if test_tier is None:
        test_tier = getattr(cls, "_e2e_tier", None)

    method_feats = tuple(getattr(method, "_e2e_features", ()))
    cls_feats = tuple(getattr(cls, "_e2e_features", ()))
    feats = tuple(dict.fromkeys([*cls_feats, *method_feats]))

    test_ms = getattr(method, "_e2e_milestone", None)
    if test_ms is None:
        test_ms = getattr(cls, "_e2e_milestone", None)

    return {
        "tier": test_tier,
        "features": feats,
        "milestone": test_ms,
    }


def external_postgres_url() -> str | None:
    """Return configured external Postgres URL from E2E_POSTGRES_URL or DATABASE_URL, if any."""
    for var in ("E2E_POSTGRES_URL", "DATABASE_URL"):
        val = os.environ.get(var, "").strip()
        if val.startswith(("postgres://", "postgresql://")):
            return val
    return None


def skip_if_no_postgres() -> str:
    """Return Postgres URL or raise external-tool SkipTest (allowed in `--mode full`)."""
    url = external_postgres_url()
    if not url:
        raise unittest.SkipTest(
            "[external-tool] E2E_POSTGRES_URL / DATABASE_URL not configured for Postgres test"
        )
    return url


def wait_until(
    pred: Callable[[], T],
    timeout: float = 15.0,
    interval: float = 0.2,
    desc: str = "",
    *,
    ignore_exceptions: tuple[type[BaseException], ...] = (),
) -> T:
    """Poll `pred()` until it returns a truthy value or `timeout` seconds elapse."""
    deadline = time.monotonic() + timeout
    last_val: Any = None
    last_exc: BaseException | None = None
    while True:
        try:
            last_val = pred()
            last_exc = None
            if last_val:
                return last_val
        except ignore_exceptions as exc:
            last_exc = exc
        if time.monotonic() >= deadline:
            label = f" ({desc})" if desc else ""
            if last_exc is not None:
                raise TimeoutError(
                    f"Timed out after {timeout:.1f}s{label}; last exception: {type(last_exc).__name__}: {last_exc}"
                ) from last_exc
            raise TimeoutError(
                f"Timed out after {timeout:.1f}s{label}; last value: {last_val!r}"
            )
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Deterministic Fixture Generators
# ---------------------------------------------------------------------------


def make_event(
    *,
    event_name: str = "page_view",
    distinct_id: str = "user_001",
    session_id: str | None = None,
    ts_ms: int | None = None,
    sent_ts_ms: int | None = None,
    props: dict[str, Any] | None = None,
    event_uuid: str | None = None,
    routing_seed: str | None = None,
    replay_offset_kb: int = 0,
) -> dict[str, Any]:
    """Construct a single Pulse `WireEvent` JSON dict."""
    now_ms = int(time.time() * 1000) if ts_ms is None else int(ts_ms)
    ev: dict[str, Any] = {
        "event": event_name,
        "distinct_id": distinct_id,
        "session_id": session_id or f"sess_{distinct_id}",
        "timestamp": now_ms,
        "properties": dict(props or {"source": "e2e", "amount": 42}),
    }
    if sent_ts_ms is not None:
        ev["client_sent_ts_ms"] = int(sent_ts_ms)
    if event_uuid is not None:
        ev["event_uuid"] = event_uuid
    if routing_seed is not None:
        ev["routing_seed"] = routing_seed
    if replay_offset_kb:
        ev["replay_offset_kb"] = int(replay_offset_kb)
    return ev


def make_events(
    count: int,
    *,
    event_name: str = "page_view",
    distinct_id_prefix: str = "user",
    base_ts_ms: int | None = None,
    step_ms: int = 100,
    props: dict[str, Any] | None = None,
    unique_users: int | None = None,
    seed_prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Generate `count` deterministic Pulse `WireEvent` JSON dicts."""
    start_ms = int(time.time() * 1000) if base_ts_ms is None else int(base_ts_ms)
    events: list[dict[str, Any]] = []
    for i in range(count):
        uid_idx = (i % unique_users) if unique_users else i
        did = f"{distinct_id_prefix}_{uid_idx:04d}"
        ev_props = dict(props or {})
        ev_props.setdefault("seq", i)
        ev_uuid = (
            str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed_prefix}:{i}"))
            if seed_prefix is not None
            else None
        )
        events.append(
            make_event(
                event_name=event_name,
                distinct_id=did,
                session_id=f"sess_{did}",
                ts_ms=start_ms + i * step_ms,
                props=ev_props,
                event_uuid=ev_uuid,
            )
        )
    return events


def make_agent_trace(
    *,
    trace_id: str | None = None,
    agent_name: str = "e2e-agent",
    agent_version: str = "v1",
    environment: str = "production",
    status: str = "ok",
    start_ts_ms: int | None = None,
    duration_ms: int = 120,
    user_id: str | None = "user_001",
    session_id: str | None = "asess_001",
    span_count: int = 2,
    model: str = "gpt-4o",
    provider: str = "openai",
    cost_usd: float = 0.01,
    input_data: Any = None,
    output_data: Any = None,
) -> dict[str, Any]:
    """Construct a `TraceIngestBatch` JSON dict (`{"traces": [...], "spans": [...]}`)."""
    now_ms = int(time.time() * 1000) if start_ts_ms is None else int(start_ts_ms)
    tid = trace_id or f"tr_{uuid.uuid4().hex[:16]}"
    trace: dict[str, Any] = {
        "trace_id": tid,
        "agent_name": agent_name,
        "agent_version": agent_version,
        "environment": environment,
        "status": status,
        "start_ts_ms": now_ms,
        "end_ts_ms": now_ms + duration_ms,
        "duration_ms": duration_ms,
        "primary_model": model,
        "primary_provider": provider,
        "total_cost_usd": cost_usd,
        "input": input_data if input_data is not None else {"prompt": f"hello {tid}"},
        "output": output_data if output_data is not None else {"answer": f"world {tid}"},
        "metadata": {"e2e": True},
        "tags": ["e2e"],
    }
    if user_id is not None:
        trace["user_id"] = user_id
    if session_id is not None:
        trace["session_id"] = session_id

    spans: list[dict[str, Any]] = []
    root_span_id = f"sp_{tid}_0"
    for i in range(max(0, span_count)):
        sp_id = f"sp_{tid}_{i}"
        span: dict[str, Any] = {
            "span_id": sp_id,
            "trace_id": tid,
            "name": "agent_root" if i == 0 else f"llm_call_{i}",
            "span_type": "agent" if i == 0 else "llm",
            "span_phase": "final",
            "status": status,
            "start_ts_ms": now_ms + i * 10,
            "end_ts_ms": now_ms + i * 10 + max(1, duration_ms // max(1, span_count)),
            "duration_ms": max(1, duration_ms // max(1, span_count)),
            "depth": 0 if i == 0 else 1,
            "provider": provider,
            "model": model,
            "input_tokens": 100,
            "output_tokens": 50,
            "total_tokens": 150,
            "cost_usd": cost_usd / max(1, span_count),
        }
        if i > 0:
            span["parent_span_id"] = root_span_id
        spans.append(span)

    return {"traces": [trace], "spans": spans}


def make_replay_chunk(
    *,
    session_id: str = "rsess_001",
    project_id: str = "",
    chunk_start_ts_ms: int | None = None,
    ops: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Construct a JSON `ReplayChunk` payload compatible with `POST /v1/replay/chunk`."""
    ts = int(time.time() * 1000) if chunk_start_ts_ms is None else int(chunk_start_ts_ms)
    default_ops: list[dict[str, Any]] = [
        {
            "op": "full_snapshot",
            "ts_offset_ms": 0,
            "width": 390,
            "height": 844,
            "nodes": [
                {
                    "node_id": 1,
                    "parent_id": 0,
                    "kind": "screen",
                    "x": 0,
                    "y": 0,
                    "w": 390,
                    "h": 844,
                    "bg_rgba": 0xFFFFFFFF,
                    "masked_label": "Home",
                },
                {
                    "node_id": 2,
                    "parent_id": 1,
                    "kind": "button",
                    "x": 20,
                    "y": 100,
                    "w": 200,
                    "h": 48,
                    "bg_rgba": 0x0066FFFF,
                    "masked_label": "Continue",
                },
            ],
        },
        {
            "op": "pointer_interaction",
            "ts_offset_ms": 250,
            "x": 100,
            "y": 120,
            "type": "tap",
        },
    ]
    return {
        "project_id": project_id,
        "session_id": session_id,
        "chunk_start_ts_ms": ts,
        "ops": ops if ops is not None else default_ops,
    }


def random_byok_hex() -> str:
    """Return a random 64-character hex string suitable for `PULSE_BYOK_MASTER_KEY`."""
    return os.urandom(32).hex()


def decode_manifest(raw: bytes | None) -> dict[str, Any]:
    """Decode a manifest payload (`b'PZST'` + zstd-compressed JSON or plain JSON fallback)."""
    if raw is None:
        raise ValueError("Cannot decode None manifest bytes")
    if raw.startswith(b"PZST"):
        return json.loads(zstd.decompress(raw[4:]).decode("utf-8"))
    return json.loads(raw.decode("utf-8"))
