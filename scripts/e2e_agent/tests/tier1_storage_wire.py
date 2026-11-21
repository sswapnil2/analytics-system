"""Tier 1 opaque-box E2E feature tests for Storage, Wire (PLST), PTWL, L0, and PLSS (F01-F12)."""

from __future__ import annotations

import hashlib
import json
import re
import struct
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from lib import fixtures, plst
from lib.auth import AuthBootstrap, ProjectCreds, TestContext
from lib.client import HttpResponse, PulseClient
from lib.node_mgr import NodeInstance, list_agent_artifacts, sqlite_tables
from lib.registry import test_case

_PLSS_FOOTER_MAGIC: bytes = b"PLSS"
_PLSS_MIN_BYTES: int = 44  # >= 20B section directory entry + 24B footer
_PLSS_MAX_BYTES: int = 128 * 1024 * 1024
_ENCRYPTED_HEADER_MAGIC: bytes = b"PLSEAGM1"
_MAX_TRUNCATED_BYTES: int = 65_536


# ---------------------------------------------------------------------------
# Private Opaque-Box Helpers (Zero Import-Time Side Effects)
# ---------------------------------------------------------------------------


def _assert_status(resp: HttpResponse, expected: int | tuple[int, ...], label: str = "") -> HttpResponse:
    allowed = (expected,) if isinstance(expected, int) else expected
    if resp.status not in allowed:
        prefix = f"{label}: " if label else ""
        preview = resp.text(errors="replace")[:400]
        raise AssertionError(f"{prefix}expected HTTP {allowed}, got {resp.status}: {preview}")
    return resp


def _assert_4xx(resp: HttpResponse, label: str = "") -> None:
    prefix = f"{label}: " if label else ""
    if resp.status == 200 or resp.status >= 500 or not (400 <= resp.status < 500):
        preview = resp.text(errors="replace")[:400]
        raise AssertionError(f"{prefix}expected 4xx rejection (never 200/500), got {resp.status}: {preview}")


def _create_temp_project(
    client: PulseClient,
    token: str,
    name: str,
    *,
    capture_user_id: bool = False,
) -> ProjectCreds:
    proj = AuthBootstrap._create_project_with_keys(client, token=token, name=name)
    if capture_user_id:
        policy = fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True)
        resp = client.request(
            "PUT",
            f"/v1/projects/{proj.id}/agent/privacy",
            token=token,
            json_body=policy,
        )
        _assert_status(resp, 200, "enable capture_user_id on temp project")
    return proj


def _spawn_isolated_node(
    ctx: TestContext,
    *,
    role: str = "all",
    extra_env: dict[str, str] | None = None,
) -> tuple[NodeInstance, TestContext]:
    if ctx.node_manager is None:
        raise RuntimeError("ctx.node_manager is required for isolated sub-node tests")
    inst = ctx.node_manager.start_node(role=role, extra_env=extra_env)
    try:
        sub_client = PulseClient(inst.base_url)
        sub_ctx = AuthBootstrap.bootstrap_context(
            sub_client,
            inst.data_dir,
            inst.root_admin_token(),
            run_id=ctx.unique_id("sub"),
            mock_llm=ctx.mock_llm,
            mock_webhook=ctx.mock_webhook,
            node_manager=ctx.node_manager,
        )
        return inst, sub_ctx
    except Exception:
        ctx.node_manager.stop(inst, graceful=False, timeout=5.0)
        raise


def _wait_for_artifacts(
    data_dir: Path,
    key: str,
    *,
    min_count: int = 1,
    predicate: Callable[[Path], bool] | None = None,
    timeout: float = 15.0,
) -> list[Path]:
    deadline = time.monotonic() + timeout
    last_found: list[Path] = []
    while time.monotonic() < deadline:
        paths = list_agent_artifacts(data_dir)[key]
        if predicate is not None:
            paths = [p for p in paths if predicate(p)]
        last_found = paths
        if len(paths) >= min_count:
            return paths
        time.sleep(0.25)
    raise TimeoutError(
        f"Timed out after {timeout:.1f}s waiting for >={min_count} {key} artifact(s) in {data_dir}; "
        f"found {len(last_found)}: {[str(p) for p in last_found]}"
    )


def _wait_until(
    fn: Callable[[], Any],
    *,
    timeout: float = 12.0,
    interval: float = 0.25,
    desc: str = "condition",
) -> Any:
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            res = fn()
            if res:
                return res
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(interval)
    if last_exc is not None:
        raise TimeoutError(f"Timed out waiting for {desc} ({timeout:.1f}s); last error: {last_exc}") from last_exc
    raise TimeoutError(f"Timed out waiting for {desc} ({timeout:.1f}s)")


def _is_encrypted_bytes(raw: bytes) -> bool:
    return raw.startswith(_ENCRYPTED_HEADER_MAGIC)


def _recent_ts_ms(offset_ms: int = 30_000) -> int:
    return int(time.time() * 1000) - offset_ms


def _ingest_plst_batch_retry_401(
    client: PulseClient,
    batch: dict[str, Any],
    *,
    api_key: str,
    timeout: float = 10.0,
    interval: float = 0.25,
    label: str = "ingest_plst_batch",
) -> HttpResponse:
    deadline = time.monotonic() + timeout
    while True:
        resp = client.ingest_plst_batch(batch, api_key=api_key)
        if resp.status != 401 or time.monotonic() >= deadline:
            return _assert_status(resp, 200, label)
        time.sleep(interval)


def _send_3phase_streaming(
    client: PulseClient,
    api_key: str,
    project_id: str,
    trace_id: str,
    span_id: str,
    session_id: str,
    *,
    reverse: bool = False,
    use_dict: bool = False,
    base_ts_ms: int | None = None,
) -> list[HttpResponse]:
    ts = base_ts_ms if base_ts_ms is not None else _recent_ts_ms()
    phase_specs = [
        ("start", "running", 10, 0, 0, {"partial": "Starting..."}),
        ("update", "running", 60, 20, 400_000, {"partial": "Thinking..."}),
        ("final", "ok", 120, 50, 1_500_000, {"text": "Final answer delivered."}),
    ]
    if reverse:
        phase_specs = list(reversed(phase_specs))

    responses: list[HttpResponse] = []
    for phase, status, dur_ms, out_tok, cost_nanos, out_payload in phase_specs:
        sp = fixtures.make_span(
            span_id,
            trace_id,
            project_id=project_id,
            name="llm.stream",
            span_type="llm",
            span_phase=phase,
            status=status,
            start_ts_ms=ts,
            duration_ms=dur_ms,
            input_tokens=100,
            output_tokens=out_tok,
            cost_nanos=cost_nanos,
            output_payload=out_payload,
        )
        tr = fixtures.make_trace(
            trace_id,
            project_id=project_id,
            session_id=session_id,
            status=status,
            start_ts_ms=ts,
            duration_ms=dur_ms,
            output_payload=out_payload,
            spans=[sp],
        )
        resp = client.ingest_plst_batch(
            {"traces": [tr], "spans": [sp]},
            api_key=api_key,
            use_dict=use_dict,
        )
        _assert_status(resp, 200, f"streaming phase={phase}")
        responses.append(resp)
    return responses


def _epoch_number(manifest_path: Path) -> int:
    m = re.search(r"epoch_(\d+)\.json$", manifest_path.name)
    if not m:
        raise AssertionError(f"Unexpected manifest filename: {manifest_path}")
    return int(m.group(1))


def _find_manifest_segments(manifest_obj: Any) -> list[dict[str, Any]]:
    """Extract segment metadata dicts from an MVCC epoch manifest regardless of wrapper key."""
    found: list[dict[str, Any]] = []
    if isinstance(manifest_obj, dict):
        if "file_size" in manifest_obj or "tail_offset" in manifest_obj or "segment_id" in manifest_obj:
            found.append(manifest_obj)
        for v in manifest_obj.values():
            found.extend(_find_manifest_segments(v))
    elif isinstance(manifest_obj, list):
        for item in manifest_obj:
            found.extend(_find_manifest_segments(item))
    return found


def _build_otlp_equivalent_from_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Convert a TraceIngestBatch into an OTLP/JSON ExportTraceServiceRequest with equivalent data."""

    def _kv_str(k: str, v: str) -> dict[str, Any]:
        return {"key": k, "value": {"stringValue": v}}

    def _kv_int(k: str, v: int) -> dict[str, Any]:
        return {"key": k, "value": {"intValue": str(v)}}

    otlp_spans: list[dict[str, Any]] = []
    trace_map = {tr["trace_id"]: tr for tr in batch.get("traces", [])}
    for sp in batch.get("spans", []):
        tr = trace_map.get(sp["trace_id"], {})
        start_ns = int(sp["start_ts_ms"]) * 1_000_000
        end_ns = int(sp.get("end_ts_ms") or (sp["start_ts_ms"] + sp.get("duration_ms", 0))) * 1_000_000
        attrs = [
            _kv_str("pulse.span_type", str(sp.get("span_type", "llm"))),
            _kv_str("pulse.span_phase", str(sp.get("span_phase", "final"))),
            _kv_str("gen_ai.system", str(sp.get("provider") or "openai")),
            _kv_str("gen_ai.request.model", str(sp.get("model") or "gpt-4o-mini")),
            _kv_str("gen_ai.response.model", str(sp.get("model") or "gpt-4o-mini")),
            _kv_int("gen_ai.usage.input_tokens", int(sp.get("input_tokens", 0))),
            _kv_int("gen_ai.usage.output_tokens", int(sp.get("output_tokens", 0))),
            _kv_str("gen_ai.prompt", json.dumps(sp.get("input"))),
            _kv_str("gen_ai.completion", json.dumps(sp.get("output"))),
        ]
        if tr.get("session_id"):
            attrs.append(_kv_str("session.id", str(tr["session_id"])))
        otlp_spans.append(
            {
                "traceId": fixtures.deterministic_hex_id(sp["trace_id"], 16),
                "spanId": fixtures.deterministic_hex_id(sp["span_id"], 8),
                "name": sp.get("name", "llm.turn"),
                "kind": 3,
                "startTimeUnixNano": str(start_ns),
                "endTimeUnixNano": str(end_ns),
                "attributes": attrs,
                "status": {"code": 1},
            }
        )
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        _kv_str("pulse.agent.name", "support-agent"),
                        _kv_str("pulse.agent.version", "1.0.0"),
                        _kv_str("deployment.environment.name", "production"),
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "pulse-e2e-wire-cmp", "version": "1.0.0"},
                        "spans": otlp_spans,
                    }
                ],
            }
        ]
    }


def _erase_session(
    client: PulseClient,
    pid: str,
    token: str,
    session_id: str,
    fallback_trace_id: str,
) -> HttpResponse:
    resp = client.request(
        "POST",
        f"/v1/projects/{pid}/agent/erasure",
        token=token,
        json_body={"level": "session", "target_id": session_id},
    )
    if resp.status in (404, 405):
        resp = client.request(
            "DELETE",
            f"/v1/projects/{pid}/agent/traces/{fallback_trace_id}",
            token=token,
            query={"level": "session"},
        )
    return resp


# ===========================================================================
# F01 — 5-Level Execution Hierarchy
# ===========================================================================


@test_case(
    id="T1-F01-01",
    tier=1,
    feature="F01",
    title="Full 5-level hierarchy round-trip (Project -> User -> Session -> Trace -> Span -> Events)",
    tags=("hierarchy", "plst", "smoke"),
)
def test_f01_01_full_hierarchy_roundtrip(ctx: TestContext) -> None:
    proj = _create_temp_project(
        ctx.client,
        ctx.owner_token,
        ctx.unique_id("proj_hier"),
        capture_user_id=True,
    )
    seed = ctx.unique_id("hier")
    batch = fixtures.make_hierarchy_batch(seed=seed, project_id=proj.id, start_ts_ms=_recent_ts_ms())
    ing = ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=True)
    _assert_status(ing, 200, "ingest hierarchy")
    ing_json = ing.json()
    assert ing_json["accepted_traces"] == 2, ing_json
    assert ing_json["accepted_spans"] == 11, ing_json

    session_id = f"sess_{seed}"
    user_id = f"user_{seed}"
    parent_tid = f"tr_{seed}_parent"
    child_tid = f"tr_{seed}_child"

    sess_resp = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions/{session_id}", token=ctx.viewer_token),
        200,
        "get session detail",
    ).json()
    assert sess_resp["summary"]["user_id"] == user_id, sess_resp
    assert sess_resp["summary"]["trace_count"] == 2, sess_resp
    sess_tids = {t["trace_id"] for t in sess_resp["traces"]}
    assert sess_tids == {parent_tid, child_tid}, sess_tids

    child_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/{child_tid}", token=ctx.viewer_token),
        200,
        "get child trace detail",
    ).json()
    assert child_detail["trace"]["parent_trace_id"] == parent_tid, child_detail["trace"]
    assert child_detail["trace"]["user_id"] == user_id, child_detail["trace"]

    parent_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/{parent_tid}", token=ctx.viewer_token),
        200,
        "get parent trace detail",
    ).json()
    assert len(parent_detail["spans"]) == 10, len(parent_detail["spans"])
    depths = {sp["depth"] for sp in parent_detail["spans"]}
    assert depths == {0, 1}, depths
    for sp in parent_detail["spans"]:
        assert len(sp["events"]) == 1, sp


@test_case(
    id="T1-F01-02",
    tier=1,
    feature="F01",
    title="Level-1 cross-project tenant isolation with identical session_id pattern",
    tags=("hierarchy", "isolation"),
)
def test_f01_02_cross_project_isolation(ctx: TestContext) -> None:
    shared_sid = ctx.unique_id("shared_sess")
    seed_a = ctx.unique_id("iso_a")
    seed_b = ctx.unique_id("iso_b")
    ts = _recent_ts_ms()
    batch_a = fixtures.make_batch(seed=seed_a, project_id=ctx.proj_a.id, session_id=shared_sid, start_ts_ms=ts)
    batch_b = fixtures.make_batch(seed=seed_b, project_id=ctx.proj_b.id, session_id=shared_sid, start_ts_ms=ts)

    _assert_status(ctx.client.ingest_plst_batch(batch_a, api_key=ctx.proj_a.pk_key), 200, "ingest proj_a")
    _assert_status(ctx.client.ingest_plst_batch(batch_b, api_key=ctx.proj_b.pk_key), 200, "ingest proj_b")

    tid_a = f"tr_{seed_a}"
    tid_b = f"tr_{seed_b}"

    cross_ab = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tid_b}",
        token=ctx.viewer_token,
    )
    _assert_status(cross_ab, 404, "cross-project proj_a -> tid_b")

    cross_ba = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_b.id}/agent/traces/{tid_a}",
        token=ctx.viewer_token,
    )
    _assert_status(cross_ba, 404, "cross-project proj_b -> tid_a")

    list_a = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/traces",
            token=ctx.viewer_token,
            query={"session_id": shared_sid},
        ),
        200,
    ).json()
    assert [t["trace_id"] for t in list_a["traces"]] == [tid_a], list_a

    list_b = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_b.id}/agent/traces",
            token=ctx.viewer_token,
            query={"session_id": shared_sid},
        ),
        200,
    ).json()
    assert [t["trace_id"] for t in list_b["traces"]] == [tid_b], list_b


@test_case(
    id="T1-F01-03",
    tier=1,
    feature="F01",
    title="Multi-trace delegation tree (workflow_id, parent_trace_id) and DAG linked_span_ids",
    tags=("hierarchy", "dag", "delegation"),
)
def test_f01_03_delegation_and_dag_links(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    wf_id = ctx.unique_id("wf")
    sid = ctx.unique_id("sess_dag")
    root_tid = ctx.unique_id("tr_root")
    child1_tid = ctx.unique_id("tr_child1")
    child2_tid = ctx.unique_id("tr_child2")
    ts = _recent_ts_ms()

    root_sp0 = fixtures.make_span(
        ctx.unique_id("sp_r0"), root_tid, project_id=pid, span_type="agent", depth=0, start_ts_ms=ts
    )
    root_sp1 = fixtures.make_span(
        ctx.unique_id("sp_r1"),
        root_tid,
        project_id=pid,
        parent_span_id=root_sp0["span_id"],
        linked_span_ids=[root_sp0["span_id"]],
        span_type="workflow",
        depth=1,
        start_ts_ms=ts + 10,
    )
    c1_sp0 = fixtures.make_span(
        ctx.unique_id("sp_c1"),
        child1_tid,
        project_id=pid,
        linked_span_ids=[root_sp0["span_id"], root_sp1["span_id"]],
        span_type="agent",
        depth=0,
        start_ts_ms=ts + 20,
    )
    c2_sp0 = fixtures.make_span(
        ctx.unique_id("sp_c2"),
        child2_tid,
        project_id=pid,
        linked_span_ids=[root_sp1["span_id"], c1_sp0["span_id"]],
        span_type="tool",
        depth=0,
        start_ts_ms=ts + 30,
    )

    batch = {
        "traces": [
            fixtures.make_trace(
                root_tid, project_id=pid, session_id=sid, workflow_id=wf_id,
                parent_trace_id=None, start_ts_ms=ts, spans=[root_sp0, root_sp1],
            ),
            fixtures.make_trace(
                child1_tid, project_id=pid, session_id=sid, workflow_id=wf_id,
                parent_trace_id=root_tid, agent_name="subagent-1", start_ts_ms=ts + 20, spans=[c1_sp0],
            ),
            fixtures.make_trace(
                child2_tid, project_id=pid, session_id=sid, workflow_id=wf_id,
                parent_trace_id=root_tid, agent_name="subagent-2", start_ts_ms=ts + 30, spans=[c2_sp0],
            ),
        ],
        "spans": [root_sp0, root_sp1, c1_sp0, c2_sp0],
    }
    _assert_status(ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key, use_dict=True), 200)

    children = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={"parent_trace_id": root_tid},
        ),
        200,
    ).json()
    child_ids = {t["trace_id"] for t in children["traces"]}
    assert child_ids == {child1_tid, child2_tid}, children

    c2_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{child2_tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert c2_detail["trace"]["workflow_id"] == wf_id
    assert c2_detail["spans"][0]["linked_span_ids"] == [root_sp1["span_id"], c1_sp0["span_id"]]


@test_case(
    id="T1-F01-04",
    tier=1,
    feature="F01",
    title="Cycle breaking on parent_span_id and depth clamping at <= 32",
    tags=("hierarchy", "cycle", "depth"),
)
def test_f01_04_cycle_breaking_and_depth_clamping(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    ts = _recent_ts_ms()
    cyc_seed = ctx.unique_id("cyc")
    cyc_batch = fixtures.make_cyclic_spans_batch(seed=cyc_seed, project_id=pid, start_ts_ms=ts)
    cyc_tid = cyc_batch["traces"][0]["trace_id"]
    sp_a_id = cyc_batch["spans"][0]["span_id"]
    # Add C -> A (with A <-> B) to verify non-cyclic descendant C is preserved cleanly
    sp_c = fixtures.make_span(
        ctx.unique_id("sp_c"),
        cyc_tid,
        project_id=pid,
        parent_span_id=sp_a_id,
        depth=2,
        name="cycle.descendant",
        span_type="custom",
        start_ts_ms=ts + 20,
    )
    cyc_batch["spans"].append(sp_c)
    cyc_batch["traces"][0]["span_count"] = len(cyc_batch["spans"])

    deep_seed = ctx.unique_id("deep")
    deep_batch = fixtures.make_deep_chain_batch(seed=deep_seed, project_id=pid, depth_count=40, start_ts_ms=ts)
    deep_tid = deep_batch["traces"][0]["trace_id"]

    _assert_status(ctx.client.ingest_plst_batch(cyc_batch, api_key=ctx.proj_a.pk_key), 200, "ingest cycle")
    _assert_status(ctx.client.ingest_plst_batch(deep_batch, api_key=ctx.proj_a.pk_key), 200, "ingest deep")

    cyc_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{cyc_tid}", token=ctx.viewer_token),
        200,
    ).json()
    parent_map = {s["span_id"]: s.get("parent_span_id") for s in cyc_detail["spans"]}
    for sid in parent_map:
        visited: set[str] = set()
        cur: str | None = sid
        while cur is not None:
            assert cur not in visited, f"Cycle still present in hydrated parent_span_id: {parent_map}"
            visited.add(cur)
            cur = parent_map.get(cur)

    deep_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{deep_tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert len(deep_detail["spans"]) == 40
    assert max(int(s["depth"]) for s in deep_detail["spans"]) <= 32, deep_detail["spans"][-1]


@test_case(
    id="T1-F01-05",
    tier=1,
    feature="F01",
    title="Per-trace span cap enforces <= 512 accepted spans and reports dropped_spans",
    tags=("hierarchy", "limits", "spans"),
)
def test_f01_05_per_trace_span_cap(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("many")
    batch = fixtures.make_many_spans_batch(seed=seed, project_id=pid, span_count=520, start_ts_ms=_recent_ts_ms())
    tid = batch["traces"][0]["trace_id"]

    ing = _assert_status(
        ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key),
        200,
        "ingest 520 spans",
    ).json()
    assert ing["accepted_traces"] == 1, ing
    assert ing["accepted_spans"] == 512, ing
    assert ing["dropped_spans"] == 8, ing

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert len(detail["spans"]) <= 512, len(detail["spans"])
    assert len(detail["spans"]) == 512, len(detail["spans"])


# ===========================================================================
# F02 — Extended Span Types, span_phase & cost_nanos
# ===========================================================================


@test_case(
    id="T1-F02-01",
    tier=1,
    feature="F02",
    title="All 10 SpanType variants including guardrail preserved via PLST",
    tags=("span_type", "guardrail", "plst"),
)
def test_f02_01_all_span_types_including_guardrail(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("types")
    batch = fixtures.make_hierarchy_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms())
    _assert_status(ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key, use_dict=True), 200)

    parent_tid = f"tr_{seed}_parent"
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{parent_tid}", token=ctx.viewer_token),
        200,
    ).json()
    hydrated_types = {sp["span_type"] for sp in detail["spans"]}
    assert hydrated_types == set(fixtures.ALL_SPAN_TYPES), hydrated_types
    guardrails = [sp for sp in detail["spans"] if sp["span_type"] == "guardrail"]
    assert len(guardrails) == 1 and guardrails[0]["span_phase"] == "final", guardrails


@test_case(
    id="T1-F02-02",
    tier=1,
    feature="F02",
    title="Sequential 3-request streaming span_phase (start -> update -> final) convergence",
    tags=("span_phase", "streaming", "plst"),
)
def test_f02_02_streaming_span_phase_convergence(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_stream")
    sid = ctx.unique_id("sp_stream")
    sess_id = ctx.unique_id("sess_stream")

    _send_3phase_streaming(ctx.client, ctx.proj_a.pk_key, pid, tid, sid, sess_id, reverse=False, use_dict=False)

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert detail["trace"]["status"] == "ok", detail["trace"]
    matching = [sp for sp in detail["spans"] if sp["span_id"] == sid]
    assert len(matching) == 1, detail["spans"]
    sp = matching[0]
    assert sp["span_phase"] == "final", sp
    assert sp["status"] == "ok", sp
    assert sp["output_tokens"] == 50, sp
    assert sp["cost_nanos"] == 1_500_000, sp


@test_case(
    id="T1-F02-03",
    tier=1,
    feature="F02",
    title="Sub-cent integer cost_nanos precision preserved without float drift",
    tags=("cost_nanos", "precision"),
)
def test_f02_03_subcent_cost_nanos_precision(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_nanos")
    sess_id = ctx.unique_id("sess_nanos")
    ts = _recent_ts_ms()
    nanos_vals = [123_456_789, 1, 987_654_321]
    spans = [
        fixtures.make_span(
            ctx.unique_id(f"sp_n{i}"),
            tid,
            project_id=pid,
            span_type="llm",
            cost_nanos=val,
            start_ts_ms=ts + i * 10,
        )
        for i, val in enumerate(nanos_vals)
    ]
    tr = fixtures.make_trace(tid, project_id=pid, session_id=sess_id, start_ts_ms=ts, spans=spans)
    _assert_status(ctx.client.ingest_plst_batch({"traces": [tr], "spans": spans}, api_key=ctx.proj_a.pk_key), 200)

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token),
        200,
    ).json()
    got_nanos = sorted(sp["cost_nanos"] for sp in detail["spans"])
    assert got_nanos == sorted(nanos_vals), got_nanos
    assert detail["trace"]["total_cost_nanos"] == 1_111_111_111, detail["trace"]
    assert abs(float(detail["trace"]["total_cost_usd"]) - 1.111111111) < 1e-9, detail["trace"]


@test_case(
    id="T1-F02-04",
    tier=1,
    feature="F02",
    title="linked_span_ids and parent_trace_id preserved across native JSON and OTLP/JSON",
    tags=("linked_span_ids", "parent_trace_id", "otlp"),
)
def test_f02_04_linked_spans_and_parent_trace_json_otlp(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    parent_tid = ctx.unique_id("tr_parent_ref")
    ts = _recent_ts_ms()

    # 1. Native JSON ingest
    native_tid = ctx.unique_id("tr_native_links")
    sp_root = fixtures.make_span(
        ctx.unique_id("sp_nr"), native_tid, project_id=pid, span_type="agent", depth=0, start_ts_ms=ts
    )
    sp_leaf = fixtures.make_span(
        ctx.unique_id("sp_nl"),
        native_tid,
        project_id=pid,
        parent_span_id=sp_root["span_id"],
        linked_span_ids=[sp_root["span_id"]],
        span_type="tool",
        depth=1,
        start_ts_ms=ts + 10,
    )
    native_tr = fixtures.make_trace(
        native_tid,
        project_id=pid,
        session_id=ctx.unique_id("sess_nat"),
        parent_trace_id=parent_tid,
        start_ts_ms=ts,
        spans=[sp_root, sp_leaf],
    )
    _assert_status(
        ctx.client.ingest_json({"traces": [native_tr], "spans": [sp_root, sp_leaf]}, api_key=ctx.proj_a.pk_key),
        200,
        "native json ingest",
    )

    # 2. OTLP/JSON ingest with links + pulse.parent_trace_id
    otlp_seed = ctx.unique_id("otlp_links")
    otlp_req = fixtures.make_otlp_export_request(
        seed=otlp_seed,
        session_id=ctx.unique_id("sess_otlp"),
        start_ts_ms=ts,
    )
    otlp_spans = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"]
    otlp_spans[0]["attributes"].append(
        {"key": "pulse.parent_trace_id", "value": {"stringValue": parent_tid}}
    )
    otlp_tid = otlp_spans[0]["traceId"]
    otlp_root_sid = otlp_spans[0]["spanId"]
    _assert_status(ctx.client.ingest_otlp_json(otlp_req, api_key=ctx.proj_a.pk_key), 200, "otlp json ingest")

    nat_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{native_tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert nat_detail["trace"]["parent_trace_id"] == parent_tid
    nat_leaf = next(s for s in nat_detail["spans"] if s["span_id"] == sp_leaf["span_id"])
    assert nat_leaf["linked_span_ids"] == [sp_root["span_id"]]

    otlp_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{otlp_tid}", token=ctx.viewer_token),
        200,
    ).json()
    assert otlp_detail["trace"]["parent_trace_id"] == parent_tid
    assert any(otlp_root_sid in s.get("linked_span_ids", []) for s in otlp_detail["spans"])


@test_case(
    id="T1-F02-05",
    tier=1,
    feature="F02",
    title="PLST lifecycle flags (FLAG_PARTIAL, FLAG_FINAL, mixed) match wire and hydrate span_phase",
    tags=("span_phase", "wire", "flags"),
)
def test_f02_05_plst_lifecycle_flags(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    ts = _recent_ts_ms()
    tid_p = ctx.unique_id("tr_flag_p")
    sp_p = fixtures.make_span(
        ctx.unique_id("sp_fp"), tid_p, project_id=pid, span_phase="start", status="running", start_ts_ms=ts
    )
    tr_p = fixtures.make_trace(
        tid_p, project_id=pid, session_id=ctx.unique_id("sess_fp"), status="running", start_ts_ms=ts, spans=[sp_p]
    )
    partial_frame = plst.encode_plst_frame({"traces": [tr_p], "spans": [sp_p]}, use_dict=False)
    assert plst.inspect_plst_header(partial_frame)["flags"] == (plst.FLAG_RAW_ZSTD | plst.FLAG_PARTIAL)

    tid_f = ctx.unique_id("tr_flag_f")
    sp_f = fixtures.make_span(
        ctx.unique_id("sp_ff"), tid_f, project_id=pid, span_phase="final", status="ok", start_ts_ms=ts
    )
    tr_f = fixtures.make_trace(
        tid_f, project_id=pid, session_id=ctx.unique_id("sess_ff"), status="ok", start_ts_ms=ts, spans=[sp_f]
    )
    final_frame = plst.encode_plst_frame({"traces": [tr_f], "spans": [sp_f]}, use_dict=False)
    assert plst.inspect_plst_header(final_frame)["flags"] == (plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL)

    mixed_frame = plst.encode_plst_frame({"traces": [tr_p, tr_f], "spans": [sp_p, sp_f]}, use_dict=False)
    assert plst.inspect_plst_header(mixed_frame)["flags"] == (
        plst.FLAG_RAW_ZSTD | plst.FLAG_PARTIAL | plst.FLAG_FINAL
    )

    _assert_status(ctx.client.ingest_plst(partial_frame, api_key=ctx.proj_a.pk_key), 200, "ingest partial")
    _assert_status(ctx.client.ingest_plst(final_frame, api_key=ctx.proj_a.pk_key), 200, "ingest final")

    d_p = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid_p}", token=ctx.viewer_token), 200
    ).json()
    assert d_p["spans"][0]["span_phase"] == "start"

    d_f = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid_f}", token=ctx.viewer_token), 200
    ).json()
    assert d_f["spans"][0]["span_phase"] == "final"


# ===========================================================================
# F03 — PLST v1 Header & Decompression Guard
# ===========================================================================


@test_case(
    id="T1-F03-01",
    tier=1,
    feature="F03",
    title="20B LE PLST v1 header verification (RAW_ZSTD|FINAL, CRC32, dict_id=0) and live ingest",
    tags=("plst", "header", "raw_zstd"),
)
def test_f03_01_plst_20b_header_raw_zstd(ctx: TestContext) -> None:
    import zlib

    pid = ctx.proj_a.id
    seed = ctx.unique_id("hdr_raw")
    batch = fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms())
    frame = plst.encode_plst_frame(batch, use_dict=False)
    magic, ver, flags, dict_id, uncomp_len, comp_len, crc32_val = struct.unpack("<4sBBHIII", frame[:20])
    assert magic == b"PLST"
    assert ver == 1
    assert flags == (plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL)
    assert dict_id == 0
    assert 0 < uncomp_len <= plst.MAX_PLST_UNCOMP_BYTES
    assert comp_len == len(frame) - 20
    assert crc32_val == (zlib.crc32(frame[20:]) & 0xFFFFFFFF)

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200, "ingest raw_zstd")
    tid = f"tr_{seed}"
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["trace_id"] == tid
    assert len(detail["spans"]) == 3


@test_case(
    id="T1-F03-02",
    tier=1,
    feature="F03",
    title="Uncompressed PLST v1 frame (FLAG_FINAL, dict_id=0, uncomp_len==comp_len) accepted",
    tags=("plst", "uncompressed", "header"),
)
def test_f03_02_plst_uncompressed_frame(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("hdr_uncomp")
    batch = fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms())
    frame = plst.encode_plst_frame(batch, flags=plst.FLAG_FINAL)
    hdr = plst.inspect_plst_header(frame)
    assert hdr["flags"] == plst.FLAG_FINAL
    assert hdr["dict_id"] == 0
    assert hdr["uncomp_len"] == hdr["comp_len"]

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200, "ingest uncompressed")
    tid = f"tr_{seed}"
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["trace_id"] == tid


@test_case(
    id="T1-F03-03",
    tier=1,
    feature="F03",
    title="Corrupt CompressedCRC32 frame rejected with 4xx and never stored",
    tags=("plst", "crc32", "guard"),
)
def test_f03_03_crc32_mismatch_rejected(ctx: TestContext) -> None:
    seed = ctx.unique_id("bad_crc")
    batch = fixtures.make_batch(seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms())
    bad_frame = plst.build_hostile_frame("crc_mismatch", base_batch=batch)
    resp = ctx.client.ingest_plst(bad_frame, api_key=ctx.proj_a.pk_key)
    _assert_4xx(resp, "crc_mismatch")

    get_resp = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces/tr_{seed}", token=ctx.viewer_token
    )
    _assert_status(get_resp, 404, "corrupt trace must not be stored")


@test_case(
    id="T1-F03-04",
    tier=1,
    feature="F03",
    title="UncompLen > 64 MiB header rejected pre-decompression with 4xx and node stays healthy",
    tags=("plst", "limit", "guard"),
)
def test_f03_04_oversized_uncomp_len_rejected(ctx: TestContext) -> None:
    bad_frame = plst.build_hostile_frame("oversized_uncomp_len")
    resp = ctx.client.ingest_plst(bad_frame, api_key=ctx.proj_a.pk_key)
    _assert_4xx(resp, "oversized_uncomp_len")
    _assert_status(ctx.client.get_healthz(), 200, "healthz after oversized_uncomp_len")


@test_case(
    id="T1-F03-05",
    tier=1,
    feature="F03",
    title="Bounded Zstd decompression rejects 65 MiB zstd_bomb with 4xx and keeps server healthy",
    tags=("plst", "zstd_bomb", "guard"),
)
def test_f03_05_zstd_bomb_rejected(ctx: TestContext) -> None:
    bomb = plst.build_hostile_frame("zstd_bomb")
    resp = ctx.client.ingest_plst(bomb, api_key=ctx.proj_a.pk_key)
    _assert_4xx(resp, "zstd_bomb")
    _assert_status(ctx.client.get_healthz(), 200, "healthz after zstd_bomb")

    follow_seed = ctx.unique_id("after_bomb")
    follow_batch = fixtures.make_batch(seed=follow_seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms())
    _assert_status(
        ctx.client.ingest_plst_batch(follow_batch, api_key=ctx.proj_a.pk_key),
        200,
        "follow-up valid ingest after zstd_bomb",
    )


# ===========================================================================
# F04 — AI Agent Zstd Dictionary (AGENT_DICT_ID = 2)
# ===========================================================================


@test_case(
    id="T1-F04-01",
    tier=1,
    feature="F04",
    title="Dictionary-compressed PLST ingest (AGENT_DICT_ID=2, FLAG_DICT_ZSTD) hydrates fully",
    tags=("plst", "zstd_dict", "dict_id_2"),
)
def test_f04_01_dict_zstd_ingest(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("dict2")
    batch = fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms())
    frame = plst.encode_plst_frame(batch, use_dict=True)
    hdr = plst.inspect_plst_header(frame)
    assert hdr["dict_id"] == plst.AGENT_DICT_ID == 2
    assert hdr["flags"] & plst.FLAG_DICT_ZSTD

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200, "ingest dict_id=2")
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["trace_id"] == f"tr_{seed}"
    assert len(detail["spans"]) == 3


@test_case(
    id="T1-F04-02",
    tier=1,
    feature="F04",
    title="AGENT_DICT_ID=2 matches 1014B dictionary and shrinks OpenAI/OTel payloads vs raw Zstd",
    tags=("zstd_dict", "compression", "otel"),
)
def test_f04_02_dict_compression_efficiency(ctx: TestContext) -> None:
    assert len(plst.DEFAULT_AGENT_DICT_BYTES) == 1014
    pid = ctx.proj_a.id
    seed = ctx.unique_id("dict_cmp")
    rich_in = {
        "messages": [
            {"role": "system", "content": "You are a helpful support agent."},
            {"role": "user", "content": "Summarize tool_calls and usage."},
        ],
        "gen_ai.system": "openai",
        "gen_ai.request.model": "gpt-4o-mini",
        "gen_ai.operation.name": "chat",
        "service.name": "support-agent",
        "deployment.environment": "production",
    }
    rich_out = {
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "Done."},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 120, "completion_tokens": 45, "total_tokens": 165},
    }
    ts = _recent_ts_ms()
    sp = fixtures.make_span(
        ctx.unique_id("sp_dc"),
        f"tr_{seed}",
        project_id=pid,
        span_type="llm",
        start_ts_ms=ts,
        input_payload=rich_in,
        output_payload=rich_out,
    )
    tr = fixtures.make_trace(
        f"tr_{seed}",
        project_id=pid,
        session_id=f"sess_{seed}",
        start_ts_ms=ts,
        input_payload=rich_in,
        output_payload=rich_out,
        spans=[sp],
    )
    batch = {"traces": [tr], "spans": [sp]}
    raw_frame = plst.encode_plst_frame(batch, use_dict=False)
    dict_frame = plst.encode_plst_frame(batch, use_dict=True)
    assert len(dict_frame) < len(raw_frame), f"dict={len(dict_frame)} raw={len(raw_frame)}"

    resp = _assert_status(ctx.client.ingest_plst(dict_frame, api_key=ctx.proj_a.pk_key), 200).json()
    assert resp["accepted_traces"] == 1, resp
    assert resp["accepted_spans"] == 1, resp

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["trace_id"] == f"tr_{seed}", detail
    assert len(detail["spans"]) == 1, detail


@test_case(
    id="T1-F04-03",
    tier=1,
    feature="F04",
    title="Unknown Zstd dictionary ID (dict_id=999) rejected with 4xx",
    tags=("zstd_dict", "validation"),
)
def test_f04_03_unknown_dict_id_rejected(ctx: TestContext) -> None:
    bad_frame = plst.build_hostile_frame("unknown_dict_id")
    assert plst.inspect_plst_header(bad_frame)["dict_id"] == 999
    resp = ctx.client.ingest_plst(bad_frame, api_key=ctx.proj_a.pk_key)
    _assert_4xx(resp, "unknown_dict_id")


@test_case(
    id="T1-F04-04",
    tier=1,
    feature="F04",
    title="Mixed DICT_ZSTD, RAW_ZSTD, and uncompressed frames converge in same session",
    tags=("zstd_dict", "interop"),
)
def test_f04_04_mixed_compression_interop_session(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    sid = ctx.unique_id("sess_mix")
    s1, s2, s3 = ctx.unique_id("m1"), ctx.unique_id("m2"), ctx.unique_id("m3")
    ts = _recent_ts_ms()

    f_dict = plst.encode_plst_frame(
        fixtures.make_batch(seed=s1, project_id=pid, session_id=sid, start_ts_ms=ts), use_dict=True
    )
    f_raw = plst.encode_plst_frame(
        fixtures.make_batch(seed=s2, project_id=pid, session_id=sid, start_ts_ms=ts), use_dict=False
    )
    f_unc = plst.encode_plst_frame(
        fixtures.make_batch(seed=s3, project_id=pid, session_id=sid, start_ts_ms=ts), flags=plst.FLAG_FINAL
    )

    for lbl, frm in (("dict", f_dict), ("raw", f_raw), ("uncomp", f_unc)):
        _assert_status(ctx.client.ingest_plst(frm, api_key=ctx.proj_a.pk_key), 200, f"ingest {lbl}")

    sess = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/sessions/{sid}", token=ctx.viewer_token), 200
    ).json()
    assert sess["summary"]["trace_count"] == 3, sess
    assert {t["trace_id"] for t in sess["traces"]} == {f"tr_{s1}", f"tr_{s2}", f"tr_{s3}"}


@test_case(
    id="T1-F04-05",
    tier=1,
    feature="F04",
    title="Dictionary-compressed streaming FLAG_DICT_ZSTD|FLAG_PARTIAL then FLAG_FINAL",
    tags=("zstd_dict", "streaming"),
)
def test_f04_05_dict_zstd_streaming_phases(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_ds")
    sp_id = ctx.unique_id("sp_ds")
    sid = ctx.unique_id("sess_ds")
    ts = _recent_ts_ms()

    sp_start = fixtures.make_span(
        sp_id, tid, project_id=pid, span_type="llm", span_phase="start", status="running",
        start_ts_ms=ts, duration_ms=15, output_tokens=0, cost_nanos=0, output_payload={"partial": "init"},
    )
    tr_start = fixtures.make_trace(
        tid, project_id=pid, session_id=sid, status="running", start_ts_ms=ts, duration_ms=15, spans=[sp_start]
    )
    f_start = plst.encode_plst_frame({"traces": [tr_start], "spans": [sp_start]}, use_dict=True)
    assert plst.inspect_plst_header(f_start)["flags"] == (plst.FLAG_DICT_ZSTD | plst.FLAG_PARTIAL)

    sp_final = fixtures.make_span(
        sp_id, tid, project_id=pid, span_type="llm", span_phase="final", status="ok",
        start_ts_ms=ts, duration_ms=120, output_tokens=77, cost_nanos=2_000_000, output_payload={"answer": "done"},
    )
    tr_final = fixtures.make_trace(
        tid, project_id=pid, session_id=sid, status="ok", start_ts_ms=ts, duration_ms=120, spans=[sp_final]
    )
    f_final = plst.encode_plst_frame({"traces": [tr_final], "spans": [sp_final]}, use_dict=True)
    assert plst.inspect_plst_header(f_final)["flags"] == (plst.FLAG_DICT_ZSTD | plst.FLAG_FINAL)

    _assert_status(ctx.client.ingest_plst(f_start, api_key=ctx.proj_a.pk_key), 200)
    _assert_status(ctx.client.ingest_plst(f_final, api_key=ctx.proj_a.pk_key), 200)

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["status"] == "ok"
    assert len(detail["spans"]) == 1
    assert detail["spans"][0]["span_phase"] == "final"
    assert detail["spans"][0]["output_tokens"] == 77


# ===========================================================================
# F05 — 5 Columnar PLST Sections & Blob Dedup
# ===========================================================================


@test_case(
    id="T1-F05-01",
    tier=1,
    feature="F05",
    title="Content-addressed Blob Table deduplicates repeated 4 KiB prompts via XXH3-64 + byte check",
    tags=("plst", "blob_dedup", "xxh3"),
)
def test_f05_01_blob_dedup_repeated_prompts(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("dedup")
    batch = fixtures.make_repeated_prompt_batch(
        seed=seed, project_id=pid, repeats=8, prompt_bytes=4096, start_ts_ms=_recent_ts_ms()
    )
    expected_prompt = batch["traces"][0]["input"]["system"]
    assert 3500 <= len(expected_prompt) <= 4096, len(expected_prompt)
    stats = plst.BlobDedupStats()
    frame = plst.encode_plst_frame(batch, use_dict=True, stats_out=stats)
    assert stats.deduped_blobs >= 14, stats
    assert stats.bytes_saved >= 14 * len(expected_prompt), stats

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200)

    for i in range(8):
        tid = f"tr_{seed}_{i:02d}"
        detail = _assert_status(
            ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
        ).json()
        assert detail["trace"]["input"]["system"] == expected_prompt
        assert detail["spans"][0]["input"]["system"] == expected_prompt


@test_case(
    id="T1-F05-02",
    tier=1,
    feature="F05",
    title="Columnar String Table interns shared strings across 20 traces and 40 spans",
    tags=("plst", "string_table", "columnar"),
)
def test_f05_02_string_table_roundtrip(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    sess_id = ctx.unique_id("sess_str")
    agent_name = ctx.unique_id("shared_agent")
    ts = _recent_ts_ms()
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for i in range(20):
        tid = ctx.unique_id(f"tr_st_{i}")
        s1 = fixtures.make_span(
            ctx.unique_id(f"sp_s1_{i}"), tid, project_id=pid, span_type="llm",
            provider="openai", model="gpt-4o-mini", start_ts_ms=ts + i * 50,
        )
        s2 = fixtures.make_span(
            ctx.unique_id(f"sp_s2_{i}"), tid, project_id=pid, parent_span_id=s1["span_id"],
            depth=1, span_type="tool", tool_name="lookup_kb", start_ts_ms=ts + i * 50 + 10,
        )
        tr = fixtures.make_trace(
            tid, project_id=pid, agent_name=agent_name, environment="staging",
            session_id=sess_id, tags=["shared-tag", "e2e"],
            start_ts_ms=ts + i * 50, spans=[s1, s2],
        )
        traces.append(tr)
        spans.extend([s1, s2])

    frame = plst.encode_plst_frame({"traces": traces, "spans": spans}, use_dict=True)
    decoded = plst.decode_plst_frame(frame)
    assert len(decoded["traces"]) == 20
    assert len(decoded["spans"]) == 40
    assert all(t["agent_name"] == agent_name for t in decoded["traces"])

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200)
    sample_detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{traces[10]['trace_id']}", token=ctx.viewer_token),
        200,
    ).json()
    assert sample_detail["trace"]["agent_name"] == agent_name
    assert sample_detail["trace"]["environment"] == "staging"
    assert sample_detail["trace"]["tags"] == ["shared-tag", "e2e"]
    assert any(s.get("tool_name") == "lookup_kb" for s in sample_detail["spans"])


@test_case(
    id="T1-F05-03",
    tier=1,
    feature="F05",
    title="Blob Table preserves empty containers ('', {}, []) distinct from null/None",
    tags=("plst", "blob_table", "nullability"),
)
def test_f05_03_empty_containers_vs_null(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_nulls")
    ts = _recent_ts_ms()
    sp_empty = fixtures.make_span(
        ctx.unique_id("sp_empty"), tid, project_id=pid, span_type="custom",
        start_ts_ms=ts, input_payload="", output_payload={}, metadata={},
    )
    sp_empty["attributes"] = []
    sp_null = fixtures.make_span(
        ctx.unique_id("sp_null"), tid, project_id=pid, span_type="custom", start_ts_ms=ts,
    )
    sp_null["input"] = None
    sp_null["output"] = None
    sp_null["metadata"] = None

    tr = fixtures.make_trace(
        tid, project_id=pid, session_id=ctx.unique_id("sess_nulls"),
        start_ts_ms=ts, input_payload="", output_payload={}, spans=[sp_empty, sp_null],
    )
    tr["metadata"] = []
    batch = {"traces": [tr], "spans": [sp_empty, sp_null]}
    frame = plst.encode_plst_frame(batch, use_dict=True)
    decoded = plst.decode_plst_frame(frame)
    assert decoded["traces"][0]["input"] == ""
    assert decoded["traces"][0]["output"] == {}
    assert decoded["traces"][0]["metadata"] == []

    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200)
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["input"] == ""
    assert detail["trace"]["output"] == {}
    assert detail["trace"]["metadata"] == []
    by_id = {s["span_id"]: s for s in detail["spans"]}
    assert by_id[sp_empty["span_id"]]["input"] == ""
    assert by_id[sp_empty["span_id"]]["output"] == {}
    assert by_id[sp_empty["span_id"]]["attributes"] == []
    assert by_id[sp_null["span_id"]]["input"] is None
    assert by_id[sp_null["span_id"]]["output"] is None
    assert by_id[sp_null["span_id"]]["metadata"] is None


@test_case(
    id="T1-F05-04",
    tier=1,
    feature="F05",
    title="Section 5 Columnar Span Events preserve per-span counts, timestamps, and attributes",
    tags=("plst", "span_events", "columnar"),
)
def test_f05_04_columnar_span_events(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_events")
    ts = _recent_ts_ms()
    sp_a = fixtures.make_span(
        ctx.unique_id("sp_ev2"), tid, project_id=pid, span_type="llm", start_ts_ms=ts,
        events=[
            fixtures.make_event("token.first", timestamp_ms=ts + 12, attributes={"idx": 0, "ok": True}),
            fixtures.make_event("token.done", timestamp_ms=ts + 95, attributes={"idx": 1, "reason": "stop"}),
        ],
    )
    sp_b = fixtures.make_span(
        ctx.unique_id("sp_ev0"), tid, project_id=pid, span_type="tool", start_ts_ms=ts + 100, events=[],
    )
    sp_c = fixtures.make_span(
        ctx.unique_id("sp_ev3"), tid, project_id=pid, span_type="guardrail", start_ts_ms=ts + 150,
        events=[
            fixtures.make_event("guard.scan", timestamp_ms=ts + 151, attributes={"rule": "pii"}),
            fixtures.make_event("guard.score", timestamp_ms=ts + 152, attributes={"score": 0.99}),
            fixtures.make_event("guard.pass", timestamp_ms=ts + 153, attributes={"action": "allow"}),
        ],
    )
    tr = fixtures.make_trace(
        tid, project_id=pid, session_id=ctx.unique_id("sess_ev"), start_ts_ms=ts, spans=[sp_a, sp_b, sp_c]
    )
    _assert_status(
        ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp_a, sp_b, sp_c]}, api_key=ctx.proj_a.pk_key, use_dict=True),
        200,
    )

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    by_id = {s["span_id"]: s for s in detail["spans"]}
    assert [e["name"] for e in by_id[sp_a["span_id"]]["events"]] == ["token.first", "token.done"]
    assert by_id[sp_a["span_id"]]["events"][0]["timestamp_ms"] == ts + 12
    assert by_id[sp_a["span_id"]]["events"][1]["attributes"] == {"idx": 1, "reason": "stop"}
    assert by_id[sp_b["span_id"]]["events"] == []
    assert len(by_id[sp_c["span_id"]]["events"]) == 3
    assert by_id[sp_c["span_id"]]["events"][1]["attributes"] == {"score": 0.99}


@test_case(
    id="T1-F05-05",
    tier=1,
    feature="F05",
    title="Blob Table preserves mixed BLOB_ENC_UTF8_TEXT and BLOB_ENC_JSON_UTF8 types without double-encoding",
    tags=("plst", "blob_table", "encoding"),
)
def test_f05_05_mixed_blob_text_and_json_encoding(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_enc")
    ts = _recent_ts_ms()
    sp_text = fixtures.make_span(
        ctx.unique_id("sp_txt"), tid, project_id=pid, span_type="llm", start_ts_ms=ts,
        input_payload="Plain UTF-8 prompt: café ☕",
        output_payload='{"raw_string_looks_like_json": true}',
    )
    sp_json = fixtures.make_span(
        ctx.unique_id("sp_json"), tid, project_id=pid, span_type="tool", start_ts_ms=ts + 10,
        input_payload={"items": [1, 2, 3], "nested": {"ok": True}},
        output_payload=["a", "b", 42],
    )
    tr = fixtures.make_trace(
        tid, project_id=pid, session_id=ctx.unique_id("sess_enc"), start_ts_ms=ts,
        input_payload="trace plain string", output_payload={"structured": [True, None, 7]},
        spans=[sp_text, sp_json],
    )
    frame = plst.encode_plst_frame({"traces": [tr], "spans": [sp_text, sp_json]}, use_dict=True)
    _assert_status(ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key), 200)

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["input"] == "trace plain string"
    assert isinstance(detail["trace"]["input"], str)
    assert detail["trace"]["output"] == {"structured": [True, None, 7]}
    by_id = {s["span_id"]: s for s in detail["spans"]}
    assert by_id[sp_text["span_id"]]["input"] == "Plain UTF-8 prompt: café ☕"
    assert isinstance(by_id[sp_text["span_id"]]["output"], str)
    assert by_id[sp_json["span_id"]]["input"] == {"items": [1, 2, 3], "nested": {"ok": True}}
    assert by_id[sp_json["span_id"]]["output"] == ["a", "b", 42]


# ===========================================================================
# F06 — Multi-Writer Crash-Safe WAL (PTWL)
# ===========================================================================


@test_case(
    id="T1-F06-01",
    tier=1,
    feature="F06",
    title="PTWL WAL segment created on ObjectStore under agent_wal/{proj}/{writer}/seq_{seq:012}.ptwl",
    tags=("wal", "ptwl", "artifacts"),
)
def test_f06_01_ptwl_wal_artifact_layout(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        batch = fixtures.make_batch(seed=ctx.unique_id("wal1"), project_id=pid, start_ts_ms=_recent_ts_ms())
        _assert_status(sub_ctx.client.ingest_plst_batch(batch, api_key=sub_ctx.proj_a.pk_key), 200)

        wal_files = _wait_for_artifacts(inst.data_dir, "agent_wal", min_count=1, timeout=10.0)
        pat = re.compile(rf"agent_wal/{re.escape(pid)}/[^/]+/seq_\d{{12}}\.ptwl$")
        matched = [p for p in wal_files if pat.search(p.as_posix())]
        assert matched, f"No matching PTWL path in {[str(p) for p in wal_files]}"
        assert all(p.stat().st_size > 0 for p in matched)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F06-02",
    tier=1,
    feature="F06",
    title="Concurrent 15-trace burst uses bounded group-commit with monotonic WAL seq numbers",
    tags=("wal", "ptwl", "group_commit"),
)
def test_f06_02_group_commit_concurrent_burst(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        seeds = [ctx.unique_id(f"gc_{i}") for i in range(15)]

        def _send_one(seed: str) -> HttpResponse:
            c = PulseClient(sub_ctx.base_url)
            b = fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms())
            return c.ingest_plst_batch(b, api_key=sub_ctx.proj_a.pk_key)

        with ThreadPoolExecutor(max_workers=8) as pool:
            resps = list(pool.map(_send_one, seeds))
        for r in resps:
            _assert_status(r, 200, "concurrent burst ingest")

        wal_files = _wait_for_artifacts(
            inst.data_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        assert 1 <= len(wal_files) <= 15, f"Unexpected WAL count {len(wal_files)}"
        seqs = [int(re.search(r"seq_(\d{12})\.ptwl$", p.name).group(1)) for p in wal_files]  # type: ignore[union-attr]
        assert len(seqs) == len(set(seqs)) and seqs == sorted(seqs), seqs

        for seed in seeds:
            _assert_status(
                sub_ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=sub_ctx.viewer_token),
                200,
            )
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F06-03",
    tier=1,
    feature="F06",
    title="Two --role ingest writers produce distinct writer_id WAL dirs visible to --role query",
    tags=("wal", "multi_writer", "split_node"),
)
def test_f06_03_multi_writer_split_ingest_query(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    shared_dir = ctx.node_manager.make_temp_data_dir("pulse_mw_")
    shared_tok = f"sk_root_{ctx.unique_id('mw')}"
    spawned: list[NodeInstance] = []
    try:
        ingest1 = ctx.node_manager.start_node(role="ingest", data_dir=shared_dir, admin_token=shared_tok)
        spawned.append(ingest1)
        ingest2 = ctx.node_manager.start_node(role="ingest", data_dir=shared_dir, admin_token=shared_tok)
        spawned.append(ingest2)
        query_node = ctx.node_manager.start_node(role="query", data_dir=shared_dir, admin_token=shared_tok)
        spawned.append(query_node)

        q_client = PulseClient(query_node.base_url)
        sub_ctx = AuthBootstrap.bootstrap_context(
            q_client,
            shared_dir,
            shared_tok,
            run_id=ctx.unique_id("mw_sub"),
            node_manager=ctx.node_manager,
        )
        pid = sub_ctx.proj_a.id
        c1 = PulseClient(ingest1.base_url)
        c2 = PulseClient(ingest2.base_url)

        seeds_1 = [ctx.unique_id(f"w1_{i}") for i in range(5)]
        seeds_2 = [ctx.unique_id(f"w2_{i}") for i in range(5)]
        ts = _recent_ts_ms()
        for idx, s in enumerate(seeds_1):
            batch = fixtures.make_batch(seed=s, project_id=pid, start_ts_ms=ts + idx * 10)
            if idx == 0:
                _ingest_plst_batch_retry_401(
                    c1, batch, api_key=sub_ctx.proj_a.pk_key, label="ingest1 initial key-sync"
                )
            else:
                _assert_status(c1.ingest_plst_batch(batch, api_key=sub_ctx.proj_a.pk_key), 200)
        for idx, s in enumerate(seeds_2):
            batch = fixtures.make_batch(seed=s, project_id=pid, start_ts_ms=ts + 100 + idx * 10)
            if idx == 0:
                _ingest_plst_batch_retry_401(
                    c2, batch, api_key=sub_ctx.proj_a.pk_key, label="ingest2 initial key-sync"
                )
            else:
                _assert_status(c2.ingest_plst_batch(batch, api_key=sub_ctx.proj_a.pk_key), 200)

        wal_files = _wait_for_artifacts(
            shared_dir,
            "agent_wal",
            min_count=2,
            predicate=lambda p: pid in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        writer_ids = {p.parent.name for p in wal_files}
        assert len(writer_ids) >= 2, f"Expected >=2 distinct writer_ids, got {writer_ids}"

        def _all_visible() -> bool:
            for s in seeds_1 + seeds_2:
                r = q_client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s}", token=sub_ctx.viewer_token)
                if r.status != 200:
                    return False
            return True

        _wait_until(_all_visible, timeout=15.0, desc="all 10 multi-writer traces visible on query node")
    finally:
        for inst in reversed(spawned):
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F06-04",
    tier=1,
    feature="F06",
    title="PTWL crash recovery: SIGKILL before L0 flush replays all 5 traces on restart",
    tags=("wal", "crash_recovery", "sigkill"),
)
def test_f06_04_wal_sigkill_crash_recovery(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        seeds = [ctx.unique_id(f"crash_{i}") for i in range(5)]
        ts = _recent_ts_ms()
        for s in seeds:
            _assert_status(
                sub_ctx.client.ingest_plst_batch(
                    fixtures.make_batch(seed=s, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
                ),
                200,
            )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        ctx.node_manager.kill(inst)
        inst2 = ctx.node_manager.start_all(data_dir=inst.data_dir, admin_token=inst.root_admin_token())
        c2 = PulseClient(inst2.base_url)
        for s in seeds:
            _assert_status(
                c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s}", token=sub_ctx.viewer_token),
                200,
                f"replayed trace tr_{s}",
            )
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F06-05",
    tier=1,
    feature="F06",
    title="Zero SQL writes on ingest: agent_traces/agent_spans absent from SQLite while PTWL exists",
    tags=("wal", "no_sql", "ptwl"),
)
def test_f06_05_zero_sql_writes_ptwl_exists(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst: NodeInstance | None = None
    try:
        if ctx.data_dir is not None:
            target_dir = ctx.data_dir
            client = ctx.client
            proj = ctx.proj_a
        else:
            inst, sub_ctx = _spawn_isolated_node(ctx)
            target_dir = inst.data_dir
            client = sub_ctx.client
            proj = sub_ctx.proj_a

        seed = ctx.unique_id("nosql")
        _assert_status(
            client.ingest_plst_batch(
                fixtures.make_batch(seed=seed, project_id=proj.id, start_ts_ms=_recent_ts_ms()),
                api_key=proj.pk_key,
            ),
            200,
        )
        tables = sqlite_tables(target_dir)
        assert "agent_traces" not in tables, tables
        assert "agent_spans" not in tables, tables

        wal_files = _wait_for_artifacts(
            target_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        assert len(wal_files) >= 1
    finally:
        if inst is not None:
            ctx.node_manager.stop(inst)


# ===========================================================================
# F07 — Sharded L0 MemTable & Fast Flush
# ===========================================================================


@test_case(
    id="T1-F07-01",
    tier=1,
    feature="F07",
    title="Near-real-time L0 MemTable visibility (<1500ms) for trace, filter list, and session",
    tags=("l0", "memtable", "nrt"),
)
def test_f07_01_immediate_l0_nrt_visibility(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("nrt")
    agent_name = ctx.unique_id("nrt_agent")
    sid = ctx.unique_id("sess_nrt")
    tid = f"tr_{seed}"
    ts = _recent_ts_ms()
    sp = fixtures.make_span(ctx.unique_id("sp_nrt"), tid, project_id=pid, span_type="llm", start_ts_ms=ts)
    tr = fixtures.make_trace(tid, project_id=pid, agent_name=agent_name, session_id=sid, start_ts_ms=ts, spans=[sp])

    t0 = time.monotonic()
    _assert_status(ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=ctx.proj_a.pk_key), 200)
    r_trace = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    )
    r_list = _assert_status(
        ctx.client.request(
            "GET", f"/v1/projects/{pid}/agent/traces", token=ctx.viewer_token, query={"agent_name": agent_name}
        ),
        200,
    )
    r_sess = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/sessions/{sid}", token=ctx.viewer_token), 200
    )
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    assert elapsed_ms < 1500.0, f"NRT ingest+3 queries took {elapsed_ms:.1f}ms (>=1500ms)"
    assert r_trace.json()["trace"]["trace_id"] == tid
    assert [t["trace_id"] for t in r_list.json()["traces"]] == [tid]
    assert r_sess.json()["summary"]["session_id"] == sid


@test_case(
    id="T1-F07-02",
    tier=1,
    feature="F07",
    title="L0 MemTable flushes automatically to PLSS segments within flush age window",
    tags=("l0", "flush", "plss"),
)
def test_f07_02_l0_auto_flush_to_plss(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        seeds = [ctx.unique_id(f"fl_{i}") for i in range(3)]
        ts = _recent_ts_ms()
        for s in seeds:
            _assert_status(
                sub_ctx.client.ingest_plst_batch(
                    fixtures.make_batch(seed=s, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
                ),
                200,
            )

        segs = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        assert len(segs) >= 1
        for s in seeds:
            _assert_status(
                sub_ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s}", token=sub_ctx.viewer_token),
                200,
            )
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F07-03",
    tier=1,
    feature="F07",
    title="L0 sharding partitions flushed PLSS segments by (trace_date, shard_XX)",
    tags=("l0", "sharding", "plss"),
)
def test_f07_03_date_and_hash_sharding(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        now_utc = datetime.now(timezone.utc) - timedelta(minutes=5)
        prev_utc = now_utc - timedelta(days=1)
        d1_str = prev_utc.date().isoformat()
        d2_str = now_utc.date().isoformat()
        ts_day1 = int(prev_utc.timestamp() * 1000)
        ts_day2 = int(now_utc.timestamp() * 1000)
        for i in range(6):
            _assert_status(
                sub_ctx.client.ingest_plst_batch(
                    fixtures.make_batch(seed=ctx.unique_id(f"d1_{i}"), project_id=pid, start_ts_ms=ts_day1),
                    api_key=sub_ctx.proj_a.pk_key,
                ),
                200,
            )
            _assert_status(
                sub_ctx.client.ingest_plst_batch(
                    fixtures.make_batch(seed=ctx.unique_id(f"d2_{i}"), project_id=pid, start_ts_ms=ts_day2),
                    api_key=sub_ctx.proj_a.pk_key,
                ),
                200,
            )

        def _both_dates_present() -> list[Path]:
            segs = [
                p
                for p in list_agent_artifacts(inst.data_dir)["agent_segments"]
                if pid in p.parts and p.name.endswith(".plss")
            ]
            has_d1 = any(f"/{d1_str}/shard_" in p.as_posix() for p in segs)
            has_d2 = any(f"/{d2_str}/shard_" in p.as_posix() for p in segs)
            return segs if (has_d1 and has_d2) else []

        segs = _wait_until(_both_dates_present, timeout=15.0, desc=f"PLSS segments for {d1_str} and {d2_str}")
        assert any(f"/{d1_str}/shard_" in p.as_posix() for p in segs)
        assert any(f"/{d2_str}/shard_" in p.as_posix() for p in segs)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F07-04",
    tier=1,
    feature="F07",
    title="Streaming start -> final updates L0 immediately and flushes single merged ok trace to PLSS",
    tags=("l0", "streaming", "merge"),
)
def test_f07_04_l0_streaming_merge_and_flush(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        tid = ctx.unique_id("tr_l0m")
        sp_id = ctx.unique_id("sp_l0m")
        sid = ctx.unique_id("sess_l0m")
        ts = _recent_ts_ms()

        sp_start = fixtures.make_span(
            sp_id, tid, project_id=pid, span_type="llm", span_phase="start", status="running",
            start_ts_ms=ts, duration_ms=10, output_tokens=0, cost_nanos=0,
        )
        tr_start = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, status="running", start_ts_ms=ts, duration_ms=10, spans=[sp_start]
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr_start], "spans": [sp_start]}, api_key=sub_ctx.proj_a.pk_key),
            200,
        )
        d1 = _assert_status(
            sub_ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=sub_ctx.viewer_token), 200
        ).json()
        assert d1["trace"]["status"] == "running"

        sp_final = fixtures.make_span(
            sp_id, tid, project_id=pid, span_type="llm", span_phase="final", status="ok",
            start_ts_ms=ts, duration_ms=100, output_tokens=60, cost_nanos=900_000,
        )
        tr_final = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, status="ok", start_ts_ms=ts, duration_ms=100, spans=[sp_final]
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr_final], "spans": [sp_final]}, api_key=sub_ctx.proj_a.pk_key),
            200,
        )
        d2 = _assert_status(
            sub_ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=sub_ctx.viewer_token), 200
        ).json()
        assert d2["trace"]["status"] == "ok"

        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        d_cold = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=sub_ctx.viewer_token), 200
        ).json()
        assert d_cold["trace"]["status"] == "ok"
        assert len(d_cold["spans"]) == 1
        assert d_cold["spans"][0]["span_phase"] == "final"
        assert d_cold["spans"][0]["output_tokens"] == 60
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F07-05",
    tier=1,
    feature="F07",
    title="Graceful SIGTERM drains L0 MemTable cleanly (exit 0) and serves all 4 traces on restart",
    tags=("l0", "shutdown", "drain"),
)
def test_f07_05_graceful_shutdown_drain(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        seeds = [ctx.unique_id(f"drain_{i}") for i in range(4)]
        ts = _recent_ts_ms()
        for s in seeds:
            ingest_resp = _assert_status(
                sub_ctx.client.ingest_plst_batch(
                    fixtures.make_batch(seed=s, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
                ),
                200,
            ).json()
            assert ingest_resp["accepted_traces"] == 1, ingest_resp

        rc = ctx.node_manager.stop(inst, graceful=True, timeout=20.0)
        assert rc == 0, f"Expected exit code 0 on graceful SIGTERM, got {rc}"

        inst2 = ctx.node_manager.start_all(data_dir=inst.data_dir, admin_token=inst.root_admin_token())
        c2 = PulseClient(inst2.base_url)
        for s in seeds:
            detail = _assert_status(
                c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s}", token=sub_ctx.viewer_token),
                200,
                f"trace tr_{s} after graceful drain",
            ).json()
            assert detail["trace"]["trace_id"] == f"tr_{s}", detail
            assert len(detail["spans"]) == 3, detail
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


# ===========================================================================
# F08 — PLSS Immutable Segment Container & Directory/Footer
# ===========================================================================


@test_case(
    id="T1-F08-01",
    tier=1,
    feature="F08",
    title="PLSS segment path layout and bounded file size (44B <= size <= 128 MiB)",
    tags=("plss", "artifacts", "layout"),
)
def test_f08_01_plss_path_layout_and_size_bounds(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=ctx.unique_id("plss_lay"), project_id=pid, start_ts_ms=_recent_ts_ms()),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        segs = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        pat = re.compile(rf"agent_segments/{re.escape(pid)}/\d{{4}}-\d{{2}}-\d{{2}}/shard_\d{{2}}/[^/]+\.plss$")
        matched = [p for p in segs if pat.search(p.as_posix())]
        assert matched, f"No PLSS path matched expected layout in {[str(p) for p in segs]}"
        for p in matched:
            sz = p.stat().st_size
            assert _PLSS_MIN_BYTES <= sz <= _PLSS_MAX_BYTES, f"Segment {p} size {sz} out of bounds"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F08-02",
    tier=1,
    feature="F08",
    title="PLSS 24B trailing footer magic ('PLSS') and cold query round-trip after restart",
    tags=("plss", "footer", "directory"),
)
def test_f08_02_plss_footer_magic_and_cold_roundtrip(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        seed = ctx.unique_id("plss_ftr")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms()),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        segs = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        newest = max(segs, key=lambda p: p.stat().st_mtime)
        raw = newest.read_bytes()
        assert len(raw) >= _PLSS_MIN_BYTES
        if not _is_encrypted_bytes(raw):
            assert _PLSS_FOOTER_MAGIC in raw[-24:], f"Missing PLSS magic in footer: {raw[-24:]!r}"

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        detail = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=sub_ctx.viewer_token), 200
        ).json()
        assert detail["trace"]["trace_id"] == f"tr_{seed}"
        assert len(detail["spans"]) == 3
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F08-03",
    tier=1,
    feature="F08",
    title="Cold --role query node hydrates trace, spans, and events purely from PLSS",
    tags=("plss", "cold_read", "query"),
)
def test_f08_03_cold_query_node_reads_plss(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    q_node: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        seed = ctx.unique_id("cold_q")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms()),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        admin_tok = inst.root_admin_token()
        ctx.node_manager.stop(inst, graceful=True)
        q_node = ctx.node_manager.start_node(role="query", data_dir=inst.data_dir, admin_token=admin_tok)
        qc = PulseClient(q_node.base_url)
        detail = _assert_status(
            qc.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=sub_ctx.viewer_token), 200
        ).json()
        assert detail["trace"]["trace_id"] == f"tr_{seed}"
        assert len(detail["spans"]) == 3
        llm_sp = next(s for s in detail["spans"] if s["span_type"] == "llm")
        assert len(llm_sp["events"]) == 1
        assert llm_sp["events"][0]["name"] == "first_token"
    finally:
        if q_node is not None:
            ctx.node_manager.stop(q_node)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F08-04",
    tier=1,
    feature="F08",
    title="PLSS segments are immutable: batch 1 segments stay byte-identical when batch 2 flushes",
    tags=("plss", "immutability"),
)
def test_f08_04_plss_segment_immutability(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        ts = _recent_ts_ms()
        s1 = ctx.unique_id("imm1")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s1, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        segs1 = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        snap1 = {p: (p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest()) for p in segs1}

        s2 = ctx.unique_id("imm2")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s2, project_id=pid, start_ts_ms=ts + 1_000), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        segs2 = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=len(snap1) + 1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        for p, (sz, digest) in snap1.items():
            assert p.exists(), f"Original immutable segment {p} disappeared before GC"
            assert p.stat().st_size == sz
            assert hashlib.sha256(p.read_bytes()).hexdigest() == digest
        assert len(segs2) > len(snap1)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F08-05",
    tier=1,
    feature="F08",
    title="Repeated cold reads across foyer/moka cache return deterministic identical payloads",
    tags=("plss", "cache", "foyer"),
)
def test_f08_05_repeated_cold_reads_deterministic(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        seed = ctx.unique_id("cache_r")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=seed, project_id=pid, start_ts_ms=_recent_ts_ms()),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        first_json = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=sub_ctx.viewer_token), 200
        ).json()
        for _ in range(9):
            cur = _assert_status(
                c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{seed}", token=sub_ctx.viewer_token), 200
            ).json()
            assert cur == first_json
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


# ===========================================================================
# F09 — PLSS Tags 0x01–0x07
# ===========================================================================


@test_case(
    id="T1-F09-01",
    tier=1,
    feature="F09",
    title="Tag 0x01/0x02 CRDT Trace Delta merge across 2 cold PLSS segments",
    tags=("plss", "tag_01", "tag_02"),
)
def test_f09_01_crdt_trace_delta_merge_across_segments(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        tid = ctx.unique_id("tr_crdt")
        sp_id = ctx.unique_id("sp_crdt")
        sid = ctx.unique_id("sess_crdt")
        ts = _recent_ts_ms()

        sp1 = fixtures.make_span(
            sp_id, tid, project_id=pid, span_type="llm", span_phase="start", status="running",
            start_ts_ms=ts, duration_ms=10, output_tokens=0, cost_nanos=0,
        )
        tr1 = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, status="running", start_ts_ms=ts, duration_ms=10, spans=[sp1]
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr1], "spans": [sp1]}, api_key=sub_ctx.proj_a.pk_key), 200
        )
        segs1 = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        sp2 = fixtures.make_span(
            sp_id, tid, project_id=pid, span_type="llm", span_phase="final", status="ok",
            start_ts_ms=ts, duration_ms=140, output_tokens=80, cost_nanos=2_200_000,
        )
        tr2 = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, status="ok", start_ts_ms=ts, duration_ms=140, spans=[sp2]
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr2], "spans": [sp2]}, api_key=sub_ctx.proj_a.pk_key), 200
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=len(segs1) + 1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        listed = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces", token=sub_ctx.viewer_token, query={"session_id": sid}),
            200,
        ).json()
        assert len(listed["traces"]) == 1, listed
        merged = listed["traces"][0]
        assert merged["trace_id"] == tid
        assert merged["status"] == "ok"
        assert merged["span_count"] == 1
        assert merged["total_cost_nanos"] == 2_200_000
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F09-02",
    tier=1,
    feature="F09",
    title="Cold Tag 0x03 Span Metrics analytics computes exact percentiles, cost, and breakdowns",
    tags=("plss", "tag_03", "analytics"),
)
def test_f09_02_cold_tag03_span_metrics_analytics(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        now_ms = int(time.time() * 1000) - 30_000
        traces: list[dict[str, Any]] = []
        spans: list[dict[str, Any]] = []
        for i in range(10):
            tid = ctx.unique_id(f"tr_an_{i}")
            is_fast = i < 5
            model = "gpt-4o-mini" if is_fast else "claude-3-5-sonnet"
            provider = "openai" if is_fast else "anthropic"
            tool_name = "search_docs" if is_fast else "fetch_order"
            dur = 100 if is_fast else 300
            cost = 1_000_000 if is_fast else 3_000_000
            s_llm = fixtures.make_span(
                ctx.unique_id(f"sp_al_{i}"), tid, project_id=pid, span_type="llm",
                provider=provider, model=model, start_ts_ms=now_ms + i * 100,
                duration_ms=dur, cost_nanos=cost,
            )
            s_tool = fixtures.make_span(
                ctx.unique_id(f"sp_at_{i}"), tid, project_id=pid, parent_span_id=s_llm["span_id"],
                depth=1, span_type="tool", tool_name=tool_name, start_ts_ms=now_ms + i * 100 + 5,
                duration_ms=20 if is_fast else 40, cost_nanos=0,
            )
            tr = fixtures.make_trace(
                tid, project_id=pid, session_id=ctx.unique_id(f"sess_an_{i}"),
                start_ts_ms=now_ms + i * 100, duration_ms=dur, spans=[s_llm, s_tool],
            )
            traces.append(tr)
            spans.extend([s_llm, s_tool])

        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=sub_ctx.proj_a.pk_key),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        ov = _assert_status(
            c2.request(
                "GET",
                f"/v1/projects/{pid}/agent/analytics",
                token=sub_ctx.viewer_token,
                query={"start_ts_ms": now_ms - 5_000, "end_ts_ms": now_ms + 60_000},
            ),
            200,
        ).json()
        assert ov["total_executions"] == 10, ov
        assert ov["latency"]["p50_ms"] > 0
        assert ov["latency"]["p95_ms"] >= ov["latency"]["p50_ms"]
        assert ov["latency"]["p99_ms"] >= ov["latency"]["p95_ms"]
        assert abs(float(ov["total_cost_usd"]) - 0.02) < 1e-6, ov["total_cost_usd"]
        models = {b["key"] for b in ov["by_model"]}
        assert {"gpt-4o-mini", "claude-3-5-sonnet"}.issubset(models), ov["by_model"]
        tools = {b["tool_name"] for b in ov["by_tool"]}
        assert {"search_docs", "fetch_order"}.issubset(tools), ov["by_tool"]
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F09-03",
    tier=1,
    feature="F09",
    title="Cold Tag 0x04 Span Tree+Events and Tag 0x05 Trace-Contiguous Payload Pack hydration",
    tags=("plss", "tag_04", "tag_05"),
)
def test_f09_03_cold_tag04_tag05_hydration(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        tid = ctx.unique_id("tr_pack")
        ts = _recent_ts_ms()
        spans: list[dict[str, Any]] = []
        for i in range(6):
            payload = {"turn": i, "blob": f"chunk_{i}_" + ("Z" * 16_000)}
            spans.append(
                fixtures.make_span(
                    ctx.unique_id(f"sp_pk_{i}"),
                    tid,
                    project_id=pid,
                    parent_span_id=spans[0]["span_id"] if i > 0 else None,
                    depth=0 if i == 0 else 1,
                    span_type="llm" if i % 2 == 0 else "tool",
                    start_ts_ms=ts + i * 25,
                    input_payload=payload,
                    output_payload={"reply_idx": i, "data": "Y" * 16_000},
                    events=[fixtures.make_event(f"ev.pk.{i}", timestamp_ms=ts + i * 25 + 5, attributes={"i": i})],
                )
            )
        tr = fixtures.make_trace(
            tid, project_id=pid, session_id=ctx.unique_id("sess_pk"), start_ts_ms=ts, spans=spans
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr], "spans": spans}, api_key=sub_ctx.proj_a.pk_key), 200
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)
        detail = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=sub_ctx.viewer_token), 200
        ).json()
        assert len(detail["spans"]) == 6
        for sp in detail["spans"]:
            assert len(sp["events"]) == 1
            assert len(sp["input"]["blob"]) > 16_000
            assert len(sp["output"]["data"]) == 16_000
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F09-04",
    tier=1,
    feature="F09",
    title="Cold Tag 0x06 Bloom filters, dictionary bitsets, and zone-map pruning",
    tags=("plss", "tag_06", "bloom", "zonemap"),
)
def test_f09_04_cold_tag06_bloom_bitset_zonemap_pruning(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        proj = _create_temp_project(
            sub_ctx.client, sub_ctx.owner_token, ctx.unique_id("proj_bloom"), capture_user_id=True
        )
        pid = proj.id
        ts = _recent_ts_ms()

        tr1_id = ctx.unique_id("tr_b1")
        s1_id = ctx.unique_id("sess_b1")
        u1_id = ctx.unique_id("user_b1")
        pt1_id = ctx.unique_id("pt_b1")
        sp1 = fixtures.make_span(
            ctx.unique_id("sp_b1"), tr1_id, project_id=pid, span_type="llm",
            provider="openai", model="gpt-4o-mini", start_ts_ms=ts, duration_ms=100, cost_nanos=1_000_000,
        )
        tr1 = fixtures.make_trace(
            tr1_id, project_id=pid, agent_name="alpha-agent", environment="production",
            session_id=s1_id, user_id=u1_id, parent_trace_id=pt1_id, status="ok",
            start_ts_ms=ts, duration_ms=100, spans=[sp1],
        )

        tr2_id = ctx.unique_id("tr_b2")
        s2_id = ctx.unique_id("sess_b2")
        u2_id = ctx.unique_id("user_b2")
        pt2_id = ctx.unique_id("pt_b2")
        sp2 = fixtures.make_span(
            ctx.unique_id("sp_b2"), tr2_id, project_id=pid, span_type="llm", status="error",
            provider="anthropic", model="claude-3-5-sonnet", start_ts_ms=ts + 200,
            duration_ms=800, cost_nanos=10_000_000, error_type="RateLimit", error_message="429",
        )
        tr2 = fixtures.make_trace(
            tr2_id, project_id=pid, agent_name="beta-agent", environment="staging",
            session_id=s2_id, user_id=u2_id, parent_trace_id=pt2_id, status="error",
            start_ts_ms=ts + 200, duration_ms=800, spans=[sp2],
        )

        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": [tr1, tr2], "spans": [sp1, sp2]}, api_key=proj.pk_key),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)

        checks: list[tuple[dict[str, Any], list[str]]] = [
            ({"session_id": s1_id}, [tr1_id]),
            ({"user_id": u2_id}, [tr2_id]),
            ({"parent_trace_id": pt1_id}, [tr1_id]),
            ({"agent_name": "beta-agent"}, [tr2_id]),
            ({"model": "gpt-4o-mini"}, [tr1_id]),
            ({"environment": "staging"}, [tr2_id]),
            ({"status": "error"}, [tr2_id]),
            ({"min_duration_ms": 500}, [tr2_id]),
            ({"min_cost_usd": 0.005}, [tr2_id]),
            ({"session_id": ctx.unique_id("unknown_sess")}, []),
        ]
        for query, expected_ids in checks:
            resp = _assert_status(
                c2.request("GET", f"/v1/projects/{pid}/agent/traces", token=sub_ctx.viewer_token, query=query),
                200,
                f"filter {query}",
            ).json()
            got_ids = [t["trace_id"] for t in resp["traces"]]
            assert got_ids == expected_ids, f"query={query} got={got_ids} expected={expected_ids}"

        _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/{tr1_id}", token=sub_ctx.viewer_token), 200
        )
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F09-05",
    tier=1,
    feature="F09",
    title="Cold Tag 0x07 Session Rollup accurately aggregates trace_count, errors, tokens, and cost",
    tags=("plss", "tag_07", "sessions"),
)
def test_f09_05_cold_tag07_session_rollup(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        sid = ctx.unique_id("sess_r7")
        base_ts = _recent_ts_ms()
        traces: list[dict[str, Any]] = []
        spans: list[dict[str, Any]] = []
        for i in range(4):
            tid = ctx.unique_id(f"tr_s7_{i}")
            st = "error" if i == 3 else "ok"
            sp = fixtures.make_span(
                ctx.unique_id(f"sp_s7_{i}"), tid, project_id=pid, span_type="llm", status=st,
                start_ts_ms=base_ts + i * 1_000, duration_ms=100,
                input_tokens=100, output_tokens=50, cost_nanos=1_000_000,
            )
            tr = fixtures.make_trace(
                tid, project_id=pid, session_id=sid, status=st,
                start_ts_ms=base_ts + i * 1_000, duration_ms=100, spans=[sp],
            )
            traces.append(tr)
            spans.append(sp)

        _assert_status(
            sub_ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=sub_ctx.proj_a.pk_key), 200
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)

        listed = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/sessions", token=sub_ctx.viewer_token), 200
        ).json()
        match = [s for s in listed["sessions"] if s["session_id"] == sid]
        assert len(match) == 1, listed
        summary = match[0]
        assert summary["trace_count"] == 4, summary
        assert summary["error_count"] == 1, summary
        assert summary["total_tokens"] == 600, summary
        assert summary["total_cost_nanos"] == 4_000_000, summary
        assert summary["first_ts_ms"] < summary["last_ts_ms"], summary

        detail = _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/sessions/{sid}", token=sub_ctx.viewer_token), 200
        ).json()
        assert detail["summary"]["trace_count"] == 4
        assert len(detail["traces"]) == 4
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


# ===========================================================================
# F10 — MVCC Manifest (epoch_{N}.json) & 15m GC Grace
# ===========================================================================


@test_case(
    id="T1-F10-01",
    tier=1,
    feature="F10",
    title="MVCC manifest epoch_{N}.json created after flush and references PLSS segments",
    tags=("manifest", "mvcc", "artifacts"),
)
def test_f10_01_mvcc_manifest_artifact_created(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=ctx.unique_id("man1"), project_id=pid, start_ts_ms=_recent_ts_ms()),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        manifests = _wait_for_artifacts(
            inst.data_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: pid in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None,
            timeout=15.0,
        )
        latest = max(manifests, key=_epoch_number)
        raw = latest.read_bytes()
        if not _is_encrypted_bytes(raw):
            parsed = json.loads(raw.decode("utf-8"))
            assert isinstance(parsed, dict)
            segs = _find_manifest_segments(parsed)
            assert len(segs) >= 1, parsed
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F10-02",
    tier=1,
    feature="F10",
    title="MVCC manifest epoch numbers advance monotonically across separate flushes",
    tags=("manifest", "mvcc", "cas"),
)
def test_f10_02_mvcc_manifest_epoch_monotonic(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        ts = _recent_ts_ms()
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=ctx.unique_id("cas1"), project_id=pid, start_ts_ms=ts),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        m1 = _wait_for_artifacts(
            inst.data_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: pid in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None,
            timeout=15.0,
        )
        n1 = max(_epoch_number(p) for p in m1)

        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=ctx.unique_id("cas2"), project_id=pid, start_ts_ms=ts + 1_000),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        m2 = _wait_for_artifacts(
            inst.data_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: pid in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None
            and _epoch_number(p) > n1,
            timeout=15.0,
        )
        n2 = max(_epoch_number(p) for p in m2)
        assert n2 > n1, (n1, n2)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F10-03",
    tier=1,
    feature="F10",
    title="Committed manifest + post-commit WAL replay survive SIGKILL with zero data loss",
    tags=("manifest", "wal_seq", "durability"),
)
def test_f10_03_manifest_plus_wal_sigkill_durability(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        ts = _recent_ts_ms()
        s1 = ctx.unique_id("dur1")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s1, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: pid in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None,
            timeout=15.0,
        )

        s2 = ctx.unique_id("dur2")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s2, project_id=pid, start_ts_ms=ts + 1_000), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        ctx.node_manager.kill(inst)

        inst2 = ctx.node_manager.start_all(data_dir=inst.data_dir, admin_token=inst.root_admin_token())
        c2 = PulseClient(inst2.base_url)
        _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s1}", token=sub_ctx.viewer_token),
            200,
            "batch 1 from manifest/PLSS",
        )
        _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s2}", token=sub_ctx.viewer_token),
            200,
            "batch 2 from WAL replay",
        )
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F10-04",
    tier=1,
    feature="F10",
    title="Manifest segment metadata matches on-disk file_size, tail_offset, and zone map range",
    tags=("manifest", "zonemap", "footer"),
)
def test_f10_04_manifest_filesize_tail_offset_zonemap(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        ts = _recent_ts_ms()
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=ctx.unique_id("mz"), project_id=pid, start_ts_ms=ts),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        segs = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        manifests = _wait_for_artifacts(
            inst.data_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: pid in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None,
            timeout=15.0,
        )
        latest = max(manifests, key=_epoch_number)
        raw = latest.read_bytes()
        if not _is_encrypted_bytes(raw):
            manifest = json.loads(raw.decode("utf-8"))
            seg_entries = _find_manifest_segments(manifest)
            assert seg_entries, manifest
            disk_sizes = {p.name: p.stat().st_size for p in segs}
            verified_entry = False
            for entry in seg_entries:
                if "file_size" in entry and "tail_offset" in entry:
                    file_size = int(entry["file_size"])
                    tail_offset = int(entry["tail_offset"])
                    assert 0 < tail_offset <= file_size, entry
                    seg_ref = str(entry.get("path") or entry.get("object_key") or entry.get("seg_id") or "")
                    for disk_name, d_sz in disk_sizes.items():
                        if disk_name in seg_ref or disk_name.removesuffix(".plss") in seg_ref:
                            assert file_size == d_sz, (entry, d_sz)
                    zm = entry.get("zone_map") if isinstance(entry.get("zone_map"), dict) else entry
                    if "min_start_ts_ms" in zm and "max_start_ts_ms" in zm:
                        assert int(zm["min_start_ts_ms"]) <= ts <= int(zm["max_start_ts_ms"]), zm
                    verified_entry = True
            assert verified_entry, f"No segment entry had file_size & tail_offset: {seg_entries}"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T1-F10-05",
    tier=1,
    feature="F10",
    title="15-minute (900s) GC grace period prevents premature segment deletion after erasure",
    tags=("manifest", "gc_grace", "compaction"),
)
def test_f10_05_gc_grace_period_preserves_live_segments(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    try:
        pid = sub_ctx.proj_a.id
        ts = _recent_ts_ms()
        s_del = ctx.unique_id("gc_del")
        s_keep = ctx.unique_id("gc_keep")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s_del, project_id=pid, start_ts_ms=ts), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        segs1 = _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s_keep, project_id=pid, start_ts_ms=ts + 1_000), api_key=sub_ctx.proj_a.pk_key
            ),
            200,
        )
        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=len(segs1) + 1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        del_resp = sub_ctx.client.request(
            "DELETE",
            f"/v1/projects/{pid}/agent/traces/tr_{s_del}",
            token=sub_ctx.admin_token,
        )
        _assert_status(del_resp, (200, 204), "delete trace")

        # Wait past one background maintenance tick (5s) and confirm live reads never 404/500
        time.sleep(6.0)
        _assert_status(
            sub_ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s_keep}", token=sub_ctx.viewer_token),
            200,
            "kept trace must stay readable",
        )
        for seg_p in segs1:
            assert seg_p.exists(), f"Segment {seg_p} deleted prematurely before 900s GC grace period"

        manifests = list_agent_artifacts(inst.data_dir)["agent_manifests"]
        proj_manifests = [p for p in manifests if pid in p.parts and p.name.endswith(".json")]
        if proj_manifests:
            latest = max(proj_manifests, key=_epoch_number)
            raw = latest.read_bytes()
            if not _is_encrypted_bytes(raw):
                parsed = json.loads(raw.decode("utf-8"))
                pending_gc = parsed.get("pending_gc", [])
                now_ms = int(time.time() * 1000)
                for gc_item in pending_gc:
                    if isinstance(gc_item, dict):
                        exp = gc_item.get("delete_after_ms") or gc_item.get("expires_at_ms") or gc_item.get("eligible_at_ms")
                        if exp is not None:
                            assert int(exp) > now_ms, gc_item
    finally:
        ctx.node_manager.stop(inst)


# ===========================================================================
# F11 — Compaction, Erasure Filtering, Retention & Query Engine
# ===========================================================================


@test_case(
    id="T1-F11-01",
    tier=1,
    feature="F11",
    title="Trace-level GDPR erasure via DELETE removes trace from detail/list and writes tombstone",
    tags=("erasure", "tombstone", "query"),
)
def test_f11_01_trace_erasure_and_tombstone(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst: NodeInstance | None = None
    try:
        if ctx.data_dir is not None:
            data_dir = ctx.data_dir
            client = ctx.client
            proj = ctx.proj_a
            admin_tok = ctx.admin_token
            viewer_tok = ctx.viewer_token
        else:
            inst, sub_ctx = _spawn_isolated_node(ctx)
            data_dir = inst.data_dir
            client = sub_ctx.client
            proj = sub_ctx.proj_a
            admin_tok = sub_ctx.admin_token
            viewer_tok = sub_ctx.viewer_token

        pid = proj.id
        sid = ctx.unique_id("sess_er")
        s_del = ctx.unique_id("er_del")
        s_keep = ctx.unique_id("er_keep")
        ts = _recent_ts_ms()
        _assert_status(
            client.ingest_plst_batch(
                fixtures.make_batch(seed=s_del, project_id=pid, session_id=sid, start_ts_ms=ts), api_key=proj.pk_key
            ),
            200,
        )
        _assert_status(
            client.ingest_plst_batch(
                fixtures.make_batch(seed=s_keep, project_id=pid, session_id=sid, start_ts_ms=ts + 100),
                api_key=proj.pk_key,
            ),
            200,
        )

        _assert_status(
            client.request("DELETE", f"/v1/projects/{pid}/agent/traces/tr_{s_del}", token=admin_tok),
            (200, 204),
            "delete trace",
        )
        _assert_status(
            client.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s_del}", token=viewer_tok),
            404,
            "erased trace must 404",
        )
        listed = _assert_status(
            client.request("GET", f"/v1/projects/{pid}/agent/traces", token=viewer_tok, query={"session_id": sid}),
            200,
        ).json()
        assert [t["trace_id"] for t in listed["traces"]] == [f"tr_{s_keep}"]
        assert "agent_erasure_tombstones" in sqlite_tables(data_dir)
    finally:
        if inst is not None:
            ctx.node_manager.stop(inst)


@test_case(
    id="T1-F11-02",
    tier=1,
    feature="F11",
    title="Keyset cursor pagination across 7 traces returns disjoint 3 + 3 + 1 pages",
    tags=("query", "pagination", "cursor"),
)
def test_f11_02_keyset_cursor_pagination(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    sid = ctx.unique_id("sess_page")
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    base_ts = _recent_ts_ms()
    for i in range(7):
        tid = ctx.unique_id(f"tr_pg_{i}")
        sp = fixtures.make_span(
            ctx.unique_id(f"sp_pg_{i}"), tid, project_id=pid, span_type="llm", start_ts_ms=base_ts + i * 100
        )
        tr = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, start_ts_ms=base_ts + i * 100, spans=[sp]
        )
        traces.append(tr)
        spans.append(sp)

    _assert_status(ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=ctx.proj_a.pk_key), 200)

    p1 = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={"session_id": sid, "limit": 3},
        ),
        200,
    ).json()
    assert p1["total"] == 7, p1
    assert len(p1["traces"]) == 3 and p1["next_cursor"], p1

    p2 = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={"session_id": sid, "limit": 3, "cursor": p1["next_cursor"]},
        ),
        200,
    ).json()
    assert len(p2["traces"]) == 3 and p2["next_cursor"], p2

    p3 = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={"session_id": sid, "limit": 3, "cursor": p2["next_cursor"]},
        ),
        200,
    ).json()
    assert len(p3["traces"]) == 1, p3
    assert p3.get("next_cursor") is None, p3

    all_ids = [t["trace_id"] for page in (p1, p2, p3) for t in page["traces"]]
    assert len(all_ids) == 7
    assert set(all_ids) == {t["trace_id"] for t in traces}


@test_case(
    id="T1-F11-03",
    tier=1,
    feature="F11",
    title="Combined trace filter (status=error, min_cost_usd) and ascending duration_ms sort",
    tags=("query", "filter", "sort"),
)
def test_f11_03_combined_filter_and_sort(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    sid = ctx.unique_id("sess_flt")
    specs = [
        ("ok", 100, 10_000_000),      # excluded: status=ok
        ("error", 400, 100_000),      # excluded: cost $0.0001 < $0.005
        ("error", 500, 8_000_000),    # included #2: $0.008, dur 500
        ("error", 200, 6_000_000),    # included #1: $0.006, dur 200
    ]
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    expected_ordered: list[str] = []
    base_ts = _recent_ts_ms()
    for idx, (st, dur, cost) in enumerate(specs):
        tid = ctx.unique_id(f"tr_fs_{idx}")
        sp = fixtures.make_span(
            ctx.unique_id(f"sp_fs_{idx}"), tid, project_id=pid, span_type="llm", status=st,
            duration_ms=dur, cost_nanos=cost, start_ts_ms=base_ts + idx * 50,
        )
        tr = fixtures.make_trace(
            tid, project_id=pid, session_id=sid, status=st, duration_ms=dur,
            start_ts_ms=base_ts + idx * 50, spans=[sp],
        )
        traces.append(tr)
        spans.append(sp)
        if idx in (2, 3):
            expected_ordered.append(tid)

    # expected ascending duration_ms order: dur 200 (idx 3) then dur 500 (idx 2)
    expected_ids = [expected_ordered[1], expected_ordered[0]]
    _assert_status(ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=ctx.proj_a.pk_key), 200)

    resp = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={
                "session_id": sid,
                "status": "error",
                "min_cost_usd": 0.005,
                "sort_by": "duration_ms",
                "sort_dir": "asc",
            },
        ),
        200,
    ).json()
    assert [t["trace_id"] for t in resp["traces"]] == expected_ids, resp


@test_case(
    id="T1-F11-04",
    tier=1,
    feature="F11",
    title="Session-level erasure on temp project hides all session traces while keeping sibling session",
    tags=("erasure", "session", "user", "tombstone"),
)
def test_f11_04_session_level_erasure(ctx: TestContext) -> None:
    proj = _create_temp_project(
        ctx.client, ctx.owner_token, ctx.unique_id("proj_er_sess"), capture_user_id=True
    )
    sess_1 = ctx.unique_id("sess_del")
    sess_2 = ctx.unique_id("sess_keep")
    uid = ctx.unique_id("usr_er")
    s1a, s1b, s2a = ctx.unique_id("e1a"), ctx.unique_id("e1b"), ctx.unique_id("e2a")
    ts = _recent_ts_ms()

    for s, sid in ((s1a, sess_1), (s1b, sess_1), (s2a, sess_2)):
        _assert_status(
            ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s, project_id=proj.id, session_id=sid, user_id=uid, start_ts_ms=ts),
                api_key=proj.pk_key,
            ),
            200,
        )

    er_resp = _erase_session(ctx.client, proj.id, ctx.admin_token, sess_1, f"tr_{s1a}")
    _assert_status(er_resp, (200, 202, 204), "session erasure")

    _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/tr_{s1a}", token=ctx.viewer_token), 404
    )
    _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/tr_{s1b}", token=ctx.viewer_token), 404
    )
    _assert_status(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/tr_{s2a}", token=ctx.viewer_token), 200
    )


@test_case(
    id="T1-F11-05",
    tier=1,
    feature="F11",
    title="O(1) daily PLSS retention pruning removes expired segment/trace while keeping recent data",
    tags=("retention", "pruning", "plss"),
)
def test_f11_05_daily_plss_retention_pruning(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst, sub_ctx = _spawn_isolated_node(ctx)
    inst2: NodeInstance | None = None
    try:
        pid = sub_ctx.proj_a.id
        _assert_status(
            sub_ctx.client.request(
                "PUT",
                f"/v1/projects/{pid}/agent/privacy",
                token=sub_ctx.admin_token,
                json_body=fixtures.make_privacy_policy(project_id=pid, retention_days=7),
            ),
            200,
        )
        expired_ts = 1_577_836_800_000  # 2020-01-01T00:00:00Z
        recent_ts = int(time.time() * 1000) - 10_000

        s_old = ctx.unique_id("ret_old")
        s_new = ctx.unique_id("ret_new")
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s_old, project_id=pid, start_ts_ms=expired_ts),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )
        _assert_status(
            sub_ctx.client.ingest_plst_batch(
                fixtures.make_batch(seed=s_new, project_id=pid, start_ts_ms=recent_ts),
                api_key=sub_ctx.proj_a.pk_key,
            ),
            200,
        )

        _wait_for_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: pid in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        # Allow background retention maintenance tick (5s) + restart to ensure pruning runs
        time.sleep(6.0)
        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)

        _assert_status(
            c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s_new}", token=sub_ctx.viewer_token),
            200,
            "recent trace retained",
        )

        def _expired_pruned() -> bool:
            r = c2.request("GET", f"/v1/projects/{pid}/agent/traces/tr_{s_old}", token=sub_ctx.viewer_token)
            return r.status == 404

        _wait_until(_expired_pruned, timeout=12.0, desc="expired 2020 trace pruned")
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2)
        else:
            ctx.node_manager.stop(inst)


# ===========================================================================
# F12 — Reliability & Wire Efficiency (PLST vs OTLP/JSON)
# ===========================================================================


@test_case(
    id="T1-F12-01",
    tier=1,
    feature="F12",
    title="PLST dict frame is strictly smaller than PLST raw, native JSON, and OTLP/JSON",
    tags=("wire_efficiency", "plst", "otlp"),
)
def test_f12_01_plst_wire_efficiency_vs_json_and_otlp(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("wire_eff")
    batch = fixtures.make_repeated_prompt_batch(
        seed=seed, project_id=pid, repeats=6, prompt_bytes=1536, start_ts_ms=_recent_ts_ms()
    )
    dict_plst = plst.encode_plst_frame(batch, use_dict=True)
    raw_plst = plst.encode_plst_frame(batch, use_dict=False)
    json_bytes = json.dumps(batch, separators=(",", ":")).encode("utf-8")
    otlp_json_bytes = json.dumps(_build_otlp_equivalent_from_batch(batch), separators=(",", ":")).encode("utf-8")

    assert len(dict_plst) < len(raw_plst) < len(json_bytes), (
        len(dict_plst), len(raw_plst), len(json_bytes)
    )
    assert len(dict_plst) < len(otlp_json_bytes), (len(dict_plst), len(otlp_json_bytes))

    resp = _assert_status(ctx.client.ingest_plst(dict_plst, api_key=ctx.proj_a.pk_key), 200).json()
    assert resp["accepted_traces"] == 6, resp
    assert resp["accepted_spans"] == 6, resp

    first_tid = batch["traces"][0]["trace_id"]
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{first_tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["trace_id"] == first_tid, detail
    assert len(detail["spans"]) >= 1, detail


@test_case(
    id="T1-F12-02",
    tier=1,
    feature="F12",
    title="Oversized 80 KiB ASCII and multi-byte UTF-8 payloads truncate safely to <= 64 KiB",
    tags=("reliability", "truncation", "limits"),
)
def test_f12_02_oversized_payload_utf8_safe_truncation(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    ts = _recent_ts_ms()
    s_ascii = ctx.unique_id("big_ascii")
    b_ascii = fixtures.make_oversized_payload_batch(
        seed=s_ascii, project_id=pid, payload_bytes=80 * 1024, start_ts_ms=ts
    )

    s_utf8 = ctx.unique_id("big_utf8")
    emoji_text = "🚀" * 24_000  # 96,000 UTF-8 bytes
    sp_u = fixtures.make_span(
        ctx.unique_id("sp_u8"), f"tr_{s_utf8}", project_id=pid, span_type="llm", start_ts_ms=ts,
        input_payload={"prompt": emoji_text}, output_payload={"completion": emoji_text},
    )
    tr_u = fixtures.make_trace(
        f"tr_{s_utf8}", project_id=pid, session_id=f"sess_{s_utf8}", start_ts_ms=ts,
        input_payload={"prompt": emoji_text}, output_payload={"completion": emoji_text}, spans=[sp_u],
    )

    r1 = _assert_status(ctx.client.ingest_plst_batch(b_ascii, api_key=ctx.proj_a.pk_key), 200).json()
    assert r1["truncated_payloads"] >= 1, r1

    r2 = _assert_status(
        ctx.client.ingest_plst_batch({"traces": [tr_u], "spans": [sp_u]}, api_key=ctx.proj_a.pk_key), 200
    ).json()
    assert r2["truncated_payloads"] >= 1, r2

    for tid in (f"tr_{s_ascii}", f"tr_{s_utf8}"):
        detail = _assert_status(
            ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
        ).json()
        tr_in = detail["trace"]["input"]
        assert isinstance(tr_in, dict) and tr_in.get("_truncated") is True, tr_in
        assert int(tr_in["_original_bytes"]) > _MAX_TRUNCATED_BYTES, tr_in
        assert len(json.dumps(tr_in, ensure_ascii=False).encode("utf-8")) <= _MAX_TRUNCATED_BYTES


@test_case(
    id="T1-F12-03",
    tier=1,
    feature="F12",
    title="Out-of-order HTTP span_phase (final -> update -> start) preserves terminal final state",
    tags=("reliability", "span_phase", "crdt"),
)
def test_f12_03_out_of_order_span_phase_crdt(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    tid = ctx.unique_id("tr_ooo")
    sp_id = ctx.unique_id("sp_ooo")
    sid = ctx.unique_id("sess_ooo")

    _send_3phase_streaming(ctx.client, ctx.proj_a.pk_key, pid, tid, sp_id, sid, reverse=True, use_dict=True)

    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["status"] == "ok", detail["trace"]
    assert len(detail["spans"]) == 1, detail["spans"]
    sp = detail["spans"][0]
    assert sp["span_phase"] == "final", sp
    assert sp["status"] == "ok", sp
    assert sp["output_tokens"] == 50, sp
    assert sp["cost_nanos"] == 1_500_000, sp


@test_case(
    id="T1-F12-04",
    tier=1,
    feature="F12",
    title="Server-side tool-retry loop detection flags has_loop_warning=True even if wire flag False",
    tags=("reliability", "loop_detection"),
)
def test_f12_04_server_side_tool_retry_loop_detection(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    seed = ctx.unique_id("loop")
    batch = fixtures.make_tool_retry_loop_batch(seed=seed, project_id=pid, retry_count=5, start_ts_ms=_recent_ts_ms())
    batch["traces"][0]["has_loop_warning"] = False
    tid = batch["traces"][0]["trace_id"]

    _assert_status(ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key), 200)
    detail = _assert_status(
        ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces/{tid}", token=ctx.viewer_token), 200
    ).json()
    assert detail["trace"]["has_loop_warning"] is True, detail["trace"]

    listed = _assert_status(
        ctx.client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces",
            token=ctx.viewer_token,
            query={"has_loop_warning": True, "session_id": batch["traces"][0]["session_id"]},
        ),
        200,
    ).json()
    assert [t["trace_id"] for t in listed["traces"]] == [tid], listed


@test_case(
    id="T1-F12-05",
    tier=1,
    feature="F12",
    title="Hostile PLST frames (magic, version, sections, OOB indices, invalid UTF-8) return 4xx cleanly",
    tags=("reliability", "hostile", "plst"),
)
def test_f12_05_hostile_plst_frames_rejected_cleanly(ctx: TestContext) -> None:
    hostile_kinds = (
        "bad_magic",
        "bad_version",
        "truncated_sections",
        "oob_string_idx",
        "oob_blob_idx",
        "invalid_utf8",
    )
    for kind in hostile_kinds:
        frame = plst.build_hostile_frame(kind)
        resp = ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key)
        _assert_4xx(resp, f"hostile={kind}")

    _assert_status(ctx.client.get_healthz(), 200, "healthz after hostile frames")

    seed = ctx.unique_id("post_hostile")
    _assert_status(
        ctx.client.ingest_plst_batch(
            fixtures.make_batch(seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()),
            api_key=ctx.proj_a.pk_key,
        ),
        200,
        "valid ingest after hostile frames",
    )
