"""Tier 4 opaque-box E2E real-world application workflow scenarios (T4-01..T4-13).

Pure Python 3.14 stdlib + `scripts/e2e_agent/lib.*` only. Zero import-time network or
subprocess side effects. Every mutating test creates throwaway projects or spawns isolated
nodes via `ctx.node_manager` and cleans up in `try/finally` without mutating `ctx.base_url`.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

from lib import fixtures, plst
from lib.auth import AuthBootstrap, ProjectCreds, TestContext
from lib.client import HttpResponse, PulseClient
from lib.node_mgr import NodeInstance, list_agent_artifacts, sqlite_tables
from lib.registry import test_case

_T = TypeVar("_T")
_REPO_ROOT = Path(__file__).resolve().parents[3]

_AGENT_METRICS: tuple[str, ...] = (
    "pulse_agent_traces_ingested_total",
    "pulse_agent_spans_ingested_total",
    "pulse_agent_blobs_deduped_total",
    "pulse_agent_evals_executed_total",
    "pulse_agent_eval_queue_dropped_total",
)

_AGENT_MCP_TOOLS: tuple[str, ...] = (
    "list_agent_traces",
    "get_agent_trace",
    "query_agent_analytics",
    "list_agent_evaluations",
)

_FORBIDDEN_SQL_TABLES: tuple[str, ...] = ("agent_traces", "agent_spans")


# ---------------------------------------------------------------------------
# Shared Helpers (strictly invoked inside test functions)
# ---------------------------------------------------------------------------


def _recent_ts_ms(offset_ms: int = 15_000) -> int:
    """Return a recent UTC epoch millisecond timestamp well within 30-day retention."""
    return int(time.time() * 1000) - offset_ms


def _poll_until(
    predicate: Callable[[], _T | None],
    *,
    timeout: float = 15.0,
    interval: float = 0.2,
    desc: str = "condition",
) -> _T:
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            res = predicate()
            if res is not None and res is not False:
                return res
            last_exc = None
        except AssertionError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(interval)
    if last_exc is not None:
        raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}: {last_exc}")
    raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}")


def _extract_items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in keys:
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    raise AssertionError(f"Expected list or dict with one of {keys}, got {type(payload).__name__}: {payload!r}")


def _assert_no_nan_inf(obj: Any, path: str = "root") -> None:
    if isinstance(obj, float):
        assert math.isfinite(obj), f"Non-finite float {obj!r} at {path}"
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _assert_no_nan_inf(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            _assert_no_nan_inf(v, f"{path}[{idx}]")


def _create_project(
    client: PulseClient,
    token: str,
    name: str,
    *,
    capture_user_id: bool = False,
) -> ProjectCreds:
    proj = AuthBootstrap._create_project_with_keys(client, token=token, name=name)
    if capture_user_id:
        policy = fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True)
        client.request(
            "PUT",
            f"/v1/projects/{proj.id}/agent/privacy",
            json_body=policy,
            token=token,
        ).raise_for_status()
    return proj


def _wait_trace_visible(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 15.0,
) -> dict[str, Any]:
    def _check() -> dict[str, Any] | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{project_id}/agent/traces/{trace_id}",
            token=token,
        )
        if resp.status == 200:
            data = resp.json()
            if isinstance(data, dict) and isinstance(data.get("trace"), dict):
                return data
        return None

    return _poll_until(_check, timeout=timeout, interval=0.1, desc=f"trace {trace_id} visibility")


def _wait_trace_gone(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 15.0,
) -> None:
    def _check() -> bool | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{project_id}/agent/traces/{trace_id}",
            token=token,
        )
        return True if resp.status == 404 else None

    _poll_until(_check, timeout=timeout, interval=0.1, desc=f"trace {trace_id} erasure (404)")


def _wait_evals(
    client: PulseClient,
    project_id: str,
    token: str,
    *,
    query: dict[str, Any] | None = None,
    min_count: int = 1,
    timeout: float = 15.0,
) -> list[dict[str, Any]]:
    def _check() -> list[dict[str, Any]] | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{project_id}/agent/evaluations",
            query=query,
            token=token,
        )
        if resp.status != 200:
            return None
        items = _extract_items(resp.json(), "evaluations", "items")
        if len(items) >= min_count:
            return items
        return None

    return _poll_until(
        _check,
        timeout=timeout,
        interval=0.15,
        desc=f">={min_count} evaluations for query={query}",
    )


def _wait_artifacts(
    data_dir: Path,
    key: str,
    *,
    min_count: int = 1,
    predicate: Callable[[Path], bool] | None = None,
    timeout: float = 15.0,
) -> list[Path]:
    def _check() -> list[Path] | None:
        paths = list_agent_artifacts(data_dir)[key]
        if predicate is not None:
            paths = [p for p in paths if predicate(p)]
        if len(paths) >= min_count:
            return paths
        return None

    return _poll_until(
        _check,
        timeout=timeout,
        interval=0.25,
        desc=f">={min_count} {key} artifact(s) in {data_dir}",
    )


def _configure_byok_llm(
    client: PulseClient,
    project_id: str,
    token: str,
    endpoint_url: str,
    *,
    model: str = "gpt-4o-mini",
) -> HttpResponse:
    return client.request(
        "PUT",
        f"/v1/projects/{project_id}/byok",
        json_body={
            "category": "llm",
            "provider": "openai",
            "label": "E2E Mock LLM",
            "secret": "sk-e2e-mock-secret-12345678",
            "config_json": {"model": model, "endpoint": endpoint_url},
        },
        token=token,
    ).raise_for_status()


def _configure_byok_webhook(
    client: PulseClient,
    project_id: str,
    token: str,
    webhook_url: str,
    signing_secret: str,
) -> HttpResponse:
    assert len(signing_secret) >= 16, "signing_secret must be >= 16 characters"
    return client.request(
        "PUT",
        f"/v1/projects/{project_id}/byok",
        json_body={
            "category": "webhook",
            "provider": "webhook",
            "label": "E2E Mock Webhook",
            "secret": webhook_url,
            "config_json": {"signing_secret": signing_secret},
        },
        token=token,
    ).raise_for_status()


def _trigger_alert_eval(
    client: PulseClient,
    project_id: str,
    rule_id: str,
    token: str,
) -> HttpResponse:
    r = client.request(
        "POST",
        f"/v1/projects/{project_id}/agent/alerts/evaluate",
        token=token,
    )
    if r.status in (404, 405):
        r = client.request(
            "POST",
            f"/v1/projects/{project_id}/agent/alerts/{rule_id}/evaluate",
            token=token,
        )
    return r.raise_for_status()


def _erase_target(
    client: PulseClient,
    project_id: str,
    token: str,
    level: str,
    target_id: str,
    trace_id_hint: str | None = None,
) -> HttpResponse:
    """Invoke 3-level GDPR erasure via DELETE .../traces/:id or POST .../agent/erasure."""
    if level == "trace":
        resp = client.request(
            "DELETE",
            f"/v1/projects/{project_id}/agent/traces/{target_id}",
            token=token,
        )
        assert resp.status in (200, 202, 204), (
            f"Trace erasure failed: {resp.status} {resp.text(errors='replace')[:300]}"
        )
        return resp

    erasure_resp = client.request(
        "POST",
        f"/v1/projects/{project_id}/agent/erasure",
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    if erasure_resp.status not in (400, 404, 405, 422):
        assert erasure_resp.status in (200, 202, 204), (
            f"POST erasure ({level}) failed: {erasure_resp.status} {erasure_resp.text(errors='replace')[:300]}"
        )
        return erasure_resp

    hint = trace_id_hint or target_id
    del_resp = client.request(
        "DELETE",
        f"/v1/projects/{project_id}/agent/traces/{hint}",
        query={"level": level, "scope": level, "target_id": target_id},
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    assert del_resp.status in (200, 202, 204), (
        f"DELETE erasure ({level}) failed: {del_resp.status} {del_resp.text(errors='replace')[:300]}"
    )
    return del_resp


def _open_sqlite_ro(data_dir: Path | str | None) -> sqlite3.Connection:
    assert data_dir is not None, "data_dir is required to inspect pulse_meta.db"
    db_path = Path(data_dir) / "pulse_meta.db"
    assert db_path.is_file(), f"SQLite metadata DB missing at {db_path}"
    return sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5.0)


def _ingest_plst_retry_401(
    client: PulseClient,
    batch: dict[str, Any],
    *,
    api_key: str,
    use_dict: bool = True,
    timeout: float = 15.0,
) -> HttpResponse:
    deadline = time.monotonic() + timeout
    while True:
        resp = client.ingest_plst_batch(batch, api_key=api_key, use_dict=use_dict)
        if resp.status not in (401, 403) or time.monotonic() >= deadline:
            return resp.raise_for_status()
        time.sleep(0.25)


def _run_cmd(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        cwd=str(cwd or _REPO_ROOT),
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _resolve_ts_sdk_entry() -> Path:
    sdk_dir = _REPO_ROOT / "sdks" / "ts-agent"
    assert sdk_dir.is_dir(), f"Missing M5 TypeScript SDK directory: {sdk_dir}"
    for rel in ("dist/index.js", "dist/src/index.js", "src/index.ts", "index.ts"):
        cand = sdk_dir / rel
        if cand.is_file():
            return cand
    raise AssertionError(
        f"No TS SDK entrypoint found under {sdk_dir} (checked dist/index.js, src/index.ts)"
    )


# ===========================================================================
# T4-01 — Multi-Turn Customer Support Session & #/replays Linkability
# ===========================================================================


@test_case(
    id="T4-01",
    tier=4,
    feature="F21",
    title="Multi-turn customer support session across L0->PLSS with #/replays wireframe linkability",
    tags=("tier4", "F01", "F06", "F07", "F14", "F15", "F21", "session", "replay"),
)
def test_t4_01_support_session_and_replay_linkability(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst: NodeInstance | None = ctx.node_manager.start_all()
    try:
        c1 = PulseClient(inst.base_url)
        sub_ctx = AuthBootstrap.bootstrap_context(
            c1,
            inst.data_dir,
            inst.root_admin_token(),
            run_id=ctx.unique_id("t4_01"),
            node_manager=ctx.node_manager,
        )
        proj = _create_project(
            c1,
            sub_ctx.owner_token,
            ctx.unique_id("t4_01_proj"),
            capture_user_id=True,
        )

        sid = ctx.unique_id("sess_support")
        uid = ctx.unique_id("cust")
        t0 = _recent_ts_ms(60_000)

        # Turn 1: lookup_order -> ok (150 in + 60 out = 210 tokens)
        tr1_id = ctx.unique_id("tr_sup_1")
        t1_root = fixtures.make_span(
            ctx.unique_id("sp1_root"), tr1_id, project_id=proj.id, span_type="agent",
            name="support.turn1", start_ts_ms=t0, duration_ms=260,
        )
        t1_llm = fixtures.make_span(
            ctx.unique_id("sp1_llm"), tr1_id, project_id=proj.id, parent_span_id=t1_root["span_id"],
            depth=1, span_type="llm", name="llm.plan", start_ts_ms=t0 + 10, duration_ms=140,
            provider="openai", model="gpt-4o-mini", input_tokens=150, cached_tokens=0,
            output_tokens=60, cost_nanos=58_500,
        )
        t1_tool = fixtures.make_span(
            ctx.unique_id("sp1_tool"), tr1_id, project_id=proj.id, parent_span_id=t1_root["span_id"],
            linked_span_ids=[t1_llm["span_id"]], depth=1, span_type="tool", name="tool.lookup_order",
            tool_name="lookup_order", start_ts_ms=t0 + 160, duration_ms=90,
        )
        t1_spans = [t1_root, t1_llm, t1_tool]
        tr1 = fixtures.make_trace(
            tr1_id, project_id=proj.id, session_id=sid, user_id=uid, agent_name="support-agent",
            status="ok", start_ts_ms=t0, duration_ms=260, spans=t1_spans,
            input_payload={"message": "Where is my refund for order 1042?"},
            output_payload={"reply": "Checking order 1042 now."},
        )
        c1.ingest_plst_batch({"traces": [tr1], "spans": t1_spans}, api_key=proj.pk_key, use_dict=True).raise_for_status()

        # Turn 2: streaming start -> final, tool issue_refund fails with GatewayTimeout (180 in + 40 out = 220 tokens)
        tr2_id = ctx.unique_id("tr_sup_2")
        sp2_root_id = ctx.unique_id("sp2_root")
        sp2_llm_id = ctx.unique_id("sp2_llm")
        sp2_tool_id = ctx.unique_id("sp2_tool")

        sp2_llm_start = fixtures.make_span(
            sp2_llm_id, tr2_id, project_id=proj.id, parent_span_id=sp2_root_id, depth=1,
            span_type="llm", span_phase="start", status="running", name="llm.refund",
            start_ts_ms=t0 + 1010, duration_ms=20, provider="openai", model="gpt-4o-mini",
            input_tokens=180, cached_tokens=0, output_tokens=0, cost_nanos=0,
        )
        tr2_start = fixtures.make_trace(
            tr2_id, project_id=proj.id, session_id=sid, user_id=uid, agent_name="support-agent",
            status="running", start_ts_ms=t0 + 1000, duration_ms=30, spans=[sp2_llm_start],
        )
        c1.ingest_plst_batch({"traces": [tr2_start], "spans": [sp2_llm_start]}, api_key=proj.pk_key, use_dict=True).raise_for_status()

        sp2_root = fixtures.make_span(
            sp2_root_id, tr2_id, project_id=proj.id, span_type="agent", span_phase="final",
            status="error", name="support.turn2", start_ts_ms=t0 + 1000, duration_ms=350,
        )
        sp2_llm_final = fixtures.make_span(
            sp2_llm_id, tr2_id, project_id=proj.id, parent_span_id=sp2_root_id, depth=1,
            span_type="llm", span_phase="final", status="ok", name="llm.refund",
            start_ts_ms=t0 + 1010, duration_ms=120, provider="openai", model="gpt-4o-mini",
            input_tokens=180, cached_tokens=0, output_tokens=40, cost_nanos=51_000,
        )
        sp2_tool_err = fixtures.make_span(
            sp2_tool_id, tr2_id, project_id=proj.id, parent_span_id=sp2_root_id,
            linked_span_ids=[sp2_llm_id], depth=1, span_type="tool", span_phase="final",
            status="error", name="tool.issue_refund", tool_name="issue_refund",
            start_ts_ms=t0 + 1140, duration_ms=200,
            error_type="GatewayTimeout", error_message="Payment gateway timed out",
        )
        t2_spans = [sp2_root, sp2_llm_final, sp2_tool_err]
        tr2_final = fixtures.make_trace(
            tr2_id, project_id=proj.id, session_id=sid, user_id=uid, agent_name="support-agent",
            status="error", failure_source="tool", error_type="GatewayTimeout",
            start_ts_ms=t0 + 1000, duration_ms=350, spans=t2_spans,
            input_payload={"message": "Please issue the refund."},
            output_payload={"reply": "Refund gateway timed out; retrying."},
        )
        c1.ingest_plst_batch({"traces": [tr2_final], "spans": t2_spans}, api_key=proj.pk_key, use_dict=True).raise_for_status()

        # Turn 3: recovery -> ok (220 in + 80 out = 300 tokens)
        tr3_id = ctx.unique_id("tr_sup_3")
        t3_root = fixtures.make_span(
            ctx.unique_id("sp3_root"), tr3_id, project_id=proj.id, span_type="agent",
            name="support.turn3", start_ts_ms=t0 + 2000, duration_ms=220,
        )
        t3_llm = fixtures.make_span(
            ctx.unique_id("sp3_llm"), tr3_id, project_id=proj.id, parent_span_id=t3_root["span_id"],
            depth=1, span_type="llm", name="llm.confirm", start_ts_ms=t0 + 2010, duration_ms=180,
            provider="openai", model="gpt-4o-mini", input_tokens=220, cached_tokens=0,
            output_tokens=80, cost_nanos=81_000,
        )
        t3_spans = [t3_root, t3_llm]
        tr3 = fixtures.make_trace(
            tr3_id, project_id=proj.id, session_id=sid, user_id=uid, agent_name="support-agent",
            status="ok", start_ts_ms=t0 + 2000, duration_ms=220, spans=t3_spans,
            input_payload={"message": "Try again please."},
            output_payload={"reply": "Refund issued for order 1042."},
        )
        c1.ingest_plst_batch({"traces": [tr3], "spans": t3_spans}, api_key=proj.pk_key, use_dict=True).raise_for_status()

        # Correlated Pulse product event + wireframe replay chunk
        c1.request(
            "POST",
            "/v1/ingest",
            api_key=proj.pk_key,
            json_body={
                "events": [
                    {
                        "event_uuid": f"e2e-{ctx.unique_id('evt')}",
                        "routing_seed": f"dev_{uid}",
                        "distinct_id": uid,
                        "session_id": sid,
                        "event_name": "Support_Chat_Opened",
                        "client_event_ts_ms": t0,
                        "client_sent_ts_ms": t0 + 100,
                        "props": {"channel": "web"},
                    }
                ]
            },
        ).raise_for_status()

        c1.request(
            "POST",
            "/v1/replay/chunk",
            api_key=proj.pk_key,
            json_body={
                "project_id": proj.id,
                "session_id": sid,
                "chunk_start_ts_ms": t0,
                "ops": [
                    {
                        "op": "full_snapshot",
                        "ts_offset_ms": 0,
                        "width": 1280,
                        "height": 800,
                        "nodes": [
                            {
                                "node_id": 1,
                                "parent_id": 0,
                                "kind": "screen",
                                "x": 0,
                                "y": 0,
                                "w": 1280,
                                "h": 800,
                                "bg_rgba": 0xFFFFFFFF,
                                "masked_label": "Support Chat",
                            }
                        ],
                    }
                ],
            },
        ).raise_for_status()

        for tid in (tr1_id, tr2_id, tr3_id):
            _wait_trace_visible(c1, proj.id, tid, sub_ctx.viewer_token)

        _wait_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        # Cold restart to verify PLSS Tag 0x07 Session Rollups + Tag 0x04/0x05 hydration
        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst.base_url)

        sess_list = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=sub_ctx.viewer_token)
            .raise_for_status()
            .json(),
            "sessions",
            "items",
        )
        sess_row = next((s for s in sess_list if s["session_id"] == sid), None)
        assert sess_row is not None, f"Session {sid} missing from cold listing: {sess_list}"
        assert sess_row["trace_count"] == 3, sess_row
        assert sess_row["error_count"] == 1, sess_row
        assert sess_row["total_tokens"] == 210 + 220 + 300, sess_row
        assert sess_row["total_cost_nanos"] > 0, sess_row
        assert sess_row["first_ts_ms"] < sess_row["last_ts_ms"], sess_row

        sess_det = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions/{sid}", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert sess_det["summary"]["session_id"] == sid
        assert sess_det["summary"]["user_id"] == uid, sess_det["summary"]
        assert {t["trace_id"] for t in sess_det["traces"]} == {tr1_id, tr2_id, tr3_id}

        replay = c2.request(
            "GET", f"/v1/projects/{proj.id}/replay/{sid}", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert replay["session_id"] == sid
        assert len(replay["chunks"]) >= 1
        assert any(
            op.get("op") == "full_snapshot"
            for ch in replay["chunks"]
            for op in ch.get("ops", [])
        ), replay

        studio_info = c2.check_studio_static(require_agents_route=True)
        assert studio_info["agents_route_detected"] is True
        entry_js = "\n".join(
            c2.request("GET", asset).raise_for_status().text(errors="replace")
            for asset in studio_info["assets"]
        )
        replay_chunks = re.findall(r"SessionsReplayWorkspace[A-Za-z0-9_.-]*\.js", entry_js)
        assert replay_chunks, "SessionsReplayWorkspace chunk not referenced by Studio entry bundle"
        replay_js = c2.request("GET", f"/assets/{replay_chunks[0]}").raise_for_status().text(errors="replace")
        assert "session" in replay_js and ("URLSearchParams" in replay_js or "replays" in replay_js)

        tables = set(sqlite_tables(inst.data_dir))
        for forbidden in _FORBIDDEN_SQL_TABLES:
            assert forbidden not in tables, f"Legacy SQL table {forbidden!r} must not exist"
        assert list_agent_artifacts(inst.data_dir)["agent_segments"], "Expected PLSS segments on disk"
    finally:
        if inst is not None:
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


# ===========================================================================
# T4-02 — RAG Pipeline with Guardrail Block, Citation Retrieval & cost_nanos
# ===========================================================================


@test_case(
    id="T4-02",
    tier=4,
    feature="F02",
    title="RAG pipeline with embedding, retrieval, LLM, guardrail block, citation events & cost_nanos",
    tags=("tier4", "F01", "F02", "F05", "F14", "F15", "F17", "rag", "guardrail", "cost_nanos"),
)
def test_t4_02_rag_guardrail_citations_and_cost_nanos(ctx: TestContext) -> None:
    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_02_rag"))

    for kind, cfg in (
        ("no_errors", {}),
        ("cost_budget", {"max_usd": 0.05}),
    ):
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id(f"rag_{kind}"),
                kind=kind,
                target_type="trace",
                enabled=True,
                config=cfg,
            ),
            token=ctx.member_token,
        ).raise_for_status()

    ts = _recent_ts_ms(20_000)
    tr_id = ctx.unique_id("tr_rag")
    sp_root = fixtures.make_span(
        ctx.unique_id("sp_root"), tr_id, project_id=proj.id, span_type="agent",
        depth=0, parent_span_id=None, name="rag.pipeline", start_ts_ms=ts, duration_ms=320,
    )
    sp_embed = fixtures.make_span(
        ctx.unique_id("sp_embed"), tr_id, project_id=proj.id, span_type="embedding",
        parent_span_id=sp_root["span_id"], depth=1, name="embed.query",
        start_ts_ms=ts + 5, duration_ms=25, cost_nanos=50_000,
    )
    sp_retr = fixtures.make_span(
        ctx.unique_id("sp_retr"), tr_id, project_id=proj.id, span_type="retrieval",
        parent_span_id=sp_root["span_id"], linked_span_ids=[sp_embed["span_id"]], depth=1,
        name="vector.search", start_ts_ms=ts + 35, duration_ms=45,
        output_payload={"docs": [{"id": "doc_1", "score": 0.94}]},
        events=[
            fixtures.make_event(
                "retrieval.citation",
                timestamp_ms=ts + 70,
                attributes={"doc_id": "doc_1", "rank": 1},
            )
        ],
    )
    sp_llm = fixtures.make_span(
        ctx.unique_id("sp_llm"), tr_id, project_id=proj.id, span_type="llm",
        parent_span_id=sp_root["span_id"], linked_span_ids=[sp_retr["span_id"]], depth=1,
        name="llm.synthesize", start_ts_ms=ts + 85, duration_ms=180,
        provider="openai", model="gpt-4o-mini",
        input_tokens=10_000, cached_tokens=4_000, output_tokens=2_000,
        cost_nanos=0, cost_usd=0.0,
    )
    sp_guard = fixtures.make_span(
        ctx.unique_id("sp_guard"), tr_id, project_id=proj.id, span_type="guardrail",
        parent_span_id=sp_root["span_id"], linked_span_ids=[sp_llm["span_id"]], depth=1,
        span_phase="final", status="error", name="guardrail.policy",
        start_ts_ms=ts + 270, duration_ms=40,
        error_type="GuardrailBlocked", error_message="Policy violation detected",
        events=[
            fixtures.make_event(
                "guardrail.block",
                timestamp_ms=ts + 305,
                attributes={"policy": "jailbreak"},
            )
        ],
    )
    spans = [sp_root, sp_embed, sp_retr, sp_llm, sp_guard]
    tr = fixtures.make_trace(
        tr_id, project_id=proj.id, session_id=ctx.unique_id("sess_rag"),
        agent_name="rag-assistant", status="error", failure_source="application",
        error_type="GuardrailBlocked", error_message="Policy violation detected",
        start_ts_ms=ts, duration_ms=320, spans=spans,
    )

    ctx.client.ingest_plst_batch(
        {"traces": [tr], "spans": spans}, api_key=proj.pk_key, use_dict=True
    ).raise_for_status()

    detail = _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.viewer_token)
    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr_id}, min_count=2)

    hydrated_spans = {s["span_id"]: s for s in detail["spans"]}
    assert len(hydrated_spans) == 5
    assert {s["span_type"] for s in detail["spans"]} == {
        "agent", "embedding", "retrieval", "llm", "guardrail"
    }
    assert hydrated_spans[sp_retr["span_id"]]["linked_span_ids"] == [sp_embed["span_id"]]
    assert hydrated_spans[sp_llm["span_id"]]["linked_span_ids"] == [sp_retr["span_id"]]
    assert hydrated_spans[sp_guard["span_id"]]["linked_span_ids"] == [sp_llm["span_id"]]

    retr_events = [e["name"] for e in hydrated_spans[sp_retr["span_id"]].get("events", [])]
    guard_events = [e["name"] for e in hydrated_spans[sp_guard["span_id"]].get("events", [])]
    assert "retrieval.citation" in retr_events, hydrated_spans[sp_retr["span_id"]]
    assert "guardrail.block" in guard_events, hydrated_spans[sp_guard["span_id"]]

    # Exact integer nano-dollar cost: (10000-4000)*150 + 4000*75 + 2000*600 = 2_400_000 (+ 50_000 embed)
    assert hydrated_spans[sp_llm["span_id"]]["cost_nanos"] == 2_400_000, hydrated_spans[sp_llm["span_id"]]
    assert detail["trace"]["total_cost_nanos"] == 2_450_000, detail["trace"]

    by_kind = {e.get("evaluator_name", ""): e for e in evals}
    no_err_ev = next((e for e in evals if e["passed"] is False), None)
    cost_ev = next((e for e in evals if e["passed"] is True), None)
    assert no_err_ev is not None and abs(float(no_err_ev["score"]) - 0.0) < 1e-6, by_kind
    assert cost_ev is not None and abs(float(cost_ev["score"]) - 1.0) < 1e-6, by_kind

    listed = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"trace_id": tr_id},
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        "traces",
        "items",
    )
    row = next(t for t in listed if t["trace_id"] == tr_id)
    assert abs(float(row["avg_eval_score"]) - 0.5) < 1e-6, row
    assert abs(float(row["eval_pass_rate"]) - 0.5) < 1e-6, row


# ===========================================================================
# T4-03 — Multi-Agent Delegation (parent_trace_id + linked_span_ids DAG)
# ===========================================================================


@test_case(
    id="T4-03",
    tier=4,
    feature="F01",
    title="Multi-agent orchestrator-to-subagent delegation via parent_trace_id and linked_span_ids DAG",
    tags=("tier4", "F01", "F02", "F09", "F11", "F15", "multi-agent", "dag"),
)
def test_t4_03_multi_agent_delegation_dag(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst: NodeInstance | None = ctx.node_manager.start_all()
    try:
        c1 = PulseClient(inst.base_url)
        sub_ctx = AuthBootstrap.bootstrap_context(
            c1,
            inst.data_dir,
            inst.root_admin_token(),
            run_id=ctx.unique_id("t4_03"),
            node_manager=ctx.node_manager,
        )
        proj = _create_project(c1, sub_ctx.owner_token, ctx.unique_id("t4_03_dag"))

        sid = ctx.unique_id("sess_multi")
        wf_id = ctx.unique_id("wf_multi")
        ts = _recent_ts_ms(30_000)

        tr_orch_id = ctx.unique_id("tr_orch")
        sp_orch_root = fixtures.make_span(
            ctx.unique_id("sp_orch_root"), tr_orch_id, project_id=proj.id,
            span_type="agent", depth=0, name="orchestrator.run", start_ts_ms=ts, duration_ms=500,
        )
        sp_plan = fixtures.make_span(
            ctx.unique_id("sp_plan"), tr_orch_id, project_id=proj.id,
            parent_span_id=sp_orch_root["span_id"], depth=1, span_type="llm",
            name="orchestrator.plan", start_ts_ms=ts + 10, duration_ms=100,
        )
        sp_handoff = fixtures.make_span(
            ctx.unique_id("sp_handoff"), tr_orch_id, project_id=proj.id,
            parent_span_id=sp_orch_root["span_id"], linked_span_ids=[sp_plan["span_id"]],
            depth=1, span_type="workflow", name="orchestrator.delegate",
            start_ts_ms=ts + 120, duration_ms=30,
        )
        orch_spans = [sp_orch_root, sp_plan, sp_handoff]
        tr_orch = fixtures.make_trace(
            tr_orch_id, project_id=proj.id, agent_name="orchestrator",
            session_id=sid, workflow_id=wf_id, parent_trace_id=None,
            start_ts_ms=ts, duration_ms=500, spans=orch_spans,
        )

        tr_res_id = ctx.unique_id("tr_research")
        sp_res_root = fixtures.make_span(
            ctx.unique_id("sp_res_root"), tr_res_id, project_id=proj.id,
            linked_span_ids=[sp_handoff["span_id"]], span_type="agent", depth=0,
            name="researcher.run", start_ts_ms=ts + 160, duration_ms=150,
        )
        sp_search = fixtures.make_span(
            ctx.unique_id("sp_search"), tr_res_id, project_id=proj.id,
            parent_span_id=sp_res_root["span_id"], depth=1, span_type="retrieval",
            name="researcher.search", start_ts_ms=ts + 170, duration_ms=110,
        )
        res_spans = [sp_res_root, sp_search]
        tr_research = fixtures.make_trace(
            tr_res_id, project_id=proj.id, agent_name="researcher",
            session_id=sid, workflow_id=wf_id, parent_trace_id=tr_orch_id,
            start_ts_ms=ts + 160, duration_ms=150, spans=res_spans,
        )

        tr_wri_id = ctx.unique_id("tr_writer")
        sp_wri_root = fixtures.make_span(
            ctx.unique_id("sp_wri_root"), tr_wri_id, project_id=proj.id,
            linked_span_ids=[sp_handoff["span_id"], sp_res_root["span_id"]],
            span_type="agent", depth=0, name="writer.run", start_ts_ms=ts + 320, duration_ms=170,
        )
        sp_draft = fixtures.make_span(
            ctx.unique_id("sp_draft"), tr_wri_id, project_id=proj.id,
            parent_span_id=sp_wri_root["span_id"], depth=1, span_type="llm",
            name="writer.draft", start_ts_ms=ts + 330, duration_ms=130,
        )
        wri_spans = [sp_wri_root, sp_draft]
        tr_writer = fixtures.make_trace(
            tr_wri_id, project_id=proj.id, agent_name="writer",
            session_id=sid, workflow_id=wf_id, parent_trace_id=tr_orch_id,
            start_ts_ms=ts + 320, duration_ms=170, spans=wri_spans,
        )

        for tr_obj, sp_list in (
            (tr_orch, orch_spans),
            (tr_research, res_spans),
            (tr_writer, wri_spans),
        ):
            c1.ingest_plst_batch(
                {"traces": [tr_obj], "spans": sp_list}, api_key=proj.pk_key, use_dict=True
            ).raise_for_status()

        for tid in (tr_orch_id, tr_res_id, tr_wri_id):
            _wait_trace_visible(c1, proj.id, tid, sub_ctx.viewer_token)

        _wait_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst.base_url)

        children_resp = c2.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"parent_trace_id": tr_orch_id},
            token=sub_ctx.viewer_token,
        ).raise_for_status().json()
        child_ids = {t["trace_id"] for t in _extract_items(children_resp, "traces", "items")}
        assert child_ids == {tr_res_id, tr_wri_id}, f"Expected exact 2 child traces, got {child_ids}"

        writer_det = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces/{tr_wri_id}", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert writer_det["trace"]["parent_trace_id"] == tr_orch_id
        wri_root_out = next(s for s in writer_det["spans"] if s["span_id"] == sp_wri_root["span_id"])
        assert wri_root_out["linked_span_ids"] == [sp_handoff["span_id"], sp_res_root["span_id"]]

        sess_det = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions/{sid}", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert sess_det["summary"]["trace_count"] == 3, sess_det["summary"]
        assert {t["agent_name"] for t in sess_det["traces"]} == {
            "orchestrator", "researcher", "writer"
        }

        ana = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/analytics", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        by_agent = {row["key"]: row for row in ana.get("by_agent", []) if "key" in row}
        if not by_agent:
            by_agent = {row["agent_name"]: row for row in ana.get("by_agent", []) if "agent_name" in row}
        assert {"orchestrator", "researcher", "writer"}.issubset(set(by_agent.keys())), ana["by_agent"]
    finally:
        if inst is not None:
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


# ===========================================================================
# T4-04 — Tool-Retry Loop Detection -> Eval Worker -> Alert Rule -> HMAC Webhook
# ===========================================================================


@test_case(
    id="T4-04",
    tier=4,
    feature="F19",
    title="Tool-retry loop detection triggers no_loop eval failure, firing alert rule, HMAC webhook & recovery",
    tags=("tier4", "F12", "F17", "F18", "F19", "loop-detection", "alerts", "webhook"),
)
def test_t4_04_loop_detection_eval_alert_hmac_recovery(ctx: TestContext) -> None:
    assert ctx.mock_webhook is not None and ctx.mock_webhook_url is not None
    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_04_loop"))
    signing_secret = "t4_loop_webhook_secret_32bytes!"
    _configure_byok_webhook(
        ctx.client, proj.id, ctx.admin_token, ctx.mock_webhook_url, signing_secret
    )

    ctx.mock_webhook.signing_secret = signing_secret
    try:
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id("no_loop"),
                kind="no_loop",
                target_type="trace",
                enabled=True,
            ),
            token=ctx.member_token,
        ).raise_for_status()

        rule = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/alerts",
            json_body=fixtures.make_alert_rule(
                name=ctx.unique_id("loop_rate_alert"),
                metric="loop_rate",
                operator="gt",
                threshold=0.2,
                window_minutes=60,
                cooldown_minutes=0,
                enabled=True,
                webhook_url=ctx.mock_webhook_url,
            ),
            token=ctx.member_token,
        ).raise_for_status().json()
        rule_id = rule["id"]

        ts = _recent_ts_ms(10_000)
        loop_batch = fixtures.make_tool_retry_loop_batch(
            seed=ctx.unique_id("loop_tr"),
            project_id=proj.id,
            retry_count=5,
            start_ts_ms=ts,
        )
        # Clear wire flag so server-side detect_execution_loop sets it
        loop_batch["traces"][0]["has_loop_warning"] = False
        loop_tr_id = loop_batch["traces"][0]["trace_id"]

        ctx.client.ingest_plst_batch(loop_batch, api_key=proj.pk_key, use_dict=True).raise_for_status()
        loop_det = _wait_trace_visible(ctx.client, proj.id, loop_tr_id, ctx.viewer_token)
        assert loop_det["trace"]["has_loop_warning"] is True, loop_det["trace"]

        evals = _wait_evals(
            ctx.client, proj.id, ctx.viewer_token, query={"trace_id": loop_tr_id}, min_count=1
        )
        loop_ev = evals[0]
        assert loop_ev["passed"] is False and abs(float(loop_ev["score"]) - 0.0) < 1e-6, loop_ev

        def _is_firing() -> bool | None:
            _trigger_alert_eval(ctx.client, proj.id, rule_id, ctx.member_token)
            rules = _extract_items(
                ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token)
                .raise_for_status()
                .json(),
                "rules",
                "alerts",
                "items",
            )
            cur = next((r for r in rules if r["id"] == rule_id), None)
            if cur is not None and cur.get("state") == "firing":
                return True
            return None

        _poll_until(_is_firing, timeout=15.0, interval=0.3, desc="loop_rate alert rule firing")
        hooks_1 = ctx.mock_webhook.wait_for_count(1, timeout=10.0)
        assert hooks_1[0]["signature_valid"] is True, hooks_1[0]

        # Ingest 5 healthy traces -> loop_rate = 1/6 = 0.167 < 0.2 -> alert resolves
        for i in range(5):
            ok_batch = fixtures.make_batch(
                seed=ctx.unique_id(f"ok_dilute_{i}"),
                project_id=proj.id,
                start_ts_ms=ts + 100 + i * 20,
            )
            ctx.client.ingest_plst_batch(ok_batch, api_key=proj.pk_key).raise_for_status()
            _wait_trace_visible(ctx.client, proj.id, ok_batch["traces"][0]["trace_id"], ctx.viewer_token)

        def _is_resolved() -> list[dict[str, Any]] | None:
            _trigger_alert_eval(ctx.client, proj.id, rule_id, ctx.member_token)
            evs = _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.viewer_token
                )
                .raise_for_status()
                .json(),
                "events",
                "items",
            )
            matching = [e for e in evs if e.get("rule_id") == rule_id]
            types = {e.get("event_type") for e in matching}
            if {"triggered", "resolved"}.issubset(types):
                return matching
            return None

        events = _poll_until(_is_resolved, timeout=15.0, interval=0.3, desc="alert triggered + resolved events")
        hooks_2 = ctx.mock_webhook.wait_for_count(2, timeout=10.0)
        assert all(h["signature_valid"] is True for h in hooks_2[:2]), hooks_2
        assert all(e.get("delivered") is True for e in events), events
    finally:
        ctx.mock_webhook.signing_secret = None


# ===========================================================================
# T4-05 — Experiment Regression Gate (promote -> datasets -> Welch t-test)
# ===========================================================================


@test_case(
    id="T4-05",
    tier=4,
    feature="F18",
    title="Production trace promotion to dataset, baseline vs candidate experiment runs, and Welch t-test regression gate",
    tags=("tier4", "F15", "F17", "F18", "datasets", "experiments", "welch-t-test"),
)
def test_t4_05_experiment_regression_gate_welch(ctx: TestContext) -> None:
    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_05_exp"))
    agent_name = ctx.unique_id("reg_agent")

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("exp_noerr"), kind="no_errors", enabled=True
        ),
        token=ctx.member_token,
    ).raise_for_status()

    ts = _recent_ts_ms(120_000)
    tr_prod_id = ctx.unique_id("tr_prod")
    sp_prod = fixtures.make_span(
        ctx.unique_id("sp_prod"), tr_prod_id, project_id=proj.id, span_type="llm",
        start_ts_ms=ts, duration_ms=120,
    )
    tr_prod = fixtures.make_trace(
        tr_prod_id, project_id=proj.id, agent_name=agent_name, agent_version="1.0.0",
        start_ts_ms=ts, duration_ms=120,
        input_payload={"message": "Cancel subscription"},
        output_payload={"reply": "Subscription canceled."},
        spans=[sp_prod],
    )
    ctx.client.ingest_plst_batch({"traces": [tr_prod], "spans": [sp_prod]}, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, tr_prod_id, ctx.viewer_token)

    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("golden_ds")),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/traces/{tr_prod_id}/promote",
        json_body={"dataset_id": ds_id},
        token=ctx.member_token,
    ).raise_for_status()

    bulk_items = [fixtures.make_dataset_item(name=f"case-{i:02d}") for i in range(19)]
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets/{ds_id}/import",
        json_body={"items": bulk_items},
        token=ctx.member_token,
    ).raise_for_status()

    ds_items = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/datasets/{ds_id}/items", token=ctx.viewer_token
        )
        .raise_for_status()
        .json(),
        "items",
        "dataset_items",
    )
    assert len(ds_items) == 20, f"Expected 20 dataset items, got {len(ds_items)}"
    promoted_item = next((it for it in ds_items if it.get("source_trace_id") == tr_prod_id), None)
    assert promoted_item is not None, ds_items
    assert promoted_item["input"] == {"message": "Cancel subscription"}

    exp_base = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/experiments",
        json_body=fixtures.make_experiment(
            dataset_id=ds_id,
            name=ctx.unique_id("exp_base"),
            agent_name=agent_name,
            agent_version="1.0.0",
            prompt_version="p_v1",
        ),
        token=ctx.member_token,
    ).raise_for_status().json()
    exp_cand = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/experiments",
        json_body=fixtures.make_experiment(
            dataset_id=ds_id,
            name=ctx.unique_id("exp_cand"),
            agent_name=agent_name,
            agent_version="2.0.0",
            prompt_version="p_v2",
        ),
        token=ctx.member_token,
    ).raise_for_status().json()
    base_id = exp_base["id"]
    cand_id = exp_cand["id"]

    def _submit_runs(start_idx: int, end_idx: int) -> None:
        for i in range(start_idx, end_idx):
            item_id = ds_items[i]["id"]
            ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/experiments/{base_id}/runs",
                json_body=fixtures.make_experiment_run(
                    dataset_item_id=item_id,
                    latency_ms=115 + (i % 4) * 5,
                    cost_usd=0.002 + (i % 3) * 0.0002,
                    avg_score=0.90 + (i % 4) * 0.01,
                    passed=True,
                ),
                token=ctx.member_token,
            ).raise_for_status()
            ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/experiments/{cand_id}/runs",
                json_body=fixtures.make_experiment_run(
                    dataset_item_id=item_id,
                    latency_ms=470 + (i % 4) * 8,
                    cost_usd=0.008 + (i % 3) * 0.0004,
                    avg_score=0.52 + (i % 4) * 0.01,
                    passed=False,
                ),
                token=ctx.member_token,
            ).raise_for_status()

    # Phase 1: n=10 < MIN_SIGNIFICANCE_SAMPLES (20) -> insufficient_data
    _submit_runs(0, 10)
    cmp_10 = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/experiments/compare",
        query={"baseline": base_id, "candidate": cand_id},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp_10)
    score_10 = cmp_10.get("score") or cmp_10.get("avg_score") or cmp_10
    assert score_10["verdict"] == "insufficient_data", cmp_10
    assert score_10.get("p_value") is None, cmp_10

    # Phase 2: n=20 >= 20 -> Welch t-test detects significant_regression
    _submit_runs(10, 20)
    cmp_20 = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/experiments/compare",
        query={"baseline": base_id, "candidate": cand_id},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp_20)
    score_20 = cmp_20.get("score") or cmp_20.get("avg_score") or cmp_20
    assert score_20["sample_size_baseline"] == 20, cmp_20
    assert score_20["sample_size_candidate"] == 20, cmp_20
    assert score_20["verdict"] == "significant_regression", cmp_20
    assert score_20["p_value"] is not None and float(score_20["p_value"]) < 0.05, cmp_20

    # Live version compare (n=20 per version with deterministic non-zero variance)
    live_traces: list[dict[str, Any]] = []
    live_spans: list[dict[str, Any]] = []
    for ver, base_dur, cost_n in (("1.0.0", 120, 1_000_000), ("2.0.0", 480, 4_000_000)):
        for i in range(20):
            tid = ctx.unique_id(f"live_{ver}_{i}")
            dur = base_dur + (i % 5) * 4
            sp = fixtures.make_span(
                ctx.unique_id(f"spl_{ver}_{i}"), tid, project_id=proj.id,
                start_ts_ms=ts + 1000 + i * 20, duration_ms=dur, cost_nanos=cost_n,
            )
            tr = fixtures.make_trace(
                tid, project_id=proj.id, agent_name=agent_name, agent_version=ver,
                start_ts_ms=ts + 1000 + i * 20, duration_ms=dur, spans=[sp],
            )
            live_traces.append(tr)
            live_spans.append(sp)

    ctx.client.ingest_plst_batch(
        {"traces": live_traces, "spans": live_spans}, api_key=proj.pk_key
    ).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, live_traces[-1]["trace_id"], ctx.viewer_token)

    live_cmp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={
            "agent_name": agent_name,
            "baseline_version": "1.0.0",
            "candidate_version": "2.0.0",
        },
        token=ctx.viewer_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(live_cmp)
    lat_delta = live_cmp.get("latency_ms") or live_cmp.get("latency") or live_cmp.get("duration_ms") or live_cmp
    assert lat_delta["sample_size_baseline"] >= 20, live_cmp
    assert lat_delta["sample_size_candidate"] >= 20, live_cmp
    assert lat_delta["verdict"] == "significant_regression", live_cmp
    assert lat_delta["p_value"] is not None and float(lat_delta["p_value"]) < 0.05, live_cmp


# ===========================================================================
# T4-06 — Split-Node Topology (--role ingest + --role query + --role compactor)
# ===========================================================================


@test_case(
    id="T4-06",
    tier=4,
    feature="F20",
    title="Split-role cluster (compactor + ingest + query) shared object store, compaction, and GC grace safety",
    tags=("tier4", "F06", "F07", "F08", "F10", "F11", "F20", "split-role", "cluster"),
)
def test_t4_06_split_node_topology_compaction_and_gc_grace(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    cluster = ctx.node_manager.start_split_cluster()
    try:
        comp_c = PulseClient(cluster["compactor"].base_url)
        ing_c = PulseClient(cluster["ingest"].base_url)
        qry_c = PulseClient(cluster["query"].base_url)
        root_tok = cluster["query"].root_admin_token()

        proj = _create_project(
            qry_c, root_tok, ctx.unique_id("t4_06_split"), capture_user_id=True
        )

        # Strict role boundary checks
        assert ing_c.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok).status == 404
        assert ing_c.mcp_call("tools/list", {}, token=root_tok, project_id=proj.id).status == 404
        assert qry_c.ingest_json(
            fixtures.make_batch(seed=ctx.unique_id("q_blk"), project_id=proj.id, start_ts_ms=_recent_ts_ms()),
            api_key=proj.pk_key,
        ).status == 404
        assert comp_c.ingest_json(
            fixtures.make_batch(seed=ctx.unique_id("c_blk"), project_id=proj.id, start_ts_ms=_recent_ts_ms()),
            api_key=proj.pk_key,
        ).status == 404
        assert comp_c.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok).status == 404
        assert comp_c.get_healthz().status == 200
        assert comp_c.get_readyz().status == 200
        assert comp_c.get_metrics().status == 200

        ts = _recent_ts_ms(25_000)
        b_keep = fixtures.make_batch(
            seed=ctx.unique_id("sp_keep"), project_id=proj.id, session_id=ctx.unique_id("s_keep"),
            user_id=ctx.unique_id("u_keep"), start_ts_ms=ts,
        )
        b_del = fixtures.make_batch(
            seed=ctx.unique_id("sp_del"), project_id=proj.id, session_id=ctx.unique_id("s_del"),
            user_id=ctx.unique_id("u_del"), start_ts_ms=ts + 500,
        )
        tr_keep_id = b_keep["traces"][0]["trace_id"]
        tr_del_id = b_del["traces"][0]["trace_id"]

        # Retry brief 401 on ingest until reload_caches_from_db() syncs new project API key
        _ingest_plst_retry_401(ing_c, b_keep, api_key=proj.pk_key, use_dict=True)
        ing_c.ingest_json(b_del, api_key=proj.pk_key).raise_for_status()

        # Near-real-time visibility via WAL/L0 on query node
        _wait_trace_visible(qry_c, proj.id, tr_keep_id, root_tok, timeout=15.0)
        _wait_trace_visible(qry_c, proj.id, tr_del_id, root_tok, timeout=15.0)

        # Wait for ingest node to flush L0 -> PLSS segments + MVCC manifest
        shared_dir = cluster["ingest"].data_dir
        segs_before = _wait_artifacts(
            shared_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )
        _wait_artifacts(
            shared_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".json"),
            timeout=15.0,
        )

        # Erase tr_del on query node, wait past compactor tick (>5s), verify GC grace protects live readers
        qry_c.request(
            "DELETE", f"/v1/projects/{proj.id}/agent/traces/{tr_del_id}", token=root_tok
        ).raise_for_status()
        _wait_trace_gone(qry_c, proj.id, tr_del_id, root_tok)

        time.sleep(6.0)
        kept = qry_c.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces/{tr_keep_id}", token=root_tok
        ).raise_for_status().json()
        assert kept["trace"]["trace_id"] == tr_keep_id
        assert len(kept["spans"]) == 3
        # Ensure GC grace period (900s) did not delete segments still referenced or recently superseded
        assert any(p.exists() for p in segs_before) or list_agent_artifacts(shared_dir)["agent_segments"]
    finally:
        for inst in cluster.values():
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


# ===========================================================================
# T4-07 — Privacy Redaction + BYOK Pricing Override + End-to-End PLST Blob Dedup
# ===========================================================================


@test_case(
    id="T4-07",
    tier=4,
    feature="F05",
    title="Privacy policy PII/key redaction, custom model pricing override, and PLST xxh3_64 blob dedup",
    tags=("tier4", "F04", "F05", "F14", "F19", "privacy", "pricing", "plst", "dedup"),
)
def test_t4_07_privacy_pricing_override_and_plst_blob_dedup(ctx: TestContext) -> None:
    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_07_priv"))

    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(
            project_id=proj.id,
            capture_inputs=True,
            capture_outputs=True,
            capture_user_id=False,
            redact_pii=True,
            redact_keys=["internal_secret", "session_cookie"],
            exclude_attributes=["debug.raw_headers"],
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="anthropic",
            model_pattern="claude-custom-",
            input_per_1m_usd=4.0,
            output_per_1m_usd=12.0,
            cached_input_per_1m_usd=1.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    shared_system_prompt = (
        "You are a compliance assistant. Contact alice@example.com or use key "
        "sk-proj-secret1234567890abcdef and SSN 123-45-6789. "
        + ("POLICY_BLOCK " * 320)
    )
    ts = _recent_ts_ms(20_000)
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for i in range(6):
        tid = ctx.unique_id(f"tr_dedup_{i}")
        sp = fixtures.make_span(
            ctx.unique_id(f"sp_dedup_{i}"),
            tid,
            project_id=proj.id,
            span_type="llm",
            provider="anthropic",
            model="claude-custom-sonnet",
            start_ts_ms=ts + i * 50,
            duration_ms=100,
            input_tokens=1000,
            cached_tokens=200,
            output_tokens=500,
            cost_nanos=0,
            cost_usd=0.0,
            input_payload={
                "system": shared_system_prompt,
                "internal_secret": "top-secret-val",
                "turn": i,
            },
            output_payload={"reply": f"Safe response {i}"},
            attributes={"debug.raw_headers": "Authorization: Bearer xyz", "turn": i},
        )
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            session_id=ctx.unique_id("sess_dedup"),
            user_id=ctx.unique_id("strip_user"),
            start_ts_ms=ts + i * 50,
            duration_ms=100,
            input_payload={
                "system": shared_system_prompt,
                "internal_secret": "top-secret-val",
            },
            output_payload={"reply": f"Safe response {i}"},
            spans=[sp],
        )
        traces.append(tr)
        spans.append(sp)

    dedup_stats = plst.BlobDedupStats()
    frame = plst.encode_plst_frame(
        {"traces": traces, "spans": spans},
        use_dict=True,
        stats_out=dedup_stats,
    )
    assert dedup_stats.deduped_blobs > 0, f"Expected PLST blob deduplication, got {dedup_stats}"

    ing = ctx.client.ingest_plst(frame, api_key=proj.pk_key).raise_for_status().json()
    assert ing["accepted_traces"] == 6, ing
    assert ing["redacted_fields"] >= 6, ing

    first_det = _wait_trace_visible(ctx.client, proj.id, traces[0]["trace_id"], ctx.viewer_token)
    last_det = _wait_trace_visible(ctx.client, proj.id, traces[-1]["trace_id"], ctx.viewer_token)

    for det in (first_det, last_det):
        assert det["trace"].get("user_id") is None, det["trace"]
        serialized = json.dumps(det)
        assert "[REDACTED]" in serialized
        for secret in (
            "alice@example.com",
            "sk-proj-secret1234567890abcdef",
            "123-45-6789",
            "top-secret-val",
            "Authorization: Bearer xyz",
        ):
            assert secret not in serialized, f"Secret leaked in trace detail: {secret}"
        sp_out = det["spans"][0]
        assert "debug.raw_headers" not in sp_out.get("attributes", {}), sp_out
        # Exact override: (1000-200)*4000 + 200*1000 + 500*12000 = 9_400_000 nanos
        assert sp_out["cost_nanos"] == 9_400_000, sp_out
        assert det["trace"]["total_cost_nanos"] == 9_400_000, det["trace"]

    ana = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token
    ).raise_for_status().json()
    assert abs(float(ana["total_cost_usd"]) - 0.0564) < 1e-6, ana


# ===========================================================================
# T4-08 — 3-Level GDPR Erasure (Trace, Session, User) Across L0, PLSS & SQL
# ===========================================================================


@test_case(
    id="T4-08",
    tier=4,
    feature="F11",
    title="3-level GDPR erasure (trace, session, user) across hot L0, cold PLSS, SQL rollups & restart",
    tags=("tier4", "F11", "F13", "F15", "erasure", "gdpr", "tombstones"),
)
def test_t4_08_gdpr_three_level_erasure(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst: NodeInstance | None = ctx.node_manager.start_all()
    try:
        c1 = PulseClient(inst.base_url)
        sub_ctx = AuthBootstrap.bootstrap_context(
            c1,
            inst.data_dir,
            inst.root_admin_token(),
            run_id=ctx.unique_id("t4_08"),
            node_manager=ctx.node_manager,
        )
        proj = _create_project(
            c1, sub_ctx.owner_token, ctx.unique_id("t4_08_gdpr"), capture_user_id=True
        )

        ts = _recent_ts_ms(30_000)

        def _make_one(label: str, sid: str, uid: str, offset: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            b = fixtures.make_batch(
                seed=ctx.unique_id(label), project_id=proj.id, session_id=sid,
                user_id=uid, start_ts_ms=ts + offset,
            )
            return b, b["traces"] + b["spans"]

        # Group A: cold PLSS trace erasure
        sess_a = ctx.unique_id("sess_a")
        user_a = ctx.unique_id("user_a")
        b_cold, _ = _make_one("cold", sess_a, user_a, 0)
        tr_cold = b_cold["traces"][0]["trace_id"]
        c1.ingest_plst_batch(b_cold, api_key=proj.pk_key, use_dict=True).raise_for_status()
        _wait_trace_visible(c1, proj.id, tr_cold, sub_ctx.viewer_token)
        _wait_artifacts(
            inst.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
            timeout=15.0,
        )

        # Group B: hot L0 session erasure (2 traces in sess_erase)
        sess_erase = ctx.unique_id("sess_erase")
        user_b = ctx.unique_id("user_b")
        b_s1, _ = _make_one("s1", sess_erase, user_b, 100)
        b_s2, _ = _make_one("s2", sess_erase, user_b, 200)
        tr_s1, tr_s2 = b_s1["traces"][0]["trace_id"], b_s2["traces"][0]["trace_id"]

        # Group C: user erasure (2 traces across 2 sessions for user_erase)
        user_erase = ctx.unique_id("user_erase")
        sess_u1, sess_u2 = ctx.unique_id("su1"), ctx.unique_id("su2")
        b_u1, _ = _make_one("u1", sess_u1, user_erase, 300)
        b_u2, _ = _make_one("u2", sess_u2, user_erase, 400)
        tr_u1, tr_u2 = b_u1["traces"][0]["trace_id"], b_u2["traces"][0]["trace_id"]

        # Group D: survivor
        sess_keep = ctx.unique_id("sess_keep")
        user_keep = ctx.unique_id("user_keep")
        b_keep, _ = _make_one("keep", sess_keep, user_keep, 500)
        tr_keep = b_keep["traces"][0]["trace_id"]

        for b in (b_s1, b_s2, b_u1, b_u2, b_keep):
            c1.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()

        for tid in (tr_s1, tr_s2, tr_u1, tr_u2, tr_keep):
            _wait_trace_visible(c1, proj.id, tid, sub_ctx.viewer_token)

        # Attach manual evaluations so agent_evaluations & agent_eval_rollups are populated
        for sid_ev, tid_ev in (
            (sess_a, tr_cold),
            (sess_erase, tr_s1),
            (sess_u1, tr_u1),
            (sess_keep, tr_keep),
        ):
            c1.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/evaluations",
                json_body=fixtures.make_manual_evaluation(
                    target_type="trace",
                    session_id=sid_ev,
                    trace_id=tid_ev,
                    evaluator_id=ctx.unique_id("ev_gdpr"),
                    score=0.85,
                    passed=True,
                ),
                token=sub_ctx.member_token,
            ).raise_for_status()

        assert _wait_trace_visible(c1, proj.id, tr_u1, sub_ctx.viewer_token)["trace"]["user_id"] == user_erase

        # Execute 3-level erasure (Admin)
        _erase_target(c1, proj.id, sub_ctx.admin_token, "trace", tr_cold)
        _erase_target(c1, proj.id, sub_ctx.admin_token, "session", sess_erase, trace_id_hint=tr_s1)
        _erase_target(c1, proj.id, sub_ctx.admin_token, "user", user_erase, trace_id_hint=tr_u1)

        erased_traces = (tr_cold, tr_s1, tr_s2, tr_u1, tr_u2)
        for tid in erased_traces:
            _wait_trace_gone(c1, proj.id, tid, sub_ctx.viewer_token)

        conn = _open_sqlite_ro(inst.data_dir)
        try:
            rows = conn.execute(
                "SELECT level, target_id FROM agent_erasure_tombstones WHERE project_id = ?",
                (proj.id,),
            ).fetchall()
        finally:
            conn.close()
        tombstones = {(str(r[0]).lower(), str(r[1])) for r in rows}
        assert ("trace", tr_cold) in tombstones, tombstones
        assert ("session", sess_erase) in tombstones, tombstones
        assert ("user", user_erase) in tombstones, tombstones

        tables = set(sqlite_tables(inst.data_dir))
        for forbidden in _FORBIDDEN_SQL_TABLES:
            assert forbidden not in tables

        # Cold restart and re-verify
        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst.base_url)

        for tid in erased_traces:
            assert c2.request(
                "GET", f"/v1/projects/{proj.id}/agent/traces/{tid}", token=sub_ctx.viewer_token
            ).status == 404
        assert c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_erase}", token=sub_ctx.viewer_token
        ).status == 404

        listed_traces = {
            t["trace_id"]
            for t in _extract_items(
                c2.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=sub_ctx.viewer_token)
                .raise_for_status()
                .json(),
                "traces",
                "items",
            )
        }
        assert listed_traces == {tr_keep}, f"Expected only survivor {tr_keep}, got {listed_traces}"

        listed_sessions = {
            s["session_id"]
            for s in _extract_items(
                c2.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=sub_ctx.viewer_token)
                .raise_for_status()
                .json(),
                "sessions",
                "items",
            )
        }
        assert sess_keep in listed_sessions
        assert listed_sessions.isdisjoint({sess_a, sess_erase, sess_u1, sess_u2}), listed_sessions

        ana = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/analytics", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert ana["total_executions"] == 1, ana

        keep_det = c2.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces/{tr_keep}", token=sub_ctx.viewer_token
        ).raise_for_status().json()
        assert keep_det["trace"]["user_id"] == user_keep
        assert len(keep_det["spans"]) == 3
        assert len(keep_det["evaluations"]) >= 1
    finally:
        if inst is not None:
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


# ===========================================================================
# T4-09 — Crash Recovery & Zero-Data-Loss Graceful Shutdown (PTWL + drain)
# ===========================================================================


@test_case(
    id="T4-09",
    tier=4,
    feature="F06",
    title="PTWL WAL replay after SIGKILL and zero-loss L0 drain on graceful SIGTERM",
    tags=("tier4", "F06", "F07", "F10", "F20", "crash-recovery", "sigkill", "sigterm"),
)
def test_t4_09_crash_recovery_and_graceful_drain(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst1: NodeInstance | None = ctx.node_manager.start_all(drain_ms=250)
    inst2: NodeInstance | None = None
    inst3: NodeInstance | None = None
    active_inst: NodeInstance | None = inst1
    try:
        c1 = PulseClient(inst1.base_url)
        root_tok = inst1.root_admin_token()
        proj = _create_project(c1, root_tok, ctx.unique_id("t4_09_crash"))
        data_dir = inst1.data_dir

        ts = _recent_ts_ms(20_000)
        kill_ids: list[str] = []
        for i in range(4):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"kill_{i}"), project_id=proj.id, start_ts_ms=ts + i * 20
            )
            kill_ids.append(b["traces"][0]["trace_id"])
            c1.ingest_plst_batch(b, api_key=proj.pk_key, use_dict=True).raise_for_status()

        _wait_artifacts(
            data_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        # Crash immediately via SIGKILL inside L0 flush window
        ctx.node_manager.kill(inst1)
        active_inst = None

        inst2 = ctx.node_manager.start_all(data_dir=data_dir, admin_token=root_tok, drain_ms=250)
        active_inst = inst2
        c2 = PulseClient(inst2.base_url)
        for tid in kill_ids:
            det = _wait_trace_visible(c2, proj.id, tid, root_tok, timeout=15.0)
            assert det["trace"]["trace_id"] == tid and len(det["spans"]) == 3

        # Phase 2: Ingest 4 more traces (PLST + OTLP/JSON + streaming start->final) + dataset, then SIGTERM drain
        term_ids: list[str] = []
        for i in range(2):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"term_{i}"), project_id=proj.id, start_ts_ms=ts + 1000 + i * 20
            )
            term_ids.append(b["traces"][0]["trace_id"])
            c2.ingest_plst_batch(b, api_key=proj.pk_key, use_dict=True).raise_for_status()

        otlp_seed = ctx.unique_id("term_otlp")
        otlp_req = fixtures.make_otlp_export_request(seed=otlp_seed, start_ts_ms=ts + 1200)
        c2.ingest_otlp_json(otlp_req, api_key=proj.pk_key).raise_for_status()
        term_ids.append(fixtures.deterministic_hex_id(f"trace:{otlp_seed}", 16))

        stream_tid = ctx.unique_id("term_stream")
        stream_sp_id = ctx.unique_id("sp_term_stream")
        for phase, status, dur in (("start", "running", 15), ("final", "ok", 110)):
            sp_s = fixtures.make_span(
                stream_sp_id, stream_tid, project_id=proj.id, span_type="llm",
                span_phase=phase, status=status, start_ts_ms=ts + 1300, duration_ms=dur,
            )
            tr_s = fixtures.make_trace(
                stream_tid, project_id=proj.id, status=status,
                start_ts_ms=ts + 1300, duration_ms=dur, spans=[sp_s],
            )
            c2.ingest_plst_batch({"traces": [tr_s], "spans": [sp_s]}, api_key=proj.pk_key).raise_for_status()
        term_ids.append(stream_tid)

        ds = c2.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/datasets",
            json_body=fixtures.make_dataset(name=ctx.unique_id("ds_persist")),
            token=root_tok,
        ).raise_for_status().json()

        rc = ctx.node_manager.stop(inst2, graceful=True, timeout=20.0)
        active_inst = None
        assert rc == 0, f"Graceful SIGTERM exited with rc={rc}"

        inst3 = ctx.node_manager.start_all(data_dir=data_dir, admin_token=root_tok)
        active_inst = inst3
        c3 = PulseClient(inst3.base_url)

        artifacts = list_agent_artifacts(data_dir)
        assert any(proj.id in p.parts for p in artifacts["agent_segments"]), artifacts
        assert any(proj.id in p.parts for p in artifacts["agent_manifests"]), artifacts

        for tid in kill_ids + term_ids:
            det = _wait_trace_visible(c3, proj.id, tid, root_tok, timeout=15.0)
            assert det["trace"]["trace_id"] == tid
            assert len(det["spans"]) >= 1

        ds_list = _extract_items(
            c3.request("GET", f"/v1/projects/{proj.id}/agent/datasets", token=root_tok)
            .raise_for_status()
            .json(),
            "datasets",
            "items",
        )
        assert any(d["id"] == ds["id"] for d in ds_list), ds_list
    finally:
        if active_inst is not None:
            ctx.node_manager.stop(active_inst, graceful=True, timeout=10.0)


# ===========================================================================
# T4-10 — BYOK LLM-as-Judge Multi-Level Evals & Rollup Join
# ===========================================================================


@test_case(
    id="T4-10",
    tier=4,
    feature="F17",
    title="Multi-level async evaluations (session, trace, span) with BYOK LLM-as-judge and rollup filtering",
    tags=("tier4", "F13", "F15", "F17", "F18", "llm-judge", "rollups", "multi-level"),
)
def test_t4_10_byok_llm_judge_multilevel_evals_and_rollups(ctx: TestContext) -> None:
    assert ctx.mock_llm is not None and ctx.mock_llm_url is not None
    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_10_ml"))
    _configure_byok_llm(ctx.client, proj.id, ctx.admin_token, ctx.mock_llm_url)

    sess_good = ctx.unique_id("sess_good")
    sess_bad = ctx.unique_id("sess_bad")
    tr_good_id = ctx.unique_id("tr_good")
    tr_bad_id = ctx.unique_id("tr_bad")

    def _judge_responder(record: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        body_txt = record.get("body", b"").decode("utf-8", errors="replace")
        is_good = (tr_good_id in body_txt) or (sess_good in body_txt) or ("Helpful answer" in body_txt)
        payload = (
            '{"score": 0.92, "passed": true, "reasoning": "Helpful"}'
            if is_good
            else '{"score": 0.30, "passed": false, "reasoning": "Incomplete"}'
        )
        return (
            200,
            {
                "id": "chatcmpl-t4-10",
                "object": "chat.completion",
                "model": "gpt-4o-mini",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": payload},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    ctx.mock_llm.custom_responder = _judge_responder
    try:
        for name, target_type, kind, cfg in (
            (ctx.unique_id("ev_sess"), "session", "output_not_empty", {}),
            (ctx.unique_id("ev_span"), "span", "latency_budget", {"max_ms": 500}),
            (ctx.unique_id("ev_llm"), "trace", "llm_judge", None),
        ):
            ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/evaluators",
                json_body=fixtures.make_evaluator_config(
                    name=name, kind=kind, target_type=target_type, enabled=True, config=cfg
                ),
                token=ctx.member_token,
            ).raise_for_status()

        ts = _recent_ts_ms(20_000)
        sp_good = fixtures.make_span(
            ctx.unique_id("sp_good"), tr_good_id, project_id=proj.id, span_type="llm",
            start_ts_ms=ts, duration_ms=100, output_payload={"reply": "Helpful answer"},
        )
        tr_good = fixtures.make_trace(
            tr_good_id, project_id=proj.id, session_id=sess_good, start_ts_ms=ts, duration_ms=100,
            output_payload={"reply": "Helpful answer"}, spans=[sp_good],
        )

        sp_bad = fixtures.make_span(
            ctx.unique_id("sp_bad"), tr_bad_id, project_id=proj.id, span_type="llm",
            start_ts_ms=ts + 200, duration_ms=900, output_payload={"reply": "Vague stub"},
        )
        tr_bad = fixtures.make_trace(
            tr_bad_id, project_id=proj.id, session_id=sess_bad, start_ts_ms=ts + 200, duration_ms=900,
            output_payload={"reply": "Vague stub"}, spans=[sp_bad],
        )

        ctx.client.ingest_plst_batch({"traces": [tr_good], "spans": [sp_good]}, api_key=proj.pk_key).raise_for_status()
        ctx.client.ingest_plst_batch({"traces": [tr_bad], "spans": [sp_bad]}, api_key=proj.pk_key).raise_for_status()

        ctx.mock_llm.wait_for_count(2, timeout=15.0)
        good_evals = _wait_evals(
            ctx.client, proj.id, ctx.viewer_token, query={"session_id": sess_good}, min_count=3
        )
        bad_evals = _wait_evals(
            ctx.client, proj.id, ctx.viewer_token, query={"session_id": sess_bad}, min_count=3
        )

        all_evals = good_evals + bad_evals
        assert {e["target_type"] for e in all_evals} == {"session", "trace", "span"}, all_evals
        for ev in all_evals:
            if ev["target_type"] == "session":
                assert ev.get("trace_id") is None and ev.get("span_id") is None, ev
            elif ev["target_type"] == "trace":
                assert ev.get("trace_id") is not None and ev.get("span_id") is None, ev
            elif ev["target_type"] == "span":
                assert ev.get("trace_id") is not None and ev.get("span_id") is not None, ev

        good_span_ev = next(e for e in good_evals if e["target_type"] == "span")
        bad_span_ev = next(e for e in bad_evals if e["target_type"] == "span")
        assert good_span_ev["passed"] is True, good_span_ev
        assert bad_span_ev["passed"] is False, bad_span_ev

        filtered = _extract_items(
            ctx.client.request(
                "GET",
                f"/v1/projects/{proj.id}/agent/traces",
                query={"min_eval_score": 0.8},
                token=ctx.viewer_token,
            ).raise_for_status().json(),
            "traces",
            "items",
        )
        filtered_ids = {t["trace_id"] for t in filtered}
        assert tr_good_id in filtered_ids and tr_bad_id not in filtered_ids, filtered

        sess_good_det = ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_good}", token=ctx.viewer_token
        ).raise_for_status().json()
        assert sess_good_det["summary"]["avg_eval_score"] is not None, sess_good_det
    finally:
        ctx.mock_llm.custom_responder = None


# ===========================================================================
# T4-11 — OTLP/JSON + PLST Dual-Ingest Convergence & MCP Agent Inspection
# ===========================================================================


@test_case(
    id="T4-11",
    tier=4,
    feature="F16",
    title="OTLP/JSON, OTLP/Protobuf, and PLST dual-ingest convergence inspected via 4 project-bound MCP tools",
    tags=("tier4", "F02", "F14", "F16", "F22", "otlp", "plst", "mcp"),
)
def test_t4_11_dual_ingest_convergence_and_mcp_inspection(ctx: TestContext) -> None:
    py_sdk_dir = _REPO_ROOT / "sdks" / "py-agent"
    assert py_sdk_dir.is_dir(), f"Missing M5 Python SDK directory: {py_sdk_dir}"

    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_11_conv"))
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("conv_noerr"), kind="no_errors", enabled=True
        ),
        token=ctx.member_token,
    ).raise_for_status()

    sess_conv = ctx.unique_id("sess_conv")
    tr_plst_id = ctx.unique_id("tr_py_conv")
    ts = _recent_ts_ms(20_000)

    # Trace 1 via Python SDK (PLST binary)
    py_script = """
import inspect, os, pulse_agent
Ctor = next(
    getattr(pulse_agent, n)
    for n in ("PulseAgentClient", "PulseAgent", "AgentClient", "PulseClient")
    if hasattr(pulse_agent, n)
)
sig = inspect.signature(Ctor.__init__)
kw = {}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = os.environ["PULSE_API_KEY"]
    elif pname in ("endpoint", "base_url", "host", "url"): kw[pname] = os.environ["PULSE_BASE_URL"]
    elif pname in ("use_binary", "use_plst"): kw[pname] = True
client = Ctor(**kw)
with client.trace(
    trace_id=os.environ["PULSE_TRACE_ID"],
    session_id=os.environ["PULSE_SESSION_ID"],
    agent_name="conv-agent",
    input={"q": "Check convergence"},
) as tr:
    with tr.generation(name="llm.conv", model="gpt-4o-mini", provider="openai", input_tokens=40, output_tokens=15) as g:
        if hasattr(g, "set_output"):
            g.set_output({"text": "ok"})
    with tr.guardrail(name="guard.conv") as gd:
        if hasattr(gd, "set_output"):
            gd.set_output({"safe": True})
    if hasattr(tr, "set_output"):
        tr.set_output({"reply": "Converged"})
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
"""
    proc = _run_cmd(
        [sys.executable, "-c", py_script],
        env={
            "PYTHONPATH": str(py_sdk_dir),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": proj.pk_key,
            "PULSE_TRACE_ID": tr_plst_id,
            "PULSE_SESSION_ID": sess_conv,
        },
        timeout=30.0,
    )
    assert proc.returncode == 0, (
        f"Python SDK PLST driver failed (rc={proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
    )

    # Trace 2 via OTLP/JSON & Trace 3 via OTLP/Protobuf
    seed_j = ctx.unique_id("conv_oj")
    seed_pb = ctx.unique_id("conv_opb")
    otlp_j = fixtures.make_otlp_export_request(
        seed=seed_j, agent_name="conv-agent", session_id=sess_conv, start_ts_ms=ts + 100
    )
    otlp_pb = fixtures.make_otlp_export_request(
        seed=seed_pb, agent_name="conv-agent", session_id=sess_conv, start_ts_ms=ts + 200
    )
    tr_otlp_j_id = fixtures.deterministic_hex_id(f"trace:{seed_j}", 16)
    tr_otlp_pb_id = fixtures.deterministic_hex_id(f"trace:{seed_pb}", 16)

    ctx.client.ingest_otlp_json(otlp_j, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_otlp_pb(otlp_pb, api_key=proj.pk_key).raise_for_status()

    for tid in (tr_plst_id, tr_otlp_j_id, tr_otlp_pb_id):
        _wait_trace_visible(ctx.client, proj.id, tid, ctx.viewer_token)
    _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr_plst_id}, min_count=1)

    # Exercise MCP JSON-RPC 2.0 with project-bound sk_key and viewer_token
    init_rpc = ctx.client.mcp_call(
        "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}}, token=proj.sk_key
    ).raise_for_status().json()
    assert "result" in init_rpc, init_rpc

    tools_rpc = ctx.client.mcp_call("tools/list", {}, req_id=2, token=proj.sk_key).raise_for_status().json()
    tool_names = {t["name"] for t in tools_rpc["result"]["tools"]}
    for name in _AGENT_MCP_TOOLS:
        assert name in tool_names, f"Missing MCP tool {name!r}: {sorted(tool_names)}"

    list_rpc = ctx.client.mcp_tool_call(
        "list_agent_traces",
        {"agent_name": "conv-agent", "limit": 20},
        req_id=3,
        token=ctx.viewer_token,
        project_id=proj.id,
    ).raise_for_status().json()
    listed_ids = {
        t["trace_id"]
        for t in _extract_items(list_rpc["result"]["structuredContent"], "traces", "items")
    }
    assert {tr_plst_id, tr_otlp_j_id, tr_otlp_pb_id}.issubset(listed_ids), listed_ids

    det_rpc = ctx.client.mcp_tool_call(
        "get_agent_trace", {"trace_id": tr_otlp_pb_id}, req_id=4, token=proj.sk_key
    ).raise_for_status().json()
    det_sc = det_rpc["result"]["structuredContent"]
    assert det_sc["trace"]["trace_id"] == tr_otlp_pb_id
    assert {"agent", "llm", "guardrail"}.issubset({s["span_type"] for s in det_sc["spans"]})
    assert any(s.get("events") for s in det_sc["spans"])

    ana_rpc = ctx.client.mcp_tool_call(
        "query_agent_analytics", {"days": 7}, req_id=5, token=proj.sk_key
    ).raise_for_status().json()
    assert ana_rpc["result"]["structuredContent"]["total_executions"] == 3, ana_rpc

    ev_rpc = ctx.client.mcp_tool_call(
        "list_agent_evaluations", {"trace_id": tr_plst_id}, req_id=6, token=proj.sk_key
    ).raise_for_status().json()
    ev_items = _extract_items(ev_rpc["result"]["structuredContent"], "evaluations", "items")
    assert any(e["passed"] is True for e in ev_items), ev_items

    # Project-binding isolation checks
    override_rpc = ctx.client.mcp_tool_call(
        "get_agent_trace",
        {"trace_id": tr_plst_id, "project_id": ctx.proj_b.id},
        req_id=7,
        token=ctx.owner_token,
        project_id=proj.id,
    ).raise_for_status().json()
    assert override_rpc.get("error", {}).get("code") == -32602 or override_rpc.get("result", {}).get("isError") is True, (
        override_rpc
    )

    cross_resp = ctx.client.mcp_tool_call(
        "list_agent_traces", {}, req_id=8, token=proj.sk_key, project_id=ctx.proj_b.id
    )
    assert cross_resp.status == 403, f"Expected 403 for cross-project sk_key on MCP, got {cross_resp.status}"


# ===========================================================================
# T4-12 — Seeded Demo Validation (--seed --no-quic), /metrics & Studio #/agents
# ===========================================================================


@test_case(
    id="T4-12",
    tier=4,
    feature="F24",
    title="pulse-node --seed --no-quic serves seeded agent data, Studio #/agents chunks, and zero-cardinality /metrics",
    tags=("tier4", "F15", "F20", "F21", "F24", "seed", "metrics", "studio"),
)
def test_t4_12_seeded_demo_metrics_and_studio(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    seeded = ctx.node_manager.start_seeded()
    try:
        sc = PulseClient(seeded.base_url)
        tok = seeded.root_admin_token()
        assert sc.get_healthz().status == 200
        assert sc.get_readyz().status == 200

        pid = "proj_fintech_in"
        ana = sc.request("GET", f"/v1/projects/{pid}/agent/analytics", token=tok).raise_for_status().json()
        _assert_no_nan_inf(ana)
        assert ana["total_executions"] > 0, ana

        sessions = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/sessions", token=tok).raise_for_status().json(),
            "sessions",
            "items",
        )
        assert sessions, "Expected seeded agent sessions"
        sess_det = sc.request(
            "GET", f"/v1/projects/{pid}/agent/sessions/{sessions[0]['session_id']}", token=tok
        ).raise_for_status().json()
        assert sess_det["traces"], sess_det

        traces = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/traces", token=tok).raise_for_status().json(),
            "traces",
            "items",
        )
        assert traces, "Expected seeded agent traces"
        tr_det = sc.request(
            "GET", f"/v1/projects/{pid}/agent/traces/{traces[0]['trace_id']}", token=tok
        ).raise_for_status().json()
        assert tr_det["spans"], tr_det

        for suffix, keys in (
            ("/evaluators", ("evaluators", "items")),
            ("/evaluations", ("evaluations", "items")),
            ("/datasets", ("datasets", "items")),
            ("/experiments", ("experiments", "items")),
        ):
            items = _extract_items(
                sc.request("GET", f"/v1/projects/{pid}/agent{suffix}", token=tok).raise_for_status().json(),
                *keys,
            )
            assert items, f"Expected non-empty seeded {suffix}"

        for suffix in ("/alerts", "/privacy", "/pricing"):
            sc.request("GET", f"/v1/projects/{pid}/agent{suffix}", token=tok).raise_for_status()

        studio = sc.check_studio_static(require_agents_route=True)
        assert studio["agents_route_detected"] is True
        entry_js = "\n".join(
            sc.request("GET", asset).raise_for_status().text(errors="replace")
            for asset in studio["assets"]
        )
        assert re.findall(r"AgentObservabilityWorkspace[A-Za-z0-9_.-]*\.js", entry_js)
        replay_chunks = re.findall(r"SessionsReplayWorkspace[A-Za-z0-9_.-]*\.js", entry_js)
        assert replay_chunks
        replay_js = sc.request("GET", f"/assets/{replay_chunks[0]}").raise_for_status().text(errors="replace")
        assert "session" in replay_js and ("URLSearchParams" in replay_js or "replays" in replay_js)

        metrics_text = sc.get_metrics().raise_for_status().text()
        assert "project_id=" not in metrics_text, "Tenant project_id= label leaked into /metrics"
        assert pid not in metrics_text, f"Tenant {pid} leaked into /metrics"
        for name in _AGENT_METRICS:
            m = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9]+(?:\.[0-9]+)?)$", metrics_text, re.MULTILINE)
            assert m is not None, f"Missing Prometheus counter {name!r}"
            assert math.isfinite(float(m.group(1))) and float(m.group(1)) >= 0.0

        tables = set(sqlite_tables(seeded.data_dir))
        for forbidden in _FORBIDDEN_SQL_TABLES:
            assert forbidden not in tables
        assert "agent_eval_rollups" in tables and "agent_erasure_tombstones" in tables
    finally:
        ctx.node_manager.stop(seeded, graceful=True, timeout=10.0)


# ===========================================================================
# T4-13 — End-to-End SDKs (ts-agent & py-agent) + examples/agent-demo + Isolation
# ===========================================================================


@test_case(
    id="T4-13",
    tier=4,
    feature="F22",
    title="TypeScript & Python SDKs + examples/agent-demo end-to-end production workflow and multi-org isolation",
    tags=("tier4", "F03", "F04", "F14", "F15", "F22", "F24", "sdk", "agent-demo", "isolation"),
)
def test_t4_13_sdks_agent_demo_and_multi_org_isolation(ctx: TestContext) -> None:
    node_bin = shutil.which("node")
    assert node_bin, "node executable not found on PATH"
    ts_entry = _resolve_ts_sdk_entry()
    py_sdk_dir = _REPO_ROOT / "sdks" / "py-agent"
    demo_dir = _REPO_ROOT / "examples" / "agent-demo"
    assert py_sdk_dir.is_dir(), f"Missing M5 Python SDK directory: {py_sdk_dir}"
    assert demo_dir.is_dir(), f"Missing M5 example directory: {demo_dir}"

    proj = _create_project(ctx.client, ctx.owner_token, ctx.unique_id("t4_13_sdk"))

    # 1. Run examples/agent-demo against live node + MockLlmServer
    demo_cmd: list[str] | None = None
    if (demo_dir / "package.json").is_file():
        demo_cmd = ["npm", "--prefix", str(demo_dir), "start"]
    elif (demo_dir / "main.py").is_file():
        demo_cmd = [sys.executable, str(demo_dir / "main.py")]
    elif (demo_dir / "demo.py").is_file():
        demo_cmd = [sys.executable, str(demo_dir / "demo.py")]
    elif (demo_dir / "index.mjs").is_file():
        demo_cmd = ["node", str(demo_dir / "index.mjs")]
    elif (demo_dir / "run.sh").is_file():
        demo_cmd = ["bash", str(demo_dir / "run.sh")]
    assert demo_cmd is not None, f"No runnable entrypoint found in {demo_dir}"

    demo_proc = _run_cmd(
        demo_cmd,
        cwd=demo_dir,
        env={
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_ENDPOINT": ctx.base_url,
            "PULSE_API_KEY": proj.pk_key,
            "OPENAI_BASE_URL": ctx.mock_llm_url or "",
            "PYTHONPATH": str(py_sdk_dir),
        },
        timeout=60.0,
    )
    assert demo_proc.returncode == 0, (
        f"examples/agent-demo failed (rc={demo_proc.returncode}):\n{demo_proc.stdout}\n{demo_proc.stderr}"
    )

    def _poll_demo_traces() -> list[dict[str, Any]] | None:
        items = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token)
            .raise_for_status()
            .json(),
            "traces",
            "items",
        )
        return items if any(t.get("span_count", 0) >= 2 for t in items) else None

    demo_traces = _poll_until(
        _poll_demo_traces,
        timeout=12.0,
        desc="agent-demo traces",
    )
    assert any(t.get("span_count", 0) >= 2 for t in demo_traces), demo_traces

    # 2. Emit multi-span PLST workflow trace via TypeScript SDK
    sid = ctx.unique_id("sess_ts_wf")
    tid = ctx.unique_id("tr_ts_wf")
    ts_driver = """
import { pathToFileURL } from 'node:url';
const mod = await import(pathToFileURL(process.env.PULSE_SDK_ENTRY).href);
const Ctor = mod.PulseAgentClient || mod.PulseAgent || mod.AgentClient || mod.default;
if (!Ctor) throw new Error('Missing TS SDK client export');
const client = new Ctor({
  endpoint: process.env.PULSE_BASE_URL,
  baseUrl: process.env.PULSE_BASE_URL,
  apiKey: process.env.PULSE_API_KEY,
  useBinary: true,
  usePlst: true,
});
const sess = client.session ? client.session({ sessionId: process.env.PULSE_SESSION_ID, id: process.env.PULSE_SESSION_ID }) : null;
const tr = sess && sess.trace
  ? sess.trace({ traceId: process.env.PULSE_TRACE_ID, agentName: 'ts-workflow-agent', input: { q: 'status' } })
  : client.trace({ traceId: process.env.PULSE_TRACE_ID, sessionId: process.env.PULSE_SESSION_ID, agentName: 'ts-workflow-agent', input: { q: 'status' } });
const gen = tr.generation({ name: 'llm.step', model: 'gpt-4o-mini', provider: 'openai', inputTokens: 60, outputTokens: 20 });
gen.end({ output: { text: 'ok' }, inputTokens: 60, outputTokens: 20 });
const tool = tr.tool({ name: 'tool.lookup', toolName: 'lookup' });
tool.end({ output: { ok: true } });
const guard = tr.guardrail({ name: 'guardrail.policy' });
guard.end({ output: { safe: true } });
tr.end({ output: { reply: 'workflow complete' } });
if (client.flush) await client.flush();
if (client.shutdown) await client.shutdown();
"""
    ts_proc = _run_cmd(
        [node_bin, "--input-type=module", "-e", ts_driver],
        env={
            "PULSE_SDK_ENTRY": str(ts_entry),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": proj.pk_key,
            "PULSE_TRACE_ID": tid,
            "PULSE_SESSION_ID": sid,
        },
        timeout=30.0,
    )
    assert ts_proc.returncode == 0, (
        f"TS SDK driver failed (rc={ts_proc.returncode}):\n{ts_proc.stdout}\n{ts_proc.stderr}"
    )

    ts_det = _wait_trace_visible(ctx.client, proj.id, tid, ctx.viewer_token)
    assert {"llm", "tool", "guardrail"}.issubset({s["span_type"] for s in ts_det["spans"]})

    sessions = _extract_items(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token)
        .raise_for_status()
        .json(),
        "sessions",
        "items",
    )
    assert any(s["session_id"] == sid for s in sessions), sessions
    ana = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token
    ).raise_for_status().json()
    assert ana["total_executions"] >= 2, ana

    # 3. Strict multi-org (404) & cross-project (403) isolation
    for path in ("/traces", f"/traces/{tid}", "/sessions", f"/sessions/{sid}", "/analytics"):
        r_org = ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent{path}", token=ctx.other_org_token
        )
        assert r_org.status == 404, f"Expected 404 for other_org_token on {path}, got {r_org.status}"

        r_proj = ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent{path}", token=ctx.proj_b.sk_key
        )
        assert r_proj.status == 403, f"Expected 403 for proj_b.sk_key on {path}, got {r_proj.status}"
