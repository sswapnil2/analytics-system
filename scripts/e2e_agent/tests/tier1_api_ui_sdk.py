"""Tier 1 opaque-box E2E feature coverage tests for Features F13-F24 (62 test cases).

Covers:
- F13: Slimmed 0011_agent_observability.sql & MetaStore split (T1-F13-01..05)
- F14: Public ingest routes POST /v1/agent/traces & /v1/otel/v1/traces (T1-F14-01..05)
- F15: Protected Session, Trace, Analytics & Compare routes + 4-tier RBAC (T1-F15-01..06)
- F16: 4 project-bound MCP tools on POST /v1/mcp (T1-F16-01..05)
- F17: Async multi-level eval worker (deterministic + BYOK llm_judge) (T1-F17-01..05)
- F18: Evaluators, Datasets, Promote & Experiments APIs (T1-F18-01..05)
- F19: Alerts, HMAC Webhooks, Privacy, Pricing & Retention pruning (T1-F19-01..06)
- F20: Node roles, graceful shutdown, Prometheus metrics & --seed (T1-F20-01..05)
- F21: Pulse Studio UI (#/agents) & Session Replay link (T1-F21-01..05)
- F22: TypeScript & Python PLST SDKs + examples/agent-demo/ (T1-F22-01..05)
- F23: Documentation & API contract consistency (T1-F23-01..05)
- F24: End-to-end acceptance & adversarial integration (T1-F24-01..05)
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, TypeVar

from lib import fixtures
from lib.auth import AuthBootstrap, ProjectCreds, TestContext
from lib.client import HttpResponse, PulseClient
from lib.node_mgr import list_agent_artifacts, sqlite_tables
from lib.registry import test_case

_T = TypeVar("_T")
_REPO_ROOT = Path(__file__).resolve().parents[3]

_CONTROL_PLANE_AGENT_TABLES: tuple[str, ...] = (
    "agent_evaluator_configs",
    "agent_evaluations",
    "agent_eval_rollups",
    "agent_datasets",
    "agent_dataset_items",
    "agent_experiments",
    "agent_experiment_runs",
    "agent_alert_rules",
    "agent_alert_events",
    "agent_privacy_policies",
    "agent_model_pricing",
    "agent_erasure_tombstones",
)

_FORBIDDEN_SQL_TABLES: tuple[str, ...] = ("agent_traces", "agent_spans")

_AGENT_METRICS: tuple[str, ...] = (
    "pulse_agent_traces_ingested_total",
    "pulse_agent_spans_ingested_total",
    "pulse_agent_blobs_deduped_total",
    "pulse_agent_evals_executed_total",
    "pulse_agent_eval_queue_dropped_total",
)

_CORE_MCP_TOOLS: tuple[str, ...] = (
    "get_lexicon_schema",
    "query_insights",
    "query_funnel",
    "query_retention",
    "inspect_frustrated_sessions",
    "query_flows",
    "get_user",
    "list_property_values",
    "list_cohorts",
)

_AGENT_MCP_TOOLS: tuple[str, ...] = (
    "list_agent_traces",
    "get_agent_trace",
    "query_agent_analytics",
    "list_agent_evaluations",
)


# ---------------------------------------------------------------------------
# Shared Helpers (invoked strictly inside test bodies, never at import time)
# ---------------------------------------------------------------------------


def _poll_until(
    predicate: Callable[[], _T | None],
    *,
    timeout: float = 10.0,
    interval: float = 0.1,
    desc: str = "condition",
) -> _T:
    """Poll `predicate` until it returns a truthy non-None value or raise AssertionError."""
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
        except Exception as exc:
            last_exc = exc
        time.sleep(interval)
    if last_exc is not None:
        raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}: {last_exc}")
    raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}")


def _recent_ts_ms(offset_ms: int = 10_000) -> int:
    """Return a recent UTC epoch millisecond timestamp well within 30-day retention."""
    return int(time.time() * 1000) - offset_ms


def _create_temp_project(
    ctx: TestContext,
    suffix: str,
    *,
    client: PulseClient | None = None,
    token: str | None = None,
) -> ProjectCreds:
    """Create a fresh isolated project with pk_ and sk_ keys to prevent cross-test pollution."""
    c = client or ctx.client
    tok = token or ctx.owner_token
    name = f"E2E {ctx.unique_id(suffix)}"
    resp = c.request(
        "POST",
        "/v1/projects",
        json_body={"name": name, "timezone": "UTC"},
        token=tok,
    ).raise_for_status()
    data = resp.json()
    proj_id = str(data["project"]["id"])
    pk_key = str(data["api_key"]["plaintext_key"])

    sk_resp = c.request(
        "POST",
        f"/v1/projects/{proj_id}/api-keys",
        json_body={"label": f"{name} Admin", "role": "admin"},
        token=tok,
    ).raise_for_status()
    sk_key = str(sk_resp.json()["plaintext_key"])
    return ProjectCreds(id=proj_id, name=name, pk_key=pk_key, sk_key=sk_key)


def _wait_trace_detail(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 10.0,
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

    return _poll_until(_check, timeout=timeout, interval=0.08, desc=f"trace {trace_id} visibility")


def _wait_trace_gone(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 10.0,
) -> None:
    def _check() -> bool | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{project_id}/agent/traces/{trace_id}",
            token=token,
        )
        return True if resp.status == 404 else None

    _poll_until(_check, timeout=timeout, interval=0.08, desc=f"trace {trace_id} to be 404")


def _wait_evals(
    client: PulseClient,
    project_id: str,
    token: str,
    *,
    query: dict[str, Any] | None = None,
    min_count: int = 1,
    timeout: float = 12.0,
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
        items = _extract_items(resp.json(), ("evaluations", "items"))
        if len(items) >= min_count:
            return items
        return None

    return _poll_until(
        _check,
        timeout=timeout,
        interval=0.1,
        desc=f">={min_count} evaluations for query={query}",
    )


def _extract_items(payload: Any, candidate_keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in candidate_keys:
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _configure_byok_llm(
    ctx: TestContext,
    project_id: str,
    *,
    token: str | None = None,
    model: str = "gpt-4o-mini",
) -> HttpResponse:
    assert ctx.mock_llm_url, "ctx.mock_llm_url must be initialized"
    return ctx.client.request(
        "PUT",
        f"/v1/projects/{project_id}/byok",
        json_body={
            "category": "llm",
            "provider": "openai",
            "label": "E2E Mock LLM",
            "secret": "sk-e2e-mock-secret-12345678",
            "config_json": {"model": model, "endpoint": ctx.mock_llm_url},
        },
        token=token or ctx.admin_token,
    ).raise_for_status()


def _configure_byok_webhook(
    ctx: TestContext,
    project_id: str,
    signing_secret: str,
    *,
    token: str | None = None,
) -> HttpResponse:
    assert ctx.mock_webhook_url, "ctx.mock_webhook_url must be initialized"
    assert len(signing_secret) >= 16, "signing_secret must be >= 16 characters"
    return ctx.client.request(
        "PUT",
        f"/v1/projects/{project_id}/byok",
        json_body={
            "category": "webhook",
            "provider": "webhook",
            "label": "E2E Mock Webhook",
            "secret": ctx.mock_webhook_url,
            "config_json": {"signing_secret": signing_secret},
        },
        token=token or ctx.admin_token,
    ).raise_for_status()


def _open_sqlite_ro(data_dir: Path | None) -> sqlite3.Connection:
    assert data_dir is not None, "ctx.data_dir is required to inspect pulse_meta.db"
    db_path = Path(data_dir) / "pulse_meta.db"
    assert db_path.is_file(), f"SQLite metadata DB missing at {db_path}"
    uri = f"file:{db_path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True, timeout=5.0)


def _sqlite_table_counts(data_dir: Path | None, tables: tuple[str, ...]) -> dict[str, int]:
    conn = _open_sqlite_ro(data_dir)
    try:
        counts: dict[str, int] = {}
        for tbl in tables:
            row = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()
            counts[tbl] = int(row[0]) if row else 0
        return counts
    finally:
        conn.close()


def _erase_target(
    client: PulseClient,
    project_id: str,
    level: str,
    target_id: str,
    trace_id_hint: str,
    token: str,
) -> HttpResponse:
    """Invoke 3-level erasure via DELETE .../traces/:id?level=... or POST .../agent/erasure."""
    resp = client.request(
        "DELETE",
        f"/v1/projects/{project_id}/agent/traces/{trace_id_hint}",
        query={"level": level, "target_id": target_id},
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    if resp.is_success():
        return resp
    if resp.status in (400, 404, 405, 422):
        alt = client.request(
            "POST",
            f"/v1/projects/{project_id}/agent/erasure",
            json_body={"level": level, "target_id": target_id},
            token=token,
        )
        if alt.is_success():
            return alt
        raise AssertionError(
            f"Erasure failed for level={level} target_id={target_id}: "
            f"DELETE->{resp.status}:{resp.text(errors='replace')[:200]} "
            f"POST->{alt.status}:{alt.text(errors='replace')[:200]}"
        )
    raise AssertionError(
        f"Erasure DELETE failed for level={level} target_id={target_id}: "
        f"{resp.status} {resp.text(errors='replace')[:200]}"
    )


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
# Feature F13 — Slimmed 0011_agent_observability.sql & MetaStore Split
# ===========================================================================


@test_case(
    id="T1-F13-01",
    tier=1,
    feature="F13",
    title="Schema split omits agent_traces/agent_spans and creates control-plane tables",
    tags=("f13", "sqlite", "schema"),
)
def test_f13_01_schema_split(ctx: TestContext) -> None:
    tables = set(sqlite_tables(ctx.data_dir))
    assert tables, f"No SQLite tables found in {ctx.data_dir}"
    for forbidden in _FORBIDDEN_SQL_TABLES:
        assert forbidden not in tables, f"Legacy SQL table {forbidden!r} must be removed"
    for required in _CONTROL_PLANE_AGENT_TABLES:
        assert required in tables, f"Missing control-plane SQL table {required!r}"

    conn = _open_sqlite_ro(ctx.data_dir)
    try:
        cols = {
            row[1]: {"type": row[2], "notnull": int(row[3])}
            for row in conn.execute("PRAGMA table_info(agent_evaluations)").fetchall()
        }
    finally:
        conn.close()

    assert "target_type" in cols, f"agent_evaluations missing target_type: {cols}"
    assert "session_id" in cols, f"agent_evaluations missing session_id: {cols}"
    assert "trace_id" in cols and cols["trace_id"]["notnull"] == 0, (
        f"agent_evaluations.trace_id must exist and be nullable: {cols.get('trace_id')}"
    )
    assert "span_id" in cols and cols["span_id"]["notnull"] == 0, (
        f"agent_evaluations.span_id must exist and be nullable: {cols.get('span_id')}"
    )


@test_case(
    id="T1-F13-02",
    tier=1,
    feature="F13",
    title="Trace and span ingestion performs zero SQL writes and writes binary artifacts",
    tags=("f13", "ingest", "sqlite", "object_store"),
)
def test_f13_02_zero_sql_writes_on_ingest(ctx: TestContext) -> None:
    before_counts = _sqlite_table_counts(ctx.data_dir, _CONTROL_PLANE_AGENT_TABLES)
    seed = ctx.unique_id("f13_zerosql")
    batch = fixtures.make_hierarchy_batch(
        seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    parent_tr_id = batch["traces"][0]["trace_id"]

    resp = ctx.client.ingest_plst_batch(
        batch,
        api_key=ctx.proj_a.pk_key,
        use_dict=True,
    ).raise_for_status()
    body = resp.json()
    assert body["accepted_traces"] == 2, f"Expected 2 accepted_traces, got {body}"
    assert body["accepted_spans"] == 11, f"Expected 11 accepted_spans, got {body}"

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, parent_tr_id, ctx.viewer_token)
    assert detail["trace"]["trace_id"] == parent_tr_id

    after_counts = _sqlite_table_counts(ctx.data_dir, _CONTROL_PLANE_AGENT_TABLES)
    assert after_counts == before_counts, (
        f"Trace/span ingest mutated SQLite control-plane tables: before={before_counts}, after={after_counts}"
    )

    assert ctx.data_dir is not None
    artifacts = list_agent_artifacts(ctx.data_dir)
    assert artifacts["agent_wal"] or artifacts["agent_segments"], (
        f"Expected binary agent_wal or agent_segments artifacts, got {artifacts}"
    )


@test_case(
    id="T1-F13-03",
    tier=1,
    feature="F13",
    title="Multi-level evaluations update agent_evaluations and agent_eval_rollups",
    tags=("f13", "eval_rollups", "sqlite"),
)
def test_f13_03_multi_level_eval_rollups(ctx: TestContext) -> None:
    seed = ctx.unique_id("f13_rollup")
    batch = fixtures.make_batch(seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms())
    tr_id = batch["traces"][0]["trace_id"]
    sid = batch["traces"][0]["session_id"]
    sp_id = batch["spans"][1]["span_id"]

    ctx.client.ingest_json(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    evals = [
        fixtures.make_manual_evaluation(
            target_type="session",
            session_id=sid,
            trace_id=None,
            span_id=None,
            evaluator_id=f"ev_{seed}_sess",
            evaluator_name="sess_quality",
            score=0.8,
            passed=True,
        ),
        fixtures.make_manual_evaluation(
            target_type="trace",
            session_id=sid,
            trace_id=tr_id,
            span_id=None,
            evaluator_id=f"ev_{seed}_tr",
            evaluator_name="trace_quality",
            score=0.6,
            passed=True,
        ),
        fixtures.make_manual_evaluation(
            target_type="span",
            session_id=sid,
            trace_id=tr_id,
            span_id=sp_id,
            evaluator_id=f"ev_{seed}_sp",
            evaluator_name="span_quality",
            score=1.0,
            passed=True,
        ),
    ]
    for ev in evals:
        ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/evaluations",
            json_body=ev,
            token=ctx.member_token,
        ).raise_for_status()

    conn = _open_sqlite_ro(ctx.data_dir)
    try:
        eval_rows = conn.execute(
            "SELECT target_type, session_id, trace_id, span_id, score "
            "FROM agent_evaluations WHERE project_id = ? AND session_id = ?",
            (ctx.proj_a.id, sid),
        ).fetchall()
        rollup_rows = conn.execute(
            "SELECT target_type, target_id, avg_score, eval_count "
            "FROM agent_eval_rollups WHERE project_id = ? AND target_id IN (?, ?, ?)",
            (ctx.proj_a.id, sid, tr_id, sp_id),
        ).fetchall()
    finally:
        conn.close()

    assert len(eval_rows) == 3, f"Expected 3 agent_evaluations rows, got {eval_rows}"
    by_type = {row[0]: row for row in eval_rows}
    assert by_type["session"][2] is None and by_type["session"][3] is None
    assert by_type["trace"][2] == tr_id and by_type["trace"][3] is None
    assert by_type["span"][2] == tr_id and by_type["span"][3] == sp_id

    rollups_map = {(r[0], r[1]): (float(r[2]), int(r[3])) for r in rollup_rows}
    assert rollups_map.get(("session", sid)) == (0.8, 1), f"Rollups: {rollups_map}"
    assert rollups_map.get(("trace", tr_id)) == (0.6, 1), f"Rollups: {rollups_map}"
    assert rollups_map.get(("span", sp_id)) == (1.0, 1), f"Rollups: {rollups_map}"

    tr_list = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces",
        query={"session_id": sid},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    traces = _extract_items(tr_list, ("traces", "items"))
    assert len(traces) == 1 and traces[0]["trace_id"] == tr_id
    assert abs(float(traces[0]["avg_eval_score"]) - 0.6) < 1e-6


@test_case(
    id="T1-F13-04",
    tier=1,
    feature="F13",
    title="Three-level erasure tombstones persist in SQLite and mask traces, sessions, and users",
    tags=("f13", "erasure", "tombstones", "sqlite"),
)
def test_f13_04_erasure_tombstones_three_levels(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f13_erase")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
        token=ctx.admin_token,
    ).raise_for_status()

    s1, s2, s3 = ctx.unique_id("er_tr"), ctx.unique_id("er_sess"), ctx.unique_id("er_usr")
    recent_ts = _recent_ts_ms()
    b1 = fixtures.make_batch(
        seed=s1, project_id=proj.id, session_id=f"sess_{s1}", user_id=f"usr_{s1}", start_ts_ms=recent_ts
    )
    b2 = fixtures.make_batch(
        seed=s2, project_id=proj.id, session_id=f"sess_{s2}", user_id=f"usr_{s2}", start_ts_ms=recent_ts
    )
    b3 = fixtures.make_batch(
        seed=s3, project_id=proj.id, session_id=f"sess_{s3}", user_id=f"usr_{s3}", start_ts_ms=recent_ts
    )
    tr1, tr2, tr3 = b1["traces"][0]["trace_id"], b2["traces"][0]["trace_id"], b3["traces"][0]["trace_id"]

    for batch in (b1, b2, b3):
        ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key).raise_for_status()
    for tr_id in (tr1, tr2, tr3):
        _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)

    _erase_target(ctx.client, proj.id, "trace", tr1, tr1, ctx.admin_token)
    _erase_target(ctx.client, proj.id, "session", f"sess_{s2}", tr2, ctx.admin_token)
    _erase_target(ctx.client, proj.id, "user", f"usr_{s3}", tr3, ctx.admin_token)

    conn = _open_sqlite_ro(ctx.data_dir)
    try:
        rows = conn.execute(
            "SELECT level, target_id FROM agent_erasure_tombstones WHERE project_id = ?",
            (proj.id,),
        ).fetchall()
    finally:
        conn.close()

    tombstones = {(str(r[0]).lower(), str(r[1])) for r in rows}
    assert ("trace", tr1) in tombstones, f"Missing trace tombstone: {tombstones}"
    assert ("session", f"sess_{s2}") in tombstones, f"Missing session tombstone: {tombstones}"
    assert ("user", f"usr_{s3}") in tombstones, f"Missing user tombstone: {tombstones}"

    for tr_id in (tr1, tr2, tr3):
        _wait_trace_gone(ctx.client, proj.id, tr_id, ctx.viewer_token)

    tr_list = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token
    ).raise_for_status().json()
    remaining_traces = {t["trace_id"] for t in _extract_items(tr_list, ("traces", "items"))}
    assert remaining_traces.isdisjoint({tr1, tr2, tr3}), f"Erased traces still listed: {remaining_traces}"

    sess_list = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token
    ).raise_for_status().json()
    remaining_sess = {s["session_id"] for s in _extract_items(sess_list, ("sessions", "items"))}
    assert remaining_sess.isdisjoint({f"sess_{s1}", f"sess_{s2}", f"sess_{s3}"}), (
        f"Erased sessions still listed: {remaining_sess}"
    )


@test_case(
    id="T1-F13-05",
    tier=1,
    feature="F13",
    title="Control-plane metadata tables enforce strict project_id scoping",
    tags=("f13", "tenancy", "sqlite"),
)
def test_f13_05_project_scoping_metadata(ctx: TestContext) -> None:
    tag = ctx.unique_id("f13_scope")
    ev_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=f"ev_a_{tag}", enabled=False),
        token=ctx.member_token,
    ).raise_for_status().json()
    ev_b = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_b.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=f"ev_b_{tag}", enabled=False),
        token=ctx.member_token,
    ).raise_for_status().json()

    ds_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"ds_a_{tag}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_b = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_b.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"ds_b_{tag}"),
        token=ctx.member_token,
    ).raise_for_status().json()

    ev_a_id, ev_b_id = ev_a["id"], ev_b["id"]
    ds_a_id, ds_b_id = ds_a["id"], ds_b["id"]

    list_ev_a = {
        e["id"]
        for e in _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{ctx.proj_a.id}/agent/evaluators", token=ctx.viewer_token
            ).raise_for_status().json(),
            ("evaluators", "items"),
        )
    }
    list_ev_b = {
        e["id"]
        for e in _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{ctx.proj_b.id}/agent/evaluators", token=ctx.viewer_token
            ).raise_for_status().json(),
            ("evaluators", "items"),
        )
    }
    assert ev_a_id in list_ev_a and ev_b_id not in list_ev_a
    assert ev_b_id in list_ev_b and ev_a_id not in list_ev_b

    list_ds_a = {
        d["id"]
        for d in _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{ctx.proj_a.id}/agent/datasets", token=ctx.viewer_token
            ).raise_for_status().json(),
            ("datasets", "items"),
        )
    }
    list_ds_b = {
        d["id"]
        for d in _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{ctx.proj_b.id}/agent/datasets", token=ctx.viewer_token
            ).raise_for_status().json(),
            ("datasets", "items"),
        )
    }
    assert ds_a_id in list_ds_a and ds_b_id not in list_ds_a
    assert ds_b_id in list_ds_b and ds_a_id not in list_ds_b

    conn = _open_sqlite_ro(ctx.data_dir)
    try:
        ev_rows = dict(
            conn.execute(
                "SELECT id, project_id FROM agent_evaluator_configs WHERE id IN (?, ?)",
                (ev_a_id, ev_b_id),
            ).fetchall()
        )
        ds_rows = dict(
            conn.execute(
                "SELECT id, project_id FROM agent_datasets WHERE id IN (?, ?)",
                (ds_a_id, ds_b_id),
            ).fetchall()
        )
    finally:
        conn.close()

    assert ev_rows == {ev_a_id: ctx.proj_a.id, ev_b_id: ctx.proj_b.id}
    assert ds_rows == {ds_a_id: ctx.proj_a.id, ds_b_id: ctx.proj_b.id}


# ===========================================================================
# Feature F14 — Public Ingest Routes (/v1/agent/traces & /v1/otel/v1/traces)
# ===========================================================================


@test_case(
    id="T1-F14-01",
    tier=1,
    feature="F14",
    title="POST /v1/agent/traces ingests PLST v1 frames with raw and dict Zstd",
    tags=("f14", "plst", "ingest"),
)
def test_f14_01_plst_raw_and_dict(ctx: TestContext) -> None:
    recent_ts = _recent_ts_ms()
    b_raw = fixtures.make_repeated_prompt_batch(
        seed=ctx.unique_id("f14_raw"), project_id=ctx.proj_a.id, repeats=3, start_ts_ms=recent_ts
    )
    b_dict = fixtures.make_repeated_prompt_batch(
        seed=ctx.unique_id("f14_dict"), project_id=ctx.proj_a.id, repeats=3, start_ts_ms=recent_ts
    )

    r_raw = ctx.client.ingest_plst_batch(
        b_raw, api_key=ctx.proj_a.pk_key, use_dict=False
    ).raise_for_status().json()
    r_dict = ctx.client.ingest_plst_batch(
        b_dict, api_key=ctx.proj_a.pk_key, use_dict=True
    ).raise_for_status().json()

    assert r_raw["accepted_traces"] == 3 and r_raw["accepted_spans"] == 3, r_raw
    assert r_dict["accepted_traces"] == 3 and r_dict["accepted_spans"] == 3, r_dict

    for idx, batch in enumerate((b_raw, b_dict)):
        tr_id = batch["traces"][idx]["trace_id"]
        detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)
        assert detail["trace"]["output"]["answer"] == f"Turn {idx} resolved"
        assert "Pulse Financial Support" in json.dumps(detail["trace"]["input"])


@test_case(
    id="T1-F14-02",
    tier=1,
    feature="F14",
    title="POST /v1/agent/traces ingests native JSON batches with 5-level hierarchy",
    tags=("f14", "json", "ingest"),
)
def test_f14_02_json_hierarchy_ingest(ctx: TestContext) -> None:
    seed = ctx.unique_id("f14_json")
    batch = fixtures.make_hierarchy_batch(
        seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    parent_tr_id = batch["traces"][0]["trace_id"]
    child_tr_id = batch["traces"][1]["trace_id"]

    resp = ctx.client.ingest_json(batch, api_key=ctx.proj_a.pk_key).raise_for_status().json()
    assert resp["accepted_traces"] == 2, resp
    assert resp["accepted_spans"] == 11, resp

    parent_detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, parent_tr_id, ctx.viewer_token)
    child_detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, child_tr_id, ctx.viewer_token)

    assert child_detail["trace"]["parent_trace_id"] == parent_tr_id
    parent_spans = parent_detail["spans"]
    assert len(parent_spans) == 10
    assert any(s.get("span_type") == "guardrail" for s in parent_spans)
    assert any(s.get("linked_span_ids") for s in parent_spans)
    assert any(s.get("events") for s in parent_spans)


@test_case(
    id="T1-F14-03",
    tier=1,
    feature="F14",
    title="POST /v1/otel/v1/traces normalizes OTLP/JSON ExportTraceServiceRequest",
    tags=("f14", "otlp", "json"),
)
def test_f14_03_otlp_json_ingest(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f14_otlpj")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
        token=ctx.admin_token,
    ).raise_for_status()

    seed = ctx.unique_id("f14_otlpj")
    otlp = fixtures.make_otlp_export_request(
        seed=seed,
        session_id=f"sess_{seed}",
        user_id=f"user_{seed}",
        start_ts_ms=_recent_ts_ms(),
    )
    trace_id = fixtures.deterministic_hex_id(f"trace:{seed}", 16)

    resp = ctx.client.ingest_otlp_json(otlp, api_key=proj.pk_key).raise_for_status()
    assert resp.status in (200, 202), f"Unexpected status {resp.status}"

    detail = _wait_trace_detail(ctx.client, proj.id, trace_id, ctx.viewer_token)
    tr = detail["trace"]
    assert tr["agent_name"] == "otlp-agent"
    assert tr["session_id"] == f"sess_{seed}"
    assert tr["user_id"] == f"user_{seed}"
    assert tr["input_tokens"] == 180
    assert tr["output_tokens"] == 64
    assert tr["total_cost_nanos"] == 1_800_000
    span_types = {s["span_type"] for s in detail["spans"]}
    assert {"agent", "llm", "guardrail"}.issubset(span_types), span_types


@test_case(
    id="T1-F14-04",
    tier=1,
    feature="F14",
    title="POST /v1/otel/v1/traces decodes OTLP/Protobuf wire payloads",
    tags=("f14", "otlp", "protobuf"),
)
def test_f14_04_otlp_protobuf_ingest(ctx: TestContext) -> None:
    seed = ctx.unique_id("f14_otlppb")
    otlp = fixtures.make_otlp_export_request(
        seed=seed, include_error_span=True, start_ts_ms=_recent_ts_ms()
    )
    trace_id = fixtures.deterministic_hex_id(f"trace:{seed}", 16)

    resp = ctx.client.ingest_otlp_pb(otlp, api_key=ctx.proj_a.pk_key).raise_for_status()
    assert resp.status in (200, 202)

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, trace_id, ctx.viewer_token)
    tr = detail["trace"]
    assert tr["status"] == "error", tr
    assert tr["error_span_count"] >= 1, tr
    assert tr["failure_source"] == "model", tr
    assert any(s.get("events") for s in detail["spans"]), "Expected OTLP span events preserved"


@test_case(
    id="T1-F14-05",
    tier=1,
    feature="F14",
    title="Ingest enforces pk_ key auth, permissive CORS, PII redaction, and non-blocking eval enqueue",
    tags=("f14", "cors", "privacy", "async"),
)
def test_f14_05_auth_cors_redaction_nonblocking(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f14_priv")
    _configure_byok_llm(ctx, proj.id)
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=f"slow_judge_{ctx.unique_id('j')}",
            kind="llm_judge",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    opts = ctx.client.request(
        "OPTIONS",
        "/v1/agent/traces",
        headers={
            "Origin": "https://app.example",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert opts.status in (200, 204), f"OPTIONS returned {opts.status}"
    assert opts.header("access-control-allow-origin") == "*", opts.headers

    bad = ctx.client.ingest_json(
        fixtures.make_batch(
            seed=ctx.unique_id("badkey"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
        ),
        api_key="pk_live_invalid_key_000000",
    )
    assert bad.status == 401, f"Expected 401 for invalid key, got {bad.status}"

    assert ctx.mock_llm is not None
    ctx.mock_llm.latency_s = 2.5
    try:
        pii_batch = fixtures.make_pii_batch(
            seed=ctx.unique_id("pii"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
        )
        tr_id = pii_batch["traces"][0]["trace_id"]

        t0 = time.monotonic()
        resp = ctx.client.ingest_json(pii_batch, api_key=proj.pk_key).raise_for_status()
        elapsed = time.monotonic() - t0

        assert elapsed < 1.0, f"Ingest blocked on slow LLM judge ({elapsed:.2f}s)"
        ingest_body = resp.json()
        assert ingest_body["redacted_fields"] > 0, ingest_body

        detail = _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)
        assert detail["trace"].get("user_id") is None, (
            f"user_id should be stripped under default privacy policy: {detail['trace']}"
        )
        serialized = json.dumps(detail)
        assert "[REDACTED]" in serialized
        for secret in (
            "alice.smith@example.com",
            "123-45-6789",
            "4111111111111111",
            "sk-proj-abc1234567890secrettokenxyz",
            "super-secret-password-999",
        ):
            assert secret not in serialized, f"Unredacted PII leaked: {secret}"
    finally:
        ctx.mock_llm.latency_s = 0.0


# ===========================================================================
# Feature F15 — Protected Session, Trace, Analytics & Compare Routes + RBAC
# ===========================================================================


@test_case(
    id="T1-F15-01",
    tier=1,
    feature="F15",
    title="Session listing and detail aggregate multi-trace conversations",
    tags=("f15", "sessions", "api"),
)
def test_f15_01_sessions_list_and_detail(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f15_sess")
    sess_a = ctx.unique_id("sess_a")
    sess_b = ctx.unique_id("sess_b")
    recent_ts = _recent_ts_ms()

    b1 = fixtures.make_batch(
        seed=ctx.unique_id("s1"), project_id=proj.id, session_id=sess_a, start_ts_ms=recent_ts
    )
    b2 = fixtures.make_batch(
        seed=ctx.unique_id("s2"), project_id=proj.id, session_id=sess_a, start_ts_ms=recent_ts
    )
    b3 = fixtures.make_tool_retry_loop_batch(
        seed=ctx.unique_id("s3"), project_id=proj.id, start_ts_ms=recent_ts
    )
    b3["traces"][0]["session_id"] = sess_b

    for b in (b1, b2, b3):
        ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_trace_detail(ctx.client, proj.id, b["traces"][0]["trace_id"], ctx.viewer_token)

    list_resp = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token
    ).raise_for_status().json()
    sessions = {s["session_id"]: s for s in _extract_items(list_resp, ("sessions", "items"))}
    assert sess_a in sessions and sess_b in sessions, sessions
    assert sessions[sess_a]["trace_count"] == 2 and sessions[sess_a]["error_count"] == 0
    assert sessions[sess_b]["trace_count"] == 1 and sessions[sess_b]["error_count"] == 1
    assert sessions[sess_a]["total_tokens"] > 0 and sessions[sess_a]["total_cost_nanos"] > 0

    detail = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_a}", token=ctx.viewer_token
    ).raise_for_status().json()
    assert detail["summary"]["session_id"] == sess_a
    assert len(detail["traces"]) == 2
    assert isinstance(detail["evaluations"], list)


@test_case(
    id="T1-F15-02",
    tier=1,
    feature="F15",
    title="Trace list supports multi-attribute filters, pagination, and eval rollup join",
    tags=("f15", "traces", "filter"),
)
def test_f15_02_trace_filters_and_rollup_join(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f15_filt")
    specs = [
        ("alpha-agent", "production", "ok", 0.95),
        ("alpha-agent", "staging", "ok", 0.25),
        ("beta-agent", "production", "error", None),
        ("alpha-agent", "production", "error", None),
    ]
    tr_ids: list[str] = []
    recent_ts = _recent_ts_ms()
    for idx, (agent, env, status, score) in enumerate(specs):
        seed = ctx.unique_id(f"flt{idx}")
        b = fixtures.make_batch(seed=seed, project_id=proj.id, start_ts_ms=recent_ts)
        b["traces"][0]["agent_name"] = agent
        b["traces"][0]["environment"] = env
        b["traces"][0]["status"] = status
        tr_id = b["traces"][0]["trace_id"]
        tr_ids.append(tr_id)
        ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
        _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)
        if score is not None:
            ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/evaluations",
                json_body=fixtures.make_manual_evaluation(
                    target_type="trace",
                    session_id=b["traces"][0]["session_id"],
                    trace_id=tr_id,
                    evaluator_id=f"ev_{idx}",
                    score=score,
                    passed=score >= 0.5,
                ),
                token=ctx.member_token,
            ).raise_for_status()

    resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={
            "agent_name": "alpha-agent",
            "status": "ok",
            "min_eval_score": 0.8,
            "limit": 1,
        },
        token=ctx.viewer_token,
    ).raise_for_status().json()
    items = _extract_items(resp, ("traces", "items"))
    assert len(items) == 1, resp
    assert items[0]["trace_id"] == tr_ids[0]
    assert abs(float(items[0]["avg_eval_score"]) - 0.95) < 1e-6
    assert items[0]["eval_pass_rate"] == 1.0
    assert resp["limit"] == 1 and resp["total"] >= 1


@test_case(
    id="T1-F15-03",
    tier=1,
    feature="F15",
    title="Trace detail hydrates full payloads and Admin DELETE erases trace",
    tags=("f15", "traces", "delete"),
)
def test_f15_03_trace_detail_and_delete(ctx: TestContext) -> None:
    batch = fixtures.make_hierarchy_batch(
        seed=ctx.unique_id("f15_del"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)
    assert detail["trace"]["input"]["message"] == "Where is my order?"
    assert detail["trace"]["output"]["reply"] == "Order #1042 is out for delivery."
    assert len(detail["spans"]) == 10
    assert isinstance(detail["evaluations"], list)

    del_resp = ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}",
        token=ctx.admin_token,
    )
    assert del_resp.status in (200, 204), f"DELETE returned {del_resp.status}: {del_resp.text()}"
    _wait_trace_gone(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)


@test_case(
    id="T1-F15-04",
    tier=1,
    feature="F15",
    title="Analytics endpoint computes KPIs, exact percentiles, failure sources, and breakdowns",
    tags=("f15", "analytics", "percentiles"),
)
def test_f15_04_analytics_percentiles_breakdowns(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f15_ana")
    base_ts = _recent_ts_ms(700_000)
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for i in range(10):
        seed = ctx.unique_id(f"ana{i}")
        is_err = i >= 8
        dur = (i + 1) * 100
        b = fixtures.make_batch(seed=seed, project_id=proj.id, start_ts_ms=base_ts + i * 60_000)
        tr = b["traces"][0]
        tr["duration_ms"] = dur
        tr["end_ts_ms"] = tr["start_ts_ms"] + dur
        if is_err:
            tr["status"] = "error"
            tr["error_span_count"] = 1
            tr["failure_source"] = "tool" if i == 8 else "model"
            err_idx = 2 if i == 8 else 1
            b["spans"][err_idx]["status"] = "error"
            b["spans"][err_idx]["error_type"] = "SimulatedError"
        traces.append(tr)
        spans.extend(b["spans"])

    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, traces[-1]["trace_id"], ctx.viewer_token)

    ana = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/analytics",
        query={"start_ts_ms": base_ts - 1000, "end_ts_ms": base_ts + 1_000_000},
        token=ctx.viewer_token,
    ).raise_for_status().json()

    assert ana["total_executions"] == 10, ana
    assert ana["successful_executions"] == 8, ana
    assert ana["failed_executions"] == 2, ana
    assert abs(float(ana["error_rate"]) - 0.2) < 1e-6, ana
    lat = ana["latency"]
    assert lat["p50_ms"] <= lat["p90_ms"] <= lat["p95_ms"] <= lat["p99_ms"], lat
    assert lat["p50_ms"] > 0, lat
    for key in ("by_agent", "by_version", "by_model", "by_tool", "by_environment", "failure_sources", "timeseries"):
        assert ana.get(key), f"Expected non-empty {key} in analytics: {ana}"


@test_case(
    id="T1-F15-05",
    tier=1,
    feature="F15",
    title="Compare endpoint runs Welch t-test with MIN_SIGNIFICANCE_SAMPLES=20 guard",
    tags=("f15", "compare", "stats"),
)
def test_f15_05_compare_welch_t_test(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f15_cmp")
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []

    recent_ts = _recent_ts_ms(60_000)

    def _add_group(version: str, count: int, base_dur: int) -> None:
        for i in range(count):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"cmp_{version}_{i}"),
                project_id=proj.id,
                start_ts_ms=recent_ts + i * 100,
            )
            tr = b["traces"][0]
            dur = base_dur + (i % 5) * 3
            tr["agent_version"] = version
            tr["duration_ms"] = dur
            tr["end_ts_ms"] = tr["start_ts_ms"] + dur
            traces.append(tr)
            spans.extend(b["spans"])

    _add_group("v1", 22, 100)
    _add_group("v2", 22, 500)
    _add_group("v3", 5, 150)

    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, traces[-1]["trace_id"], ctx.viewer_token)

    cmp_sig = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={"baseline_version": "v1", "candidate_version": "v2"},
        token=ctx.viewer_token,
    ).raise_for_status().json()

    lat_delta = cmp_sig.get("latency_ms") or cmp_sig.get("latency") or cmp_sig
    assert lat_delta["sample_size_baseline"] >= 20, cmp_sig
    assert lat_delta["sample_size_candidate"] >= 20, cmp_sig
    assert lat_delta["verdict"] == "significant_regression", cmp_sig
    assert lat_delta["p_value"] is not None and lat_delta["p_value"] < 0.05, cmp_sig

    cmp_small = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={"baseline_version": "v1", "candidate_version": "v3"},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    small_delta = cmp_small.get("latency_ms") or cmp_small.get("latency") or cmp_small
    assert small_delta["verdict"] == "insufficient_data", cmp_small
    assert small_delta["p_value"] is None, cmp_small


@test_case(
    id="T1-F15-06",
    tier=1,
    feature="F15",
    title="4-tier RBAC and multi-org/project isolation enforced across agent routes",
    tags=("f15", "rbac", "tenancy"),
)
def test_f15_06_rbac_and_isolation(ctx: TestContext) -> None:
    batch = fixtures.make_batch(
        seed=ctx.unique_id("f15_rbac"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    # Viewer: GET 200, POST/PUT/DELETE 403
    assert ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces", token=ctx.viewer_token
    ).status == 200
    assert ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("v_ev"), enabled=False),
        token=ctx.viewer_token,
    ).status == 403
    assert ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}",
        token=ctx.viewer_token,
    ).status == 403

    # Member: POST evaluator 200/201, PUT privacy & DELETE trace 403
    mem_post = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("m_ev"), enabled=False),
        token=ctx.member_token,
    )
    assert mem_post.status in (200, 201), mem_post.text()
    assert ctx.client.request(
        "PUT",
        f"/v1/projects/{ctx.proj_a.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=ctx.proj_a.id, capture_user_id=False),
        token=ctx.member_token,
    ).status == 403
    assert ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}",
        token=ctx.member_token,
    ).status == 403

    # Admin & Owner: Admin endpoints succeed
    assert ctx.client.request(
        "PUT",
        f"/v1/projects/{ctx.proj_b.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=ctx.proj_b.id, capture_user_id=False),
        token=ctx.admin_token,
    ).status == 200
    assert ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/privacy",
        token=ctx.owner_token,
    ).status == 200

    # Cross-org user returns 404; ProjectAdmin sk_key on another project returns 403
    assert ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces", token=ctx.other_org_token
    ).status == 404
    assert ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_b.id}/agent/traces", token=ctx.proj_a.sk_key
    ).status == 403
    del_resp = ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}",
        token=ctx.admin_token,
    )
    assert del_resp.status in (200, 204), f"DELETE returned {del_resp.status}: {del_resp.text()}"


# ===========================================================================
# Feature F16 — 4 Project-Bound MCP Tools on /v1/mcp
# ===========================================================================


@test_case(
    id="T1-F16-01",
    tier=1,
    feature="F16",
    title="MCP tools/list advertises all 4 agent tools alongside core tools",
    tags=("f16", "mcp", "tools_list"),
)
def test_f16_01_mcp_tools_list(ctx: TestContext) -> None:
    init_resp = ctx.client.mcp_call(
        "initialize",
        {"protocolVersion": "2025-06-18", "capabilities": {}},
        token=ctx.viewer_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    assert "result" in init_resp, init_resp

    list_resp = ctx.client.mcp_call(
        "tools/list",
        {},
        req_id=2,
        token=ctx.viewer_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    tools = {t["name"]: t for t in list_resp["result"]["tools"]}

    for name in (*_CORE_MCP_TOOLS, *_AGENT_MCP_TOOLS):
        assert name in tools, f"Missing MCP tool {name!r}; got {sorted(tools)}"
        assert tools[name]["inputSchema"]["type"] == "object"


@test_case(
    id="T1-F16-02",
    tier=1,
    feature="F16",
    title="MCP list_agent_traces and get_agent_trace inspect ingested traces",
    tags=("f16", "mcp", "traces"),
)
def test_f16_02_mcp_traces(ctx: TestContext) -> None:
    batch = fixtures.make_batch(
        seed=ctx.unique_id("f16_tr"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    list_rpc = ctx.client.mcp_tool_call(
        "list_agent_traces",
        {"limit": 10},
        token=ctx.viewer_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    res_list = list_rpc["result"]
    assert not res_list.get("isError"), res_list
    assert res_list["content"][0]["text"]
    listed = _extract_items(res_list["structuredContent"], ("traces", "items"))
    assert any(t["trace_id"] == tr_id for t in listed), f"{tr_id} missing from {listed}"

    det_rpc = ctx.client.mcp_tool_call(
        "get_agent_trace",
        {"trace_id": tr_id},
        req_id=2,
        token=ctx.viewer_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    res_det = det_rpc["result"]
    assert not res_det.get("isError"), res_det
    assert res_det["content"][0]["text"]
    sc = res_det["structuredContent"]
    assert sc["trace"]["trace_id"] == tr_id
    assert len(sc["spans"]) == 3


@test_case(
    id="T1-F16-03",
    tier=1,
    feature="F16",
    title="MCP query_agent_analytics returns overview KPIs and breakdowns",
    tags=("f16", "mcp", "analytics"),
)
def test_f16_03_mcp_analytics(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f16_ana")
    recent_ts = _recent_ts_ms()
    for i in range(3):
        b = fixtures.make_batch(
            seed=ctx.unique_id(f"ma{i}"), project_id=proj.id, start_ts_ms=recent_ts + i * 100
        )
        ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_trace_detail(ctx.client, proj.id, b["traces"][0]["trace_id"], ctx.viewer_token)

    rpc = ctx.client.mcp_tool_call(
        "query_agent_analytics",
        {"days": 7},
        token=ctx.viewer_token,
        project_id=proj.id,
    ).raise_for_status().json()
    res = rpc["result"]
    assert not res.get("isError"), res
    sc = res["structuredContent"]
    assert sc["total_executions"] >= 3, sc
    assert "p95_ms" in sc["latency"], sc
    assert "total_cost_usd" in sc, sc
    assert sc["by_agent"], sc


@test_case(
    id="T1-F16-04",
    tier=1,
    feature="F16",
    title="MCP list_agent_evaluations returns multi-level evaluation records",
    tags=("f16", "mcp", "evaluations"),
)
def test_f16_04_mcp_evaluations(ctx: TestContext) -> None:
    seed = ctx.unique_id("f16_ev")
    batch = fixtures.make_batch(seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms())
    tr_id = batch["traces"][0]["trace_id"]
    sid = batch["traces"][0]["session_id"]
    ctx.client.ingest_json(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    ev_name = f"mcp_eval_{seed}"
    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluations",
        json_body=fixtures.make_manual_evaluation(
            target_type="trace",
            session_id=sid,
            trace_id=tr_id,
            evaluator_id=f"ev_{seed}",
            evaluator_name=ev_name,
            score=0.92,
            passed=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    rpc = ctx.client.mcp_tool_call(
        "list_agent_evaluations",
        {"trace_id": tr_id, "limit": 20},
        token=ctx.viewer_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    res = rpc["result"]
    assert not res.get("isError"), res
    items = _extract_items(res["structuredContent"], ("evaluations", "items"))
    match = next((i for i in items if i["evaluator_name"] == ev_name), None)
    assert match is not None, items
    assert abs(float(match["score"]) - 0.92) < 1e-6
    assert match["passed"] is True
    assert match["target_type"] == "trace"


@test_case(
    id="T1-F16-05",
    tier=1,
    feature="F16",
    title="MCP rejects project_id override and prevents cross-project trace leakage",
    tags=("f16", "mcp", "tenancy"),
)
def test_f16_05_mcp_project_binding(ctx: TestContext) -> None:
    batch_b = fixtures.make_batch(
        seed=ctx.unique_id("f16_b"), project_id=ctx.proj_b.id, start_ts_ms=_recent_ts_ms()
    )
    tr_b = batch_b["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch_b, api_key=ctx.proj_b.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_b.id, tr_b, ctx.viewer_token)

    override_rpc = ctx.client.mcp_tool_call(
        "get_agent_trace",
        {"trace_id": tr_b, "project_id": ctx.proj_b.id},
        token=ctx.owner_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    assert override_rpc.get("error", {}).get("code") == -32602, override_rpc

    leak_rpc = ctx.client.mcp_tool_call(
        "get_agent_trace",
        {"trace_id": tr_b},
        req_id=2,
        token=ctx.owner_token,
        project_id=ctx.proj_a.id,
    ).raise_for_status().json()
    res = leak_rpc.get("result", {})
    assert res.get("isError") is True or "error" in leak_rpc, (
        f"Cross-project trace {tr_b} leaked via MCP on proj_a: {leak_rpc}"
    )


# ===========================================================================
# Feature F17 — Async Multi-Level Eval Worker (agent_eval_worker.rs)
# ===========================================================================


@test_case(
    id="T1-F17-01",
    tier=1,
    feature="F17",
    title="Async eval worker executes deterministic evaluators automatically on ingest",
    tags=("f17", "eval_worker", "deterministic"),
)
def test_f17_01_deterministic_auto_eval(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f17_det")
    kinds = ("no_errors", "latency_budget", "tool_sequence", "output_not_empty", "json_schema_keys")
    for kind in kinds:
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=f"auto_{kind}",
                kind=kind,
                target_type="trace",
                enabled=True,
            ),
            token=ctx.member_token,
        ).raise_for_status()

    batch = fixtures.make_batch(
        seed=ctx.unique_id("det"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key).raise_for_status()

    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr_id}, min_count=5)
    assert len(evals) >= 5, evals
    for ev in evals:
        assert ev["passed"] is True, ev
        assert abs(float(ev["score"]) - 1.0) < 1e-6, ev

    tr_detail = _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)
    assert abs(float(tr_detail["trace"]["avg_eval_score"]) - 1.0) < 1e-6


@test_case(
    id="T1-F17-02",
    tier=1,
    feature="F17",
    title="Async eval worker runs BYOK llm_judge via MockLlmServer",
    tags=("f17", "eval_worker", "llm_judge"),
)
def test_f17_02_byok_llm_judge(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f17_llm")
    _configure_byok_llm(ctx, proj.id)
    assert ctx.mock_llm is not None
    ctx.mock_llm.response_content = '{"score": 0.88, "passed": true, "reasoning": "Accurate lookup"}'

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=f"judge_{ctx.unique_id('lj')}",
            kind="llm_judge",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    batch = fixtures.make_batch(
        seed=ctx.unique_id("lj"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_json(batch, api_key=proj.pk_key).raise_for_status()

    reqs = ctx.mock_llm.wait_for_count(1, timeout=10.0)
    assert len(reqs) >= 1
    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr_id}, min_count=1)
    judge_ev = next((e for e in evals if e.get("evaluator_type") == "llm_judge"), None)
    assert judge_ev is not None, evals
    assert abs(float(judge_ev["score"]) - 0.88) < 1e-6
    assert judge_ev["passed"] is True
    assert judge_ev["reasoning"] == "Accurate lookup"


@test_case(
    id="T1-F17-03",
    tier=1,
    feature="F17",
    title="Eval worker evaluates session and span targets and updates rollups",
    tags=("f17", "eval_worker", "multi_level"),
)
def test_f17_03_session_and_span_targets(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f17_ml")
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name="span_cost_check",
            kind="cost_budget",
            target_type="span",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name="session_output_check",
            kind="output_not_empty",
            target_type="session",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    batch = fixtures.make_batch(
        seed=ctx.unique_id("ml"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    sid = batch["traces"][0]["session_id"]
    ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key).raise_for_status()

    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"session_id": sid}, min_count=2)
    span_evals = [e for e in evals if e.get("target_type") == "span"]
    sess_evals = [e for e in evals if e.get("target_type") == "session"]
    assert span_evals and all(e.get("span_id") is not None for e in span_evals), span_evals
    assert sess_evals and all(
        e.get("trace_id") is None and e.get("span_id") is None for e in sess_evals
    ), sess_evals

    sess_detail = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()
    assert sess_detail["summary"]["avg_eval_score"] is not None, sess_detail


@test_case(
    id="T1-F17-04",
    tier=1,
    feature="F17",
    title="Eval worker caps concurrent BYOK LLM judge calls to <= 8 under burst",
    tags=("f17", "eval_worker", "concurrency"),
)
def test_f17_04_llm_concurrency_cap(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f17_conc")
    _configure_byok_llm(ctx, proj.id)
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=f"conc_judge_{ctx.unique_id('cj')}",
            kind="llm_judge",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    assert ctx.mock_llm is not None
    lock = threading.Lock()
    state = {"in_flight": 0, "max_in_flight": 0}

    def _responder(_record: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        with lock:
            state["in_flight"] += 1
            if state["in_flight"] > state["max_in_flight"]:
                state["max_in_flight"] = state["in_flight"]
        try:
            time.sleep(0.25)
            return (
                200,
                {
                    "id": "chatcmpl-conc",
                    "object": "chat.completion",
                    "model": "gpt-4o-mini",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": '{"score": 0.9, "passed": true, "reasoning": "ok"}',
                            },
                            "finish_reason": "stop",
                        }
                    ],
                },
            )
        finally:
            with lock:
                state["in_flight"] -= 1

    ctx.mock_llm.custom_responder = _responder
    try:
        recent_ts = _recent_ts_ms()
        batches = [
            fixtures.make_batch(
                seed=ctx.unique_id(f"burst{i}"), project_id=proj.id, start_ts_ms=recent_ts
            )
            for i in range(16)
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futs = [
                pool.submit(ctx.client.ingest_json, b, api_key=proj.pk_key)
                for b in batches
            ]
            for fut in futs:
                fut.result().raise_for_status()

        reqs = ctx.mock_llm.wait_for_count(16, timeout=15.0)
        assert len(reqs) >= 16
        assert 1 <= state["max_in_flight"] <= 8, (
            f"Expected max_in_flight in [1, 8], got {state['max_in_flight']}"
        )
    finally:
        ctx.mock_llm.custom_responder = None


@test_case(
    id="T1-F17-05",
    tier=1,
    feature="F17",
    title="Failing LLM judge does not stall deterministic evaluators or subsequent traces",
    tags=("f17", "eval_worker", "resilience"),
)
def test_f17_05_llm_judge_failure_resilience(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f17_resil")
    _configure_byok_llm(ctx, proj.id)
    for kind in ("no_errors", "llm_judge"):
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=f"resil_{kind}", kind=kind, enabled=True
            ),
            token=ctx.member_token,
        ).raise_for_status()

    assert ctx.mock_llm is not None
    ctx.mock_llm.status_code = 500
    b1 = fixtures.make_batch(
        seed=ctx.unique_id("res1"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    tr1 = b1["traces"][0]["trace_id"]
    ctx.client.ingest_json(b1, api_key=proj.pk_key).raise_for_status()

    ev1 = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr1}, min_count=1)
    det_ev = next((e for e in ev1 if e.get("evaluator_type") == "deterministic"), None)
    assert det_ev is not None and det_ev["passed"] is True, ev1

    ctx.mock_llm.status_code = 200
    b2 = fixtures.make_batch(
        seed=ctx.unique_id("res2"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    tr2 = b2["traces"][0]["trace_id"]
    ctx.client.ingest_json(b2, api_key=proj.pk_key).raise_for_status()

    ev2 = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr2}, min_count=2)
    judge_ev = next((e for e in ev2 if e.get("evaluator_type") == "llm_judge"), None)
    assert judge_ev is not None and judge_ev["passed"] is True, ev2


# ===========================================================================
# Feature F18 — Evaluators, Datasets, Promote & Experiments APIs
# ===========================================================================


@test_case(
    id="T1-F18-01",
    tier=1,
    feature="F18",
    title="Evaluator config CRUD and on-demand POST .../traces/:id/evaluate",
    tags=("f18", "evaluators", "api"),
)
def test_f18_01_evaluator_crud_and_ondemand(ctx: TestContext) -> None:
    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("ondemand"),
            kind="no_errors",
            enabled=False,
        ),
        token=ctx.member_token,
    ).raise_for_status().json()
    ev_id = ev_cfg["id"]

    listed = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{ctx.proj_a.id}/agent/evaluators", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("evaluators", "items"),
    )
    assert any(e["id"] == ev_id for e in listed)

    batch = fixtures.make_batch(
        seed=ctx.unique_id("f18_od"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}/evaluate",
        json_body={"evaluator_ids": [ev_id]},
        token=ctx.member_token,
    ).raise_for_status()

    evals = _wait_evals(
        ctx.client, ctx.proj_a.id, ctx.viewer_token, query={"trace_id": tr_id}, min_count=1
    )
    assert any(e["evaluator_id"] == ev_id for e in evals), evals

    ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators/{ev_id}",
        token=ctx.member_token,
    ).raise_for_status()
    after = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{ctx.proj_a.id}/agent/evaluators", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("evaluators", "items"),
    )
    assert all(e["id"] != ev_id for e in after)


@test_case(
    id="T1-F18-02",
    tier=1,
    feature="F18",
    title="Dataset and item CRUD plus bulk JSON import",
    tags=("f18", "datasets", "api"),
)
def test_f18_02_dataset_item_crud_and_import(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("ds")),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    item1 = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
        json_body=fixtures.make_dataset_item(name="item-0"),
        token=ctx.member_token,
    ).raise_for_status().json()
    items = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("items", "dataset_items"),
    )
    assert len(items) == 1

    bulk = [fixtures.make_dataset_item(name=f"bulk-{i}") for i in range(1, 4)]
    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/import",
        json_body={"items": bulk},
        token=ctx.member_token,
    ).raise_for_status()
    items = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("items", "dataset_items"),
    )
    assert len(items) == 4, len(items)

    ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items/{item1['id']}",
        token=ctx.member_token,
    ).raise_for_status()
    items = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("items", "dataset_items"),
    )
    assert len(items) == 3

    ctx.client.request(
        "DELETE",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}",
        token=ctx.member_token,
    ).raise_for_status()
    ds_list = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{ctx.proj_a.id}/agent/datasets", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("datasets", "items"),
    )
    assert all(d["id"] != ds_id for d in ds_list)


@test_case(
    id="T1-F18-03",
    tier=1,
    feature="F18",
    title="POST .../traces/:trace_id/promote converts production trace to dataset item",
    tags=("f18", "promote", "datasets"),
)
def test_f18_03_promote_trace_to_dataset(ctx: TestContext) -> None:
    batch = fixtures.make_batch(
        seed=ctx.unique_id("f18_prom"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("prom_ds")),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}/promote",
        json_body={"dataset_id": ds_id},
        token=ctx.member_token,
    ).raise_for_status()

    items = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("items", "dataset_items"),
    )
    assert len(items) == 1, items
    promoted = items[0]
    assert promoted["source_trace_id"] == tr_id
    assert promoted["input"] == batch["traces"][0]["input"]
    assert promoted["expected_output"] == batch["traces"][0]["output"]
    assert "order_lookup" in json.dumps(promoted.get("expected_behavior", {}))


@test_case(
    id="T1-F18-04",
    tier=1,
    feature="F18",
    title="Experiment creation, run submission, and detail aggregation",
    tags=("f18", "experiments", "api"),
)
def test_f18_04_experiment_lifecycle(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("exp_ds")),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    item_ids: list[str] = []
    for i in range(3):
        it = ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            json_body=fixtures.make_dataset_item(name=f"exp-item-{i}"),
            token=ctx.member_token,
        ).raise_for_status().json()
        item_ids.append(it["id"])

    exp = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds_id, name=ctx.unique_id("exp")),
        token=ctx.member_token,
    ).raise_for_status().json()
    exp_id = exp["id"]

    for idx, item_id in enumerate(item_ids):
        ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_id}/runs",
            json_body=fixtures.make_experiment_run(
                dataset_item_id=item_id,
                latency_ms=100 + idx * 20,
                avg_score=0.8 + idx * 0.05,
                passed=True,
            ),
            token=ctx.member_token,
        ).raise_for_status()

    detail = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_id}",
        token=ctx.viewer_token,
    ).raise_for_status().json()
    exp_obj = detail.get("experiment", detail)
    runs = detail.get("runs", [])
    assert exp_obj["completed_items"] == 3, detail
    assert exp_obj["avg_score"] is not None
    assert exp_obj["avg_latency_ms"] is not None
    assert len(runs) == 3


@test_case(
    id="T1-F18-05",
    tier=1,
    feature="F18",
    title="Experiment regression compare enforces insufficient_data (<20) and Welch t-test (>=20)",
    tags=("f18", "experiments", "compare"),
)
def test_f18_05_experiment_compare_welch(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("cmp_ds")),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    item_ids: list[str] = []
    for i in range(22):
        it = ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            json_body=fixtures.make_dataset_item(name=f"cmp-item-{i}"),
            token=ctx.member_token,
        ).raise_for_status().json()
        item_ids.append(it["id"])

    def _create_with_runs(name: str, count: int, score_base: float, lat_base: int) -> str:
        exp = ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/experiments",
            json_body=fixtures.make_experiment(dataset_id=ds_id, name=ctx.unique_id(name)),
            token=ctx.member_token,
        ).raise_for_status().json()
        eid = exp["id"]
        for i in range(count):
            ctx.client.request(
                "POST",
                f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{eid}/runs",
                json_body=fixtures.make_experiment_run(
                    dataset_item_id=item_ids[i],
                    latency_ms=lat_base + (i % 4) * 5,
                    avg_score=score_base + (i % 3) * 0.01,
                    passed=score_base >= 0.7,
                ),
                token=ctx.member_token,
            ).raise_for_status()
        return eid

    small_a = _create_with_runs("small_a", 5, 0.90, 100)
    small_b = _create_with_runs("small_b", 5, 0.50, 350)
    small_cmp = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": small_a, "candidate": small_b},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    small_metric = small_cmp.get("score") or small_cmp.get("avg_score") or small_cmp
    assert small_metric["verdict"] == "insufficient_data", small_cmp
    assert small_metric["p_value"] is None, small_cmp

    base_22 = _create_with_runs("base22", 22, 0.92, 100)
    cand_22 = _create_with_runs("cand22", 22, 0.55, 400)
    sig_cmp = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": base_22, "candidate": cand_22},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    sig_metric = sig_cmp.get("score") or sig_cmp.get("avg_score") or sig_cmp
    assert sig_metric["sample_size_baseline"] == 22, sig_cmp
    assert sig_metric["sample_size_candidate"] == 22, sig_cmp
    assert sig_metric["verdict"] == "significant_regression", sig_cmp
    assert sig_metric["p_value"] is not None and sig_metric["p_value"] < 0.05, sig_cmp


# ===========================================================================
# Feature F19 — Alerts, Webhooks, Privacy, Pricing & Retention Pruning
# ===========================================================================


@test_case(
    id="T1-F19-01",
    tier=1,
    feature="F19",
    title="Agent alert rules CRUD supports all 7 monitoring metrics",
    tags=("f19", "alerts", "crud"),
)
def test_f19_01_alert_rules_crud(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f19_alcrud")
    rule_ids: list[str] = []
    for metric in fixtures.ALL_ALERT_METRICS:
        rule = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/alerts",
            json_body=fixtures.make_alert_rule(name=f"rule_{metric}", metric=metric),
            token=ctx.member_token,
        ).raise_for_status().json()
        assert rule["metric"] == metric
        assert rule["state"] == "ok"
        rule_ids.append(rule["id"])

    listed = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("rules", "alerts", "items"),
    )
    assert {r["id"] for r in listed} == set(rule_ids)

    ctx.client.request(
        "DELETE",
        f"/v1/projects/{proj.id}/agent/alerts/{rule_ids[0]}",
        token=ctx.member_token,
    ).raise_for_status()
    after = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("rules", "alerts", "items"),
    )
    assert len(after) == len(fixtures.ALL_ALERT_METRICS) - 1


@test_case(
    id="T1-F19-02",
    tier=1,
    feature="F19",
    title="Alert evaluation transitions state, enforces cooldown, and sends HMAC-signed webhooks",
    tags=("f19", "alerts", "webhooks", "hmac"),
)
def test_f19_02_alert_firing_and_hmac_webhook(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f19_hook")
    signing_secret = "e2e_webhook_secret_12345"
    _configure_byok_webhook(ctx, proj.id, signing_secret)
    assert ctx.mock_webhook is not None
    ctx.mock_webhook.signing_secret = signing_secret

    rule = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(
            name=f"fail_rate_{ctx.unique_id('al')}",
            metric="failure_rate",
            operator="gt",
            threshold=0.1,
            cooldown_minutes=5,
            webhook_url=ctx.mock_webhook_url,
        ),
        token=ctx.member_token,
    ).raise_for_status().json()
    rule_id = rule["id"]

    now_ms = int(time.time() * 1000)
    for i in range(4):
        b = fixtures.make_tool_retry_loop_batch(
            seed=ctx.unique_id(f"alerr{i}"),
            project_id=proj.id,
            retry_count=2,
            start_ts_ms=now_ms - 1000 + i * 10,
        )
        ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
        _wait_trace_detail(ctx.client, proj.id, b["traces"][0]["trace_id"], ctx.viewer_token)

    def _trigger_eval() -> None:
        r = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/alerts/evaluate",
            token=ctx.member_token,
        )
        if r.status in (404, 405):
            r = ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/alerts/{rule_id}/evaluate",
                token=ctx.member_token,
            )
            r.raise_for_status()
        elif not r.is_success():
            r.raise_for_status()

    _trigger_eval()
    hooks = ctx.mock_webhook.wait_for_count(1, timeout=10.0)
    assert len(hooks) == 1
    assert hooks[0]["signature_valid"] is True, f"Invalid HMAC webhook signature: {hooks[0]}"

    # Re-evaluate immediately: cooldown must suppress duplicate webhook delivery
    _trigger_eval()
    time.sleep(0.3)
    assert len(ctx.mock_webhook.requests) == 1, (
        f"Cooldown failed: expected 1 webhook, got {len(ctx.mock_webhook.requests)}"
    )

    events = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("events", "items"),
    )
    assert any(
        e["rule_id"] == rule_id and e["event_type"] == "triggered" and e["delivered"] is True
        for e in events
    ), events

    rules_after = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("rules", "alerts", "items"),
    )
    updated_rule = next(r for r in rules_after if r["id"] == rule_id)
    assert updated_rule["state"] == "firing", updated_rule


@test_case(
    id="T1-F19-03",
    tier=1,
    feature="F19",
    title="Privacy policy PUT/GET controls capture flags, key redaction, and attribute exclusion",
    tags=("f19", "privacy", "redaction"),
)
def test_f19_03_privacy_policy_enforcement(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f19_priv")
    defaults = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/privacy", token=ctx.viewer_token
    ).raise_for_status().json()
    assert defaults["capture_user_id"] is False
    assert defaults["redact_pii"] is True

    policy = fixtures.make_privacy_policy(
        project_id=proj.id,
        capture_user_id=True,
        capture_outputs=False,
        redact_keys=["custom_secret"],
        exclude_attributes=["debug.trace"],
    )
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=policy,
        token=ctx.admin_token,
    ).raise_for_status()

    seed = ctx.unique_id("priv")
    batch = fixtures.make_batch(
        seed=seed, project_id=proj.id, user_id=f"usr_{seed}", start_ts_ms=_recent_ts_ms()
    )
    batch["traces"][0]["input"] = {"custom_secret": "top-secret-xyz", "prompt": "hello"}
    batch["spans"][1]["input"] = {"custom_secret": "top-secret-xyz", "prompt": "hello"}
    batch["spans"][1]["attributes"] = {"debug.trace": "internal-stack", "keep.attr": "yes"}
    tr_id = batch["traces"][0]["trace_id"]

    ctx.client.ingest_json(batch, api_key=proj.pk_key).raise_for_status()
    detail = _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)

    assert detail["trace"]["user_id"] == f"usr_{seed}"
    assert detail["trace"]["output"] in (None, {}), detail["trace"]
    assert detail["trace"]["input"]["custom_secret"] == "[REDACTED]"
    llm_span = next(s for s in detail["spans"] if s["span_type"] == "llm")
    assert "debug.trace" not in llm_span["attributes"]
    assert llm_span["attributes"].get("keep.attr") == "yes"


@test_case(
    id="T1-F19-04",
    tier=1,
    feature="F19",
    title="Custom pricing rules override token cost calculation in cost_nanos",
    tags=("f19", "pricing", "override"),
)
def test_f19_04_custom_pricing_override(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f19_price")
    rule = fixtures.make_pricing_rule(
        provider="openai",
        model_pattern="custom-e2e-",
        input_per_1m_usd=2.0,
        output_per_1m_usd=8.0,
        cached_input_per_1m_usd=1.0,
    )
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=rule,
        token=ctx.admin_token,
    ).raise_for_status()

    rules = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/pricing", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("rules", "pricing", "items"),
    )
    assert any(r["model_pattern"] == "custom-e2e-" for r in rules), rules

    seed = ctx.unique_id("cp")
    tr_id = f"tr_{seed}"
    ts = _recent_ts_ms()
    sp = fixtures.make_span(
        f"sp_{seed}",
        tr_id,
        project_id=proj.id,
        span_type="llm",
        provider="openai",
        model="custom-e2e-large",
        start_ts_ms=ts,
        input_tokens=1000,
        cached_tokens=200,
        output_tokens=500,
        cost_nanos=0,
        cost_usd=0.0,
    )
    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=f"sess_{seed}",
        start_ts_ms=ts,
        spans=[sp],
    )
    ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()

    detail = _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)
    # 800 * 2000 + 200 * 1000 + 500 * 8000 = 1_600_000 + 200_000 + 4_000_000 = 5_800_000 nanos
    assert detail["spans"][0]["cost_nanos"] == 5_800_000, detail["spans"][0]
    assert detail["trace"]["total_cost_nanos"] == 5_800_000, detail["trace"]


@test_case(
    id="T1-F19-05",
    tier=1,
    feature="F19",
    title="Built-in model pricing fallback computes exact cost_nanos",
    tags=("f19", "pricing", "builtin"),
)
def test_f19_05_builtin_pricing_fallback(ctx: TestContext) -> None:
    seed = ctx.unique_id("bp")
    tr_id = f"tr_{seed}"
    ts = _recent_ts_ms()
    sp = fixtures.make_span(
        f"sp_{seed}",
        tr_id,
        project_id=ctx.proj_a.id,
        span_type="llm",
        provider="openai",
        model="gpt-4o-mini",
        start_ts_ms=ts,
        input_tokens=10_000,
        cached_tokens=4_000,
        output_tokens=2_000,
        cost_nanos=0,
        cost_usd=0.0,
    )
    tr = fixtures.make_trace(
        tr_id,
        project_id=ctx.proj_a.id,
        session_id=f"sess_{seed}",
        start_ts_ms=ts,
        spans=[sp],
    )
    ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=ctx.proj_a.pk_key).raise_for_status()

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)
    # 6000 * 150 + 4000 * 75 + 2000 * 600 = 2_400_000 nanos
    assert detail["spans"][0]["cost_nanos"] == 2_400_000, detail["spans"][0]
    assert detail["trace"]["total_cost_nanos"] == 2_400_000, detail["trace"]


@test_case(
    id="T1-F19-06",
    tier=1,
    feature="F19",
    title="Retention pruning removes expired daily agent segments while keeping fresh traces",
    tags=("f19", "retention", "pruning"),
)
def test_f19_06_retention_pruning(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f19_ret")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, retention_days=1),
        token=ctx.admin_token,
    ).raise_for_status()

    now_ms = int(time.time() * 1000)
    old_ts_ms = now_ms - (10 * 86_400 * 1000)
    old_batch = fixtures.make_batch(
        seed=ctx.unique_id("ret_old"), project_id=proj.id, start_ts_ms=old_ts_ms
    )
    fresh_batch = fixtures.make_batch(
        seed=ctx.unique_id("ret_new"), project_id=proj.id, start_ts_ms=now_ms - 1000
    )
    old_tr = old_batch["traces"][0]["trace_id"]
    fresh_tr = fresh_batch["traces"][0]["trace_id"]

    ctx.client.ingest_plst_batch(old_batch, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch(fresh_batch, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, fresh_tr, ctx.viewer_token)

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()

    def _old_pruned() -> bool | None:
        resp = ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token
        )
        if resp.status != 200:
            return None
        ids = {t["trace_id"] for t in _extract_items(resp.json(), ("traces", "items"))}
        if fresh_tr in ids and old_tr not in ids:
            return True
        return None

    _poll_until(_old_pruned, timeout=20.0, interval=0.25, desc="expired trace retention pruning")


# ===========================================================================
# Feature F20 — Node Roles, Graceful Shutdown, Prometheus & --seed
# ===========================================================================


@test_case(
    id="T1-F20-01",
    tier=1,
    feature="F20",
    title="Split-role cluster enforces route isolation and shares agent telemetry",
    tags=("f20", "roles", "split_cluster"),
)
def test_f20_01_split_role_cluster(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    cluster = ctx.node_manager.start_split_cluster()
    try:
        ingest_c = PulseClient(cluster["ingest"].base_url)
        query_c = PulseClient(cluster["query"].base_url)
        root_tok = cluster["query"].root_admin_token()

        proj = _create_temp_project(ctx, "f20_split", client=query_c, token=root_tok)

        assert ingest_c.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok
        ).status == 404
        batch = fixtures.make_batch(
            seed=ctx.unique_id("split"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
        )
        tr_id = batch["traces"][0]["trace_id"]
        assert query_c.ingest_plst_batch(batch, api_key=proj.pk_key).status == 404

        deadline = time.monotonic() + 10.0
        ingest_resp = ingest_c.ingest_plst_batch(batch, api_key=proj.pk_key)
        while ingest_resp.status == 401 and time.monotonic() < deadline:
            time.sleep(0.25)
            ingest_resp = ingest_c.ingest_plst_batch(batch, api_key=proj.pk_key)
        ingest_resp.raise_for_status()
        detail = _wait_trace_detail(query_c, proj.id, tr_id, root_tok, timeout=15.0)
        assert detail["trace"]["trace_id"] == tr_id
    finally:
        for inst in cluster.values():
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


@test_case(
    id="T1-F20-02",
    tier=1,
    feature="F20",
    title="Graceful SIGTERM shutdown flushes buffered traces with zero data loss",
    tags=("f20", "shutdown", "durability"),
)
def test_f20_02_graceful_sigterm_durability(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    n1 = ctx.node_manager.start_all()
    n2 = None
    try:
        c1 = PulseClient(n1.base_url)
        root_tok = n1.root_admin_token()
        proj = _create_temp_project(ctx, "f20_sig", client=c1, token=root_tok)

        trace_ids: list[str] = []
        recent_ts = _recent_ts_ms()
        for i in range(5):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"sig{i}"), project_id=proj.id, start_ts_ms=recent_ts + i * 100
            )
            trace_ids.append(b["traces"][0]["trace_id"])
            c1.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()

        n2 = ctx.node_manager.restart(n1, graceful_stop=True)
        c2 = PulseClient(n2.base_url)
        for tr_id in trace_ids:
            detail = _wait_trace_detail(c2, proj.id, tr_id, root_tok, timeout=10.0)
            assert detail["trace"]["trace_id"] == tr_id
    finally:
        if n2 is not None:
            ctx.node_manager.stop(n2, graceful=True, timeout=10.0)
        else:
            ctx.node_manager.stop(n1, graceful=False, timeout=5.0)


@test_case(
    id="T1-F20-03",
    tier=1,
    feature="F20",
    title="Prometheus /metrics exposes 5 agent counters with zero tenant cardinality",
    tags=("f20", "metrics", "prometheus"),
)
def test_f20_03_prometheus_agent_counters(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f20_met")
    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("m_noerr"), kind="no_errors", enabled=True
        ),
        token=ctx.member_token,
    ).raise_for_status().json()
    ev_id = ev_cfg["id"]
    try:
        batch = fixtures.make_repeated_prompt_batch(
            seed=ctx.unique_id("f20_met"),
            project_id=proj.id,
            repeats=3,
            start_ts_ms=_recent_ts_ms(),
        )
        ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=True).raise_for_status()
        _wait_evals(
            ctx.client,
            proj.id,
            ctx.viewer_token,
            query={"trace_id": batch["traces"][0]["trace_id"]},
            min_count=1,
        )

        resp = ctx.client.get_metrics()
        assert resp.status == 200
        body = resp.text()
        for metric_name in _AGENT_METRICS:
            assert metric_name in body, f"Missing Prometheus counter {metric_name!r}"
        assert proj.id not in body, f"Tenant ID {proj.id} leaked into /metrics"
        assert ctx.proj_a.id not in body, f"Tenant ID {ctx.proj_a.id} leaked into /metrics"
        assert ctx.proj_b.id not in body, f"Tenant ID {ctx.proj_b.id} leaked into /metrics"
    finally:
        ctx.client.request(
            "DELETE",
            f"/v1/projects/{proj.id}/agent/evaluators/{ev_id}",
            token=ctx.member_token,
        )


@test_case(
    id="T1-F20-04",
    tier=1,
    feature="F20",
    title="pulse-node --seed --no-quic serves seeded agent sessions, traces, evals, datasets, and experiments",
    tags=("f20", "seed", "cli"),
)
def test_f20_04_seeded_agent_data(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    seeded = ctx.node_manager.start_seeded()
    try:
        sc = PulseClient(seeded.base_url)
        tok = seeded.root_admin_token()
        pid = "proj_fintech_in"

        sess = sc.request("GET", f"/v1/projects/{pid}/agent/sessions", token=tok).raise_for_status().json()
        traces = sc.request("GET", f"/v1/projects/{pid}/agent/traces", token=tok).raise_for_status().json()
        evals_cfg = sc.request("GET", f"/v1/projects/{pid}/agent/evaluators", token=tok).raise_for_status().json()
        evals = sc.request("GET", f"/v1/projects/{pid}/agent/evaluations", token=tok).raise_for_status().json()
        datasets = sc.request("GET", f"/v1/projects/{pid}/agent/datasets", token=tok).raise_for_status().json()
        exps = sc.request("GET", f"/v1/projects/{pid}/agent/experiments", token=tok).raise_for_status().json()
        ana = sc.request("GET", f"/v1/projects/{pid}/agent/analytics", token=tok).raise_for_status().json()

        assert _extract_items(sess, ("sessions", "items")), sess
        trace_items = _extract_items(traces, ("traces", "items"))
        assert trace_items, traces
        assert _extract_items(evals_cfg, ("evaluators", "items")), evals_cfg
        assert _extract_items(evals, ("evaluations", "items")), evals
        assert _extract_items(datasets, ("datasets", "items")), datasets
        assert _extract_items(exps, ("experiments", "items")), exps
        assert ana["total_executions"] > 0, ana

        det = sc.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces/{trace_items[0]['trace_id']}",
            token=tok,
        ).raise_for_status().json()
        assert det["spans"], det
    finally:
        ctx.node_manager.stop(seeded, graceful=True, timeout=10.0)


@test_case(
    id="T1-F20-05",
    tier=1,
    feature="F20",
    title="Healthz and readyz probes succeed across ingest, query, and compactor roles",
    tags=("f20", "health", "roles"),
)
def test_f20_05_healthz_readyz_all_roles(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    cluster = ctx.node_manager.start_split_cluster()
    try:
        for role, inst in cluster.items():
            rc = PulseClient(inst.base_url)
            hz = rc.get_healthz()
            rz = rc.get_readyz()
            assert hz.status == 200, f"{role} /healthz -> {hz.status}"
            assert rz.status == 200, f"{role} /readyz -> {rz.status}"
    finally:
        for inst in cluster.values():
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


# ===========================================================================
# Feature F21 — Pulse Studio UI (#/agents) & Session Replay Link
# ===========================================================================


@test_case(
    id="T1-F21-01",
    tier=1,
    feature="F21",
    title="Studio SPA root HTML, cache/security headers, and #/agents entry route markers",
    tags=("ui", "studio", "static", "agents"),
)
def test_f21_01_studio_root_and_agents_route(ctx: TestContext) -> None:
    info = ctx.client.check_studio_static(require_agents_route=True)
    assert info["status"] == 200 and info["agents_route_detected"] is True

    root = ctx.client.request("GET", "/")
    assert root.status == 200
    assert '<div id="root"></div>' in root.text()
    assert "no-cache" in (root.header("cache-control") or "").lower(), root.headers
    assert (root.header("x-content-type-options") or "").lower() == "nosniff", root.headers

    for asset in info["assets"]:
        aresp = ctx.client.request("GET", asset)
        assert aresp.status == 200
        assert "immutable" in (aresp.header("cache-control") or "").lower(), (
            f"Asset {asset} missing immutable Cache-Control: {aresp.headers}"
        )

    spa = ctx.client.request("GET", "/agents")
    assert spa.status == 200 and '<div id="root"></div>' in spa.text()
    missing_api = ctx.client.request("GET", "/v1/__nonexistent_route__")
    assert missing_api.status == 404


@test_case(
    id="T1-F21-02",
    tier=1,
    feature="F21",
    title="Studio lazy AgentObservabilityWorkspace chunk delivery and SessionsReplayWorkspace ?session= support",
    tags=("ui", "studio", "chunks", "replay_link"),
)
def test_f21_02_studio_lazy_chunks(ctx: TestContext) -> None:
    info = ctx.client.check_studio_static(require_agents_route=True)
    entry_texts = [ctx.client.request("GET", asset).text(errors="replace") for asset in info["assets"]]
    combined_entry = "\n".join(entry_texts)

    agent_chunks = re.findall(r"AgentObservabilityWorkspace[A-Za-z0-9_.-]*\.js", combined_entry)
    replay_chunks = re.findall(r"SessionsReplayWorkspace[A-Za-z0-9_.-]*\.js", combined_entry)
    assert agent_chunks, "Entry JS does not reference AgentObservabilityWorkspace chunk"
    assert replay_chunks, "Entry JS does not reference SessionsReplayWorkspace chunk"

    agent_resp = ctx.client.request("GET", f"/assets/{agent_chunks[0]}")
    assert agent_resp.status == 200, f"Failed to fetch /assets/{agent_chunks[0]}: {agent_resp.status}"
    agent_js = agent_resp.text(errors="replace").lower()
    for marker in ("sessions", "traces", "datasets", "experiments", "replays"):
        assert marker in agent_js, f"AgentObservabilityWorkspace chunk missing marker {marker!r}"
    assert ("evaluators" in agent_js or "evaluations" in agent_js), (
        "AgentObservabilityWorkspace chunk missing evaluators/evaluations markers"
    )

    replay_resp = ctx.client.request("GET", f"/assets/{replay_chunks[0]}")
    assert replay_resp.status == 200, f"Failed to fetch /assets/{replay_chunks[0]}: {replay_resp.status}"
    replay_js = replay_resp.text(errors="replace")
    assert "session" in replay_js and (
        "URLSearchParams" in replay_js or "replays" in replay_js
    ), "SessionsReplayWorkspace chunk missing ?session= query-param handling"


@test_case(
    id="T1-F21-03",
    tier=1,
    feature="F21",
    title="Agent session to Pulse wireframe replay (#/replays?session=<id>) end-to-end data parity",
    tags=("ui", "studio", "session", "replay"),
)
def test_f21_03_session_to_replay_parity(ctx: TestContext) -> None:
    seed = ctx.unique_id("f21_rep")
    sid = f"sess_{seed}"
    now_ms = int(time.time() * 1000)

    batch = fixtures.make_batch(
        seed=seed, project_id=ctx.proj_a.id, session_id=sid, start_ts_ms=now_ms - 1000
    )
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()

    ctx.client.request(
        "POST",
        "/v1/ingest",
        api_key=ctx.proj_a.pk_key,
        json_body={
            "events": [
                {
                    "event_uuid": f"e2e-{seed}-0001",
                    "routing_seed": f"dev_{seed}",
                    "distinct_id": f"usr_{seed}",
                    "session_id": sid,
                    "event_name": "Agent_Session_Start",
                    "client_event_ts_ms": now_ms - 500,
                    "client_sent_ts_ms": now_ms,
                    "props": {"source": "agent"},
                }
            ]
        },
    ).raise_for_status()

    ctx.client.request(
        "POST",
        "/v1/replay/chunk",
        api_key=ctx.proj_a.pk_key,
        json_body={
            "project_id": ctx.proj_a.id,
            "session_id": sid,
            "chunk_start_ts_ms": now_ms,
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
                            "masked_label": "Agent Session Portal",
                        }
                    ],
                }
            ],
        },
    ).raise_for_status()

    _wait_trace_detail(ctx.client, ctx.proj_a.id, batch["traces"][0]["trace_id"], ctx.viewer_token)

    sess_det = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/sessions/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()
    assert sess_det["summary"]["session_id"] == sid
    assert sess_det["summary"]["trace_count"] >= 1

    replay = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/replay/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()
    assert replay["session_id"] == sid
    assert len(replay["chunks"]) >= 1
    assert any(
        op.get("op") == "full_snapshot"
        for ch in replay["chunks"]
        for op in ch.get("ops", [])
    ), replay


@test_case(
    id="T1-F21-04",
    tier=1,
    feature="F21",
    title="Studio ps_ session full-tab API contract across all 6 #/agents workspace tabs",
    tags=("ui", "studio", "api_contract", "tabs"),
)
def test_f21_04_studio_full_tab_api_contract(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f21_tabs")
    seed = ctx.unique_id("tabs")
    batch = fixtures.make_hierarchy_batch(
        seed=seed, project_id=proj.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    sid = batch["traces"][0]["session_id"]
    ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=f"tab_ev_{seed}", enabled=False),
        token=ctx.member_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluations",
        json_body=fixtures.make_manual_evaluation(
            target_type="trace", session_id=sid, trace_id=tr_id, evaluator_id=f"ev_{seed}"
        ),
        token=ctx.member_token,
    ).raise_for_status()
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"tab_ds_{seed}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets/{ds['id']}/items",
        json_body=fixtures.make_dataset_item(name="tab-item"),
        token=ctx.member_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds["id"], name=f"tab_exp_{seed}"),
        token=ctx.member_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(name=f"tab_al_{seed}"),
        token=ctx.member_token,
    ).raise_for_status()

    endpoints = (
        ("/analytics", {}),
        ("/compare", {"baseline_version": "1.0.0", "candidate_version": "1.0.0"}),
        ("/sessions", {}),
        (f"/sessions/{sid}", {}),
        ("/traces", {}),
        (f"/traces/{tr_id}", {}),
        ("/evaluators", {}),
        ("/evaluations", {}),
        ("/datasets", {}),
        ("/experiments", {}),
        ("/alerts", {}),
        ("/alerts/events", {}),
        ("/privacy", {}),
        ("/pricing", {}),
    )
    responses: dict[str, Any] = {}
    for suffix, query in endpoints:
        r = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent{suffix}",
            query=query or None,
            token=ctx.viewer_token,
        )
        assert r.status == 200, f"GET .../agent{suffix} -> {r.status}: {r.text(errors='replace')[:200]}"
        responses[suffix] = r.json()

    tr_det = responses[f"/traces/{tr_id}"]
    assert len(tr_det["spans"]) == 10
    assert any(s.get("linked_span_ids") for s in tr_det["spans"])
    assert any(s.get("span_phase") == "final" for s in tr_det["spans"])
    assert any(s.get("events") for s in tr_det["spans"])
    assert any("cost_nanos" in s for s in tr_det["spans"])
    assert "retention_days" in responses["/privacy"] and "redact_pii" in responses["/privacy"]


@test_case(
    id="T1-F21-05",
    tier=1,
    feature="F21",
    title="Studio UI RBAC parity (viewer vs member vs admin) and web TypeScript/unit tests",
    tags=("ui", "studio", "rbac", "frontend_tests"),
)
def test_f21_05_studio_rbac_and_web_tests(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f21_rbac")
    ev_payload = fixtures.make_evaluator_config(name=ctx.unique_id("ui_rbac"), enabled=False)
    priv_payload = fixtures.make_privacy_policy(project_id=proj.id)

    assert ctx.client.request(
        "POST", f"/v1/projects/{proj.id}/agent/evaluators", json_body=ev_payload, token=ctx.viewer_token
    ).status == 403
    assert ctx.client.request(
        "PUT", f"/v1/projects/{proj.id}/agent/privacy", json_body=priv_payload, token=ctx.viewer_token
    ).status == 403

    assert ctx.client.request(
        "POST", f"/v1/projects/{proj.id}/agent/evaluators", json_body=ev_payload, token=ctx.member_token
    ).status in (200, 201)
    assert ctx.client.request(
        "PUT", f"/v1/projects/{proj.id}/agent/privacy", json_body=priv_payload, token=ctx.member_token
    ).status == 403

    assert ctx.client.request(
        "PUT", f"/v1/projects/{proj.id}/agent/privacy", json_body=priv_payload, token=ctx.admin_token
    ).status == 200

    workspace_tsx = _REPO_ROOT / "web" / "src" / "workspaces" / "AgentObservabilityWorkspace.tsx"
    assert workspace_tsx.is_file(), f"Missing M4 workspace component: {workspace_tsx}"
    unit_tests = sorted((_REPO_ROOT / "web" / "src" / "lib").glob("agent*.test.ts"))
    assert unit_tests, "Missing web/src/lib/agent*.test.ts unit test files"

    for args in (["npm", "--prefix", "web", "run", "typecheck"], ["npm", "--prefix", "web", "test"]):
        proc = _run_cmd(args, timeout=120.0)
        assert proc.returncode == 0, (
            f"Command {' '.join(args)} failed (rc={proc.returncode}):\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )


# ===========================================================================
# Feature F22 — TypeScript & Python PLST SDKs + examples/agent-demo/
# ===========================================================================


@test_case(
    id="T1-F22-01",
    tier=1,
    feature="F22",
    title="TypeScript SDK (@pulse-analytics/agent) PLST binary context propagation -> live pulse-node query",
    tags=("sdk", "ts", "plst", "context"),
)
def test_f22_01_ts_sdk_plst_context(ctx: TestContext) -> None:
    node_bin = shutil.which("node")
    assert node_bin, "node executable not found on PATH"
    entry = _resolve_ts_sdk_entry()

    seed = ctx.unique_id("ts_sdk")
    sid = f"sess_{seed}"
    tid = f"tr_{seed}"

    js_script = """
import { pathToFileURL } from 'node:url';
const mod = await import(pathToFileURL(process.env.PULSE_SDK_ENTRY).href);
const Ctor = mod.PulseAgentClient || mod.PulseAgent || mod.AgentClient || mod.default;
if (!Ctor) {
  throw new Error('Missing SDK client export in ' + Object.keys(mod).join(','));
}
const client = new Ctor({
  endpoint: process.env.PULSE_BASE_URL,
  baseUrl: process.env.PULSE_BASE_URL,
  apiKey: process.env.PULSE_API_KEY,
  useBinary: true,
  usePlst: true,
});
const tid = process.env.PULSE_TRACE_ID;
const sid = process.env.PULSE_SESSION_ID;
const sess = client.session ? client.session({ sessionId: sid, id: sid }) : null;
const tr = sess && sess.trace
  ? sess.trace({ traceId: tid, agentName: 'ts-sdk-agent', input: { q: 'hello' } })
  : client.trace({ traceId: tid, sessionId: sid, agentName: 'ts-sdk-agent', input: { q: 'hello' } });
const gen = tr.generation({ name: 'llm.chat', model: 'gpt-4o-mini', provider: 'openai', spanPhase: 'start', inputTokens: 80, outputTokens: 25 });
if (gen.update) gen.update({ spanPhase: 'update', output: { partial: 'hi' } });
gen.end({ spanPhase: 'final', output: { text: 'done' }, inputTokens: 80, outputTokens: 25 });
const tool = tr.tool({ name: 'tool.lookup', toolName: 'lookup' });
tool.end({ output: { ok: true } });
const guard = tr.guardrail({ name: 'guardrail.pii' });
guard.end({ output: { safe: true } });
tr.end({ output: { reply: 'done' } });
if (client.flush) await client.flush();
if (client.shutdown) await client.shutdown();
"""
    proc = _run_cmd(
        [node_bin, "--input-type=module", "-e", js_script],
        env={
            "PULSE_SDK_ENTRY": str(entry),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": ctx.proj_a.pk_key,
            "PULSE_TRACE_ID": tid,
            "PULSE_SESSION_ID": sid,
        },
        timeout=30.0,
    )
    assert proc.returncode == 0, (
        f"TS SDK driver failed (rc={proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tid, ctx.viewer_token)
    assert detail["trace"]["session_id"] == sid
    assert detail["trace"]["agent_name"] == "ts-sdk-agent"
    assert detail["trace"]["total_tokens"] > 0
    span_types = {s["span_type"] for s in detail["spans"]}
    assert {"llm", "tool", "guardrail"}.issubset(span_types), span_types
    assert any(s.get("parent_span_id") is not None for s in detail["spans"])

    sess_det = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/sessions/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()
    assert sess_det["summary"]["trace_count"] >= 1


@test_case(
    id="T1-F22-02",
    tier=1,
    feature="F22",
    title="Python SDK (pulse-agent) PLST binary context propagation, client-side redaction & flush/shutdown -> live query",
    tags=("sdk", "py", "plst", "redaction"),
)
def test_f22_02_py_sdk_plst_and_redaction(ctx: TestContext) -> None:
    sdk_dir = _REPO_ROOT / "sdks" / "py-agent"
    assert sdk_dir.is_dir(), f"Missing M5 Python SDK directory: {sdk_dir}"

    seed = ctx.unique_id("py_sdk")
    sid = f"sess_{seed}"
    tid = f"tr_{seed}"

    py_script = """
import os
import pulse_agent
Ctor = getattr(pulse_agent, "PulseAgentClient", None) or getattr(pulse_agent, "PulseAgent", None) or getattr(pulse_agent, "AgentClient", None)
if Ctor is None:
    raise RuntimeError(f"Missing client export in pulse_agent: {dir(pulse_agent)}")
client = Ctor(
    endpoint=os.environ["PULSE_BASE_URL"],
    api_key=os.environ["PULSE_API_KEY"],
    use_binary=True,
    redact_pii=True,
)
with client.trace(
    trace_id=os.environ["PULSE_TRACE_ID"],
    session_id=os.environ["PULSE_SESSION_ID"],
    agent_name="py-sdk-agent",
    input={"prompt": "Contact alice@example.com with sk-proj-secret123"},
) as tr:
    with tr.generation(name="llm.gen", model="gpt-4o-mini", provider="openai", input_tokens=50, output_tokens=20) as gen:
        gen.set_output({"text": "Reply for alice@example.com"})
    with tr.tool(name="tool.search", tool_name="search") as tl:
        tl.set_output({"token": "sk-proj-secret123"})
    with tr.guardrail(name="guardrail.check") as gd:
        gd.set_output({"safe": True})
    tr.set_output({"reply": "Handled securely"})
client.flush()
client.shutdown()
"""
    proc = _run_cmd(
        [sys.executable, "-c", py_script],
        env={
            "PYTHONPATH": str(sdk_dir),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": ctx.proj_a.pk_key,
            "PULSE_TRACE_ID": tid,
            "PULSE_SESSION_ID": sid,
        },
        timeout=30.0,
    )
    assert proc.returncode == 0, (
        f"Python SDK driver failed (rc={proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )

    detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tid, ctx.viewer_token)
    serialized = json.dumps(detail)
    assert "[REDACTED]" in serialized, detail
    assert "sk-proj-secret123" not in serialized
    assert "alice@example.com" not in serialized
    span_types = {s["span_type"] for s in detail["spans"]}
    assert {"llm", "tool", "guardrail"}.issubset(span_types), span_types


@test_case(
    id="T1-F22-03",
    tier=1,
    feature="F22",
    title="SDK JSON fallback, sampling (0.0/1.0), retry-with-jitter, and OpenAI/Anthropic/Gemini/LangChain/RAG adapters",
    tags=("sdk", "ts", "py", "adapters", "fallback"),
)
def test_f22_03_sdk_fallback_sampling_retry_adapters(ctx: TestContext) -> None:
    node_bin = shutil.which("node")
    assert node_bin, "node executable not found on PATH"
    ts_entry = _resolve_ts_sdk_entry()
    py_sdk_dir = _REPO_ROOT / "sdks" / "py-agent"
    assert py_sdk_dir.is_dir(), f"Missing M5 Python SDK directory: {py_sdk_dir}"

    # Local transient 503 -> 200 server for retry verification
    attempts = {"count": 0}
    lock = threading.Lock()

    class _RetryHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length > 0:
                self.rfile.read(length)
            with lock:
                attempts["count"] += 1
                n = attempts["count"]
            if n == 1:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                body = b'{"accepted_traces":1,"accepted_spans":1,"truncated_payloads":0,"redacted_fields":0,"dropped_spans":0,"warnings":[]}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), _RetryHandler)
    srv_thread = threading.Thread(target=srv.serve_forever, daemon=True)
    srv_thread.start()
    try:
        retry_url = f"http://127.0.0.1:{srv.server_address[1]}"
        seed = ctx.unique_id("f22_adp")
        tr_json = f"tr_{seed}_json"
        tr_drop = f"tr_{seed}_drop"
        tr_adp = f"tr_{seed}_adp"

        py_script = """
import os
import pulse_agent
Ctor = getattr(pulse_agent, "PulseAgentClient", None) or getattr(pulse_agent, "PulseAgent", None)
# 1. JSON fallback + sample_rate=1.0
c_json = Ctor(endpoint=os.environ["PULSE_BASE_URL"], api_key=os.environ["PULSE_API_KEY"], use_binary=False, sample_rate=1.0)
with c_json.trace(trace_id=os.environ["TR_JSON"], agent_name="py-json-fallback") as t:
    with t.generation(name="gen", model="gpt-4o-mini", provider="openai", input_tokens=10, output_tokens=5):
        pass
c_json.shutdown()

# 2. Sample rate 0.0 suppression
c_drop = Ctor(endpoint=os.environ["PULSE_BASE_URL"], api_key=os.environ["PULSE_API_KEY"], use_binary=False, sample_rate=0.0)
with c_drop.trace(trace_id=os.environ["TR_DROP"], agent_name="py-dropped"):
    pass
c_drop.shutdown()

# 3. Retry on transient 503
c_retry = Ctor(endpoint=os.environ["RETRY_URL"], api_key=os.environ["PULSE_API_KEY"], max_retries=3, retry_base_ms=20)
with c_retry.trace(trace_id="tr_retry_test", agent_name="py-retry"):
    pass
c_retry.shutdown()

# 4. Adapters (OpenAI/Anthropic/Gemini/LangChain/RAG)
c_adp = Ctor(endpoint=os.environ["PULSE_BASE_URL"], api_key=os.environ["PULSE_API_KEY"], use_binary=True)
adapters_mod = getattr(pulse_agent, "adapters", None)
def _find_adp(*names):
    for n in names:
        fn = getattr(pulse_agent, n, None) or (getattr(adapters_mod, n, None) if adapters_mod else None)
        if fn is not None:
            return fn
    return None

exercised_adapters = 0
with c_adp.trace(trace_id=os.environ["TR_ADP"], agent_name="py-adapters") as t:
    oa = _find_adp("wrap_openai", "openai_adapter")
    if oa is not None:
        fn = lambda **kw: {"usage": {"prompt_tokens": 12, "completion_tokens": 6}, "choices": [{"message": {"content": "ok"}}]}
        res = oa(fn, t)
        if callable(res) and res is not fn:
            res(model="gpt-4o-mini")
        exercised_adapters += 1
    an = _find_adp("wrap_anthropic", "anthropic_adapter")
    if an is not None:
        fn = lambda **kw: {"usage": {"input_tokens": 12, "output_tokens": 6}, "content": [{"text": "ok"}]}
        res = an(fn, t)
        if callable(res) and res is not fn:
            res(model="claude-3-5-sonnet")
        exercised_adapters += 1
    gm = _find_adp("wrap_gemini", "gemini_adapter")
    if gm is not None:
        fn = lambda **kw: {"text": "ok", "usage_metadata": {"prompt_token_count": 10, "candidates_token_count": 4}}
        res = gm(fn, t)
        if callable(res) and res is not fn:
            res(model="gemini-2.0-flash")
        exercised_adapters += 1
    lc = _find_adp("LangChainCallbackHandler")
    if lc is not None:
        cb = lc(t)
        assert cb is not None
        if hasattr(cb, "on_llm_start") and hasattr(cb, "on_llm_end"):
            cb.on_llm_start({"name": "ChatOpenAI"}, ["hello"])
            cb.on_llm_end({"generations": [[{"text": "world"}]]})
        exercised_adapters += 1
    rag = _find_adp("trace_rag_retrieval", "rag_adapter")
    if rag is not None:
        rag(t, query="refund", docs=[{"id": "d1", "score": 0.9}])
        exercised_adapters += 1
assert exercised_adapters >= 1, f"Expected >= 1 Python SDK adapter exercised, got {exercised_adapters}"
c_adp.shutdown()
"""
        py_proc = _run_cmd(
            [sys.executable, "-c", py_script],
            env={
                "PYTHONPATH": str(py_sdk_dir),
                "PULSE_BASE_URL": ctx.base_url,
                "PULSE_API_KEY": ctx.proj_a.pk_key,
                "RETRY_URL": retry_url,
                "TR_JSON": tr_json,
                "TR_DROP": tr_drop,
                "TR_ADP": tr_adp,
            },
            timeout=30.0,
        )
        assert py_proc.returncode == 0, (
            f"Python SDK fallback/retry/adapters failed:\n{py_proc.stdout}\n{py_proc.stderr}"
        )

        # Also verify TS SDK JSON fallback + adapters
        tr_ts_json = f"tr_{seed}_tsjson"
        tr_ts_adp = f"tr_{seed}_tsadp"
        ts_script = """
import { pathToFileURL } from 'node:url';
const mod = await import(pathToFileURL(process.env.PULSE_SDK_ENTRY).href);
const Ctor = mod.PulseAgentClient || mod.PulseAgent || mod.AgentClient || mod.default;
const c = new Ctor({ endpoint: process.env.PULSE_BASE_URL, baseUrl: process.env.PULSE_BASE_URL, apiKey: process.env.PULSE_API_KEY, useBinary: false, usePlst: false });
const tr = c.trace({ traceId: process.env.TR_TS_JSON, agentName: 'ts-json-fallback' });
const g = tr.generation({ name: 'llm.call', model: 'gpt-4o-mini', provider: 'openai', inputTokens: 15, outputTokens: 8 });
g.end({ output: { text: 'ok' } });
tr.end({ output: { text: 'ok' } });

// TS SDK Adapters (OpenAI/Anthropic/Gemini/LangChain/RAG)
const adpMod = mod.adapters || {};
const pickAdp = (...names) => {
  for (const n of names) {
    if (mod[n]) return mod[n];
    if (adpMod[n]) return adpMod[n];
  }
  return null;
};
const trAdp = c.trace({ traceId: process.env.TR_TS_ADP, agentName: 'ts-adapters' });
let tsExercisedAdapters = 0;
const wrapOpenAI = pickAdp('wrapOpenAI', 'wrap_openai', 'openaiAdapter');
if (wrapOpenAI) {
  const fn = async () => ({ usage: { prompt_tokens: 10, completion_tokens: 5 }, choices: [{ message: { content: 'ok' } }] });
  const res = await wrapOpenAI(fn, trAdp);
  if (typeof res === 'function' && res !== fn) await res({ model: 'gpt-4o-mini' });
  tsExercisedAdapters += 1;
}
const wrapAnthropic = pickAdp('wrapAnthropic', 'wrap_anthropic', 'anthropicAdapter');
if (wrapAnthropic) {
  const fn = async () => ({ usage: { input_tokens: 10, output_tokens: 5 }, content: [{ text: 'ok' }] });
  const res = await wrapAnthropic(fn, trAdp);
  if (typeof res === 'function' && res !== fn) await res({ model: 'claude-3-5-sonnet' });
  tsExercisedAdapters += 1;
}
const wrapGemini = pickAdp('wrapGemini', 'wrap_gemini', 'geminiAdapter');
if (wrapGemini) {
  const fn = async () => ({ text: 'ok', usageMetadata: { promptTokenCount: 8, candidatesTokenCount: 4 } });
  const res = await wrapGemini(fn, trAdp);
  if (typeof res === 'function' && res !== fn) await res({ model: 'gemini-2.0-flash' });
  tsExercisedAdapters += 1;
}
const LcHandler = pickAdp('LangChainCallbackHandler');
if (LcHandler) {
  const cb = new LcHandler(trAdp);
  if (cb && typeof cb.handleLLMStart === 'function' && typeof cb.handleLLMEnd === 'function') {
    await cb.handleLLMStart({ name: 'ChatOpenAI' }, ['hello']);
    await cb.handleLLMEnd({ generations: [[{ text: 'world' }]] });
  } else if (cb && typeof cb.onLlmStart === 'function' && typeof cb.onLlmEnd === 'function') {
    await cb.onLlmStart({ name: 'ChatOpenAI' }, ['hello']);
    await cb.onLlmEnd({ generations: [[{ text: 'world' }]] });
  }
  tsExercisedAdapters += 1;
}
const traceRag = pickAdp('traceRagRetrieval', 'trace_rag_retrieval', 'ragAdapter');
if (traceRag) {
  if (traceRag.length >= 3) {
    await traceRag(trAdp, 'refund', [{ id: 'd1', score: 0.9 }]);
  } else {
    await traceRag(trAdp, { query: 'refund', docs: [{ id: 'd1', score: 0.9 }] });
  }
  tsExercisedAdapters += 1;
}
if (tsExercisedAdapters < 1) {
  throw new Error(`Expected >= 1 TS SDK adapter exercised, got ${tsExercisedAdapters}`);
}
trAdp.end({ output: { text: 'adapters-ok' } });
if (c.flush) await c.flush();
if (c.shutdown) await c.shutdown();
"""
        ts_proc = _run_cmd(
            [node_bin, "--input-type=module", "-e", ts_script],
            env={
                "PULSE_SDK_ENTRY": str(ts_entry),
                "PULSE_BASE_URL": ctx.base_url,
                "PULSE_API_KEY": ctx.proj_a.pk_key,
                "TR_TS_JSON": tr_ts_json,
                "TR_TS_ADP": tr_ts_adp,
            },
            timeout=30.0,
        )
        assert ts_proc.returncode == 0, (
            f"TS SDK JSON fallback/adapters failed:\n{ts_proc.stdout}\n{ts_proc.stderr}"
        )

        assert attempts["count"] >= 2, f"Expected retry (>=2 attempts), got {attempts['count']}"
        _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_json, ctx.viewer_token)
        _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_ts_json, ctx.viewer_token)
        adp_detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_adp, ctx.viewer_token)
        assert len(adp_detail["spans"]) >= 1, (
            f"Expected >= 1 span from Python SDK adapters, got {adp_detail}"
        )
        ts_adp_detail = _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_ts_adp, ctx.viewer_token)
        assert len(ts_adp_detail["spans"]) >= 1, (
            f"Expected >= 1 span from TS SDK adapters, got {ts_adp_detail}"
        )
        assert ctx.client.request(
            "GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_drop}", token=ctx.viewer_token
        ).status == 404
    finally:
        srv.shutdown()
        srv.server_close()


@test_case(
    id="T1-F22-04",
    tier=1,
    feature="F22",
    title="TS and Python SDK zero-runtime-dependency contracts, tsc typecheck, and native SDK unit test suites",
    tags=("sdk", "unit_tests", "typecheck", "zero_deps"),
)
def test_f22_04_sdk_zero_deps_and_unit_tests(ctx: TestContext) -> None:
    ts_pkg = _REPO_ROOT / "sdks" / "ts-agent" / "package.json"
    py_proj = _REPO_ROOT / "sdks" / "py-agent" / "pyproject.toml"
    assert ts_pkg.is_file(), f"Missing {ts_pkg}"
    assert py_proj.is_file(), f"Missing {py_proj}"

    pkg_data = json.loads(ts_pkg.read_text(encoding="utf-8"))
    assert not pkg_data.get("dependencies"), (
        f"TS SDK must have zero runtime dependencies, got {pkg_data.get('dependencies')}"
    )

    py_data = tomllib.loads(py_proj.read_text(encoding="utf-8"))
    py_deps = py_data.get("project", {}).get("dependencies", [])
    assert not py_deps, f"Python SDK must have zero runtime dependencies, got {py_deps}"

    for cmd in (
        ["npm", "--prefix", "sdks/ts-agent", "run", "typecheck"],
        ["npm", "--prefix", "sdks/ts-agent", "test"],
        [sys.executable, "-m", "unittest", "discover", "-s", "sdks/py-agent/tests"],
    ):
        proc = _run_cmd(cmd, timeout=120.0)
        assert proc.returncode == 0, (
            f"SDK verification command {' '.join(cmd)} failed (rc={proc.returncode}):\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )


@test_case(
    id="T1-F22-05",
    tier=1,
    feature="F22",
    title="Runnable examples/agent-demo/ end-to-end execution against live pulse-node",
    tags=("examples", "agent_demo", "e2e"),
)
def test_f22_05_agent_demo_e2e(ctx: TestContext) -> None:
    demo_dir = _REPO_ROOT / "examples" / "agent-demo"
    assert demo_dir.is_dir(), f"Missing M5 example directory: {demo_dir}"

    proj = _create_temp_project(ctx, "f22_demo")
    env = {
        "PULSE_BASE_URL": ctx.base_url,
        "PULSE_ENDPOINT": ctx.base_url,
        "PULSE_API_KEY": proj.pk_key,
        "OPENAI_BASE_URL": ctx.mock_llm_url or "",
        "PYTHONPATH": str(_REPO_ROOT / "sdks" / "py-agent"),
    }

    cmd: list[str] | None = None
    if (demo_dir / "package.json").is_file():
        cmd = ["npm", "--prefix", str(demo_dir), "start"]
    elif (demo_dir / "main.py").is_file():
        cmd = [sys.executable, str(demo_dir / "main.py")]
    elif (demo_dir / "demo.py").is_file():
        cmd = [sys.executable, str(demo_dir / "demo.py")]
    elif (demo_dir / "index.mjs").is_file():
        cmd = ["node", str(demo_dir / "index.mjs")]
    elif (demo_dir / "run.sh").is_file():
        cmd = ["bash", str(demo_dir / "run.sh")]

    assert cmd is not None, f"No runnable entrypoint found in {demo_dir}"
    proc = _run_cmd(cmd, cwd=demo_dir, env=env, timeout=60.0)
    assert proc.returncode == 0, (
        f"examples/agent-demo failed (rc={proc.returncode}):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )

    def _has_demo_traces() -> list[dict[str, Any]] | None:
        resp = ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token
        )
        if resp.status != 200:
            return None
        items = _extract_items(resp.json(), ("traces", "items"))
        return items if items else None

    traces = _poll_until(_has_demo_traces, timeout=10.0, desc="agent-demo traces")
    assert any(t.get("span_count", 0) >= 2 for t in traces), traces
    sessions = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("sessions", "items"),
    )
    assert len(sessions) >= 1
    ana = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token
    ).raise_for_status().json()
    assert ana["total_executions"] >= 1, ana


# ===========================================================================
# Feature F23 — Documentation & API Contract Consistency
# ===========================================================================


@test_case(
    id="T1-F23-01",
    tier=1,
    feature="F23",
    title="docs/rest-api.md agent route documentation and live HTTP endpoint parity",
    tags=("docs", "rest_api", "contract"),
)
def test_f23_01_rest_api_doc_parity(ctx: TestContext) -> None:
    doc_path = _REPO_ROOT / "docs" / "rest-api.md"
    assert doc_path.is_file(), f"Missing {doc_path}"
    text = doc_path.read_text(encoding="utf-8")

    required_tokens = (
        "/v1/agent/traces",
        "/v1/otel/v1/traces",
        "x-pulse-key",
        "Authorization: Bearer",
        "/agent/sessions",
        "/agent/traces",
        "/agent/analytics",
        "/agent/compare",
        "/agent/evaluators",
        "/agent/evaluations",
        "/agent/datasets",
        "/agent/experiments",
        "/agent/alerts",
        "/agent/privacy",
        "/agent/pricing",
        "/promote",
    )
    for tok in required_tokens:
        assert tok in text, f"docs/rest-api.md missing documented token {tok!r}"

    live_probes = (
        ("POST", "/v1/agent/traces", None, None),
        ("POST", "/v1/otel/v1/traces", None, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/sessions", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/analytics", ctx.viewer_token, None),
        (
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/compare",
            ctx.viewer_token,
            {"baseline_version": "v1", "candidate_version": "v2"},
        ),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/evaluators", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/evaluations", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/datasets", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/experiments", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/alerts", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/privacy", ctx.viewer_token, None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/pricing", ctx.viewer_token, None),
    )
    for method, path, token, query in live_probes:
        resp = ctx.client.request(method, path, token=token, query=query)
        assert resp.status != 404, f"Documented route {method} {path} returned 404"


@test_case(
    id="T1-F23-02",
    tier=1,
    feature="F23",
    title="docs/mcp.md agent MCP tool docs and live POST /v1/mcp tools/list schema parity",
    tags=("docs", "mcp", "contract"),
)
def test_f23_02_mcp_doc_parity(ctx: TestContext) -> None:
    doc_path = _REPO_ROOT / "docs" / "mcp.md"
    assert doc_path.is_file(), f"Missing {doc_path}"
    text = doc_path.read_text(encoding="utf-8")

    for tool_name in (*_CORE_MCP_TOOLS, *_AGENT_MCP_TOOLS):
        assert tool_name in text, f"docs/mcp.md missing tool {tool_name!r}"
    assert "x-pulse-project" in text and "sk_live_" in text

    rpc = ctx.client.mcp_call(
        "tools/list", {}, token=ctx.viewer_token, project_id=ctx.proj_a.id
    ).raise_for_status().json()
    tools = {t["name"]: t for t in rpc["result"]["tools"]}
    for name in _AGENT_MCP_TOOLS:
        assert name in tools, f"Live MCP tools/list missing {name!r}"
        assert tools[name]["inputSchema"].get("type") == "object"


@test_case(
    id="T1-F23-03",
    tier=1,
    feature="F23",
    title="docs/agent-observability-architecture.md binary PLST/PTWL/PLSS + SQL split parity with PULSE_DATA_DIR",
    tags=("docs", "architecture", "storage_parity"),
)
def test_f23_03_architecture_doc_storage_parity(ctx: TestContext) -> None:
    doc_path = _REPO_ROOT / "docs" / "agent-observability-architecture.md"
    assert doc_path.is_file(), f"Missing {doc_path}"
    text = doc_path.read_text(encoding="utf-8")

    for marker in (
        "PLST",
        "PTWL",
        "PLSS",
        "AGENT_DICT_ID",
        "agent_eval_rollups",
        "agent_erasure_tombstones",
    ):
        assert marker in text, f"docs/agent-observability-architecture.md missing {marker!r}"
    assert "CREATE TABLE IF NOT EXISTS agent_traces" not in text
    assert "CREATE TABLE IF NOT EXISTS agent_spans" not in text

    batch = fixtures.make_batch(
        seed=ctx.unique_id("f23_arch"), project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, batch["traces"][0]["trace_id"], ctx.viewer_token)

    tables = set(sqlite_tables(ctx.data_dir))
    assert "agent_traces" not in tables and "agent_spans" not in tables
    assert "agent_eval_rollups" in tables and "agent_erasure_tombstones" in tables
    assert ctx.data_dir is not None
    artifacts = list_agent_artifacts(ctx.data_dir)
    assert any(str(p).endswith(".ptwl") for p in artifacts["agent_wal"]) or artifacts["agent_segments"], artifacts


@test_case(
    id="T1-F23-04",
    tier=1,
    feature="F23",
    title="README.md, docs/README.md, and examples/README.md agent observability & SDK documentation consistency",
    tags=("docs", "readme", "examples"),
)
def test_f23_04_readme_and_doc_links(ctx: TestContext) -> None:
    root_readme = _REPO_ROOT / "README.md"
    docs_readme = _REPO_ROOT / "docs" / "README.md"
    ex_readme = _REPO_ROOT / "examples" / "README.md"
    for p in (root_readme, docs_readme, ex_readme):
        assert p.is_file(), f"Missing doc file {p}"

    root_text = root_readme.read_text(encoding="utf-8")
    assert "No server SDKs yet" not in root_text, "Stale 'No server SDKs yet' remains in README.md"
    for marker in ("#/agents", "sdks/ts-agent", "sdks/py-agent", "/v1/agent/traces"):
        assert marker in root_text, f"README.md missing {marker!r}"

    docs_text = docs_readme.read_text(encoding="utf-8")
    assert "agent-observability-architecture.md" in docs_text

    ex_text = ex_readme.read_text(encoding="utf-8")
    assert "agent-demo" in ex_text

    link_re = re.compile(r"\[[^\]]+\]\(([^)#\s]+)")
    for md_file, content in ((root_readme, root_text), (docs_readme, docs_text), (ex_readme, ex_text)):
        for target in link_re.findall(content):
            if "://" in target or target.startswith("mailto:"):
                continue
            resolved = (md_file.parent / target).resolve()
            assert resolved.exists(), f"Broken relative link {target!r} in {md_file}"


@test_case(
    id="T1-F23-05",
    tier=1,
    feature="F23",
    title="Documented CLI flags (pulse-node --help), /healthz, /readyz, and /metrics contract parity",
    tags=("docs", "cli", "metrics", "health"),
)
def test_f23_05_cli_health_metrics_parity(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    bin_path = ctx.node_manager.resolve_binary()
    proc = _run_cmd([str(bin_path), "--help"], timeout=15.0)
    assert proc.returncode == 0, f"pulse-node --help failed: {proc.stderr}"
    help_text = proc.stdout + proc.stderr
    for flag in ("--role", "--host", "--port", "--data-dir", "--web-dir", "--project-id", "--no-quic", "--seed"):
        assert flag in help_text, f"pulse-node --help missing flag {flag!r}"

    assert ctx.client.get_healthz().status == 200
    assert ctx.client.get_readyz().status == 200
    m = ctx.client.get_metrics()
    assert m.status == 200
    m_text = m.text()
    for metric_name in _AGENT_METRICS:
        assert metric_name in m_text, f"/metrics missing {metric_name!r}"
    assert "project_id=" not in m_text, "/metrics must not expose tenant project_id labels"


# ===========================================================================
# Feature F24 — End-to-End Acceptance & Adversarial Integration
# ===========================================================================


@test_case(
    id="T1-F24-01",
    tier=1,
    feature="F24",
    title="pulse-node --seed --no-quic boots cleanly and serves seeded agent sessions, traces, evals, datasets & experiments",
    tags=("acceptance", "seed", "cli", "studio"),
)
def test_f24_01_seeded_acceptance(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    seeded = ctx.node_manager.start_seeded()
    try:
        sc = PulseClient(seeded.base_url)
        tok = seeded.root_admin_token()
        assert sc.get_readyz().status == 200
        assert sc.request("GET", "/v1/status", token=tok).status == 200

        pid = "proj_fintech_in"
        sessions = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/sessions", token=tok).raise_for_status().json(),
            ("sessions", "items"),
        )
        traces = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/traces", token=tok).raise_for_status().json(),
            ("traces", "items"),
        )
        evals = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/evaluations", token=tok).raise_for_status().json(),
            ("evaluations", "items"),
        )
        datasets = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/datasets", token=tok).raise_for_status().json(),
            ("datasets", "items"),
        )
        exps = _extract_items(
            sc.request("GET", f"/v1/projects/{pid}/agent/experiments", token=tok).raise_for_status().json(),
            ("experiments", "items"),
        )
        ana = sc.request("GET", f"/v1/projects/{pid}/agent/analytics", token=tok).raise_for_status().json()

        assert sessions and traces and evals and datasets and exps
        assert ana["total_executions"] > 0, ana
        assert len({e.get("target_type") for e in evals}) >= 2 or len(evals) >= 3
        sc.check_studio_static(require_agents_route=True)
    finally:
        ctx.node_manager.stop(seeded, graceful=True, timeout=10.0)


@test_case(
    id="T1-F24-02",
    tier=1,
    feature="F24",
    title="Existing ./scripts/verify_e2e.sh 9 core Pulse analytics gates pass with zero regression against seeded node",
    tags=("acceptance", "verify_e2e", "regression"),
)
def test_f24_02_verify_e2e_zero_regression(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    seeded = ctx.node_manager.start_seeded()
    try:
        sc = PulseClient(seeded.base_url)
        tok = seeded.root_admin_token()
        api_key = seeded.extract_boot_ingest_key()
        if not api_key:
            kresp = sc.request(
                "POST",
                "/v1/projects/proj_fintech_in/api-keys",
                json_body={"label": "verify_e2e_ingest", "role": "ingest_and_flags"},
                token=tok,
            ).raise_for_status().json()
            api_key = str(kresp["plaintext_key"])

        proc = _run_cmd(
            ["bash", str(_REPO_ROOT / "scripts" / "verify_e2e.sh")],
            env={
                "PULSE_BASE_URL": seeded.base_url,
                "PULSE_ADMIN_TOKEN": tok,
                "PULSE_API_KEY": api_key,
                "PULSE_PROJECT_ID": "proj_fintech_in",
            },
            timeout=120.0,
        )
        assert proc.returncode == 0, (
            f"verify_e2e.sh failed (rc={proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
        assert "ALL 9 E2E VERIFICATION GATES PASSED" in proc.stdout, proc.stdout
    finally:
        ctx.node_manager.stop(seeded, graceful=True, timeout=10.0)


@test_case(
    id="T1-F24-03",
    tier=1,
    feature="F24",
    title="Full-stack acceptance journey: PLST + OTLP/JSON + OTLP/Protobuf -> Eval -> Promote -> Dataset -> Experiment -> MCP -> Erasure -> /metrics",
    tags=("acceptance", "full_stack", "e2e"),
)
def test_f24_03_full_stack_acceptance(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "f24_full")
    seed = ctx.unique_id("full")

    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=f"full_noerr_{seed}", kind="no_errors", enabled=True),
        token=ctx.member_token,
    ).raise_for_status().json()

    recent_ts = _recent_ts_ms()
    plst_b = fixtures.make_hierarchy_batch(
        seed=f"{seed}_p", project_id=proj.id, start_ts_ms=recent_ts
    )
    otlp_j = fixtures.make_otlp_export_request(seed=f"{seed}_oj", start_ts_ms=recent_ts)
    otlp_pb = fixtures.make_otlp_export_request(seed=f"{seed}_opb", start_ts_ms=recent_ts)

    ctx.client.ingest_plst_batch(plst_b, api_key=proj.pk_key, use_dict=True).raise_for_status()
    ctx.client.ingest_otlp_json(otlp_j, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_otlp_pb(otlp_pb, api_key=proj.pk_key).raise_for_status()

    tr_id = plst_b["traces"][0]["trace_id"]
    _wait_trace_detail(ctx.client, proj.id, tr_id, ctx.viewer_token)
    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"trace_id": tr_id}, min_count=1)
    assert any(e["evaluator_id"] == ev_cfg["id"] and e["passed"] is True for e in evals)

    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"full_ds_{seed}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    prom = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/traces/{tr_id}/promote",
        json_body={"dataset_id": ds["id"]},
        token=ctx.member_token,
    ).raise_for_status().json()
    item_id = prom["id"]

    exp = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds["id"], name=f"full_exp_{seed}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/experiments/{exp['id']}/runs",
        json_body=fixtures.make_experiment_run(dataset_item_id=item_id, trace_id=tr_id),
        token=ctx.member_token,
    ).raise_for_status()

    mcp_tr = ctx.client.mcp_tool_call(
        "get_agent_trace", {"trace_id": tr_id}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()
    assert not mcp_tr["result"].get("isError")
    mcp_ana = ctx.client.mcp_tool_call(
        "query_agent_analytics", {"days": 7}, req_id=2, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()
    assert mcp_ana["result"]["structuredContent"]["total_executions"] >= 4

    ctx.client.request(
        "DELETE", f"/v1/projects/{proj.id}/agent/traces/{tr_id}", token=ctx.admin_token
    ).raise_for_status()
    _wait_trace_gone(ctx.client, proj.id, tr_id, ctx.viewer_token)

    metrics_text = ctx.client.get_metrics().raise_for_status().text()
    assert "pulse_agent_traces_ingested_total" in metrics_text
    assert "pulse_agent_evals_executed_total" in metrics_text

    tables = set(sqlite_tables(ctx.data_dir))
    assert "agent_traces" not in tables and "agent_spans" not in tables


@test_case(
    id="T1-F24-04",
    tier=1,
    feature="F24",
    title="Multi-org and cross-project tenant isolation acceptance across REST, Ingest, and MCP",
    tags=("acceptance", "tenant_isolation", "security"),
)
def test_f24_04_tenant_isolation_acceptance(ctx: TestContext) -> None:
    seed = ctx.unique_id("f24_iso")
    batch = fixtures.make_batch(
        seed=seed, project_id=ctx.proj_a.id, start_ts_ms=_recent_ts_ms()
    )
    tr_id = batch["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, ctx.proj_a.id, tr_id, ctx.viewer_token)

    ev = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=f"iso_ev_{seed}", enabled=False),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"iso_ds_{seed}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    al = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(name=f"iso_al_{seed}", enabled=False),
        token=ctx.member_token,
    ).raise_for_status().json()

    try:
        # 1. proj_b listings must never contain proj_a resources
        b_traces = {
            t["trace_id"]
            for t in _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{ctx.proj_b.id}/agent/traces", token=ctx.viewer_token
                ).raise_for_status().json(),
                ("traces", "items"),
            )
        }
        assert tr_id not in b_traces
        b_evals = {
            e["id"]
            for e in _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{ctx.proj_b.id}/agent/evaluators", token=ctx.viewer_token
                ).raise_for_status().json(),
                ("evaluators", "items"),
            )
        }
        assert ev["id"] not in b_evals
        b_ds = {
            d["id"]
            for d in _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{ctx.proj_b.id}/agent/datasets", token=ctx.viewer_token
                ).raise_for_status().json(),
                ("datasets", "items"),
            )
        }
        assert ds["id"] not in b_ds
        b_al = {
            r["id"]
            for r in _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{ctx.proj_b.id}/agent/alerts", token=ctx.viewer_token
                ).raise_for_status().json(),
                ("rules", "alerts", "items"),
            )
        }
        assert al["id"] not in b_al

        # 2. other_org_token rejected (403/404) on all proj_a routes
        for path in ("/sessions", "/traces", f"/traces/{tr_id}", "/analytics", "/evaluators", "/datasets", "/alerts"):
            r = ctx.client.request(
                "GET", f"/v1/projects/{ctx.proj_a.id}/agent{path}", token=ctx.other_org_token
            )
            assert r.status in (403, 404), f"other_org_token got {r.status} on {path}"

        # 3. proj_b.sk_key rejected (403) on proj_a REST and MCP
        assert ctx.client.request(
            "GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces", token=ctx.proj_b.sk_key
        ).status == 403
        mcp_resp = ctx.client.mcp_tool_call(
            "list_agent_traces", {}, token=ctx.proj_b.sk_key, project_id=ctx.proj_a.id
        )
        assert mcp_resp.status == 403, f"Expected 403 for cross-project sk_key on MCP, got {mcp_resp.status}"
    finally:
        ctx.client.request(
            "DELETE", f"/v1/projects/{ctx.proj_a.id}/agent/alerts/{al['id']}", token=ctx.member_token
        )
        ctx.client.request(
            "DELETE", f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds['id']}", token=ctx.member_token
        )
        ctx.client.request(
            "DELETE", f"/v1/projects/{ctx.proj_a.id}/agent/evaluators/{ev['id']}", token=ctx.member_token
        )


@test_case(
    id="T1-F24-05",
    tier=1,
    feature="F24",
    title="Crash-safe restart persistence (PTWL WAL + MetaStore) and graceful shutdown health/readiness acceptance",
    tags=("acceptance", "persistence", "restart"),
)
def test_f24_05_restart_persistence_acceptance(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    n1 = ctx.node_manager.start_all()
    n2 = None
    try:
        c1 = PulseClient(n1.base_url)
        assert c1.get_healthz().status == 200 and c1.get_readyz().status == 200
        tok = n1.root_admin_token()
        proj = _create_temp_project(ctx, "f24_persist", client=c1, token=tok)

        seed = ctx.unique_id("persist")
        batch = fixtures.make_batch(seed=seed, project_id=proj.id, start_ts_ms=_recent_ts_ms())
        tr_id = batch["traces"][0]["trace_id"]
        sid = batch["traces"][0]["session_id"]

        c1.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=True).raise_for_status()
        det1 = _wait_trace_detail(c1, proj.id, tr_id, tok)

        ev = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(name=f"p_ev_{seed}", enabled=False),
            token=tok,
        ).raise_for_status().json()
        c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluations",
            json_body=fixtures.make_manual_evaluation(
                target_type="trace", session_id=sid, trace_id=tr_id, evaluator_id=ev["id"]
            ),
            token=tok,
        ).raise_for_status()

        n2 = ctx.node_manager.restart(n1, graceful_stop=True)
        c2 = PulseClient(n2.base_url)
        assert c2.get_readyz().status == 200

        det2 = _wait_trace_detail(c2, proj.id, tr_id, tok)
        assert det2["trace"]["trace_id"] == tr_id
        assert det2["trace"]["total_cost_nanos"] == det1["trace"]["total_cost_nanos"]
        assert len(det2["spans"]) == len(det1["spans"])
        assert any(e["evaluator_id"] == ev["id"] for e in det2["evaluations"]), det2

        artifacts = list_agent_artifacts(n2.data_dir)
        assert artifacts["agent_wal"] or artifacts["agent_segments"], artifacts
        tables = set(sqlite_tables(n2.data_dir))
        assert "agent_traces" not in tables and "agent_spans" not in tables
    finally:
        if n2 is not None:
            ctx.node_manager.stop(n2, graceful=True, timeout=10.0)
        else:
            ctx.node_manager.stop(n1, graceful=False, timeout=5.0)
