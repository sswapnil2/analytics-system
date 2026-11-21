"""Tier 2 Opaque-Box E2E Tests: Boundary, Negative, RBAC, Tenancy & Corner Cases (F13-F24).

Covers Features F13 through F24 with >= 5 isolated, self-contained tests per feature (>= 60 total).
Pure Python 3.14 stdlib only. Never mutates shared `ctx.proj_a` permanently and never stops/restarts
the shared `ctx` node (`ctx.base_url`); uses throwaway projects or `ctx.node_manager` isolated nodes.
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import math
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

from lib import fixtures
from lib.auth import ProjectCreds, TestContext
from lib.client import PulseClient
from lib.node_mgr import list_agent_artifacts, sqlite_tables
from lib.registry import test_case

REPO_ROOT = Path(__file__).resolve().parents[3]

EXPECTED_META_TABLES: tuple[str, ...] = (
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

FORBIDDEN_DATA_TABLES: tuple[str, ...] = (
    "agent_traces",
    "agent_spans",
)


# ---------------------------------------------------------------------------
# Local Helpers (pure stdlib, strict isolation)
# ---------------------------------------------------------------------------


def _poll_until(
    predicate: Callable[[], Any],
    *,
    timeout: float = 12.0,
    interval: float = 0.2,
    desc: str = "condition",
) -> Any:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            res = predicate()
            if res:
                return res
        except Exception as exc:  # noqa: BLE001 - retry transient poll checks
            last_err = exc
        time.sleep(interval)
    extra = f" (last error: {last_err})" if last_err else ""
    raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}{extra}")


def _create_throwaway_project(ctx: TestContext, prefix: str) -> ProjectCreds:
    name = ctx.unique_id(prefix)
    resp = ctx.client.request(
        "POST",
        "/v1/projects",
        json_body={"name": name, "timezone": "UTC"},
        token=ctx.owner_token,
    ).raise_for_status()
    data = resp.json()
    pid = str(data["project"]["id"])
    pk_key = str(data["api_key"]["plaintext_key"])

    sk_resp = ctx.client.request(
        "POST",
        f"/v1/projects/{pid}/api-keys",
        json_body={"label": f"{name} admin", "role": "admin"},
        token=ctx.owner_token,
    ).raise_for_status()
    sk_key = str(sk_resp.json()["plaintext_key"])
    return ProjectCreds(id=pid, name=name, pk_key=pk_key, sk_key=sk_key)


def _setup_isolated_project(client: PulseClient, root_token: str, name: str) -> ProjectCreds:
    resp = client.request(
        "POST",
        "/v1/projects",
        json_body={"name": name, "timezone": "UTC"},
        token=root_token,
    ).raise_for_status()
    data = resp.json()
    pid = str(data["project"]["id"])
    pk_key = str(data["api_key"]["plaintext_key"])

    sk_resp = client.request(
        "POST",
        f"/v1/projects/{pid}/api-keys",
        json_body={"label": f"{name} admin", "role": "admin"},
        token=root_token,
    ).raise_for_status()
    sk_key = str(sk_resp.json()["plaintext_key"])
    return ProjectCreds(id=pid, name=name, pk_key=pk_key, sk_key=sk_key)


def _wait_trace_visible(
    client: PulseClient,
    pid: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 12.0,
) -> dict[str, Any]:
    def _fetch() -> dict[str, Any] | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{pid}/agent/traces/{trace_id}",
            token=token,
        )
        if resp.status == 200:
            body = resp.json()
            if isinstance(body, dict) and isinstance(body.get("trace"), dict):
                return body
        return None

    return _poll_until(_fetch, timeout=timeout, interval=0.15, desc=f"trace {trace_id}")


def _erase_agent_target(
    client: PulseClient,
    pid: str,
    token: str,
    level: str,
    target_id: str,
) -> int:
    if level == "trace":
        resp = client.request(
            "DELETE",
            f"/v1/projects/{pid}/agent/traces/{target_id}",
            token=token,
        )
        assert resp.status in (200, 202, 204), f"Trace erasure failed: {resp.status} {resp.text(errors='replace')}"
        return resp.status

    erasure_resp = client.request(
        "POST",
        f"/v1/projects/{pid}/agent/erasure",
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    if erasure_resp.status not in (404, 405):
        assert erasure_resp.status in (200, 202, 204), (
            f"POST erasure ({level}) failed: {erasure_resp.status} {erasure_resp.text(errors='replace')}"
        )
        return erasure_resp.status

    del_resp = client.request(
        "DELETE",
        f"/v1/projects/{pid}/agent/traces/{target_id}",
        query={"level": level, "scope": level},
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    assert del_resp.status in (200, 202, 204), (
        f"DELETE erasure ({level}) failed: {del_resp.status} {del_resp.text(errors='replace')}"
    )
    return del_resp.status


def _configure_byok_llm(client: PulseClient, pid: str, token: str, endpoint_url: str) -> None:
    resp = client.request(
        "PUT",
        f"/v1/projects/{pid}/byok",
        json_body={
            "category": "llm",
            "provider": "openai",
            "label": "E2E Mock Judge",
            "secret": "sk-mock-e2e-judge-key-123456",
            "config_json": {"endpoint": endpoint_url, "model": "gpt-4o-mini"},
        },
        token=token,
    )
    resp.raise_for_status()


def _configure_byok_webhook(
    client: PulseClient,
    pid: str,
    token: str,
    webhook_url: str,
    signing_secret: str,
) -> None:
    resp = client.request(
        "PUT",
        f"/v1/projects/{pid}/byok",
        json_body={
            "category": "webhook",
            "provider": "custom",
            "label": "E2E Mock Webhook",
            "secret": webhook_url,
            "config_json": {"signing_secret": signing_secret},
        },
        token=token,
    )
    resp.raise_for_status()


def _assert_no_nan_inf(obj: Any, path: str = "root") -> None:
    if isinstance(obj, float):
        assert math.isfinite(obj), f"Non-finite float {obj!r} at {path}"
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _assert_no_nan_inf(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            _assert_no_nan_inf(v, f"{path}[{idx}]")


def _unused_closed_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _extract_items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            val = payload.get(key)
            if isinstance(val, list):
                return val
    raise AssertionError(f"Expected list or dict with one of {keys}, got {type(payload).__name__}: {payload!r}")


class _MockFlakyIngestServer:
    """Pure-stdlib loopback HTTP server for testing SDK retry/fallback/redaction/sampling."""

    def __init__(self, scripted_statuses: list[int] | None = None, retry_after: str = "1") -> None:
        self.scripted_statuses = list(scripted_statuses or [])
        self.retry_after = retry_after
        self._lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        port = int(self._server.server_address[1])
        return f"http://127.0.0.1:{port}"

    def start(self) -> _MockFlakyIngestServer:
        parent = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length > 0 else b""
                headers_lower = {k.lower(): v for k, v in self.headers.items()}
                with parent._lock:
                    parent.requests.append(
                        {
                            "path": self.path,
                            "headers": headers_lower,
                            "body": raw,
                            "ts": time.monotonic(),
                        }
                    )
                    status = parent.scripted_statuses.pop(0) if parent.scripted_statuses else 200

                if status == 429:
                    body = b'{"status":"error","code":"rate_limited","message":"slow down"}'
                elif status >= 400:
                    body = json.dumps({"status": "error", "message": f"mock {status}"}).encode("utf-8")
                else:
                    body = (
                        b'{"accepted_traces":1,"accepted_spans":1,"truncated_payloads":0,'
                        b'"redacted_fields":0,"dropped_spans":0,"warnings":[]}'
                    )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                if status == 429:
                    self.send_header("Retry-After", parent.retry_after)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        class _ReuseServer(ThreadingHTTPServer):
            daemon_threads = True
            allow_reuse_address = True

        self._server = _ReuseServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None


# ---------------------------------------------------------------------------
# F13: Slimmed 0011_agent_observability.sql & MetaStore Split
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F13-01",
    tier=2,
    feature="F13",
    title="Zero agent_traces/agent_spans SQLite tables after heavy multi-protocol ingest",
    tags=("sqlite", "schema", "metastore", "binary-split"),
)
def test_f13_zero_trace_span_sqlite_tables_after_heavy_ingest(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('f13')}"
    inst = ctx.node_manager.start_node(role="all", admin_token=root_tok)
    try:
        client = PulseClient(inst.base_url)
        proj = _setup_isolated_project(client, root_tok, ctx.unique_id("f13_schema"))

        for i in range(6):
            batch = fixtures.make_hierarchy_batch(seed=ctx.unique_id(f"plst_{i}"), project_id=proj.id)
            client.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=(i % 2 == 0)).raise_for_status()
            jbatch = fixtures.make_batch(seed=ctx.unique_id(f"json_{i}"), project_id=proj.id)
            client.ingest_json(jbatch, api_key=proj.pk_key).raise_for_status()
            otlp = fixtures.make_otlp_export_request(seed=ctx.unique_id(f"otlp_{i}"))
            client.ingest_otlp_json(otlp, api_key=proj.pk_key).raise_for_status()

        tables = sqlite_tables(inst.data_dir)
        for forbidden in FORBIDDEN_DATA_TABLES:
            assert forbidden not in tables, f"Forbidden SQL table {forbidden!r} found in {tables}"
        for expected in EXPECTED_META_TABLES:
            assert expected in tables, f"Missing expected control-plane table {expected!r} in {tables}"

        artifacts = list_agent_artifacts(inst.data_dir)
        assert len(artifacts["agent_wal"]) > 0, "Expected PTWL WAL artifacts in object_store/agent_wal"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F13-02",
    tier=2,
    feature="F13",
    title="Session-level evaluation persists nullable trace_id/span_id as NULL in SQLite",
    tags=("sqlite", "evaluations", "session", "nullability"),
)
def test_f13_session_evaluation_nullable_trace_and_span_ids(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('f13_null')}"
    inst = ctx.node_manager.start_node(role="all", admin_token=root_tok)
    try:
        client = PulseClient(inst.base_url)
        proj = _setup_isolated_project(client, root_tok, ctx.unique_id("f13_null"))
        sess_id = ctx.unique_id("sess_only")
        tr_id = ctx.unique_id("tr_sess")
        batch = fixtures.make_batch(seed=ctx.unique_id("b"), project_id=proj.id, session_id=sess_id)
        batch["traces"][0]["trace_id"] = tr_id
        for sp in batch["spans"]:
            sp["trace_id"] = tr_id
        client.ingest_json(batch, api_key=proj.pk_key).raise_for_status()
        _wait_trace_visible(client, proj.id, tr_id, root_tok)

        eval_payload = fixtures.make_manual_evaluation(
            target_type="session",
            session_id=sess_id,
            trace_id=None,
            span_id=None,
            evaluator_id=ctx.unique_id("ev_sess"),
            evaluator_name="session_helpfulness",
            score=0.88,
            passed=True,
        )
        post_resp = client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluations",
            json_body=eval_payload,
            token=root_tok,
        )
        post_resp.raise_for_status()
        created = post_resp.json()
        assert created["target_type"] == "session"
        assert created["session_id"] == sess_id
        assert created.get("trace_id") is None
        assert created.get("span_id") is None

        db_path = inst.data_dir / "pulse_meta.db"
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5.0)
        try:
            row = conn.execute(
                "SELECT target_type, session_id, trace_id, span_id, score "
                "FROM agent_evaluations WHERE project_id = ? AND session_id = ?",
                (proj.id, sess_id),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None, "Session evaluation row not persisted in agent_evaluations"
        assert row[0] == "session"
        assert row[1] == sess_id
        assert row[2] is None and row[3] is None, f"Expected NULL trace_id/span_id, got {row}"
        assert abs(float(row[4]) - 0.88) < 1e-6

        sess_detail = client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/sessions/{sess_id}",
            token=root_tok,
        ).raise_for_status().json()
        assert any(e["target_type"] == "session" and e.get("trace_id") is None for e in sess_detail.get("evaluations", []))
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F13-03",
    tier=2,
    feature="F13",
    title="Zero-eval rollup returns null avg_eval_score and eval_pass_rate (never 0.0 or NaN)",
    tags=("rollups", "null-safety", "nan"),
)
def test_f13_zero_eval_rollup_returns_null_not_zero_or_nan(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f13_zero_eval")
    sess_id = ctx.unique_id("sess_noeval")
    tr_id = ctx.unique_id("tr_noeval")
    batch = fixtures.make_batch(seed=ctx.unique_id("noeval"), project_id=proj.id, session_id=sess_id)
    batch["traces"][0]["trace_id"] = tr_id
    for sp in batch["spans"]:
        sp["trace_id"] = tr_id
    ctx.client.ingest_json(batch, api_key=proj.pk_key).raise_for_status()
    detail = _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    assert detail["trace"].get("avg_eval_score") is None
    assert detail["trace"].get("eval_pass_rate") is None
    assert detail.get("evaluations") == []

    traces_resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(traces_resp)
    matched = [t for t in traces_resp["traces"] if t["trace_id"] == tr_id]
    assert len(matched) == 1
    assert matched[0].get("avg_eval_score") is None
    assert matched[0].get("eval_pass_rate") is None

    sess_resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/sessions/{sess_id}",
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(sess_resp)
    assert sess_resp["summary"].get("avg_eval_score") is None
    assert sess_resp["summary"].get("eval_pass_rate") is None


@test_case(
    id="T2-F13-04",
    tier=2,
    feature="F13",
    title="SQLite/LIKE special-character IDs and search queries are injection- and wildcard-safe",
    tags=("sqlite", "sql-injection", "escape-like", "unicode"),
)
def test_f13_sqlite_like_special_chars_and_injection_safety(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f13_sqli")
    clean_seed = ctx.unique_id("clean").replace("_", "-")
    normal_tr = ctx.unique_id("tr-clean").replace("_", "-")
    normal_batch = fixtures.make_batch(
        seed=clean_seed,
        project_id=proj.id,
        session_id="sess-clean",
        user_id="user-clean",
    )
    normal_batch["traces"][0]["trace_id"] = normal_tr
    normal_batch["traces"][0]["agent_name"] = "clean-support-agent"
    normal_batch["traces"][0]["workflow_id"] = "wf-clean"
    for sp in normal_batch["spans"]:
        sp["trace_id"] = normal_tr
    ctx.client.ingest_json(normal_batch, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, normal_tr, ctx.owner_token)

    hostile_tokens = [
        "' OR 1=1 --",
        "%",
        "_",
        'a"b\\c',
        "agent_🚀_v1",
    ]
    for tok in hostile_tokens:
        r_list = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"search": tok},
            token=ctx.owner_token,
        )
        assert r_list.status in (200, 400), f"Unexpected status {r_list.status} for search={tok!r}"
        if r_list.status == 200:
            found_ids = [t["trace_id"] for t in r_list.json().get("traces", [])]
            assert normal_tr not in found_ids, f"LIKE wildcard/SQLi {tok!r} leaked unrelated trace {normal_tr}"

        r_ds = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/datasets",
            json_body=fixtures.make_dataset(name=f"ds_{tok}"),
            token=ctx.owner_token,
        )
        assert r_ds.status in (200, 201, 400), f"Unexpected {r_ds.status} creating dataset with {tok!r}"

        encoded_tok = urllib.parse.quote(tok, safe="")
        r_trace = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces/{encoded_tok}",
            token=ctx.owner_token,
        )
        assert r_trace.status in (400, 404), f"Expected 400/404 for hostile trace_id {tok!r}, got {r_trace.status}"


@test_case(
    id="T2-F13-05",
    tier=2,
    feature="F13",
    title="agent_erasure_tombstones 3-level schema & durability across node restart",
    tags=("erasure", "tombstones", "restart", "sqlite"),
)
def test_f13_erasure_tombstones_schema_and_restart_durability(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('f13_er')}"
    inst = ctx.node_manager.start_node(role="all", admin_token=root_tok)
    try:
        client = PulseClient(inst.base_url)
        proj = _setup_isolated_project(client, root_tok, ctx.unique_id("f13_er"))
        client.request(
            "PUT",
            f"/v1/projects/{proj.id}/agent/privacy",
            json_body=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
            token=root_tok,
        ).raise_for_status()

        tr_erase = ctx.unique_id("tr_erase")
        sess_erase = ctx.unique_id("sess_erase")
        tr_sess_erase = ctx.unique_id("tr_s")
        user_erase = ctx.unique_id("usr_erase")
        tr_user_erase = ctx.unique_id("tr_u")
        tr_keep = ctx.unique_id("tr_keep")

        for tid, sid, uid in [
            (tr_erase, ctx.unique_id("s_a"), ctx.unique_id("u_a")),
            (tr_sess_erase, sess_erase, ctx.unique_id("u_b")),
            (tr_user_erase, ctx.unique_id("s_c"), user_erase),
            (tr_keep, ctx.unique_id("s_keep"), ctx.unique_id("u_keep")),
        ]:
            b = fixtures.make_batch(seed=tid, project_id=proj.id, session_id=sid, user_id=uid)
            b["traces"][0]["trace_id"] = tid
            for sp in b["spans"]:
                sp["trace_id"] = tid
            client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
            _wait_trace_visible(client, proj.id, tid, root_tok)

        pre_user = client.request(
            "GET", f"/v1/projects/{proj.id}/agent/traces/{tr_user_erase}", token=root_tok
        ).raise_for_status().json()
        assert pre_user["trace"].get("user_id") == user_erase, (
            f"Expected capture_user_id=true to retain user_id={user_erase!r}, got {pre_user['trace'].get('user_id')!r}"
        )

        _erase_agent_target(client, proj.id, root_tok, "trace", tr_erase)
        _erase_agent_target(client, proj.id, root_tok, "session", sess_erase)
        _erase_agent_target(client, proj.id, root_tok, "user", user_erase)

        db_path = inst.data_dir / "pulse_meta.db"
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5.0)
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(agent_erasure_tombstones)").fetchall()]
            rows = conn.execute(
                "SELECT level, target_id FROM agent_erasure_tombstones WHERE project_id = ?",
                (proj.id,),
            ).fetchall()
        finally:
            conn.close()
        for req_col in ("id", "project_id", "level", "target_id", "created_at"):
            assert req_col in cols, f"Missing column {req_col} in agent_erasure_tombstones: {cols}"
        levels_seen = {(r[0], r[1]) for r in rows}
        assert ("trace", tr_erase) in levels_seen
        assert ("session", sess_erase) in levels_seen
        assert ("user", user_erase) in levels_seen

        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        client2 = PulseClient(inst.base_url)

        assert client2.request("GET", f"/v1/projects/{proj.id}/agent/traces/{tr_erase}", token=root_tok).status == 404
        assert client2.request("GET", f"/v1/projects/{proj.id}/agent/traces/{tr_user_erase}", token=root_tok).status == 404
        assert client2.request("GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_erase}", token=root_tok).status == 404
        listed = client2.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok).raise_for_status().json()
        listed_ids = {t["trace_id"] for t in listed["traces"]}
        assert tr_erase not in listed_ids
        assert tr_sess_erase not in listed_ids
        assert tr_user_erase not in listed_ids
        assert tr_keep in listed_ids
        assert all(t.get("user_id") != user_erase and t.get("session_id") != sess_erase for t in listed["traces"])
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F13-06",
    tier=2,
    feature="F13",
    title="agent_eval_rollups separates session, trace, and span rollup aggregates cleanly",
    tags=("rollups", "multi-level", "evaluations"),
)
def test_f13_eval_rollups_isolate_session_trace_and_span_targets(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f13_rollup")
    sess_id = ctx.unique_id("sess_r")
    b1 = fixtures.make_batch(seed=ctx.unique_id("r1"), project_id=proj.id, session_id=sess_id)
    b2 = fixtures.make_batch(seed=ctx.unique_id("r2"), project_id=proj.id, session_id=sess_id)
    tr1 = b1["traces"][0]["trace_id"]
    tr2 = b2["traces"][0]["trace_id"]
    sp1 = b1["spans"][0]["span_id"]

    for b, tid in [(b1, tr1), (b2, tr2)]:
        ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
        _wait_trace_visible(ctx.client, proj.id, tid, ctx.owner_token)

    for payload in [
        fixtures.make_manual_evaluation(
            target_type="trace",
            session_id=sess_id,
            trace_id=tr1,
            evaluator_id="e1",
            score=0.4,
            passed=False,
        ),
        fixtures.make_manual_evaluation(
            target_type="trace",
            session_id=sess_id,
            trace_id=tr1,
            evaluator_id="e2",
            score=0.8,
            passed=True,
        ),
        fixtures.make_manual_evaluation(
            target_type="span",
            session_id=sess_id,
            trace_id=tr1,
            span_id=sp1,
            evaluator_id="e3",
            score=1.0,
            passed=True,
        ),
        fixtures.make_manual_evaluation(
            target_type="session",
            session_id=sess_id,
            trace_id=None,
            span_id=None,
            evaluator_id="e4",
            score=0.25,
            passed=False,
        ),
    ]:
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluations",
            json_body=payload,
            token=ctx.owner_token,
        ).raise_for_status()

    tr1_detail = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/traces/{tr1}", token=ctx.owner_token
    ).raise_for_status().json()
    tr2_detail = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/traces/{tr2}", token=ctx.owner_token
    ).raise_for_status().json()
    assert tr2_detail["trace"].get("avg_eval_score") is None
    assert abs(float(tr1_detail["trace"]["avg_eval_score"]) - 0.6) < 0.05
    assert abs(float(tr1_detail["trace"]["eval_pass_rate"]) - 0.5) < 0.05

    sess_detail = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_id}", token=ctx.owner_token
    ).raise_for_status().json()
    assert abs(float(sess_detail["summary"]["avg_eval_score"]) - 0.25) < 0.05


# ---------------------------------------------------------------------------
# F14: Public Ingest Routes (POST /v1/agent/traces, POST /v1/otel/v1/traces)
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F14-01",
    tier=2,
    feature="F14",
    title="Ingest routes reject > 8 MiB payloads with 413 (or clean close) without crashing",
    tags=("ingest", "body-limit", "8mib"),
)
def test_f14_ingest_rejects_over_8mib_body(ctx: TestContext) -> None:
    oversized = b"{" + (b" " * (8 * 1024 * 1024 + 1024)) + b"}"
    for path, ctype in [
        ("/v1/agent/traces", "application/json"),
        ("/v1/agent/traces", "application/x-pulse-trace"),
        ("/v1/otel/v1/traces", "application/json"),
        ("/v1/otel/v1/traces", "application/x-protobuf"),
    ]:
        try:
            resp = ctx.client.request(
                "POST",
                path,
                body=oversized,
                headers={"Content-Type": ctype},
                api_key=ctx.proj_a.pk_key,
                timeout=30.0,
            )
            assert resp.status in (413, 400), (
                f"Expected 413/400 for >8MiB body on {path} ({ctype}), got {resp.status}"
            )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, http.client.HTTPException, OSError):
            pass
        assert ctx.client.get_healthz().status == 200, "Server unhealthy after >8MiB payload"


@test_case(
    id="T2-F14-02",
    tier=2,
    feature="F14",
    title="Public ingest rejects missing, unknown, and sk_admin keys with 401/403",
    tags=("ingest", "auth", "api-keys"),
)
def test_f14_ingest_rejects_missing_invalid_and_sk_keys(ctx: TestContext) -> None:
    batch = fixtures.make_batch(seed=ctx.unique_id("auth_neg"), project_id=ctx.proj_a.id)
    otlp = fixtures.make_otlp_export_request(seed=ctx.unique_id("otlp_neg"))

    for endpoint, fn in [
        ("/v1/agent/traces", lambda key: ctx.client.ingest_json(batch, api_key=key)),
        ("/v1/otel/v1/traces", lambda key: ctx.client.ingest_otlp_json(otlp, api_key=key)),
    ]:
        r_missing = fn(None)
        assert r_missing.status == 401, f"{endpoint} missing key expected 401, got {r_missing.status}"

        r_bogus = fn("pk_live_invalid_nonexistent_000000")
        assert r_bogus.status == 401, f"{endpoint} invalid key expected 401, got {r_bogus.status}"

        r_sk = fn(ctx.proj_a.sk_key)
        assert r_sk.status in (401, 403), f"{endpoint} sk_ key expected 401/403, got {r_sk.status}"


@test_case(
    id="T2-F14-03",
    tier=2,
    feature="F14",
    title="Ingest rate limiter returns 429 with Retry-After and isolates projects",
    tags=("ingest", "rate-limit", "429", "retry-after"),
)
def test_f14_ingest_rate_limit_429_and_retry_after(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('rl')}"
    inst = ctx.node_manager.start_node(
        role="all",
        admin_token=root_tok,
        extra_env={
            "PULSE_INGEST_EVENTS_PER_SEC": "2",
            "PULSE_INGEST_BURST_SECS": "1",
        },
    )
    try:
        client = PulseClient(inst.base_url)
        proj1 = _setup_isolated_project(client, root_tok, ctx.unique_id("rl_p1"))
        proj2 = _setup_isolated_project(client, root_tok, ctx.unique_id("rl_p2"))

        # Overdraw burst (2 tokens) with a 10-span batch so the very next request triggers 429
        big_batch = fixtures.make_batch(seed=ctx.unique_id("rl_big"), project_id=proj1.id)
        tr_id = big_batch["traces"][0]["trace_id"]
        extra_spans = [
            fixtures.make_span(ctx.unique_id(f"sp_{i}"), tr_id, project_id=proj1.id)
            for i in range(10)
        ]
        big_batch["spans"].extend(extra_spans)
        r1 = client.ingest_json(big_batch, api_key=proj1.pk_key)
        assert r1.status == 200, f"Initial overdraw batch should pass, got {r1.status}"

        hit_429 = None
        for idx in range(5):
            b2 = fixtures.make_batch(seed=ctx.unique_id(f"rl_next_{idx}"), project_id=proj1.id)
            r2 = client.ingest_json(b2, api_key=proj1.pk_key)
            if r2.status == 429:
                hit_429 = r2
                break
        assert hit_429 is not None, "Expected 429 Too Many Requests after exhausting rate-limit bucket"
        ra_hdr = hit_429.header("retry-after")
        assert ra_hdr is not None and int(ra_hdr) >= 1, f"Expected numeric Retry-After >= 1, got {ra_hdr!r}"
        err_json = hit_429.json()
        assert err_json.get("code") == "rate_limited" or "rate" in str(err_json).lower()

        # Independent project proj2 must NOT be throttled by proj1's bucket
        b_p2 = fixtures.make_batch(seed=ctx.unique_id("rl_p2_b"), project_id=proj2.id)
        r_p2 = client.ingest_json(b_p2, api_key=proj2.pk_key)
        assert r_p2.status == 200, f"proj2 should be isolated from proj1 rate limit, got {r_p2.status}"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F14-04",
    tier=2,
    feature="F14",
    title="Ingest rejects unsupported Content-Types (text/plain, application/xml) with 415/400",
    tags=("ingest", "content-type", "415"),
)
def test_f14_ingest_unsupported_content_type(ctx: TestContext) -> None:
    for path in ("/v1/agent/traces", "/v1/otel/v1/traces"):
        for ctype in ("text/plain", "application/xml", "multipart/form-data"):
            resp = ctx.client.request(
                "POST",
                path,
                body=b"<trace>bad</trace>",
                headers={"Content-Type": ctype},
                api_key=ctx.proj_a.pk_key,
            )
            assert resp.status in (400, 415), (
                f"Expected 400/415 for {ctype} on {path}, got {resp.status}: {resp.text(errors='replace')[:200]}"
            )


@test_case(
    id="T2-F14-05",
    tier=2,
    feature="F14",
    title="Malformed OTLP/JSON and corrupt OTLP/Protobuf wire bytes return 400",
    tags=("ingest", "otlp", "protobuf", "malformed"),
)
def test_f14_malformed_otlp_json_and_protobuf(ctx: TestContext) -> None:
    bad_json_bodies = [
        b"{not valid json",
        b'{"resourceSpans": 12345}',
        b'{"resourceSpans": [{"scopeSpans": "invalid"}]}',
        b"\xff\xfe\xfd",
    ]
    for raw in bad_json_bodies:
        resp = ctx.client.request(
            "POST",
            "/v1/otel/v1/traces",
            body=raw,
            headers={"Content-Type": "application/json"},
            api_key=ctx.proj_a.pk_key,
        )
        assert resp.status == 400, f"Expected 400 for malformed OTLP/JSON {raw!r}, got {resp.status}"

    bad_pb_bodies = [
        b"\xff\xff\xff\x7f\x00",
        b"\x0a\x99\x99\x99\x99\x01",
        b"\x0f\x00\x00",
    ]
    for raw_pb in bad_pb_bodies:
        resp = ctx.client.ingest_otlp_pb(raw_pb, api_key=ctx.proj_a.pk_key)
        assert resp.status == 400, f"Expected 400 for corrupt OTLP/Protobuf {raw_pb!r}, got {resp.status}"


@test_case(
    id="T2-F14-06",
    tier=2,
    feature="F14",
    title="Empty ingest batches, invalid UTF-8 native JSON, and permissive CORS preflight",
    tags=("ingest", "cors", "empty-batch", "utf8"),
)
def test_f14_empty_batch_bad_utf8_and_permissive_cors(ctx: TestContext) -> None:
    empty_resp = ctx.client.ingest_json({"traces": [], "spans": []}, api_key=ctx.proj_a.pk_key)
    assert empty_resp.status == 200, f"Empty batch failed: {empty_resp.status}"
    assert empty_resp.json()["accepted_traces"] == 0
    assert empty_resp.json()["accepted_spans"] == 0

    bad_utf8 = ctx.client.request(
        "POST",
        "/v1/agent/traces",
        body=b'{"traces":[{"trace_id":"\xff\xfe"}]}',
        headers={"Content-Type": "application/json"},
        api_key=ctx.proj_a.pk_key,
    )
    assert bad_utf8.status == 400, f"Invalid UTF-8 JSON expected 400, got {bad_utf8.status}"

    opt_resp = ctx.client.request(
        "OPTIONS",
        "/v1/agent/traces",
        headers={
            "Origin": "https://sdk-app.example.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type,x-pulse-key",
        },
    )
    assert opt_resp.status in (200, 204), f"OPTIONS preflight returned {opt_resp.status}"
    assert opt_resp.header("access-control-allow-origin") == "*"


# ---------------------------------------------------------------------------
# F15: Protected Session, Trace, Analytics & Compare Routes + RBAC
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F15-01",
    tier=2,
    feature="F15",
    title="Viewer role can read all agent GET routes and is blocked 403 on all mutations",
    tags=("rbac", "viewer", "403"),
)
def test_f15_viewer_read_allowed_and_mutations_forbidden(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    for path in (
        f"/v1/projects/{pid}/agent/sessions",
        f"/v1/projects/{pid}/agent/traces",
        f"/v1/projects/{pid}/agent/analytics",
        f"/v1/projects/{pid}/agent/compare?agent_name=support-agent&baseline_version=1.0.0&candidate_version=2.0.0",
        f"/v1/projects/{pid}/agent/evaluators",
        f"/v1/projects/{pid}/agent/evaluations",
        f"/v1/projects/{pid}/agent/datasets",
        f"/v1/projects/{pid}/agent/experiments",
        f"/v1/projects/{pid}/agent/alerts",
        f"/v1/projects/{pid}/agent/alerts/events",
        f"/v1/projects/{pid}/agent/privacy",
        f"/v1/projects/{pid}/agent/pricing",
    ):
        r = ctx.client.request("GET", path, token=ctx.viewer_token)
        assert r.status == 200, f"Viewer GET {path} expected 200, got {r.status}: {r.text(errors='replace')[:200]}"

    mutations = [
        ("POST", f"/v1/projects/{pid}/agent/evaluators", fixtures.make_evaluator_config(name=ctx.unique_id("v_ev"))),
        ("POST", f"/v1/projects/{pid}/agent/datasets", fixtures.make_dataset(name=ctx.unique_id("v_ds"))),
        ("POST", f"/v1/projects/{pid}/agent/alerts", fixtures.make_alert_rule(name=ctx.unique_id("v_al"))),
        ("PUT", f"/v1/projects/{pid}/agent/privacy", fixtures.make_privacy_policy(project_id=pid)),
        ("POST", f"/v1/projects/{pid}/agent/pricing", fixtures.make_pricing_rule()),
        ("DELETE", f"/v1/projects/{pid}/agent/traces/{ctx.unique_id('nonexistent')}", None),
    ]
    for method, path, body in mutations:
        r = ctx.client.request(method, path, json_body=body, token=ctx.viewer_token)
        assert r.status == 403, f"Viewer {method} {path} expected 403, got {r.status}"


@test_case(
    id="T2-F15-02",
    tier=2,
    feature="F15",
    title="Member role permitted on standard mutations and blocked 403 on Admin erasure/privacy/pricing",
    tags=("rbac", "member", "admin", "403"),
)
def test_f15_member_vs_admin_boundaries(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f15_member")
    tr_id = ctx.unique_id("tr_mem")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id
    ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    ds_resp = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("mem_ds")),
        token=ctx.member_token,
    )
    assert ds_resp.status in (200, 201), f"Member POST dataset failed: {ds_resp.status}"
    ds_id = ds_resp.json()["id"]

    del_ds = ctx.client.request(
        "DELETE",
        f"/v1/projects/{proj.id}/agent/datasets/{ds_id}",
        token=ctx.member_token,
    )
    assert del_ds.status in (200, 204), f"Member DELETE dataset failed: {del_ds.status}"

    for method, path, body in [
        ("DELETE", f"/v1/projects/{proj.id}/agent/traces/{tr_id}", None),
        ("PUT", f"/v1/projects/{proj.id}/agent/privacy", fixtures.make_privacy_policy(project_id=proj.id)),
        ("POST", f"/v1/projects/{proj.id}/agent/pricing", fixtures.make_pricing_rule()),
    ]:
        r = ctx.client.request(method, path, json_body=body, token=ctx.member_token)
        assert r.status == 403, f"Member {method} {path} expected 403 Admin-only, got {r.status}"

    r_admin = ctx.client.request(
        "DELETE",
        f"/v1/projects/{proj.id}/agent/traces/{tr_id}",
        token=ctx.admin_token,
    )
    assert r_admin.status in (200, 202, 204), f"Admin DELETE trace expected 2xx, got {r_admin.status}"


@test_case(
    id="T2-F15-03",
    tier=2,
    feature="F15",
    title="Cross-organization user access to Org A project returns 404 and leaks zero data",
    tags=("tenancy", "cross-org", "404"),
)
def test_f15_cross_org_isolation_returns_404(ctx: TestContext) -> None:
    secret_marker = ctx.unique_id("org_a_secret")
    b = fixtures.make_batch(seed=secret_marker, project_id=ctx.proj_a.id)
    tr_id = b["traces"][0]["trace_id"]
    ctx.client.ingest_json(b, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, ctx.proj_a.id, tr_id, ctx.owner_token)

    for method, path, body in [
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces", None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}", None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/sessions", None),
        ("GET", f"/v1/projects/{ctx.proj_a.id}/agent/analytics", None),
        ("POST", f"/v1/projects/{ctx.proj_a.id}/agent/datasets", fixtures.make_dataset(name=ctx.unique_id("x"))),
        ("DELETE", f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_id}", None),
    ]:
        r = ctx.client.request(method, path, json_body=body, token=ctx.other_org_token)
        assert r.status == 404, f"Cross-org {method} {path} expected 404, got {r.status}"
        assert secret_marker not in r.text(errors="replace")


@test_case(
    id="T2-F15-04",
    tier=2,
    feature="F15",
    title="ProjectAdmin sk_live_ key cross-project access returns 403 and resource IDOR returns 404",
    tags=("tenancy", "sk-key", "idor", "403", "404"),
)
def test_f15_sk_key_cross_project_403_and_resource_idor_404(ctx: TestContext) -> None:
    r_cross = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces",
        token=ctx.proj_b.sk_key,
    )
    assert r_cross.status == 403, f"proj_b sk_key on proj_a expected 403, got {r_cross.status}"

    r_rev = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_b.id}/agent/analytics",
        token=ctx.proj_a.sk_key,
    )
    assert r_rev.status == 403, f"proj_a sk_key on proj_b expected 403, got {r_rev.status}"

    # Resource IDOR: dataset/trace created in proj_a queried under proj_b URL
    ds_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("idor_ds")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    ds_a_id = ds_a["id"]
    r_idor = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_b.id}/agent/datasets/{ds_a_id}/items",
        token=ctx.owner_token,
    )
    assert r_idor.status == 404, f"Cross-project dataset IDOR expected 404, got {r_idor.status}"


@test_case(
    id="T2-F15-05",
    tier=2,
    feature="F15",
    title="Version compare Welch t-test enforces MIN_SIGNIFICANCE_SAMPLES=20 (n=19 insufficient vs n=20 significant)",
    tags=("compare", "welch-t-test", "bva", "stats"),
)
def test_f15_compare_welch_sample_threshold_19_vs_20(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f15_welch")
    agent_name = ctx.unique_id("cmp_agent")
    base_ver = "1.0.0"
    cand_ver = "2.0.0"

    def _send_trace(idx: int, ver: str, dur_ms: int) -> str:
        tid = ctx.unique_id(f"cmp_{ver}_{idx}")
        sp = fixtures.make_span(
            ctx.unique_id(f"sp_{ver}_{idx}"),
            tid,
            project_id=proj.id,
            duration_ms=dur_ms,
            cost_nanos=1_000_000 if ver == base_ver else 5_000_000,
        )
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            agent_name=agent_name,
            agent_version=ver,
            duration_ms=dur_ms,
            spans=[sp],
        )
        ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
        return tid

    last_base = ""
    for i in range(19):
        last_base = _send_trace(i, base_ver, 100 + (i % 5))
    last_cand = ""
    for i in range(20):
        last_cand = _send_trace(i, cand_ver, 900 + (i % 5))

    _wait_trace_visible(ctx.client, proj.id, last_base, ctx.owner_token)
    _wait_trace_visible(ctx.client, proj.id, last_cand, ctx.owner_token)

    cmp_19 = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={
            "agent_name": agent_name,
            "baseline_version": base_ver,
            "candidate_version": cand_ver,
        },
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp_19)

    # Find latency delta object in compare response
    lat_delta = cmp_19.get("latency_ms") or cmp_19.get("latency") or cmp_19.get("duration_ms")
    assert isinstance(lat_delta, dict), f"Missing latency MetricDelta in compare response: {cmp_19}"
    assert lat_delta["verdict"] == "insufficient_data", f"Expected insufficient_data at n=19, got {lat_delta}"
    assert lat_delta.get("p_value") is None

    # Add 20th baseline sample -> Welch's t-test now runs and detects significant regression
    extra_tid = _send_trace(19, base_ver, 102)
    _wait_trace_visible(ctx.client, proj.id, extra_tid, ctx.owner_token)

    cmp_20 = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={
            "agent_name": agent_name,
            "baseline_version": base_ver,
            "candidate_version": cand_ver,
        },
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp_20)
    lat_delta_20 = cmp_20.get("latency_ms") or cmp_20.get("latency") or cmp_20.get("duration_ms")
    assert isinstance(lat_delta_20, dict)
    assert lat_delta_20["verdict"] == "significant_regression", f"Expected significant_regression at n=20, got {lat_delta_20}"
    assert lat_delta_20["p_value"] is not None and 0.0 <= float(lat_delta_20["p_value"]) < 0.05


@test_case(
    id="T2-F15-06",
    tier=2,
    feature="F15",
    title="Trace list limit clamping (0 / >100) and cursor pagination yield complete disjoint pages",
    tags=("pagination", "cursor", "limit-clamp"),
)
def test_f15_trace_list_limit_clamping_and_cursor_pagination(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f15_page")
    ingested_ids: set[str] = set()
    base_ts = fixtures.DEFAULT_BASE_TS_MS
    for i in range(5):
        tid = ctx.unique_id(f"pg_{i}")
        b = fixtures.make_batch(seed=tid, project_id=proj.id, start_ts_ms=base_ts + i * 1000)
        b["traces"][0]["trace_id"] = tid
        for sp in b["spans"]:
            sp["trace_id"] = tid
        ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
        ingested_ids.add(tid)
    _wait_trace_visible(ctx.client, proj.id, next(iter(ingested_ids)), ctx.owner_token)

    _poll_until(
        lambda: len(
            ctx.client.request(
                "GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.owner_token
            ).raise_for_status().json()["traces"]
        ) == 5,
        desc="all 5 traces visible",
    )

    clamped = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={"limit": 999},
        token=ctx.owner_token,
    ).raise_for_status().json()
    assert clamped["limit"] <= 100

    seen_ids: list[str] = []
    cursor: str | None = None
    for _ in range(10):
        q: dict[str, Any] = {"limit": 2}
        if cursor is not None:
            q["cursor"] = cursor
        page = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query=q,
            token=ctx.owner_token,
        ).raise_for_status().json()
        page_ids = [t["trace_id"] for t in page["traces"]]
        assert len(page_ids) <= 2
        seen_ids.extend(page_ids)
        cursor = page.get("next_cursor")
        if not cursor or not page_ids:
            break

    assert len(seen_ids) == len(set(seen_ids)), f"Duplicate IDs across cursor pages: {seen_ids}"
    assert set(seen_ids) == ingested_ids


# ---------------------------------------------------------------------------
# F16: 4 Project-Bound MCP Tools on /v1/mcp
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F16-01",
    tier=2,
    feature="F16",
    title="MCP blocks cross-project arguments.project_id override (-32602) on all 4 agent tools",
    tags=("mcp", "tenancy", "json-rpc", "-32602"),
)
def test_f16_mcp_cross_project_override_rejected(ctx: TestContext) -> None:
    marker_b = ctx.unique_id("projb_secret_mcp")
    b = fixtures.make_batch(seed=marker_b, project_id=ctx.proj_b.id)
    tr_b = b["traces"][0]["trace_id"]
    ctx.client.ingest_json(b, api_key=ctx.proj_b.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, ctx.proj_b.id, tr_b, ctx.owner_token)

    for idx, (tool_name, args) in enumerate(
        [
            ("list_agent_traces", {"project_id": ctx.proj_b.id}),
            ("get_agent_trace", {"project_id": ctx.proj_b.id, "trace_id": tr_b}),
            ("query_agent_analytics", {"project_id": ctx.proj_b.id}),
            ("list_agent_evaluations", {"project_id": ctx.proj_b.id}),
        ],
        start=1,
    ):
        resp = ctx.client.mcp_tool_call(
            tool_name,
            args,
            req_id=idx,
            token=ctx.proj_a.sk_key,
        )
        assert resp.status == 200, f"MCP returned HTTP {resp.status}"
        data = resp.json()
        assert data.get("error", {}).get("code") == -32602, (
            f"{tool_name} cross-project override expected -32602, got {data}"
        )
        assert marker_b not in resp.text(errors="replace")

    hdr_override = ctx.client.mcp_tool_call(
        "list_agent_traces",
        {},
        req_id=99,
        token=ctx.proj_a.sk_key,
        project_id=ctx.proj_b.id,
    )
    assert hdr_override.status == 403, f"x-pulse-project override with proj_a sk_key expected 403, got {hdr_override.status}"


@test_case(
    id="T2-F16-02",
    tier=2,
    feature="F16",
    title="MCP returns -32602 for unknown tool / missing name and -32601 for unknown method",
    tags=("mcp", "json-rpc", "error-codes"),
)
def test_f16_mcp_unknown_tool_and_method_errors(ctx: TestContext) -> None:
    r_unknown_tool = ctx.client.mcp_tool_call(
        "nonexistent_agent_tool",
        {},
        req_id=10,
        token=ctx.proj_a.sk_key,
    )
    assert r_unknown_tool.status == 200
    assert r_unknown_tool.json()["error"]["code"] == -32602

    r_missing_name = ctx.client.mcp_call(
        "tools/call",
        {"arguments": {}},
        req_id=11,
        token=ctx.proj_a.sk_key,
    )
    assert r_missing_name.status == 200
    assert r_missing_name.json()["error"]["code"] == -32602

    r_unknown_method = ctx.client.mcp_call(
        "tools/not_a_method",
        {},
        req_id=12,
        token=ctx.proj_a.sk_key,
    )
    assert r_unknown_method.status == 200
    assert r_unknown_method.json()["error"]["code"] == -32601


@test_case(
    id="T2-F16-03",
    tier=2,
    feature="F16",
    title="MCP get_agent_trace returns isError=true for missing arg, nonexistent ID, or cross-project trace",
    tags=("mcp", "isError", "idor"),
)
def test_f16_mcp_get_agent_trace_is_error_on_missing_or_foreign_trace(ctx: TestContext) -> None:
    b = fixtures.make_batch(seed=ctx.unique_id("mcp_idor"), project_id=ctx.proj_b.id)
    tr_b = b["traces"][0]["trace_id"]
    ctx.client.ingest_json(b, api_key=ctx.proj_b.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, ctx.proj_b.id, tr_b, ctx.owner_token)

    for args in (
        {},
        {"trace_id": ctx.unique_id("nonexistent_trace")},
        {"trace_id": tr_b},
    ):
        resp = ctx.client.mcp_tool_call(
            "get_agent_trace",
            args,
            req_id=20,
            token=ctx.proj_a.sk_key,
        )
        assert resp.status == 200
        body = resp.json()
        assert "error" not in body or body["error"] is None, f"Expected tool isError result, got RPC error: {body}"
        res = body["result"]
        assert res.get("isError") is True, f"Expected isError=true for args={args}, got {res}"
        assert len(res.get("content", [])) > 0 and res["content"][0].get("text")


@test_case(
    id="T2-F16-04",
    tier=2,
    feature="F16",
    title="MCP JSON-RPC notification (id: null) returns 202 Accepted and GET /v1/mcp returns 405",
    tags=("mcp", "notification", "202", "405"),
)
def test_f16_mcp_notification_202_and_get_405(ctx: TestContext) -> None:
    notif_null = ctx.client.request(
        "POST",
        "/v1/mcp",
        json_body={"jsonrpc": "2.0", "id": None, "method": "notifications/initialized"},
        token=ctx.proj_a.sk_key,
    )
    assert notif_null.status == 202, f"Expected 202 Accepted for id=null notification, got {notif_null.status}"
    assert notif_null.body == b""

    notif_omitted = ctx.client.mcp_call(
        "notifications/initialized",
        {},
        req_id=None,
        token=ctx.proj_a.sk_key,
    )
    assert notif_omitted.status == 202
    assert notif_omitted.body == b""

    get_mcp = ctx.client.request("GET", "/v1/mcp", token=ctx.proj_a.sk_key)
    assert get_mcp.status == 405, f"GET /v1/mcp expected 405, got {get_mcp.status}"


@test_case(
    id="T2-F16-05",
    tier=2,
    feature="F16",
    title="MCP rejects pk_ ingest keys with 403 and unauthenticated requests with 401",
    tags=("mcp", "auth", "401", "403"),
)
def test_f16_mcp_rejects_pk_key_403_and_unauth_401(ctx: TestContext) -> None:
    r_pk = ctx.client.mcp_call("tools/list", {}, req_id=30, token=ctx.proj_a.pk_key)
    assert r_pk.status == 403, f"pk_ key on /v1/mcp expected 403, got {r_pk.status}"

    r_unauth = ctx.client.mcp_call("tools/list", {}, req_id=31, token=None)
    assert r_unauth.status == 401, f"Unauthenticated /v1/mcp expected 401, got {r_unauth.status}"


# ---------------------------------------------------------------------------
# F17: Async Multi-Level Eval Worker (agent_eval_worker.rs)
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F17-01",
    tier=2,
    feature="F17",
    title="LLM judge mock HTTP 500 does not block ingest or break deterministic evaluators",
    tags=("eval-worker", "llm-judge", "500", "resilience"),
)
def test_f17_llm_judge_http_500_nonblocking_and_isolated(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f17_500")
    _configure_byok_llm(ctx.client, proj.id, ctx.owner_token, ctx.mock_llm_url)

    ctx.mock_llm.status_code = 500
    ctx.mock_llm.raw_body_override = b'{"error":{"message":"upstream 500"}}'

    llm_ev = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("llm_500"), kind="llm_judge"),
        token=ctx.owner_token,
    ).raise_for_status().json()

    det_ev = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("det_ok"), kind="no_errors"),
        token=ctx.owner_token,
    ).raise_for_status().json()

    tr_id = ctx.unique_id("tr_500")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id
    ing = ctx.client.ingest_json(b, api_key=proj.pk_key)
    assert ing.status == 200
    _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    ctx.mock_llm.wait_for_count(1, timeout=10.0)

    def _det_completed() -> list[dict[str, Any]]:
        evals = _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"trace_id": tr_id}, token=ctx.owner_token
            ).raise_for_status().json(),
            "evaluations",
        )
        return [e for e in evals if e.get("evaluator_id") == det_ev["id"] and e.get("trace_id") == tr_id]

    det_rows = _poll_until(_det_completed, timeout=10.0, desc="deterministic eval despite LLM 500")
    assert det_rows[0]["passed"] is True
    assert llm_ev["id"]


@test_case(
    id="T2-F17-02",
    tier=2,
    feature="F17",
    title="Slow LLM judge response does not stall ingest HTTP latency",
    tags=("eval-worker", "timeout", "latency", "async"),
)
def test_f17_slow_llm_judge_does_not_block_ingest(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f17_slow")
    _configure_byok_llm(ctx.client, proj.id, ctx.owner_token, ctx.mock_llm_url)
    ctx.mock_llm.latency_s = 2.5

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("llm_slow"), kind="llm_judge"),
        token=ctx.owner_token,
    ).raise_for_status()

    tr_id = ctx.unique_id("tr_slow")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id

    t0 = time.monotonic()
    resp = ctx.client.ingest_json(b, api_key=proj.pk_key)
    elapsed = time.monotonic() - t0
    assert resp.status == 200
    assert elapsed < 1.5, f"Ingest blocked by slow LLM judge ({elapsed:.2f}s >= 1.5s)"

    ctx.mock_llm.wait_for_count(1, timeout=10.0)


@test_case(
    id="T2-F17-03",
    tier=2,
    feature="F17",
    title="Malformed non-JSON LLM judge response records score=0.0, passed=false, error without crash",
    tags=("eval-worker", "llm-judge", "malformed-json"),
)
def test_f17_malformed_non_json_llm_judge_response(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f17_badjson")
    _configure_byok_llm(ctx.client, proj.id, ctx.owner_token, ctx.mock_llm_url)
    ctx.mock_llm.response_content = "NOT VALID JSON {{{ missing brace"

    llm_ev = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("llm_bad"), kind="llm_judge"),
        token=ctx.owner_token,
    ).raise_for_status().json()

    tr_id = ctx.unique_id("tr_badjson")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id
    ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    def _find_llm_eval() -> dict[str, Any] | None:
        evals = _extract_items(
            ctx.client.request(
                "GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"trace_id": tr_id}, token=ctx.owner_token
            ).raise_for_status().json(),
            "evaluations",
        )
        for e in evals:
            if e.get("evaluator_id") == llm_ev["id"] and e.get("trace_id") == tr_id:
                return e
        return None

    ev_row = _poll_until(_find_llm_eval, timeout=12.0, desc="malformed LLM judge evaluation row")
    assert float(ev_row["score"]) == 0.0
    assert ev_row["passed"] is False
    assert ev_row.get("error") or "parse" in str(ev_row.get("reasoning", "")).lower()


@test_case(
    id="T2-F17-04",
    tier=2,
    feature="F17",
    title="Invalid regex_match evaluator pattern rejected cleanly or records regex_error without 500",
    tags=("evaluators", "regex", "invalid-config"),
)
def test_f17_invalid_regex_evaluator_handling(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f17_regex")
    cfg = fixtures.make_evaluator_config(
        name=ctx.unique_id("bad_regex"),
        kind="regex_match",
        config={"pattern": "[unclosed_group("},
    )
    create_resp = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=cfg,
        token=ctx.owner_token,
    )
    assert create_resp.status in (200, 201, 400), f"Unexpected status {create_resp.status}"
    if create_resp.status == 400:
        return

    ev_id = create_resp.json()["id"]
    tr_id = ctx.unique_id("tr_regex")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id
    ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    run_resp = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/traces/{tr_id}/evaluate",
        json_body={"evaluator_ids": [ev_id]},
        token=ctx.owner_token,
    )
    assert run_resp.status in (200, 202, 400), f"Unexpected {run_resp.status}"
    evals = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"trace_id": tr_id}, token=ctx.owner_token
        ).raise_for_status().json(),
        "evaluations",
    )
    matched = [e for e in evals if e.get("evaluator_id") == ev_id]
    if matched:
        assert matched[0]["passed"] is False
        assert float(matched[0]["score"]) == 0.0
        assert matched[0].get("label") == "regex_error" or matched[0].get("error")


@test_case(
    id="T2-F17-05",
    tier=2,
    feature="F17",
    title="Evaluator sample_rate=0.0 skips auto-eval on ingest while manual trigger executes",
    tags=("eval-worker", "sample-rate", "manual-eval"),
)
def test_f17_sample_rate_zero_skips_auto_eval_but_manual_runs(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f17_sr0")
    ev_cfg = fixtures.make_evaluator_config(
        name=ctx.unique_id("sr0_eval"),
        kind="output_not_empty",
        sample_rate=0.0,
    )
    ev = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=ev_cfg,
        token=ctx.owner_token,
    ).raise_for_status().json()

    tr_id = ctx.unique_id("tr_sr0")
    b = fixtures.make_batch(seed=tr_id, project_id=proj.id)
    b["traces"][0]["trace_id"] = tr_id
    for sp in b["spans"]:
        sp["trace_id"] = tr_id
    ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)

    time.sleep(0.8)
    auto_evals = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"trace_id": tr_id}, token=ctx.owner_token
        ).raise_for_status().json(),
        "evaluations",
    )
    assert all(e.get("evaluator_id") != ev["id"] for e in auto_evals), (
        f"sample_rate=0.0 should not trigger auto-eval, got {auto_evals}"
    )

    trig = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/traces/{tr_id}/evaluate",
        json_body={"evaluator_ids": [ev["id"]]},
        token=ctx.owner_token,
    )
    trig.raise_for_status()

    manual_evals = _poll_until(
        lambda: [
            e
            for e in _extract_items(
                ctx.client.request(
                    "GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"trace_id": tr_id}, token=ctx.owner_token
                ).raise_for_status().json(),
                "evaluations",
            )
            if e.get("evaluator_id") == ev["id"]
        ],
        timeout=8.0,
        desc="manual evaluation for sample_rate=0.0",
    )
    assert manual_evals[0]["passed"] is True


# ---------------------------------------------------------------------------
# F18: Evaluators, Datasets, Promote & Experiments APIs
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F18-01",
    tier=2,
    feature="F18",
    title="Promote nonexistent or cross-project trace_id returns 404",
    tags=("promote", "404", "idor"),
)
def test_f18_promote_nonexistent_and_foreign_trace_returns_404(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("prom_ds")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    r_missing = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{ctx.unique_id('no_trace')}/promote",
        json_body={"dataset_id": ds["id"]},
        token=ctx.owner_token,
    )
    assert r_missing.status == 404, f"Promote missing trace expected 404, got {r_missing.status}"

    b = fixtures.make_batch(seed=ctx.unique_id("prom_b"), project_id=ctx.proj_b.id)
    tr_b = b["traces"][0]["trace_id"]
    ctx.client.ingest_json(b, api_key=ctx.proj_b.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, ctx.proj_b.id, tr_b, ctx.owner_token)

    r_foreign = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces/{tr_b}/promote",
        json_body={"dataset_id": ds["id"]},
        token=ctx.owner_token,
    )
    assert r_foreign.status == 404, f"Promote foreign trace expected 404, got {r_foreign.status}"


@test_case(
    id="T2-F18-02",
    tier=2,
    feature="F18",
    title="Dataset import rejects nonexistent/foreign dataset (404) and handles empty items without 500",
    tags=("datasets", "import", "404", "empty"),
)
def test_f18_dataset_import_nonexistent_foreign_and_empty(ctx: TestContext) -> None:
    r_missing = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ctx.unique_id('no_ds')}/import",
        json_body={"items": [fixtures.make_dataset_item(name="x")]},
        token=ctx.owner_token,
    )
    assert r_missing.status == 404, f"Import into missing dataset expected 404, got {r_missing.status}"

    ds_b = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_b.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("ds_b")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    r_cross = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_b['id']}/import",
        json_body={"items": [fixtures.make_dataset_item(name="x")]},
        token=ctx.owner_token,
    )
    assert r_cross.status == 404, f"Import into proj_b dataset via proj_a expected 404, got {r_cross.status}"

    ds_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("ds_a_empty")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    r_empty = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_a['id']}/import",
        json_body={"items": []},
        token=ctx.owner_token,
    )
    assert r_empty.status in (200, 400), f"Empty dataset import expected 200/400, got {r_empty.status}"


@test_case(
    id="T2-F18-03",
    tier=2,
    feature="F18",
    title="Duplicate dataset item IDs on import upsert idempotently without duplicate rows",
    tags=("datasets", "import", "idempotency"),
)
def test_f18_dataset_import_duplicate_items_idempotent(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("dup_ds")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    ds_id = ds["id"]
    fixed_id = ctx.unique_id("item_dup")

    item1 = fixtures.make_dataset_item(name="v1")
    item1["id"] = fixed_id
    item2 = fixtures.make_dataset_item(name="v2", expected_output={"reply": "updated"})
    item2["id"] = fixed_id

    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/import",
        json_body={"items": [item1, item2]},
        token=ctx.owner_token,
    ).raise_for_status()

    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/import",
        json_body={"items": [item2]},
        token=ctx.owner_token,
    ).raise_for_status()

    items = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds_id}/items",
            token=ctx.owner_token,
        ).raise_for_status().json(),
        "items",
    )
    matching = [it for it in items if it["id"] == fixed_id]
    assert len(matching) == 1, f"Expected idempotent single item for {fixed_id}, got {len(matching)}"
    assert matching[0]["name"] == "v2"


@test_case(
    id="T2-F18-04",
    tier=2,
    feature="F18",
    title="experiments/compare rejects missing query params (400) and nonexistent/foreign IDs (404)",
    tags=("experiments", "compare", "400", "404", "idor"),
)
def test_f18_experiments_compare_missing_params_and_cross_project_404(ctx: TestContext) -> None:
    ds_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("ecmp_ds_a")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    exp_a = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds_a["id"], name=ctx.unique_id("exp_a")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    ds_b = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_b.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("ecmp_ds_b")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    exp_b = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_b.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds_b["id"], name=ctx.unique_id("exp_b")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    r_no_params = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        token=ctx.owner_token,
    )
    assert r_no_params.status == 400, f"Missing baseline/candidate expected 400, got {r_no_params.status}"

    r_one_param = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": exp_a["id"]},
        token=ctx.owner_token,
    )
    assert r_one_param.status == 400, f"Missing candidate expected 400, got {r_one_param.status}"

    r_missing = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": exp_a["id"], "candidate": ctx.unique_id("exp_missing")},
        token=ctx.owner_token,
    )
    assert r_missing.status == 404, f"Missing candidate ID expected 404, got {r_missing.status}"

    r_cross = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": exp_a["id"], "candidate": exp_b["id"]},
        token=ctx.owner_token,
    )
    assert r_cross.status == 404, f"Cross-project experiment compare expected 404, got {r_cross.status}"


@test_case(
    id="T2-F18-05",
    tier=2,
    feature="F18",
    title="experiments/compare guards n<20 with insufficient_data vs n=20 significance",
    tags=("experiments", "compare", "welch-t-test", "bva"),
)
def test_f18_experiments_compare_sample_threshold_guard(ctx: TestContext) -> None:
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("exp_thr_ds")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    item = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/datasets/{ds['id']}/items",
        json_body=fixtures.make_dataset_item(name="thr_item"),
        token=ctx.owner_token,
    ).raise_for_status().json()

    exp_base = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds["id"], name=ctx.unique_id("exp_base"), agent_version="1.0.0"),
        token=ctx.owner_token,
    ).raise_for_status().json()
    exp_cand = ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds["id"], name=ctx.unique_id("exp_cand"), agent_version="2.0.0"),
        token=ctx.owner_token,
    ).raise_for_status().json()

    for i in range(19):
        ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_base['id']}/runs",
            json_body=fixtures.make_experiment_run(dataset_item_id=item["id"], avg_score=0.50 + (i % 3) * 0.01, latency_ms=200),
            token=ctx.owner_token,
        ).raise_for_status()
        ctx.client.request(
            "POST",
            f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_cand['id']}/runs",
            json_body=fixtures.make_experiment_run(dataset_item_id=item["id"], avg_score=0.95 + (i % 3) * 0.01, latency_ms=200),
            token=ctx.owner_token,
        ).raise_for_status()

    cmp19 = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": exp_base["id"], "candidate": exp_cand["id"]},
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp19)
    score_delta_19 = cmp19.get("avg_score") or cmp19.get("score")
    assert isinstance(score_delta_19, dict), f"Missing score MetricDelta in {cmp19}"
    assert score_delta_19["verdict"] == "insufficient_data"
    assert score_delta_19.get("p_value") is None

    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_base['id']}/runs",
        json_body=fixtures.make_experiment_run(dataset_item_id=item["id"], avg_score=0.51, latency_ms=200),
        token=ctx.owner_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/{exp_cand['id']}/runs",
        json_body=fixtures.make_experiment_run(dataset_item_id=item["id"], avg_score=0.96, latency_ms=200),
        token=ctx.owner_token,
    ).raise_for_status()

    cmp20 = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/experiments/compare",
        query={"baseline": exp_base["id"], "candidate": exp_cand["id"]},
        token=ctx.owner_token,
    ).raise_for_status().json()
    _assert_no_nan_inf(cmp20)
    score_delta_20 = cmp20.get("avg_score") or cmp20.get("score")
    assert isinstance(score_delta_20, dict)
    assert score_delta_20["verdict"] == "significant_improvement"
    assert score_delta_20["p_value"] is not None and float(score_delta_20["p_value"]) < 0.05


@test_case(
    id="T2-F18-06",
    tier=2,
    feature="F18",
    title="Viewer token blocked 403 across all evaluator, dataset, promote, and experiment mutations",
    tags=("rbac", "viewer", "403"),
)
def test_f18_viewer_blocked_on_all_eval_dataset_promote_experiment_mutations(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{pid}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("v_blk_ds")),
        token=ctx.owner_token,
    ).raise_for_status().json()
    exp = ctx.client.request(
        "POST",
        f"/v1/projects/{pid}/agent/experiments",
        json_body=fixtures.make_experiment(dataset_id=ds["id"], name=ctx.unique_id("v_blk_exp")),
        token=ctx.owner_token,
    ).raise_for_status().json()

    for method, path, body in [
        ("POST", f"/v1/projects/{pid}/agent/evaluators", fixtures.make_evaluator_config(name="v1")),
        ("DELETE", f"/v1/projects/{pid}/agent/evaluators/ev_missing", None),
        ("POST", f"/v1/projects/{pid}/agent/traces/tr_missing/evaluate", {}),
        ("POST", f"/v1/projects/{pid}/agent/datasets", fixtures.make_dataset(name="v2")),
        ("DELETE", f"/v1/projects/{pid}/agent/datasets/{ds['id']}", None),
        ("POST", f"/v1/projects/{pid}/agent/datasets/{ds['id']}/items", fixtures.make_dataset_item()),
        ("POST", f"/v1/projects/{pid}/agent/datasets/{ds['id']}/import", {"items": [fixtures.make_dataset_item()]}),
        ("POST", f"/v1/projects/{pid}/agent/traces/tr_missing/promote", {"dataset_id": ds["id"]}),
        ("POST", f"/v1/projects/{pid}/agent/experiments", fixtures.make_experiment(dataset_id=ds["id"])),
        ("POST", f"/v1/projects/{pid}/agent/experiments/{exp['id']}/runs", fixtures.make_experiment_run(dataset_item_id="i1")),
    ]:
        r = ctx.client.request(method, path, json_body=body, token=ctx.viewer_token)
        assert r.status == 403, f"Viewer {method} {path} expected 403, got {r.status}"


# ---------------------------------------------------------------------------
# F19: Alerts, Webhooks, Privacy, Pricing & Retention
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F19-01",
    tier=2,
    feature="F19",
    title="Alert creation rejects invalid metric, invalid operator, and empty name with 400",
    tags=("alerts", "validation", "400"),
)
def test_f19_alert_invalid_metric_operator_and_empty_name(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    bad_metric = fixtures.make_alert_rule(name=ctx.unique_id("al_bad_m"))
    bad_metric["metric"] = "unknown_metric_xyz"
    r1 = ctx.client.request("POST", f"/v1/projects/{pid}/agent/alerts", json_body=bad_metric, token=ctx.owner_token)
    assert r1.status == 400, f"Invalid alert metric expected 400, got {r1.status}"

    bad_op = fixtures.make_alert_rule(name=ctx.unique_id("al_bad_op"), operator="neq")
    r2 = ctx.client.request("POST", f"/v1/projects/{pid}/agent/alerts", json_body=bad_op, token=ctx.owner_token)
    assert r2.status == 400, f"Invalid alert operator expected 400, got {r2.status}"

    empty_name = fixtures.make_alert_rule(name="   ")
    r3 = ctx.client.request("POST", f"/v1/projects/{pid}/agent/alerts", json_body=empty_name, token=ctx.owner_token)
    assert r3.status == 400, f"Empty alert name expected 400, got {r3.status}"


@test_case(
    id="T2-F19-02",
    tier=2,
    feature="F19",
    title="Alert cooldown deduplication suppresses duplicate firing events/webhooks during window",
    tags=("alerts", "cooldown", "dedup", "webhook"),
)
def test_f19_alert_cooldown_deduplication(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f19_cd")
    secret = "e2e_webhook_secret_min_16_chars"
    ctx.mock_webhook.signing_secret = secret
    _configure_byok_webhook(ctx.client, proj.id, ctx.owner_token, ctx.mock_webhook_url, secret)

    rule = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(
            name=ctx.unique_id("cd_rule"),
            metric="failure_rate",
            operator="gt",
            threshold=0.1,
            window_minutes=60,
            cooldown_minutes=15,
            webhook_url=ctx.mock_webhook_url,
        ),
        token=ctx.owner_token,
    ).raise_for_status().json()

    now_ms = int(time.time() * 1000)
    b1 = fixtures.make_batch(seed=ctx.unique_id("fail1"), project_id=proj.id, start_ts_ms=now_ms - 2000)
    b1["traces"][0]["status"] = "error"
    b1["spans"][0]["status"] = "error"
    ctx.client.ingest_json(b1, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, b1["traces"][0]["trace_id"], ctx.owner_token)

    def _trigger_and_check_events() -> list[dict[str, Any]]:
        eval_trig = ctx.client.request("POST", f"/v1/projects/{proj.id}/agent/alerts/evaluate", token=ctx.owner_token)
        assert eval_trig.status in (200, 202, 404, 405)
        evs = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.owner_token)
            .raise_for_status()
            .json(),
            "events",
        )
        return [e for e in evs if e.get("rule_id") == rule["id"] and e.get("event_type") == "triggered"]

    evs1 = _poll_until(_trigger_and_check_events, timeout=20.0, interval=0.5, desc="initial alert triggered event")
    assert len(evs1) == 1
    ctx.mock_webhook.wait_for_count(1, timeout=10.0)

    # Ingest another failing trace and re-evaluate during 15m cooldown -> must NOT spam another triggered event
    b2 = fixtures.make_batch(seed=ctx.unique_id("fail2"), project_id=proj.id, start_ts_ms=now_ms - 1000)
    b2["traces"][0]["status"] = "error"
    b2["spans"][0]["status"] = "error"
    ctx.client.ingest_json(b2, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, b2["traces"][0]["trace_id"], ctx.owner_token)

    ctx.client.request("POST", f"/v1/projects/{proj.id}/agent/alerts/evaluate", token=ctx.owner_token)
    time.sleep(1.0)
    evs2 = _trigger_and_check_events()
    assert len(evs2) == 1, f"Expected cooldown deduplication to keep 1 triggered event, got {len(evs2)}"
    assert len(ctx.mock_webhook.requests) == 1


@test_case(
    id="T2-F19-03",
    tier=2,
    feature="F19",
    title="Alert firing -> ok -> resolved lifecycle with valid HMAC-SHA256 webhook signatures",
    tags=("alerts", "recovery", "hmac-sha256", "webhook"),
)
def test_f19_alert_recovery_resolved_and_hmac_signature(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f19_rec")
    secret = "e2e_hmac_signing_secret_32bytes!"
    ctx.mock_webhook.signing_secret = secret
    _configure_byok_webhook(ctx.client, proj.id, ctx.owner_token, ctx.mock_webhook_url, secret)

    rule = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(
            name=ctx.unique_id("rec_rule"),
            metric="failure_rate",
            operator="gt",
            threshold=0.4,
            window_minutes=60,
            cooldown_minutes=0,
            webhook_url=ctx.mock_webhook_url,
        ),
        token=ctx.owner_token,
    ).raise_for_status().json()

    now_ms = int(time.time() * 1000)
    bf = fixtures.make_batch(seed=ctx.unique_id("rec_fail"), project_id=proj.id, start_ts_ms=now_ms - 5000)
    bf["traces"][0]["status"] = "error"
    bf["spans"][0]["status"] = "error"
    ctx.client.ingest_json(bf, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, bf["traces"][0]["trace_id"], ctx.owner_token)

    def _wait_firing() -> bool:
        ctx.client.request("POST", f"/v1/projects/{proj.id}/agent/alerts/evaluate", token=ctx.owner_token)
        rules = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.owner_token)
            .raise_for_status()
            .json(),
            "rules",
            "alerts",
        )
        return any(r["id"] == rule["id"] and r.get("state") == "firing" for r in rules)

    _poll_until(_wait_firing, timeout=20.0, interval=0.5, desc="alert state=firing")
    reqs_trig = ctx.mock_webhook.wait_for_count(1, timeout=10.0)
    assert reqs_trig[0]["signature_valid"] is True, f"Invalid HMAC on triggered webhook: {reqs_trig[0]}"

    # Drive failure_rate down to 1/4 = 0.25 (< 0.4)
    for i in range(3):
        bo = fixtures.make_batch(seed=ctx.unique_id(f"rec_ok_{i}"), project_id=proj.id, start_ts_ms=now_ms - 1000 + i * 100)
        ctx.client.ingest_json(bo, api_key=proj.pk_key).raise_for_status()
        _wait_trace_visible(ctx.client, proj.id, bo["traces"][0]["trace_id"], ctx.owner_token)

    def _wait_resolved() -> dict[str, Any] | None:
        ctx.client.request("POST", f"/v1/projects/{proj.id}/agent/alerts/evaluate", token=ctx.owner_token)
        evs = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.owner_token)
            .raise_for_status()
            .json(),
            "events",
        )
        for e in evs:
            if e.get("rule_id") == rule["id"] and e.get("event_type") == "resolved":
                return e
        return None

    resolved_ev = _poll_until(_wait_resolved, timeout=20.0, interval=0.5, desc="alert resolved event")
    assert resolved_ev["event_type"] == "resolved"
    reqs_all = ctx.mock_webhook.wait_for_count(2, timeout=10.0)
    assert all(r["signature_valid"] is True for r in reqs_all), "HMAC signature verification failed on alert webhooks"


@test_case(
    id="T2-F19-04",
    tier=2,
    feature="F19",
    title="SSRF webhook URL rejected when private endpoints disabled, and unreachable webhook records delivered=false",
    tags=("alerts", "ssrf", "webhook", "resilience"),
)
def test_f19_ssrf_rejection_and_unreachable_webhook_delivered_false(ctx: TestContext) -> None:
    # 1. Isolated node WITHOUT PULSE_ALLOW_PRIVATE_ENDPOINTS=1 must block SSRF metadata/loopback URLs
    root_tok = f"sk_root_e2e_{ctx.unique_id('ssrf')}"
    inst = ctx.node_manager.start_node(
        role="all",
        admin_token=root_tok,
        extra_env={"PULSE_ALLOW_PRIVATE_ENDPOINTS": "0"},
    )
    try:
        iso_client = PulseClient(inst.base_url)
        iso_proj = _setup_isolated_project(iso_client, root_tok, ctx.unique_id("ssrf_proj"))
        for bad_url in (
            "http://169.254.169.254/latest/meta-data/",
            "http://127.0.0.1:8080/hook",
            "https://localhost/hook",
        ):
            r = iso_client.request(
                "POST",
                f"/v1/projects/{iso_proj.id}/agent/alerts",
                json_body=fixtures.make_alert_rule(name=ctx.unique_id("ssrf"), webhook_url=bad_url),
                token=root_tok,
            )
            assert r.status == 400, f"Expected SSRF 400 for {bad_url}, got {r.status}: {r.text(errors='replace')}"
    finally:
        ctx.node_manager.stop(inst)

    # 2. Unreachable closed loopback port on shared node records delivered=false without crashing
    proj = _create_throwaway_project(ctx, "f19_dead_hook")
    dead_url = f"http://127.0.0.1:{_unused_closed_port()}/dead_hook"
    _configure_byok_webhook(ctx.client, proj.id, ctx.owner_token, dead_url, "dead_hook_signing_secret_123")
    rule = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/alerts",
        json_body=fixtures.make_alert_rule(
            name=ctx.unique_id("dead_rule"),
            metric="failure_rate",
            operator="gt",
            threshold=0.1,
            window_minutes=60,
            webhook_url=dead_url,
        ),
        token=ctx.owner_token,
    ).raise_for_status().json()

    b = fixtures.make_batch(seed=ctx.unique_id("dead_fail"), project_id=proj.id, start_ts_ms=int(time.time() * 1000) - 1000)
    b["traces"][0]["status"] = "error"
    b["spans"][0]["status"] = "error"
    ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, proj.id, b["traces"][0]["trace_id"], ctx.owner_token)

    def _find_failed_event() -> dict[str, Any] | None:
        ctx.client.request("POST", f"/v1/projects/{proj.id}/agent/alerts/evaluate", token=ctx.owner_token)
        evs = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.owner_token)
            .raise_for_status()
            .json(),
            "events",
        )
        for e in evs:
            if e.get("rule_id") == rule["id"]:
                return e
        return None

    ev = _poll_until(_find_failed_event, timeout=20.0, interval=0.5, desc="unreachable webhook alert event")
    assert ev["delivered"] is False
    assert ev.get("delivery_error"), f"Expected non-empty delivery_error, got {ev}"
    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F19-05",
    tier=2,
    feature="F19",
    title="Member and Viewer blocked 403 on PUT .../privacy and POST .../pricing; retention/sample bounds clamped",
    tags=("rbac", "privacy", "pricing", "403"),
)
def test_f19_member_viewer_blocked_on_privacy_pricing_and_bounds(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f19_rbac")
    for tok in (ctx.viewer_token, ctx.member_token):
        r_priv = ctx.client.request(
            "PUT",
            f"/v1/projects/{proj.id}/agent/privacy",
            json_body=fixtures.make_privacy_policy(project_id=proj.id),
            token=tok,
        )
        assert r_priv.status == 403
        r_price = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/pricing",
            json_body=fixtures.make_pricing_rule(),
            token=tok,
        )
        assert r_price.status == 403

    # Admin updates privacy with out-of-range sample_rate / retention_days -> clamped or 400, never 500
    r_ok = ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, retention_days=99999, sample_rate=5.0),
        token=ctx.admin_token,
    )
    assert r_ok.status in (200, 400), f"Unexpected status {r_ok.status}"
    if r_ok.status == 200:
        pol = r_ok.json()
        assert 1 <= int(pol["retention_days"]) <= 3650
        assert 0.0 <= float(pol["sample_rate"]) <= 1.0


@test_case(
    id="T2-F19-06",
    tier=2,
    feature="F19",
    title="Privacy capture toggles, redact_keys, exclude_attributes & BYOK pricing override applied on ingest",
    tags=("privacy", "pricing", "redaction", "cost-nanos"),
)
def test_f19_privacy_capture_redaction_and_pricing_override(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f19_priv_price")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(
            project_id=proj.id,
            capture_inputs=False,
            capture_outputs=True,
            capture_user_id=False,
            redact_pii=True,
            redact_keys=["secret_api_key"],
            exclude_attributes=["internal.debug"],
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="custom-tier2-",
            input_per_1m_usd=10.0,
            output_per_1m_usd=20.0,
            cached_input_per_1m_usd=2.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    tr_id = ctx.unique_id("tr_pp")
    sp_id = ctx.unique_id("sp_pp")
    sp = fixtures.make_span(
        sp_id,
        tr_id,
        project_id=proj.id,
        model="custom-tier2-v1",
        provider="openai",
        input_tokens=100_000,
        output_tokens=50_000,
        cached_tokens=0,
        cost_nanos=0,
        cost_usd=0.0,
        input_payload={"user_prompt": "hidden"},
        output_payload={"secret_api_key": "sk-span-secret"},
        attributes={"internal.debug": "drop_me", "keep_attr": "ok"},
    )
    trace = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        user_id="should_be_stripped_uid",
        input_payload={"prompt": "secret prompt"},
        output_payload={"secret_api_key": "sk-top-secret-999", "email": "alice@example.com"},
        spans=[sp],
    )
    ing = ctx.client.ingest_json({"traces": [trace], "spans": [sp]}, api_key=proj.pk_key).raise_for_status().json()
    assert ing["redacted_fields"] >= 1

    detail = _wait_trace_visible(ctx.client, proj.id, tr_id, ctx.owner_token)
    t_out = detail["trace"]
    assert t_out.get("user_id") is None, f"capture_user_id=false failed: {t_out.get('user_id')}"
    assert t_out.get("input") is None, f"capture_inputs=false failed: {t_out.get('input')}"
    spans_out = detail.get("spans") or []
    assert len(spans_out) == 1, f"Expected 1 hydrated span, got {spans_out}"
    sp_out = spans_out[0]
    assert sp_out.get("input") is None, f"span capture_inputs=false failed: {sp_out.get('input')}"
    assert isinstance(sp_out.get("output"), dict) and sp_out["output"].get("secret_api_key") == "[REDACTED]", (
        f"Expected redacted span output, got {sp_out.get('output')}"
    )
    sp_attrs = sp_out.get("attributes") or {}
    assert "internal.debug" not in sp_attrs, f"exclude_attributes failed to drop internal.debug: {sp_attrs}"
    assert sp_attrs.get("keep_attr") == "ok", f"Expected keep_attr=='ok' retained, got {sp_attrs}"
    raw_detail = json.dumps(detail)
    assert "sk-top-secret-999" not in raw_detail
    assert "sk-span-secret" not in raw_detail
    assert "alice@example.com" not in raw_detail
    assert "drop_me" not in raw_detail

    # Expected cost: 100k/1M * $10 + 50k/1M * $20 = $1.00 + $1.00 = $2.00 (2_000_000_000 nanos)
    assert abs(float(t_out["total_cost_usd"]) - 2.0) < 0.05, f"Expected ~$2.00 from pricing override, got {t_out['total_cost_usd']}"


# ---------------------------------------------------------------------------
# F20: Node Roles, Graceful Shutdown, Prometheus & --seed
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F20-01",
    tier=2,
    feature="F20",
    title="--role ingest serves ingest/healthz/metrics and returns 404 on query/admin/MCP routes",
    tags=("roles", "ingest-node", "404"),
)
def test_f20_role_ingest_rejects_query_routes(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('r_ing')}"
    inst = ctx.node_manager.start_node(role="ingest", admin_token=root_tok)
    try:
        c = PulseClient(inst.base_url)
        assert c.get_healthz().status == 200
        assert c.get_readyz().status == 200
        assert c.get_metrics().status == 200

        for method, path in [
            ("GET", "/v1/status"),
            ("GET", "/v1/projects/proj_default/agent/traces"),
            ("GET", "/v1/projects/proj_default/agent/sessions"),
            ("POST", "/v1/mcp"),
        ]:
            r = c.request(method, path, token=root_tok, json_body={} if method == "POST" else None)
            assert r.status == 404, f"--role ingest {method} {path} expected 404, got {r.status}"

        pk = inst.extract_boot_ingest_key()
        if pk:
            b = fixtures.make_batch(seed=ctx.unique_id("ing_only"), project_id="proj_default")
            assert c.ingest_json(b, api_key=pk).status == 200
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F20-02",
    tier=2,
    feature="F20",
    title="--role query serves query/admin routes and returns 404 on public ingest routes",
    tags=("roles", "query-node", "404"),
)
def test_f20_role_query_rejects_ingest_routes(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('r_qry')}"
    inst = ctx.node_manager.start_node(role="query", admin_token=root_tok)
    try:
        c = PulseClient(inst.base_url)
        assert c.get_healthz().status == 200
        assert c.get_readyz().status == 200
        assert c.get_metrics().status == 200
        assert c.request("GET", "/v1/projects/proj_default/agent/traces", token=root_tok).status == 200

        pk = inst.extract_boot_ingest_key() or "pk_live_dummy"
        b = fixtures.make_batch(seed=ctx.unique_id("q_no_ing"), project_id="proj_default")
        assert c.ingest_json(b, api_key=pk).status == 404
        assert c.ingest_otlp_json(fixtures.make_otlp_export_request(), api_key=pk).status == 404
        assert c.request("POST", "/v1/ingest", json_body={"batch": []}, api_key=pk).status == 404
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F20-03",
    tier=2,
    feature="F20",
    title="--role compactor serves healthz/readyz/metrics and rejects all /v1/* routes with 404",
    tags=("roles", "compactor-node", "404"),
)
def test_f20_role_compactor_rejects_ingest_and_query(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('r_cmp')}"
    inst = ctx.node_manager.start_node(role="compactor", admin_token=root_tok)
    try:
        c = PulseClient(inst.base_url)
        assert c.get_healthz().status == 200
        assert c.get_readyz().status == 200
        assert c.get_metrics().status == 200

        for method, path in [
            ("POST", "/v1/agent/traces"),
            ("POST", "/v1/otel/v1/traces"),
            ("GET", "/v1/status"),
            ("GET", "/v1/projects/proj_default/agent/traces"),
            ("POST", "/v1/mcp"),
        ]:
            r = c.request(method, path, token=root_tok, json_body={})
            assert r.status == 404, f"--role compactor {method} {path} expected 404, got {r.status}"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F20-04",
    tier=2,
    feature="F20",
    title="/metrics exposes 5 zero-cardinality agent counters with zero project_id label leakage",
    tags=("metrics", "prometheus", "cardinality", "tenancy"),
)
def test_f20_metrics_zero_project_id_leakage_and_agent_counters(ctx: TestContext) -> None:
    b = fixtures.make_batch(seed=ctx.unique_id("met_b"), project_id=ctx.proj_a.id)
    ctx.client.ingest_plst_batch(b, api_key=ctx.proj_a.pk_key).raise_for_status()

    resp = ctx.client.get_metrics()
    assert resp.status == 200
    text = resp.text()
    assert "project_id=" not in text, "Tenant project_id= label leaked into /metrics"
    assert ctx.proj_a.id not in text, f"Raw project ID {ctx.proj_a.id} leaked into /metrics"
    assert ctx.proj_b.id not in text

    required_counters = (
        "pulse_agent_traces_ingested_total",
        "pulse_agent_spans_ingested_total",
        "pulse_agent_blobs_deduped_total",
        "pulse_agent_evals_executed_total",
        "pulse_agent_eval_queue_dropped_total",
    )
    for name in required_counters:
        m = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9]+(?:\.[0-9]+)?)$", text, re.MULTILINE)
        assert m is not None, f"Missing Prometheus counter {name} in /metrics"
        assert float(m.group(1)) >= 0.0
        # Ensure counter has no tenant label
        line = m.group(0)
        assert "project" not in line and "tenant" not in line


@test_case(
    id="T2-F20-05",
    tier=2,
    feature="F20",
    title="PULSE_METRICS_TOKEN enforces 401 on unauthenticated/wrong Bearer /metrics and 200 on valid token",
    tags=("metrics", "auth", "401"),
)
def test_f20_metrics_token_enforces_401(ctx: TestContext) -> None:
    secret_tok = f"met_tok_{ctx.unique_id('m')}"
    inst = ctx.node_manager.start_node(
        role="all",
        admin_token=f"sk_root_e2e_{ctx.unique_id('adm')}",
        extra_env={"PULSE_METRICS_TOKEN": secret_tok},
    )
    try:
        c = PulseClient(inst.base_url)
        assert c.get_metrics().status == 401
        assert c.get_metrics(token="wrong_metrics_token").status == 401
        ok = c.get_metrics(token=secret_tok)
        assert ok.status == 200
        assert "pulse_http_requests_total" in ok.text()
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F20-06",
    tier=2,
    feature="F20",
    title="--seed --no-quic boots seeded agent data and is idempotent on restart with same --data-dir",
    tags=("seed", "idempotency", "restart"),
)
def test_f20_seed_idempotency_across_restart(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('seed')}"
    inst = ctx.node_manager.start_seeded(admin_token=root_tok)
    try:
        c1 = PulseClient(inst.base_url)
        pid = "proj_fintech_in"
        t1 = c1.request("GET", f"/v1/projects/{pid}/agent/traces", query={"limit": 100}, token=root_tok).raise_for_status().json()
        s1 = c1.request("GET", f"/v1/projects/{pid}/agent/sessions", query={"limit": 100}, token=root_tok).raise_for_status().json()
        d1 = c1.request("GET", f"/v1/projects/{pid}/agent/datasets", token=root_tok).raise_for_status().json()
        e1 = c1.request("GET", f"/v1/projects/{pid}/agent/experiments", token=root_tok).raise_for_status().json()

        assert t1["total"] > 0, "Expected seeded agent traces"
        assert s1["total"] > 0, "Expected seeded agent sessions"
        assert len(_extract_items(d1, "datasets")) > 0, "Expected seeded agent datasets"
        assert len(_extract_items(e1, "experiments")) > 0, "Expected seeded agent experiments"

        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst.base_url)
        t2 = c2.request("GET", f"/v1/projects/{pid}/agent/traces", query={"limit": 100}, token=root_tok).raise_for_status().json()
        s2 = c2.request("GET", f"/v1/projects/{pid}/agent/sessions", query={"limit": 100}, token=root_tok).raise_for_status().json()
        d2 = c2.request("GET", f"/v1/projects/{pid}/agent/datasets", token=root_tok).raise_for_status().json()
        e2 = c2.request("GET", f"/v1/projects/{pid}/agent/experiments", token=root_tok).raise_for_status().json()

        assert t2["total"] == t1["total"], f"Seeded traces duplicated on restart: {t1['total']} -> {t2['total']}"
        assert s2["total"] == s1["total"], f"Seeded sessions duplicated on restart: {s1['total']} -> {s2['total']}"
        assert len(_extract_items(d2, "datasets")) == len(_extract_items(d1, "datasets"))
        assert len(_extract_items(e2, "experiments")) == len(_extract_items(e1, "experiments"))
    finally:
        ctx.node_manager.stop(inst)


# ---------------------------------------------------------------------------
# F21: Pulse Studio UI (#/agents) & Session Replay Link
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F21-01",
    tier=2,
    feature="F21",
    title="Studio SPA serves #/agents bundle with security headers while protected agent APIs return 401",
    tags=("ui", "studio", "security-headers", "401"),
)
def test_f21_studio_spa_security_headers_and_api_401(ctx: TestContext) -> None:
    root_resp = ctx.client.request("GET", "/")
    assert root_resp.status == 200
    assert root_resp.header("x-content-type-options") == "nosniff"
    assert root_resp.header("x-frame-options") == "DENY"
    csp = root_resp.header("content-security-policy") or ""
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp

    studio = ctx.client.check_studio_static(require_agents_route=True)
    assert studio["agents_route_detected"] is True

    for path in (
        f"/v1/projects/{ctx.proj_a.id}/agent/traces",
        f"/v1/projects/{ctx.proj_a.id}/agent/sessions",
        f"/v1/projects/{ctx.proj_a.id}/agent/analytics",
    ):
        r = ctx.client.request("GET", path)
        assert r.status == 401, f"Unauthenticated {path} expected 401, got {r.status}"


@test_case(
    id="T2-F21-02",
    tier=2,
    feature="F21",
    title="Unknown /v1/* paths return 404 JSON/status instead of SPA index.html fallback",
    tags=("ui", "routing", "404", "spa-fallback"),
)
def test_f21_unknown_v1_api_routes_do_not_fallback_to_html(ctx: TestContext) -> None:
    for path in (
        f"/v1/projects/{ctx.proj_a.id}/agent/does-not-exist",
        "/v1/agent/unknown-endpoint",
        "/v1/nonexistent/route",
    ):
        r = ctx.client.request("GET", path, token=ctx.owner_token)
        assert r.status == 404, f"Unknown API route {path} expected 404, got {r.status}"
        assert "<div id=\"root\">" not in r.text(errors="replace")


@test_case(
    id="T2-F21-03",
    tier=2,
    feature="F21",
    title="Oversized >64 KiB JSON payloads are bounded with _truncated=true for UI trace viewer",
    tags=("ui", "payload-cap", "truncation"),
)
def test_f21_oversized_payload_truncated_to_64kib_for_ui(ctx: TestContext) -> None:
    b = fixtures.make_oversized_payload_batch(
        seed=ctx.unique_id("ui_big"),
        project_id=ctx.proj_a.id,
        payload_bytes=80 * 1024,
    )
    tr_id = b["traces"][0]["trace_id"]
    ing = ctx.client.ingest_json(b, api_key=ctx.proj_a.pk_key).raise_for_status().json()
    assert ing["truncated_payloads"] >= 1

    detail = _wait_trace_visible(ctx.client, ctx.proj_a.id, tr_id, ctx.owner_token)
    tr_in = detail["trace"]["input"]
    assert isinstance(tr_in, dict) and tr_in.get("_truncated") is True, f"Expected _truncated=true, got {type(tr_in)}"
    assert len(json.dumps(tr_in).encode("utf-8")) <= 65_536
    sp_out = detail["spans"][0]["output"]
    assert isinstance(sp_out, dict) and sp_out.get("_truncated") is True
    assert len(json.dumps(sp_out).encode("utf-8")) <= 65_536


@test_case(
    id="T2-F21-04",
    tier=2,
    feature="F21",
    title="Empty project UI APIs return zeroed finite KPIs without NaN, Infinity, or 500",
    tags=("ui", "empty-project", "kpis", "nan"),
)
def test_f21_empty_project_zeroed_kpis(ctx: TestContext) -> None:
    empty_proj = _create_throwaway_project(ctx, "f21_empty")
    analytics = ctx.client.request(
        "GET", f"/v1/projects/{empty_proj.id}/agent/analytics", token=ctx.owner_token
    ).raise_for_status().json()
    _assert_no_nan_inf(analytics)
    assert analytics["total_executions"] == 0
    assert analytics["error_rate"] == 0.0
    assert analytics["success_rate"] == 0.0
    assert analytics["cost_per_execution_usd"] == 0.0
    assert analytics["latency"]["p95_ms"] == 0.0
    assert analytics.get("avg_eval_score") is None
    assert analytics.get("eval_pass_rate") is None

    sessions = ctx.client.request(
        "GET", f"/v1/projects/{empty_proj.id}/agent/sessions", token=ctx.owner_token
    ).raise_for_status().json()
    assert sessions["sessions"] == [] and sessions["total"] == 0

    traces = ctx.client.request(
        "GET", f"/v1/projects/{empty_proj.id}/agent/traces", token=ctx.owner_token
    ).raise_for_status().json()
    assert traces["traces"] == [] and traces["total"] == 0


@test_case(
    id="T2-F21-05",
    tier=2,
    feature="F21",
    title="Studio bundle links agent sessions to #/replays and session detail matches trace hierarchy",
    tags=("ui", "replays", "sessions", "deep-link"),
)
def test_f21_studio_session_replay_link_and_session_detail(ctx: TestContext) -> None:
    root_html = ctx.client.request("GET", "/").raise_for_status().text()
    assets = re.findall(r'(?:src|href)="(/assets/[^"]+\.js)"', root_html)
    assert assets, "Expected compiled Studio /assets/*.js"
    bundle_text = "\n".join(ctx.client.request("GET", a).raise_for_status().text(errors="replace") for a in assets)
    assert "agents" in bundle_text
    assert "replays" in bundle_text or "replay" in bundle_text

    sess_id = ctx.unique_id("ui_sess")
    b = fixtures.make_hierarchy_batch(seed=ctx.unique_id("ui_hier"), project_id=ctx.proj_a.id)
    for t in b["traces"]:
        t["session_id"] = sess_id
    ctx.client.ingest_json(b, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_trace_visible(ctx.client, ctx.proj_a.id, b["traces"][0]["trace_id"], ctx.owner_token)

    sess_detail = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/sessions/{sess_id}",
        token=ctx.owner_token,
    ).raise_for_status().json()
    assert sess_detail["summary"]["session_id"] == sess_id
    assert sess_detail["summary"]["trace_count"] >= len(b["traces"])


# ---------------------------------------------------------------------------
# F22: TypeScript & Python PLST SDKs + examples/agent-demo/
# ---------------------------------------------------------------------------


def _run_py_sdk_probe(script_body: str, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
    py_sdk_dir = REPO_ROOT / "sdks" / "py-agent"
    assert py_sdk_dir.is_dir(), f"Python SDK directory not found: {py_sdk_dir}"
    env = dict(os.environ)
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{py_sdk_dir}{os.pathsep}{existing_pp}" if existing_pp else str(py_sdk_dir)
    return subprocess.run(
        [sys.executable, "-c", script_body],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@test_case(
    id="T2-F22-01",
    tier=2,
    feature="F22",
    title="Python SDK retries on 429/503, drops permanent 400/413, and handles unreachable host without throwing",
    tags=("sdk", "python", "retry", "resilience"),
)
def test_f22_python_sdk_retry_and_unreachable_resilience(ctx: TestContext) -> None:
    srv_retry = _MockFlakyIngestServer(scripted_statuses=[429, 503, 200], retry_after="0").start()
    srv_400 = _MockFlakyIngestServer(scripted_statuses=[400, 200, 200]).start()
    srv_413 = _MockFlakyIngestServer(scripted_statuses=[413, 200, 200]).start()
    try:
        probe = f"""
import inspect
import pulse_agent

cls = None
for name in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient"):
    if hasattr(pulse_agent, name):
        cls = getattr(pulse_agent, name)
        break
assert cls is not None, f"No client class in pulse_agent: {{dir(pulse_agent)}}"

sig = inspect.signature(cls.__init__)

def _exercise(target_url: str, label: str, max_retries: int = 4) -> None:
    kw = {{}}
    for pname in sig.parameters:
        if pname in ("api_key", "key"): kw[pname] = "pk_live_test"
        elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = target_url
        elif pname == "flush_interval_ms": kw[pname] = 50
        elif pname == "flush_interval_s": kw[pname] = 0.05
        elif pname == "max_retries": kw[pname] = max_retries
        elif pname == "retry_base_ms": kw[pname] = 20
        elif pname == "use_plst": kw[pname] = False
    client = cls(**kw)
    if hasattr(client, "trace"):
        with client.trace(name=label, agent_name="sdk-test") as tr:
            if hasattr(tr, "set_output"): tr.set_output({{"ok": True}})
    elif hasattr(client, "record_trace"):
        client.record_trace({{"trace_id": f"t_{{label}}", "agent_name": "sdk-test", "start_ts_ms": 1730000000000}})
    if hasattr(client, "flush"):
        client.flush()
    if hasattr(client, "shutdown"):
        client.shutdown()
    elif hasattr(client, "close"):
        client.close()

_exercise({srv_retry.url!r}, "retry-check", max_retries=4)
_exercise({srv_400.url!r}, "drop-400", max_retries=4)
_exercise({srv_413.url!r}, "drop-413", max_retries=4)
_exercise("http://127.0.0.1:{_unused_closed_port()}", "dead-check", max_retries=1)
"""
        proc = _run_py_sdk_probe(probe, timeout=25.0)
        assert proc.returncode == 0, f"SDK probe failed:\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}"
        assert len(srv_retry.requests) >= 3, f"Expected retries after 429 & 503, got {len(srv_retry.requests)} requests"
        assert len(srv_400.requests) == 1, (
            f"Expected permanent 400 to drop without retry (1 attempt), got {len(srv_400.requests)} requests"
        )
        assert len(srv_413.requests) == 1, (
            f"Expected permanent 413 to drop without retry (1 attempt), got {len(srv_413.requests)} requests"
        )
    finally:
        srv_retry.stop()
        srv_400.stop()
        srv_413.stop()


@test_case(
    id="T2-F22-02",
    tier=2,
    feature="F22",
    title="Python SDK sample_rate=0.0 sends zero HTTP requests",
    tags=("sdk", "python", "sampling"),
)
def test_f22_python_sdk_sample_rate_zero_sends_zero_http(ctx: TestContext) -> None:
    srv = _MockFlakyIngestServer().start()
    try:
        probe = f"""
import inspect, pulse_agent
cls = next(getattr(pulse_agent, n) for n in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient") if hasattr(pulse_agent, n))
sig = inspect.signature(cls.__init__)
kw = {{}}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = "pk_live_test"
    elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = {srv.url!r}
    elif pname == "sample_rate": kw[pname] = 0.0
client = cls(**kw)
for i in range(5):
    if hasattr(client, "trace"):
        with client.trace(name=f"sampled_out_{{i}}", agent_name="sdk-test"):
            pass
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
"""
        proc = _run_py_sdk_probe(probe)
        assert proc.returncode == 0, f"SDK sample_rate=0 probe failed:\n{proc.stderr}"
        assert len(srv.requests) == 0, f"sample_rate=0.0 must send 0 HTTP requests, got {len(srv.requests)}"
    finally:
        srv.stop()


@test_case(
    id="T2-F22-03",
    tier=2,
    feature="F22",
    title="Python SDK client-side redaction strips redact_keys before wire transmission",
    tags=("sdk", "python", "redaction", "privacy"),
)
def test_f22_python_sdk_client_side_redaction(ctx: TestContext) -> None:
    srv = _MockFlakyIngestServer().start()
    try:
        probe = f"""
import inspect, pulse_agent
cls = next(getattr(pulse_agent, n) for n in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient") if hasattr(pulse_agent, n))
sig = inspect.signature(cls.__init__)
kw = {{}}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = "pk_live_test"
    elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = {srv.url!r}
    elif pname == "redact_keys": kw[pname] = ["secret_token", "api_secret"]
    elif pname == "use_plst": kw[pname] = False
client = cls(**kw)
if hasattr(client, "trace"):
    with client.trace(
        name="redact-test",
        agent_name="sdk-test",
        input={{"secret_token": "DO_NOT_LEAK_TOKEN_987", "safe": "hello"}},
    ) as tr:
        if hasattr(tr, "set_output"):
            tr.set_output({{"api_secret": "DO_NOT_LEAK_SECRET_654", "reply": "ok"}})
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
"""
        proc = _run_py_sdk_probe(probe)
        assert proc.returncode == 0, f"SDK redaction probe failed:\n{proc.stderr}"
        assert len(srv.requests) >= 1, "Expected SDK to transmit redacted batch"
        raw_wire = b"".join(r["body"] for r in srv.requests)
        assert b"DO_NOT_LEAK_TOKEN_987" not in raw_wire
        assert b"DO_NOT_LEAK_SECRET_654" not in raw_wire
    finally:
        srv.stop()


@test_case(
    id="T2-F22-04",
    tier=2,
    feature="F22",
    title="Python SDK clips oversized >64 KiB payloads and supports JSON fallback to live node",
    tags=("sdk", "python", "clipping", "json-fallback"),
)
def test_f22_python_sdk_oversized_clipping_and_json_fallback(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f22_clip")
    probe = f"""
import inspect, pulse_agent
cls = next(getattr(pulse_agent, n) for n in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient") if hasattr(pulse_agent, n))
sig = inspect.signature(cls.__init__)
kw = {{}}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = {proj.pk_key!r}
    elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = {ctx.base_url!r}
    elif pname == "use_plst": kw[pname] = False
client = cls(**kw)
big_str = "X" * (90 * 1024)
if hasattr(client, "trace"):
    with client.trace(name="clip-test", agent_name="sdk-clip-agent", input={{"blob": big_str}}) as tr:
        if hasattr(tr, "set_output"):
            tr.set_output({{"blob": big_str}})
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
"""
    proc = _run_py_sdk_probe(probe)
    assert proc.returncode == 0, f"SDK clipping probe failed:\n{proc.stderr}"

    traces = _poll_until(
        lambda: ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"agent_name": "sdk-clip-agent"},
            token=ctx.owner_token,
        ).raise_for_status().json().get("traces"),
        timeout=10.0,
        desc="SDK JSON fallback trace visible",
    )
    tid = traces[0]["trace_id"]
    detail = _wait_trace_visible(ctx.client, proj.id, tid, ctx.owner_token)
    assert len(json.dumps(detail["trace"].get("input")).encode("utf-8")) <= 65_536


@test_case(
    id="T2-F22-05",
    tier=2,
    feature="F22",
    title="TypeScript & Python SDK unit test suites and live PLST round-trip pass",
    tags=("sdk", "typescript", "python", "plst"),
)
def test_f22_ts_and_py_sdk_suites_and_live_plst(ctx: TestContext) -> None:
    ts_dir = REPO_ROOT / "sdks" / "ts-agent"
    py_dir = REPO_ROOT / "sdks" / "py-agent"
    assert ts_dir.is_dir(), f"Missing TS SDK directory {ts_dir}"
    assert py_dir.is_dir(), f"Missing Python SDK directory {py_dir}"

    py_tests = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", str(py_dir / "tests")],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60.0,
    )
    assert py_tests.returncode == 0, f"py-agent unittest failed:\n{py_tests.stdout}\n{py_tests.stderr}"

    ts_tests = subprocess.run(
        ["npm", "--prefix", str(ts_dir), "test"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=90.0,
    )
    assert ts_tests.returncode == 0, f"ts-agent npm test failed:\n{ts_tests.stdout}\n{ts_tests.stderr}"

    proj = _create_throwaway_project(ctx, "f22_plst_rt")
    agent_name = ctx.unique_id("py-plst-rt").replace("_", "-")
    sess_id = ctx.unique_id("sdk_sess")
    marker = ctx.unique_id("plst_marker")
    probe = f"""
import inspect, pulse_agent
cls = next(getattr(pulse_agent, n) for n in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient") if hasattr(pulse_agent, n))
sig = inspect.signature(cls.__init__)
kw = {{}}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = {proj.pk_key!r}
    elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = {ctx.base_url!r}
    elif pname in ("use_plst", "use_binary"): kw[pname] = True
    elif pname == "flush_interval_ms": kw[pname] = 50
    elif pname == "flush_interval_s": kw[pname] = 0.05
client = cls(**kw)
if hasattr(client, "trace"):
    tr_sig = inspect.signature(client.trace)
    tr_kw = {{"name": "plst-roundtrip", "agent_name": {agent_name!r}}}
    if "session_id" in tr_sig.parameters: tr_kw["session_id"] = {sess_id!r}
    if "input" in tr_sig.parameters: tr_kw["input"] = {{"prompt": "roundtrip " + {marker!r}}}
    tr_kw = {{k: v for k, v in tr_kw.items() if k in tr_sig.parameters}}
    with client.trace(**tr_kw) as tr:
        if hasattr(tr, "generation"):
            gen_sig = inspect.signature(tr.generation)
            g_kw = {{"name": "llm.rt", "model": "gpt-4o-mini", "provider": "openai", "input_tokens": 42, "output_tokens": 18}}
            g_kw = {{k: v for k, v in g_kw.items() if k in gen_sig.parameters}}
            with tr.generation(**g_kw) as gen:
                if hasattr(gen, "set_output"):
                    gen.set_output({{"answer": {marker!r}}})
        if hasattr(tr, "set_output"):
            tr.set_output({{"reply": {marker!r}}})
elif hasattr(client, "record_trace"):
    client.record_trace({{
        "trace_id": "tr_" + {marker!r},
        "session_id": {sess_id!r},
        "agent_name": {agent_name!r},
        "start_ts_ms": 1730000000000,
        "input": {{"prompt": "roundtrip " + {marker!r}}},
        "output": {{"reply": {marker!r}}},
    }})
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
"""
    proc = _run_py_sdk_probe(probe, timeout=25.0)
    assert proc.returncode == 0, f"py-agent live PLST probe failed:\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}"

    traces = _poll_until(
        lambda: ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"agent_name": agent_name},
            token=ctx.owner_token,
        ).raise_for_status().json().get("traces"),
        timeout=12.0,
        desc="live PLST SDK trace visible",
    )
    tid = traces[0]["trace_id"]
    detail = _wait_trace_visible(ctx.client, proj.id, tid, ctx.owner_token)
    assert detail["trace"]["agent_name"] == agent_name
    assert marker in json.dumps(detail), f"Expected marker {marker!r} hydrated from live PLST trace, got {detail}"


@test_case(
    id="T2-F22-06",
    tier=2,
    feature="F22",
    title="Python SDK ring-buffer backpressure drops cleanly without deadlock or OOM",
    tags=("sdk", "python", "backpressure", "ring-buffer"),
)
def test_f22_python_sdk_ring_buffer_backpressure(ctx: TestContext) -> None:
    closed = _unused_closed_port()
    probe = f"""
import inspect, time, pulse_agent
cls = next(getattr(pulse_agent, n) for n in ("PulseAgent", "PulseAgentClient", "AgentClient", "PulseClient") if hasattr(pulse_agent, n))
sig = inspect.signature(cls.__init__)
kw = {{}}
for pname in sig.parameters:
    if pname in ("api_key", "key"): kw[pname] = "pk_live_bp"
    elif pname in ("host", "base_url", "endpoint", "url"): kw[pname] = "http://127.0.0.1:{closed}"
    elif pname in ("max_queue_size", "queue_size", "capacity"): kw[pname] = 16
    elif pname == "max_retries": kw[pname] = 0
t0 = time.monotonic()
client = cls(**kw)
for i in range(200):
    if hasattr(client, "trace"):
        with client.trace(name=f"bp_{{i}}", agent_name="bp-agent"):
            pass
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
elif hasattr(client, "close"): client.close()
elapsed = time.monotonic() - t0
assert elapsed < 10.0, f"Backpressure shutdown took too long: {{elapsed:.2f}}s"
"""
    proc = _run_py_sdk_probe(probe, timeout=15.0)
    assert proc.returncode == 0, f"SDK backpressure probe failed:\n{proc.stderr}"


# ---------------------------------------------------------------------------
# F23: Documentation & HTTP Edge Semantics Consistency
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F23-01",
    tier=2,
    feature="F23",
    title="Known paths with wrong HTTP method return 405 while unknown subpaths return 404",
    tags=("http", "405", "404", "routing"),
)
def test_f23_method_not_allowed_405_vs_unknown_subpath_404(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    for method, path in [
        ("GET", "/v1/agent/traces"),
        ("GET", "/v1/otel/v1/traces"),
        ("GET", "/v1/mcp"),
        ("PUT", f"/v1/projects/{pid}/agent/analytics"),
        ("DELETE", f"/v1/projects/{pid}/agent/sessions"),
    ]:
        r = ctx.client.request(method, path, token=ctx.owner_token, api_key=ctx.proj_a.pk_key)
        assert r.status == 405, f"{method} {path} expected 405 Method Not Allowed, got {r.status}"

    for method, path in [
        ("GET", f"/v1/projects/{pid}/agent/nonexistent_subpath"),
        ("POST", "/v1/agent/nonexistent_ingest"),
    ]:
        r = ctx.client.request(method, path, token=ctx.owner_token, api_key=ctx.proj_a.pk_key)
        assert r.status == 404, f"{method} {path} expected 404 Not Found, got {r.status}"


@test_case(
    id="T2-F23-02",
    tier=2,
    feature="F23",
    title="Missing, empty, Basic-scheme, and malformed Bearer auth headers return 401",
    tags=("http", "auth", "401", "headers"),
)
def test_f23_missing_and_malformed_auth_headers_return_401(ctx: TestContext) -> None:
    path = f"/v1/projects/{ctx.proj_a.id}/agent/traces"
    for hdrs in (
        {},
        {"Authorization": ""},
        {"Authorization": "Bearer "},
        {"Authorization": "Basic dXNlcjpwYXNz"},
        {"Authorization": "Bearer not.a.valid.jwt.or.key"},
        {"x-pulse-admin-key": ""},
    ):
        r = ctx.client.request("GET", path, headers=hdrs)
        assert r.status == 401, f"Headers {hdrs!r} on {path} expected 401, got {r.status}"


@test_case(
    id="T2-F23-03",
    tier=2,
    feature="F23",
    title="Malformed query params and long/special resource IDs never cause HTTP 500",
    tags=("http", "query-params", "error-handling", "no-500"),
)
def test_f23_malformed_query_params_and_ids_never_500(ctx: TestContext) -> None:
    pid = ctx.proj_a.id
    for q in (
        {"limit": "-5"},
        {"limit": "0"},
        {"limit": "not_an_int"},
        {"start_ts_ms": "abc", "end_ts_ms": "xyz"},
        {"start_ts_ms": "9999999999999", "end_ts_ms": "1"},
        {"min_duration_ms": "NaN"},
    ):
        r = ctx.client.request("GET", f"/v1/projects/{pid}/agent/traces", query=q, token=ctx.owner_token)
        assert r.status in (200, 400), f"Query {q!r} returned {r.status}: {r.text(errors='replace')[:200]}"

    long_id = "id_" + ("a" * 300)
    for ep in (
        f"/v1/projects/{pid}/agent/traces/{long_id}",
        f"/v1/projects/{pid}/agent/sessions/{long_id}",
        f"/v1/projects/{pid}/agent/datasets/{long_id}/items",
        f"/v1/projects/{pid}/agent/experiments/{long_id}",
    ):
        r = ctx.client.request("GET", ep, token=ctx.owner_token)
        assert r.status in (400, 404), f"Long ID path {ep} returned {r.status}"


@test_case(
    id="T2-F23-04",
    tier=2,
    feature="F23",
    title="Admin routes restrict CORS origins while public ingest allows permissive CORS",
    tags=("http", "cors", "security"),
)
def test_f23_restricted_admin_cors_vs_permissive_ingest_cors(ctx: TestContext) -> None:
    evil_origin = "https://evil-attacker.example"
    r_pub = ctx.client.request(
        "OPTIONS",
        "/v1/agent/traces",
        headers={
            "Origin": evil_origin,
            "Access-Control-Request-Method": "POST",
        },
    )
    assert r_pub.status in (200, 204)
    assert r_pub.header("access-control-allow-origin") == "*"

    r_adm = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces",
        headers={"Origin": evil_origin},
        token=ctx.owner_token,
    )
    assert r_adm.status == 200
    acao = r_adm.header("access-control-allow-origin")
    assert acao not in ("*", evil_origin), f"Admin route leaked CORS ACAO={acao!r} for untrusted origin"


@test_case(
    id="T2-F23-05",
    tier=2,
    feature="F23",
    title="Documentation files accurately reflect PLST/PTWL/PLSS architecture, routes, and 4 MCP tools",
    tags=("docs", "contract-parity", "mcp"),
)
def test_f23_docs_architecture_rest_and_mcp_consistency(ctx: TestContext) -> None:
    rest_doc = (REPO_ROOT / "docs" / "rest-api.md").read_text(encoding="utf-8")
    mcp_doc = (REPO_ROOT / "docs" / "mcp.md").read_text(encoding="utf-8")
    arch_doc = (REPO_ROOT / "docs" / "agent-observability-architecture.md").read_text(encoding="utf-8")
    readme_doc = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    for doc_text, label in ((rest_doc, "rest-api.md"), (arch_doc, "agent-observability-architecture.md")):
        assert "/v1/agent/traces" in doc_text, f"{label} missing /v1/agent/traces"
        assert "/v1/otel/v1/traces" in doc_text, f"{label} missing /v1/otel/v1/traces"
        assert "PLST" in doc_text, f"{label} missing PLST protocol reference"
        assert "application/x-pulse-trace" in doc_text, f"{label} missing application/x-pulse-trace"

    for tool in (
        "list_agent_traces",
        "get_agent_trace",
        "query_agent_analytics",
        "list_agent_evaluations",
    ):
        assert tool in mcp_doc, f"docs/mcp.md missing agent tool {tool}"

    assert "PLSS" in arch_doc and "PTWL" in arch_doc
    assert "agent_traces" not in arch_doc or "removed" in arch_doc.lower()
    assert "#/agents" in readme_doc or "agent" in readme_doc.lower()


# ---------------------------------------------------------------------------
# F24: End-to-End Acceptance & Adversarial Integration
# ---------------------------------------------------------------------------


@test_case(
    id="T2-F24-01",
    tier=2,
    feature="F24",
    title="Concurrent multi-protocol ingest (PLST + PLST-dict + JSON + OTLP/JSON + OTLP/PB) zero errors",
    tags=("stress", "concurrency", "multi-protocol"),
)
def test_f24_concurrent_multi_protocol_ingest_stress(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f24_multi")
    expected_trace_ids: set[str] = set()
    jobs: list[tuple[str, Any, str]] = []

    for i in range(5):
        tid_p = ctx.unique_id(f"mp_plst_{i}")
        bp = fixtures.make_batch(seed=tid_p, project_id=proj.id)
        bp["traces"][0]["trace_id"] = tid_p
        for sp in bp["spans"]:
            sp["trace_id"] = tid_p
        expected_trace_ids.add(tid_p)
        jobs.append(("plst_raw" if i % 2 == 0 else "plst_dict", bp, tid_p))

        tid_j = ctx.unique_id(f"mp_json_{i}")
        bj = fixtures.make_batch(seed=tid_j, project_id=proj.id)
        bj["traces"][0]["trace_id"] = tid_j
        for sp in bj["spans"]:
            sp["trace_id"] = tid_j
        expected_trace_ids.add(tid_j)
        jobs.append(("json", bj, tid_j))

        ot_j = fixtures.make_otlp_export_request(seed=ctx.unique_id(f"mp_oj_{i}"))
        tr_oj = ot_j["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
        expected_trace_ids.add(tr_oj)
        jobs.append(("otlp_json", ot_j, tr_oj))

        ot_pb = fixtures.make_otlp_export_request(seed=ctx.unique_id(f"mp_opb_{i}"))
        tr_opb = ot_pb["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
        expected_trace_ids.add(tr_opb)
        jobs.append(("otlp_pb", ot_pb, tr_opb))

    def _run_job(item: tuple[str, Any, str]) -> int:
        kind, payload, _ = item
        c = PulseClient(ctx.base_url)
        if kind == "plst_raw":
            r = c.ingest_plst_batch(payload, api_key=proj.pk_key, use_dict=False)
        elif kind == "plst_dict":
            r = c.ingest_plst_batch(payload, api_key=proj.pk_key, use_dict=True)
        elif kind == "json":
            r = c.ingest_json(payload, api_key=proj.pk_key)
        elif kind == "otlp_json":
            r = c.ingest_otlp_json(payload, api_key=proj.pk_key)
        else:
            r = c.ingest_otlp_pb(payload, api_key=proj.pk_key)
        return r.status

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(_run_job, jobs))

    assert all(s == 200 for s in statuses), f"Expected all 200s under concurrent ingest, got {statuses}"

    def _all_present() -> bool:
        resp = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"limit": 100},
            token=ctx.owner_token,
        ).raise_for_status().json()
        got_ids = {t["trace_id"] for t in resp["traces"]}
        return expected_trace_ids.issubset(got_ids)

    _poll_until(_all_present, timeout=15.0, desc="all 20 concurrent multi-protocol traces visible")


@test_case(
    id="T2-F24-02",
    tier=2,
    feature="F24",
    title="Concurrent ingest and 3-level erasure race leaves zero leaked erased targets and preserves siblings",
    tags=("stress", "concurrency", "erasure"),
)
def test_f24_concurrent_ingest_and_3_level_erasure(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f24_erase_race")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
        token=ctx.admin_token,
    ).raise_for_status()

    erased_tr = ctx.unique_id("race_tr")
    erased_sess = ctx.unique_id("race_sess")
    tr_sess = ctx.unique_id("tr_s2")
    erased_user = ctx.unique_id("race_usr")
    tr_user = ctx.unique_id("tr_u3")
    keep_tr = ctx.unique_id("race_keep")

    for tid, sid, uid in [
        (erased_tr, ctx.unique_id("s1"), ctx.unique_id("u1")),
        (tr_sess, erased_sess, ctx.unique_id("u2")),
        (tr_user, ctx.unique_id("s3"), erased_user),
        (keep_tr, ctx.unique_id("s_keep"), ctx.unique_id("u_keep")),
    ]:
        b = fixtures.make_batch(seed=tid, project_id=proj.id, session_id=sid, user_id=uid)
        b["traces"][0]["trace_id"] = tid
        for sp in b["spans"]:
            sp["trace_id"] = tid
        ctx.client.ingest_json(b, api_key=proj.pk_key).raise_for_status()

    pre_user = _wait_trace_visible(ctx.client, proj.id, tr_user, ctx.owner_token)
    assert pre_user["trace"].get("user_id") == erased_user, (
        f"Expected user_id={erased_user!r} retained before erasure, got {pre_user['trace'].get('user_id')!r}"
    )

    def _ingest_more(idx: int) -> int:
        c = PulseClient(ctx.base_url)
        tid = ctx.unique_id(f"race_bg_{idx}")
        b = fixtures.make_batch(seed=tid, project_id=proj.id, session_id=erased_sess if idx % 2 == 0 else ctx.unique_id("s_ok"))
        b["traces"][0]["trace_id"] = tid
        for sp in b["spans"]:
            sp["trace_id"] = tid
        return c.ingest_json(b, api_key=proj.pk_key).status

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(_ingest_more, i) for i in range(8)]
        _erase_agent_target(ctx.client, proj.id, ctx.admin_token, "trace", erased_tr)
        _erase_agent_target(ctx.client, proj.id, ctx.admin_token, "session", erased_sess)
        _erase_agent_target(ctx.client, proj.id, ctx.admin_token, "user", erased_user)
        statuses = [f.result() for f in futs]

    assert all(s == 200 for s in statuses)
    _wait_trace_visible(ctx.client, proj.id, keep_tr, ctx.owner_token)

    assert ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/{erased_tr}", token=ctx.owner_token).status == 404
    assert ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/{tr_sess}", token=ctx.owner_token).status == 404
    assert ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces/{tr_user}", token=ctx.owner_token).status == 404

    traces = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/traces", query={"limit": 100}, token=ctx.owner_token
    ).raise_for_status().json()["traces"]
    trace_ids = {t["trace_id"] for t in traces}
    assert erased_tr not in trace_ids
    assert tr_sess not in trace_ids
    assert tr_user not in trace_ids
    for t in traces:
        assert t["trace_id"] != erased_tr
        assert t.get("session_id") != erased_sess
        assert t.get("user_id") != erased_user
    assert any(t["trace_id"] == keep_tr for t in traces)


@test_case(
    id="T2-F24-03",
    tier=2,
    feature="F24",
    title="Adversarial failing LLM judge burst alongside deterministic evals keeps node healthy",
    tags=("stress", "eval-worker", "adversarial"),
)
def test_f24_failing_llm_judge_and_burst_ingest_resilience(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "f24_llm_stress")
    _configure_byok_llm(ctx.client, proj.id, ctx.owner_token, ctx.mock_llm_url)

    call_idx = 0
    call_lock = threading.Lock()

    def _chaos_responder(_req: dict[str, Any]) -> tuple[int, dict[str, Any] | bytes]:
        nonlocal call_idx
        with call_lock:
            call_idx += 1
            n = call_idx
        if n % 3 == 0:
            return 500, {"error": "chaos 500"}
        if n % 3 == 1:
            return 200, b"not-even-json{"
        return 200, {
            "id": "cmpl-chaos",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "corrupt json response"}, "finish_reason": "stop"}],
        }

    ctx.mock_llm.custom_responder = _chaos_responder
    try:
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(name=ctx.unique_id("chaos_llm"), kind="llm_judge"),
            token=ctx.owner_token,
        ).raise_for_status()
        det_ev = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(name=ctx.unique_id("chaos_det"), kind="no_errors"),
            token=ctx.owner_token,
        ).raise_for_status().json()

        tids = []
        for i in range(6):
            tid = ctx.unique_id(f"chaos_tr_{i}")
            tids.append(tid)
            b = fixtures.make_batch(seed=tid, project_id=proj.id)
            b["traces"][0]["trace_id"] = tid
            for sp in b["spans"]:
                sp["trace_id"] = tid
            assert ctx.client.ingest_json(b, api_key=proj.pk_key).status == 200

        _wait_trace_visible(ctx.client, proj.id, tids[-1], ctx.owner_token)

        def _det_all_done() -> bool:
            evals = _extract_items(
                ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/evaluations", query={"limit": 100}, token=ctx.owner_token)
                .raise_for_status()
                .json(),
                "evaluations",
            )
            det_tids = {e["trace_id"] for e in evals if e.get("evaluator_id") == det_ev["id"]}
            return set(tids).issubset(det_tids)

        _poll_until(_det_all_done, timeout=15.0, desc="deterministic evals under LLM judge chaos")
        assert ctx.client.get_healthz().status == 200
    finally:
        ctx.mock_llm.custom_responder = None


@test_case(
    id="T2-F24-04",
    tier=2,
    feature="F24",
    title="Split-role cluster (compactor + ingest + query) enforces role isolation and cross-node visibility",
    tags=("split-role", "cluster", "wal", "query"),
)
def test_f24_split_role_cluster_isolation_and_visibility(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('split')}"
    cluster = ctx.node_manager.start_split_cluster(admin_token=root_tok)
    try:
        c_ing = PulseClient(cluster["ingest"].base_url)
        c_qry = PulseClient(cluster["query"].base_url)
        c_cmp = PulseClient(cluster["compactor"].base_url)

        # Create project on query/control node
        proj = _setup_isolated_project(c_qry, root_tok, ctx.unique_id("split_proj"))

        # Role boundary assertions
        assert c_ing.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok).status == 404
        assert c_cmp.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=root_tok).status == 404
        assert c_cmp.get_readyz().status == 200

        b = fixtures.make_batch(seed=ctx.unique_id("split_b"), project_id=proj.id)
        tr_id = b["traces"][0]["trace_id"]
        assert c_qry.ingest_json(b, api_key=proj.pk_key).status == 404

        # Ingest node reloads project/API key cache within a few seconds; retry briefly on 401
        def _ingest_after_cache_sync() -> bool:
            r = c_ing.ingest_plst_batch(b, api_key=proj.pk_key, use_dict=True)
            if r.status == 200:
                return True
            assert r.status in (401, 403), f"Unexpected ingest status on split cluster: {r.status}"
            return False

        _poll_until(_ingest_after_cache_sync, timeout=15.0, interval=0.5, desc="ingest node API key cache sync")

        detail = _wait_trace_visible(c_qry, proj.id, tr_id, root_tok, timeout=20.0)
        assert detail["trace"]["trace_id"] == tr_id
    finally:
        for role_inst in cluster.values():
            ctx.node_manager.stop(role_inst)


@test_case(
    id="T2-F24-05",
    tier=2,
    feature="F24",
    title="Graceful shutdown drain and restart preserves ingested PLST/OTLP traces and SQL metadata",
    tags=("shutdown", "drain", "restart", "durability"),
)
def test_f24_graceful_shutdown_drain_and_restart_durability(ctx: TestContext) -> None:
    root_tok = f"sk_root_e2e_{ctx.unique_id('drain')}"
    inst = ctx.node_manager.start_node(role="all", admin_token=root_tok, drain_ms=300)
    try:
        c1 = PulseClient(inst.base_url)
        proj = _setup_isolated_project(c1, root_tok, ctx.unique_id("drain_proj"))

        b_plst = fixtures.make_hierarchy_batch(seed=ctx.unique_id("dr_plst"), project_id=proj.id)
        tr_plst = b_plst["traces"][0]["trace_id"]
        c1.ingest_plst_batch(b_plst, api_key=proj.pk_key, use_dict=True).raise_for_status()

        otlp = fixtures.make_otlp_export_request(seed=ctx.unique_id("dr_otlp"))
        tr_otlp = otlp["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
        c1.ingest_otlp_pb(otlp, api_key=proj.pk_key).raise_for_status()

        ds = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/datasets",
            json_body=fixtures.make_dataset(name=ctx.unique_id("dr_ds")),
            token=root_tok,
        ).raise_for_status().json()
        ev = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(name=ctx.unique_id("dr_ev"), kind="no_errors"),
            token=root_tok,
        ).raise_for_status().json()

        _wait_trace_visible(c1, proj.id, tr_plst, root_tok)
        _wait_trace_visible(c1, proj.id, tr_otlp, root_tok)

        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst.base_url)

        assert _wait_trace_visible(c2, proj.id, tr_plst, root_tok)["trace"]["trace_id"] == tr_plst
        assert _wait_trace_visible(c2, proj.id, tr_otlp, root_tok)["trace"]["trace_id"] == tr_otlp

        datasets = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/datasets", token=root_tok).raise_for_status().json(),
            "datasets",
        )
        evaluators = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/evaluators", token=root_tok).raise_for_status().json(),
            "evaluators",
        )
        assert any(d["id"] == ds["id"] for d in datasets)
        assert any(e["id"] == ev["id"] for e in evaluators)
    finally:
        ctx.node_manager.stop(inst)
