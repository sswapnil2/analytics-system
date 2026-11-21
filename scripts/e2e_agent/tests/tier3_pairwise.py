"""Tier 3 opaque-box cross-feature pairwise E2E tests (T3-01 .. T3-26).

Exercises 2-way and 3-way interactions across all 24 features (F01-F24).
Pure Python 3.14 stdlib + `lib.*` only. Zero import-time network/subprocess
side effects. Every test isolates mutations to throwaway projects or
`ctx.node_manager`-managed nodes cleaned up in `try/finally`.
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
from pathlib import Path
from typing import Any, Callable, TypeVar

from lib import fixtures, plst
from lib.auth import AuthBootstrap, ProjectCreds, TestContext
from lib.client import HttpResponse, PulseClient
from lib.node_mgr import NodeInstance, list_agent_artifacts, sqlite_tables
from lib.registry import test_case

_T = TypeVar("_T")
_REPO_ROOT = Path(__file__).resolve().parents[3]

_FORBIDDEN_SQL_TABLES: tuple[str, ...] = ("agent_traces", "agent_spans")
_AGENT_MCP_TOOLS: tuple[str, ...] = (
    "list_agent_traces",
    "get_agent_trace",
    "query_agent_analytics",
    "list_agent_evaluations",
)


# ---------------------------------------------------------------------------
# Local Opaque-Box Helpers (Zero Import-Time Side Effects)
# ---------------------------------------------------------------------------


def _recent_ts_ms(offset_ms: int = 10_000) -> int:
    """Return a recent UTC epoch millisecond timestamp well within retention."""
    return int(time.time() * 1000) - offset_ms


def _poll_until(
    predicate: Callable[[], _T | None],
    *,
    timeout: float = 12.0,
    interval: float = 0.15,
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
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(interval)
    if last_exc is not None:
        raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}: {last_exc}")
    raise AssertionError(f"Timed out after {timeout:.1f}s waiting for {desc}")


def _create_temp_project(
    ctx: TestContext,
    suffix: str,
    *,
    client: PulseClient | None = None,
    token: str | None = None,
    capture_user_id: bool = False,
) -> ProjectCreds:
    """Create an isolated throwaway project with pk_ and sk_ keys."""
    c = client or ctx.client
    tok = token or ctx.owner_token
    proj = AuthBootstrap._create_project_with_keys(
        c,
        token=tok,
        name=f"E2E {ctx.unique_id(suffix)}",
    )
    if capture_user_id:
        c.request(
            "PUT",
            f"/v1/projects/{proj.id}/agent/privacy",
            json_body=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
            token=tok,
        ).raise_for_status()
    return proj


def _extract_items(payload: Any, candidate_keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in candidate_keys:
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _extract_compare_delta(payload: Any, keys: tuple[str, ...]) -> dict[str, Any]:
    assert isinstance(payload, dict), f"Expected dict compare response, got {payload!r}"
    for key in keys:
        val = payload.get(key)
        if isinstance(val, dict) and "verdict" in val:
            return val
    if "verdict" in payload:
        return payload
    raise AssertionError(f"Missing MetricDelta for {keys} in {payload!r}")


def _wait_trace_detail(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 12.0,
    expected_status: str | None = None,
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
                if expected_status is not None and data["trace"].get("status") != expected_status:
                    return None
                return data
        return None

    return _poll_until(_check, timeout=timeout, interval=0.1, desc=f"trace {trace_id} visibility")


def _wait_trace_gone(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    token: str,
    *,
    timeout: float = 12.0,
) -> None:
    def _check() -> bool | None:
        resp = client.request(
            "GET",
            f"/v1/projects/{project_id}/agent/traces/{trace_id}",
            token=token,
        )
        return True if resp.status == 404 else None

    _poll_until(_check, timeout=timeout, interval=0.1, desc=f"trace {trace_id} to become 404")


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
        items = _extract_items(resp.json(), ("evaluations", "items"))
        return items if len(items) >= min_count else None

    return _poll_until(
        _check,
        timeout=timeout,
        interval=0.15,
        desc=f">={min_count} evaluations for query={query}",
    )


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


def _trigger_alert_evaluate(
    client: PulseClient,
    project_id: str,
    rule_ids: list[str],
    token: str,
) -> None:
    r = client.request(
        "POST",
        f"/v1/projects/{project_id}/agent/alerts/evaluate",
        token=token,
    )
    if r.is_success():
        return
    if r.status in (404, 405):
        for rid in rule_ids:
            client.request(
                "POST",
                f"/v1/projects/{project_id}/agent/alerts/{rid}/evaluate",
                token=token,
            ).raise_for_status()
        return
    r.raise_for_status()


def _erase_target(
    client: PulseClient,
    project_id: str,
    level: str,
    target_id: str,
    trace_id_hint: str,
    token: str,
) -> HttpResponse:
    """Invoke 3-level erasure via POST .../agent/erasure or DELETE .../traces/:id?level=..."""
    if level != "trace":
        post_resp = client.request(
            "POST",
            f"/v1/projects/{project_id}/agent/erasure",
            json_body={"level": level, "target_id": target_id},
            token=token,
        )
        if post_resp.is_success():
            return post_resp
        if post_resp.status not in (404, 405):
            raise AssertionError(
                f"Erasure POST failed for level={level} target_id={target_id}: "
                f"{post_resp.status} {post_resp.text(errors='replace')[:200]}"
            )

    del_resp = client.request(
        "DELETE",
        f"/v1/projects/{project_id}/agent/traces/{trace_id_hint}",
        query={"level": level, "target_id": target_id},
        json_body={"level": level, "target_id": target_id},
        token=token,
    )
    if del_resp.is_success():
        return del_resp
    if del_resp.status in (400, 404, 405, 422):
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
            f"DELETE->{del_resp.status}:{del_resp.text(errors='replace')[:200]} "
            f"POST->{alt.status}:{alt.text(errors='replace')[:200]}"
        )
    raise AssertionError(
        f"Erasure DELETE failed for level={level} target_id={target_id}: "
        f"{del_resp.status} {del_resp.text(errors='replace')[:200]}"
    )


def _open_sqlite_ro(data_dir: Path | None) -> sqlite3.Connection:
    assert data_dir is not None, "data_dir is required to inspect pulse_meta.db"
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


def _wait_for_artifacts(
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
        return paths if len(paths) >= min_count else None

    return _poll_until(_check, timeout=timeout, interval=0.25, desc=f">={min_count} {key} artifact(s)")


def _epoch_number(manifest_path: Path) -> int:
    m = re.search(r"epoch_(\d+)\.json$", manifest_path.name)
    if not m:
        raise AssertionError(f"Unexpected manifest filename: {manifest_path}")
    return int(m.group(1))


def _is_encrypted_bytes(raw: bytes) -> bool:
    return raw.startswith(b"PLSEAGM1")


def _ingest_plst_batch_retry_401(
    client: PulseClient,
    batch: dict[str, Any],
    *,
    api_key: str,
    use_dict: bool = False,
    timeout: float = 10.0,
) -> HttpResponse:
    deadline = time.monotonic() + timeout
    while True:
        resp = client.ingest_plst_batch(batch, api_key=api_key, use_dict=use_dict)
        if resp.status != 401 or time.monotonic() >= deadline:
            return resp.raise_for_status()
        time.sleep(0.25)


def _parse_prom_counter(metrics_text: str, name: str) -> float:
    m = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9]+(?:\.[0-9]+)?)$", metrics_text, re.MULTILINE)
    assert m is not None, f"Missing Prometheus counter {name!r} in /metrics"
    return float(m.group(1))


def _resolve_ts_sdk_entry() -> Path:
    sdk_dir = _REPO_ROOT / "sdks" / "ts-agent"
    assert sdk_dir.is_dir(), f"Missing M5 TypeScript SDK directory: {sdk_dir}"
    for rel in ("dist/index.js", "dist/src/index.js", "src/index.ts", "index.ts"):
        cand = sdk_dir / rel
        if cand.is_file():
            return cand
    raise AssertionError(f"No TS SDK entrypoint found under {sdk_dir}")


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


# ===========================================================================
# Tier 3 Cross-Feature Pairwise Tests (T3-01 .. T3-26)
# ===========================================================================


@test_case(
    id="T3-01",
    tier=3,
    feature="F01",
    title="OTLP/Protobuf parent_trace_id + links preserves 5-level hierarchy across protected trace & session filters",
    tags=("F01", "F14", "F15", "otlp_pb", "hierarchy"),
)
def test_t3_01_otlp_pb_hierarchy_and_protected_queries(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_01", capture_user_id=True)
    seed = ctx.unique_id("t301")
    sid = f"sess_{seed}"
    uid = f"user_{seed}"

    t_parent_hex = fixtures.deterministic_hex_id(f"{seed}_parent_tr", 16)
    t_child_hex = fixtures.deterministic_hex_id(f"{seed}_child_tr", 16)
    sp_root_hex = fixtures.deterministic_hex_id(f"{seed}_root_sp", 8)
    sp_child_hex = fixtures.deterministic_hex_id(f"{seed}_child_sp", 8)

    ts_ms = _recent_ts_ms()
    start_ns = str(ts_ms * 1_000_000)
    end_ns = str((ts_ms + 150) * 1_000_000)

    def _kv_s(k: str, v: str) -> dict[str, Any]:
        return {"key": k, "value": {"stringValue": v}}

    req_parent = {
        "resourceSpans": [
            {
                "resource": {"attributes": [_kv_s("service.name", "root-orchestrator")]},
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": t_parent_hex,
                                "spanId": sp_root_hex,
                                "name": "orchestrator.plan",
                                "kind": 1,
                                "startTimeUnixNano": start_ns,
                                "endTimeUnixNano": end_ns,
                                "attributes": [
                                    _kv_s("pulse.span_type", "agent"),
                                    _kv_s("session.id", sid),
                                    _kv_s("enduser.id", uid),
                                ],
                                "status": {"code": 1},
                            }
                        ]
                    }
                ],
            }
        ]
    }
    req_child = {
        "resourceSpans": [
            {
                "resource": {"attributes": [_kv_s("service.name", "worker-agent")]},
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": t_child_hex,
                                "spanId": sp_child_hex,
                                "name": "worker.execute",
                                "kind": 3,
                                "startTimeUnixNano": str((ts_ms + 20) * 1_000_000),
                                "endTimeUnixNano": str((ts_ms + 140) * 1_000_000),
                                "attributes": [
                                    _kv_s("pulse.span_type", "llm"),
                                    _kv_s("gen_ai.system", "openai"),
                                    _kv_s("gen_ai.request.model", "gpt-4o-mini"),
                                    _kv_s("session.id", sid),
                                    _kv_s("enduser.id", uid),
                                    _kv_s("pulse.parent_trace_id", t_parent_hex),
                                ],
                                "links": [{"traceId": t_parent_hex, "spanId": sp_root_hex}],
                                "status": {"code": 1},
                            }
                        ]
                    }
                ],
            }
        ]
    }

    ctx.client.ingest_otlp_pb(req_parent, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_otlp_pb(req_child, api_key=proj.pk_key).raise_for_status()

    _wait_trace_detail(ctx.client, proj.id, t_parent_hex, ctx.viewer_token)
    child_det = _wait_trace_detail(ctx.client, proj.id, t_child_hex, ctx.viewer_token)

    filtered = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={"parent_trace_id": t_parent_hex},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    filtered_ids = [t["trace_id"] for t in _extract_items(filtered, ("traces", "items"))]
    assert filtered_ids == [t_child_hex], filtered

    assert child_det["trace"]["parent_trace_id"] == t_parent_hex, child_det["trace"]
    assert any(sp_root_hex in (sp.get("linked_span_ids") or []) for sp in child_det["spans"]), child_det["spans"]

    sess_det = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/sessions/{sid}",
        token=ctx.viewer_token,
    ).raise_for_status().json()
    assert sess_det["summary"]["trace_count"] == 2, sess_det["summary"]
    assert sess_det["summary"].get("user_id") == uid, sess_det["summary"]


@test_case(
    id="T3-02",
    tier=3,
    feature="F02",
    title="Streaming span_phase (start->update->final) merges via Tag 0x02 CRDT and deduplicates loop count",
    tags=("F02", "F09", "F12", "span_phase", "crdt", "loop"),
)
def test_t3_02_streaming_span_phase_crdt_and_loop_dedup(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_02")
    seed = ctx.unique_id("t302")
    tid = f"tr_{seed}"
    sid = f"sess_{seed}"
    ts = _recent_ts_ms()
    span_ids = [f"sp_{seed}_{i}" for i in range(10)]

    for phase, st, dur, out_tok, nanos in (
        ("start", "running", 10, 0, 0),
        ("update", "running", 60, 15, 100_000),
        ("final", "ok", 140, 45, 500_000),
    ):
        spans = [
            fixtures.make_span(
                sp_id,
                tid,
                project_id=proj.id,
                name=f"llm.step.{idx}",
                span_type="llm",
                span_phase=phase,
                status=st,
                start_ts_ms=ts + idx * 10,
                duration_ms=dur,
                input_tokens=50,
                output_tokens=out_tok,
                cost_nanos=nanos,
            )
            for idx, sp_id in enumerate(span_ids)
        ]
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            session_id=sid,
            status=st,
            start_ts_ms=ts,
            duration_ms=dur + 100,
            spans=spans,
        )
        ctx.client.ingest_plst_batch({"traces": [tr], "spans": spans}, api_key=proj.pk_key).raise_for_status()

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    det = _wait_trace_detail(ctx.client, proj.id, tid, ctx.viewer_token, expected_status="ok")
    assert len(det["spans"]) == 10, len(det["spans"])
    assert all(s["span_phase"] == "final" for s in det["spans"]), det["spans"]
    assert det["trace"]["total_cost_nanos"] == 5_000_000, det["trace"]
    assert det["trace"]["status"] == "ok", det["trace"]
    assert det["trace"].get("has_loop_warning") is False, det["trace"]

    no_loop_list = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={"session_id": sid, "has_loop_warning": False},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    assert any(t["trace_id"] == tid for t in _extract_items(no_loop_list, ("traces", "items"))), no_loop_list


@test_case(
    id="T3-03",
    tier=3,
    feature="F03",
    title="PLST v1 FLAG_PARTIAL/FLAG_FINAL frames enforce server privacy redaction and custom pricing override",
    tags=("F03", "F14", "F19", "plst", "privacy", "pricing"),
)
def test_t3_03_plst_flags_privacy_and_pricing_override(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_03", capture_user_id=False)
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(
            project_id=proj.id,
            capture_user_id=True,
            redact_pii=True,
            redact_keys=["secret_token"],
            exclude_attributes=["internal.debug"],
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="custom-plst-",
            input_per_1m_usd=4.0,
            output_per_1m_usd=12.0,
            cached_input_per_1m_usd=1.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    seed = ctx.unique_id("t303")
    tid = f"tr_{seed}"
    sp_id = f"sp_{seed}"
    ts = _recent_ts_ms()

    for phase, st, out_tok, flags in (
        ("start", "running", 0, plst.FLAG_RAW_ZSTD | plst.FLAG_PARTIAL),
        ("final", "ok", 250, plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL),
    ):
        sp = fixtures.make_span(
            sp_id,
            tid,
            project_id=proj.id,
            span_type="llm",
            span_phase=phase,
            status=st,
            provider="openai",
            model="custom-plst-1",
            start_ts_ms=ts,
            duration_ms=10 if phase == "start" else 120,
            input_tokens=1000,
            cached_tokens=500,
            output_tokens=out_tok,
            cost_nanos=0,
            cost_usd=0.0,
            input_payload={"prompt": "Contact alice@example.com", "secret_token": "tok-abc-999"},
            output_payload={"reply": "ok"},
            attributes={"internal.debug": "secret-trace", "keep.attr": "yes"},
        )
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            session_id=f"sess_{seed}",
            user_id=f"usr_{seed}",
            status=st,
            start_ts_ms=ts,
            duration_ms=10 if phase == "start" else 120,
            input_payload={"prompt": "Contact alice@example.com", "secret_token": "tok-abc-999"},
            spans=[sp],
        )
        frame = plst.encode_plst_frame({"traces": [tr], "spans": [sp]}, use_dict=False, flags=flags)
        hdr = plst.inspect_plst_header(frame)
        assert hdr["flags"] == flags
        ctx.client.ingest_plst(frame, api_key=proj.pk_key).raise_for_status()

    det = _wait_trace_detail(ctx.client, proj.id, tid, ctx.viewer_token, expected_status="ok")
    serialized = json.dumps(det)
    assert "alice@example.com" not in serialized, det
    assert "tok-abc-999" not in serialized, det
    assert det["trace"]["input"]["secret_token"] == "[REDACTED]", det["trace"]
    assert "[REDACTED]" in str(det["trace"]["input"]["prompt"]), det["trace"]

    llm_sp = det["spans"][0]
    assert "internal.debug" not in llm_sp["attributes"], llm_sp["attributes"]
    assert llm_sp["attributes"].get("keep.attr") == "yes", llm_sp["attributes"]
    # (1000 - 500) * 4000 + 500 * 1000 + 250 * 12000 = 2_000_000 + 500_000 + 3_000_000 = 5_500_000
    assert llm_sp["cost_nanos"] == 5_500_000, llm_sp
    assert det["trace"]["total_cost_nanos"] == 5_500_000, det["trace"]


@test_case(
    id="T3-04",
    tier=3,
    feature="F04",
    title="AGENT_DICT_ID=2 (FLAG_DICT_ZSTD) + xxh3_64 blob dedup drives zero-cardinality Prometheus counters",
    tags=("F04", "F05", "F20", "zstd_dict", "dedup", "metrics"),
)
def test_t3_04_zstd_dict_blob_dedup_and_prometheus_metrics(ctx: TestContext) -> None:
    base_metrics = ctx.client.get_metrics().raise_for_status().text()
    base_tr = _parse_prom_counter(base_metrics, "pulse_agent_traces_ingested_total")
    base_sp = _parse_prom_counter(base_metrics, "pulse_agent_spans_ingested_total")
    base_dedup = _parse_prom_counter(base_metrics, "pulse_agent_blobs_deduped_total")

    batch = fixtures.make_repeated_prompt_batch(
        seed=ctx.unique_id("t304"),
        project_id=ctx.proj_a.id,
        repeats=6,
        prompt_bytes=4096,
        start_ts_ms=_recent_ts_ms(),
    )
    stats = plst.BlobDedupStats()
    frame = plst.encode_plst_frame(batch, use_dict=True, stats_out=stats)
    hdr = plst.inspect_plst_header(frame)
    assert hdr["dict_id"] == plst.AGENT_DICT_ID
    assert (hdr["flags"] & plst.FLAG_DICT_ZSTD) != 0
    assert stats.deduped_blobs > 0, stats

    ctx.client.ingest_plst(frame, api_key=ctx.proj_a.pk_key).raise_for_status()

    def _metrics_updated() -> str | None:
        txt = ctx.client.get_metrics().raise_for_status().text()
        if (
            _parse_prom_counter(txt, "pulse_agent_traces_ingested_total") >= base_tr + 6
            and _parse_prom_counter(txt, "pulse_agent_spans_ingested_total") >= base_sp + 6
            and _parse_prom_counter(txt, "pulse_agent_blobs_deduped_total") > base_dedup
        ):
            return txt
        return None

    after_metrics = _poll_until(_metrics_updated, timeout=10.0, desc="Prometheus agent counter increments")
    assert "project_id=" not in after_metrics
    assert ctx.proj_a.id not in after_metrics

    hydrated_inputs = [
        _wait_trace_detail(ctx.client, ctx.proj_a.id, tr["trace_id"], ctx.viewer_token)["trace"]["input"]
        for tr in batch["traces"]
    ]
    assert all(inp == hydrated_inputs[0] for inp in hydrated_inputs), "Deduped inputs diverged"


@test_case(
    id="T3-05",
    tier=3,
    feature="F05",
    title="PLST Section 2 xxh3_64 blob dedup + >64 KiB UTF-8 multi-byte truncation across PLSS Tag 0x05",
    tags=("F05", "F08", "F12", "dedup", "truncation", "plss"),
)
def test_t3_05_blob_dedup_utf8_truncation_and_plss_hydration(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_05")
    seed = ctx.unique_id("t305")
    ts = _recent_ts_ms()

    chunk = '日本語テスト🚀\\"quoted\\"_'
    big_text = chunk * ((72 * 1024) // len(chunk.encode("utf-8")) + 1)
    assert len(big_text.encode("utf-8")) > 65_536
    shared_payload = {"prompt": big_text}

    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for i in range(3):
        tid = f"tr_{seed}_{i}"
        sp = fixtures.make_span(
            f"sp_{seed}_{i}",
            tid,
            project_id=proj.id,
            span_type="llm",
            start_ts_ms=ts + i * 20,
            input_payload=shared_payload,
            output_payload={"ok": True},
        )
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            session_id=f"sess_{seed}",
            start_ts_ms=ts + i * 20,
            input_payload=shared_payload,
            output_payload={"ok": True},
            spans=[sp],
        )
        traces.append(tr)
        spans.append(sp)

    ing = ctx.client.ingest_plst_batch(
        {"traces": traces, "spans": spans},
        api_key=proj.pk_key,
        use_dict=True,
    ).raise_for_status().json()
    assert ing["accepted_traces"] == 3, ing
    assert ing["truncated_payloads"] >= 3, ing

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        segs = _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )
        assert segs

    hydrated: list[dict[str, Any]] = []
    for tr in traces:
        det = _wait_trace_detail(ctx.client, proj.id, tr["trace_id"], ctx.viewer_token)
        inp = det["trace"]["input"]
        assert isinstance(inp, dict) and inp.get("_truncated") is True, inp
        assert int(inp.get("_original_bytes", 0)) > 65_536, inp
        encoded = json.dumps(inp, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        assert len(encoded) <= 65_536, len(encoded)
        encoded.decode("utf-8")
        hydrated.append(inp)

    assert all(h == hydrated[0] for h in hydrated), "Hydrated truncated payloads must match across deduped traces"


@test_case(
    id="T3-06",
    tier=3,
    feature="F06",
    title="Multi-writer PTWL concurrent ingest survives SIGKILL crash via WAL replay into L0/PLSS and evals",
    tags=("F06", "F07", "F17", "wal", "crash", "eval"),
)
def test_t3_06_multi_writer_ptwl_sigkill_replay_and_eval(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst = ctx.node_manager.start_all()
    inst2: NodeInstance | None = None
    try:
        c1 = PulseClient(inst.base_url)
        root_tok = inst.root_admin_token()
        proj = _create_temp_project(ctx, "t3_06", client=c1, token=root_tok)

        ev_cfg = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id("t306_noerr"),
                kind="no_errors",
                target_type="trace",
                enabled=True,
            ),
            token=root_tok,
        ).raise_for_status().json()

        seeds = [ctx.unique_id(f"t306_{i}") for i in range(12)]
        ts = _recent_ts_ms()

        def _send(s: str) -> HttpResponse:
            wc = PulseClient(inst.base_url)
            b = fixtures.make_batch(seed=s, project_id=proj.id, start_ts_ms=ts)
            return wc.ingest_plst_batch(b, api_key=proj.pk_key)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            resps = list(pool.map(_send, seeds))
        assert all(r.status == 200 for r in resps), [r.status for r in resps]

        _wait_for_artifacts(
            inst.data_dir,
            "agent_wal",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".ptwl"),
            timeout=10.0,
        )
        ctx.node_manager.kill(inst)

        inst2 = ctx.node_manager.start_all(data_dir=inst.data_dir, admin_token=root_tok)
        c2 = PulseClient(inst2.base_url)

        for s in seeds:
            tid = f"tr_{s}"
            det = _wait_trace_detail(c2, proj.id, tid, root_tok, timeout=15.0)
            assert det["trace"]["trace_id"] == tid
            if not det.get("evaluations"):
                c2.request(
                    "POST",
                    f"/v1/projects/{proj.id}/agent/traces/{tid}/evaluate",
                    json_body={"evaluator_ids": [ev_cfg["id"]]},
                    token=root_tok,
                ).raise_for_status()

        c2.request("POST", "/v1/admin/flush", token=root_tok).raise_for_status()
        _wait_for_artifacts(
            inst2.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

        evals = _wait_evals(c2, proj.id, root_tok, min_count=12, timeout=20.0)
        eval_by_tr = {e["trace_id"]: e for e in evals if e.get("evaluator_id") == ev_cfg["id"]}
        assert {f"tr_{s}" for s in seeds}.issubset(eval_by_tr.keys()), eval_by_tr
        for s in seeds:
            ev = eval_by_tr[f"tr_{s}"]
            assert ev["passed"] is True and abs(float(ev["score"]) - 1.0) < 1e-6, ev
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2, graceful=True, timeout=10.0)
        else:
            ctx.node_manager.stop(inst, graceful=False, timeout=5.0)


@test_case(
    id="T3-07",
    tier=3,
    feature="F07",
    title="Cross-tier query merges flushed PLSS segments and unflushed L0 MemTable across pagination & analytics",
    tags=("F07", "F11", "F15", "l0", "pagination", "analytics"),
)
def test_t3_07_cross_tier_l0_plus_plss_pagination_and_analytics(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_07")
    base_ts = _recent_ts_ms(120_000)

    def _make_group(prefix: str, durs: list[int], offset_ms: int) -> list[str]:
        traces: list[dict[str, Any]] = []
        spans: list[dict[str, Any]] = []
        ids: list[str] = []
        for idx, dur in enumerate(durs):
            seed = ctx.unique_id(f"{prefix}_{idx}")
            tid = f"tr_{seed}"
            sp = fixtures.make_span(
                f"sp_{seed}",
                tid,
                project_id=proj.id,
                span_type="llm",
                start_ts_ms=base_ts + offset_ms + idx * 100,
                duration_ms=dur,
            )
            tr = fixtures.make_trace(
                tid,
                project_id=proj.id,
                session_id=f"sess_{seed}",
                start_ts_ms=base_ts + offset_ms + idx * 100,
                duration_ms=dur,
                spans=[sp],
            )
            traces.append(tr)
            spans.append(sp)
            ids.append(tid)
        ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
        return ids

    cold_ids = _make_group("cold", [100 * (i + 1) for i in range(12)], 0)
    _wait_trace_detail(ctx.client, proj.id, cold_ids[-1], ctx.viewer_token)
    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    hot_ids = _make_group("hot", [1300 + 100 * i for i in range(12)], 20_000)
    _wait_trace_detail(ctx.client, proj.id, hot_ids[-1], ctx.viewer_token)

    seen: list[str] = []
    page_sizes: list[int] = []
    cursor: str | None = None
    for _ in range(5):
        q: dict[str, Any] = {"limit": 10}
        if cursor is not None:
            q["cursor"] = cursor
        page = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query=q,
            token=ctx.viewer_token,
        ).raise_for_status().json()
        items = _extract_items(page, ("traces", "items"))
        page_sizes.append(len(items))
        seen.extend(t["trace_id"] for t in items)
        cursor = page.get("next_cursor")
        if not cursor:
            break

    assert page_sizes == [10, 10, 4], f"Expected 3 pages [10, 10, 4], got {page_sizes}"
    assert len(seen) == 24 and len(set(seen)) == 24, f"Expected 24 unique traces, got {len(seen)}"
    assert set(seen) == set(cold_ids) | set(hot_ids)

    ana = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/analytics",
        token=ctx.viewer_token,
    ).raise_for_status().json()
    assert ana["total_executions"] == 24, ana
    lat = ana["latency"]
    assert 0 < lat["p50_ms"] <= lat["p95_ms"] <= lat["p99_ms"], lat


@test_case(
    id="T3-08",
    tier=3,
    feature="F08",
    title="PLSS EOF-coalesced Tag 0x06 Bloom/Zone-Map pruning + Tag 0x03 Span Metrics filter analytics",
    tags=("F08", "F09", "F15", "plss", "bloom", "analytics"),
)
def test_t3_08_plss_tag06_pruning_and_tag03_filtered_analytics(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_08")
    ts = _recent_ts_ms(30_000)

    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    staging_ids: list[str] = []
    for env_name, provider, model, tool_name, cost, dur, count in (
        ("production", "openai", "gpt-4o", "db_query", 2_000_000, 200, 3),
        ("staging", "anthropic", "claude-3-5-sonnet", "web_search", 3_000_000, 400, 3),
    ):
        for i in range(count):
            seed = ctx.unique_id(f"t308_{env_name}_{i}")
            tid = f"tr_{seed}"
            sp_llm = fixtures.make_span(
                f"sp_l_{seed}",
                tid,
                project_id=proj.id,
                span_type="llm",
                provider=provider,
                model=model,
                start_ts_ms=ts + len(traces) * 50,
                duration_ms=dur,
                cost_nanos=cost,
            )
            sp_tool = fixtures.make_span(
                f"sp_t_{seed}",
                tid,
                project_id=proj.id,
                parent_span_id=sp_llm["span_id"],
                depth=1,
                span_type="tool",
                tool_name=tool_name,
                start_ts_ms=ts + len(traces) * 50 + 5,
                duration_ms=25,
                cost_nanos=0,
            )
            tr = fixtures.make_trace(
                tid,
                project_id=proj.id,
                environment=env_name,
                session_id=f"sess_{seed}",
                start_ts_ms=ts + len(traces) * 50,
                duration_ms=dur,
                spans=[sp_llm, sp_tool],
            )
            traces.append(tr)
            spans.extend([sp_llm, sp_tool])
            if env_name == "staging":
                staging_ids.append(tid)

    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, staging_ids[-1], ctx.viewer_token)
    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    ana = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/analytics",
        query={"environment": "staging"},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    assert ana["total_executions"] == 3, ana
    assert abs(float(ana["total_cost_usd"]) - 0.009) < 1e-6, ana
    assert {b["key"] for b in ana["by_model"]} == {"claude-3-5-sonnet"}, ana["by_model"]
    assert {b["tool_name"] for b in ana["by_tool"]} == {"web_search"}, ana["by_tool"]

    tr_resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={"model": "claude-3-5-sonnet", "environment": "staging"},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    got_ids = {t["trace_id"] for t in _extract_items(tr_resp, ("traces", "items"))}
    assert got_ids == set(staging_ids), got_ids


@test_case(
    id="T3-09",
    tier=3,
    feature="F09",
    title="PLSS Tag 0x07 Session Rollup joins with SQLite agent_eval_rollups (target_type='session')",
    tags=("F09", "F15", "F17", "session_rollup", "eval_rollup"),
)
def test_t3_09_session_rollup_joined_with_session_eval_rollups(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_09")
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("sess_out"),
            kind="output_not_empty",
            target_type="session",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("tr_noerr"),
            kind="no_errors",
            target_type="trace",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    seed = ctx.unique_id("t309")
    sess_a = f"sess_a_{seed}"
    sess_b = f"sess_b_{seed}"
    ts = _recent_ts_ms(20_000)

    def _ingest_session_traces(sid: str, specs: list[tuple[str, int]]) -> list[str]:
        tids: list[str] = []
        for idx, (st, cost) in enumerate(specs):
            tid = f"tr_{sid}_{idx}"
            sp = fixtures.make_span(
                f"sp_{sid}_{idx}",
                tid,
                project_id=proj.id,
                span_type="llm",
                status=st,
                start_ts_ms=ts + idx * 50,
                cost_nanos=cost,
                output_payload={"reply": "response text"},
            )
            tr = fixtures.make_trace(
                tid,
                project_id=proj.id,
                session_id=sid,
                status=st,
                start_ts_ms=ts + idx * 50,
                output_payload={"reply": "response text"},
                spans=[sp],
            )
            ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
            tids.append(tid)
        return tids

    sess_a_traces = _ingest_session_traces(
        sess_a, [("ok", 1_000_000), ("error", 1_500_000), ("ok", 2_500_000)]
    )
    _ingest_session_traces(sess_b, [("ok", 1_000_000), ("ok", 1_000_000)])

    _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"session_id": sess_a}, min_count=4, timeout=15.0)
    _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"session_id": sess_b}, min_count=3, timeout=15.0)

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    sessions = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("sessions", "items"),
    )
    by_sid = {s["session_id"]: s for s in sessions}
    assert sess_a in by_sid and sess_b in by_sid, by_sid
    assert by_sid[sess_a]["avg_eval_score"] is not None, by_sid[sess_a]

    det_a = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_a}", token=ctx.viewer_token
    ).raise_for_status().json()
    sum_a = det_a["summary"]
    assert sum_a["trace_count"] == 3, sum_a
    assert sum_a["error_count"] == 1, sum_a
    assert sum_a["total_cost_nanos"] == 5_000_000, sum_a
    assert sum_a["avg_eval_score"] is not None and sum_a["eval_pass_rate"] is not None, sum_a
    assert {t["trace_id"] for t in det_a["traces"]} == set(sess_a_traces), det_a["traces"]
    sess_evals = [e for e in det_a["evaluations"] if e.get("target_type") == "session"]
    assert sess_evals and all(e.get("trace_id") is None for e in sess_evals), det_a["evaluations"]


@test_case(
    id="T3-10",
    tier=3,
    feature="F10",
    title="Split-role cluster MVCC epoch_{N}.json progression, 15m pending_gc grace & zero-404 concurrent reads",
    tags=("F10", "F11", "F20", "mvcc", "gc_grace", "split_cluster"),
)
def test_t3_10_split_cluster_mvcc_epoch_and_zero_404_reads(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    cluster = ctx.node_manager.start_split_cluster()
    try:
        ingest_c = PulseClient(cluster["ingest"].base_url)
        query_c = PulseClient(cluster["query"].base_url)
        root_tok = cluster["query"].root_admin_token()
        shared_dir = cluster["query"].data_dir

        proj = _create_temp_project(ctx, "t3_10", client=query_c, token=root_tok)
        known_b = fixtures.make_batch(
            seed=ctx.unique_id("t310_known"), project_id=proj.id, start_ts_ms=_recent_ts_ms()
        )
        known_tid = known_b["traces"][0]["trace_id"]
        _ingest_plst_batch_retry_401(ingest_c, known_b, api_key=proj.pk_key)
        _wait_trace_detail(query_c, proj.id, known_tid, root_tok, timeout=15.0)

        stop_evt = threading.Event()
        read_errors: list[tuple[int, str]] = []
        read_count = {"n": 0}

        def _reader() -> None:
            rc = PulseClient(cluster["query"].base_url)
            while not stop_evt.is_set():
                r = rc.request("GET", f"/v1/projects/{proj.id}/agent/traces/{known_tid}", token=root_tok)
                read_count["n"] += 1
                if r.status != 200:
                    read_errors.append((r.status, r.text(errors="replace")[:150]))
                time.sleep(0.03)

        t_reader = threading.Thread(target=_reader, daemon=True)
        t_reader.start()
        try:
            ts = _recent_ts_ms()
            for wave in range(3):
                for i in range(10):
                    wb = fixtures.make_batch(
                        seed=ctx.unique_id(f"t310_w{wave}_{i}"),
                        project_id=proj.id,
                        start_ts_ms=ts + wave * 1000 + i * 10,
                    )
                    ingest_c.ingest_plst_batch(wb, api_key=proj.pk_key).raise_for_status()
                query_c.request("POST", "/v1/admin/flush", token=root_tok)
                time.sleep(0.4)
        finally:
            stop_evt.set()
            t_reader.join(timeout=5.0)

        assert read_count["n"] >= 5, f"Reader thread barely ran: {read_count['n']}"
        assert not read_errors, f"Concurrent reader observed non-200 responses: {read_errors[:5]}"

        manifests = _wait_for_artifacts(
            shared_dir,
            "agent_manifests",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and re.match(r"^epoch_\d+\.json$", p.name) is not None,
            timeout=15.0,
        )
        epochs = sorted(_epoch_number(p) for p in manifests)
        assert len(epochs) == len(set(epochs)) and epochs == sorted(epochs) and epochs[-1] >= 1, epochs

        latest = max(manifests, key=_epoch_number)
        raw = latest.read_bytes()
        if not _is_encrypted_bytes(raw):
            parsed = json.loads(raw.decode("utf-8"))
            assert isinstance(parsed, dict)
            now_ms = int(time.time() * 1000)
            segs_on_disk = {
                p.name
                for p in list_agent_artifacts(shared_dir)["agent_segments"]
                if proj.id in p.parts
            }
            for gc_item in parsed.get("pending_gc", []):
                if isinstance(gc_item, dict):
                    exp = (
                        gc_item.get("eligible_after_ms")
                        if gc_item.get("eligible_after_ms") is not None
                        else (
                            gc_item.get("delete_after_ms")
                            or gc_item.get("expires_at_ms")
                            or gc_item.get("eligible_at_ms")
                        )
                    )
                    ref = str(
                        gc_item.get("object_path")
                        or gc_item.get("segment_id")
                        or gc_item.get("path")
                        or gc_item.get("object_key")
                        or gc_item.get("seg_id")
                        or ""
                    )
                    if exp is not None and int(exp) > now_ms and ref:
                        assert any(s in ref or s.removesuffix(".plss") in ref for s in segs_on_disk), (
                            f"Pending GC segment {ref} prematurely deleted before grace period"
                        )
    finally:
        for inst in cluster.values():
            ctx.node_manager.stop(inst, graceful=True, timeout=10.0)


@test_case(
    id="T3-11",
    tier=3,
    feature="F11",
    title="3-level GDPR erasure (Trace/Session/User) purges across L0, PLSS segments, and SQLite rollups/tombstones",
    tags=("F11", "F13", "F17", "erasure", "tombstones", "rollups"),
)
def test_t3_11_three_level_erasure_across_l0_plss_and_sqlite(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_11", capture_user_id=True)
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("t311_ev"),
            kind="no_errors",
            target_type="trace",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status()

    seed = ctx.unique_id("t311")
    user_keep = f"u_keep_{seed}"
    user_erase = f"u_erase_{seed}"
    sess_keep = f"s_keep_{seed}"
    sess_erase = f"s_erase_{seed}"
    ts = _recent_ts_ms()

    specs = [
        (f"tr_t_{seed}", sess_keep, user_keep),
        (f"tr_s_{seed}", sess_erase, user_keep),
        (f"tr_u1_{seed}", f"s_u1_{seed}", user_erase),
        (f"tr_u2_{seed}", f"s_u2_{seed}", user_erase),
        (f"tr_surv_{seed}", sess_keep, user_keep),
    ]
    for idx, (tid, sid, uid) in enumerate(specs):
        b = fixtures.make_batch(seed=f"{seed}_{idx}", project_id=proj.id, session_id=sid, user_id=uid, start_ts_ms=ts + idx * 50)
        b["traces"][0]["trace_id"] = tid
        for sp in b["spans"]:
            sp["trace_id"] = tid
        ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()

    _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=5, timeout=15.0)
    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    _erase_target(ctx.client, proj.id, "trace", specs[0][0], specs[0][0], ctx.admin_token)
    _erase_target(ctx.client, proj.id, "session", sess_erase, specs[1][0], ctx.admin_token)
    _erase_target(ctx.client, proj.id, "user", user_erase, specs[2][0], ctx.admin_token)

    for erased_tid, _, _ in specs[:4]:
        _wait_trace_gone(ctx.client, proj.id, erased_tid, ctx.viewer_token)

    survivor_tid = specs[4][0]
    _wait_trace_detail(ctx.client, proj.id, survivor_tid, ctx.viewer_token)
    assert (
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/sessions/{sess_erase}", token=ctx.viewer_token
        ).status
        == 404
    )

    listed = _extract_items(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token).raise_for_status().json(),
        ("traces", "items"),
    )
    assert [t["trace_id"] for t in listed] == [survivor_tid], listed

    ana = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token
    ).raise_for_status().json()
    assert ana["total_executions"] == 1, ana

    if ctx.data_dir is not None:
        conn = _open_sqlite_ro(ctx.data_dir)
        try:
            rows = conn.execute(
                "SELECT level, target_id FROM agent_erasure_tombstones WHERE project_id = ?",
                (proj.id,),
            ).fetchall()
            assert len(rows) == 3, rows
            assert {str(r[0]).lower() for r in rows} == {"trace", "session", "user"}, rows
        finally:
            conn.close()


@test_case(
    id="T3-12",
    tier=3,
    feature="F12",
    title="Tool-retry loop detection + FailureSource::Tool drives no_loop evaluator and has_loop_warning query",
    tags=("F12", "F15", "F17", "loop", "failure_source", "eval"),
)
def test_t3_12_tool_retry_loop_failure_source_eval_and_filter(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_12")
    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("t312_noloop"),
            kind="no_loop",
            target_type="trace",
            enabled=True,
        ),
        token=ctx.member_token,
    ).raise_for_status().json()

    ts = _recent_ts_ms()
    loop_b = fixtures.make_tool_retry_loop_batch(
        seed=ctx.unique_id("t312_loop"),
        project_id=proj.id,
        retry_count=4,
        start_ts_ms=ts,
    )
    loop_tid = loop_b["traces"][0]["trace_id"]

    ok_seed = ctx.unique_id("t312_ok")
    ok_tid = f"tr_{ok_seed}"
    ok_spans = [
        fixtures.make_span(
            f"sp_{ok_seed}_{idx}",
            ok_tid,
            project_id=proj.id,
            span_type="tool",
            tool_name=tname,
            start_ts_ms=ts + 500 + idx * 20,
            duration_ms=15,
        )
        for idx, tname in enumerate(("lookup_user", "fetch_cart", "calculate_tax"))
    ]
    ok_tr = fixtures.make_trace(
        ok_tid,
        project_id=proj.id,
        session_id=f"sess_{ok_seed}",
        start_ts_ms=ts + 500,
        duration_ms=100,
        spans=ok_spans,
    )

    ctx.client.ingest_plst_batch(loop_b, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch({"traces": [ok_tr], "spans": ok_spans}, api_key=proj.pk_key).raise_for_status()

    evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=2, timeout=15.0)
    by_tr = {e["trace_id"]: e for e in evals if e.get("evaluator_id") == ev_cfg["id"]}
    assert loop_tid in by_tr and ok_tid in by_tr, by_tr
    assert by_tr[loop_tid]["passed"] is False, by_tr[loop_tid]
    assert float(by_tr[loop_tid]["score"]) == 0.0, by_tr[loop_tid]
    assert by_tr[loop_tid].get("label") == "loop_detected", by_tr[loop_tid]
    assert by_tr[ok_tid]["passed"] is True, by_tr[ok_tid]

    loop_list = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        query={"has_loop_warning": True},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    assert [t["trace_id"] for t in _extract_items(loop_list, ("traces", "items"))] == [loop_tid], loop_list

    ana = ctx.client.request(
        "GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token
    ).raise_for_status().json()
    assert ana["loop_warning_count"] == 1, ana
    assert ana["failure_sources"].get("tool", 0) >= 1, ana


@test_case(
    id="T3-13",
    tier=3,
    feature="F13",
    title="Zero SQL writes on ingest + async agent_eval_rollups population drives min/max_eval_score filters",
    tags=("F13", "F15", "F17", "sql_split", "eval_rollups", "filter"),
)
def test_t3_13_zero_sql_on_ingest_and_eval_rollup_score_filters(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_13")
    if ctx.data_dir is not None:
        tables = set(sqlite_tables(ctx.data_dir))
        for forbidden in _FORBIDDEN_SQL_TABLES:
            assert forbidden not in tables, f"Legacy table {forbidden!r} must be removed"
        pre_counts = _sqlite_table_counts(ctx.data_dir, ("agent_evaluations", "agent_eval_rollups"))
    else:
        pre_counts = None

    ts = _recent_ts_ms()
    pass_b = fixtures.make_batch(seed=ctx.unique_id("t313_ok"), project_id=proj.id, start_ts_ms=ts)
    fail_b = fixtures.make_batch(seed=ctx.unique_id("t313_err"), project_id=proj.id, start_ts_ms=ts + 100)
    fail_b["traces"][0]["status"] = "error"
    fail_b["spans"][1]["status"] = "error"

    tr_pass = pass_b["traces"][0]["trace_id"]
    tr_fail = fail_b["traces"][0]["trace_id"]

    ctx.client.ingest_plst_batch(pass_b, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch(fail_b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, tr_pass, ctx.viewer_token)
    _wait_trace_detail(ctx.client, proj.id, tr_fail, ctx.viewer_token)

    if ctx.data_dir is not None and pre_counts is not None:
        mid_counts = _sqlite_table_counts(ctx.data_dir, ("agent_evaluations", "agent_eval_rollups"))
        assert mid_counts == pre_counts, f"Ingest wrote unexpected SQL rows: {pre_counts} -> {mid_counts}"

    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("t313_ev"), kind="no_errors", target_type="trace", enabled=False
        ),
        token=ctx.member_token,
    ).raise_for_status().json()

    for tid in (tr_pass, tr_fail):
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/traces/{tid}/evaluate",
            json_body={"evaluator_ids": [ev_cfg["id"]]},
            token=ctx.member_token,
        ).raise_for_status()

    _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=2, timeout=15.0)

    if ctx.data_dir is not None:
        conn = _open_sqlite_ro(ctx.data_dir)
        try:
            rollup_ids = {
                row[0]
                for row in conn.execute(
                    "SELECT target_id FROM agent_eval_rollups WHERE project_id = ? AND target_type = 'trace'",
                    (proj.id,),
                ).fetchall()
            }
            assert {tr_pass, tr_fail}.issubset(rollup_ids), rollup_ids
        finally:
            conn.close()

    high = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"min_eval_score": 0.8},
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("traces", "items"),
    )
    assert [t["trace_id"] for t in high] == [tr_pass], high

    low = _extract_items(
        ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            query={"max_eval_score": 0.2},
            token=ctx.viewer_token,
        ).raise_for_status().json(),
        ("traces", "items"),
    )
    assert [t["trace_id"] for t in low] == [tr_fail], low


@test_case(
    id="T3-14",
    tier=3,
    feature="F14",
    title="Triple-protocol convergence (PLST + OTLP/JSON + OTLP/PB) with custom pricing override and PII redaction",
    tags=("F14", "F02", "F19", "plst", "otlp", "pricing", "privacy"),
)
def test_t3_14_triple_protocol_pricing_and_privacy_convergence(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_14")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(
            project_id=proj.id, redact_pii=True, redact_keys=["secret"]
        ),
        token=ctx.admin_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="gpt-4o-mini",
            input_per_1m_usd=10.0,
            output_per_1m_usd=20.0,
            cached_input_per_1m_usd=0.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    seed = ctx.unique_id("t314")
    ts = _recent_ts_ms()

    # 1. PLST binary
    plst_tid = f"tr_plst_{seed}"
    plst_sp = fixtures.make_span(
        f"sp_plst_{seed}",
        plst_tid,
        project_id=proj.id,
        span_type="llm",
        provider="openai",
        model="gpt-4o-mini",
        start_ts_ms=ts,
        input_tokens=100,
        output_tokens=50,
        cached_tokens=0,
        cost_nanos=0,
        cost_usd=0.0,
        input_payload={"prompt": "Contact alice@example.com", "secret": "hidden-plst"},
    )
    plst_tr = fixtures.make_trace(
        plst_tid,
        project_id=proj.id,
        session_id=f"sess_{seed}",
        start_ts_ms=ts,
        input_payload={"prompt": "Contact alice@example.com", "secret": "hidden-plst"},
        spans=[plst_sp],
    )
    ctx.client.ingest_plst_batch({"traces": [plst_tr], "spans": [plst_sp]}, api_key=proj.pk_key).raise_for_status()

    def _otlp_req(suffix: str) -> tuple[str, dict[str, Any]]:
        tr_hex = fixtures.deterministic_hex_id(f"{seed}_{suffix}", 16)
        sp_hex = fixtures.deterministic_hex_id(f"{seed}_sp_{suffix}", 8)
        payload = {
            "resourceSpans": [
                {
                    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "otlp-conv"}}]},
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": tr_hex,
                                    "spanId": sp_hex,
                                    "name": "llm.chat",
                                    "kind": 3,
                                    "startTimeUnixNano": str(ts * 1_000_000),
                                    "endTimeUnixNano": str((ts + 100) * 1_000_000),
                                    "attributes": [
                                        {"key": "pulse.span_type", "value": {"stringValue": "llm"}},
                                        {"key": "gen_ai.system", "value": {"stringValue": "openai"}},
                                        {"key": "gen_ai.request.model", "value": {"stringValue": "gpt-4o-mini"}},
                                        {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "100"}},
                                        {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "50"}},
                                        {"key": "gen_ai.prompt", "value": {"stringValue": "Contact alice@example.com"}},
                                        {"key": "session.id", "value": {"stringValue": f"sess_{seed}"}},
                                    ],
                                    "status": {"code": 1},
                                }
                            ]
                        }
                    ],
                }
            ]
        }
        return tr_hex, payload

    # 2. OTLP/JSON & 3. OTLP/Protobuf
    oj_tid, oj_req = _otlp_req("oj")
    opb_tid, opb_req = _otlp_req("opb")
    ctx.client.ingest_otlp_json(oj_req, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_otlp_pb(opb_req, api_key=proj.pk_key).raise_for_status()

    # Expected: 100 * 10_000 + 50 * 20_000 = 2_000_000 nanos
    for tid in (plst_tid, oj_tid, opb_tid):
        det = _wait_trace_detail(ctx.client, proj.id, tid, ctx.viewer_token)
        assert det["trace"]["total_cost_nanos"] == 2_000_000, f"{tid}: {det['trace']}"
        serialized = json.dumps(det)
        assert "alice@example.com" not in serialized, f"{tid} leaked email: {serialized}"
        assert "[REDACTED]" in serialized, f"{tid} missing [REDACTED]: {serialized}"


@test_case(
    id="T3-15",
    tier=3,
    feature="F15",
    title="GET .../compare Welch t-test (n>=20) across latency, custom-priced cost_nanos, and deterministic eval_score",
    tags=("F15", "F17", "F19", "compare", "welch", "pricing"),
)
def test_t3_15_compare_welch_across_latency_cost_and_eval_score(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_15")
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="custom-cmp-",
            input_per_1m_usd=5.0,
            output_per_1m_usd=10.0,
            cached_input_per_1m_usd=0.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("t315_ev"), kind="no_errors", target_type="trace", enabled=True
        ),
        token=ctx.member_token,
    ).raise_for_status()

    ts = _recent_ts_ms(60_000)
    traces: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for version, st, base_dur, base_in, out_tok in (
        ("v1", "ok", 100, 100, 50),
        ("v2", "error", 450, 1000, 500),
    ):
        for i in range(20):
            seed = ctx.unique_id(f"t315_{version}_{i}")
            tid = f"tr_{seed}"
            dur = base_dur + (i % 5) * 6
            in_tok = base_in + (i % 4) * 15
            sp = fixtures.make_span(
                f"sp_{seed}",
                tid,
                project_id=proj.id,
                span_type="llm",
                status=st,
                provider="openai",
                model="custom-cmp-1",
                start_ts_ms=ts + len(traces) * 50,
                duration_ms=dur,
                input_tokens=in_tok,
                output_tokens=out_tok,
                cached_tokens=0,
                cost_nanos=0,
                cost_usd=0.0,
            )
            tr = fixtures.make_trace(
                tid,
                project_id=proj.id,
                agent_version=version,
                session_id=f"sess_{seed}",
                status=st,
                start_ts_ms=ts + len(traces) * 50,
                duration_ms=dur,
                spans=[sp],
            )
            traces.append(tr)
            spans.append(sp)

    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
    _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"limit": 100}, min_count=40, timeout=20.0)

    cmp_resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/compare",
        query={"baseline_version": "v1", "candidate_version": "v2"},
        token=ctx.viewer_token,
    ).raise_for_status().json()

    lat_delta = _extract_compare_delta(cmp_resp, ("latency_ms", "latency", "duration_ms"))
    cost_delta = _extract_compare_delta(cmp_resp, ("cost_usd", "cost", "avg_cost_usd"))
    eval_delta = _extract_compare_delta(cmp_resp, ("eval_score", "avg_eval_score", "score"))

    for label, delta in (("latency", lat_delta), ("cost", cost_delta), ("eval_score", eval_delta)):
        assert delta["sample_size_baseline"] == 20, f"{label}: {delta}"
        assert delta["sample_size_candidate"] == 20, f"{label}: {delta}"
        assert delta["verdict"] == "significant_regression", f"{label}: {delta}"
        assert delta["p_value"] is not None and float(delta["p_value"]) < 0.05, f"{label}: {delta}"


@test_case(
    id="T3-16",
    tier=3,
    feature="F16",
    title="All 4 MCP tools reflect flushed PLSS hydration, async evals, and post-erasure hiding",
    tags=("F16", "F11", "F17", "mcp", "plss", "erasure"),
)
def test_t3_16_mcp_tools_plss_evals_and_erasure_parity(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_16")
    ev_cfg = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("t316_ev"), kind="no_errors", target_type="trace", enabled=True
        ),
        token=ctx.member_token,
    ).raise_for_status().json()

    ts = _recent_ts_ms()
    b1 = fixtures.make_batch(seed=ctx.unique_id("t316_1"), project_id=proj.id, start_ts_ms=ts)
    b2 = fixtures.make_batch(seed=ctx.unique_id("t316_2"), project_id=proj.id, start_ts_ms=ts + 100)
    tr1 = b1["traces"][0]["trace_id"]
    tr2 = b2["traces"][0]["trace_id"]

    ctx.client.ingest_plst_batch(b1, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch(b2, api_key=proj.pk_key).raise_for_status()
    _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=2, timeout=15.0)

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()
    if ctx.data_dir is not None:
        _wait_for_artifacts(
            ctx.data_dir,
            "agent_segments",
            min_count=1,
            predicate=lambda p: proj.id in p.parts and p.name.endswith(".plss"),
        )

    m_list = ctx.client.mcp_tool_call(
        "list_agent_traces", {"limit": 10}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    assert not m_list.get("isError"), m_list
    listed_ids = {t["trace_id"] for t in _extract_items(m_list["structuredContent"], ("traces", "items"))}
    assert listed_ids == {tr1, tr2}, listed_ids

    m_get = ctx.client.mcp_tool_call(
        "get_agent_trace", {"trace_id": tr1}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    assert not m_get.get("isError"), m_get
    assert len(m_get["structuredContent"]["spans"]) == 3

    m_ana = ctx.client.mcp_tool_call(
        "query_agent_analytics", {"days": 7}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    assert not m_ana.get("isError"), m_ana
    assert m_ana["structuredContent"]["total_executions"] == 2

    m_evs = ctx.client.mcp_tool_call(
        "list_agent_evaluations", {"limit": 20}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    assert not m_evs.get("isError"), m_evs
    ev_items = _extract_items(m_evs["structuredContent"], ("evaluations", "items"))
    assert {e["trace_id"] for e in ev_items if e.get("evaluator_id") == ev_cfg["id"]} == {tr1, tr2}, ev_items

    _erase_target(ctx.client, proj.id, "trace", tr1, tr1, ctx.admin_token)
    _wait_trace_gone(ctx.client, proj.id, tr1, ctx.viewer_token)

    m_gone = ctx.client.mcp_tool_call(
        "get_agent_trace", {"trace_id": tr1}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()
    assert m_gone.get("result", {}).get("isError") is True or "error" in m_gone, m_gone

    m_list_after = ctx.client.mcp_tool_call(
        "list_agent_traces", {"limit": 10}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    after_ids = {t["trace_id"] for t in _extract_items(m_list_after["structuredContent"], ("traces", "items"))}
    assert after_ids == {tr2}, after_ids

    m_ana_after = ctx.client.mcp_tool_call(
        "query_agent_analytics", {"days": 7}, token=ctx.viewer_token, project_id=proj.id
    ).raise_for_status().json()["result"]
    assert m_ana_after["structuredContent"]["total_executions"] == 1, m_ana_after


@test_case(
    id="T3-17",
    tier=3,
    feature="F17",
    title="Non-blocking bounded EvalJob channel & <=8 LLM judge cap under burst PLST ingest updates Prometheus",
    tags=("F17", "F14", "F20", "eval_worker", "llm_judge", "metrics"),
)
def test_t3_17_burst_plst_nonblocking_llm_concurrency_and_metrics(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_17")
    _configure_byok_llm(ctx, proj.id)
    for kind in ("no_errors", "llm_judge"):
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id(f"t317_{kind}"), kind=kind, enabled=True
            ),
            token=ctx.member_token,
        ).raise_for_status()

    base_eval_counter = _parse_prom_counter(
        ctx.client.get_metrics().raise_for_status().text(),
        "pulse_agent_evals_executed_total",
    )

    assert ctx.mock_llm is not None
    lock = threading.Lock()
    state = {"in_flight": 0, "max_in_flight": 0}

    def _responder(_record: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        with lock:
            state["in_flight"] += 1
            if state["in_flight"] > state["max_in_flight"]:
                state["max_in_flight"] = state["in_flight"]
        try:
            time.sleep(0.15)
            return (
                200,
                {
                    "id": "chatcmpl-t317",
                    "object": "chat.completion",
                    "model": "gpt-4o-mini",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": '{"score": 0.91, "passed": true, "reasoning": "solid"}',
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
        ts = _recent_ts_ms()
        batches = [
            fixtures.make_batch(seed=ctx.unique_id(f"t317_b{i}"), project_id=proj.id, start_ts_ms=ts)
            for i in range(20)
        ]

        def _ingest_one(b: dict[str, Any]) -> HttpResponse:
            c = PulseClient(ctx.base_url)
            return c.ingest_plst_batch(b, api_key=proj.pk_key)

        t0 = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            resps = list(pool.map(_ingest_one, batches))
        ingest_elapsed = time.monotonic() - t0
        assert all(r.status == 200 for r in resps), [r.status for r in resps]
        assert ingest_elapsed < 5.0, f"Ingest appears blocked by LLM evals: {ingest_elapsed:.2f}s"

        ctx.mock_llm.wait_for_count(20, timeout=20.0)
        assert 1 <= state["max_in_flight"] <= 8, f"Invalid max_in_flight={state['max_in_flight']}"

        evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, query={"limit": 100}, min_count=40, timeout=20.0)
        det_traces = {e["trace_id"] for e in evals if e.get("evaluator_type") == "deterministic"}
        llm_traces = {e["trace_id"] for e in evals if e.get("evaluator_type") == "llm_judge"}
        expected_tids = {b["traces"][0]["trace_id"] for b in batches}
        assert expected_tids.issubset(det_traces), det_traces
        assert expected_tids.issubset(llm_traces), llm_traces

        after_eval_counter = _parse_prom_counter(
            ctx.client.get_metrics().raise_for_status().text(),
            "pulse_agent_evals_executed_total",
        )
        assert after_eval_counter >= base_eval_counter + 40, (base_eval_counter, after_eval_counter)
    finally:
        ctx.mock_llm.custom_responder = None


@test_case(
    id="T3-18",
    tier=3,
    feature="F18",
    title="Promote production trace -> dataset (n=20) -> experiments/compare Welch t-test + 4-tier RBAC",
    tags=("F18", "F15", "F17", "promote", "datasets", "experiments", "rbac"),
)
def test_t3_18_promote_dataset_experiments_compare_and_rbac(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_18")
    seed = ctx.unique_id("t318")
    tid = f"tr_{seed}"
    ts = _recent_ts_ms()
    sp_tool = fixtures.make_span(
        f"sp_tool_{seed}",
        tid,
        project_id=proj.id,
        span_type="tool",
        tool_name="order_lookup",
        start_ts_ms=ts,
        output_payload={"order_id": "1042", "status": "shipped"},
    )
    tr = fixtures.make_trace(
        tid,
        project_id=proj.id,
        session_id=f"sess_{seed}",
        start_ts_ms=ts,
        input_payload={"message": "Status of order 1042?"},
        output_payload={"reply": "Order #1042"},
        spans=[sp_tool],
    )
    ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp_tool]}, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, tid, ctx.viewer_token)

    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=f"ds_{seed}"),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds_id = ds["id"]

    assert (
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/traces/{tid}/promote",
            json_body={"dataset_id": ds_id},
            token=ctx.viewer_token,
        ).status
        == 403
    )
    assert (
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/experiments",
            json_body=fixtures.make_experiment(dataset_id=ds_id, name=f"v_exp_{seed}"),
            token=ctx.viewer_token,
        ).status
        == 403
    )

    promoted = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/traces/{tid}/promote",
        json_body={"dataset_id": ds_id},
        token=ctx.member_token,
    ).raise_for_status().json()
    assert promoted["source_trace_id"] == tid, promoted
    assert promoted["input"] == {"message": "Status of order 1042?"}, promoted
    assert promoted["expected_output"] == {"reply": "Order #1042"}, promoted
    assert "order_lookup" in json.dumps(promoted.get("expected_behavior", {})), promoted

    bulk_items = [fixtures.make_dataset_item(name=f"bulk-{i}") for i in range(19)]
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets/{ds_id}/import",
        json_body={"items": bulk_items},
        token=ctx.member_token,
    ).raise_for_status()

    all_items = _extract_items(
        ctx.client.request(
            "GET", f"/v1/projects/{proj.id}/agent/datasets/{ds_id}/items", token=ctx.viewer_token
        ).raise_for_status().json(),
        ("items", "dataset_items"),
    )
    assert len(all_items) == 20, len(all_items)

    def _run_experiment(name: str, score_base: float, lat_base: int) -> str:
        exp = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/experiments",
            json_body=fixtures.make_experiment(dataset_id=ds_id, name=name),
            token=ctx.member_token,
        ).raise_for_status().json()
        eid = exp["id"]
        for i, item in enumerate(all_items):
            ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/experiments/{eid}/runs",
                json_body=fixtures.make_experiment_run(
                    dataset_item_id=item["id"],
                    latency_ms=lat_base + (i % 4) * 5,
                    avg_score=score_base + (i % 3) * 0.01,
                    passed=score_base >= 0.7,
                ),
                token=ctx.member_token,
            ).raise_for_status()
        return eid

    exp_base = _run_experiment(f"base_{seed}", 0.90, 100)
    exp_cand = _run_experiment(f"cand_{seed}", 0.48, 350)

    cmp_res = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/experiments/compare",
        query={"baseline": exp_base, "candidate": exp_cand},
        token=ctx.viewer_token,
    ).raise_for_status().json()
    score_delta = _extract_compare_delta(cmp_res, ("score", "avg_score"))
    assert score_delta["sample_size_baseline"] == 20, score_delta
    assert score_delta["sample_size_candidate"] == 20, score_delta
    assert score_delta["verdict"] == "significant_regression", score_delta
    assert score_delta["p_value"] is not None and float(score_delta["p_value"]) < 0.05, score_delta


@test_case(
    id="T3-19",
    tier=3,
    feature="F19",
    title="eval_score_drop alert ok->firing->ok driven by F17 evaluations with HMAC-SHA256 triggered & resolved webhooks",
    tags=("F19", "F17", "F14", "alerts", "webhooks", "hmac", "recovery"),
)
def test_t3_19_eval_score_drop_alert_lifecycle_and_hmac_webhooks(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_19")
    signing_secret = "t3_hmac_secret_123456"
    _configure_byok_webhook(ctx, proj.id, signing_secret)
    assert ctx.mock_webhook is not None
    ctx.mock_webhook.signing_secret = signing_secret
    try:
        ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id("t319_ev"), kind="no_errors", target_type="trace", enabled=True
            ),
            token=ctx.member_token,
        ).raise_for_status()

        rule = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/alerts",
            json_body=fixtures.make_alert_rule(
                name=ctx.unique_id("t319_drop"),
                metric="eval_score_drop",
                operator="lt",
                threshold=0.7,
                window_minutes=15,
                cooldown_minutes=0,
                webhook_url=ctx.mock_webhook_url,
            ),
            token=ctx.member_token,
        ).raise_for_status().json()
        rule_id = rule["id"]

        ts = _recent_ts_ms()
        for i in range(3):
            b = fixtures.make_batch(seed=ctx.unique_id(f"t319_bad_{i}"), project_id=proj.id, start_ts_ms=ts + i * 20)
            b["traces"][0]["status"] = "error"
            b["spans"][1]["status"] = "error"
            ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=3, timeout=15.0)

        def _wait_firing() -> bool | None:
            _trigger_alert_evaluate(ctx.client, proj.id, [rule_id], ctx.member_token)
            rules = _extract_items(
                ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token)
                .raise_for_status()
                .json(),
                ("rules", "alerts", "items"),
            )
            return True if any(r["id"] == rule_id and r.get("state") == "firing" for r in rules) else None

        _poll_until(_wait_firing, timeout=15.0, interval=0.3, desc="eval_score_drop rule firing")
        hooks1 = ctx.mock_webhook.wait_for_count(1, timeout=10.0)
        assert hooks1[0]["signature_valid"] is True, hooks1[0]

        for i in range(10):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"t319_good_{i}"), project_id=proj.id, start_ts_ms=ts + 200 + i * 20
            )
            ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=13, timeout=15.0)

        def _wait_resolved() -> bool | None:
            _trigger_alert_evaluate(ctx.client, proj.id, [rule_id], ctx.member_token)
            rules = _extract_items(
                ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token)
                .raise_for_status()
                .json(),
                ("rules", "alerts", "items"),
            )
            return True if any(r["id"] == rule_id and r.get("state") == "ok" for r in rules) else None

        _poll_until(_wait_resolved, timeout=15.0, interval=0.3, desc="eval_score_drop rule resolved")
        hooks2 = ctx.mock_webhook.wait_for_count(2, timeout=10.0)
        assert all(h["signature_valid"] is True for h in hooks2), hooks2

        events = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.viewer_token)
            .raise_for_status()
            .json(),
            ("events", "items"),
        )
        by_type = {e["event_type"]: e for e in events if e.get("rule_id") == rule_id}
        assert {"triggered", "resolved"}.issubset(by_type.keys()), events
        assert by_type["triggered"]["delivered"] is True
        assert by_type["resolved"]["delivered"] is True
    finally:
        ctx.mock_webhook.signing_secret = None


@test_case(
    id="T3-20",
    tier=3,
    feature="F20",
    title="Graceful SIGTERM restart preserves PTWL WAL traces alongside SQLite MetaStore configs & tombstones",
    tags=("F20", "F06", "F13", "shutdown", "wal", "sqlite"),
)
def test_t3_20_graceful_sigterm_restart_wal_and_sqlite_durability(ctx: TestContext) -> None:
    assert ctx.node_manager is not None
    inst = ctx.node_manager.start_all()
    inst2: NodeInstance | None = None
    try:
        c1 = PulseClient(inst.base_url)
        root_tok = inst.root_admin_token()
        proj = _create_temp_project(ctx, "t3_20", client=c1, token=root_tok)

        ev = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id("t320_ev"), kind="no_errors", enabled=False
            ),
            token=root_tok,
        ).raise_for_status().json()
        ds = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/datasets",
            json_body=fixtures.make_dataset(name=ctx.unique_id("t320_ds")),
            token=root_tok,
        ).raise_for_status().json()
        item = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/datasets/{ds['id']}/items",
            json_body=fixtures.make_dataset_item(name="t320-item"),
            token=root_tok,
        ).raise_for_status().json()
        al = c1.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/alerts",
            json_body=fixtures.make_alert_rule(name=ctx.unique_id("t320_al"), enabled=False),
            token=root_tok,
        ).raise_for_status().json()

        ts = _recent_ts_ms()
        b_keep = fixtures.make_batch(seed=ctx.unique_id("t320_keep"), project_id=proj.id, start_ts_ms=ts)
        b_erase = fixtures.make_batch(seed=ctx.unique_id("t320_del"), project_id=proj.id, start_ts_ms=ts + 50)
        tr_keep = b_keep["traces"][0]["trace_id"]
        tr_erase = b_erase["traces"][0]["trace_id"]

        c1.ingest_plst_batch(b_keep, api_key=proj.pk_key).raise_for_status()
        c1.ingest_plst_batch(b_erase, api_key=proj.pk_key).raise_for_status()
        _erase_target(c1, proj.id, "trace", tr_erase, tr_erase, root_tok)

        inst2 = ctx.node_manager.restart(inst, graceful_stop=True)
        c2 = PulseClient(inst2.base_url)

        det_keep = _wait_trace_detail(c2, proj.id, tr_keep, root_tok, timeout=15.0)
        assert det_keep["trace"]["trace_id"] == tr_keep
        assert c2.request("GET", f"/v1/projects/{proj.id}/agent/traces/{tr_erase}", token=root_tok).status == 404

        evs = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/evaluators", token=root_tok).raise_for_status().json(),
            ("evaluators", "items"),
        )
        assert any(e["id"] == ev["id"] for e in evs), evs
        ds_items = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/datasets/{ds['id']}/items", token=root_tok)
            .raise_for_status()
            .json(),
            ("items", "dataset_items"),
        )
        assert any(it["id"] == item["id"] for it in ds_items), ds_items
        als = _extract_items(
            c2.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=root_tok).raise_for_status().json(),
            ("rules", "alerts", "items"),
        )
        assert any(r["id"] == al["id"] for r in als), als

        tables = set(sqlite_tables(inst2.data_dir))
        assert "agent_traces" not in tables and "agent_spans" not in tables
    finally:
        if inst2 is not None:
            ctx.node_manager.stop(inst2, graceful=True, timeout=10.0)
        else:
            ctx.node_manager.stop(inst, graceful=False, timeout=5.0)


@test_case(
    id="T3-21",
    tier=3,
    feature="F21",
    title="Pulse Studio #/agents & #/replays?session= chunks align with parent_trace_id, guardrail spans & /v1/replay",
    tags=("F21", "F01", "F15", "studio", "replays", "guardrail"),
)
def test_t3_21_studio_agents_replay_and_guardrail_hierarchy_parity(ctx: TestContext) -> None:
    info = ctx.client.check_studio_static(require_agents_route=True)
    entry_js = "\n".join(ctx.client.request("GET", a).raise_for_status().text(errors="replace") for a in info["assets"])

    agent_chunks = re.findall(r"AgentObservabilityWorkspace[A-Za-z0-9_.-]*\.js", entry_js)
    replay_chunks = re.findall(r"SessionsReplayWorkspace[A-Za-z0-9_.-]*\.js", entry_js)
    assert agent_chunks and replay_chunks, (agent_chunks, replay_chunks)

    agent_chunk_js = ctx.client.request("GET", f"/assets/{agent_chunks[0]}").raise_for_status().text(errors="replace").lower()
    for marker in ("sessions", "traces", "replays"):
        assert marker in agent_chunk_js, f"Agent chunk missing {marker!r}"

    replay_chunk_js = ctx.client.request("GET", f"/assets/{replay_chunks[0]}").raise_for_status().text(errors="replace")
    assert "session" in replay_chunk_js and (
        "URLSearchParams" in replay_chunk_js or "replays" in replay_chunk_js
    ), "SessionsReplayWorkspace chunk missing ?session= handling"

    seed = ctx.unique_id("t321")
    now_ms = int(time.time() * 1000)
    batch = fixtures.make_hierarchy_batch(seed=seed, project_id=ctx.proj_a.id, start_ts_ms=now_ms - 5_000)
    sid = batch["traces"][0]["session_id"]
    parent_tid = batch["traces"][0]["trace_id"]
    child_tid = batch["traces"][1]["trace_id"]

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
                            "masked_label": "Agent Hierarchy Session",
                        }
                    ],
                }
            ],
        },
    ).raise_for_status()

    parent_det = _wait_trace_detail(ctx.client, ctx.proj_a.id, parent_tid, ctx.viewer_token)
    child_det = _wait_trace_detail(ctx.client, ctx.proj_a.id, child_tid, ctx.viewer_token)
    sess_det = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/agent/sessions/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()
    replay_det = ctx.client.request(
        "GET", f"/v1/projects/{ctx.proj_a.id}/replay/{sid}", token=ctx.viewer_token
    ).raise_for_status().json()

    assert sess_det["summary"]["session_id"] == sid
    assert replay_det["session_id"] == sid and len(replay_det["chunks"]) >= 1
    assert child_det["trace"]["parent_trace_id"] == parent_tid
    assert any(s["span_type"] == "guardrail" for s in parent_det["spans"]), parent_det["spans"]
    assert any(s.get("linked_span_ids") for s in parent_det["spans"]), parent_det["spans"]
    assert any("cost_nanos" in s for s in parent_det["spans"])


@test_case(
    id="T3-22",
    tier=3,
    feature="F22",
    title="TypeScript & Python SDKs PLST (AGENT_DICT_ID=2) client redaction + server ModelPricingRule cost calculation",
    tags=("F22", "F04", "F19", "ts_sdk", "py_sdk", "plst", "pricing"),
)
def test_t3_22_ts_and_py_sdks_plst_redaction_and_server_pricing(ctx: TestContext) -> None:
    node_bin = shutil.which("node")
    assert node_bin, "node executable not found on PATH"
    ts_entry = _resolve_ts_sdk_entry()
    py_sdk_dir = _REPO_ROOT / "sdks" / "py-agent"
    assert py_sdk_dir.is_dir(), f"Missing M5 Python SDK directory: {py_sdk_dir}"

    proj = _create_temp_project(ctx, "t3_22")
    ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="gpt-4o-mini",
            input_per_1m_usd=3.0,
            output_per_1m_usd=9.0,
            cached_input_per_1m_usd=0.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    seed = ctx.unique_id("t322")
    ts_tid = f"tr_ts_{seed}"
    py_tid = f"tr_py_{seed}"

    ts_script = """
import { pathToFileURL } from 'node:url';
const mod = await import(pathToFileURL(process.env.PULSE_SDK_ENTRY).href);
const Ctor = mod.PulseAgentClient || mod.PulseAgent || mod.AgentClient || mod.default;
const client = new Ctor({
  endpoint: process.env.PULSE_BASE_URL,
  baseUrl: process.env.PULSE_BASE_URL,
  apiKey: process.env.PULSE_API_KEY,
  useBinary: true,
  usePlst: true,
  redactKeys: ['api_secret'],
});
const tr = client.trace({
  traceId: process.env.PULSE_TRACE_ID,
  agentName: 't3-ts-sdk',
  input: { api_secret: 'ts-top-secret-999', prompt: 'hello' },
});
const gen = tr.generation({
  name: 'llm.gen',
  model: 'gpt-4o-mini',
  provider: 'openai',
  inputTokens: 1000,
  outputTokens: 500,
  input: { api_secret: 'ts-span-secret-999', prompt: 'hello' },
});
gen.end({ output: { text: 'ok' }, inputTokens: 1000, outputTokens: 500 });
tr.end({ output: { reply: 'ok' } });
if (client.flush) await client.flush();
if (client.shutdown) await client.shutdown();
"""
    ts_proc = _run_cmd(
        [node_bin, "--input-type=module", "-e", ts_script],
        env={
            "PULSE_SDK_ENTRY": str(ts_entry),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": proj.pk_key,
            "PULSE_TRACE_ID": ts_tid,
        },
        timeout=30.0,
    )
    assert ts_proc.returncode == 0, f"TS SDK failed:\n{ts_proc.stdout}\n{ts_proc.stderr}"

    py_script = """
import inspect, os, pulse_agent
Ctor = next(getattr(pulse_agent, n) for n in ("PulseAgentClient", "PulseAgent", "AgentClient") if hasattr(pulse_agent, n))
sig = inspect.signature(Ctor.__init__)
kw = {}
for p in sig.parameters:
    if p in ("endpoint", "base_url", "host", "url"): kw[p] = os.environ["PULSE_BASE_URL"]
    elif p in ("api_key", "key"): kw[p] = os.environ["PULSE_API_KEY"]
    elif p in ("use_binary", "use_plst"): kw[p] = True
    elif p == "redact_keys": kw[p] = ["api_secret"]
client = Ctor(**kw)
with client.trace(
    trace_id=os.environ["PULSE_TRACE_ID"],
    agent_name="t3-py-sdk",
    input={"api_secret": "py-top-secret-999", "prompt": "hello"},
) as tr:
    with tr.generation(
        name="llm.gen",
        model="gpt-4o-mini",
        provider="openai",
        input_tokens=1000,
        output_tokens=500,
        input={"api_secret": "py-span-secret-999", "prompt": "hello"},
    ) as gen:
        if hasattr(gen, "set_output"):
            gen.set_output({"text": "ok"})
    if hasattr(tr, "set_output"):
        tr.set_output({"reply": "ok"})
if hasattr(client, "flush"): client.flush()
if hasattr(client, "shutdown"): client.shutdown()
"""
    py_proc = _run_cmd(
        [sys.executable, "-c", py_script],
        env={
            "PYTHONPATH": str(py_sdk_dir),
            "PULSE_BASE_URL": ctx.base_url,
            "PULSE_API_KEY": proj.pk_key,
            "PULSE_TRACE_ID": py_tid,
        },
        timeout=30.0,
    )
    assert py_proc.returncode == 0, f"Python SDK failed:\n{py_proc.stdout}\n{py_proc.stderr}"

    # Expected: 1000 * 3000 + 500 * 9000 = 7_500_000 nanos
    for tid, secret_marker in ((ts_tid, "ts-top-secret-999"), (py_tid, "py-top-secret-999")):
        det = _wait_trace_detail(ctx.client, proj.id, tid, ctx.viewer_token)
        raw = json.dumps(det)
        assert secret_marker not in raw and "span-secret-999" not in raw, raw
        assert "[REDACTED]" in raw, raw
        assert det["trace"]["total_cost_nanos"] == 7_500_000, det["trace"]


@test_case(
    id="T3-23",
    tier=3,
    feature="F23",
    title="Documentation (rest-api.md, mcp.md) matches live CORS OPTIONS, 405 methods & MCP tools/list schemas",
    tags=("F23", "F14", "F16", "docs", "cors", "mcp"),
)
def test_t3_23_docs_vs_live_cors_405_and_mcp_schemas(ctx: TestContext) -> None:
    rest_doc = (_REPO_ROOT / "docs" / "rest-api.md").read_text(encoding="utf-8")
    mcp_doc = (_REPO_ROOT / "docs" / "mcp.md").read_text(encoding="utf-8")

    for tok in (
        "/v1/agent/traces",
        "/v1/otel/v1/traces",
        "/agent/sessions",
        "/agent/traces",
        "/agent/analytics",
        "/agent/compare",
        "/agent/evaluators",
        "/agent/datasets",
        "/agent/experiments",
        "/agent/alerts",
        "/agent/privacy",
        "/agent/pricing",
    ):
        assert tok in rest_doc, f"docs/rest-api.md missing {tok!r}"

    for tool_name in _AGENT_MCP_TOOLS:
        assert tool_name in mcp_doc, f"docs/mcp.md missing {tool_name!r}"

    evil_origin = "https://untrusted-origin.example"
    opt_pub = ctx.client.request(
        "OPTIONS",
        "/v1/agent/traces",
        headers={"Origin": evil_origin, "Access-Control-Request-Method": "POST"},
    )
    assert opt_pub.status in (200, 204), opt_pub.status
    assert opt_pub.header("access-control-allow-origin") == "*", opt_pub.headers

    get_prot = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/traces",
        headers={"Origin": evil_origin},
        token=ctx.viewer_token,
    )
    assert get_prot.status == 200
    acao = get_prot.header("access-control-allow-origin")
    assert acao not in ("*", evil_origin), f"Protected route leaked ACAO={acao!r}"

    assert ctx.client.request("GET", "/v1/agent/traces", api_key=ctx.proj_a.pk_key).status == 405

    tools_rpc = ctx.client.mcp_call(
        "tools/list", {}, token=ctx.viewer_token, project_id=ctx.proj_a.id
    ).raise_for_status().json()
    tools = {t["name"]: t for t in tools_rpc["result"]["tools"]}
    for tool_name in _AGENT_MCP_TOOLS:
        assert tool_name in tools, f"Missing live MCP tool {tool_name!r}"
        schema = tools[tool_name]["inputSchema"]
        assert schema.get("type") == "object"
        assert isinstance(schema.get("properties"), dict)
    assert "trace_id" in tools["get_agent_trace"]["inputSchema"]["properties"]


@test_case(
    id="T3-24",
    tier=3,
    feature="F24",
    title="Retention pruning (retention_days=1) + flush evicts expired PLSS partitions while keeping fresh/SQL & /readyz",
    tags=("F24", "F08", "F19", "retention", "plss", "health"),
)
def test_t3_24_retention_pruning_preserves_fresh_sql_and_readyz(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_24")
    ctx.client.request(
        "PUT",
        f"/v1/projects/{proj.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(project_id=proj.id, retention_days=1),
        token=ctx.admin_token,
    ).raise_for_status()

    ev = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(name=ctx.unique_id("t324_ev"), enabled=False),
        token=ctx.member_token,
    ).raise_for_status().json()
    ds = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/datasets",
        json_body=fixtures.make_dataset(name=ctx.unique_id("t324_ds")),
        token=ctx.member_token,
    ).raise_for_status().json()

    now_ms = int(time.time() * 1000)
    old_b = fixtures.make_batch(
        seed=ctx.unique_id("t324_old"), project_id=proj.id, start_ts_ms=now_ms - 10 * 86_400 * 1000
    )
    fresh_b = fixtures.make_batch(
        seed=ctx.unique_id("t324_new"), project_id=proj.id, start_ts_ms=now_ms - 10_000
    )
    old_tid = old_b["traces"][0]["trace_id"]
    fresh_tid = fresh_b["traces"][0]["trace_id"]

    ctx.client.ingest_plst_batch(old_b, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch(fresh_b, api_key=proj.pk_key).raise_for_status()
    _wait_trace_detail(ctx.client, proj.id, fresh_tid, ctx.viewer_token)

    ctx.client.request("POST", "/v1/admin/flush", token=ctx.root_admin_token).raise_for_status()

    def _pruned() -> bool | None:
        r = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token)
        if r.status != 200:
            return None
        ids = {t["trace_id"] for t in _extract_items(r.json(), ("traces", "items"))}
        return True if (fresh_tid in ids and old_tid not in ids) else None

    _poll_until(_pruned, timeout=20.0, interval=0.25, desc="expired trace retention pruning")

    assert ctx.client.get_readyz().status == 200
    evs = _extract_items(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/evaluators", token=ctx.viewer_token)
        .raise_for_status()
        .json(),
        ("evaluators", "items"),
    )
    dss = _extract_items(
        ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/datasets", token=ctx.viewer_token)
        .raise_for_status()
        .json(),
        ("datasets", "items"),
    )
    assert any(e["id"] == ev["id"] for e in evs), evs
    assert any(d["id"] == ds["id"] for d in dss), dss


@test_case(
    id="T3-25",
    tier=3,
    feature="F15",
    title="Multi-tenant isolation across privacy policies, pricing rules, pk_ ingest, sk_ keys, and other_org_token",
    tags=("F15", "F14", "F19", "tenancy", "rbac", "privacy", "pricing"),
)
def test_t3_25_multi_tenant_policy_pricing_and_key_isolation(ctx: TestContext) -> None:
    p_a = _create_temp_project(ctx, "t3_25_a")
    p_b = _create_temp_project(ctx, "t3_25_b")

    ctx.client.request(
        "PUT",
        f"/v1/projects/{p_a.id}/agent/privacy",
        json_body=fixtures.make_privacy_policy(
            project_id=p_a.id, capture_outputs=False, capture_user_id=True
        ),
        token=ctx.admin_token,
    ).raise_for_status()
    ctx.client.request(
        "POST",
        f"/v1/projects/{p_a.id}/agent/pricing",
        json_body=fixtures.make_pricing_rule(
            provider="openai",
            model_pattern="gpt-4o-mini",
            input_per_1m_usd=100.0,
            output_per_1m_usd=100.0,
            cached_input_per_1m_usd=0.0,
        ),
        token=ctx.admin_token,
    ).raise_for_status()

    ts = _recent_ts_ms()

    def _send(proj: ProjectCreds, suffix: str) -> str:
        tid = f"tr_{ctx.unique_id(f't325_{suffix}')}"
        sp = fixtures.make_span(
            f"sp_{tid}",
            tid,
            project_id=proj.id,
            span_type="llm",
            provider="openai",
            model="gpt-4o-mini",
            start_ts_ms=ts,
            input_tokens=1000,
            output_tokens=500,
            cached_tokens=0,
            cost_nanos=0,
            cost_usd=0.0,
            output_payload={"answer": "val"},
        )
        tr = fixtures.make_trace(
            tid,
            project_id=proj.id,
            session_id=f"sess_{tid}",
            user_id="u_iso",
            start_ts_ms=ts,
            output_payload={"answer": "val"},
            spans=[sp],
        )
        ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
        return tid

    tid_a = _send(p_a, "a")
    tid_b = _send(p_b, "b")

    det_a = _wait_trace_detail(ctx.client, p_a.id, tid_a, ctx.viewer_token)
    det_b = _wait_trace_detail(ctx.client, p_b.id, tid_b, ctx.viewer_token)

    assert det_a["trace"].get("output") in (None, {}), det_a["trace"]
    assert det_a["trace"].get("user_id") == "u_iso", det_a["trace"]
    # 1000 * 100_000 + 500 * 100_000 = 150_000_000 nanos
    assert det_a["trace"]["total_cost_nanos"] == 150_000_000, det_a["trace"]

    assert det_b["trace"].get("output") == {"answer": "val"}, det_b["trace"]
    assert det_b["trace"].get("user_id") is None, det_b["trace"]
    # Built-in gpt-4o-mini ($0.15/$0.60): 1000 * 150 + 500 * 600 = 450_000 nanos
    assert det_b["trace"]["total_cost_nanos"] == 450_000, det_b["trace"]

    assert (
        ctx.client.request("GET", f"/v1/projects/{p_b.id}/agent/traces", token=p_a.sk_key).status
        == 403
    )
    assert (
        ctx.client.request("GET", f"/v1/projects/{p_a.id}/agent/traces", token=ctx.other_org_token).status
        in (403, 404)
    )


@test_case(
    id="T3-26",
    tier=3,
    feature="F19",
    title="Tool-retry loop triggers loop_rate & tool_failure_rate alerts, no_loop evals, and HMAC webhooks",
    tags=("F19", "F12", "F17", "alerts", "loop", "webhook", "hmac"),
)
def test_t3_26_loop_triggers_alerts_evals_and_hmac_webhooks(ctx: TestContext) -> None:
    proj = _create_temp_project(ctx, "t3_26")
    signing_secret = "t3_loop_hook_secret_1"
    _configure_byok_webhook(ctx, proj.id, signing_secret)
    assert ctx.mock_webhook is not None
    ctx.mock_webhook.signing_secret = signing_secret
    try:
        ev_cfg = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/evaluators",
            json_body=fixtures.make_evaluator_config(
                name=ctx.unique_id("t326_noloop"), kind="no_loop", target_type="trace", enabled=True
            ),
            token=ctx.member_token,
        ).raise_for_status().json()

        rule_ids: list[str] = []
        for metric in ("loop_rate", "tool_failure_rate"):
            r = ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/alerts",
                json_body=fixtures.make_alert_rule(
                    name=ctx.unique_id(f"t326_{metric}"),
                    metric=metric,
                    operator="gt",
                    threshold=0.2,
                    window_minutes=15,
                    cooldown_minutes=5,
                    webhook_url=ctx.mock_webhook_url,
                ),
                token=ctx.member_token,
            ).raise_for_status().json()
            rule_ids.append(r["id"])

        ts = _recent_ts_ms()
        for i in range(3):
            b = fixtures.make_tool_retry_loop_batch(
                seed=ctx.unique_id(f"t326_lp{i}"),
                project_id=proj.id,
                retry_count=4,
                start_ts_ms=ts + i * 30,
            )
            ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
            _wait_trace_detail(ctx.client, proj.id, b["traces"][0]["trace_id"], ctx.viewer_token)

        evals = _wait_evals(ctx.client, proj.id, ctx.viewer_token, min_count=3, timeout=15.0)
        noloop_evals = [e for e in evals if e.get("evaluator_id") == ev_cfg["id"]]
        assert len(noloop_evals) == 3, noloop_evals
        assert all(e["passed"] is False and e.get("label") == "loop_detected" for e in noloop_evals), noloop_evals

        _trigger_alert_evaluate(ctx.client, proj.id, rule_ids, ctx.member_token)
        hooks = ctx.mock_webhook.wait_for_count(2, timeout=12.0)
        assert len(hooks) == 2
        assert all(h["signature_valid"] is True for h in hooks), hooks

        rules_after = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts", token=ctx.viewer_token)
            .raise_for_status()
            .json(),
            ("rules", "alerts", "items"),
        )
        by_id = {r["id"]: r for r in rules_after}
        for rid in rule_ids:
            assert by_id[rid]["state"] == "firing", by_id[rid]

        events = _extract_items(
            ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/alerts/events", token=ctx.viewer_token)
            .raise_for_status()
            .json(),
            ("events", "items"),
        )
        delivered_rules = {e["rule_id"] for e in events if e.get("event_type") == "triggered" and e.get("delivered") is True}
        assert set(rule_ids).issubset(delivered_rules), events
    finally:
        ctx.mock_webhook.signing_secret = None
