"""Tier 5 Adversarial Coverage Hardening E2E Suite."""

from __future__ import annotations

import base64
import hashlib
import http.client
import http.server
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import HttpStatusError, PulseHttpClient  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance, free_port  # noqa: E402
from lib.oracle import (  # noqa: E402
    decode_manifest,
    feature,
    make_agent_trace,
    make_event,
    make_events,
    make_replay_chunk,
    random_byok_hex,
    require_milestone,
    skip_if_no_postgres,
    tier,
    wait_until,
)


def _create_project(client: PulseHttpClient, name: str) -> tuple[str, str]:
    body = client.create_project(name).raise_for_status().json()
    return body["project"]["id"], body["api_key"]["plaintext_key"]


def _insights_total(
    client: PulseHttpClient,
    project_id: str,
    event_name: str,
    *,
    start_ms: int,
    end_ms: int,
) -> float:
    resp = client.query(
        {
            "project_id": project_id,
            "kind": "insights",
            "query": {
                "time_range": {"start_ts_ms": start_ms, "end_ts_ms": end_ms},
                "interval": "day",
                "series": [{"label": "cnt", "event_name": event_name, "metric": "totals"}],
            },
        }
    ).raise_for_status().json()
    series = resp.get("series") or []
    return float(series[0]["overall_value"]) if series else 0.0


def _wait_project_open(
    client: PulseHttpClient,
    project_id: str,
    start_ms: int,
    end_ms: int,
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    def _try_query() -> dict[str, Any] | None:
        resp = client.query(
            {
                "project_id": project_id,
                "kind": "insights",
                "query": {
                    "time_range": {"start_ts_ms": start_ms, "end_ts_ms": end_ms},
                    "interval": "day",
                    "series": [{"label": "cnt", "event_name": "page_view", "metric": "totals"}],
                },
            }
        )
        if resp.status == 200:
            return resp.json()
        return None

    res = wait_until(_try_query, timeout=timeout, interval=0.25, desc=f"project {project_id} open")
    assert res is not None
    return res


def _wait_w1_drained(client: PulseHttpClient, *, timeout: float = 15.0) -> None:
    wait_until(
        lambda: (
            (m := client.parsed_metrics()).sum_by_name("pulse_writer_buffer_events") == 0.0
            and m.sum_by_name("pulse_ingest_log_lag_bytes") == 0.0
        ),
        timeout=timeout,
        interval=0.05,
        desc="W1 writer buffer and ingest log lag drained to 0",
    )


def _incompressible_str(seed: str, length: int) -> str:
    chunks: list[str] = []
    cur = 0
    counter = 0
    while cur < length:
        digest = hashlib.sha256(f"{seed}:{counter}".encode("utf-8")).digest()
        b85 = base64.b85encode(digest).decode("ascii")
        chunks.append(b85)
        cur += len(b85)
        counter += 1
    return "".join(chunks)[:length]


def _chmod_tree(root: Path, dir_mode: int, file_mode: int | None = None) -> None:
    if not root.exists():
        return
    if dir_mode & 0o700 == 0o700:
        os.chmod(root, dir_mode)
    for dirpath, dirnames, filenames in os.walk(root, topdown=(dir_mode & 0o200 != 0)):
        if dir_mode & 0o200 == 0:
            for f in filenames:
                if file_mode is not None:
                    try:
                        os.chmod(Path(dirpath) / f, file_mode)
                    except OSError:
                        pass
            for d in dirnames:
                try:
                    os.chmod(Path(dirpath) / d, dir_mode)
                except OSError:
                    pass
            try:
                os.chmod(dirpath, dir_mode)
            except OSError:
                pass
        else:
            try:
                os.chmod(dirpath, dir_mode)
            except OSError:
                pass
            for d in dirnames:
                try:
                    os.chmod(Path(dirpath) / d, dir_mode)
                except OSError:
                    pass
            if file_mode is not None:
                for f in filenames:
                    try:
                        os.chmod(Path(dirpath) / f, file_mode)
                    except OSError:
                        pass


class _WebhookSink:
    """Tiny stdlib HTTP server capturing alert webhook deliveries."""

    def __init__(self) -> None:
        self.deliveries: list[tuple[str, dict[str, Any]]] = []
        self._lock = threading.Lock()
        self.port = free_port()
        sink = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:
                cl = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(cl) if cl > 0 else b"{}"
                with sink._lock:
                    sink.deliveries.append(
                        (self.path, json.loads(raw.decode("utf-8", errors="replace")))
                    )
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        self._httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def url(self, path: str = "/hook") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def start(self) -> _WebhookSink:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5.0)


def _create_isolated_pg_db(test_case: unittest.TestCase, base_pg_url: str) -> str:
    """Create a fresh isolated Postgres database for the test and drop it on cleanup."""
    parsed = urllib.parse.urlsplit(base_pg_url)
    admin_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/postgres", parsed.query, ""))
    db_name = f"pulse_t5_{uuid.uuid4().hex[:12]}"
    proc = subprocess.run(
        ["psql", admin_url, "-v", "ON_ERROR_STOP=1", "-c", f'CREATE DATABASE "{db_name}";'],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"Failed to create isolated Postgres DB {db_name}: {proc.stderr}")

    def _drop_db() -> None:
        subprocess.run(
            ["psql", admin_url, "-c", f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE);'],
            capture_output=True,
            text=True,
            check=False,
        )

    test_case.addCleanup(_drop_db)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, f"/{db_name}", parsed.query, ""))


def _psql_exec(pg_url: str, sql: str) -> str:
    proc = subprocess.run(
        ["psql", pg_url, "-v", "ON_ERROR_STOP=1", "-Atc", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"psql failed ({sql}): {proc.stderr}")
    return proc.stdout.strip()


@tier(5)
class TestTier5Adversarial(unittest.TestCase):
    """Adversarial coverage hardening suite (crash matrices, concurrency, split-role, Postgres)."""

    @feature("F19", "F20")
    @require_milestone("T3")
    def test_adv_01_s12_full_7_step_crash_matrix(self) -> None:
        """Adv-01 (S12): SIGKILL across missing upload steps 1, 2, 3, 5, and 7 + intent sweep."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=300,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "Adv01 7-Step Crash")
            now_ms = int(time.time() * 1000)

            # Steps 1..6: crash both BEFORE (step1, step5 via on_before_operation + unreachable fault)
            # and AFTER (step2, step3, step4, step6 via on_operation) each S3 upload step.
            # Run setup (engage/ingest/drain/flush) with upload_interval_secs=300 so background
            # uploads never race setup, then SIGKILL (skipping graceful shutdown publish), arm
            # the S3 kill callback, and restart with upload_interval_secs=1 to trigger the crash.
            crash_targets = [
                ("step1_before_profiles", f"published/{pid}/snapshots/profiles_", True),
                ("step2_after_profiles", f"published/{pid}/snapshots/profiles_", False),
                ("step3_after_identity", f"published/{pid}/snapshots/identity_", False),
                ("step4_after_segments", f"segments/{pid}/", False),
                ("step5_before_pub", f"published/{pid}/pub_", True),
                ("step6_after_pub", f"published/{pid}/pub_", False),
            ]
            expected_events = 0
            for idx, (label, prefix, is_before) in enumerate(crash_targets):
                pubs_before_step = len(
                    [k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k and k.endswith(".json")]
                )
                client.engage(
                    {"distinct_id": f"u_adv1_{idx}", "$set": {"step": label}},
                    api_key=api_key,
                ).raise_for_status()
                client.ingest(
                    {
                        "events": make_events(
                            4,
                            event_name="adv1_ev",
                            distinct_id_prefix=f"u_adv1_{idx}",
                            base_ts_ms=now_ms + idx * 1000,
                            seed_prefix=f"adv1_{idx}",
                        )
                    },
                    api_key=api_key,
                ).raise_for_status()
                expected_events += 4
                _wait_w1_drained(client)
                client.admin_flush().raise_for_status()

                # Kill without graceful shutdown so final publish does not drain dirty local state.
                node.stop(graceful=False)

                killed = threading.Event()
                active_node: list[NodeInstance] = []
                if is_before:
                    def _kill_before(
                        op: str,
                        key: str,
                        *,
                        p: str = prefix,
                        ev: threading.Event = killed,
                        box: list[NodeInstance] = active_node,
                    ) -> None:
                        if op == "PUT" and key.startswith(p) and not ev.is_set():
                            ev.set()
                            s3.set_fault(unreachable=True)
                            if box and box[0].proc:
                                box[0].proc.kill()

                    s3.on_before_operation(_kill_before)
                else:
                    def _kill_after(
                        op: str,
                        key: str,
                        *,
                        p: str = prefix,
                        ev: threading.Event = killed,
                        box: list[NodeInstance] = active_node,
                    ) -> None:
                        if op == "PUT" and key.startswith(p) and not ev.is_set():
                            ev.set()
                            if box and box[0].proc:
                                box[0].proc.kill()

                    s3.on_operation(_kill_after)

                node = cluster.start_node(
                    data_dir=node.data_dir,
                    hot_store="local",
                    upload_interval_secs=1,
                    writer_lease_ttl_secs=3,
                    wait_ready=False,
                )
                assert isinstance(node, NodeInstance)
                active_node.append(node)
                try:
                    node.wait_ready()
                    _wait_project_open(node.client(), pid, now_ms - 60_000, now_ms + 60_000)
                except (RuntimeError, OSError, http.client.HTTPException):
                    if not killed.is_set():
                        raise
                wait_until(killed.is_set, timeout=20.0, desc=f"crash triggered at {label}")
                s3.clear_faults()
                s3.clear_before_operations()
                s3.clear_callbacks()
                node.stop(graceful=False)
                if label == "step5_before_pub":
                    pubs_after_crash = [
                        k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k and k.endswith(".json")
                    ]
                    self.assertEqual(
                        len(pubs_after_crash),
                        pubs_before_step,
                        "step5_before_pub must kill before pub_ PUT persists to S3",
                    )

                node = cluster.start_node(
                    data_dir=node.data_dir,
                    hot_store="local",
                    upload_interval_secs=1,
                    writer_lease_ttl_secs=3,
                )
                assert isinstance(node, NodeInstance)
                client = node.client()
                _wait_project_open(client, pid, now_ms - 60_000, now_ms + 60_000)
                wait_until(
                    lambda exp=expected_events: _insights_total(
                        client, pid, "adv1_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000
                    )
                    == float(exp),
                    timeout=20.0,
                    ignore_exceptions=(HttpStatusError,),
                    desc=f"post-{label} recovery preserves {expected_events} events",
                )
                self.assertEqual(
                    client.get_user(pid, f"u_adv1_{idx}_0000").status,
                    200,
                )
                # Flush/publish cleanly via graceful stop and return to 300s interval for next setup.
                node.stop(graceful=True)
                node = cluster.start_node(
                    data_dir=node.data_dir,
                    hot_store="local",
                    upload_interval_secs=300,
                    writer_lease_ttl_secs=3,
                )
                assert isinstance(node, NodeInstance)
                client = node.client()
                _wait_project_open(client, pid, now_ms - 60_000, now_ms + 60_000)

            # Step 7 (step7_after_publish_before_hot_delete): allow publish to succeed while
            # hot/segments/ dir is read-only so Step 7 cannot unlink local .pvtx, SIGKILL,
            # restore permissions, and assert startup sweep!
            client.ingest(
                {
                    "events": make_events(
                        3,
                        event_name="adv1_ev",
                        distinct_id_prefix="u_adv1_step7",
                        base_ts_ms=now_ms + 10_000,
                        seed_prefix="adv1_step7",
                    )
                },
                api_key=api_key,
            ).raise_for_status()
            expected_events += 3
            _wait_w1_drained(client)
            client.admin_flush().raise_for_status()

            hot_seg_files = node.local_files(f"hot/hot/segments/{pid}/")
            self.assertGreaterEqual(len(hot_seg_files), 1)
            locked_dir = (node.data_dir / hot_seg_files[0]).parent
            self.addCleanup(lambda d=locked_dir: _chmod_tree(d, 0o755, 0o644))
            _chmod_tree(locked_dir, 0o555, 0o444)

            node.stop(graceful=False)
            # Restart with 1s upload interval, wait for pub_ PUT (Step 6 completes, Step 7 delete
            # fails with EACCES), then SIGKILL immediately.
            node_s7 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node_s7, NodeInstance)
            pubs_before = len(
                [k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k and k.endswith(".json")]
            )
            _wait_project_open(node_s7.client(), pid, now_ms - 60_000, now_ms + 60_000)
            wait_until(
                lambda: len(
                    [k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k and k.endswith(".json")]
                )
                > pubs_before,
                timeout=20.0,
                desc="Step 6 pub_ written while Step 7 delete blocked",
            )
            node_s7.kill()
            _chmod_tree(locked_dir, 0o755, 0o644)

            node_final = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node_final, NodeInstance)
            cf = node_final.client()
            _wait_project_open(cf, pid, now_ms - 60_000, now_ms + 60_000)
            wait_until(
                lambda: len(node_final.local_files(f"hot/hot/segments/{pid}/")) == 0,
                timeout=15.0,
                desc="startup sweep cleans leftover Step 7 local segments",
            )
            self.assertEqual(
                _insights_total(cf, pid, "adv1_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                float(expected_events),
            )

    @feature("F12", "F26")
    @require_milestone("T4")
    def test_adv_02_cold_merge_erasure_and_crash_recovery(self) -> None:
        """Adv-02: Cold S3 merge physically purges DPDP-erased user and recovers from mid-cold-merge SIGKILL."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env={
                    "PULSE_COLD_MERGE_IDLE_SECS": "60",
                    "PULSE_COLD_MERGE_INTERVAL_SECS": "60",
                    "PULSE_CLOSED_DATE_UPLOAD_SECS": "0",
                    "PULSE_NUM_BUCKETS": "1",
                },
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "Adv02 ColdMerge Erase")
            closed_ts = int(time.time() * 1000) - 3 * 86_400_000

            # Seed 2 separate closed-date S3 segments for (day - 3, bucket 0).
            client.ingest(
                {"events": [make_event(event_name="adv2_ev", distinct_id="u_erase_cm", ts_ms=closed_ts)]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(client)
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            client.ingest(
                {"events": [make_event(event_name="adv2_ev", distinct_id="u_keep_cm", ts_ms=closed_ts + 500)]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(client)
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000001.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)
            self.assertEqual(len(s3.list_objects(f"segments/{pid}/")), 2)

            # DPDP-erase u_erase_cm, then restart with cold merge enabled (idle=1s) and inject
            # SIGKILL right after merged cold segment PUTs before Step 6 pub_2 completes!
            client.delete_user(pid, "u_erase_cm").raise_for_status()
            node.stop(graceful=True)

            cm_env = {
                "PULSE_COLD_MERGE_IDLE_SECS": "1",
                "PULSE_COLD_MERGE_INTERVAL_SECS": "1",
                "PULSE_CLOSED_DATE_UPLOAD_SECS": "0",
                "PULSE_NUM_BUCKETS": "1",
            }
            killed = threading.Event()
            merged_put_seen = threading.Event()

            def _on_s3(op: str, key: str) -> None:
                if op == "PUT" and key.startswith(f"segments/{pid}/"):
                    merged_put_seen.set()
                elif op == "PUT" and key == f"published/{pid}/pub_000000000002.json" and merged_put_seen.is_set() and not killed.is_set():
                    killed.set()
                    if node_cm.proc:
                        node_cm.proc.kill()

            s3.on_before_operation(_on_s3)
            node_cm = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env=cm_env,
            )
            assert isinstance(node_cm, NodeInstance)
            _wait_project_open(node_cm.client(), pid, closed_ts - 60_000, closed_ts + 60_000)
            wait_until(killed.is_set, timeout=25.0, desc="mid-cold-merge SIGKILL before pub_2")
            s3.clear_before_operations()
            s3.clear_callbacks()
            node_cm.stop(graceful=False)

            # Restart and verify consolidation to 1 segment + u_erase_cm absent + u_keep_cm intact!
            node_rec = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env=cm_env,
            )
            assert isinstance(node_rec, NodeInstance)
            cr = node_rec.client()
            _wait_project_open(cr, pid, closed_ts - 60_000, closed_ts + 60_000)
            wait_until(
                lambda: len(s3.list_objects(f"segments/{pid}/")) == 1,
                timeout=30.0,
                desc="cold merge consolidated and GC'd old S3 segments",
            )
            self.assertEqual(
                _insights_total(cr, pid, "adv2_ev", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                1.0,
            )
            exported = cr.export_events(pid, format="json").raise_for_status().json()
            self.assertEqual(exported["count"], 1)
            self.assertEqual(exported["events"][0]["distinct_id"], "u_keep_cm")

            # Verify erased user is physically absent from merged S3 segment even on a fresh
            # read-only Query node pointed at a clean metadata DB with zero erased_uids!
            clean_q_dir = cluster.node_mgr.make_temp_data_dir("adv2_clean_q_")
            q_clean = cluster.start_node(
                role="query",
                data_dir=clean_q_dir,
                hot_store="local",
                upload_interval_secs=1,
            )
            assert isinstance(q_clean, NodeInstance)
            q_clean.stop(graceful=True)

            proj_rows = node_rec.sqlite_query(
                "SELECT id, org_id, name, timezone, created_at_ms FROM projects WHERE id = ?",
                (pid,),
            )
            self.assertEqual(len(proj_rows), 1)
            pr = proj_rows[0]
            with sqlite3.connect(str(clean_q_dir / "pulse_meta.db")) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO projects (id, org_id, name, timezone, created_at_ms) VALUES (?, ?, ?, ?, ?)",
                    (pr["id"], pr["org_id"], pr["name"], pr["timezone"], pr["created_at_ms"]),
                )
                erased_cnt = conn.execute("SELECT COUNT(*) FROM erased_uids").fetchone()[0]
                self.assertEqual(erased_cnt, 0)
                conn.commit()

            q_clean = cluster.start_node(
                role="query",
                data_dir=clean_q_dir,
                hot_store="local",
                upload_interval_secs=1,
            )
            assert isinstance(q_clean, NodeInstance)
            cq_clean = q_clean.client()
            _wait_project_open(cq_clean, pid, closed_ts - 60_000, closed_ts + 60_000)
            self.assertEqual(
                _insights_total(cq_clean, pid, "adv2_ev", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                1.0,
            )
            cq_exported = cq_clean.export_events(pid, format="json").raise_for_status().json()
            self.assertEqual(cq_exported["count"], 1)
            self.assertEqual(cq_exported["events"][0]["distinct_id"], "u_keep_cm")

            pubs = sorted(k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k and k.endswith(".json"))
            latest_pub = decode_manifest(s3.get_object(pubs[-1]))
            total_l1_rows = sum(
                int(seg.get("footer", {}).get("row_count", 0))
                for part in latest_pub.get("partitions", [])
                for seg in part.get("segments", [])
            )
            self.assertEqual(total_l1_rows, 1, f"Erased user must be physically stripped from merged segment: {latest_pub}")

    @feature("F12", "F17")
    @require_milestone("W3")
    def test_adv_03_s07_split_role_alerts_mcp_cohort_and_query_reengage(self) -> None:
        """Adv-03 (S07+Bug5): Split-role Postgres cluster (Writer+Ingest+Query), cohorts, export, /v1/mcp, re-engage, and live auto alert webhook."""
        base_pg = skip_if_no_postgres()
        pg_url = _create_isolated_pg_db(self, base_pg)
        sink = _WebhookSink().start()
        self.addCleanup(sink.stop)

        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            kek = random_byok_hex()
            common_env = {
                "PULSE_VAULT_KEK": kek,
                "PULSE_META_RELOAD_INTERVAL_MS": "200",
                "PULSE_ALERT_EVAL_INTERVAL_SECS": "1",
                "PULSE_ALLOW_PRIVATE_ENDPOINTS": "1",
            }
            ingest = cluster.start_node(
                role="ingest",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
                node_name="ingest-adv3",
                extra_env=common_env,
            )
            writer = cluster.start_node(
                role="writer",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                node_name="writer-adv3",
                extra_env=common_env,
            )
            query = cluster.start_node(
                role="query",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                node_name="query-adv3",
                extra_env=common_env,
            )
            assert isinstance(ingest, NodeInstance)
            assert isinstance(writer, NodeInstance)
            assert isinstance(query, NodeInstance)

            cw = writer.client()
            cq = query.client()
            pid, api_key = _create_project(cq, "Adv03 Split Alerts & MCP")
            now_ms = int(time.time() * 1000)

            # Ingest aliased user (anon_a -> user_a) and user_b via Writer
            id_a = cw.identify(
                {"anon_id": "anon_a", "user_id": "user_a", "user_properties": {"plan": "pro"}},
                api_key=api_key,
            ).raise_for_status().json()
            id_b = cw.identify(
                {"anon_id": "anon_b", "user_id": "user_b", "user_properties": {"plan": "pro"}},
                api_key=api_key,
            ).raise_for_status().json()
            uid_a = int(id_a["canonical_uid"])
            uid_b = int(id_b["canonical_uid"])

            cw.ingest(
                {
                    "events": [
                        make_event(event_name="adv3_ev", distinct_id="anon_a", ts_ms=now_ms - 2000),
                        make_event(event_name="adv3_ev", distinct_id="user_b", ts_ms=now_ms - 1000),
                    ]
                },
                api_key=api_key,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(cq, pid, "adv3_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 2.0,
                timeout=20.0,
                desc="query sees 2 adv3_ev events before erasure",
            )

            # Create dynamic cohort (performed_event) + static cohort (users) via Writer -> Ingest
            dyn_coh = cw.request(
                "POST",
                f"/v1/projects/{pid}/cohorts",
                json_body={
                    "name": "dyn_adv3",
                    "definition": {"kind": "performed_event", "event_name": "adv3_ev", "min_count": 1},
                },
                token=writer.admin_token,
            ).raise_for_status().json()
            stat_coh = cw.request(
                "POST",
                f"/v1/projects/{pid}/cohorts",
                json_body={
                    "name": "stat_adv3",
                    "definition": {"kind": "users", "canonical_uids": [uid_a, uid_b]},
                },
                token=writer.admin_token,
            ).raise_for_status().json()
            dyn_id = dyn_coh["id"]
            stat_id = stat_coh["id"]

            # Wait for query-0 cohort convergence (~5s refresh cadence)
            def _query_sees_both_cohorts() -> bool:
                r = cq.mcp_call("tools/call", {"name": "list_cohorts", "arguments": {}}, project_id=pid)
                if r.status != 200:
                    return False
                cohorts = r.json().get("result", {}).get("structuredContent", {}).get("cohorts", [])
                by_id = {c["id"]: c.get("users") for c in cohorts}
                return by_id.get(dyn_id) == 2 and by_id.get(stat_id) == 2

            wait_until(_query_sees_both_cohorts, timeout=20.0, desc="query-0 sees both cohorts with 2 users")

            # Freeze publish, erase anon_a via Writer, verify Query + Owner hide user_a without restart!
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            cw.delete_user(pid, "anon_a").raise_for_status()

            def _erased_converged_on_query() -> bool:
                exp = cq.request(
                    "GET",
                    f"/v1/projects/{pid}/cohorts/{stat_id}/export",
                    query={"format": "json"},
                    token=query.admin_token,
                )
                if exp.status != 200:
                    return False
                users = exp.json().get("users") or []
                return len(users) == 1 and users[0].get("distinct_id") == "user_b"

            wait_until(_erased_converged_on_query, timeout=20.0, desc="cohort export hides erased user on query-0")

            for client in (cw, cq):
                exp_stat = client.request(
                    "GET",
                    f"/v1/projects/{pid}/cohorts/{stat_id}/export",
                    query={"format": "json"},
                    token=query.admin_token,
                ).raise_for_status().json()
                self.assertEqual([u["distinct_id"] for u in exp_stat["users"]], ["user_b"])

                exp_dyn = client.request(
                    "GET",
                    f"/v1/projects/{pid}/cohorts/{dyn_id}/export",
                    query={"format": "json"},
                    token=query.admin_token,
                ).raise_for_status().json()
                self.assertEqual([u["distinct_id"] for u in exp_dyn["users"]], ["user_b"])

            # Verify MCP list_cohorts, get_user, and query_insights (with series + global_filter.cohort) on Query
            mcp_cohs = cq.mcp_call("tools/call", {"name": "list_cohorts", "arguments": {}}, project_id=pid).raise_for_status().json()
            coh_map = {c["id"]: c["users"] for c in mcp_cohs["result"]["structuredContent"]["cohorts"]}
            self.assertEqual(coh_map[stat_id], 1)
            self.assertEqual(coh_map[dyn_id], 1)

            mcp_user = cq.mcp_call(
                "tools/call", {"name": "get_user", "arguments": {"distinct_id": "user_a"}}, project_id=pid
            ).raise_for_status().json()
            u_sc = mcp_user["result"]["structuredContent"]
            self.assertIsNone(u_sc.get("properties"))
            self.assertEqual(u_sc.get("recent_events"), [])

            mcp_ins = cq.mcp_call(
                "tools/call",
                {
                    "name": "query_insights",
                    "arguments": {
                        "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
                        "interval": "day",
                        "series": [{"label": "A", "event_name": "adv3_ev", "metric": {"type": "totals"}}],
                        "global_filter": {"predicates": [], "cohort": {"kind": "cohort_ref", "cohort_id": stat_id}},
                    },
                },
                project_id=pid,
            ).raise_for_status().json()
            self.assertEqual(mcp_ins["result"]["structuredContent"]["series"][0]["overall_value"], 1.0)

            # Clear publish fault, re-engage user_a, wait for publish + query visibility without resurrecting old events
            s3.clear_faults()
            time.sleep(0.05)
            cw.engage({"distinct_id": "user_a", "$set": {"plan": "reengaged"}}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: (
                    cq.get_user(pid, "user_a").status == 200
                    and (cq.get_user(pid, "user_a").json().get("profile") or {}).get("props", {}).get("plan") == "reengaged"
                ),
                timeout=20.0,
                desc="query-0 sees re-engaged user_a profile",
            )
            self.assertEqual(
                _insights_total(cq, pid, "adv3_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                1.0,
            )

            # Configure alert webhook + rule and verify background timer on Ingest fires automatically!
            cw.request(
                "PUT",
                f"/v1/projects/{pid}/byok",
                json_body={
                    "category": "webhook",
                    "provider": "webhook",
                    "secret": sink.url("/ev"),
                    "config_json": {"signing_secret": "adv3_webhook_secret_12345"},
                },
                token=writer.admin_token,
            ).raise_for_status()
            cw.create_alert(
                pid,
                {
                    "name": "adv3_spike",
                    "event_name": "adv3_alert_ev",
                    "operator": "gte",
                    "threshold": 2.0,
                    "webhook_url": sink.url("/ev"),
                },
                token=writer.admin_token,
            ).raise_for_status()
            cw.ingest(
                {"events": make_events(3, event_name="adv3_alert_ev", distinct_id_prefix="u_al", base_ts_ms=now_ms + 5000)},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: len(sink.deliveries) >= 1, timeout=15.0, desc="split-role background alert webhook fired")
            time.sleep(1.5)
            self.assertEqual(len(sink.deliveries), 1)

    @feature("F16", "F17")
    @require_milestone("T4")
    def test_adv_04_s10_active_query_pin_holds_s3_segment_beyond_grace(self) -> None:
        """Adv-04 (S10): Active query pin holds superseded S3 segment beyond 2-interval grace until query finishes."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            owner = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
                extra_env={
                    "PULSE_COLD_MERGE_IDLE_SECS": "4",
                    "PULSE_COLD_MERGE_INTERVAL_SECS": "1",
                    "PULSE_CLOSED_DATE_UPLOAD_SECS": "0",
                    "PULSE_NUM_BUCKETS": "1",
                },
            )
            assert isinstance(owner, NodeInstance)
            ca = owner.client()
            pid, api_key = _create_project(ca, "Adv04 Active Pin")
            closed_ts = int(time.time() * 1000) - 3 * 86_400_000

            for i in range(2):
                ca.ingest(
                    {"events": [make_event(event_name="adv4_ev", distinct_id=f"u_adv4_{i}", ts_ms=closed_ts + i * 100)]},
                    api_key=api_key,
                ).raise_for_status()
                _wait_w1_drained(ca)
                ca.admin_flush().raise_for_status()
                wait_until(
                    lambda seq=i: f"published/{pid}/pub_{seq:012}.json" in s3.list_objects(f"published/{pid}/"),
                    timeout=20.0,
                )

            old_segs = sorted(s3.list_objects(f"segments/{pid}/"))
            self.assertEqual(len(old_segs), 2)
            pinned_target = old_segs[0]

            # Block the active query's first GET on `pinned_target` so `owner` holds StorageSnapshot pin
            # across cold merge (pub_2) and beyond 2 * upload_interval (2s) grace!
            pin_entered = threading.Event()
            pin_release = threading.Event()
            self.addCleanup(pin_release.set)

            def _on_s3(op: str, key: str) -> None:
                if op == "GET" and key == pinned_target and not pin_entered.is_set():
                    pin_entered.set()
                    pin_release.wait(timeout=30.0)

            s3.on_before_operation(_on_s3)
            query_result: dict[str, Any] = {}

            def _run_slow_query() -> None:
                r = owner.client(timeout=35.0).export_events(pid, format="json", limit=100)
                query_result["status"] = r.status
                query_result["body"] = r.json()

            qt = threading.Thread(target=_run_slow_query, daemon=True)
            qt.start()
            self.assertTrue(pin_entered.wait(timeout=15.0), "active query did not enter S3 GET")

            # Wait for cold merge to supersede old_segs (pub_2) and wait > 2.5s (past 2s grace)
            wait_until(
                lambda: f"published/{pid}/pub_000000000002.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="cold merge published pub_2 while query holds pin",
            )
            self.assertTrue(qt.is_alive(), "query must still be holding StorageSnapshot pin across pub_2")
            time.sleep(2.8)
            # Force another upload/GC pass while query pin is still held
            ca.ingest(
                {"events": [make_event(event_name="adv4_ev_today", distinct_id="u_t", ts_ms=int(time.time() * 1000))]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(ca)
            ca.admin_flush().raise_for_status()
            wait_until(
                lambda: f"published/{pid}/pub_000000000003.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
            )

            # Superseded S3 segments MUST still exist and 0 DELETEs sent while pinned!
            self.assertTrue(qt.is_alive(), "query must still be active holding pin after pub_3")
            seg_deletes = sum(
                v for (_prefix, op), v in s3.get_counts(f"segments/{pid}/").items() if op == "DELETE"
            )
            self.assertEqual(seg_deletes, 0)
            for seg in old_segs:
                self.assertIsNotNone(s3.head_object(seg), f"Pinned segment {seg} deleted prematurely!")

            # Release active query pin and verify GC deletes superseded segments on next cycle!
            pin_release.set()
            qt.join(timeout=15.0)
            self.assertEqual(query_result.get("status"), 200)
            self.assertEqual(query_result["body"]["count"], 2)
            self.assertEqual(
                _insights_total(ca, pid, "adv4_ev", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                2.0,
            )

            s3.clear_before_operations()
            s3.clear_callbacks()
            ca.ingest(
                {"events": [make_event(event_name="adv4_ev_today", distinct_id="u_t2", ts_ms=int(time.time() * 1000))]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(ca)
            ca.admin_flush().raise_for_status()
            wait_until(
                lambda: all(s3.head_object(seg) is None for seg in old_segs),
                timeout=20.0,
                desc="superseded S3 segments deleted once query pin released",
            )

    @feature("F10", "F11", "F27")
    @require_milestone("T4")
    def test_adv_05_pg_restore_mixed_events_replay_agent_erasure(self) -> None:
        """Adv-05: Postgres + BYOK restore --from-published with UID 0 & >=HWM erasures, replay continuation, and agent traces."""
        base_pg = skip_if_no_postgres()
        pg_url = _create_isolated_pg_db(self, base_pg)
        byok = random_byok_hex()

        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                database_url=pg_url,
                hot_store="local",
                byok_master_key=byok,
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "Adv05 PG Restore")
            now_ms = int(time.time() * 1000)

            # UID 0 (`u_uid0`), UID 1 (`u_keep`), plus 2 replay chunks and 2 agent traces
            client.identify({"anon_id": "a_uid0", "user_id": "u_uid0", "user_properties": {"role": "first"}}, api_key=api_key).raise_for_status()
            client.identify({"anon_id": "a_keep", "user_id": "u_keep", "user_properties": {"role": "kept"}}, api_key=api_key).raise_for_status()
            client.ingest(
                {
                    "events": [
                        make_event(event_name="adv5_ev", distinct_id="u_uid0", ts_ms=now_ms - 3000),
                        make_event(event_name="adv5_ev", distinct_id="u_keep", ts_ms=now_ms - 2000),
                    ]
                },
                api_key=api_key,
            ).raise_for_status()
            client.replay_chunk(
                make_replay_chunk(session_id="rs_adv5", project_id=pid, chunk_start_ts_ms=now_ms - 2000),
                api_key=api_key,
                binary=True,
            ).raise_for_status()
            client.replay_chunk(
                make_replay_chunk(session_id="rs_adv5", project_id=pid, chunk_start_ts_ms=now_ms - 1000),
                api_key=api_key,
                binary=True,
            ).raise_for_status()
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_adv5_keep", start_ts_ms=now_ms - 1000), api_key=api_key).raise_for_status()
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_adv5_erase", start_ts_ms=now_ms - 500), api_key=api_key).raise_for_status()
            _wait_w1_drained(client)
            client.admin_flush().raise_for_status()

            wait_until(
                lambda: (
                    f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/")
                    and len(s3.list_objects(f"replays/{pid}/rs_adv5/")) == 2
                    and f"agent_published/{pid}/pub_000000000000.json" in s3.list_objects(f"agent_published/{pid}/")
                ),
                timeout=25.0,
                desc="initial events, 2 replay chunks, and agent traces published to S3",
            )

            # Pre-restore erasures: UID 0 (pre-HWM), post-publish >= HWM user, and agent trace
            client.ingest(
                {"events": [make_event(event_name="adv5_ev", distinct_id="u_ge_hwm", ts_ms=now_ms)]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(client)
            client.delete_user(pid, "u_uid0").raise_for_status()
            client.delete_user(pid, "u_ge_hwm").raise_for_status()
            client.erase_agent(pid, level="trace", target_id="tr_adv5_erase").raise_for_status()

            node.stop(graceful=False)
            shutil.rmtree(node.data_dir)
            restored_dir = cluster.node_mgr.make_temp_data_dir("adv5_restored_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url, "PULSE_BYOK_MASTER_KEY": byok},
            )
            self.assertEqual(res.returncode, 0, res.stderr)

            node2 = cluster.start_node(
                data_dir=restored_dir,
                database_url=pg_url,
                hot_store="local",
                byok_master_key=byok,
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)

            self.assertIsNone(c2.get_user(pid, "u_uid0").raise_for_status().json().get("profile"))
            self.assertIsNone(c2.get_user(pid, "u_ge_hwm").raise_for_status().json().get("profile"))
            self.assertEqual(
                _insights_total(c2, pid, "adv5_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                1.0,
            )

            # Append chunk2 to SAME replay session rs_adv5 -> must continue at seq 2 (no overwrite of 0000000000.plsr!)
            c2.replay_chunk(
                make_replay_chunk(session_id="rs_adv5", project_id=pid, chunk_start_ts_ms=now_ms + 1000),
                api_key=api_key,
                binary=True,
            ).raise_for_status()
            wait_until(
                lambda: len(s3.list_objects(f"replays/{pid}/rs_adv5/")) == 3,
                timeout=20.0,
                desc="post-restore replay continues monotonically to 3 S3 chunks",
            )
            rep_body = c2.get_replay(pid, "rs_adv5").raise_for_status().json()
            self.assertEqual(len(rep_body["chunks"]), 3)

            # Post-restore ingest at >= HWM succeeds & agent erasure preserved
            c2.ingest(
                {"events": [make_event(event_name="adv5_ev", distinct_id="u_post_new", ts_ms=now_ms + 2000)]},
                api_key=api_key,
            ).raise_for_status()
            _wait_w1_drained(c2)
            self.assertEqual(
                _insights_total(c2, pid, "adv5_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                2.0,
            )
            tr_ids = {t["trace_id"] for t in c2.list_agent_traces(pid).raise_for_status().json()["traces"]}
            self.assertEqual(tr_ids, {"tr_adv5_keep"})

    @feature("F21")
    @require_milestone("T3")
    def test_adv_06_multi_project_hot_segment_backpressure_and_429_recovery(self) -> None:
        """Adv-06: Per-project 2x hot_segment_bytes 429 isolation + zero-leak drain after S3 recovery."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env={
                    "PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "16384",
                    "PULSE_HOT_SEGMENTS_MAX_BYTES": "262144",
                },
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pa, ka = _create_project(client, "Adv06 Proj A")
            pb, kb = _create_project(client, "Adv06 Proj B")
            now_ms = int(time.time() * 1000)
            _wait_project_open(client, pa, now_ms - 60_000, now_ms + 60_000)
            _wait_project_open(client, pb, now_ms - 60_000, now_ms + 60_000)

            s3.set_fault(fail_prefixes=["segments/", "published/"], status=503)
            acked_a = 0
            hit_429 = False
            for i in range(40):
                r = client.ingest(
                    {
                        "events": make_events(
                            20,
                            event_name="adv6_a",
                            distinct_id_prefix=f"ua_{i}",
                            base_ts_ms=now_ms + i * 10,
                            props={"pad": _incompressible_str(f"adv6_{i}", 1500)},
                        )
                    },
                    api_key=ka,
                )
                if r.status == 429:
                    self.assertIsNotNone(r.header("retry-after"))
                    hit_429 = True
                    break
                r.raise_for_status()
                acked_a += int(r.json()["accepted"])
                _wait_w1_drained(client)
                client.admin_flush().raise_for_status()
            self.assertTrue(hit_429, "Project A must hit 429 HotSegmentBackpressure")

            # Project B below node 2x cap continues returning 200!
            client.ingest(
                {"events": make_events(5, event_name="adv6_b", distinct_id_prefix="ub", base_ts_ms=now_ms)},
                api_key=kb,
            ).raise_for_status()

            # Clear S3 fault; wait past the 2s upload failure cooldown for an ingest retry to wake
            # `request_upload_if_due()` and drain `pulse_hot_segment_bytes` strictly to 0!
            s3.clear_faults()
            wait_until(
                lambda: client.ingest(
                    {"events": [make_event(event_name="adv6_a", distinct_id="ua_after", ts_ms=now_ms + 999)]},
                    api_key=ka,
                ).status
                == 200,
                timeout=25.0,
                desc="Project A resumes 200 OK after S3 recovery and upload cooldown",
            )
            acked_a += 1
            wait_until(
                lambda: client.parsed_metrics().sum_by_name("pulse_hot_segment_bytes") == 0.0,
                timeout=25.0,
                desc="pulse_hot_segment_bytes drains strictly to 0 after S3 recovery",
            )
            self.assertEqual(node.local_files(f"hot/hot/segments/{pa}/"), [])
            _wait_w1_drained(client)
            self.assertEqual(
                _insights_total(client, pa, "adv6_a", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                float(acked_a),
            )
            self.assertEqual(
                _insights_total(client, pb, "adv6_b", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                5.0,
            )

    @feature("F22", "F23")
    @require_milestone("W1")
    def test_adv_07_s14_w1_sigkill_flush_bound_and_dlq_erasure_isolation(self) -> None:
        """Adv-07 (S14): 60000ms vs 20ms SIGKILL flush bound + schema-cap dead-letter CLI w/ aliased user erasure."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            # Part A1: 60000ms flush bound + immediate SIGKILL loses buffered event
            n60 = cluster.start_node(
                role="all",
                hot_store="local",
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_WRITER_FLUSH_MS": "60000"},
            )
            assert isinstance(n60, NodeInstance)
            c60 = n60.client()
            p60, k60 = _create_project(c60, "Adv07 60s")
            now_ms = int(time.time() * 1000)
            c60.ingest(
                {"events": [make_event(event_name="adv7_60s", distinct_id="u60", ts_ms=now_ms)]},
                api_key=k60,
            ).raise_for_status()
            n60.kill()
            n60_up = n60.restart(graceful_stop=False)
            assert isinstance(n60_up, NodeInstance)
            _wait_project_open(n60_up.client(), p60, now_ms - 60_000, now_ms + 60_000)
            self.assertEqual(
                _insights_total(n60_up.client(), p60, "adv7_60s", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                0.0,
            )

            # Part A2: 20ms flush bound + 0.25s wait + SIGKILL preserves WAL-committed event
            n20 = cluster.start_node(
                role="all",
                hot_store="local",
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_WRITER_FLUSH_MS": "20"},
            )
            assert isinstance(n20, NodeInstance)
            c20 = n20.client()
            p20, k20 = _create_project(c20, "Adv07 20ms")
            c20.ingest(
                {"events": [make_event(event_name="adv7_20ms", distinct_id="u20", ts_ms=now_ms)]},
                api_key=k20,
            ).raise_for_status()
            time.sleep(0.25)
            n20.kill()
            n20_up = n20.restart(graceful_stop=False)
            assert isinstance(n20_up, NodeInstance)
            _wait_project_open(n20_up.client(), p20, now_ms - 60_000, now_ms + 60_000)
            self.assertEqual(
                _insights_total(n20_up.client(), p20, "adv7_20ms", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                1.0,
            )

            # Part B: Schema-cap dead-letter CLI filtering aliased erased user while keeping sibling.
            # `p20` already contains `adv7_20ms` (1 slot), so 8191 more names fill the 8192 cap.
            c20_up = n20_up.client()
            c20_up.identify({"anon_id": "a_dlq_erase", "user_id": "u_dlq_erase"}, api_key=k20).raise_for_status()
            idx = 0
            while idx < 8191:
                chunk = [
                    make_event(event_name=f"adv7_d_{idx + j:04d}", distinct_id="u_seed", ts_ms=now_ms + 1000 + idx + j)
                    for j in range(min(256, 8191 - idx))
                ]
                c20_up.ingest({"events": chunk}, api_key=k20).raise_for_status()
                idx += len(chunk)
            _wait_w1_drained(c20_up, timeout=30.0)

            c20_up.ingest(
                {
                    "events": [
                        make_event(event_name="adv7_over_erased", distinct_id="a_dlq_erase", ts_ms=now_ms - 500),
                        make_event(event_name="adv7_over_kept", distinct_id="u_dlq_kept", ts_ms=now_ms - 500),
                    ]
                },
                api_key=k20,
            ).raise_for_status()
            wait_until(
                lambda: c20_up.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 2.0,
                timeout=15.0,
            )

            c20_up.delete_user(p20, "u_dlq_erase").raise_for_status()
            c20_up.admin_flush().raise_for_status()
            n20_up.stop(graceful=True)

            dl_proc = cluster.node_mgr.run_cli(
                ["dead-letters", "--project-id", p20, "--data-dir", str(n20_up.data_dir), "--json"],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(dl_proc.returncode, 0, dl_proc.stderr)
            items = json.loads(dl_proc.stdout)
            names = [it["event"]["event_name"] for it in items]
            self.assertEqual(names, ["adv7_over_kept"])

    @feature("F28", "F29", "F30", "F31")
    @require_milestone("W3")
    def test_adv_08_s18_w3_429_hol_isolation_and_live_pg_owner_migration(self) -> None:
        """Adv-08 (S18): Writer 429 HOL isolation under SIGSTOP + live Postgres restore & project_owner migration."""
        base_pg = skip_if_no_postgres()
        pg_url = _create_isolated_pg_db(self, base_pg)

        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            kek = random_byok_hex()
            common_env = {
                "PULSE_VAULT_KEK": kek,
                "PULSE_OWNER_CACHE_TTL_MS": "60000",
                "PULSE_WRITER_LOG_PROJECT_MAX_BYTES": "2048",
            }
            ing0 = cluster.start_node(
                role="ingest",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
                node_name="ingest-0",
                extra_env=common_env,
            )
            ing1 = cluster.start_node(
                role="ingest",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
                node_name="ingest-1",
                extra_env=common_env,
            )
            writer = cluster.start_node(
                role="writer",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                node_name="writer-0",
                extra_env=common_env,
            )
            query = cluster.start_node(
                role="query",
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                node_name="query-0",
                extra_env=common_env,
            )
            assert isinstance(ing0, NodeInstance)
            assert isinstance(ing1, NodeInstance)
            assert isinstance(writer, NodeInstance)
            assert isinstance(query, NodeInstance)

            cw = writer.client()
            cq = query.client()
            p1, k1 = _create_project(cq, "Adv08 P1")
            p2, k2 = _create_project(cq, "Adv08 P2")
            now_ms = int(time.time() * 1000)

            # Pin P1 -> ingest-0 and P2 -> ingest-1 in live Postgres project_owner
            _psql_exec(
                pg_url,
                f"UPDATE project_owner SET node_name = 'ingest-0', address = '{ing0.base_url}' WHERE project_id = '{p1}';",
            )
            _psql_exec(
                pg_url,
                f"UPDATE project_owner SET node_name = 'ingest-1', address = '{ing1.base_url}' WHERE project_id = '{p2}';",
            )

            # Warm Writer cache and publish baseline P1 & P2 state to S3
            cw.ingest(
                {"events": [make_event(event_name="adv8_p1_base", distinct_id="u1_0", ts_ms=now_ms - 1000)]},
                api_key=k1,
            ).raise_for_status()
            cw.ingest(
                {"events": [make_event(event_name="adv8_p2_base", distinct_id="u2_0", ts_ms=now_ms - 1000)]},
                api_key=k2,
            ).raise_for_status()
            wait_until(
                lambda: (
                    f"published/{p1}/pub_000000000000.json" in s3.list_objects(f"published/{p1}/")
                    and _insights_total(cq, p1, "adv8_p1_base", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0
                    and _insights_total(cq, p2, "adv8_p2_base", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0
                ),
                timeout=25.0,
                desc="baseline P1 and P2 published and queryable",
            )

            # SIGSTOP ingest-0, flood P1 on writer-0 until 429 writer_log_backpressure
            assert ing0.proc is not None
            ing0.proc.send_signal(signal.SIGSTOP)
            self.addCleanup(lambda p=ing0.proc: p.send_signal(signal.SIGCONT) if p.poll() is None else None)

            p1_buffered = 0
            hit_429 = False
            for i in range(25):
                r = cw.ingest(
                    {
                        "events": [
                            make_event(
                                event_name="adv8_p1_buf",
                                distinct_id=f"u1_b_{i}",
                                ts_ms=now_ms + i * 10,
                                props={"pad": _incompressible_str(f"adv8_{i}", 900)},
                            )
                        ]
                    },
                    api_key=k1,
                )
                if r.status == 429:
                    self.assertEqual(r.json().get("code"), "writer_log_backpressure")
                    hit_429 = True
                    break
                r.raise_for_status()
                p1_buffered += 1
                time.sleep(0.03)
            self.assertTrue(hit_429, "P1 must hit 429 writer_log_backpressure on writer-0")

            # While P1 is 429'd and ingest-0 is frozen, P2 on the SAME writer-0 succeeds & reaches query-0!
            cw.ingest(
                {"events": [make_event(event_name="adv8_p2_live", distinct_id="u2_live", ts_ms=now_ms + 500)]},
                api_key=k2,
            ).raise_for_status()
            wait_until(
                lambda: _insights_total(cq, p2, "adv8_p2_live", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0,
                timeout=20.0,
                desc="P2 succeeds without HOL blocking while P1 is 429'd",
            )

            # Kill ingest-0, restore P1 from S3 into fresh ingest-2, migrate project_owner in Postgres,
            # and verify writer-0 drains P1's buffered frames cleanly with 0 loss!
            ing0.proc.send_signal(signal.SIGCONT)
            ing0.kill()
            ing2_dir = cluster.node_mgr.make_temp_data_dir("adv8_ing2_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=ing2_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url},
            )
            self.assertEqual(res.returncode, 0, res.stderr)

            ing2 = cluster.start_node(
                role="ingest",
                data_dir=ing2_dir,
                database_url=pg_url,
                hot_store="local",
                upload_interval_secs=1,
                writer_lease_ttl_secs=3,
                node_name="ingest-2",
                extra_env=common_env,
            )
            assert isinstance(ing2, NodeInstance)
            _psql_exec(
                pg_url,
                f"UPDATE project_owner SET node_name = 'ingest-2', address = '{ing2.base_url}', updated_at_ms = {int(time.time() * 1000)} WHERE project_id = '{p1}';",
            )

            wait_until(
                lambda: _insights_total(cq, p1, "adv8_p1_buf", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == float(p1_buffered),
                timeout=30.0,
                desc="writer-0 drains all buffered P1 frames to migrated owner ingest-2",
            )
            self.assertEqual(
                _insights_total(cq, p1, "adv8_p1_base", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                1.0,
            )
