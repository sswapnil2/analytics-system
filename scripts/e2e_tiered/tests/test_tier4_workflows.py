"""Tier 4 Realistic End-to-End Multi-Feature Workload Scenarios (S01–S18)."""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import PulseHttpClient  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance, free_port  # noqa: E402
from lib.oracle import (  # noqa: E402
    external_postgres_url,
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


def _insights_total(client: PulseHttpClient, project_id: str, event_name: str, *, start_ms: int, end_ms: int) -> float:
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


def _shared_meta_url(cluster: ClusterHarness, label: str) -> str:
    pg = external_postgres_url()
    if pg:
        return pg
    url, _ = _shared_sqlite_meta_url(cluster, label)
    return url


def _shared_sqlite_meta_url(cluster: ClusterHarness, label: str) -> tuple[str, Path]:
    shared_dir = cluster.node_mgr.make_temp_data_dir(f"pulse_{label}_")
    db_path = (shared_dir / "shared_meta.db").resolve()
    return f"sqlite://{db_path}", db_path


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


def _parse_wal_epoch(rel_paths: list[str]) -> int:
    for rel in rel_paths:
        fname = rel.rsplit("/", 1)[-1]
        if fname.startswith("epoch_") and "_seq_" in fname and fname.endswith(".wal"):
            ep_str = fname[len("epoch_") :].split("_seq_", 1)[0]
            if ep_str.isdigit():
                return int(ep_str)
    return 1


class _WebhookSink:
    """Tiny stdlib HTTP server capturing alert webhook deliveries."""

    def __init__(self) -> None:
        self.deliveries: list[dict[str, Any]] = []
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
                    sink.deliveries.append(json.loads(raw.decode("utf-8", errors="replace")))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        self._httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook"

    def start(self) -> _WebhookSink:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5.0)


@tier(4)
class TestTier4RealWorldWorkloads(unittest.TestCase):
    """18 realistic end-to-end multi-feature workload scenarios (S01–S18)."""

    @feature("F01", "F02", "F03")
    @require_milestone("T0a")
    def test_s01_mixed_ingest_byok_corrupt_wal_quarantine_and_restart_recovery(self) -> None:
        """S01: Mixed event + agent trace load with BYOK encryption + corrupt WAL quarantine + restart recovery."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            byok = random_byok_hex()
            node = cluster.start_node(
                role="all",
                hot_store="local",
                byok_master_key=byok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            wal_root = node.data_dir / "wal"
            self.addCleanup(lambda: _chmod_tree(wal_root, 0o755, 0o644))

            client = node.client()
            pid, api_key = _create_project(client, "S01 Mixed BYOK Recovery")
            now_ms = int(time.time() * 1000)

            # Ingest JSON + PLSB events and JSON + PLST agent traces
            client.ingest(
                {"events": make_events(12, event_name="s01_signup", distinct_id_prefix="u_s01", base_ts_ms=now_ms)},
                api_key=api_key,
            ).raise_for_status()
            client.ingest_plsb(
                make_events(8, event_name="s01_checkout", distinct_id_prefix="u_s01", base_ts_ms=now_ms + 2000),
                api_key=api_key,
            ).raise_for_status()
            for i in range(5):
                client.ingest_agent_traces(
                    make_agent_trace(trace_id=f"tr_s01_{i}", start_ts_ms=now_ms + i * 10),
                    api_key=api_key,
                    binary=(i % 2 == 1),
                ).raise_for_status()

            # Verify PLSEAGM2 encryption on local WALs & 0 S3 WAL calls
            wal_files = node.local_files("wal/wal/")
            agent_wals = node.local_files("wal/agent_wal/")
            self.assertGreaterEqual(len(wal_files), 1)
            self.assertGreaterEqual(len(agent_wals), 1)
            for wf in wal_files + agent_wals:
                self.assertTrue(node.read_local_bytes(wf).startswith(b"PLSEAGM2"))
            self.assertEqual(s3.put_count("wal/"), 0)
            self.assertEqual(s3.put_count("agent_wal/"), 0)

            # Transient unwritable WAL dir -> 503 for both events and traces
            _chmod_tree(wal_root, 0o555)
            self.assertEqual(
                client.ingest({"events": [make_event(event_name="s01_signup", distinct_id="u_drop", ts_ms=now_ms)]}, api_key=api_key).status,
                503,
            )
            self.assertEqual(
                client.ingest_agent_traces(make_agent_trace(trace_id="tr_s01_drop", start_ts_ms=now_ms), api_key=api_key).status,
                503,
            )
            _chmod_tree(wal_root, 0o755, 0o644)

            epoch = _parse_wal_epoch(wal_files)
            node.kill()

            # Corrupt one extra WAL frame on disk and restart
            corrupt_path = f"wal/wal/{pid}/bucket_000/epoch_{epoch:010}_seq_000000999998.wal"
            node.write_local_bytes(corrupt_path, b"PWL2" + b"\x00" * 36)

            node2 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                byok_master_key=byok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            wait_until(
                lambda: c2.query(
                    {
                        "project_id": pid,
                        "kind": "insights",
                        "query": {
                            "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
                            "interval": "day",
                            "series": [{"label": "cnt", "event_name": "s01_signup", "metric": "totals"}],
                        },
                    }
                ).status == 200,
                timeout=15.0,
                desc="recovered project opens after lease TTL",
            )
            wait_until(lambda: len(node2.local_files(f"wal/wal_corrupt/{pid}/")) >= 1, timeout=15.0)
            self.assertEqual(s3.put_count("wal_corrupt/"), 0)
            self.assertEqual(_insights_total(c2, pid, "s01_signup", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 12.0)
            self.assertEqual(_insights_total(c2, pid, "s01_checkout", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 8.0)
            traces = c2.list_agent_traces(pid).raise_for_status().json()["traces"]
            self.assertEqual(len(traces), 5)

    @feature("F01", "F02", "F04", "F05")
    @require_milestone("T0b")
    def test_s02_hot_store_upgrade_object_sigkill_refusal_clean_stop_local_upgrade(self) -> None:
        """S02: Hot-store upgrade (object -> SIGKILL refusal -> clean stop -> local upgrade -> full query preservation)."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="object", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S02 Upgrade")
            now_ms = int(time.time() * 1000)

            client.engage({"distinct_id": "u_s02", "$set": {"plan": "pro"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": make_events(50, event_name="s02_ev", distinct_id_prefix="u_s02", base_ts_ms=now_ms)}, api_key=api_key).raise_for_status()
            for i in range(10):
                client.ingest_agent_traces(make_agent_trace(trace_id=f"tr_s02_{i}", start_ts_ms=now_ms + i), api_key=api_key).raise_for_status()
            node.kill()

            # Switching directly to local after SIGKILL must be refused
            rc, _, _ = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)

            # Boot object once, stop cleanly, then upgrade to local
            node_obj = cluster.start_node(data_dir=node.data_dir, hot_store="object", writer_lease_ttl_secs=3)
            assert isinstance(node_obj, NodeInstance)
            node_obj.stop(graceful=True)

            node_loc = cluster.start_node(data_dir=node.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node_loc, NodeInstance)
            cl = node_loc.client()
            self.assertEqual(_insights_total(cl, pid, "s02_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 50.0)
            self.assertEqual(len(cl.list_agent_traces(pid).raise_for_status().json()["traces"]), 10)
            self.assertEqual(cl.get_user(pid, "u_s02").raise_for_status().json()["profile"]["props"]["plan"], "pro")

    @feature("F04", "F05", "F06")
    @require_milestone("T0b")
    def test_s03_local_identity_profiles_compaction_sigkill_multipart_fsync(self) -> None:
        """S03: Local identity/ & profiles/ high-churn compaction + immediate SIGKILL crash durability."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S03 High Churn Fsync")
            s3.reset_counts()

            for i in range(100):
                client.identify(
                    {
                        "anon_id": f"anon_s03_{i:03d}",
                        "user_id": f"user_s03_{i:03d}",
                        "user_properties": {"score": i * 10, "payload": "P" * 512},
                    },
                    api_key=api_key,
                ).raise_for_status()
            self.assertEqual(s3.put_count("identity/"), 0)
            self.assertEqual(s3.put_count("profiles/"), 0)
            node.kill()

            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            users = c2.list_users(pid, limit=200).raise_for_status().json()
            self.assertEqual(len(users), 100)
            self.assertEqual(c2.get_user(pid, "user_s03_099").raise_for_status().json()["profile"]["props"]["score"], 990)

    @feature("F07", "F08", "F14")
    @require_milestone("T2")
    def test_s04_continuous_multi_cycle_publishing_dense_seq_hwm_byok(self) -> None:
        """S04: Continuous multi-cycle publishing with dense publish_seq, T_id >= T_prof >= T_freeze HWM snapshots & BYOK."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            byok = random_byok_hex()
            node = cluster.start_node(
                role="all",
                hot_store="local",
                byok_master_key=byok,
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S04 Multi Publish")
            now_ms = int(time.time() * 1000)

            for cycle in range(3):
                client.engage({"distinct_id": f"u_s04_{cycle}", "$set": {"cycle": cycle}}, api_key=api_key).raise_for_status()
                client.ingest(
                    {"events": make_events(10, event_name="s04_ev", distinct_id_prefix=f"u_s04_c{cycle}", base_ts_ms=now_ms + cycle * 1000)},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()
                wait_until(
                    lambda seq=cycle: f"published/{pid}/pub_{seq:012}.json" in s3.list_objects(f"published/{pid}/"),
                    timeout=20.0,
                )

            pub_objs = sorted(k for k in s3.list_objects(f"published/{pid}/") if k.endswith(".json"))
            self.assertEqual(
                [f"published/{pid}/pub_{i:012}.json" for i in range(3)],
                [k for k in pub_objs if "/pub_" in k],
            )
            for k in pub_objs:
                self.assertTrue(s3.get_object(k).startswith(b"PLSEAGM2"))

    @feature("F05", "F08", "F10", "F11")
    @require_milestone("T1")
    def test_s05_volume_wipe_refusal_restore_from_published_and_post_restore_hwm(self) -> None:
        """S05: Volume wipe -> startup mode-marker refusal -> restore --from-published -> post-restore ingestion (uid >= HWM)."""
        pg_url = skip_if_no_postgres()
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                database_url=pg_url,
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S05 Volume Wipe Restore")
            now_ms = int(time.time() * 1000)

            client.engage({"distinct_id": "u_pub_1", "$set": {"tier": "pro"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": make_events(10, event_name="s05_pub", distinct_id_prefix="u_s05", base_ts_ms=now_ms)}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            # Unpublished post-publish event before volume loss
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            client.ingest({"events": [make_event(event_name="s05_unpub", distinct_id="u_unpub", ts_ms=now_ms + 500)]}, api_key=api_key).raise_for_status()
            node.kill()
            s3.clear_faults()

            # Simulate partial volume loss (wipe identity/ and profiles/ while mode marker remains)
            for rel in list(node.local_files()):
                if "identity/" in rel or "profiles/" in rel:
                    node.rm_local(rel)
            rc, _, _ = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                database_url=pg_url,
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)

            restored_dir = cluster.node_mgr.make_temp_data_dir("s05_restored_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url},
            )
            self.assertEqual(res.returncode, 0, res.stderr)

            node2 = cluster.start_node(
                data_dir=restored_dir,
                hot_store="local",
                database_url=pg_url,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(_insights_total(c2, pid, "s05_pub", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 10.0)
            self.assertEqual(_insights_total(c2, pid, "s05_unpub", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 0.0)
            new_id = c2.identify({"anon_id": "a_new", "user_id": "u_new"}, api_key=api_key).raise_for_status().json()
            self.assertGreaterEqual(int(new_id["canonical_uid"]), 10)

    @feature("F10", "F11", "F12")
    @require_milestone("T1")
    def test_s06_erasure_reconciliation_lt_and_ge_hwm_backdated_import_audit_log(self) -> None:
        """S06: Pre-restore < HWM erasure + >= HWM create/erase + backdated import post-restore + audit log preservation."""
        pg_url = skip_if_no_postgres()
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                database_url=pg_url,
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S06 Erasure Audit")
            now_ms = int(time.time() * 1000)

            client.engage({"distinct_id": "u_lt_hwm", "$set": {"tier": "gold"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": [make_event(event_name="s06_ev", distinct_id="u_lt_hwm", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            client.engage({"distinct_id": "u_ge_hwm", "$set": {"tier": "silver"}}, api_key=api_key).raise_for_status()
            client.delete_user(pid, "u_lt_hwm").raise_for_status()
            client.delete_user(pid, "u_ge_hwm").raise_for_status()
            node.kill()
            s3.clear_faults()

            restored_dir = cluster.node_mgr.make_temp_data_dir("s06_restored_")
            cluster.node_mgr.run_cli(["restore", "--from-published"], data_dir=restored_dir, env={**s3.env_vars(), "DATABASE_URL": pg_url})
            node2 = cluster.start_node(data_dir=restored_dir, hot_store="local", database_url=pg_url, writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()

            self.assertEqual(len(c2.list_deletions(pid).raise_for_status().json()), 2)
            self.assertIsNone(c2.get_user(pid, "u_lt_hwm").raise_for_status().json().get("profile"))
            self.assertEqual(_insights_total(c2, pid, "s06_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 0.0)

            # New user reusing >= HWM slot with backdated import is counted
            c2.import_events(pid, {"events": [make_event(event_name="s06_import", distinct_id="u_new_post", ts_ms=now_ms - 5000)]}).raise_for_status()
            self.assertEqual(_insights_total(c2, pid, "s06_import", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 1.0)

    @feature("F12", "F17")
    @require_milestone("T2")
    def test_s07_aliased_dpdp_erasure_across_owner_and_remote_query_node_plus_reengage(self) -> None:
        """S07: Aliased user DPDP erasure across owning node + remote --role query node + /users, /export, /v1/mcp + re-engage."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            meta_url = _shared_meta_url(cluster, "s07")
            owner = cluster.start_node(
                role="all",
                data_dir=cluster.node_mgr.make_temp_data_dir("s07_owner_"),
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(owner, NodeInstance)
            ca = owner.client()
            pid, api_key = _create_project(ca, "S07 Aliased Erasure")
            now_ms = int(time.time() * 1000)

            ca.identify({"anon_id": "anon_s07", "user_id": "usr_s07", "user_properties": {"plan": "pro"}}, api_key=api_key).raise_for_status()
            ca.ingest({"events": [make_event(event_name="s07_ev", distinct_id="anon_s07", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            ca.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            qnode = cluster.start_node(
                role="query",
                data_dir=cluster.node_mgr.make_temp_data_dir("s07_query_"),
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
            )
            assert isinstance(qnode, NodeInstance)
            cq = qnode.client()
            wait_until(lambda: len(cq.list_users(pid).raise_for_status().json()) == 1, timeout=15.0)

            # Freeze publish, erase usr_s07 on owner, verify immediate hide on both nodes
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            ca.delete_user(pid, "usr_s07").raise_for_status()

            wait_until(lambda: cq.list_users(pid).raise_for_status().json() == [], timeout=15.0)
            for cl in (ca, cq):
                self.assertEqual(cl.list_users(pid).raise_for_status().json(), [])
                self.assertIsNone(cl.get_user(pid, "anon_s07").raise_for_status().json().get("profile"))
                self.assertEqual(cl.export_events(pid, format="json").raise_for_status().json()["count"], 0)
                self.assertEqual(_insights_total(cl, pid, "s07_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 0.0)

            # Re-engage after erasure makes new profile visible
            time.sleep(0.05)
            ca.engage({"distinct_id": "usr_s07", "$set": {"plan": "starter"}}, api_key=api_key).raise_for_status()
            self.assertEqual(ca.get_user(pid, "usr_s07").raise_for_status().json()["profile"]["props"]["plan"], "starter")

    @feature("F08", "F09", "F10")
    @require_milestone("T1")
    def test_s08_t1_snapshot_benchmark_counting_puts_and_restore_completeness(self) -> None:
        """S08: T1 snapshot benchmark verification (Counting S3 PUTs/hr + restore completeness check into empty dir)."""
        pg_url = skip_if_no_postgres()
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                database_url=pg_url,
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S08 Snapshot Bench")
            now_ms = int(time.time() * 1000)

            s3.reset_counts()
            evs = make_events(200, event_name="s08_ev", distinct_id_prefix="u_s08", base_ts_ms=now_ms, unique_users=200)
            client.ingest({"events": evs}, api_key=api_key).raise_for_status()
            for u in range(0, 200, 10):
                client.engage({"distinct_id": f"u_s08_{u:04d}", "$set": {"cohort": u}}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=25.0)

            pub_puts = s3.put_count("published/")
            self.assertGreaterEqual(pub_puts, 1)
            self.assertLess(pub_puts, 100)
            node.stop(graceful=True)

            empty_dir = cluster.node_mgr.make_temp_data_dir("s08_restore_")
            res = cluster.node_mgr.run_cli(["restore", "--from-published"], data_dir=empty_dir, env={**s3.env_vars(), "DATABASE_URL": pg_url})
            self.assertEqual(res.returncode, 0, res.stderr)

            node2 = cluster.start_node(data_dir=empty_dir, hot_store="local", database_url=pg_url, writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(_insights_total(c2, pid, "s08_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 200.0)
            self.assertEqual(len(c2.list_users(pid, limit=300).raise_for_status().json()), 20)

    @feature("F07", "F14", "F15", "F20")
    @require_milestone("T3")
    def test_s09_sparse_epoch_deposed_writer_fencing_and_step6_self_crash_recovery(self) -> None:
        """S09: Sparse-epoch deposed writer fencing & step-6 self-crash recovery."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="local", upload_interval_secs=2, writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S09 Fencing & Step6")
            now_ms = int(time.time() * 1000)

            client.ingest({"events": [make_event(event_name="s09_ev", distinct_id="u1", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            # Part A: step-6 crash right after pub_1 S3 PUT
            killed = threading.Event()
            s3.on_operation(
                lambda op, key: (killed.set(), node.proc.kill() if node.proc else None)
                if op == "PUT" and key == f"published/{pid}/pub_000000000001.json" and not killed.is_set()
                else None
            )
            client.ingest({"events": [make_event(event_name="s09_ev", distinct_id="u2", ts_ms=now_ms + 10)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(killed.is_set, timeout=20.0)
            s3.clear_callbacks()
            node.stop(graceful=False)

            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", upload_interval_secs=2, writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            self.assertEqual(_insights_total(node2.client(), pid, "s09_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 2.0)
            node2.stop(graceful=True)

            # Part B: Depose writer by advancing S3 pub_2 with higher writer_generation and lower source_local_epoch
            pub1 = json.loads(s3.get_object(f"published/{pid}/pub_000000000001.json").decode("utf-8"))
            pub2 = dict(pub1)
            pub2["publish_seq"] = int(pub1["publish_seq"]) + 1
            pub2["writer_generation"] = int(pub1["writer_generation"]) + 1
            pub2["source_local_epoch"] = 1
            s3.put_object_raw(f"published/{pid}/pub_000000000002.json", json.dumps(pub2).encode("utf-8"))

            fenced = cluster.node_mgr.start_node(
                data_dir=node2.data_dir,
                mock_s3=s3,
                hot_store="local",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                wait_ready=True,
            )
            if isinstance(fenced, NodeInstance):
                self.assertEqual(
                    fenced.client().ingest({"events": [make_event(event_name="s09_ev", distinct_id="u3", ts_ms=now_ms + 20)]}, api_key=api_key).status,
                    503,
                )

    @feature("F16", "F17", "F26")
    @require_milestone("T4")
    def test_s10_remote_query_node_2_interval_grace_and_pin_protection(self) -> None:
        """S10: Remote query node + 2-publish-interval S3 segment deletion grace + active query pin protection across restarts."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            meta_url = _shared_meta_url(cluster, "s10")
            owner = cluster.start_node(
                role="all",
                data_dir=cluster.node_mgr.make_temp_data_dir("s10_owner_"),
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"},
            )
            assert isinstance(owner, NodeInstance)
            ca = owner.client()
            pid, api_key = _create_project(ca, "S10 Grace Pin")
            closed_ts = int(time.time() * 1000) - 3 * 86_400_000

            for i in range(3):
                ca.ingest({"events": [make_event(event_name="s10_ev", distinct_id="u_s10", ts_ms=closed_ts + i * 100)]}, api_key=api_key).raise_for_status()
                ca.admin_flush().raise_for_status()
                wait_until(lambda seq=i: f"published/{pid}/pub_{seq:012}.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            old_segs = set(s3.list_objects(f"segments/{pid}/"))
            qnode = cluster.start_node(
                role="query",
                data_dir=cluster.node_mgr.make_temp_data_dir("s10_query_"),
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
            )
            assert isinstance(qnode, NodeInstance)
            cq = qnode.client()

            # Restart owner while cold merge / grace window progresses
            owner = owner.restart(graceful_stop=True)
            assert isinstance(owner, NodeInstance)
            wait_until(lambda: not old_segs.issubset(set(s3.list_objects(f"segments/{pid}/"))), timeout=35.0)
            self.assertEqual(_insights_total(cq, pid, "s10_ev", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000), 3.0)

    @feature("F18", "F19", "F21")
    @require_milestone("T3")
    def test_s11_local_segment_tier_7step_upload_64mib_cap_closed_date_idle_zero_s3(self) -> None:
        """S11: High-volume local segment tier + 7-step upload cycle + >merge cap split + closed-date trickle + 429 backpressure + idle 0 S3 calls."""
        def _incompressible_pad(tag: str) -> str:
            raw = b"".join(hashlib.sha256(f"{tag}:{k}".encode("utf-8")).digest() for k in range(16))
            return base64.b85encode(raw).decode("ascii")

        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=3,
                writer_lease_ttl_secs=3,
                extra_env={
                    "PULSE_MERGE_PART_MAX_BYTES": str(64 * 1024),
                    "PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": str(64 * 1024),
                    "PULSE_HOT_SEGMENTS_MAX_BYTES": str(128 * 1024),
                },
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S11 Upload & Idle")
            now_ms = int(time.time() * 1000)

            # Small closed-date late event (<= 4 MiB) stays local past short 3s active timer
            yesterday_ms = now_ms - 86_400_000
            client.ingest({"events": [make_event(event_name="s11_late", distinct_id="u_late", ts_ms=yesterday_ms)]}, api_key=api_key).raise_for_status()

            # Ingest incompressible payloads across multiple flushes exceeding 64 KiB merge cap -> multi-segment split
            hot_count = 0
            for b in range(6):
                evs = [
                    make_event(
                        event_name="s11_hot",
                        distinct_id=f"u_hot_{b}_{i}",
                        ts_ms=now_ms + b * 100 + i,
                        props={"pad": _incompressible_pad(f"s11a:{b}:{i}")},
                    )
                    for i in range(60)
                ]
                client.ingest({"events": evs}, api_key=api_key).raise_for_status()
                client.admin_flush().raise_for_status()
                hot_count += len(evs)

            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=25.0)
            wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 2, timeout=20.0, desc="multi-segment split under merge cap")
            self.assertEqual(_insights_total(client, pid, "s11_hot", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), float(hot_count))
            self.assertEqual(_insights_total(client, pid, "s11_late", start_ms=yesterday_ms - 60_000, end_ms=yesterday_ms + 60_000), 1.0)

            # Trigger F21 429 backpressure under S3 outage with incompressible segments, then recover with zero loss
            s3.set_fault(fail_prefixes=["segments/", "published/"], status=503)
            bp_acked = 0
            hit_429 = False
            for b in range(40):
                evs = [
                    make_event(
                        event_name="s11_hot",
                        distinct_id=f"u_bp_{b}_{i}",
                        ts_ms=now_ms + 10_000 + b * 100 + i,
                        props={"pad": _incompressible_pad(f"s11bp:{b}:{i}")},
                    )
                    for i in range(60)
                ]
                r = client.ingest({"events": evs}, api_key=api_key)
                if r.status == 429:
                    self.assertIsNotNone(r.header("retry-after"))
                    hit_429 = True
                    break
                r.raise_for_status()
                bp_acked += int(r.json()["accepted"])
                client.admin_flush().raise_for_status()
            self.assertTrue(hit_429, "Expected HTTP 429 once hot/segments/ exceeded 2x byte bound")

            s3.clear_faults()
            wait_until(
                lambda: client.ingest({"events": [make_event(event_name="s11_hot", distinct_id="u_s11_rec", ts_ms=now_ms + 50_000)]}, api_key=api_key).status == 200,
                timeout=25.0,
                desc="ingest recovers to 200 after S3 fault cleared",
            )
            self.assertEqual(
                _insights_total(client, pid, "s11_hot", start_ms=now_ms - 60_000, end_ms=now_ms + 120_000),
                float(hot_count + bp_acked + 1),
            )

            # Verify idle zero S3 calls after upload settles
            time.sleep(3.5)
            s3.reset_counts()
            time.sleep(4.0)
            self.assertEqual(s3.total_calls(), 0, f"Idle project made S3 calls: {s3.get_counts()}")

    @feature("F15", "F19", "F20")
    @require_milestone("T3")
    def test_s12_fault_injection_crash_matrix_upload_steps_and_s3_down_dedup_guard(self) -> None:
        """S12: Fault-injection matrix crashing across upload boundaries + S3-down restart dedup error guard."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="local", upload_interval_secs=2, writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S12 Crash Matrix")
            now_ms = int(time.time() * 1000)

            # Crash after step 4 (segments/ PUT) then recover; crash after step 6 (published/ PUT) then recover
            for step_idx, prefix in enumerate((f"segments/{pid}/", f"published/{pid}/")):
                killed = threading.Event()
                s3.on_operation(
                    lambda op, key, p=prefix, ev=killed: (ev.set(), node.proc.kill() if node.proc else None)
                    if op == "PUT" and key.startswith(p) and not ev.is_set()
                    else None
                )
                client.ingest(
                    {"events": make_events(5, event_name="s12_ev", distinct_id_prefix=f"u_s12_{step_idx}", base_ts_ms=now_ms + step_idx * 1000, seed_prefix=f"s12_{step_idx}")},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()
                wait_until(killed.is_set, timeout=20.0)
                s3.clear_callbacks()
                node.stop(graceful=False)
                node = cluster.start_node(data_dir=node.data_dir, hot_store="local", upload_interval_secs=2, writer_lease_ttl_secs=3)
                assert isinstance(node, NodeInstance)
                client = node.client()

            wait_until(lambda: _insights_total(client, pid, "s12_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 10.0, timeout=20.0)

            # F20: S3 unreachable during restart dedup seeding must refuse project open rather than accepting duplicates
            node.stop(graceful=True)
            s3.set_fault(unreachable=True)
            node_down = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                wait_ready=False,
            )
            assert isinstance(node_down, NodeInstance)
            time.sleep(2.0)
            if node_down.proc is not None and node_down.proc.poll() is None:
                r_dup = node_down.client().ingest(
                    {"events": make_events(5, event_name="s12_ev", distinct_id_prefix="u_s12_0", base_ts_ms=now_ms, seed_prefix="s12_0")},
                    api_key=api_key,
                )
                self.assertGreaterEqual(r_dup.status, 500)
            s3.clear_faults()
            node_down.stop(graceful=False)

            node_rec = cluster.start_node(data_dir=node.data_dir, hot_store="local", upload_interval_secs=2, writer_lease_ttl_secs=3)
            assert isinstance(node_rec, NodeInstance)
            self.assertEqual(_insights_total(node_rec.client(), pid, "s12_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 10.0)

    @feature("F19", "F21")
    @require_milestone("T3")
    def test_s13_s3_outage_sustained_ingest_1x_2x_429_recovery_zero_loss(self) -> None:
        """S13: S3 outage under sustained ingest -> hot/segments/ 1x/2x 429 backpressure -> S3 recovery -> 0 lost events."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=300,
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "65536", "PULSE_HOT_SEGMENTS_MAX_BYTES": "131072"},
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S13 S3 Outage 429")
            now_ms = int(time.time() * 1000)

            s3.set_fault(fail_prefixes=["segments/", "published/"], status=503)
            acked = 0
            hit_429 = False
            for i in range(60):
                r = client.ingest(
                    {"events": make_events(40, event_name="s13_ev", distinct_id_prefix=f"u_s13_{i}", base_ts_ms=now_ms + i * 10, props={"pad": "B" * 512})},
                    api_key=api_key,
                )
                if r.status == 429:
                    self.assertIsNotNone(r.header("retry-after"))
                    hit_429 = True
                    break
                r.raise_for_status()
                acked += int(r.json()["accepted"])
                client.admin_flush().raise_for_status()
            self.assertTrue(hit_429)

            s3.clear_faults()
            wait_until(
                lambda: client.ingest({"events": [make_event(event_name="s13_ev", distinct_id="u_final", ts_ms=now_ms + 5000)]}, api_key=api_key).status == 200,
                timeout=25.0,
            )
            self.assertEqual(_insights_total(client, pid, "s13_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), float(acked + 1))

    @feature("F22", "F23", "F24")
    @require_milestone("W1")
    def test_s14_w1_async_ingest_pii_masking_retries_sigkill_dead_letter_erasure(self) -> None:
        """S14: W1 async event ingest + Lexicon PII masking + duplicate retries + SIGKILL flush-bound + dead-letter CLI w/ erasure."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S14 W1 Async")
            now_ms = int(time.time() * 1000)

            client.update_lexicon_property(pid, "secret_pan", {"pii_masked": True}).raise_for_status()
            evs = make_events(15, event_name="s14_ev", distinct_id_prefix="u_s14", base_ts_ms=now_ms, props={"secret_pan": "ABCDE1234F"}, seed_prefix="s14")
            r1 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            r2 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r1["deduped"], 0)
            self.assertEqual(r2["deduped"], 0)

            wait_until(lambda: client.parsed_metrics().sum_by_name("pulse_ingest_events_deduped_total") >= 15.0, timeout=15.0)
            self.assertEqual(_insights_total(client, pid, "s14_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 15.0)

            # Verify PII masked in export
            exported = client.export_events(pid, format="json").raise_for_status().json()
            for row in exported["events"]:
                self.assertNotIn("ABCDE1234F", json.dumps(row))

            # SIGKILL recovery preserves flushed WAL events without duplicates
            node.kill()
            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(_insights_total(c2, pid, "s14_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 15.0)

            # F23: Trigger per-project schema-cap dead-lettering (8192 event dictionary cap), inspect CLI, then DPDP-erase user
            c2.identify({"anon_id": "a_s14_dl", "user_id": "u_s14_dl"}, api_key=api_key).raise_for_status()
            idx = 0
            while idx < 8192:
                chunk = [
                    make_event(event_name=f"s14_dict_{idx + j:04d}", distinct_id="u_s14_dict", ts_ms=now_ms + 1000 + idx + j)
                    for j in range(min(256, 8192 - idx))
                ]
                c2.ingest({"events": chunk}, api_key=api_key).raise_for_status()
                idx += len(chunk)
            wait_until(lambda: c2.parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") == 0.0, timeout=30.0)

            c2.ingest(
                {"events": [make_event(event_name="s14_overflow_8193", distinct_id="u_s14_dl", ts_ms=now_ms + 20_000)]},
                api_key=api_key,
            ).raise_for_status()
            wait_until(lambda: c2.parsed_metrics().sum_by_name("pulse_ingest_dead_lettered_events_total") >= 1.0, timeout=15.0)
            wait_until(lambda: any("wal_dead/" in p for p in node2.local_files()), timeout=15.0)

            caps = cluster.node_mgr.probe_capabilities()
            dl_cmd = next((c for c in ("dead-letters", "dead-letter", "wal-dead", "inspect-dead-letters") if caps.has_subcommand(c)), None)
            self.assertIsNotNone(dl_cmd, f"Expected dead-letter CLI subcommand in {sorted(caps.subcommands)}")
            assert dl_cmd is not None

            node2.stop(graceful=True)
            cli_before = cluster.node_mgr.run_cli(
                [dl_cmd, "--project-id", pid, "--data-dir", str(node2.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(cli_before.returncode, 0, cli_before.stderr)
            self.assertIn("s14_overflow_8193", cli_before.stdout)
            self.assertIn("u_s14_dl", cli_before.stdout)

            node3 = cluster.start_node(data_dir=node2.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node3, NodeInstance)
            node3.client().delete_user(pid, "u_s14_dl").raise_for_status()
            node3.stop(graceful=True)

            cli_after = cluster.node_mgr.run_cli(
                [dl_cmd, "--project-id", pid, "--data-dir", str(node3.data_dir)],
                env=cluster.mock_s3.env_vars(),
            )
            self.assertEqual(cli_after.returncode, 0, cli_after.stderr)
            self.assertNotIn("s14_overflow_8193", cli_after.stdout)
            self.assertNotIn("u_s14_dl", cli_after.stdout)

    @feature("F04", "F22", "F25")
    @require_milestone("W2")
    def test_s15_w2_sync_profile_identify_engage_durability_and_error_propagation(self) -> None:
        """S15: W2 synchronous profile /v1/identify & /v1/engage durability + error propagation on storage failure."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            prof_root = node.data_dir / "profiles"
            wal_prof_root = node.data_dir / "wal" / "profiles"
            self.addCleanup(lambda: (_chmod_tree(prof_root, 0o755, 0o644), _chmod_tree(wal_prof_root, 0o755, 0o644)))

            client = node.client()
            pid, api_key = _create_project(client, "S15 W2 Sync Profile")

            id_res = client.identify(
                {"anon_id": "a_s15", "user_id": "u_s15", "user_properties": {"tier": "gold"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertGreater(int(id_res["canonical_uid"]), 0)
            self.assertEqual(client.get_user(pid, "u_s15").raise_for_status().json()["profile"]["props"]["tier"], "gold")

            node.kill()
            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(c2.get_user(pid, "u_s15").raise_for_status().json()["profile"]["props"]["tier"], "gold")

            target_prof = prof_root if (node2.data_dir / "profiles").exists() else (node2.data_dir / "wal" / "profiles")
            _chmod_tree(target_prof, 0o555, 0o444)
            self.assertIn(c2.identify({"anon_id": "a_err", "user_id": "u_err"}, api_key=api_key).status, (500, 503))
            self.assertIn(c2.engage({"distinct_id": "u_s15", "$set": {"tier": "plat"}}, api_key=api_key).status, (500, 503))
            _chmod_tree(target_prof, 0o755, 0o644)

    @feature("F16", "F26", "F27")
    @require_milestone("T4")
    def test_s16_t4_cold_s3_merge_local_replay_agent_idle_zero_s3_physical_erasure(self) -> None:
        """S16: T4 cold S3 merge across multi-day partitions + local hot/replay/ & agent_segments/ + 0 idle S3 + physical agent erasure."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"},
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "S16 T4 Cold & Agent/Replay")
            now_ms = int(time.time() * 1000)

            pub_seq = 0
            for day_off in (3, 2, 1):
                ts = now_ms - day_off * 86_400_000
                for c in range(2):
                    client.ingest({"events": [make_event(event_name="s16_cold", distinct_id="u_s16", ts_ms=ts + c * 100)]}, api_key=api_key).raise_for_status()
                    client.admin_flush().raise_for_status()
                    target_pub = f"published/{pid}/pub_{pub_seq:012}.json"
                    wait_until(lambda k=target_pub: k in s3.list_objects(f"published/{pid}/"), timeout=20.0)
                    pub_seq += 1

            initial_segs = set(s3.list_objects(f"segments/{pid}/"))
            self.assertGreaterEqual(len(initial_segs), 6, f"Expected >=6 segments across 3 closed days, got {initial_segs}")

            # Verify cold S3 merge consolidates closed-day segments (<= 1 per active bucket per closed day = <= 3) after grace
            wait_until(
                lambda: len(s3.list_objects(f"segments/{pid}/")) <= 3
                and not initial_segs.issubset(set(s3.list_objects(f"segments/{pid}/"))),
                timeout=35.0,
                desc="cold merge consolidated closed-day S3 segments",
            )
            self.assertEqual(
                _insights_total(client, pid, "s16_cold", start_ms=now_ms - 4 * 86_400_000, end_ms=now_ms + 60_000),
                6.0,
            )

            client.replay_chunk(make_replay_chunk(session_id="rs_s16", project_id=pid, chunk_start_ts_ms=now_ms), api_key=api_key, binary=True).raise_for_status()
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_s16_erase", start_ts_ms=now_ms), api_key=api_key, binary=True).raise_for_status()
            client.erase_agent(pid, level="trace", target_id="tr_s16_erase", expires_at=now_ms + 1500).raise_for_status()

            wait_until(lambda: any(k.startswith(f"replays/{pid}/") for k in s3.list_objects("replays/")), timeout=20.0)
            wait_until(lambda: any(k.startswith(f"agent_segments/{pid}/") for k in s3.list_objects("agent_segments/")), timeout=20.0)
            time.sleep(2.5)
            client.admin_flush().raise_for_status()
            self.assertEqual(client.get_agent_trace(pid, "tr_s16_erase").status, 404)
            self.assertEqual(client.get_replay(pid, "rs_s16", byte_range="bytes=0-15").status, 206)

            time.sleep(2.5)
            s3.reset_counts()
            time.sleep(4.0)
            self.assertEqual(s3.total_calls(), 0, f"Expected 0 idle S3 calls, got {s3.get_counts()}")

    @feature("F28", "F29", "F30", "F31")
    @require_milestone("W3")
    def test_s17_multinode_w3_placement_single_owner_alert_and_forwarding(self) -> None:
        """S17: Multi-node W3 cluster (2 writers + 2 storage): least-loaded placement, single-owner alert firing, cross-node forwarding."""
        sink = _WebhookSink().start()
        self.addCleanup(sink.stop)
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url, db_path = _shared_sqlite_meta_url(cluster, "s17")
            topo = cluster.start_w3_cluster(
                num_writers=2,
                num_storage=2,
                database_url=meta_url,
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_ALERT_EVAL_INTERVAL_SECS": "1"},
            )
            s0, s1 = topo["storages"]
            w0, w1 = topo["writers"]
            sc = s0.client()

            projects = [_create_project(sc, f"S17_P{i}") for i in range(4)]
            p1, k1 = projects[0]
            rows = cluster.node_mgr.sqlite_query(
                "SELECT node_name, COUNT(*) AS cnt FROM project_owner GROUP BY node_name",
                db_path=db_path,
            )
            counts = {r["node_name"]: int(r["cnt"]) for r in rows}
            self.assertEqual(sum(counts.values()), 4)
            self.assertEqual(counts.get("storage-0", 0), 2)
            self.assertEqual(counts.get("storage-1", 0), 2)

            sc.create_alert(p1, {"name": "s17_spike", "event_name": "s17_alert_ev", "operator": "gte", "threshold": 3.0, "webhook_url": sink.url}).raise_for_status()
            now_ms = int(time.time() * 1000)

            # Route through both writers
            w0.client().identify({"anon_id": "a_s17", "user_id": "u_s17", "user_properties": {"plan": "pro"}}, api_key=k1).raise_for_status()
            w1.client().ingest({"events": make_events(5, event_name="s17_alert_ev", distinct_id_prefix="u_s17", base_ts_ms=now_ms)}, api_key=k1).raise_for_status()
            w1.client().ingest_agent_traces(make_agent_trace(trace_id="tr_s17", start_ts_ms=now_ms), api_key=k1).raise_for_status()

            wait_until(lambda: _insights_total(w0.client(), p1, "s17_alert_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 5.0, timeout=20.0)
            wait_until(lambda: w0.client().get_agent_trace(p1, "tr_s17").status == 200, timeout=20.0)
            self.assertEqual(w1.client().get_user(p1, "u_s17").raise_for_status().json()["profile"]["props"]["plan"], "pro")

            wait_until(lambda: len(sink.deliveries) >= 1, timeout=25.0, desc="alert webhook fired")
            time.sleep(2.0)
            self.assertEqual(len(sink.deliveries), 1, f"Expected single-owner alert delivery, got {len(sink.deliveries)}")

    @feature("F28", "F29", "F30", "F31")
    @require_milestone("W3")
    def test_s18_multinode_w3_resilience_sigkill_resend_no_hol_503_and_owner_migration(self) -> None:
        """S18: Multi-node W3 resilience: writer SIGKILL mid-forward, flush-purged resend dedup, no HOL on 429, owner crash 503 vs async buffer, owner migration."""
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url, db_path = _shared_sqlite_meta_url(cluster, "s18")
            topo = cluster.start_w3_cluster(num_writers=2, num_storage=2, database_url=meta_url, writer_lease_ttl_secs=3)
            s0, s1 = topo["storages"]
            w0, w1 = topo["writers"]
            sc = s0.client()

            p1, k1 = _create_project(sc, "S18_P1")
            p2, k2 = _create_project(sc, "S18_P2")
            now_ms = int(time.time() * 1000)

            # Ingest on W0 and SIGKILL mid-forward, then restart W0
            evs = make_events(60, event_name="s18_ev", distinct_id_prefix="u_s18", base_ts_ms=now_ms, seed_prefix="s18")
            w0.client().ingest({"events": evs}, api_key=k1).raise_for_status()
            w0.kill()
            w0_up = w0.restart(graceful_stop=False)
            assert isinstance(w0_up, NodeInstance)
            wait_until(lambda: _insights_total(sc, p1, "s18_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 60.0, timeout=20.0)

            # Stop owner of P1 -> sync/query return 503 while async /v1/ingest buffers 200; P2 on other_s stays 200
            rows1 = cluster.node_mgr.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p1,), db_path=db_path)
            rows2 = cluster.node_mgr.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p2,), db_path=db_path)
            o1_name = rows1[0]["node_name"]
            owner_p1 = s0 if o1_name == "storage-0" else s1
            other_s = s1 if owner_p1 is s0 else s0
            if rows2[0]["node_name"] == o1_name:
                other_name = "storage-1" if o1_name == "storage-0" else "storage-0"
                cluster.node_mgr.sqlite_query(
                    "UPDATE project_owner SET node_name = ?, address = ? WHERE project_id = ?",
                    (other_name, other_s.base_url, p2),
                    db_path=db_path,
                    readonly=False,
                )

            owner_p1.stop(graceful=True)
            self.assertEqual(w1.client().identify({"anon_id": "a_s18_down", "user_id": "u_s18_down"}, api_key=k1).status, 503)
            self.assertEqual(w1.client().ingest({"events": make_events(10, event_name="s18_buf", distinct_id_prefix="u_buf", base_ts_ms=now_ms)}, api_key=k1).status, 200)
            self.assertEqual(w1.client().ingest({"events": make_events(5, event_name="s18_p2", distinct_id_prefix="u_p2", base_ts_ms=now_ms)}, api_key=k2).status, 200)
            wait_until(lambda: _insights_total(other_s.client(), p2, "s18_p2", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 5.0, timeout=15.0)

            owner_p1_up = owner_p1.restart(graceful_stop=True)
            assert isinstance(owner_p1_up, NodeInstance)
            wait_until(lambda: _insights_total(owner_p1_up.client(), p1, "s18_buf", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 10.0, timeout=20.0)
