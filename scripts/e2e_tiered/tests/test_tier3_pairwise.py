"""Tier 3 Pairwise Cross-Feature Interaction E2E Tests (covering all F01–F31)."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import unittest
from pathlib import Path

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import PulseHttpClient  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance  # noqa: E402
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


def _create_project(client: PulseHttpClient, name: str) -> tuple[str, str]:
    body = client.create_project(name).raise_for_status().json()
    return body["project"]["id"], body["api_key"]["plaintext_key"]


def _shared_meta_url(cluster: ClusterHarness, label: str = "meta") -> str:
    pg = external_postgres_url()
    if pg:
        return pg
    shared_dir = cluster.node_mgr.make_temp_data_dir(f"pulse_{label}_")
    return f"sqlite://{(shared_dir / 'shared_meta.db').resolve()}"


def _parse_wal_epoch(rel_paths: list[str]) -> int:
    for rel in rel_paths:
        fname = rel.rsplit("/", 1)[-1]
        if fname.startswith("epoch_") and "_seq_" in fname and fname.endswith(".wal"):
            ep_str = fname[len("epoch_") :].split("_seq_", 1)[0]
            if ep_str.isdigit():
                return int(ep_str)
    return 1


@tier(3)
class TestTier3PairwiseStorageAndWriters(unittest.TestCase):
    """35 pairwise cross-feature interaction tests covering all 31 features (F01–F31)."""

    # ------------------------------------------------------------------
    # T0a Pairs (F01–F03) — verified live against baseline 7757e8b
    # ------------------------------------------------------------------

    @feature("F01", "F03")
    @require_milestone("T0a")
    def test_p01_f01_f03_unwritable_local_wal_returns_503_for_events_and_traces(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                byok_master_key=random_byok_hex(),
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            wal_root = node.data_dir / "wal"
            self.addCleanup(lambda: _chmod_tree(wal_root, 0o755, 0o644))

            client = node.client()
            pid, api_key = _create_project(client, "P01 Unwritable WAL")
            now_ms = int(time.time() * 1000)

            # Baseline valid ingest seeds bucket dirs
            client.ingest(
                {"events": [make_event(event_name="p01_ok", distinct_id="u_init", ts_ms=now_ms)]},
                api_key=api_key,
            ).raise_for_status()
            client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p01_ok", start_ts_ms=now_ms),
                api_key=api_key,
            ).raise_for_status()
            s3.reset_counts()

            # Make entire local WAL tree read-only -> event, agent JSON, and OTLP JSON must return 503
            _chmod_tree(wal_root, 0o555)
            r_ev = client.ingest(
                {"events": [make_event(event_name="p01_ok", distinct_id="u_blocked", ts_ms=now_ms + 10)]},
                api_key=api_key,
            )
            r_ag = client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p01_blocked", start_ts_ms=now_ms + 10),
                api_key=api_key,
            )
            r_otlp = client.ingest_otel_traces(
                {
                    "resourceSpans": [
                        {
                            "scopeSpans": [
                                {
                                    "spans": [
                                        {
                                            "traceId": "0123456789abcdef0123456789abcdef",
                                            "spanId": "0123456789abcdef",
                                            "name": "otlp_blocked",
                                            "startTimeUnixNano": str((now_ms + 10) * 1_000_000),
                                            "endTimeUnixNano": str((now_ms + 20) * 1_000_000),
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                api_key=api_key,
            )
            _chmod_tree(wal_root, 0o755, 0o644)

            self.assertEqual(r_ev.status, 503, f"Expected 503 for event WAL failure, got {r_ev.status}: {r_ev.text()}")
            self.assertEqual(r_ag.status, 503, f"Expected 503 for agent WAL failure, got {r_ag.status}: {r_ag.text()}")
            self.assertEqual(r_otlp.status, 503, f"Expected 503 for OTLP WAL failure, got {r_otlp.status}: {r_otlp.text()}")
            self.assertEqual(r_ag.json().get("code"), "service_unavailable")

            # Verify zero S3 WAL writes and refused items were NOT applied
            self.assertEqual(s3.put_count("wal/"), 0)
            self.assertEqual(s3.put_count("agent_wal/"), 0)
            self.assertEqual(_insights_total(client, pid, "p01_ok", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 1.0)
            traces = client.list_agent_traces(pid).raise_for_status().json()["traces"]
            self.assertEqual({t["trace_id"] for t in traces}, {"tr_p01_ok"})

            # Recovery once permissions restored
            client.ingest(
                {"events": [make_event(event_name="p01_ok", distinct_id="u_after", ts_ms=now_ms + 20)]},
                api_key=api_key,
            ).raise_for_status()
            self.assertEqual(_insights_total(client, pid, "p01_ok", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 2.0)

    @feature("F01", "F02")
    @require_milestone("T0a")
    def test_p02_f01_f02_byok_corrupt_wal_quarantine_and_mode_switch_refusal(self) -> None:
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
            client = node.client()
            pid, api_key = _create_project(client, "P02 BYOK Corrupt WAL")
            now_ms = int(time.time() * 1000)

            client.ingest(
                {"events": make_events(4, event_name="p02_ev", distinct_id_prefix="u_p02", base_ts_ms=now_ms)},
                api_key=api_key,
            ).raise_for_status()
            client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p02_1", start_ts_ms=now_ms),
                api_key=api_key,
            ).raise_for_status()

            local_wal_files = node.local_files("wal/wal/")
            self.assertGreaterEqual(len(local_wal_files), 1)
            for wf in local_wal_files:
                self.assertTrue(
                    node.read_local_bytes(wf).startswith(b"PLSEAGM2"),
                    f"Expected PLSEAGM2 envelope on local WAL {wf}",
                )
            epoch = _parse_wal_epoch(local_wal_files)

            # SIGKILL leaving local WALs on disk
            node.kill()

            # Switching to object mode with unflushed local WALs must refuse startup
            rc, out, err = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="object",
                byok_master_key=byok,
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)
            self.assertIn("unflushed WAL", out + err)

            # Inject a corrupt WAL frame locally; recovery must quarantine it to local wal_corrupt/
            corrupt_rel = f"wal/wal/{pid}/bucket_000/epoch_{epoch:010}_seq_000000999999.wal"
            node.write_local_bytes(corrupt_rel, b"PWL2" + b"\xde\xad\xbe\xef" * 8)

            node2 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                byok_master_key=byok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            # Trigger lazy project open (after killed node's 3s lease expires) so recovery runs
            wait_until(
                lambda: c2.query(
                    {
                        "project_id": pid,
                        "kind": "insights",
                        "query": {
                            "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
                            "interval": "day",
                            "series": [{"label": "cnt", "event_name": "p02_ev", "metric": "totals"}],
                        },
                    }
                ).status == 200,
                timeout=15.0,
                desc="recovered project opens after lease TTL",
            )
            wait_until(
                lambda: len(node2.local_files(f"wal/wal_corrupt/{pid}/")) >= 1,
                timeout=15.0,
                desc="corrupt WAL quarantined locally",
            )
            self.assertEqual(s3.put_count("wal_corrupt/"), 0)
            self.assertEqual(s3.put_count("wal/"), 0)
            self.assertEqual(_insights_total(c2, pid, "p02_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 4.0)

            # Clean shutdown drains local WALs -> switching to object mode now succeeds
            node2.stop(graceful=True)
            self.assertEqual(node2.local_files("wal/wal/"), [])
            self.assertEqual(node2.local_files("wal/agent_wal/"), [])

            node3 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="object",
                byok_master_key=byok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node3, NodeInstance)
            c3 = node3.client()
            self.assertEqual(_insights_total(c3, pid, "p02_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 4.0)
            self.assertEqual(len(c3.list_agent_traces(pid).raise_for_status().json()["traces"]), 1)

    @feature("F02", "F03")
    @require_milestone("T0a")
    def test_p03_f02_f03_object_mode_wal_failure_and_unclean_upgrade_guard(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="object",
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P03 Object Mode WAL")
            now_ms = int(time.time() * 1000)

            client.ingest(
                {"events": [make_event(event_name="p03_ev", distinct_id="u1", ts_ms=now_ms)]},
                api_key=api_key,
            ).raise_for_status()

            # S3 WAL failure in object mode returns 503
            s3.set_fault(fail_prefixes=["wal/", "agent_wal/"], status=503, ops=["PUT"])
            r_ev = client.ingest(
                {"events": [make_event(event_name="p03_ev", distinct_id="u_fail", ts_ms=now_ms + 5)]},
                api_key=api_key,
            )
            r_ag = client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p03_fail", start_ts_ms=now_ms + 5),
                api_key=api_key,
            )
            s3.clear_faults()
            self.assertEqual(r_ev.status, 503)
            self.assertEqual(r_ag.status, 503)

            # Ingest valid data and SIGKILL so S3 wal/ objects remain
            client.ingest(
                {"events": [make_event(event_name="p03_ev", distinct_id="u2", ts_ms=now_ms + 10)]},
                api_key=api_key,
            ).raise_for_status()
            self.assertTrue(s3.list_objects("wal/"), "Expected S3 wal/ objects before SIGKILL")
            node.kill()

            # Local mode must refuse startup over unflushed S3 WAL
            rc, out, err = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)
            self.assertIn("unflushed WAL", out + err)

            # Boot object mode -> open project (after 3s lease expires) -> clean stop -> upgrade to local succeeds
            node2 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="object",
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
                            "series": [{"label": "cnt", "event_name": "p03_ev", "metric": "totals"}],
                        },
                    }
                ).status == 200,
                timeout=15.0,
                desc="object node opens project and replays S3 WAL",
            )
            node2.stop(graceful=True)
            self.assertEqual(s3.list_objects("wal/"), [])

            node3 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node3, NodeInstance)
            self.assertEqual(
                _insights_total(node3.client(), pid, "p03_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                2.0,
            )

    # ------------------------------------------------------------------
    # T0b Pairs (F04–F06)
    # ------------------------------------------------------------------

    @feature("F01", "F04")
    @require_milestone("T0b")
    def test_p04_f01_f04_hot_routed_below_byok_for_wal_identity_and_profiles(self) -> None:
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
            client = node.client()
            pid, api_key = _create_project(client, "P04 HotRouted BYOK")
            s3.reset_counts()

            now_ms = int(time.time() * 1000)
            client.identify(
                {"anon_id": "anon_p04", "user_id": "user_p04", "user_properties": {"tier": "pro"}},
                api_key=api_key,
            ).raise_for_status()
            client.engage(
                {"distinct_id": "user_p04", "$set": {"country": "IN"}},
                api_key=api_key,
            ).raise_for_status()
            client.ingest(
                {"events": make_events(10, event_name="p04_ev", distinct_id_prefix="u_p04", base_ts_ms=now_ms)},
                api_key=api_key,
            ).raise_for_status()
            client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p04", start_ts_ms=now_ms),
                api_key=api_key,
            ).raise_for_status()

            # Zero S3 calls to hot prefixes during request handling
            for prefix in ("wal/", "agent_wal/", "identity/", "profiles/"):
                self.assertEqual(s3.put_count(prefix), 0, f"Unexpected S3 PUT to {prefix}")
                self.assertEqual(s3.get_counts().get((prefix.rstrip("/"), "GET"), 0), 0)

            # Local identity/ & profiles/ exist and are PLSEAGM2 encrypted
            id_files = [f for f in node.local_files() if f"identity/{pid}" in f or f.startswith("identity/")]
            prof_files = [f for f in node.local_files() if f"profiles/{pid}" in f or f.startswith("profiles/")]
            self.assertTrue(id_files, f"Expected local identity/ files, got {node.local_files()}")
            self.assertTrue(prof_files, f"Expected local profiles/ files, got {node.local_files()}")
            for rel in id_files + prof_files:
                self.assertTrue(node.read_local_bytes(rel).startswith(b"PLSEAGM2"), f"{rel} missing PLSEAGM2 header")

            # Wrong BYOK key on restart must fail cleanly
            node.stop(graceful=True)
            rc, _, _ = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                byok_master_key=random_byok_hex(),
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)

    @feature("F02", "F05")
    @require_milestone("T0b")
    def test_p05_f02_f05_mode_marker_and_wal_mismatch_refusal(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P05 Mode Marker")
            client.ingest(
                {"events": [make_event(event_name="p05_ev", distinct_id="u1")]},
                api_key=api_key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
            node.stop(graceful=True)

            # Mode marker refuses object switch even when WAL is cleanly drained
            rc, out, err = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="object",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc, 0)
            self.assertTrue("local" in (out + err).lower() or "mode" in (out + err).lower())

            # Partial volume loss (missing local identity/) while project exists in MetaStore/S3 must refuse
            for rel in list(node.local_files()):
                if "identity/" in rel:
                    node.rm_local(rel)
            rc2, out2, err2 = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                expect_fail=True,
            )
            self.assertNotEqual(rc2, 0)
            self.assertIn("restore", (out2 + err2).lower())

    @feature("F04", "F06")
    @require_milestone("T0b")
    def test_p06_f04_f06_local_slatedb_multipart_fsync_crash_durability(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P06 Multipart Fsync")

            # Generate enough identity + profile churn to force SlateDB SST multipart writes
            for i in range(60):
                client.identify(
                    {
                        "anon_id": f"anon_p06_{i:03d}",
                        "user_id": f"user_p06_{i:03d}",
                        "user_properties": {"idx": i, "blob": "x" * 256},
                    },
                    api_key=api_key,
                ).raise_for_status()
            node.kill()

            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            users = c2.list_users(pid, limit=100).raise_for_status().json()
            self.assertEqual(len(users), 60)
            u59 = c2.get_user(pid, "user_p06_059").raise_for_status().json()
            self.assertEqual(u59["profile"]["props"]["idx"], 59)

    # ------------------------------------------------------------------
    # T1 Pairs (F07–F13)
    # ------------------------------------------------------------------

    @feature("F07", "F08")
    @require_milestone("T1")
    def test_p07_f07_f08_published_manifest_and_ordered_byok_snapshots_hwm(self) -> None:
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
            pid, api_key = _create_project(client, "P07 Pub HWM")

            now_ms = int(time.time() * 1000)
            client.engage({"distinct_id": "u_p07_a", "$set": {"plan": "pro"}}, api_key=api_key).raise_for_status()
            client.ingest(
                {"events": make_events(5, event_name="p07_ev", distinct_id_prefix="u_p07", base_ts_ms=now_ms)},
                api_key=api_key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()

            wait_until(
                lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="pub_000000000000.json published",
            )
            pub0_raw = s3.get_object(f"published/{pid}/pub_000000000000.json")
            assert pub0_raw is not None
            self.assertTrue(pub0_raw.startswith(b"PLSEAGM2"))

            # Fault S3 published/ PUTs -> publish must fail before creating pub_000000000001.json
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            client.ingest(
                {"events": make_events(5, event_name="p07_ev", distinct_id_prefix="u_p07_b", base_ts_ms=now_ms + 5000)},
                api_key=api_key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
            time.sleep(3.0)
            self.assertNotIn(f"published/{pid}/pub_000000000001.json", s3.list_objects(f"published/{pid}/"))

            s3.clear_faults()
            wait_until(
                lambda: f"published/{pid}/pub_000000000001.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="dense pub_000000000001.json after S3 recovery",
            )

    @feature("F08", "F09")
    @require_milestone("T1")
    def test_p08_f08_f09_snapshot_benchmark_amplification_and_hwm_completeness(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P08 Bench HWM")
            now_ms = int(time.time() * 1000)

            s3.reset_counts()
            for cycle in range(2):
                evs = make_events(25, event_name="p08_ev", distinct_id_prefix=f"u_p08_c{cycle}", base_ts_ms=now_ms + cycle * 1000)
                client.ingest({"events": evs}, api_key=api_key).raise_for_status()
                client.engage({"distinct_id": f"u_p08_c{cycle}_0000", "$set": {"cycle": cycle}}, api_key=api_key).raise_for_status()
                client.admin_flush().raise_for_status()
                target_pub = f"published/{pid}/pub_{cycle:012}.json"
                wait_until(
                    lambda k=target_pub: k in s3.list_objects(f"published/{pid}/"),
                    timeout=20.0,
                    desc=f"publish {target_pub}",
                )

            pub_puts = s3.put_count("published/")
            self.assertGreaterEqual(pub_puts, 2)
            # Bounded snapshot PUTs per cycle (no per-event/per-user PUT amplification)
            self.assertLess(pub_puts, 100, f"Unexpected PUT amplification in published/: {pub_puts}")
            pub1 = json.loads(s3.get_object(f"published/{pid}/pub_000000000001.json").decode("utf-8"))
            self.assertGreaterEqual(int(pub1["high_water_mark"]), 50)
            self.assertEqual(int(pub1["publish_seq"]), 1)

    @feature("F08", "F10")
    @require_milestone("T1")
    def test_p09_f08_f10_path_bound_encrypted_snapshots_and_restore(self) -> None:
        pg_url = skip_if_no_postgres()
        byok = random_byok_hex()
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                database_url=pg_url,
                byok_master_key=byok,
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P09 Encrypted Restore")
            now_ms = int(time.time() * 1000)

            client.identify({"anon_id": "a_p09", "user_id": "u_p09", "user_properties": {"role": "vip"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": make_events(5, event_name="p09_ev", distinct_id_prefix="u_p09", base_ts_ms=now_ms)}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)
            node.stop(graceful=True)

            # Wrong BYOK key must fail restore
            bad_dir = cluster.node_mgr.make_temp_data_dir("p09_bad_")
            bad_res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=bad_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url, "PULSE_BYOK_MASTER_KEY": random_byok_hex()},
            )
            self.assertNotEqual(bad_res.returncode, 0)

            # Correct BYOK key restores cleanly and prints timestamp + data-loss warning
            good_dir = cluster.node_mgr.make_temp_data_dir("p09_good_")
            good_res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=good_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url, "PULSE_BYOK_MASTER_KEY": byok},
            )
            self.assertEqual(good_res.returncode, 0, f"restore failed: {good_res.stdout}\n{good_res.stderr}")
            combined = (good_res.stdout + "\n" + good_res.stderr).lower()
            self.assertTrue("loss" in combined or "warn" in combined)

            node2 = cluster.start_node(
                data_dir=good_dir,
                hot_store="local",
                database_url=pg_url,
                byok_master_key=byok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(_insights_total(c2, pid, "p09_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 5.0)
            self.assertEqual(c2.get_user(pid, "u_p09").raise_for_status().json()["profile"]["props"]["role"], "vip")

    @feature("F10", "F11")
    @require_milestone("T1")
    def test_p10_f10_f11_restore_erasure_reconciliation_and_uid_ge_hwm_cleanup(self) -> None:
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
            pid, api_key = _create_project(client, "P10 Restore Erasure")
            now_ms = int(time.time() * 1000)

            # User A created before publish (uid < HWM)
            client.engage({"distinct_id": "u_before_hwm", "$set": {"secret": "a"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": [make_event(event_name="p10_ev", distinct_id="u_before_hwm", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            # Freeze publish by pausing S3 published/ PUTs while creating & erasing post-publish user B (uid >= HWM)
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            client.engage({"distinct_id": "u_after_hwm", "$set": {"secret": "b"}}, api_key=api_key).raise_for_status()
            client.ingest({"events": [make_event(event_name="p10_ev", distinct_id="u_after_hwm", ts_ms=now_ms + 100)]}, api_key=api_key).raise_for_status()
            client.delete_user(pid, "u_before_hwm").raise_for_status()
            client.delete_user(pid, "u_after_hwm").raise_for_status()
            node.kill()
            s3.clear_faults()

            restored_dir = cluster.node_mgr.make_temp_data_dir("p10_restored_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url},
            )
            self.assertEqual(res.returncode, 0, f"restore failed: {res.stderr}")

            node2 = cluster.start_node(
                data_dir=restored_dir,
                hot_store="local",
                database_url=pg_url,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()

            # Pre-HWM user A stays erased, audit log preserved (2 entries)
            self.assertEqual(c2.get_user(pid, "u_before_hwm").json().get("profile"), None)
            self.assertEqual(len(c2.list_deletions(pid).raise_for_status().json()), 2)

            # New user V created post-restore (reuses uid >= HWM) sending backdated event must NOT be hidden
            client2_key = c2.create_api_key(pid, "post-restore").raise_for_status().json()["plaintext_key"]
            c2.import_events(
                pid,
                {"events": [make_event(event_name="p10_backdated", distinct_id="u_reused_slot", ts_ms=now_ms - 10_000)]},
            ).raise_for_status()
            self.assertEqual(_insights_total(c2, pid, "p10_backdated", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 1.0)
            self.assertTrue(client2_key.startswith("pk_live_"))

    @feature("F11", "F12")
    @require_milestone("T1")
    def test_p11_f11_f12_erased_uids_defense_in_depth_and_reengage_after_restore(self) -> None:
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
            pid, api_key = _create_project(client, "P11 Erasure Reengage")
            now_ms = int(time.time() * 1000)

            client.engage({"distinct_id": "u_p11", "$set": {"plan": "gold"}}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            client.delete_user(pid, "u_p11").raise_for_status()
            self.assertEqual(client.list_users(pid, search="u_p11").raise_for_status().json(), [])
            mcp_res = client.mcp_call("tools/list", project_id=pid)
            self.assertIn(mcp_res.status, (200, 404))
            node.stop(graceful=True)

            restored_dir = cluster.node_mgr.make_temp_data_dir("p11_restored_")
            cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url},
            )
            node2 = cluster.start_node(
                data_dir=restored_dir,
                hot_store="local",
                database_url=pg_url,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(c2.list_users(pid, search="u_p11").raise_for_status().json(), [])

            # Re-engage after erasure (updated_at_ms > erased_at_ms) becomes visible
            time.sleep(0.05)
            c2.engage({"distinct_id": "u_p11", "$set": {"plan": "starter"}}, api_key=api_key).raise_for_status()
            detail = c2.get_user(pid, "u_p11").raise_for_status().json()
            self.assertEqual(detail["profile"]["props"]["plan"], "starter")

    @feature("F10", "F13")
    @require_milestone("T1")
    def test_p12_f10_f13_sqlite_startup_warning_vs_restore_sqlite_refusal(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            logs = node.read_logs(tail_bytes=0)
            self.assertTrue(
                "sqlite" in logs.lower() and "database_url" in logs.lower(),
                f"Expected SQLite warning mentioning DATABASE_URL in logs:\n{logs[:1000]}",
            )
            node.stop(graceful=True)

            # restore --from-published with SQLite DATABASE_URL must refuse immediately
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=cluster.node_mgr.make_temp_data_dir("p12_sqlite_"),
                env={**s3.env_vars(), "DATABASE_URL": f"sqlite://{(node.data_dir / 'pulse_meta.db').resolve()}"},
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("database_url", (res.stdout + res.stderr).lower())

    # ------------------------------------------------------------------
    # T2 & T3 Pairs (F14–F21)
    # ------------------------------------------------------------------

    @feature("F07", "F14")
    @require_milestone("T2")
    def test_p13_f07_f14_local_manifest_commits_vs_dense_published_cadence(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=8,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P13 Local Manifest")
            s3.reset_counts()
            now_ms = int(time.time() * 1000)

            for i in range(4):
                client.ingest(
                    {"events": [make_event(event_name="p13_ev", distinct_id=f"u_{i}", ts_ms=now_ms + i)]},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()

            local_manifests = [f for f in node.local_files() if f"manifests/{pid}/" in f]
            self.assertGreaterEqual(len(local_manifests), 1)
            self.assertEqual(s3.put_count("published/"), 0, "L0 flushes must not publish to S3 before interval")

            wait_until(
                lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="published manifest written on upload interval",
            )

    @feature("F07", "F15")
    @require_milestone("T2")
    def test_p14_f07_f15_dense_publish_seq_fencing_and_deposed_writer_refusal(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P14 Fencing")
            now_ms = int(time.time() * 1000)

            client.ingest({"events": [make_event(event_name="p14_ev", distinct_id="u1", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)
            node.stop(graceful=True)

            # Simulate newer writer generation publishing pub_000000000001.json with lower source_local_epoch
            pub0 = json.loads(s3.get_object(f"published/{pid}/pub_000000000000.json").decode("utf-8"))
            pub1 = dict(pub0)
            pub1["publish_seq"] = int(pub0["publish_seq"]) + 1
            pub1["writer_generation"] = int(pub0["writer_generation"]) + 1
            pub1["source_local_epoch"] = 1
            pub1["published_at_ms"] = int(time.time() * 1000)
            s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub1).encode("utf-8"))

            # Stale writer A restarts and attempts write-open / ingest -> must be fenced (startup fail or 503)
            res = cluster.node_mgr.start_node(
                data_dir=node.data_dir,
                mock_s3=s3,
                hot_store="local",
                admin_token=cluster.admin_token,
                writer_lease_ttl_secs=3,
                wait_ready=True,
            )
            if isinstance(res, NodeInstance):
                r = res.client().ingest(
                    {"events": [make_event(event_name="p14_ev", distinct_id="u2", ts_ms=now_ms + 10)]},
                    api_key=api_key,
                )
                self.assertEqual(r.status, 503, f"Deposed writer must fail write-open with 503, got {r.status}")
            # S3 pub_1 must remain untouched with higher writer_generation
            cur_pub1 = json.loads(s3.get_object(f"published/{pid}/pub_000000000001.json").decode("utf-8"))
            self.assertEqual(cur_pub1["writer_generation"], pub1["writer_generation"])

    @feature("F14", "F18")
    @require_milestone("T3")
    def test_p15_f14_f18_local_manifest_tracks_hot_segments_without_s3_puts(self) -> None:
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
            pid, api_key = _create_project(client, "P15 Local Segments")
            s3.reset_counts()
            now_ms = int(time.time() * 1000)

            for i in range(5):
                client.ingest(
                    {"events": [make_event(event_name="p15_ev", distinct_id=f"u_{i}", ts_ms=now_ms + i * 10)]},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()

            hot_segs = [f for f in node.local_files() if f"hot/segments/{pid}/" in f and f.endswith(".pvtx")]
            self.assertGreaterEqual(len(hot_segs), 5, f"Expected >=5 local .pvtx segments, got {hot_segs}")
            self.assertEqual(s3.put_count("segments/"), 0)
            self.assertEqual(s3.put_count("published/"), 0)

            # Query & export transparently combine local hot/segments/
            self.assertEqual(_insights_total(client, pid, "p15_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 5.0)
            exported = client.export_events(pid, format="json").raise_for_status().json()
            self.assertEqual(exported["count"], 5)

    @feature("F15", "F20")
    @require_milestone("T3")
    def test_p16_f15_f20_step6_self_crash_recovery_and_startup_sweep(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P16 Step6 Recovery")
            now_ms = int(time.time() * 1000)

            client.ingest({"events": [make_event(event_name="p16_ev", distinct_id="u1", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            # Trigger cycle 2 and SIGKILL immediately upon S3 PUT of pub_000000000001.json (step-6 boundary)
            killed = threading.Event()

            def on_s3(op: str, key: str) -> None:
                if op == "PUT" and key == f"published/{pid}/pub_000000000001.json" and not killed.is_set():
                    killed.set()
                    if node.proc is not None:
                        node.proc.kill()

            s3.on_operation(on_s3)
            client.ingest({"events": [make_event(event_name="p16_ev", distinct_id="u2", ts_ms=now_ms + 50)]}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(killed.is_set, timeout=20.0, desc="step-6 pub_1 kill hook")
            s3.clear_callbacks()
            node.stop(graceful=False)

            # Restart must recognize step-6 self-crash, sweep local hot/segments/, and publish pub_2 on next cycle
            node2 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            self.assertEqual(_insights_total(c2, pid, "p16_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 2.0)
            c2.ingest({"events": [make_event(event_name="p16_ev", distinct_id="u3", ts_ms=now_ms + 100)]}, api_key=api_key).raise_for_status()
            c2.admin_flush().raise_for_status()
            wait_until(
                lambda: f"published/{pid}/pub_000000000002.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="pub_2 after step-6 self-crash recovery",
            )

    @feature("F12", "F17")
    @require_milestone("T2")
    def test_p17_f12_f17_remote_query_node_filters_erased_uids_before_publish(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            meta_url = _shared_meta_url(cluster, "p17")
            dir_a = cluster.node_mgr.make_temp_data_dir("p17_owner_")
            dir_b = cluster.node_mgr.make_temp_data_dir("p17_query_")

            owner = cluster.start_node(
                role="all",
                data_dir=dir_a,
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                node_name="owner-a",
            )
            assert isinstance(owner, NodeInstance)
            ca = owner.client()
            pid, api_key = _create_project(ca, "P17 Remote Query Erasure")
            now_ms = int(time.time() * 1000)

            ca.identify({"anon_id": "anon_p17", "user_id": "usr_p17", "user_properties": {"plan": "pro"}}, api_key=api_key).raise_for_status()
            ca.ingest({"events": [make_event(event_name="p17_ev", distinct_id="usr_p17", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            ca.admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            query_node = cluster.start_node(
                role="query",
                data_dir=dir_b,
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
                node_name="query-b",
            )
            assert isinstance(query_node, NodeInstance)
            cb = query_node.client()
            wait_until(
                lambda: _insights_total(cb, pid, "p17_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0,
                timeout=15.0,
                desc="query node sees published event",
            )

            # Pause S3 published/ so snapshot stays stale, then erase usr_p17 on owner
            s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=503, ops=["PUT"])
            ca.delete_user(pid, "usr_p17").raise_for_status()

            s3.reset_counts()
            wait_until(
                lambda: cb.list_users(pid, search="usr_p17").raise_for_status().json() == [],
                timeout=15.0,
                desc="query node filters erased profile immediately",
            )
            self.assertIsNone(cb.get_user(pid, "anon_p17").raise_for_status().json().get("profile"))
            self.assertEqual(_insights_total(cb, pid, "p17_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 0.0)
            self.assertEqual(s3.put_count("published/"), 0)
            self.assertEqual(s3.put_count("segments/"), 0)

    @feature("F16", "F17", "F26")
    @require_milestone("T4")
    def test_p18_f16_f17_f26_two_interval_grace_remote_query_and_cold_s3_merge(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            meta_url = _shared_meta_url(cluster, "p18")
            dir_a = cluster.node_mgr.make_temp_data_dir("p18_owner_")
            dir_b = cluster.node_mgr.make_temp_data_dir("p18_query_")

            owner = cluster.start_node(
                role="all",
                data_dir=dir_a,
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"},
            )
            assert isinstance(owner, NodeInstance)
            ca = owner.client()
            pid, api_key = _create_project(ca, "P18 Grace Cold Merge")

            # Ingest closed-date events (3 days ago) across 3 separate publish cycles to create >=3 S3 segments
            closed_ts = int(time.time() * 1000) - 3 * 86_400_000
            for i in range(3):
                ca.ingest(
                    {"events": [make_event(event_name="p18_cold", distinct_id="u_same_bucket", ts_ms=closed_ts + i * 1000)]},
                    api_key=api_key,
                ).raise_for_status()
                ca.admin_flush().raise_for_status()
                wait_until(
                    lambda seq=i: f"published/{pid}/pub_{seq:012}.json" in s3.list_objects(f"published/{pid}/"),
                    timeout=20.0,
                )

            initial_segs = set(s3.list_objects(f"segments/{pid}/"))
            self.assertGreaterEqual(len(initial_segs), 3)

            query_node = cluster.start_node(
                role="query",
                data_dir=dir_b,
                database_url=meta_url,
                hot_store="local",
                upload_interval_secs=2,
            )
            assert isinstance(query_node, NodeInstance)
            cb = query_node.client()
            self.assertEqual(
                _insights_total(cb, pid, "p18_cold", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                3.0,
            )

            # Wait for cold merge to supersede initial segments and publish
            wait_until(
                lambda: len(set(s3.list_objects(f"segments/{pid}/")) - initial_segs) >= 1,
                timeout=25.0,
                desc="cold merge writes compacted S3 segment",
            )
            # Remote query node continues answering without 404 across grace window, and old segments are eventually deleted
            self.assertEqual(
                _insights_total(cb, pid, "p18_cold", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                3.0,
            )
            wait_until(
                lambda: not initial_segs.issubset(set(s3.list_objects(f"segments/{pid}/"))),
                timeout=30.0,
                desc="superseded segments deleted after 2-publish grace",
            )
            self.assertEqual(
                _insights_total(cb, pid, "p18_cold", start_ms=closed_ts - 60_000, end_ms=closed_ts + 60_000),
                3.0,
            )

    @feature("F19", "F20")
    @require_milestone("T3")
    def test_p19_f19_f20_seven_step_upload_crash_intent_sweep_and_dedup_error(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P19 Intent Sweep")
            now_ms = int(time.time() * 1000)

            killed = threading.Event()

            def on_s3(op: str, key: str) -> None:
                if op == "PUT" and key.startswith(f"segments/{pid}/") and not killed.is_set():
                    killed.set()
                    if node.proc is not None:
                        node.proc.kill()

            s3.on_operation(on_s3)
            client.ingest(
                {"events": make_events(5, event_name="p19_ev", distinct_id_prefix="u_p19", base_ts_ms=now_ms, seed_prefix="p19")},
                api_key=api_key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(killed.is_set, timeout=20.0, desc="crash after step-4 segment PUT")
            s3.clear_callbacks()
            node.stop(graceful=False)

            orphan_segs = list(s3.list_objects(f"segments/{pid}/"))
            self.assertGreaterEqual(len(orphan_segs), 1)
            self.assertEqual(s3.list_objects(f"published/{pid}/"), [])

            # Startup sweep must delete orphan step-4 S3 segments, then re-upload cleanly
            node2 = cluster.start_node(
                data_dir=node.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node2, NodeInstance)
            c2 = node2.client()
            wait_until(
                lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"),
                timeout=20.0,
                desc="pub_0 after intent sweep",
            )
            self.assertEqual(_insights_total(c2, pid, "p19_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 5.0)
            node2.stop(graceful=True)

            # S3 unreachable during restart dedup seeding must fail project open rather than accepting duplicates
            s3.set_fault(unreachable=True)
            node3 = cluster.start_node(
                data_dir=node2.data_dir,
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
                wait_ready=False,
            )
            assert isinstance(node3, NodeInstance)
            time.sleep(2.0)
            if node3.proc is not None and node3.proc.poll() is None:
                r_dup = node3.client().ingest(
                    {"events": make_events(5, event_name="p19_ev", distinct_id_prefix="u_p19", base_ts_ms=now_ms, seed_prefix="p19")},
                    api_key=api_key,
                )
                self.assertGreaterEqual(r_dup.status, 500)
            s3.clear_faults()

    @feature("F19", "F21")
    @require_milestone("T3")
    def test_p20_f19_f21_s3_outage_1x_immediate_upload_and_2x_429_backpressure(self) -> None:
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
            pid, api_key = _create_project(client, "P20 Byte Bound 429")
            now_ms = int(time.time() * 1000)

            s3.set_fault(fail_prefixes=["segments/", "published/"], status=503)
            accepted_total = 0
            saw_429 = False
            for b in range(60):
                batch = make_events(
                    40,
                    event_name="p20_ev",
                    distinct_id_prefix=f"u_p20_{b}",
                    base_ts_ms=now_ms + b * 10,
                    props={"pad": "A" * 512},
                )
                resp = client.ingest({"events": batch}, api_key=api_key)
                if resp.status == 429:
                    self.assertIsNotNone(resp.header("retry-after"))
                    saw_429 = True
                    break
                resp.raise_for_status()
                accepted_total += int(resp.json()["accepted"])
                client.admin_flush().raise_for_status()

            self.assertTrue(saw_429, "Expected HTTP 429 once hot/segments/ exceeded 2x byte bound")
            s3.clear_faults()

            # Immediate upload drains hot/segments/ and restores 200
            wait_until(
                lambda: client.ingest(
                    {"events": [make_event(event_name="p20_ev", distinct_id="u_recovered", ts_ms=now_ms + 99999)]},
                    api_key=api_key,
                ).status == 200,
                timeout=25.0,
                desc="200 resumes after S3 recovery drains hot/segments/",
            )
            self.assertEqual(
                _insights_total(client, pid, "p20_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 200_000),
                float(accepted_total + 1),
            )

    @feature("F18", "F21")
    @require_milestone("T3")
    def test_p21_f18_f21_eager_upload_zero_interval_vs_local_segment_bounds(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=0,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P21 Eager Interval 0")
            s3.reset_counts()
            now_ms = int(time.time() * 1000)

            for i in range(4):
                client.ingest(
                    {"events": [make_event(event_name="p21_ev", distinct_id=f"u_{i}", ts_ms=now_ms + i)]},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()

            wait_until(lambda: s3.put_count("segments/") >= 1, timeout=15.0, desc="eager upload with interval=0")
            self.assertEqual(_insights_total(client, pid, "p21_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 4.0)

    # ------------------------------------------------------------------
    # W1 & W2 Pairs (F22–F25)
    # ------------------------------------------------------------------

    @feature("F22", "F23")
    @require_milestone("W1")
    def test_p22_f22_f23_async_ingest_dictionary_overflow_dead_letter_cli(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P22 Async DeadLetter")
            now_ms = int(time.time() * 1000)

            # Fill event dictionary up to 8192 event names
            idx = 0
            while idx < 8192:
                chunk = [
                    make_event(event_name=f"dict_ev_{idx + j:04d}", distinct_id="u_dict", ts_ms=now_ms + idx + j)
                    for j in range(min(256, 8192 - idx))
                ]
                r = client.ingest({"events": chunk}, api_key=api_key)
                r.raise_for_status()
                self.assertEqual(r.json()["deduped"], 0)
                idx += len(chunk)

            # Send mixed batch: valid existing event + overflow #8193 + valid existing event
            overflow_batch = [
                make_event(event_name="dict_ev_0000", distinct_id="u_valid_1", ts_ms=now_ms + 20_000),
                make_event(event_name="dict_ev_overflow_8193", distinct_id="u_dead_p22", ts_ms=now_ms + 20_001),
                make_event(event_name="dict_ev_0001", distinct_id="u_valid_2", ts_ms=now_ms + 20_002),
            ]
            r_over = client.ingest({"events": overflow_batch}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r_over["accepted"], 3)
            self.assertEqual(r_over["deduped"], 0)

            wait_until(
                lambda: len(node.local_files(f"wal/wal_dead/{pid}/")) >= 1,
                timeout=15.0,
                desc="dead-letter written locally",
            )
            self.assertEqual(_insights_total(client, pid, "dict_ev_0000", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 2.0)

            caps = cluster.node_mgr.probe_capabilities()
            dl_cmd = next((c for c in ("dead-letters", "dead-letter", "wal-dead") if caps.has_subcommand(c)), None)
            self.assertIsNotNone(dl_cmd, f"Expected dead-letter CLI subcommand in {sorted(caps.subcommands)}")
            assert dl_cmd is not None

            cli_res = cluster.node_mgr.run_cli([dl_cmd, "--project-id", pid], data_dir=node.data_dir)
            self.assertEqual(cli_res.returncode, 0, cli_res.stderr)
            self.assertIn("dict_ev_overflow_8193", cli_res.stdout)

            # Erasing u_dead_p22 must exclude its dead-lettered event from CLI output
            client.delete_user(pid, "u_dead_p22").raise_for_status()
            cli_after = cluster.node_mgr.run_cli([dl_cmd, "--project-id", pid], data_dir=node.data_dir)
            self.assertEqual(cli_after.returncode, 0, cli_after.stderr)
            self.assertNotIn("dict_ev_overflow_8193", cli_after.stdout)

    @feature("F22", "F24")
    @require_milestone("W1")
    def test_p23_f22_f24_async_ingest_buffer_and_apply_lag_metrics(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            metrics_tok = "sk_metrics_p23"
            node = cluster.start_node(
                role="all",
                hot_store="local",
                metrics_token=metrics_tok,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P23 Metrics Lag")
            now_ms = int(time.time() * 1000)

            # Unauthenticated /metrics rejected when PULSE_METRICS_TOKEN set
            self.assertEqual(client.metrics().status, 401)
            m0 = client.parsed_metrics(token=metrics_tok)
            self.assertEqual(m0.sum_by_name("pulse_ingest_events_accepted_total"), 0.0)

            evs = make_events(20, event_name="p23_ev", distinct_id_prefix="u_p23", base_ts_ms=now_ms, seed_prefix="p23")
            r1 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            r2 = client.ingest({"events": evs}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r1["deduped"], 0)
            self.assertEqual(r2["deduped"], 0)

            wait_until(
                lambda: client.parsed_metrics(token=metrics_tok).sum_by_name("pulse_ingest_events_deduped_total") >= 20.0,
                timeout=15.0,
                desc="deduped counter incremented in /metrics",
            )
            m1 = client.parsed_metrics(token=metrics_tok)
            self.assertGreaterEqual(m1.sum_by_name("pulse_ingest_events_accepted_total"), 20.0)
            self.assertEqual(m1.sum_by_name("pulse_ingest_log_lag_bytes"), 0.0)
            self.assertEqual(_insights_total(client, pid, "p23_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 20.0)

    @feature("F22", "F25")
    @require_milestone("W2")
    def test_p24_f22_f25_mixed_batch_sync_identify_before_async_event_buffer(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P24 Mixed Sync Before Async")
            now_ms = int(time.time() * 1000)

            # Seed two distinct identified users to force a 409 IdentifiedMergeConflict
            client.identify({"anon_id": "a_1", "user_id": "u_real_1"}, api_key=api_key).raise_for_status()
            client.identify({"anon_id": "a_2", "user_id": "u_real_2"}, api_key=api_key).raise_for_status()

            mixed_conflict = {
                "batch": [
                    {"type": "identify", "anonymousId": "a_1", "userId": "u_real_2"},
                    {"type": "track", "event": "p24_should_not_queue", "userId": "u_real_1", "timestamp": now_ms},
                ]
            }
            r_conf = client.ingest(mixed_conflict, api_key=api_key)
            self.assertEqual(r_conf.status, 409)
            time.sleep(0.3)
            self.assertEqual(_insights_total(client, pid, "p24_should_not_queue", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 0.0)

            # Valid mixed batch updates profile sync and queues event async (deduped: 0)
            mixed_ok = {
                "batch": [
                    {"type": "identify", "anonymousId": "anon_ok", "userId": "u_ok", "traits": {"plan": "pro"}},
                    {"type": "track", "event": "p24_ok_ev", "userId": "u_ok", "timestamp": now_ms + 50},
                ]
            }
            r_ok = client.ingest(mixed_ok, api_key=api_key).raise_for_status().json()
            self.assertEqual(r_ok["accepted"], 1)
            self.assertEqual(r_ok["deduped"], 0)
            self.assertGreaterEqual(r_ok.get("profiles_updated", 0), 1)
            self.assertEqual(client.get_user(pid, "u_ok").raise_for_status().json()["profile"]["props"]["plan"], "pro")
            wait_until(
                lambda: _insights_total(client, pid, "p24_ok_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0,
                timeout=10.0,
            )

    @feature("F04", "F25")
    @require_milestone("W2")
    def test_p25_f04_f25_sync_profile_local_storage_failure_error_propagation(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(role="all", hot_store="local", writer_lease_ttl_secs=3)
            assert isinstance(node, NodeInstance)
            prof_root = node.data_dir / "profiles"
            wal_prof_root = node.data_dir / "wal" / "profiles"
            self.addCleanup(lambda: (_chmod_tree(prof_root, 0o755, 0o644), _chmod_tree(wal_prof_root, 0o755, 0o644)))

            client = node.client()
            pid, api_key = _create_project(client, "P25 W2 Error Propagation")

            id_res = client.identify(
                {"anon_id": "a_p25", "user_id": "u_p25", "user_properties": {"tier": "gold"}},
                api_key=api_key,
            ).raise_for_status().json()
            self.assertIsInstance(id_res["canonical_uid"], int)

            eng_res = client.engage({"distinct_id": "u_p25", "$set": {"active": True}}, api_key=api_key).raise_for_status().json()
            self.assertIn("canonical_uid", eng_res)

            # Lock local profiles directory to force apply_engage / on_identify storage error
            target_prof = prof_root if prof_root.exists() else wal_prof_root
            _chmod_tree(target_prof, 0o555, 0o444)
            r_eng_err = client.engage({"distinct_id": "u_p25", "$set": {"should_fail": True}}, api_key=api_key)
            r_id_err = client.identify(
                {"anon_id": "a_p25_fail", "user_id": "u_p25_fail", "user_properties": {"x": 1}},
                api_key=api_key,
            )
            _chmod_tree(target_prof, 0o755, 0o644)

            self.assertIn(r_eng_err.status, (500, 503), f"apply_engage must propagate error, got {r_eng_err.status}")
            self.assertIn(r_id_err.status, (500, 503), f"on_identify must propagate error, got {r_id_err.status}")

    # ------------------------------------------------------------------
    # T4 Pairs (F26–F27)
    # ------------------------------------------------------------------

    @feature("F26", "F27")
    @require_milestone("T4")
    def test_p26_f26_f27_cold_s3_merge_and_agent_replay_local_hot_tier_idle_zero_s3(self) -> None:
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
            pid, api_key = _create_project(client, "P26 T4 Idle Zero")

            closed_ts = int(time.time() * 1000) - 2 * 86_400_000
            for i in range(2):
                client.ingest(
                    {"events": [make_event(event_name="p26_ev", distinct_id="u_p26", ts_ms=closed_ts + i * 100)]},
                    api_key=api_key,
                ).raise_for_status()
                client.admin_flush().raise_for_status()
                wait_until(lambda seq=i: f"published/{pid}/pub_{seq:012}.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            s3.reset_counts()
            client.replay_chunk(make_replay_chunk(session_id="rs_p26", project_id=pid, chunk_start_ts_ms=closed_ts), api_key=api_key, binary=True).raise_for_status()
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_p26", start_ts_ms=closed_ts), api_key=api_key, binary=True).raise_for_status()
            self.assertEqual(s3.put_count("replays/"), 0)
            self.assertEqual(s3.put_count("agent_segments/"), 0)
            self.assertEqual(s3.put_count("agent_manifests/"), 0)

            wait_until(lambda: any(k.startswith(f"replays/{pid}/") for k in s3.list_objects("replays/")), timeout=20.0)
            wait_until(lambda: any(k.startswith(f"agent_segments/{pid}/") for k in s3.list_objects("agent_segments/")), timeout=20.0)

            # Once settled and idle, S3 call count across multiple ticks must be 0
            time.sleep(2.5)
            s3.reset_counts()
            time.sleep(5.0)
            self.assertEqual(s3.total_calls(), 0, f"Expected 0 idle S3 calls, got {s3.get_counts()}")

    @feature("F19", "F27")
    @require_milestone("T4")
    def test_p27_f19_f27_replay_and_agent_cycle_upload_with_physical_erasure(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=2,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P27 Replay Agent Erasure")
            now_ms = int(time.time() * 1000)

            rc = client.replay_chunk(
                make_replay_chunk(session_id="rs_p27", project_id=pid, chunk_start_ts_ms=now_ms),
                api_key=api_key,
                binary=True,
            ).raise_for_status().json()
            self.assertIn("replay_offset_kb", rc)
            self.assertEqual(client.get_replay(pid, "rs_p27", byte_range="bytes=0-15").status, 206)

            client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p27_keep", start_ts_ms=now_ms),
                api_key=api_key,
            ).raise_for_status()
            client.ingest_agent_traces(
                make_agent_trace(trace_id="tr_p27_erase", start_ts_ms=now_ms + 10),
                api_key=api_key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()

            client.erase_agent(pid, level="trace", target_id="tr_p27_erase", expires_at=now_ms + 1500).raise_for_status()
            wait_until(lambda: any(k.startswith(f"replays/{pid}/") for k in s3.list_objects("replays/")), timeout=20.0)
            self.assertEqual(client.get_replay(pid, "rs_p27", byte_range="bytes=0-15").status, 206)

            # Even after tombstone expiry, erased trace must never reappear
            time.sleep(2.5)
            client.admin_flush().raise_for_status()
            self.assertEqual(client.get_agent_trace(pid, "tr_p27_erase").status, 404)
            self.assertEqual(client.get_agent_trace(pid, "tr_p27_keep").status, 200)

    # ------------------------------------------------------------------
    # W3 Pairs (F28–F31) & Cross-Milestone Interactions
    # ------------------------------------------------------------------

    @feature("F28", "F29")
    @require_milestone("W3")
    def test_p28_f28_f29_per_project_writer_log_and_exactly_once_offsets(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url = _shared_meta_url(cluster, "p28")
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=meta_url, writer_lease_ttl_secs=3)
            storage = topo["storages"][0]
            writer = topo["writers"][0]

            sc = storage.client()
            wc = writer.client()
            pid, api_key = _create_project(sc, "P28 Writer Log Offsets")
            now_ms = int(time.time() * 1000)

            evs = make_events(120, event_name="p28_ev", distinct_id_prefix="u_p28", base_ts_ms=now_ms, seed_prefix="p28")
            for i in range(0, 120, 20):
                r = wc.ingest({"events": evs[i : i + 20]}, api_key=api_key).raise_for_status().json()
                self.assertEqual(r["deduped"], 0)
            writer.kill()

            # Restart writer -> preserves writer_id & replays/dedups via (writer_id, bucket) offsets
            writer2 = writer.restart(graceful_stop=False)
            assert isinstance(writer2, NodeInstance)
            wait_until(
                lambda: _insights_total(sc, pid, "p28_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 120.0,
                timeout=20.0,
                desc="all 120 events delivered exactly once",
            )
            sc.admin_flush().raise_for_status()
            self.assertEqual(_insights_total(sc, pid, "p28_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 120.0)

    @feature("F29", "F30")
    @require_milestone("W3")
    def test_p29_f29_f30_project_owner_move_and_not_owner_forwarder_refresh(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url = _shared_meta_url(cluster, "p29")
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=2, database_url=meta_url, upload_interval_secs=2, writer_lease_ttl_secs=3)
            s0, s1 = topo["storages"]
            w0 = topo["writers"][0]
            c0 = s0.client()
            pid, api_key = _create_project(c0, "P29 Owner Move")
            now_ms = int(time.time() * 1000)

            # Determine initial owner from shared MetaStore
            if meta_url.startswith("sqlite://"):
                db_path = Path(meta_url[len("sqlite://") :])
                rows = cluster.node_mgr.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (pid,), db_path=db_path)
                self.assertEqual(len(rows), 1)
                cur_owner_name = rows[0]["node_name"]
                new_owner = s1 if cur_owner_name == "storage-0" else s0
                new_owner_name = "storage-1" if cur_owner_name == "storage-0" else "storage-0"
            else:
                new_owner = s1
                new_owner_name = "storage-1"

            w0.client().ingest({"events": make_events(5, event_name="p29_ev", distinct_id_prefix="u_p29_a", base_ts_ms=now_ms)}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: _insights_total(c0, pid, "p29_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 5.0,
                timeout=15.0,
            )

            # Update project_owner to new_owner -> forwarder refreshes on NotOwner and delivers subsequent batch
            if meta_url.startswith("sqlite://"):
                db_path = Path(meta_url[len("sqlite://") :])
                cluster.node_mgr.sqlite_query(
                    "UPDATE project_owner SET node_name = ?, address = ?, updated_at_ms = ? WHERE project_id = ?",
                    (new_owner_name, new_owner.base_url, int(time.time() * 1000), pid),
                    db_path=db_path,
                    readonly=False,
                )
            w0.client().ingest({"events": make_events(5, event_name="p29_ev", distinct_id_prefix="u_p29_b", base_ts_ms=now_ms + 1000)}, api_key=api_key).raise_for_status()
            wait_until(
                lambda: _insights_total(new_owner.client(), pid, "p29_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) >= 5.0,
                timeout=20.0,
                desc="forwarder refreshes project_owner and delivers to new owner",
            )

    @feature("F30", "F31")
    @require_milestone("W3")
    def test_p30_f30_f31_non_owner_guard_sync_query_forwarding_and_owner_down_503(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url = _shared_meta_url(cluster, "p30")
            topo = cluster.start_w3_cluster(num_writers=2, num_storage=2, database_url=meta_url, writer_lease_ttl_secs=3)
            s0, s1 = topo["storages"]
            w0, w1 = topo["writers"]
            sc = s0.client()

            p1, k1 = _create_project(sc, "P30_1")
            p2, k2 = _create_project(sc, "P30_2")
            now_ms = int(time.time() * 1000)

            # Sync profile call sent to writer W1 forwards transparently to owner
            id_resp = w1.client().identify({"anon_id": "a_p30", "user_id": "u_p30", "user_properties": {"tier": "pro"}}, api_key=k1).raise_for_status().json()
            self.assertIn("canonical_uid", id_resp)

            # Query sent to non-owner or writer forwards transparently
            u_resp = w0.client().get_user(p1, "u_p30").raise_for_status().json()
            self.assertEqual(u_resp["profile"]["props"]["tier"], "pro")

            # Identify owner of P1 and stop it -> sync/query for P1 return 503 while async /v1/ingest buffers 200
            owner_s = s0
            if meta_url.startswith("sqlite://"):
                rows = cluster.node_mgr.sqlite_query("SELECT node_name FROM project_owner WHERE project_id = ?", (p1,), db_path=Path(meta_url[len("sqlite://") :]))
                owner_s = s0 if rows[0]["node_name"] == "storage-0" else s1
            other_s = s1 if owner_s is s0 else s0
            owner_s.stop(graceful=True)

            self.assertEqual(w0.client().identify({"anon_id": "a_down", "user_id": "u_down"}, api_key=k1).status, 503)
            self.assertEqual(w0.client().query({"project_id": p1, "kind": "insights", "query": {"time_range": {"start_ts_ms": now_ms - 1000, "end_ts_ms": now_ms + 1000}, "series": [{"label": "x", "event_name": "e"}]}}).status, 503)
            self.assertEqual(w0.client().ingest({"events": [make_event(event_name="p30_buf", distinct_id="u1", ts_ms=now_ms)]}, api_key=k1).status, 200)
            self.assertEqual(w0.client().ingest_agent_traces(make_agent_trace(trace_id="tr_p30_buf", start_ts_ms=now_ms), api_key=k1).status, 200)

            # P2 on surviving storage node stays 200
            self.assertEqual(w0.client().identify({"anon_id": "a_p2", "user_id": "u_p2"}, api_key=k2).status, 200)
            self.assertEqual(other_s.client().get_user(p2, "u_p2").status, 200)

    @feature("F28", "F31")
    @require_milestone("W3")
    def test_p31_f28_f31_writer_pii_masking_before_log_and_async_agent_traces(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url = _shared_meta_url(cluster, "p31")
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=meta_url, writer_lease_ttl_secs=3)
            s0 = topo["storages"][0]
            w0 = topo["writers"][0]
            sc = s0.client()
            pid, api_key = _create_project(sc, "P31 PII Masking")
            now_ms = int(time.time() * 1000)

            sc.update_lexicon_property(pid, "pan", {"pii": True, "masking": "mask"}).raise_for_status()
            s0.stop(graceful=True)

            w0.client().ingest(
                {"events": [make_event(event_name="kyc_submit", distinct_id="u_p31", ts_ms=now_ms, props={"pan": "ABCDE1234F"})]},
                api_key=api_key,
            ).raise_for_status()
            w0.client().ingest_agent_traces(
                make_agent_trace(trace_id="tr_p31_async", start_ts_ms=now_ms),
                api_key=api_key,
            ).raise_for_status()

            wait_until(lambda: any("writer_log" in f for f in w0.local_files()), timeout=10.0)
            for rel in w0.local_files("writer_log"):
                self.assertNotIn(b"ABCDE1234F", w0.read_local_bytes(rel), f"Unmasked PII leaked to {rel}")

            s0_up = s0.restart(graceful_stop=True)
            assert isinstance(s0_up, NodeInstance)
            wait_until(
                lambda: _insights_total(s0_up.client(), pid, "kyc_submit", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 1.0,
                timeout=20.0,
            )
            wait_until(
                lambda: s0_up.client().get_agent_trace(pid, "tr_p31_async").status == 200,
                timeout=20.0,
            )

    @feature("F24", "F29")
    @require_milestone("W3")
    def test_p32_f24_f29_w3_forwarding_lag_metrics_and_no_hol_blocking(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            meta_url = _shared_meta_url(cluster, "p32")
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=meta_url, writer_lease_ttl_secs=3)
            s0 = topo["storages"][0]
            w0 = topo["writers"][0]
            sc = s0.client()
            p1, k1 = _create_project(sc, "P32_1")
            p2, k2 = _create_project(sc, "P32_2")
            now_ms = int(time.time() * 1000)

            s0.stop(graceful=True)
            w0.client().ingest({"events": make_events(25, event_name="p32_p1", distinct_id_prefix="u1", base_ts_ms=now_ms)}, api_key=k1).raise_for_status()
            wait_until(
                lambda: w0.client().parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") > 0.0,
                timeout=10.0,
                desc="forwarding/log lag metric rises while storage is down",
            )

            s0_up = s0.restart(graceful_stop=True)
            assert isinstance(s0_up, NodeInstance)
            w0.client().ingest({"events": make_events(10, event_name="p32_p2", distinct_id_prefix="u2", base_ts_ms=now_ms)}, api_key=k2).raise_for_status()
            wait_until(
                lambda: _insights_total(s0_up.client(), p2, "p32_p2", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 10.0,
                timeout=20.0,
            )
            wait_until(
                lambda: w0.client().parsed_metrics().sum_by_name("pulse_ingest_log_lag_bytes") == 0.0,
                timeout=20.0,
                desc="lag returns to 0 after drain",
            )

    @feature("F06", "F18")
    @require_milestone("T3")
    def test_p33_f06_f18_multipart_fsync_large_local_segment_durability(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=300,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            pid, api_key = _create_project(client, "P33 Large Local Seg Fsync")
            now_ms = int(time.time() * 1000)

            for b in range(4):
                evs = make_events(50, event_name="p33_ev", distinct_id_prefix=f"u_{b}", base_ts_ms=now_ms + b * 100, props={"blob": "Z" * 512})
                client.ingest({"events": evs}, api_key=api_key).raise_for_status()
            client.admin_flush().raise_for_status()
            node.kill()

            node2 = cluster.start_node(data_dir=node.data_dir, hot_store="local", upload_interval_secs=300, writer_lease_ttl_secs=3)
            assert isinstance(node2, NodeInstance)
            self.assertEqual(
                _insights_total(node2.client(), pid, "p33_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000),
                200.0,
            )

    @feature("F10", "F29")
    @require_milestone("W3")
    def test_p34_f10_f29_restore_preserves_writer_offsets_against_resend(self) -> None:
        pg_url = skip_if_no_postgres()
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            topo = cluster.start_w3_cluster(num_writers=1, num_storage=1, database_url=pg_url, upload_interval_secs=2, writer_lease_ttl_secs=3)
            s0 = topo["storages"][0]
            w0 = topo["writers"][0]
            pid, api_key = _create_project(s0.client(), "P34 Restore Offsets")
            now_ms = int(time.time() * 1000)

            w0.client().ingest({"events": make_events(10, event_name="p34_ev", distinct_id_prefix="u_p34", base_ts_ms=now_ms)}, api_key=api_key).raise_for_status()
            wait_until(lambda: _insights_total(s0.client(), pid, "p34_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000) == 10.0, timeout=15.0)
            s0.client().admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            pub0 = json.loads(s3.get_object(f"published/{pid}/pub_000000000000.json").decode("utf-8"))
            self.assertIn("writer_offsets", pub0)
            s0.kill()

            restored_dir = cluster.node_mgr.make_temp_data_dir("p34_restored_")
            res = cluster.node_mgr.run_cli(
                ["restore", "--from-published"],
                data_dir=restored_dir,
                env={**s3.env_vars(), "DATABASE_URL": pg_url},
            )
            self.assertEqual(res.returncode, 0, res.stderr)

            s0_restored = cluster.start_node(
                role="all",
                data_dir=restored_dir,
                database_url=pg_url,
                hot_store="local",
                node_name="storage-0",
                writer_lease_ttl_secs=3,
            )
            assert isinstance(s0_restored, NodeInstance)
            self.assertEqual(_insights_total(s0_restored.client(), pid, "p34_ev", start_ms=now_ms - 60_000, end_ms=now_ms + 60_000), 10.0)

    @feature("F15", "F30")
    @require_milestone("W3")
    def test_p35_f15_f30_owner_guard_and_publish_seq_fencing_prevent_split_brain(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            s3 = cluster.mock_s3
            assert s3 is not None
            meta_url = _shared_meta_url(cluster, "p35")
            topo = cluster.start_w3_cluster(num_writers=0, num_storage=2, database_url=meta_url, upload_interval_secs=2, writer_lease_ttl_secs=3)
            s0, s1 = topo["storages"]
            pid, api_key = _create_project(s0.client(), "P35 Owner SplitBrain")
            now_ms = int(time.time() * 1000)

            owner_s = s0
            non_owner_s = s1
            if meta_url.startswith("sqlite://"):
                rows = cluster.node_mgr.sqlite_query(
                    "SELECT node_name FROM project_owner WHERE project_id = ?",
                    (pid,),
                    db_path=Path(meta_url[len("sqlite://") :]),
                )
                owner_s = s0 if rows[0]["node_name"] == "storage-0" else s1
                non_owner_s = s1 if owner_s is s0 else s0

            owner_s.client().ingest({"events": [make_event(event_name="p35_ev", distinct_id="u1", ts_ms=now_ms)]}, api_key=api_key).raise_for_status()
            owner_s.client().admin_flush().raise_for_status()
            wait_until(lambda: f"published/{pid}/pub_000000000000.json" in s3.list_objects(f"published/{pid}/"), timeout=20.0)

            # Owner continues serving and writing cleanly (SlateDB unfenced)
            self.assertEqual(
                owner_s.client().ingest(
                    {"events": [make_event(event_name="p35_ev", distinct_id="u2", ts_ms=now_ms + 10)]},
                    api_key=api_key,
                ).status,
                200,
            )
            # Even when owner stops, non-owner MUST NOT hijack lease or write-open P1
            owner_s.stop(graceful=True)
            r_non = non_owner_s.client().identify({"anon_id": "a_hijack", "user_id": "u_hijack"}, api_key=api_key)
            self.assertEqual(r_non.status, 503, f"Non-owner must refuse/fail with 503 when owner is down, got {r_non.status}")
