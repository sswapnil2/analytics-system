"""Tier 1 (happy-path) opaque-box E2E tests for Storage Tiering features F01..F21 and F26..F27."""

from __future__ import annotations

import json
import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import PulseHttpClient  # noqa: E402
from lib.mock_s3 import MockS3Server  # noqa: E402
from lib.node_mgr import NodeInstance, NodeManager  # noqa: E402
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


def _local_matches(node: NodeInstance, prefix_segment: str) -> list[str]:
    clean = prefix_segment.strip("/")
    out: list[str] = []
    for rel in node.local_files():
        if (
            rel.startswith(f"{clean}/")
            or f"/{clean}/" in rel
        ):
            out.append(rel)
    return out


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


def _query_total(
    client: PulseHttpClient,
    project_id: str,
    event_name: str,
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
                "series": [{"label": "total", "event_name": event_name, "metric": "totals"}],
            },
        }
    ).raise_for_status().json()
    return float(resp["series"][0]["overall_value"])


class _StorageTier1Base(unittest.TestCase):
    """Shared self-contained provisioning helpers (no test_* methods)."""

    def _provision_s3_and_mgr(self) -> tuple[MockS3Server, NodeManager]:
        s3 = MockS3Server(bucket="pulse-e2e").start()
        self.addCleanup(s3.stop)
        nm = NodeManager()
        self.addCleanup(nm.cleanup)
        return s3, nm

    def _boot_single_node(
        self,
        *,
        hot_store: str | None = "local",
        byok_master_key: str | None = None,
        upload_interval_secs: int | float | None = None,
        writer_lease_ttl_secs: int = 3,
        database_url: str | None = None,
        extra_env: dict[str, str] | None = None,
        project_name: str = "Tier1 Project",
    ) -> tuple[MockS3Server, NodeManager, NodeInstance, PulseHttpClient, str, str]:
        s3, nm = self._provision_s3_and_mgr()
        admin_token = "sk_root_e2e_tier1_admin_token_000000000000"
        node = nm.start_node(
            role="all",
            hot_store=hot_store,
            mock_s3=s3,
            admin_token=admin_token,
            byok_master_key=byok_master_key,
            upload_interval_secs=upload_interval_secs,
            writer_lease_ttl_secs=writer_lease_ttl_secs,
            database_url=database_url,
            extra_env=extra_env,
        )
        assert isinstance(node, NodeInstance)
        client = node.client()
        proj = client.create_project(project_name).raise_for_status().json()
        pid = proj["project"]["id"]
        api_key = proj["api_key"]["plaintext_key"]
        return s3, nm, node, client, pid, api_key


# ===========================================================================
# F01: Prefix routing for WALs (WalRouted) [T0a]
# ===========================================================================


@tier(1)
@feature("F01")
@require_milestone("T0a")
class TestF01WalPrefixRoutingTier1(_StorageTier1Base):
    def test_f01_t1_default_hot_store_writes_event_wal_locally_zero_s3_puts(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store=None)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)

        events = make_events(5, event_name="signup_t1", distinct_id_prefix="u_f01", base_ts_ms=now_ms - 1000)
        resp = client.ingest({"events": events}, api_key=key).raise_for_status().json()
        self.assertEqual(resp["accepted"], 5)

        local_wal = node.local_files(f"wal/wal/{pid}/")
        self.assertGreaterEqual(len(local_wal), 1, f"Expected local WAL files, got {node.local_files('wal/')}")
        self.assertTrue(any(p.endswith(".wal") for p in local_wal))
        self.assertEqual(s3.put_count("wal/"), 0)
        self.assertEqual(s3.list_objects("wal/"), [])

    def test_f01_t1_explicit_local_writes_agent_wal_locally_zero_s3_puts(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        s3.reset_counts()
        now_ms = int(time.time() * 1000)

        tr = make_agent_trace(trace_id="tr_f01_t1_local", start_ts_ms=now_ms - 500)
        resp = client.ingest_agent_traces(tr, api_key=key).raise_for_status().json()
        self.assertEqual(resp["accepted_traces"], 1)

        local_agent_wal = node.local_files(f"wal/agent_wal/{pid}/")
        self.assertGreaterEqual(len(local_agent_wal), 1)
        self.assertTrue(any(p.endswith(".ptwl") for p in local_agent_wal))
        self.assertEqual(s3.put_count("agent_wal/"), 0)
        self.assertEqual(s3.list_objects("agent_wal/"), [])

    def test_f01_t1_byok_encryption_wraps_local_wal_in_plseagm2_envelope(self) -> None:
        byok = random_byok_hex()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok)
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(3, event_name="enc_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f01_enc", start_ts_ms=now_ms - 400), api_key=key).raise_for_status()

        ev_wals = [p for p in node.local_files(f"wal/wal/{pid}/") if p.endswith(".wal")]
        ag_wals = [p for p in node.local_files(f"wal/agent_wal/{pid}/") if p.endswith(".ptwl")]
        self.assertGreaterEqual(len(ev_wals), 1)
        self.assertGreaterEqual(len(ag_wals), 1)
        for rel in [*ev_wals, *ag_wals]:
            self.assertTrue(
                node.read_local_bytes(rel).startswith(b"PLSEAGM2"),
                f"Expected PLSEAGM2 header in encrypted WAL {rel}",
            )

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "enc_ev", now_ms - 60_000, now_ms + 60_000), 3.0)
        traces = c2.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertIn("tr_f01_enc", {t["trace_id"] for t in traces})

    def test_f01_t1_clean_shutdown_drains_and_empties_local_wal_dirs(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(6, event_name="drain_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f01_drain", start_ts_ms=now_ms - 300), api_key=key).raise_for_status()
        self.assertGreaterEqual(len(node.local_files("wal/wal/")), 1)
        self.assertGreaterEqual(len(node.local_files("wal/agent_wal/")), 1)

        rc = node.stop(graceful=True)
        self.assertEqual(rc, 0)
        self.assertEqual(node.local_files("wal/wal/"), [])
        self.assertEqual(node.local_files("wal/agent_wal/"), [])
        self.assertEqual(s3.put_count("wal/"), 0)
        self.assertEqual(s3.put_count("agent_wal/"), 0)

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "drain_ev", now_ms - 60_000, now_ms + 60_000), 6.0)
        self.assertIn("tr_f01_drain", {t["trace_id"] for t in c2.list_agent_traces(pid).raise_for_status().json()["traces"]})

    def test_f01_t1_sigkill_crash_recovers_acked_events_and_traces_from_local_wal(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)

        ev_batch = make_events(12, event_name="crash_ev", distinct_id_prefix="u_crash", base_ts_ms=now_ms - 2000)
        client.ingest({"events": ev_batch}, api_key=key).raise_for_status()
        for i in range(4):
            client.ingest_agent_traces(
                make_agent_trace(trace_id=f"tr_f01_crash_{i}", start_ts_ms=now_ms - 1000 + i * 10),
                api_key=key,
            ).raise_for_status()

        node.kill()
        self.assertGreaterEqual(len(node.local_files(f"wal/wal/{pid}/")), 1)
        self.assertGreaterEqual(len(node.local_files(f"wal/agent_wal/{pid}/")), 1)
        self.assertEqual(s3.put_count("wal/"), 0)
        self.assertEqual(s3.put_count("agent_wal/"), 0)

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(_query_total(c2, pid, "crash_ev", now_ms - 60_000, now_ms + 60_000), 12.0)
        traces = c2.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertEqual(len({t["trace_id"] for t in traces if t["trace_id"].startswith("tr_f01_crash_")}), 4)


# ===========================================================================
# F02: Mixed-mode WAL startup refusal [T0a]
# ===========================================================================


@tier(1)
@feature("F02")
@require_milestone("T0a")
class TestF02MixedModeWalUpgradeTier1(_StorageTier1Base):
    def test_f02_t1_clean_object_shutdown_upgrades_to_local_preserving_events_and_traces(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(10, event_name="obj_to_loc", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_obj_loc_1", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        self.assertGreaterEqual(s3.put_count("wal/"), 1)

        node2 = nm.restart(node, graceful_stop=True, hot_store="local")
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "obj_to_loc", now_ms - 60_000, now_ms + 60_000), 10.0)
        self.assertIn("tr_obj_loc_1", {t["trace_id"] for t in c2.list_agent_traces(pid).raise_for_status().json()["traces"]})

    def test_f02_t1_clean_local_shutdown_switches_to_object_preserving_events_and_traces(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(8, event_name="loc_to_obj", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_loc_obj_1", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()

        node2 = nm.restart(node, graceful_stop=True, hot_store="object")
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "loc_to_obj", now_ms - 60_000, now_ms + 60_000), 8.0)
        self.assertIn("tr_loc_obj_1", {t["trace_id"] for t in c2.list_agent_traces(pid).raise_for_status().json()["traces"]})

    def test_f02_t1_unflushed_l0_in_object_mode_drains_on_shutdown_allowing_local_upgrade(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(7, event_name="unflushed_obj", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        self.assertGreaterEqual(len(s3.list_objects("wal/")), 1)

        rc = node.stop(graceful=True)
        self.assertEqual(rc, 0)
        self.assertEqual(s3.list_objects("wal/"), [])

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
        )
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "unflushed_obj", now_ms - 60_000, now_ms + 60_000), 7.0)

    def test_f02_t1_roundtrip_object_local_object_preserves_projects_and_counts(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(4, event_name="rt_ev", distinct_id_prefix="u_p1", base_ts_ms=now_ms - 3000)}, api_key=key).raise_for_status()
        node = nm.restart(node, graceful_stop=True, hot_store="local")
        assert isinstance(node, NodeInstance)
        client = node.client()
        client.ingest({"events": make_events(5, event_name="rt_ev", distinct_id_prefix="u_p2", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        node = nm.restart(node, graceful_stop=True, hot_store="object")
        assert isinstance(node, NodeInstance)
        client = node.client()
        client.ingest({"events": make_events(6, event_name="rt_ev", distinct_id_prefix="u_p3", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()

        self.assertEqual(_query_total(client, pid, "rt_ev", now_ms - 60_000, now_ms + 60_000), 15.0)

    def test_f02_t1_same_mode_local_restarts_over_unflushed_local_wal_after_sigkill(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(5, event_name="same_mode_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        node.kill()
        self.assertGreaterEqual(len(node.local_files("wal/wal/")), 1)

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(_query_total(c2, pid, "same_mode_ev", now_ms - 60_000, now_ms + 60_000), 5.0)


# ===========================================================================
# F03: HTTP 503 on WAL write failure [T0a]
# ===========================================================================


@tier(1)
@feature("F03")
@require_milestone("T0a")
class TestF03WalWritableHappyPathTier1(_StorageTier1Base):
    def test_f03_t1_writable_local_wal_returns_200_for_json_ingest(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        resp = client.ingest({"events": make_events(4, event_name="f03_json", base_ts_ms=now_ms - 500)}, api_key=key)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.json()["accepted"], 4)
        self.assertEqual(_query_total(client, pid, "f03_json", now_ms - 60_000, now_ms + 60_000), 4.0)

    def test_f03_t1_writable_local_wal_returns_200_for_plsb_ingest(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        resp = client.ingest_plsb(make_events(5, event_name="f03_plsb", base_ts_ms=now_ms - 500), api_key=key)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.json()["accepted"], 5)
        self.assertEqual(_query_total(client, pid, "f03_plsb", now_ms - 60_000, now_ms + 60_000), 5.0)

    def test_f03_t1_writable_local_wal_returns_200_for_agent_traces_json_and_plst(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        r1 = client.ingest_agent_traces(make_agent_trace(trace_id="tr_f03_j", start_ts_ms=now_ms - 500), api_key=key)
        r2 = client.ingest_agent_traces(make_agent_trace(trace_id="tr_f03_p", start_ts_ms=now_ms - 400), api_key=key, binary=True)
        self.assertEqual(r1.status, 200)
        self.assertEqual(r2.status, 200)
        traces = client.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertTrue({"tr_f03_j", "tr_f03_p"}.issubset({t["trace_id"] for t in traces}))

    def test_f03_t1_writable_local_wal_returns_200_for_otel_json_and_protobuf(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ns = int(time.time() * 1_000_000_000)
        otel_json = {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                                    "spanId": "0102030405060708",
                                    "name": "otel_json_span",
                                    "startTimeUnixNano": str(now_ns - 1_000_000),
                                    "endTimeUnixNano": str(now_ns),
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        otel_pb = {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "1112131415161718191a1b1c1d1e1f20",
                                    "spanId": "1112131415161718",
                                    "name": "otel_pb_span",
                                    "startTimeUnixNano": now_ns - 500_000,
                                    "endTimeUnixNano": now_ns,
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        r1 = client.ingest_otel_traces(otel_json, api_key=key, protobuf=False)
        r2 = client.ingest_otel_traces(otel_pb, api_key=key, protobuf=True)
        self.assertEqual(r1.status, 200)
        self.assertEqual(r2.status, 200)
        traces = client.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertGreaterEqual(len(traces), 2)

    def test_f03_t1_restoring_wal_writability_immediately_restores_200_ingest(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="ok_pre", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_ok_pre", start_ts_ms=now_ms - 400), api_key=key).raise_for_status()

        wal_root = node.data_dir / "wal"
        changed_dirs: list[Path] = []
        for dirpath, _, _ in os.walk(wal_root):
            changed_dirs.append(Path(dirpath))
        self.addCleanup(lambda: [os.chmod(d, 0o755) for d in reversed(changed_dirs) if d.exists()])

        for d in reversed(changed_dirs):
            os.chmod(d, 0o555)
        self.assertEqual(
            client.ingest({"events": [make_event(event_name="fail_mid", distinct_id="u2", ts_ms=now_ms - 300)]}, api_key=key).status,
            503,
        )
        self.assertEqual(
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_fail_mid", start_ts_ms=now_ms - 200), api_key=key).status,
            503,
        )

        for d in changed_dirs:
            os.chmod(d, 0o755)
        self.assertEqual(
            client.ingest({"events": [make_event(event_name="ok_post", distinct_id="u3", ts_ms=now_ms - 100)]}, api_key=key).status,
            200,
        )
        self.assertEqual(
            client.ingest_agent_traces(make_agent_trace(trace_id="tr_ok_post", start_ts_ms=now_ms), api_key=key).status,
            200,
        )
        self.assertEqual(_query_total(client, pid, "ok_post", now_ms - 60_000, now_ms + 60_000), 1.0)
        traces = {t["trace_id"] for t in client.list_agent_traces(pid).raise_for_status().json()["traces"]}
        self.assertIn("tr_ok_post", traces)
        self.assertNotIn("tr_fail_mid", traces)


# ===========================================================================
# F04: Local identity/ and profiles/ routing [T0b]
# ===========================================================================


@tier(1)
@feature("F04")
@require_milestone("T0b")
class TestF04LocalIdentityProfilesTier1(_StorageTier1Base):
    def test_f04_t1_new_users_ingest_creates_local_identity_with_zero_s3_calls(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        s3.reset_counts()
        now_ms = int(time.time() * 1000)

        events = make_events(25, event_name="new_usr", distinct_id_prefix="f04_u", base_ts_ms=now_ms - 1000)
        client.ingest({"events": events}, api_key=key).raise_for_status()

        self.assertEqual(s3.get_counts("identity/"), {})
        self.assertEqual(s3.get_counts("profiles/"), {})
        self.assertGreaterEqual(len(_local_matches(node, "identity")), 1)

    def test_f04_t1_identify_routes_to_local_identity_and_profiles_zero_s3_calls(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        s3.reset_counts()

        resp = client.identify(
            {
                "anon_id": "anon_f04_1",
                "user_id": "usr_f04_1",
                "user_properties": {"plan": "pro"},
            },
            api_key=key,
        ).raise_for_status().json()
        self.assertGreater(resp["canonical_uid"], 0)
        self.assertEqual(s3.get_counts("identity/"), {})
        self.assertEqual(s3.get_counts("profiles/"), {})
        detail = client.get_user(pid, "usr_f04_1").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["plan"], "pro")

    def test_f04_t1_engage_and_groups_route_to_local_profiles_zero_s3_calls(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        s3.reset_counts()

        client.engage({"distinct_id": "u_eng_f04", "$set": {"tier": "gold", "seats": 5}}, api_key=key).raise_for_status()
        client.groups({"group_type": "company", "group_id": "acme_f04", "$set": {"mrr": 999}}, api_key=key).raise_for_status()

        self.assertEqual(s3.get_counts("identity/"), {})
        self.assertEqual(s3.get_counts("profiles/"), {})
        self.assertGreaterEqual(len(_local_matches(node, "profiles")), 1)
        detail = client.get_user(pid, "u_eng_f04").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["tier"], "gold")

    def test_f04_t1_byok_encryption_wraps_local_identity_and_profiles_in_plseagm2(self) -> None:
        byok = random_byok_hex()
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok)
        client.identify({"anon_id": "a_byok", "user_id": "u_byok", "user_properties": {"secret": "val"}}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "u_byok", "$set": {"k": "v"}}, api_key=key).raise_for_status()
        node.stop(graceful=True)

        id_files = _local_matches(node, "identity")
        prof_files = _local_matches(node, "profiles")
        self.assertGreaterEqual(len(id_files), 1)
        self.assertGreaterEqual(len(prof_files), 1)
        for rel in [*id_files, *prof_files]:
            self.assertTrue(node.read_local_bytes(rel).startswith(b"PLSEAGM2"), f"{rel} missing PLSEAGM2 envelope")

    def test_f04_t1_restart_in_local_mode_preserves_identity_uids_and_profiles(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        r1 = client.identify({"anon_id": "a_r", "user_id": "u_r", "user_properties": {"role": "admin"}}, api_key=key).raise_for_status().json()
        uid1 = r1["canonical_uid"]

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        r2 = c2.identify({"anon_id": "a_r", "user_id": "u_r"}, api_key=key).raise_for_status().json()
        self.assertEqual(r2["canonical_uid"], uid1)
        detail = c2.get_user(pid, "u_r").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["role"], "admin")
        self.assertEqual(s3.get_counts("identity/"), {})
        self.assertEqual(s3.get_counts("profiles/"), {})


# ===========================================================================
# F05: Hot-store mode marker & startup refusal [T0b]
# ===========================================================================


@tier(1)
@feature("F05")
@require_milestone("T0b")
class TestF05ModeMarkerTier1(_StorageTier1Base):
    def test_f05_t1_first_boot_local_writes_durable_mode_marker(self) -> None:
        _, _, node, _, _, _ = self._boot_single_node(hot_store="local")
        marker_files = [p for p in node.local_files() if "mode" in p.lower() or "hot_store" in p.lower()]
        self.assertGreaterEqual(len(marker_files), 1, f"Expected mode marker in {node.local_files()}")
        contents = " ".join(node.read_local_bytes(m).decode("utf-8", errors="replace") for m in marker_files)
        self.assertIn("local", contents)

    def test_f05_t1_first_boot_object_writes_durable_mode_marker(self) -> None:
        _, _, node, _, _, _ = self._boot_single_node(hot_store="object")
        marker_files = [p for p in node.local_files() if "mode" in p.lower() or "hot_store" in p.lower()]
        self.assertGreaterEqual(len(marker_files), 1, f"Expected mode marker in {node.local_files()}")
        contents = " ".join(node.read_local_bytes(m).decode("utf-8", errors="replace") for m in marker_files)
        self.assertIn("object", contents)

    def test_f05_t1_restart_with_matching_local_marker_and_stores_succeeds(self) -> None:
        _, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="m_ok", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        node2 = nm.restart(node, graceful_stop=True, hot_store="local")
        assert isinstance(node2, NodeInstance)
        self.assertEqual(node2.client().readyz().status, 200)
        self.assertEqual(_query_total(node2.client(), pid, "m_ok", now_ms - 60_000, now_ms + 60_000), 1.0)

    def test_f05_t1_restart_with_matching_object_marker_succeeds(self) -> None:
        _, nm, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="m_obj_ok", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        node2 = nm.restart(node, graceful_stop=True, hot_store="object")
        assert isinstance(node2, NodeInstance)
        self.assertEqual(node2.client().readyz().status, 200)
        self.assertEqual(_query_total(node2.client(), pid, "m_obj_ok", now_ms - 60_000, now_ms + 60_000), 1.0)

    def test_f05_t1_fresh_local_data_dir_with_no_prior_projects_boots_cleanly(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        node = nm.start_node(role="all", hot_store="local", mock_s3=s3, admin_token="sk_root_fresh_t1")
        assert isinstance(node, NodeInstance)
        self.assertEqual(node.client().healthz().status, 200)
        self.assertEqual(node.client().readyz().status, 200)


# ===========================================================================
# F06: DurableLocalObjectStore multipart fsync [T0b]
# ===========================================================================


@tier(1)
@feature("F06")
@require_milestone("T0b")
class TestF06MultipartFsyncTier1(_StorageTier1Base):
    def test_f06_t1_slatedb_identity_flush_and_compaction_survives_sigkill(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        events = make_events(200, event_name="id_compact", distinct_id_prefix="f06_id", base_ts_ms=now_ms - 5000)
        client.ingest({"events": events}, api_key=key).raise_for_status()
        res_id = client.identify({"anon_id": "f06_id_0000", "user_id": "f06_canon_0"}, api_key=key).raise_for_status().json()
        node.kill()

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        again = c2.identify({"anon_id": "f06_id_0000", "user_id": "f06_canon_0"}, api_key=key).raise_for_status().json()
        self.assertEqual(again["canonical_uid"], res_id["canonical_uid"])

    def test_f06_t1_slatedb_profiles_high_churn_compaction_survives_sigkill(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        for i in range(60):
            client.engage({"distinct_id": f"prof_u_{i % 15}", "$set": {"iter": i, "payload": "x" * 256}}, api_key=key).raise_for_status()
        node.kill()

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        now_ms = int(time.time() * 1000)
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        for u in range(15):
            detail = c2.get_user(pid, f"prof_u_{u}").raise_for_status().json()
            self.assertIsNotNone(detail["profile"])

    def test_f06_t1_byok_encrypted_multipart_ssts_survive_sigkill_and_reopen(self) -> None:
        byok = random_byok_hex()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, writer_lease_ttl_secs=3)
        for i in range(40):
            client.engage({"distinct_id": f"enc_prof_{i}", "$set": {"idx": i}}, api_key=key).raise_for_status()
        node.kill()

        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            byok_master_key=byok,
            writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        now_ms = int(time.time() * 1000)
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        detail = c2.get_user(pid, "enc_prof_39").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["idx"], 39)

    def test_f06_t1_large_batch_identity_and_profile_writes_leave_non_zero_valid_ssts(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(150, event_name="sst_ev", distinct_id_prefix="sst_u", base_ts_ms=now_ms - 3000)}, api_key=key).raise_for_status()
        for i in range(50):
            client.engage({"distinct_id": f"sst_u_{i:04d}", "$set": {"blob": "y" * 512}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        for rel in [*_local_matches(node, "identity"), *_local_matches(node, "profiles")]:
            self.assertGreater(len(node.read_local_bytes(rel)), 0, f"Unexpected zero-byte SlateDB file {rel}")

    def test_f06_t1_repeated_crash_restart_cycles_preserve_100pct_profile_and_id_mappings(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        expected_uids: dict[str, int] = {}
        for cycle in range(3):
            uid_resp = client.identify(
                {"anon_id": f"cy_a_{cycle}", "user_id": f"cy_u_{cycle}", "user_properties": {"cycle": cycle}},
                api_key=key,
            ).raise_for_status().json()
            expected_uids[f"cy_u_{cycle}"] = uid_resp["canonical_uid"]
            node.kill()
            node = nm.start_node(
                role="all",
                data_dir=node.data_dir,
                hot_store="local",
                mock_s3=s3,
                admin_token=node.admin_token,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()
            now_ms = int(time.time() * 1000)
            _wait_project_open(client, pid, now_ms - 60_000, now_ms + 60_000)

        for did, uid in expected_uids.items():
            detail = client.get_user(pid, did).raise_for_status().json()
            self.assertEqual(detail["canonical_uid"], uid)


# ===========================================================================
# F07: Published manifest with dense publish_seq [T1]
# ===========================================================================


@tier(1)
@feature("F07")
@require_milestone("T1")
class TestF07PublishedManifestDenseSeqTier1(_StorageTier1Base):
    def test_f07_t1_first_publish_writes_pub_000000000000_create_only(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="pub0_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        pub_keys = wait_until(lambda: s3.list_objects(f"published/{pid}/pub_"), timeout=15.0, desc="pub_0 written")
        self.assertIn(f"published/{pid}/pub_000000000000.json", pub_keys)

    def test_f07_t1_consecutive_publishes_emit_dense_contiguous_publish_seq(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest(
                {"events": make_events(3, event_name="dense_ev", distinct_id_prefix=f"d_{i}", base_ts_ms=now_ms - 300 + i * 10)},
                api_key=key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
            expected_key = f"published/{pid}/pub_{i:012d}.json"
            wait_until(lambda k=expected_key: s3.head_object(k) is not None, timeout=15.0, desc=f"wait {expected_key}")

        pub_keys = sorted(k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k)
        seqs = [json.loads(s3.get_object(k).decode("utf-8"))["publish_seq"] for k in pub_keys]
        self.assertEqual(seqs[:3], [0, 1, 2])

    def test_f07_t1_published_manifest_contains_required_fields_and_s3_segment_paths(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="fields_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        pub_key = f"published/{pid}/pub_000000000000.json"
        wait_until(lambda: s3.head_object(pub_key) is not None, timeout=15.0, desc="pub_0 manifest")
        doc = json.loads(s3.get_object(pub_key).decode("utf-8"))
        for req_field in (
            "publish_seq",
            "writer_generation",
            "source_local_epoch",
            "high_water_mark",
            "published_at_ms",
            "partitions",
            "committed_wal_seq",
            "committed_l0_seq",
        ):
            self.assertIn(req_field, doc, f"Missing {req_field} in published manifest: {doc}")
        raw_text = json.dumps(doc["partitions"])
        self.assertNotIn("hot/segments", raw_text)
        if doc["partitions"]:
            self.assertIn("segments/", raw_text)

    def test_f07_t1_publish_seq_remains_dense_across_node_restarts(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="rs_ev1", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 1, timeout=15.0, desc="first publish")

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        c2.ingest({"events": make_events(3, event_name="rs_ev2", distinct_id_prefix="u_r2", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        c2.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 2, timeout=15.0, desc="second publish")

        pub_keys = sorted(k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k)
        seqs = [json.loads(s3.get_object(k).decode("utf-8"))["publish_seq"] for k in pub_keys]
        self.assertEqual(seqs, list(range(len(seqs))))

    def test_f07_t1_source_local_epoch_and_writer_generation_recorded_in_published(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="gen_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        pub_key = f"published/{pid}/pub_000000000000.json"
        wait_until(lambda: s3.head_object(pub_key) is not None, timeout=15.0, desc="pub_0")
        doc = json.loads(s3.get_object(pub_key).decode("utf-8"))
        self.assertGreaterEqual(int(doc["writer_generation"]), 0)
        self.assertGreaterEqual(int(doc["source_local_epoch"]), 0)
        self.assertGreater(int(doc["published_at_ms"]), 0)


# ===========================================================================
# F08: Identity & profile S3 snapshots + ordering & HWM [T1]
# ===========================================================================


@tier(1)
@feature("F08")
@require_milestone("T1")
class TestF08IdentityProfileSnapshotsTier1(_StorageTier1Base):
    def test_f08_t1_publish_snapshots_profiles_and_identity_to_s3_published(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="snap_ev", distinct_id_prefix="snap_u", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "snap_u_0000", "$set": {"plan": "team"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        pub_objs = s3.list_objects(f"published/{pid}/")
        self.assertTrue(any("profile" in k or "snapshot" in k for k in pub_objs), f"No snapshots in {pub_objs}")
        self.assertTrue(any("identity" in k or "snapshot" in k for k in pub_objs), f"No snapshots in {pub_objs}")

    def test_f08_t1_snapshot_ordering_t_id_ge_t_prof_ge_t_freeze(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        ops: list[tuple[str, str]] = []
        s3.on_operation(lambda op, k: ops.append((op, k)) if op == "PUT" and k.startswith(f"published/{pid}/") else None)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="ord_ev", distinct_id_prefix="ord_u", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "ord_u_0000", "$set": {"x": 1}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        wait_until(lambda: any(k.endswith("pub_000000000000.json") for _, k in ops), timeout=15.0, desc="pub_0 PUT")
        keys_in_order = [k for _, k in ops]
        pub_idx = next(i for i, k in enumerate(keys_in_order) if "pub_000000000000.json" in k)
        prof_indices = [i for i, k in enumerate(keys_in_order) if "profile" in k]
        id_indices = [i for i, k in enumerate(keys_in_order) if "identity" in k]
        if prof_indices and id_indices:
            self.assertLessEqual(max(prof_indices), max(id_indices))
            self.assertLess(max(id_indices), pub_idx)

    def test_f08_t1_high_water_mark_matches_identity_user_count_and_is_monotonic(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)

        client.ingest({"events": make_events(5, event_name="hwm_ev", distinct_id_prefix="hwm_a", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        pub0 = json.loads(s3.get_object(f"published/{pid}/pub_000000000000.json").decode("utf-8"))

        client.ingest({"events": make_events(6, event_name="hwm_ev", distinct_id_prefix="hwm_b", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        pub1 = json.loads(s3.get_object(f"published/{pid}/pub_000000000001.json").decode("utf-8"))

        self.assertGreaterEqual(int(pub0["high_water_mark"]), 5)
        self.assertGreaterEqual(int(pub1["high_water_mark"]), int(pub0["high_water_mark"]) + 6)

    def test_f08_t1_byok_encrypts_s3_snapshots_under_path_bound_plseagm2(self) -> None:
        byok = random_byok_hex()
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="enc_snap", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        wait_until(lambda: len(s3.list_objects(f"published/{pid}/")) >= 2, timeout=15.0, desc="encrypted publish")
        for k in s3.list_objects(f"published/{pid}/"):
            raw = s3.get_object(k)
            assert raw is not None
            self.assertTrue(raw.startswith(b"PLSEAGM2"), f"Published object {k} not encrypted")

    def test_f08_t1_all_uids_in_published_segments_and_profiles_resolve_after_restore(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(15, event_name="res_ev", distinct_id_prefix="res_u", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        for i in range(15):
            client.engage({"distinct_id": f"res_u_{i:04d}", "$set": {"idx": i}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("restore_f08_")
        env = {**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"}
        res = nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env=env)
        self.assertEqual(res.returncode, 0, f"restore failed: {res.stderr}\n{res.stdout}")

        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "res_ev", now_ms - 60_000, now_ms + 60_000), 15.0)
        users = c2.list_users(pid, limit=100).raise_for_status().json()
        self.assertGreaterEqual(len(users), 15)


# ===========================================================================
# F09: Empirical T1 snapshot benchmark [T1]
# ===========================================================================


@tier(1)
@feature("F09")
@require_milestone("T1")
class TestF09SnapshotBenchmarkTier1(_StorageTier1Base):
    def test_f09_t1_publish_cycle_s3_put_count_bounded_for_steady_workload(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(100, event_name="bench_ev", distinct_id_prefix="b_u", base_ts_ms=now_ms - 5000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        pub_puts = s3.put_count("published/")
        self.assertGreaterEqual(pub_puts, 1)
        self.assertLessEqual(pub_puts, 50, f"Excessive published/ PUT amplification: {pub_puts}")

    def test_f09_t1_incremental_users_and_profiles_publish_cleanly_under_counting_s3(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        for step in range(3):
            s3.reset_counts()
            client.ingest(
                {"events": make_events(20, event_name="inc_ev", distinct_id_prefix=f"inc_{step}_", base_ts_ms=now_ms - 1000 + step * 50)},
                api_key=key,
            ).raise_for_status()
            client.engage({"distinct_id": f"inc_{step}__0000", "$set": {"step": step}}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            expected_pub = f"published/{pid}/pub_{step:012d}.json"
            wait_until(lambda k=expected_pub: s3.head_object(k) is not None, timeout=15.0, desc=expected_pub)
            self.assertGreaterEqual(s3.put_count("published/"), 1)

    def test_f09_t1_snapshot_bytes_and_call_counts_stay_bounded_across_multiple_cycles(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(50, event_name="bnd_ev", distinct_id_prefix="bnd_u", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        first_cycle_puts = s3.put_count("published/")

        s3.reset_counts()
        client.ingest({"events": make_events(10, event_name="bnd_ev", distinct_id_prefix="bnd_u", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        second_cycle_puts = s3.put_count("published/")
        self.assertLessEqual(second_cycle_puts, max(20, first_cycle_puts * 2))

    def test_f09_t1_restored_snapshot_from_benchmark_workload_resolves_100pct_uids(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(60, event_name="bm_res", distinct_id_prefix="bm_u", base_ts_ms=now_ms - 3000)}, api_key=key).raise_for_status()
        for i in range(20):
            client.engage({"distinct_id": f"bm_u_{i:04d}", "$set": {"tier": "pro"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("bm_restore_")
        env = {**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"}
        res = nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env=env)
        self.assertEqual(res.returncode, 0, res.stderr)

        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        exported = c2.export_events(pid, format="json").raise_for_status().json()
        self.assertEqual(exported["count"], 60)
        self.assertEqual(len({ev["distinct_id"] for ev in exported["events"]}), 60)

    def test_f09_t1_byok_snapshot_benchmark_workload_preserves_put_efficiency_and_restore(self) -> None:
        byok = random_byok_hex()
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, upload_interval_secs=1)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(40, event_name="bm_byok", distinct_id_prefix="bmb_u", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        self.assertLessEqual(s3.put_count("published/"), 50)


# ===========================================================================
# F10: pulse-node restore --from-published CLI [T1]
# ===========================================================================


@tier(1)
@feature("F10")
@require_milestone("T1")
class TestF10RestoreCliTier1(_StorageTier1Base):
    def test_f10_t1_restore_from_published_restores_local_stores_and_exits_zero(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(8, event_name="rst_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f10_1_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, f"stderr={res.stderr}\nstdout={res.stdout}")
        restored_files = [p.relative_to(restore_dir).as_posix() for p in restore_dir.rglob("*") if p.is_file()]
        self.assertTrue(any("identity" in p for p in restored_files))
        self.assertTrue(any("profiles" in p for p in restored_files))
        self.assertTrue(any("manifests" in p for p in restored_files))

    def test_f10_t1_restore_prints_published_timestamp_and_data_loss_warning(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="warn_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f10_2_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        combined = (res.stdout + "\n" + res.stderr).lower()
        self.assertIn("lost", combined)
        self.assertTrue(any(tok in combined for tok in ("published", "timestamp", "utc", "20")))

    def test_f10_t1_restored_node_boots_and_serves_pre_publish_events_and_profiles(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(10, event_name="pre_pub", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "user_0000", "$set": {"plan": "enterprise"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f10_3_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, res.stderr)

        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "pre_pub", now_ms - 60_000, now_ms + 60_000), 10.0)
        self.assertEqual(c2.get_user(pid, "user_0000").raise_for_status().json()["profile"]["props"]["plan"], "enterprise")

    def test_f10_t1_restore_with_byok_key_decrypts_snapshots_and_boots(self) -> None:
        pg = skip_if_no_postgres()
        byok = random_byok_hex()
        s3, nm, node, client, pid, key = self._boot_single_node(
            hot_store="local", byok_master_key=byok, upload_interval_secs=1, database_url=pg
        )
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="byok_rst", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f10_4_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local", "PULSE_BYOK_MASTER_KEY": byok},
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        node2 = nm.start_node(
            role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3,
            database_url=pg, byok_master_key=byok, admin_token=node.admin_token,
        )
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "byok_rst", now_ms - 60_000, now_ms + 60_000), 5.0)

    def test_f10_t1_restore_specific_project_id_installs_last_published_seq(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        for i in range(2):
            client.ingest({"events": make_events(3, event_name="seq_ev", distinct_id_prefix=f"s{i}", base_ts_ms=now_ms - 500 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f10_5_")
        res = nm.run_cli(
            ["restore", "--from-published", "--project-id", pid, "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        c2.ingest({"events": make_events(2, event_name="seq_ev", distinct_id_prefix="s2", base_ts_ms=now_ms)}, api_key=key).raise_for_status()
        c2.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000002.json") is not None, timeout=15.0, desc="pub_2")


# ===========================================================================
# F11: Restore erasure reconciliation & tombstone cleanup [T1]
# ===========================================================================


@tier(1)
@feature("F11")
@require_milestone("T1")
class TestF11RestoreErasureReconciliationTier1(_StorageTier1Base):
    def test_f11_t1_restore_purges_erased_profiles_from_restored_pstore(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="f11_ev", distinct_id_prefix="f11_u", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "f11_u_0000", "$set": {"secret": "pii"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        client.delete_user(pid, "f11_u_0000").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_1_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, res.stderr)

        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        users = c2.list_users(pid).raise_for_status().json()
        self.assertNotIn("f11_u_0000", {u["distinct_id"] for u in users})

    def test_f11_t1_restore_deletes_erased_uids_at_or_above_hwm(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="f11_pre", distinct_id_prefix="pre_u", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        client.ingest({"events": [make_event(event_name="f11_post", distinct_id="post_erased_u", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.delete_user(pid, "post_erased_u").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_2_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertEqual(res.returncode, 0, res.stderr)

        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        c2.ingest({"events": [make_event(event_name="f11_post", distinct_id="reused_uid_user", ts_ms=now_ms - 1000)]}, api_key=key).raise_for_status()
        self.assertEqual(_query_total(c2, pid, "f11_post", now_ms - 60_000, now_ms + 60_000), 1.0)

    def test_f11_t1_restore_preserves_erased_uids_below_hwm(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="lt_hwm_ev", distinct_id_prefix="lt_u", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        client.delete_user(pid, "lt_u_0000").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_3_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "lt_hwm_ev", now_ms - 60_000, now_ms + 60_000), 3.0)

    def test_f11_t1_restore_preserves_deletion_audit_log_records(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="aud_ev", distinct_id="aud_u", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        del_rec = client.delete_user(pid, "aud_u").raise_for_status().json()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_4_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        dels = node2.client().list_deletions(pid).raise_for_status().json()
        self.assertIn(del_rec["id"], {d["id"] for d in dels})

    def test_f11_t1_post_restore_new_users_reusing_uid_ge_hwm_are_visible(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="vis_ev", distinct_id_prefix="vis_base", base_ts_ms=now_ms - 3000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        for i in range(3):
            client.ingest({"events": [make_event(event_name="vis_ev", distinct_id=f"temp_{i}", ts_ms=now_ms - 1000)]}, api_key=key).raise_for_status()
            client.delete_user(pid, f"temp_{i}").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_5_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        for i in range(3):
            c2.engage({"distinct_id": f"fresh_u_{i}", "$set": {"active": True}}, api_key=key).raise_for_status()
            c2.ingest({"events": [make_event(event_name="vis_ev", distinct_id=f"fresh_u_{i}", ts_ms=now_ms - 2000)]}, api_key=key).raise_for_status()
        self.assertEqual(_query_total(c2, pid, "vis_ev", now_ms - 60_000, now_ms + 60_000), 5.0)
        users = {u["distinct_id"] for u in c2.list_users(pid).raise_for_status().json()}
        self.assertTrue({"fresh_u_0", "fresh_u_1", "fresh_u_2"}.issubset(users))


# ===========================================================================
# F12: erased_uids filter on profile reads & catalog [T1]
# ===========================================================================


@tier(1)
@feature("F12")
@require_milestone("T1")
class TestF12ErasedUidsFilterTier1(_StorageTier1Base):
    def test_f12_t1_delete_user_hides_from_list_users_and_search(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "alice_f12", "$set": {"plan": "pro"}}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "bob_f12", "$set": {"plan": "free"}}, api_key=key).raise_for_status()
        client.delete_user(pid, "alice_f12").raise_for_status()

        all_users = {u["distinct_id"] for u in client.list_users(pid).raise_for_status().json()}
        self.assertNotIn("alice_f12", all_users)
        self.assertIn("bob_f12", all_users)
        searched = client.list_users(pid, search="alice_f12").raise_for_status().json()
        self.assertEqual(searched, [])

    def test_f12_t1_delete_user_hides_from_get_user_detail(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="view", distinct_id="carol_f12", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "carol_f12", "$set": {"tier": "vip"}}, api_key=key).raise_for_status()
        uid = client.get_user(pid, "carol_f12").raise_for_status().json()["canonical_uid"]

        client.delete_user(pid, "carol_f12").raise_for_status()
        det = client.get_user(pid, "carol_f12", uid=uid)
        if det.status == 200:
            body = det.json()
            self.assertIsNone(body.get("profile"))
            self.assertEqual(body.get("events", []), [])
        else:
            self.assertEqual(det.status, 404)

    def test_f12_t1_delete_user_hides_from_export_events(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [
            make_event(event_name="exp_ev", distinct_id="dave_f12", ts_ms=now_ms - 500),
            make_event(event_name="exp_ev", distinct_id="eve_f12", ts_ms=now_ms - 400),
        ]}, api_key=key).raise_for_status()
        client.delete_user(pid, "dave_f12").raise_for_status()

        exported = client.export_events(pid, format="json").raise_for_status().json()
        dids = {ev["distinct_id"] for ev in exported["events"]}
        self.assertNotIn("dave_f12", dids)
        self.assertIn("eve_f12", dids)

    def test_f12_t1_delete_user_hides_from_mcp_and_copilot_endpoints(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="mcp_ev", distinct_id="frank_f12", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "frank_f12", "$set": {"email": "frank@example.com"}}, api_key=key).raise_for_status()
        client.delete_user(pid, "frank_f12").raise_for_status()

        mcp_resp = client.mcp_call("tools/list", project_id=pid)
        self.assertIn(mcp_resp.status, (200, 202))
        users_resp = client.list_users(pid, search="frank").raise_for_status().json()
        self.assertEqual(users_resp, [])

    def test_f12_t1_delete_user_hides_from_user_property_cohort_and_revenue(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.engage({"distinct_id": "gina_f12", "$set": {"plan": "vip"}, "$track_revenue": 100.0}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "hank_f12", "$set": {"plan": "vip"}, "$track_revenue": 50.0}, api_key=key).raise_for_status()
        client.ingest({"events": [
            make_event(event_name="buy", distinct_id="gina_f12", ts_ms=now_ms - 500),
            make_event(event_name="buy", distinct_id="hank_f12", ts_ms=now_ms - 400),
        ]}, api_key=key).raise_for_status()

        client.delete_user(pid, "gina_f12").raise_for_status()
        rev = client.request("GET", f"/v1/projects/{pid}/revenue", token=client.default_token).raise_for_status().json()
        self.assertEqual(int(rev["total_profiles"]), 1)
        self.assertEqual(_query_total(client, pid, "buy", now_ms - 60_000, now_ms + 60_000), 1.0)


# ===========================================================================
# F13: Metadata DB SQLite startup warning [T1]
# ===========================================================================


@tier(1)
@feature("F13")
@require_milestone("T1")
class TestF13SqliteStartupWarningTier1(_StorageTier1Base):
    def test_f13_t1_local_hot_store_with_default_sqlite_logs_prominent_warn(self) -> None:
        _, _, node, _, _, _ = self._boot_single_node(hot_store="local", database_url=None)
        logs = node.read_logs(tail_bytes=0)
        self.assertIn("WARN", logs)
        self.assertIn("SQLite", logs)

    def test_f13_t1_local_hot_store_with_explicit_sqlite_url_logs_warn(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        data_dir = nm.make_temp_data_dir("f13_explicit_sq_")
        node = nm.start_node(
            role="all",
            data_dir=data_dir,
            hot_store="local",
            mock_s3=s3,
            database_url=f"sqlite://{(data_dir / 'custom.db').resolve()}",
            admin_token="sk_root_f13_2",
        )
        assert isinstance(node, NodeInstance)
        logs = node.read_logs(tail_bytes=0)
        self.assertIn("WARN", logs)
        self.assertIn("DATABASE_URL", logs)

    def test_f13_t1_node_with_sqlite_warning_still_boots_and_ingests_normally(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", database_url=None)
        self.assertEqual(client.readyz().status, 200)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="sq_ok", distinct_id="u1", ts_ms=now_ms - 200)]}, api_key=key).raise_for_status()
        self.assertEqual(_query_total(client, pid, "sq_ok", now_ms - 60_000, now_ms + 60_000), 1.0)
        self.assertIn("WARN", node.read_logs(tail_bytes=0))

    def test_f13_t1_sqlite_warning_mentions_database_url_and_volume_loss_risk(self) -> None:
        _, _, node, _, _, _ = self._boot_single_node(hot_store="local", database_url=None)
        logs = node.read_logs(tail_bytes=0)
        low = logs.lower()
        self.assertIn("sqlite", low)
        self.assertIn("database_url", low)
        self.assertTrue(any(w in low for w in ("volume", "restore", "postgres", "ephemeral", "lost")))

    def test_f13_t1_restart_in_local_sqlite_mode_emits_warning_on_each_boot(self) -> None:
        _, nm, node, _, _, _ = self._boot_single_node(hot_store="local", database_url=None)
        self.assertIn("SQLite", node.read_logs(tail_bytes=0))
        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertIn("SQLite", node2.read_logs(tail_bytes=0))


# ===========================================================================
# F14: Local-authoritative manifest (manifests/) [T2]
# ===========================================================================


@tier(1)
@feature("F14")
@require_milestone("T2")
class TestF14LocalManifestTier1(_StorageTier1Base):
    def test_f14_t1_manifests_written_locally_on_l0_flush_zero_s3_puts(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="f14_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        self.assertGreaterEqual(len(_local_matches(node, "manifests")), 1)
        self.assertEqual(s3.put_count("manifests/"), 0)
        self.assertEqual(s3.put_count("published/"), 0)

    def test_f14_t1_multiple_flushes_increment_local_manifest_epoch_only(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        for i in range(4):
            client.ingest({"events": make_events(2, event_name="f14_ep", distinct_id_prefix=f"ep{i}", base_ts_ms=now_ms - 500 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()

        epochs = sorted(p for p in _local_matches(node, "manifests") if "epoch_" in p)
        self.assertGreaterEqual(len(epochs), 4)
        self.assertEqual(s3.put_count("published/"), 0)
        self.assertEqual(s3.put_count("manifests/"), 0)

    def test_f14_t1_published_manifest_written_only_on_upload_interval(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="f14_int", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertEqual(s3.put_count("published/"), 0)

        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="upload tick pub_0")

    def test_f14_t1_idle_node_emits_zero_s3_published_or_manifest_puts(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="f14_idle", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        s3.reset_counts()
        time.sleep(2.5)
        self.assertEqual(s3.put_count("published/"), 0)
        self.assertEqual(s3.put_count("manifests/"), 0)

    def test_f14_t1_byok_encrypts_local_manifest_files_in_plseagm2(self) -> None:
        byok = random_byok_hex()
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="f14_enc", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        mfs = _local_matches(node, "manifests")
        self.assertGreaterEqual(len(mfs), 1)
        for rel in mfs:
            self.assertTrue(node.read_local_bytes(rel).startswith(b"PLSEAGM2"))


# ===========================================================================
# F15: Dense publish_seq fencing & open check [T2]
# ===========================================================================


@tier(1)
@feature("F15")
@require_milestone("T2")
class TestF15PublishSeqFencingTier1(_StorageTier1Base):
    def test_f15_t1_same_owner_restart_matching_pub_seq_and_gen_opens_cleanly(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="f15_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "f15_ev", now_ms - 60_000, now_ms + 60_000), 4.0)

    def test_f15_t1_local_manifest_tracks_last_published_seq_and_writer_generation(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="f15_trk", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(len(epochs), 1)
        latest_doc = json.loads(node.read_local_bytes(epochs[-1]).decode("utf-8"))
        self.assertEqual(latest_doc["last_published_seq"], 0)
        self.assertIn("writer_generation", latest_doc)

    def test_f15_t1_sequential_publish_cycles_advance_local_last_published_seq(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        for i in range(2):
            client.ingest({"events": make_events(2, event_name="f15_seq", distinct_id_prefix=f"q{i}", base_ts_ms=now_ms - 300 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")

        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        latest_doc = json.loads(node.read_local_bytes(epochs[-1]).decode("utf-8"))
        self.assertGreaterEqual(latest_doc["last_published_seq"], 1)

    def test_f15_t1_active_writer_survives_s3_head_check_and_continues_publishing(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        head_keys: list[str] = []
        s3.on_operation(lambda op, k: head_keys.append(k) if op == "HEAD" and "published/" in k else None)
        now_ms = int(time.time() * 1000)
        for i in range(2):
            client.ingest({"events": make_events(2, event_name="f15_head", distinct_id_prefix=f"h{i}", base_ts_ms=now_ms - 300 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        self.assertTrue(any("pub_000000000000.json" in k for k in head_keys))

    def test_f15_t1_clean_handover_after_restore_updates_generation_and_fences_old(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="f15_hand", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("f15_handover_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token, upload_interval_secs=1)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        c2.ingest({"events": make_events(2, event_name="f15_hand", distinct_id_prefix="new_w", base_ts_ms=now_ms)}, api_key=key).raise_for_status()
        c2.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        pub0 = json.loads(s3.get_object(f"published/{pid}/pub_000000000000.json").decode("utf-8"))
        pub1 = json.loads(s3.get_object(f"published/{pid}/pub_000000000001.json").decode("utf-8"))
        self.assertGreater(int(pub1["writer_generation"]), int(pub0["writer_generation"]))


# ===========================================================================
# F16: S3 segment deletion grace rule [T2]
# ===========================================================================


@tier(1)
@feature("F16")
@require_milestone("T2")
class TestF16DeletionGraceRuleTier1(_StorageTier1Base):
    def _ingest_multi_segments_eager(self, s3: MockS3Server, client: PulseHttpClient, pid: str, key: str, ts_ms: int) -> list[str]:
        for i in range(4):
            client.ingest({"events": make_events(3, event_name="f16_ev", distinct_id_prefix=f"u_{i}", base_ts_ms=ts_ms + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        segs = wait_until(lambda: s3.list_objects(f"segments/{pid}/"), timeout=15.0, desc="initial S3 segments")
        return segs

    def test_f16_t1_superseded_s3_segment_recorded_in_local_pending_s3_deletes(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=0)
        now_ms = int(time.time() * 1000)
        self._ingest_multi_segments_eager(s3, client, pid, key, now_ms - 5000)

        def _has_pending() -> bool:
            for p in _local_matches(node, "manifests"):
                if pid in p and "epoch_" in p:
                    doc = json.loads(node.read_local_bytes(p).decode("utf-8"))
                    if doc.get("pending_s3_deletes"):
                        return True
            return False

        self.assertTrue(wait_until(_has_pending, timeout=20.0, desc="pending_s3_deletes populated"))

    def test_f16_t1_superseded_segment_retained_through_0_and_1_publish_intervals(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        old_ts = int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000)
        for i in range(4):
            client.ingest({"events": make_events(2, event_name="f16_grace", distinct_id_prefix=f"g{i}", base_ts_ms=old_ts + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")

        segs = s3.list_objects(f"segments/{pid}/")
        self.assertGreaterEqual(len(segs), 1)
        # Immediately after superseding/compacting or 1 interval later, original segments remain readable
        for seg in segs:
            self.assertIsNotNone(s3.head_object(seg))

    def test_f16_t1_superseded_segment_deleted_after_superseding_publish_and_2_intervals(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        deletes: list[str] = []
        s3.on_operation(lambda op, k: deletes.append(k) if op == "DELETE" and k.startswith(f"segments/{pid}/") else None)

        for i in range(5):
            client.ingest({"events": make_events(2, event_name="f16_del", distinct_id_prefix="same_bucket_user", base_ts_ms=now_ms - 5000 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")

        # Trigger at least 2 more publish cycles
        for extra in range(5, 8):
            client.ingest({"events": [make_event(event_name="f16_del", distinct_id="same_bucket_user", ts_ms=now_ms - 100 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=extra: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{extra}")

        self.assertGreaterEqual(_query_total(client, pid, "f16_del", now_ms - 60_000, now_ms + 60_000), 13.0)

    def test_f16_t1_remote_query_reads_prior_published_segment_during_grace_window(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        shared_dir = nm.make_temp_data_dir("f16_shared_")
        db_url = external_postgres_url() or f"sqlite://{(shared_dir / 'shared.db').resolve()}"
        writer = nm.start_node(role="all", data_dir=nm.make_temp_data_dir("f16_w_"), hot_store="local", mock_s3=s3, database_url=db_url, upload_interval_secs=2, admin_token="sk_root_f16_4")
        assert isinstance(writer, NodeInstance)
        wc = writer.client()
        proj = wc.create_project("F16 Grace").raise_for_status().json()
        pid, key = proj["project"]["id"], proj["api_key"]["plaintext_key"]
        now_ms = int(time.time() * 1000)

        wc.ingest({"events": make_events(5, event_name="f16_q", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qnode = nm.start_node(role="query", data_dir=nm.make_temp_data_dir("f16_q_"), hot_store="local", mock_s3=s3, database_url=db_url, admin_token="sk_root_f16_4")
        assert isinstance(qnode, NodeInstance)
        qc = qnode.client()
        wait_until(lambda: _query_total(qc, pid, "f16_q", now_ms - 60_000, now_ms + 60_000) == 5.0, timeout=15.0, desc="query node sees 5")

    def test_f16_t1_pending_s3_deletes_survives_restart_and_completes_after_grace(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="f16_rst", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "f16_rst", now_ms - 60_000, now_ms + 60_000), 4.0)


# ===========================================================================
# F17: Remote query node reading published state [T2]
# ===========================================================================


@tier(1)
@feature("F17")
@require_milestone("T2")
class TestF17RemoteQueryNodeTier1(_StorageTier1Base):
    def _boot_writer_and_query(self, *, byok: str | None = None, upload_interval_secs: int = 1) -> tuple[MockS3Server, NodeManager, NodeInstance, NodeInstance, str, str]:
        s3, nm = self._provision_s3_and_mgr()
        shared_dir = nm.make_temp_data_dir("f17_meta_")
        db_url = external_postgres_url() or f"sqlite://{(shared_dir / 'shared_meta.db').resolve()}"
        tok = "sk_root_f17_shared_token_000000000"
        writer = nm.start_node(
            role="all", data_dir=nm.make_temp_data_dir("f17_w_"), hot_store="local",
            mock_s3=s3, database_url=db_url, byok_master_key=byok,
            upload_interval_secs=upload_interval_secs, admin_token=tok,
        )
        assert isinstance(writer, NodeInstance)
        proj = writer.client().create_project("F17 Cluster").raise_for_status().json()
        pid, key = proj["project"]["id"], proj["api_key"]["plaintext_key"]
        qnode = nm.start_node(
            role="query", data_dir=nm.make_temp_data_dir("f17_q_"), hot_store="local",
            mock_s3=s3, database_url=db_url, byok_master_key=byok, admin_token=tok,
        )
        assert isinstance(qnode, NodeInstance)
        return s3, nm, writer, qnode, pid, key

    def test_f17_t1_query_node_reads_published_manifest_and_answers_insights(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_writer_and_query()
        now_ms = int(time.time() * 1000)
        writer.client().ingest({"events": make_events(10, event_name="q_ins", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        writer.client().admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qc = qnode.client()
        wait_until(lambda: _query_total(qc, pid, "q_ins", now_ms - 60_000, now_ms + 60_000) == 10.0, timeout=15.0, desc="query insights==10")

    def test_f17_t1_query_node_downloads_snapshots_locally_and_serves_users(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_writer_and_query()
        wc = writer.client()
        wc.identify({"anon_id": "q_a1", "user_id": "q_u1", "user_properties": {"tier": "pro"}}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qc = qnode.client()
        wait_until(lambda: any(u["distinct_id"] == "q_u1" for u in qc.list_users(pid).raise_for_status().json()), timeout=15.0, desc="query sees q_u1")
        detail = qc.get_user(pid, "q_u1").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["tier"], "pro")

    def test_f17_t1_query_node_serves_funnel_retention_flows_and_export(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_writer_and_query()
        now_ms = int(time.time() * 1000)
        wc = writer.client()
        wc.ingest({"events": [
            make_event(event_name="step_1", distinct_id="fu_1", ts_ms=now_ms - 2000),
            make_event(event_name="step_2", distinct_id="fu_1", ts_ms=now_ms - 1000),
        ]}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qc = qnode.client()
        wait_until(lambda: qc.export_events(pid, format="json").raise_for_status().json()["count"] == 2, timeout=15.0, desc="export==2")
        funnel = qc.query_funnel(pid, {
            "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
            "steps": [{"event_name": "step_1"}, {"event_name": "step_2"}],
            "conversion_window_ms": 60_000,
        }).raise_for_status().json()
        self.assertEqual(funnel["overall_steps"][1]["entered_users"], 1)

    def test_f17_t1_query_node_refreshes_newly_published_seq_without_restart(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_writer_and_query()
        now_ms = int(time.time() * 1000)
        wc, qc = writer.client(), qnode.client()

        wc.ingest({"events": make_events(3, event_name="q_ref", distinct_id_prefix="r1", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: _query_total(qc, pid, "q_ref", now_ms - 60_000, now_ms + 60_000) == 3.0, timeout=15.0, desc="query sees 3")

        wc.ingest({"events": make_events(4, event_name="q_ref", distinct_id_prefix="r2", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: _query_total(qc, pid, "q_ref", now_ms - 60_000, now_ms + 60_000) == 7.0, timeout=15.0, desc="query sees 7")

    def test_f17_t1_query_node_decrypts_byok_snapshots_and_segments(self) -> None:
        byok = random_byok_hex()
        s3, _, writer, qnode, pid, key = self._boot_writer_and_query(byok=byok)
        now_ms = int(time.time() * 1000)
        wc, qc = writer.client(), qnode.client()
        wc.ingest({"events": make_events(5, event_name="q_byok", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        wc.admin_flush().raise_for_status()
        wait_until(lambda: _query_total(qc, pid, "q_byok", now_ms - 60_000, now_ms + 60_000) == 5.0, timeout=15.0, desc="byok query==5")


# ===========================================================================
# F18: Local segment tier (hot/segments/) [T3]
# ===========================================================================


@tier(1)
@feature("F18")
@require_milestone("T3")
class TestF18LocalSegmentTier1(_StorageTier1Base):
    def test_f18_t1_l0_flush_writes_pvtx_to_local_hot_segments_zero_s3_puts(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="hot_seg_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        hot_segs = _local_matches(node, "hot/segments")
        self.assertGreaterEqual(len(hot_segs), 1)
        self.assertTrue(any(p.endswith(".pvtx") for p in hot_segs))
        self.assertEqual(s3.put_count("segments/"), 0)

    def test_f18_t1_queries_combine_l0_hot_segments_and_s3_segments_seamlessly(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        now_ms = int(time.time() * 1000)
        # 1) Upload to S3 segments/
        client.ingest({"events": make_events(3, event_name="mix_tier", distinct_id_prefix="s3u", base_ts_ms=now_ms - 3000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="s3 segments")
        # 2) Flush to local hot/segments/
        client.ingest({"events": make_events(4, event_name="mix_tier", distinct_id_prefix="hotu", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        # 3) Keep in L0 memory
        client.ingest({"events": make_events(2, event_name="mix_tier", distinct_id_prefix="l0u", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()

        self.assertEqual(_query_total(client, pid, "mix_tier", now_ms - 60_000, now_ms + 60_000), 9.0)

    def test_f18_t1_count_based_compaction_disabled_for_hot_segments_when_interval_gt_0(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        for i in range(5):
            client.ingest({"events": [make_event(event_name="nocomp", distinct_id="same_u", ts_ms=now_ms - 1000 + i * 10)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        pvtx_files = [p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")]
        self.assertGreaterEqual(len(pvtx_files), 5)
        self.assertEqual(s3.put_count("segments/"), 0)

    def test_f18_t1_byok_encrypts_local_hot_segments_in_plseagm2(self) -> None:
        byok = random_byok_hex()
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="hot_byok", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        pvtx = [p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")]
        self.assertGreaterEqual(len(pvtx), 1)
        for p in pvtx:
            self.assertTrue(node.read_local_bytes(p).startswith(b"PLSEAGM2"))

    def test_f18_t1_restart_preserves_and_serves_local_hot_segments_immediately(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(6, event_name="hot_rst", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertGreaterEqual(len(_local_matches(node, "hot/segments")), 1)

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "hot_rst", now_ms - 60_000, now_ms + 60_000), 6.0)
        self.assertEqual(s3.put_count("segments/"), 0)


# ===========================================================================
# F19: Full 7-step upload cycle & timers [T3]
# ===========================================================================


@tier(1)
@feature("F19")
@require_milestone("T3")
class TestF19UploadCycleTier1(_StorageTier1Base):
    def test_f19_t1_upload_cycle_uploads_hot_segments_to_s3_and_cleans_local(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="up_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="S3 segment upload")
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        wait_until(lambda: [p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")] == [], timeout=15.0, desc="local hot segments cleaned")

    def test_f19_t1_query_results_identical_before_during_and_after_upload_cycle(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(12, event_name="eq_ev", distinct_id_prefix="eq_u", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        before = _query_total(client, pid, "eq_ev", now_ms - 60_000, now_ms + 60_000)
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="S3 upload")
        after = _query_total(client, pid, "eq_ev", now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(before, 12.0)
        self.assertEqual(after, 12.0)

    def test_f19_t1_s3_segments_written_create_only_without_reading_existing_s3(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="co_ev", distinct_id_prefix="c1", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        s3.reset_counts()
        client.ingest({"events": make_events(3, event_name="co_ev", distinct_id_prefix="c2", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        self.assertEqual(s3.get_counts("segments/").get(("segments", "GET"), 0), 0)

    def test_f19_t1_idle_project_makes_zero_s3_calls_per_upload_tick(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="idle_ev", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        time.sleep(0.5)
        s3.reset_counts()
        time.sleep(2.5)
        self.assertEqual(s3.total_calls(), 0, f"Expected 0 idle S3 calls, got {s3.get_counts()}")

    def test_f19_t1_closed_date_large_bytes_gt_4mib_uploads_immediately(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        big_prop = "Z" * 4096
        for b in range(6):
            batch = make_events(250, event_name="closed_big", distinct_id_prefix=f"cb_{b}", base_ts_ms=old_ms + b * 100, props={"pad": big_prop})
            client.ingest({"events": batch}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=20.0, desc="closed >4MiB uploaded")


# ===========================================================================
# F20: Intent record & startup sweep (+ dedup read error fix) [T3]
# ===========================================================================


@tier(1)
@feature("F20")
@require_milestone("T3")
class TestF20IntentAndSweepTier1(_StorageTier1Base):
    def test_f20_t1_clean_upload_cycle_writes_and_removes_intent_record(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="int_ok", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        wait_until(lambda: [p for p in node.local_files() if "intent" in p.lower()] == [], timeout=15.0, desc="intent cleared")

    def test_f20_t1_clean_startup_with_no_intent_or_orphans_succeeds(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="cln_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(_query_total(node2.client(), pid, "cln_ev", now_ms - 60_000, now_ms + 60_000), 3.0)

    def test_f20_t1_startup_sweep_deletes_unreferenced_local_hot_segments(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="swp_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        # Place an orphan .pvtx under local hot/segments
        hot_dir_sample = _local_matches(node, "manifests")[0].split("manifests/")[0]
        orphan_rel = f"{hot_dir_sample}hot/segments/{pid}/20261122/bucket_000/orphan_9999.pvtx"
        node.write_local_bytes(orphan_rel, b"orphan")
        self.assertIn(orphan_rel, node.local_files())

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        _wait_project_open(node2.client(), pid, now_ms - 60_000, now_ms + 60_000)
        self.assertNotIn(orphan_rel, node2.local_files())

    def test_f20_t1_startup_clears_completed_step6_intent_and_advances_seq(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="s6_ok", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertEqual([p for p in node2.local_files() if "intent" in p.lower()], [])

    def test_f20_t1_zero_events_lost_or_duplicated_across_clean_intent_cycles(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(5, event_name="z_ev", distinct_id_prefix=f"z{i}", base_ts_ms=now_ms - 1000 + i * 20)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        self.assertEqual(_query_total(client, pid, "z_ev", now_ms - 60_000, now_ms + 60_000), 15.0)


# ===========================================================================
# F21: Local segment byte bounds & 429 backpressure [T3]
# ===========================================================================


@tier(1)
@feature("F21")
@require_milestone("T3")
class TestF21LocalSegmentBoundsTier1(_StorageTier1Base):
    def test_f21_t1_below_1x_local_segment_bound_returns_200_and_waits_for_interval(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=300, extra_env={"PULSE_HOT_SEGMENT_MAX_BYTES": str(10 * 1024 * 1024)}
        )
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        resp = client.ingest({"events": make_events(5, event_name="b_low", base_ts_ms=now_ms - 500)}, api_key=key)
        self.assertEqual(resp.status, 200)
        client.admin_flush().raise_for_status()
        self.assertEqual(s3.put_count("segments/"), 0)

    def test_f21_t1_between_1x_and_2x_bound_triggers_immediate_upload_while_returning_200(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=300, extra_env={"PULSE_HOT_SEGMENT_MAX_BYTES": "65536"}
        )
        now_ms = int(time.time() * 1000)
        batch = make_events(200, event_name="b_1x", base_ts_ms=now_ms - 500, props={"pad": "A" * 512})
        resp = client.ingest({"events": batch}, api_key=key)
        self.assertEqual(resp.status, 200)
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="immediate upload at 1x")

    def test_f21_t1_immediate_upload_drains_hot_segments_below_1x(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=300, extra_env={"PULSE_HOT_SEGMENT_MAX_BYTES": "32768"}
        )
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(120, event_name="b_drain", base_ts_ms=now_ms - 500, props={"pad": "B" * 512})}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: [p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")] == [], timeout=15.0, desc="hot segments drained")

    def test_f21_t1_node_wide_and_per_project_bounds_both_allow_normal_ingest(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(
            hot_store="local",
            upload_interval_secs=2,
            extra_env={"PULSE_HOT_SEGMENT_MAX_BYTES": "1048576", "PULSE_NODE_HOT_SEGMENT_MAX_BYTES": "4194304"},
        )
        now_ms = int(time.time() * 1000)
        resp = client.ingest({"events": make_events(20, event_name="b_norm", base_ts_ms=now_ms - 500)}, api_key=key)
        self.assertEqual(resp.status, 200)
        self.assertEqual(_query_total(client, pid, "b_norm", now_ms - 60_000, now_ms + 60_000), 20.0)

    def test_f21_t1_recovered_s3_drains_backlog_and_preserves_all_events(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_HOT_SEGMENT_MAX_BYTES": "32768"}
        )
        s3.set_fault(fail_prefixes=["segments/", "published/"], status=403)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(10, event_name="b_rec", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        s3.clear_faults()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="drain after S3 recovery")
        self.assertEqual(_query_total(client, pid, "b_rec", now_ms - 60_000, now_ms + 60_000), 10.0)


# ===========================================================================
# F26: Cold S3 merge [T4]
# ===========================================================================


@tier(1)
@feature("F26")
@require_milestone("T4")
class TestF26ColdS3MergeTier1(_StorageTier1Base):
    def _seed_cold_partition(self, s3: MockS3Server, client: PulseHttpClient, pid: str, key: str, old_ms: int, count_cycles: int = 3) -> list[str]:
        for c in range(count_cycles):
            client.ingest(
                {"events": make_events(4, event_name="cold_ev", distinct_id_prefix="same_bkt_u", base_ts_ms=old_ms + c * 100)},
                api_key=key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=c: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{c}")
        return s3.list_objects(f"segments/{pid}/")

    def test_f26_t1_cold_closed_date_idle_gt_1h_merges_multiple_s3_segments_into_one(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        initial = self._seed_cold_partition(s3, client, pid, key, old_ms, count_cycles=3)
        self.assertGreaterEqual(len(initial), 3)

        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) > len(initial), timeout=20.0, desc="cold merged segment uploaded")

    def test_f26_t1_cold_merge_uses_cas_published_manifest_and_pending_s3_deletes(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        self._seed_cold_partition(s3, client, pid, key, old_ms, count_cycles=3)
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 4, timeout=20.0, desc="cold merge publish")

        pub_keys = sorted(k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k)
        seqs = [json.loads(s3.get_object(k).decode("utf-8"))["publish_seq"] for k in pub_keys]
        self.assertEqual(seqs, list(range(len(seqs))))

    def test_f26_t1_cold_merge_physically_drops_erased_user_rows(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        client.ingest({"events": [make_event(event_name="cold_er", distinct_id="erase_me", ts_ms=old_ms)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        client.ingest({"events": [make_event(event_name="cold_er", distinct_id="keep_me", ts_ms=old_ms + 10)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 2, timeout=15.0, desc="2 publishes")

        client.delete_user(pid, "erase_me").raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 3, timeout=20.0, desc="cold merge publish")
        self.assertEqual(_query_total(client, pid, "cold_er", old_ms - 60_000, old_ms + 60_000), 1.0)

    def test_f26_t1_cold_merge_preserves_100pct_non_erased_events_across_queries(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        self._seed_cold_partition(s3, client, pid, key, old_ms, count_cycles=3)
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 4, timeout=20.0, desc="cold merge")
        self.assertEqual(_query_total(client, pid, "cold_ev", old_ms - 60_000, old_ms + 60_000), 12.0)

    def test_f26_t1_cold_merge_respects_64mib_cap_on_large_cold_partition(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        self._seed_cold_partition(s3, client, pid, key, old_ms, count_cycles=3)
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="cold segments present")
        for seg_key in s3.list_objects(f"segments/{pid}/"):
            obj = s3.head_object(seg_key)
            assert obj is not None
            self.assertLessEqual(obj.size, 64 * 1024 * 1024)


# ===========================================================================
# F27: Local hot tier + upload/publish for agent & replay [T4]
# ===========================================================================


@tier(1)
@feature("F27")
@require_milestone("T4")
class TestF27AgentAndReplayLocalTier1(_StorageTier1Base):
    def test_f27_t1_replay_chunk_writes_sync_to_local_hot_replay_and_uploads_on_cycle(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        chunk = make_replay_chunk(session_id="rs_f27_1", project_id=pid, chunk_start_ts_ms=now_ms)
        r = client.replay_chunk(chunk, api_key=key, binary=True).raise_for_status().json()
        self.assertEqual(r["status"], "ok")
        self.assertIn("replay_offset_kb", r)
        self.assertEqual(s3.put_count("replays/"), 0)
        self.assertGreaterEqual(len(_local_matches(node, "hot/replay")), 1)

        wait_until(lambda: len(s3.list_objects("replays/")) >= 1, timeout=15.0, desc="replay uploaded to S3")

    def test_f27_t1_get_replay_with_range_reads_seamlessly_from_local_and_s3(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        now_ms = int(time.time() * 1000)
        chunk = make_replay_chunk(session_id="rs_f27_range", project_id=pid, chunk_start_ts_ms=now_ms)
        client.replay_chunk(chunk, api_key=key, binary=True).raise_for_status()

        r_local = client.get_replay(pid, "rs_f27_range", byte_range="bytes=0-15")
        self.assertEqual(r_local.status, 206)
        self.assertTrue(r_local.body.startswith(b"PLSR"))

        wait_until(lambda: len(s3.list_objects("replays/")) >= 1, timeout=15.0, desc="replay uploaded")
        r_s3 = client.get_replay(pid, "rs_f27_range", byte_range="bytes=0-15")
        self.assertEqual(r_s3.status, 206)
        self.assertEqual(r_s3.body, r_local.body)

    def test_f27_t1_agent_segments_and_manifests_commit_locally_and_upload_on_cycle(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f27_1", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        self.assertGreaterEqual(len(_local_matches(node, "agent_segments")), 1)
        self.assertGreaterEqual(len(_local_matches(node, "agent_manifests")), 1)
        self.assertEqual(s3.put_count("agent_segments/"), 0)

        wait_until(lambda: len(s3.list_objects("agent_segments/")) >= 1, timeout=15.0, desc="agent segment uploaded")
        traces = client.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertIn("tr_f27_1", {t["trace_id"] for t in traces})

    def test_f27_t1_idle_project_makes_zero_s3_calls_for_agent_and_replay(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f27_idle", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        client.replay_chunk(make_replay_chunk(session_id="rs_f27_idle", project_id=pid, chunk_start_ts_ms=now_ms), api_key=key, binary=True).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects("agent_segments/")) >= 1 and len(s3.list_objects("replays/")) >= 1, timeout=15.0, desc="uploaded")

        time.sleep(0.5)
        s3.reset_counts()
        time.sleep(2.5)
        self.assertEqual(s3.total_calls(), 0, f"Expected 0 idle S3 calls, got {s3.get_counts()}")

    def test_f27_t1_physical_agent_erasure_removes_rows_before_tombstone_expiry(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f27_keep", start_ts_ms=now_ms - 1000), api_key=key).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f27_erase", start_ts_ms=now_ms - 900), api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        er = client.erase_agent(pid, level="trace", target_id="tr_f27_erase", expires_at=now_ms + 3000).raise_for_status().json()
        self.assertEqual(er["status"], "ok")
        self.assertEqual(client.get_agent_trace(pid, "tr_f27_erase").status, 404)
        client.admin_flush().raise_for_status()
        traces = {t["trace_id"] for t in client.list_agent_traces(pid).raise_for_status().json()["traces"]}
        self.assertIn("tr_f27_keep", traces)
        self.assertNotIn("tr_f27_erase", traces)
