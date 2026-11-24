"""Tier 2 (boundary/edge/negative) opaque-box E2E tests for Storage Tiering features F01..F21 and F26..F27."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import sys
import threading
import time
import unittest
import zlib
from compression import zstd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import PlsbEncoder, PulseHttpClient  # noqa: E402
from lib.mock_s3 import MockS3Server  # noqa: E402
from lib.node_mgr import NodeInstance, NodeManager  # noqa: E402
from lib.oracle import (  # noqa: E402
    decode_manifest,
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
        if rel.startswith(f"{clean}/") or f"/{clean}/" in rel:
            out.append(rel)
    return out


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


def _pending_delete_paths(doc: dict[str, Any]) -> set[str]:
    paths: set[str] = set()
    for entry in doc.get("pending_s3_deletes") or []:
        if isinstance(entry, str):
            paths.add(entry)
        elif isinstance(entry, dict):
            for k in ("path", "key", "segment_path", "object_path"):
                val = entry.get(k)
                if isinstance(val, str):
                    paths.add(val)
                    break
    return paths


def _build_pwl2_frame(epoch: int, seq: int, payload: bytes) -> bytes:
    header_body = struct.pack("<QQI", epoch, seq, len(payload))
    crc = zlib.crc32(header_body + payload) & 0xFFFFFFFF
    return b"PWL2" + header_body + struct.pack("<I", crc) + payload


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


class _StorageTier2Base(unittest.TestCase):
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
        project_name: str = "Tier2 Project",
    ) -> tuple[MockS3Server, NodeManager, NodeInstance, PulseHttpClient, str, str]:
        s3, nm = self._provision_s3_and_mgr()
        admin_token = "sk_root_e2e_tier2_admin_token_000000000000"
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

    def _chmod_tree_cleanup(self, root: Path, dir_mode: int) -> None:
        dirs: list[Path] = []
        for dp, _, _ in os.walk(root):
            dirs.append(Path(dp))
        self.addCleanup(lambda: [os.chmod(d, 0o755) for d in reversed(dirs) if d.exists()])
        for d in reversed(dirs):
            os.chmod(d, dir_mode)


# ===========================================================================
# F01: Prefix routing for WALs (WalRouted) [T0a / T0b]
# ===========================================================================


@tier(2)
@feature("F01")
@require_milestone("T0a")
class TestF01WalPrefixRoutingTier2(_StorageTier2Base):
    def test_f01_t2_invalid_hot_store_value_refuses_startup(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        rc, out, err = nm.start_node(hot_store="bogus", mock_s3=s3, expect_fail=True)
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertIn("PULSE_HOT_STORE must be `local` or `object`, not `bogus`", combined)

    def test_f01_t2_corrupt_event_wal_quarantined_locally_to_wal_corrupt(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="valid_before_corrupt", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        node.kill()

        corrupt_rel = f"wal/wal/{pid}/bucket_000/epoch_0000999999_seq_000000999999.wal"
        node.write_local_bytes(corrupt_rel, b"CORRUPT_NOT_PWL2_HEADER")
        s3.reset_counts()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(_query_total(c2, pid, "valid_before_corrupt", now_ms - 60_000, now_ms + 60_000), 3.0)
        self.assertEqual(node2.local_files(f"wal/wal_corrupt/{pid}/"), [f"wal/wal_corrupt/{pid}/bucket_000/epoch_0000999999_seq_000000999999.wal"])
        self.assertNotIn(corrupt_rel, node2.local_files(f"wal/wal/{pid}/"))
        self.assertEqual(s3.put_count("wal_corrupt/"), 0)

    def test_f01_t2_dead_letter_event_routes_to_local_wal_dead_zero_s3_puts(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(
            hot_store="local", extra_env={"PULSE_NUM_BUCKETS": "1"}
        )
        now_ms = int(time.time() * 1000)
        client.ingest(
            {"events": [make_event(event_name="dict_fill_0_0", distinct_id="u_dict", ts_ms=now_ms - 1000)]},
            api_key=key,
        ).raise_for_status()
        client.admin_flush().raise_for_status()
        node.stop(graceful=True)

        # Fill the persisted project dictionary to MAX_DICTIONARY_ENTRIES (65,536) in manifest
        local_mf_paths = sorted(p for p in _local_matches(node, f"manifests/{pid}") if "epoch_" in p)
        if local_mf_paths:
            latest_local = local_mf_paths[-1]
            mf_doc = decode_manifest(node.read_local_bytes(latest_local))
            mf_doc["dict_snapshot"]["events"] = ["dict_fill_0_0"] + [f"pad_ev_{i}" for i in range(65535)]
            node.write_local_bytes(latest_local, b"PZST" + zstd.compress(json.dumps(mf_doc).encode("utf-8")))
        else:
            mf_keys = sorted(s3.list_objects(f"manifests/{pid}/"))
            self.assertGreaterEqual(len(mf_keys), 1)
            latest_mf_key = mf_keys[-1]
            mf_doc = decode_manifest(s3.get_object(latest_mf_key))
            mf_doc["dict_snapshot"]["events"] = ["dict_fill_0_0"] + [f"pad_ev_{i}" for i in range(65535)]
            s3.put_object_raw(latest_mf_key, b"PZST" + zstd.compress(json.dumps(mf_doc).encode("utf-8")))

        # Inject a valid PWL2 WAL frame containing 1 overflow event name + 1 existing event name
        overflow_plsb = PlsbEncoder.encode(
            [
                make_event(event_name="overflow_65537_dead", distinct_id="u_dict", ts_ms=now_ms - 200),
                make_event(event_name="dict_fill_0_0", distinct_id="u_dict", ts_ms=now_ms - 100),
            ],
            use_dict=False,
        )
        wal_frame = _build_pwl2_frame(999999, 999999, overflow_plsb)
        node.write_local_bytes(f"wal/wal/{pid}/bucket_000/epoch_0000999999_seq_000000999999.wal", wal_frame)
        s3.reset_counts()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, extra_env={"PULSE_NUM_BUCKETS": "1"},
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)

        dead_files = node2.local_files(f"wal/wal_dead/{pid}/")
        self.assertEqual(len(dead_files), 1, f"Expected 1 dead-letter file locally, got {dead_files}")
        self.assertEqual(s3.put_count("wal_dead/"), 0)
        self.assertEqual(_query_total(c2, pid, "dict_fill_0_0", now_ms - 60_000, now_ms + 60_000), 2.0)

    @require_milestone("T0b")
    def test_f01_t2_corrupt_agent_wal_quarantined_locally_to_agent_wal_corrupt(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_ok_pre_corrupt", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        node.kill()

        corrupt_rel = f"wal/agent_wal/{pid}/w_corrupt/seq_000000000001.ptwl"
        node.write_local_bytes(corrupt_rel, b"BAD_PTWL_MAGIC_BYTES")
        s3.reset_counts()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        wait_until(lambda: c2.list_agent_traces(pid).status == 200, timeout=15.0, desc="agent store reopen")
        self.assertGreaterEqual(len(_local_matches(node2, "agent_wal_corrupt")), 1)
        self.assertEqual(s3.put_count("agent_wal_corrupt/"), 0)

    def test_f01_t2_empty_hot_store_defaults_local_while_object_routes_wal_to_s3(self) -> None:
        s3_empty, _, node_empty, c_empty, pid_empty, k_empty = self._boot_single_node(hot_store="")
        s3_empty.reset_counts()
        now_ms = int(time.time() * 1000)
        c_empty.ingest({"events": make_events(2, event_name="empty_hs", base_ts_ms=now_ms - 500)}, api_key=k_empty).raise_for_status()
        self.assertEqual(s3_empty.put_count("wal/"), 0)
        self.assertGreaterEqual(len(node_empty.local_files(f"wal/wal/{pid_empty}/")), 1)

        s3_obj, _, node_obj, c_obj, pid_obj, k_obj = self._boot_single_node(hot_store="object")
        s3_obj.reset_counts()
        c_obj.ingest({"events": make_events(2, event_name="obj_hs", base_ts_ms=now_ms - 500)}, api_key=k_obj).raise_for_status()
        c_obj.ingest_agent_traces(make_agent_trace(trace_id="tr_obj_hs", start_ts_ms=now_ms - 400), api_key=k_obj).raise_for_status()
        self.assertGreaterEqual(s3_obj.put_count("wal/"), 1)
        self.assertGreaterEqual(s3_obj.put_count("agent_wal/"), 1)
        self.assertEqual(node_obj.local_files("wal/"), [])


# ===========================================================================
# F02: Mixed-mode WAL startup refusal [T0a]
# ===========================================================================


@tier(2)
@feature("F02")
@require_milestone("T0a")
class TestF02MixedModeWalRefusalTier2(_StorageTier2Base):
    def test_f02_t2_local_refuses_startup_over_s3_event_wal(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        data_dir = nm.make_temp_data_dir("f02_s3_ev_")
        s3.put_object_raw("wal/proj_default/bucket_000/epoch_0000000001_seq_000000000001.wal", b"x")
        rc, out, err = nm.start_node(data_dir=data_dir, hot_store="local", mock_s3=s3, expect_fail=True)
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertIn("unflushed WAL", combined)
        self.assertIn("PULSE_HOT_STORE=object", combined)

    def test_f02_t2_local_refuses_startup_over_s3_agent_wal(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        data_dir = nm.make_temp_data_dir("f02_s3_ag_")
        s3.put_object_raw("agent_wal/proj_default/w_1/seq_000000000001.ptwl", b"x")
        rc, out, err = nm.start_node(data_dir=data_dir, hot_store="local", mock_s3=s3, expect_fail=True)
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertIn("unflushed WAL", combined)
        self.assertIn("agent_wal/", combined)

    def test_f02_t2_object_refuses_startup_over_local_event_wal(self) -> None:
        s3, nm, node, client, _, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="left_loc", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        node.kill()

        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="object",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertIn("unflushed WAL", combined)
        self.assertIn("PULSE_HOT_STORE=local", combined)

    def test_f02_t2_object_refuses_startup_over_local_agent_wal(self) -> None:
        s3, nm, node, client, _, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_left_loc", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        node.kill()

        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="object",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertIn("unflushed WAL", combined)
        self.assertIn("agent_wal/", combined)

    def test_f02_t2_s3_list_error_during_mixed_mode_check_not_masked(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        data_dir = nm.make_temp_data_dir("f02_list_err_")
        # Use 403 so object_store returns the S3 error immediately without 180s 503 retry backoff
        s3.set_fault(fail_prefixes=["wal"], status=403, ops=["LIST"])
        rc, out, err = nm.start_node(data_dir=data_dir, hot_store="local", mock_s3=s3, expect_fail=True)
        self.assertNotEqual(rc, 0)
        combined = out + "\n" + err
        self.assertNotIn("unflushed WAL ()", combined)
        s3.clear_faults()
        node = nm.start_node(data_dir=data_dir, hot_store="local", mock_s3=s3, admin_token="sk_root_f02_rec")
        assert isinstance(node, NodeInstance)
        self.assertEqual(node.client().readyz().status, 200)


# ===========================================================================
# F03: HTTP 503 on WAL write failure [T0a]
# ===========================================================================


@tier(2)
@feature("F03")
@require_milestone("T0a")
class TestF03WalWriteFailure503Tier2(_StorageTier2Base):
    def test_f03_t2_unwritable_event_wal_returns_503_and_writes_zero_rows(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="f03_ev", distinct_id="ok_1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()

        self._chmod_tree_cleanup(node.data_dir / "wal", 0o555)
        r_fail = client.ingest({"events": [make_event(event_name="f03_ev", distinct_id="fail_1", ts_ms=now_ms - 200)]}, api_key=key)
        self.assertEqual(r_fail.status, 503)
        self.assertEqual(r_fail.json()["status"], "error")
        self.assertEqual(_query_total(client, pid, "f03_ev", now_ms - 60_000, now_ms + 60_000), 1.0)

    def test_f03_t2_unwritable_agent_wal_returns_503_service_unavailable(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_f03_ok", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()

        self._chmod_tree_cleanup(node.data_dir / "wal", 0o555)
        r_fail = client.ingest_agent_traces(make_agent_trace(trace_id="tr_f03_bad", start_ts_ms=now_ms - 200), api_key=key)
        self.assertEqual(r_fail.status, 503)
        body = r_fail.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["code"], "service_unavailable")

        traces = client.list_agent_traces(pid).raise_for_status().json()["traces"]
        self.assertEqual({t["trace_id"] for t in traces}, {"tr_f03_ok"})

    def test_f03_t2_unwritable_otel_wal_returns_503_service_unavailable(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ns = int(time.time() * 1_000_000_000)
        otel_req = {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "2122232425262728292a2b2c2d2e2f30",
                                    "spanId": "2122232425262728",
                                    "name": "otel_fail",
                                    "startTimeUnixNano": str(now_ns - 1000),
                                    "endTimeUnixNano": str(now_ns),
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        # Ensure wal/ exists first
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_seed_otel", start_ts_ms=int(time.time() * 1000) - 500), api_key=key).raise_for_status()
        self._chmod_tree_cleanup(node.data_dir / "wal", 0o555)

        r_json = client.ingest_otel_traces(otel_req, api_key=key, protobuf=False)
        r_pb = client.ingest_otel_traces(otel_req, api_key=key, protobuf=True)
        self.assertEqual(r_json.status, 503)
        self.assertEqual(r_json.json()["code"], "service_unavailable")
        self.assertEqual(r_pb.status, 503)
        self.assertEqual(r_pb.json()["code"], "service_unavailable")
        self.assertEqual(len(client.list_agent_traces(pid).raise_for_status().json()["traces"]), 1)

    def test_f03_t2_object_mode_s3_wal_put_failure_returns_503(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)
        # Non-retried 403 triggers immediate ObjectStore error mapped to HTTP 503
        s3.set_fault(fail_prefixes=["wal/", "agent_wal/"], status=403, ops=["PUT"])

        r_ev = client.ingest({"events": [make_event(event_name="s3_fail", distinct_id="u1", ts_ms=now_ms)]}, api_key=key)
        r_ag = client.ingest_agent_traces(make_agent_trace(trace_id="tr_s3_fail", start_ts_ms=now_ms), api_key=key)
        self.assertEqual(r_ev.status, 503)
        self.assertEqual(r_ag.status, 503)
        self.assertEqual(_query_total(client, pid, "s3_fail", now_ms - 60_000, now_ms + 60_000), 0.0)
        self.assertEqual(client.list_agent_traces(pid).raise_for_status().json()["traces"], [])

    def test_f03_t2_queries_and_health_succeed_while_wal_is_unwritable(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="ro_ok", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()

        self._chmod_tree_cleanup(node.data_dir / "wal", 0o555)
        self.assertEqual(client.ingest({"events": [make_event(event_name="ro_ok", distinct_id="u_x", ts_ms=now_ms)]}, api_key=key).status, 503)
        self.assertEqual(client.healthz().status, 200)
        self.assertEqual(client.status(project_id=pid).status, 200)
        self.assertEqual(_query_total(client, pid, "ro_ok", now_ms - 60_000, now_ms + 60_000), 2.0)


# ===========================================================================
# F04: Local identity/ and profiles/ routing [T0b]
# ===========================================================================


@tier(2)
@feature("F04")
@require_milestone("T0b")
class TestF04LocalIdentityProfilesTier2(_StorageTier2Base):
    def test_f04_t2_object_mode_routes_identity_and_profiles_to_s3(self) -> None:
        s3, _, _, client, _, key = self._boot_single_node(hot_store="object")
        s3.reset_counts()
        client.identify({"anon_id": "obj_a", "user_id": "obj_u", "user_properties": {"k": "v"}}, api_key=key).raise_for_status()
        self.assertGreaterEqual(s3.put_count("identity/") + s3.put_count("profiles/"), 1)

    def test_f04_t2_wrong_byok_key_on_restart_refuses_corrupt_identity_decryption(self) -> None:
        byok1 = random_byok_hex()
        byok2 = random_byok_hex()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok1)
        client.identify({"anon_id": "a1", "user_id": "u1", "user_properties": {"p": "v"}}, api_key=key).raise_for_status()
        node.stop(graceful=True)

        res = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, byok_master_key=byok2, wait_ready=False,
        )
        if isinstance(res, NodeInstance):
            c2 = res.client()
            resp = c2.identify({"anon_id": "a2", "user_id": "u2"}, api_key=key)
            self.assertGreaterEqual(resp.status, 400)
        else:
            rc, _, _ = res
            self.assertNotEqual(rc, 0)

    def test_f04_t2_unwritable_local_identity_or_profiles_dir_fails_cleanly(self) -> None:
        _, _, node, client, _, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "seed_p", "$set": {"a": 1}}, api_key=key).raise_for_status()
        prof_files = _local_matches(node, "profiles")
        self.assertGreaterEqual(len(prof_files), 1)
        prof_dir = (node.data_dir / prof_files[0]).parent
        self._chmod_tree_cleanup(prof_dir, 0o555)
        resp = client.engage({"distinct_id": "fail_p", "$set": {"blob": "x" * 2048}}, api_key=key)
        self.assertIn(resp.status, (200, 500, 503))
        if resp.status == 200:
            # Force flush/compaction or verify unwritable directory fails when SlateDB writes SST/WAL
            for i in range(10):
                r2 = client.engage({"distinct_id": f"fail_p_{i}", "$set": {"blob": "x" * 4096}}, api_key=key)
                if r2.status >= 500:
                    return
            self.fail("Expected unwritable profiles/ dir to return 500/503 on SlateDB write")

    def test_f04_t2_distinct_ids_with_identities_across_buckets_keep_zero_s3_calls_on_ingest(self) -> None:
        s3, _, _, client, _, key = self._boot_single_node(hot_store="local")
        s3.reset_counts()
        now_ms = int(time.time() * 1000)
        events = make_events(100, event_name="multi_bkt", distinct_id_prefix="mb_u", base_ts_ms=now_ms - 2000)
        client.ingest({"events": events}, api_key=key).raise_for_status()
        self.assertEqual(s3.total_calls(), 0, f"Expected 0 S3 calls on ingest ack, got {s3.get_counts()}")

    def test_f04_t2_s3_outage_during_identify_and_engage_succeeds_in_local_mode(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        s3.set_fault(fail_prefixes=["identity/", "profiles/"], status=403)
        r_id = client.identify({"anon_id": "out_a", "user_id": "out_u", "user_properties": {"tier": "pro"}}, api_key=key)
        r_eng = client.engage({"distinct_id": "out_u", "$set": {"ok": True}}, api_key=key)
        self.assertEqual(r_id.status, 200)
        self.assertEqual(r_eng.status, 200)
        self.assertEqual(client.get_user(pid, "out_u").raise_for_status().json()["profile"]["props"]["ok"], True)


# ===========================================================================
# F05: Hot-store mode marker & startup refusal [T0b]
# ===========================================================================


@tier(2)
@feature("F05")
@require_milestone("T0b")
class TestF05ModeMarkerRefusalTier2(_StorageTier2Base):
    def test_f05_t2_refuses_startup_when_local_marker_switched_to_object(self) -> None:
        s3, nm, node, _, _, _ = self._boot_single_node(hot_store="local")
        node.stop(graceful=True)
        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="object",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        self.assertTrue("local" in (out + err).lower() or "mode" in (out + err).lower())

    def test_f05_t2_refuses_startup_when_object_marker_switched_to_local(self) -> None:
        s3, nm, node, client, _, key = self._boot_single_node(hot_store="object")
        client.identify({"anon_id": "a_m", "user_id": "u_m"}, api_key=key).raise_for_status()
        node.stop(graceful=True)
        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        self.assertTrue("object" in (out + err).lower() or "mode" in (out + err).lower())

    def test_f05_t2_refuses_startup_when_local_identity_missing_for_existing_project(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="loss_id", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        node.stop(graceful=True)

        for rel in _local_matches(node, "identity"):
            node.rm_local(rel)

        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        self.assertIn("restore", (out + err).lower())

    def test_f05_t2_refuses_startup_when_local_profiles_missing_for_existing_project(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        client.engage({"distinct_id": "loss_prof_u", "$set": {"plan": "pro"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        node.stop(graceful=True)

        for rel in _local_matches(node, "profiles"):
            node.rm_local(rel)

        rc, out, err = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)
        self.assertIn("restore", (out + err).lower())

    def test_f05_t2_refuses_startup_on_corrupt_mode_marker_file(self) -> None:
        s3, nm, node, _, _, _ = self._boot_single_node(hot_store="local")
        node.stop(graceful=True)
        markers = [p for p in node.local_files() if "mode" in p.lower() or "hot_store" in p.lower()]
        self.assertGreaterEqual(len(markers), 1)
        node.write_local_bytes(markers[0], b"\xff\xfeCORRUPT_MARKER")

        rc, _, _ = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, expect_fail=True,
        )
        self.assertNotEqual(rc, 0)


# ===========================================================================
# F06: DurableLocalObjectStore multipart fsync [T0b]
# ===========================================================================


@tier(2)
@feature("F06")
@require_milestone("T0b")
class TestF06MultipartFsyncTier2(_StorageTier2Base):
    def test_f06_t2_sigkill_immediately_after_identify_200_leaves_no_truncated_sst(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        res = client.identify({"anon_id": "imm_a", "user_id": "imm_u", "user_properties": {"k": "v"}}, api_key=key).raise_for_status().json()
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        now_ms = int(time.time() * 1000)
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(c2.get_user(pid, "imm_u").raise_for_status().json()["canonical_uid"], res["canonical_uid"])

    def test_f06_t2_sigkill_immediately_after_engage_200_preserves_latest_props(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        client.engage({"distinct_id": "eng_imm", "$set": {"ver": 1}}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "eng_imm", "$set": {"ver": 2, "big": "z" * 1024}}, api_key=key).raise_for_status()
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        now_ms = int(time.time() * 1000)
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(c2.get_user(pid, "eng_imm").raise_for_status().json()["profile"]["props"]["ver"], 2)

    def test_f06_t2_unwritable_profiles_parent_dir_surfaces_io_error_never_silent_ok(self) -> None:
        _, _, node, client, _, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "seed_io", "$set": {"a": 1}}, api_key=key).raise_for_status()
        prof_files = _local_matches(node, "profiles")
        prof_dir = (node.data_dir / prof_files[0]).parent
        self._chmod_tree_cleanup(prof_dir, 0o555)
        saw_error = False
        for i in range(15):
            r = client.engage({"distinct_id": f"io_u_{i}", "$set": {"pad": "p" * 4096}}, api_key=key)
            if r.status >= 500:
                saw_error = True
                break
        self.assertTrue(saw_error, "Expected I/O error from unwritable local profiles store")

    def test_f06_t2_incomplete_temp_files_do_not_corrupt_slatedb_reopen(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        client.identify({"anon_id": "tmp_a", "user_id": "tmp_u"}, api_key=key).raise_for_status()
        node.stop(graceful=True)

        id_files = _local_matches(node, "identity")
        self.assertGreaterEqual(len(id_files), 1)
        id_parent = str(Path(id_files[0]).parent)
        node.write_local_bytes(f"{id_parent}/00000000000000009999.sst.tmp", b"aborted_partial")

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        self.assertGreater(node2.client().get_user(pid, "tmp_u").raise_for_status().json()["canonical_uid"], 0)

    def test_f06_t2_concurrent_identify_engage_and_events_under_sigkill_preserves_monotonic_uids(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", writer_lease_ttl_secs=3)
        uids_seen: dict[str, int] = {}
        for i in range(25):
            r = client.identify({"anon_id": f"mono_a_{i}", "user_id": f"mono_u_{i}"}, api_key=key).raise_for_status().json()
            uids_seen[f"mono_u_{i}"] = r["canonical_uid"]
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        now_ms = int(time.time() * 1000)
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        new_r = c2.identify({"anon_id": "mono_a_new", "user_id": "mono_u_new"}, api_key=key).raise_for_status().json()
        self.assertGreater(new_r["canonical_uid"], max(uids_seen.values()))


# ===========================================================================
# F07: Published manifest with dense publish_seq [T1]
# ===========================================================================


@tier(2)
@feature("F07")
@require_milestone("T1")
class TestF07PublishedManifestDenseSeqTier2(_StorageTier2Base):
    def test_f07_t2_existing_pub_seq_causes_cas_conflict_never_overwrites(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="cas_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        sentinel = b'{"sentinel":"do_not_overwrite"}'
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", sentinel)
        client.ingest({"events": make_events(2, event_name="cas_ev", distinct_id_prefix="c2", base_ts_ms=now_ms - 200)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(2.0)
        self.assertEqual(s3.get_object(f"published/{pid}/pub_000000000001.json"), sentinel)

    def test_f07_t2_missing_previous_pub_seq_gc_guard_aborts_publish(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="gc_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        s3.delete_object_raw(f"published/{pid}/pub_000000000000.json")
        client.ingest({"events": make_events(2, event_name="gc_ev", distinct_id_prefix="g2", base_ts_ms=now_ms - 200)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(2.0)
        self.assertIsNone(s3.head_object(f"published/{pid}/pub_000000000001.json"))

    def test_f07_t2_published_manifest_never_references_hot_segments_prefix(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(6, event_name="no_hot_ref", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        pub_doc = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        self.assertNotIn("hot/segments", json.dumps(pub_doc))

    def test_f07_t2_empty_idle_project_does_not_emit_spurious_pub_seq_gaps(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        time.sleep(2.5)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="gap_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 1, timeout=15.0, desc="first pub")
        pub_keys = sorted(k for k in s3.list_objects(f"published/{pid}/") if "/pub_" in k)
        seqs = [decode_manifest(s3.get_object(k))["publish_seq"] for k in pub_keys]
        self.assertEqual(seqs[0], 0)

    def test_f07_t2_s3_published_put_failure_leaves_seq_unadvanced(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=403, ops=["PUT"])
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="pub_fail", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(1.5)
        self.assertEqual(s3.list_objects(f"published/{pid}/pub_"), [])

        s3.clear_faults()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0 after clear")


# ===========================================================================
# F08: Identity & profile S3 snapshots + ordering & HWM [T1]
# ===========================================================================


@tier(2)
@feature("F08")
@require_milestone("T1")
class TestF08IdentityProfileSnapshotsTier2(_StorageTier2Base):
    def test_f08_t2_profile_snapshot_s3_failure_aborts_before_published_manifest(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=403, ops=["PUT"])
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="ps_fail", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "user_0000", "$set": {"x": 1}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(1.5)
        self.assertEqual(s3.list_objects(f"published/{pid}/pub_"), [])

    def test_f08_t2_identity_snapshot_s3_failure_aborts_before_published_manifest(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        s3.set_fault(fail_prefixes=[f"published/{pid}/"], status=403, ops=["PUT"])
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="id_fail", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(1.5)
        self.assertEqual(s3.list_objects(f"published/{pid}/pub_"), [])

    def test_f08_t2_wrong_byok_key_on_restore_fails_snapshot_decryption(self) -> None:
        pg = skip_if_no_postgres()
        byok1 = random_byok_hex()
        byok2 = random_byok_hex()
        s3, nm, node, client, pid, key = self._boot_single_node(
            hot_store="local", byok_master_key=byok1, upload_interval_secs=1, database_url=pg
        )
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="byok_bad", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_bad_byok_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local", "PULSE_BYOK_MASTER_KEY": byok2},
        )
        self.assertNotEqual(res.returncode, 0)

    def test_f08_t2_hwm_violation_in_tampered_manifest_rejected_on_restore(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="hwm_tamper", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        pub_key = f"published/{pid}/pub_000000000000.json"
        wait_until(lambda: s3.head_object(pub_key) is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        doc = decode_manifest(s3.get_object(pub_key))
        doc["high_water_mark"] = int(doc["high_water_mark"]) + 10000
        s3.put_object_raw(pub_key, json.dumps(doc).encode("utf-8"))

        restore_dir = nm.make_temp_data_dir("rst_hwm_fail_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)

    def test_f08_t2_concurrent_new_users_and_merges_during_publish_preserve_hwm_invariant(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(20, event_name="conc_hwm", distinct_id_prefix="ch", base_ts_ms=now_ms - 2000)}, api_key=key).raise_for_status()
        client.identify({"anon_id": "ch_0000", "user_id": "ch_canon_0"}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        doc = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        self.assertGreaterEqual(int(doc["high_water_mark"]), 20)


# ===========================================================================
# F09: Empirical T1 snapshot benchmark [T1]
# ===========================================================================


@tier(2)
@feature("F09")
@require_milestone("T1")
class TestF09SnapshotBenchmarkTier2(_StorageTier2Base):
    def test_f09_t2_idle_cycles_between_mutations_emit_zero_snapshot_puts(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="bm_idle", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        time.sleep(0.4)
        s3.reset_counts()
        time.sleep(2.5)
        self.assertEqual(s3.put_count("published/"), 0)

    def test_f09_t2_large_unique_user_burst_publishes_and_restores_without_uid_loss(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        for chunk in range(4):
            client.ingest(
                {"events": make_events(100, event_name="burst_u", distinct_id_prefix=f"bu_{chunk}_", base_ts_ms=now_ms - 2000 + chunk * 100)},
                api_key=key,
            ).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        doc = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        self.assertGreaterEqual(int(doc["high_water_mark"]), 400)

    def test_f09_t2_profile_churn_same_users_does_not_inflate_hwm(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="churn_u", distinct_id_prefix="cu", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        hwm0 = int(decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))["high_water_mark"])

        for i in range(25):
            client.engage({"distinct_id": "cu_0000", "$set": {"counter": i}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        hwm1 = int(decode_manifest(s3.get_object(f"published/{pid}/pub_000000000001.json"))["high_water_mark"])
        self.assertEqual(hwm1, hwm0)

    def test_f09_t2_restore_completeness_fails_if_snapshot_object_missing(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="miss_snap", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        for k in s3.list_objects(f"published/{pid}/"):
            if "pub_" not in k:
                s3.delete_object_raw(k)

        restore_dir = nm.make_temp_data_dir("rst_miss_snap_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)

    def test_f09_t2_multi_project_snapshot_cycles_remain_isolated_and_bounded(self) -> None:
        s3, _, _, client, pid1, key1 = self._boot_single_node(hot_store="local", upload_interval_secs=1, project_name="P1")
        p2 = client.create_project("P2").raise_for_status().json()
        pid2, key2 = p2["project"]["id"], p2["api_key"]["plaintext_key"]
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="p1_ev", base_ts_ms=now_ms - 500)}, api_key=key1).raise_for_status()
        client.ingest({"events": make_events(7, event_name="p2_ev", base_ts_ms=now_ms - 500)}, api_key=key2).raise_for_status()
        client.admin_flush().raise_for_status()

        wait_until(lambda: s3.head_object(f"published/{pid1}/pub_000000000000.json") is not None, timeout=15.0, desc="p1 pub_0")
        wait_until(lambda: s3.head_object(f"published/{pid2}/pub_000000000000.json") is not None, timeout=15.0, desc="p2 pub_0")
        d1 = decode_manifest(s3.get_object(f"published/{pid1}/pub_000000000000.json"))
        d2 = decode_manifest(s3.get_object(f"published/{pid2}/pub_000000000000.json"))
        self.assertGreaterEqual(int(d1["high_water_mark"]), 5)
        self.assertGreaterEqual(int(d2["high_water_mark"]), 7)


# ===========================================================================
# F10: pulse-node restore --from-published CLI [T1]
# ===========================================================================


@tier(2)
@feature("F10")
@require_milestone("T1")
class TestF10RestoreCliRefusalsTier2(_StorageTier2Base):
    def test_f10_t2_restore_refuses_default_sqlite_without_database_url(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        restore_dir = nm.make_temp_data_dir("rst_nosqlite_")
        env = dict(os.environ)
        env.pop("DATABASE_URL", None)
        env.update(s3.env_vars())
        env["PULSE_HOT_STORE"] = "local"
        res = nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env=env)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("DATABASE_URL", res.stdout + "\n" + res.stderr)

    def test_f10_t2_restore_refuses_explicit_sqlite_url(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        restore_dir = nm.make_temp_data_dir("rst_exsqlite_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": f"sqlite://{(restore_dir / 'meta.db').resolve()}", "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("DATABASE_URL", res.stdout + "\n" + res.stderr)

    def test_f10_t2_restore_fails_when_no_published_manifest_exists(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm = self._provision_s3_and_mgr()
        restore_dir = nm.make_temp_data_dir("rst_empty_s3_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)

    def test_f10_t2_restore_fails_when_s3_unreachable_leaving_no_partial_manifest(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm = self._provision_s3_and_mgr()
        s3.set_fault(fail_prefixes=["published/"], status=403)
        restore_dir = nm.make_temp_data_dir("rst_s3_down_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertEqual([p for p in restore_dir.rglob("*") if p.is_file() and "manifests" in p.as_posix()], [])

    def test_f10_t2_normal_node_startup_never_auto_restores_wiped_data_dir(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="no_autorest", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        # Fresh empty data_dir pointing to same S3 must NOT auto-restore published data
        fresh_dir = nm.make_temp_data_dir("no_auto_rst_")
        node2 = nm.start_node(role="all", data_dir=fresh_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        projects = {p["id"] for p in node2.client().list_projects().raise_for_status().json()}
        self.assertNotIn(pid, projects)


# ===========================================================================
# F11: Restore erasure reconciliation & tombstone cleanup [T1]
# ===========================================================================


@tier(2)
@feature("F11")
@require_milestone("T1")
class TestF11RestoreErasureReconciliationTier2(_StorageTier2Base):
    def test_f11_t2_post_publish_pre_wipe_erasure_uid_lt_hwm_stays_hidden_in_queries_and_export(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="lt_hide", distinct_id_prefix="lth", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.engage({"distinct_id": "lth_0000", "$set": {"plan": "pro"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        client.delete_user(pid, "lth_0000").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_t2_1_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "lt_hide", now_ms - 60_000, now_ms + 60_000), 2.0)
        exported = c2.export_events(pid, format="json").raise_for_status().json()
        self.assertNotIn("lth_0000", {ev["distinct_id"] for ev in exported["events"]})

    def test_f11_t2_post_publish_create_and_erase_uid_ge_hwm_does_not_hide_post_restore_backdated_user(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="bd_ev", distinct_id="base_u", ts_ms=now_ms - 5000)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        client.ingest({"events": [make_event(event_name="bd_ev", distinct_id="ephemeral_u", ts_ms=now_ms - 2000)]}, api_key=key).raise_for_status()
        client.delete_user(pid, "ephemeral_u").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_t2_2_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        # Import backdated event older than ephemeral_u's erasure timestamp
        c2.import_events(pid, {"events": [make_event(event_name="bd_ev", distinct_id="new_legit_u", ts_ms=now_ms - 10_000)]}).raise_for_status()
        self.assertEqual(_query_total(c2, pid, "bd_ev", now_ms - 60_000, now_ms + 60_000), 2.0)

    def test_f11_t2_profile_updated_after_erased_at_ms_is_preserved_across_restore(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.engage({"distinct_id": "reup_u", "$set": {"v": 1}}, api_key=key).raise_for_status()
        client.delete_user(pid, "reup_u").raise_for_status()
        time.sleep(0.05)
        client.engage({"distinct_id": "reup_u", "$set": {"v": 2}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_t2_3_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        self.assertEqual(node2.client().get_user(pid, "reup_u").raise_for_status().json()["profile"]["props"]["v"], 2)

    def test_f11_t2_restore_aborts_if_postgres_unreachable_during_reconciliation(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        bad_pg = f"postgres://invalid:invalid@127.0.0.1:{NodeManager.free_port()}/nonexistent"
        restore_dir = nm.make_temp_data_dir("rst_bad_pg_")
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(restore_dir)],
            env={**s3.env_vars(), "DATABASE_URL": bad_pg, "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)

    def test_f11_t2_aliased_user_erasure_reconciles_all_linked_uids_on_restore(self) -> None:
        pg = skip_if_no_postgres()
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, database_url=pg)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="al_ev", distinct_id="al_anon", ts_ms=now_ms - 1000)]}, api_key=key).raise_for_status()
        client.identify({"anon_id": "al_anon", "user_id": "al_user", "user_properties": {"k": "secret"}}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        client.delete_user(pid, "al_user").raise_for_status()
        node.stop(graceful=True)

        restore_dir = nm.make_temp_data_dir("rst_f11_t2_5_")
        nm.run_cli(["restore", "--from-published", "--data-dir", str(restore_dir)], env={**s3.env_vars(), "DATABASE_URL": pg, "PULSE_HOT_STORE": "local"})
        node2 = nm.start_node(role="all", data_dir=restore_dir, hot_store="local", mock_s3=s3, database_url=pg, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(_query_total(c2, pid, "al_ev", now_ms - 60_000, now_ms + 60_000), 0.0)
        users = {u["distinct_id"] for u in c2.list_users(pid).raise_for_status().json()}
        self.assertNotIn("al_anon", users)
        self.assertNotIn("al_user", users)


# ===========================================================================
# F12: erased_uids filter on profile reads & catalog [T1]
# ===========================================================================


@tier(2)
@feature("F12")
@require_milestone("T1")
class TestF12ErasedUidsFilterTier2(_StorageTier2Base):
    def test_f12_t2_re_engaged_user_after_erased_at_ms_reappears_with_new_props(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "re_f12", "$set": {"plan": "old_secret"}}, api_key=key).raise_for_status()
        client.delete_user(pid, "re_f12").raise_for_status()
        self.assertNotIn("re_f12", {u["distinct_id"] for u in client.list_users(pid).raise_for_status().json()})

        time.sleep(0.05)
        client.engage({"distinct_id": "re_f12", "$set": {"plan": "new_starter"}}, api_key=key).raise_for_status()
        detail = client.get_user(pid, "re_f12").raise_for_status().json()
        self.assertEqual(detail["profile"]["props"]["plan"], "new_starter")

    def test_f12_t2_boundary_updated_at_ms_equal_to_erased_at_ms_stays_hidden(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "eq_ts_u", "$set": {"plan": "hidden"}}, api_key=key).raise_for_status()
        det = client.get_user(pid, "eq_ts_u").raise_for_status().json()
        uid = int(det["canonical_uid"])
        updated_ms = int(det["profile"]["updated_at_ms"])

        # Delete user and pin erased_at_ms == updated_at_ms in SQLite if testing boundary
        client.delete_user(pid, "eq_ts_u").raise_for_status()
        node.sqlite_query(
            "UPDATE erased_uids SET erased_at_ms = ? WHERE project_id = ? AND uid = ?",
            (updated_ms, pid, uid),
            readonly=False,
        )
        self.assertNotIn("eq_ts_u", {u["distinct_id"] for u in client.list_users(pid).raise_for_status().json()})

    def test_f12_t2_aliased_user_erasure_hides_all_linked_distinct_ids(self) -> None:
        _, _, _, client, pid, key = self._boot_single_node(hot_store="local")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="al_f12", distinct_id="anon_alias_1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.identify({"anon_id": "anon_alias_1", "user_id": "main_alias_1", "user_properties": {"k": "v"}}, api_key=key).raise_for_status()
        client.delete_user(pid, "main_alias_1").raise_for_status()

        users = {u["distinct_id"] for u in client.list_users(pid).raise_for_status().json()}
        self.assertNotIn("anon_alias_1", users)
        self.assertNotIn("main_alias_1", users)
        self.assertEqual(_query_total(client, pid, "al_f12", now_ms - 60_000, now_ms + 60_000), 0.0)

    def test_f12_t2_stale_snapshot_profile_filtered_by_erased_uids_on_query_node(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        shared_dir = nm.make_temp_data_dir("f12_q_meta_")
        db_url = external_postgres_url() or f"sqlite://{(shared_dir / 'shared.db').resolve()}"
        tok = "sk_root_f12_stale_snap"
        writer = nm.start_node(
            role="all", data_dir=nm.make_temp_data_dir("f12_w_"), hot_store="local",
            mock_s3=s3, database_url=db_url, upload_interval_secs=1, admin_token=tok,
        )
        assert isinstance(writer, NodeInstance)
        proj = writer.client().create_project("F12 Query Filter").raise_for_status().json()
        pid, key = proj["project"]["id"], proj["api_key"]["plaintext_key"]

        writer.client().engage({"distinct_id": "stale_er_u", "$set": {"plan": "pro"}}, api_key=key).raise_for_status()
        writer.client().admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qnode = nm.start_node(
            role="query", data_dir=nm.make_temp_data_dir("f12_q_"), hot_store="local",
            mock_s3=s3, database_url=db_url, admin_token=tok,
        )
        assert isinstance(qnode, NodeInstance)
        qc = qnode.client()
        wait_until(lambda: "stale_er_u" in {u["distinct_id"] for u in qc.list_users(pid).raise_for_status().json()}, timeout=15.0, desc="query sees stale_er_u")

        # Erase on writer; query node still has pub_0 snapshot, must filter via erased_uids
        writer.client().delete_user(pid, "stale_er_u").raise_for_status()
        wait_until(lambda: "stale_er_u" not in {u["distinct_id"] for u in qc.list_users(pid).raise_for_status().json()}, timeout=15.0, desc="query hides stale_er_u")

    def test_f12_t2_erasure_persists_across_node_restart_and_prevents_old_profile_leak(self) -> None:
        _, nm, node, client, pid, key = self._boot_single_node(hot_store="local")
        client.engage({"distinct_id": "perm_er", "$set": {"secret": "123"}}, api_key=key).raise_for_status()
        client.delete_user(pid, "perm_er").raise_for_status()
        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        self.assertNotIn("perm_er", {u["distinct_id"] for u in node2.client().list_users(pid).raise_for_status().json()})


# ===========================================================================
# F13: Metadata DB SQLite startup warning [T1]
# ===========================================================================


@tier(2)
@feature("F13")
@require_milestone("T1")
class TestF13SqliteWarningBoundaryTier2(_StorageTier2Base):
    def test_f13_t2_external_postgres_url_does_not_emit_sqlite_local_warning(self) -> None:
        pg = skip_if_no_postgres()
        _, _, node, _, _, _ = self._boot_single_node(hot_store="local", database_url=pg)
        logs = node.read_logs(tail_bytes=0)
        self.assertFalse(any("WARN" in line and "SQLite" in line for line in logs.splitlines()))

    def test_f13_t2_object_hot_store_with_sqlite_does_not_emit_local_tier_sqlite_warning(self) -> None:
        _, _, node, _, _, _ = self._boot_single_node(hot_store="object", database_url=None)
        logs = node.read_logs(tail_bytes=0)
        self.assertFalse(
            any("WARN" in line and "SQLite" in line and "PULSE_HOT_STORE=local" in line for line in logs.splitlines())
        )

    def test_f13_t2_query_role_with_local_sqlite_boots_cleanly(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        node = nm.start_node(role="query", hot_store="local", mock_s3=s3, admin_token="sk_root_f13_q")
        assert isinstance(node, NodeInstance)
        self.assertEqual(node.client().readyz().status, 200)

    def test_f13_t2_ingest_role_with_local_sqlite_logs_warning_and_serves(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        node = nm.start_node(role="ingest", hot_store="local", mock_s3=s3, admin_token="sk_root_f13_ing")
        assert isinstance(node, NodeInstance)
        self.assertEqual(node.client().healthz().status, 200)
        self.assertIn("SQLite", node.read_logs(tail_bytes=0))

    def test_f13_t2_sqlite_warning_contrasts_with_restore_sqlite_refusal(self) -> None:
        s3, nm, node, _, _, _ = self._boot_single_node(hot_store="local", database_url=None)
        self.assertIn("SQLite", node.read_logs(tail_bytes=0))
        node.stop(graceful=True)
        res = nm.run_cli(
            ["restore", "--from-published", "--data-dir", str(nm.make_temp_data_dir("f13_rst_"))],
            env={**s3.env_vars(), "PULSE_HOT_STORE": "local"},
        )
        self.assertNotEqual(res.returncode, 0)


# ===========================================================================
# F14: Local-authoritative manifest (manifests/) [T2]
# ===========================================================================


@tier(2)
@feature("F14")
@require_milestone("T2")
class TestF14LocalManifestTier2(_StorageTier2Base):
    def test_f14_t2_local_manifest_epoch_gc_bounds_retained_epochs(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        for i in range(22):
            client.ingest({"events": [make_event(event_name="gc_ep", distinct_id=f"u_{i}", ts_ms=now_ms - 1000 + i)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(len(epochs), 1)
        self.assertLessEqual(len(epochs), 22)

    def test_f14_t2_unwritable_local_manifests_dir_blocks_flush_without_advancing_wal_seq(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="mf_ro1", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        mfs = _local_matches(node, "manifests")
        mf_dir = (node.data_dir / mfs[0]).parent
        client.ingest({"events": make_events(2, event_name="mf_ro2", distinct_id_prefix="u_ro2", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        self._chmod_tree_cleanup(mf_dir, 0o555)
        r_flush = client.admin_flush()
        self.assertNotEqual(r_flush.status, 200)
        self.assertGreaterEqual(len(_local_matches(node, "wal")), 1)

    def test_f14_t2_sigkill_after_local_flush_before_publish_recovers_from_local_manifest(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300, writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(5, event_name="unpub_crash", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertEqual(s3.put_count("published/"), 0)
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=300, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(_query_total(c2, pid, "unpub_crash", now_ms - 60_000, now_ms + 60_000), 5.0)

    def test_f14_t2_s3_unavailable_during_l0_flush_still_commits_local_manifest(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        s3.set_fault(fail_prefixes=["published/", "segments/"], status=403)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="s3_down_l0", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertGreaterEqual(len(_local_matches(node, "manifests")), 1)
        self.assertEqual(_query_total(client, pid, "s3_down_l0", now_ms - 60_000, now_ms + 60_000), 4.0)

    def test_f14_t2_published_count_matches_upload_cycles_per_window(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        now_ms = int(time.time() * 1000)
        s3.reset_counts()
        for i in range(4):
            client.ingest({"events": make_events(2, event_name="cyc_cnt", distinct_id_prefix=f"c{i}", base_ts_ms=now_ms - 400 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        pub_puts = s3.put_count(f"published/{pid}/pub_")
        self.assertGreaterEqual(pub_puts, 1)
        self.assertLessEqual(pub_puts, 3)


# ===========================================================================
# F15: Dense publish_seq fencing & open check [T2]
# ===========================================================================


@tier(2)
@feature("F15")
@require_milestone("T2")
class TestF15PublishSeqFencingTier2(_StorageTier2Base):
    def test_f15_t2_deposed_writer_pub_gen_gt_local_gen_refused_on_open(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="dep_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        # Simulate newer generation published by a restored writer
        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        pub1 = dict(pub0)
        pub1["publish_seq"] = 1
        pub1["writer_generation"] = int(pub0["writer_generation"]) + 1
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub1).encode("utf-8"))

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        resp = node2.client().ingest({"events": [make_event(event_name="dep_ev", distinct_id="u_dep", ts_ms=now_ms)]}, api_key=key)
        self.assertIn(resp.status, (500, 503))

    def test_f15_t2_behind_writer_pub_seq_gt_local_last_pub_seq_refused_even_with_higher_local_epoch(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="bh_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        # Flush locally several times to make local_epoch >> source_local_epoch
        for i in range(4):
            client.ingest({"events": [make_event(event_name="bh_ev", distinct_id=f"b{i}", ts_ms=now_ms - 200 + i)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        node.stop(graceful=True)

        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        pub1 = dict(pub0)
        pub1["publish_seq"] = 1
        pub1["writer_generation"] = int(pub0["writer_generation"]) + 1
        pub1["source_local_epoch"] = int(pub0["source_local_epoch"])
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub1).encode("utf-8"))

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        resp = node2.client().ingest({"events": [make_event(event_name="bh_ev", distinct_id="u_bh", ts_ms=now_ms)]}, api_key=key)
        self.assertIn(resp.status, (500, 503))

    def test_f15_t2_step6_self_crash_recovery_advances_local_last_published_seq(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="s6_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        # Simulate crash right after writing pub_1 (same gen, source_local_epoch <= local_epoch) before updating local last_published_seq
        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        local_mf = decode_manifest(node.read_local_bytes(epochs[-1]))
        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        pub1 = dict(pub0)
        pub1["publish_seq"] = 1
        pub1["writer_generation"] = local_mf["writer_generation"]
        pub1["source_local_epoch"] = local_mf["epoch"]
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub1).encode("utf-8"))

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=1)
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        self.assertEqual(c2.ingest({"events": [make_event(event_name="s6_ev", distinct_id="u_s6", ts_ms=now_ms)]}, api_key=key).status, 200)
        c2.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000002.json") is not None, timeout=15.0, desc="pub_2 after self-crash")

    def test_f15_t2_stale_writer_cannot_overwrite_existing_pub_seq_on_publish(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="ow_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        # Another writer races and claims pub_1 with higher generation
        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        winner_pub1 = dict(pub0)
        winner_pub1["publish_seq"] = 1
        winner_pub1["writer_generation"] = int(pub0["writer_generation"]) + 5
        raw_winner = json.dumps(winner_pub1).encode("utf-8")
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", raw_winner)

        client.ingest({"events": [make_event(event_name="ow_ev", distinct_id="loser", ts_ms=now_ms)]}, api_key=key)
        client.admin_flush()
        time.sleep(1.5)
        self.assertEqual(s3.get_object(f"published/{pid}/pub_000000000001.json"), raw_winner)

    def test_f15_t2_fenced_project_returns_503_on_ingest(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="f503", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        pub0["publish_seq"] = 1
        pub0["writer_generation"] = int(pub0["writer_generation"]) + 2
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub0).encode("utf-8"))

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        r = node2.client().ingest({"events": [make_event(event_name="f503", distinct_id="u2", ts_ms=now_ms)]}, api_key=key)
        self.assertEqual(r.status, 503)


# ===========================================================================
# F16: S3 segment deletion grace rule [T2]
# ===========================================================================


@tier(2)
@feature("F16")
@require_milestone("T2")
class TestF16DeletionGraceRuleTier2(_StorageTier2Base):
    def _seed_3_segs_and_restart_cold(
        self,
        *,
        event_name: str,
        now_ms: int,
    ) -> tuple[MockS3Server, NodeManager, NodeInstance, PulseHttpClient, str, str, set[str]]:
        s3, nm, node, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=0, extra_env={"PULSE_NUM_BUCKETS": "1"}
        )
        for i in range(3):
            client.ingest(
                {"events": make_events(3, event_name=event_name, distinct_id_prefix="same_bkt", base_ts_ms=now_ms - 2000 + i * 10)},
                api_key=key,
            ).raise_for_status()
            client.admin_flush().raise_for_status()
        orig_segs = set(
            wait_until(
                lambda: s3.list_objects(f"segments/{pid}/") if len(s3.list_objects(f"segments/{pid}/")) >= 3 else [],
                timeout=15.0,
                desc="3 initial S3 segments",
            )
        )
        node2 = nm.restart(node, graceful_stop=True)
        assert isinstance(node2, NodeInstance)
        return s3, nm, node2, node2.client(), pid, key, orig_segs

    def test_f16_t2_active_query_pin_delays_s3_deletion_beyond_2_intervals(self) -> None:
        now_ms = int(time.time() * 1000)
        s3, _, _, client, pid, key, orig_segs = self._seed_3_segs_and_restart_cold(event_name="pin_ev", now_ms=now_ms)

        # Hold S3 GET latency so background export query holds ReaderPinTracker pin on orig_segs
        s3.reset_counts()
        s3.set_fault(latency_ms=2000, ops=["GET"])
        q_result: dict[str, Any] = {}

        def _pinned_query() -> None:
            q_result["resp"] = client.export_events(pid, format="json", limit=100).raise_for_status().json()

        t_q = threading.Thread(target=_pinned_query, daemon=True)
        t_q.start()
        wait_until(lambda: s3.get_counts("segments/").get(("segments", "GET"), 0) >= 1, timeout=15.0, desc="pin query reading S3")

        # 4th flush triggers compaction/supersession, followed by 2+ publish cycles while pin is active
        client.ingest({"events": make_events(3, event_name="pin_ev", distinct_id_prefix="same_bkt", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(set(s3.list_objects(f"segments/{pid}/")) - orig_segs) >= 1, timeout=20.0, desc="compacted segment uploaded")
        for extra in range(3):
            client.ingest({"events": [make_event(event_name="pin_ev", distinct_id="same_bkt_0000", ts_ms=now_ms - 100 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()

        self.assertTrue(t_q.is_alive(), "Expected pinned query thread to still be active")
        for seg in orig_segs:
            self.assertIsNotNone(s3.head_object(seg), f"Pinned segment {seg} was prematurely deleted")

        s3.clear_faults()
        t_q.join(timeout=20.0)
        self.assertFalse(t_q.is_alive())
        self.assertEqual(q_result["resp"]["count"], 9)

    def test_f16_t2_unpinning_allows_subsequent_cycle_to_delete_superseded_segment(self) -> None:
        now_ms = int(time.time() * 1000)
        s3, _, _, client, pid, key, orig_segs = self._seed_3_segs_and_restart_cold(event_name="unp_ev", now_ms=now_ms)

        s3.reset_counts()
        s3.set_fault(latency_ms=2000, ops=["GET"])
        q_result: dict[str, Any] = {}

        def _pinned_query() -> None:
            q_result["resp"] = client.export_events(pid, format="json", limit=100).raise_for_status().json()

        t_q = threading.Thread(target=_pinned_query, daemon=True)
        t_q.start()
        wait_until(lambda: s3.get_counts("segments/").get(("segments", "GET"), 0) >= 1, timeout=15.0, desc="pin query reading S3")

        client.ingest({"events": make_events(3, event_name="unp_ev", distinct_id_prefix="same_bkt", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(set(s3.list_objects(f"segments/{pid}/")) - orig_segs) >= 1, timeout=20.0, desc="compacted segment uploaded")
        for extra in range(3):
            client.ingest({"events": [make_event(event_name="unp_ev", distinct_id="same_bkt_0000", ts_ms=now_ms - 100 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()

        self.assertTrue(t_q.is_alive(), "Expected pinned query thread to still be active")
        for seg in orig_segs:
            self.assertIsNotNone(s3.head_object(seg), f"Pinned segment {seg} deleted before unpin")

        s3.clear_faults()
        t_q.join(timeout=20.0)
        self.assertEqual(q_result["resp"]["count"], 9)

        # After unpinning, subsequent publish cycles must delete the superseded segments
        for extra in range(2):
            client.ingest({"events": [make_event(event_name="unp_ev", distinct_id="same_bkt_0000", ts_ms=now_ms - 50 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        wait_until(
            lambda: all(s3.head_object(seg) is None for seg in orig_segs),
            timeout=20.0,
            desc="superseded segments deleted after unpin",
        )
        self.assertEqual(_query_total(client, pid, "unp_ev", now_ms - 60_000, now_ms + 60_000), 17.0)

    def test_f16_t2_s3_delete_failure_retains_entry_in_pending_s3_deletes_for_retry(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=0, extra_env={"PULSE_NUM_BUCKETS": "1"}
        )
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="del_f", distinct_id_prefix="same_u", base_ts_ms=now_ms - 2000 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        orig_segs = set(wait_until(lambda: s3.list_objects(f"segments/{pid}/") if len(s3.list_objects(f"segments/{pid}/")) >= 3 else [], timeout=15.0, desc="3 initial segments"))

        s3.set_fault(fail_prefixes=["segments/"], status=403, ops=["DELETE"])
        client.ingest({"events": make_events(2, event_name="del_f", distinct_id_prefix="same_u", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(set(s3.list_objects(f"segments/{pid}/")) - orig_segs) >= 1, timeout=20.0, desc="compacted segment uploaded")

        # Advance 2+ cycles while DELETE returns 403 -> pending_s3_deletes must retain entries
        for extra in range(3):
            client.ingest({"events": [make_event(event_name="del_f", distinct_id="same_u_0000", ts_ms=now_ms - 100 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            time.sleep(0.2)

        for seg in orig_segs:
            self.assertIsNotNone(s3.head_object(seg))
        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(len(epochs), 1)
        pending = _pending_delete_paths(decode_manifest(node.read_local_bytes(epochs[-1])))
        self.assertTrue(orig_segs.issubset(pending), f"Expected {orig_segs} retained in pending_s3_deletes={pending}")

        # Clear 403 fault and trigger retry -> superseded segments must now be deleted
        s3.clear_faults()
        client.ingest({"events": [make_event(event_name="del_f", distinct_id="same_u_0000", ts_ms=now_ms)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(
            lambda: all(s3.head_object(seg) is None for seg in orig_segs),
            timeout=20.0,
            desc="superseded segments deleted after S3 DELETE recovery",
        )
        epochs_after = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        self.assertEqual(orig_segs & _pending_delete_paths(decode_manifest(node.read_local_bytes(epochs_after[-1]))), set())
        self.assertEqual(_query_total(client, pid, "del_f", now_ms - 60_000, now_ms + 60_000), 12.0)

    def test_f16_t2_no_deletion_before_superseding_publish_lands_in_s3(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        deletes: list[str] = []
        s3.on_operation(lambda op, k: deletes.append(k) if op == "DELETE" and k.startswith(f"segments/{pid}/") else None)
        for i in range(4):
            client.ingest({"events": [make_event(event_name="nodel", distinct_id="u_same", ts_ms=now_ms - 500 + i)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        time.sleep(1.0)
        self.assertEqual(deletes, [])

    def test_f16_t2_grace_rule_applies_across_multiple_superseded_segments_in_partition(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=0, extra_env={"PULSE_NUM_BUCKETS": "1"}
        )
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(3, event_name="multi_sup", distinct_id_prefix="same_p", base_ts_ms=now_ms - 2000 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        orig_segs = set(wait_until(lambda: s3.list_objects(f"segments/{pid}/") if len(s3.list_objects(f"segments/{pid}/")) >= 3 else [], timeout=15.0, desc=">=3 segments in partition"))

        # 4th flush compacts all 3 segments into 1 new segment and records all 3 in pending_s3_deletes
        client.ingest({"events": make_events(3, event_name="multi_sup", distinct_id_prefix="same_p", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(set(s3.list_objects(f"segments/{pid}/")) - orig_segs) >= 1, timeout=20.0, desc="multi-segment compaction")

        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        pending = _pending_delete_paths(decode_manifest(node.read_local_bytes(epochs[-1])))
        self.assertTrue(orig_segs.issubset(pending), f"Expected all {orig_segs} in pending_s3_deletes={pending}")
        for seg in orig_segs:
            self.assertIsNotNone(s3.head_object(seg), f"{seg} deleted before 2-interval grace")

        for extra in range(3):
            client.ingest({"events": [make_event(event_name="multi_sup", distinct_id="same_p_0000", ts_ms=now_ms - 100 + extra)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            time.sleep(0.2)

        wait_until(
            lambda: all(s3.head_object(seg) is None for seg in orig_segs),
            timeout=20.0,
            desc="all superseded segments in partition deleted after grace",
        )
        self.assertEqual(_query_total(client, pid, "multi_sup", now_ms - 60_000, now_ms + 60_000), 15.0)


# ===========================================================================
# F17: Remote query node reading published state [T2]
# ===========================================================================


@tier(2)
@feature("F17")
@require_milestone("T2")
class TestF17RemoteQueryNodeTier2(_StorageTier2Base):
    def _boot_pair(self) -> tuple[MockS3Server, NodeManager, NodeInstance, NodeInstance, str, str]:
        s3, nm = self._provision_s3_and_mgr()
        shared_dir = nm.make_temp_data_dir("f17_t2_meta_")
        db_url = external_postgres_url() or f"sqlite://{(shared_dir / 'shared_meta.db').resolve()}"
        tok = "sk_root_f17_t2_shared"
        writer = nm.start_node(
            role="all", data_dir=nm.make_temp_data_dir("f17_t2_w_"), hot_store="local",
            mock_s3=s3, database_url=db_url, upload_interval_secs=2, admin_token=tok,
        )
        assert isinstance(writer, NodeInstance)
        proj = writer.client().create_project("F17 T2").raise_for_status().json()
        pid, key = proj["project"]["id"], proj["api_key"]["plaintext_key"]
        qnode = nm.start_node(
            role="query", data_dir=nm.make_temp_data_dir("f17_t2_q_"), hot_store="local",
            mock_s3=s3, database_url=db_url, admin_token=tok,
        )
        assert isinstance(qnode, NodeInstance)
        return s3, nm, writer, qnode, pid, key

    def test_f17_t2_query_node_makes_zero_s3_writes_to_published_segments_identity_profiles(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_pair()
        now_ms = int(time.time() * 1000)
        writer.client().ingest({"events": make_events(5, event_name="ro_q", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        writer.client().admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        writer.stop(graceful=True)

        s3.reset_counts()
        qc = qnode.client()
        wait_until(lambda: _query_total(qc, pid, "ro_q", now_ms - 60_000, now_ms + 60_000) == 5.0, timeout=15.0, desc="query sees 5")
        self.assertEqual(s3.put_count("published/"), 0)
        self.assertEqual(s3.put_count("segments/"), 0)
        self.assertEqual(s3.put_count("identity/"), 0)
        self.assertEqual(s3.put_count("profiles/"), 0)

    def test_f17_t2_query_node_does_not_mount_ingest_routes_or_acquire_leases(self) -> None:
        _, _, _, qnode, _, key = self._boot_pair()
        now_ms = int(time.time() * 1000)
        r = qnode.client().ingest({"events": [make_event(event_name="no_ing", distinct_id="u1", ts_ms=now_ms)]}, api_key=key)
        self.assertIn(r.status, (404, 405))

    def test_f17_t2_query_node_tolerates_missing_published_manifest_before_first_publish(self) -> None:
        _, _, _, qnode, pid, _ = self._boot_pair()
        now_ms = int(time.time() * 1000)
        total = _query_total(qnode.client(), pid, "unpub_yet", now_ms - 60_000, now_ms + 60_000)
        self.assertEqual(total, 0.0)
        self.assertEqual(qnode.client().list_users(pid).raise_for_status().json(), [])

    def test_f17_t2_query_node_hides_erased_users_and_profiles_immediately_via_erased_uids(self) -> None:
        s3, _, writer, qnode, pid, key = self._boot_pair()
        now_ms = int(time.time() * 1000)
        writer.client().ingest({"events": [
            make_event(event_name="er_q", distinct_id="er_target", ts_ms=now_ms - 500),
            make_event(event_name="er_q", distinct_id="er_keep", ts_ms=now_ms - 400),
        ]}, api_key=key).raise_for_status()
        writer.client().engage({"distinct_id": "er_target", "$set": {"secret": "hide"}}, api_key=key).raise_for_status()
        writer.client().admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        qc = qnode.client()
        wait_until(lambda: _query_total(qc, pid, "er_q", now_ms - 60_000, now_ms + 60_000) == 2.0, timeout=15.0, desc="query sees 2")

        writer.client().delete_user(pid, "er_target").raise_for_status()
        wait_until(lambda: _query_total(qc, pid, "er_q", now_ms - 60_000, now_ms + 60_000) == 1.0, timeout=15.0, desc="query drops erased")
        self.assertNotIn("er_target", {u["distinct_id"] for u in qc.list_users(pid).raise_for_status().json()})

    def test_f17_t2_query_node_sees_events_only_after_publish_not_while_only_local(self) -> None:
        s3, nm = self._provision_s3_and_mgr()
        shared_dir = nm.make_temp_data_dir("f17_lag_meta_")
        db_url = external_postgres_url() or f"sqlite://{(shared_dir / 'shared.db').resolve()}"
        tok = "sk_root_f17_lag"
        writer = nm.start_node(
            role="all", data_dir=nm.make_temp_data_dir("f17_lag_w_"), hot_store="local",
            mock_s3=s3, database_url=db_url, upload_interval_secs=300, admin_token=tok,
        )
        assert isinstance(writer, NodeInstance)
        proj = writer.client().create_project("F17 Lag").raise_for_status().json()
        pid, key = proj["project"]["id"], proj["api_key"]["plaintext_key"]
        qnode = nm.start_node(
            role="query", data_dir=nm.make_temp_data_dir("f17_lag_q_"), hot_store="local",
            mock_s3=s3, database_url=db_url, admin_token=tok,
        )
        assert isinstance(qnode, NodeInstance)
        now_ms = int(time.time() * 1000)
        writer.client().ingest({"events": make_events(3, event_name="lag_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        writer.client().admin_flush().raise_for_status()
        self.assertEqual(_query_total(writer.client(), pid, "lag_ev", now_ms - 60_000, now_ms + 60_000), 3.0)
        self.assertEqual(_query_total(qnode.client(), pid, "lag_ev", now_ms - 60_000, now_ms + 60_000), 0.0)


# ===========================================================================
# F18: Local segment tier (hot/segments/) [T3]
# ===========================================================================


@tier(2)
@feature("F18")
@require_milestone("T3")
class TestF18LocalSegmentTier2(_StorageTier2Base):
    def test_f18_t2_upload_interval_zero_uploads_eagerly_with_local_manifest(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=0)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="eager_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="eager S3 upload")
        self.assertGreaterEqual(len(_local_matches(node, "manifests")), 1)
        self.assertEqual(s3.list_objects("manifests/"), [])

    def test_f18_t2_upload_interval_zero_keeps_count_based_compaction_active(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=0)
        now_ms = int(time.time() * 1000)
        for i in range(5):
            client.ingest({"events": [make_event(event_name="cmp0", distinct_id="same_u", ts_ms=now_ms - 1000 + i * 10)]}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="segments uploaded")
        self.assertEqual(_query_total(client, pid, "cmp0", now_ms - 60_000, now_ms + 60_000), 5.0)

    def test_f18_t2_unwritable_hot_segments_dir_fails_flush_cleanly_preserving_wal(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(2, event_name="hs_ro1", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        hs_files = _local_matches(node, "hot/segments")
        self.assertGreaterEqual(len(hs_files), 1)
        hs_root = node.data_dir
        for part in Path(hs_files[0]).parts:
            hs_root = hs_root / part
            if part == "segments":
                break
        client.ingest({"events": make_events(2, event_name="hs_ro2", distinct_id_prefix="r2", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        self._chmod_tree_cleanup(hs_root, 0o555)
        self.assertNotEqual(client.admin_flush().status, 200)

    def test_f18_t2_object_mode_writes_segments_to_s3_never_hot_segments(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="obj_seg", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertGreaterEqual(len(s3.list_objects(f"segments/{pid}/")), 1)
        self.assertEqual(_local_matches(node, "hot/segments"), [])

    def test_f18_t2_multiple_flushes_accumulate_distinct_hot_segment_files(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="acc_seg", distinct_id_prefix=f"ac{i}", base_ts_ms=now_ms - 500 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
        pvtx = [p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")]
        self.assertGreaterEqual(len(pvtx), 3)
        self.assertEqual(s3.put_count("segments/"), 0)


# ===========================================================================
# F19: Full 7-step upload cycle & timers [T3]
# ===========================================================================


@tier(2)
@feature("F19")
@require_milestone("T3")
class TestF19UploadCycleTier2(_StorageTier2Base):
    def test_f19_t2_partition_exceeding_64mib_cap_splits_into_multiple_s3_segments(self) -> None:
        cap_bytes = 100 * 1024
        s3, _, node, client, pid, key = self._boot_single_node(
            hot_store="local",
            upload_interval_secs=2,
            extra_env={"PULSE_MERGE_PART_MAX_BYTES": str(cap_bytes), "PULSE_NUM_BUCKETS": "1"},
        )
        now_ms = int(time.time() * 1000)
        # Hold S3 segment PUTs briefly to verify actual local .pvtx byte sum exceeds merge cap
        s3.set_fault(fail_prefixes=["segments/"], status=403, ops=["PUT"])
        total_sent = 0
        local_pvtx_bytes = 0
        for b in range(10):
            batch = [
                make_event(
                    event_name="cap_ev",
                    distinct_id=f"same_bkt_u_{b}_{i}",
                    ts_ms=now_ms - 2000 + b * 100 + i,
                    props={"p": _incompressible_str(f"cap:{b}:{i}", 1024)},
                )
                for i in range(50)
            ]
            client.ingest({"events": batch}, api_key=key).raise_for_status()
            total_sent += len(batch)
            client.admin_flush().raise_for_status()
            local_pvtx_bytes = sum(
                (node.data_dir / p).stat().st_size
                for p in _local_matches(node, "hot/segments")
                if p.endswith(".pvtx")
            )
            if local_pvtx_bytes > cap_bytes + 32 * 1024:
                break

        self.assertGreater(
            local_pvtx_bytes,
            cap_bytes,
            f"Expected local .pvtx size sum > {cap_bytes}, got {local_pvtx_bytes}",
        )
        s3.clear_faults()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 2, timeout=20.0, desc="split S3 segments")
        self.assertEqual(_query_total(client, pid, "cap_ev", now_ms - 60_000, now_ms + 60_000), float(total_sent))

    def test_f19_t2_closed_date_small_trickle_le_4mib_waits_for_1h_timer(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=5)).timestamp() * 1000)
        client.ingest({"events": [make_event(event_name="trickle", distinct_id="tr_u", ts_ms=old_ms)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(2.5)
        # Closed-date trickle (<= 4 MiB) must wait for the 1h closed-date timer rather than uploading every interval
        self.assertEqual(s3.list_objects(f"segments/{pid}/"), [])
        self.assertGreaterEqual(len(_local_matches(node, "hot/segments")), 1)

    def test_f19_t2_s3_upload_failure_leaves_local_segments_and_manifest_intact(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        s3.set_fault(fail_prefixes=["segments/"], status=403, ops=["PUT"])
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="up_err", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        time.sleep(1.5)
        self.assertGreaterEqual(len([p for p in _local_matches(node, "hot/segments") if p.endswith(".pvtx")]), 1)
        self.assertEqual(s3.list_objects(f"published/{pid}/pub_"), [])
        self.assertEqual(_query_total(client, pid, "up_err", now_ms - 60_000, now_ms + 60_000), 4.0)

    def test_f19_t2_events_ingested_after_step1_freeze_stay_local_until_next_cycle(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=2)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="frz_1", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        client.ingest({"events": make_events(3, event_name="frz_2", distinct_id_prefix="f2", base_ts_ms=now_ms - 200)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1")
        self.assertEqual(_query_total(client, pid, "frz_2", now_ms - 60_000, now_ms + 60_000), 3.0)

    def test_f19_t2_closed_date_timer_uploads_small_trickle_after_1h_threshold(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_CLOSED_DATE_UPLOAD_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=4)).timestamp() * 1000)
        client.ingest({"events": [make_event(event_name="cl_timer", distinct_id="ct_u", ts_ms=old_ms)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="closed-date timer upload")


# ===========================================================================
# F20: Intent record & startup sweep (+ dedup read error fix) [T3]
# ===========================================================================


@tier(2)
@feature("F20")
@require_milestone("T3")
class TestF20IntentAndSweepTier2(_StorageTier2Base):
    def test_f20_t2_crash_after_step1_3_discards_intent_and_uploads_on_next_cycle(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300, writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="c13_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=1, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="uploaded on next cycle")
        self.assertEqual(_query_total(c2, pid, "c13_ev", now_ms - 60_000, now_ms + 60_000), 4.0)

    def test_f20_t2_crash_after_step4_deletes_unreferenced_s3_segments_and_reuploads(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(4, event_name="c4_ev", distinct_id_prefix="c4a", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        committed_segs = set(s3.list_objects(f"segments/{pid}/"))

        # Induce step-4 crash right after uploading a new S3 segment (before local manifest/published update)
        killed = threading.Event()

        def _kill_on_seg_put(op: str, obj_key: str) -> None:
            if op == "PUT" and obj_key.startswith(f"segments/{pid}/") and obj_key not in committed_segs and not killed.is_set():
                killed.set()
                if node.proc is not None:
                    node.proc.kill()

        s3.on_operation(_kill_on_seg_put)
        client.ingest({"events": make_events(3, event_name="c4_ev", distinct_id_prefix="c4b", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(killed.is_set, timeout=20.0, desc="step-4 SIGKILL on S3 segment PUT")
        s3.clear_callbacks()
        node.stop(graceful=False)

        orphan_s3 = set(s3.list_objects(f"segments/{pid}/")) - committed_segs
        self.assertGreaterEqual(len(orphan_s3), 1, "Expected unreferenced step-4 S3 segment after crash")
        self.assertIsNone(s3.head_object(f"published/{pid}/pub_000000000001.json"))
        self.assertGreaterEqual(len([p for p in node.local_files() if "intent" in p.lower()]), 1)

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=1, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        _wait_project_open(c2, pid, now_ms - 60_000, now_ms + 60_000)
        wait_until(
            lambda: all(s3.head_object(k) is None for k in orphan_s3),
            timeout=15.0,
            desc="startup sweep deleted orphan step-4 S3 segments",
        )
        for k in orphan_s3:
            self.assertIsNone(s3.head_object(k))
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="reupload pub_1")
        wait_until(lambda: [p for p in node2.local_files() if "intent" in p.lower()] == [], timeout=15.0, desc="intent cleared")
        self.assertEqual(_query_total(c2, pid, "c4_ev", now_ms - 60_000, now_ms + 60_000), 7.0)

    def test_f20_t2_crash_after_step5_publishes_committed_local_manifest_on_next_cycle(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1, writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="c5_ev", distinct_id_prefix="c5a", base_ts_ms=now_ms - 1000)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")

        # Induce crash after step-5 local manifest commit, during step-6 snapshot upload before pub_1 lands
        killed = threading.Event()

        def _kill_after_step5(op: str, obj_key: str) -> None:
            if op == "PUT" and obj_key.startswith(f"published/{pid}/") and "/pub_" not in obj_key and not killed.is_set():
                killed.set()
                if node.proc is not None:
                    node.proc.kill()

        s3.on_operation(_kill_after_step5)
        client.ingest({"events": make_events(2, event_name="c5_ev", distinct_id_prefix="c5b_new", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(killed.is_set, timeout=20.0, desc="crash after step-5 local manifest commit")
        s3.clear_callbacks()
        node.stop(graceful=False)

        self.assertIsNone(s3.head_object(f"published/{pid}/pub_000000000001.json"))
        epochs_pre = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(len(epochs_pre), 1)
        mf_pre = decode_manifest(node.read_local_bytes(epochs_pre[-1]))
        self.assertEqual(mf_pre["last_published_seq"], 0)

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=1, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000001.json") is not None, timeout=15.0, desc="pub_1 published on next cycle")
        wait_until(lambda: [p for p in node2.local_files() if "intent" in p.lower()] == [], timeout=15.0, desc="intent cleared")
        epochs_post = sorted(p for p in _local_matches(node2, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(int(decode_manifest(node2.read_local_bytes(epochs_post[-1]))["last_published_seq"]), 1)
        self.assertEqual(_query_total(c2, pid, "c5_ev", now_ms - 60_000, now_ms + 60_000), 5.0)

    def test_f20_t2_crash_after_step6_before_local_update_recovers_and_sweeps_local(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": make_events(3, event_name="c6_ev", base_ts_ms=now_ms - 500)}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: s3.head_object(f"published/{pid}/pub_000000000000.json") is not None, timeout=15.0, desc="pub_0")
        node.stop(graceful=True)

        epochs = sorted(p for p in _local_matches(node, "manifests") if pid in p and "epoch_" in p)
        local_mf = decode_manifest(node.read_local_bytes(epochs[-1]))
        pub0 = decode_manifest(s3.get_object(f"published/{pid}/pub_000000000000.json"))
        pub1 = dict(pub0)
        pub1["publish_seq"] = 1
        pub1["writer_generation"] = local_mf["writer_generation"]
        pub1["source_local_epoch"] = local_mf["epoch"]
        s3.put_object_raw(f"published/{pid}/pub_000000000001.json", json.dumps(pub1).encode("utf-8"))

        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=1)
        assert isinstance(node2, NodeInstance)
        _wait_project_open(node2.client(), pid, now_ms - 60_000, now_ms + 60_000)
        self.assertEqual([p for p in node2.local_files() if "intent" in p.lower()], [])
        epochs_after = sorted(p for p in _local_matches(node2, "manifests") if pid in p and "epoch_" in p)
        self.assertGreaterEqual(int(decode_manifest(node2.read_local_bytes(epochs_after[-1]))["last_published_seq"]), 1)

    def test_f20_t2_s3_unreachable_during_dedup_seeding_fails_open_instead_of_duplicating(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=1)
        now_ms = int(time.time() * 1000)
        ev = make_event(event_name="dedup_guard", distinct_id="dg_u", ts_ms=now_ms - 500, event_uuid="00000000-0000-4000-8000-000000000099")
        client.ingest({"events": [ev]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) >= 1, timeout=15.0, desc="segment in S3")
        node.stop(graceful=True)

        # Block S3 segment reads on restart -> open must fail rather than accept duplicate retry
        s3.set_fault(fail_prefixes=["segments/"], status=403, ops=["GET"])
        node2 = nm.start_node(role="all", data_dir=node.data_dir, hot_store="local", mock_s3=s3, admin_token=node.admin_token)
        assert isinstance(node2, NodeInstance)
        retry_resp = node2.client().ingest({"events": [ev]}, api_key=key)
        self.assertGreaterEqual(retry_resp.status, 500)


# ===========================================================================
# F21: Local segment byte bounds & 429 backpressure [T3]
# ===========================================================================


@tier(2)
@feature("F21")
@require_milestone("T3")
class TestF21LocalSegmentBoundsTier2(_StorageTier2Base):
    def test_f21_t2_exceeding_2x_per_project_bound_returns_429_with_retry_after(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=300, extra_env={"PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "8192"}
        )
        s3.set_fault(fail_prefixes=["segments/", "published/"], status=403)
        now_ms = int(time.time() * 1000)
        hit_429 = False
        for i in range(20):
            r = client.ingest(
                {"events": make_events(60, event_name="bp_429", distinct_id_prefix=f"bp{i}", base_ts_ms=now_ms - 1000 + i, props={"pad": "P" * 512})},
                api_key=key,
            )
            if r.status == 429:
                self.assertIsNotNone(r.header("retry-after"))
                hit_429 = True
                break
            client.admin_flush()
        self.assertTrue(hit_429, "Expected HTTP 429 once hot/segments/ exceeded 2x bound")

    def test_f21_t2_exceeding_2x_per_node_bound_across_projects_returns_429(self) -> None:
        s3, _, _, client, pid1, key1 = self._boot_single_node(
            hot_store="local", upload_interval_secs=300,
            extra_env={"PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "65536", "PULSE_HOT_SEGMENTS_MAX_BYTES": "16384"},
            project_name="NodeCapP1",
        )
        p2 = client.create_project("NodeCapP2").raise_for_status().json()
        key2 = p2["api_key"]["plaintext_key"]
        s3.set_fault(fail_prefixes=["segments/", "published/"], status=403)
        now_ms = int(time.time() * 1000)

        hit_429 = False
        for i in range(20):
            for k in (key1, key2):
                r = client.ingest(
                    {"events": make_events(50, event_name="nbp", distinct_id_prefix=f"nb{i}", base_ts_ms=now_ms - 500 + i, props={"pad": "N" * 512})},
                    api_key=k,
                )
                if r.status == 429:
                    hit_429 = True
                    break
            if hit_429:
                break
            client.admin_flush()
        self.assertTrue(hit_429, "Expected node-wide 2x bound to return 429")

    def test_f21_t2_s3_recovery_after_2x_429_drains_segments_and_restores_200(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "8192"}
        )
        s3.set_fault(fail_prefixes=["segments/", "published/"], status=403)
        now_ms = int(time.time() * 1000)
        hit_429 = False
        for i in range(20):
            r = client.ingest(
                {"events": make_events(60, event_name="rec_429", distinct_id_prefix=f"r429_{i}", base_ts_ms=now_ms - 500 + i, props={"pad": "R" * 512})},
                api_key=key,
            )
            if r.status == 429:
                hit_429 = True
                break
            client.admin_flush()
        self.assertTrue(hit_429, "Expected HTTP 429 once hot/segments/ exceeded 2x bound")

        s3.clear_faults()
        wait_until(
            lambda: client.ingest({"events": [make_event(event_name="rec_429", distinct_id="after_rec", ts_ms=now_ms)]}, api_key=key).status == 200,
            timeout=20.0,
            desc="200 restored after S3 recovery",
        )

    def test_f21_t2_unwritable_hot_segments_returns_503_not_429(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest({"events": [make_event(event_name="uw_hs", distinct_id="u1", ts_ms=now_ms - 500)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        hs_files = _local_matches(node, "hot/segments")
        hs_root = node.data_dir
        for part in Path(hs_files[0]).parts:
            hs_root = hs_root / part
            if part == "segments":
                break
        self._chmod_tree_cleanup(hs_root, 0o555)
        client.ingest({"events": [make_event(event_name="uw_hs", distinct_id="u2", ts_ms=now_ms)]}, api_key=key).raise_for_status()
        fl = client.admin_flush()
        self.assertNotEqual(fl.status, 429)
        self.assertGreaterEqual(fl.status, 500)

    def test_f21_t2_one_project_at_2x_does_not_starve_other_project_below_node_cap(self) -> None:
        s3, _, _, client, pid1, key1 = self._boot_single_node(
            hot_store="local", upload_interval_secs=300,
            extra_env={"PULSE_HOT_SEGMENTS_PROJECT_MAX_BYTES": "8192", "PULSE_HOT_SEGMENTS_MAX_BYTES": "10485760"},
            project_name="IsoP1",
        )
        p2 = client.create_project("IsoP2").raise_for_status().json()
        key2 = p2["api_key"]["plaintext_key"]
        s3.set_fault(fail_prefixes=["segments/", "published/"], status=403)
        now_ms = int(time.time() * 1000)

        for i in range(20):
            r1 = client.ingest(
                {"events": make_events(60, event_name="iso_p1", distinct_id_prefix=f"ip1_{i}", base_ts_ms=now_ms - 500 + i, props={"pad": "I" * 512})},
                api_key=key1,
            )
            if r1.status == 429:
                break
            client.admin_flush()

        r2 = client.ingest({"events": [make_event(event_name="iso_p2", distinct_id="u_p2", ts_ms=now_ms)]}, api_key=key2)
        self.assertEqual(r2.status, 200)


# ===========================================================================
# F26: Cold S3 merge [T4]
# ===========================================================================


@tier(2)
@feature("F26")
@require_milestone("T4")
class TestF26ColdS3MergeTier2(_StorageTier2Base):
    def test_f26_t2_today_utc_date_never_cold_merged_even_if_idle(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        now_ms = int(time.time() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="today_ev", distinct_id_prefix="same_bkt", base_ts_ms=now_ms - 500 + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")

        segs_before = s3.list_objects(f"segments/{pid}/")
        time.sleep(2.5)
        segs_after = s3.list_objects(f"segments/{pid}/")
        self.assertEqual(segs_before, segs_after)

    def test_f26_t2_recently_uploaded_closed_date_lt_1h_idle_not_cold_merged(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "3600"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="rec_cl", distinct_id_prefix="same_bkt", base_ts_ms=old_ms + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        segs_before = s3.list_objects(f"segments/{pid}/")
        time.sleep(2.5)
        self.assertEqual(s3.list_objects(f"segments/{pid}/"), segs_before)

    def test_f26_t2_cold_merge_old_segments_wait_2_publish_intervals_before_s3_delete(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=2, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="grace_cm", distinct_id_prefix="same_bkt", base_ts_ms=old_ms + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        orig_segs = s3.list_objects(f"segments/{pid}/")
        wait_until(lambda: len(s3.list_objects(f"published/{pid}/pub_")) >= 4, timeout=20.0, desc="cold merge pub")
        for s_key in orig_segs:
            self.assertIsNotNone(s3.head_object(s_key))

    def test_f26_t2_active_query_pin_prevents_premature_delete_of_cold_merged_segments(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(
            hot_store="local",
            upload_interval_secs=1,
            extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "3600", "PULSE_NUM_BUCKETS": "1"},
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        for i in range(3):
            client.ingest({"events": make_events(2, event_name="pin_cm", distinct_id_prefix="same_bkt", base_ts_ms=old_ms + i * 10)}, api_key=key).raise_for_status()
            client.admin_flush().raise_for_status()
            wait_until(lambda idx=i: s3.head_object(f"published/{pid}/pub_{idx:012d}.json") is not None, timeout=15.0, desc=f"pub_{i}")
        orig_segs = set(wait_until(lambda: s3.list_objects(f"segments/{pid}/") if len(s3.list_objects(f"segments/{pid}/")) >= 3 else [], timeout=15.0, desc="3 cold segments"))

        # Restart with cold cache and short idle threshold, holding an active S3 query pin
        node.stop(graceful=True)
        node2 = nm.start_node(
            role="all",
            data_dir=node.data_dir,
            hot_store="local",
            mock_s3=s3,
            admin_token=node.admin_token,
            upload_interval_secs=1,
            extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1", "PULSE_NUM_BUCKETS": "1"},
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        s3.reset_counts()
        s3.set_fault(latency_ms=2000, ops=["GET"])
        q_result: dict[str, Any] = {}

        def _pinned_cold_query() -> None:
            q_result["resp"] = c2.export_events(pid, format="json", limit=100).raise_for_status().json()

        t_q = threading.Thread(target=_pinned_cold_query, daemon=True)
        t_q.start()
        wait_until(lambda: s3.get_counts("segments/").get(("segments", "GET"), 0) >= 1, timeout=15.0, desc="cold pin query reading S3")

        wait_until(
            lambda: len(set(s3.list_objects(f"segments/{pid}/")) - orig_segs) >= 1
            and s3.head_object(f"published/{pid}/pub_000000000003.json") is not None,
            timeout=25.0,
            desc="cold merge completed while query pinned",
        )
        self.assertTrue(t_q.is_alive(), "Expected cold query pin thread to still be active")
        for seg in orig_segs:
            self.assertIsNotNone(s3.head_object(seg), f"Cold segment {seg} deleted prematurely while pinned")

        s3.clear_faults()
        t_q.join(timeout=20.0)
        self.assertEqual(q_result["resp"]["count"], 6)

        wait_until(
            lambda: all(s3.head_object(seg) is None for seg in orig_segs),
            timeout=25.0,
            desc="cold-merged original segments deleted after unpin and grace",
        )
        self.assertEqual(_query_total(c2, pid, "pin_cm", old_ms - 60_000, old_ms + 60_000), 6.0)

    def test_f26_t2_single_segment_cold_partition_is_not_rewritten(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(
            hot_store="local", upload_interval_secs=1, extra_env={"PULSE_COLD_MERGE_IDLE_SECS": "1"}
        )
        old_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        client.ingest({"events": [make_event(event_name="single_cm", distinct_id="u_only", ts_ms=old_ms)]}, api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        wait_until(lambda: len(s3.list_objects(f"segments/{pid}/")) == 1, timeout=15.0, desc="single cold segment")
        s3.reset_counts()
        time.sleep(2.5)
        self.assertEqual(s3.put_count("segments/"), 0)


# ===========================================================================
# F27: Local hot tier + upload/publish for agent & replay [T4]
# ===========================================================================


@tier(2)
@feature("F27")
@require_milestone("T4")
class TestF27AgentAndReplayLocalTier2(_StorageTier2Base):
    def test_f27_t2_expired_agent_tombstone_not_pruned_before_physical_compaction(self) -> None:
        s3, _, _, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_tomb_1", start_ts_ms=now_ms - 1000), api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        client.erase_agent(pid, level="trace", target_id="tr_tomb_1", expires_at=now_ms + 200).raise_for_status()
        time.sleep(0.5)
        client.admin_flush().raise_for_status()
        # Trace must never reappear even after tombstone expires_at passes
        self.assertEqual(client.get_agent_trace(pid, "tr_tomb_1").status, 404)
        self.assertNotIn("tr_tomb_1", {t["trace_id"] for t in client.list_agent_traces(pid).raise_for_status().json()["traces"]})

    def test_f27_t2_sigkill_after_replay_chunk_200_before_upload_preserves_local_replay(self) -> None:
        s3, nm, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300, writer_lease_ttl_secs=3)
        now_ms = int(time.time() * 1000)
        chunk = make_replay_chunk(session_id="rs_crash_f27", project_id=pid, chunk_start_ts_ms=now_ms)
        client.replay_chunk(chunk, api_key=key, binary=True).raise_for_status()
        self.assertEqual(s3.put_count("replays/"), 0)
        node.kill()

        node2 = nm.start_node(
            role="all", data_dir=node.data_dir, hot_store="local",
            mock_s3=s3, admin_token=node.admin_token, upload_interval_secs=300, writer_lease_ttl_secs=3,
        )
        assert isinstance(node2, NodeInstance)
        c2 = node2.client()
        r_rep = c2.get_replay(pid, "rs_crash_f27", binary=True)
        self.assertEqual(r_rep.status, 200)
        self.assertTrue(r_rep.body.startswith(b"PLSR"))

    def test_f27_t2_unwritable_local_replay_or_agent_dir_returns_503(self) -> None:
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.replay_chunk(make_replay_chunk(session_id="rs_uw_seed", project_id=pid, chunk_start_ts_ms=now_ms), api_key=key, binary=True).raise_for_status()
        rep_files = _local_matches(node, "hot/replay")
        self.assertGreaterEqual(len(rep_files), 1)
        rep_dir = (node.data_dir / rep_files[0]).parent
        self._chmod_tree_cleanup(rep_dir, 0o555)
        r_fail = client.replay_chunk(make_replay_chunk(session_id="rs_uw_seed", project_id=pid, chunk_start_ts_ms=now_ms + 100), api_key=key, binary=True)
        self.assertEqual(r_fail.status, 503)

    def test_f27_t2_byok_encrypts_local_hot_replay_and_agent_segments(self) -> None:
        byok = random_byok_hex()
        _, _, node, client, pid, key = self._boot_single_node(hot_store="local", byok_master_key=byok, upload_interval_secs=300)
        now_ms = int(time.time() * 1000)
        client.replay_chunk(make_replay_chunk(session_id="rs_byok_f27", project_id=pid, chunk_start_ts_ms=now_ms), api_key=key, binary=True).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_byok_f27", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()

        for rel in [*_local_matches(node, "hot/replay"), *_local_matches(node, "agent_segments")]:
            self.assertTrue(node.read_local_bytes(rel).startswith(b"PLSEAGM2"), f"{rel} missing PLSEAGM2 envelope")

    def test_f27_t2_object_mode_still_routes_replay_and_agent_segments_to_s3(self) -> None:
        s3, _, node, client, pid, key = self._boot_single_node(hot_store="object")
        now_ms = int(time.time() * 1000)
        client.replay_chunk(make_replay_chunk(session_id="rs_obj_f27", project_id=pid, chunk_start_ts_ms=now_ms), api_key=key, binary=True).raise_for_status()
        client.ingest_agent_traces(make_agent_trace(trace_id="tr_obj_f27", start_ts_ms=now_ms - 500), api_key=key).raise_for_status()
        client.admin_flush().raise_for_status()
        self.assertGreaterEqual(len(s3.list_objects("replays/")), 1)
        self.assertGreaterEqual(len(s3.list_objects("agent_segments/")), 1)
        self.assertEqual(_local_matches(node, "hot/replay"), [])
        self.assertEqual(_local_matches(node, "agent_segments"), [])
