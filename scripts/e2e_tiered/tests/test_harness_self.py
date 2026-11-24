"""Self-tests for the pure-stdlib E2E harness (`MockS3Server`, `MetricsParser`, `oracle`, and live `pulse-node`)."""

from __future__ import annotations

import http.client
import json
import math
import subprocess
import sys
import threading
import time
import unittest
import urllib.parse
import xml.etree.ElementTree as ET
from compression import zstd
from pathlib import Path

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib.client import MetricsParser, PlsbEncoder, PlsrEncoder, PlstEncoder  # noqa: E402
from lib.mock_s3 import MockS3Server  # noqa: E402
from lib.node_mgr import ClusterHarness, NodeInstance  # noqa: E402
from lib.oracle import (  # noqa: E402
    MILESTONES,
    decode_manifest,
    feature,
    get_active_gating,
    get_test_metadata,
    is_milestone_enabled,
    make_agent_trace,
    make_events,
    make_replay_chunk,
    random_byok_hex,
    require_milestone,
    set_active_gating,
    tier,
    wait_until,
)


def _s3_http(
    endpoint: str,
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    parsed = urllib.parse.urlsplit(endpoint)
    conn = http.client.HTTPConnection(parsed.hostname or "127.0.0.1", parsed.port or 80, timeout=10.0)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, raw
    finally:
        conn.close()


@tier("self")
class TestMockS3Server(unittest.TestCase):
    """Offline unit tests verifying S3 REST semantics, counters, and fault injection."""

    def setUp(self) -> None:
        self.s3 = MockS3Server(bucket="pulse-e2e").start()
        self.addCleanup(self.s3.stop)

    def test_put_get_head_delete_and_conditional_cas(self) -> None:
        # PUT object
        st, hdrs, _ = _s3_http(self.s3.endpoint, "PUT", "/pulse-e2e/segments/p1/seg_1.bin", body=b"hello-s3-world")
        self.assertEqual(st, 200)
        etag = hdrs["etag"]
        self.assertTrue(etag.startswith('"') and etag.endswith('"'))

        # Conditional If-None-Match: * must return 412 PreconditionFailed XML
        st2, _, body2 = _s3_http(
            self.s3.endpoint,
            "PUT",
            "/pulse-e2e/segments/p1/seg_1.bin",
            body=b"overwrite",
            headers={"If-None-Match": "*"},
        )
        self.assertEqual(st2, 412)
        self.assertIn(b"PreconditionFailed", body2)
        self.assertEqual(self.s3.get_object("segments/p1/seg_1.bin"), b"hello-s3-world")

        # Conditional If-Match mismatch -> 412, match -> 200
        st_bad, _, _ = _s3_http(
            self.s3.endpoint,
            "PUT",
            "/pulse-e2e/segments/p1/seg_1.bin",
            body=b"v2",
            headers={"If-Match": '"deadbeef"'},
        )
        self.assertEqual(st_bad, 412)
        st_ok, _, _ = _s3_http(
            self.s3.endpoint,
            "PUT",
            "/pulse-e2e/segments/p1/seg_1.bin",
            body=b"hello-v2",
            headers={"If-Match": etag},
        )
        self.assertEqual(st_ok, 200)

        # HEAD existing & missing
        st_h, hdrs_h, body_h = _s3_http(self.s3.endpoint, "HEAD", "/pulse-e2e/segments/p1/seg_1.bin")
        self.assertEqual(st_h, 200)
        self.assertEqual(hdrs_h["content-length"], "8")
        self.assertIn("last-modified", hdrs_h)
        self.assertEqual(body_h, b"")

        st_hm, _, _ = _s3_http(self.s3.endpoint, "HEAD", "/pulse-e2e/segments/missing.bin")
        self.assertEqual(st_hm, 404)

        # GET full & Range 206 & 416
        st_g, _, body_g = _s3_http(self.s3.endpoint, "GET", "/pulse-e2e/segments/p1/seg_1.bin")
        self.assertEqual(st_g, 200)
        self.assertEqual(body_g, b"hello-v2")

        st_r, hdrs_r, body_r = _s3_http(
            self.s3.endpoint,
            "GET",
            "/pulse-e2e/segments/p1/seg_1.bin",
            headers={"Range": "bytes=2-5"},
        )
        self.assertEqual(st_r, 206)
        self.assertEqual(body_r, b"llo-")
        self.assertEqual(hdrs_r["content-range"], "bytes 2-5/8")
        self.assertTrue(hdrs_r["etag"].startswith('"'))

        st_416, _, _ = _s3_http(
            self.s3.endpoint,
            "GET",
            "/pulse-e2e/segments/p1/seg_1.bin",
            headers={"Range": "bytes=100-200"},
        )
        self.assertEqual(st_416, 416)

        # DELETE single
        st_d, _, _ = _s3_http(self.s3.endpoint, "DELETE", "/pulse-e2e/segments/p1/seg_1.bin")
        self.assertEqual(st_d, 204)
        self.assertIsNone(self.s3.head_object("segments/p1/seg_1.bin"))

    def test_list_v2_pagination_delimiter_and_batch_delete(self) -> None:
        for k in (
            "segments/a/1.bin",
            "segments/a/2.bin",
            "segments/a/3.bin",
            "segments/b/1.bin",
            "published/p1/pub_000000000000.json",
        ):
            self.s3.put_object_raw(k, b"x")

        # Paginated ListObjectsV2 (max-keys=2)
        collected: list[str] = []
        token = ""
        pages = 0
        while True:
            qs = f"?list-type=2&prefix=segments/&max-keys=2"
            if token:
                qs += f"&continuation-token={urllib.parse.quote(token)}"
            st, _, raw = _s3_http(self.s3.endpoint, "GET", f"/pulse-e2e{qs}")
            self.assertEqual(st, 200)
            pages += 1
            root = ET.fromstring(raw)
            ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
            for c in root.findall(f"{ns}Contents"):
                collected.append(c.findtext(f"{ns}Key") or "")
            truncated = root.findtext(f"{ns}IsTruncated") == "true"
            if not truncated:
                break
            token = root.findtext(f"{ns}NextContinuationToken") or ""
            self.assertTrue(token)

        self.assertEqual(pages, 2)
        self.assertEqual(
            collected,
            ["segments/a/1.bin", "segments/a/2.bin", "segments/a/3.bin", "segments/b/1.bin"],
        )

        # Delimiter listing
        st_d, _, raw_d = _s3_http(
            self.s3.endpoint,
            "GET",
            "/pulse-e2e?list-type=2&prefix=segments/&delimiter=/",
        )
        self.assertEqual(st_d, 200)
        root_d = ET.fromstring(raw_d)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        prefixes = [cp.findtext(f"{ns}Prefix") for cp in root_d.findall(f"{ns}CommonPrefixes")]
        self.assertEqual(prefixes, ["segments/a/", "segments/b/"])

        # Batch DeleteObjects
        del_xml = (
            b"<Delete>"
            b"<Object><Key>segments/a/1.bin</Key></Object>"
            b"<Object><Key>segments/a/2.bin</Key></Object>"
            b"</Delete>"
        )
        st_bd, _, raw_bd = _s3_http(self.s3.endpoint, "POST", "/pulse-e2e?delete", body=del_xml)
        self.assertEqual(st_bd, 200)
        self.assertIn(b"<Deleted><Key>segments/a/1.bin</Key></Deleted>", raw_bd)
        self.assertEqual(self.s3.list_objects("segments/a/"), ["segments/a/3.bin"])

    def test_multipart_upload_and_copy_object(self) -> None:
        # Initiate multipart
        st, _, raw = _s3_http(self.s3.endpoint, "POST", "/pulse-e2e/identity/snap_1.bin?uploads")
        self.assertEqual(st, 200)
        root = ET.fromstring(raw)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        upload_id = root.findtext(f"{ns}UploadId")
        self.assertTrue(upload_id)

        # Upload parts out of order
        st2, h2, _ = _s3_http(
            self.s3.endpoint,
            "PUT",
            f"/pulse-e2e/identity/snap_1.bin?partNumber=2&uploadId={upload_id}",
            body=b"WORLD",
        )
        st1, h1, _ = _s3_http(
            self.s3.endpoint,
            "PUT",
            f"/pulse-e2e/identity/snap_1.bin?partNumber=1&uploadId={upload_id}",
            body=b"HELLO-",
        )
        self.assertEqual((st1, st2), (200, 200))

        # Complete multipart
        complete_xml = (
            f"<CompleteMultipartUpload>"
            f"<Part><PartNumber>1</PartNumber><ETag>{h1['etag']}</ETag></Part>"
            f"<Part><PartNumber>2</PartNumber><ETag>{h2['etag']}</ETag></Part>"
            f"</CompleteMultipartUpload>"
        ).encode("utf-8")
        st_c, _, raw_c = _s3_http(
            self.s3.endpoint,
            "POST",
            f"/pulse-e2e/identity/snap_1.bin?uploadId={upload_id}",
            body=complete_xml,
        )
        self.assertEqual(st_c, 200)
        self.assertIn(b"CompleteMultipartUploadResult", raw_c)
        self.assertEqual(self.s3.get_object("identity/snap_1.bin"), b"HELLO-WORLD")

        # Abort multipart
        st_ai, _, raw_ai = _s3_http(self.s3.endpoint, "POST", "/pulse-e2e/identity/aborted.bin?uploads")
        self.assertEqual(st_ai, 200)
        uid2 = ET.fromstring(raw_ai).findtext(f"{ns}UploadId")
        st_ab, _, _ = _s3_http(self.s3.endpoint, "DELETE", f"/pulse-e2e/identity/aborted.bin?uploadId={uid2}")
        self.assertEqual(st_ab, 204)
        self.assertIsNone(self.s3.get_object("identity/aborted.bin"))

        # CopyObject + If-None-Match CAS
        st_cp, _, raw_cp = _s3_http(
            self.s3.endpoint,
            "PUT",
            "/pulse-e2e/snapshots/snap_copy.bin",
            headers={"x-amz-copy-source": "/pulse-e2e/identity/snap_1.bin"},
        )
        self.assertEqual(st_cp, 200)
        self.assertIn(b"CopyObjectResult", raw_cp)
        self.assertEqual(self.s3.get_object("snapshots/snap_copy.bin"), b"HELLO-WORLD")

        st_cp_cas, _, _ = _s3_http(
            self.s3.endpoint,
            "PUT",
            "/pulse-e2e/snapshots/snap_copy.bin",
            headers={"x-amz-copy-source": "/pulse-e2e/identity/snap_1.bin", "If-None-Match": "*"},
        )
        self.assertEqual(st_cp_cas, 412)

    def test_fault_injection_counters_and_callbacks(self) -> None:
        events_seen: list[tuple[str, str]] = []
        self.s3.on_operation(lambda op, key: events_seen.append((op, key)))

        # Prefix-scoped fault injection
        self.s3.set_fault(fail_prefixes=["published/"], status=503, ops=["PUT"])
        st_fail, _, _ = _s3_http(self.s3.endpoint, "PUT", "/pulse-e2e/published/p1/pub_0.json", body=b"{}")
        self.assertEqual(st_fail, 503)
        st_ok, _, _ = _s3_http(self.s3.endpoint, "PUT", "/pulse-e2e/segments/p1/s.bin", body=b"abc")
        self.assertEqual(st_ok, 200)

        self.s3.clear_faults()
        st_ok2, _, _ = _s3_http(self.s3.endpoint, "PUT", "/pulse-e2e/published/p1/pub_0.json", body=b"{}")
        self.assertEqual(st_ok2, 200)

        # Unreachable drops socket
        self.s3.set_fault(unreachable=True)
        with self.assertRaises((http.client.HTTPException, OSError)):
            _s3_http(self.s3.endpoint, "GET", "/pulse-e2e/segments/p1/s.bin")
        self.s3.clear_faults()

        # Counters check
        self.assertEqual(self.s3.put_count("segments/"), 1)
        self.assertEqual(self.s3.put_count("published/"), 2)  # 1 faulted + 1 succeeded
        self.assertEqual(self.s3.put_count("wal/"), 0)
        self.assertGreaterEqual(self.s3.total_calls(), 4)
        self.assertIn(("PUT", "segments/p1/s.bin"), events_seen)

        self.s3.reset_counts()
        self.assertEqual(self.s3.total_calls(), 0)
        self.assertEqual(self.s3.get_counts(), {})

        # Thread-safety check
        def worker(idx: int) -> None:
            for j in range(10):
                _s3_http(self.s3.endpoint, "PUT", f"/pulse-e2e/concurrent/w{idx}_{j}.bin", body=b"ok")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.s3.put_count("concurrent/"), 40)


@tier("self")
class TestMetricsParserAndOracle(unittest.TestCase):
    """Offline unit tests for MetricsParser, oracle decorators, wait_until, and binary encoders."""

    def test_metrics_parser(self) -> None:
        text = """
# HELP pulse_ready Node readiness
# TYPE pulse_ready gauge
pulse_ready 1
pulse_ingest_events_accepted_total{project="p1",env="prod\\"1"} 42
pulse_latency_bucket{le="+Inf"} +Inf
pulse_weird_nan NaN
"""
        parsed = MetricsParser.parse(text)
        self.assertIsInstance(parsed, dict)
        self.assertEqual(parsed[("pulse_ready", frozenset())], 1.0)
        self.assertEqual(
            parsed[("pulse_ingest_events_accepted_total", frozenset({("project", "p1"), ("env", 'prod"1')}))],
            42.0,
        )
        self.assertEqual(parsed.get_metric("pulse_latency_bucket", le="+Inf"), math.inf)
        self.assertTrue(math.isnan(parsed[("pulse_weird_nan", frozenset())]))
        self.assertEqual(parsed.sum_by_name("pulse_ingest_events_accepted_total"), 42.0)

    def test_metrics_parser_braces_in_labels_and_escapes(self) -> None:
        text = (
            'http_requests_total{path="/v1/projects/{id}",method="GET"} 17\n'
            'err_total{msg="C:\\\\new\\\\path"} 5\n'
            'quote_metric{msg="say \\"hi\\"\\nbye"} 9\n'
        )
        parsed = MetricsParser.parse(text)
        self.assertEqual(
            parsed.get_metric("http_requests_total", path="/v1/projects/{id}", method="GET"),
            17.0,
        )
        self.assertEqual(
            parsed.get_metric("err_total", msg=r"C:\new\path"),
            5.0,
        )
        self.assertEqual(
            parsed.get_metric("quote_metric", msg='say "hi"\nbye'),
            9.0,
        )

    def test_runner_rejects_max_milestone_in_full_mode(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                str(_HARNESS_ROOT / "runner.py"),
                "--mode",
                "full",
                "--max-milestone",
                "T0a",
                "--no-build",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("--max-milestone", proc.stderr + proc.stdout)

    def test_decode_manifest_pzst_and_plain_json(self) -> None:
        sample = {"epoch": 3, "last_published_seq": 1, "pending_s3_deletes": []}
        raw_json = json.dumps(sample).encode("utf-8")
        raw_pzst = b"PZST" + zstd.compress(raw_json)
        self.assertEqual(decode_manifest(raw_json), sample)
        self.assertEqual(decode_manifest(raw_pzst), sample)

    def test_milestone_gating_and_metadata(self) -> None:
        self.assertEqual(MILESTONES[0], "T0a")
        self.assertEqual(MILESTONES[-1], "W3")

        prev_mode, prev_max = get_active_gating()
        try:
            set_active_gating(mode="baseline", max_milestone=None)
            self.assertTrue(is_milestone_enabled("T0a"))
            self.assertFalse(is_milestone_enabled("T0b"))

            set_active_gating(mode="full", max_milestone=None)
            self.assertTrue(is_milestone_enabled("W3"))

            @tier(2)
            @feature("F07", "F15")
            @require_milestone("T2")
            def sample_fn() -> str:
                return "ran"

            self.assertEqual(getattr(sample_fn, "_e2e_tier"), 2)
            self.assertEqual(getattr(sample_fn, "_e2e_features"), ("F07", "F15"))
            self.assertEqual(getattr(sample_fn, "_e2e_milestone"), "T2")
            self.assertEqual(sample_fn(), "ran")

            set_active_gating(mode="baseline", max_milestone="T1")
            with self.assertRaises(unittest.SkipTest):
                sample_fn()
        finally:
            set_active_gating(mode=prev_mode, max_milestone=prev_max)

        meta = get_test_metadata(self)
        self.assertEqual(meta["tier"], "self")

    def test_wait_until_and_fixtures_and_encoders(self) -> None:
        counter = {"n": 0}

        def inc() -> bool:
            counter["n"] += 1
            return counter["n"] >= 3

        self.assertTrue(wait_until(inc, timeout=2.0, interval=0.01, desc="counter>=3"))

        with self.assertRaises(TimeoutError):
            wait_until(lambda: False, timeout=0.05, interval=0.01, desc="always-false")

        evs = make_events(3, props={"price": 19.5, "ok": True, "count": 2, "note": "hi", "empty": None})
        plsb_raw = PlsbEncoder.encode(evs, use_dict=False)
        plsb_dict = PlsbEncoder.encode(evs, use_dict=True)
        self.assertTrue(plsb_raw.startswith(b"PLSB\x01\x01"))
        self.assertTrue(plsb_dict.startswith(b"PLSB\x01\x02"))

        chunk = make_replay_chunk()
        plsr = PlsrEncoder.encode(chunk)
        self.assertTrue(plsr.startswith(b"PLSR\x01\x01"))

        trace_batch = make_agent_trace()
        plst = PlstEncoder.encode(trace_batch)
        self.assertTrue(plst.startswith(b"PLST\x01"))

        key_hex = random_byok_hex()
        self.assertEqual(len(key_hex), 64)
        bytes.fromhex(key_hex)


@tier("self")
@feature("F01")
@require_milestone("T0a")
class TestLiveMockS3PulseNodeIntegration(unittest.TestCase):
    """Live opaque-box integration test: pulse-node + MockS3Server (S3 protocol + prefix counts)."""

    def test_live_node_ingest_flush_query_with_mock_s3(self) -> None:
        with ClusterHarness(use_mock_s3=True) as cluster:
            assert cluster.mock_s3 is not None
            s3 = cluster.mock_s3

            node = cluster.start_node(
                role="all",
                hot_store="local",
                upload_interval_secs=0,
                writer_lease_ttl_secs=3,
            )
            assert isinstance(node, NodeInstance)
            client = node.client()

            # Verify /healthz, /readyz, /metrics
            self.assertEqual(client.healthz().status, 200)
            self.assertEqual(client.readyz().status, 200)
            metrics = client.parsed_metrics()
            self.assertEqual(metrics.get_metric("pulse_ready"), 1.0)

            # Create project & extract ingest API key
            proj_resp = client.create_project("E2E MockS3 Smoke").raise_for_status().json()
            project_id = proj_resp["project"]["id"]
            api_key = proj_resp["api_key"]["plaintext_key"]
            self.assertTrue(api_key.startswith("pk_live_"))

            # Reset S3 counters after startup/project creation to measure ingest + flush cleanly
            s3.reset_counts()

            now_ms = int(time.time() * 1000)
            json_events = make_events(5, event_name="signup_clicked", distinct_id_prefix="u_json", base_ts_ms=now_ms - 5000)
            r1 = client.ingest({"events": json_events}, api_key=api_key).raise_for_status().json()
            self.assertEqual(r1["accepted"], 5)

            # Also exercise PLSB binary frame encoding against live pulse-node
            plsb_events = make_events(3, event_name="checkout_done", distinct_id_prefix="u_plsb", base_ts_ms=now_ms - 2000)
            r2 = client.ingest_plsb(plsb_events, api_key=api_key).raise_for_status().json()
            self.assertEqual(r2["accepted"], 3)

            # Ingest agent trace (JSON + PLST binary)
            tr1 = make_agent_trace(trace_id="tr_self_json_1", start_ts_ms=now_ms - 1000)
            ar1 = client.ingest_agent_traces(tr1, api_key=api_key).raise_for_status().json()
            self.assertEqual(ar1["accepted_traces"], 1)

            tr2 = make_agent_trace(trace_id="tr_self_plst_1", start_ts_ms=now_ms - 500)
            ar2 = client.ingest_agent_traces(tr2, api_key=api_key, binary=True).raise_for_status().json()
            self.assertEqual(ar2["accepted_traces"], 1)

            # Under PULSE_HOT_STORE=local, 0 WAL PUTs may hit MockS3Server!
            self.assertEqual(s3.put_count("wal/"), 0, f"Unexpected wal/ PUTs: {s3.get_counts()}")
            self.assertEqual(s3.put_count("agent_wal/"), 0, f"Unexpected agent_wal/ PUTs: {s3.get_counts()}")

            # Flush L0 to segments on MockS3Server
            flush_res = client.admin_flush().raise_for_status().json()
            self.assertEqual(flush_res["status"], "ok")

            # Verify segments/ PUTs arrived in MockS3Server and still 0 WAL PUTs
            self.assertGreaterEqual(
                s3.put_count("segments/"),
                1,
                f"Expected >= 1 segments/ PUT after flush; counts={s3.get_counts()}, objects={s3.list_objects()}",
            )
            self.assertTrue(
                any(k.startswith("segments/") for k in s3.list_objects()),
                f"Expected segments/ objects in MockS3; objects={s3.list_objects()}",
            )
            self.assertEqual(s3.put_count("wal/"), 0)
            self.assertEqual(s3.put_count("agent_wal/"), 0)

            # Query ingested events via /v1/query
            q_resp = client.query(
                {
                    "project_id": project_id,
                    "kind": "insights",
                    "query": {
                        "time_range": {"start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000},
                        "interval": "day",
                        "series": [
                            {"label": "Signups", "event_name": "signup_clicked", "metric": "totals"},
                            {"label": "Checkouts", "event_name": "checkout_done", "metric": "totals"},
                        ],
                    },
                }
            ).raise_for_status().json()
            totals = {s["label"]: s["overall_value"] for s in q_resp["series"]}
            self.assertEqual(totals["Signups"], 5.0)
            self.assertEqual(totals["Checkouts"], 3.0)

            # Query ingested agent traces
            tr_resp = client.list_agent_traces(project_id).raise_for_status().json()
            trace_ids = {t["trace_id"] for t in tr_resp["traces"]}
            self.assertIn("tr_self_json_1", trace_ids)
            self.assertIn("tr_self_plst_1", trace_ids)

            # Exercise PlsrEncoder live + Range 206 replay retrieval
            replay = make_replay_chunk(session_id="rsess_self_1", project_id=project_id, chunk_start_ts_ms=now_ms)
            rc_resp = client.replay_chunk(replay, api_key=api_key, binary=True).raise_for_status().json()
            self.assertEqual(rc_resp["status"], "ok")
            r_206 = client.get_replay(project_id, "rsess_self_1", byte_range="bytes=0-15")
            self.assertEqual(r_206.status, 206)
            self.assertTrue(r_206.body.startswith(b"PLSR"))

            # Exercise local disk helpers & SQLite inspection
            node.write_local_bytes("tmp_check/probe.bin", b"probe")
            self.assertIn("tmp_check/probe.bin", node.local_files("tmp_check/"))
            self.assertEqual(node.read_local_bytes("tmp_check/probe.bin"), b"probe")
            node.chmod_local("tmp_check/probe.bin", 0o600)
            node.rm_local("tmp_check")
            self.assertEqual(node.local_files("tmp_check/"), [])
            proj_rows = node.sqlite_query("SELECT id FROM projects WHERE id = ?", (project_id,))
            self.assertEqual(len(proj_rows), 1)
            self.assertEqual(proj_rows[0]["id"], project_id)

            # Exercise probe_capabilities, run_cli, and expect_fail startup refusal
            caps = cluster.node_mgr.probe_capabilities()
            self.assertTrue(caps.has_flag("--role"))
            ver_proc = cluster.node_mgr.run_cli(["--version"])
            self.assertEqual(ver_proc.returncode, 0)
            self.assertIn("pulse-node", ver_proc.stdout)

            fail_rc, _, fail_err = cluster.node_mgr.start_node(
                extra_args=["--invalid-e2e-flag"],
                expect_fail=True,
            )
            self.assertNotEqual(fail_rc, 0)
            self.assertIn("unknown argument", fail_err.lower())
