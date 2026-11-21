"""Comprehensive offline unit tests for the Pulse E2E opaque-box harness."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
import unittest
from unittest import mock
from compression import zstd
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

_HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(_HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(_HARNESS_ROOT))

from lib import fixtures, plst  # noqa: E402
from lib.auth import AuthBootstrap  # noqa: E402
from lib.client import PulseClient, encode_otlp_trace_protobuf  # noqa: E402
from lib.mocks import (  # noqa: E402
    MockLlmServer,
    MockWebhookServer,
    compute_webhook_signature,
    verify_webhook_signature,
)
from lib.node_mgr import NodeInstance, NodeManager, list_agent_artifacts, sqlite_tables  # noqa: E402
from lib.registry import (  # noqa: E402
    TestRegistry,
    ThresholdError,
    VALID_FEATURES,
    discover_tests,
    test_case,
    verify_thresholds,
)


def _decode_pb_fields(data: bytes) -> list[tuple[int, int, Any]]:
    """Minimal pure-Python protobuf wire parser for verifying OTLP/Protobuf payloads."""
    pos = 0
    end = len(data)
    fields: list[tuple[int, int, Any]] = []

    def read_varint() -> int:
        nonlocal pos
        shift = 0
        val = 0
        while pos < end:
            b = data[pos]
            pos += 1
            val |= (b & 0x7F) << shift
            if not (b & 0x80):
                return val
            shift += 7
        raise ValueError("Truncated varint")

    while pos < end:
        key = read_varint()
        field_num = key >> 3
        wire_type = key & 0x07
        if wire_type == 0:
            fields.append((field_num, wire_type, read_varint()))
        elif wire_type == 1:
            if pos + 8 > end:
                raise ValueError("Truncated 64-bit wire field")
            fields.append((field_num, wire_type, data[pos : pos + 8]))
            pos += 8
        elif wire_type == 2:
            length = read_varint()
            if pos + length > end:
                raise ValueError("Truncated length-delimited field")
            fields.append((field_num, wire_type, data[pos : pos + length]))
            pos += length
        else:
            raise ValueError(f"Unsupported wire type in test parser: {wire_type}")
    return fields


class TestPlstWireAndXxh3(unittest.TestCase):
    """Tests pure-Python xxh3_64, PLST v1 framing, blob dedup, and hostile frames."""

    def test_xxh3_64_matches_bucketing_vectors_json(self) -> None:
        vectors_path = _HARNESS_ROOT.parent.parent / "sdks" / "bucketing_vectors.json"
        vectors = json.loads(vectors_path.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(vectors), 12)
        for vec in vectors:
            key = f"{vec['flag_key']}:{vec['distinct_id']}"
            actual = plst.xxh3_64_str(key)
            self.assertEqual(f"{actual:016x}", vec["xxh3_rollout_hex"], msg=key)
            self.assertEqual(actual % 10000, vec["rollout_slot"], msg=key)

    def test_xxh3_64_boundary_lengths(self) -> None:
        expected = [
            (b"", 0x2D06800538D394C2),
            (b"a", 0xE6C632B61E964E1F),
            (b"abc", 0x78AF5F94892F3950),
            (b"abcd", 0x6497A96F53A89890),
            (b"abcdefgh", 0x6F45A76842A96483),
            (b"abcdefghi", 0xE0DDE4FC174590A0),
            (b"0123456789abcdef", 0x64439946D8FA212D),
            (b"0123456789abcdefg", 0xD2E63ED5466F5C32),
            (b"A" * 64, 0xD61189C19E55A44E),
            (b"B" * 128, 0xD65A651FB2D3AFCE),
            (b"C" * 129, 0xF7094A751DEC3AF9),
            (b"D" * 240, 0xBA48DB51A9A60541),
            (b"E" * 241, 0x1CF3FC7014D15CC4),
            (b"F" * 1024, 0x41D205D161F0D4EC),
            (b"G" * 1025, 0x10E83CE91168F8E5),
            (bytes((i * 37) & 0xFF for i in range(2500)), 0x50C27428FE890C9C),
        ]
        for payload, want in expected:
            self.assertEqual(plst.xxh3_64(payload), want, msg=f"len={len(payload)}")

    def test_plst_roundtrip_raw_dict_and_uncompressed(self) -> None:
        batch = fixtures.make_hierarchy_batch(seed="wire_rt")
        raw_frame = plst.encode_plst_frame(batch, use_dict=False)
        hdr = plst.inspect_plst_header(raw_frame)
        self.assertEqual(hdr["magic"], b"PLST")
        self.assertEqual(hdr["version"], 1)
        self.assertEqual(hdr["flags"], plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL)
        self.assertEqual(hdr["dict_id"], 0)
        self.assertEqual(hdr["comp_len"], len(raw_frame) - 20)

        decoded_raw = plst.decode_plst_frame(raw_frame)
        self.assertEqual(len(decoded_raw["traces"]), len(batch["traces"]))
        self.assertEqual(len(decoded_raw["spans"]), len(batch["spans"]))
        self.assertEqual(decoded_raw["traces"][1]["parent_trace_id"], batch["traces"][0]["trace_id"])
        guardrail = next(s for s in decoded_raw["spans"] if s["span_type"] == "guardrail")
        self.assertEqual(guardrail["span_phase"], "final")
        self.assertTrue(len(guardrail["linked_span_ids"]) >= 1)
        self.assertEqual(len(guardrail["events"]), 1)

        # Default DICT_ZSTD round-trip (raw prefix dictionary)
        dict_frame = plst.encode_plst_frame(batch, use_dict=True)
        d_hdr = plst.inspect_plst_header(dict_frame)
        self.assertEqual(d_hdr["flags"], plst.FLAG_DICT_ZSTD | plst.FLAG_FINAL)
        self.assertEqual(d_hdr["dict_id"], plst.AGENT_DICT_ID)
        decoded_dict = plst.decode_plst_frame(dict_frame)
        self.assertEqual(decoded_dict["traces"][0]["trace_id"], batch["traces"][0]["trace_id"])

        # Formatted ZstdDict via PULSE_AGENT_DICT_PATH
        samples = [json.dumps(batch).encode("utf-8") for _ in range(12)]
        trained = zstd.train_dict(samples, 4096).dict_content
        self.assertTrue(trained.startswith(b"\x37\xa4\x30\xec"))
        with tempfile.TemporaryDirectory() as td:
            dict_path = Path(td) / "agent.zdict"
            dict_path.write_bytes(trained)
            prev_env = os.environ.get("PULSE_AGENT_DICT_PATH")
            os.environ["PULSE_AGENT_DICT_PATH"] = str(dict_path)
            try:
                env_frame = plst.encode_plst_frame(batch, use_dict=True)
                decoded_env = plst.decode_plst_frame(env_frame)
                self.assertEqual(len(decoded_env["spans"]), len(batch["spans"]))
            finally:
                if prev_env is None:
                    os.environ.pop("PULSE_AGENT_DICT_PATH", None)
                else:
                    os.environ["PULSE_AGENT_DICT_PATH"] = prev_env

        # Uncompressed with FLAG_PARTIAL
        uncomp_frame = plst.encode_plst_frame(batch, flags=plst.FLAG_PARTIAL)
        u_hdr = plst.inspect_plst_header(uncomp_frame)
        self.assertEqual(u_hdr["uncomp_len"], u_hdr["comp_len"])
        decoded_uncomp = plst.decode_plst_frame(uncomp_frame)
        self.assertEqual(len(decoded_uncomp["traces"]), 2)

    def test_blob_dedup_and_collision_safety(self) -> None:
        batch = fixtures.make_repeated_prompt_batch(seed="dedup_self", repeats=6, prompt_bytes=1024)
        stats = plst.BlobDedupStats()
        frame = plst.encode_plst_frame(batch, stats_out=stats)
        self.assertGreater(stats.deduped_blobs, 0)
        self.assertLess(stats.unique_blobs, stats.total_blobs)
        decoded = plst.decode_plst_frame(frame)
        self.assertEqual(len(decoded["traces"]), 6)
        self.assertEqual(decoded["traces"][0]["input"], decoded["traces"][5]["input"])

        # Simulate hash collision: distinct blobs with identical hash MUST NOT be merged
        collide_batch = {
            "traces": [
                fixtures.make_trace(
                    "tr_col_1",
                    input_payload={"msg": "alpha"},
                    output_payload={"msg": "beta"},
                )
            ],
            "spans": [],
        }
        col_stats = plst.BlobDedupStats()
        raw_sec, col_stats = plst._encode_sections_with_stats(
            collide_batch,
            blob_hash_fn=lambda _b: 0xDEADBEEFCAFEBABE,
        )
        self.assertEqual(col_stats.deduped_blobs, 0)
        self.assertGreaterEqual(col_stats.collision_checks, 1)
        self.assertGreaterEqual(col_stats.unique_blobs, 2)
        # Note: _decode_sections validates real xxh3_64 hashes, so also verify regular encoding
        # preserves distinct payloads when real hashes differ.
        self.assertNotEqual(raw_sec, b"")

    def test_all_hostile_frames_rejected(self) -> None:
        hostile_kinds = [
            "bad_magic",
            "bad_version",
            "bad_flags",
            "oversized_uncomp_len",
            "crc_mismatch",
            "comp_len_mismatch",
            "truncated_header",
            "truncated_body",
            "unknown_dict_id",
            "zstd_bomb",
            "truncated_sections",
            "oob_string_idx",
            "oob_blob_idx",
            "invalid_utf8",
        ]
        for kind in hostile_kinds:
            frame = plst.build_hostile_frame(kind)
            with self.subTest(kind=kind):
                with self.assertRaises(plst.PlstError):
                    plst.decode_plst_frame(frame)

    def test_default_agent_dict_matches_rust_protocol(self) -> None:
        self.assertEqual(len(plst.DEFAULT_AGENT_DICT_BYTES), 1014)
        self.assertEqual(
            hashlib.sha256(plst.DEFAULT_AGENT_DICT_BYTES).hexdigest(),
            "7d9a2067671e1e0d8396caedf72b8189b755ca1c6a5a1d47a833e66895ba1385",
        )

    def test_decode_plst_frame_small_uncomp_len_zstd_bomb_raises_limit_error(self) -> None:
        comp = zstd.compress(b"A" * (2 * 1024 * 1024), level=1)
        crc = __import__("zlib").crc32(comp) & 0xFFFFFFFF
        frame = plst.HEADER_STRUCT.pack(
            plst.PLST_MAGIC,
            plst.PLST_VERSION,
            plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL,
            0,
            16,
            len(comp),
            crc,
        ) + comp
        tracemalloc.start()
        try:
            with self.assertRaises(plst.PlstLimitError):
                plst.decode_plst_frame(frame)
            _cur, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 256 * 1024)

    def test_missing_pulse_agent_dict_path_raises_plst_dict_error(self) -> None:
        batch = fixtures.make_batch(seed="missing_dict")
        with tempfile.TemporaryDirectory() as td:
            missing_path = str(Path(td) / "missing.zdict")
            with mock.patch.dict(os.environ, {"PULSE_AGENT_DICT_PATH": missing_path}):
                with self.assertRaises(plst.PlstDictError):
                    plst.load_agent_zstd_dict()
                with self.assertRaises(plst.PlstDictError):
                    plst.encode_plst_frame(batch, use_dict=True)

    def test_none_cost_usd_fields_handled_cleanly(self) -> None:
        tr_zero = fixtures.make_trace("tr_none_cost")
        tr_zero["total_cost_usd"] = None
        tr_zero["total_cost_nanos"] = None
        sp_zero = fixtures.make_span("sp_none_cost", "tr_none_cost")
        sp_zero["cost_usd"] = None
        sp_zero["cost_nanos"] = None

        tr_nanos = fixtures.make_trace("tr_nanos_only")
        tr_nanos["total_cost_usd"] = None
        tr_nanos["total_cost_nanos"] = 123_456
        sp_nanos = fixtures.make_span("sp_nanos_only", "tr_nanos_only")
        sp_nanos["cost_usd"] = None
        sp_nanos["cost_nanos"] = 789_000

        frame = plst.encode_plst_frame({"traces": [tr_zero, tr_nanos], "spans": [sp_zero, sp_nanos]})
        decoded = plst.decode_plst_frame(frame)
        self.assertEqual(decoded["traces"][0]["total_cost_nanos"], 0)
        self.assertEqual(decoded["traces"][0]["total_cost_usd"], 0.0)
        self.assertEqual(decoded["spans"][0]["cost_nanos"], 0)
        self.assertEqual(decoded["spans"][0]["cost_usd"], 0.0)
        self.assertEqual(decoded["traces"][1]["total_cost_nanos"], 123_456)
        self.assertEqual(decoded["spans"][1]["cost_nanos"], 789_000)

    def test_encode_plst_frame_respects_monkeypatched_xxh3_64(self) -> None:
        collide_batch = {
            "traces": [
                fixtures.make_trace(
                    "tr_patch_hash",
                    input_payload={"msg": "alpha"},
                    output_payload={"msg": "beta"},
                )
            ],
            "spans": [],
        }
        stats = plst.BlobDedupStats()
        with mock.patch.object(plst, "xxh3_64", return_value=0xDEADBEEFCAFEBABE) as mock_hash:
            frame = plst.encode_plst_frame(collide_batch, stats_out=stats)
            self.assertGreaterEqual(mock_hash.call_count, 2)
            self.assertGreaterEqual(stats.collision_checks, 1)
            self.assertEqual(stats.deduped_blobs, 0)
            self.assertGreaterEqual(stats.unique_blobs, 2)
            decoded = plst.decode_plst_frame(frame)
            self.assertEqual(decoded["traces"][0]["input"], {"msg": "alpha"})
            self.assertEqual(decoded["traces"][0]["output"], {"msg": "beta"})
        with self.assertRaises(plst.PlstCorruptError):
            plst.decode_plst_frame(frame)

    def test_rust_parity_eval_empty_blobs_orphan_spans_flags_and_amplification(self) -> None:
        # 1. avg_eval_score / eval_pass_rate + empty "" / {} / [] vs None + orphan span + lifecycle flags
        tr = fixtures.make_trace(
            "tr_parity_1",
            status="ok",
            input_payload="",
            output_payload={},
        )
        tr["metadata"] = []
        tr["avg_eval_score"] = 0.875
        tr["eval_pass_rate"] = 0.5
        sp_attached = fixtures.make_span(
            "sp_attached",
            "tr_parity_1",
            span_phase="final",
            status="ok",
            input_payload=[],
            output_payload="",
            attributes={},
            events=[{"name": "ev_empty", "timestamp_ms": 1_730_000_000_005, "attributes": {}}],
        )
        sp_attached["metadata"] = None
        sp_orphan = fixtures.make_span(
            "sp_orphan",
            "tr_streaming_only",
            span_phase="start",
            status="running",
        )
        mixed_frame = plst.encode_plst_frame(
            {"traces": [tr], "spans": [sp_attached, sp_orphan]},
            use_dict=True,
        )
        hdr = plst.inspect_plst_header(mixed_frame)
        self.assertEqual(hdr["flags"], plst.FLAG_DICT_ZSTD | plst.FLAG_PARTIAL | plst.FLAG_FINAL)
        dec = plst.decode_plst_frame(mixed_frame)
        self.assertEqual(dec["traces"][0]["avg_eval_score"], 0.875)
        self.assertEqual(dec["traces"][0]["eval_pass_rate"], 0.5)
        self.assertEqual(dec["traces"][0]["input"], "")
        self.assertEqual(dec["traces"][0]["output"], {})
        self.assertEqual(dec["traces"][0]["metadata"], [])
        self.assertEqual(dec["spans"][0]["input"], [])
        self.assertEqual(dec["spans"][0]["output"], "")
        self.assertEqual(dec["spans"][0]["attributes"], {})
        self.assertIsNone(dec["spans"][0]["metadata"])
        self.assertEqual(dec["spans"][0]["events"][0]["attributes"], {})
        self.assertEqual(dec["spans"][1]["trace_id"], "tr_streaming_only")

        # Partial-only flags
        tr_run = fixtures.make_trace("tr_run_only", status="running")
        sp_run = fixtures.make_span("sp_run_only", "tr_run_only", span_phase="update", status="running")
        partial_frame = plst.encode_plst_frame({"traces": [tr_run], "spans": [sp_run]}, use_dict=False)
        self.assertEqual(
            plst.inspect_plst_header(partial_frame)["flags"],
            plst.FLAG_RAW_ZSTD | plst.FLAG_PARTIAL,
        )

        # Empty batch defaults to FLAG_FINAL
        empty_frame = plst.encode_plst_frame({"traces": [], "spans": []}, use_dict=True)
        self.assertEqual(
            plst.inspect_plst_header(empty_frame)["flags"],
            plst.FLAG_DICT_ZSTD | plst.FLAG_FINAL,
        )

        # 2. Materialized 64 MiB anti-amplification guard (blobs & strings)
        big_blob_str = "x" * (512 * 1024)
        amp_spans = [
            fixtures.make_span(f"sp_amp_{i}", "tr_amp", input_payload=big_blob_str, output_payload=None)
            for i in range(140)
        ]
        amp_blob_frame = plst.encode_plst_frame({"traces": [], "spans": amp_spans}, use_dict=True)
        with self.assertRaises(plst.PlstLimitError):
            plst.decode_plst_frame(amp_blob_frame)

        big_err_str = "y" * (512 * 1024)
        amp_traces = [
            fixtures.make_trace(f"tr_amp_{i}", error_message=big_err_str, input_payload=None, output_payload=None)
            for i in range(140)
        ]
        amp_str_frame = plst.encode_plst_frame({"traces": amp_traces, "spans": []}, use_dict=True)
        with self.assertRaises(plst.PlstLimitError):
            plst.decode_plst_frame(amp_str_frame)


class TestFixturesAndOtlpProtobuf(unittest.TestCase):
    """Tests deterministic fixtures, corner builders, and OTLP/Protobuf wire encoding."""

    def test_hierarchy_and_corner_fixtures(self) -> None:
        hier = fixtures.make_hierarchy_batch(seed="h1")
        span_types = {sp["span_type"] for sp in hier["spans"]}
        self.assertEqual(span_types, set(fixtures.ALL_SPAN_TYPES))

        pii = fixtures.make_pii_batch(seed="p1")
        self.assertIn("alice.smith@example.com", json.dumps(pii))
        self.assertIn("sk-proj-", json.dumps(pii))

        oversized = fixtures.make_oversized_payload_batch(seed="o1")
        self.assertGreater(len(oversized["traces"][0]["input"]["prompt"]), 64 * 1024)

        cyclic = fixtures.make_cyclic_spans_batch(seed="c1")
        self.assertEqual(cyclic["spans"][0]["parent_span_id"], cyclic["spans"][1]["span_id"])
        self.assertEqual(cyclic["spans"][1]["parent_span_id"], cyclic["spans"][0]["span_id"])

        deep = fixtures.make_deep_chain_batch(seed="d1", depth_count=36)
        self.assertEqual(len(deep["spans"]), 36)

        many = fixtures.make_many_spans_batch(seed="m1", span_count=520)
        self.assertEqual(len(many["spans"]), 520)

        loop_batch = fixtures.make_tool_retry_loop_batch(seed="l1", retry_count=5)
        self.assertTrue(loop_batch["traces"][0]["has_loop_warning"])

        stream_ooo = fixtures.make_streaming_phase_batch(seed="s1", reverse_order=True)
        self.assertEqual([s["span_phase"] for s in stream_ooo["spans"]], ["final", "update", "start"])

    def test_control_plane_builders_cover_all_enums(self) -> None:
        for kind in fixtures.ALL_EVAL_KINDS:
            ev = fixtures.make_evaluator_config(name=f"ev_{kind}", kind=kind)
            self.assertEqual(ev["kind"], kind)
        for target_type in fixtures.ALL_EVAL_TARGET_TYPES:
            mev = fixtures.make_manual_evaluation(target_type=target_type)
            self.assertEqual(mev["target_type"], target_type)
        for metric in fixtures.ALL_ALERT_METRICS:
            rule = fixtures.make_alert_rule(metric=metric)
            self.assertEqual(rule["metric"], metric)

        pol = fixtures.make_privacy_policy()
        self.assertTrue(pol["redact_pii"])
        pr = fixtures.make_pricing_rule()
        self.assertGreater(pr["output_per_1m_usd"], 0.0)
        ds = fixtures.make_dataset()
        item = fixtures.make_dataset_item()
        exp = fixtures.make_experiment(dataset_id="ds_1")
        run = fixtures.make_experiment_run(dataset_item_id="item_1")
        self.assertEqual(ds["name"], "e2e-golden-dataset")
        self.assertIn("input", item)
        self.assertEqual(item["expected_behavior"], {"expected_tools": ["order_lookup"]})
        llm_ev = fixtures.make_evaluator_config(name="ev_llm", kind="llm_judge")
        self.assertIn("rubric", llm_ev["config"])
        self.assertNotIn("prompt", llm_ev["config"])
        self.assertEqual(exp["dataset_id"], "ds_1")
        self.assertTrue(run["passed"])

    def test_otlp_protobuf_wire_tags(self) -> None:
        req = fixtures.make_otlp_export_request(seed="pb_1", include_error_span=True)
        req["resourceSpans"][0]["scopeSpans"][0]["spans"][1]["traceState"] = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"
        pb = encode_otlp_trace_protobuf(req)
        top_fields = _decode_pb_fields(pb)
        self.assertEqual(len(top_fields), 1)
        self.assertEqual(top_fields[0][0], 1)  # resource_spans = 1
        self.assertEqual(top_fields[0][1], 2)  # length-delimited

        rs_fields = _decode_pb_fields(top_fields[0][2])
        scope_spans_bytes = next(val for fnum, wtype, val in rs_fields if fnum == 2 and wtype == 2)
        ss_fields = _decode_pb_fields(scope_spans_bytes)
        span_msgs = [val for fnum, wtype, val in ss_fields if fnum == 2 and wtype == 2]
        self.assertEqual(len(span_msgs), 3)

        root_by_tag = {fnum: (wtype, val) for fnum, wtype, val in _decode_pb_fields(span_msgs[0])}
        self.assertEqual((root_by_tag[2][0], len(root_by_tag[2][1])), (2, 8))  # span_id at tag 2
        self.assertNotIn(3, root_by_tag)  # omitted traceState -> no tag 3

        llm_span_fields = _decode_pb_fields(span_msgs[1])
        by_tag = {fnum: (wtype, val) for fnum, wtype, val in llm_span_fields}
        self.assertEqual(len(by_tag[1][1]), 16)  # trace_id = 16 bytes
        self.assertEqual((by_tag[2][0], len(by_tag[2][1])), (2, 8))  # span_id = 8 bytes at tag 2
        self.assertEqual(
            (by_tag[3][0], by_tag[3][1].decode("utf-8")),
            (2, "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"),
        )  # trace_state at tag 3
        self.assertEqual(len(by_tag[4][1]), 8)   # parent_span_id = 8 bytes
        self.assertEqual(by_tag[7][0], 1)        # start_time_unix_nano fixed64
        self.assertEqual(by_tag[8][0], 1)        # end_time_unix_nano fixed64
        status_fields = dict((fnum, val) for fnum, _, val in _decode_pb_fields(by_tag[15][1]))
        self.assertEqual(status_fields[3], 2)    # StatusCode ERROR = 2


class TestRegistryAndThresholds(unittest.TestCase):
    """Tests TestRegistry, deterministic discovery, duplicate checks, and thresholds."""

    def test_registry_validation_and_sorting(self) -> None:
        reg = TestRegistry()

        @test_case("T2_B", 2, "F02", "Second", tags=("b",), registry=reg)
        def _fn_b(_ctx: Any) -> None:
            pass

        @test_case("T1_A", 1, "F01", "First", tags=("a",), registry=reg)
        def _fn_a(_ctx: Any) -> None:
            pass

        ordered = reg.all_cases()
        self.assertEqual([c.id for c in ordered], ["T1_A", "T2_B"])

        with self.assertRaises(ValueError):
            @test_case("T1_A", 1, "F01", "Dup", registry=reg)
            def _fn_dup(_ctx: Any) -> None:
                pass

        with self.assertRaises(ValueError):
            @test_case("T_BOOL", True, "F01", "Bool tier", registry=reg)  # type: ignore[arg-type]
            def _fn_bool(_ctx: Any) -> None:
                pass

        with self.assertRaises(ValueError):
            @test_case("T_FEAT", 1, "F99", "Bad feature", registry=reg)
            def _fn_feat(_ctx: Any) -> None:
                pass

    def test_discover_tests_idempotent_and_filtered(self) -> None:
        reg = TestRegistry()
        with tempfile.TemporaryDirectory() as td:
            tdir = Path(td)
            (tdir / "tier1_sample.py").write_text(
                "from lib.registry import test_case\n"
                "@test_case('E2E-T1-002', 1, 'F02', 'Second T1', tags=('wire',))\n"
                "def t2(ctx): pass\n"
                "@test_case('E2E-T1-001', 1, 'F01', 'First T1', tags=('hierarchy',))\n"
                "def t1(ctx): pass\n",
                encoding="utf-8",
            )
            (tdir / "tier3_sample.py").write_text(
                "from lib.registry import test_case\n"
                "@test_case('E2E-T3-001', 3, 'F05', 'Pairwise test', tags=('pairwise',))\n"
                "def t3(ctx): pass\n",
                encoding="utf-8",
            )
            first = discover_tests(tests_dir=tdir, registry=reg)
            second = discover_tests(tests_dir=tdir, registry=reg)
            self.assertEqual([c.id for c in first], ["E2E-T1-001", "E2E-T1-002", "E2E-T3-001"])
            self.assertEqual([c.id for c in second], ["E2E-T1-001", "E2E-T1-002", "E2E-T3-001"])

            only_t3 = discover_tests(tiers=[3], tests_dir=tdir, registry=reg)
            self.assertEqual([c.id for c in only_t3], ["E2E-T3-001"])

            by_tag = discover_tests(filter_expr="wire", tests_dir=tdir, registry=reg)
            self.assertEqual([c.id for c in by_tag], ["E2E-T1-002"])

    def test_verify_thresholds_pass_and_deficit_failures(self) -> None:
        reg = TestRegistry()
        for tier in (1, 2):
            for feat in VALID_FEATURES:
                for i in range(5):
                    test_case(
                        f"T{tier}_{feat}_{i}",
                        tier,
                        feat,
                        f"Case {tier} {feat} {i}",
                        registry=reg,
                    )(lambda _ctx: None)
        for i in range(24):
            test_case(f"T3_{i:02d}", 3, VALID_FEATURES[i % 24], f"T3 {i}", registry=reg)(lambda _ctx: None)
        for i in range(12):
            test_case(f"T4_{i:02d}", 4, VALID_FEATURES[i % 24], f"T4 {i}", registry=reg)(lambda _ctx: None)

        cases = reg.all_cases()
        summary = verify_thresholds(cases)
        self.assertEqual(summary["total"], 276)

        # Dropping 1 case from Tier 1 F24 must fail ThresholdError
        deficient = [c for c in cases if c.id != "T1_F24_4"]
        with self.assertRaises(ThresholdError) as ctx:
            verify_thresholds(deficient)
        self.assertTrue(any("F24=4" in v for v in ctx.exception.violations))


class TestMockServers(unittest.TestCase):
    """Tests loopback MockLlmServer and MockWebhookServer HMAC verification."""

    def test_mock_llm_server_modes(self) -> None:
        with MockLlmServer() as llm:
            client = PulseClient(f"http://127.0.0.1:{llm.port}")
            resp = client.request(
                "POST",
                "/v1/chat/completions",
                json_body={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
            )
            self.assertEqual(resp.status, 200)
            self.assertIn("Mock LLM judge pass", resp.json()["choices"][0]["message"]["content"])

            llm.status_code = 502
            llm.raw_body_override = b"{not-json"
            bad = client.request("POST", "/v1/chat/completions", json_body={"model": "gpt-4o-mini"})
            self.assertEqual(bad.status, 502)
            self.assertEqual(bad.body, b"{not-json")
            self.assertEqual(len(llm.wait_for_count(2, timeout=2.0)), 2)

    def test_mock_webhook_hmac_verification(self) -> None:
        secret = "e2e-webhook-secret-16"
        with MockWebhookServer(signing_secret=secret) as hook:
            client = PulseClient(f"http://127.0.0.1:{hook.port}")
            body = b'{"event":"alert.triggered"}'
            ts = str(int(time.time()))
            sig = compute_webhook_signature(secret, ts, body)
            resp = client.request(
                "POST",
                "/hook",
                headers={
                    "Content-Type": "application/json",
                    "X-Pulse-Timestamp": ts,
                    "X-Pulse-Signature-256": sig,
                },
                body=body,
            )
            self.assertEqual(resp.status, 200)

            bad_resp = client.request(
                "POST",
                "/hook",
                headers={
                    "Content-Type": "application/json",
                    "X-Pulse-Timestamp": ts,
                    "X-Pulse-Signature-256": "sha256=" + ("0" * 64),
                },
                body=body,
            )
            self.assertEqual(bad_resp.status, 200)
            reqs = hook.wait_for_count(2, timeout=2.0)
            self.assertTrue(reqs[0]["signature_valid"])
            self.assertFalse(reqs[1]["signature_valid"])

        self.assertFalse(verify_webhook_signature(secret, "1000", body, sig, max_skew_secs=60, now_secs=5000))
        upper_ws = f"  SHA256={sig.split('=', 1)[1].upper()}  "
        self.assertTrue(verify_webhook_signature(secret, f" {ts} ", body, upper_ws, now_secs=int(ts)))
        self.assertFalse(verify_webhook_signature(secret, ts, body, "  MD5=" + ("0" * 64), now_secs=int(ts)))


class TestNodeManagerAndClientOffline(unittest.TestCase):
    """Tests NodeManager helpers, SQLite/ObjectStore inspection, PulseClient, and AuthBootstrap."""

    def test_node_manager_cli_env_sqlite_and_artifacts(self) -> None:
        mgr = NodeManager()
        port = mgr.free_port()
        self.assertGreater(port, 0)

        with tempfile.TemporaryDirectory() as td:
            data_dir = Path(td)
            args = mgr.build_cli_args(role="ingest", port=port, data_dir=data_dir, seed=True)
            self.assertIn("--role", args)
            self.assertIn("ingest", args)
            self.assertIn("--no-quic", args)
            self.assertIn("--seed", args)

            env = mgr.build_env(admin_token="sk_root_test", drain_ms=250)
            self.assertEqual(env["PULSE_ALLOW_PRIVATE_ENDPOINTS"], "1")
            self.assertEqual(env["PULSE_SHUTDOWN_DRAIN_MS"], "250")
            self.assertEqual(env["PULSE_ADMIN_TOKEN"], "sk_root_test")

            # Create sample SQLite DB + ObjectStore files + log + root_admin_token
            db_path = data_dir / "pulse_meta.db"
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE agent_evaluator_configs (id TEXT PRIMARY KEY)")
            conn.execute("CREATE TABLE agent_eval_rollups (target_id TEXT PRIMARY KEY)")
            conn.commit()
            conn.close()

            (data_dir / "root_admin_token").write_text("sk_root_disk_123\n", encoding="utf-8")
            log_path = data_dir / "node.log"
            log_path.write_text("Generated the public ingest key for client SDKs: pk_live_boot123\n", encoding="utf-8")
            wal_file = data_dir / "object_store" / "agent_wal" / "proj_1" / "w1" / "seq_000000000001.ptwl"
            seg_file = data_dir / "object_store" / "agent_segments" / "proj_1" / "2026-11-19" / "shard_00" / "seg1.plss"
            man_file = data_dir / "object_store" / "agent_manifests" / "proj_1" / "epoch_1.json"
            for p in (wal_file, seg_file, man_file):
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b"stub")

            self.assertEqual(
                sqlite_tables(data_dir),
                ["agent_eval_rollups", "agent_evaluator_configs"],
            )
            artifacts = list_agent_artifacts(data_dir)
            self.assertEqual(len(artifacts["agent_wal"]), 1)
            self.assertEqual(len(artifacts["agent_segments"]), 1)
            self.assertEqual(len(artifacts["agent_manifests"]), 1)

            inst = NodeInstance(
                role="all",
                host="127.0.0.1",
                port=port,
                base_url=f"http://127.0.0.1:{port}",
                data_dir=data_dir,
                web_dir=None,
                admin_token=None,
                proc=None,
                log_path=log_path,
            )
            self.assertEqual(inst.root_admin_token(), "sk_root_disk_123")
            self.assertEqual(inst.extract_boot_ingest_key(), "pk_live_boot123")
            self.assertEqual(len(inst.sqlite_tables()), 2)

    def test_client_and_auth_bootstrap_against_local_stub(self) -> None:
        recorded: list[tuple[str, str, dict[str, str], bytes]] = []
        with tempfile.TemporaryDirectory() as td:
            data_dir = Path(td)
            proj_counter = 0

            class _StubHandler(BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def log_message(self, format: str, *args: Any) -> None:
                    return

                def _send_json(self, code: int, obj: Any) -> None:
                    raw = json.dumps(obj).encode("utf-8")
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

                def _send_bytes(self, code: int, content_type: str, raw: bytes) -> None:
                    self.send_response(code)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

                def do_GET(self) -> None:
                    if self.path == "/":
                        self._send_bytes(
                            200,
                            "text/html",
                            b'<html><body><div id="root"></div><script src="/assets/index-abc.js"></script></body></html>',
                        )
                    elif self.path == "/assets/index-abc.js":
                        self._send_bytes(200, "application/javascript", b'const r="#/agents";')
                    elif self.path == "/healthz":
                        self._send_bytes(200, "text/plain", b"ok")
                    elif self.path == "/readyz":
                        self._send_bytes(200, "text/plain", b"ready")
                    else:
                        self._send_json(404, {"error": "not_found"})

                def do_POST(self) -> None:
                    nonlocal proj_counter
                    length = int(self.headers.get("Content-Length", "0") or "0")
                    body = self.rfile.read(length) if length > 0 else b""
                    ct_count = sum(1 for k, _ in self.headers.items() if k.lower() == "content-type")
                    hdrs = {k.lower(): v for k, v in self.headers.items()}
                    hdrs["_content_type_count"] = str(ct_count)
                    recorded.append(("POST", self.path, hdrs, body))

                    if self.path == "/v1/auth/signup":
                        payload = json.loads(body)
                        if "setup_code" in payload:
                            self._send_json(200, {"token": "ps_owner_tok", "user": {"role": "owner"}})
                        else:
                            email = payload["email"]
                            outbox = data_dir / "outbox"
                            with outbox.open("a", encoding="utf-8") as fh:
                                fh.write(f"2026-11-19T00:00:00Z verify {email} http://127.0.0.1/#/verify/verif_tok_64\n")
                            self._send_json(200, {"verification_required": True, "email": email})
                        return
                    if self.path == "/v1/auth/verify-email":
                        self._send_json(200, {"token": "ps_other_org_tok"})
                        return
                    if self.path == "/v1/org/invites":
                        role = json.loads(body)["role"]
                        self._send_json(200, {"invite_url": f"http://127.0.0.1/#/invite/inv_{role}"})
                        return
                    if self.path == "/v1/auth/invites/accept":
                        tok = json.loads(body)["token"]
                        role = tok.replace("inv_", "")
                        self._send_json(200, {"token": f"ps_{role}_tok"})
                        return
                    if self.path == "/v1/projects":
                        proj_counter += 1
                        pid = f"proj_{proj_counter}"
                        self._send_json(
                            200,
                            {
                                "project": {"id": pid, "name": json.loads(body)["name"]},
                                "api_key": {"plaintext_key": f"pk_live_{pid}"},
                            },
                        )
                        return
                    if self.path.startswith("/v1/projects/") and self.path.endswith("/api-keys"):
                        pid = self.path.split("/")[3]
                        self._send_json(200, {"plaintext_key": f"sk_live_{pid}"})
                        return
                    if self.path == "/v1/agent/traces":
                        if hdrs.get("content-type") == "application/x-pulse-trace":
                            decoded = plst.decode_plst_frame(body)
                            self._send_json(200, {"accepted_traces": len(decoded["traces"])})
                        else:
                            self._send_json(200, {"accepted_traces": 1})
                        return
                    if self.path == "/v1/otel/v1/traces":
                        self._send_json(200, {"partialSuccess": {}})
                        return
                    if self.path == "/v1/mcp":
                        self._send_json(200, {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})
                        return
                    self._send_json(400, {"error": "bad_request"})

            srv = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
            th = threading.Thread(target=srv.serve_forever, daemon=True)
            th.start()
            try:
                client = PulseClient(f"http://127.0.0.1:{srv.server_address[1]}")
                ctx = AuthBootstrap.bootstrap_context(client, data_dir, "sk_root_stub", run_id="self1")
                self.assertEqual(ctx.owner_token, "ps_owner_tok")
                self.assertEqual(ctx.admin_token, "ps_admin_tok")
                self.assertEqual(ctx.member_token, "ps_member_tok")
                self.assertEqual(ctx.viewer_token, "ps_viewer_tok")
                self.assertEqual(ctx.other_org_token, "ps_other_org_tok")
                self.assertTrue(ctx.proj_a.pk_key.startswith("pk_live_"))
                self.assertTrue(ctx.proj_a.sk_key.startswith("sk_live_"))
                self.assertNotEqual(ctx.unique_id("t"), ctx.unique_id("t"))

                batch = fixtures.make_batch(seed="stub")
                self.assertEqual(client.ingest_plst_batch(batch, api_key=ctx.proj_a.pk_key).status, 200)
                plst_frame = plst.encode_plst_frame(batch)
                self.assertEqual(
                    client.ingest_plst(
                        plst_frame,
                        api_key=ctx.proj_a.pk_key,
                        headers={"content-type": "application/x-pulse-trace"},
                    ).status,
                    200,
                )
                self.assertEqual(recorded[-1][2]["_content_type_count"], "1")
                self.assertEqual(client.ingest_json(batch, api_key=ctx.proj_a.pk_key).status, 200)
                otlp = fixtures.make_otlp_export_request(seed="stub")
                self.assertEqual(client.ingest_otlp_json(otlp, api_key=ctx.proj_a.pk_key).status, 200)
                self.assertEqual(client.ingest_otlp_pb(otlp, api_key=ctx.proj_a.pk_key).status, 200)
                self.assertEqual(
                    client.ingest_otlp_pb(
                        otlp,
                        api_key=ctx.proj_a.pk_key,
                        headers={"content-type": "application/x-protobuf"},
                    ).status,
                    200,
                )
                self.assertEqual(recorded[-1][2]["_content_type_count"], "1")
                mcp_resp = client.mcp_tool_call("list_agent_traces", {}, token=ctx.viewer_token, project_id=ctx.proj_a.id)
                self.assertTrue(mcp_resp.json()["result"]["ok"])

                studio = client.check_studio_static(require_agents_route=True)
                self.assertTrue(studio["agents_route_detected"])

                # Non-raising 404 check
                self.assertEqual(client.request("GET", "/v1/does-not-exist").status, 404)
            finally:
                srv.shutdown()
                srv.server_close()
                th.join(timeout=5.0)


class TestRunnerCliOffline(unittest.TestCase):
    """Tests runner.py CLI subcommands offline via subprocess."""

    def test_dry_run_list_tests_and_enforce_thresholds(self) -> None:
        runner_path = str(_HARNESS_ROOT / "runner.py")
        with tempfile.TemporaryDirectory() as td:
            report_path = Path(td) / "dry_report.json"

            dry = subprocess.run(
                [sys.executable, runner_path, "--dry-run", "--tier", "all", "--json-report", str(report_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(dry.returncode, 0, msg=dry.stderr)
            self.assertIn("Offline harness contract checks: OK", dry.stdout)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["schema_version"], 1)
            self.assertEqual(report["mode"], "dry-run")

            listed = subprocess.run(
                [sys.executable, runner_path, "--list-tests"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(listed.returncode, 0, msg=listed.stderr)
            self.assertIn("Total:", listed.stdout)

            # Live suite satisfies >=276 coverage thresholds: --dry-run --enforce-thresholds exits 0
            enforced_report_path = Path(td) / "dry_enforced_report.json"
            enforced = subprocess.run(
                [
                    sys.executable,
                    runner_path,
                    "--dry-run",
                    "--enforce-thresholds",
                    "--json-report",
                    str(enforced_report_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(enforced.returncode, 0, msg=enforced.stderr)
            self.assertIn("Offline harness contract checks: OK", enforced.stdout)
            self.assertNotIn("THRESHOLD VIOLATION", enforced.stderr)
            enforced_report = json.loads(enforced_report_path.read_text(encoding="utf-8"))
            self.assertTrue(enforced_report["thresholds_enforced"])
            self.assertTrue(enforced_report["thresholds_ok"])
            self.assertEqual(enforced_report["threshold_violations"], [])
            self.assertGreaterEqual(enforced_report["counts_by_tier"]["1"], 120)
            self.assertGreaterEqual(enforced_report["counts_by_tier"]["2"], 120)
            self.assertGreaterEqual(enforced_report["counts_by_tier"]["3"], 24)
            self.assertGreaterEqual(enforced_report["counts_by_tier"]["4"], 12)
            self.assertGreaterEqual(enforced_report["summary"]["total_selected"], 276)
            self.assertEqual(
                enforced_report["summary"]["total_selected"],
                sum(enforced_report["counts_by_tier"].values()),
            )

            listed_enforced = subprocess.run(
                [sys.executable, runner_path, "--list-tests", "--enforce-thresholds"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(listed_enforced.returncode, 0, msg=listed_enforced.stderr)
            self.assertIn("Total:", listed_enforced.stdout)
            self.assertEqual(listed_enforced.stderr, "")

    def test_enforce_thresholds_deficit_exits_2_and_writes_json_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            iso = Path(td) / "isolated_harness"
            iso.mkdir()
            shutil.copy2(_HARNESS_ROOT / "runner.py", iso / "runner.py")
            shutil.copytree(_HARNESS_ROOT / "lib", iso / "lib", ignore=shutil.ignore_patterns("__pycache__"))
            tests_dir = iso / "tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "tier1_partial.py").write_text(
                "from lib.registry import test_case\n\n"
                "@test_case('E2E-T1-PARTIAL-001', 1, 'F01', 'Partial stub', tags=('stub',))\n"
                "def _stub(_ctx):\n"
                "    pass\n",
                encoding="utf-8",
            )
            iso_runner = str(iso / "runner.py")

            dry_fail_report = Path(td) / "dry_fail_report.json"
            dry_enforced = subprocess.run(
                [
                    sys.executable,
                    iso_runner,
                    "--dry-run",
                    "--enforce-thresholds",
                    "--json-report",
                    str(dry_fail_report),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(dry_enforced.returncode, 2, msg=dry_enforced.stderr)
            self.assertIn("THRESHOLD VIOLATION", dry_enforced.stderr)
            self.assertIn("Tier 1: 1/120", dry_enforced.stdout)
            self.assertIn("Total: 1/276", dry_enforced.stdout)
            self.assertTrue(dry_fail_report.exists())
            dry_report = json.loads(dry_fail_report.read_text(encoding="utf-8"))
            self.assertEqual(dry_report["mode"], "dry-run")
            self.assertTrue(dry_report["thresholds_enforced"])
            self.assertFalse(dry_report["thresholds_ok"])
            self.assertEqual(dry_report["counts_by_tier"], {"1": 1, "2": 0, "3": 0, "4": 0})
            self.assertTrue(dry_report["threshold_violations"])

            # Live mode --enforce-thresholds --json-report exit 2 must write JSON report before spawning pulse-node
            live_fail_report = Path(td) / "live_fail_report.json"
            live_enforced = subprocess.run(
                [sys.executable, iso_runner, "--enforce-thresholds", "--json-report", str(live_fail_report)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(live_enforced.returncode, 2, msg=live_enforced.stderr)
            self.assertIn("THRESHOLD VIOLATION", live_enforced.stderr)
            self.assertTrue(live_fail_report.exists())
            live_report = json.loads(live_fail_report.read_text(encoding="utf-8"))
            self.assertEqual(live_report["mode"], "live")
            self.assertTrue(live_report["thresholds_enforced"])
            self.assertFalse(live_report["thresholds_ok"])
            self.assertEqual(live_report["summary"], {"total_selected": 1, "passed": 0, "failed": 0})
            self.assertEqual(live_report["results"], [])
            self.assertTrue(live_report["threshold_violations"])


if __name__ == "__main__":
    unittest.main()
