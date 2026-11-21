import hashlib
from pathlib import Path
import struct
import sys
import unittest
from unittest import mock
import zlib

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import _bootstrap  # noqa: F401
from pulse_agent import (
    AGENT_DICT_ID,
    AGENT_ZSTD_DICT,
    FLAG_DICT_ZSTD,
    FLAG_FINAL,
    FLAG_PARTIAL,
    FLAG_RAW_ZSTD,
    PLST_HEADER_LEN,
    AgentSpan,
    AgentTrace,
    FailureSource,
    ProtocolError,
    SpanEvent,
    SpanPhase,
    SpanType,
    TraceIngestBatch,
    TraceStatus,
    decode_plst_batch,
    encode_plst_batch,
    has_zstd,
)
from pulse_agent import protocol as proto_mod


def _sample_complex_batch() -> TraceIngestBatch:
    shared_payload = {"messages": [{"role": "user", "content": "Compare revenue vs cost"}], "temp": 0.2}
    tr1 = AgentTrace(
        trace_id="tr_1",
        project_id="proj_a",
        agent_name="planner-agent",
        agent_version="1.2.0",
        prompt_version="p-v3",
        model_config_version="mc-1",
        tool_config_version="tc-2",
        app_release="2026.11.20",
        environment="production",
        session_id="sess_1",
        user_id="user_42",
        workflow_id="wf_99",
        parent_trace_id=None,
        status=TraceStatus.OK,
        start_ts_ms=1_730_000_000_000,
        end_ts_ms=1_730_000_001_250,
        duration_ms=1250,
        input=shared_payload,
        output="Final answer: 123",
        failure_source=None,
        metadata={"region": "ap-south-1", "attempt": 1},
        tags=["prod", "rag", "finance"],
        span_count=3,
        llm_call_count=1,
        tool_call_count=1,
        error_span_count=0,
        input_tokens=150,
        output_tokens=60,
        cached_tokens=30,
        total_tokens=210,
        total_cost_nanos=420_000,
        total_cost_usd=0.00042,
        primary_model="gpt-4o-mini",
        primary_provider="openai",
        avg_eval_score=0.95,
        eval_pass_rate=1.0,
        has_loop_warning=False,
    )
    tr2 = AgentTrace(
        trace_id="tr_2",
        project_id="",
        agent_name="worker-agent",
        agent_version="unversioned",
        environment="staging",
        parent_trace_id="tr_1",
        status=TraceStatus.ERROR,
        start_ts_ms=1_730_000_000_100,
        end_ts_ms=None,
        duration_ms=0,
        input="123",
        output=123,
        error_type="ToolTimeout",
        error_message="timed out",
        failure_source=FailureSource.TOOL,
        tags=[],
        total_cost_nanos=0,
        total_cost_usd=0.001,  # tests usd_to_cost_nanos fallback
    )
    sp1 = AgentSpan(
        span_id="sp_ret",
        trace_id="tr_1",
        project_id="proj_a",
        span_phase=SpanPhase.FINAL,
        depth=0,
        name="rag.retrieve",
        span_type=SpanType.RETRIEVAL,
        status=TraceStatus.OK,
        start_ts_ms=1_730_000_000_050,
        end_ts_ms=1_730_000_000_250,
        duration_ms=200,
        input={"query": "revenue"},
        output={"docs": ["d1", "d2"]},
        events=[SpanEvent(name="docs.fetched", timestamp_ms=1_730_000_000_200, attributes={"count": 2})],
    )
    sp2 = AgentSpan(
        span_id="sp_llm",
        trace_id="tr_1",
        project_id="proj_a",
        parent_span_id="sp_ret",
        linked_span_ids=["sp_ret"],
        span_phase=SpanPhase.UPDATE,
        depth=1,
        name="openai.chat",
        span_type=SpanType.LLM,
        status=TraceStatus.RUNNING,
        start_ts_ms=1_730_000_000_300,
        end_ts_ms=None,
        duration_ms=400,
        provider="openai",
        model="gpt-4o-mini",
        model_id="gpt-4o-mini-2024-07-18",
        model_version="2024-07-18",
        prompt_template_id="tpl_1",
        prompt_template_version="v2",
        input_tokens=150,
        output_tokens=60,
        cached_tokens=30,
        total_tokens=210,
        cost_nanos=420_000,
        cost_usd=0.00042,
        temperature=0.25,
        finish_reason="stop",
        model_params={"top_p": 0.9},
        input=shared_payload,  # tests blob dedup with tr1.input
        output="Partial text",
        attributes={"stream": True},
        events=[
            SpanEvent(name="token.chunk", timestamp_ms=1_730_000_000_450, attributes={"len": 12}),
        ],
    )
    sp_orphan = AgentSpan(
        span_id="sp_guard",
        trace_id="tr_orphan_stream",
        project_id="proj_a",
        span_phase=SpanPhase.START,
        depth=0,
        name="guardrail.pii",
        span_type=SpanType.GUARDRAIL,
        status=TraceStatus.RUNNING,
        start_ts_ms=1_730_000_000_010,
        tool_name="pii_filter",
        tool_version="1.0",
        tool_retry_count=1,
        error_type=None,
        error_message=None,
        error_stack=None,
    )
    return TraceIngestBatch(traces=[tr1, tr2], spans=[sp1, sp2, sp_orphan])


class TestPlstProtocol(unittest.TestCase):
    def test_zstd_dict_bytes_and_sha256(self) -> None:
        self.assertEqual(len(AGENT_ZSTD_DICT), 1014)
        digest = hashlib.sha256(AGENT_ZSTD_DICT).hexdigest()
        self.assertEqual(digest, "7d9a2067671e1e0d8396caedf72b8189b755ca1c6a5a1d47a833e66895ba1385")

    def test_roundtrip_all_compression_modes(self) -> None:
        batch = _sample_complex_batch()
        modes = ["none"]
        if has_zstd():
            modes.extend(["raw", "dict", "auto"])

        for mode in modes:
            frame = encode_plst_batch(batch, compression=mode)  # type: ignore[arg-type]
            magic, ver, flags, dict_id, uncomp_len, comp_len, crc = struct.unpack_from(
                "<4sBBHIII", frame, 0
            )
            self.assertEqual(magic, b"PLST")
            self.assertEqual(ver, 1)
            self.assertEqual(flags & (FLAG_PARTIAL | FLAG_FINAL), FLAG_PARTIAL | FLAG_FINAL)
            self.assertEqual(comp_len, len(frame) - PLST_HEADER_LEN)
            self.assertEqual(crc, zlib.crc32(frame[PLST_HEADER_LEN:]) & 0xFFFFFFFF)
            if mode == "none":
                self.assertEqual(flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD), 0)
                self.assertEqual(dict_id, 0)
                self.assertEqual(comp_len, uncomp_len)
            elif mode == "raw":
                self.assertEqual(flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD), FLAG_RAW_ZSTD)
                self.assertEqual(dict_id, 0)
            else:
                self.assertEqual(flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD), FLAG_DICT_ZSTD)
                self.assertEqual(dict_id, AGENT_DICT_ID)

            decoded = decode_plst_batch(frame)
            self.assertEqual(len(decoded.traces), 2)
            self.assertEqual(len(decoded.spans), 3)
            self.assertEqual(decoded.traces[0].trace_id, "tr_1")
            self.assertEqual(decoded.traces[0].tags, ["prod", "rag", "finance"])
            self.assertEqual(decoded.traces[1].project_id, "")
            self.assertEqual(decoded.traces[1].input, "123")
            self.assertEqual(decoded.traces[1].output, 123)
            self.assertEqual(decoded.traces[1].total_cost_nanos, 1_000_000)
            self.assertEqual(decoded.spans[1].linked_span_ids, ["sp_ret"])
            self.assertEqual(decoded.spans[1].span_phase, SpanPhase.UPDATE)
            self.assertEqual(decoded.spans[2].span_type, SpanType.GUARDRAIL)
            self.assertEqual(decoded.spans[2].span_phase, SpanPhase.START)
            self.assertEqual(decoded.spans[0].events[0].name, "docs.fetched")
            self.assertEqual(decoded.spans[1].events[0].attributes, {"len": 12})

    def test_empty_batch_sets_flag_final(self) -> None:
        frame = encode_plst_batch(TraceIngestBatch(), compression="none")
        flags = frame[5]
        self.assertEqual(flags, FLAG_FINAL)
        decoded = decode_plst_batch(frame)
        self.assertEqual(decoded.traces, [])
        self.assertEqual(decoded.spans, [])

    def test_python_lt_314_zstd_unavailable_fallback(self) -> None:
        batch = _sample_complex_batch()
        with mock.patch.object(proto_mod, "_zstd", None):
            self.assertFalse(proto_mod.has_zstd())
            frame = proto_mod.encode_plst_batch(batch, compression="auto")
            flags = frame[5]
            dict_id = struct.unpack_from("<H", frame, 6)[0]
            self.assertEqual(flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD), 0)
            self.assertEqual(dict_id, 0)
            decoded = proto_mod.decode_plst_batch(frame)
            self.assertEqual(len(decoded.traces), 2)
            self.assertEqual(len(decoded.spans), 3)
            with self.assertRaises(ProtocolError):
                proto_mod.encode_plst_batch(batch, compression="dict")

    def test_header_and_crc_validation_errors(self) -> None:
        frame = bytearray(encode_plst_batch(_sample_complex_batch(), compression="none"))

        bad_magic = bytearray(frame)
        bad_magic[0:4] = b"NOPE"
        with self.assertRaises(ProtocolError):
            decode_plst_batch(bytes(bad_magic))

        bad_crc = bytearray(frame)
        bad_crc[-1] ^= 0xFF
        with self.assertRaises(ProtocolError):
            decode_plst_batch(bytes(bad_crc))

        bad_dict = bytearray(frame)
        bad_dict[6] = 9
        with self.assertRaises(ProtocolError):
            decode_plst_batch(bytes(bad_dict))

        with self.assertRaises(ProtocolError):
            decode_plst_batch(bytes(frame[:-1]))

        with self.assertRaises(ProtocolError):
            decode_plst_batch(bytes(frame) + b"\x00")

    def test_zstd_bomb_bounded_decompression(self) -> None:
        if not has_zstd():
            self.skipTest("compression.zstd unavailable")
        from compression import zstd

        bomb_plain = b"\x00" * (65 * 1024 * 1024)
        bomb_raw = zstd.compress(bomb_plain, level=1)
        header = struct.pack(
            "<4sBBHIII",
            b"PLST",
            1,
            FLAG_RAW_ZSTD | FLAG_FINAL,
            0,
            1024,  # spoofed small uncomp_len
            len(bomb_raw),
            zlib.crc32(bomb_raw) & 0xFFFFFFFF,
        )
        with self.assertRaises(ProtocolError) as ctx:
            decode_plst_batch(header + bomb_raw)
        self.assertIn("64 MiB", str(ctx.exception))

        # Concatenated zstd frames in body must also be rejected
        concat_body = zstd.compress(b"abc") + zstd.compress(b"def")
        concat_hdr = struct.pack(
            "<4sBBHIII",
            b"PLST",
            1,
            FLAG_RAW_ZSTD | FLAG_FINAL,
            0,
            3,
            len(concat_body),
            zlib.crc32(concat_body) & 0xFFFFFFFF,
        )
        with self.assertRaises(ProtocolError):
            decode_plst_batch(concat_hdr + concat_body)

    def test_unsorted_dict_blob_dedup_and_canonical_bytes(self) -> None:
        b1 = TraceIngestBatch(
            traces=[
                AgentTrace(
                    trace_id="t1",
                    agent_name="a",
                    start_ts_ms=1000,
                    input={"z": 1, "a": {"y": 2, "x": 3}},
                )
            ],
            spans=[
                AgentSpan(
                    span_id="s1",
                    trace_id="t1",
                    name="s",
                    start_ts_ms=1000,
                    output={"a": {"x": 3, "y": 2}, "z": 1},
                )
            ],
        )
        b2 = TraceIngestBatch(
            traces=[
                AgentTrace(
                    trace_id="t1",
                    agent_name="a",
                    start_ts_ms=1000,
                    input={"a": {"x": 3, "y": 2}, "z": 1},
                )
            ],
            spans=[
                AgentSpan(
                    span_id="s1",
                    trace_id="t1",
                    name="s",
                    start_ts_ms=1000,
                    output={"z": 1, "a": {"y": 2, "x": 3}},
                )
            ],
        )
        self.assertEqual(
            encode_plst_batch(b1, compression="none"),
            encode_plst_batch(b2, compression="none"),
        )


if __name__ == "__main__":
    unittest.main()
