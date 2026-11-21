import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { describe, it } from "node:test";
import zlib from "node:zlib";

import {
  AGENT_DICT_ID,
  AGENT_ZSTD_DICT,
  crc32,
  crc32Pure,
  decodePlstBatch,
  encodeColumnarPayload,
  encodePlstBatch,
  FLAG_DICT_ZSTD,
  FLAG_FINAL,
  FLAG_PARTIAL,
  FLAG_RAW_ZSTD,
  PlstError,
  supportsZstd,
  xxh3_64,
  xxh3_64Bytes,
} from "../dist/index.js";

function makeSampleBatch() {
  const sharedPayload = { prompt: "Summarize quarterly revenue", metadata: { region: "us-east" } };
  return {
    traces: [
      {
        trace_id: "tr_002",
        project_id: "proj_1",
        agent_name: "planner-agent",
        agent_version: "1.2.0",
        prompt_version: "p_v3",
        model_config_version: "mc_1",
        tool_config_version: null,
        app_release: "2026.11.20",
        environment: "production",
        session_id: "sess_abc",
        user_id: "user_99",
        workflow_id: "wf_alpha",
        parent_trace_id: "tr_001",
        status: "ok",
        start_ts_ms: 1_700_000_005_000,
        end_ts_ms: 1_700_000_006_250,
        duration_ms: 1250,
        input: sharedPayload,
        output: "",
        error_type: null,
        error_message: null,
        failure_source: null,
        metadata: { emptyStr: "", count: 3 },
        tags: ["vip", "rag", ""],
        span_count: 2,
        llm_call_count: 1,
        tool_call_count: 0,
        error_span_count: 0,
        input_tokens: 120,
        output_tokens: 45,
        cached_tokens: 20,
        total_tokens: 165,
        total_cost_nanos: 1_234_567,
        total_cost_usd: 0.001234567,
        primary_model: "gpt-4o",
        primary_provider: "openai",
        avg_eval_score: 0.925,
        eval_pass_rate: 1.0,
        has_loop_warning: false,
      },
      {
        trace_id: "tr_001",
        project_id: "",
        agent_name: "root-agent",
        agent_version: "unversioned",
        prompt_version: null,
        model_config_version: null,
        tool_config_version: null,
        app_release: null,
        environment: "staging",
        session_id: null,
        user_id: null,
        workflow_id: null,
        parent_trace_id: null,
        status: "running",
        start_ts_ms: 1_700_000_001_000, // negative delta from tr_002
        end_ts_ms: null,
        duration_ms: 0,
        input: null,
        output: null,
        error_type: "",
        error_message: null,
        failure_source: "tool",
        metadata: null,
        tags: [],
        span_count: 0,
        llm_call_count: 0,
        tool_call_count: 0,
        error_span_count: 0,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        total_tokens: 0,
        total_cost_nanos: 0,
        total_cost_usd: 0,
        primary_model: null,
        primary_provider: null,
        avg_eval_score: null,
        eval_pass_rate: null,
        has_loop_warning: true,
      },
    ],
    spans: [
      {
        span_id: "sp_1",
        trace_id: "tr_002",
        project_id: "proj_1",
        parent_span_id: null,
        linked_span_ids: ["sp_orphan", "sp_other"],
        span_phase: "update",
        depth: 0,
        name: "llm.generate",
        span_type: "llm",
        status: "running",
        start_ts_ms: 1_700_000_005_100,
        end_ts_ms: null,
        duration_ms: 0,
        provider: "openai",
        model: "gpt-4o",
        model_id: "gpt-4o-2024-11-20",
        model_version: "2024-11-20",
        prompt_template_id: "tmpl_1",
        prompt_template_version: "v2",
        input_tokens: 120,
        output_tokens: 45,
        cached_tokens: 20,
        total_tokens: 165,
        cost_nanos: 1_234_567,
        cost_usd: 0.001234567,
        temperature: 0.25,
        finish_reason: "stop",
        model_params: { top_p: 0.9 },
        tool_name: null,
        tool_version: null,
        tool_retry_count: 0,
        input: sharedPayload, // duplicates trace input blob!
        output: "Hello world",
        attributes: { "gen_ai.system": "openai" },
        events: [
          {
            name: "first_token",
            timestamp_ms: 1_700_000_005_300,
            attributes: { ttft_ms: 200 },
          },
          {
            name: "earlier_event",
            timestamp_ms: 1_700_000_005_150, // negative delta from previous event
            attributes: null,
          },
        ],
        metadata: null,
        error_type: null,
        error_message: null,
        error_stack: null,
      },
      {
        span_id: "sp_guard",
        trace_id: "tr_orphan_only", // orphan span -> trace_index = 0xFFFFFFFF
        project_id: "proj_1",
        parent_span_id: "sp_1",
        linked_span_ids: [],
        span_phase: "final",
        depth: 1,
        name: "pii.guardrail",
        span_type: "guardrail",
        status: "ok",
        start_ts_ms: 1_700_000_004_000, // negative delta from sp_1
        end_ts_ms: 1_700_000_004_050,
        duration_ms: 50,
        provider: null,
        model: null,
        model_id: null,
        model_version: null,
        prompt_template_id: null,
        prompt_template_version: null,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        total_tokens: 0,
        cost_nanos: 0,
        cost_usd: 0,
        temperature: null,
        finish_reason: null,
        model_params: null,
        tool_name: null,
        tool_version: null,
        tool_retry_count: 0,
        input: "",
        output: { passed: true },
        attributes: null,
        events: [],
        metadata: null,
        error_type: null,
        error_message: null,
        error_stack: null,
      },
    ],
  };
}

describe("PLST v1 Binary Codec & Primitives", () => {
  it("embeds exact 1014-byte AGENT_ZSTD_DICT matching Rust SHA-256", () => {
    assert.equal(AGENT_DICT_ID, 2);
    assert.equal(AGENT_ZSTD_DICT.byteLength, 1014);
    const sha = createHash("sha256").update(AGENT_ZSTD_DICT).digest("hex");
    assert.equal(sha, "7d9a2067671e1e0d8396caedf72b8189b755ca1c6a5a1d47a833e66895ba1385");
  });

  it("computes pure-TS CRC32 matching node:zlib.crc32", () => {
    const samples = [
      new Uint8Array(0),
      new TextEncoder().encode("hello"),
      new TextEncoder().encode("Pulse Analytics PLST v1 UTF-8 🚀"),
      Uint8Array.from({ length: 513 }, (_, i) => (i * 131 + 7) & 0xff),
    ];
    for (const s of samples) {
      const expected = zlib.crc32(s) >>> 0;
      assert.equal(crc32Pure(s), expected);
      assert.equal(crc32(s), expected);
    }
  });

  it("computes XXH3-64 matching Rust xxhash_rust vectors", () => {
    assert.equal(xxh3_64Bytes(new Uint8Array(0)), 0x2d06800538d394c2n);
    const makeBytes = (n) => Uint8Array.from({ length: n }, (_, i) => (i * 131 + 7) & 0xff);
    assert.equal(xxh3_64Bytes(makeBytes(16)), 0x86abf6baccea0858n);
    assert.equal(xxh3_64Bytes(makeBytes(128)), 0x10d17f72c0ccba41n);
    assert.equal(xxh3_64Bytes(makeBytes(240)), 0xb6cfaf343fab81e6n);
    assert.equal(xxh3_64Bytes(makeBytes(1025)), 0x66c4487c41e127a7n);
    assert.equal(xxh3_64("sess_abc"), xxh3_64Bytes(new TextEncoder().encode("sess_abc")));
  });

  it("round-trips empty batch with FLAG_FINAL set", () => {
    const empty = { traces: [], spans: [] };
    const frame = encodePlstBatch(empty, { compression: "none" });
    assert.equal(String.fromCharCode(...frame.subarray(0, 4)), "PLST");
    assert.equal(frame[4], 1);
    assert.equal(frame[5], FLAG_FINAL);
    const decoded = decodePlstBatch(frame);
    assert.deepEqual(decoded, empty);
  });

  it("deduplicates identical blobs and distinguishes empty string from null", () => {
    const batch = makeSampleBatch();
    const columnar = encodeColumnarPayload(batch);
    const dv = new DataView(columnar.buffer, columnar.byteOffset, columnar.byteLength);
    // Section 1 string_count + offsets + data
    const strCount = dv.getUint32(0, true);
    const strEndOff = dv.getUint32(4 + strCount * 4, true);
    const blobSectionOffset = 4 + (strCount + 1) * 4 + strEndOff;
    const blobCount = dv.getUint32(blobSectionOffset, true);
    // Trace 0 has: input(sharedPayload), output(""), metadata({emptyStr:"", count:3}) -> 3
    // Span 0 has: model_params({top_p:0.9}), input(sharedPayload -> DEDUP!), output("Hello world"), attributes({gen_ai.system:"openai"}), ev0.attrs({ttft_ms:200}) -> +4 = 7
    // Span 1 has: input("") -> DEDUP with trace 0 output("")!, output({passed:true}) -> +1 = 8
    assert.equal(blobCount, 8);
  });

  it("round-trips lossless across uncompressed, raw Zstd, and dict Zstd", () => {
    const batch = makeSampleBatch();
    const modes = supportsZstd() ? ["none", "raw", "dict"] : ["none"];
    for (const mode of modes) {
      const frame = encodePlstBatch(batch, { compression: mode });
      const dv = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
      const flags = frame[5];
      assert.equal((flags & (FLAG_PARTIAL | FLAG_FINAL)) !== 0, true);
      if (mode === "none") {
        assert.equal(flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD), 0);
        assert.equal(dv.getUint16(6, true), 0);
      } else if (mode === "raw") {
        assert.equal(flags & FLAG_RAW_ZSTD, FLAG_RAW_ZSTD);
        assert.equal(dv.getUint16(6, true), 0);
      } else {
        assert.equal(flags & FLAG_DICT_ZSTD, FLAG_DICT_ZSTD);
        assert.equal(dv.getUint16(6, true), AGENT_DICT_ID);
      }

      const decoded = decodePlstBatch(frame);
      assert.equal(decoded.traces.length, batch.traces.length);
      assert.equal(decoded.spans.length, batch.spans.length);
      assert.deepEqual(decoded.traces[0], batch.traces[0]);
      assert.deepEqual(decoded.traces[1], batch.traces[1]);
      assert.deepEqual(decoded.spans[0], batch.spans[0]);
      assert.deepEqual(decoded.spans[1], batch.spans[1]);
      assert.strictEqual(decoded.traces[0].output, "");
      assert.strictEqual(decoded.traces[1].error_type, "");
      assert.strictEqual(decoded.traces[1].input, null);
      assert.strictEqual(decoded.spans[1].input, "");
    }
  });

  it("rejects corrupted magic, CRC mismatch, invalid flags, and trailing bytes", () => {
    const frame = encodePlstBatch(makeSampleBatch(), { compression: "none" });

    assert.throws(() => decodePlstBatch(frame.subarray(0, 10)), PlstError);

    const badMagic = frame.slice();
    badMagic[0] = 0x58;
    assert.throws(() => decodePlstBatch(badMagic), (e) => e instanceof PlstError && e.code === "INVALID_MAGIC");

    const badCrc = frame.slice();
    badCrc[badCrc.length - 1] ^= 0xff;
    assert.throws(() => decodePlstBatch(badCrc), (e) => e instanceof PlstError && e.code === "CRC_MISMATCH");

    const badFlags = frame.slice();
    badFlags[5] = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | FLAG_FINAL;
    assert.throws(() => decodePlstBatch(badFlags), (e) => e instanceof PlstError && e.code === "UNSUPPORTED_FLAGS");

    const trailing = new Uint8Array(frame.length + 1);
    trailing.set(frame);
    trailing[trailing.length - 1] = 0;
    assert.throws(() => decodePlstBatch(trailing), (e) => e instanceof PlstError && e.code === "TRAILING_BYTES");
  });

  it("enforces maxOutputLength on zstdDecompress when header lies about uncompLen", () => {
    if (!supportsZstd()) return;
    const bomb = zlib.zstdCompressSync(new Uint8Array(64 * 1024 * 1024 + 1024));
    const frame = new Uint8Array(20 + bomb.length);
    const dv = new DataView(frame.buffer);
    frame.set([0x50, 0x4c, 0x53, 0x54], 0); // "PLST"
    frame[4] = 1;
    frame[5] = FLAG_RAW_ZSTD | FLAG_FINAL;
    dv.setUint16(6, 0, true);
    dv.setUint32(8, 100, true); // spoofed small uncomp_len
    dv.setUint32(12, bomb.length, true);
    dv.setUint32(16, crc32(bomb), true);
    frame.set(bomb, 20);

    assert.throws(
      () => decodePlstBatch(frame),
      (e) => e instanceof PlstError && e.code === "RESOURCE_LIMIT_EXCEEDED",
    );
  });
});
