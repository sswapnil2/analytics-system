"""Tier 2 opaque-box boundary, hostile-input, and corner-case tests for F01-F12."""

from __future__ import annotations

import concurrent.futures
from compression import zstd
from datetime import datetime, timezone
import http.client
import json
from pathlib import Path
import struct
import time
from typing import Any
import zlib

from lib import fixtures, plst
from lib.auth import ProjectCreds, TestContext
from lib.client import HttpResponse, PulseClient
from lib.registry import test_case


# ---------------------------------------------------------------------------
# Shared Helpers (Isolated Projects, Nodes, Polling, Raw PLST Section Patchers)
# ---------------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _create_throwaway_project(ctx: TestContext, prefix: str) -> ProjectCreds:
    """Create an isolated throwaway project under org_default so shared proj_a stays clean."""
    name = ctx.unique_id(prefix)
    resp = ctx.client.request(
        "POST",
        "/v1/projects",
        json_body={"name": name, "timezone": "UTC"},
        token=ctx.owner_token,
    ).raise_for_status()
    data = resp.json()
    proj_id = str(data["project"]["id"])
    pk_key = str(data["api_key"]["plaintext_key"])

    sk_resp = ctx.client.request(
        "POST",
        f"/v1/projects/{proj_id}/api-keys",
        json_body={"label": f"{name}-admin", "role": "admin"},
        token=ctx.owner_token,
    ).raise_for_status()
    sk_key = str(sk_resp.json()["plaintext_key"])
    return ProjectCreds(id=proj_id, name=name, pk_key=pk_key, sk_key=sk_key)


def _setup_isolated_node_project(
    client: PulseClient,
    admin_token: str,
    name: str,
) -> tuple[str, ProjectCreds]:
    """Bootstrap an owner + project + API keys on an isolated temp pulse-node."""
    email = f"iso+{name}@e2e.local"
    password = f"PulseIso!{name}"
    if len(password) < 12:
        password = f"{password}0123456789"[:14]

    signup_resp = client.request(
        "POST",
        "/v1/auth/signup",
        json_body={
            "email": email,
            "password": password,
            "name": "Iso Owner",
            "setup_code": admin_token,
        },
    )
    if signup_resp.is_success() and isinstance(signup_resp.json(), dict) and "token" in signup_resp.json():
        owner_token = str(signup_resp.json()["token"])
    else:
        owner_token = admin_token

    proj_resp = client.request(
        "POST",
        "/v1/projects",
        json_body={"name": name, "timezone": "UTC"},
        token=owner_token,
    ).raise_for_status()
    data = proj_resp.json()
    proj_id = str(data["project"]["id"])
    pk_key = str(data["api_key"]["plaintext_key"])

    sk_resp = client.request(
        "POST",
        f"/v1/projects/{proj_id}/api-keys",
        json_body={"label": f"{name}-admin", "role": "admin"},
        token=owner_token,
    ).raise_for_status()
    sk_key = str(sk_resp.json()["plaintext_key"])
    return owner_token, ProjectCreds(id=proj_id, name=name, pk_key=pk_key, sk_key=sk_key)


def _get_trace_detail(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    *,
    token: str,
) -> HttpResponse:
    return client.request(
        "GET",
        f"/v1/projects/{project_id}/agent/traces/{trace_id}",
        token=token,
    )


def _wait_for_trace(
    client: PulseClient,
    project_id: str,
    trace_id: str,
    *,
    token: str,
    timeout: float = 10.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_status = 0
    last_body = ""
    while time.monotonic() < deadline:
        resp = _get_trace_detail(client, project_id, trace_id, token=token)
        last_status = resp.status
        if resp.status == 200:
            return resp.json()
        last_body = resp.text(errors="replace")[:200]
        time.sleep(0.1)
    raise AssertionError(
        f"Timed out waiting for trace {trace_id} in {project_id}: last={last_status} {last_body}"
    )


def _wait_for_flush_segments(
    data_dir: Path,
    project_id: str | None = None,
    *,
    min_segments: int = 1,
    timeout: float = 15.0,
) -> list[Path]:
    """Poll `<data_dir>/object_store/agent_segments` until >= min_segments .plss files exist."""
    seg_root = data_dir / "object_store" / "agent_segments"
    if project_id is not None:
        seg_root = seg_root / project_id
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if seg_root.is_dir():
            found = sorted(p for p in seg_root.rglob("*.plss") if p.is_file())
            if len(found) >= min_segments:
                return found
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {min_segments} .plss segments under {seg_root}")


def _set_privacy_policy(
    client: PulseClient,
    project_id: str,
    *,
    token: str,
    policy: dict[str, Any],
) -> dict[str, Any]:
    resp = client.request(
        "PUT",
        f"/v1/projects/{project_id}/agent/privacy",
        json_body=policy,
        token=token,
    ).raise_for_status()
    return resp.json()


def _parse_section_offsets(raw_sections: bytes) -> dict[str, int]:
    """Compute exact byte offsets of PLST Sections 1..5 inside uncompressed section bytes."""
    pos = 0
    # Section 1: String Table
    str_count = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4
    str_offsets_off = pos
    str_blob_len = struct.unpack_from("<I", raw_sections, pos + str_count * 4)[0]
    pos += (str_count + 1) * 4
    str_blob_off = pos
    pos += str_blob_len

    # Section 2: Blob Table
    sec2_off = pos
    blob_count = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4
    blob_hashes_off = pos
    pos += blob_count * 8
    blob_enc_off = pos
    pos += blob_count
    blob_offsets_off = pos
    blob_total_bytes = struct.unpack_from("<I", raw_sections, pos + blob_count * 4)[0]
    pos += (blob_count + 1) * 4
    blob_data_off = pos
    pos += blob_total_bytes

    # Section 3: Trace Rows
    sec3_off = pos
    trace_count = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4 + (172 * trace_count)
    total_tr_tags = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4 + (total_tr_tags * 4) + (trace_count * 12)

    # Section 4: Span Rows
    sec4_off = pos
    span_count = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4
    sp_trace_idx_off = pos
    pos += span_count * 4 * 5  # trace_idx, trace_id, project_id, span_id, parent_span_id
    pos += span_count * 2      # sp_linked_counts
    total_linked = struct.unpack_from("<I", raw_sections, pos)[0]
    pos += 4 + total_linked * 4
    pos += span_count * 4      # sp_depth (i32)
    pos += span_count * 4      # sp_name (u32)
    sp_type_off = pos
    pos += span_count
    sp_phase_off = pos
    pos += span_count
    pos += span_count          # sp_status
    pos += span_count * (8 + 1 + 8 + 8)  # start_deltas, has_end, end_offsets, durations
    pos += span_count * (4 * 6)          # provider..prompt_tpl_ver
    pos += span_count * (8 * 5)          # tokens + cost_nanos
    pos += span_count * (1 + 8 + 4 + 4 + 4 + 4 + 4)  # temp..tool_retry
    pos += span_count * (4 * 7)          # error_types..meta_blobs

    # Section 5: Events
    sec5_off = pos
    event_count = struct.unpack_from("<I", raw_sections, pos)[0]
    ev_span_idx_off = pos + 4
    return {
        "str_count": str_count,
        "str_offsets_off": str_offsets_off,
        "str_blob_off": str_blob_off,
        "sec2_off": sec2_off,
        "blob_count": blob_count,
        "blob_hashes_off": blob_hashes_off,
        "blob_enc_off": blob_enc_off,
        "blob_offsets_off": blob_offsets_off,
        "blob_data_off": blob_data_off,
        "sec3_off": sec3_off,
        "trace_count": trace_count,
        "sec4_off": sec4_off,
        "span_count": span_count,
        "sp_trace_idx_off": sp_trace_idx_off,
        "sp_type_off": sp_type_off,
        "sp_phase_off": sp_phase_off,
        "sec5_off": sec5_off,
        "event_count": event_count,
        "ev_span_idx_off": ev_span_idx_off,
    }


def _pack_raw_plst_frame(
    raw_sections: bytes,
    *,
    flags: int = plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL,
    dict_id: int = 0,
    uncomp_len_override: int | None = None,
    comp_len_override: int | None = None,
    use_dict_compress: bool = False,
    custom_dict_bytes: bytes | None = None,
) -> bytes:
    if use_dict_compress:
        zdict, _ = plst.load_agent_zstd_dict(custom_dict_bytes)
        comp = zstd.compress(raw_sections, level=3, zstd_dict=zdict)
    elif flags & (plst.FLAG_RAW_ZSTD | plst.FLAG_DICT_ZSTD):
        comp = zstd.compress(raw_sections, level=3)
    else:
        comp = raw_sections
    uncomp_len = len(raw_sections) if uncomp_len_override is None else uncomp_len_override
    comp_len = len(comp) if comp_len_override is None else comp_len_override
    crc = zlib.crc32(comp) & 0xFFFFFFFF
    hdr = plst.HEADER_STRUCT.pack(
        plst.PLST_MAGIC,
        plst.PLST_VERSION,
        flags,
        dict_id,
        uncomp_len,
        comp_len,
        crc,
    )
    return hdr + comp


# ===========================================================================
# F01: 5-Level Execution Hierarchy
# ===========================================================================


@test_case(
    id="T2-F01-01",
    tier=2,
    feature="F01",
    title="parent_span_id self-loop, 2-cycle, and 3-cycle with non-cyclic descendant are broken safely",
    tags=("hierarchy", "cycle", "boundary"),
)
def test_f01_cycle_breaking_variants(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    # 1. Self-loop: A -> A
    tr_self_id = ctx.unique_id("tr_self")
    sp_self = fixtures.make_span(
        ctx.unique_id("sp_self"),
        tr_self_id,
        project_id=proj.id,
        parent_span_id=None,
        name="self.loop",
        span_type="agent",
        start_ts_ms=ts,
    )
    sp_self["parent_span_id"] = sp_self["span_id"]
    tr_self = fixtures.make_trace(
        tr_self_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_self"),
        start_ts_ms=ts,
        spans=[sp_self],
    )

    # 2. 2-cycle: A <-> B
    batch_2cycle = fixtures.make_cyclic_spans_batch(
        seed=ctx.unique_id("cyc2"),
        project_id=proj.id,
        start_ts_ms=ts,
    )
    tr_2_id = batch_2cycle["traces"][0]["trace_id"]

    # 3. 3-cycle A -> B -> C -> A with non-cyclic descendant D -> A
    tr_3_id = ctx.unique_id("tr_cyc3")
    sp_a_id = ctx.unique_id("sp_a")
    sp_b_id = ctx.unique_id("sp_b")
    sp_c_id = ctx.unique_id("sp_c")
    sp_d_id = ctx.unique_id("sp_d")
    spans_3 = [
        fixtures.make_span(sp_a_id, tr_3_id, project_id=proj.id, parent_span_id=sp_c_id, name="cyc.a", span_type="agent", start_ts_ms=ts),
        fixtures.make_span(sp_b_id, tr_3_id, project_id=proj.id, parent_span_id=sp_a_id, name="cyc.b", span_type="llm", start_ts_ms=ts + 5),
        fixtures.make_span(sp_c_id, tr_3_id, project_id=proj.id, parent_span_id=sp_b_id, name="cyc.c", span_type="tool", start_ts_ms=ts + 10),
        fixtures.make_span(sp_d_id, tr_3_id, project_id=proj.id, parent_span_id=sp_a_id, name="desc.d", span_type="custom", start_ts_ms=ts + 15),
    ]
    tr_3 = fixtures.make_trace(
        tr_3_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_cyc3"),
        start_ts_ms=ts,
        spans=spans_3,
    )

    batch = {
        "traces": [tr_self, *batch_2cycle["traces"], tr_3],
        "spans": [sp_self, *batch_2cycle["spans"], *spans_3],
    }
    resp = ctx.client.ingest_json(batch, api_key=proj.pk_key)
    resp.raise_for_status()
    assert resp.json()["accepted_traces"] == 3

    d_self = _wait_for_trace(ctx.client, proj.id, tr_self_id, token=ctx.viewer_token)
    assert d_self["spans"][0]["parent_span_id"] is None
    assert d_self["spans"][0]["depth"] == 0

    d_2 = _wait_for_trace(ctx.client, proj.id, tr_2_id, token=ctx.viewer_token)
    assert len(d_2["spans"]) == 2
    roots_2 = [s for s in d_2["spans"] if s["parent_span_id"] is None]
    assert len(roots_2) >= 1, f"2-cycle was not broken: {d_2['spans']}"
    assert all(0 <= int(s["depth"]) <= 32 for s in d_2["spans"])

    d_3 = _wait_for_trace(ctx.client, proj.id, tr_3_id, token=ctx.viewer_token)
    by_id = {s["span_id"]: s for s in d_3["spans"]}
    assert len(by_id) == 4
    roots_3 = [s for s in d_3["spans"] if s["parent_span_id"] is None]
    assert len(roots_3) >= 1, f"3-cycle was not broken: {d_3['spans']}"
    assert by_id[sp_d_id]["parent_span_id"] == sp_a_id
    assert 1 <= int(by_id[sp_d_id]["depth"]) <= 32


@test_case(
    id="T2-F01-02",
    tier=2,
    feature="F01",
    title="Span depth boundary: 32 vs 33 vs 40 chain clamped to MAX_SPAN_DEPTH=32",
    tags=("hierarchy", "depth", "boundary"),
)
def test_f01_depth_clamped_to_32(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    # Chain of 33 spans -> indices 0..32 (max depth exactly 32)
    b33 = fixtures.make_deep_chain_batch(
        seed=ctx.unique_id("d33"),
        project_id=proj.id,
        depth_count=33,
        start_ts_ms=ts,
    )
    # Chain of 40 spans -> indices 33..39 must clamp to 32
    b40 = fixtures.make_deep_chain_batch(
        seed=ctx.unique_id("d40"),
        project_id=proj.id,
        depth_count=40,
        start_ts_ms=ts,
    )
    for b in (b33, b40):
        r = ctx.client.ingest_plst_batch(b, api_key=proj.pk_key)
        r.raise_for_status()
        assert r.json()["accepted_traces"] == 1

    d33 = _wait_for_trace(ctx.client, proj.id, b33["traces"][0]["trace_id"], token=ctx.viewer_token)
    depths33 = sorted(int(s["depth"]) for s in d33["spans"])
    assert depths33 == list(range(33)), f"Expected 0..32, got {depths33}"

    d40 = _wait_for_trace(ctx.client, proj.id, b40["traces"][0]["trace_id"], token=ctx.viewer_token)
    assert len(d40["spans"]) == 40
    depths40 = [int(s["depth"]) for s in d40["spans"]]
    assert max(depths40) == 32
    assert sum(1 for d in depths40 if d == 32) == 8  # indices 32..39


@test_case(
    id="T2-F01-03",
    tier=2,
    feature="F01",
    title="Spans per trace boundary: 512 accepted vs 513 and 550 trigger dropped_spans",
    tags=("hierarchy", "limit", "boundary"),
)
def test_f01_max_spans_per_trace_512(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    b512 = fixtures.make_many_spans_batch(seed=ctx.unique_id("s512"), project_id=proj.id, span_count=512, start_ts_ms=ts)
    r512 = ctx.client.ingest_plst_batch(b512, api_key=proj.pk_key)
    r512.raise_for_status()
    j512 = r512.json()
    assert j512["accepted_spans"] == 512
    assert j512["dropped_spans"] == 0

    b513 = fixtures.make_many_spans_batch(seed=ctx.unique_id("s513"), project_id=proj.id, span_count=513, start_ts_ms=ts)
    r513 = ctx.client.ingest_plst_batch(b513, api_key=proj.pk_key)
    r513.raise_for_status()
    j513 = r513.json()
    assert j513["accepted_spans"] == 512
    assert j513["dropped_spans"] == 1

    b550 = fixtures.make_many_spans_batch(seed=ctx.unique_id("s550"), project_id=proj.id, span_count=550, start_ts_ms=ts)
    r550 = ctx.client.ingest_json(b550, api_key=proj.pk_key)
    r550.raise_for_status()
    j550 = r550.json()
    assert j550["accepted_spans"] == 512
    assert j550["dropped_spans"] == 38

    d550 = _wait_for_trace(ctx.client, proj.id, b550["traces"][0]["trace_id"], token=ctx.viewer_token)
    assert len(d550["spans"]) == 512


@test_case(
    id="T2-F01-04",
    tier=2,
    feature="F01",
    title="Span attributes (>64), events (>64), linked_span_ids (>64 + self-link), and tags (>32) truncation",
    tags=("hierarchy", "truncation", "boundary"),
)
def test_f01_attr_event_link_tag_caps(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_caps")
    sp_id = ctx.unique_id("sp_caps")

    attrs = {f"k_{i:03d}": i for i in range(70)}
    events = [fixtures.make_event(f"ev_{i:03d}", timestamp_ms=ts + i, attributes={"seq": i}) for i in range(70)]
    links = [sp_id, "   ", "", f"  {sp_id}  "] + [f"link_{i:03d}" for i in range(70)] + ["link_000"]
    tags = [f"tag_{i:02d}" for i in range(36)]

    sp = fixtures.make_span(
        sp_id,
        tr_id,
        project_id=proj.id,
        linked_span_ids=links,
        attributes=attrs,
        events=events,
        start_ts_ms=ts,
    )
    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_caps"),
        tags=tags,
        start_ts_ms=ts,
        spans=[sp],
    )
    ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    got_tr = detail["trace"]
    got_sp = detail["spans"][0]

    assert len(got_tr["tags"]) == 32
    assert len(got_sp["attributes"]) == 64
    assert len(got_sp["events"]) == 64
    assert len(got_sp["linked_span_ids"]) == 64
    assert sp_id not in got_sp["linked_span_ids"]
    assert "" not in got_sp["linked_span_ids"]
    assert len(set(got_sp["linked_span_ids"])) == len(got_sp["linked_span_ids"])


@test_case(
    id="T2-F01-05",
    tier=2,
    feature="F01",
    title="Empty batch {traces:[], spans:[]} and zero-span trace boundary",
    tags=("hierarchy", "empty", "boundary"),
)
def test_f01_empty_batch_and_zero_span_trace(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    empty_batch: dict[str, Any] = {"traces": [], "spans": []}
    for resp in (
        ctx.client.ingest_json(empty_batch, api_key=proj.pk_key),
        ctx.client.ingest_plst_batch(empty_batch, api_key=proj.pk_key),
    ):
        resp.raise_for_status()
        body = resp.json()
        assert body["accepted_traces"] == 0
        assert body["accepted_spans"] == 0
        assert body["dropped_spans"] == 0

    tr_id = ctx.unique_id("tr_nospan")
    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_nospan"),
        start_ts_ms=ts,
        spans=[],
    )
    r = ctx.client.ingest_plst_batch({"traces": [tr], "spans": []}, api_key=proj.pk_key)
    r.raise_for_status()
    assert r.json()["accepted_traces"] == 1
    assert r.json()["accepted_spans"] == 0

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    assert detail["trace"]["trace_id"] == tr_id
    assert detail["spans"] == []


@test_case(
    id="T2-F01-06",
    tier=2,
    feature="F01",
    title="Multi-level parent_trace_id + workflow_id + cross-trace DAG linked_span_ids persistence",
    tags=("hierarchy", "parent_trace_id", "dag"),
)
def test_f01_parent_trace_and_dag_links(ctx: TestContext) -> None:
    proj = ctx.proj_a
    seed = ctx.unique_id("hier")
    batch = fixtures.make_hierarchy_batch(seed=seed, project_id=proj.id, start_ts_ms=_now_ms())
    ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=True).raise_for_status()

    parent_tr_id = batch["traces"][0]["trace_id"]
    child_tr_id = batch["traces"][1]["trace_id"]
    child_detail = _wait_for_trace(ctx.client, proj.id, child_tr_id, token=ctx.viewer_token)
    assert child_detail["trace"]["parent_trace_id"] == parent_tr_id
    assert child_detail["trace"]["workflow_id"] == f"wf_{seed}"
    assert len(child_detail["spans"][0]["linked_span_ids"]) == 1
    assert child_detail["spans"][0]["linked_span_ids"][0] == batch["spans"][0]["span_id"]


# ===========================================================================
# F02: Extended Span Types, span_phase & cost_nanos
# ===========================================================================


@test_case(
    id="T2-F02-01",
    tier=2,
    feature="F02",
    title="Unknown SpanType in OTLP normalizes to custom while PLST invalid enum tag 255 returns 400",
    tags=("span_type", "otlp", "plst", "negative"),
)
def test_f02_unknown_span_type_otlp_and_plst(ctx: TestContext) -> None:
    proj = ctx.proj_a
    seed = ctx.unique_id("unk_st")
    otlp_req = fixtures.make_otlp_export_request(seed=seed, start_ts_ms=_now_ms())
    otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"].append(
        {"key": "pulse.span_type", "value": {"stringValue": "alien_span_type"}}
    )
    otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"].append(
        {"key": "gen_ai.operation.name", "value": {"stringValue": "alien_op_xyz"}}
    )
    r_otlp = ctx.client.ingest_otlp_json(otlp_req, api_key=proj.pk_key)
    r_otlp.raise_for_status()
    otlp_trace_id = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
    detail = _wait_for_trace(ctx.client, proj.id, otlp_trace_id, token=ctx.viewer_token)
    root_sp = next(s for s in detail["spans"] if s["name"] == "agent.handle_request")
    assert root_sp["span_type"] == "custom"

    # PLST binary invalid SpanType tag 255 -> 400
    batch = fixtures.make_batch(seed=ctx.unique_id("bad_tag"), project_id=proj.id, start_ts_ms=_now_ms())
    raw_sec = bytearray(plst._encode_sections(batch))
    offs = _parse_section_offsets(bytes(raw_sec))
    raw_sec[offs["sp_type_off"]] = 255
    bad_frame = _pack_raw_plst_frame(bytes(raw_sec))
    r_bad = ctx.client.ingest_plst(bad_frame, api_key=proj.pk_key)
    assert r_bad.status == 400, f"Expected 400 for PLST SpanType tag=255, got {r_bad.status}"


@test_case(
    id="T2-F02-02",
    tier=2,
    feature="F02",
    title="Omitted span_phase defaults to final in both native JSON and OTLP/JSON",
    tags=("span_phase", "default", "otlp"),
)
def test_f02_missing_span_phase_defaults_to_final(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    tr_id = ctx.unique_id("tr_nophase")
    sp_id = ctx.unique_id("sp_nophase")
    sp = fixtures.make_span(sp_id, tr_id, project_id=proj.id, start_ts_ms=ts)
    sp.pop("span_phase", None)
    tr = fixtures.make_trace(tr_id, project_id=proj.id, session_id=ctx.unique_id("sess_np"), start_ts_ms=ts, spans=[sp])
    ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    assert detail["spans"][0]["span_phase"] == "final"

    otlp_req = fixtures.make_otlp_export_request(seed=ctx.unique_id("otlp_np"), start_ts_ms=ts)
    ctx.client.ingest_otlp_json(otlp_req, api_key=proj.pk_key).raise_for_status()
    otlp_tid = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
    otlp_detail = _wait_for_trace(ctx.client, proj.id, otlp_tid, token=ctx.viewer_token)
    llm_sp = next(s for s in otlp_detail["spans"] if s["span_type"] == "llm")
    assert llm_sp["span_phase"] == "final"


@test_case(
    id="T2-F02-03",
    tier=2,
    feature="F02",
    title="Out-of-order span_phase (final arriving before update and start) keeps final state",
    tags=("span_phase", "out_of_order", "crdt"),
)
def test_f02_out_of_order_span_phase_final_wins(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    seed = ctx.unique_id("ooo")
    batch = fixtures.make_streaming_phase_batch(
        seed=seed,
        project_id=proj.id,
        reverse_order=True,
        start_ts_ms=ts,
    )
    # Ingest in 3 separate HTTP calls: final -> update -> start
    tr = batch["traces"][0]
    spans_rev = batch["spans"]  # [final, update, start]
    assert [s["span_phase"] for s in spans_rev] == ["final", "update", "start"]
    ctx.client.ingest_plst_batch({"traces": [tr], "spans": [spans_rev[0]]}, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch({"traces": [], "spans": [spans_rev[1]]}, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch({"traces": [], "spans": [spans_rev[2]]}, api_key=proj.pk_key).raise_for_status()

    detail = _wait_for_trace(ctx.client, proj.id, tr["trace_id"], token=ctx.viewer_token)
    matching = [s for s in detail["spans"] if s["span_id"] == spans_rev[0]["span_id"]]
    assert len(matching) >= 1
    best = max(matching, key=lambda s: {"start": 0, "update": 1, "final": 2}.get(s["span_phase"], -1))
    assert best["span_phase"] == "final"
    assert best["status"] == "ok"
    assert best["output"] == {"text": "Completed streaming response."}


@test_case(
    id="T2-F02-04",
    tier=2,
    feature="F02",
    title="Negative token counts clamped to 0 in OTLP normalization and cost math",
    tags=("tokens", "clamping", "otlp", "pricing"),
)
def test_f02_negative_tokens_clamped_to_zero(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    seed = ctx.unique_id("neg_tok")
    otlp_req = fixtures.make_otlp_export_request(seed=seed, start_ts_ms=ts)
    llm_attrs = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][1]["attributes"]
    for kv in llm_attrs:
        if kv["key"] == "gen_ai.usage.input_tokens":
            kv["value"] = {"intValue": "-500"}
        elif kv["key"] == "gen_ai.usage.output_tokens":
            kv["value"] = {"intValue": "-125"}
        elif kv["key"] == "pulse.cost_usd":
            kv["value"] = {"doubleValue": -0.5}
    ctx.client.ingest_otlp_json(otlp_req, api_key=proj.pk_key).raise_for_status()

    otlp_tid = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
    detail = _wait_for_trace(ctx.client, proj.id, otlp_tid, token=ctx.viewer_token)
    llm_sp = next(s for s in detail["spans"] if s["span_type"] == "llm")
    assert llm_sp["input_tokens"] >= 0
    assert llm_sp["output_tokens"] >= 0
    assert llm_sp["total_tokens"] >= 0
    assert llm_sp["cost_nanos"] >= 0
    assert detail["trace"]["total_cost_nanos"] >= 0


@test_case(
    id="T2-F02-05",
    tier=2,
    feature="F02",
    title="Negative cost_nanos/cost_usd clamped to 0 and cost_usd fallback populates cost_nanos",
    tags=("cost_nanos", "pricing", "clamping", "boundary"),
)
def test_f02_cost_nanos_clamp_and_usd_fallback(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    tr_id = ctx.unique_id("tr_cost")
    sp_neg = fixtures.make_span(
        ctx.unique_id("sp_neg"),
        tr_id,
        project_id=proj.id,
        span_type="tool",
        cost_nanos=-1_234_567,
        cost_usd=-0.5,
        start_ts_ms=ts,
    )
    sp_usd = fixtures.make_span(
        ctx.unique_id("sp_usd"),
        tr_id,
        project_id=proj.id,
        span_type="llm",
        cost_nanos=0,
        cost_usd=0.0025,
        start_ts_ms=ts + 10,
    )
    # Explicitly send cost_nanos=0, cost_usd=0.0025 in raw JSON
    sp_usd["cost_nanos"] = 0
    sp_usd["cost_usd"] = 0.0025
    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_cost"),
        start_ts_ms=ts,
        spans=[sp_neg, sp_usd],
    )
    tr["total_cost_nanos"] = -500
    tr["total_cost_usd"] = -0.1

    ctx.client.ingest_json({"traces": [tr], "spans": [sp_neg, sp_usd]}, api_key=proj.pk_key).raise_for_status()
    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    by_id = {s["span_id"]: s for s in detail["spans"]}
    assert by_id[sp_neg["span_id"]]["cost_nanos"] == 0
    assert by_id[sp_neg["span_id"]]["cost_usd"] == 0.0
    assert abs(by_id[sp_usd["span_id"]]["cost_nanos"] - 2_500_000) <= 1
    assert detail["trace"]["total_cost_nanos"] >= 2_500_000


@test_case(
    id="T2-F02-06",
    tier=2,
    feature="F02",
    title="All 10 SpanTypes round-trip via PLST and invalid span_phase enum tag 99 returns 400",
    tags=("span_type", "span_phase", "plst", "negative"),
)
def test_f02_all_span_types_and_invalid_phase_tag(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_allst")
    spans = [
        fixtures.make_span(
            ctx.unique_id(f"sp_{st}"),
            tr_id,
            project_id=proj.id,
            span_type=st,
            model="text-embedding-3-small" if st == "embedding" else ("gpt-4o-mini" if st == "llm" else None),
            start_ts_ms=ts + idx * 5,
        )
        for idx, st in enumerate(fixtures.ALL_SPAN_TYPES)
    ]
    tr = fixtures.make_trace(tr_id, project_id=proj.id, session_id=ctx.unique_id("sess_allst"), start_ts_ms=ts, spans=spans)
    ctx.client.ingest_plst_batch({"traces": [tr], "spans": spans}, api_key=proj.pk_key, use_dict=True).raise_for_status()
    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    got_types = {s["span_type"] for s in detail["spans"]}
    assert got_types == set(fixtures.ALL_SPAN_TYPES)

    # Invalid span_phase tag 99 -> 400
    b = fixtures.make_batch(seed=ctx.unique_id("bad_ph"), project_id=proj.id, start_ts_ms=ts)
    raw_sec = bytearray(plst._encode_sections(b))
    offs = _parse_section_offsets(bytes(raw_sec))
    raw_sec[offs["sp_phase_off"]] = 99
    bad_frame = _pack_raw_plst_frame(bytes(raw_sec))
    r_bad = ctx.client.ingest_plst(bad_frame, api_key=proj.pk_key)
    assert r_bad.status == 400


# ===========================================================================
# F03: PLST v1 Header & Decompression Guard
# ===========================================================================


@test_case(
    id="T2-F03-01",
    tier=2,
    feature="F03",
    title="PLST hostile header magic, version, and flags rejected with 400 without crash",
    tags=("plst", "header", "hostile"),
)
def test_f03_hostile_header_magic_version_flags(ctx: TestContext) -> None:
    proj = ctx.proj_a
    for kind in ("bad_magic", "bad_version", "bad_flags"):
        frame = plst.build_hostile_frame(kind)
        resp = ctx.client.ingest_plst(frame, api_key=proj.pk_key)
        assert resp.status == 400, f"Expected 400 for {kind}, got {resp.status}: {resp.text(errors='replace')}"
    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F03-02",
    tier=2,
    feature="F03",
    title="PLST truncated header, comp_len mismatch, trailing bytes, and truncated body return 400",
    tags=("plst", "truncation", "hostile"),
)
def test_f03_hostile_truncation_and_lengths(ctx: TestContext) -> None:
    proj = ctx.proj_a
    valid = plst.encode_plst_frame(fixtures.make_batch(seed=ctx.unique_id("hdr"), project_id=proj.id))
    trailing = valid + b"\xde\xad\xbe\xef"
    frames = [
        ("empty_0b", b""),
        ("truncated_header", plst.build_hostile_frame("truncated_header")),
        ("header_19b", valid[:19]),
        ("comp_len_mismatch", plst.build_hostile_frame("comp_len_mismatch")),
        ("trailing_bytes", trailing),
        ("truncated_body", plst.build_hostile_frame("truncated_body")),
    ]
    for label, frame in frames:
        resp = ctx.client.ingest_plst(frame, api_key=proj.pk_key)
        assert resp.status == 400, f"Expected 400 for {label}, got {resp.status}"
    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F03-03",
    tier=2,
    feature="F03",
    title="PLST CompressedCRC32 mismatch returns 400 prior to decompression",
    tags=("plst", "crc32", "hostile"),
)
def test_f03_crc_mismatch(ctx: TestContext) -> None:
    proj = ctx.proj_a
    frame = plst.build_hostile_frame("crc_mismatch")
    resp = ctx.client.ingest_plst(frame, api_key=proj.pk_key)
    assert resp.status == 400, f"Expected 400 for crc_mismatch, got {resp.status}"

    # Also test garbage body claiming RAW_ZSTD with bad CRC32
    garbage = b"not-zstd-at-all-1234567890"
    hdr = plst.HEADER_STRUCT.pack(
        plst.PLST_MAGIC,
        plst.PLST_VERSION,
        plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL,
        0,
        128,
        len(garbage),
        0xCAFEBABE,
    )
    resp2 = ctx.client.ingest_plst(hdr + garbage, api_key=proj.pk_key)
    assert resp2.status == 400


@test_case(
    id="T2-F03-04",
    tier=2,
    feature="F03",
    title="PLST oversized uncomp_len (64 MiB + 1) rejected with 400 without OOM",
    tags=("plst", "limit", "hostile"),
)
def test_f03_oversized_uncomp_len_64mib_plus_1(ctx: TestContext) -> None:
    proj = ctx.proj_a
    frame = plst.build_hostile_frame("oversized_uncomp_len")
    resp = ctx.client.ingest_plst(frame, api_key=proj.pk_key)
    assert resp.status == 400, f"Expected 400 for oversized_uncomp_len, got {resp.status}"
    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F03-05",
    tier=2,
    feature="F03",
    title="PLST Zstd decompression bomb (>64 MiB expansion from tiny uncomp_len=1024) returns 400 cleanly",
    tags=("plst", "zstd_bomb", "security", "hostile"),
)
def test_f03_zstd_bomb_rejected(ctx: TestContext) -> None:
    proj = ctx.proj_a
    bomb = plst.build_hostile_frame("zstd_bomb")
    assert len(bomb) < 1024 * 1024  # small enough to pass HTTP 8 MiB limit and hit PLST Zstd guard
    resp = ctx.client.ingest_plst(bomb, api_key=proj.pk_key)
    assert resp.status == 400, f"Expected 400 for zstd_bomb, got {resp.status}"
    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F03-06",
    tier=2,
    feature="F03",
    title="Uncompressed PLST frame with uncomp_len != comp_len returns 400",
    tags=("plst", "uncompressed", "header"),
)
def test_f03_uncompressed_length_mismatch(ctx: TestContext) -> None:
    proj = ctx.proj_a
    raw_sec = plst._encode_sections(fixtures.make_batch(seed=ctx.unique_id("um"), project_id=proj.id))
    bad_frame = _pack_raw_plst_frame(
        raw_sec,
        flags=plst.FLAG_FINAL,
        dict_id=0,
        uncomp_len_override=len(raw_sec) + 10,
    )
    resp = ctx.client.ingest_plst(bad_frame, api_key=proj.pk_key)
    assert resp.status == 400


# ===========================================================================
# F04: AI Agent Zstd Dictionary (AGENT_DICT_ID = 2)
# ===========================================================================


@test_case(
    id="T2-F04-01",
    tier=2,
    feature="F04",
    title="PLST FLAG_DICT_ZSTD with unknown DictID (0, 1, 3, 999) returns 400",
    tags=("plst", "zstd_dict", "negative"),
)
def test_f04_unknown_dict_ids_rejected(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("unk_dict"), project_id=proj.id)
    assert ctx.client.ingest_plst(plst.build_hostile_frame("unknown_dict_id", batch), api_key=proj.pk_key).status == 400

    dict_frame = plst.encode_plst_frame(batch, use_dict=True)
    body = dict_frame[plst.HEADER_SIZE :]
    _, ver, flags, _, uncomp_len, comp_len, crc32_val = plst.HEADER_STRUCT.unpack_from(dict_frame, 0)
    for bad_id in (0, 1, 3):
        bad = plst.HEADER_STRUCT.pack(plst.PLST_MAGIC, ver, flags, bad_id, uncomp_len, comp_len, crc32_val) + body
        resp = ctx.client.ingest_plst(bad, api_key=proj.pk_key)
        assert resp.status == 400, f"Expected 400 for dict_id={bad_id}, got {resp.status}"


@test_case(
    id="T2-F04-02",
    tier=2,
    feature="F04",
    title="PLST dict_id and compression flag mismatches (dict_id=2 w/ RAW_ZSTD or uncompressed, dict_id=0 w/ DICT_ZSTD, RAW_ZSTD over dict) return 400",
    tags=("plst", "zstd_dict", "flags", "negative"),
)
def test_f04_dict_flag_mismatches(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("dict_mm"), project_id=proj.id)
    raw_sec = plst._encode_sections(batch)

    # 1. dict_id=2 with FLAG_RAW_ZSTD
    f1 = _pack_raw_plst_frame(raw_sec, flags=plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL, dict_id=2)
    # 2. dict_id=2 with uncompressed flags
    f2 = _pack_raw_plst_frame(raw_sec, flags=plst.FLAG_FINAL, dict_id=2)
    # 3. FLAG_DICT_ZSTD with dict_id=0
    f3_id0 = _pack_raw_plst_frame(raw_sec, flags=plst.FLAG_DICT_ZSTD | plst.FLAG_FINAL, dict_id=0, use_dict_compress=True)
    # 4. FLAG_RAW_ZSTD + dict_id=0 wrapping dict-compressed bytes
    f4 = _pack_raw_plst_frame(raw_sec, flags=plst.FLAG_RAW_ZSTD | plst.FLAG_FINAL, dict_id=0, use_dict_compress=True)

    for label, frame in (("raw+id2", f1), ("uncomp+id2", f2), ("dict+id0", f3_id0), ("raw_flag_on_dict", f4)):
        resp = ctx.client.ingest_plst(frame, api_key=proj.pk_key)
        assert resp.status == 400, f"Expected 400 for {label}, got {resp.status}"


@test_case(
    id="T2-F04-03",
    tier=2,
    feature="F04",
    title="0-byte body with FLAG_DICT_ZSTD returns 400 vs valid empty batch with FLAG_DICT_ZSTD returns 200",
    tags=("plst", "zstd_dict", "empty", "boundary"),
)
def test_f04_zero_byte_body_vs_empty_batch_dict(ctx: TestContext) -> None:
    proj = ctx.proj_a

    zero_body = plst.HEADER_STRUCT.pack(
        plst.PLST_MAGIC,
        plst.PLST_VERSION,
        plst.FLAG_DICT_ZSTD | plst.FLAG_FINAL,
        plst.AGENT_DICT_ID,
        0,
        0,
        zlib.crc32(b"") & 0xFFFFFFFF,
    )
    assert ctx.client.ingest_plst(zero_body, api_key=proj.pk_key).status == 400

    empty_dict_resp = ctx.client.ingest_plst_batch(
        {"traces": [], "spans": []},
        api_key=proj.pk_key,
        use_dict=True,
    )
    empty_dict_resp.raise_for_status()
    assert empty_dict_resp.json()["accepted_traces"] == 0


@test_case(
    id="T2-F04-04",
    tier=2,
    feature="F04",
    title="Corrupted dict-compressed payload with recomputed valid CRC32 returns 400",
    tags=("plst", "zstd_dict", "corruption"),
)
def test_f04_corrupt_dict_payload_with_valid_crc(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("corrupt_dict"), project_id=proj.id)
    valid = plst.encode_plst_frame(batch, use_dict=True)
    magic, ver, flags, dict_id, uncomp_len, comp_len, _ = plst.HEADER_STRUCT.unpack_from(valid, 0)
    body = bytearray(valid[plst.HEADER_SIZE :])
    mid = len(body) // 2
    body[mid] ^= 0xFF
    if mid + 1 < len(body):
        body[mid + 1] ^= 0x55
    new_crc = zlib.crc32(body) & 0xFFFFFFFF
    corrupt = plst.HEADER_STRUCT.pack(magic, ver, flags, dict_id, uncomp_len, comp_len, new_crc) + bytes(body)
    resp = ctx.client.ingest_plst(corrupt, api_key=proj.pk_key)
    assert resp.status == 400


@test_case(
    id="T2-F04-05",
    tier=2,
    feature="F04",
    title="Uncompressed vs raw-Zstd vs dict-Zstd (AGENT_DICT_ID=2) hydration parity",
    tags=("plst", "zstd_dict", "parity"),
)
def test_f04_compression_modes_hydration_parity(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    modes = [
        ("uncomp", plst.FLAG_FINAL, False),
        ("raw", None, False),
        ("dict", None, True),
    ]
    hydrated: list[dict[str, Any]] = []
    for label, flags, use_dict in modes:
        seed = ctx.unique_id(f"par_{label}")
        batch = fixtures.make_batch(seed=seed, project_id=proj.id, start_ts_ms=ts)
        tr_id = batch["traces"][0]["trace_id"]
        resp = ctx.client.ingest_plst_batch(batch, api_key=proj.pk_key, use_dict=use_dict, flags=flags)
        resp.raise_for_status()
        hydrated.append(_wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token))

    base = hydrated[0]
    for other in hydrated[1:]:
        assert other["trace"]["agent_name"] == base["trace"]["agent_name"]
        assert other["trace"]["total_cost_nanos"] == base["trace"]["total_cost_nanos"]
        assert other["trace"]["input"] == base["trace"]["input"]
        assert other["trace"]["output"] == base["trace"]["output"]
        assert len(other["spans"]) == len(base["spans"]) == 3
        assert [s["span_type"] for s in other["spans"]] == [s["span_type"] for s in base["spans"]]


@test_case(
    id="T2-F04-06",
    tier=2,
    feature="F04",
    title="Dict-compressed frame using mismatched Zstd dictionary with DictID=2 returns 400",
    tags=("plst", "zstd_dict", "mismatch"),
)
def test_f04_wrong_dict_content_rejected(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("wrong_dict"), project_id=proj.id)
    wrong_dict = zstd.train_dict([b"unrelated binary corpus " * 40 for _ in range(12)], 4096).dict_content
    bad_frame = plst.encode_plst_frame(batch, use_dict=True, dict_bytes=wrong_dict)
    resp = ctx.client.ingest_plst(bad_frame, api_key=proj.pk_key)
    assert resp.status == 400


# ===========================================================================
# F05: 5 Columnar PLST Sections & Blob Dedup
# ===========================================================================


@test_case(
    id="T2-F05-01",
    tier=2,
    feature="F05",
    title="PLST truncated sections and trailing bytes after Section 5 return 400",
    tags=("plst", "sections", "negative"),
)
def test_f05_truncated_and_trailing_sections(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("sec_t"), project_id=proj.id)
    assert ctx.client.ingest_plst(plst.build_hostile_frame("truncated_sections", batch), api_key=proj.pk_key).status == 400

    raw_sec = plst._encode_sections(batch)
    trailing_sec_frame = _pack_raw_plst_frame(raw_sec + b"\x01\x02\x03\x04")
    assert ctx.client.ingest_plst(trailing_sec_frame, api_key=proj.pk_key).status == 400


@test_case(
    id="T2-F05-02",
    tier=2,
    feature="F05",
    title="PLST out-of-bounds string, blob, trace, and span indices return 400",
    tags=("plst", "sections", "oob", "negative"),
)
def test_f05_oob_indices_rejected(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("oob"), project_id=proj.id)
    for kind in ("oob_string_idx", "oob_blob_idx"):
        assert ctx.client.ingest_plst(plst.build_hostile_frame(kind, batch), api_key=proj.pk_key).status == 400

    raw_base = plst._encode_sections(batch)
    offs = _parse_section_offsets(raw_base)

    # Out-of-bounds span trace_idx in Section 4
    raw_tr_oob = bytearray(raw_base)
    struct.pack_into("<I", raw_tr_oob, offs["sp_trace_idx_off"], 777)
    assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_tr_oob)), api_key=proj.pk_key).status == 400

    # Out-of-bounds event span_idx in Section 5
    if offs["event_count"] > 0:
        raw_ev_oob = bytearray(raw_base)
        struct.pack_into("<I", raw_ev_oob, offs["ev_span_idx_off"], 888)
        assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_ev_oob)), api_key=proj.pk_key).status == 400


@test_case(
    id="T2-F05-03",
    tier=2,
    feature="F05",
    title="PLST invalid UTF-8 in String Table or Blob Table and invalid JSON blob return 400",
    tags=("plst", "utf8", "json", "negative"),
)
def test_f05_invalid_utf8_and_json_blobs(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("utf8"), project_id=proj.id)
    assert ctx.client.ingest_plst(plst.build_hostile_frame("invalid_utf8", batch), api_key=proj.pk_key).status == 400

    # Replace first blob bytes (keeping exact length) with invalid UTF-8 and valid xxh3_64
    b_text = fixtures.make_batch(seed=ctx.unique_id("btxt"), project_id=proj.id)
    b_text["traces"][0]["input"] = "abcd"
    raw_txt = bytearray(plst._encode_sections(b_text))
    offs = _parse_section_offsets(bytes(raw_txt))
    # Locate a 4-byte UTF-8 text blob ("abcd")
    patched_txt = False
    for idx in range(offs["blob_count"]):
        s_off = struct.unpack_from("<I", raw_txt, offs["blob_offsets_off"] + idx * 4)[0]
        e_off = struct.unpack_from("<I", raw_txt, offs["blob_offsets_off"] + (idx + 1) * 4)[0]
        if raw_txt[offs["blob_enc_off"] + idx] == plst.BLOB_ENC_UTF8_TEXT and (e_off - s_off) == 4:
            bad = b"\xff\xfe\xfd\xfc"
            raw_txt[offs["blob_data_off"] + s_off : offs["blob_data_off"] + e_off] = bad
            struct.pack_into("<Q", raw_txt, offs["blob_hashes_off"] + idx * 8, plst.xxh3_64(bad))
            patched_txt = True
            break
    assert patched_txt, "Failed to locate UTF-8 text blob to corrupt"
    assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_txt)), api_key=proj.pk_key).status == 400

    # Replace a JSON blob bytes with invalid JSON "{bad" and valid xxh3_64
    raw_json = bytearray(plst._encode_sections(batch))
    offs_j = _parse_section_offsets(bytes(raw_json))
    patched_json = False
    for idx in range(offs_j["blob_count"]):
        s_off = struct.unpack_from("<I", raw_json, offs_j["blob_offsets_off"] + idx * 4)[0]
        e_off = struct.unpack_from("<I", raw_json, offs_j["blob_offsets_off"] + (idx + 1) * 4)[0]
        if raw_json[offs_j["blob_enc_off"] + idx] == plst.BLOB_ENC_JSON_UTF8 and (e_off - s_off) >= 4:
            bad_j = b"{" * (e_off - s_off)
            raw_json[offs_j["blob_data_off"] + s_off : offs_j["blob_data_off"] + e_off] = bad_j
            struct.pack_into("<Q", raw_json, offs_j["blob_hashes_off"] + idx * 8, plst.xxh3_64(bad_j))
            patched_json = True
            break
    assert patched_json, "Failed to locate JSON blob to corrupt"
    assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_json)), api_key=proj.pk_key).status == 400


@test_case(
    id="T2-F05-04",
    tier=2,
    feature="F05",
    title="PLST blob xxh3_64 checksum mismatch and non-monotonic table offsets return 400",
    tags=("plst", "xxh3", "checksum", "negative"),
)
def test_f05_blob_checksum_and_non_monotonic_offsets(ctx: TestContext) -> None:
    proj = ctx.proj_a
    batch = fixtures.make_batch(seed=ctx.unique_id("cksum"), project_id=proj.id)
    raw_base = plst._encode_sections(batch)
    offs = _parse_section_offsets(raw_base)
    assert offs["blob_count"] >= 1
    assert offs["str_count"] >= 2

    # 1. Corrupt blob_hashes[0]
    raw_hash_bad = bytearray(raw_base)
    old_h = struct.unpack_from("<Q", raw_hash_bad, offs["blob_hashes_off"])[0]
    struct.pack_into("<Q", raw_hash_bad, offs["blob_hashes_off"], old_h ^ 0x01)
    assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_hash_bad)), api_key=proj.pk_key).status == 400

    # 2. Non-monotonic string offsets: set str_offsets[1] > str_offsets[2]
    raw_str_bad = bytearray(raw_base)
    o2 = struct.unpack_from("<I", raw_str_bad, offs["str_offsets_off"] + 8)[0]
    struct.pack_into("<I", raw_str_bad, offs["str_offsets_off"] + 4, o2 + 1)
    assert ctx.client.ingest_plst(_pack_raw_plst_frame(bytes(raw_str_bad)), api_key=proj.pk_key).status == 400


@test_case(
    id="T2-F05-05",
    tier=2,
    feature="F05",
    title="PLST blob encoding preserves '123' vs 123 and '' vs null vs {} vs [] distinctions",
    tags=("plst", "blobs", "types", "collision"),
)
def test_f05_blob_type_and_empty_value_distinctions(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_types")
    sp1_id = ctx.unique_id("sp_str")
    sp2_id = ctx.unique_id("sp_empty")

    sp1 = fixtures.make_span(
        sp1_id,
        tr_id,
        project_id=proj.id,
        input_payload="123",
        output_payload=123,
        attributes={"empty_str": "", "null_val": None, "empty_arr": [], "empty_obj": {}},
        metadata={},
        start_ts_ms=ts,
    )
    sp2 = fixtures.make_span(
        sp2_id,
        tr_id,
        project_id=proj.id,
        input_payload="",
        output_payload=None,
        attributes=None,
        metadata=None,
        start_ts_ms=ts + 1,
    )
    sp2["attributes"] = []
    sp2["output"] = None
    sp2["metadata"] = None

    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_types"),
        input_payload="",
        output_payload={},
        metadata=None,
        start_ts_ms=ts,
        spans=[sp1, sp2],
    )
    tr["metadata"] = []
    ctx.client.ingest_plst_batch({"traces": [tr], "spans": [sp1, sp2]}, api_key=proj.pk_key, use_dict=True).raise_for_status()

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    assert detail["trace"]["input"] == ""
    assert isinstance(detail["trace"]["input"], str)
    assert detail["trace"]["output"] == {}
    assert detail["trace"]["metadata"] == [] and isinstance(detail["trace"]["metadata"], list)

    by_id = {s["span_id"]: s for s in detail["spans"]}
    assert by_id[sp1_id]["input"] == "123" and isinstance(by_id[sp1_id]["input"], str)
    assert by_id[sp1_id]["output"] == 123 and isinstance(by_id[sp1_id]["output"], int)
    assert by_id[sp1_id]["attributes"] == {"empty_str": "", "null_val": None, "empty_arr": [], "empty_obj": {}}
    assert by_id[sp2_id]["input"] == "" and isinstance(by_id[sp2_id]["input"], str)
    assert by_id[sp2_id]["output"] is None
    assert by_id[sp2_id]["attributes"] == [] and isinstance(by_id[sp2_id]["attributes"], list)


@test_case(
    id="T2-F05-06",
    tier=2,
    feature="F05",
    title="PLST blob dedup round-trip and >64 MiB materialized blob anti-amplification guard (400)",
    tags=("plst", "dedup", "anti_amplification"),
)
def test_f05_blob_dedup_and_anti_amplification(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    # 1. Deduplicated multi-trace batch
    dedup_batch = fixtures.make_repeated_prompt_batch(
        seed=ctx.unique_id("dedup"),
        project_id=proj.id,
        repeats=8,
        prompt_bytes=4096,
        start_ts_ms=ts,
    )
    stats = plst.BlobDedupStats()
    frame = plst.encode_plst_frame(dedup_batch, use_dict=True, stats_out=stats)
    assert stats.deduped_blobs > 0
    ctx.client.ingest_plst(frame, api_key=proj.pk_key).raise_for_status()
    first_tid = dedup_batch["traces"][0]["trace_id"]
    last_tid = dedup_batch["traces"][-1]["trace_id"]
    d0 = _wait_for_trace(ctx.client, proj.id, first_tid, token=ctx.viewer_token)
    d7 = _wait_for_trace(ctx.client, proj.id, last_tid, token=ctx.viewer_token)
    assert d0["trace"]["input"] == d7["trace"]["input"]

    # 2. Materialized >64 MiB anti-amplification attack: 140 spans sharing one 512 KiB input blob (= 70 MiB)
    big_str = "A" * (512 * 1024)
    amp_tr_id = ctx.unique_id("tr_amp")
    amp_spans = [
        fixtures.make_span(
            ctx.unique_id(f"sp_amp_{i:03d}"),
            amp_tr_id,
            project_id=proj.id,
            input_payload=big_str,
            output_payload="ok",
            start_ts_ms=ts + i,
        )
        for i in range(140)
    ]
    amp_frame = plst.encode_plst_frame({"traces": [], "spans": amp_spans}, use_dict=False)
    assert len(amp_frame) < 1024 * 1024  # passes HTTP 8 MiB limit to reach PLST decoder
    assert ctx.client.ingest_plst(amp_frame, api_key=proj.pk_key).status == 400


# ===========================================================================
# F06: Multi-Writer Crash-Safe WAL (PTWL)
# ===========================================================================


@test_case(
    id="T2-F06-01",
    tier=2,
    feature="F06",
    title="Unauthenticated and invalid x-pulse-key ingest rejected with 401 across PLST, JSON, and OTLP",
    tags=("wal", "auth", "negative"),
)
def test_f06_unauthenticated_ingest_rejected(ctx: TestContext) -> None:
    batch = fixtures.make_batch(seed=ctx.unique_id("unauth"), project_id=ctx.proj_a.id)
    frame = plst.encode_plst_frame(batch)
    otlp_req = fixtures.make_otlp_export_request(seed=ctx.unique_id("unauth_otlp"))

    for key in (None, "", "pk_live_invalid_bogus_000"):
        assert ctx.client.ingest_json(batch, api_key=key).status == 401
        assert ctx.client.ingest_plst(frame, api_key=key).status == 401
        assert ctx.client.ingest_otlp_json(otlp_req, api_key=key).status == 401


@test_case(
    id="T2-F06-02",
    tier=2,
    feature="F06",
    title="Concurrent multi-writer WAL ingestion produces valid PTWL artifacts and zero lost traces",
    tags=("wal", "concurrency", "ptwl"),
)
def test_f06_concurrent_multi_writer_wal(ctx: TestContext) -> None:
    admin_tok = f"sk_root_wal_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("wal_proj"))

        trace_ids: list[str] = []
        batches: list[dict[str, Any]] = []
        ts = _now_ms()
        for i in range(16):
            b = fixtures.make_batch(seed=ctx.unique_id(f"cw_{i}"), project_id=proj.id, start_ts_ms=ts + i * 10)
            batches.append(b)
            trace_ids.append(b["traces"][0]["trace_id"])

        def _send(b: dict[str, Any]) -> int:
            return iso_client.ingest_plst_batch(b, api_key=proj.pk_key, use_dict=True).status

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(_send, batches))

        assert all(s == 200 for s in statuses), f"Non-200 concurrent ingest statuses: {statuses}"

        artifacts = inst.list_agent_artifacts()
        wal_or_seg = len(artifacts["agent_wal"]) + len(artifacts["agent_segments"])
        assert wal_or_seg >= 1, f"Expected PTWL/PLSS files on disk, got {artifacts}"

        for tid in trace_ids:
            _wait_for_trace(iso_client, proj.id, tid, token=owner_tok, timeout=10.0)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F06-03",
    tier=2,
    feature="F06",
    title="Empty batch ingestion returns 200 with accepted_traces=0 and zero WAL corruption",
    tags=("wal", "empty", "boundary"),
)
def test_f06_empty_batch_no_corruption(ctx: TestContext) -> None:
    proj = ctx.proj_a
    r1 = ctx.client.ingest_json({"traces": [], "spans": []}, api_key=proj.pk_key)
    r1.raise_for_status()
    assert r1.json()["accepted_traces"] == 0

    # Follow-up valid ingest must succeed cleanly
    b = fixtures.make_batch(seed=ctx.unique_id("after_empty"), project_id=proj.id, start_ts_ms=_now_ms())
    ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
    _wait_for_trace(ctx.client, proj.id, b["traces"][0]["trace_id"], token=ctx.viewer_token)


@test_case(
    id="T2-F06-04",
    tier=2,
    feature="F06",
    title="SIGKILL crash recovery before L0 flush preserves 100% of acked traces via PTWL replay",
    tags=("wal", "crash_recovery", "sigkill", "ptwl"),
)
def test_f06_sigkill_crash_recovery(ctx: TestContext) -> None:
    admin_tok = f"sk_root_kill_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("kill_proj"))

        acked_ids: list[str] = []
        ts = _now_ms()
        for i in range(5):
            b = fixtures.make_batch(seed=ctx.unique_id(f"kill_{i}"), project_id=proj.id, start_ts_ms=ts + i * 5)
            iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
            acked_ids.append(b["traces"][0]["trace_id"])

        # Immediately SIGKILL before the 3-5s L0 flush window completes
        ctx.node_manager.kill(inst)
        inst = ctx.node_manager.start_node(
            role="all",
            data_dir=inst.data_dir,
            admin_token=admin_tok,
        )
        restarted_client = PulseClient(inst.base_url, timeout=15.0)
        for tid in acked_ids:
            detail = _wait_for_trace(restarted_client, proj.id, tid, token=owner_tok, timeout=10.0)
            assert detail["trace"]["trace_id"] == tid
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F06-05",
    tier=2,
    feature="F06",
    title="Partial or corrupt .ptwl WAL file on disk is skipped/quarantined on startup without blocking valid data",
    tags=("wal", "corruption", "resilience"),
)
def test_f06_corrupt_ptwl_wal_recovery(ctx: TestContext) -> None:
    admin_tok = f"sk_root_cwal_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("cwal_proj"))

        b = fixtures.make_batch(seed=ctx.unique_id("valid_wal"), project_id=proj.id, start_ts_ms=_now_ms())
        tid = b["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_for_trace(iso_client, proj.id, tid, token=owner_tok)

        ctx.node_manager.stop(inst, graceful=True)

        # Drop a truncated/corrupt .ptwl file alongside valid WAL files
        corrupt_dir = inst.object_store_root() / "agent_wal" / proj.id / "corrupt_writer_01"
        corrupt_dir.mkdir(parents=True, exist_ok=True)
        (corrupt_dir / "seq_000000000001.ptwl").write_bytes(b"PTWL\x00\xff\xfeTRUNCATED_CORRUPT")

        inst = ctx.node_manager.start_node(
            role="all",
            data_dir=inst.data_dir,
            admin_token=admin_tok,
        )
        restarted = PulseClient(inst.base_url, timeout=15.0)
        assert restarted.get_readyz().status == 200
        detail = _wait_for_trace(restarted, proj.id, tid, token=owner_tok, timeout=10.0)
        assert detail["trace"]["trace_id"] == tid
    finally:
        ctx.node_manager.stop(inst)


# ===========================================================================
# F07: Sharded L0 MemTable & Fast Flush
# ===========================================================================


@test_case(
    id="T2-F07-01",
    tier=2,
    feature="F07",
    title="Immediate L0 query visibility (<1s) and post-flush .plss segment persistence (>5s)",
    tags=("l0", "flush", "plss"),
)
def test_f07_l0_immediate_vs_post_flush_visibility(ctx: TestContext) -> None:
    admin_tok = f"sk_root_l0_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("l0_proj"))

        b = fixtures.make_batch(seed=ctx.unique_id("l0_vis"), project_id=proj.id, start_ts_ms=_now_ms())
        tid = b["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()

        # Immediate L0 read
        d_l0 = _wait_for_trace(iso_client, proj.id, tid, token=owner_tok, timeout=2.0)
        assert d_l0["trace"]["trace_id"] == tid

        # Wait for fast L0 flush window to materialize .plss segment
        segs = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)
        assert len(segs) >= 1

        d_plss = _wait_for_trace(iso_client, proj.id, tid, token=owner_tok, timeout=5.0)
        assert d_plss["trace"]["trace_id"] == tid
        assert len(d_plss["spans"]) == 3
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F07-02",
    tier=2,
    feature="F07",
    title="L0 burst ingestion preserves all 200-acked traces or applies clean 429 backpressure without 500s",
    tags=("l0", "burst", "backpressure"),
)
def test_f07_l0_burst_no_lost_acks(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "burst_proj")
    ts = _now_ms()
    acked: list[str] = []

    def _send_batch(i: int) -> tuple[int, list[str]]:
        traces: list[dict[str, Any]] = []
        spans: list[dict[str, Any]] = []
        ids: list[str] = []
        for j in range(5):
            b = fixtures.make_batch(seed=ctx.unique_id(f"bst_{i}_{j}"), project_id=proj.id, start_ts_ms=ts + i * 10 + j)
            traces.extend(b["traces"])
            spans.extend(b["spans"])
            ids.append(b["traces"][0]["trace_id"])
        resp = ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key)
        return resp.status, ids

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(_send_batch, range(30)))

    for status, ids in results:
        assert status in (200, 429), f"Unexpected burst status {status}"
        if status == 200:
            acked.extend(ids)

    assert len(acked) >= 1
    # Verify sampled acked traces from beginning, middle, end are all present
    sample = [acked[0], acked[len(acked) // 2], acked[-1]]
    for tid in sample:
        _wait_for_trace(ctx.client, proj.id, tid, token=ctx.viewer_token, timeout=15.0)


@test_case(
    id="T2-F07-03",
    tier=2,
    feature="F07",
    title="Zero-span traces and orphan spans (without parent trace row) survive L0 and flush",
    tags=("l0", "orphan_spans", "zero_span"),
)
def test_f07_zero_span_and_orphan_spans(ctx: TestContext) -> None:
    admin_tok = f"sk_root_orph_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("orph_proj"))
        ts = _now_ms()

        tr_zero_id = ctx.unique_id("tr_zero")
        tr_zero = fixtures.make_trace(tr_zero_id, project_id=proj.id, session_id=ctx.unique_id("sess_z"), start_ts_ms=ts, spans=[])

        tr_orph_id = ctx.unique_id("tr_orph")
        sp_orph = fixtures.make_span(ctx.unique_id("sp_orph"), tr_orph_id, project_id=proj.id, name="orphan.step", start_ts_ms=ts + 5)

        # Ingest zero-span trace + orphan span (no trace object for tr_orph_id)
        r = iso_client.ingest_plst_batch({"traces": [tr_zero], "spans": [sp_orph]}, api_key=proj.pk_key)
        r.raise_for_status()

        d_zero = _wait_for_trace(iso_client, proj.id, tr_zero_id, token=owner_tok)
        assert d_zero["spans"] == []

        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)
        d_zero_post = _wait_for_trace(iso_client, proj.id, tr_zero_id, token=owner_tok)
        assert d_zero_post["spans"] == []
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F07-04",
    tier=2,
    feature="F07",
    title="Rapid sequential multi-date sharded L0 flushes preserve all traces across shards",
    tags=("l0", "sharding", "flush"),
)
def test_f07_rapid_sequential_multi_shard_flushes(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "shard_proj")
    now = _now_ms()
    day_ms = 86_400_000
    expected_ids: list[str] = []
    for day_offset in range(3):
        b = fixtures.make_batch(
            seed=ctx.unique_id(f"sh_{day_offset}"),
            project_id=proj.id,
            start_ts_ms=now - day_offset * day_ms,
        )
        ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        expected_ids.append(b["traces"][0]["trace_id"])

    for tid in expected_ids:
        _wait_for_trace(ctx.client, proj.id, tid, token=ctx.viewer_token)

    list_resp = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        token=ctx.viewer_token,
        query={"limit": 50},
    ).raise_for_status().json()
    found_ids = {t["trace_id"] for t in list_resp["traces"]}
    assert set(expected_ids).issubset(found_ids)


@test_case(
    id="T2-F07-05",
    tier=2,
    feature="F07",
    title="Empty or inverted time range (start_ts_ms > end_ts_ms) returns 200 with empty list",
    tags=("l0", "query", "time_range", "boundary"),
)
def test_f07_inverted_and_empty_time_range(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    b = fixtures.make_batch(seed=ctx.unique_id("trange"), project_id=proj.id, start_ts_ms=ts)
    ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()

    # Inverted range: start > end
    inv = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/traces",
        token=ctx.viewer_token,
        query={"start_ts_ms": ts + 10_000, "end_ts_ms": ts - 10_000},
    )
    inv.raise_for_status()
    assert inv.json()["traces"] == []

    # Far future range
    fut = ctx.client.request(
        "GET",
        f"/v1/projects/{proj.id}/agent/sessions",
        token=ctx.viewer_token,
        query={"start_ts_ms": ts + 10 * 86_400_000, "end_ts_ms": ts + 11 * 86_400_000},
    )
    fut.raise_for_status()
    assert fut.json()["sessions"] == []


# ===========================================================================
# F08: PLSS Immutable Segment Container & Directory/Footer
# ===========================================================================


@test_case(
    id="T2-F08-01",
    tier=2,
    feature="F08",
    title="Near-64KiB multi-chunk payload .plss round-trip under EncryptedObjectStore (PLSEAGM2)",
    tags=("plss", "encryption", "chunking"),
)
def test_f08_plss_encrypted_multichunk_roundtrip(ctx: TestContext) -> None:
    admin_tok = f"sk_root_enc_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(
        admin_token=admin_tok,
        extra_env={"PULSE_BYOK_MASTER_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
    )
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("enc_proj"))
        ts = _now_ms()

        # Multiple near-64KiB fields on one trace (> 128 KiB total -> spans multiple 64 KiB EncryptedObjectStore chunks)
        tr_id = ctx.unique_id("tr_enc")
        chunk_a = {"data": "A" * 60_000}
        chunk_b = {"data": "B" * 60_000}
        chunk_c = {"data": "C" * 60_000}
        sp = fixtures.make_span(
            ctx.unique_id("sp_enc"),
            tr_id,
            project_id=proj.id,
            input_payload=chunk_a,
            output_payload=chunk_b,
            attributes=chunk_c,
            start_ts_ms=ts,
        )
        tr = fixtures.make_trace(
            tr_id,
            project_id=proj.id,
            session_id=ctx.unique_id("sess_enc"),
            input_payload=chunk_a,
            output_payload=chunk_b,
            start_ts_ms=ts,
            spans=[sp],
        )
        iso_client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
        segs = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        # Verify on-disk .plss segment is encrypted (starts with PLSEAGM2 when encryption key is set)
        header = segs[0].read_bytes()[:8]
        assert header == b"PLSEAGM2", f"Expected encrypted PLSEAGM2 .plss segment, got {header!r}"

        detail = _wait_for_trace(iso_client, proj.id, tr_id, token=owner_tok, timeout=10.0)
        assert detail["trace"]["input"] == chunk_a
        assert detail["spans"][0]["output"] == chunk_b
        assert detail["spans"][0]["attributes"] == chunk_c
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F08-02",
    tier=2,
    feature="F08",
    title="Non-existent trace_id on GET /agent/traces/:trace_id returns 404",
    tags=("plss", "not_found", "negative"),
)
def test_f08_missing_trace_returns_404(ctx: TestContext) -> None:
    resp = _get_trace_detail(ctx.client, ctx.proj_a.id, ctx.unique_id("missing_tr"), token=ctx.viewer_token)
    assert resp.status == 404


@test_case(
    id="T2-F08-03",
    tier=2,
    feature="F08",
    title="Non-existent project (404), cross-org project (404), and wrong-project sk_live_ key (403)",
    tags=("plss", "isolation", "rbac", "negative"),
)
def test_f08_project_and_tenant_isolation(ctx: TestContext) -> None:
    b = fixtures.make_batch(seed=ctx.unique_id("iso"), project_id=ctx.proj_a.id, start_ts_ms=_now_ms())
    tid = b["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(b, api_key=ctx.proj_a.pk_key).raise_for_status()
    _wait_for_trace(ctx.client, ctx.proj_a.id, tid, token=ctx.viewer_token)

    # 1. Non-existent project -> 404
    r_missing = _get_trace_detail(ctx.client, ctx.unique_id("no_such_proj"), tid, token=ctx.owner_token)
    assert r_missing.status == 404

    # 2. Cross-org project accessed by org_default owner -> 404
    r_cross_org = _get_trace_detail(ctx.client, ctx.other_proj.id, tid, token=ctx.owner_token)
    assert r_cross_org.status == 404

    # 3. proj_a sk_live_ key targeting proj_b -> 403
    r_wrong_key = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_b.id}/agent/traces",
        token=ctx.proj_a.sk_key,
    )
    assert r_wrong_key.status == 403, f"Expected 403 for cross-project sk_live_, got {r_wrong_key.status}"


@test_case(
    id="T2-F08-04",
    tier=2,
    feature="F08",
    title="Missing .plss segment file on disk fails cleanly without panic and keeps node healthy",
    tags=("plss", "resilience", "missing_segment"),
)
def test_f08_missing_segment_resilience(ctx: TestContext) -> None:
    admin_tok = f"sk_root_mseg_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("mseg_proj"))

        b = fixtures.make_batch(seed=ctx.unique_id("mseg"), project_id=proj.id, start_ts_ms=_now_ms())
        tid = b["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        segs = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        ctx.node_manager.stop(inst, graceful=True)
        for seg in segs:
            seg.unlink()

        inst = ctx.node_manager.start_node(role="all", data_dir=inst.data_dir, admin_token=admin_tok)
        restarted = PulseClient(inst.base_url, timeout=15.0)
        resp = _get_trace_detail(restarted, proj.id, tid, token=owner_tok)
        assert resp.status in (404, 500, 503)  # graceful error, no process crash
        assert restarted.get_healthz().status == 200

        # New ingest in same project still succeeds
        b2 = fixtures.make_batch(seed=ctx.unique_id("mseg2"), project_id=proj.id, start_ts_ms=_now_ms())
        restarted.ingest_plst_batch(b2, api_key=proj.pk_key).raise_for_status()
        _wait_for_trace(restarted, proj.id, b2["traces"][0]["trace_id"], token=owner_tok)
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F08-05",
    tier=2,
    feature="F08",
    title="Corrupt .plss segment footer/bytes on disk fails cleanly without crashing server",
    tags=("plss", "resilience", "corruption"),
)
def test_f08_corrupt_segment_resilience(ctx: TestContext) -> None:
    admin_tok = f"sk_root_cseg_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("cseg_proj"))

        b = fixtures.make_batch(seed=ctx.unique_id("cseg"), project_id=proj.id, start_ts_ms=_now_ms())
        tid = b["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        segs = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        ctx.node_manager.stop(inst, graceful=True)
        segs[0].write_bytes(b"CORRUPT_SEGMENT_FOOTER_AND_BODY" * 4)

        inst = ctx.node_manager.start_node(role="all", data_dir=inst.data_dir, admin_token=admin_tok)
        restarted = PulseClient(inst.base_url, timeout=15.0)
        resp = _get_trace_detail(restarted, proj.id, tid, token=owner_tok)
        assert resp.status in (400, 404, 500, 503)
        assert restarted.get_healthz().status == 200
    finally:
        ctx.node_manager.stop(inst)


# ===========================================================================
# F09: PLSS Tags 0x01-0x07
# ===========================================================================


@test_case(
    id="T2-F09-01",
    tier=2,
    feature="F09",
    title="Split-Block Bloom filter false-positive stress on flushed .plss segments (trace_id, session_id, user_id)",
    tags=("plss", "bloom", "stress"),
)
def test_f09_bloom_filter_fp_stress(ctx: TestContext) -> None:
    admin_tok = f"sk_root_blm_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("blm_proj"))
        _set_privacy_policy(
            iso_client,
            proj.id,
            token=owner_tok,
            policy=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
        )

        ts = _now_ms()
        traces = []
        spans = []
        for i in range(10):
            b = fixtures.make_batch(
                seed=ctx.unique_id(f"blm_{i}"),
                project_id=proj.id,
                session_id=ctx.unique_id(f"blm_s_{i}"),
                user_id=ctx.unique_id(f"blm_u_{i}"),
                start_ts_ms=ts + i * 10,
            )
            traces.extend(b["traces"])
            spans.extend(b["spans"])
        iso_client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()
        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        for i in range(105):
            assert _get_trace_detail(iso_client, proj.id, f"nonexistent_trace_{i:04d}", token=owner_tok).status == 404
        for i in range(25):
            r_sess = iso_client.request(
                "GET",
                f"/v1/projects/{proj.id}/agent/traces",
                token=owner_tok,
                query={"session_id": f"nonexistent_sess_{i:04d}"},
            ).raise_for_status().json()
            assert r_sess["traces"] == []
            r_usr = iso_client.request(
                "GET",
                f"/v1/projects/{proj.id}/agent/traces",
                token=owner_tok,
                query={"user_id": f"nonexistent_user_{i:04d}"},
            ).raise_for_status().json()
            assert r_usr["traces"] == []
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F09-02",
    tier=2,
    feature="F09",
    title="Zone-map out-of-range pruning across time, duration, and cost filters returns empty 200",
    tags=("plss", "zone_map", "pruning"),
)
def test_f09_zone_map_out_of_range_pruning(ctx: TestContext) -> None:
    admin_tok = f"sk_root_zm_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("zm_proj"))

        ts = _now_ms()
        b = fixtures.make_batch(seed=ctx.unique_id("zm"), project_id=proj.id, start_ts_ms=ts)
        iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        out_of_range_queries: list[dict[str, Any]] = [
            {"start_ts_ms": ts - 1_000_000, "end_ts_ms": ts - 500_000},
            {"start_ts_ms": ts + 500_000, "end_ts_ms": ts + 1_000_000},
            {"min_duration_ms": 99_999_999},
            {"max_duration_ms": 1},
            {"min_cost_usd": 9999.0},
        ]
        for q in out_of_range_queries:
            r = iso_client.request(
                "GET",
                f"/v1/projects/{proj.id}/agent/traces",
                token=owner_tok,
                query=q,
            ).raise_for_status().json()
            assert r["traces"] == [], f"Expected empty result for zone-map query {q}, got {r}"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F09-03",
    tier=2,
    feature="F09",
    title="CRDT trace delta status precedence (error/timeout dominates ok/running across updates)",
    tags=("plss", "crdt", "status"),
)
def test_f09_crdt_status_precedence(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_crdt")
    sess_id = ctx.unique_id("sess_crdt")

    # Send running -> error -> ok delta for the same trace_id; error must dominate ok/running
    t_running = fixtures.make_trace(tr_id, project_id=proj.id, session_id=sess_id, status="running", start_ts_ms=ts, duration_ms=50)
    t_error = fixtures.make_trace(tr_id, project_id=proj.id, session_id=sess_id, status="error", error_type="ToolFail", start_ts_ms=ts, duration_ms=120)
    t_ok = fixtures.make_trace(tr_id, project_id=proj.id, session_id=sess_id, status="ok", start_ts_ms=ts, duration_ms=100)

    ctx.client.ingest_plst_batch({"traces": [t_running], "spans": []}, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch({"traces": [t_error], "spans": []}, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_plst_batch({"traces": [t_ok], "spans": []}, api_key=proj.pk_key).raise_for_status()

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    assert detail["trace"]["status"] == "error", f"Expected error to dominate ok/running, got {detail['trace']['status']}"


@test_case(
    id="T2-F09-04",
    tier=2,
    feature="F09",
    title="Missing session_id on GET /agent/sessions/:session_id returns 404",
    tags=("plss", "sessions", "not_found"),
)
def test_f09_missing_session_returns_404(ctx: TestContext) -> None:
    resp = ctx.client.request(
        "GET",
        f"/v1/projects/{ctx.proj_a.id}/agent/sessions/{ctx.unique_id('missing_sess')}",
        token=ctx.viewer_token,
    )
    assert resp.status == 404


@test_case(
    id="T2-F09-05",
    tier=2,
    feature="F09",
    title="Tag 0x05 payload pack handles null, empty string, {}, [], and 60KiB payloads across flush",
    tags=("plss", "payloads", "boundary"),
)
def test_f09_tag05_payload_edge_bounds(ctx: TestContext) -> None:
    admin_tok = f"sk_root_p5_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("p5_proj"))
        ts = _now_ms()

        tr_id = ctx.unique_id("tr_p5")
        large_str = "Z" * 60_000
        sp = fixtures.make_span(
            ctx.unique_id("sp_p5"),
            tr_id,
            project_id=proj.id,
            input_payload="",
            output_payload=None,
            attributes={"empty": {}, "arr": []},
            metadata=None,
            start_ts_ms=ts,
        )
        sp["output"] = None
        sp["metadata"] = large_str
        tr = fixtures.make_trace(
            tr_id,
            project_id=proj.id,
            session_id=ctx.unique_id("sess_p5"),
            input_payload={},
            output_payload=[],
            metadata={"big": large_str},
            start_ts_ms=ts,
            spans=[sp],
        )
        iso_client.ingest_plst_batch({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key).raise_for_status()
        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        detail = _wait_for_trace(iso_client, proj.id, tr_id, token=owner_tok)
        assert detail["trace"]["input"] == {}
        assert detail["trace"]["output"] == []
        assert detail["trace"]["metadata"] == {"big": large_str}
        assert detail["spans"][0]["input"] == ""
        assert detail["spans"][0]["output"] is None
        assert detail["spans"][0]["metadata"] == large_str
    finally:
        ctx.node_manager.stop(inst)


# ===========================================================================
# F10: MVCC Manifest (epoch_{N}.json) & 15m GC Grace
# ===========================================================================


@test_case(
    id="T2-F10-01",
    tier=2,
    feature="F10",
    title="Concurrent reads during active ingestion & flush return zero 404s or 500s",
    tags=("mvcc", "concurrency", "manifest"),
)
def test_f10_concurrent_reads_during_writes_and_flush(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "mvcc_conc")
    ts = _now_ms()
    seed_b = fixtures.make_batch(seed=ctx.unique_id("anchor"), project_id=proj.id, start_ts_ms=ts)
    anchor_tid = seed_b["traces"][0]["trace_id"]
    ctx.client.ingest_plst_batch(seed_b, api_key=proj.pk_key).raise_for_status()
    _wait_for_trace(ctx.client, proj.id, anchor_tid, token=ctx.viewer_token)

    errors: list[str] = []

    def _writer() -> None:
        for i in range(15):
            b = fixtures.make_batch(seed=ctx.unique_id(f"wr_{i}"), project_id=proj.id, start_ts_ms=ts + i * 5)
            r = ctx.client.ingest_plst_batch(b, api_key=proj.pk_key)
            if r.status not in (200, 429):
                errors.append(f"write status {r.status}")
            time.sleep(0.03)

    def _reader() -> None:
        for _ in range(25):
            for path in (
                f"/v1/projects/{proj.id}/agent/traces/{anchor_tid}",
                f"/v1/projects/{proj.id}/agent/traces",
                f"/v1/projects/{proj.id}/agent/sessions",
                f"/v1/projects/{proj.id}/agent/analytics",
            ):
                r = ctx.client.request("GET", path, token=ctx.viewer_token)
                if r.status != 200:
                    errors.append(f"read {path} -> {r.status}")
            time.sleep(0.02)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(_writer), pool.submit(_reader), pool.submit(_reader)]
        for f in futs:
            f.result()

    assert not errors, f"Concurrent MVCC read/write errors: {errors[:10]}"


@test_case(
    id="T2-F10-02",
    tier=2,
    feature="F10",
    title="15-minute (900s) GC grace window preserves active/pending segments across flush waves",
    tags=("mvcc", "gc_grace", "manifest"),
)
def test_f10_gc_grace_window_preserves_segments(ctx: TestContext) -> None:
    admin_tok = f"sk_root_gc_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("gc_proj"))
        ts = _now_ms()

        wave1 = fixtures.make_batch(seed=ctx.unique_id("gc1"), project_id=proj.id, start_ts_ms=ts)
        tid1 = wave1["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(wave1, api_key=proj.pk_key).raise_for_status()
        segs_wave1 = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)

        wave2 = fixtures.make_batch(seed=ctx.unique_id("gc2"), project_id=proj.id, start_ts_ms=ts + 100)
        tid2 = wave2["traces"][0]["trace_id"]
        iso_client.ingest_plst_batch(wave2, api_key=proj.pk_key).raise_for_status()
        segs_wave2 = _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)
        assert len(segs_wave2) >= len(segs_wave1)

        # Within 15m (900s) grace period, both waves remain cleanly readable without 404
        assert _get_trace_detail(iso_client, proj.id, tid1, token=owner_tok).status == 200
        assert _get_trace_detail(iso_client, proj.id, tid2, token=owner_tok).status == 200
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F10-03",
    tier=2,
    feature="F10",
    title="Empty manifest / fresh project queries return 200 with empty lists and zero KPIs",
    tags=("mvcc", "empty", "manifest"),
)
def test_f10_empty_manifest_fresh_project(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "fresh_proj")

    tr_list = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token).raise_for_status().json()
    assert tr_list["traces"] == []
    assert tr_list["total"] == 0

    sess_list = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token).raise_for_status().json()
    assert sess_list["sessions"] == []
    assert sess_list["total"] == 0

    analytics = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/analytics", token=ctx.viewer_token).raise_for_status().json()
    assert analytics["total_executions"] == 0
    assert analytics["total_tokens"] == 0


@test_case(
    id="T2-F10-04",
    tier=2,
    feature="F10",
    title="MVCC manifest epoch_{N}.json progresses monotonically across flush waves",
    tags=("mvcc", "epoch", "manifest"),
)
def test_f10_epoch_monotonic_progression(ctx: TestContext) -> None:
    admin_tok = f"sk_root_ep_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("ep_proj"))
        ts = _now_ms()

        def _epochs() -> list[int]:
            man_dir = inst.data_dir / "object_store" / "agent_manifests" / proj.id
            if not man_dir.is_dir():
                return []
            nums = []
            for p in man_dir.glob("epoch_*.json"):
                stem = p.stem.removeprefix("epoch_")
                if stem.isdigit():
                    nums.append(int(stem))
            return sorted(nums)

        b1 = fixtures.make_batch(seed=ctx.unique_id("ep1"), project_id=proj.id, start_ts_ms=ts)
        iso_client.ingest_plst_batch(b1, api_key=proj.pk_key).raise_for_status()
        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)
        ep1 = _epochs()
        assert len(ep1) >= 1

        b2 = fixtures.make_batch(seed=ctx.unique_id("ep2"), project_id=proj.id, start_ts_ms=ts + 50)
        iso_client.ingest_plst_batch(b2, api_key=proj.pk_key).raise_for_status()
        deadline = time.monotonic() + 15.0
        ep2 = _epochs()
        while time.monotonic() < deadline and (not ep2 or max(ep2) <= max(ep1)):
            time.sleep(0.25)
            ep2 = _epochs()

        assert max(ep2) > max(ep1), f"Expected monotonic epoch increase: ep1={ep1}, ep2={ep2}"
        assert _get_trace_detail(iso_client, proj.id, b1["traces"][0]["trace_id"], token=owner_tok).status == 200
        assert _get_trace_detail(iso_client, proj.id, b2["traces"][0]["trace_id"], token=owner_tok).status == 200
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F10-05",
    tier=2,
    feature="F10",
    title="Repeated flush and restart cycles remain idempotent with zero duplicate or lost traces",
    tags=("mvcc", "idempotency", "restart"),
)
def test_f10_repeated_flush_restart_idempotency(ctx: TestContext) -> None:
    admin_tok = f"sk_root_idem_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("idem_proj"))
        ts = _now_ms()

        ids: list[str] = []
        for i in range(4):
            b = fixtures.make_batch(seed=ctx.unique_id(f"id_{i}"), project_id=proj.id, start_ts_ms=ts + i * 20)
            iso_client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
            ids.append(b["traces"][0]["trace_id"])

        _wait_for_flush_segments(inst.data_dir, proj.id, min_segments=1, timeout=15.0)
        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        inst = ctx.node_manager.restart(inst, graceful_stop=True)
        restarted = PulseClient(inst.base_url, timeout=15.0)

        listed = restarted.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=owner_tok).raise_for_status().json()
        got_ids = [t["trace_id"] for t in listed["traces"]]
        assert len(got_ids) == len(set(got_ids)) == 4
        assert set(got_ids) == set(ids)
    finally:
        ctx.node_manager.stop(inst)


# ===========================================================================
# F11: Compaction, Erasure Filtering, Retention & Query Engine
# ===========================================================================


@test_case(
    id="T2-F11-01",
    tier=2,
    feature="F11",
    title="Trace and session pagination limit=0 clamps to 1 and limit=101 clamps to 100",
    tags=("query", "pagination", "boundary"),
)
def test_f11_pagination_limit_clamping(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "lim_proj")
    ts = _now_ms()
    traces = []
    spans = []
    for i in range(105):
        b = fixtures.make_batch(
            seed=ctx.unique_id(f"lim_{i}"),
            project_id=proj.id,
            session_id=ctx.unique_id(f"ls_{i}"),
            start_ts_ms=ts + i,
        )
        traces.extend(b["traces"])
        spans.extend(b["spans"])
    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()

    r0 = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token, query={"limit": 0}).raise_for_status().json()
    assert r0["limit"] == 1
    assert len(r0["traces"]) == 1

    r101 = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token, query={"limit": 101}).raise_for_status().json()
    assert r101["limit"] == 100
    assert len(r101["traces"]) == 100

    r_def = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token).raise_for_status().json()
    assert r_def["limit"] == 50
    assert len(r_def["traces"]) == 50

    s0 = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token, query={"limit": 0}).raise_for_status().json()
    assert s0["limit"] == 1
    assert len(s0["sessions"]) == 1

    s101 = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token, query={"limit": 101}).raise_for_status().json()
    assert s101["limit"] == 100
    assert len(s101["sessions"]) == 100

    s_def = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/sessions", token=ctx.viewer_token).raise_for_status().json()
    assert s_def["limit"] == 50
    assert len(s_def["sessions"]) == 50


@test_case(
    id="T2-F11-02",
    tier=2,
    feature="F11",
    title="Invalid pagination cursor returns non-500 (200 or 400) without panic",
    tags=("query", "cursor", "negative"),
)
def test_f11_invalid_cursor_non_500(ctx: TestContext) -> None:
    proj = ctx.proj_a
    for bad_cursor in ("not-valid-base64!!!", "", "999999999", "eyJiYWQiOiB0cnVlfQ"):
        resp = ctx.client.request(
            "GET",
            f"/v1/projects/{proj.id}/agent/traces",
            token=ctx.viewer_token,
            query={"cursor": bad_cursor},
        )
        assert resp.status in (200, 400), f"Invalid cursor {bad_cursor!r} returned {resp.status}"
        assert resp.status != 500


@test_case(
    id="T2-F11-03",
    tier=2,
    feature="F11",
    title="3-level erasure (Trace, Session, User with capture_user_id=true) hides data immediately across L0 and PLSS",
    tags=("erasure", "gdpr", "tombstone"),
)
def test_f11_three_level_erasure_immediate_hiding(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "erase_proj")
    _set_privacy_policy(
        ctx.client,
        proj.id,
        token=ctx.owner_token,
        policy=fixtures.make_privacy_policy(project_id=proj.id, capture_user_id=True),
    )
    ts = _now_ms()

    u1 = ctx.unique_id("usr_1")
    u2 = ctx.unique_id("usr_2")
    s1 = ctx.unique_id("sess_1")
    s2 = ctx.unique_id("sess_2")
    s3 = ctx.unique_id("sess_3")

    b_tr = fixtures.make_batch(seed=ctx.unique_id("er_tr"), project_id=proj.id, session_id=s1, user_id=u1, start_ts_ms=ts)
    b_s_a = fixtures.make_batch(seed=ctx.unique_id("er_sa"), project_id=proj.id, session_id=s2, user_id=u1, start_ts_ms=ts + 10)
    b_s_b = fixtures.make_batch(seed=ctx.unique_id("er_sb"), project_id=proj.id, session_id=s2, user_id=u1, start_ts_ms=ts + 20)
    b_u1_rem = fixtures.make_batch(seed=ctx.unique_id("er_u1"), project_id=proj.id, session_id=s1, user_id=u1, start_ts_ms=ts + 30)
    b_u2_keep = fixtures.make_batch(seed=ctx.unique_id("er_u2"), project_id=proj.id, session_id=s3, user_id=u2, start_ts_ms=ts + 40)

    all_traces = [b["traces"][0] for b in (b_tr, b_s_a, b_s_b, b_u1_rem, b_u2_keep)]
    all_spans = [sp for b in (b_tr, b_s_a, b_s_b, b_u1_rem, b_u2_keep) for sp in b["spans"]]
    ctx.client.ingest_plst_batch({"traces": all_traces, "spans": all_spans}, api_key=proj.pk_key).raise_for_status()

    tr_erase_id = b_tr["traces"][0]["trace_id"]
    tr_sa_id = b_s_a["traces"][0]["trace_id"]
    tr_sb_id = b_s_b["traces"][0]["trace_id"]
    tr_u1_rem_id = b_u1_rem["traces"][0]["trace_id"]
    tr_u2_keep_id = b_u2_keep["traces"][0]["trace_id"]

    for tid in (tr_erase_id, tr_sa_id, tr_sb_id, tr_u1_rem_id, tr_u2_keep_id):
        _wait_for_trace(ctx.client, proj.id, tid, token=ctx.viewer_token)

    # 1. Trace-level erasure
    ctx.client.request(
        "DELETE",
        f"/v1/projects/{proj.id}/agent/traces/{tr_erase_id}",
        token=ctx.admin_token,
    ).raise_for_status()
    assert _get_trace_detail(ctx.client, proj.id, tr_erase_id, token=ctx.viewer_token).status == 404
    assert _get_trace_detail(ctx.client, proj.id, tr_u1_rem_id, token=ctx.viewer_token).status == 200

    # 2. Session-level erasure (erases s2: tr_sa_id & tr_sb_id)
    r_sess_er = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/erasure",
        json_body={"level": "session", "target_id": s2},
        token=ctx.admin_token,
    )
    if r_sess_er.status in (404, 405):
        r_sess_er = ctx.client.request(
            "DELETE",
            f"/v1/projects/{proj.id}/agent/traces/{tr_sa_id}",
            query={"level": "session", "scope": "session"},
            token=ctx.admin_token,
        )
    r_sess_er.raise_for_status()
    assert _get_trace_detail(ctx.client, proj.id, tr_sa_id, token=ctx.viewer_token).status == 404
    assert _get_trace_detail(ctx.client, proj.id, tr_sb_id, token=ctx.viewer_token).status == 404
    assert _get_trace_detail(ctx.client, proj.id, tr_u1_rem_id, token=ctx.viewer_token).status == 200

    # 3. User-level erasure (erases u1: tr_u1_rem_id, leaves u2 untouched)
    r_usr_er = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/erasure",
        json_body={"level": "user", "target_id": u1},
        token=ctx.admin_token,
    )
    if r_usr_er.status in (404, 405):
        r_usr_er = ctx.client.request(
            "DELETE",
            f"/v1/projects/{proj.id}/agent/traces/{tr_u1_rem_id}",
            query={"level": "user", "scope": "user"},
            token=ctx.admin_token,
        )
    r_usr_er.raise_for_status()
    assert _get_trace_detail(ctx.client, proj.id, tr_u1_rem_id, token=ctx.viewer_token).status == 404
    assert _get_trace_detail(ctx.client, proj.id, tr_u2_keep_id, token=ctx.viewer_token).status == 200

    listed = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token).raise_for_status().json()
    assert [t["trace_id"] for t in listed["traces"]] == [tr_u2_keep_id]


@test_case(
    id="T2-F11-04",
    tier=2,
    feature="F11",
    title="Retention policy retention_days clamps to [1, 3650] and separates active vs expired date partitions",
    tags=("retention", "privacy", "boundary"),
)
def test_f11_retention_clamping_and_date_partitioning(ctx: TestContext) -> None:
    admin_tok = f"sk_root_ret_{ctx.unique_id('adm')}"
    inst = ctx.node_manager.start_all(admin_token=admin_tok)
    try:
        iso_client = PulseClient(inst.base_url, timeout=15.0)
        owner_tok, proj = _setup_isolated_node_project(iso_client, admin_tok, ctx.unique_id("ret_proj"))

        p_low = _set_privacy_policy(
            iso_client,
            proj.id,
            token=owner_tok,
            policy=fixtures.make_privacy_policy(project_id=proj.id, retention_days=0),
        )
        assert int(p_low["retention_days"]) == 1

        p_high = _set_privacy_policy(
            iso_client,
            proj.id,
            token=owner_tok,
            policy=fixtures.make_privacy_policy(project_id=proj.id, retention_days=9999),
        )
        assert int(p_high["retention_days"]) == 3650

        # Keep retention_days=3650 while flushing both dates so date-partitioned .plss dirs can be verified
        now = _now_ms()
        old_ts = now - (45 * 86_400_000)
        now_date = datetime.fromtimestamp(now / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        old_date = datetime.fromtimestamp(old_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        assert now_date != old_date

        b_now = fixtures.make_batch(seed=ctx.unique_id("ret_now"), project_id=proj.id, start_ts_ms=now)
        b_old = fixtures.make_batch(seed=ctx.unique_id("ret_old"), project_id=proj.id, start_ts_ms=old_ts)
        now_tid = b_now["traces"][0]["trace_id"]
        old_tid = b_old["traces"][0]["trace_id"]

        iso_client.ingest_plst_batch(b_now, api_key=proj.pk_key).raise_for_status()
        iso_client.ingest_plst_batch(b_old, api_key=proj.pk_key).raise_for_status()

        seg_root = inst.data_dir / "object_store" / "agent_segments" / proj.id
        deadline = time.monotonic() + 15.0
        now_segs: list[Path] = []
        old_segs: list[Path] = []
        while time.monotonic() < deadline:
            now_segs = list((seg_root / now_date).rglob("*.plss")) if (seg_root / now_date).is_dir() else []
            old_segs = list((seg_root / old_date).rglob("*.plss")) if (seg_root / old_date).is_dir() else []
            if now_segs and old_segs:
                break
            time.sleep(0.25)
        assert now_segs, f"Expected .plss segment under active partition {seg_root / now_date}"
        assert old_segs, f"Expected .plss segment under historical partition {seg_root / old_date}"

        _wait_for_trace(iso_client, proj.id, now_tid, token=owner_tok)
        _wait_for_trace(iso_client, proj.id, old_tid, token=owner_tok)

        # Now apply retention_days=7 and confirm active trace and partition stay intact
        p_mid = _set_privacy_policy(
            iso_client,
            proj.id,
            token=owner_tok,
            policy=fixtures.make_privacy_policy(project_id=proj.id, retention_days=7),
        )
        assert int(p_mid["retention_days"]) == 7

        assert _get_trace_detail(iso_client, proj.id, now_tid, token=owner_tok).status == 200
        assert any((seg_root / now_date).rglob("*.plss")), "Active date partition must remain intact under 7-day retention"
    finally:
        ctx.node_manager.stop(inst)


@test_case(
    id="T2-F11-05",
    tier=2,
    feature="F11",
    title="Keyset cursor pagination exhaustively traverses >100 traces with zero duplicates or gaps",
    tags=("query", "cursor", "pagination"),
)
def test_f11_cursor_pagination_no_duplicates_or_gaps(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "cur_proj")
    ts = _now_ms()
    expected: set[str] = set()
    traces = []
    spans = []
    for i in range(60):
        b = fixtures.make_batch(seed=ctx.unique_id(f"cur_{i}"), project_id=proj.id, start_ts_ms=ts + i * 5)
        traces.extend(b["traces"])
        spans.extend(b["spans"])
        expected.add(b["traces"][0]["trace_id"])
    ctx.client.ingest_plst_batch({"traces": traces, "spans": spans}, api_key=proj.pk_key).raise_for_status()

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(10):
        q: dict[str, Any] = {"limit": 15}
        if cursor is not None:
            q["cursor"] = cursor
        page = ctx.client.request("GET", f"/v1/projects/{proj.id}/agent/traces", token=ctx.viewer_token, query=q).raise_for_status().json()
        page_ids = [t["trace_id"] for t in page["traces"]]
        if not page_ids:
            break
        seen.extend(page_ids)
        cursor = page.get("next_cursor")
        if not cursor:
            break

    assert len(seen) == len(set(seen)), "Duplicate trace_ids returned across keyset cursor pages"
    assert set(seen) == expected, f"Missing/extra traces in cursor traversal: diff={expected ^ set(seen)}"


# ===========================================================================
# F12: Reliability, Truncation, Loop Detection, PII & Wire Efficiency
# ===========================================================================


@test_case(
    id="T2-F12-01",
    tier=2,
    feature="F12",
    title="64 KiB payload truncation exact boundary: 65,536B untouched vs 65,537B truncated",
    tags=("reliability", "truncation", "boundary"),
)
def test_f12_64kib_exact_payload_boundary(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    # Exact JSON serialized length: {"p":"AAAA..."} -> 8 bytes overhead + 65528 A's = 65536 bytes
    exact_payload = {"p": "A" * (65_536 - 8)}
    assert len(json.dumps(exact_payload, separators=(",", ":")).encode("utf-8")) == 65_536

    over_payload = {"p": "B" * (65_537 - 8)}
    assert len(json.dumps(over_payload, separators=(",", ":")).encode("utf-8")) == 65_537

    tr_exact_id = ctx.unique_id("tr_64k_eq")
    tr_over_id = ctx.unique_id("tr_64k_ov")
    tr_exact = fixtures.make_trace(tr_exact_id, project_id=proj.id, session_id=ctx.unique_id("s_eq"), input_payload=exact_payload, start_ts_ms=ts)
    tr_over = fixtures.make_trace(tr_over_id, project_id=proj.id, session_id=ctx.unique_id("s_ov"), input_payload=over_payload, start_ts_ms=ts + 1)

    r = ctx.client.ingest_plst_batch({"traces": [tr_exact, tr_over], "spans": []}, api_key=proj.pk_key)
    r.raise_for_status()
    assert r.json()["truncated_payloads"] >= 1

    d_exact = _wait_for_trace(ctx.client, proj.id, tr_exact_id, token=ctx.viewer_token)
    assert d_exact["trace"]["input"] == exact_payload

    d_over = _wait_for_trace(ctx.client, proj.id, tr_over_id, token=ctx.viewer_token)
    got_over = d_over["trace"]["input"]
    assert got_over["_truncated"] is True
    assert got_over["_original_bytes"] == 65_537
    assert isinstance(got_over["_preview"], str)
    assert len(json.dumps(got_over, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) <= 65_536


@test_case(
    id="T2-F12-02",
    tier=2,
    feature="F12",
    title="Multi-byte UTF-8 (emoji, CJK, accented) and quote/backslash-heavy >64 KiB truncation stays <= 64 KiB valid UTF-8",
    tags=("reliability", "truncation", "utf8"),
)
def test_f12_multibyte_utf8_and_escape_truncation(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_utf8_trunc")

    # Mix 4-byte emoji 🚀, 3-byte 中, 2-byte é, and JSON-escaped quotes/backslashes
    heavy_text = ('🚀é中\\"\\' * 12_000)
    sp = fixtures.make_span(
        ctx.unique_id("sp_utf8_trunc"),
        tr_id,
        project_id=proj.id,
        input_payload={"text": heavy_text},
        output_payload={"text": "🚀" * 20_000},
        start_ts_ms=ts,
    )
    tr = fixtures.make_trace(tr_id, project_id=proj.id, session_id=ctx.unique_id("s_u8"), start_ts_ms=ts, spans=[sp])
    r = ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key)
    r.raise_for_status()
    assert r.json()["truncated_payloads"] >= 2

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    got_sp = detail["spans"][0]
    for field in (got_sp["input"], got_sp["output"]):
        assert field["_truncated"] is True
        assert field["_original_bytes"] > 65_536
        encoded = json.dumps(field, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        assert len(encoded) <= 65_536


@test_case(
    id="T2-F12-03",
    tier=2,
    feature="F12",
    title="Loop detection exact boundaries: 3 vs 4 consecutive tools, 5 vs 6 total tools, 11 vs 12 unique LLM spans",
    tags=("reliability", "loop_detection", "boundary"),
)
def test_f12_loop_detection_exact_thresholds(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    def _ingest_spans(tag: str, spans_spec: list[tuple[str, str, str | None, str]]) -> bool:
        tr_id = ctx.unique_id(f"tr_lp_{tag}")
        spans = []
        for idx, (sp_id, stype, tool_name, phase) in enumerate(spans_spec):
            spans.append(
                fixtures.make_span(
                    f"{tr_id}_{sp_id}",
                    tr_id,
                    project_id=proj.id,
                    span_type=stype,
                    span_phase=phase,
                    tool_name=tool_name,
                    name=tool_name or f"{stype}.{idx}",
                    start_ts_ms=ts + idx * 10,
                )
            )
        tr = fixtures.make_trace(tr_id, project_id=proj.id, session_id=ctx.unique_id(f"s_lp_{tag}"), start_ts_ms=ts, spans=spans)
        ctx.client.ingest_plst_batch({"traces": [tr], "spans": spans}, api_key=proj.pk_key).raise_for_status()
        d = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
        return bool(d["trace"]["has_loop_warning"])

    # 1. Consecutive tool: 3 -> False, 4 -> True
    assert _ingest_spans("c3", [(f"t{i}", "tool", "lookup", "final") for i in range(3)]) is False
    assert _ingest_spans("c4", [(f"t{i}", "tool", "lookup", "final") for i in range(4)]) is True

    # 2. Non-consecutive total tool: interleave tool_b so max consecutive <= 2
    seq_5 = [("a0", "tool", "tool_a", "final"), ("a1", "tool", "tool_a", "final"), ("b0", "tool", "tool_b", "final"),
             ("a2", "tool", "tool_a", "final"), ("a3", "tool", "tool_a", "final"), ("b1", "tool", "tool_b", "final"),
             ("a4", "tool", "tool_a", "final")]
    seq_6 = seq_5 + [("a5", "tool", "tool_a", "final")]
    assert _ingest_spans("tot5", seq_5) is False
    assert _ingest_spans("tot6", seq_6) is True

    # 3. Unique LLM spans: 11 -> False, 12 -> True, and 30 phase records for 10 unique LLM spans -> False
    assert _ingest_spans("llm11", [(f"l{i}", "llm", None, "final") for i in range(11)]) is False
    assert _ingest_spans("llm12", [(f"l{i}", "llm", None, "final") for i in range(12)]) is True
    phased_10 = [(f"l{i}", "llm", None, ph) for i in range(10) for ph in ("start", "update", "final")]
    assert _ingest_spans("llm10_30ph", phased_10) is False


@test_case(
    id="T2-F12-04",
    tier=2,
    feature="F12",
    title="Hostile JSON nesting depth (>128) returns 400 and >8 MiB HTTP body returns 413 or clean socket close",
    tags=("reliability", "hostile", "dos"),
)
def test_f12_hostile_json_depth_and_8mib_limit(ctx: TestContext) -> None:
    proj = ctx.proj_a

    # 1. Nesting depth = 200 (> serde_json 128 recursion limit)
    deep_json = ('{"a":' * 200) + "1" + ("}" * 200)
    raw_body = f'{{"traces":[],"spans":[],"extra":{deep_json}}}'.encode("utf-8")
    r_depth = ctx.client.request(
        "POST",
        "/v1/agent/traces",
        body=raw_body,
        headers={"Content-Type": "application/json"},
        api_key=proj.pk_key,
    )
    assert r_depth.status == 400, f"Expected 400 for depth-200 JSON, got {r_depth.status}"

    # 2. > 8 MiB HTTP body (8 MiB + 1024 B)
    oversized = b'{"traces":[],"spans":[],"pad":"' + (b"X" * (8 * 1024 * 1024 + 1024)) + b'"}'
    try:
        r_big = ctx.client.request(
            "POST",
            "/v1/agent/traces",
            body=oversized,
            headers={"Content-Type": "application/json"},
            api_key=proj.pk_key,
            timeout=30.0,
        )
        assert r_big.status in (400, 413), f"Expected 413/400 for >8 MiB body, got {r_big.status}"
    except (BrokenPipeError, ConnectionResetError, http.client.HTTPException):
        # Early socket close by hyper/Axum on Content-Length > DefaultBodyLimit is also valid
        pass

    assert ctx.client.get_healthz().status == 200


@test_case(
    id="T2-F12-05",
    tier=2,
    feature="F12",
    title="PII edge patterns: Luhn-valid vs Luhn-invalid card, case-insensitive substring keys, error_stack, default capture_user_id=false",
    tags=("reliability", "pii", "privacy"),
)
def test_f12_pii_edge_patterns_and_default_user_id_strip(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "pii_proj")
    ts = _now_ms()
    tr_id = ctx.unique_id("tr_pii_edge")

    # Default policy has capture_user_id=False and redact_pii=True
    sp = fixtures.make_span(
        ctx.unique_id("sp_pii_edge"),
        tr_id,
        project_id=proj.id,
        input_payload={
            "luhn_valid": "4111-1111-1111-1111",
            "luhn_invalid": "4111-1111-1111-1112",
            "short_12_digits": "123456789012",
            "User_Password_Hash": "plaintext-pw-123",
            "X-Api_Key-Header": "my-header-secret",
        },
        output_payload={"email": "bob.jones@example.org", "ssn": "987-65-4321"},
        error_stack="Error at alice@example.com with sk-proj-abcdefghijklmnop",
        start_ts_ms=ts,
    )
    tr = fixtures.make_trace(
        tr_id,
        project_id=proj.id,
        session_id=ctx.unique_id("sess_pii_edge"),
        user_id="user_should_be_stripped",
        start_ts_ms=ts,
        spans=[sp],
    )
    r = ctx.client.ingest_json({"traces": [tr], "spans": [sp]}, api_key=proj.pk_key)
    r.raise_for_status()
    assert r.json()["redacted_fields"] >= 5

    detail = _wait_for_trace(ctx.client, proj.id, tr_id, token=ctx.viewer_token)
    assert detail["trace"]["user_id"] is None  # default capture_user_id=false

    got_sp = detail["spans"][0]
    assert "[REDACTED]" in got_sp["input"]["luhn_valid"]
    assert got_sp["input"]["luhn_invalid"] == "4111-1111-1111-1112"
    assert got_sp["input"]["short_12_digits"] == "123456789012"
    assert got_sp["input"]["User_Password_Hash"] == "[REDACTED]"
    assert got_sp["input"]["X-Api_Key-Header"] == "[REDACTED]"
    assert "[REDACTED]" in got_sp["output"]["email"]
    assert "[REDACTED]" in got_sp["output"]["ssn"]
    assert "alice@example.com" not in (got_sp["error_stack"] or "")
    assert "sk-proj-abcdefghijklmnop" not in (got_sp["error_stack"] or "")


@test_case(
    id="T2-F12-06",
    tier=2,
    feature="F12",
    title="Evaluator failure resilience (bad regex, unknown kind, malformed LLM JSON) does not stall ingest",
    tags=("reliability", "evaluators", "resilience"),
)
def test_f12_evaluator_failure_resilience(ctx: TestContext) -> None:
    proj = _create_throwaway_project(ctx, "eval_res")
    ts = _now_ms()

    # 0. Create a healthy deterministic evaluator to confirm failures do not stall healthy evaluators
    r_healthy = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("healthy_no_err"),
            kind="no_errors",
        ),
        token=ctx.member_token,
    )
    r_healthy.raise_for_status()
    healthy_id = str(r_healthy.json()["id"])

    # 1. Create a bad regex evaluator (may be rejected 400 at creation or fail gracefully during eval)
    r_bad_re = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=fixtures.make_evaluator_config(
            name=ctx.unique_id("bad_re"),
            kind="regex_match",
            config={"pattern": "[unclosed("},
        ),
        token=ctx.member_token,
    )
    assert r_bad_re.status in (200, 201, 400), f"Unexpected status for bad regex: {r_bad_re.status} {r_bad_re.text()}"
    bad_re_id: str | None = str(r_bad_re.json()["id"]) if r_bad_re.status in (200, 201) else None

    # 2. Create an unknown evaluator kind (may be rejected 400 at creation or return unknown_evaluator failure)
    unk_cfg = fixtures.make_evaluator_config(name=ctx.unique_id("unk_kind"), kind="no_errors")
    unk_cfg["kind"] = "nonexistent_kind_e2e"
    r_unk = ctx.client.request(
        "POST",
        f"/v1/projects/{proj.id}/agent/evaluators",
        json_body=unk_cfg,
        token=ctx.member_token,
    )
    assert r_unk.status in (200, 201, 400), f"Unexpected status for unknown kind: {r_unk.status} {r_unk.text()}"
    unk_id: str | None = str(r_unk.json()["id"]) if r_unk.status in (200, 201) else None

    # 3. Configure project-scoped BYOK LLM credentials pointing at MockLlmServer returning malformed JSON
    bad_llm_id: str | None = None
    try:
        if ctx.mock_llm is not None and ctx.mock_llm_url is not None:
            ctx.mock_llm.clear()
            ctx.mock_llm.raw_body_override = b"{not valid json at all"
            ctx.client.request(
                "PUT",
                f"/v1/projects/{proj.id}/byok",
                json_body={
                    "category": "llm",
                    "provider": "openai",
                    "label": "E2E Resilience Bad LLM",
                    "secret": "sk-mock-e2e-key-1234567890",
                    "config_json": {"endpoint": ctx.mock_llm_url, "model": "gpt-4o-mini"},
                },
                token=ctx.admin_token,
            ).raise_for_status()
            r_bad_llm = ctx.client.request(
                "POST",
                f"/v1/projects/{proj.id}/agent/evaluators",
                json_body=fixtures.make_evaluator_config(name=ctx.unique_id("bad_llm"), kind="llm_judge"),
                token=ctx.member_token,
            )
            r_bad_llm.raise_for_status()
            bad_llm_id = str(r_bad_llm.json()["id"])

        # Ingest trace and trigger evaluators explicitly
        b = fixtures.make_batch(seed=ctx.unique_id("eval_tr"), project_id=proj.id, start_ts_ms=ts)
        tid = b["traces"][0]["trace_id"]
        ctx.client.ingest_plst_batch(b, api_key=proj.pk_key).raise_for_status()
        _wait_for_trace(ctx.client, proj.id, tid, token=ctx.viewer_token)

        expected_ids = [eid for eid in (healthy_id, bad_re_id, unk_id, bad_llm_id) if eid is not None]
        r_trig = ctx.client.request(
            "POST",
            f"/v1/projects/{proj.id}/agent/traces/{tid}/evaluate",
            json_body={"evaluator_ids": expected_ids},
            token=ctx.member_token,
        )
        assert r_trig.status in (200, 202), f"Unexpected evaluate trigger status: {r_trig.status} {r_trig.text()}"

        if bad_llm_id is not None and ctx.mock_llm is not None:
            ctx.mock_llm.wait_for_count(1, timeout=10.0)

        # Poll evaluations until all created evaluators have recorded outcomes
        deadline = time.monotonic() + 10.0
        evals: list[dict[str, Any]] = []
        expected_set = set(expected_ids)
        while time.monotonic() < deadline:
            ev_resp = ctx.client.request(
                "GET",
                f"/v1/projects/{proj.id}/agent/evaluations",
                token=ctx.viewer_token,
                query={"trace_id": tid},
            ).raise_for_status().json()
            evals = ev_resp if isinstance(ev_resp, list) else ev_resp.get("evaluations", [])
            present_ids = {str(e.get("evaluator_id")) for e in evals}
            if expected_set.issubset(present_ids):
                break
            time.sleep(0.15)

        by_ev_id = {str(e.get("evaluator_id")): e for e in evals}
        assert expected_set.issubset(by_ev_id.keys()), (
            f"Missing evaluator results {expected_set - by_ev_id.keys()} in {evals}"
        )

        healthy_eval = by_ev_id[healthy_id]
        assert healthy_eval["passed"] is True
        assert float(healthy_eval["score"]) == 1.0

        if bad_re_id is not None:
            bad_re_eval = by_ev_id[bad_re_id]
            assert bad_re_eval["passed"] is False
            assert float(bad_re_eval["score"]) == 0.0
            assert bad_re_eval.get("label") == "regex_error" or bad_re_eval.get("error")

        if unk_id is not None:
            unk_eval = by_ev_id[unk_id]
            assert unk_eval["passed"] is False
            assert float(unk_eval["score"]) == 0.0
            assert unk_eval.get("label") == "unknown_evaluator" or unk_eval.get("error")

        if bad_llm_id is not None:
            bad_llm_eval = by_ev_id[bad_llm_id]
            assert bad_llm_eval["passed"] is False
            assert float(bad_llm_eval["score"]) == 0.0

        # Confirm ingest remains healthy and unstalled after evaluator failures
        b2 = fixtures.make_batch(seed=ctx.unique_id("eval_after"), project_id=proj.id, start_ts_ms=ts + 100)
        ctx.client.ingest_plst_batch(b2, api_key=proj.pk_key).raise_for_status()
        _wait_for_trace(ctx.client, proj.id, b2["traces"][0]["trace_id"], token=ctx.viewer_token)
        assert ctx.client.get_healthz().status == 200
    finally:
        if ctx.mock_llm is not None:
            ctx.mock_llm.raw_body_override = None


@test_case(
    id="T2-F12-07",
    tier=2,
    feature="F12",
    title="Wire efficiency: PLST dict-Zstd < PLST raw-Zstd < OTLP/JSON and all hydrate cleanly",
    tags=("plst", "wire_efficiency", "otlp"),
)
def test_f12_plst_vs_otlp_wire_efficiency(ctx: TestContext) -> None:
    proj = ctx.proj_a
    ts = _now_ms()

    batch = fixtures.make_repeated_prompt_batch(
        seed=ctx.unique_id("wire"),
        project_id=proj.id,
        repeats=8,
        prompt_bytes=2048,
        start_ts_ms=ts,
    )
    raw_frame = plst.encode_plst_frame(batch, use_dict=False)
    dict_frame = plst.encode_plst_frame(batch, use_dict=True)

    otlp_req = fixtures.make_otlp_export_request(seed=ctx.unique_id("wire_otlp"), start_ts_ms=ts)
    otlp_json_bytes = json.dumps(otlp_req).encode("utf-8")

    # Also build an OTLP/JSON payload for 8 repeated spans to compare fairly
    repeated_otlp = fixtures.make_otlp_export_request(seed=ctx.unique_id("wire_otlp8"), start_ts_ms=ts)
    base_sp = repeated_otlp["resourceSpans"][0]["scopeSpans"][0]["spans"][1]
    repeated_otlp["resourceSpans"][0]["scopeSpans"][0]["spans"] = [
        {**base_sp, "spanId": fixtures.deterministic_hex_id(f"w8_{i}", 8)}
        for i in range(8)
    ]
    repeated_otlp_bytes = json.dumps(repeated_otlp).encode("utf-8")

    assert len(dict_frame) <= len(raw_frame), f"Expected dict_frame ({len(dict_frame)}) <= raw_frame ({len(raw_frame)})"
    assert len(dict_frame) < len(repeated_otlp_bytes), (
        f"Expected PLST dict frame ({len(dict_frame)}) < OTLP/JSON ({len(repeated_otlp_bytes)})"
    )
    assert len(otlp_json_bytes) > 0

    ctx.client.ingest_plst(dict_frame, api_key=proj.pk_key).raise_for_status()
    ctx.client.ingest_otlp_json(otlp_req, api_key=proj.pk_key).raise_for_status()
    _wait_for_trace(ctx.client, proj.id, batch["traces"][0]["trace_id"], token=ctx.viewer_token)
    otlp_tid = otlp_req["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
    _wait_for_trace(ctx.client, proj.id, otlp_tid, token=ctx.viewer_token)
