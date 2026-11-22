"""Pure-stdlib HTTP client, PLSB/PLSR/PLST/OTLP encoders, and Prometheus MetricsParser."""

from __future__ import annotations

import hashlib
import http.client
import json
import math
import re
import ssl
import struct
import time
import urllib.parse
import uuid
import zlib
from compression import zstd
from dataclasses import dataclass
from typing import Any


class HttpStatusError(RuntimeError):
    """Raised by HttpResponse.raise_for_status() on non-2xx responses."""

    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        preview = response.text(errors="replace")[:512]
        super().__init__(f"HTTP {response.status}: {preview}")


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Structured HTTP response that never throws on 3xx/4xx/5xx status codes."""

    status: int
    headers: dict[str, str]
    body: bytes

    def text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self.body.decode(encoding, errors=errors)

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    def is_success(self) -> bool:
        return 200 <= self.status < 300

    def raise_for_status(self) -> HttpResponse:
        if not self.is_success():
            raise HttpStatusError(self)
        return self


# ---------------------------------------------------------------------------
# Pure-Python XXH3-64 (seed = 0, matching xxhash-rust for PLST blob dedup)
# ---------------------------------------------------------------------------

_MASK32 = 0xFFFFFFFF
_MASK64 = 0xFFFFFFFFFFFFFFFF
_PRIME32_1 = 0x9E3779B1
_PRIME32_2 = 0x85EBCA77
_PRIME32_3 = 0xC2B2AE3D
_PRIME64_1 = 0x9E3779B185EBCA87
_PRIME64_2 = 0xC2B2AE3D27D4EB4F
_PRIME64_3 = 0x165667B19E3779F9
_PRIME64_4 = 0x85EBCA77C2B2AE63
_PRIME64_5 = 0x27D4EB2F165667C5
_STRIPE_LEN = 64
_SECRET_CONSUME_RATE = 8
_SECRET_SIZE_MIN = 136
_SECRET_LASTACC_START = 7
_SECRET_MERGEACCS_START = 11
_ACC_NB = 8
_MID_SIZE_MAX = 240

_DEFAULT_SECRET: bytes = bytes(
    [
        0xB8, 0xFE, 0x6C, 0x39, 0x23, 0xA4, 0x4B, 0xBE, 0x7C, 0x01, 0x81, 0x2C, 0xF7, 0x21, 0xAD, 0x1C,
        0xDE, 0xD4, 0x6D, 0xE9, 0x83, 0x90, 0x97, 0xDB, 0x72, 0x40, 0xA4, 0xA4, 0xB7, 0xB3, 0x67, 0x1F,
        0xCB, 0x79, 0xE6, 0x4E, 0xCC, 0xC0, 0xE5, 0x78, 0x82, 0x5A, 0xD0, 0x7D, 0xCC, 0xFF, 0x72, 0x21,
        0xB8, 0x08, 0x46, 0x74, 0xF7, 0x43, 0x24, 0x8E, 0xE0, 0x35, 0x90, 0xE6, 0x81, 0x3A, 0x26, 0x4C,
        0x3C, 0x28, 0x52, 0xBB, 0x91, 0xC3, 0x00, 0xCB, 0x88, 0xD0, 0x65, 0x8B, 0x1B, 0x53, 0x2E, 0xA3,
        0x71, 0x64, 0x48, 0x97, 0xA2, 0x0D, 0xF9, 0x4E, 0x38, 0x19, 0xEF, 0x46, 0xA9, 0xDE, 0xAC, 0xD8,
        0xA8, 0xFA, 0x76, 0x3F, 0xE3, 0x9C, 0x34, 0x3F, 0xF9, 0xDC, 0xBB, 0xC7, 0xC7, 0x0B, 0x4F, 0x1D,
        0x8A, 0x51, 0xE0, 0x4B, 0xCD, 0xB4, 0x59, 0x31, 0xC8, 0x9F, 0x7E, 0xC9, 0xD9, 0x78, 0x73, 0x64,
        0xEA, 0xC5, 0xAC, 0x83, 0x34, 0xD3, 0xEB, 0xC3, 0xC5, 0x81, 0xA0, 0xFF, 0xFA, 0x13, 0x63, 0xEB,
        0x17, 0x0D, 0xDD, 0x51, 0xB7, 0xF0, 0xDA, 0x49, 0xD3, 0x16, 0x55, 0x26, 0x29, 0xD4, 0x68, 0x9E,
        0x2B, 0x16, 0xBE, 0x58, 0x7D, 0x47, 0xA1, 0xFC, 0x8F, 0xF8, 0xB8, 0xD1, 0x7A, 0xD0, 0x31, 0xCE,
        0x45, 0xCB, 0x3A, 0x8F, 0x95, 0x16, 0x04, 0x28, 0xAF, 0xD7, 0xFB, 0xCA, 0xBB, 0x4B, 0x40, 0x7E,
    ]
)


def _read32_le(buf: memoryview | bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 4], "little")


def _read64_le(buf: memoryview | bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 8], "little")


def _swap32(x: int) -> int:
    return (
        ((x & 0x000000FF) << 24)
        | ((x & 0x0000FF00) << 8)
        | ((x & 0x00FF0000) >> 8)
        | ((x & 0xFF000000) >> 24)
    ) & _MASK32


def _swap64(x: int) -> int:
    return int.from_bytes(x.to_bytes(8, "little"), "big")


def _rotl64(x: int, r: int) -> int:
    return ((x << r) | (x >> (64 - r))) & _MASK64


def _xorshift64(v: int, shift: int) -> int:
    return (v ^ (v >> shift)) & _MASK64


def _mul128_fold64(lhs: int, rhs: int) -> int:
    prod = lhs * rhs
    return ((prod & _MASK64) ^ (prod >> 64)) & _MASK64


def _xxh64_avalanche(h: int) -> int:
    h = _xorshift64(h, 33)
    h = (h * _PRIME64_2) & _MASK64
    h = _xorshift64(h, 29)
    h = (h * _PRIME64_3) & _MASK64
    return _xorshift64(h, 32)


def _xxh3_avalanche(h: int) -> int:
    h = _xorshift64(h, 37)
    h = (h * 0x165667919E3779F9) & _MASK64
    return _xorshift64(h, 32)


def _rrmxmx(h64: int, length: int) -> int:
    h64 ^= _rotl64(h64, 49) ^ _rotl64(h64, 24)
    h64 = (h64 * 0x9FB21C651E98DF25) & _MASK64
    h64 ^= (h64 >> 35) + length
    h64 = (h64 * 0x9FB21C651E98DF25) & _MASK64
    return _xorshift64(h64, 28)


def _mix16b(buf: memoryview, in_off: int, secret: bytes, sec_off: int) -> int:
    in_lo = _read64_le(buf, in_off)
    in_hi = _read64_le(buf, in_off + 8)
    k_lo = _read64_le(secret, sec_off)
    k_hi = _read64_le(secret, sec_off + 8)
    return _mul128_fold64(in_lo ^ k_lo, in_hi ^ k_hi)


def _hash_0_to_16(buf: memoryview, secret: bytes) -> int:
    length = len(buf)
    if length > 8:
        flip1 = (_read64_le(secret, 24) ^ _read64_le(secret, 32)) & _MASK64
        flip2 = (_read64_le(secret, 40) ^ _read64_le(secret, 48)) & _MASK64
        in_lo = _read64_le(buf, 0) ^ flip1
        in_hi = _read64_le(buf, length - 8) ^ flip2
        acc = (length + _swap64(in_lo) + in_hi + _mul128_fold64(in_lo, in_hi)) & _MASK64
        return _xxh3_avalanche(acc)
    if length >= 4:
        in1 = _read32_le(buf, 0)
        in2 = _read32_le(buf, length - 4)
        bitflip = (_read64_le(secret, 8) ^ _read64_le(secret, 16)) & _MASK64
        keyed = ((in2 + (in1 << 32)) ^ bitflip) & _MASK64
        return _rrmxmx(keyed, length)
    if length > 0:
        c1 = buf[0]
        c2 = buf[length >> 1]
        c3 = buf[length - 1]
        combined = ((c1 << 16) | (c2 << 24) | c3 | (length << 8)) & _MASK32
        bitflip = (_read32_le(secret, 0) ^ _read32_le(secret, 4)) & _MASK32
        return _xxh64_avalanche((combined ^ bitflip) & _MASK64)
    return _xxh64_avalanche((_read64_le(secret, 56) ^ _read64_le(secret, 64)) & _MASK64)


def _hash_17_to_128(buf: memoryview, secret: bytes) -> int:
    length = len(buf)
    acc = (length * _PRIME64_1) & _MASK64
    if length > 32:
        if length > 64:
            if length > 96:
                acc = (acc + _mix16b(buf, 48, secret, 96) + _mix16b(buf, length - 64, secret, 112)) & _MASK64
            acc = (acc + _mix16b(buf, 32, secret, 64) + _mix16b(buf, length - 48, secret, 80)) & _MASK64
        acc = (acc + _mix16b(buf, 16, secret, 32) + _mix16b(buf, length - 32, secret, 48)) & _MASK64
    acc = (acc + _mix16b(buf, 0, secret, 0) + _mix16b(buf, length - 16, secret, 16)) & _MASK64
    return _xxh3_avalanche(acc)


def _hash_129_to_240(buf: memoryview, secret: bytes) -> int:
    length = len(buf)
    acc = (length * _PRIME64_1) & _MASK64
    nb_rounds = length // 16
    for i in range(8):
        acc = (acc + _mix16b(buf, 16 * i, secret, 16 * i)) & _MASK64
    acc = _xxh3_avalanche(acc)
    for i in range(8, nb_rounds):
        acc = (acc + _mix16b(buf, 16 * i, secret, 16 * (i - 8) + 3)) & _MASK64
    acc = (acc + _mix16b(buf, length - 16, secret, _SECRET_SIZE_MIN - 17)) & _MASK64
    return _xxh3_avalanche(acc)


def _hash_long(buf: memoryview, secret: bytes) -> int:
    acc = [
        _PRIME32_3, _PRIME64_1, _PRIME64_2, _PRIME64_3,
        _PRIME64_4, _PRIME32_2, _PRIME64_5, _PRIME32_1,
    ]
    length = len(buf)
    nb_stripes_per_block = (len(secret) - _STRIPE_LEN) // _SECRET_CONSUME_RATE
    block_len = _STRIPE_LEN * nb_stripes_per_block
    nb_blocks = (length - 1) // block_len

    for n in range(nb_blocks):
        base = n * block_len
        for s in range(nb_stripes_per_block):
            in_off = base + s * _STRIPE_LEN
            sec_off = s * _SECRET_CONSUME_RATE
            for i in range(_ACC_NB):
                dv = _read64_le(buf, in_off + 8 * i)
                dk = dv ^ _read64_le(secret, sec_off + 8 * i)
                acc[i ^ 1] = (acc[i ^ 1] + dv) & _MASK64
                acc[i] = (acc[i] + (dk & _MASK32) * ((dk >> 32) & _MASK32)) & _MASK64
        sec_off = len(secret) - _STRIPE_LEN
        for i in range(_ACC_NB):
            acc[i] = ((_xorshift64(acc[i], 47) ^ _read64_le(secret, sec_off + 8 * i)) * _PRIME32_1) & _MASK64

    nb_stripes = (length - 1 - block_len * nb_blocks) // _STRIPE_LEN
    tail_base = nb_blocks * block_len
    for s in range(nb_stripes):
        in_off = tail_base + s * _STRIPE_LEN
        sec_off = s * _SECRET_CONSUME_RATE
        for i in range(_ACC_NB):
            dv = _read64_le(buf, in_off + 8 * i)
            dk = dv ^ _read64_le(secret, sec_off + 8 * i)
            acc[i ^ 1] = (acc[i ^ 1] + dv) & _MASK64
            acc[i] = (acc[i] + (dk & _MASK32) * ((dk >> 32) & _MASK32)) & _MASK64
    in_off = length - _STRIPE_LEN
    sec_off = len(secret) - _STRIPE_LEN - _SECRET_LASTACC_START
    for i in range(_ACC_NB):
        dv = _read64_le(buf, in_off + 8 * i)
        dk = dv ^ _read64_le(secret, sec_off + 8 * i)
        acc[i ^ 1] = (acc[i ^ 1] + dv) & _MASK64
        acc[i] = (acc[i] + (dk & _MASK32) * ((dk >> 32) & _MASK32)) & _MASK64

    result = (length * _PRIME64_1) & _MASK64
    for i in range(4):
        sb = _SECRET_MERGEACCS_START + 16 * i
        lhs = acc[2 * i] ^ _read64_le(secret, sb)
        rhs = acc[2 * i + 1] ^ _read64_le(secret, sb + 8)
        result = (result + _mul128_fold64(lhs, rhs)) & _MASK64
    return _xxh3_avalanche(result)


def xxh3_64(data: bytes | bytearray | memoryview) -> int:
    """Compute unsigned 64-bit XXH3 hash (seed = 0)."""
    mv = memoryview(data)
    length = len(mv)
    if length <= 16:
        return _hash_0_to_16(mv, _DEFAULT_SECRET)
    if length <= 128:
        return _hash_17_to_128(mv, _DEFAULT_SECRET)
    if length <= _MID_SIZE_MAX:
        return _hash_129_to_240(mv, _DEFAULT_SECRET)
    return _hash_long(mv, _DEFAULT_SECRET)


def xxh3_64_str(text: str) -> int:
    """Compute unsigned 64-bit XXH3 hash of a UTF-8 string."""
    return xxh3_64(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# PLSB (Events) Binary Encoder (matches crates/pulse-wire/src/batch.rs)
# ---------------------------------------------------------------------------

_PRESET_EVENT_DICT: bytes = (
    b"App_Open\x00Session_Start\x00KYC_PAN_Submitted\x00Video_KYC_Completed\x00"
    b"UPI_Autopay_Mandate_Approved\x00First_Disbursal_Success\x00Checkout_Started\x00Payment_Success\x00"
    b"NSDL_Gateway_504_Timeout\x00OTP_Verified\x00Browse_Credit_Card\x00View_Loan_Calculator\x00"
    b"amount\x00currency\x00INR\x00platform\x00Android\x00iOS\x00Web\x00network\x00Jio_4G\x00Airtel_5G\x00"
    b"app_version\x00city\x00Bengaluru\x00Mumbai\x00Delhi\x00Hyderabad\x00Pune\x00Chennai\x00"
    b"payment_method\x00upi\x00card\x00netbanking\x00merchant_id\x00sku_id\x00$exp_\x00control\x00treatment"
)


def _event_uuid_bytes(ev: dict[str, Any], idx: int) -> bytes:
    raw = ev.get("event_uuid") or ev.get("message_id") or ev.get("messageId") or ev.get("insert_id")
    if isinstance(raw, (bytes, bytearray)) and len(raw) == 16:
        return bytes(raw)
    if isinstance(raw, list) and len(raw) == 16:
        return bytes(int(b) & 0xFF for b in raw)
    if isinstance(raw, str) and raw:
        try:
            return uuid.UUID(raw).bytes
        except ValueError:
            return hashlib.sha256(raw.encode("utf-8")).digest()[:16]
    seed = f"{ev.get('distinct_id', '')}:{ev.get('event') or ev.get('event_name') or ''}:{ev.get('timestamp', 0)}:{idx}"
    return hashlib.sha256(seed.encode("utf-8")).digest()[:16]


class PlsbEncoder:
    """Encoder for `PLSB` v1 binary event batches (`application/x-pulse-frame`)."""

    MAGIC = b"PLSB"
    VERSION = 1
    FLAG_RAW_ZSTD = 0x01
    FLAG_DICT_ZSTD = 0x02

    @classmethod
    def encode(cls, events: list[dict[str, Any]], *, use_dict: bool = False, dict_id: int = 1) -> bytes:
        str_to_id: dict[str, int] = {}
        str_chunks: list[bytes] = []
        str_offsets: list[int] = [0]
        str_cursor = 0

        def intern(s: str) -> int:
            nonlocal str_cursor
            idx = str_to_id.get(s)
            if idx is None:
                idx = len(str_offsets) - 1
                enc = s.encode("utf-8")
                str_chunks.append(enc)
                str_cursor += len(enc)
                str_offsets.append(str_cursor)
                str_to_id[s] = idx
            return idx

        uuids = bytearray()
        routing_ids: list[int] = []
        distinct_ids: list[int] = []
        session_ids: list[int] = []
        event_name_ids: list[int] = []
        event_ts_deltas: list[int] = []
        sent_ts_deltas: list[int] = []
        replay_offsets: list[int] = []
        prop_counts: list[int] = []
        prop_key_ids: list[int] = []
        prop_tags: bytearray = bytearray()
        bool_vals: bytearray = bytearray()
        int_vals: list[int] = []
        float_vals: list[float] = []
        str_val_ids: list[int] = []

        prev_event_ts = 0
        prev_sent_ts = 0

        for idx, ev in enumerate(events):
            distinct_id = str(
                ev.get("distinct_id")
                or ev.get("user_id")
                or ev.get("userId")
                or ev.get("anonymous_id")
                or ev.get("anonymousId")
                or "user_anon"
            )
            routing_seed = str(
                ev.get("routing_seed")
                or ev.get("device_id")
                or ev.get("anonymous_id")
                or distinct_id
            )
            session_id = str(ev.get("session_id") or ev.get("sessionId") or f"sess_{distinct_id}")
            event_name = str(ev.get("event_name") or ev.get("event") or ev.get("name") or "event")
            ev_ts = int(
                ev.get("client_event_ts_ms")
                or ev.get("timestamp_ms")
                or ev.get("timestamp")
                or int(time.time() * 1000)
            )
            sent_ts = int(ev.get("client_sent_ts_ms") or ev.get("sent_at") or 0)
            replay_kb = int(ev.get("replay_offset_kb") or 0)

            uuids.extend(_event_uuid_bytes(ev, idx))
            routing_ids.append(intern(routing_seed))
            distinct_ids.append(intern(distinct_id))
            session_ids.append(intern(session_id))
            event_name_ids.append(intern(event_name))
            event_ts_deltas.append(ev_ts - prev_event_ts)
            prev_event_ts = ev_ts
            sent_ts_deltas.append(sent_ts - prev_sent_ts)
            prev_sent_ts = sent_ts
            replay_offsets.append(replay_kb)

            raw_props = ev.get("props") if ev.get("props") is not None else ev.get("properties")
            items = list(raw_props.items()) if isinstance(raw_props, dict) else list(raw_props or [])
            items = items[:0xFFFF]
            prop_counts.append(len(items))
            for k, v in items:
                prop_key_ids.append(intern(str(k)))
                if v is None:
                    prop_tags.append(0)
                elif isinstance(v, bool):
                    prop_tags.append(1)
                    bool_vals.append(1 if v else 0)
                elif isinstance(v, int):
                    prop_tags.append(2)
                    int_vals.append(v)
                elif isinstance(v, float):
                    prop_tags.append(3)
                    float_vals.append(v)
                else:
                    prop_tags.append(4)
                    str_val_ids.append(intern(str(v) if isinstance(v, str) else json.dumps(v)))

        n = len(events)
        unique_strings = len(str_offsets) - 1
        total_props = len(prop_tags)

        payload = bytearray()
        payload.extend(struct.pack("<II", n, unique_strings))
        payload.extend(struct.pack(f"<{len(str_offsets)}I", *str_offsets))
        for ch in str_chunks:
            payload.extend(ch)
        payload.extend(uuids)
        if n:
            fmt_u32_n = f"<{n}I"
            fmt_i64_n = f"<{n}q"
            payload.extend(struct.pack(fmt_u32_n, *routing_ids))
            payload.extend(struct.pack(fmt_u32_n, *distinct_ids))
            payload.extend(struct.pack(fmt_u32_n, *session_ids))
            payload.extend(struct.pack(fmt_u32_n, *event_name_ids))
            payload.extend(struct.pack(fmt_i64_n, *event_ts_deltas))
            payload.extend(struct.pack(fmt_i64_n, *sent_ts_deltas))
            payload.extend(struct.pack(fmt_u32_n, *replay_offsets))
            payload.extend(struct.pack(f"<{n}H", *prop_counts))
        payload.extend(struct.pack("<I", total_props))
        if total_props:
            payload.extend(struct.pack(f"<{total_props}I", *prop_key_ids))
            payload.extend(prop_tags)
        payload.extend(struct.pack("<I", len(bool_vals)))
        payload.extend(bool_vals)
        payload.extend(struct.pack("<I", len(int_vals)))
        if int_vals:
            payload.extend(struct.pack(f"<{len(int_vals)}q", *int_vals))
        payload.extend(struct.pack("<I", len(float_vals)))
        if float_vals:
            payload.extend(struct.pack(f"<{len(float_vals)}d", *float_vals))
        payload.extend(struct.pack("<I", len(str_val_ids)))
        if str_val_ids:
            payload.extend(struct.pack(f"<{len(str_val_ids)}I", *str_val_ids))

        if use_dict:
            zdict = zstd.ZstdDict(_PRESET_EVENT_DICT, is_raw=True)
            compressed = zstd.compress(bytes(payload), zstd_dict=zdict)
            return cls.MAGIC + bytes([cls.VERSION, cls.FLAG_DICT_ZSTD]) + struct.pack("<H", dict_id) + compressed
        compressed = zstd.compress(bytes(payload))
        return cls.MAGIC + bytes([cls.VERSION, cls.FLAG_RAW_ZSTD]) + compressed


# ---------------------------------------------------------------------------
# PLSR (Replay) Binary Encoder (matches crates/pulse-wire/src/replay.rs)
# ---------------------------------------------------------------------------

_NODE_KIND_TAGS = {
    "screen": 0,
    "container": 1,
    "button": 2,
    "masked_input": 3,
    "text": 4,
    "image_placeholder": 5,
    "error_banner": 6,
}


def _write_replay_str(out: bytearray, s: str) -> None:
    raw = s.encode("utf-8")
    out.extend(struct.pack("<I", len(raw)))
    out.extend(raw)


def _write_wire_node(out: bytearray, node: dict[str, Any]) -> None:
    kind_str = str(node.get("kind", "text")).lower()
    kind_tag = _NODE_KIND_TAGS.get(kind_str, 4)
    label = "████" if kind_tag == 3 else str(node.get("masked_label", ""))
    out.extend(
        struct.pack(
            "<IIBHHHHI",
            int(node.get("node_id", 1)),
            int(node.get("parent_id", 0)),
            kind_tag,
            int(node.get("x", 0)),
            int(node.get("y", 0)),
            int(node.get("w", 100)),
            int(node.get("h", 40)),
            int(node.get("bg_rgba", 0)),
        )
    )
    _write_replay_str(out, label)


class PlsrEncoder:
    """Encoder for `PLSR` v1 binary replay chunks (`application/x-pulse-frame`)."""

    MAGIC = b"PLSR"
    VERSION = 1
    FLAG_RAW_ZSTD = 0x01

    @classmethod
    def encode(cls, chunk: dict[str, Any]) -> bytes:
        raw = bytearray()
        _write_replay_str(raw, str(chunk.get("project_id", "")))
        _write_replay_str(raw, str(chunk.get("session_id", "")))
        raw.extend(struct.pack("<q", int(chunk.get("chunk_start_ts_ms", 0))))
        ops = list(chunk.get("ops") or [])
        raw.extend(struct.pack("<I", len(ops)))

        for op in ops:
            tag = str(op.get("op", ""))
            ts_off = int(op.get("ts_offset_ms", 0))
            if tag == "full_snapshot":
                if ts_off == 0:
                    raw.append(1)
                else:
                    raw.append(6)
                    raw.extend(struct.pack("<I", ts_off))
                nodes = list(op.get("nodes") or [])
                raw.extend(struct.pack("<HHI", int(op.get("width", 390)), int(op.get("height", 844)), len(nodes)))
                for node in nodes:
                    _write_wire_node(raw, node)
            elif tag == "node_upsert":
                if ts_off == 0:
                    raw.append(2)
                else:
                    raw.append(7)
                    raw.extend(struct.pack("<I", ts_off))
                _write_wire_node(raw, op)
            elif tag == "node_remove":
                if ts_off == 0:
                    raw.append(3)
                else:
                    raw.append(8)
                    raw.extend(struct.pack("<I", ts_off))
                raw.extend(struct.pack("<I", int(op.get("node_id", 0))))
            elif tag == "pointer_interaction":
                raw.append(4)
                raw.extend(struct.pack("<IHH", ts_off, int(op.get("x", 0)), int(op.get("y", 0))))
                kind = str(op.get("type", "tap")).lower()
                if kind == "tap":
                    raw.append(0)
                elif kind == "rage_tap":
                    raw.append(1)
                    raw.append(int(op.get("count", 3)) & 0xFF)
                elif kind == "dead_tap":
                    raw.append(2)
                else:
                    raw.append(3)
            elif tag == "network_breadcrumb":
                raw.append(5)
                raw.extend(struct.pack("<I", ts_off))
                _write_replay_str(raw, str(op.get("method", "GET")))
                _write_replay_str(raw, str(op.get("path", "/")))
                raw.extend(struct.pack("<HI", int(op.get("status_code", 200)), int(op.get("duration_ms", 10))))
            else:
                raise ValueError(f"Unsupported replay op: {tag!r}")

        compressed = zstd.compress(bytes(raw))
        return cls.MAGIC + bytes([cls.VERSION, cls.FLAG_RAW_ZSTD]) + struct.pack("<I", len(compressed)) + compressed


# ---------------------------------------------------------------------------
# PLST (Agent Traces) Binary Encoder (matches crates/pulse-agent/src/protocol.rs)
# ---------------------------------------------------------------------------

_NO_IDX = 0xFFFFFFFF
_TRACE_STATUS = {"ok": 0, "running": 1, "in_progress": 1, "error": 2, "failed": 2, "timeout": 3, "cancelled": 4}
_FAILURE_SRC = {"llm": 1, "tool": 2, "guardrail": 3, "timeout": 4, "cancelled": 5, "user": 6, "unknown": 6}
_SPAN_TYPE = {"agent": 0, "llm": 1, "tool": 2, "retrieval": 3, "embedding": 4, "guardrail": 5, "workflow": 6, "handoff": 7, "router": 8, "custom": 9}
_SPAN_PHASE = {"start": 0, "update": 1, "final": 2}


def _delta_i64(vals: list[int]) -> list[int]:
    out: list[int] = []
    prev = 0
    for v in vals:
        out.append(v - prev)
        prev = v
    return out


class PlstEncoder:
    """Encoder for `PLST` v1 binary agent trace batches (`application/x-pulse-trace`)."""

    MAGIC = b"PLST"
    VERSION = 1
    FLAG_RAW_ZSTD = 0x01
    FLAG_FINAL = 0x08

    @classmethod
    def encode(cls, batch: dict[str, Any]) -> bytes:
        traces = list(batch.get("traces") or [])
        spans = list(batch.get("spans") or [])

        strings: list[bytes] = []
        str_offsets: list[int] = [0]
        str_cursor = 0
        str_map: dict[str, int] = {}

        def intern_str(val: str | None) -> int:
            nonlocal str_cursor
            if val is None:
                return _NO_IDX
            s = str(val)
            idx = str_map.get(s)
            if idx is None:
                idx = len(strings)
                enc = s.encode("utf-8")
                strings.append(enc)
                str_cursor += len(enc)
                str_offsets.append(str_cursor)
                str_map[s] = idx
            return idx

        blobs: list[tuple[int, int, bytes]] = []
        blob_offsets: list[int] = [0]
        blob_cursor = 0
        blob_buckets: dict[int, list[int]] = {}

        def intern_blob(val: Any) -> int:
            nonlocal blob_cursor
            if val is None:
                return _NO_IDX
            if isinstance(val, str):
                tag, raw_b = 1, val.encode("utf-8")
            else:
                tag, raw_b = 0, json.dumps(val, separators=(",", ":"), sort_keys=True).encode("utf-8")
            h = xxh3_64(raw_b)
            for ex_idx in blob_buckets.get(h, ()):
                if blobs[ex_idx][1] == tag and blobs[ex_idx][2] == raw_b:
                    return ex_idx
            idx = len(blobs)
            blobs.append((h, tag, raw_b))
            blob_cursor += len(raw_b)
            blob_offsets.append(blob_cursor)
            blob_buckets.setdefault(h, []).append(idx)
            return idx

        tr_idx_by_id = {str(t.get("trace_id", "")): i for i, t in enumerate(traces)}

        tr_cols: dict[str, list[Any]] = {
            k: []
            for k in (
                "trace_id", "project_id", "session_id", "user_id", "workflow_id", "parent_trace_id",
                "agent_name", "agent_version", "prompt_version", "model_cfg_ver", "tool_cfg_ver",
                "app_release", "environment", "status", "failure_src", "start_ts", "end_present",
                "end_off", "dur", "span_cnt", "llm_cnt", "tool_cnt", "err_cnt", "in_tok", "out_tok",
                "cached_tok", "tot_tok", "cost_nanos", "pri_model", "pri_provider", "has_eval",
                "eval_bits", "has_pass", "pass_bits", "has_loop", "err_type", "err_msg",
                "tag_counts", "tag_indices", "in_blob", "out_blob", "meta_blob",
            )
        }
        for tr in traces:
            tr_cols["trace_id"].append(intern_str(tr.get("trace_id", "")))
            tr_cols["project_id"].append(intern_str(tr.get("project_id", "")))
            tr_cols["session_id"].append(intern_str(tr.get("session_id")))
            tr_cols["user_id"].append(intern_str(tr.get("user_id")))
            tr_cols["workflow_id"].append(intern_str(tr.get("workflow_id")))
            tr_cols["parent_trace_id"].append(intern_str(tr.get("parent_trace_id")))
            tr_cols["agent_name"].append(intern_str(tr.get("agent_name", "default")))
            tr_cols["agent_version"].append(intern_str(tr.get("agent_version", "unversioned")))
            tr_cols["prompt_version"].append(intern_str(tr.get("prompt_version")))
            tr_cols["model_cfg_ver"].append(intern_str(tr.get("model_config_version")))
            tr_cols["tool_cfg_ver"].append(intern_str(tr.get("tool_config_version")))
            tr_cols["app_release"].append(intern_str(tr.get("app_release")))
            tr_cols["environment"].append(intern_str(tr.get("environment", "production")))
            tr_cols["status"].append(_TRACE_STATUS.get(str(tr.get("status", "ok")).lower(), 0))
            fs = tr.get("failure_source")
            tr_cols["failure_src"].append(0 if fs is None else _FAILURE_SRC.get(str(fs).lower(), 6))
            st = int(tr.get("start_ts_ms", 0) or 0)
            tr_cols["start_ts"].append(st)
            et = tr.get("end_ts_ms")
            tr_cols["end_present"].append(1 if et is not None else 0)
            tr_cols["end_off"].append((int(et) - st) if et is not None else 0)
            tr_cols["dur"].append(int(tr.get("duration_ms", 0) or 0))
            tr_cols["span_cnt"].append(int(tr.get("span_count", 0) or 0))
            tr_cols["llm_cnt"].append(int(tr.get("llm_call_count", 0) or 0))
            tr_cols["tool_cnt"].append(int(tr.get("tool_call_count", 0) or 0))
            tr_cols["err_cnt"].append(int(tr.get("error_span_count", 0) or 0))
            tr_cols["in_tok"].append(int(tr.get("input_tokens", 0) or 0))
            tr_cols["out_tok"].append(int(tr.get("output_tokens", 0) or 0))
            tr_cols["cached_tok"].append(int(tr.get("cached_tokens", 0) or 0))
            tr_cols["tot_tok"].append(int(tr.get("total_tokens", 0) or 0))
            cn = int(tr.get("total_cost_nanos") or round(float(tr.get("total_cost_usd", 0.0) or 0.0) * 1_000_000_000))
            tr_cols["cost_nanos"].append(cn)
            tr_cols["pri_model"].append(intern_str(tr.get("primary_model")))
            tr_cols["pri_provider"].append(intern_str(tr.get("primary_provider")))
            ae = tr.get("avg_eval_score")
            tr_cols["has_eval"].append(1 if ae is not None else 0)
            tr_cols["eval_bits"].append(struct.unpack("<Q", struct.pack("<d", float(ae)))[0] if ae is not None else 0)
            pr = tr.get("eval_pass_rate")
            tr_cols["has_pass"].append(1 if pr is not None else 0)
            tr_cols["pass_bits"].append(struct.unpack("<Q", struct.pack("<d", float(pr)))[0] if pr is not None else 0)
            tr_cols["has_loop"].append(1 if tr.get("has_loop_warning") else 0)
            tr_cols["err_type"].append(intern_str(tr.get("error_type")))
            tr_cols["err_msg"].append(intern_str(tr.get("error_message")))
            tags = list(tr.get("tags") or [])
            tr_cols["tag_counts"].append(len(tags))
            for t in tags:
                tr_cols["tag_indices"].append(intern_str(str(t)))
            tr_cols["in_blob"].append(intern_blob(tr.get("input")))
            tr_cols["out_blob"].append(intern_blob(tr.get("output")))
            tr_cols["meta_blob"].append(intern_blob(tr.get("metadata")))

        sp_cols: dict[str, list[Any]] = {
            k: []
            for k in (
                "trace_idx", "trace_id", "project_id", "span_id", "parent_span_id", "linked_counts",
                "linked_indices", "depth", "name", "type", "phase", "status", "start_ts",
                "end_present", "end_off", "dur", "provider", "model", "model_id", "model_ver",
                "tpl_id", "tpl_ver", "in_tok", "out_tok", "cached_tok", "tot_tok", "cost_nanos",
                "temp_present", "temp_bits", "finish_reason", "model_params_blob", "tool_name",
                "tool_ver", "tool_retry", "err_type", "err_msg", "err_stack", "in_blob",
                "out_blob", "attrs_blob", "meta_blob",
            )
        }
        ev_span_idx: list[int] = []
        ev_ts_ms: list[int] = []
        ev_name_idx: list[int] = []
        ev_attrs_blob: list[int] = []

        for s_idx, sp in enumerate(spans):
            tid = str(sp.get("trace_id", ""))
            sp_cols["trace_idx"].append(tr_idx_by_id.get(tid, _NO_IDX))
            sp_cols["trace_id"].append(intern_str(tid))
            sp_cols["project_id"].append(intern_str(sp.get("project_id", "")))
            sp_cols["span_id"].append(intern_str(sp.get("span_id", "")))
            sp_cols["parent_span_id"].append(intern_str(sp.get("parent_span_id")))
            linked = list(sp.get("linked_span_ids") or [])
            sp_cols["linked_counts"].append(len(linked))
            for lid in linked:
                sp_cols["linked_indices"].append(intern_str(str(lid)))
            sp_cols["depth"].append(int(sp.get("depth", 0) or 0))
            sp_cols["name"].append(intern_str(sp.get("name", "span")))
            sp_cols["type"].append(_SPAN_TYPE.get(str(sp.get("span_type", "custom")).lower(), 9))
            sp_cols["phase"].append(_SPAN_PHASE.get(str(sp.get("span_phase", "final")).lower(), 2))
            sp_cols["status"].append(_TRACE_STATUS.get(str(sp.get("status", "ok")).lower(), 0))
            st = int(sp.get("start_ts_ms", 0) or 0)
            sp_cols["start_ts"].append(st)
            et = sp.get("end_ts_ms")
            sp_cols["end_present"].append(1 if et is not None else 0)
            sp_cols["end_off"].append((int(et) - st) if et is not None else 0)
            sp_cols["dur"].append(int(sp.get("duration_ms", 0) or 0))
            sp_cols["provider"].append(intern_str(sp.get("provider")))
            sp_cols["model"].append(intern_str(sp.get("model")))
            sp_cols["model_id"].append(intern_str(sp.get("model_id")))
            sp_cols["model_ver"].append(intern_str(sp.get("model_version")))
            sp_cols["tpl_id"].append(intern_str(sp.get("prompt_template_id")))
            sp_cols["tpl_ver"].append(intern_str(sp.get("prompt_template_version")))
            sp_cols["in_tok"].append(int(sp.get("input_tokens", 0) or 0))
            sp_cols["out_tok"].append(int(sp.get("output_tokens", 0) or 0))
            sp_cols["cached_tok"].append(int(sp.get("cached_tokens", 0) or 0))
            sp_cols["tot_tok"].append(int(sp.get("total_tokens", 0) or 0))
            cn = int(sp.get("cost_nanos") or round(float(sp.get("cost_usd", 0.0) or 0.0) * 1_000_000_000))
            sp_cols["cost_nanos"].append(cn)
            temp = sp.get("temperature")
            sp_cols["temp_present"].append(1 if temp is not None else 0)
            sp_cols["temp_bits"].append(struct.unpack("<Q", struct.pack("<d", float(temp)))[0] if temp is not None else 0)
            sp_cols["finish_reason"].append(intern_str(sp.get("finish_reason")))
            sp_cols["model_params_blob"].append(intern_blob(sp.get("model_params")))
            sp_cols["tool_name"].append(intern_str(sp.get("tool_name")))
            sp_cols["tool_ver"].append(intern_str(sp.get("tool_version")))
            sp_cols["tool_retry"].append(int(sp.get("tool_retry_count", 0) or 0))
            sp_cols["err_type"].append(intern_str(sp.get("error_type")))
            sp_cols["err_msg"].append(intern_str(sp.get("error_message")))
            sp_cols["err_stack"].append(intern_str(sp.get("error_stack")))
            sp_cols["in_blob"].append(intern_blob(sp.get("input")))
            sp_cols["out_blob"].append(intern_blob(sp.get("output")))
            sp_cols["attrs_blob"].append(intern_blob(sp.get("attributes")))
            sp_cols["meta_blob"].append(intern_blob(sp.get("metadata")))
            for ev in sp.get("events") or []:
                ev_span_idx.append(s_idx)
                ev_ts_ms.append(int(ev.get("timestamp_ms", 0) or 0))
                ev_name_idx.append(intern_str(ev.get("name", "event")))
                ev_attrs_blob.append(intern_blob(ev.get("attributes")))

        def pack_vec(fmt: str, vals: list[Any]) -> bytes:
            return struct.pack(f"<{len(vals)}{fmt}", *vals) if vals else b""

        out = bytearray()
        # Section 1: String Table
        out.extend(struct.pack("<I", len(strings)))
        out.extend(pack_vec("I", str_offsets))
        for b in strings:
            out.extend(b)
        # Section 2: Blob Table
        out.extend(struct.pack("<I", len(blobs)))
        out.extend(pack_vec("Q", [b[0] for b in blobs]))
        out.extend(bytes(b[1] for b in blobs))
        out.extend(pack_vec("I", blob_offsets))
        for _, _, rb in blobs:
            out.extend(rb)
        # Section 3: Traces
        out.extend(struct.pack("<I", len(traces)))
        for k in (
            "trace_id", "project_id", "session_id", "user_id", "workflow_id", "parent_trace_id",
            "agent_name", "agent_version", "prompt_version", "model_cfg_ver", "tool_cfg_ver",
            "app_release", "environment",
        ):
            out.extend(pack_vec("I", tr_cols[k]))
        out.extend(bytes(tr_cols["status"]))
        out.extend(bytes(tr_cols["failure_src"]))
        out.extend(pack_vec("q", _delta_i64(tr_cols["start_ts"])))
        out.extend(bytes(tr_cols["end_present"]))
        out.extend(pack_vec("q", tr_cols["end_off"]))
        out.extend(pack_vec("q", tr_cols["dur"]))
        for k in ("span_cnt", "llm_cnt", "tool_cnt", "err_cnt"):
            out.extend(pack_vec("i", tr_cols[k]))
        for k in ("in_tok", "out_tok", "cached_tok", "tot_tok", "cost_nanos"):
            out.extend(pack_vec("q", tr_cols[k]))
        out.extend(pack_vec("I", tr_cols["pri_model"]))
        out.extend(pack_vec("I", tr_cols["pri_provider"]))
        out.extend(bytes(tr_cols["has_eval"]))
        out.extend(pack_vec("Q", tr_cols["eval_bits"]))
        out.extend(bytes(tr_cols["has_pass"]))
        out.extend(pack_vec("Q", tr_cols["pass_bits"]))
        out.extend(bytes(tr_cols["has_loop"]))
        out.extend(pack_vec("I", tr_cols["err_type"]))
        out.extend(pack_vec("I", tr_cols["err_msg"]))
        out.extend(pack_vec("H", tr_cols["tag_counts"]))
        out.extend(struct.pack("<I", len(tr_cols["tag_indices"])))
        out.extend(pack_vec("I", tr_cols["tag_indices"]))
        out.extend(pack_vec("I", tr_cols["in_blob"]))
        out.extend(pack_vec("I", tr_cols["out_blob"]))
        out.extend(pack_vec("I", tr_cols["meta_blob"]))
        # Section 4: Spans
        out.extend(struct.pack("<I", len(spans)))
        for k in ("trace_idx", "trace_id", "project_id", "span_id", "parent_span_id"):
            out.extend(pack_vec("I", sp_cols[k]))
        out.extend(pack_vec("H", sp_cols["linked_counts"]))
        out.extend(struct.pack("<I", len(sp_cols["linked_indices"])))
        out.extend(pack_vec("I", sp_cols["linked_indices"]))
        out.extend(pack_vec("i", sp_cols["depth"]))
        out.extend(pack_vec("I", sp_cols["name"]))
        out.extend(bytes(sp_cols["type"]))
        out.extend(bytes(sp_cols["phase"]))
        out.extend(bytes(sp_cols["status"]))
        out.extend(pack_vec("q", _delta_i64(sp_cols["start_ts"])))
        out.extend(bytes(sp_cols["end_present"]))
        out.extend(pack_vec("q", sp_cols["end_off"]))
        out.extend(pack_vec("q", sp_cols["dur"]))
        for k in ("provider", "model", "model_id", "model_ver", "tpl_id", "tpl_ver"):
            out.extend(pack_vec("I", sp_cols[k]))
        for k in ("in_tok", "out_tok", "cached_tok", "tot_tok", "cost_nanos"):
            out.extend(pack_vec("q", sp_cols[k]))
        out.extend(bytes(sp_cols["temp_present"]))
        out.extend(pack_vec("Q", sp_cols["temp_bits"]))
        out.extend(pack_vec("I", sp_cols["finish_reason"]))
        out.extend(pack_vec("I", sp_cols["model_params_blob"]))
        out.extend(pack_vec("I", sp_cols["tool_name"]))
        out.extend(pack_vec("I", sp_cols["tool_ver"]))
        out.extend(pack_vec("i", sp_cols["tool_retry"]))
        for k in ("err_type", "err_msg", "err_stack", "in_blob", "out_blob", "attrs_blob", "meta_blob"):
            out.extend(pack_vec("I", sp_cols[k]))
        # Section 5: Span Events
        out.extend(struct.pack("<I", len(ev_span_idx)))
        out.extend(pack_vec("I", ev_span_idx))
        out.extend(pack_vec("q", _delta_i64(ev_ts_ms)))
        out.extend(pack_vec("I", ev_name_idx))
        out.extend(pack_vec("I", ev_attrs_blob))

        uncomp = bytes(out)
        comp = zstd.compress(uncomp)
        crc = zlib.crc32(comp) & _MASK32
        flags = cls.FLAG_RAW_ZSTD | cls.FLAG_FINAL
        header = struct.pack("<4sBBHIII", cls.MAGIC, cls.VERSION, flags, 0, len(uncomp), len(comp), crc)
        return header + comp


# ---------------------------------------------------------------------------
# OTLP / Protobuf Encoder
# ---------------------------------------------------------------------------


def _pb_varint(value: int) -> bytes:
    val = int(value) & _MASK64
    out = bytearray()
    while True:
        b = val & 0x7F
        val >>= 7
        if val:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _pb_ld(field_num: int, payload: bytes) -> bytes:
    return _pb_varint((field_num << 3) | 2) + _pb_varint(len(payload)) + payload


def _pb_str(field_num: int, text: str) -> bytes:
    return _pb_ld(field_num, text.encode("utf-8"))


def encode_otlp_trace_protobuf(req: dict[str, Any]) -> bytes:
    """Encode a minimal OTLP ExportTraceServiceRequest JSON dict into protobuf bytes."""
    out = bytearray()
    for rs in req.get("resourceSpans", []) or []:
        rs_buf = bytearray()
        for ss in rs.get("scopeSpans", []) or []:
            ss_buf = bytearray()
            for sp in ss.get("spans", []) or []:
                sp_buf = bytearray()
                tid = str(sp.get("traceId", ""))
                sid = str(sp.get("spanId", ""))
                tid_b = bytes.fromhex(tid) if len(tid) == 32 else hashlib.sha256(tid.encode()).digest()[:16]
                sid_b = bytes.fromhex(sid) if len(sid) == 16 else hashlib.sha256(sid.encode()).digest()[:8]
                sp_buf.extend(_pb_ld(1, tid_b))
                sp_buf.extend(_pb_ld(2, sid_b))
                if sp.get("name"):
                    sp_buf.extend(_pb_str(5, str(sp["name"])))
                if sp.get("startTimeUnixNano"):
                    sp_buf.extend(_pb_varint((7 << 3) | 1) + struct.pack("<Q", int(sp["startTimeUnixNano"])))
                if sp.get("endTimeUnixNano"):
                    sp_buf.extend(_pb_varint((8 << 3) | 1) + struct.pack("<Q", int(sp["endTimeUnixNano"])))
                ss_buf.extend(_pb_ld(2, bytes(sp_buf)))
            rs_buf.extend(_pb_ld(2, bytes(ss_buf)))
        out.extend(_pb_ld(1, bytes(rs_buf)))
    return bytes(out)


# ---------------------------------------------------------------------------
# Prometheus MetricsParser
# ---------------------------------------------------------------------------

_METRIC_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)(?:\s+[^\s]+)?$"
)
_LABEL_PAIR_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"')


def _unescape_prom_label(val: str) -> str:
    return val.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")


class MetricsParser(dict[tuple[str, frozenset[tuple[str, str]]], float]):
    """Prometheus text exposition parser mapping `(metric_name, frozenset(labels)) -> float`."""

    def __init__(self, text: str = "") -> None:
        super().__init__()
        if text:
            self.update(self.parse(text))

    @classmethod
    def parse(cls, text: str) -> MetricsParser:
        parsed = cls()
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            m = _METRIC_LINE_RE.match(line)
            if not m:
                continue
            name, labels_raw, val_raw = m.group(1), m.group(2), m.group(3)
            if labels_raw:
                labels = frozenset(
                    (lm.group(1), _unescape_prom_label(lm.group(2)))
                    for lm in _LABEL_PAIR_RE.finditer(labels_raw)
                )
            else:
                labels = frozenset()
            low = val_raw.lower()
            if low == "+inf":
                val = math.inf
            elif low == "-inf":
                val = -math.inf
            elif low == "nan":
                val = math.nan
            else:
                val = float(val_raw)
            parsed[(name, labels)] = val
        return parsed

    def get_metric(self, name: str, **labels: str) -> float | None:
        key = (name, frozenset(labels.items()))
        return self.get(key)

    def sum_by_name(self, name: str) -> float:
        return sum(v for (n, _), v in self.items() if n == name)


# ---------------------------------------------------------------------------
# PulseHttpClient
# ---------------------------------------------------------------------------


class PulseHttpClient:
    """Zero-dependency HTTP/REST client covering all Pulse endpoints."""

    def __init__(
        self,
        base_url: str,
        *,
        admin_token: str | None = None,
        api_key: str | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"Invalid Pulse base_url: {base_url!r}")
        self._scheme = parsed.scheme
        self._host = parsed.hostname or "127.0.0.1"
        self._port = parsed.port or (443 if self._scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self.default_token = admin_token
        self.default_api_key = api_key
        self.timeout = timeout

    def _connect(self, timeout: float) -> http.client.HTTPConnection:
        if self._scheme == "https":
            return http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=timeout,
                context=ssl._create_unverified_context(),
            )
        return http.client.HTTPConnection(self._host, self._port, timeout=timeout)

    def _build_path(self, path: str, query: dict[str, Any] | None) -> str:
        clean = path if path.startswith("/") else f"/{path}"
        full = f"{self._base_path}{clean}" if self._base_path else clean
        if query:
            filtered = [
                (k, str(v).lower() if isinstance(v, bool) else str(v))
                for k, v in query.items()
                if v is not None
            ]
            if filtered:
                qs = urllib.parse.urlencode(filtered)
                sep = "&" if "?" in full else "?"
                full = f"{full}{sep}{qs}"
        return full

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | bytearray | str | None = None,
        json_body: Any = None,
        token: str | None = None,
        api_key: str | None = None,
        project_id: str | None = None,
        query: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        req_headers = dict(headers) if headers else {}
        has_lower = {k.lower() for k in req_headers}
        eff_token = token if token is not None else None
        if eff_token is not None and "authorization" not in has_lower:
            req_headers["Authorization"] = f"Bearer {eff_token}"
        if api_key is not None and "x-pulse-key" not in has_lower:
            req_headers["x-pulse-key"] = api_key
        if project_id is not None and "x-pulse-project" not in has_lower:
            req_headers["x-pulse-project"] = project_id

        payload: bytes | None
        if json_body is not None:
            payload = json.dumps(json_body).encode("utf-8")
            if "content-type" not in {k.lower() for k in req_headers}:
                req_headers["Content-Type"] = "application/json"
        elif isinstance(body, str):
            payload = body.encode("utf-8")
        elif isinstance(body, (bytes, bytearray)):
            payload = bytes(body)
        else:
            payload = None

        target = self._build_path(path, query)
        conn = self._connect(self.timeout if timeout is None else timeout)
        try:
            conn.request(method.upper(), target, body=payload, headers=req_headers)
            resp = conn.getresponse()
            raw = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return HttpResponse(status=resp.status, headers=resp_headers, body=raw)
        finally:
            conn.close()

    def _tok(self, token: str | None) -> str | None:
        return token if token is not None else self.default_token

    def _key(self, api_key: str | None) -> str | None:
        return api_key if api_key is not None else self.default_api_key

    # --- Health, Metrics & Admin ---

    def healthz(self) -> HttpResponse:
        return self.request("GET", "/healthz")

    def readyz(self) -> HttpResponse:
        return self.request("GET", "/readyz")

    def metrics(self, *, token: str | None = None) -> HttpResponse:
        return self.request("GET", "/metrics", token=token)

    def parsed_metrics(self, *, token: str | None = None) -> MetricsParser:
        resp = self.metrics(token=token).raise_for_status()
        return MetricsParser.parse(resp.text())

    def status(self, *, project_id: str | None = None, token: str | None = None) -> HttpResponse:
        query = {"project_id": project_id} if project_id else None
        return self.request("GET", "/v1/status", query=query, token=self._tok(token))

    def admin_flush(self, *, token: str | None = None) -> HttpResponse:
        return self.request("POST", "/v1/admin/flush", token=self._tok(token))

    # --- Projects & API Keys ---

    def list_projects(self, *, token: str | None = None) -> HttpResponse:
        return self.request("GET", "/v1/projects", token=self._tok(token))

    def create_project(
        self,
        name: str,
        *,
        timezone: str | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        body: dict[str, Any] = {"name": name}
        if timezone is not None:
            body["timezone"] = timezone
        return self.request("POST", "/v1/projects", json_body=body, token=self._tok(token))

    def list_api_keys(self, project_id: str, *, token: str | None = None) -> HttpResponse:
        return self.request("GET", f"/v1/projects/{project_id}/api-keys", token=self._tok(token))

    def create_api_key(
        self,
        project_id: str,
        label: str,
        *,
        role: str = "ingest_and_flags",
        token: str | None = None,
    ) -> HttpResponse:
        return self.request(
            "POST",
            f"/v1/projects/{project_id}/api-keys",
            json_body={"label": label, "role": role},
            token=self._tok(token),
        )

    def revoke_api_key(self, project_id: str, key_id: str, *, token: str | None = None) -> HttpResponse:
        return self.request(
            "DELETE",
            f"/v1/projects/{project_id}/api-keys/{key_id}",
            token=self._tok(token),
        )

    # --- Event, Profile & Replay Ingest ---

    def ingest(
        self,
        payload: dict[str, Any] | list[dict[str, Any]] | bytes | str,
        *,
        api_key: str | None = None,
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        req_headers = dict(headers) if headers else {}
        if isinstance(payload, (bytes, bytearray)):
            if content_type:
                req_headers.setdefault("Content-Type", content_type)
            return self.request("POST", "/v1/ingest", body=bytes(payload), headers=req_headers, api_key=self._key(api_key))
        if isinstance(payload, str):
            req_headers.setdefault("Content-Type", content_type or "application/json")
            return self.request("POST", "/v1/ingest", body=payload, headers=req_headers, api_key=self._key(api_key))
        return self.request("POST", "/v1/ingest", json_body=payload, headers=req_headers, api_key=self._key(api_key))

    def ingest_plsb(
        self,
        events: list[dict[str, Any]],
        *,
        api_key: str | None = None,
        use_dict: bool = False,
    ) -> HttpResponse:
        frame = PlsbEncoder.encode(events, use_dict=use_dict)
        return self.ingest(frame, api_key=api_key, content_type="application/x-pulse-frame")

    def identify(self, payload: dict[str, Any], *, api_key: str | None = None) -> HttpResponse:
        return self.request("POST", "/v1/identify", json_body=payload, api_key=self._key(api_key))

    def engage(self, payload: dict[str, Any] | list[dict[str, Any]], *, api_key: str | None = None) -> HttpResponse:
        return self.request("POST", "/v1/engage", json_body=payload, api_key=self._key(api_key))

    def groups(self, payload: dict[str, Any] | list[dict[str, Any]], *, api_key: str | None = None) -> HttpResponse:
        return self.request("POST", "/v1/groups", json_body=payload, api_key=self._key(api_key))

    def replay_chunk(
        self,
        payload: dict[str, Any] | bytes,
        *,
        api_key: str | None = None,
        binary: bool = False,
    ) -> HttpResponse:
        if binary or isinstance(payload, (bytes, bytearray)):
            raw = bytes(payload) if isinstance(payload, (bytes, bytearray)) else PlsrEncoder.encode(payload)
            return self.request(
                "POST",
                "/v1/replay/chunk",
                body=raw,
                headers={"Content-Type": "application/x-pulse-frame"},
                api_key=self._key(api_key),
            )
        return self.request("POST", "/v1/replay/chunk", json_body=payload, api_key=self._key(api_key))

    # --- Users, Deletions, Export & Import ---

    def list_users(
        self,
        project_id: str,
        *,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
        token: str | None = None,
    ) -> HttpResponse:
        return self.request(
            "GET",
            f"/v1/projects/{project_id}/users",
            query={"search": search, "limit": limit, "offset": offset},
            token=self._tok(token),
        )

    def get_user(
        self,
        project_id: str,
        distinct_id: str,
        *,
        uid: int | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        enc_did = urllib.parse.quote(distinct_id, safe="")
        query = {"uid": uid} if uid is not None else None
        return self.request(
            "GET",
            f"/v1/projects/{project_id}/users/{enc_did}",
            query=query,
            token=self._tok(token),
        )

    def delete_user(self, project_id: str, distinct_id: str, *, token: str | None = None) -> HttpResponse:
        enc_did = urllib.parse.quote(distinct_id, safe="")
        return self.request(
            "DELETE",
            f"/v1/projects/{project_id}/users/{enc_did}",
            token=self._tok(token),
        )

    def list_deletions(self, project_id: str, *, token: str | None = None) -> HttpResponse:
        return self.request("GET", f"/v1/projects/{project_id}/deletions", token=self._tok(token))

    def export_events(
        self,
        project_id: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        limit: int = 5000,
        format: str = "json",
        token: str | None = None,
    ) -> HttpResponse:
        return self.request(
            "GET",
            f"/v1/projects/{project_id}/export",
            query={
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
                "limit": limit,
                "format": format,
            },
            token=self._tok(token),
        )

    def import_events(
        self,
        project_id: str,
        payload: dict[str, Any] | list[dict[str, Any]] | bytes | str,
        *,
        content_type: str = "application/json",
        token: str | None = None,
    ) -> HttpResponse:
        if isinstance(payload, (dict, list)):
            return self.request(
                "POST",
                f"/v1/projects/{project_id}/import",
                json_body=payload,
                token=self._tok(token),
            )
        return self.request(
            "POST",
            f"/v1/projects/{project_id}/import",
            body=payload,
            headers={"Content-Type": content_type},
            token=self._tok(token),
        )

    # --- Analytics Queries, Sessions & Replay Retrieval ---

    def query(self, payload: dict[str, Any], *, token: str | None = None) -> HttpResponse:
        return self.request("POST", "/v1/query", json_body=payload, token=self._tok(token))

    def query_insights(
        self, project_id: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "POST", f"/v1/projects/{project_id}/query/insights", json_body=payload, token=self._tok(token)
        )

    def query_funnel(
        self, project_id: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "POST", f"/v1/projects/{project_id}/query/funnel", json_body=payload, token=self._tok(token)
        )

    def query_retention(
        self, project_id: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "POST", f"/v1/projects/{project_id}/query/retention", json_body=payload, token=self._tok(token)
        )

    def query_flows(
        self, project_id: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "POST", f"/v1/projects/{project_id}/query/flows", json_body=payload, token=self._tok(token)
        )

    def sessions(
        self,
        *,
        project_id: str | None = None,
        params: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        q = dict(params) if params else {}
        if project_id is not None:
            q["project_id"] = project_id
        return self.request("GET", "/v1/sessions", query=q or None, token=self._tok(token))

    def project_sessions(
        self,
        project_id: str,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        if payload is not None:
            return self.request(
                "POST", f"/v1/projects/{project_id}/sessions", json_body=payload, token=self._tok(token)
            )
        return self.request(
            "GET", f"/v1/projects/{project_id}/sessions", query=params, token=self._tok(token)
        )

    def get_replay(
        self,
        project_id: str,
        session_id: str,
        *,
        byte_range: str | None = None,
        byte_offset: int | None = None,
        binary: bool = False,
        token: str | None = None,
    ) -> HttpResponse:
        headers: dict[str, str] = {}
        if byte_range is not None:
            headers["Range"] = byte_range
        query: dict[str, Any] = {}
        if byte_offset is not None:
            query["byte_offset"] = byte_offset
        if binary:
            query["format"] = "binary"
        enc_sid = urllib.parse.quote(session_id, safe="")
        return self.request(
            "GET",
            f"/v1/projects/{project_id}/replay/{enc_sid}",
            headers=headers or None,
            query=query or None,
            token=self._tok(token),
        )

    # --- Agent Observability ---

    def ingest_agent_traces(
        self,
        payload: dict[str, Any] | bytes,
        *,
        api_key: str | None = None,
        binary: bool = False,
    ) -> HttpResponse:
        if binary or isinstance(payload, (bytes, bytearray)):
            raw = bytes(payload) if isinstance(payload, (bytes, bytearray)) else PlstEncoder.encode(payload)
            return self.request(
                "POST",
                "/v1/agent/traces",
                body=raw,
                headers={"Content-Type": "application/x-pulse-trace"},
                api_key=self._key(api_key),
            )
        return self.request("POST", "/v1/agent/traces", json_body=payload, api_key=self._key(api_key))

    def ingest_otel_traces(
        self,
        payload: dict[str, Any] | bytes,
        *,
        api_key: str | None = None,
        protobuf: bool = False,
    ) -> HttpResponse:
        if protobuf or isinstance(payload, (bytes, bytearray)):
            raw = (
                bytes(payload)
                if isinstance(payload, (bytes, bytearray))
                else encode_otlp_trace_protobuf(payload)
            )
            return self.request(
                "POST",
                "/v1/otel/v1/traces",
                body=raw,
                headers={"Content-Type": "application/x-protobuf"},
                api_key=self._key(api_key),
            )
        return self.request("POST", "/v1/otel/v1/traces", json_body=payload, api_key=self._key(api_key))

    def list_agent_traces(
        self, project_id: str, *, params: dict[str, Any] | None = None, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "GET", f"/v1/projects/{project_id}/agent/traces", query=params, token=self._tok(token)
        )

    def get_agent_trace(
        self, project_id: str, trace_id: str, *, token: str | None = None
    ) -> HttpResponse:
        enc_tid = urllib.parse.quote(trace_id, safe="")
        return self.request(
            "GET", f"/v1/projects/{project_id}/agent/traces/{enc_tid}", token=self._tok(token)
        )

    def delete_agent_trace(
        self,
        project_id: str,
        trace_id: str,
        *,
        level: str | None = None,
        target_id: str | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        enc_tid = urllib.parse.quote(trace_id, safe="")
        query = {}
        if level is not None:
            query["level"] = level
        if target_id is not None:
            query["target_id"] = target_id
        return self.request(
            "DELETE",
            f"/v1/projects/{project_id}/agent/traces/{enc_tid}",
            query=query or None,
            token=self._tok(token),
        )

    def list_agent_sessions(
        self, project_id: str, *, params: dict[str, Any] | None = None, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "GET", f"/v1/projects/{project_id}/agent/sessions", query=params, token=self._tok(token)
        )

    def get_agent_session(
        self, project_id: str, session_id: str, *, token: str | None = None
    ) -> HttpResponse:
        enc_sid = urllib.parse.quote(session_id, safe="")
        return self.request(
            "GET", f"/v1/projects/{project_id}/agent/sessions/{enc_sid}", token=self._tok(token)
        )

    def erase_agent(
        self,
        project_id: str,
        *,
        level: str,
        target_id: str,
        expires_at: int | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        body: dict[str, Any] = {"level": level, "target_id": target_id}
        if expires_at is not None:
            body["expires_at"] = expires_at
        return self.request(
            "POST", f"/v1/projects/{project_id}/agent/erasure", json_body=body, token=self._tok(token)
        )

    # --- Lexicon, Alerts, MCP & Edge Flags ---

    def update_lexicon_event(
        self, project_id: str, event_name: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        enc_name = urllib.parse.quote(event_name, safe="")
        return self.request(
            "PUT",
            f"/v1/projects/{project_id}/lexicon/events/{enc_name}",
            json_body=payload,
            token=self._tok(token),
        )

    def update_lexicon_property(
        self, project_id: str, prop_name: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        enc_name = urllib.parse.quote(prop_name, safe="")
        return self.request(
            "PUT",
            f"/v1/projects/{project_id}/lexicon/properties/{enc_name}",
            json_body=payload,
            token=self._tok(token),
        )

    def list_alerts(self, project_id: str, *, token: str | None = None) -> HttpResponse:
        return self.request("GET", f"/v1/projects/{project_id}/alerts", token=self._tok(token))

    def create_alert(
        self, project_id: str, payload: dict[str, Any], *, token: str | None = None
    ) -> HttpResponse:
        return self.request(
            "POST", f"/v1/projects/{project_id}/alerts", json_body=payload, token=self._tok(token)
        )

    def mcp_call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        req_id: int | str | None = 1,
        project_id: str | None = None,
        token: str | None = None,
    ) -> HttpResponse:
        rpc: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            rpc["params"] = params
        if req_id is not None:
            rpc["id"] = req_id
        return self.request(
            "POST",
            "/v1/mcp",
            json_body=rpc,
            project_id=project_id,
            token=self._tok(token),
        )

    def get_flags(self, api_key: str | None = None, *, if_none_match: str | None = None) -> HttpResponse:
        key = self._key(api_key)
        if not key:
            raise ValueError("api_key is required for GET /v1/flags/:key")
        headers = {"If-None-Match": if_none_match} if if_none_match else None
        return self.request("GET", f"/v1/flags/{urllib.parse.quote(key, safe='')}", headers=headers)
