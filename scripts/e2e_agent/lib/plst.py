"""Pure-Python 3.14 stdlib PLST v1 wire codec, XXH3-64 hash, and hostile frame builders."""

from __future__ import annotations

import json
import math
import os
import struct
import zlib
from collections.abc import Callable
from compression import zstd
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PLST_MAGIC: bytes = b"PLST"
PLST_VERSION: int = 1
AGENT_DICT_ID: int = 2
MAX_PLST_UNCOMP_BYTES: int = 64 * 1024 * 1024
MAX_PLST_TRACES: int = 100_000
MAX_PLST_SPANS: int = 500_000
MAX_PLST_EVENTS: int = 2_000_000
MAX_PLST_STRINGS: int = 2_000_000
MAX_PLST_BLOBS: int = 500_000
MAX_PLST_SINGLE_STRING_BYTES: int = 1024 * 1024
MAX_FIELD_STRING_BYTES: int = MAX_PLST_SINGLE_STRING_BYTES
MAX_PLST_MATERIALIZED_BYTES: int = 64 * 1024 * 1024

BLOB_ENC_JSON_UTF8: int = 0
BLOB_ENC_UTF8_TEXT: int = 1

FLAG_RAW_ZSTD: int = 0x01
FLAG_DICT_ZSTD: int = 0x02
FLAG_PARTIAL: int = 0x04
FLAG_FINAL: int = 0x08
VALID_FLAGS_MASK: int = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | FLAG_PARTIAL | FLAG_FINAL

HEADER_STRUCT: struct.Struct = struct.Struct("<4sBBHIII")
HEADER_SIZE: int = HEADER_STRUCT.size  # 20 bytes
NO_INDEX: int = 0xFFFFFFFF

_ZSTD_FORMATTED_MAGIC: bytes = b"\x37\xa4\x30\xec"

DEFAULT_AGENT_DICT_BYTES: bytes = (
    b'{"messages":[{"role":"system","content":\x00'
    b'{"role":"user","content":\x00'
    b'{"role":"assistant","content":\x00'
    b'{"role":"tool","tool_call_id":\x00'
    b'"tool_calls":[{"id":\x00'
    b'"type":"function","function":{"name":\x00'
    b'"arguments":\x00'
    b'"choices":[{"index":0,"message":\x00'
    b'"finish_reason":"stop"\x00'
    b'"finish_reason":"tool_calls"\x00'
    b'"usage":{"prompt_tokens":\x00'
    b'"completion_tokens":\x00'
    b'"total_tokens":\x00'
    b'gen_ai.system\x00gen_ai.request.model\x00gen_ai.response.model\x00'
    b'gen_ai.usage.input_tokens\x00gen_ai.usage.output_tokens\x00'
    b'gen_ai.operation.name\x00gen_ai.tool.name\x00gen_ai.response.finish_reasons\x00'
    b'llm.model_name\x00llm.token_count.prompt\x00llm.token_count.completion\x00'
    b'input.value\x00output.value\x00service.name\x00deployment.environment\x00'
    b'openai\x00anthropic\x00google\x00bedrock\x00azure\x00'
    b'gpt-4o\x00gpt-4o-mini\x00gpt-4.1\x00gpt-4.1-mini\x00o3-mini\x00'
    b'claude-sonnet-4\x00claude-3-5-sonnet\x00claude-3-5-haiku\x00'
    b'gemini-2.5-pro\x00gemini-2.5-flash\x00'
    b'production\x00staging\x00development\x00unversioned\x00'
    b'agent\x00llm\x00tool\x00retrieval\x00embedding\x00guardrail\x00workflow\x00'
    b'start\x00update\x00final\x00ok\x00error\x00timeout\x00cancelled\x00'
    b'[REDACTED]\x00_truncated\x00_preview\x00_original_bytes\x00'
)

_TRACE_STATUS_TO_U8: dict[str, int] = {
    "ok": 0,
    "running": 1,
    "in_progress": 1,
    "pending": 1,
    "error": 2,
    "failed": 2,
    "failure": 2,
    "timeout": 3,
    "timed_out": 3,
    "cancelled": 4,
    "canceled": 4,
}
_U8_TO_TRACE_STATUS: dict[int, str] = {
    0: "ok",
    1: "running",
    2: "error",
    3: "timeout",
    4: "cancelled",
}

_SPAN_PHASE_TO_U8: dict[str, int] = {
    "start": 0,
    "update": 1,
    "final": 2,
}
_U8_TO_SPAN_PHASE: dict[int, str] = {
    0: "start",
    1: "update",
    2: "final",
}

_SPAN_TYPE_TO_U8: dict[str, int] = {
    "agent": 0,
    "chain": 0,
    "llm": 1,
    "generation": 1,
    "chat": 1,
    "completion": 1,
    "tool": 2,
    "function": 2,
    "tool_call": 2,
    "retrieval": 3,
    "retriever": 3,
    "rag": 3,
    "embedding": 4,
    "embeddings": 4,
    "guardrail": 5,
    "safety": 5,
    "moderation": 5,
    "http": 6,
    "network": 6,
    "db": 7,
    "database": 7,
    "sql": 7,
    "workflow": 8,
    "step": 8,
    "graph": 8,
    "custom": 9,
}
_U8_TO_SPAN_TYPE: dict[int, str] = {
    0: "agent",
    1: "llm",
    2: "tool",
    3: "retrieval",
    4: "embedding",
    5: "guardrail",
    6: "http",
    7: "db",
    8: "workflow",
    9: "custom",
}

_FAILURE_SOURCE_TO_U8: dict[str, int] = {
    "model": 1,
    "llm": 1,
    "tool": 2,
    "application": 3,
    "app": 3,
    "code": 3,
    "prompt": 4,
    "external": 5,
    "http": 5,
    "network": 5,
    "db": 5,
    "unknown": 6,
}
_U8_TO_FAILURE_SOURCE: dict[int, str] = {
    1: "model",
    2: "tool",
    3: "application",
    4: "prompt",
    5: "external",
    6: "unknown",
}


class PlstError(ValueError):
    """Base error raised for invalid or unsafe PLST frames."""


class PlstCorruptError(PlstError):
    """Raised when a PLST frame header, CRC32, Zstd stream, or section is corrupt."""


class PlstLimitError(PlstError):
    """Raised when a PLST frame exceeds safety size limits."""


class PlstDictError(PlstError):
    """Raised when a PLST frame requests an unknown or mismatched dictionary ID."""


@dataclass(slots=True)
class BlobDedupStats:
    """Statistics produced while building Section 2 (content-addressed Blob Table)."""

    total_blobs: int = 0
    unique_blobs: int = 0
    deduped_blobs: int = 0
    collision_checks: int = 0
    bytes_saved: int = 0


# ---------------------------------------------------------------------------
# Pure-Python XXH3-64 (seed = 0) matching xxhash_rust::xxh3::xxh3_64
# ---------------------------------------------------------------------------

_MASK64: int = 0xFFFFFFFFFFFFFFFF
_MASK32: int = 0xFFFFFFFF

_PRIME32_1: int = 0x9E3779B1
_PRIME32_2: int = 0x85EBCA77
_PRIME32_3: int = 0xC2B2AE3D

_PRIME64_1: int = 0x9E3779B185EBCA87
_PRIME64_2: int = 0xC2B2AE3D27D4EB4F
_PRIME64_3: int = 0x165667B19E3779F9
_PRIME64_4: int = 0x85EBCA77C2B2AE63
_PRIME64_5: int = 0x27D4EB2F165667C5

_STRIPE_LEN: int = 64
_SECRET_CONSUME_RATE: int = 8
_ACC_NB: int = 8
_SECRET_MERGEACCS_START: int = 11
_SECRET_LASTACC_START: int = 7
_MID_SIZE_MAX: int = 240
_SECRET_SIZE_MIN: int = 136

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


def _read32_le(buf: bytes | bytearray | memoryview, off: int) -> int:
    return int.from_bytes(buf[off : off + 4], "little")


def _read64_le(buf: bytes | bytearray | memoryview, off: int) -> int:
    return int.from_bytes(buf[off : off + 8], "little")


def _rotl64(val: int, bits: int) -> int:
    v = val & _MASK64
    return ((v << bits) | (v >> (64 - bits))) & _MASK64


def _bswap64(val: int) -> int:
    v = val & _MASK64
    return (
        ((v & 0x00000000000000FF) << 56)
        | ((v & 0x000000000000FF00) << 40)
        | ((v & 0x0000000000FF0000) << 24)
        | ((v & 0x00000000FF000000) << 8)
        | ((v & 0x000000FF00000000) >> 8)
        | ((v & 0x0000FF0000000000) >> 24)
        | ((v & 0x00FF000000000000) >> 40)
        | ((v & 0xFF00000000000000) >> 56)
    )


def _xorshift64(val: int, shift: int) -> int:
    v = val & _MASK64
    return v ^ (v >> shift)


def _xxh64_avalanche(val: int) -> int:
    h = val & _MASK64
    h ^= h >> 33
    h = (h * _PRIME64_2) & _MASK64
    h ^= h >> 29
    h = (h * _PRIME64_3) & _MASK64
    h ^= h >> 32
    return h


def _xxh3_avalanche(val: int) -> int:
    h = _xorshift64(val, 37)
    h = (h * 0x165667919E3779F9) & _MASK64
    return _xorshift64(h, 32)


def _strong_avalanche(val: int, length: int) -> int:
    h = val & _MASK64
    h ^= _rotl64(h, 49) ^ _rotl64(h, 24)
    h = (h * 0x9FB21C651E98DF25) & _MASK64
    h ^= ((h >> 35) + length) & _MASK64
    h = (h * 0x9FB21C651E98DF25) & _MASK64
    return _xorshift64(h, 28)


def _mul128_fold64(lhs: int, rhs: int) -> int:
    prod = (lhs & _MASK64) * (rhs & _MASK64)
    lo = prod & _MASK64
    hi = (prod >> 64) & _MASK64
    return lo ^ hi


def _mix16b(buf: memoryview, in_off: int, secret: bytes, sec_off: int) -> int:
    in_lo = _read64_le(buf, in_off) ^ _read64_le(secret, sec_off)
    in_hi = _read64_le(buf, in_off + 8) ^ _read64_le(secret, sec_off + 8)
    return _mul128_fold64(in_lo, in_hi)


def _hash_0_to_16(buf: memoryview, secret: bytes) -> int:
    length = len(buf)
    if length > 8:
        flip1 = _read64_le(secret, 24) ^ _read64_le(secret, 32)
        flip2 = _read64_le(secret, 40) ^ _read64_le(secret, 48)
        in_lo = _read64_le(buf, 0) ^ flip1
        in_hi = _read64_le(buf, length - 8) ^ flip2
        acc = (length + _bswap64(in_lo) + in_hi + _mul128_fold64(in_lo, in_hi)) & _MASK64
        return _xxh3_avalanche(acc)
    if length >= 4:
        in1 = _read32_le(buf, 0)
        in2 = _read32_le(buf, length - 4)
        flip = _read64_le(secret, 8) ^ _read64_le(secret, 16)
        in64 = (in2 + (in1 << 32)) & _MASK64
        return _strong_avalanche(in64 ^ flip, length)
    if length > 0:
        c1 = buf[0]
        c2 = buf[length >> 1]
        c3 = buf[length - 1]
        combo = (c1 << 16) | (c2 << 24) | c3 | (length << 8)
        flip = (_read32_le(secret, 0) ^ _read32_le(secret, 4)) & _MASK32
        return _xxh64_avalanche(combo ^ flip)
    return _xxh64_avalanche(_read64_le(secret, 56) ^ _read64_le(secret, 64))


def _hash_17_to_128(buf: memoryview, secret: bytes) -> int:
    length = len(buf)
    acc = (length * _PRIME64_1) & _MASK64
    if length > 32:
        if length > 64:
            if length > 96:
                acc = (acc + _mix16b(buf, 48, secret, 96)) & _MASK64
                acc = (acc + _mix16b(buf, length - 64, secret, 112)) & _MASK64
            acc = (acc + _mix16b(buf, 32, secret, 64)) & _MASK64
            acc = (acc + _mix16b(buf, length - 48, secret, 80)) & _MASK64
        acc = (acc + _mix16b(buf, 16, secret, 32)) & _MASK64
        acc = (acc + _mix16b(buf, length - 32, secret, 48)) & _MASK64
    acc = (acc + _mix16b(buf, 0, secret, 0)) & _MASK64
    acc = (acc + _mix16b(buf, length - 16, secret, 16)) & _MASK64
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


def _accumulate_512(
    acc: list[int], buf: memoryview, in_off: int, secret: bytes, sec_off: int
) -> None:
    for i in range(_ACC_NB):
        data_val = _read64_le(buf, in_off + 8 * i)
        data_key = data_val ^ _read64_le(secret, sec_off + 8 * i)
        acc[i ^ 1] = (acc[i ^ 1] + data_val) & _MASK64
        prod = (data_key & _MASK32) * ((data_key >> 32) & _MASK32)
        acc[i] = (acc[i] + prod) & _MASK64


def _scramble_acc(acc: list[int], secret: bytes, sec_off: int) -> None:
    for i in range(_ACC_NB):
        key = _read64_le(secret, sec_off + 8 * i)
        val = _xorshift64(acc[i], 47) ^ key
        acc[i] = (val * _PRIME32_1) & _MASK64


def _hash_long(buf: memoryview, secret: bytes) -> int:
    acc = [
        _PRIME32_3,
        _PRIME64_1,
        _PRIME64_2,
        _PRIME64_3,
        _PRIME64_4,
        _PRIME32_2,
        _PRIME64_5,
        _PRIME32_1,
    ]
    length = len(buf)
    nb_stripes_per_block = (len(secret) - _STRIPE_LEN) // _SECRET_CONSUME_RATE
    block_len = _STRIPE_LEN * nb_stripes_per_block
    nb_blocks = (length - 1) // block_len

    for n in range(nb_blocks):
        base = n * block_len
        for s in range(nb_stripes_per_block):
            _accumulate_512(acc, buf, base + s * _STRIPE_LEN, secret, s * _SECRET_CONSUME_RATE)
        _scramble_acc(acc, secret, len(secret) - _STRIPE_LEN)

    nb_stripes = (length - 1 - block_len * nb_blocks) // _STRIPE_LEN
    tail_base = nb_blocks * block_len
    for s in range(nb_stripes):
        _accumulate_512(acc, buf, tail_base + s * _STRIPE_LEN, secret, s * _SECRET_CONSUME_RATE)
    _accumulate_512(
        acc,
        buf,
        length - _STRIPE_LEN,
        secret,
        len(secret) - _STRIPE_LEN - _SECRET_LASTACC_START,
    )

    result = (length * _PRIME64_1) & _MASK64
    for i in range(4):
        sec_base = _SECRET_MERGEACCS_START + 16 * i
        lhs = acc[2 * i] ^ _read64_le(secret, sec_base)
        rhs = acc[2 * i + 1] ^ _read64_le(secret, sec_base + 8)
        result = (result + _mul128_fold64(lhs, rhs)) & _MASK64
    return _xxh3_avalanche(result)


def xxh3_64(data: bytes | bytearray | memoryview) -> int:
    """Compute unsigned 64-bit XXH3 hash (seed = 0) matching xxhash-rust."""
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
    """Compute unsigned 64-bit XXH3 hash of a UTF-8 encoded string."""
    return xxh3_64(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Dictionary loading and bounded Zstd decompression
# ---------------------------------------------------------------------------


def load_agent_zstd_dict(dict_bytes: bytes | None = None) -> tuple[zstd.ZstdDict, bytes]:
    """Load the AI Agent Zstd dictionary (explicit bytes, PULSE_AGENT_DICT_PATH, or default)."""
    if dict_bytes is not None:
        raw = bytes(dict_bytes)
    else:
        env_path = os.environ.get("PULSE_AGENT_DICT_PATH", "").strip()
        if env_path:
            try:
                raw = Path(env_path).read_bytes()
            except OSError as exc:
                raise PlstDictError(
                    f"Failed to read PULSE_AGENT_DICT_PATH {env_path!r}: {exc}"
                ) from exc
        else:
            raw = DEFAULT_AGENT_DICT_BYTES
    if not raw:
        raise PlstDictError("Agent Zstd dictionary bytes cannot be empty")
    is_raw = not raw.startswith(_ZSTD_FORMATTED_MAGIC)
    try:
        zdict = zstd.ZstdDict(raw, is_raw=is_raw)
    except (zstd.ZstdError, ValueError, TypeError) as exc:
        raise PlstDictError(f"Failed to load Agent Zstd dictionary: {exc}") from exc
    return zdict, raw


def decompress_zstd_bounded(
    comp: bytes | bytearray | memoryview,
    max_bytes: int = MAX_PLST_UNCOMP_BYTES,
    zdict: zstd.ZstdDict | None = None,
) -> bytes:
    """Decompress a Zstd payload using bounded streaming chunks to prevent Zstd bombs."""
    if max_bytes < 0 or max_bytes > MAX_PLST_UNCOMP_BYTES:
        raise PlstLimitError(f"Invalid max_bytes ceiling: {max_bytes}")
    comp_bytes = bytes(comp)
    if not comp_bytes:
        raise PlstCorruptError("Compressed Zstd payload is empty")

    dec = zstd.ZstdDecompressor(zstd_dict=zdict)
    chunks: list[bytes] = []
    total = 0
    chunk_cap = 65536
    first = True

    try:
        while not dec.eof:
            remaining = max_bytes - total
            step_limit = min(chunk_cap, remaining + 1)
            out = dec.decompress(comp_bytes if first else b"", max_length=step_limit)
            first = False
            if out:
                total += len(out)
                if total > max_bytes:
                    raise PlstLimitError(
                        f"Decompressed payload exceeded limit of {max_bytes} bytes"
                    )
                chunks.append(out)
            if not dec.eof and not out and dec.needs_input:
                raise PlstCorruptError("Truncated Zstd stream before EOF")
    except PlstError:
        raise
    except zstd.ZstdError as exc:
        raise PlstCorruptError(f"Zstd decompression failed: {exc}") from exc

    if dec.unused_data:
        raise PlstCorruptError("Trailing garbage bytes after Zstd frame")

    return b"".join(chunks)


# ---------------------------------------------------------------------------
# Low-level binary writer / reader helpers for the 5 columnar sections
# ---------------------------------------------------------------------------


class _BufReader:
    """Bounds-checked little-endian binary reader."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._mv = memoryview(data)
        self._pos = 0
        self._end = len(data)

    @property
    def pos(self) -> int:
        return self._pos

    @property
    def remaining(self) -> int:
        return self._end - self._pos

    def read_bytes(self, length: int) -> bytes:
        if length < 0 or self._pos + length > self._end:
            raise PlstCorruptError(
                f"Truncated section payload: need {length} bytes at offset {self._pos}, "
                f"only {self.remaining} available"
            )
        out = bytes(self._mv[self._pos : self._pos + length])
        self._pos += length
        return out

    def unpack(self, fmt: struct.Struct) -> tuple[Any, ...]:
        if self._pos + fmt.size > self._end:
            raise PlstCorruptError(
                f"Truncated section field ({fmt.format}): need {fmt.size} bytes at offset {self._pos}, "
                f"only {self.remaining} available"
            )
        vals = fmt.unpack_from(self._mv, self._pos)
        self._pos += fmt.size
        return vals

    def u8(self) -> int:
        return self.unpack(_U8)[0]

    def u16(self) -> int:
        return self.unpack(_U16)[0]

    def u32(self) -> int:
        return self.unpack(_U32)[0]

    def i32(self) -> int:
        return self.unpack(_I32)[0]

    def u64(self) -> int:
        return self.unpack(_U64)[0]

    def i64(self) -> int:
        return self.unpack(_I64)[0]

    def f64(self) -> float:
        return self.unpack(_F64)[0]

    def u8_vec(self, count: int) -> list[int]:
        if count > self.remaining:
            raise PlstCorruptError(f"Truncated u8 array of {count} items")
        return [self.u8() for _ in range(count)]

    def u16_vec(self, count: int) -> list[int]:
        if count * 2 > self.remaining:
            raise PlstCorruptError(f"Truncated u16 array of {count} items")
        return [self.u16() for _ in range(count)]

    def u32_vec(self, count: int) -> list[int]:
        if count * 4 > self.remaining:
            raise PlstCorruptError(f"Truncated u32 array of {count} items")
        return [self.u32() for _ in range(count)]

    def i32_vec(self, count: int) -> list[int]:
        if count * 4 > self.remaining:
            raise PlstCorruptError(f"Truncated i32 array of {count} items")
        return [self.i32() for _ in range(count)]

    def i64_vec(self, count: int) -> list[int]:
        if count * 8 > self.remaining:
            raise PlstCorruptError(f"Truncated i64 array of {count} items")
        if count == 0:
            return []
        vals = list(struct.unpack_from(f"<{count}q", self._mv, self._pos))
        self._pos += count * 8
        return vals

    def u64_vec(self, count: int) -> list[int]:
        if count * 8 > self.remaining:
            raise PlstCorruptError(f"Truncated u64 array of {count} items")
        if count == 0:
            return []
        vals = list(struct.unpack_from(f"<{count}Q", self._mv, self._pos))
        self._pos += count * 8
        return vals

    def f64_vec(self, count: int) -> list[float]:
        if count * 8 > self.remaining:
            raise PlstCorruptError(f"Truncated f64 array of {count} items")
        return [self.f64() for _ in range(count)]


_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_I32 = struct.Struct("<i")
_U64 = struct.Struct("<Q")
_I64 = struct.Struct("<q")
_F64 = struct.Struct("<d")
_I64_MAX: int = (1 << 63) - 1


def _wrap_i64(val: int) -> int:
    return ((int(val) + (1 << 63)) & _MASK64) - (1 << 63)


def _delta_encode_i64(values: list[int]) -> list[int]:
    out: list[int] = []
    prev = 0
    for v in values:
        cur = _wrap_i64(int(v))
        out.append(_wrap_i64(cur - prev))
        prev = cur
    return out


def _delta_decode_i64(deltas: list[int]) -> list[int]:
    out: list[int] = []
    prev = 0
    for d in deltas:
        prev = _wrap_i64(prev + d)
        out.append(prev)
    return out


def _f64_to_u64_bits(val: float) -> int:
    return _U64.unpack(_F64.pack(float(val)))[0]


def _u64_bits_to_f64(bits: int) -> float:
    return _F64.unpack(_U64.pack(int(bits) & _MASK64))[0]


def _int_or(d: dict[str, Any], key: str, default: int = 0) -> int:
    val = d.get(key)
    return default if val is None else int(val)


def _float_or(d: dict[str, Any], key: str, default: float = 0.0) -> float:
    val = d.get(key)
    return default if val is None else float(val)


def _opt_float(d: dict[str, Any], key: str) -> float | None:
    val = d.get(key)
    return None if val is None else float(val)


def _usd_to_cost_nanos(usd: float) -> int:
    if not math.isfinite(usd):
        return _I64_MAX if usd > 0.0 else 0
    if usd <= 0.0:
        return 0
    scaled = round(usd * 1_000_000_000)
    return _I64_MAX if scaled >= _I64_MAX else int(scaled)


def _pack_u32_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}I", *values))


def _pack_i32_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}i", *values))


def _pack_u64_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}Q", *values))


def _pack_i64_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}q", *values))


def _pack_f64_vec(out: bytearray, values: list[float]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}d", *values))


def _pack_u16_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(struct.pack(f"<{len(values)}H", *values))


def _pack_u8_vec(out: bytearray, values: list[int]) -> None:
    if values:
        out.extend(bytes(values))


def _serialize_blob_value(value: Any) -> tuple[int, bytes] | None:
    """Convert a trace/span JSON payload field into `(encoding_tag, raw_bytes)` or None."""
    if value is None:
        return None
    if isinstance(value, str):
        return (BLOB_ENC_UTF8_TEXT, value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value).decode("utf-8").encode("utf-8")
        return (BLOB_ENC_UTF8_TEXT, raw)
    return (
        BLOB_ENC_JSON_UTF8,
        json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8"),
    )


def _deserialize_blob_value(encoding_tag: int, raw: bytes) -> Any:
    if encoding_tag == BLOB_ENC_UTF8_TEXT:
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PlstCorruptError(f"Invalid UTF-8 in text blob: {exc}") from exc
    if encoding_tag == BLOB_ENC_JSON_UTF8:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PlstCorruptError(f"Invalid UTF-8 in JSON blob: {exc}") from exc
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise PlstCorruptError(f"Invalid JSON in blob table: {exc}") from exc
    raise PlstCorruptError(f"Unknown blob encoding tag: {encoding_tag}")


# ---------------------------------------------------------------------------
# 5 Columnar Sections Encoder / Decoder (isolated for protocol.rs alignment)
# ---------------------------------------------------------------------------


def _encode_sections_with_stats(
    batch: dict[str, Any],
    *,
    blob_hash_fn: Callable[[bytes], int] | None = None,
) -> tuple[bytes, BlobDedupStats]:
    """Encode a TraceIngestBatch dict into the 5 columnar PLST sections."""
    hash_fn = xxh3_64 if blob_hash_fn is None else blob_hash_fn
    traces: list[dict[str, Any]] = list(batch.get("traces", []))
    spans: list[dict[str, Any]] = list(batch.get("spans", []))

    if len(traces) > MAX_PLST_TRACES:
        raise PlstLimitError(f"Trace count {len(traces)} exceeds MAX_PLST_TRACES ({MAX_PLST_TRACES})")
    if len(spans) > MAX_PLST_SPANS:
        raise PlstLimitError(f"Span count {len(spans)} exceeds MAX_PLST_SPANS ({MAX_PLST_SPANS})")

    strings: list[str] = []
    str_chunks: list[bytes] = []
    str_offsets: list[int] = [0]
    str_cursor = 0
    str_to_idx: dict[str, int] = {}

    def intern_str(val: str | None) -> int:
        nonlocal str_cursor
        if val is None:
            return NO_INDEX
        s = str(val)
        encoded = s.encode("utf-8")
        if len(encoded) > MAX_PLST_SINGLE_STRING_BYTES:
            raise PlstLimitError(
                f"String table entry exceeds {MAX_PLST_SINGLE_STRING_BYTES} bytes ({len(encoded)})"
            )
        idx = str_to_idx.get(s)
        if idx is None:
            if len(strings) >= MAX_PLST_STRINGS:
                raise PlstLimitError(f"String table exceeds MAX_PLST_STRINGS ({MAX_PLST_STRINGS})")
            idx = len(strings)
            if idx == NO_INDEX or str_cursor + len(encoded) > 0xFFFFFFFF:
                raise PlstLimitError("String table exceeds u32 index/offset range")
            strings.append(s)
            str_chunks.append(encoded)
            str_cursor += len(encoded)
            str_offsets.append(str_cursor)
            str_to_idx[s] = idx
        return idx

    blobs: list[tuple[int, int, bytes]] = []  # (hash, encoding_tag, raw_bytes)
    blob_offsets: list[int] = [0]
    blob_cursor = 0
    hash_buckets: dict[int, list[int]] = {}
    dedup_stats = BlobDedupStats()

    def intern_blob(val: Any) -> int:
        nonlocal blob_cursor
        serialized = _serialize_blob_value(val)
        if serialized is None:
            return NO_INDEX
        encoding_tag, raw_bytes = serialized
        dedup_stats.total_blobs += 1
        h = hash_fn(raw_bytes) & _MASK64
        candidates = hash_buckets.get(h)
        if candidates:
            for existing_idx in candidates:
                dedup_stats.collision_checks += 1
                _, ex_tag, ex_bytes = blobs[existing_idx]
                if ex_tag == encoding_tag and len(ex_bytes) == len(raw_bytes) and ex_bytes == raw_bytes:
                    dedup_stats.deduped_blobs += 1
                    dedup_stats.bytes_saved += len(raw_bytes)
                    return existing_idx
        if len(blobs) >= MAX_PLST_BLOBS:
            raise PlstLimitError(f"Blob table exceeds MAX_PLST_BLOBS ({MAX_PLST_BLOBS})")
        new_idx = len(blobs)
        if new_idx == NO_INDEX or blob_cursor + len(raw_bytes) > 0xFFFFFFFF:
            raise PlstLimitError("Blob table exceeds u32 index/offset range")
        blobs.append((h, encoding_tag, raw_bytes))
        blob_cursor += len(raw_bytes)
        blob_offsets.append(blob_cursor)
        hash_buckets.setdefault(h, []).append(new_idx)
        dedup_stats.unique_blobs += 1
        return new_idx

    trace_idx_by_id: dict[str, int] = {}
    for idx, tr in enumerate(traces):
        tid = str(tr.get("trace_id", ""))
        trace_idx_by_id.setdefault(tid, idx)

    # Pre-collect Section 3 (Trace Header/Delta Rows)
    tr_trace_id: list[int] = []
    tr_project_id: list[int] = []
    tr_session_id: list[int] = []
    tr_user_id: list[int] = []
    tr_workflow_id: list[int] = []
    tr_parent_trace_id: list[int] = []
    tr_agent_name: list[int] = []
    tr_agent_version: list[int] = []
    tr_prompt_version: list[int] = []
    tr_model_cfg_ver: list[int] = []
    tr_tool_cfg_ver: list[int] = []
    tr_app_release: list[int] = []
    tr_environment: list[int] = []

    tr_status: list[int] = []
    tr_failure_source: list[int] = []
    tr_start_ts: list[int] = []
    tr_end_present: list[int] = []
    tr_end_offset: list[int] = []
    tr_duration_ms: list[int] = []

    tr_span_count: list[int] = []
    tr_llm_count: list[int] = []
    tr_tool_count: list[int] = []
    tr_err_span_count: list[int] = []
    tr_in_tok: list[int] = []
    tr_out_tok: list[int] = []
    tr_cached_tok: list[int] = []
    tr_tot_tok: list[int] = []
    tr_cost_nanos: list[int] = []

    tr_pri_model: list[int] = []
    tr_pri_provider: list[int] = []
    tr_has_avg_eval: list[int] = []
    tr_avg_eval_bits: list[int] = []
    tr_has_pass_rate: list[int] = []
    tr_pass_rate_bits: list[int] = []
    tr_has_loop: list[int] = []
    tr_err_type: list[int] = []
    tr_err_msg: list[int] = []

    tr_tag_counts: list[int] = []
    tr_tag_indices: list[int] = []
    tr_in_blob: list[int] = []
    tr_out_blob: list[int] = []
    tr_meta_blob: list[int] = []

    for tr in traces:
        tr_trace_id.append(intern_str(tr.get("trace_id", "")))
        tr_project_id.append(intern_str(tr.get("project_id", "")))
        tr_session_id.append(intern_str(tr.get("session_id")))
        tr_user_id.append(intern_str(tr.get("user_id")))
        tr_workflow_id.append(intern_str(tr.get("workflow_id")))
        tr_parent_trace_id.append(intern_str(tr.get("parent_trace_id")))
        tr_agent_name.append(intern_str(tr.get("agent_name", "default")))
        tr_agent_version.append(intern_str(tr.get("agent_version", "unversioned")))
        tr_prompt_version.append(intern_str(tr.get("prompt_version")))
        tr_model_cfg_ver.append(intern_str(tr.get("model_config_version")))
        tr_tool_cfg_ver.append(intern_str(tr.get("tool_config_version")))
        tr_app_release.append(intern_str(tr.get("app_release")))
        tr_environment.append(intern_str(tr.get("environment", "production")))

        tr_status.append(_TRACE_STATUS_TO_U8.get(str(tr.get("status", "ok")).lower(), 0))
        fs = tr.get("failure_source")
        tr_failure_source.append(0 if fs is None else _FAILURE_SOURCE_TO_U8.get(str(fs).lower(), 6))
        start_ts = _int_or(tr, "start_ts_ms", 0)
        tr_start_ts.append(start_ts)
        end_ts = tr.get("end_ts_ms")
        if end_ts is not None:
            tr_end_present.append(1)
            tr_end_offset.append(_wrap_i64(int(end_ts) - start_ts))
        else:
            tr_end_present.append(0)
            tr_end_offset.append(0)
        tr_duration_ms.append(_int_or(tr, "duration_ms", 0))

        tr_span_count.append(_int_or(tr, "span_count", 0))
        tr_llm_count.append(_int_or(tr, "llm_call_count", 0))
        tr_tool_count.append(_int_or(tr, "tool_call_count", 0))
        tr_err_span_count.append(_int_or(tr, "error_span_count", 0))
        tr_in_tok.append(_int_or(tr, "input_tokens", 0))
        tr_out_tok.append(_int_or(tr, "output_tokens", 0))
        tr_cached_tok.append(_int_or(tr, "cached_tokens", 0))
        tr_tot_tok.append(_int_or(tr, "total_tokens", 0))
        cost_nanos = _int_or(tr, "total_cost_nanos", 0)
        if cost_nanos == 0:
            cost_nanos = _usd_to_cost_nanos(_float_or(tr, "total_cost_usd", 0.0))
        tr_cost_nanos.append(cost_nanos)

        tr_pri_model.append(intern_str(tr.get("primary_model")))
        tr_pri_provider.append(intern_str(tr.get("primary_provider")))
        avg_eval = _opt_float(tr, "avg_eval_score")
        if avg_eval is not None:
            tr_has_avg_eval.append(1)
            tr_avg_eval_bits.append(_f64_to_u64_bits(avg_eval))
        else:
            tr_has_avg_eval.append(0)
            tr_avg_eval_bits.append(0)
        pass_rate = _opt_float(tr, "eval_pass_rate")
        if pass_rate is not None:
            tr_has_pass_rate.append(1)
            tr_pass_rate_bits.append(_f64_to_u64_bits(pass_rate))
        else:
            tr_has_pass_rate.append(0)
            tr_pass_rate_bits.append(0)
        tr_has_loop.append(1 if tr.get("has_loop_warning") else 0)
        tr_err_type.append(intern_str(tr.get("error_type")))
        tr_err_msg.append(intern_str(tr.get("error_message")))

        tags = list(tr.get("tags") or [])
        if len(tags) > 0xFFFF:
            raise PlstLimitError(f"Trace tag count {len(tags)} exceeds u16::MAX")
        tr_tag_counts.append(len(tags))
        for tag in tags:
            tr_tag_indices.append(intern_str(str(tag)))
        tr_in_blob.append(intern_blob(tr.get("input")))
        tr_out_blob.append(intern_blob(tr.get("output")))
        tr_meta_blob.append(intern_blob(tr.get("metadata")))

    # Pre-collect Section 4 (Span Rows) and Section 5 (Columnar Span Events)
    sp_trace_idx: list[int] = []
    sp_trace_id: list[int] = []
    sp_project_id: list[int] = []
    sp_span_id: list[int] = []
    sp_parent_span_id: list[int] = []
    sp_linked_counts: list[int] = []
    sp_linked_indices: list[int] = []
    sp_depth: list[int] = []

    sp_name: list[int] = []
    sp_type: list[int] = []
    sp_phase: list[int] = []
    sp_status: list[int] = []
    sp_start_ts: list[int] = []
    sp_end_present: list[int] = []
    sp_end_offset: list[int] = []
    sp_duration_ms: list[int] = []

    sp_provider: list[int] = []
    sp_model: list[int] = []
    sp_model_id: list[int] = []
    sp_model_version: list[int] = []
    sp_prompt_tpl_id: list[int] = []
    sp_prompt_tpl_ver: list[int] = []
    sp_in_tok: list[int] = []
    sp_out_tok: list[int] = []
    sp_cached_tok: list[int] = []
    sp_tot_tok: list[int] = []
    sp_cost_nanos: list[int] = []
    sp_temp_present: list[int] = []
    sp_temp_bits: list[int] = []
    sp_finish_reason: list[int] = []
    sp_model_params_blob: list[int] = []
    sp_tool_name: list[int] = []
    sp_tool_version: list[int] = []
    sp_tool_retry: list[int] = []

    sp_err_type: list[int] = []
    sp_err_msg: list[int] = []
    sp_err_stack: list[int] = []
    sp_in_blob: list[int] = []
    sp_out_blob: list[int] = []
    sp_attrs_blob: list[int] = []
    sp_meta_blob: list[int] = []

    ev_span_idx: list[int] = []
    ev_ts_ms: list[int] = []
    ev_name_idx: list[int] = []
    ev_attrs_blob: list[int] = []

    for s_idx, sp in enumerate(spans):
        tid_str = str(sp.get("trace_id", ""))
        sp_trace_idx.append(trace_idx_by_id.get(tid_str, NO_INDEX))
        sp_trace_id.append(intern_str(tid_str))
        sp_project_id.append(intern_str(sp.get("project_id", "")))
        sp_span_id.append(intern_str(sp.get("span_id", "")))
        sp_parent_span_id.append(intern_str(sp.get("parent_span_id")))
        linked = list(sp.get("linked_span_ids") or [])
        if len(linked) > 0xFFFF:
            raise PlstLimitError(f"Span linked_span_ids count {len(linked)} exceeds u16::MAX")
        sp_linked_counts.append(len(linked))
        for lid in linked:
            sp_linked_indices.append(intern_str(str(lid)))
        sp_depth.append(_int_or(sp, "depth", 0))

        sp_name.append(intern_str(sp.get("name", "span")))
        sp_type.append(_SPAN_TYPE_TO_U8.get(str(sp.get("span_type", "custom")).lower(), 9))
        sp_phase.append(_SPAN_PHASE_TO_U8.get(str(sp.get("span_phase", "final")).lower(), 2))
        sp_status.append(_TRACE_STATUS_TO_U8.get(str(sp.get("status", "ok")).lower(), 0))
        start_ts = _int_or(sp, "start_ts_ms", 0)
        sp_start_ts.append(start_ts)
        end_ts = sp.get("end_ts_ms")
        if end_ts is not None:
            sp_end_present.append(1)
            sp_end_offset.append(_wrap_i64(int(end_ts) - start_ts))
        else:
            sp_end_present.append(0)
            sp_end_offset.append(0)
        sp_duration_ms.append(_int_or(sp, "duration_ms", 0))

        sp_provider.append(intern_str(sp.get("provider")))
        sp_model.append(intern_str(sp.get("model")))
        sp_model_id.append(intern_str(sp.get("model_id")))
        sp_model_version.append(intern_str(sp.get("model_version")))
        sp_prompt_tpl_id.append(intern_str(sp.get("prompt_template_id")))
        sp_prompt_tpl_ver.append(intern_str(sp.get("prompt_template_version")))
        sp_in_tok.append(_int_or(sp, "input_tokens", 0))
        sp_out_tok.append(_int_or(sp, "output_tokens", 0))
        sp_cached_tok.append(_int_or(sp, "cached_tokens", 0))
        sp_tot_tok.append(_int_or(sp, "total_tokens", 0))
        cost_nanos = _int_or(sp, "cost_nanos", 0)
        if cost_nanos == 0:
            cost_nanos = _usd_to_cost_nanos(_float_or(sp, "cost_usd", 0.0))
        sp_cost_nanos.append(cost_nanos)

        temp = _opt_float(sp, "temperature")
        if temp is not None:
            sp_temp_present.append(1)
            sp_temp_bits.append(_f64_to_u64_bits(temp))
        else:
            sp_temp_present.append(0)
            sp_temp_bits.append(0)
        sp_finish_reason.append(intern_str(sp.get("finish_reason")))
        sp_model_params_blob.append(intern_blob(sp.get("model_params")))
        sp_tool_name.append(intern_str(sp.get("tool_name")))
        sp_tool_version.append(intern_str(sp.get("tool_version")))
        sp_tool_retry.append(_int_or(sp, "tool_retry_count", 0))

        sp_err_type.append(intern_str(sp.get("error_type")))
        sp_err_msg.append(intern_str(sp.get("error_message")))
        sp_err_stack.append(intern_str(sp.get("error_stack")))
        sp_in_blob.append(intern_blob(sp.get("input")))
        sp_out_blob.append(intern_blob(sp.get("output")))
        sp_attrs_blob.append(intern_blob(sp.get("attributes")))
        sp_meta_blob.append(intern_blob(sp.get("metadata")))

        events = list(sp.get("events") or [])
        for ev in events:
            if len(ev_span_idx) >= MAX_PLST_EVENTS:
                raise PlstLimitError(f"Span events exceed MAX_PLST_EVENTS ({MAX_PLST_EVENTS})")
            ev_span_idx.append(s_idx)
            ev_ts_ms.append(_int_or(ev, "timestamp_ms", 0))
            ev_name_idx.append(intern_str(ev.get("name", "event")))
            ev_attrs_blob.append(intern_blob(ev.get("attributes")))

    out = bytearray()

    # Section 1: String Table
    out.extend(_U32.pack(len(strings)))
    _pack_u32_vec(out, str_offsets)
    for b in str_chunks:
        out.extend(b)

    # Section 2: Content-Addressed Blob Table (Columnar)
    out.extend(_U32.pack(len(blobs)))
    _pack_u64_vec(out, [b[0] for b in blobs])
    _pack_u8_vec(out, [b[1] for b in blobs])
    _pack_u32_vec(out, blob_offsets)
    for _, _, raw_b in blobs:
        out.extend(raw_b)

    # Section 3: Trace Header / Delta Rows
    out.extend(_U32.pack(len(traces)))
    for vec in (
        tr_trace_id,
        tr_project_id,
        tr_session_id,
        tr_user_id,
        tr_workflow_id,
        tr_parent_trace_id,
        tr_agent_name,
        tr_agent_version,
        tr_prompt_version,
        tr_model_cfg_ver,
        tr_tool_cfg_ver,
        tr_app_release,
        tr_environment,
    ):
        _pack_u32_vec(out, vec)
    _pack_u8_vec(out, tr_status)
    _pack_u8_vec(out, tr_failure_source)
    _pack_i64_vec(out, _delta_encode_i64(tr_start_ts))
    _pack_u8_vec(out, tr_end_present)
    _pack_i64_vec(out, tr_end_offset)
    _pack_i64_vec(out, tr_duration_ms)
    _pack_i32_vec(out, tr_span_count)
    _pack_i32_vec(out, tr_llm_count)
    _pack_i32_vec(out, tr_tool_count)
    _pack_i32_vec(out, tr_err_span_count)
    _pack_i64_vec(out, tr_in_tok)
    _pack_i64_vec(out, tr_out_tok)
    _pack_i64_vec(out, tr_cached_tok)
    _pack_i64_vec(out, tr_tot_tok)
    _pack_i64_vec(out, tr_cost_nanos)
    _pack_u32_vec(out, tr_pri_model)
    _pack_u32_vec(out, tr_pri_provider)
    _pack_u8_vec(out, tr_has_avg_eval)
    _pack_u64_vec(out, tr_avg_eval_bits)
    _pack_u8_vec(out, tr_has_pass_rate)
    _pack_u64_vec(out, tr_pass_rate_bits)
    _pack_u8_vec(out, tr_has_loop)
    _pack_u32_vec(out, tr_err_type)
    _pack_u32_vec(out, tr_err_msg)
    _pack_u16_vec(out, tr_tag_counts)
    out.extend(_U32.pack(len(tr_tag_indices)))
    _pack_u32_vec(out, tr_tag_indices)
    _pack_u32_vec(out, tr_in_blob)
    _pack_u32_vec(out, tr_out_blob)
    _pack_u32_vec(out, tr_meta_blob)

    # Section 4: Span Rows
    out.extend(_U32.pack(len(spans)))
    _pack_u32_vec(out, sp_trace_idx)
    _pack_u32_vec(out, sp_trace_id)
    _pack_u32_vec(out, sp_project_id)
    _pack_u32_vec(out, sp_span_id)
    _pack_u32_vec(out, sp_parent_span_id)
    _pack_u16_vec(out, sp_linked_counts)
    out.extend(_U32.pack(len(sp_linked_indices)))
    _pack_u32_vec(out, sp_linked_indices)
    _pack_i32_vec(out, sp_depth)
    _pack_u32_vec(out, sp_name)
    _pack_u8_vec(out, sp_type)
    _pack_u8_vec(out, sp_phase)
    _pack_u8_vec(out, sp_status)
    _pack_i64_vec(out, _delta_encode_i64(sp_start_ts))
    _pack_u8_vec(out, sp_end_present)
    _pack_i64_vec(out, sp_end_offset)
    _pack_i64_vec(out, sp_duration_ms)
    _pack_u32_vec(out, sp_provider)
    _pack_u32_vec(out, sp_model)
    _pack_u32_vec(out, sp_model_id)
    _pack_u32_vec(out, sp_model_version)
    _pack_u32_vec(out, sp_prompt_tpl_id)
    _pack_u32_vec(out, sp_prompt_tpl_ver)
    _pack_i64_vec(out, sp_in_tok)
    _pack_i64_vec(out, sp_out_tok)
    _pack_i64_vec(out, sp_cached_tok)
    _pack_i64_vec(out, sp_tot_tok)
    _pack_i64_vec(out, sp_cost_nanos)
    _pack_u8_vec(out, sp_temp_present)
    _pack_u64_vec(out, sp_temp_bits)
    _pack_u32_vec(out, sp_finish_reason)
    _pack_u32_vec(out, sp_model_params_blob)
    _pack_u32_vec(out, sp_tool_name)
    _pack_u32_vec(out, sp_tool_version)
    _pack_i32_vec(out, sp_tool_retry)
    _pack_u32_vec(out, sp_err_type)
    _pack_u32_vec(out, sp_err_msg)
    _pack_u32_vec(out, sp_err_stack)
    _pack_u32_vec(out, sp_in_blob)
    _pack_u32_vec(out, sp_out_blob)
    _pack_u32_vec(out, sp_attrs_blob)
    _pack_u32_vec(out, sp_meta_blob)

    # Section 5: Columnar Span Events
    out.extend(_U32.pack(len(ev_span_idx)))
    _pack_u32_vec(out, ev_span_idx)
    _pack_i64_vec(out, _delta_encode_i64(ev_ts_ms))
    _pack_u32_vec(out, ev_name_idx)
    _pack_u32_vec(out, ev_attrs_blob)

    return bytes(out), dedup_stats


def _encode_sections(batch: dict[str, Any]) -> bytes:
    """Encode a TraceIngestBatch dict into the 5 columnar PLST sections."""
    raw, _ = _encode_sections_with_stats(batch)
    return raw


def _decode_sections(raw: bytes) -> dict[str, Any]:
    """Decode and strictly validate the 5 columnar PLST sections."""
    r = _BufReader(raw)

    # Section 1: String Table
    str_count = r.u32()
    if str_count > MAX_PLST_STRINGS:
        raise PlstLimitError(f"String table count {str_count} exceeds MAX_PLST_STRINGS ({MAX_PLST_STRINGS})")
    offsets = r.u32_vec(str_count + 1)
    if offsets[0] != 0:
        raise PlstCorruptError(f"String table offsets[0] must be 0, got {offsets[0]}")
    str_blob_len = offsets[-1]
    str_blob = r.read_bytes(str_blob_len)
    strings: list[str] = []
    str_byte_lens: list[int] = []
    for i in range(str_count):
        start = offsets[i]
        end = offsets[i + 1]
        if end < start or end > str_blob_len:
            raise PlstCorruptError("Non-monotonic string table offsets")
        seg_len = end - start
        if seg_len > MAX_PLST_SINGLE_STRING_BYTES:
            raise PlstLimitError(
                f"String table entry {i} exceeds {MAX_PLST_SINGLE_STRING_BYTES} bytes ({seg_len})"
            )
        seg = str_blob[start:end]
        try:
            strings.append(seg.decode("utf-8", errors="strict"))
        except UnicodeDecodeError as exc:
            raise PlstCorruptError(f"Invalid UTF-8 in string table entry {i}: {exc}") from exc
        str_byte_lens.append(seg_len)

    str_materialized = 0

    def get_req_str(idx: int) -> str:
        nonlocal str_materialized
        if idx == NO_INDEX or idx >= str_count:
            raise PlstCorruptError(f"Invalid string index {idx} (count={str_count})")
        str_materialized += str_byte_lens[idx]
        if str_materialized > MAX_PLST_MATERIALIZED_BYTES:
            raise PlstLimitError("Materialized string bytes exceed 64 MiB anti-amplification cap")
        return strings[idx]

    def get_str(idx: int) -> str | None:
        if idx == NO_INDEX:
            return None
        return get_req_str(idx)

    # Section 2: Content-Addressed Blob Table (Columnar)
    blob_count = r.u32()
    if blob_count > MAX_PLST_BLOBS:
        raise PlstLimitError(f"Blob count {blob_count} exceeds MAX_PLST_BLOBS ({MAX_PLST_BLOBS})")
    blob_hashes = r.u64_vec(blob_count)
    blob_encodings = r.u8_vec(blob_count)
    for enc_tag in blob_encodings:
        if enc_tag not in (BLOB_ENC_JSON_UTF8, BLOB_ENC_UTF8_TEXT):
            raise PlstCorruptError(f"Unknown blob encoding tag: {enc_tag}")
    blob_offsets = r.u32_vec(blob_count + 1)
    if blob_offsets[0] != 0:
        raise PlstCorruptError(f"Blob table offsets[0] must be 0, got {blob_offsets[0]}")
    blob_total_bytes = blob_offsets[-1]
    blob_data = r.read_bytes(blob_total_bytes)
    blob_ranges: list[tuple[int, int]] = []
    for i in range(blob_count):
        start = blob_offsets[i]
        end = blob_offsets[i + 1]
        if end < start or end > blob_total_bytes:
            raise PlstCorruptError("Non-monotonic blob table offsets")
        raw_b = blob_data[start:end]
        actual_hash = xxh3_64(raw_b)
        if actual_hash != blob_hashes[i]:
            raise PlstCorruptError(
                f"Blob {i} xxh3_64 mismatch: expected {blob_hashes[i]:#018x}, got {actual_hash:#018x}"
            )
        blob_ranges.append((start, end))

    blob_cache: list[Any] = [None] * blob_count
    blob_cached_mask: list[bool] = [False] * blob_count
    blob_materialized = 0

    def get_blob(idx: int, default: Any = None) -> Any:
        nonlocal blob_materialized
        if idx == NO_INDEX:
            return default
        if idx >= blob_count:
            raise PlstCorruptError(f"Out-of-bounds blob index {idx} >= {blob_count}")
        start, end = blob_ranges[idx]
        blob_materialized += end - start
        if blob_materialized > MAX_PLST_MATERIALIZED_BYTES:
            raise PlstLimitError("Materialized blob bytes exceed 64 MiB anti-amplification cap")
        if not blob_cached_mask[idx]:
            blob_cache[idx] = _deserialize_blob_value(blob_encodings[idx], blob_data[start:end])
            blob_cached_mask[idx] = True
        return blob_cache[idx]

    # Section 3: Trace Header / Delta Rows
    trace_count = r.u32()
    if trace_count > MAX_PLST_TRACES:
        raise PlstLimitError(f"Trace count {trace_count} exceeds MAX_PLST_TRACES ({MAX_PLST_TRACES})")
    tr_trace_id = r.u32_vec(trace_count)
    tr_project_id = r.u32_vec(trace_count)
    tr_session_id = r.u32_vec(trace_count)
    tr_user_id = r.u32_vec(trace_count)
    tr_workflow_id = r.u32_vec(trace_count)
    tr_parent_trace_id = r.u32_vec(trace_count)
    tr_agent_name = r.u32_vec(trace_count)
    tr_agent_version = r.u32_vec(trace_count)
    tr_prompt_version = r.u32_vec(trace_count)
    tr_model_cfg_ver = r.u32_vec(trace_count)
    tr_tool_cfg_ver = r.u32_vec(trace_count)
    tr_app_release = r.u32_vec(trace_count)
    tr_environment = r.u32_vec(trace_count)

    tr_status = r.u8_vec(trace_count)
    tr_failure_source = r.u8_vec(trace_count)
    tr_start_ts = _delta_decode_i64(r.i64_vec(trace_count))
    tr_end_present = r.u8_vec(trace_count)
    tr_end_offset = r.i64_vec(trace_count)
    tr_duration_ms = r.i64_vec(trace_count)

    tr_span_count = r.i32_vec(trace_count)
    tr_llm_count = r.i32_vec(trace_count)
    tr_tool_count = r.i32_vec(trace_count)
    tr_err_span_count = r.i32_vec(trace_count)
    tr_in_tok = r.i64_vec(trace_count)
    tr_out_tok = r.i64_vec(trace_count)
    tr_cached_tok = r.i64_vec(trace_count)
    tr_tot_tok = r.i64_vec(trace_count)
    tr_cost_nanos = r.i64_vec(trace_count)

    tr_pri_model = r.u32_vec(trace_count)
    tr_pri_provider = r.u32_vec(trace_count)
    tr_has_avg_eval = r.u8_vec(trace_count)
    tr_avg_eval_bits = r.u64_vec(trace_count)
    tr_has_pass_rate = r.u8_vec(trace_count)
    tr_pass_rate_bits = r.u64_vec(trace_count)
    tr_has_loop = r.u8_vec(trace_count)
    tr_err_type = r.u32_vec(trace_count)
    tr_err_msg = r.u32_vec(trace_count)

    tr_tag_counts = r.u16_vec(trace_count)
    total_tr_tags = r.u32()
    if sum(tr_tag_counts) != total_tr_tags:
        raise PlstCorruptError(
            f"Trace tag count sum mismatch: sum={sum(tr_tag_counts)}, total={total_tr_tags}"
        )
    tr_tag_indices = r.u32_vec(total_tr_tags)
    tr_in_blob = r.u32_vec(trace_count)
    tr_out_blob = r.u32_vec(trace_count)
    tr_meta_blob = r.u32_vec(trace_count)

    traces: list[dict[str, Any]] = []
    tag_cursor = 0
    for i in range(trace_count):
        st = _U8_TO_TRACE_STATUS.get(tr_status[i])
        if st is None:
            raise PlstCorruptError(f"Invalid trace status discriminant: {tr_status[i]}")
        fs_code = tr_failure_source[i]
        if fs_code == 0:
            fs_val = None
        else:
            fs_val = _U8_TO_FAILURE_SOURCE.get(fs_code)
            if fs_val is None:
                raise PlstCorruptError(f"Invalid failure source discriminant: {fs_code}")
        has_end = tr_end_present[i]
        if has_end == 0:
            end_ts_val = None
        elif has_end == 1:
            end_ts_val = _wrap_i64(tr_start_ts[i] + tr_end_offset[i])
        else:
            raise PlstCorruptError(f"Invalid has_end_ts flag: {has_end}")

        has_avg = tr_has_avg_eval[i]
        if has_avg == 0:
            avg_eval_val = None
        elif has_avg == 1:
            avg_eval_val = _u64_bits_to_f64(tr_avg_eval_bits[i])
        else:
            raise PlstCorruptError(f"Invalid has_avg_eval_score flag: {has_avg}")

        has_pass = tr_has_pass_rate[i]
        if has_pass == 0:
            pass_rate_val = None
        elif has_pass == 1:
            pass_rate_val = _u64_bits_to_f64(tr_pass_rate_bits[i])
        else:
            raise PlstCorruptError(f"Invalid has_eval_pass_rate flag: {has_pass}")

        has_loop = tr_has_loop[i]
        if has_loop not in (0, 1):
            raise PlstCorruptError(f"Invalid has_loop_warning flag: {has_loop}")

        t_cnt = tr_tag_counts[i]
        tags = [get_req_str(tr_tag_indices[tag_cursor + j]) for j in range(t_cnt)]
        tag_cursor += t_cnt
        cost_nanos_val = tr_cost_nanos[i]
        traces.append(
            {
                "trace_id": get_req_str(tr_trace_id[i]),
                "project_id": get_req_str(tr_project_id[i]),
                "agent_name": get_req_str(tr_agent_name[i]),
                "agent_version": get_req_str(tr_agent_version[i]),
                "prompt_version": get_str(tr_prompt_version[i]),
                "model_config_version": get_str(tr_model_cfg_ver[i]),
                "tool_config_version": get_str(tr_tool_cfg_ver[i]),
                "app_release": get_str(tr_app_release[i]),
                "environment": get_req_str(tr_environment[i]),
                "session_id": get_str(tr_session_id[i]),
                "user_id": get_str(tr_user_id[i]),
                "workflow_id": get_str(tr_workflow_id[i]),
                "parent_trace_id": get_str(tr_parent_trace_id[i]),
                "status": st,
                "failure_source": fs_val,
                "start_ts_ms": tr_start_ts[i],
                "end_ts_ms": end_ts_val,
                "duration_ms": tr_duration_ms[i],
                "input": get_blob(tr_in_blob[i], None),
                "output": get_blob(tr_out_blob[i], None),
                "metadata": get_blob(tr_meta_blob[i], None),
                "error_type": get_str(tr_err_type[i]),
                "error_message": get_str(tr_err_msg[i]),
                "tags": tags,
                "span_count": tr_span_count[i],
                "llm_call_count": tr_llm_count[i],
                "tool_call_count": tr_tool_count[i],
                "error_span_count": tr_err_span_count[i],
                "input_tokens": tr_in_tok[i],
                "output_tokens": tr_out_tok[i],
                "cached_tokens": tr_cached_tok[i],
                "total_tokens": tr_tot_tok[i],
                "total_cost_nanos": cost_nanos_val,
                "total_cost_usd": cost_nanos_val / 1_000_000_000,
                "primary_model": get_str(tr_pri_model[i]),
                "primary_provider": get_str(tr_pri_provider[i]),
                "avg_eval_score": avg_eval_val,
                "eval_pass_rate": pass_rate_val,
                "has_loop_warning": bool(has_loop),
            }
        )

    # Section 4: Span Rows
    span_count = r.u32()
    if span_count > MAX_PLST_SPANS:
        raise PlstLimitError(f"Span count {span_count} exceeds MAX_PLST_SPANS ({MAX_PLST_SPANS})")
    sp_trace_idx = r.u32_vec(span_count)
    sp_trace_id = r.u32_vec(span_count)
    sp_project_id = r.u32_vec(span_count)
    sp_span_id = r.u32_vec(span_count)
    sp_parent_span_id = r.u32_vec(span_count)
    sp_linked_counts = r.u16_vec(span_count)
    total_linked = r.u32()
    if sum(sp_linked_counts) != total_linked:
        raise PlstCorruptError(
            f"Span linked_span_ids count sum mismatch: sum={sum(sp_linked_counts)}, total={total_linked}"
        )
    sp_linked_indices = r.u32_vec(total_linked)
    sp_depth = r.i32_vec(span_count)

    sp_name = r.u32_vec(span_count)
    sp_type = r.u8_vec(span_count)
    sp_phase = r.u8_vec(span_count)
    sp_status = r.u8_vec(span_count)
    sp_start_ts = _delta_decode_i64(r.i64_vec(span_count))
    sp_end_present = r.u8_vec(span_count)
    sp_end_offset = r.i64_vec(span_count)
    sp_duration_ms = r.i64_vec(span_count)

    sp_provider = r.u32_vec(span_count)
    sp_model = r.u32_vec(span_count)
    sp_model_id = r.u32_vec(span_count)
    sp_model_version = r.u32_vec(span_count)
    sp_prompt_tpl_id = r.u32_vec(span_count)
    sp_prompt_tpl_ver = r.u32_vec(span_count)
    sp_in_tok = r.i64_vec(span_count)
    sp_out_tok = r.i64_vec(span_count)
    sp_cached_tok = r.i64_vec(span_count)
    sp_tot_tok = r.i64_vec(span_count)
    sp_cost_nanos = r.i64_vec(span_count)
    sp_temp_present = r.u8_vec(span_count)
    sp_temp_bits = r.u64_vec(span_count)
    sp_finish_reason = r.u32_vec(span_count)
    sp_model_params_blob = r.u32_vec(span_count)
    sp_tool_name = r.u32_vec(span_count)
    sp_tool_version = r.u32_vec(span_count)
    sp_tool_retry = r.i32_vec(span_count)

    sp_err_type = r.u32_vec(span_count)
    sp_err_msg = r.u32_vec(span_count)
    sp_err_stack = r.u32_vec(span_count)
    sp_in_blob = r.u32_vec(span_count)
    sp_out_blob = r.u32_vec(span_count)
    sp_attrs_blob = r.u32_vec(span_count)
    sp_meta_blob = r.u32_vec(span_count)

    spans: list[dict[str, Any]] = []
    linked_cursor = 0
    for i in range(span_count):
        trace_id_val = get_req_str(sp_trace_id[i])
        tr_idx = sp_trace_idx[i]
        if tr_idx != NO_INDEX:
            if tr_idx >= trace_count or traces[tr_idx]["trace_id"] != trace_id_val:
                raise PlstCorruptError(f"Invalid span trace_idx {tr_idx} for trace_id {trace_id_val!r}")

        st = _U8_TO_TRACE_STATUS.get(sp_status[i])
        if st is None:
            raise PlstCorruptError(f"Invalid span status discriminant: {sp_status[i]}")
        stype = _U8_TO_SPAN_TYPE.get(sp_type[i])
        if stype is None:
            raise PlstCorruptError(f"Invalid span type discriminant: {sp_type[i]}")
        sphase = _U8_TO_SPAN_PHASE.get(sp_phase[i])
        if sphase is None:
            raise PlstCorruptError(f"Invalid span phase discriminant: {sp_phase[i]}")

        sp_has_end = sp_end_present[i]
        if sp_has_end == 0:
            sp_end_val = None
        elif sp_has_end == 1:
            sp_end_val = _wrap_i64(sp_start_ts[i] + sp_end_offset[i])
        else:
            raise PlstCorruptError(f"Invalid span has_end_ts flag: {sp_has_end}")

        sp_has_t = sp_temp_present[i]
        if sp_has_t == 0:
            temp_val = None
        elif sp_has_t == 1:
            temp_val = _u64_bits_to_f64(sp_temp_bits[i])
        else:
            raise PlstCorruptError(f"Invalid has_temperature flag: {sp_has_t}")

        l_cnt = sp_linked_counts[i]
        linked_ids = [get_req_str(sp_linked_indices[linked_cursor + j]) for j in range(l_cnt)]
        linked_cursor += l_cnt

        sp_cost_nanos_val = sp_cost_nanos[i]
        spans.append(
            {
                "span_id": get_req_str(sp_span_id[i]),
                "trace_id": trace_id_val,
                "project_id": get_req_str(sp_project_id[i]),
                "parent_span_id": get_str(sp_parent_span_id[i]),
                "linked_span_ids": linked_ids,
                "depth": sp_depth[i],
                "name": get_req_str(sp_name[i]),
                "span_type": stype,
                "span_phase": sphase,
                "status": st,
                "start_ts_ms": sp_start_ts[i],
                "end_ts_ms": sp_end_val,
                "duration_ms": sp_duration_ms[i],
                "provider": get_str(sp_provider[i]),
                "model": get_str(sp_model[i]),
                "model_id": get_str(sp_model_id[i]),
                "model_version": get_str(sp_model_version[i]),
                "prompt_template_id": get_str(sp_prompt_tpl_id[i]),
                "prompt_template_version": get_str(sp_prompt_tpl_ver[i]),
                "input_tokens": sp_in_tok[i],
                "output_tokens": sp_out_tok[i],
                "cached_tokens": sp_cached_tok[i],
                "total_tokens": sp_tot_tok[i],
                "cost_nanos": sp_cost_nanos_val,
                "cost_usd": sp_cost_nanos_val / 1_000_000_000,
                "temperature": temp_val,
                "finish_reason": get_str(sp_finish_reason[i]),
                "model_params": get_blob(sp_model_params_blob[i], None),
                "tool_name": get_str(sp_tool_name[i]),
                "tool_version": get_str(sp_tool_version[i]),
                "tool_retry_count": sp_tool_retry[i],
                "input": get_blob(sp_in_blob[i], None),
                "output": get_blob(sp_out_blob[i], None),
                "attributes": get_blob(sp_attrs_blob[i], None),
                "metadata": get_blob(sp_meta_blob[i], None),
                "error_type": get_str(sp_err_type[i]),
                "error_message": get_str(sp_err_msg[i]),
                "error_stack": get_str(sp_err_stack[i]),
                "events": [],
            }
        )

    # Section 5: Columnar Span Events
    total_events = r.u32()
    if total_events > MAX_PLST_EVENTS:
        raise PlstLimitError(f"Event count {total_events} exceeds MAX_PLST_EVENTS ({MAX_PLST_EVENTS})")
    ev_span_idx = r.u32_vec(total_events)
    ev_ts_ms = _delta_decode_i64(r.i64_vec(total_events))
    ev_name_idx = r.u32_vec(total_events)
    ev_attrs_blob = r.u32_vec(total_events)

    if r.remaining != 0:
        raise PlstCorruptError(f"Trailing {r.remaining} unparsed bytes after Section 5")

    for i in range(total_events):
        s_idx = ev_span_idx[i]
        if s_idx >= span_count:
            raise PlstCorruptError(f"Event span_idx {s_idx} >= span_count {span_count}")
        spans[s_idx]["events"].append(
            {
                "name": get_req_str(ev_name_idx[i]),
                "timestamp_ms": ev_ts_ms[i],
                "attributes": get_blob(ev_attrs_blob[i], None),
            }
        )

    return {"traces": traces, "spans": spans}


# ---------------------------------------------------------------------------
# Public PLST v1 Frame Codec & Hostile Frame Builders
# ---------------------------------------------------------------------------


def _compute_lifecycle_flags(batch: dict[str, Any]) -> int:
    traces = batch.get("traces") or []
    spans = batch.get("spans") or []
    has_partial = any(
        _TRACE_STATUS_TO_U8.get(str(tr.get("status", "ok")).lower(), 0) == 1
        for tr in traces
    ) or any(
        _SPAN_PHASE_TO_U8.get(str(sp.get("span_phase", "final")).lower(), 2) != 2
        for sp in spans
    )
    has_final = any(
        _TRACE_STATUS_TO_U8.get(str(tr.get("status", "ok")).lower(), 0) != 1
        for tr in traces
    ) or any(
        _SPAN_PHASE_TO_U8.get(str(sp.get("span_phase", "final")).lower(), 2) == 2
        for sp in spans
    )
    if not has_partial and not has_final:
        return FLAG_FINAL
    return (FLAG_PARTIAL if has_partial else 0) | (FLAG_FINAL if has_final else 0)


def encode_plst_frame(
    batch: dict[str, Any],
    *,
    use_dict: bool = False,
    dict_bytes: bytes | None = None,
    flags: int | None = None,
    zstd_level: int = 3,
    stats_out: BlobDedupStats | None = None,
) -> bytes:
    """Encode a TraceIngestBatch dict into a 20B-header PLST v1 binary frame."""
    raw_sections, dedup_stats = _encode_sections_with_stats(batch)
    if stats_out is not None:
        stats_out.total_blobs = dedup_stats.total_blobs
        stats_out.unique_blobs = dedup_stats.unique_blobs
        stats_out.deduped_blobs = dedup_stats.deduped_blobs
        stats_out.collision_checks = dedup_stats.collision_checks
        stats_out.bytes_saved = dedup_stats.bytes_saved

    uncomp_len = len(raw_sections)
    if uncomp_len > MAX_PLST_UNCOMP_BYTES:
        raise PlstLimitError(
            f"Uncompressed PLST payload {uncomp_len} exceeds {MAX_PLST_UNCOMP_BYTES}"
        )

    if flags is None:
        resolved_flags = (FLAG_DICT_ZSTD if use_dict else FLAG_RAW_ZSTD) | _compute_lifecycle_flags(batch)
    else:
        resolved_flags = int(flags)
        if resolved_flags & ~VALID_FLAGS_MASK:
            raise PlstCorruptError(f"Unsupported PLST flags: {resolved_flags:#04x}")
        if (resolved_flags & FLAG_RAW_ZSTD) and (resolved_flags & FLAG_DICT_ZSTD):
            raise PlstCorruptError("FLAG_RAW_ZSTD and FLAG_DICT_ZSTD are mutually exclusive")

    if resolved_flags & FLAG_DICT_ZSTD:
        zdict, _ = load_agent_zstd_dict(dict_bytes)
        comp_payload = zstd.compress(raw_sections, level=zstd_level, zstd_dict=zdict)
        dict_id = AGENT_DICT_ID
    elif resolved_flags & FLAG_RAW_ZSTD:
        comp_payload = zstd.compress(raw_sections, level=zstd_level)
        dict_id = 0
    else:
        comp_payload = raw_sections
        dict_id = 0

    comp_len = len(comp_payload)
    crc32_val = zlib.crc32(comp_payload) & _MASK32
    header = HEADER_STRUCT.pack(
        PLST_MAGIC,
        PLST_VERSION,
        resolved_flags,
        dict_id,
        uncomp_len,
        comp_len,
        crc32_val,
    )
    return header + comp_payload


def decode_plst_frame(
    frame: bytes | bytearray | memoryview,
    *,
    dict_bytes: bytes | None = None,
    max_uncomp_bytes: int = MAX_PLST_UNCOMP_BYTES,
) -> dict[str, Any]:
    """Verify 20B PLST v1 header + CRC32 + bounded Zstd decode, returning batch dict."""
    mv = memoryview(frame)
    if len(mv) < HEADER_SIZE:
        raise PlstCorruptError(f"Truncated PLST header: {len(mv)} < {HEADER_SIZE} bytes")

    magic, version, flags, dict_id, uncomp_len, comp_len, expected_crc = HEADER_STRUCT.unpack_from(mv, 0)

    if magic != PLST_MAGIC:
        raise PlstCorruptError(f"Invalid PLST magic: {magic!r}")
    if version != PLST_VERSION:
        raise PlstCorruptError(f"Unsupported PLST version: {version}")
    if flags & ~VALID_FLAGS_MASK:
        raise PlstCorruptError(f"Unsupported PLST flags: {flags:#04x}")
    if (flags & FLAG_RAW_ZSTD) and (flags & FLAG_DICT_ZSTD):
        raise PlstCorruptError("Conflicting compression flags (RAW_ZSTD and DICT_ZSTD both set)")
    if uncomp_len > max_uncomp_bytes or uncomp_len > MAX_PLST_UNCOMP_BYTES:
        raise PlstLimitError(f"PLST uncomp_len {uncomp_len} exceeds limit {min(max_uncomp_bytes, MAX_PLST_UNCOMP_BYTES)}")

    effective_max = min(uncomp_len, max_uncomp_bytes, MAX_PLST_UNCOMP_BYTES)

    body = mv[HEADER_SIZE:]
    if comp_len != len(body):
        raise PlstCorruptError(
            f"PLST comp_len mismatch: header={comp_len}, actual_body={len(body)}"
        )

    actual_crc = zlib.crc32(body) & _MASK32
    if actual_crc != expected_crc:
        raise PlstCorruptError(
            f"PLST compressed CRC32 mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        )

    if flags & FLAG_DICT_ZSTD:
        if dict_id != AGENT_DICT_ID:
            raise PlstDictError(f"Unknown PLST dict_id {dict_id} (expected {AGENT_DICT_ID})")
        zdict, _ = load_agent_zstd_dict(dict_bytes)
        raw_sections = decompress_zstd_bounded(body, max_bytes=effective_max, zdict=zdict)
    elif flags & FLAG_RAW_ZSTD:
        if dict_id != 0:
            raise PlstDictError(f"Unexpected non-zero dict_id {dict_id} with FLAG_RAW_ZSTD")
        raw_sections = decompress_zstd_bounded(body, max_bytes=effective_max, zdict=None)
    else:
        if dict_id != 0:
            raise PlstDictError(f"Unexpected non-zero dict_id {dict_id} on uncompressed PLST frame")
        raw_sections = bytes(body)

    if len(raw_sections) != uncomp_len:
        raise PlstCorruptError(
            f"PLST uncompressed length mismatch: header={uncomp_len}, actual={len(raw_sections)}"
        )

    return _decode_sections(raw_sections)


def inspect_plst_header(frame: bytes | bytearray | memoryview) -> dict[str, Any]:
    """Unpack the 20B PLST header into a readable dict without decompressing the body."""
    if len(frame) < HEADER_SIZE:
        raise PlstCorruptError(f"Truncated PLST header: {len(frame)} < {HEADER_SIZE} bytes")
    magic, version, flags, dict_id, uncomp_len, comp_len, crc32_val = HEADER_STRUCT.unpack_from(frame, 0)
    return {
        "magic": magic,
        "version": version,
        "flags": flags,
        "dict_id": dict_id,
        "uncomp_len": uncomp_len,
        "comp_len": comp_len,
        "compressed_crc32": crc32_val,
    }


def _repack_raw_sections_frame(
    raw_sections: bytes,
    *,
    flags: int = FLAG_RAW_ZSTD | FLAG_FINAL,
    dict_id: int = 0,
    uncomp_len_override: int | None = None,
) -> bytes:
    comp_payload = zstd.compress(raw_sections, level=3)
    uncomp_len = len(raw_sections) if uncomp_len_override is None else uncomp_len_override
    crc32_val = zlib.crc32(comp_payload) & _MASK32
    header = HEADER_STRUCT.pack(
        PLST_MAGIC,
        PLST_VERSION,
        flags,
        dict_id,
        uncomp_len,
        len(comp_payload),
        crc32_val,
    )
    return header + comp_payload


def _default_hostile_base_batch() -> dict[str, Any]:
    return {
        "traces": [
            {
                "trace_id": "tr_hostile_base",
                "project_id": "proj_hostile",
                "agent_name": "hostile-probe",
                "agent_version": "1.0.0",
                "environment": "production",
                "status": "ok",
                "start_ts_ms": 1_700_000_000_000,
                "end_ts_ms": 1_700_000_000_100,
                "duration_ms": 100,
                "input": {"prompt": "hello"},
                "output": {"answer": "world"},
                "tags": ["probe"],
                "span_count": 1,
                "total_cost_nanos": 1_000_000,
                "total_cost_usd": 0.001,
            }
        ],
        "spans": [
            {
                "span_id": "sp_hostile_base",
                "trace_id": "tr_hostile_base",
                "project_id": "proj_hostile",
                "name": "llm_step",
                "span_type": "llm",
                "span_phase": "final",
                "status": "ok",
                "start_ts_ms": 1_700_000_000_000,
                "end_ts_ms": 1_700_000_000_100,
                "duration_ms": 100,
                "cost_nanos": 1_000_000,
                "cost_usd": 0.001,
                "events": [{"name": "token", "timestamp_ms": 1_700_000_000_050, "attributes": {"k": "v"}}],
            }
        ],
    }


def build_hostile_frame(kind: str, base_batch: dict[str, Any] | None = None) -> bytes:
    """Build deterministic malformed/adversarial PLST frames for security & fuzz testing."""
    batch = base_batch if base_batch is not None else _default_hostile_base_batch()
    valid = encode_plst_frame(batch, use_dict=False)
    magic, ver, flags, dict_id, uncomp_len, comp_len, crc32_val = HEADER_STRUCT.unpack_from(valid, 0)
    body = valid[HEADER_SIZE:]

    if kind == "bad_magic":
        return HEADER_STRUCT.pack(b"NOPE", ver, flags, dict_id, uncomp_len, comp_len, crc32_val) + body

    if kind == "bad_version":
        return HEADER_STRUCT.pack(magic, 99, flags, dict_id, uncomp_len, comp_len, crc32_val) + body

    if kind == "bad_flags":
        bad_flags = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | 0x80
        return HEADER_STRUCT.pack(magic, ver, bad_flags, dict_id, uncomp_len, comp_len, crc32_val) + body

    if kind == "oversized_uncomp_len":
        huge = MAX_PLST_UNCOMP_BYTES + 1
        return HEADER_STRUCT.pack(magic, ver, flags, dict_id, huge, comp_len, crc32_val) + body

    if kind == "crc_mismatch":
        return HEADER_STRUCT.pack(magic, ver, flags, dict_id, uncomp_len, comp_len, crc32_val ^ 0x12345678) + body

    if kind == "comp_len_mismatch":
        return HEADER_STRUCT.pack(magic, ver, flags, dict_id, uncomp_len, comp_len + 5, crc32_val) + body

    if kind == "truncated_header":
        return valid[:12]

    if kind == "truncated_body":
        trunc = body[: max(1, len(body) // 2)]
        trunc_crc = zlib.crc32(trunc) & _MASK32
        return HEADER_STRUCT.pack(magic, ver, flags, dict_id, uncomp_len, len(trunc), trunc_crc) + trunc

    if kind == "unknown_dict_id":
        dict_frame = encode_plst_frame(batch, use_dict=True)
        d_body = dict_frame[HEADER_SIZE:]
        _, d_ver, d_flags, _, d_uncomp, d_comp, d_crc = HEADER_STRUCT.unpack_from(dict_frame, 0)
        return HEADER_STRUCT.pack(PLST_MAGIC, d_ver, d_flags, 999, d_uncomp, d_comp, d_crc) + d_body

    if kind == "zstd_bomb":
        # Real Zstd stream expanding to 65 MiB while claiming uncomp_len = 1024 in the header.
        bomb_payload = b"\x00" * (MAX_PLST_UNCOMP_BYTES + 1024 * 1024)
        comp_bomb = zstd.compress(bomb_payload, level=3)
        bomb_crc = zlib.crc32(comp_bomb) & _MASK32
        return HEADER_STRUCT.pack(
            PLST_MAGIC,
            PLST_VERSION,
            FLAG_RAW_ZSTD | FLAG_FINAL,
            0,
            1024,
            len(comp_bomb),
            bomb_crc,
        ) + comp_bomb

    if kind == "truncated_sections":
        raw_sections = _encode_sections(batch)
        return _repack_raw_sections_frame(raw_sections[: max(4, len(raw_sections) // 2)])

    if kind == "oob_string_idx":
        raw_sections = bytearray(_encode_sections(batch))
        # Section 1: u32 str_count, u32[str_count+1] offsets, str_blob;
        # Section 2 (columnar): u32 blob_count, u64[blob_count] hashes, u8[blob_count] encodings,
        #                       u32[blob_count+1] offsets, blob_data.
        str_count = _U32.unpack_from(raw_sections, 0)[0]
        off_end = 4 + (str_count + 1) * 4
        str_blob_len = _U32.unpack_from(raw_sections, 4 + str_count * 4)[0]
        pos = off_end + str_blob_len
        blob_count = _U32.unpack_from(raw_sections, pos)[0]
        pos += 4 + blob_count * 8 + blob_count
        blob_data_len = _U32.unpack_from(raw_sections, pos + blob_count * 4)[0]
        pos += (blob_count + 1) * 4 + blob_data_len
        # Now at Section 3: trace_count (u32), followed by tr_trace_ids[0] (u32)
        trace_count = _U32.unpack_from(raw_sections, pos)[0]
        if trace_count == 0:
            raise ValueError("oob_string_idx requires at least 1 trace")
        struct.pack_into("<I", raw_sections, pos + 4, 999_999)
        return _repack_raw_sections_frame(bytes(raw_sections))

    if kind == "oob_blob_idx":
        # Corrupt Section 3's first tr_input_blobs entry to an out-of-bounds blob index.
        raw_sections = bytearray(_encode_sections(batch))
        str_count = _U32.unpack_from(raw_sections, 0)[0]
        off_end = 4 + (str_count + 1) * 4
        str_blob_len = _U32.unpack_from(raw_sections, 4 + str_count * 4)[0]
        pos = off_end + str_blob_len
        blob_count = _U32.unpack_from(raw_sections, pos)[0]
        pos += 4 + blob_count * 8 + blob_count
        blob_data_len = _U32.unpack_from(raw_sections, pos + blob_count * 4)[0]
        pos += (blob_count + 1) * 4 + blob_data_len
        trace_count = _U32.unpack_from(raw_sections, pos)[0]
        if trace_count == 0:
            raise ValueError("oob_blob_idx requires at least 1 trace")
        # Section 3 fixed-stride columns before total_tags:
        # 13*u32 + 2*u8 + i64 + u8 + 2*i64 + 4*i32 + 5*i64 + 2*u32 + 2*(u8+u64) + u8 + 2*u32 + u16 = 172 bytes/trace
        total_tags_offset = pos + 4 + (172 * trace_count)
        total_tags = _U32.unpack_from(raw_sections, total_tags_offset)[0]
        in_blob_offset = total_tags_offset + 4 + (4 * total_tags)
        struct.pack_into("<I", raw_sections, in_blob_offset, 888_888)
        return _repack_raw_sections_frame(bytes(raw_sections))

    if kind == "invalid_utf8":
        raw_sections = bytearray(_encode_sections(batch))
        str_count = _U32.unpack_from(raw_sections, 0)[0]
        if str_count == 0:
            raise ValueError("invalid_utf8 requires at least 1 string")
        str_blob_start = 4 + (str_count + 1) * 4
        raw_sections[str_blob_start] = 0xFF
        return _repack_raw_sections_frame(bytes(raw_sections))

    raise ValueError(f"Unknown hostile frame kind: {kind!r}")
