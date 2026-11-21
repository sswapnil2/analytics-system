"""Pure-Python 64-bit XXH3 (seed=0) matching xxhash-rust 0.8.21 bit-for-bit."""

from __future__ import annotations

import struct
from typing import Union

BytesLike = Union[bytes, bytearray, memoryview]

_MASK64 = 0xFFFFFFFFFFFFFFFF
_MASK32 = 0xFFFFFFFF

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
_ACC_NB = 8
_SECRET_MERGEACCS_START = 11
_SECRET_LASTACC_START = 7
_MID_SIZE_MAX = 240
_SECRET_SIZE_MIN = 136

_DEFAULT_SECRET = bytes(
    [
        0xB8, 0xFE, 0x6C, 0x39, 0x23, 0xA4, 0x4B, 0xBE,
        0x7C, 0x01, 0x81, 0x2C, 0xF7, 0x21, 0xAD, 0x1C,
        0xDE, 0xD4, 0x6D, 0xE9, 0x83, 0x90, 0x97, 0xDB,
        0x72, 0x40, 0xA4, 0xA4, 0xB7, 0xB3, 0x67, 0x1F,
        0xCB, 0x79, 0xE6, 0x4E, 0xCC, 0xC0, 0xE5, 0x78,
        0x82, 0x5A, 0xD0, 0x7D, 0xCC, 0xFF, 0x72, 0x21,
        0xB8, 0x08, 0x46, 0x74, 0xF7, 0x43, 0x24, 0x8E,
        0xE0, 0x35, 0x90, 0xE6, 0x81, 0x3A, 0x26, 0x4C,
        0x3C, 0x28, 0x52, 0xBB, 0x91, 0xC3, 0x00, 0xCB,
        0x88, 0xD0, 0x65, 0x8B, 0x1B, 0x53, 0x2E, 0xA3,
        0x71, 0x64, 0x48, 0x97, 0xA2, 0x0D, 0xF9, 0x4E,
        0x38, 0x19, 0xEF, 0x46, 0xA9, 0xDE, 0xAC, 0xD8,
        0xA8, 0xFA, 0x76, 0x3F, 0xE3, 0x9C, 0x34, 0x3F,
        0xF9, 0xDC, 0xBB, 0xC7, 0xC7, 0x0B, 0x4F, 0x1D,
        0x8A, 0x51, 0xE0, 0x4B, 0xCD, 0xB4, 0x59, 0x31,
        0xC8, 0x9F, 0x7E, 0xC9, 0xD9, 0x78, 0x73, 0x64,
        0xEA, 0xC5, 0xAC, 0x83, 0x34, 0xD3, 0xEB, 0xC3,
        0xC5, 0x81, 0xA0, 0xFF, 0xFA, 0x13, 0x63, 0xEB,
        0x17, 0x0D, 0xDD, 0x51, 0xB7, 0xF0, 0xDA, 0x49,
        0xD3, 0x16, 0x55, 0x26, 0x29, 0xD4, 0x68, 0x9E,
        0x2B, 0x16, 0xBE, 0x58, 0x7D, 0x47, 0xA1, 0xFC,
        0x8F, 0xF8, 0xB8, 0xD1, 0x7A, 0xD0, 0x31, 0xCE,
        0x45, 0xCB, 0x3A, 0x8F, 0x95, 0x16, 0x04, 0x28,
        0xAF, 0xD7, 0xFB, 0xCA, 0xBB, 0x4B, 0x40, 0x7E,
    ]
)

_U32_LE = struct.Struct("<I")
_U64_LE = struct.Struct("<Q")


def _read32_le(buf: memoryview, offset: int) -> int:
    return _U32_LE.unpack_from(buf, offset)[0]


def _read64_le(buf: memoryview, offset: int) -> int:
    return _U64_LE.unpack_from(buf, offset)[0]


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
    return ((prod & _MASK64) ^ (prod >> 64)) & _MASK64


def _mix16b(data: memoryview, in_off: int, secret: memoryview, sec_off: int) -> int:
    in_lo = _read64_le(data, in_off) ^ _read64_le(secret, sec_off)
    in_hi = _read64_le(data, in_off + 8) ^ _read64_le(secret, sec_off + 8)
    return _mul128_fold64(in_lo, in_hi)


def _hash_0_to_16(data: memoryview, secret: memoryview) -> int:
    length = len(data)
    if length > 8:
        flip1 = _read64_le(secret, 24) ^ _read64_le(secret, 32)
        flip2 = _read64_le(secret, 40) ^ _read64_le(secret, 48)
        in_lo = _read64_le(data, 0) ^ flip1
        in_hi = _read64_le(data, length - 8) ^ flip2
        acc = (length + _bswap64(in_lo) + in_hi + _mul128_fold64(in_lo, in_hi)) & _MASK64
        return _xxh3_avalanche(acc)
    if length >= 4:
        in1 = _read32_le(data, 0)
        in2 = _read32_le(data, length - 4)
        flip = _read64_le(secret, 8) ^ _read64_le(secret, 16)
        in64 = (in2 + (in1 << 32)) & _MASK64
        return _strong_avalanche(in64 ^ flip, length)
    if length > 0:
        c1 = data[0]
        c2 = data[length >> 1]
        c3 = data[length - 1]
        combo = (c1 << 16) | (c2 << 24) | c3 | (length << 8)
        flip = (_read32_le(secret, 0) ^ _read32_le(secret, 4)) & _MASK32
        return _xxh64_avalanche(combo ^ flip)
    return _xxh64_avalanche(_read64_le(secret, 56) ^ _read64_le(secret, 64))


def _hash_17_to_128(data: memoryview, secret: memoryview) -> int:
    length = len(data)
    acc = (length * _PRIME64_1) & _MASK64
    if length > 32:
        if length > 64:
            if length > 96:
                acc = (acc + _mix16b(data, 48, secret, 96)) & _MASK64
                acc = (acc + _mix16b(data, length - 64, secret, 112)) & _MASK64
            acc = (acc + _mix16b(data, 32, secret, 64)) & _MASK64
            acc = (acc + _mix16b(data, length - 48, secret, 80)) & _MASK64
        acc = (acc + _mix16b(data, 16, secret, 32)) & _MASK64
        acc = (acc + _mix16b(data, length - 32, secret, 48)) & _MASK64
    acc = (acc + _mix16b(data, 0, secret, 0)) & _MASK64
    acc = (acc + _mix16b(data, length - 16, secret, 16)) & _MASK64
    return _xxh3_avalanche(acc)


def _hash_129_to_240(data: memoryview, secret: memoryview) -> int:
    length = len(data)
    acc = (length * _PRIME64_1) & _MASK64
    nb_rounds = length // 16
    for i in range(8):
        acc = (acc + _mix16b(data, 16 * i, secret, 16 * i)) & _MASK64
    acc = _xxh3_avalanche(acc)
    for i in range(8, nb_rounds):
        acc = (acc + _mix16b(data, 16 * i, secret, 16 * (i - 8) + 3)) & _MASK64
    acc = (acc + _mix16b(data, length - 16, secret, _SECRET_SIZE_MIN - 17)) & _MASK64
    return _xxh3_avalanche(acc)


def _accumulate_512(
    acc: list[int],
    data: memoryview,
    in_off: int,
    secret: memoryview,
    sec_off: int,
) -> None:
    for i in range(_ACC_NB):
        data_val = _read64_le(data, in_off + 8 * i)
        data_key = data_val ^ _read64_le(secret, sec_off + 8 * i)
        acc[i ^ 1] = (acc[i ^ 1] + data_val) & _MASK64
        prod = (data_key & _MASK32) * ((data_key >> 32) & _MASK32)
        acc[i] = (acc[i] + prod) & _MASK64


def _scramble_acc(acc: list[int], secret: memoryview, sec_off: int) -> None:
    for i in range(_ACC_NB):
        key = _read64_le(secret, sec_off + 8 * i)
        val = _xorshift64(acc[i], 47) ^ key
        acc[i] = (val * _PRIME32_1) & _MASK64


def _hash_long(data: memoryview, secret: memoryview) -> int:
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
    sec_len = len(secret)
    nb_stripes_per_block = (sec_len - _STRIPE_LEN) // _SECRET_CONSUME_RATE
    block_len = _STRIPE_LEN * nb_stripes_per_block
    length = len(data)
    nb_blocks = (length - 1) // block_len

    for n in range(nb_blocks):
        block_base = n * block_len
        for s in range(nb_stripes_per_block):
            _accumulate_512(acc, data, block_base + s * _STRIPE_LEN, secret, s * _SECRET_CONSUME_RATE)
        _scramble_acc(acc, secret, sec_len - _STRIPE_LEN)

    nb_stripes = (length - 1 - block_len * nb_blocks) // _STRIPE_LEN
    tail_base = nb_blocks * block_len
    for s in range(nb_stripes):
        _accumulate_512(acc, data, tail_base + s * _STRIPE_LEN, secret, s * _SECRET_CONSUME_RATE)

    _accumulate_512(
        acc,
        data,
        length - _STRIPE_LEN,
        secret,
        sec_len - _STRIPE_LEN - _SECRET_LASTACC_START,
    )

    result = (length * _PRIME64_1) & _MASK64
    for i in range(4):
        sec_base = _SECRET_MERGEACCS_START + 16 * i
        lhs = acc[2 * i] ^ _read64_le(secret, sec_base)
        rhs = acc[2 * i + 1] ^ _read64_le(secret, sec_base + 8)
        result = (result + _mul128_fold64(lhs, rhs)) & _MASK64
    return _xxh3_avalanche(result)


_SECRET_VIEW = memoryview(_DEFAULT_SECRET)


def xxh3_64(data: Union[BytesLike, str]) -> int:
    """Computes unsigned 64-bit XXH3 hash (seed=0) matching xxhash_rust::xxh3::xxh3_64."""
    if isinstance(data, str):
        raw = data.encode("utf-8")
    else:
        raw = data
    view = memoryview(raw)
    if view.ndim != 1 or view.itemsize != 1:
        view = view.cast("B")
    length = len(view)
    if length <= 16:
        return _hash_0_to_16(view, _SECRET_VIEW)
    if length <= 128:
        return _hash_17_to_128(view, _SECRET_VIEW)
    if length <= _MID_SIZE_MAX:
        return _hash_129_to_240(view, _SECRET_VIEW)
    return _hash_long(view, _SECRET_VIEW)
