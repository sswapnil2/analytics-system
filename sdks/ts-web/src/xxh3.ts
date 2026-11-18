/**
 * Zero-dependency 64-bit XXH3 (`xxh3_64` with seed = 0) matching `xxhash_rust::xxh3::xxh3_64` bit-for-bit (`P2-03`).
 */

const MASK64 = 0xffffffffffffffffn;
const MASK32 = 0xffffffffn;

const PRIME32_1 = 0x9e3779b1n;
const PRIME32_2 = 0x85ebca77n;
const PRIME32_3 = 0xc2b2ae3dn;

const PRIME64_1 = 0x9e3779b185ebca87n;
const PRIME64_2 = 0xc2b2ae3d27d4eb4fn;
const PRIME64_3 = 0x165667b19e3779f9n;
const PRIME64_4 = 0x85ebca77c2b2ae63n;
const PRIME64_5 = 0x27d4eb2f165667c5n;

const STRIPE_LEN = 64;
const SECRET_CONSUME_RATE = 8;
const ACC_NB = 8;
const SECRET_MERGEACCS_START = 11;
const SECRET_LASTACC_START = 7;
const MID_SIZE_MAX = 240;
const SECRET_SIZE_MIN = 136;

const DEFAULT_SECRET = new Uint8Array([
  0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
  0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
  0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
  0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
  0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
  0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
  0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
  0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
  0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
  0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
  0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
  0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
]);

function u64(x: bigint): bigint {
  return x & MASK64;
}

function read32LE(buf: Uint8Array, offset: number): bigint {
  return (
    BigInt(buf[offset]!) |
    (BigInt(buf[offset + 1]!) << 8n) |
    (BigInt(buf[offset + 2]!) << 16n) |
    (BigInt(buf[offset + 3]!) << 24n)
  );
}

function read64LE(buf: Uint8Array, offset: number): bigint {
  return read32LE(buf, offset) | (read32LE(buf, offset + 4) << 32n);
}

function rotl64(val: bigint, bits: bigint): bigint {
  const v = u64(val);
  return u64((v << bits) | (v >> (64n - bits)));
}

function bswap64(val: bigint): bigint {
  const v = u64(val);
  return (
    ((v & 0x00000000000000ffn) << 56n) |
    ((v & 0x000000000000ff00n) << 40n) |
    ((v & 0x0000000000ff0000n) << 24n) |
    ((v & 0x00000000ff000000n) << 8n) |
    ((v & 0x000000ff00000000n) >> 8n) |
    ((v & 0x0000ff0000000000n) >> 24n) |
    ((v & 0x00ff000000000000n) >> 40n) |
    ((v & 0xff00000000000000n) >> 56n)
  );
}

function xorshift64(val: bigint, shift: bigint): bigint {
  const v = u64(val);
  return v ^ (v >> shift);
}

function xxh64Avalanche(val: bigint): bigint {
  let h = u64(val);
  h ^= h >> 33n;
  h = u64(h * PRIME64_2);
  h ^= h >> 29n;
  h = u64(h * PRIME64_3);
  h ^= h >> 32n;
  return h;
}

function xxh3Avalanche(val: bigint): bigint {
  let h = xorshift64(val, 37n);
  h = u64(h * 0x165667919e3779f9n);
  return xorshift64(h, 32n);
}

function strongAvalanche(val: bigint, len: bigint): bigint {
  let h = u64(val);
  h ^= rotl64(h, 49n) ^ rotl64(h, 24n);
  h = u64(h * 0x9fb21c651e98df25n);
  h ^= u64((h >> 35n) + len);
  h = u64(h * 0x9fb21c651e98df25n);
  return xorshift64(h, 28n);
}

function mul128Fold64(lhs: bigint, rhs: bigint): bigint {
  const prod = u64(lhs) * u64(rhs);
  const lo = prod & MASK64;
  const hi = (prod >> 64n) & MASK64;
  return lo ^ hi;
}

function mix16B(input: Uint8Array, inOff: number, secret: Uint8Array, secOff: number): bigint {
  const inputLo = read64LE(input, inOff) ^ read64LE(secret, secOff);
  const inputHi = read64LE(input, inOff + 8) ^ read64LE(secret, secOff + 8);
  return mul128Fold64(inputLo, inputHi);
}

function hash0To16(input: Uint8Array, secret: Uint8Array): bigint {
  const len = input.length;
  if (len > 8) {
    const flip1 = read64LE(secret, 24) ^ read64LE(secret, 32);
    const flip2 = read64LE(secret, 40) ^ read64LE(secret, 48);
    const inputLo = read64LE(input, 0) ^ flip1;
    const inputHi = read64LE(input, len - 8) ^ flip2;
    const acc = u64(BigInt(len) + bswap64(inputLo) + inputHi + mul128Fold64(inputLo, inputHi));
    return xxh3Avalanche(acc);
  }
  if (len >= 4) {
    const input1 = read32LE(input, 0);
    const input2 = read32LE(input, len - 4);
    const flip = read64LE(secret, 8) ^ read64LE(secret, 16);
    const input64 = u64(input2 + (input1 << 32n));
    return strongAvalanche(input64 ^ flip, BigInt(len));
  }
  if (len > 0) {
    const c1 = BigInt(input[0]!);
    const c2 = BigInt(input[len >> 1]!);
    const c3 = BigInt(input[len - 1]!);
    const combo = (c1 << 16n) | (c2 << 24n) | c3 | (BigInt(len) << 8n);
    const flip = (read32LE(secret, 0) ^ read32LE(secret, 4)) & MASK32;
    return xxh64Avalanche(combo ^ flip);
  }
  return xxh64Avalanche(read64LE(secret, 56) ^ read64LE(secret, 64));
}

function hash17To128(input: Uint8Array, secret: Uint8Array): bigint {
  const len = input.length;
  let acc = u64(BigInt(len) * PRIME64_1);
  if (len > 32) {
    if (len > 64) {
      if (len > 96) {
        acc = u64(acc + mix16B(input, 48, secret, 96));
        acc = u64(acc + mix16B(input, len - 64, secret, 112));
      }
      acc = u64(acc + mix16B(input, 32, secret, 64));
      acc = u64(acc + mix16B(input, len - 48, secret, 80));
    }
    acc = u64(acc + mix16B(input, 16, secret, 32));
    acc = u64(acc + mix16B(input, len - 32, secret, 48));
  }
  acc = u64(acc + mix16B(input, 0, secret, 0));
  acc = u64(acc + mix16B(input, len - 16, secret, 16));
  return xxh3Avalanche(acc);
}

function hash129To240(input: Uint8Array, secret: Uint8Array): bigint {
  const len = input.length;
  let acc = u64(BigInt(len) * PRIME64_1);
  const nbRounds = Math.floor(len / 16);
  for (let i = 0; i < 8; i++) {
    acc = u64(acc + mix16B(input, 16 * i, secret, 16 * i));
  }
  acc = xxh3Avalanche(acc);
  for (let i = 8; i < nbRounds; i++) {
    acc = u64(acc + mix16B(input, 16 * i, secret, 16 * (i - 8) + 3));
  }
  acc = u64(acc + mix16B(input, len - 16, secret, SECRET_SIZE_MIN - 17));
  return xxh3Avalanche(acc);
}

function accumulate512(acc: bigint[], input: Uint8Array, inOff: number, secret: Uint8Array, secOff: number): void {
  for (let i = 0; i < ACC_NB; i++) {
    const dataVal = read64LE(input, inOff + 8 * i);
    const dataKey = dataVal ^ read64LE(secret, secOff + 8 * i);
    acc[i ^ 1] = u64(acc[i ^ 1]! + dataVal);
    const prod = (dataKey & MASK32) * ((dataKey >> 32n) & MASK32);
    acc[i] = u64(acc[i]! + prod);
  }
}

function scrambleAcc(acc: bigint[], secret: Uint8Array, secOff: number): void {
  for (let i = 0; i < ACC_NB; i++) {
    const key = read64LE(secret, secOff + 8 * i);
    let val = xorshift64(acc[i]!, 47n);
    val ^= key;
    acc[i] = u64(val * PRIME32_1);
  }
}

function hashLong(input: Uint8Array, secret: Uint8Array): bigint {
  const acc: bigint[] = [
    PRIME32_3,
    PRIME64_1,
    PRIME64_2,
    PRIME64_3,
    PRIME64_4,
    PRIME32_2,
    PRIME64_5,
    PRIME32_1,
  ];

  const nbStripesPerBlock = Math.floor((secret.length - STRIPE_LEN) / SECRET_CONSUME_RATE);
  const blockLen = STRIPE_LEN * nbStripesPerBlock;
  const nbBlocks = Math.floor((input.length - 1) / blockLen);

  for (let n = 0; n < nbBlocks; n++) {
    for (let s = 0; s < nbStripesPerBlock; s++) {
      accumulate512(acc, input, n * blockLen + s * STRIPE_LEN, secret, s * SECRET_CONSUME_RATE);
    }
    scrambleAcc(acc, secret, secret.length - STRIPE_LEN);
  }

  const nbStripes = Math.floor((input.length - 1 - blockLen * nbBlocks) / STRIPE_LEN);
  for (let s = 0; s < nbStripes; s++) {
    accumulate512(acc, input, nbBlocks * blockLen + s * STRIPE_LEN, secret, s * SECRET_CONSUME_RATE);
  }
  accumulate512(
    acc,
    input,
    input.length - STRIPE_LEN,
    secret,
    secret.length - STRIPE_LEN - SECRET_LASTACC_START,
  );

  let result = u64(BigInt(input.length) * PRIME64_1);
  for (let i = 0; i < 4; i++) {
    const secBase = SECRET_MERGEACCS_START + 16 * i;
    const lhs = acc[2 * i]! ^ read64LE(secret, secBase);
    const rhs = acc[2 * i + 1]! ^ read64LE(secret, secBase + 8);
    result = u64(result + mul128Fold64(lhs, rhs));
  }
  return xxh3Avalanche(result);
}

const utf8Encoder = new TextEncoder();

export function xxh3_64Bytes(input: Uint8Array): bigint {
  if (input.length <= 16) return hash0To16(input, DEFAULT_SECRET);
  if (input.length <= 128) return hash17To128(input, DEFAULT_SECRET);
  if (input.length <= MID_SIZE_MAX) return hash129To240(input, DEFAULT_SECRET);
  return hashLong(input, DEFAULT_SECRET);
}

export function xxh3_64(text: string): bigint {
  return xxh3_64Bytes(utf8Encoder.encode(text));
}

export function flagRolloutSlot(flagKey: string, distinctId: string): number {
  return Number(xxh3_64(`${flagKey}:${distinctId}`) % 10000n);
}

/** Matches `pulse_wire::cohort_member_hash`: lowercase 16-hex `xxh3_64("cohort:{id}:{distinctId}")`. */
export function cohortMemberHash(cohortId: string, distinctId: string): string {
  return xxh3_64(`cohort:${cohortId}:${distinctId}`).toString(16).padStart(16, "0");
}

export function flagVariantSlot(flagKey: string, distinctId: string, totalWeight: number): number {
  if (totalWeight <= 0) return 0;
  return Number(xxh3_64(`var:${flagKey}:${distinctId}`) % BigInt(totalWeight));
}
