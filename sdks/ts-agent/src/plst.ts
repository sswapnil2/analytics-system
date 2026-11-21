import { crc32 } from "./crc32.js";
import { AGENT_DICT_ID, AGENT_ZSTD_DICT } from "./dict.js";
import { costNanosToUsd, usdToCostNanos } from "./pricing.js";
import type {
  AgentSpan,
  AgentTrace,
  FailureSource,
  JsonValue,
  PlstCompressionMode,
  SpanEvent,
  SpanPhase,
  SpanType,
  TraceIngestBatch,
  TraceStatus,
} from "./types.js";
import { xxh3_64Bytes } from "./xxh3.js";

export const PLST_MAGIC = new Uint8Array([0x50, 0x4c, 0x53, 0x54]); // "PLST"
export const PLST_VERSION = 1;
export const PLST_HEADER_LEN = 20;

export const MAX_PLST_UNCOMP_BYTES = 64 * 1024 * 1024;
export const MAX_PLST_TRACES = 100_000;
export const MAX_PLST_SPANS = 500_000;
export const MAX_PLST_EVENTS = 2_000_000;
export const MAX_PLST_STRINGS = 2_000_000;
export const MAX_PLST_BLOBS = 500_000;
export const MAX_PLST_SINGLE_STRING_BYTES = 1024 * 1024;
export const MAX_PLST_MATERIALIZED_BYTES = 64 * 1024 * 1024;

export const FLAG_RAW_ZSTD = 0x01;
export const FLAG_DICT_ZSTD = 0x02;
export const FLAG_PARTIAL = 0x04;
export const FLAG_FINAL = 0x08;
const KNOWN_FLAGS_MASK = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | FLAG_PARTIAL | FLAG_FINAL;

const NONE_IDX = 0xffffffff;
const BLOB_ENC_JSON_UTF8 = 0;
const BLOB_ENC_UTF8_TEXT = 1;
const ZSTD_LEVEL = 3;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export class PlstError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "PlstError";
    this.code = code;
  }
}

interface ZstdOptions {
  dictionary?: Uint8Array;
  params?: Record<number, number>;
  maxOutputLength?: number;
}

interface ZlibZstdModule {
  zstdCompressSync?: (buf: Uint8Array, options?: ZstdOptions) => Uint8Array;
  zstdDecompressSync?: (buf: Uint8Array, options?: ZstdOptions) => Uint8Array;
  constants?: {
    ZSTD_c_compressionLevel?: number;
  };
}

interface GlobalWithProcess {
  process?: {
    getBuiltinModule?: (id: string) => unknown;
  };
}

function getZlibZstd(): ZlibZstdModule | null {
  const proc = (globalThis as GlobalWithProcess).process;
  if (!proc || typeof proc.getBuiltinModule !== "function") {
    return null;
  }
  const mod =
    (proc.getBuiltinModule("node:zlib") as ZlibZstdModule | undefined) ??
    (proc.getBuiltinModule("zlib") as ZlibZstdModule | undefined);
  if (
    mod &&
    typeof mod.zstdCompressSync === "function" &&
    typeof mod.zstdDecompressSync === "function"
  ) {
    return mod;
  }
  return null;
}

export function supportsZstd(): boolean {
  return getZlibZstd() !== null;
}

function toPlainUint8Array(buf: Uint8Array): Uint8Array {
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

function zstdCompress(uncomp: Uint8Array, useDict: boolean): Uint8Array {
  const zlib = getZlibZstd();
  if (!zlib || typeof zlib.zstdCompressSync !== "function") {
    throw new PlstError(
      "ZSTD_UNAVAILABLE",
      "Zstd compression requested but node:zlib zstdCompressSync is unavailable",
    );
  }
  const levelKey = zlib.constants?.ZSTD_c_compressionLevel;
  const options: ZstdOptions = {};
  if (useDict) {
    options.dictionary = AGENT_ZSTD_DICT;
  }
  if (typeof levelKey === "number") {
    options.params = { [levelKey]: ZSTD_LEVEL };
  }
  return toPlainUint8Array(zlib.zstdCompressSync(uncomp, options));
}

function zstdDecompress(comp: Uint8Array, useDict: boolean): Uint8Array {
  const zlib = getZlibZstd();
  if (!zlib || typeof zlib.zstdDecompressSync !== "function") {
    throw new PlstError(
      "ZSTD_UNAVAILABLE",
      "Zstd decompression requested but node:zlib zstdDecompressSync is unavailable",
    );
  }
  const options: ZstdOptions = useDict
    ? { dictionary: AGENT_ZSTD_DICT, maxOutputLength: MAX_PLST_UNCOMP_BYTES }
    : { maxOutputLength: MAX_PLST_UNCOMP_BYTES };
  try {
    return toPlainUint8Array(zlib.zstdDecompressSync(comp, options));
  } catch (err) {
    const code = (err as { code?: string } | null)?.code;
    if (code === "ERR_BUFFER_TOO_LARGE") {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "decompressed PLST payload exceeds 64 MiB limit",
      );
    }
    throw err;
  }
}

class ByteWriter {
  private buf: Uint8Array;
  private view: DataView;
  private pos = 0;

  constructor(initialCapacity = 4096) {
    this.buf = new Uint8Array(initialCapacity);
    this.view = new DataView(this.buf.buffer);
  }

  private ensure(extra: number): void {
    const needed = this.pos + extra;
    if (needed <= this.buf.length) return;
    let nextCap = this.buf.length * 2;
    while (nextCap < needed) {
      nextCap *= 2;
    }
    const next = new Uint8Array(nextCap);
    next.set(this.buf.subarray(0, this.pos));
    this.buf = next;
    this.view = new DataView(this.buf.buffer);
  }

  writeU8(v: number): void {
    this.ensure(1);
    this.buf[this.pos++] = v & 0xff;
  }

  writeU16LE(v: number): void {
    this.ensure(2);
    this.view.setUint16(this.pos, v, true);
    this.pos += 2;
  }

  writeU32LE(v: number): void {
    this.ensure(4);
    this.view.setUint32(this.pos, v >>> 0, true);
    this.pos += 4;
  }

  writeI32LE(v: number): void {
    this.ensure(4);
    this.view.setInt32(this.pos, v | 0, true);
    this.pos += 4;
  }

  writeI64LE(v: bigint): void {
    this.ensure(8);
    this.view.setBigInt64(this.pos, BigInt.asIntN(64, v), true);
    this.pos += 8;
  }

  writeU64LE(v: bigint): void {
    this.ensure(8);
    this.view.setBigUint64(this.pos, BigInt.asUintN(64, v), true);
    this.pos += 8;
  }

  writeF64LE(v: number): void {
    this.ensure(8);
    this.view.setFloat64(this.pos, v, true);
    this.pos += 8;
  }

  writeBytes(bytes: Uint8Array): void {
    if (bytes.length === 0) return;
    this.ensure(bytes.length);
    this.buf.set(bytes, this.pos);
    this.pos += bytes.length;
  }

  writeU8Slice(items: readonly number[]): void {
    this.ensure(items.length);
    for (let i = 0; i < items.length; i++) {
      this.buf[this.pos++] = items[i]! & 0xff;
    }
  }

  writeU16Slice(items: readonly number[]): void {
    this.ensure(items.length * 2);
    for (let i = 0; i < items.length; i++) {
      this.view.setUint16(this.pos, items[i]!, true);
      this.pos += 2;
    }
  }

  writeU32Slice(items: readonly number[]): void {
    this.ensure(items.length * 4);
    for (let i = 0; i < items.length; i++) {
      this.view.setUint32(this.pos, items[i]! >>> 0, true);
      this.pos += 4;
    }
  }

  writeI32Slice(items: readonly number[]): void {
    this.ensure(items.length * 4);
    for (let i = 0; i < items.length; i++) {
      this.view.setInt32(this.pos, items[i]! | 0, true);
      this.pos += 4;
    }
  }

  writeI64Slice(items: readonly bigint[]): void {
    this.ensure(items.length * 8);
    for (let i = 0; i < items.length; i++) {
      this.view.setBigInt64(this.pos, BigInt.asIntN(64, items[i]!), true);
      this.pos += 8;
    }
  }

  writeU64Slice(items: readonly bigint[]): void {
    this.ensure(items.length * 8);
    for (let i = 0; i < items.length; i++) {
      this.view.setBigUint64(this.pos, BigInt.asUintN(64, items[i]!), true);
      this.pos += 8;
    }
  }

  finish(): Uint8Array {
    return this.buf.slice(0, this.pos);
  }
}

class ByteReader {
  private readonly buf: Uint8Array;
  private readonly view: DataView;
  private pos = 0;

  constructor(buf: Uint8Array) {
    this.buf = buf;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  remaining(): number {
    return this.buf.length - this.pos;
  }

  private require(n: number, label: string): void {
    if (this.pos + n > this.buf.length) {
      throw new PlstError("TRUNCATED_FRAME", `truncated PLST frame at ${label}`);
    }
  }

  readU8(label: string): number {
    this.require(1, label);
    return this.buf[this.pos++]!;
  }

  readU16(label: string): number {
    this.require(2, label);
    const v = this.view.getUint16(this.pos, true);
    this.pos += 2;
    return v;
  }

  readU32(label: string): number {
    this.require(4, label);
    const v = this.view.getUint32(this.pos, true);
    this.pos += 4;
    return v;
  }

  readSlice(len: number, label: string): Uint8Array {
    this.require(len, label);
    const slice = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return slice;
  }

  readU8Vec(count: number, label: string): Uint8Array {
    return this.readSlice(count, label);
  }

  readU16Vec(count: number, label: string): number[] {
    this.require(count * 2, label);
    const out = new Array<number>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getUint16(this.pos, true);
      this.pos += 2;
    }
    return out;
  }

  readU32Vec(count: number, label: string): number[] {
    this.require(count * 4, label);
    const out = new Array<number>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getUint32(this.pos, true);
      this.pos += 4;
    }
    return out;
  }

  readI32Vec(count: number, label: string): number[] {
    this.require(count * 4, label);
    const out = new Array<number>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getInt32(this.pos, true);
      this.pos += 4;
    }
    return out;
  }

  readI64Vec(count: number, label: string): bigint[] {
    this.require(count * 8, label);
    const out = new Array<bigint>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getBigInt64(this.pos, true);
      this.pos += 8;
    }
    return out;
  }

  readU64Vec(count: number, label: string): bigint[] {
    this.require(count * 8, label);
    const out = new Array<bigint>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getBigUint64(this.pos, true);
      this.pos += 8;
    }
    return out;
  }
}

class StringTableBuilder {
  private readonly strings: Uint8Array[] = [];
  private readonly map = new Map<string, number>();
  private totalBytes = 0;

  intern(s: string): number {
    const existing = this.map.get(s);
    if (existing !== undefined) {
      return existing;
    }
    const bytes = textEncoder.encode(s);
    if (bytes.length > MAX_PLST_SINGLE_STRING_BYTES) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "single string exceeds MAX_PLST_SINGLE_STRING_BYTES",
      );
    }
    if (this.strings.length >= MAX_PLST_STRINGS) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "string table exceeds MAX_PLST_STRINGS",
      );
    }
    const nextTotal = this.totalBytes + bytes.length;
    if (nextTotal > 0xffffffff) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "string table exceeds u32 offset range",
      );
    }
    const idx = this.strings.length;
    if (idx === NONE_IDX) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "string index collided with NONE_IDX",
      );
    }
    this.strings.push(bytes);
    this.map.set(s, idx);
    this.totalBytes = nextTotal;
    return idx;
  }

  internOpt(s: string | null | undefined): number {
    if (s === null || s === undefined) {
      return NONE_IDX;
    }
    return this.intern(s);
  }

  writeTo(out: ByteWriter): void {
    out.writeU32LE(this.strings.length);
    let offset = 0;
    out.writeU32LE(offset);
    for (let i = 0; i < this.strings.length; i++) {
      offset += this.strings[i]!.length;
      out.writeU32LE(offset);
    }
    for (let i = 0; i < this.strings.length; i++) {
      out.writeBytes(this.strings[i]!);
    }
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

class BlobTableBuilder {
  private readonly hashes: bigint[] = [];
  private readonly encodings: number[] = [];
  private readonly chunks: Uint8Array[] = [];
  private readonly buckets = new Map<string, number[]>();
  private totalBytes = 0;

  internValue(val: JsonValue | undefined): number {
    if (val === null || val === undefined) {
      return NONE_IDX;
    }
    let encoding: number;
    let bytes: Uint8Array;
    if (typeof val === "string") {
      encoding = BLOB_ENC_UTF8_TEXT;
      bytes = textEncoder.encode(val);
    } else {
      encoding = BLOB_ENC_JSON_UTF8;
      const jsonStr = JSON.stringify(val) ?? "null";
      bytes = textEncoder.encode(jsonStr);
    }

    if (bytes.length > 0xffffffff) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "blob byte length exceeds u32",
      );
    }

    const hash = xxh3_64Bytes(bytes);
    const bucketKey = `${hash.toString(16)}_${encoding}_${bytes.length}`;
    const candidates = this.buckets.get(bucketKey);
    if (candidates !== undefined) {
      for (let i = 0; i < candidates.length; i++) {
        const id = candidates[i]!;
        if (bytesEqual(this.chunks[id]!, bytes)) {
          return id;
        }
      }
    }

    if (this.hashes.length >= MAX_PLST_BLOBS) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "blob table exceeds MAX_PLST_BLOBS",
      );
    }
    const nextTotal = this.totalBytes + bytes.length;
    if (nextTotal > 0xffffffff) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "blob table exceeds u32 offset range",
      );
    }

    const id = this.hashes.length;
    if (id === NONE_IDX) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "blob index collided with NONE_IDX",
      );
    }

    this.hashes.push(hash);
    this.encodings.push(encoding);
    this.chunks.push(bytes);
    this.totalBytes = nextTotal;
    if (candidates !== undefined) {
      candidates.push(id);
    } else {
      this.buckets.set(bucketKey, [id]);
    }
    return id;
  }

  writeTo(out: ByteWriter): void {
    out.writeU32LE(this.hashes.length);
    out.writeU64Slice(this.hashes);
    out.writeU8Slice(this.encodings);
    let offset = 0;
    out.writeU32LE(offset);
    for (let i = 0; i < this.chunks.length; i++) {
      offset += this.chunks[i]!.length;
      out.writeU32LE(offset);
    }
    for (let i = 0; i < this.chunks.length; i++) {
      out.writeBytes(this.chunks[i]!);
    }
  }
}

function traceStatusToU8(status: TraceStatus): number {
  switch (status) {
    case "ok":
      return 0;
    case "running":
      return 1;
    case "error":
      return 2;
    case "timeout":
      return 3;
    case "cancelled":
      return 4;
    default:
      return 0;
  }
}

function traceStatusFromU8(tag: number): TraceStatus {
  switch (tag) {
    case 0:
      return "ok";
    case 1:
      return "running";
    case 2:
      return "error";
    case 3:
      return "timeout";
    case 4:
      return "cancelled";
    default:
      throw new PlstError("INVALID_ENUM_TAG", `invalid trace_status tag: ${tag}`);
  }
}

function failureSourceToU8(fs: FailureSource | null | undefined): number {
  switch (fs) {
    case null:
    case undefined:
      return 0;
    case "model":
      return 1;
    case "tool":
      return 2;
    case "application":
      return 3;
    case "prompt":
      return 4;
    case "external":
      return 5;
    case "unknown":
      return 6;
    default:
      return 6;
  }
}

function failureSourceFromU8(tag: number): FailureSource | null {
  switch (tag) {
    case 0:
      return null;
    case 1:
      return "model";
    case 2:
      return "tool";
    case 3:
      return "application";
    case 4:
      return "prompt";
    case 5:
      return "external";
    case 6:
      return "unknown";
    default:
      throw new PlstError("INVALID_ENUM_TAG", `invalid failure_source tag: ${tag}`);
  }
}

function spanTypeToU8(st: SpanType): number {
  switch (st) {
    case "agent":
      return 0;
    case "llm":
      return 1;
    case "tool":
      return 2;
    case "retrieval":
      return 3;
    case "embedding":
      return 4;
    case "guardrail":
      return 5;
    case "http":
      return 6;
    case "db":
      return 7;
    case "workflow":
      return 8;
    case "custom":
      return 9;
    default:
      return 9;
  }
}

function spanTypeFromU8(tag: number): SpanType {
  switch (tag) {
    case 0:
      return "agent";
    case 1:
      return "llm";
    case 2:
      return "tool";
    case 3:
      return "retrieval";
    case 4:
      return "embedding";
    case 5:
      return "guardrail";
    case 6:
      return "http";
    case 7:
      return "db";
    case 8:
      return "workflow";
    case 9:
      return "custom";
    default:
      throw new PlstError("INVALID_ENUM_TAG", `invalid span_type tag: ${tag}`);
  }
}

function spanPhaseToU8(sp: SpanPhase): number {
  switch (sp) {
    case "start":
      return 0;
    case "update":
      return 1;
    case "final":
      return 2;
    default:
      return 2;
  }
}

function spanPhaseFromU8(tag: number): SpanPhase {
  switch (tag) {
    case 0:
      return "start";
    case 1:
      return "update";
    case 2:
      return "final";
    default:
      throw new PlstError("INVALID_ENUM_TAG", `invalid span_phase tag: ${tag}`);
  }
}

function toI64BigInt(n: number | bigint): bigint {
  if (typeof n === "bigint") {
    return BigInt.asIntN(64, n);
  }
  if (!Number.isFinite(n)) {
    return 0n;
  }
  return BigInt.asIntN(64, BigInt(Math.trunc(n)));
}

function f64ToU64Bits(val: number): bigint {
  const buf = new ArrayBuffer(8);
  const dv = new DataView(buf);
  dv.setFloat64(0, val, true);
  return dv.getBigUint64(0, true);
}

function u64BitsToF64(bits: bigint): number {
  const buf = new ArrayBuffer(8);
  const dv = new DataView(buf);
  dv.setBigUint64(0, bits, true);
  return dv.getFloat64(0, true);
}

export function encodeColumnarPayload(batch: TraceIngestBatch): Uint8Array {
  const strings = new StringTableBuilder();
  const blobs = new BlobTableBuilder();

  const traceCount = batch.traces.length;
  const spanCount = batch.spans.length;

  const traceIdxById = new Map<string, number>();
  for (let i = 0; i < traceCount; i++) {
    const tid = batch.traces[i]!.trace_id;
    if (!traceIdxById.has(tid)) {
      traceIdxById.set(tid, i);
    }
  }

  const trTraceIds = new Array<number>(traceCount);
  const trProjectIds = new Array<number>(traceCount);
  const trSessionIds = new Array<number>(traceCount);
  const trUserIds = new Array<number>(traceCount);
  const trWorkflowIds = new Array<number>(traceCount);
  const trParentTraceIds = new Array<number>(traceCount);
  const trAgentNames = new Array<number>(traceCount);
  const trAgentVersions = new Array<number>(traceCount);
  const trPromptVersions = new Array<number>(traceCount);
  const trModelCfgVersions = new Array<number>(traceCount);
  const trToolCfgVersions = new Array<number>(traceCount);
  const trAppReleases = new Array<number>(traceCount);
  const trEnvironments = new Array<number>(traceCount);

  const trStatuses = new Array<number>(traceCount);
  const trFailureSources = new Array<number>(traceCount);
  const trStartDeltas = new Array<bigint>(traceCount);
  const trHasEnd = new Array<number>(traceCount);
  const trEndOffsets = new Array<bigint>(traceCount);
  const trDurations = new Array<bigint>(traceCount);

  const trSpanCounts = new Array<number>(traceCount);
  const trLlmCounts = new Array<number>(traceCount);
  const trToolCounts = new Array<number>(traceCount);
  const trErrSpanCounts = new Array<number>(traceCount);
  const trInTokens = new Array<bigint>(traceCount);
  const trOutTokens = new Array<bigint>(traceCount);
  const trCachedTokens = new Array<bigint>(traceCount);
  const trTotalTokens = new Array<bigint>(traceCount);
  const trCostNanos = new Array<bigint>(traceCount);

  const trPrimaryModels = new Array<number>(traceCount);
  const trPrimaryProviders = new Array<number>(traceCount);
  const trHasAvgEval = new Array<number>(traceCount);
  const trAvgEvalBits = new Array<bigint>(traceCount);
  const trHasPassRate = new Array<number>(traceCount);
  const trPassRateBits = new Array<bigint>(traceCount);
  const trHasLoop = new Array<number>(traceCount);
  const trErrorTypes = new Array<number>(traceCount);
  const trErrorMessages = new Array<number>(traceCount);

  const trTagCounts = new Array<number>(traceCount);
  const trTagStrIds: number[] = [];
  const trInputBlobs = new Array<number>(traceCount);
  const trOutputBlobs = new Array<number>(traceCount);
  const trMetadataBlobs = new Array<number>(traceCount);

  let prevTrStart = 0n;
  for (let i = 0; i < traceCount; i++) {
    const t = batch.traces[i]!;
    trTraceIds[i] = strings.intern(t.trace_id);
    trProjectIds[i] = strings.intern(t.project_id ?? "");
    trSessionIds[i] = strings.internOpt(t.session_id);
    trUserIds[i] = strings.internOpt(t.user_id);
    trWorkflowIds[i] = strings.internOpt(t.workflow_id);
    trParentTraceIds[i] = strings.internOpt(t.parent_trace_id);
    trAgentNames[i] = strings.intern(t.agent_name);
    trAgentVersions[i] = strings.intern(t.agent_version ?? "unversioned");
    trPromptVersions[i] = strings.internOpt(t.prompt_version);
    trModelCfgVersions[i] = strings.internOpt(t.model_config_version);
    trToolCfgVersions[i] = strings.internOpt(t.tool_config_version);
    trAppReleases[i] = strings.internOpt(t.app_release);
    trEnvironments[i] = strings.intern(t.environment ?? "production");

    trStatuses[i] = traceStatusToU8(t.status ?? "ok");
    trFailureSources[i] = failureSourceToU8(t.failure_source);
    const startTs = toI64BigInt(t.start_ts_ms);
    trStartDeltas[i] = BigInt.asIntN(64, startTs - prevTrStart);
    prevTrStart = startTs;

    if (t.end_ts_ms !== null && t.end_ts_ms !== undefined) {
      trHasEnd[i] = 1;
      trEndOffsets[i] = BigInt.asIntN(64, toI64BigInt(t.end_ts_ms) - startTs);
    } else {
      trHasEnd[i] = 0;
      trEndOffsets[i] = 0n;
    }
    trDurations[i] = toI64BigInt(t.duration_ms ?? 0);

    trSpanCounts[i] = t.span_count ?? 0;
    trLlmCounts[i] = t.llm_call_count ?? 0;
    trToolCounts[i] = t.tool_call_count ?? 0;
    trErrSpanCounts[i] = t.error_span_count ?? 0;
    trInTokens[i] = toI64BigInt(t.input_tokens ?? 0);
    trOutTokens[i] = toI64BigInt(t.output_tokens ?? 0);
    trCachedTokens[i] = toI64BigInt(t.cached_tokens ?? 0);
    trTotalTokens[i] = toI64BigInt(t.total_tokens ?? 0);

    const costNanos =
      t.total_cost_nanos !== undefined && t.total_cost_nanos !== 0
        ? t.total_cost_nanos
        : usdToCostNanos(t.total_cost_usd ?? 0);
    trCostNanos[i] = toI64BigInt(costNanos);

    trPrimaryModels[i] = strings.internOpt(t.primary_model);
    trPrimaryProviders[i] = strings.internOpt(t.primary_provider);
    if (t.avg_eval_score !== null && t.avg_eval_score !== undefined) {
      trHasAvgEval[i] = 1;
      trAvgEvalBits[i] = f64ToU64Bits(t.avg_eval_score);
    } else {
      trHasAvgEval[i] = 0;
      trAvgEvalBits[i] = 0n;
    }
    if (t.eval_pass_rate !== null && t.eval_pass_rate !== undefined) {
      trHasPassRate[i] = 1;
      trPassRateBits[i] = f64ToU64Bits(t.eval_pass_rate);
    } else {
      trHasPassRate[i] = 0;
      trPassRateBits[i] = 0n;
    }
    trHasLoop[i] = t.has_loop_warning ? 1 : 0;
    trErrorTypes[i] = strings.internOpt(t.error_type);
    trErrorMessages[i] = strings.internOpt(t.error_message);

    const tags = t.tags ?? [];
    if (tags.length > 0xffff) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "trace tag count exceeds u16::MAX",
      );
    }
    trTagCounts[i] = tags.length;
    for (let j = 0; j < tags.length; j++) {
      trTagStrIds.push(strings.intern(tags[j]!));
    }
    trInputBlobs[i] = blobs.internValue(t.input);
    trOutputBlobs[i] = blobs.internValue(t.output);
    trMetadataBlobs[i] = blobs.internValue(t.metadata);
  }

  const spTraceIndices = new Array<number>(spanCount);
  const spTraceIds = new Array<number>(spanCount);
  const spProjectIds = new Array<number>(spanCount);
  const spSpanIds = new Array<number>(spanCount);
  const spParentSpanIds = new Array<number>(spanCount);
  const spLinkedCounts = new Array<number>(spanCount);
  const spLinkedStrIds: number[] = [];
  const spDepths = new Array<number>(spanCount);

  const spNames = new Array<number>(spanCount);
  const spTypes = new Array<number>(spanCount);
  const spPhases = new Array<number>(spanCount);
  const spStatuses = new Array<number>(spanCount);
  const spStartDeltas = new Array<bigint>(spanCount);
  const spHasEnd = new Array<number>(spanCount);
  const spEndOffsets = new Array<bigint>(spanCount);
  const spDurations = new Array<bigint>(spanCount);

  const spProviders = new Array<number>(spanCount);
  const spModels = new Array<number>(spanCount);
  const spModelIds = new Array<number>(spanCount);
  const spModelVersions = new Array<number>(spanCount);
  const spPromptTplIds = new Array<number>(spanCount);
  const spPromptTplVersions = new Array<number>(spanCount);
  const spInTokens = new Array<bigint>(spanCount);
  const spOutTokens = new Array<bigint>(spanCount);
  const spCachedTokens = new Array<bigint>(spanCount);
  const spTotalTokens = new Array<bigint>(spanCount);
  const spCostNanos = new Array<bigint>(spanCount);
  const spHasTemp = new Array<number>(spanCount);
  const spTempBits = new Array<bigint>(spanCount);
  const spFinishReasons = new Array<number>(spanCount);
  const spModelParamsBlobs = new Array<number>(spanCount);
  const spToolNames = new Array<number>(spanCount);
  const spToolVersions = new Array<number>(spanCount);
  const spToolRetries = new Array<number>(spanCount);

  const spErrorTypes = new Array<number>(spanCount);
  const spErrorMessages = new Array<number>(spanCount);
  const spErrorStacks = new Array<number>(spanCount);
  const spInputBlobs = new Array<number>(spanCount);
  const spOutputBlobs = new Array<number>(spanCount);
  const spAttrBlobs = new Array<number>(spanCount);
  const spMetaBlobs = new Array<number>(spanCount);

  const evSpanIndices: number[] = [];
  const evTsDeltas: bigint[] = [];
  const evNames: number[] = [];
  const evAttrBlobs: number[] = [];

  let prevSpStart = 0n;
  let prevEvTs = 0n;

  for (let i = 0; i < spanCount; i++) {
    const s = batch.spans[i]!;
    const trIdx = traceIdxById.get(s.trace_id);
    spTraceIndices[i] = trIdx !== undefined ? trIdx : NONE_IDX;
    spTraceIds[i] = strings.intern(s.trace_id);
    spProjectIds[i] = strings.intern(s.project_id ?? "");
    spSpanIds[i] = strings.intern(s.span_id);
    spParentSpanIds[i] = strings.internOpt(s.parent_span_id);

    const linked = s.linked_span_ids ?? [];
    if (linked.length > 0xffff) {
      throw new PlstError(
        "RESOURCE_LIMIT_EXCEEDED",
        "span linked_span_ids count exceeds u16::MAX",
      );
    }
    spLinkedCounts[i] = linked.length;
    for (let j = 0; j < linked.length; j++) {
      spLinkedStrIds.push(strings.intern(linked[j]!));
    }
    spDepths[i] = s.depth ?? 0;

    spNames[i] = strings.intern(s.name);
    spTypes[i] = spanTypeToU8(s.span_type ?? "custom");
    spPhases[i] = spanPhaseToU8(s.span_phase ?? "final");
    spStatuses[i] = traceStatusToU8(s.status ?? "ok");

    const startTs = toI64BigInt(s.start_ts_ms);
    spStartDeltas[i] = BigInt.asIntN(64, startTs - prevSpStart);
    prevSpStart = startTs;

    if (s.end_ts_ms !== null && s.end_ts_ms !== undefined) {
      spHasEnd[i] = 1;
      spEndOffsets[i] = BigInt.asIntN(64, toI64BigInt(s.end_ts_ms) - startTs);
    } else {
      spHasEnd[i] = 0;
      spEndOffsets[i] = 0n;
    }
    spDurations[i] = toI64BigInt(s.duration_ms ?? 0);

    spProviders[i] = strings.internOpt(s.provider);
    spModels[i] = strings.internOpt(s.model);
    spModelIds[i] = strings.internOpt(s.model_id);
    spModelVersions[i] = strings.internOpt(s.model_version);
    spPromptTplIds[i] = strings.internOpt(s.prompt_template_id);
    spPromptTplVersions[i] = strings.internOpt(s.prompt_template_version);

    spInTokens[i] = toI64BigInt(s.input_tokens ?? 0);
    spOutTokens[i] = toI64BigInt(s.output_tokens ?? 0);
    spCachedTokens[i] = toI64BigInt(s.cached_tokens ?? 0);
    spTotalTokens[i] = toI64BigInt(s.total_tokens ?? 0);

    const costNanos =
      s.cost_nanos !== undefined && s.cost_nanos !== 0
        ? s.cost_nanos
        : usdToCostNanos(s.cost_usd ?? 0);
    spCostNanos[i] = toI64BigInt(costNanos);

    if (s.temperature !== null && s.temperature !== undefined) {
      spHasTemp[i] = 1;
      spTempBits[i] = f64ToU64Bits(s.temperature);
    } else {
      spHasTemp[i] = 0;
      spTempBits[i] = 0n;
    }
    spFinishReasons[i] = strings.internOpt(s.finish_reason);
    spModelParamsBlobs[i] = blobs.internValue(s.model_params);
    spToolNames[i] = strings.internOpt(s.tool_name);
    spToolVersions[i] = strings.internOpt(s.tool_version);
    spToolRetries[i] = s.tool_retry_count ?? 0;

    spErrorTypes[i] = strings.internOpt(s.error_type);
    spErrorMessages[i] = strings.internOpt(s.error_message);
    spErrorStacks[i] = strings.internOpt(s.error_stack);
    spInputBlobs[i] = blobs.internValue(s.input);
    spOutputBlobs[i] = blobs.internValue(s.output);
    spAttrBlobs[i] = blobs.internValue(s.attributes);
    spMetaBlobs[i] = blobs.internValue(s.metadata);

    const events = s.events ?? [];
    for (let j = 0; j < events.length; j++) {
      if (evSpanIndices.length >= MAX_PLST_EVENTS) {
        throw new PlstError(
          "RESOURCE_LIMIT_EXCEEDED",
          "span events exceed MAX_PLST_EVENTS",
        );
      }
      const ev = events[j]!;
      const evTs = toI64BigInt(ev.timestamp_ms);
      evSpanIndices.push(i);
      evTsDeltas.push(BigInt.asIntN(64, evTs - prevEvTs));
      prevEvTs = evTs;
      evNames.push(strings.intern(ev.name));
      evAttrBlobs.push(blobs.internValue(ev.attributes));
    }
  }

  const out = new ByteWriter(4096);

  // Section 1: String Table
  strings.writeTo(out);
  // Section 2: Blob Table
  blobs.writeTo(out);

  // Section 3: Trace Rows
  out.writeU32LE(traceCount);
  out.writeU32Slice(trTraceIds);
  out.writeU32Slice(trProjectIds);
  out.writeU32Slice(trSessionIds);
  out.writeU32Slice(trUserIds);
  out.writeU32Slice(trWorkflowIds);
  out.writeU32Slice(trParentTraceIds);
  out.writeU32Slice(trAgentNames);
  out.writeU32Slice(trAgentVersions);
  out.writeU32Slice(trPromptVersions);
  out.writeU32Slice(trModelCfgVersions);
  out.writeU32Slice(trToolCfgVersions);
  out.writeU32Slice(trAppReleases);
  out.writeU32Slice(trEnvironments);

  out.writeU8Slice(trStatuses);
  out.writeU8Slice(trFailureSources);
  out.writeI64Slice(trStartDeltas);
  out.writeU8Slice(trHasEnd);
  out.writeI64Slice(trEndOffsets);
  out.writeI64Slice(trDurations);

  out.writeI32Slice(trSpanCounts);
  out.writeI32Slice(trLlmCounts);
  out.writeI32Slice(trToolCounts);
  out.writeI32Slice(trErrSpanCounts);
  out.writeI64Slice(trInTokens);
  out.writeI64Slice(trOutTokens);
  out.writeI64Slice(trCachedTokens);
  out.writeI64Slice(trTotalTokens);
  out.writeI64Slice(trCostNanos);

  out.writeU32Slice(trPrimaryModels);
  out.writeU32Slice(trPrimaryProviders);
  out.writeU8Slice(trHasAvgEval);
  out.writeU64Slice(trAvgEvalBits);
  out.writeU8Slice(trHasPassRate);
  out.writeU64Slice(trPassRateBits);
  out.writeU8Slice(trHasLoop);
  out.writeU32Slice(trErrorTypes);
  out.writeU32Slice(trErrorMessages);

  out.writeU16Slice(trTagCounts);
  out.writeU32LE(trTagStrIds.length);
  out.writeU32Slice(trTagStrIds);
  out.writeU32Slice(trInputBlobs);
  out.writeU32Slice(trOutputBlobs);
  out.writeU32Slice(trMetadataBlobs);

  // Section 4: Span Rows
  out.writeU32LE(spanCount);
  out.writeU32Slice(spTraceIndices);
  out.writeU32Slice(spTraceIds);
  out.writeU32Slice(spProjectIds);
  out.writeU32Slice(spSpanIds);
  out.writeU32Slice(spParentSpanIds);
  out.writeU16Slice(spLinkedCounts);
  out.writeU32LE(spLinkedStrIds.length);
  out.writeU32Slice(spLinkedStrIds);
  out.writeI32Slice(spDepths);

  out.writeU32Slice(spNames);
  out.writeU8Slice(spTypes);
  out.writeU8Slice(spPhases);
  out.writeU8Slice(spStatuses);
  out.writeI64Slice(spStartDeltas);
  out.writeU8Slice(spHasEnd);
  out.writeI64Slice(spEndOffsets);
  out.writeI64Slice(spDurations);

  out.writeU32Slice(spProviders);
  out.writeU32Slice(spModels);
  out.writeU32Slice(spModelIds);
  out.writeU32Slice(spModelVersions);
  out.writeU32Slice(spPromptTplIds);
  out.writeU32Slice(spPromptTplVersions);
  out.writeI64Slice(spInTokens);
  out.writeI64Slice(spOutTokens);
  out.writeI64Slice(spCachedTokens);
  out.writeI64Slice(spTotalTokens);
  out.writeI64Slice(spCostNanos);
  out.writeU8Slice(spHasTemp);
  out.writeU64Slice(spTempBits);
  out.writeU32Slice(spFinishReasons);
  out.writeU32Slice(spModelParamsBlobs);
  out.writeU32Slice(spToolNames);
  out.writeU32Slice(spToolVersions);
  out.writeI32Slice(spToolRetries);

  out.writeU32Slice(spErrorTypes);
  out.writeU32Slice(spErrorMessages);
  out.writeU32Slice(spErrorStacks);
  out.writeU32Slice(spInputBlobs);
  out.writeU32Slice(spOutputBlobs);
  out.writeU32Slice(spAttrBlobs);
  out.writeU32Slice(spMetaBlobs);

  // Section 5: Columnar Span Events
  out.writeU32LE(evSpanIndices.length);
  out.writeU32Slice(evSpanIndices);
  out.writeI64Slice(evTsDeltas);
  out.writeU32Slice(evNames);
  out.writeU32Slice(evAttrBlobs);

  return out.finish();
}

export interface EncodePlstOptions {
  compression?: PlstCompressionMode;
}

export function encodePlstBatch(
  batch: TraceIngestBatch,
  options: EncodePlstOptions = {},
): Uint8Array {
  if (batch.traces.length > MAX_PLST_TRACES) {
    throw new PlstError(
      "RESOURCE_LIMIT_EXCEEDED",
      "trace count exceeds MAX_PLST_TRACES",
    );
  }
  if (batch.spans.length > MAX_PLST_SPANS) {
    throw new PlstError(
      "RESOURCE_LIMIT_EXCEEDED",
      "span count exceeds MAX_PLST_SPANS",
    );
  }

  const uncomp = encodeColumnarPayload(batch);
  if (uncomp.length > MAX_PLST_UNCOMP_BYTES) {
    throw new PlstError(
      "RESOURCE_LIMIT_EXCEEDED",
      "uncompressed PLST payload exceeds 64 MiB limit",
    );
  }

  const mode: PlstCompressionMode = options.compression ?? "auto";
  let effectiveMode: "dict" | "raw" | "none";
  if (mode === "auto") {
    effectiveMode = supportsZstd() ? "dict" : "none";
  } else {
    effectiveMode = mode;
  }

  let compFlag = 0;
  let dictId = 0;
  let body: Uint8Array;

  if (effectiveMode === "none") {
    compFlag = 0;
    dictId = 0;
    body = uncomp;
  } else if (effectiveMode === "raw") {
    compFlag = FLAG_RAW_ZSTD;
    dictId = 0;
    body = zstdCompress(uncomp, false);
  } else {
    compFlag = FLAG_DICT_ZSTD;
    dictId = AGENT_DICT_ID;
    body = zstdCompress(uncomp, true);
  }

  if (uncomp.length > 0xffffffff || body.length > 0xffffffff) {
    throw new PlstError(
      "RESOURCE_LIMIT_EXCEEDED",
      "payload length exceeds u32",
    );
  }

  const hasPartial =
    batch.traces.some((t) => t.status === "running") ||
    batch.spans.some((s) => s.span_phase !== "final");
  const hasFinal =
    batch.traces.some((t) => t.status !== "running") ||
    batch.spans.some((s) => s.span_phase === "final");
  const lifecycleFlags =
    !hasPartial && !hasFinal
      ? FLAG_FINAL
      : (hasPartial ? FLAG_PARTIAL : 0) | (hasFinal ? FLAG_FINAL : 0);
  const flags = compFlag | lifecycleFlags;

  const compressedCrc = crc32(body);

  const frame = new Uint8Array(PLST_HEADER_LEN + body.length);
  const view = new DataView(frame.buffer);
  frame.set(PLST_MAGIC, 0);
  frame[4] = PLST_VERSION;
  frame[5] = flags;
  view.setUint16(6, dictId, true);
  view.setUint32(8, uncomp.length, true);
  view.setUint32(12, body.length, true);
  view.setUint32(16, compressedCrc, true);
  frame.set(body, PLST_HEADER_LEN);
  return frame;
}

export function decodePlstBatch(bytes: Uint8Array): TraceIngestBatch {
  if (bytes.length < PLST_HEADER_LEN) {
    throw new PlstError("TRUNCATED_FRAME", "truncated PLST frame at 20B header");
  }
  for (let i = 0; i < 4; i++) {
    if (bytes[i] !== PLST_MAGIC[i]) {
      throw new PlstError("INVALID_MAGIC", "invalid PLST magic");
    }
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = bytes[4]!;
  if (version !== PLST_VERSION) {
    throw new PlstError("UNSUPPORTED_VERSION", `unsupported PLST version: ${version}`);
  }
  const flags = bytes[5]!;
  if ((flags & ~KNOWN_FLAGS_MASK) !== 0) {
    throw new PlstError("UNSUPPORTED_FLAGS", `unsupported PLST flags: 0x${flags.toString(16)}`);
  }
  const compBits = flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD);
  if (compBits === (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD)) {
    throw new PlstError("UNSUPPORTED_FLAGS", `mutually exclusive Zstd flags: 0x${flags.toString(16)}`);
  }
  const dictId = view.getUint16(6, true);
  if ((flags & FLAG_DICT_ZSTD) !== 0) {
    if (dictId !== AGENT_DICT_ID) {
      throw new PlstError("UNKNOWN_DICTIONARY", `unknown PLST dictionary id: ${dictId}`);
    }
  } else if (dictId !== 0) {
    throw new PlstError("INVALID_HEADER", "dict_id must be 0 when FLAG_DICT_ZSTD is unset");
  }

  const uncompLen = view.getUint32(8, true);
  if (uncompLen > MAX_PLST_UNCOMP_BYTES) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "header uncomp_len exceeds 64 MiB limit");
  }
  const compLen = view.getUint32(12, true);
  const expectedCrc = view.getUint32(16, true);

  const actualBodyLen = bytes.length - PLST_HEADER_LEN;
  if (actualBodyLen < compLen) {
    throw new PlstError("TRUNCATED_FRAME", "truncated PLST frame at body");
  }
  if (actualBodyLen > compLen) {
    throw new PlstError("TRAILING_BYTES", `unexpected trailing bytes: ${actualBodyLen - compLen}`);
  }
  if (compBits === 0 && compLen !== uncompLen) {
    throw new PlstError("LENGTH_MISMATCH", `length mismatch: expected ${uncompLen}, got ${compLen}`);
  }

  const body = bytes.subarray(PLST_HEADER_LEN);
  const actualCrc = crc32(body);
  if (actualCrc !== expectedCrc) {
    throw new PlstError(
      "CRC_MISMATCH",
      `PLST compressed CRC32 mismatch: expected 0x${expectedCrc.toString(16)}, got 0x${actualCrc.toString(16)}`,
    );
  }

  let payload: Uint8Array;
  if (compBits === 0) {
    payload = body;
  } else if (compBits === FLAG_RAW_ZSTD) {
    payload = zstdDecompress(body, false);
    if (payload.length !== uncompLen) {
      throw new PlstError("LENGTH_MISMATCH", `decompressed length mismatch: expected ${uncompLen}, got ${payload.length}`);
    }
  } else {
    payload = zstdDecompress(body, true);
    if (payload.length !== uncompLen) {
      throw new PlstError("LENGTH_MISMATCH", `decompressed length mismatch: expected ${uncompLen}, got ${payload.length}`);
    }
  }

  return decodeColumnarPayload(payload);
}

export function decodeColumnarPayload(payload: Uint8Array): TraceIngestBatch {
  const cursor = new ByteReader(payload);
  let materializedBytes = 0;

  // Section 1: String Table
  const stringCount = cursor.readU32("string_table.count");
  if (stringCount > MAX_PLST_STRINGS) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "string table count exceeds MAX_PLST_STRINGS");
  }
  const strOffsets = cursor.readU32Vec(stringCount + 1, "string_table.offsets");
  if (strOffsets[0] !== 0) {
    throw new PlstError("INVALID_HEADER", "string table offsets[0] must be 0");
  }
  const strTotalBytes = strOffsets[strOffsets.length - 1] ?? 0;
  const strData = cursor.readSlice(strTotalBytes, "string_table.data");
  const strings = new Array<string>(stringCount);
  for (let i = 0; i < stringCount; i++) {
    const start = strOffsets[i]!;
    const end = strOffsets[i + 1]!;
    if (end < start || end > strTotalBytes) {
      throw new PlstError("INVALID_HEADER", "non-monotonic string table offsets");
    }
    if (end - start > MAX_PLST_SINGLE_STRING_BYTES) {
      throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "single string exceeds MAX_PLST_SINGLE_STRING_BYTES");
    }
    try {
      strings[i] = textDecoder.decode(strData.subarray(start, end));
    } catch {
      throw new PlstError("INVALID_UTF8", "invalid UTF-8 in string table");
    }
  }

  const getReqStr = (idx: number): string => {
    if (idx === NONE_IDX || idx >= strings.length) {
      throw new PlstError("INVALID_STRING_INDEX", `invalid string index: ${idx}`);
    }
    const s = strings[idx]!;
    materializedBytes += strOffsets[idx + 1]! - strOffsets[idx]!;
    if (materializedBytes > MAX_PLST_MATERIALIZED_BYTES) {
      throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "materialized bytes exceed 64 MiB cap");
    }
    return s;
  };
  const getOptStr = (idx: number): string | null =>
    idx === NONE_IDX ? null : getReqStr(idx);

  // Section 2: Blob Table
  const blobCount = cursor.readU32("blob_table.count");
  if (blobCount > MAX_PLST_BLOBS) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "blob count exceeds MAX_PLST_BLOBS");
  }
  const blobHashes = cursor.readU64Vec(blobCount, "blob_table.xxh3_64");
  const blobEncodings = cursor.readU8Vec(blobCount, "blob_table.encodings");
  for (let i = 0; i < blobCount; i++) {
    const enc = blobEncodings[i]!;
    if (enc !== BLOB_ENC_JSON_UTF8 && enc !== BLOB_ENC_UTF8_TEXT) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid blob_encoding tag: ${enc}`);
    }
  }
  const blobOffsets = cursor.readU32Vec(blobCount + 1, "blob_table.offsets");
  if (blobOffsets[0] !== 0) {
    throw new PlstError("INVALID_HEADER", "blob table offsets[0] must be 0");
  }
  const blobTotalBytes = blobOffsets[blobOffsets.length - 1] ?? 0;
  const blobData = cursor.readSlice(blobTotalBytes, "blob_table.data");
  for (let i = 0; i < blobCount; i++) {
    const start = blobOffsets[i]!;
    const end = blobOffsets[i + 1]!;
    if (end < start || end > blobTotalBytes) {
      throw new PlstError("INVALID_HEADER", "non-monotonic blob table offsets");
    }
    const actual = xxh3_64Bytes(blobData.subarray(start, end));
    if (actual !== blobHashes[i]!) {
      throw new PlstError(
        "BLOB_CHECKSUM_MISMATCH",
        `blob ${i} xxh3_64 mismatch: expected 0x${blobHashes[i]!.toString(16)}, got 0x${actual.toString(16)}`,
      );
    }
  }

  const parsedBlobCache = new Array<JsonValue | undefined>(blobCount);
  const getBlobValue = (idx: number): JsonValue => {
    if (idx === NONE_IDX) {
      return null;
    }
    if (idx >= blobCount) {
      throw new PlstError("INVALID_BLOB_INDEX", `invalid blob index: ${idx}`);
    }
    const start = blobOffsets[idx]!;
    const end = blobOffsets[idx + 1]!;
    materializedBytes += end - start;
    if (materializedBytes > MAX_PLST_MATERIALIZED_BYTES) {
      throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "materialized blob bytes exceed 64 MiB cap");
    }
    const cached = parsedBlobCache[idx];
    if (cached !== undefined) {
      return cached;
    }
    const slice = blobData.subarray(start, end);
    let str: string;
    try {
      str = textDecoder.decode(slice);
    } catch {
      throw new PlstError("INVALID_UTF8", "invalid UTF-8 in blob table");
    }
    const val: JsonValue =
      blobEncodings[idx] === BLOB_ENC_UTF8_TEXT
        ? str
        : (JSON.parse(str) as JsonValue);
    parsedBlobCache[idx] = val;
    return val;
  };

  // Section 3: Trace Rows
  const traceCount = cursor.readU32("traces.count");
  if (traceCount > MAX_PLST_TRACES) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "trace count exceeds MAX_PLST_TRACES");
  }
  const trTraceIds = cursor.readU32Vec(traceCount, "traces.trace_ids");
  const trProjectIds = cursor.readU32Vec(traceCount, "traces.project_ids");
  const trSessionIds = cursor.readU32Vec(traceCount, "traces.session_ids");
  const trUserIds = cursor.readU32Vec(traceCount, "traces.user_ids");
  const trWorkflowIds = cursor.readU32Vec(traceCount, "traces.workflow_ids");
  const trParentTraceIds = cursor.readU32Vec(traceCount, "traces.parent_trace_ids");
  const trAgentNames = cursor.readU32Vec(traceCount, "traces.agent_names");
  const trAgentVersions = cursor.readU32Vec(traceCount, "traces.agent_versions");
  const trPromptVersions = cursor.readU32Vec(traceCount, "traces.prompt_versions");
  const trModelCfgVersions = cursor.readU32Vec(traceCount, "traces.model_config_versions");
  const trToolCfgVersions = cursor.readU32Vec(traceCount, "traces.tool_config_versions");
  const trAppReleases = cursor.readU32Vec(traceCount, "traces.app_releases");
  const trEnvironments = cursor.readU32Vec(traceCount, "traces.environments");

  const trStatuses = cursor.readU8Vec(traceCount, "traces.statuses");
  const trFailureSources = cursor.readU8Vec(traceCount, "traces.failure_sources");
  const trStartDeltas = cursor.readI64Vec(traceCount, "traces.start_ts_deltas");
  const trHasEnd = cursor.readU8Vec(traceCount, "traces.has_end_ts");
  const trEndOffsets = cursor.readI64Vec(traceCount, "traces.end_ts_offsets");
  const trDurations = cursor.readI64Vec(traceCount, "traces.duration_ms");

  const trSpanCounts = cursor.readI32Vec(traceCount, "traces.span_counts");
  const trLlmCounts = cursor.readI32Vec(traceCount, "traces.llm_call_counts");
  const trToolCounts = cursor.readI32Vec(traceCount, "traces.tool_call_counts");
  const trErrSpanCounts = cursor.readI32Vec(traceCount, "traces.error_span_counts");
  const trInTokens = cursor.readI64Vec(traceCount, "traces.input_tokens");
  const trOutTokens = cursor.readI64Vec(traceCount, "traces.output_tokens");
  const trCachedTokens = cursor.readI64Vec(traceCount, "traces.cached_tokens");
  const trTotalTokens = cursor.readI64Vec(traceCount, "traces.total_tokens");
  const trCostNanos = cursor.readI64Vec(traceCount, "traces.total_cost_nanos");

  const trPrimaryModels = cursor.readU32Vec(traceCount, "traces.primary_models");
  const trPrimaryProviders = cursor.readU32Vec(traceCount, "traces.primary_providers");
  const trHasAvgEval = cursor.readU8Vec(traceCount, "traces.has_avg_eval");
  const trAvgEvalBits = cursor.readU64Vec(traceCount, "traces.avg_eval_bits");
  const trHasPassRate = cursor.readU8Vec(traceCount, "traces.has_pass_rate");
  const trPassRateBits = cursor.readU64Vec(traceCount, "traces.pass_rate_bits");
  const trHasLoop = cursor.readU8Vec(traceCount, "traces.has_loop_warning");
  const trErrorTypes = cursor.readU32Vec(traceCount, "traces.error_types");
  const trErrorMessages = cursor.readU32Vec(traceCount, "traces.error_messages");

  const trTagCounts = cursor.readU16Vec(traceCount, "traces.tag_counts");
  const totalTags = cursor.readU32("traces.total_tags");
  let sumTags = 0;
  for (let i = 0; i < traceCount; i++) {
    sumTags += trTagCounts[i]!;
  }
  if (sumTags !== totalTags) {
    throw new PlstError("LENGTH_MISMATCH", `tag count mismatch: expected ${sumTags}, got ${totalTags}`);
  }
  const trTagStrIds = cursor.readU32Vec(totalTags, "traces.tag_str_ids");
  const trInputBlobs = cursor.readU32Vec(traceCount, "traces.input_blobs");
  const trOutputBlobs = cursor.readU32Vec(traceCount, "traces.output_blobs");
  const trMetadataBlobs = cursor.readU32Vec(traceCount, "traces.metadata_blobs");

  const traces = new Array<AgentTrace>(traceCount);
  let prevTrStart = 0n;
  let tagCursor = 0;
  for (let i = 0; i < traceCount; i++) {
    const startTs = BigInt.asIntN(64, prevTrStart + trStartDeltas[i]!);
    prevTrStart = startTs;
    const hasEnd = trHasEnd[i]!;
    if (hasEnd !== 0 && hasEnd !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid has_end_ts tag: ${hasEnd}`);
    }
    const endTsMs =
      hasEnd === 1 ? Number(BigInt.asIntN(64, startTs + trEndOffsets[i]!)) : null;

    const hasAvg = trHasAvgEval[i]!;
    if (hasAvg !== 0 && hasAvg !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid has_avg_eval_score tag: ${hasAvg}`);
    }
    const hasPass = trHasPassRate[i]!;
    if (hasPass !== 0 && hasPass !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid has_eval_pass_rate tag: ${hasPass}`);
    }
    const hasLoop = trHasLoop[i]!;
    if (hasLoop !== 0 && hasLoop !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid has_loop_warning tag: ${hasLoop}`);
    }

    const tc = trTagCounts[i]!;
    const tags = new Array<string>(tc);
    for (let j = 0; j < tc; j++) {
      tags[j] = getReqStr(trTagStrIds[tagCursor + j]!);
    }
    tagCursor += tc;

    const totalCostNanos = trCostNanos[i]!;
    traces[i] = {
      trace_id: getReqStr(trTraceIds[i]!),
      project_id: getReqStr(trProjectIds[i]!),
      agent_name: getReqStr(trAgentNames[i]!),
      agent_version: getReqStr(trAgentVersions[i]!),
      prompt_version: getOptStr(trPromptVersions[i]!),
      model_config_version: getOptStr(trModelCfgVersions[i]!),
      tool_config_version: getOptStr(trToolCfgVersions[i]!),
      app_release: getOptStr(trAppReleases[i]!),
      environment: getReqStr(trEnvironments[i]!),
      session_id: getOptStr(trSessionIds[i]!),
      user_id: getOptStr(trUserIds[i]!),
      workflow_id: getOptStr(trWorkflowIds[i]!),
      parent_trace_id: getOptStr(trParentTraceIds[i]!),
      status: traceStatusFromU8(trStatuses[i]!),
      start_ts_ms: Number(startTs),
      end_ts_ms: endTsMs,
      duration_ms: Number(trDurations[i]!),
      input: getBlobValue(trInputBlobs[i]!),
      output: getBlobValue(trOutputBlobs[i]!),
      error_type: getOptStr(trErrorTypes[i]!),
      error_message: getOptStr(trErrorMessages[i]!),
      failure_source: failureSourceFromU8(trFailureSources[i]!),
      metadata: getBlobValue(trMetadataBlobs[i]!),
      tags,
      span_count: trSpanCounts[i]!,
      llm_call_count: trLlmCounts[i]!,
      tool_call_count: trToolCounts[i]!,
      error_span_count: trErrSpanCounts[i]!,
      input_tokens: Number(trInTokens[i]!),
      output_tokens: Number(trOutTokens[i]!),
      cached_tokens: Number(trCachedTokens[i]!),
      total_tokens: Number(trTotalTokens[i]!),
      total_cost_nanos: Number(totalCostNanos),
      total_cost_usd: costNanosToUsd(totalCostNanos),
      primary_model: getOptStr(trPrimaryModels[i]!),
      primary_provider: getOptStr(trPrimaryProviders[i]!),
      avg_eval_score: hasAvg === 1 ? u64BitsToF64(trAvgEvalBits[i]!) : null,
      eval_pass_rate: hasPass === 1 ? u64BitsToF64(trPassRateBits[i]!) : null,
      has_loop_warning: hasLoop === 1,
    };
  }

  // Section 4: Span Rows
  const spanCount = cursor.readU32("spans.count");
  if (spanCount > MAX_PLST_SPANS) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "span count exceeds MAX_PLST_SPANS");
  }
  const spTraceIndices = cursor.readU32Vec(spanCount, "spans.trace_indices");
  const spTraceIds = cursor.readU32Vec(spanCount, "spans.trace_id_str_ids");
  const spProjectIds = cursor.readU32Vec(spanCount, "spans.project_id_str_ids");
  const spSpanIds = cursor.readU32Vec(spanCount, "spans.span_id_str_ids");
  const spParentSpanIds = cursor.readU32Vec(spanCount, "spans.parent_span_id_str_ids");
  const spLinkedCounts = cursor.readU16Vec(spanCount, "spans.linked_counts");
  const totalLinked = cursor.readU32("spans.total_linked");
  let sumLinked = 0;
  for (let i = 0; i < spanCount; i++) {
    sumLinked += spLinkedCounts[i]!;
  }
  if (sumLinked !== totalLinked) {
    throw new PlstError("LENGTH_MISMATCH", `linked count mismatch: expected ${sumLinked}, got ${totalLinked}`);
  }
  const spLinkedStrIds = cursor.readU32Vec(totalLinked, "spans.linked_span_str_ids");
  const spDepths = cursor.readI32Vec(spanCount, "spans.depths");

  const spNames = cursor.readU32Vec(spanCount, "spans.names");
  const spTypes = cursor.readU8Vec(spanCount, "spans.span_types");
  const spPhases = cursor.readU8Vec(spanCount, "spans.span_phases");
  const spStatuses = cursor.readU8Vec(spanCount, "spans.statuses");
  const spStartDeltas = cursor.readI64Vec(spanCount, "spans.start_ts_deltas");
  const spHasEnd = cursor.readU8Vec(spanCount, "spans.has_end_ts");
  const spEndOffsets = cursor.readI64Vec(spanCount, "spans.end_ts_offsets");
  const spDurations = cursor.readI64Vec(spanCount, "spans.duration_ms");

  const spProviders = cursor.readU32Vec(spanCount, "spans.providers");
  const spModels = cursor.readU32Vec(spanCount, "spans.models");
  const spModelIds = cursor.readU32Vec(spanCount, "spans.model_ids");
  const spModelVersions = cursor.readU32Vec(spanCount, "spans.model_versions");
  const spPromptTplIds = cursor.readU32Vec(spanCount, "spans.prompt_template_ids");
  const spPromptTplVersions = cursor.readU32Vec(spanCount, "spans.prompt_template_versions");
  const spInTokens = cursor.readI64Vec(spanCount, "spans.input_tokens");
  const spOutTokens = cursor.readI64Vec(spanCount, "spans.output_tokens");
  const spCachedTokens = cursor.readI64Vec(spanCount, "spans.cached_tokens");
  const spTotalTokens = cursor.readI64Vec(spanCount, "spans.total_tokens");
  const spCostNanos = cursor.readI64Vec(spanCount, "spans.cost_nanos");
  const spHasTemp = cursor.readU8Vec(spanCount, "spans.has_temperature");
  const spTempBits = cursor.readU64Vec(spanCount, "spans.temperature_bits");
  const spFinishReasons = cursor.readU32Vec(spanCount, "spans.finish_reasons");
  const spModelParamsBlobs = cursor.readU32Vec(spanCount, "spans.model_params_blobs");
  const spToolNames = cursor.readU32Vec(spanCount, "spans.tool_names");
  const spToolVersions = cursor.readU32Vec(spanCount, "spans.tool_versions");
  const spToolRetries = cursor.readI32Vec(spanCount, "spans.tool_retry_counts");

  const spErrorTypes = cursor.readU32Vec(spanCount, "spans.error_types");
  const spErrorMessages = cursor.readU32Vec(spanCount, "spans.error_messages");
  const spErrorStacks = cursor.readU32Vec(spanCount, "spans.error_stacks");
  const spInputBlobs = cursor.readU32Vec(spanCount, "spans.input_blobs");
  const spOutputBlobs = cursor.readU32Vec(spanCount, "spans.output_blobs");
  const spAttrBlobs = cursor.readU32Vec(spanCount, "spans.attributes_blobs");
  const spMetaBlobs = cursor.readU32Vec(spanCount, "spans.metadata_blobs");

  const spans = new Array<AgentSpan>(spanCount);
  let prevSpStart = 0n;
  let linkedCursor = 0;
  for (let i = 0; i < spanCount; i++) {
    const traceId = getReqStr(spTraceIds[i]!);
    const trIdx = spTraceIndices[i]!;
    if (trIdx !== NONE_IDX) {
      if (trIdx >= traces.length || traces[trIdx]!.trace_id !== traceId) {
        throw new PlstError("INVALID_TRACE_INDEX", `invalid trace index: ${trIdx}`);
      }
    }
    const startTs = BigInt.asIntN(64, prevSpStart + spStartDeltas[i]!);
    prevSpStart = startTs;
    const hasEnd = spHasEnd[i]!;
    if (hasEnd !== 0 && hasEnd !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid span_has_end_ts tag: ${hasEnd}`);
    }
    const endTsMs =
      hasEnd === 1 ? Number(BigInt.asIntN(64, startTs + spEndOffsets[i]!)) : null;

    const hasTemp = spHasTemp[i]!;
    if (hasTemp !== 0 && hasTemp !== 1) {
      throw new PlstError("INVALID_ENUM_TAG", `invalid has_temperature tag: ${hasTemp}`);
    }

    const lc = spLinkedCounts[i]!;
    const linkedSpanIds = new Array<string>(lc);
    for (let j = 0; j < lc; j++) {
      linkedSpanIds[j] = getReqStr(spLinkedStrIds[linkedCursor + j]!);
    }
    linkedCursor += lc;

    const costNanos = spCostNanos[i]!;
    spans[i] = {
      span_id: getReqStr(spSpanIds[i]!),
      trace_id: traceId,
      project_id: getReqStr(spProjectIds[i]!),
      parent_span_id: getOptStr(spParentSpanIds[i]!),
      linked_span_ids: linkedSpanIds,
      span_phase: spanPhaseFromU8(spPhases[i]!),
      depth: spDepths[i]!,
      name: getReqStr(spNames[i]!),
      span_type: spanTypeFromU8(spTypes[i]!),
      status: traceStatusFromU8(spStatuses[i]!),
      start_ts_ms: Number(startTs),
      end_ts_ms: endTsMs,
      duration_ms: Number(spDurations[i]!),
      provider: getOptStr(spProviders[i]!),
      model: getOptStr(spModels[i]!),
      model_id: getOptStr(spModelIds[i]!),
      model_version: getOptStr(spModelVersions[i]!),
      prompt_template_id: getOptStr(spPromptTplIds[i]!),
      prompt_template_version: getOptStr(spPromptTplVersions[i]!),
      input_tokens: Number(spInTokens[i]!),
      output_tokens: Number(spOutTokens[i]!),
      cached_tokens: Number(spCachedTokens[i]!),
      total_tokens: Number(spTotalTokens[i]!),
      cost_nanos: Number(costNanos),
      cost_usd: costNanosToUsd(costNanos),
      temperature: hasTemp === 1 ? u64BitsToF64(spTempBits[i]!) : null,
      finish_reason: getOptStr(spFinishReasons[i]!),
      model_params: getBlobValue(spModelParamsBlobs[i]!),
      tool_name: getOptStr(spToolNames[i]!),
      tool_version: getOptStr(spToolVersions[i]!),
      tool_retry_count: spToolRetries[i]!,
      input: getBlobValue(spInputBlobs[i]!),
      output: getBlobValue(spOutputBlobs[i]!),
      attributes: getBlobValue(spAttrBlobs[i]!),
      events: [],
      metadata: getBlobValue(spMetaBlobs[i]!),
      error_type: getOptStr(spErrorTypes[i]!),
      error_message: getOptStr(spErrorMessages[i]!),
      error_stack: getOptStr(spErrorStacks[i]!),
    };
  }

  // Section 5: Columnar Span Events
  const eventCount = cursor.readU32("events.count");
  if (eventCount > MAX_PLST_EVENTS) {
    throw new PlstError("RESOURCE_LIMIT_EXCEEDED", "event count exceeds MAX_PLST_EVENTS");
  }
  const evSpanIndices = cursor.readU32Vec(eventCount, "events.span_indices");
  const evTsDeltas = cursor.readI64Vec(eventCount, "events.timestamp_deltas");
  const evNames = cursor.readU32Vec(eventCount, "events.names");
  const evAttrBlobs = cursor.readU32Vec(eventCount, "events.attributes_blobs");

  let prevEvTs = 0n;
  for (let i = 0; i < eventCount; i++) {
    const sIdx = evSpanIndices[i]!;
    if (sIdx >= spans.length) {
      throw new PlstError("INVALID_SPAN_INDEX", `invalid span index: ${sIdx}`);
    }
    const ts = BigInt.asIntN(64, prevEvTs + evTsDeltas[i]!);
    prevEvTs = ts;
    const ev: SpanEvent = {
      name: getReqStr(evNames[i]!),
      timestamp_ms: Number(ts),
      attributes: getBlobValue(evAttrBlobs[i]!),
    };
    spans[sIdx]!.events.push(ev);
  }

  if (cursor.remaining() !== 0) {
    throw new PlstError("TRAILING_BYTES", `unexpected trailing bytes in PLST frame: ${cursor.remaining()}`);
  }

  return { traces, spans };
}
