/**
 * IEEE 802.3 CRC-32 matching `crc32fast::hash`.
 * Uses `node:zlib.crc32` when available synchronously via `process.getBuiltinModule`,
 * with a pure-TypeScript lookup-table fallback for browsers and Edge runtimes.
 */

interface ZlibCrc32Module {
  crc32?: (data: Uint8Array) => number;
}

interface GlobalWithProcess {
  process?: {
    getBuiltinModule?: (id: string) => unknown;
  };
}

let cachedTable: Uint32Array | null = null;

function getCrc32Table(): Uint32Array {
  if (cachedTable !== null) {
    return cachedTable;
  }
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) !== 0 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[i] = c >>> 0;
  }
  cachedTable = table;
  return table;
}

export function crc32Pure(bytes: Uint8Array): number {
  const table = getCrc32Table();
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    crc = table[(crc ^ bytes[i]!) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function getNativeCrc32(): ((data: Uint8Array) => number) | null {
  const proc = (globalThis as GlobalWithProcess).process;
  if (!proc || typeof proc.getBuiltinModule !== "function") {
    return null;
  }
  const mod =
    (proc.getBuiltinModule("node:zlib") as ZlibCrc32Module | undefined) ??
    (proc.getBuiltinModule("zlib") as ZlibCrc32Module | undefined);
  if (mod && typeof mod.crc32 === "function") {
    return mod.crc32;
  }
  return null;
}

export function crc32(bytes: Uint8Array): number {
  const nativeFn = getNativeCrc32();
  if (nativeFn !== null) {
    return nativeFn(bytes) >>> 0;
  }
  return crc32Pure(bytes);
}
