import type { PulseStorage } from "@pulse-analytics/ts-web";

/** Minimal subset of `@react-native-async-storage/async-storage` used by Pulse. */
export interface AsyncStorageLike {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
}

/** Minimal subset of `react-native-mmkv` used by Pulse. */
export interface MmkvLike {
  getString(key: string): string | undefined;
  set(key: string, value: string): void;
  delete(key: string): void;
}

/** Wraps `@react-native-async-storage/async-storage` as a `PulseStorage`. */
export function createAsyncStorageAdapter(asyncStorage: AsyncStorageLike): PulseStorage {
  return {
    getItem: (key) => asyncStorage.getItem(key),
    setItem: (key, value) => asyncStorage.setItem(key, value),
    removeItem: (key) => asyncStorage.removeItem(key),
  };
}

/** Wraps a `react-native-mmkv` instance as a synchronous `PulseStorage`. */
export function createMmkvStorageAdapter(mmkv: MmkvLike): PulseStorage {
  return {
    getItem: (key) => mmkv.getString(key) ?? null,
    setItem: (key, value) => {
      mmkv.set(key, value);
    },
    removeItem: (key) => {
      mmkv.delete(key);
    },
  };
}

/** In-memory `PulseStorage` fallback when no persistent RN storage is configured. */
export function createMemoryStorage(): PulseStorage {
  const map = new Map<string, string>();
  return {
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => {
      map.set(key, value);
    },
    removeItem: (key) => {
      map.delete(key);
    },
  };
}
