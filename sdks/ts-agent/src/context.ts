import type { PulseContext } from "./types.js";

interface AsyncLocalStorageLike<T> {
  getStore(): T | undefined;
  run<R>(store: T, callback: () => R): R;
}

interface AsyncHooksModule {
  AsyncLocalStorage?: new <T>() => AsyncLocalStorageLike<T>;
}

interface GlobalWithProcess {
  process?: {
    getBuiltinModule?: (id: string) => unknown;
    versions?: { node?: string };
  };
}

let cachedAlsCtor: (new <T>() => AsyncLocalStorageLike<T>) | null | undefined;

function detectAsyncLocalStorageSync(): (new <T>() => AsyncLocalStorageLike<T>) | null {
  if (cachedAlsCtor !== undefined) {
    return cachedAlsCtor;
  }
  const proc = (globalThis as GlobalWithProcess).process;
  if (proc && typeof proc.getBuiltinModule === "function") {
    const mod =
      (proc.getBuiltinModule("node:async_hooks") as AsyncHooksModule | undefined) ??
      (proc.getBuiltinModule("async_hooks") as AsyncHooksModule | undefined);
    if (mod && typeof mod.AsyncLocalStorage === "function") {
      cachedAlsCtor = mod.AsyncLocalStorage;
      return cachedAlsCtor;
    }
  }
  cachedAlsCtor = null;
  return null;
}

function isPromiseLike<T>(val: unknown): val is PromiseLike<T> {
  return (
    val !== null &&
    (typeof val === "object" || typeof val === "function") &&
    typeof (val as { then?: unknown }).then === "function"
  );
}

export class ContextManager {
  private als: AsyncLocalStorageLike<PulseContext> | null;
  private readonly fallbackStack: PulseContext[] = [];
  private readonly allowAls: boolean;

  constructor(options: { forceFallback?: boolean } = {}) {
    this.allowAls = !options.forceFallback;
    if (this.allowAls) {
      const Ctor = detectAsyncLocalStorageSync();
      this.als = Ctor ? new Ctor<PulseContext>() : null;
    } else {
      this.als = null;
    }
  }

  get usingAsyncLocalStorage(): boolean {
    return this.als !== null;
  }

  async initNodeAsyncContext(): Promise<boolean> {
    if (!this.allowAls || this.als !== null) {
      return this.als !== null;
    }
    const proc = (globalThis as GlobalWithProcess).process;
    if (!proc || !proc.versions?.node) {
      return false;
    }
    try {
      const modName = ["node", "async_hooks"].join(":");
      const mod = (await import(/* @vite-ignore */ modName)) as AsyncHooksModule;
      if (typeof mod.AsyncLocalStorage === "function") {
        cachedAlsCtor = mod.AsyncLocalStorage;
        this.als = new mod.AsyncLocalStorage<PulseContext>();
        return true;
      }
    } catch {
      // Non-Node or restricted bundler environment; retain stack fallback.
    }
    return false;
  }

  getContext(): PulseContext {
    if (this.als !== null) {
      return this.als.getStore() ?? {};
    }
    return this.fallbackStack.length > 0
      ? this.fallbackStack[this.fallbackStack.length - 1]!
      : {};
  }

  runWithContext<R>(patch: Partial<PulseContext>, fn: () => R): R {
    const parent = this.getContext();
    const merged: PulseContext = { ...parent };
    for (const [k, v] of Object.entries(patch) as Array<[keyof PulseContext, unknown]>) {
      if (k === "parentSpanId" && v === null) {
        delete merged.parentSpanId;
      } else if (v !== undefined) {
        (merged as Record<string, unknown>)[k] = v;
      }
    }

    if (this.als !== null) {
      return this.als.run(merged, fn);
    }

    this.fallbackStack.push(merged);
    let popped = false;
    const pop = (): void => {
      if (!popped) {
        popped = true;
        this.fallbackStack.pop();
      }
    };

    try {
      const result = fn();
      if (isPromiseLike(result)) {
        return Promise.resolve(result).then(
          (val) => {
            pop();
            return val;
          },
          (err) => {
            pop();
            throw err;
          },
        ) as unknown as R;
      }
      pop();
      return result;
    } catch (err) {
      pop();
      throw err;
    }
  }
}

export const defaultContextManager = new ContextManager();
