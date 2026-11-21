import type { OverflowPolicy } from "./types.js";

export interface PushResult<T> {
  accepted: boolean;
  dropped?: T;
}

export class RingBuffer<T> {
  private readonly buf: Array<T | undefined>;
  private readonly capacity: number;
  private readonly overflowPolicy: OverflowPolicy;
  private head = 0;
  private tail = 0;
  private count = 0;

  constructor(capacity = 2048, overflowPolicy: OverflowPolicy = "drop_oldest") {
    this.capacity = Math.max(1, Math.trunc(capacity));
    this.overflowPolicy = overflowPolicy;
    this.buf = new Array<T | undefined>(this.capacity);
  }

  get length(): number {
    return this.count;
  }

  get maxCapacity(): number {
    return this.capacity;
  }

  push(item: T): PushResult<T> {
    if (this.count < this.capacity) {
      this.buf[this.tail] = item;
      this.tail = (this.tail + 1) % this.capacity;
      this.count += 1;
      return { accepted: true };
    }
    if (this.overflowPolicy === "drop_newest") {
      return { accepted: false, dropped: item };
    }
    const evicted = this.buf[this.head]!;
    this.buf[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.buf[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;
    return { accepted: true, dropped: evicted };
  }

  shift(): T | undefined {
    if (this.count === 0) {
      return undefined;
    }
    const item = this.buf[this.head];
    this.buf[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.count -= 1;
    return item;
  }

  peek(): T | undefined {
    if (this.count === 0) return undefined;
    return this.buf[this.head];
  }

  drain(maxItems: number): T[] {
    const n = Math.min(this.count, Math.max(0, Math.trunc(maxItems)));
    const out = new Array<T>(n);
    for (let i = 0; i < n; i++) {
      out[i] = this.shift()!;
    }
    return out;
  }

  clear(): void {
    this.buf.fill(undefined);
    this.head = 0;
    this.tail = 0;
    this.count = 0;
  }
}
