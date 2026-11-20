import type { PulseWebClient, WireNode } from "@pulse-analytics/ts-web";

/** Snapshot returned by the native iOS/Android `PulseNativeRecorder` module. */
export interface NativeWireframeSnapshot {
  width: number;
  height: number;
  nodes: WireNode[];
}

/** Native module interface implemented by `PulseNativeRecorder.swift` (iOS) and `PulseNativeRecorderModule.kt` (Android). */
export interface PulseNativeRecorderModule {
  /**
   * Walks all visible native windows (including modals, dialogs, and bottom sheets),
   * computes ancestor clip rects, extracts actual sRGB background colors, and masks all text/inputs.
   */
  captureWireframe(options?: { maxNodes?: number }): Promise<NativeWireframeSnapshot>;
  /** Optional synchronous CSPRNG backed by `SecRandomCopyBytes` (iOS) / `SecureRandom` (Android). */
  randomBytesSync?(length: number): number[];
}

export interface NativeRecorderOptions {
  /** Snapshot polling interval in milliseconds (default `1000`). Set to `0` for manual/event-driven only. */
  intervalMs?: number;
  /** Maximum nodes captured per frame (default `160`). */
  maxNodes?: number;
}

/**
 * Coordinates the native iOS/Android view-hierarchy wireframe recorder and feeds frames into
 * `PulseWebClient.recordWireframeSnapshot()` so only incremental diffs (`node_upsert`/`node_remove`)
 * are sent over the wire.
 */
export class NativeWireframeRecorder {
  private readonly client: PulseWebClient;
  private readonly nativeModule: PulseNativeRecorderModule;
  private readonly intervalMs: number;
  private readonly maxNodes: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight: Promise<boolean> | null = null;
  private running = false;

  constructor(
    client: PulseWebClient,
    nativeModule: PulseNativeRecorderModule,
    options: NativeRecorderOptions = {},
  ) {
    this.client = client;
    this.nativeModule = nativeModule;
    this.intervalMs = options.intervalMs ?? 1_000;
    this.maxNodes = options.maxNodes ?? 160;
  }

  public start(): void {
    if (this.running) return;
    this.running = true;
    void this.captureOnce();
    if (this.intervalMs > 0) {
      this.timer = setInterval(() => {
        void this.captureOnce();
      }, this.intervalMs);
    }
  }

  public stop(): void {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  public isRunning(): boolean {
    return this.running;
  }

  /** Captures a single native wireframe frame and submits it for incremental diffing. */
  public async captureOnce(forceFull = false): Promise<boolean> {
    if (!this.client.isRecordingSession()) return false;
    if (this.inFlight) {
      await this.inFlight;
    }
    if (!this.client.isRecordingSession()) return false;
    const run = (async () => {
      try {
        const snap = await this.nativeModule.captureWireframe({ maxNodes: this.maxNodes });
        if (!snap || !Array.isArray(snap.nodes) || snap.width <= 0 || snap.height <= 0) {
          return false;
        }
        this.client.recordWireframeSnapshot(snap.width, snap.height, snap.nodes, { forceFull });
        return true;
      } catch {
        // Never crash the host app if native view traversal fails during a transition.
        return false;
      }
    })();
    this.inFlight = run.finally(() => {
      if (this.inFlight === run) this.inFlight = null;
    });
    return run;
  }
}
