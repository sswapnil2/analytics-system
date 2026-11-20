import {
  FIXED_MASK,
  PulseWebClient,
  cohortMemberHash,
  evaluateEdgeFlag,
  flagRolloutSlot,
  flagVariantSlot,
  maskPiiText,
  xxh3_64,
  type EdgeFlagRule,
  type EventProperties,
  type NodeKind,
  type PropPrimitive,
  type PulseStorage,
  type PulseWebConfig,
  type RevenueParams,
  type WireNode,
  type WireframeOp,
} from "@pulse-analytics/ts-web";
import {
  NativeWireframeRecorder,
  type NativeRecorderOptions,
  type NativeWireframeSnapshot,
  type PulseNativeRecorderModule,
} from "./recorder.js";
import {
  createAsyncStorageAdapter,
  createMemoryStorage,
  createMmkvStorageAdapter,
  type AsyncStorageLike,
  type MmkvLike,
} from "./storage.js";

export {
  FIXED_MASK,
  NativeWireframeRecorder,
  cohortMemberHash,
  createAsyncStorageAdapter,
  createMemoryStorage,
  createMmkvStorageAdapter,
  evaluateEdgeFlag,
  flagRolloutSlot,
  flagVariantSlot,
  maskPiiText,
  xxh3_64,
};
export type {
  AsyncStorageLike,
  EdgeFlagRule,
  EventProperties,
  MmkvLike,
  NativeRecorderOptions,
  NativeWireframeSnapshot,
  NodeKind,
  PropPrimitive,
  PulseNativeRecorderModule,
  PulseStorage,
  RevenueParams,
  WireNode,
  WireframeOp,
};

/** Minimal React Native `AppState` shape (passed in or auto-detected). */
export interface AppStateLike {
  currentState?: string;
  addEventListener(
    type: "change",
    listener: (nextState: string) => void,
  ): { remove(): void };
}

/** Minimal React Native `Platform` shape. */
export interface PlatformLike {
  OS: string;
  Version?: string | number;
}

/** Minimal React Native `Dimensions` shape. */
export interface DimensionsLike {
  get(dim: "window" | "screen"): { width: number; height: number };
}

/** Minimal React Native `ErrorUtils` global present in Hermes / Metro bundles. */
interface ErrorUtilsLike {
  getGlobalHandler?: () => ((error: unknown, isFatal?: boolean) => void) | undefined;
  setGlobalHandler?: (handler: (error: unknown, isFatal?: boolean) => void) => void;
}

/** Minimal React Native touch event shape (`onTouchStart` / `GestureResponderEvent`). */
export interface TouchEventLike {
  nativeEvent?: {
    pageX?: number;
    pageY?: number;
    locationX?: number;
    locationY?: number;
  };
}

export interface PulseReactNativeConfig
  extends Omit<
    PulseWebConfig,
    "autocapture" | "maskSelectors" | "blockSelectors" | "unmaskSelectors"
  > {
  /** Persistent storage backend (e.g. `createAsyncStorageAdapter(AsyncStorage)` or `createMmkvStorageAdapter(mmkv)`). */
  storage?: PulseStorage;
  /** Optional React Native `AppState` module for background flush & lifecycle tracking. */
  appState?: AppStateLike;
  /** Optional React Native `Platform` module for `$os` / `$os_version` context. */
  platform?: PlatformLike;
  /** Optional React Native `Dimensions` module for `$screen_width` / `$screen_height` context. */
  dimensions?: DimensionsLike;
  /** Optional native wireframe recorder module (`NativeModules.PulseNativeRecorder`). */
  nativeRecorder?: PulseNativeRecorderModule | null;
  /** App version string attached as `$app_version`. */
  appVersion?: string;
  /** App build number attached as `$app_build`. */
  buildNumber?: string;
  /** Capture `$app_backgrounded` / `$app_foregrounded` lifecycle events (default `true` when `appState` is provided). */
  captureAppLifecycle?: boolean;
  /** Native wireframe snapshot polling interval in ms (default `1000`). */
  replayIntervalMs?: number;
  /** Maximum native wireframe nodes per snapshot (default `160`). */
  replayMaxNodes?: number;
}

function stripUrlForBreadcrumb(url: string): string {
  const trimmed = url.trim();
  const cut = trimmed.search(/[?#]/);
  let clean = cut === -1 ? trimmed : trimmed.slice(0, cut);
  const schemeEnd = clean.indexOf("://");
  if (schemeEnd !== -1) {
    const hostStart = schemeEnd + 3;
    const pathStart = clean.indexOf("/", hostStart);
    const authorityEnd = pathStart === -1 ? clean.length : pathStart;
    const at = clean.lastIndexOf("@", authorityEnd - 1);
    if (at >= hostStart) {
      clean = clean.slice(0, hostStart) + clean.slice(at + 1);
    }
  }
  return clean.slice(0, 256);
}

/**
 * First-class React Native SDK for Pulse Analytics.
 *
 * Solves all mobile/Hermes constraints out of the box:
 * - Pluggable sync/async storage (`AsyncStorage`, `MMKV`, or in-memory fallback)
 * - Cryptographically secure IDs via native `SecRandomCopyBytes`/`SecureRandom` (no `Math.random` fallback on Hermes)
 * - Default 10s HTTP fetch timeout so hung mobile connections never stall the send queue
 * - Hermes `ErrorUtils` global error handler + global `fetch` network breadcrumbs
 * - `AppState` background flushing & lifecycle tracking
 * - Native iOS/Android wireframe replay (real colors, clipping rects, and modals/bottom sheets) with incremental diffing
 */
export class PulseReactNativeClient {
  private readonly core: PulseWebClient;
  private readonly cfg: PulseReactNativeConfig;
  private readonly endpoint: string;
  private readonly nativeRecorder: NativeWireframeRecorder | null;

  private appStateSub: { remove(): void } | null = null;
  private restoreErrorHandler: (() => void) | null = null;
  private restoreFetch: (() => void) | null = null;
  private currentScreen: string | null = null;
  private lastAppState: string;

  constructor(cfg: PulseReactNativeConfig) {
    this.cfg = cfg;
    this.endpoint = cfg.endpointUrl.replace(/\/+$/, "");
    this.lastAppState = cfg.appState?.currentState ?? "active";

    const nativeMod = cfg.nativeRecorder ?? null;
    const nativeRandomBytes =
      cfg.randomBytes ??
      (nativeMod && typeof nativeMod.randomBytesSync === "function"
        ? (length: number) => new Uint8Array(nativeMod.randomBytesSync!(length))
        : undefined);

    const storage = cfg.storage ?? createMemoryStorage();

    this.core = new PulseWebClient({
      endpointUrl: cfg.endpointUrl,
      apiKey: cfg.apiKey,
      flushBatchSize: cfg.flushBatchSize,
      flushIntervalMs: cfg.flushIntervalMs,
      enableWireframeReplay: cfg.enableWireframeReplay,
      maxQueuedEvents: cfg.maxQueuedEvents,
      optOutByDefault: cfg.optOutByDefault,
      propertyBlacklist: cfg.propertyBlacklist,
      beforeSend: cfg.beforeSend,
      replaySampleRate: cfg.replaySampleRate,
      replayTriggerEvents: cfg.replayTriggerEvents,
      // Disable browser-only DOM hooks inside core; RN client installs Hermes/RN hooks below.
      captureErrors: false,
      captureNetwork: false,
      autocapture: false,
      debug: cfg.debug,
      fetchTimeoutMs: cfg.fetchTimeoutMs ?? 10_000,
      storage,
      randomBytes: nativeRandomBytes,
      contextProvider: () => this.collectMobileContext(),
    });

    this.nativeRecorder = nativeMod
      ? new NativeWireframeRecorder(this.core, nativeMod, {
          intervalMs: cfg.replayIntervalMs ?? 1_000,
          maxNodes: cfg.replayMaxNodes ?? 160,
        })
      : null;

    if (cfg.captureErrors !== false) {
      this.attachHermesErrorHandler();
    }
    if (cfg.captureNetwork ?? Boolean(cfg.enableWireframeReplay)) {
      this.attachGlobalFetchCapture();
    }
    if (cfg.appState) {
      this.attachAppStateListener(cfg.appState);
    }
    if (cfg.enableWireframeReplay && this.nativeRecorder && this.core.isRecordingSession()) {
      this.nativeRecorder.start();
    }
  }

  private collectMobileContext(): EventProperties {
    const ctx: EventProperties = {
      $lib: "pulse-react-native",
    };
    if (this.cfg.platform) {
      const os = this.cfg.platform.OS;
      ctx.$os = os === "ios" ? "iOS" : os === "android" ? "Android" : os;
      if (this.cfg.platform.Version !== undefined) {
        ctx.$os_version = String(this.cfg.platform.Version);
      }
      ctx.$device = "Mobile";
    }
    if (this.cfg.dimensions) {
      try {
        const win = this.cfg.dimensions.get("window");
        if (typeof win?.width === "number") ctx.$screen_width = Math.round(win.width);
        if (typeof win?.height === "number") ctx.$screen_height = Math.round(win.height);
      } catch {
        // Ignore Dimensions errors during early boot.
      }
    }
    if (this.cfg.appVersion) ctx.$app_version = this.cfg.appVersion;
    if (this.cfg.buildNumber) ctx.$app_build = this.cfg.buildNumber;
    if (this.currentScreen) ctx.$screen_name = this.currentScreen;
    if (this.cfg.contextProvider) {
      Object.assign(ctx, this.cfg.contextProvider());
    }
    return ctx;
  }

  private attachHermesErrorHandler(): void {
    const g = globalThis as unknown as { ErrorUtils?: ErrorUtilsLike };
    const errorUtils = g.ErrorUtils;
    if (!errorUtils?.setGlobalHandler) return;

    const prevHandler = errorUtils.getGlobalHandler?.();
    const wrapped = (error: unknown, isFatal?: boolean) => {
      try {
        const msg =
          error instanceof Error
            ? `${error.name}: ${error.message}`
            : String(error ?? "Unknown error");
        this.core.recordConsoleError(isFatal ? `[Fatal] ${msg}` : msg);
        if (isFatal) {
          void this.core.flush();
        }
      } catch {
        // Never break host error reporting.
      }
      prevHandler?.(error, isFatal);
    };
    errorUtils.setGlobalHandler(wrapped);
    this.restoreErrorHandler = () => {
      if (prevHandler) errorUtils.setGlobalHandler?.(prevHandler);
    };
  }

  private attachGlobalFetchCapture(): void {
    const g = globalThis as unknown as { fetch?: typeof fetch };
    if (typeof g.fetch !== "function") return;
    const origFetch = g.fetch;
    const own = this.endpoint;

    g.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : (input as Request).url;
      if (url.startsWith(own)) {
        return origFetch(input as RequestInfo, init);
      }
      const method =
        init?.method ??
        (typeof input === "object" && "method" in input ? (input as Request).method : "GET");
      const started = Date.now();
      try {
        const res = await origFetch(input as RequestInfo, init);
        this.core.recordNetworkBreadcrumb(
          method.toUpperCase(),
          stripUrlForBreadcrumb(url),
          res.status,
          Math.max(0, Math.round(Date.now() - started)),
        );
        return res;
      } catch (err) {
        this.core.recordNetworkBreadcrumb(
          method.toUpperCase(),
          stripUrlForBreadcrumb(url),
          0,
          Math.max(0, Math.round(Date.now() - started)),
        );
        throw err;
      }
    }) as typeof fetch;

    this.restoreFetch = () => {
      g.fetch = origFetch;
    };
  }

  private attachAppStateListener(appState: AppStateLike): void {
    this.appStateSub = appState.addEventListener("change", (nextState: string) => {
      const prev = this.lastAppState;
      this.lastAppState = nextState;
      const trackLifecycle = this.cfg.captureAppLifecycle !== false;

      if (nextState === "background" || nextState === "inactive") {
        if (prev === "active") {
          this.nativeRecorder?.stop();
          if (trackLifecycle) {
            this.core.track("$app_backgrounded", { prev_state: prev });
          }
          void this.core.flush();
        }
      } else if (nextState === "active" && prev !== "active") {
        if (trackLifecycle) {
          this.core.track("$app_foregrounded", { prev_state: prev });
        }
        if (this.cfg.enableWireframeReplay && this.core.isRecordingSession()) {
          this.nativeRecorder?.start();
        }
      }
    });
  }

  // --- Lifecycle & Hydration ---

  public ready(): Promise<void> {
    return this.core.ready();
  }

  public getDistinctId(): string {
    return this.core.getDistinctId();
  }

  public getSessionId(): string {
    return this.core.getSessionId();
  }

  public getQueuedEventCount(): number {
    return this.core.getQueuedEventCount();
  }

  public clearBackoffForRetry(): void {
    this.core.clearBackoffForRetry();
  }

  // --- Tracking, Screens, Identity, Profiles, Groups, Revenue ---

  public track(eventName: string, props: EventProperties = {}): void {
    this.core.track(eventName, props);
  }

  /** Tracks a screen view (`$screen`) and triggers a native wireframe snapshot when recording. */
  public trackScreen(screenName: string, props: EventProperties = {}): void {
    this.currentScreen = screenName;
    this.core.track("$screen", { $screen_name: screenName, ...props });
    if (this.nativeRecorder && this.core.isRecordingSession()) {
      void this.nativeRecorder.captureOnce();
    }
  }

  public identify(userId: string, userProps?: EventProperties): Promise<void> {
    return this.core.identify(userId, userProps);
  }

  public reset(): void {
    this.core.reset();
  }

  public register(props: EventProperties): void {
    this.core.register(props);
  }

  public registerOnce(props: EventProperties): void {
    this.core.registerOnce(props);
  }

  public unregister(key: string): void {
    this.core.unregister(key);
  }

  public timeEvent(eventName: string): void {
    this.core.timeEvent(eventName);
  }

  public optOut(): void {
    this.nativeRecorder?.stop();
    this.core.optOut();
  }

  public optIn(): void {
    this.core.optIn();
  }

  public hasOptedOut(): boolean {
    return this.core.hasOptedOut();
  }

  public engage(ops: Record<string, unknown>): Promise<void> {
    return this.core.engage(ops);
  }

  public setUserProperties(props: EventProperties): Promise<void> {
    return this.core.setUserProperties(props);
  }

  public setUserPropertiesOnce(props: EventProperties): Promise<void> {
    return this.core.setUserPropertiesOnce(props);
  }

  public incrementUserProperty(prop: string, by = 1): Promise<void> {
    return this.core.incrementUserProperty(prop, by);
  }

  public setGroup(groupType: string, groupId: string): void {
    this.core.setGroup(groupType, groupId);
  }

  public setGroupProperties(
    groupType: string,
    groupId: string,
    props: EventProperties,
  ): Promise<void> {
    return this.core.setGroupProperties(groupType, groupId, props);
  }

  public trackRevenue(params: RevenueParams): Promise<void> {
    return this.core.trackRevenue(params);
  }

  // --- Feature Flags & Experiments ---

  public getVariantSync(flagKey: string): string | null {
    return this.core.getVariantSync(flagKey);
  }

  public isEnabled(flagKey: string): boolean {
    return this.core.isEnabled(flagKey);
  }

  public getVariantPayload(flagKey: string): unknown | null {
    return this.core.getVariantPayload(flagKey);
  }

  public trackExposure(flagKey: string, variant: string | null): void {
    this.core.trackExposure(flagKey, variant);
  }

  public getVariant(
    flagKey: string,
    options?: { trackExposure?: boolean },
  ): Promise<string | null> {
    return this.core.getVariant(flagKey, options);
  }

  public refreshFlags(): Promise<void> {
    return this.core.refreshFlags();
  }

  // --- Session Replay & Touch Capture ---

  public startSessionRecording(): void {
    this.core.startSessionRecording();
    this.nativeRecorder?.start();
  }

  public stopSessionRecording(): void {
    this.nativeRecorder?.stop();
    this.core.stopSessionRecording();
  }

  public pauseSessionRecording(): void {
    this.nativeRecorder?.stop();
    this.core.pauseSessionRecording();
  }

  public resumeSessionRecording(): void {
    this.core.resumeSessionRecording();
    if (this.core.isRecordingSession()) {
      this.nativeRecorder?.start();
    }
  }

  public isRecordingSession(): boolean {
    return this.core.isRecordingSession();
  }

  public recordWireframeOp(op: WireframeOp): void {
    this.core.recordWireframeOp(op);
  }

  public recordWireframeOps(ops: WireframeOp[]): void {
    this.core.recordWireframeOps(ops);
  }

  public recordWireframeSnapshot(
    width: number,
    height: number,
    nodes: WireNode[],
    options?: { forceFull?: boolean },
  ): void {
    this.core.recordWireframeSnapshot(width, height, nodes, options);
  }

  public captureNativeSnapshot(forceFull = false): Promise<boolean> {
    if (!this.nativeRecorder) return Promise.resolve(false);
    return this.nativeRecorder.captureOnce(forceFull);
  }

  public recordPointerInteraction(
    x: number,
    y: number,
    kind:
      | "tap"
      | "dead_tap"
      | "scroll"
      | { type: "tap" }
      | { type: "dead_tap" }
      | { type: "scroll" }
      | { type: "rage_tap"; count: number } = "tap",
  ): void {
    this.core.recordPointerInteraction(x, y, kind);
  }

  public recordTap(x: number, y: number, interactive = true): void {
    this.core.recordTap(x, y, interactive);
  }

  /** Convenience handler to attach to a root `<View onTouchStart={(e) => pulse.handleTouchStart(e)} />`. */
  public handleTouchStart(event: TouchEventLike, interactive = true): void {
    const x = event?.nativeEvent?.pageX ?? event?.nativeEvent?.locationX;
    const y = event?.nativeEvent?.pageY ?? event?.nativeEvent?.locationY;
    if (typeof x === "number" && typeof y === "number") {
      this.core.recordTap(x, y, interactive);
    }
  }

  public recordNetworkBreadcrumb(
    method: string,
    path: string,
    statusCode: number,
    durationMs: number,
  ): void {
    this.core.recordNetworkBreadcrumb(method, path, statusCode, durationMs);
  }

  public recordConsoleError(message: string): void {
    this.core.recordConsoleError(message);
  }

  public flush(): Promise<void> {
    return this.core.flush();
  }

  public shutdown(): void {
    this.nativeRecorder?.stop();
    this.appStateSub?.remove();
    this.appStateSub = null;
    this.restoreErrorHandler?.();
    this.restoreErrorHandler = null;
    this.restoreFetch?.();
    this.restoreFetch = null;
    this.core.shutdown();
  }
}

/**
 * Helper for `@react-navigation/native`'s `<NavigationContainer onReady={...} onStateChange={...}>`.
 * Automatically calls `client.trackScreen(routeName)` on every distinct screen transition.
 */
export function createNavigationTracker(
  client: PulseReactNativeClient,
  getCurrentRouteName: () => string | undefined,
): { onReady: () => void; onStateChange: () => void } {
  let lastRoute: string | undefined;
  const check = () => {
    const next = getCurrentRouteName();
    if (next && next !== lastRoute) {
      lastRoute = next;
      client.trackScreen(next);
    }
  };
  return {
    onReady: check,
    onStateChange: check,
  };
}
