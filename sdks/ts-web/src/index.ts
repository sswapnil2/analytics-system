/**
 * Pulse Analytics — Zero-Dependency Web & React Native SDK (`@pulse-analytics/ts-web`)
 *
 * Privacy-by-Design Invariant:
 * Never records screenshots, canvases, or unmasked text/inputs. All text, titles, inputs, and
 * `contenteditable` elements are masked to fixed-length `"████"` on the client unless explicitly
 * allowlisted via `data-pulse-unmask` (inputs and contenteditable remain masked unconditionally).
 */

import { cohortMemberHash, flagRolloutSlot, flagVariantSlot, xxh3_64 } from "./xxh3.js";

export { cohortMemberHash, flagRolloutSlot, flagVariantSlot, xxh3_64 };

export type PropPrimitive = string | number | boolean | null;
export type EventProperties = Record<string, PropPrimitive>;

export type NodeKind =
  | "screen"
  | "container"
  | "button"
  | "masked_input"
  | "text"
  | "image_placeholder"
  | "error_banner";

export interface WireNode {
  node_id: number;
  parent_id: number;
  kind: NodeKind;
  x: number;
  y: number;
  w: number;
  h: number;
  bg_rgba: number;
  masked_label: string;
}

export type WireframeOp =
  | { op: "full_snapshot"; ts_offset_ms: number; width: number; height: number; nodes: WireNode[] }
  | ({ op: "node_upsert"; ts_offset_ms: number } & WireNode)
  | { op: "node_remove"; ts_offset_ms?: number; node_id: number }
  | {
      op: "pointer_interaction";
      ts_offset_ms: number;
      x: number;
      y: number;
      kind:
        | { type: "tap" }
        | { type: "rage_tap"; count: number }
        | { type: "dead_tap" }
        | { type: "scroll" };
    }
  | {
      op: "network_breadcrumb";
      ts_offset_ms: number;
      method: string;
      path: string;
      status_code: number;
      duration_ms: number;
    };

/** Pluggable key-value storage backend (supports sync `localStorage`/MMKV and async `AsyncStorage`). */
export interface PulseStorage {
  getItem(key: string): string | null | Promise<string | null>;
  setItem(key: string, value: string): void | Promise<void>;
  removeItem(key: string): void | Promise<void>;
}

export interface AutocaptureConfig {
  pageviews?: boolean;
  clicks?: boolean;
  forms?: boolean;
  allowSelectors?: string[];
  blockSelectors?: string[];
}

export interface RevenueParams {
  amount: number;
  currency?: string;
  productId?: string;
  quantity?: number;
  revenueType?: string;
  props?: EventProperties;
}

export interface PulseWebConfig {
  endpointUrl: string;
  apiKey: string;
  flushBatchSize?: number;
  flushIntervalMs?: number;
  enableWireframeReplay?: boolean;
  maxQueuedEvents?: number;
  optOutByDefault?: boolean;
  propertyBlacklist?: string[];
  beforeSend?: (event: Record<string, unknown>) => Record<string, unknown> | null;
  autocapture?: boolean | AutocaptureConfig;
  replaySampleRate?: number;
  replayTriggerEvents?: string[];
  maskSelectors?: string[];
  blockSelectors?: string[];
  unmaskSelectors?: string[];
  /** Send uncaught errors and unhandled promise rejections as `$error` (default `true`). */
  captureErrors?: boolean;
  /**
   * Add every `fetch`/XHR call (method, path without query string, status, duration) to the
   * replay timeline. Defaults to `enableWireframeReplay`. Calls to the Pulse endpoint are skipped.
   */
  captureNetwork?: boolean;
  /** Log every tracked event and each send result to the console. */
  debug?: boolean;
  /**
   * Abort and retry hung HTTP requests after this many milliseconds (default `10_000`).
   * Set to `0` to disable timeouts. Prevents stalled send queues on flaky mobile networks.
   */
  fetchTimeoutMs?: number;
  /**
   * Custom key-value storage backend (sync or async, e.g. React Native AsyncStorage or MMKV).
   * Falls back to `localStorage` in browsers and an in-memory map elsewhere.
   */
  storage?: PulseStorage;
  /**
   * Cryptographic random byte provider for environments without `crypto.randomUUID` /
   * `crypto.getRandomValues` (such as Hermes on React Native).
   */
  randomBytes?: (length: number) => Uint8Array;
  /** Optional platform context provider merged into every tracked event. */
  contextProvider?: () => EventProperties;
  /**
   * Re-fetch flags in the background when they are older than this (default `300_000`, 5 minutes),
   * so rollbacks and guardrail stops reach running apps. `0` fetches only on first use and
   * `refreshFlags()`.
   */
  flagsTtlMs?: number;
}

export interface EdgeFlagRule {
  key: string;
  enabled: boolean;
  rollout_bps: number;
  targeting_cohort_id?: string;
  /** Sorted `cohortMemberHash(targeting_cohort_id, distinctId)` of the cohort's members. */
  cohort_member_hashes?: string[];
  variants: Array<{ key: string; weight_bps: number; payload?: unknown }>;
}

interface PersistedWebState {
  routingSeed: string;
  anonId: string;
  userId: string | null;
  sessionId: string;
  lastActivityMs: number;
  optedOut?: boolean;
  explicitOptSet?: boolean;
  superProps?: EventProperties;
  firstTouchUtms?: EventProperties;
  lastTouchUtms?: EventProperties;
  groups?: Record<string, string>;
  exposuresSent?: string[];
}

interface SealedReplayPacket {
  sessionId: string;
  chunkStartTsMs: number;
  ops: WireframeOp[];
}

export const FIXED_MASK = "████";
const ERROR_BANNER_NODE_ID = 99_000;

/** Box of the visible `[role=alert]` showing `message` (or the last visible alert), in viewport px. */
function visibleAlertBox(message: string): { x: number; y: number; w: number; h: number } | null {
  if (typeof document === "undefined" || typeof document.querySelectorAll !== "function") return null;
  const alerts = Array.from(document.querySelectorAll("[role='alert']"))
    .map((el) => ({ el, r: el.getBoundingClientRect() }))
    .filter(({ r }) => r.width >= 4 && r.height >= 4);
  const hit = alerts.find(({ el }) => (el.textContent || "").includes(message)) ?? alerts[alerts.length - 1];
  if (!hit) return null;
  return {
    x: Math.max(0, Math.round(hit.r.left)),
    y: Math.max(0, Math.round(hit.r.top)),
    w: Math.min(65535, Math.round(hit.r.width)),
    h: Math.min(65535, Math.round(hit.r.height)),
  };
}
const SESSION_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_QUEUED_EVENTS = 2_000;
const DEFAULT_FETCH_TIMEOUT_MS = 10_000;
const DEFAULT_FLAGS_TTL_MS = 300_000;
const MAX_QUEUED_REPLAY_PACKETS = 64;
const SNAPSHOT_THROTTLE_MS = 1_000;

/** `https://api.x.com/v1/kyc?pan=ABC#x` → `https://api.x.com/v1/kyc`: query strings, fragments, userinfo, and mailto/tel URIs often carry PII. */
function stripQuery(url: string, maxLen = 256): string {
  const trimmed = url.trim();
  const lower = trimmed.toLowerCase();
  if (lower.startsWith("mailto:")) return "mailto:[redacted]";
  if (lower.startsWith("tel:")) return "tel:[redacted]";
  const cut = trimmed.search(/[?#]/);
  let clean = cut === -1 ? trimmed : trimmed.slice(0, cut);
  // Strip embedded credentials (`https://user:pass@host/path` -> `https://host/path`).
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
  return clean.slice(0, maxLen);
}

/**
 * Default-deny PII masking: returns fixed-length `"████"` for every non-empty string
 * unless `explicitlyUnmasked === true`. Never leaks prefix labels or character lengths.
 */
export function maskPiiText(raw: string, explicitlyUnmasked = false): string {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  if (explicitlyUnmasked) {
    return trimmed.slice(0, 64);
  }
  return FIXED_MASK;
}

export function evaluateEdgeFlag(rule: EdgeFlagRule, distinctId: string): string | null {
  if (!rule.enabled || rule.rollout_bps <= 0) return null;
  // A targeted rule with no hashes matches no one (fail closed).
  if (rule.targeting_cohort_id) {
    const hashes = rule.cohort_member_hashes ?? [];
    if (!hashes.includes(cohortMemberHash(rule.targeting_cohort_id, distinctId))) {
      return null;
    }
  }
  const slot = flagRolloutSlot(rule.key, distinctId);
  if (slot >= rule.rollout_bps) return null;
  if (rule.variants.length === 0) return "on";

  const totalWeight = rule.variants.reduce((acc, v) => acc + v.weight_bps, 0);
  if (totalWeight <= 0) return rule.variants[0]?.key ?? null;
  const vSlot = flagVariantSlot(rule.key, distinctId, totalWeight);
  let acc = 0;
  for (const v of rule.variants) {
    acc += v.weight_bps;
    if (vSlot < acc) return v.key;
  }
  return rule.variants[rule.variants.length - 1]?.key ?? null;
}

let fallbackEntropyCounter = 0;

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    out += bytes[i].toString(16).padStart(2, "0");
  }
  return out;
}

function randomId(prefix: string, randomBytesFn?: (length: number) => Uint8Array): string {
  if (randomBytesFn) {
    try {
      const bytes = randomBytesFn(8);
      if (bytes && bytes.length >= 8) {
        return `${prefix}_${bytesToHex(bytes.subarray(0, 8))}`;
      }
    } catch {
      // Fall through to Web Crypto / PRNG fallback.
    }
  }
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`;
  }
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    const bytes = new Uint8Array(8);
    crypto.getRandomValues(bytes);
    return `${prefix}_${bytesToHex(bytes)}`;
  }
  // High-entropy fallback for JS engines without Web Crypto (e.g. unpolyfilled Hermes):
  // mix wall-clock ms, high-res monotonic timer (when available), a monotonic counter, and two PRNG words.
  fallbackEntropyCounter = (fallbackEntropyCounter + 1) >>> 0;
  const hiRes =
    typeof performance !== "undefined" && typeof performance.now === "function"
      ? Math.floor(performance.now() * 1000) >>> 0
      : 0;
  const w1 = ((Date.now() >>> 0) ^ hiRes ^ ((Math.random() * 0x1_0000_0000) >>> 0)) >>> 0;
  const w2 =
    ((Math.imul(fallbackEntropyCounter, 0x9e3779b1) >>> 0) ^
      ((Math.random() * 0x1_0000_0000) >>> 0)) >>>
    0;
  return `${prefix}_${w1.toString(16).padStart(8, "0")}${w2.toString(16).padStart(8, "0")}`;
}

function isPromiseLike<T>(v: unknown): v is PromiseLike<T> {
  return Boolean(v) && (typeof v === "object" || typeof v === "function") && typeof (v as PromiseLike<T>).then === "function";
}

function defaultStorage(): PulseStorage {
  try {
    if (typeof localStorage !== "undefined" && typeof localStorage.getItem === "function") {
      return localStorage;
    }
  } catch {
    // Sandboxed iframes or restricted environments.
  }
  const mem = new Map<string, string>();
  return {
    getItem: (k) => mem.get(k) ?? null,
    setItem: (k, v) => {
      mem.set(k, v);
    },
    removeItem: (k) => {
      mem.delete(k);
    },
  };
}

function wireNodesEqual(a: WireNode, b: WireNode): boolean {
  return (
    a.parent_id === b.parent_id &&
    a.kind === b.kind &&
    a.x === b.x &&
    a.y === b.y &&
    a.w === b.w &&
    a.h === b.h &&
    a.bg_rgba === b.bg_rgba &&
    a.masked_label === b.masked_label
  );
}

function sanitizeWireNode(node: WireNode): WireNode {
  if (node.kind === "masked_input") {
    return { ...node, masked_label: FIXED_MASK };
  }
  return node;
}

const ATTRIBUTION_PARAMS = [
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "gclid",
  "fbclid",
  "ttclid",
  "li_fat_id",
  "msclkid",
] as const;

function detectBrowserAndOs(ua: string): { $browser: string; $os: string; $device: string } {
  let $browser = "Unknown";
  if (ua.includes("Edg/")) $browser = "Edge";
  else if (ua.includes("Chrome/")) $browser = "Chrome";
  else if (ua.includes("Firefox/")) $browser = "Firefox";
  else if (ua.includes("Safari/")) $browser = "Safari";

  let $os = "Unknown";
  if (ua.includes("Android")) $os = "Android";
  else if (ua.includes("iPhone") || ua.includes("iPad")) $os = "iOS";
  else if (ua.includes("Mac OS X")) $os = "macOS";
  else if (ua.includes("Windows")) $os = "Windows";
  else if (ua.includes("Linux")) $os = "Linux";

  const $device =
    ua.includes("Mobi") || ua.includes("Android") || ua.includes("iPhone") ? "Mobile" : "Desktop";
  return { $browser, $os, $device };
}

function collectDefaultContext(): EventProperties {
  const ctx: EventProperties = {};
  if (typeof navigator !== "undefined") {
    if (navigator.userAgent) {
      const parsed = detectBrowserAndOs(navigator.userAgent);
      ctx.$browser = parsed.$browser;
      ctx.$os = parsed.$os;
      ctx.$device = parsed.$device;
    }
    if (navigator.language) ctx.$locale = navigator.language;
  }
  if (typeof window !== "undefined") {
    if (window.location?.href) ctx.$current_url = stripQuery(window.location.href, 512);
    if (window.location?.pathname) ctx.$pathname = window.location.pathname;
    if (typeof window.innerWidth === "number") ctx.$screen_width = window.innerWidth;
    if (typeof window.innerHeight === "number") ctx.$screen_height = window.innerHeight;
  }
  if (typeof document !== "undefined" && typeof document.referrer === "string" && document.referrer) {
    ctx.$referrer = stripQuery(document.referrer, 512);
    try {
      ctx.$referring_domain = new URL(document.referrer).hostname;
    } catch {
      // Ignore malformed referrer URL.
    }
  }
  return ctx;
}

export class PulseWebClient {
  private readonly endpoint: string;
  private readonly apiKey: string;
  private readonly storageKey: string;
  private readonly storage: PulseStorage;
  private readonly flushBatchSize: number;
  private readonly maxQueuedEvents: number;
  private readonly fetchTimeoutMs: number;
  private readonly cfg: PulseWebConfig;
  private readonly hydrationPromise: Promise<void>;
  private userIdSetExplicitly = false;

  private routingSeed: string;
  private anonId: string;
  private userId: string | null = null;
  private sessionId: string;
  private lastActivityMs: number;
  private seqCounter = 0;
  private replayOffsetKb = 0;

  private optedOut: boolean;
  private explicitOptSet = false;
  private superProps: EventProperties = {};
  private firstTouchUtms: EventProperties = {};
  private lastTouchUtms: EventProperties = {};
  private groups: Record<string, string> = {};
  private exposuresSent: Set<string> = new Set();
  private timedEvents: Map<string, number> = new Map();

  private recordingActive = false;
  private recordingPaused = false;
  private replayTriggered = false;
  private readonly replayTriggerSet: Set<string>;
  private readonly propertyBlacklist: Set<string>;

  private eventQueue: Array<Record<string, unknown>> = [];
  private engageQueue: Array<Record<string, unknown>> = [];
  private replayQueue: SealedReplayPacket[] = [];
  private replayOps: WireframeOp[] = [];
  private replayOpsSessionId: string | null = null;
  private chunkStartTsMs: number | null = null;

  /** Incremental wireframe diff state (reset on session rotation / stop / reset). */
  private lastSnapshotViewport: { w: number; h: number } | null = null;
  private lastSnapshotNodes: Map<number, WireNode> = new Map();
  private domNodeIds: WeakMap<object, number> = new WeakMap();
  private nextDomNodeId = 2;

  private recentClicks: Array<{ ts: number; x: number; y: number }> = [];
  private flagsEtag: string | null = null;
  private flagRules: Map<string, EdgeFlagRule> = new Map();
  private flagsCheckedAtMs = 0;
  private flagsRefresh: Promise<void> | null = null;
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private inFlightFlush: Promise<void> | null = null;
  private consecutiveFailures = 0;
  private backoffUntilMs = 0;

  private boundClickListener: ((ev: MouseEvent) => void) | null = null;
  private boundAutocaptureClickListener: ((ev: MouseEvent) => void) | null = null;
  private boundAutocaptureSubmitListener: ((ev: Event) => void) | null = null;
  private boundPopstateListener: (() => void) | null = null;
  private restoreHistory: (() => void) | null = null;
  private restoreNetwork: (() => void) | null = null;
  private boundErrorListener: ((ev: ErrorEvent) => void) | null = null;
  private boundRejectionListener: ((ev: PromiseRejectionEvent) => void) | null = null;
  private mutationObserver: { disconnect(): void } | null = null;
  private lastSnapshotMs = 0;
  /** Events handed to `fetch` but not yet acknowledged; kept in the persisted queue until then. */
  private inFlightEvents: Array<Record<string, unknown>> = [];
  private boundVisibilityListener: (() => void) | null = null;

  constructor(cfg: PulseWebConfig) {
    this.cfg = cfg;
    this.endpoint = cfg.endpointUrl.replace(/\/+$/, "");
    this.apiKey = cfg.apiKey;
    this.storageKey = `pulse_sdk_state_${xxh3_64(cfg.apiKey).toString(16)}`;
    this.storage = cfg.storage ?? defaultStorage();
    this.flushBatchSize = cfg.flushBatchSize ?? 20;
    this.maxQueuedEvents = cfg.maxQueuedEvents ?? DEFAULT_MAX_QUEUED_EVENTS;
    this.fetchTimeoutMs = cfg.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS;
    this.propertyBlacklist = new Set(cfg.propertyBlacklist ?? []);
    this.replayTriggerSet = new Set(cfg.replayTriggerEvents ?? []);

    const now = Date.now();
    // Initialize defaults synchronously; if storage is synchronous, apply immediately,
    // otherwise hydrate asynchronously and expose via `ready()`.
    this.routingSeed = this.newId("dev");
    this.anonId = this.newId("anon");
    this.sessionId = this.newId("sess");
    this.lastActivityMs = now;
    this.optedOut = Boolean(cfg.optOutByDefault);

    let stateMaybe: string | null | Promise<string | null> = null;
    try {
      stateMaybe = this.storage.getItem(this.storageKey);
    } catch {
      stateMaybe = null;
    }

    if (isPromiseLike<string | null>(stateMaybe)) {
      this.initReplaySamplingState();
      this.hydrationPromise = this.hydrateAsync(stateMaybe, now);
    } else {
      const loaded = this.parsePersistedState(stateMaybe);
      if (loaded) this.applyLoadedState(loaded, now);
      this.captureUrlAttribution();
      this.initReplaySamplingState();
      this.persistState();
      const pendingMaybe = this.readQueueRaw();
      if (isPromiseLike<string | null>(pendingMaybe)) {
        this.hydrationPromise = Promise.resolve(pendingMaybe)
          .then((raw) => {
            const pending = this.parsePersistedQueue(raw);
            if (pending.length > 0) this.enqueueEventsBounded(pending, true);
          })
          .catch(() => {});
      } else {
        const pending = this.parsePersistedQueue(pendingMaybe);
        if (pending.length > 0) this.enqueueEventsBounded(pending);
        this.hydrationPromise = Promise.resolve();
      }
    }

    const interval = cfg.flushIntervalMs ?? 5000;
    if (interval > 0) {
      this.flushTimer = setInterval(() => {
        void this.flush();
      }, interval);
    }

    if (
      typeof window !== "undefined" &&
      typeof document !== "undefined" &&
      typeof window.addEventListener === "function"
    ) {
      // Unconditional visibilitychange listener so both events and replay ops flush on page hide!
      this.boundVisibilityListener = () => {
        if (document.visibilityState === "hidden") {
          this.flushBeacon();
        }
      };
      window.addEventListener("visibilitychange", this.boundVisibilityListener);

      if (cfg.enableWireframeReplay) {
        this.attachReplayClickListener();
        this.attachSnapshotObserver();
      }
      if (cfg.captureErrors !== false) {
        this.attachErrorListeners();
      }
      if (cfg.captureNetwork ?? Boolean(cfg.enableWireframeReplay)) {
        this.attachNetworkCapture();
      }
      if (cfg.autocapture) {
        this.attachAutocaptureListeners(cfg.autocapture);
      }
    }
  }

  private newId(prefix: string): string {
    return randomId(prefix, this.cfg.randomBytes);
  }

  /** Resolves once async storage hydration (e.g. React Native AsyncStorage) has completed. */
  public ready(): Promise<void> {
    return this.hydrationPromise;
  }

  private async hydrateAsync(
    statePromise: PromiseLike<string | null>,
    now: number,
  ): Promise<void> {
    try {
      const rawState = await statePromise;
      const loaded = this.parsePersistedState(rawState);
      if (loaded) {
        this.applyLoadedState(loaded, now, true);
      }
      this.captureUrlAttribution();
      this.initReplaySamplingState();
      this.persistState();

      const rawQueue = await Promise.resolve(this.readQueueRaw());
      const pending = this.parsePersistedQueue(rawQueue);
      if (pending.length > 0) {
        this.enqueueEventsBounded(pending, true);
      }
    } catch {
      // Keep in-memory defaults if async storage fails.
      this.initReplaySamplingState();
    }
  }

  private applyLoadedState(loaded: PersistedWebState, now: number, mergeLive = false): void {
    this.routingSeed = loaded.routingSeed;
    this.anonId = loaded.anonId;
    if (!mergeLive || !this.userIdSetExplicitly) {
      this.userId = loaded.userId;
    }
    if (now - loaded.lastActivityMs > SESSION_TIMEOUT_MS) {
      this.sessionId = this.newId("sess");
      this.exposuresSent = new Set();
    } else {
      this.sessionId = loaded.sessionId;
      this.exposuresSent = new Set([...(loaded.exposuresSent ?? []), ...this.exposuresSent]);
    }
    this.lastActivityMs = Math.max(now, this.lastActivityMs);
    if (!mergeLive || !this.explicitOptSet) {
      this.explicitOptSet = Boolean(loaded.explicitOptSet);
      this.optedOut = this.explicitOptSet
        ? Boolean(loaded.optedOut)
        : Boolean(this.cfg.optOutByDefault);
    }
    this.superProps = { ...(loaded.superProps ?? {}), ...this.superProps };
    this.firstTouchUtms = { ...(loaded.firstTouchUtms ?? {}), ...this.firstTouchUtms };
    this.lastTouchUtms = { ...(loaded.lastTouchUtms ?? {}), ...this.lastTouchUtms };
    this.groups = { ...(loaded.groups ?? {}), ...this.groups };
  }

  private initReplaySamplingState(): void {
    const rate = this.cfg.replaySampleRate ?? 1.0;
    const sampled =
      rate >= 1.0
        ? true
        : rate <= 0.0
          ? false
          : Number(xxh3_64(this.sessionId) % 10000n) < Math.round(rate * 10000);
    if (!this.cfg.enableWireframeReplay && this.replayTriggerSet.size === 0) {
      this.recordingActive = false;
      return;
    }
    this.recordingActive = sampled && this.replayTriggerSet.size === 0;
  }

  private captureUrlAttribution(): void {
    if (typeof window === "undefined" || !window.location?.search) return;
    try {
      const params = new URLSearchParams(window.location.search);
      for (const key of ATTRIBUTION_PARAMS) {
        const val = params.get(key);
        if (val) {
          this.lastTouchUtms[key] = val;
          const firstKey = `initial_${key}`;
          if (!(firstKey in this.firstTouchUtms)) {
            this.firstTouchUtms[firstKey] = val;
          }
        }
      }
    } catch {
      // Ignore malformed search query strings.
    }
  }

  private attachAutocaptureListeners(opt: boolean | AutocaptureConfig): void {
    if (!opt) return;
    const cfg: AutocaptureConfig =
      opt === true ? { pageviews: true, clicks: true, forms: true } : opt;

    // One `$pageview` per distinct path + query; hash changes and same-URL replaceState
    // calls (scroll restoration, state updates) are not new pages.
    let lastPage: string | null = null;
    const emitPageview = () => {
      const loc = window.location;
      const page = loc ? `${loc.pathname ?? ""}${loc.search ?? ""}` : "";
      if (page === lastPage) return;
      lastPage = page;
      this.captureUrlAttribution();
      this.track("$pageview", collectDefaultContext());
    };

    if (cfg.pageviews !== false) {
      emitPageview();
      this.boundPopstateListener = emitPageview;
      window.addEventListener("popstate", this.boundPopstateListener);
      // Single-page apps navigate with history.pushState/replaceState, which fire no event.
      const hist = window.history;
      if (hist && typeof hist.pushState === "function") {
        const origPush = hist.pushState;
        const origReplace = hist.replaceState;
        hist.pushState = function (this: History, ...args: Parameters<History["pushState"]>) {
          const r = origPush.apply(this, args);
          emitPageview();
          return r;
        };
        hist.replaceState = function (this: History, ...args: Parameters<History["replaceState"]>) {
          const r = origReplace.apply(this, args);
          emitPageview();
          return r;
        };
        this.restoreHistory = () => {
          hist.pushState = origPush;
          hist.replaceState = origReplace;
        };
      }
    }

    const matchesFilter = (el: HTMLElement | null): boolean => {
      if (!el || typeof el.closest !== "function") return false;
      if (cfg.blockSelectors?.some((sel) => el.closest(sel))) return false;
      if (cfg.allowSelectors?.length) {
        return cfg.allowSelectors.some((sel) => el.closest(sel));
      }
      return true;
    };

    if (cfg.clicks !== false) {
      this.boundAutocaptureClickListener = (ev: MouseEvent) => {
        const target = ev.target as HTMLElement | null;
        if (!matchesFilter(target)) return;
        const interactive = target?.closest?.("button, a, [role='button'], input, select") as
          | HTMLElement
          | null;
        if (!interactive) return;
        const explicitlyUnmasked = Boolean(interactive.closest?.("[data-pulse-unmask]"));
        const rawHref = (interactive as HTMLAnchorElement).href || "";
        this.track("$autocapture", {
          $event_type: "click",
          tag: (interactive.tagName || "").toLowerCase(),
          masked_text: maskPiiText(interactive.textContent || "", explicitlyUnmasked),
          href: rawHref ? stripQuery(rawHref, 512) : null,
        });
      };
      window.addEventListener("click", this.boundAutocaptureClickListener);
    }

    if (cfg.forms !== false) {
      this.boundAutocaptureSubmitListener = (ev: Event) => {
        const target = ev.target as HTMLElement | null;
        if (!matchesFilter(target)) return;
        this.track("$autocapture", {
          $event_type: "submit",
          tag: (target?.tagName || "form").toLowerCase(),
        });
      };
      window.addEventListener("submit", this.boundAutocaptureSubmitListener);
    }
  }

  /** Uncaught errors and unhandled rejections → `$error` (and an error marker in the replay). */
  private attachErrorListeners(): void {
    this.boundErrorListener = (ev: ErrorEvent) => {
      const where = ev.filename ? ` (${stripQuery(ev.filename)}:${ev.lineno ?? 0})` : "";
      this.recordConsoleError(`${ev.message || "Script error"}${where}`);
    };
    this.boundRejectionListener = (ev: PromiseRejectionEvent) => {
      const r = ev.reason as { message?: unknown } | undefined;
      const msg = typeof r?.message === "string" ? r.message : String(ev.reason);
      this.recordConsoleError(`Unhandled rejection: ${msg}`);
    };
    window.addEventListener("error", this.boundErrorListener);
    window.addEventListener("unhandledrejection", this.boundRejectionListener);
  }

  /** Wraps `fetch` and `XMLHttpRequest` to put each call on the replay timeline. */
  private attachNetworkCapture(): void {
    const w = window as unknown as {
      fetch?: typeof fetch;
      XMLHttpRequest?: { prototype: { open: (...a: unknown[]) => void; send: (...a: unknown[]) => void } };
    };
    const own = this.endpoint;
    const record = (method: string, url: string, status: number, started: number) => {
      if (url.startsWith(own)) return;
      this.recordNetworkBreadcrumb(method.toUpperCase(), stripQuery(url), status, Math.round(Date.now() - started));
    };
    const restores: Array<() => void> = [];

    if (typeof w.fetch === "function") {
      const origFetch = w.fetch;
      w.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : input instanceof URL ? input.href : (input as Request).url;
        const method = init?.method ?? (typeof input === "object" && "method" in input ? (input as Request).method : "GET");
        const started = Date.now();
        try {
          const res = await origFetch(input as RequestInfo, init);
          record(method, url, res.status, started);
          return res;
        } catch (err) {
          record(method, url, 0, started);
          throw err;
        }
      }) as typeof fetch;
      restores.push(() => {
        w.fetch = origFetch;
      });
    }

    const xhrProto = w.XMLHttpRequest?.prototype;
    if (xhrProto) {
      const origOpen = xhrProto.open;
      const origSend = xhrProto.send;
      xhrProto.open = function (this: { __pulse?: { method: string; url: string } }, ...args: unknown[]) {
        this.__pulse = { method: String(args[0] ?? "GET"), url: String(args[1] ?? "") };
        return origOpen.apply(this, args);
      };
      xhrProto.send = function (
        this: { __pulse?: { method: string; url: string }; status?: number; addEventListener?: (t: string, f: () => void) => void },
        ...args: unknown[]
      ) {
        const meta = this.__pulse;
        const started = Date.now();
        if (meta && typeof this.addEventListener === "function") {
          this.addEventListener("loadend", () => record(meta.method, meta.url, this.status ?? 0, started));
        }
        return origSend.apply(this, args);
      };
      restores.push(() => {
        xhrProto.open = origOpen;
        xhrProto.send = origSend;
      });
    }
    this.restoreNetwork = () => restores.forEach((r) => r());
  }

  /** Takes a wireframe snapshot on load and when the page changes (throttled to 1 per second). */
  private attachSnapshotObserver(): void {
    const snap = () => {
      const now = Date.now();
      if (now - this.lastSnapshotMs < SNAPSHOT_THROTTLE_MS) return;
      this.lastSnapshotMs = now;
      this.captureDomWireframeSnapshot();
    };
    snap();
    const MO = (globalThis as { MutationObserver?: new (cb: () => void) => { observe(t: unknown, o: unknown): void; disconnect(): void } }).MutationObserver;
    if (MO && document.body) {
      let pending: ReturnType<typeof setTimeout> | null = null;
      const mo = new MO(() => {
        if (pending) return;
        pending = setTimeout(() => {
          pending = null;
          snap();
        }, SNAPSHOT_THROTTLE_MS);
      });
      mo.observe(document.body, { childList: true, subtree: true, attributes: false });
      this.mutationObserver = mo;
    }
  }

  private parsePersistedState(raw: string | null): PersistedWebState | null {
    try {
      if (!raw) return null;
      const parsed = JSON.parse(raw) as Partial<PersistedWebState>;
      if (
        typeof parsed.routingSeed === "string" &&
        typeof parsed.anonId === "string" &&
        typeof parsed.sessionId === "string" &&
        typeof parsed.lastActivityMs === "number"
      ) {
        return {
          routingSeed: parsed.routingSeed,
          anonId: parsed.anonId,
          userId: typeof parsed.userId === "string" ? parsed.userId : null,
          sessionId: parsed.sessionId,
          lastActivityMs: parsed.lastActivityMs,
          optedOut: typeof parsed.optedOut === "boolean" ? parsed.optedOut : undefined,
          explicitOptSet: typeof parsed.explicitOptSet === "boolean" ? parsed.explicitOptSet : undefined,
          superProps: parsed.superProps && typeof parsed.superProps === "object" ? parsed.superProps : undefined,
          firstTouchUtms: parsed.firstTouchUtms && typeof parsed.firstTouchUtms === "object" ? parsed.firstTouchUtms : undefined,
          lastTouchUtms: parsed.lastTouchUtms && typeof parsed.lastTouchUtms === "object" ? parsed.lastTouchUtms : undefined,
          groups: parsed.groups && typeof parsed.groups === "object" ? parsed.groups : undefined,
          exposuresSent: Array.isArray(parsed.exposuresSent) ? parsed.exposuresSent : undefined,
        };
      }
    } catch {
      // Ignore malformed state JSON.
    }
    return null;
  }

  private get queueKey(): string {
    return `${this.storageKey}_queue`;
  }

  private readQueueRaw(): string | null | Promise<string | null> {
    try {
      return this.storage.getItem(this.queueKey);
    } catch {
      return null;
    }
  }

  private parsePersistedQueue(raw: string | null): Array<Record<string, unknown>> {
    try {
      const parsed = raw ? (JSON.parse(raw) as unknown) : [];
      return Array.isArray(parsed)
        ? parsed.filter((e): e is Record<string, unknown> => !!e && typeof e === "object")
        : [];
    } catch {
      return [];
    }
  }

  /** Saves unsent and in-flight events so a reload or crash does not lose them. */
  private persistQueue(): void {
    try {
      const all = [...this.inFlightEvents, ...this.eventQueue];
      if (all.length === 0) {
        const res = this.storage.removeItem(this.queueKey);
        if (isPromiseLike(res)) Promise.resolve(res).catch(() => {});
        return;
      }
      // ponytail: whole-queue rewrite per change; fine at the default 2,000-event cap.
      const res = this.storage.setItem(this.queueKey, JSON.stringify(all.slice(-this.maxQueuedEvents)));
      if (isPromiseLike(res)) Promise.resolve(res).catch(() => {});
    } catch {
      // Quota or SecurityError: the in-memory queue still works.
    }
  }

  private debugLog(...args: unknown[]): void {
    if (this.cfg.debug && typeof console !== "undefined") console.debug("[pulse]", ...args);
  }

  private persistState(): void {
    try {
      const payload: PersistedWebState = {
        routingSeed: this.routingSeed,
        anonId: this.anonId,
        userId: this.userId,
        sessionId: this.sessionId,
        lastActivityMs: this.lastActivityMs,
        optedOut: this.optedOut,
        explicitOptSet: this.explicitOptSet,
        superProps: this.superProps,
        firstTouchUtms: this.firstTouchUtms,
        lastTouchUtms: this.lastTouchUtms,
        groups: this.groups,
        exposuresSent: Array.from(this.exposuresSent),
      };
      const res = this.storage.setItem(this.storageKey, JSON.stringify(payload));
      if (isPromiseLike(res)) Promise.resolve(res).catch(() => {});
    } catch {
      // Ignore storage quota / SecurityError.
    }
  }

  public getDistinctId(): string {
    return this.userId ?? this.anonId;
  }

  public getSessionId(): string {
    return this.sessionId;
  }

  public getQueuedEventCount(): number {
    return this.eventQueue.length;
  }

  public clearBackoffForRetry(): void {
    this.consecutiveFailures = 0;
    this.backoffUntilMs = 0;
  }

  private sealPendingReplayOps(): void {
    if (this.replayOps.length === 0) return;
    const ops = this.replayOps.splice(0, this.replayOps.length);
    const sessionId = this.replayOpsSessionId ?? this.sessionId;
    const chunkStartTsMs = this.chunkStartTsMs ?? Date.now();
    this.replayOpsSessionId = null;
    this.chunkStartTsMs = null;

    if (this.replayQueue.length >= MAX_QUEUED_REPLAY_PACKETS) {
      this.replayQueue.shift();
    }
    this.replayQueue.push({ sessionId, chunkStartTsMs, ops });
  }

  private resetSnapshotDiffState(): void {
    this.lastSnapshotViewport = null;
    this.lastSnapshotNodes.clear();
  }

  private touchSession(now: number): void {
    if (now > this.lastActivityMs && now - this.lastActivityMs > SESSION_TIMEOUT_MS) {
      // Seal pending replay ops under the outgoing session BEFORE rotating!
      this.sealPendingReplayOps();
      this.resetSnapshotDiffState();
      this.sessionId = this.newId("sess");
      this.replayOffsetKb = 0;
    }
    if (now > this.lastActivityMs) {
      this.lastActivityMs = now;
      this.persistState();
    }
  }

  private enqueueEventsBounded(events: Array<Record<string, unknown>>, prepend = false): void {
    if (prepend) {
      this.eventQueue = [...events, ...this.eventQueue];
    } else {
      this.eventQueue.push(...events);
    }
    if (this.eventQueue.length > this.maxQueuedEvents) {
      this.eventQueue.splice(0, this.eventQueue.length - this.maxQueuedEvents);
    }
    this.persistQueue();
  }

  // --- Consent & Opt-Out (`Gap 4`) ---

  public optOut(): void {
    this.optedOut = true;
    this.explicitOptSet = true;
    this.eventQueue = [];
    this.inFlightEvents = [];
    this.persistQueue();
    this.engageQueue = [];
    this.replayQueue = [];
    this.replayOps = [];
    this.timedEvents.clear();
    this.persistState();
  }

  public optIn(): void {
    this.optedOut = false;
    this.explicitOptSet = true;
    this.persistState();
  }

  public hasOptedOut(): boolean {
    return this.optedOut;
  }

  public clearOptInOut(): void {
    this.explicitOptSet = false;
    this.optedOut = Boolean(this.cfg.optOutByDefault);
    this.persistState();
  }

  // --- Super Properties & Timed Events (`Gaps 9, 10`) ---

  public register(props: EventProperties): void {
    Object.assign(this.superProps, props);
    this.persistState();
  }

  public registerOnce(props: EventProperties): void {
    for (const [k, v] of Object.entries(props)) {
      if (!(k in this.superProps)) {
        this.superProps[k] = v;
      }
    }
    this.persistState();
  }

  public unregister(key: string): void {
    delete this.superProps[key];
    this.persistState();
  }

  public timeEvent(eventName: string): void {
    if (this.optedOut) return;
    this.timedEvents.set(eventName, Date.now());
  }

  public track(eventName: string, props: EventProperties = {}): void {
    if (this.optedOut) return;
    const now = Date.now();
    this.touchSession(now);
    const distinctId = this.getDistinctId();
    this.seqCounter += 1;

    if (this.replayTriggerSet.has(eventName) && !this.replayTriggered) {
      this.replayTriggered = true;
      this.startSessionRecording();
    }

    const merged: EventProperties = {
      ...collectDefaultContext(),
      ...(this.cfg.contextProvider ? this.cfg.contextProvider() : {}),
      ...this.firstTouchUtms,
      ...this.lastTouchUtms,
      ...this.superProps,
    };
    for (const [gk, gv] of Object.entries(this.groups)) {
      merged[`$group_${gk}`] = gv;
    }

    const startTime = this.timedEvents.get(eventName);
    if (typeof startTime === "number") {
      this.timedEvents.delete(eventName);
      const durationMs = Math.max(0, now - startTime);
      merged.$duration_ms = durationMs;
      merged.$duration = Math.round(durationMs) / 1000;
    }

    Object.assign(merged, props);
    for (const blocked of this.propertyBlacklist) {
      delete merged[blocked];
    }

    let eventObj: Record<string, unknown> | null = {
      event_uuid: `${distinctId}:${this.sessionId}:${eventName}:${now}:${this.seqCounter}`,
      routing_seed: this.routingSeed,
      distinct_id: distinctId,
      session_id: this.sessionId,
      event_name: eventName,
      client_event_ts_ms: now,
      client_sent_ts_ms: 0,
      props: merged,
      replay_offset_kb: this.replayOffsetKb,
    };

    if (this.cfg.beforeSend) {
      eventObj = this.cfg.beforeSend(eventObj);
      if (!eventObj) return;
    }

    this.enqueueEventsBounded([eventObj]);
    this.debugLog("track", eventObj);

    if (this.eventQueue.length >= this.flushBatchSize) {
      void this.flush();
    }
  }

  public async identify(userId: string, userProps?: EventProperties): Promise<void> {
    if (this.optedOut) return;
    const now = Date.now();
    this.touchSession(now);
    const prevAnon = this.anonId;
    this.userId = userId;
    this.userIdSetExplicitly = true;
    this.persistState();

    const bodyPayload: Record<string, unknown> = {
      anon_id: prevAnon,
      user_id: userId,
      routing_seed: this.routingSeed,
    };
    if (userProps) {
      bodyPayload.user_properties = userProps;
    }

    const res = await this.fetchWithTimeout(`${this.endpoint}/v1/identify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-pulse-key": this.apiKey,
      },
      body: JSON.stringify(bodyPayload),
    });
    if (!res.ok) {
      throw new Error(`identify failed with HTTP ${res.status}`);
    }
  }

  // --- User Profiles, Groups & Revenue (`Gaps 1, 5, 8`) ---

  public async engage(ops: Record<string, unknown>): Promise<void> {
    if (this.optedOut) return;
    const payload = {
      $distinct_id: this.getDistinctId(),
      routing_seed: this.routingSeed,
      ...ops,
    };
    try {
      const res = await this.fetchWithTimeout(`${this.endpoint}/v1/engage`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify(payload),
      });
      if (res.ok) return;
    } catch {
      // Queue offline retry below
    }
    this.engageQueue.push(payload);
  }

  public setUserProperties(props: EventProperties): Promise<void> {
    return this.engage({ $set: props });
  }

  public setUserPropertiesOnce(props: EventProperties): Promise<void> {
    return this.engage({ $setOnce: props });
  }

  public incrementUserProperty(prop: string, by = 1): Promise<void> {
    return this.engage({ $add: { [prop]: by } });
  }

  public unsetUserProperties(keys: string[]): Promise<void> {
    return this.engage({ $unset: keys });
  }

  public appendUserProperty(prop: string, value: PropPrimitive): Promise<void> {
    return this.engage({ $append: { [prop]: value } });
  }

  public unionUserProperty(prop: string, values: PropPrimitive[]): Promise<void> {
    return this.engage({ $union: { [prop]: values } });
  }

  public removeUserProperty(prop: string, value: PropPrimitive): Promise<void> {
    return this.engage({ $remove: { [prop]: value } });
  }

  /**
   * Forgets this user on this device. It does not erase server-side data: the ingest key is
   * public, so erasure needs an admin call to DELETE /v1/projects/:id/users/:distinct_id.
   */
  public async deleteUser(): Promise<void> {
    this.reset();
  }

  public setGroup(groupType: string, groupId: string): void {
    if (this.optedOut) return;
    this.groups[groupType] = groupId;
    this.persistState();
  }

  public async setGroupProperties(
    groupType: string,
    groupId: string,
    props: EventProperties,
  ): Promise<void> {
    if (this.optedOut) return;
    this.groups[groupType] = groupId;
    this.persistState();
    await this.fetchWithTimeout(`${this.endpoint}/v1/groups`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-pulse-key": this.apiKey,
      },
      body: JSON.stringify({
        group_type: groupType,
        group_id: groupId,
        $set: props,
      }),
    });
  }

  public async trackRevenue(params: RevenueParams): Promise<void> {
    if (this.optedOut) return;
    const currency = params.currency ?? "USD";
    const quantity = params.quantity ?? 1;
    const totalAmount = params.amount * quantity;
    const evProps: EventProperties = {
      ...(params.props ?? {}),
      amount: totalAmount,
      currency,
      quantity,
    };
    if (params.productId) evProps.product_id = params.productId;
    if (params.revenueType) evProps.revenue_type = params.revenueType;
    this.track("$revenue", evProps);
    await this.engage({
      $track_charge: {
        $amount: totalAmount,
        currency,
        ...(params.productId ? { product_id: params.productId } : {}),
        ...(params.revenueType ? { revenue_type: params.revenueType } : {}),
      },
    });
  }

  /**
   * Resets identity on logout, rotating `anonId` and `sessionId` so shared-device logins
   * never merge two distinct authenticated users.
   */
  public reset(): void {
    this.sealPendingReplayOps();
    this.resetSnapshotDiffState();
    this.userId = null;
    this.userIdSetExplicitly = false;
    this.anonId = this.newId("anon");
    this.sessionId = this.newId("sess");
    this.replayOffsetKb = 0;
    this.recentClicks = [];
    this.exposuresSent.clear();
    this.timedEvents.clear();
    this.lastActivityMs = Date.now();
    this.persistState();
  }

  // --- Flags, Experiments & Exposure Tracking (`Gap 13`) ---

  public getVariantSync(flagKey: string): string | null {
    this.refreshFlagsIfStale();
    const rule = this.flagRules.get(flagKey);
    if (!rule) return null;
    return evaluateEdgeFlag(rule, this.getDistinctId());
  }

  public isEnabled(flagKey: string): boolean {
    const v = this.getVariantSync(flagKey);
    return v !== null && v !== "off" && v !== "false" && v !== "control";
  }

  public getVariantPayload(flagKey: string): unknown | null {
    const rule = this.flagRules.get(flagKey);
    if (!rule) return null;
    const chosen = evaluateEdgeFlag(rule, this.getDistinctId());
    if (!chosen) return null;
    return rule.variants.find((v) => v.key === chosen)?.payload ?? null;
  }

  public trackExposure(flagKey: string, variant: string | null): void {
    if (this.optedOut || !variant) return;
    const dedupKey = `${flagKey}:${variant}`;
    if (this.exposuresSent.has(dedupKey)) return;
    this.exposuresSent.add(dedupKey);
    this.persistState();
    this.track("$experiment_started", { flag_key: flagKey, variant });
  }

  public async getVariant(
    flagKey: string,
    options?: { trackExposure?: boolean },
  ): Promise<string | null> {
    if (this.flagRules.size === 0) {
      await this.refreshFlags();
    }
    const variant = this.getVariantSync(flagKey);
    if (options?.trackExposure && variant) {
      this.trackExposure(flagKey, variant);
    }
    return variant;
  }

  private refreshFlagsIfStale(): void {
    const ttl = this.cfg.flagsTtlMs ?? DEFAULT_FLAGS_TTL_MS;
    if (ttl <= 0 || this.flagsCheckedAtMs === 0 || Date.now() - this.flagsCheckedAtMs < ttl) return;
    void this.refreshFlags();
  }

  /** Fetches flags now; concurrent calls share one request. */
  public refreshFlags(): Promise<void> {
    this.flagsRefresh ??= this.fetchFlags().finally(() => {
      this.flagsRefresh = null;
    });
    return this.flagsRefresh;
  }

  private async fetchFlags(): Promise<void> {
    this.flagsCheckedAtMs = Date.now();
    try {
      const headers: Record<string, string> = {};
      if (this.flagsEtag) headers["If-None-Match"] = this.flagsEtag;
      const res = await this.fetchWithTimeout(`${this.endpoint}/v1/flags/${encodeURIComponent(this.apiKey)}`, {
        headers,
      });
      if (res.status === 304 || !res.ok) return;
      const etag = res.headers.get("etag");
      if (etag) this.flagsEtag = etag;
      const body = (await res.json()) as { flags: EdgeFlagRule[] };
      this.flagRules.clear();
      for (const f of body.flags) {
        this.flagRules.set(f.key, f);
      }
    } catch {
      // Keep existing cached rules on transient network errors.
    }
  }

  // --- Session Replay Controls (`Gap 11`) ---

  public startSessionRecording(): void {
    this.recordingActive = true;
    this.recordingPaused = false;
  }

  public stopSessionRecording(): void {
    this.recordingActive = false;
    this.recordingPaused = false;
    this.sealPendingReplayOps();
    this.resetSnapshotDiffState();
  }

  public pauseSessionRecording(): void {
    this.recordingPaused = true;
  }

  public resumeSessionRecording(): void {
    if (this.recordingActive) {
      this.recordingPaused = false;
    }
  }

  public isRecordingSession(): boolean {
    return !this.optedOut && this.recordingActive && !this.recordingPaused;
  }

  public getSessionReplayUrl(): string {
    return `${this.endpoint}/#/replays?session=${encodeURIComponent(this.sessionId)}`;
  }

  public recordNetworkBreadcrumb(
    method: string,
    path: string,
    statusCode: number,
    durationMs: number,
  ): void {
    if (this.optedOut || (this.cfg.enableWireframeReplay && !this.isRecordingSession())) return;
    const now = Date.now();
    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    this.replayOps.push({
      op: "network_breadcrumb",
      ts_offset_ms: Math.max(0, now - this.chunkStartTsMs),
      method: method.slice(0, 16),
      path: path.slice(0, 256),
      status_code: statusCode,
      duration_ms: durationMs,
    });
  }

  public recordConsoleError(message: string): void {
    if (this.optedOut) return;
    if (this.isRecordingSession()) {
      const now = Date.now();
      if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
      if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
      // One stable node, so repeated errors replace the banner instead of stacking copies.
      // Draw it over the page's own alert when one is visible, else as a toast in the top-left corner.
      const box = visibleAlertBox(message) ?? { x: 16, y: 16, w: 340, h: 44 };
      this.replayOps.push({
        op: "node_upsert",
        ts_offset_ms: Math.max(0, now - this.chunkStartTsMs),
        node_id: ERROR_BANNER_NODE_ID,
        parent_id: 1,
        kind: "error_banner",
        ...box,
        bg_rgba: 0xdc2626ff,
        masked_label: maskPiiText(message, false),
      });
    }
    this.track("$error", { message: message.slice(0, 256) });
  }

  /**
   * Public API for pushing a single wireframe replay op (e.g. from custom or React Native recorders).
   * Enforces privacy masking on `masked_input` nodes.
   */
  public recordWireframeOp(op: WireframeOp): void {
    if (this.optedOut || !this.isRecordingSession()) return;
    const now = Date.now();
    this.touchSession(now);
    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    if (op.op === "full_snapshot") {
      this.replayOps.push({
        ...op,
        nodes: op.nodes.map(sanitizeWireNode),
      });
    } else if (op.op === "node_upsert") {
      this.replayOps.push({
        ...sanitizeWireNode(op),
        op: "node_upsert",
        ts_offset_ms: op.ts_offset_ms,
      });
    } else {
      this.replayOps.push(op);
    }
  }

  /** Public API for pushing multiple wireframe replay ops in order. */
  public recordWireframeOps(ops: WireframeOp[]): void {
    if (ops.length === 0 || this.optedOut || !this.isRecordingSession()) return;
    for (const op of ops) {
      this.recordWireframeOp(op);
    }
  }

  /**
   * Records a wireframe screen snapshot with automatic incremental diffing.
   *
   * On the first snapshot (or when the viewport dimensions change, `forceFull` is set, or the diff
   * would not save ops over a full snapshot), emits a `full_snapshot`. On subsequent snapshots,
   * emits only `node_upsert` for added/changed nodes and `node_remove` for removed nodes (or
   * nothing at all if the wireframe is unchanged).
   */
  public recordWireframeSnapshot(
    width: number,
    height: number,
    nodes: WireNode[],
    options?: { forceFull?: boolean },
  ): void {
    if (this.optedOut || !this.isRecordingSession()) return;
    const now = Date.now();
    this.touchSession(now);
    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    const tsOffsetMs = Math.max(0, now - this.chunkStartTsMs);

    const vpW = Math.min(65535, Math.max(1, Math.round(width)));
    const vpH = Math.min(65535, Math.max(1, Math.round(height)));
    const sanitized = nodes.map(sanitizeWireNode);
    const nextMap = new Map<number, WireNode>();
    for (const n of sanitized) {
      nextMap.set(n.node_id, n);
    }

    const canDiff =
      !options?.forceFull &&
      this.lastSnapshotViewport !== null &&
      this.lastSnapshotViewport.w === vpW &&
      this.lastSnapshotViewport.h === vpH &&
      this.lastSnapshotNodes.size > 0;

    if (canDiff) {
      const upserts: WireNode[] = [];
      const removes: number[] = [];

      for (const [id, nextNode] of nextMap) {
        const prev = this.lastSnapshotNodes.get(id);
        if (!prev || !wireNodesEqual(prev, nextNode)) {
          upserts.push(nextNode);
        }
      }
      for (const prevId of this.lastSnapshotNodes.keys()) {
        if (!nextMap.has(prevId)) {
          removes.push(prevId);
        }
      }

      // No visual changes since the last snapshot: emit nothing (0 bytes).
      if (upserts.length === 0 && removes.length === 0) {
        return;
      }

      // Emit incremental diff whenever it is smaller than a full snapshot.
      if (upserts.length + removes.length < sanitized.length) {
        for (const node of upserts) {
          this.replayOps.push({
            op: "node_upsert",
            ts_offset_ms: tsOffsetMs,
            ...node,
          });
        }
        for (const nodeId of removes) {
          this.replayOps.push({
            op: "node_remove",
            ts_offset_ms: tsOffsetMs,
            node_id: nodeId,
          });
        }
        this.lastSnapshotNodes = nextMap;
        return;
      }
    }

    this.replayOps.push({
      op: "full_snapshot",
      ts_offset_ms: tsOffsetMs,
      width: vpW,
      height: vpH,
      nodes: sanitized,
    });
    this.lastSnapshotViewport = { w: vpW, h: vpH };
    this.lastSnapshotNodes = nextMap;
  }

  /** Public API to record a pointer interaction (`tap`, `dead_tap`, `scroll`, or `rage_tap`). */
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
    if (this.optedOut || !this.isRecordingSession()) return;
    const now = Date.now();
    this.touchSession(now);
    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    const resolvedKind = typeof kind === "string" ? ({ type: kind } as const) : kind;
    this.replayOps.push({
      op: "pointer_interaction",
      ts_offset_ms: Math.max(0, now - this.chunkStartTsMs),
      x: Math.max(0, Math.min(65535, Math.round(x))),
      y: Math.max(0, Math.min(65535, Math.round(y))),
      kind: resolvedKind,
    });
  }

  /**
   * Public tap helper with built-in rage-tap detection (3+ taps within 800ms & 40px radius).
   * Useful for React Native touch handlers as well as web click listeners.
   */
  public recordTap(x: number, y: number, interactive = true): void {
    if (this.optedOut || !this.isRecordingSession()) return;
    const now = Date.now();
    this.touchSession(now);
    const rx = Math.max(0, Math.min(65535, Math.round(x)));
    const ry = Math.max(0, Math.min(65535, Math.round(y)));

    this.recentClicks = this.recentClicks.filter((c) => now - c.ts <= 800);
    this.recentClicks.push({ ts: now, x: rx, y: ry });
    const cluster = this.recentClicks.filter((c) => {
      const dx = c.x - rx;
      const dy = c.y - ry;
      return dx * dx + dy * dy <= 1600;
    });

    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    const offset = Math.max(0, now - this.chunkStartTsMs);

    if (cluster.length >= 3) {
      this.recentClicks = [];
      this.replayOps.push({
        op: "pointer_interaction",
        ts_offset_ms: offset,
        x: rx,
        y: ry,
        kind: { type: "rage_tap", count: cluster.length },
      });
      this.track("$rage_tap", { x: rx, y: ry, tap_count: cluster.length });
    } else if (!interactive) {
      this.replayOps.push({
        op: "pointer_interaction",
        ts_offset_ms: offset,
        x: rx,
        y: ry,
        kind: { type: "dead_tap" },
      });
    } else {
      this.replayOps.push({
        op: "pointer_interaction",
        ts_offset_ms: offset,
        x: rx,
        y: ry,
        kind: { type: "tap" },
      });
    }
  }

  private getDomNodeId(el: Element): number {
    let id = this.domNodeIds.get(el);
    if (id === undefined) {
      id = this.nextDomNodeId++;
      this.domNodeIds.set(el, id);
    }
    return id;
  }

  /**
   * Walks live DOM bounding boxes and emits a default-deny masked wireframe snapshot
   * (incrementally diffed against the previous snapshot using stable per-element node IDs).
   */
  public captureDomWireframeSnapshot(options?: { forceFull?: boolean }): void {
    if (this.optedOut || !this.isRecordingSession()) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    const nodes: WireNode[] = [];
    const vpW = Math.min(65535, Math.max(320, window.innerWidth));
    const vpH = Math.min(65535, Math.max(480, window.innerHeight));

    const docUnmasked = Boolean(document.documentElement?.hasAttribute("data-pulse-unmask"));
    nodes.push({
      node_id: 1,
      parent_id: 0,
      kind: "screen",
      x: 0,
      y: 0,
      w: vpW,
      h: vpH,
      bg_rgba: 0x090d16ff,
      masked_label: maskPiiText(document.title || "Screen", docUnmasked),
    });

    const elements = document.body ? Array.from(document.body.querySelectorAll("*")).slice(0, 120) : [];
    for (const el of elements) {
      if (this.cfg.blockSelectors?.some((sel) => typeof (el as HTMLElement).closest === "function" && (el as HTMLElement).closest(sel))) {
        continue;
      }
      const rect = el.getBoundingClientRect();
      if (rect.width < 4 || rect.height < 4) continue;
      if (rect.bottom < 0 || rect.top > vpH) continue;

      const tag = el.tagName.toLowerCase();
      const htmlEl = el as HTMLElement;
      const isEditable =
        tag === "input" ||
        tag === "textarea" ||
        tag === "select" ||
        htmlEl.isContentEditable ||
        el.hasAttribute("contenteditable");
      const forceMasked = Boolean(
        this.cfg.maskSelectors?.some((sel) => typeof htmlEl.closest === "function" && htmlEl.closest(sel)),
      );
      const customUnmask = Boolean(
        this.cfg.unmaskSelectors?.some((sel) => typeof htmlEl.closest === "function" && htmlEl.closest(sel)),
      );
      const explicitlyUnmasked =
        !isEditable && !forceMasked && (customUnmask || Boolean(el.closest("[data-pulse-unmask]")));

      let kind: NodeKind = "container";
      let label = "";

      if (isEditable) {
        kind = "masked_input";
        label = FIXED_MASK;
      } else if (tag === "button" || el.getAttribute("role") === "button") {
        kind = "button";
        label = maskPiiText(el.textContent || "", explicitlyUnmasked);
      } else if (tag === "img" || tag === "svg") {
        kind = "image_placeholder";
        label = "media";
      } else if (el.getAttribute("role") === "alert") {
        kind = "error_banner";
        label = maskPiiText(el.textContent || "", explicitlyUnmasked);
      } else if (el.children.length === 0 && (el.textContent || "").trim().length > 0) {
        kind = "text";
        label = maskPiiText(el.textContent || "", explicitlyUnmasked);
      } else {
        continue;
      }

      nodes.push({
        node_id: this.getDomNodeId(el),
        parent_id: 1,
        kind,
        x: Math.max(0, Math.round(rect.left)),
        y: Math.max(0, Math.round(rect.top)),
        w: Math.min(65535, Math.round(rect.width)),
        h: Math.min(65535, Math.round(rect.height)),
        bg_rgba: kind === "button" ? 0x3b82f6ff : 0x162033ff,
        masked_label: label,
      });
    }

    this.recordWireframeSnapshot(vpW, vpH, nodes, options);
  }

  private attachReplayClickListener(): void {
    this.boundClickListener = (ev: MouseEvent) => {
      if (this.optedOut || !this.isRecordingSession()) return;
      const target = ev.target as HTMLElement | null;
      const interactive = Boolean(
        target?.closest("button, a, input, select, textarea, [role='button']"),
      );
      this.recordTap(ev.clientX, ev.clientY, interactive);
    };
    window.addEventListener("click", this.boundClickListener);
  }

  /**
   * Executes `fetch` with a hard timeout (`fetchTimeoutMs`, default 10s) using `AbortController`
   * plus a `Promise.race` safeguard so hung sockets on mobile networks never stall the send queue.
   */
  private async fetchWithTimeout(url: string, init: RequestInit): Promise<Response> {
    const timeoutMs = this.fetchTimeoutMs;
    if (timeoutMs <= 0) {
      return fetch(url, init);
    }
    const ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        ctrl?.abort();
        reject(new Error(`Pulse request timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    });
    try {
      const req = fetch(url, ctrl ? { ...init, signal: ctrl.signal } : init);
      // Suppress unhandled rejection if the underlying fetch rejects after timeoutPromise wins.
      req.catch(() => {});
      return await Promise.race([req, timeoutPromise]);
    } finally {
      if (timer !== null) clearTimeout(timer);
    }
  }

  /**
   * Exponential backoff with full jitter (a uniform delay up to a cap that doubles per failure),
   * so browsers don't all retry in lockstep after an outage. A `Retry-After` header is a floor.
   */
  private recordFailureBackoff(res?: Response): void {
    const exp = Math.min(6, this.consecutiveFailures);
    this.consecutiveFailures += 1;
    const cap = Math.min(60_000, 1_000 * Math.pow(2, exp));
    const retryAfterS = Number(res?.headers.get("retry-after") ?? NaN);
    const floor = Number.isFinite(retryAfterS) ? Math.min(retryAfterS * 1000, 10 * 60_000) : 0;
    this.backoffUntilMs = Date.now() + Math.max(floor, Math.random() * cap);
  }

  public async flush(): Promise<void> {
    if (this.inFlightFlush) {
      return this.inFlightFlush;
    }
    this.inFlightFlush = this.flushInternal().finally(() => {
      this.inFlightFlush = null;
    });
    return this.inFlightFlush;
  }

  private async flushInternal(): Promise<void> {
    await this.hydrationPromise;
    this.sealPendingReplayOps();
    if (Date.now() < this.backoffUntilMs) {
      return;
    }

    while (this.replayQueue.length > 0) {
      const pkt = this.replayQueue.shift()!;
      try {
        const res = await this.fetchWithTimeout(`${this.endpoint}/v1/replay/chunk`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-pulse-key": this.apiKey,
          },
          body: JSON.stringify({
            project_id: "",
            session_id: pkt.sessionId,
            chunk_start_ts_ms: pkt.chunkStartTsMs,
            ops: pkt.ops,
          }),
        });
        if (res.ok) {
          const body = (await res.json()) as { replay_offset_kb?: number };
          if (typeof body.replay_offset_kb === "number" && pkt.sessionId === this.sessionId) {
            this.replayOffsetKb = body.replay_offset_kb;
          }
          this.consecutiveFailures = 0;
          this.backoffUntilMs = 0;
        } else if (res.status === 400 || res.status === 413 || res.status === 422) {
          // Permanent client error: drop packet so queue does not wedge.
        } else {
          this.replayQueue.unshift(pkt);
          this.recordFailureBackoff(res);
          return;
        }
      } catch {
        this.replayQueue.unshift(pkt);
        this.recordFailureBackoff();
        return;
      }
    }

    if (this.engageQueue.length > 0) {
      const engageBatch = this.engageQueue.splice(0, this.engageQueue.length);
      try {
        const res = await this.fetchWithTimeout(`${this.endpoint}/v1/engage`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-pulse-key": this.apiKey,
          },
          body: JSON.stringify(engageBatch),
        });
        if (!res.ok && res.status >= 500) {
          this.engageQueue.unshift(...engageBatch);
        }
      } catch {
        this.engageQueue.unshift(...engageBatch);
      }
    }

    if (this.eventQueue.length > 0) {
      const batch = this.eventQueue.splice(0, this.eventQueue.length);
      this.inFlightEvents = batch;
      const sentNow = Date.now();
      const wireBatch = batch.map((ev) => ({ ...ev, client_sent_ts_ms: sentNow }));
      try {
        const res = await this.fetchWithTimeout(`${this.endpoint}/v1/ingest`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-pulse-key": this.apiKey,
          },
          body: JSON.stringify({ events: wireBatch }),
        });
        this.inFlightEvents = [];
        if (res.ok) {
          this.consecutiveFailures = 0;
          this.backoffUntilMs = 0;
          this.persistQueue();
          if (this.cfg.debug) {
            const body = (await res.json().catch(() => ({}))) as { accepted?: number; deduped?: number; rejected?: unknown[] };
            this.debugLog(`sent ${batch.length} events`, body);
            if (body.rejected?.length) console.warn("[pulse] events rejected by the server", body.rejected);
          }
        } else if (res.status === 400 || res.status === 413 || res.status === 422) {
          // Drop malformed batch permanently.
          this.persistQueue();
          this.debugLog(`batch refused with ${res.status}; dropped`, await res.text().catch(() => ""));
        } else {
          this.enqueueEventsBounded(batch, true);
          this.recordFailureBackoff(res);
          this.debugLog(`send failed with ${res.status}; will retry`);
        }
      } catch (err) {
        this.inFlightEvents = [];
        this.enqueueEventsBounded(batch, true);
        this.recordFailureBackoff();
        this.debugLog("send failed; will retry", err);
      }
    }
  }

  /**
   * Flushes both replay chunks and events on page hide using header-authenticated `fetch(..., { keepalive: true })` (`P2-05`, `P3-05`).
   */
  public flushBeacon(): void {
    this.sealPendingReplayOps();
    while (this.replayQueue.length > 0) {
      const pkt = this.replayQueue.shift()!;
      fetch(`${this.endpoint}/v1/replay/chunk`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify({
          project_id: "",
          session_id: pkt.sessionId,
          chunk_start_ts_ms: pkt.chunkStartTsMs,
          ops: pkt.ops,
        }),
        keepalive: true,
      }).catch(() => {
        if (this.replayQueue.length < MAX_QUEUED_REPLAY_PACKETS) {
          this.replayQueue.unshift(pkt);
        }
      });
    }

    if (this.engageQueue.length > 0) {
      const engageBatch = this.engageQueue.splice(0, this.engageQueue.length);
      fetch(`${this.endpoint}/v1/engage`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify(engageBatch),
        keepalive: true,
      }).catch(() => {
        this.engageQueue.unshift(...engageBatch);
      });
    }

    if (this.eventQueue.length > 0) {
      const batch = this.eventQueue.splice(0, this.eventQueue.length);
      // The page may be gone before the response arrives: keep the batch persisted as in-flight
      // until it is acknowledged; if the page dies first, the next load resends it (deduped).
      this.inFlightEvents = [...this.inFlightEvents, ...batch];
      this.persistQueue();
      const sentNow = Date.now();
      const wireBatch = batch.map((ev) => ({ ...ev, client_sent_ts_ms: sentNow }));
      fetch(`${this.endpoint}/v1/ingest`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify({ events: wireBatch }),
        keepalive: true,
      })
        .then((res) => {
          this.inFlightEvents = this.inFlightEvents.filter((e) => !batch.includes(e));
          if (res.ok || res.status === 400 || res.status === 413 || res.status === 422) {
            this.persistQueue();
          } else {
            this.enqueueEventsBounded(batch, true);
          }
        })
        .catch(() => {
          this.inFlightEvents = this.inFlightEvents.filter((e) => !batch.includes(e));
          this.enqueueEventsBounded(batch, true);
        });
    }
  }

  public shutdown(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    if (typeof window !== "undefined" && typeof window.removeEventListener === "function") {
      if (this.boundClickListener) {
        window.removeEventListener("click", this.boundClickListener);
        this.boundClickListener = null;
      }
      if (this.boundAutocaptureClickListener) {
        window.removeEventListener("click", this.boundAutocaptureClickListener);
        this.boundAutocaptureClickListener = null;
      }
      if (this.boundAutocaptureSubmitListener) {
        window.removeEventListener("submit", this.boundAutocaptureSubmitListener);
        this.boundAutocaptureSubmitListener = null;
      }
      this.restoreHistory?.();
      this.restoreHistory = null;
      this.restoreNetwork?.();
      this.restoreNetwork = null;
      this.mutationObserver?.disconnect();
      this.mutationObserver = null;
      if (this.boundErrorListener) {
        window.removeEventListener("error", this.boundErrorListener);
        this.boundErrorListener = null;
      }
      if (this.boundRejectionListener) {
        window.removeEventListener("unhandledrejection", this.boundRejectionListener);
        this.boundRejectionListener = null;
      }
      if (this.boundPopstateListener) {
        window.removeEventListener("popstate", this.boundPopstateListener);
        this.boundPopstateListener = null;
      }
      if (this.boundVisibilityListener) {
        window.removeEventListener("visibilitychange", this.boundVisibilityListener);
        this.boundVisibilityListener = null;
      }
    }
    void this.flush();
  }
}
