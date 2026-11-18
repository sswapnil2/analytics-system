/**
 * Pulse Analytics — Zero-Dependency Web & React Native SDK (`@pulse-analytics/ts-web`)
 *
 * Privacy-by-Design Invariant (`P3-01`):
 * Never records screenshots, canvases, or unmasked text/inputs. All text, titles, inputs, and
 * `contenteditable` elements are masked to fixed-length `"████"` on the client unless explicitly
 * allowlisted via `data-pulse-unmask` (inputs and contenteditable remain masked unconditionally).
 */

import { flagRolloutSlot, flagVariantSlot, xxh3_64 } from "./xxh3.js";

export { flagRolloutSlot, flagVariantSlot, xxh3_64 };

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
  | { op: "full_snapshot"; width: number; height: number; nodes: WireNode[] }
  | ({ op: "node_upsert" } & WireNode)
  | { op: "node_remove"; node_id: number }
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
}

export interface EdgeFlagRule {
  key: string;
  enabled: boolean;
  rollout_bps: number;
  targeting_cohort_id?: string;
  cohort_members?: string[];
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
const SESSION_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_QUEUED_EVENTS = 2_000;
const MAX_QUEUED_REPLAY_PACKETS = 64;

/**
 * Default-deny PII masking (`P3-01`): returns fixed-length `"████"` for every non-empty string
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
  if (Array.isArray(rule.cohort_members)) {
    if (!rule.cohort_members.includes(distinctId)) {
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

function randomId(prefix: string): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`;
  }
  const r1 = Math.random().toString(36).slice(2, 10);
  const r2 = Date.now().toString(36);
  return `${prefix}_${r2}${r1}`;
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
    if (window.location?.href) ctx.$current_url = window.location.href.slice(0, 512);
    if (window.location?.pathname) ctx.$pathname = window.location.pathname;
    if (typeof window.innerWidth === "number") ctx.$screen_width = window.innerWidth;
    if (typeof window.innerHeight === "number") ctx.$screen_height = window.innerHeight;
  }
  if (typeof document !== "undefined" && typeof document.referrer === "string" && document.referrer) {
    ctx.$referrer = document.referrer.slice(0, 512);
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
  private readonly flushBatchSize: number;
  private readonly maxQueuedEvents: number;
  private readonly cfg: PulseWebConfig;

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

  private recentClicks: Array<{ ts: number; x: number; y: number }> = [];
  private flagsEtag: string | null = null;
  private flagRules: Map<string, EdgeFlagRule> = new Map();
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private inFlightFlush: Promise<void> | null = null;
  private consecutiveFailures = 0;
  private backoffUntilMs = 0;

  private boundClickListener: ((ev: MouseEvent) => void) | null = null;
  private boundAutocaptureClickListener: ((ev: MouseEvent) => void) | null = null;
  private boundAutocaptureSubmitListener: ((ev: Event) => void) | null = null;
  private boundPopstateListener: (() => void) | null = null;
  private boundVisibilityListener: (() => void) | null = null;

  constructor(cfg: PulseWebConfig) {
    this.cfg = cfg;
    this.endpoint = cfg.endpointUrl.replace(/\/+$/, "");
    this.apiKey = cfg.apiKey;
    this.storageKey = `pulse_sdk_state_${xxh3_64(cfg.apiKey).toString(16)}`;
    this.flushBatchSize = cfg.flushBatchSize ?? 20;
    this.maxQueuedEvents = cfg.maxQueuedEvents ?? DEFAULT_MAX_QUEUED_EVENTS;
    this.propertyBlacklist = new Set(cfg.propertyBlacklist ?? []);
    this.replayTriggerSet = new Set(cfg.replayTriggerEvents ?? []);

    const now = Date.now();
    const loaded = this.loadPersistedState();
    if (loaded) {
      this.routingSeed = loaded.routingSeed;
      this.anonId = loaded.anonId;
      this.userId = loaded.userId;
      if (now - loaded.lastActivityMs > SESSION_TIMEOUT_MS) {
        this.sessionId = randomId("sess");
        this.exposuresSent = new Set();
      } else {
        this.sessionId = loaded.sessionId;
        this.exposuresSent = new Set(loaded.exposuresSent ?? []);
      }
      this.lastActivityMs = now;
      this.explicitOptSet = Boolean(loaded.explicitOptSet);
      this.optedOut = this.explicitOptSet ? Boolean(loaded.optedOut) : Boolean(cfg.optOutByDefault);
      this.superProps = loaded.superProps ?? {};
      this.firstTouchUtms = loaded.firstTouchUtms ?? {};
      this.lastTouchUtms = loaded.lastTouchUtms ?? {};
      this.groups = loaded.groups ?? {};
    } else {
      this.routingSeed = randomId("dev");
      this.anonId = randomId("anon");
      this.sessionId = randomId("sess");
      this.lastActivityMs = now;
      this.optedOut = Boolean(cfg.optOutByDefault);
    }

    this.captureUrlAttribution();
    this.initReplaySamplingState();
    this.persistState();

    const interval = cfg.flushIntervalMs ?? 5000;
    if (interval > 0) {
      this.flushTimer = setInterval(() => {
        void this.flush();
      }, interval);
    }

    if (typeof window !== "undefined" && typeof document !== "undefined") {
      // Unconditional visibilitychange listener (`P2-05`) so both events and replay ops flush on page hide!
      this.boundVisibilityListener = () => {
        if (document.visibilityState === "hidden") {
          this.flushBeacon();
        }
      };
      window.addEventListener("visibilitychange", this.boundVisibilityListener);

      if (cfg.enableWireframeReplay) {
        this.attachReplayClickListener();
      }
      if (cfg.autocapture) {
        this.attachAutocaptureListeners(cfg.autocapture);
      }
    }
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

    const emitPageview = () => {
      this.captureUrlAttribution();
      this.track("$pageview", collectDefaultContext());
    };

    if (cfg.pageviews !== false) {
      emitPageview();
      this.boundPopstateListener = emitPageview;
      window.addEventListener("popstate", this.boundPopstateListener);
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
        this.track("$autocapture", {
          $event_type: "click",
          tag: (interactive.tagName || "").toLowerCase(),
          masked_text: maskPiiText(interactive.textContent || "", explicitlyUnmasked),
          href: (interactive as HTMLAnchorElement).href || null,
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

  private loadPersistedState(): PersistedWebState | null {
    try {
      if (typeof localStorage === "undefined") return null;
      const raw = localStorage.getItem(this.storageKey);
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
      // Ignore storage quota / SecurityError in sandboxed contexts (`P2-04`).
    }
    return null;
  }

  private persistState(): void {
    try {
      if (typeof localStorage === "undefined") return;
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
      localStorage.setItem(this.storageKey, JSON.stringify(payload));
    } catch {
      // Ignore storage quota / SecurityError (`P2-04`).
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

  private touchSession(now: number): void {
    if (now > this.lastActivityMs && now - this.lastActivityMs > SESSION_TIMEOUT_MS) {
      // Seal pending replay ops under the outgoing session BEFORE rotating (`P3-05`)!
      this.sealPendingReplayOps();
      this.sessionId = randomId("sess");
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
  }

  // --- Consent & Opt-Out (`Gap 4`) ---

  public optOut(): void {
    this.optedOut = true;
    this.explicitOptSet = true;
    this.eventQueue = [];
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
    this.persistState();

    const bodyPayload: Record<string, unknown> = {
      anon_id: prevAnon,
      user_id: userId,
      routing_seed: this.routingSeed,
    };
    if (userProps) {
      bodyPayload.user_properties = userProps;
    }

    const res = await fetch(`${this.endpoint}/v1/identify`, {
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
      const res = await fetch(`${this.endpoint}/v1/engage`, {
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
    await fetch(`${this.endpoint}/v1/groups`, {
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
   * Resets identity on logout (`P1-08`), rotating `anonId` and `sessionId` so shared-device logins
   * never merge two distinct authenticated users.
   */
  public reset(): void {
    this.sealPendingReplayOps();
    this.userId = null;
    this.anonId = randomId("anon");
    this.sessionId = randomId("sess");
    this.replayOffsetKb = 0;
    this.recentClicks = [];
    this.exposuresSent.clear();
    this.timedEvents.clear();
    this.lastActivityMs = Date.now();
    this.persistState();
  }

  // --- Flags, Experiments & Exposure Tracking (`Gap 13`) ---

  public getVariantSync(flagKey: string): string | null {
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

  public async refreshFlags(): Promise<void> {
    try {
      const headers: Record<string, string> = {};
      if (this.flagsEtag) headers["If-None-Match"] = this.flagsEtag;
      const res = await fetch(`${this.endpoint}/v1/flags/${encodeURIComponent(this.apiKey)}`, {
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
    const now = Date.now();
    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    this.replayOps.push({
      op: "node_upsert",
      node_id: 99_000 + this.replayOps.length,
      parent_id: 1,
      kind: "error_banner",
      x: 16,
      y: 16,
      w: 340,
      h: 44,
      bg_rgba: 0xdc2626ff,
      masked_label: maskPiiText(message, true),
    });
    this.track("$error", { message: message.slice(0, 256) });
  }

  /**
   * Walks live DOM bounding boxes and emits a default-deny masked wireframe snapshot (`P3-01`).
   */
  public captureDomWireframeSnapshot(): void {
    if (this.optedOut || !this.isRecordingSession()) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;
    const now = Date.now();
    this.touchSession(now);

    const nodes: WireNode[] = [];
    let nextId = 1;
    const vpW = Math.min(65535, Math.max(320, window.innerWidth));
    const vpH = Math.min(65535, Math.max(480, window.innerHeight));

    const docUnmasked = Boolean(document.documentElement?.hasAttribute("data-pulse-unmask"));
    nodes.push({
      node_id: nextId++,
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
        node_id: nextId++,
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

    if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
    if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
    this.replayOps.push({
      op: "full_snapshot",
      width: vpW,
      height: vpH,
      nodes,
    });
  }

  private attachReplayClickListener(): void {
    this.boundClickListener = (ev: MouseEvent) => {
      if (this.optedOut || !this.isRecordingSession()) return;
      const now = Date.now();
      this.touchSession(now);
      const x = Math.max(0, Math.round(ev.clientX));
      const y = Math.max(0, Math.round(ev.clientY));

      this.recentClicks = this.recentClicks.filter((c) => now - c.ts <= 800);
      this.recentClicks.push({ ts: now, x, y });
      const cluster = this.recentClicks.filter((c) => {
        const dx = c.x - x;
        const dy = c.y - y;
        return dx * dx + dy * dy <= 1600;
      });

      const target = ev.target as HTMLElement | null;
      const interactive = Boolean(
        target?.closest("button, a, input, select, textarea, [role='button']"),
      );

      if (this.chunkStartTsMs === null) this.chunkStartTsMs = now;
      if (this.replayOpsSessionId === null) this.replayOpsSessionId = this.sessionId;
      const offset = Math.max(0, now - this.chunkStartTsMs);

      if (cluster.length >= 3) {
        // Clear click history immediately after rage-tap detection (`P3-05`)!
        this.recentClicks = [];
        this.replayOps.push({
          op: "pointer_interaction",
          ts_offset_ms: offset,
          x,
          y,
          kind: { type: "rage_tap", count: cluster.length },
        });
        this.track("$rage_tap", { x, y, tap_count: cluster.length });
      } else if (!interactive) {
        this.replayOps.push({
          op: "pointer_interaction",
          ts_offset_ms: offset,
          x,
          y,
          kind: { type: "dead_tap" },
        });
      } else {
        this.replayOps.push({
          op: "pointer_interaction",
          ts_offset_ms: offset,
          x,
          y,
          kind: { type: "tap" },
        });
      }
    };
    window.addEventListener("click", this.boundClickListener);
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
    this.sealPendingReplayOps();
    if (Date.now() < this.backoffUntilMs) {
      return;
    }

    while (this.replayQueue.length > 0) {
      const pkt = this.replayQueue.shift()!;
      try {
        const res = await fetch(`${this.endpoint}/v1/replay/chunk`, {
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
        const res = await fetch(`${this.endpoint}/v1/engage`, {
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
      const sentNow = Date.now();
      const wireBatch = batch.map((ev) => ({ ...ev, client_sent_ts_ms: sentNow }));
      try {
        const res = await fetch(`${this.endpoint}/v1/ingest`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-pulse-key": this.apiKey,
          },
          body: JSON.stringify({ events: wireBatch }),
        });
        if (res.ok) {
          this.consecutiveFailures = 0;
          this.backoffUntilMs = 0;
        } else if (res.status === 400 || res.status === 413 || res.status === 422) {
          // Drop malformed batch permanently.
        } else {
          this.enqueueEventsBounded(batch, true);
          this.recordFailureBackoff(res);
        }
      } catch {
        this.enqueueEventsBounded(batch, true);
        this.recordFailureBackoff();
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
      }).catch(() => {
        this.enqueueEventsBounded(batch, true);
      });
    }
  }

  public shutdown(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    if (typeof window !== "undefined") {
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
