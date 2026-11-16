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

export interface PulseWebConfig {
  endpointUrl: string;
  apiKey: string;
  flushBatchSize?: number;
  flushIntervalMs?: number;
  enableWireframeReplay?: boolean;
  maxQueuedEvents?: number;
}

export interface EdgeFlagRule {
  key: string;
  enabled: boolean;
  rollout_bps: number;
  targeting_cohort_id?: string;
  cohort_members?: string[];
  variants: Array<{ key: string; weight_bps: number }>;
}

interface PersistedWebState {
  routingSeed: string;
  anonId: string;
  userId: string | null;
  sessionId: string;
  lastActivityMs: number;
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

export class PulseWebClient {
  private readonly endpoint: string;
  private readonly apiKey: string;
  private readonly storageKey: string;
  private readonly flushBatchSize: number;
  private readonly maxQueuedEvents: number;

  private routingSeed: string;
  private anonId: string;
  private userId: string | null = null;
  private sessionId: string;
  private lastActivityMs: number;
  private seqCounter = 0;
  private replayOffsetKb = 0;

  private eventQueue: Array<Record<string, unknown>> = [];
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
  private boundVisibilityListener: (() => void) | null = null;

  constructor(cfg: PulseWebConfig) {
    this.endpoint = cfg.endpointUrl.replace(/\/+$/, "");
    this.apiKey = cfg.apiKey;
    this.storageKey = `pulse_sdk_state_${xxh3_64(cfg.apiKey).toString(16)}`;
    this.flushBatchSize = cfg.flushBatchSize ?? 20;
    this.maxQueuedEvents = cfg.maxQueuedEvents ?? DEFAULT_MAX_QUEUED_EVENTS;

    const now = Date.now();
    const loaded = this.loadPersistedState();
    if (loaded) {
      this.routingSeed = loaded.routingSeed;
      this.anonId = loaded.anonId;
      this.userId = loaded.userId;
      if (now - loaded.lastActivityMs > SESSION_TIMEOUT_MS) {
        this.sessionId = randomId("sess");
      } else {
        this.sessionId = loaded.sessionId;
      }
      this.lastActivityMs = now;
    } else {
      this.routingSeed = randomId("dev");
      this.anonId = randomId("anon");
      this.sessionId = randomId("sess");
      this.lastActivityMs = now;
    }
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

  public track(eventName: string, props: EventProperties = {}): void {
    const now = Date.now();
    this.touchSession(now);
    const distinctId = this.getDistinctId();
    this.seqCounter += 1;

    this.enqueueEventsBounded([
      {
        event_uuid: `${distinctId}:${this.sessionId}:${eventName}:${now}:${this.seqCounter}`,
        routing_seed: this.routingSeed,
        distinct_id: distinctId,
        session_id: this.sessionId,
        event_name: eventName,
        client_event_ts_ms: now,
        client_sent_ts_ms: 0,
        props,
        replay_offset_kb: this.replayOffsetKb,
      },
    ]);

    if (this.eventQueue.length >= this.flushBatchSize) {
      void this.flush();
    }
  }

  public async identify(userId: string): Promise<void> {
    const now = Date.now();
    this.touchSession(now);
    const prevAnon = this.anonId;
    this.userId = userId;
    this.persistState();

    const res = await fetch(`${this.endpoint}/v1/identify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-pulse-key": this.apiKey,
      },
      body: JSON.stringify({
        anon_id: prevAnon,
        user_id: userId,
        routing_seed: this.routingSeed,
      }),
    });
    if (!res.ok) {
      throw new Error(`identify failed with HTTP ${res.status}`);
    }
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
    this.lastActivityMs = Date.now();
    this.persistState();
  }

  public async getVariant(flagKey: string): Promise<string | null> {
    if (this.flagRules.size === 0) {
      await this.refreshFlags();
    }
    const rule = this.flagRules.get(flagKey);
    if (!rule) return null;
    return evaluateEdgeFlag(rule, this.getDistinctId());
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

  /**
   * Walks live DOM bounding boxes and emits a default-deny masked wireframe snapshot (`P3-01`).
   */
  public captureDomWireframeSnapshot(): void {
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
      const explicitlyUnmasked = !isEditable && Boolean(el.closest("[data-pulse-unmask]"));

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

  private recordFailureBackoff(): void {
    const exp = Math.min(6, this.consecutiveFailures);
    this.consecutiveFailures += 1;
    this.backoffUntilMs = Date.now() + Math.min(60_000, 1_000 * Math.pow(2, exp));
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
          this.recordFailureBackoff();
          return;
        }
      } catch {
        this.replayQueue.unshift(pkt);
        this.recordFailureBackoff();
        return;
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
          this.recordFailureBackoff();
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
      if (this.boundVisibilityListener) {
        window.removeEventListener("visibilitychange", this.boundVisibilityListener);
        this.boundVisibilityListener = null;
      }
    }
    void this.flush();
  }
}
