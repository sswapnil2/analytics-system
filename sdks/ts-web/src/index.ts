/**
 * Pulse Analytics — Zero-Dependency Web & React Native SDK (`@pulse-analytics/ts-web`)
 *
 * Privacy-by-Design Invariant:
 * Never records screenshots, canvases, or unmasked inputs. Only structural wireframe bounding boxes
 * and strictly masked (`████`) PII placeholders leave the client.
 */

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
  | { op: "node_upsert" } & WireNode
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
}

interface EdgeFlagRule {
  key: string;
  enabled: boolean;
  rollout_bps: number;
  variants: Array<{ key: string; weight_bps: number }>;
}

const SESSION_TIMEOUT_MS = 30 * 60 * 1000;
const EMAIL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
const PHONE_RE = /(?:\+?91[-\s]?)?[6-9]\d{9}/g;
const PAN_RE = /\b[A-Z]{5}[0-9]{4}[A-Z]\b/g;

export function maskPiiText(raw: string, forceMaskAll = false): string {
  if (!raw) return forceMaskAll ? "████" : "";
  if (forceMaskAll) {
    const len = Math.min(16, Math.max(4, raw.trim().length));
    return "█".repeat(len);
  }
  return raw
    .replace(PAN_RE, "██████████")
    .replace(EMAIL_RE, "████████")
    .replace(PHONE_RE, "██████████");
}

function deterministicHash32(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function randomId(prefix: string): string {
  const r1 = Math.random().toString(36).slice(2, 10);
  const r2 = Date.now().toString(36);
  return `${prefix}_${r2}${r1}`;
}

export class PulseWebClient {
  private readonly endpoint: string;
  private readonly apiKey: string;
  private readonly flushBatchSize: number;
  private routingSeed: string;
  private anonId: string;
  private userId: string | null = null;
  private sessionId: string;
  private lastActivityMs: number;
  private replayOffsetKb = 0;

  private eventQueue: Array<Record<string, unknown>> = [];
  private replayOps: WireframeOp[] = [];
  private chunkStartTsMs: number | null = null;
  private recentClicks: Array<{ ts: number; x: number; y: number }> = [];
  private flagsEtag: string | null = null;
  private flagRules: Map<string, EdgeFlagRule> = new Map();
  private flushTimer: ReturnType<typeof setInterval> | null = null;

  constructor(cfg: PulseWebConfig) {
    this.endpoint = cfg.endpointUrl.replace(/\/+$/, "");
    this.apiKey = cfg.apiKey;
    this.flushBatchSize = cfg.flushBatchSize ?? 20;
    this.lastActivityMs = Date.now();
    this.routingSeed = randomId("dev");
    this.anonId = randomId("anon");
    this.sessionId = randomId("sess");

    const interval = cfg.flushIntervalMs ?? 5000;
    if (interval > 0) {
      this.flushTimer = setInterval(() => {
        void this.flush();
      }, interval);
    }

    if (cfg.enableWireframeReplay && typeof window !== "undefined" && typeof document !== "undefined") {
      this.attachDomListeners();
    }
  }

  public getDistinctId(): string {
    return this.userId ?? this.anonId;
  }

  public getSessionId(): string {
    return this.sessionId;
  }

  private touchSession(now: number): void {
    if (now - this.lastActivityMs > SESSION_TIMEOUT_MS) {
      this.sessionId = randomId("sess");
      this.replayOffsetKb = 0;
    }
    this.lastActivityMs = now;
  }

  public track(eventName: string, props: EventProperties = {}): void {
    const now = Date.now();
    this.touchSession(now);
    const distinctId = this.getDistinctId();

    this.eventQueue.push({
      event_uuid: `${distinctId}:${this.sessionId}:${eventName}:${now}:${this.eventQueue.length}`,
      routing_seed: this.routingSeed,
      distinct_id: distinctId,
      session_id: this.sessionId,
      event_name: eventName,
      client_event_ts_ms: now,
      client_sent_ts_ms: now,
      props,
      replay_offset_kb: this.replayOffsetKb,
    });

    if (this.eventQueue.length >= this.flushBatchSize) {
      void this.flush();
    }
  }

  public async identify(userId: string): Promise<void> {
    const now = Date.now();
    this.touchSession(now);
    const prevAnon = this.anonId;
    this.userId = userId;

    await fetch(`${this.endpoint}/v1/identify`, {
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
  }

  public async getVariant(flagKey: string): Promise<string | null> {
    if (this.flagRules.size === 0) {
      await this.refreshFlags();
    }
    const rule = this.flagRules.get(flagKey);
    if (!rule || !rule.enabled || rule.rollout_bps <= 0) return null;

    const did = this.getDistinctId();
    const slot = deterministicHash32(`${flagKey}:${did}`) % 10000;
    if (slot >= rule.rollout_bps) return null;
    if (rule.variants.length === 0) return "on";

    const totalWeight = rule.variants.reduce((acc, v) => acc + v.weight_bps, 0);
    if (totalWeight <= 0) return rule.variants[0]?.key ?? null;
    const vSlot = deterministicHash32(`var:${flagKey}:${did}`) % totalWeight;
    let acc = 0;
    for (const v of rule.variants) {
      acc += v.weight_bps;
      if (vSlot < acc) return v.key;
    }
    return rule.variants[rule.variants.length - 1]?.key ?? null;
  }

  public async refreshFlags(): Promise<void> {
    const headers: Record<string, string> = {};
    if (this.flagsEtag) headers["If-None-Match"] = this.flagsEtag;
    const res = await fetch(`${this.endpoint}/v1/flags/${encodeURIComponent(this.apiKey)}`, {
      headers,
    });
    if (res.status === 304) return;
    if (!res.ok) return;
    const etag = res.headers.get("etag");
    if (etag) this.flagsEtag = etag;
    const body = (await res.json()) as { flags: EdgeFlagRule[] };
    this.flagRules.clear();
    for (const f of body.flags) {
      this.flagRules.set(f.key, f);
    }
  }

  /**
   * Walks the live DOM bounding boxes and emits a privacy-masked structural wireframe snapshot.
   */
  public captureDomWireframeSnapshot(): void {
    if (typeof window === "undefined" || typeof document === "undefined") return;
    const now = Date.now();
    this.touchSession(now);

    const nodes: WireNode[] = [];
    let nextId = 1;
    const vpW = Math.min(65535, Math.max(320, window.innerWidth));
    const vpH = Math.min(65535, Math.max(480, window.innerHeight));

    nodes.push({
      node_id: nextId++,
      parent_id: 0,
      kind: "screen",
      x: 0,
      y: 0,
      w: vpW,
      h: vpH,
      bg_rgba: 0x090d16ff,
      masked_label: document.title || "Screen",
    });

    const elements = document.body ? Array.from(document.body.querySelectorAll("*")).slice(0, 120) : [];
    for (const el of elements) {
      const rect = el.getBoundingClientRect();
      if (rect.width < 4 || rect.height < 4) continue;
      if (rect.bottom < 0 || rect.top > vpH) continue;

      const tag = el.tagName.toLowerCase();
      const hasMaskAttr = el.hasAttribute("data-pulse-mask");
      let kind: NodeKind = "container";
      let label = "";

      if (tag === "input" || tag === "textarea") {
        kind = "masked_input";
        const inp = el as HTMLInputElement;
        const placeholder = inp.placeholder ? `${inp.placeholder}: ` : "";
        label = `${placeholder}${maskPiiText(inp.value || "secret", true)}`;
      } else if (tag === "button" || el.getAttribute("role") === "button") {
        kind = "button";
        label = maskPiiText((el.textContent || "").trim().slice(0, 40), hasMaskAttr);
      } else if (tag === "img" || tag === "svg") {
        kind = "image_placeholder";
        label = "media";
      } else if (el.getAttribute("role") === "alert") {
        kind = "error_banner";
        label = maskPiiText((el.textContent || "").trim().slice(0, 64), hasMaskAttr);
      } else if (el.children.length === 0 && (el.textContent || "").trim().length > 0) {
        kind = "text";
        label = maskPiiText((el.textContent || "").trim().slice(0, 64), hasMaskAttr);
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
    this.replayOps.push({
      op: "full_snapshot",
      width: vpW,
      height: vpH,
      nodes,
    });
  }

  private attachDomListeners(): void {
    window.addEventListener("click", (ev: MouseEvent) => {
      const now = Date.now();
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
      const offset = Math.max(0, now - this.chunkStartTsMs);

      if (cluster.length >= 3) {
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
    });

    window.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "hidden") {
        this.flushBeacon();
      }
    });
  }

  public async flush(): Promise<void> {
    if (this.replayOps.length > 0) {
      const ops = this.replayOps.splice(0, this.replayOps.length);
      const startTs = this.chunkStartTsMs ?? Date.now();
      this.chunkStartTsMs = null;
      const res = await fetch(`${this.endpoint}/v1/replay/chunk`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify({
          project_id: "",
          session_id: this.sessionId,
          chunk_start_ts_ms: startTs,
          ops,
        }),
      });
      if (res.ok) {
        const body = (await res.json()) as { replay_offset_kb?: number };
        if (typeof body.replay_offset_kb === "number") {
          this.replayOffsetKb = body.replay_offset_kb;
        }
      }
    }

    if (this.eventQueue.length > 0) {
      const batch = this.eventQueue.splice(0, this.eventQueue.length);
      const sentNow = Date.now();
      for (const ev of batch) {
        ev.client_sent_ts_ms = sentNow;
      }
      await fetch(`${this.endpoint}/v1/ingest`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-pulse-key": this.apiKey,
        },
        body: JSON.stringify({ events: batch }),
      });
    }
  }

  public flushBeacon(): void {
    if (this.eventQueue.length === 0 || typeof navigator === "undefined" || !navigator.sendBeacon) {
      return;
    }
    const batch = this.eventQueue.splice(0, this.eventQueue.length);
    const blob = new Blob([JSON.stringify({ events: batch })], { type: "application/json" });
    navigator.sendBeacon(
      `${this.endpoint}/v1/ingest?api_key=${encodeURIComponent(this.apiKey)}`,
      blob,
    );
  }

  public shutdown(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    void this.flush();
  }
}
