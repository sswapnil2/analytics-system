import React, { useEffect, useMemo, useRef, useState } from "react";
import { Pause, Play, RotateCcw, ShieldCheck, VideoOff, WifiOff, Zap } from "lucide-react";
import { api, ReplayChunk, WireNode } from "../api/client";
import { errorMessage, formatDuration, isNotFound } from "../lib/format";
import { EmptyState, ErrorNotice, Spinner, btn } from "./ui";

interface TimelineAction {
  absOffsetMs: number;
  op: ReplayChunk["ops"][number];
}

/** Text colour that stays readable on a recorded button colour (light buttons get dark text). */
function onColor(bgRgba: number): string {
  const r = (bgRgba >>> 24) & 0xff;
  const g = (bgRgba >>> 16) & 0xff;
  const b = (bgRgba >>> 8) & 0xff;
  const a = (bgRgba & 0xff) / 255;
  if (a < 0.5) return "rgb(var(--ink))";
  return 0.299 * r + 0.587 * g + 0.114 * b > 160 ? "#141A17" : "#FFFFFF";
}

function rgbaHex(bgRgba: number): string {
  const r = (bgRgba >>> 24) & 0xff;
  const g = (bgRgba >>> 16) & 0xff;
  const b = (bgRgba >>> 8) & 0xff;
  const a = (bgRgba & 0xff) / 255;
  if (a <= 0.01) return "transparent";
  return `rgba(${r}, ${g}, ${b}, ${a.toFixed(2)})`;
}

function clock(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

// ---------------------------------------------------------------------------
// Loader: fetches one session's recording and owns every non-happy state.
// ---------------------------------------------------------------------------

type ReplayState =
  | { kind: "loading" }
  | { kind: "ready"; chunks: ReplayChunk[] }
  | { kind: "missing" }
  | { kind: "error"; message: string };

export const SessionReplay: React.FC<{
  projectId: string;
  sessionId: string;
  offsetKb?: number;
}> = ({ projectId, sessionId, offsetKb = 0 }) => {
  const [state, setState] = useState<ReplayState>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    api
      .getReplay(projectId, sessionId, offsetKb)
      .then((res) => {
        if (cancelled) return;
        setState(res.chunks.length > 0 ? { kind: "ready", chunks: res.chunks } : { kind: "missing" });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setState(isNotFound(err) ? { kind: "missing" } : { kind: "error", message: errorMessage(err) });
      });
    return () => {
      cancelled = true;
    };
  }, [projectId, sessionId, offsetKb, attempt]);

  if (state.kind === "loading") {
    return (
      <div className="flex h-64 items-center justify-center">
        <Spinner label="Loading recording" />
      </div>
    );
  }
  if (state.kind === "missing") {
    return (
      <EmptyState
        icon={VideoOff}
        title="No recording for this session"
        body="Pulse records a session only when the app has screen recording turned on in its SDK settings. The events for this session are still counted in every report."
      />
    );
  }
  if (state.kind === "error") {
    return (
      <ErrorNotice
        message={`Couldn't load this recording: ${state.message}`}
        onRetry={() => setAttempt((n) => n + 1)}
      />
    );
  }
  return <WireframePlayer sessionId={sessionId} chunks={state.chunks} />;
};

// ---------------------------------------------------------------------------
// Player
// ---------------------------------------------------------------------------

export const WireframePlayer: React.FC<{ sessionId: string; chunks: ReplayChunk[] }> = ({
  sessionId,
  chunks,
}) => {
  const [playing, setPlaying] = useState<boolean>(true);
  const [speed, setSpeed] = useState<1 | 2 | 4>(1);
  const [cursorMs, setCursorMs] = useState<number>(0);
  const wrapRef = useRef<HTMLDivElement>(null);
  const [fitW, setFitW] = useState<number>(Infinity);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([entry]) => setFitW(entry.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const { timeline, durationMs, viewport, moments } = useMemo(() => {
    const items: TimelineAction[] = [];
    const found: Moment[] = [];
    let vp = { w: 390, h: 844 };
    if (chunks.length === 0) {
      return { timeline: items, durationMs: 1000, viewport: vp, moments: found };
    }
    const baseStart = chunks[0].chunk_start_ts_ms;

    for (const chunk of chunks) {
      const chunkDelta = Math.max(0, chunk.chunk_start_ts_ms - baseStart);
      let runningOffset = chunkDelta;
      for (const op of chunk.ops) {
        if (op.op === "full_snapshot") {
          vp = { w: op.width || 390, h: op.height || 844 };
          items.push({ absOffsetMs: chunkDelta, op });
        } else if (op.op === "node_upsert" || op.op === "node_remove") {
          items.push({ absOffsetMs: runningOffset, op });
        } else if (op.op === "pointer_interaction") {
          const t = chunkDelta + op.ts_offset_ms;
          runningOffset = Math.max(runningOffset, t);
          items.push({ absOffsetMs: t, op });
          if (op.kind.type === "rage_tap") found.push({ ts: t, kind: "rage", count: op.kind.count });
          if (op.kind.type === "dead_tap") found.push({ ts: t, kind: "dead" });
        } else if (op.op === "network_breadcrumb") {
          const t = chunkDelta + op.ts_offset_ms;
          runningOffset = Math.max(runningOffset, t);
          items.push({ absOffsetMs: t, op });
          found.push({
            ts: t,
            kind: op.status_code >= 400 ? "net-error" : "net",
            method: op.method,
            path: op.path,
            status: op.status_code,
            dur: op.duration_ms,
          });
        }
      }
    }

    items.sort((a, b) => a.absOffsetMs - b.absOffsetMs);
    found.sort((a, b) => a.ts - b.ts);
    const maxTs = Math.max(2000, items.length > 0 ? items[items.length - 1].absOffsetMs + 800 : 2000);
    return { timeline: items, durationMs: maxTs, viewport: vp, moments: found };
  }, [chunks]);

  useEffect(() => {
    setCursorMs(0);
    setPlaying(!window.matchMedia?.("(prefers-reduced-motion: reduce)").matches);
  }, [sessionId, chunks]);

  useEffect(() => {
    if (!playing) return;
    const step = 50;
    const id = setInterval(() => {
      setCursorMs((prev) => {
        const next = prev + step * speed;
        if (next >= durationMs) {
          setPlaying(false);
          return durationMs;
        }
        return next;
      });
    }, step);
    return () => clearInterval(id);
  }, [playing, speed, durationMs]);

  const { activeNodes, activePointers } = useMemo(() => {
    const nodeMap = new Map<number, WireNode>();
    const pointers: Array<{
      x: number;
      y: number;
      kind: "tap" | "rage_tap" | "dead_tap" | "scroll";
      count?: number;
    }> = [];
    for (const item of timeline) {
      if (item.absOffsetMs > cursorMs) break;
      const op = item.op;
      if (op.op === "full_snapshot") {
        nodeMap.clear();
        for (const n of op.nodes) nodeMap.set(n.node_id, n);
      } else if (op.op === "node_upsert") {
        nodeMap.set(op.node_id, op);
      } else if (op.op === "node_remove") {
        nodeMap.delete(op.node_id);
      } else if (op.op === "pointer_interaction") {
        const age = cursorMs - item.absOffsetMs;
        if (age >= 0 && age <= 950) {
          pointers.push({
            x: op.x,
            y: op.y,
            kind: op.kind.type,
            count: op.kind.type === "rage_tap" ? op.kind.count : undefined,
          });
        }
      }
    }
    return { activeNodes: Array.from(nodeMap.values()), activePointers: pointers };
  }, [timeline, cursorMs]);

  const seek = (ts: number) => {
    setCursorMs(ts);
    setPlaying(false);
  };

  // Web recordings are landscape (e.g. 1024×768): show them in a browser-style frame stacked above
  // the controls, and shrink any recording to fit the available width instead of clipping it.
  const landscape = viewport.w > viewport.h;
  const scale = Math.min(0.72, (fitW - 12) / viewport.w);
  const frameW = Math.round(viewport.w * scale);
  const frameH = Math.round(viewport.h * scale);

  return (
    <div
      ref={wrapRef}
      className={`flex flex-col items-center gap-6 ${landscape ? "" : "xl:flex-row xl:items-start"}`}
    >
      {/* Device */}
      <figure className="flex shrink-0 flex-col items-center gap-2">
        <div
          className={`relative overflow-hidden border-[6px] border-ink/85 bg-panel ${
            landscape ? "rounded-lg" : "rounded-[30px]"
          }`}
          style={{ width: frameW + 12, height: frameH + 12 }}
          aria-label="Screen recording"
          role="img"
        >
          {!landscape && (
            <div className="absolute left-1/2 top-1.5 z-30 h-3.5 w-20 -translate-x-1/2 rounded-full bg-ink/85" />
          )}
          <div className="relative h-full w-full">
            {activeNodes.map((n) => {
              const isScreen = n.kind === "screen";
              const isBtn = n.kind === "button";
              const isMasked = n.kind === "masked_input";
              const isErr = n.kind === "error_banner";
              return (
                <div
                  key={n.node_id}
                  style={{
                    position: "absolute",
                    left: Math.round(n.x * scale),
                    top: Math.round(n.y * scale),
                    width: Math.round(n.w * scale),
                    height: Math.round(n.h * scale),
                    backgroundColor: isScreen ? undefined : rgbaHex(n.bg_rgba),
                    color: isBtn && !isErr && !isScreen ? onColor(n.bg_rgba) : undefined,
                  }}
                  className={
                    isScreen
                      ? "z-0 bg-panel"
                      : isErr
                        ? "z-20 flex items-center rounded-lg border border-danger bg-danger-soft px-2 text-[11px] font-medium text-danger"
                        : isBtn
                          ? "z-10 flex items-center justify-center rounded-lg text-xs font-semibold"
                          : isMasked
                            ? "z-10 flex items-center rounded-lg border border-line-strong bg-sunken px-2 font-mono text-xs text-muted"
                            : "z-10 flex items-center px-1 text-[11px] text-ink-2"
                  }
                >
                  {isScreen ? (
                    <div className="absolute left-3 right-3 top-6 border-b border-line pb-1.5 text-xs font-semibold text-ink">
                      {n.masked_label}
                    </div>
                  ) : (
                    <span className="truncate">{n.masked_label}</span>
                  )}
                </div>
              );
            })}

            {activePointers.map((pt, idx) => {
              const isRage = pt.kind === "rage_tap";
              const isDead = pt.kind === "dead_tap";
              return (
                <div
                  key={idx}
                  style={{
                    position: "absolute",
                    left: Math.round(pt.x * scale) - 16,
                    top: Math.round(pt.y * scale) - 16,
                  }}
                  className="pointer-events-none z-40"
                >
                  <div
                    className={`h-8 w-8 rounded-full border-2 motion-safe:animate-ping ${
                      isRage ? "border-danger bg-danger/40" : isDead ? "border-warn bg-warn/40" : "border-brand bg-brand/30"
                    }`}
                  />
                  {isRage && (
                    <div className="absolute -left-3 -top-6 whitespace-nowrap rounded bg-danger px-1.5 py-0.5 text-[10px] font-semibold text-on-brand">
                      Rage tap ×{pt.count ?? 4}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
        <figcaption className="flex items-center gap-1.5 text-xs text-muted">
          <ShieldCheck className="h-3.5 w-3.5 text-ok" />
          Wireframe only. Typed values are masked on the device.
        </figcaption>
      </figure>

      {/* Controls and moments */}
      <div className="w-full min-w-0 flex-1 space-y-5">
        <div className="space-y-2">
          <div className="relative pt-4">
            {moments
              .filter((m) => m.kind === "rage" || m.kind === "net-error")
              .map((m, idx) => (
                <button
                  key={idx}
                  type="button"
                  onClick={() => seek(m.ts)}
                  aria-label={`Jump to ${momentTitle(m)} at ${clock(m.ts)}`}
                  title={`${momentTitle(m)} at ${clock(m.ts)}`}
                  style={{ left: `${Math.min(98, Math.max(2, (m.ts / durationMs) * 100))}%` }}
                  className={`absolute top-0 h-3 w-1.5 -translate-x-1/2 rounded-sm ${
                    m.kind === "rage" ? "bg-danger" : "bg-warn"
                  }`}
                />
              ))}
            <input
              type="range"
              min={0}
              max={durationMs}
              value={cursorMs}
              onChange={(e) => seek(Number(e.target.value))}
              className="w-full cursor-pointer accent-brand"
              aria-label="Playback position"
            />
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => {
                  if (cursorMs >= durationMs) setCursorMs(0);
                  setPlaying((p) => !p);
                }}
                className={btn.primary}
              >
                {playing ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                {playing ? "Pause" : "Play"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setCursorMs(0);
                  setPlaying(true);
                }}
                className={btn.secondary}
                aria-label="Restart"
                title="Restart"
              >
                <RotateCcw className="h-4 w-4" />
              </button>
              <div className="ml-1 flex rounded-lg border border-line-strong p-0.5" role="group" aria-label="Playback speed">
                {([1, 2, 4] as const).map((sp) => (
                  <button
                    key={sp}
                    type="button"
                    onClick={() => setSpeed(sp)}
                    aria-pressed={speed === sp}
                    className={`rounded px-2 py-0.5 text-xs tabular ${
                      speed === sp ? "bg-brand text-on-brand" : "text-muted hover:text-ink"
                    }`}
                  >
                    {sp}×
                  </button>
                ))}
              </div>
            </div>
            <span className="text-sm text-muted tabular">
              {clock(cursorMs)} / {clock(durationMs)}
            </span>
          </div>
        </div>

        <div>
          <h3 className="mb-2 text-sm font-semibold text-ink">What happened</h3>
          {moments.length === 0 ? (
            <p className="text-sm text-muted">No taps of note or network errors in this recording.</p>
          ) : (
            <ol className="divide-y divide-line rounded-lg border border-line">
              {moments.map((m, idx) => (
                <li key={idx}>
                  <button
                    type="button"
                    onClick={() => seek(m.ts)}
                    className="flex w-full items-start gap-3 px-3 py-2 text-left text-sm hover:bg-hover"
                  >
                    <span className="w-10 shrink-0 text-muted tabular">{clock(m.ts)}</span>
                    {m.kind === "rage" && <Zap className="mt-0.5 h-4 w-4 shrink-0 text-danger" />}
                    {m.kind === "dead" && <Zap className="mt-0.5 h-4 w-4 shrink-0 text-warn" />}
                    {(m.kind === "net" || m.kind === "net-error") && (
                      <WifiOff
                        className={`mt-0.5 h-4 w-4 shrink-0 ${m.kind === "net-error" ? "text-warn" : "text-faint"}`}
                      />
                    )}
                    <span className="min-w-0 flex-1">
                      <span className="text-ink">{momentTitle(m)}</span>
                      {m.path && (
                        <span className="block truncate font-mono text-xs text-muted">
                          {m.method} {m.path}
                        </span>
                      )}
                    </span>
                    {m.dur !== undefined && (
                      <span className="shrink-0 text-xs text-muted tabular">{formatDuration(m.dur)}</span>
                    )}
                  </button>
                </li>
              ))}
            </ol>
          )}
        </div>
      </div>
    </div>
  );
};

interface Moment {
  ts: number;
  kind: "rage" | "dead" | "net" | "net-error";
  count?: number;
  method?: string;
  path?: string;
  status?: number;
  dur?: number;
}

function momentTitle(m: Moment): string {
  switch (m.kind) {
    case "rage":
      return `Rage tap: ${m.count ?? "several"} quick taps on one spot`;
    case "dead":
      return "Tap with no response";
    case "net-error":
      return `Request failed (${m.status})`;
    case "net":
      return `Request ${m.status}`;
  }
}
