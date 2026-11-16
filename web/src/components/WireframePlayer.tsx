import React, { useEffect, useMemo, useState } from "react";
import { Pause, Play, RotateCcw, ShieldCheck, WifiOff, Zap } from "lucide-react";
import { ReplayChunk, WireNode } from "../api/client";

interface Props {
  sessionId: string;
  chunks: ReplayChunk[];
}

interface TimelineAction {
  absOffsetMs: number;
  op: ReplayChunk["ops"][number];
}

function rgbaHex(bgRgba: number): string {
  const r = (bgRgba >>> 24) & 0xff;
  const g = (bgRgba >>> 16) & 0xff;
  const b = (bgRgba >>> 8) & 0xff;
  const a = (bgRgba & 0xff) / 255;
  if (a <= 0.01) return "transparent";
  return `rgba(${r}, ${g}, ${b}, ${a.toFixed(2)})`;
}

export const WireframePlayer: React.FC<Props> = ({ sessionId, chunks }) => {
  const [playing, setPlaying] = useState<boolean>(true);
  const [speed, setSpeed] = useState<1 | 2 | 4>(1);
  const [cursorMs, setCursorMs] = useState<number>(0);

  const { timeline, durationMs, viewport, networkBreadcrumbs, rageMarkers } = useMemo(() => {
    if (chunks.length === 0) {
      return {
        timeline: [] as TimelineAction[],
        durationMs: 1000,
        viewport: { w: 390, h: 844 },
        networkBreadcrumbs: [] as Array<{
          ts: number;
          method: string;
          path: string;
          status: number;
          dur: number;
        }>,
        rageMarkers: [] as Array<{ ts: number; count: number }>,
      };
    }

    const baseStart = chunks[0].chunk_start_ts_ms;
    const items: TimelineAction[] = [];
    let vp = { w: 390, h: 844 };
    const nets: Array<{ ts: number; method: string; path: string; status: number; dur: number }> =
      [];
    const rages: Array<{ ts: number; count: number }> = [];

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
          if (op.kind.type === "rage_tap") {
            rages.push({ ts: t, count: op.kind.count });
          }
        } else if (op.op === "network_breadcrumb") {
          const t = chunkDelta + op.ts_offset_ms;
          runningOffset = Math.max(runningOffset, t);
          items.push({ absOffsetMs: t, op });
          nets.push({
            ts: t,
            method: op.method,
            path: op.path,
            status: op.status_code,
            dur: op.duration_ms,
          });
        }
      }
    }

    items.sort((a, b) => a.absOffsetMs - b.absOffsetMs);
    const maxTs = Math.max(
      2000,
      items.length > 0 ? items[items.length - 1].absOffsetMs + 800 : 2000,
    );

    return {
      timeline: items,
      durationMs: maxTs,
      viewport: vp,
      networkBreadcrumbs: nets,
      rageMarkers: rages,
    };
  }, [chunks]);

  useEffect(() => {
    setCursorMs(0);
    setPlaying(true);
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

  // Reconstruct wireframe DOM state and active pointer interactions at `cursorMs`
  const { activeNodes, activePointers } = useMemo(() => {
    const nodeMap = new Map<number, WireNode>();
    const pointers: Array<{
      x: number;
      y: number;
      ageMs: number;
      kind: "tap" | "rage_tap" | "dead_tap" | "scroll";
      count?: number;
    }> = [];

    for (const item of timeline) {
      if (item.absOffsetMs > cursorMs) break;
      const op = item.op;
      if (op.op === "full_snapshot") {
        nodeMap.clear();
        for (const n of op.nodes) {
          nodeMap.set(n.node_id, n);
        }
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
            ageMs: age,
            kind: op.kind.type,
            count: op.kind.type === "rage_tap" ? op.kind.count : undefined,
          });
        }
      }
    }

    return {
      activeNodes: Array.from(nodeMap.values()),
      activePointers: pointers,
    };
  }, [timeline, cursorMs]);

  const scale = 0.72;
  const frameW = Math.round(viewport.w * scale);
  const frameH = Math.round(viewport.h * scale);

  return (
    <div className="flex flex-col lg:flex-row gap-5 items-start">
      {/* Phone Wireframe Viewport */}
      <div className="flex flex-col items-center">
        <div className="mb-2 flex items-center gap-2 text-xs text-emerald-400 bg-emerald-950/50 border border-emerald-800/50 px-2.5 py-1 rounded-full">
          <ShieldCheck className="w-3.5 h-3.5" />
          <span>Privacy-by-Design Binary Wireframe • Zero Pixels • Client-Masked PII</span>
        </div>

        <div
          className="relative rounded-[32px] border-[6px] border-slate-800 bg-[#070b13] shadow-2xl overflow-hidden"
          style={{ width: frameW + 12, height: frameH + 12 }}
        >
          {/* Phone Notch */}
          <div className="absolute top-1.5 left-1/2 -translate-x-1/2 w-24 h-3.5 bg-slate-900 rounded-full z-30 border border-slate-800" />

          <div className="relative w-full h-full">
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
                    backgroundColor: rgbaHex(n.bg_rgba),
                  }}
                  className={`transition-all duration-150 ${
                    isScreen
                      ? "z-0"
                      : isErr
                        ? "z-20 rounded-lg border border-red-400/80 px-2.5 py-1.5 flex items-center text-[11px] font-medium text-white shadow-lg shadow-red-950/60"
                        : isBtn
                          ? "z-10 rounded-lg border border-blue-400/40 flex items-center justify-center text-xs font-semibold text-white shadow-md"
                          : isMasked
                            ? "z-10 rounded-lg border border-amber-500/40 px-2.5 flex items-center font-mono text-xs text-amber-200 tracking-wider"
                            : "z-10 rounded-lg px-2 py-1 flex items-center text-[11px] text-slate-300"
                  }`}
                >
                  {isScreen ? (
                    <div className="absolute top-6 left-3 right-3 text-xs font-semibold text-slate-200 border-b border-slate-800/80 pb-1.5">
                      {n.masked_label}
                    </div>
                  ) : (
                    <span className="truncate">{n.masked_label}</span>
                  )}
                </div>
              );
            })}

            {/* Pointer & Rage Tap Overlays */}
            {activePointers.map((pt, idx) => {
              const isRage = pt.kind === "rage_tap";
              const isDead = pt.kind === "dead_tap";
              return (
                <div
                  key={idx}
                  style={{
                    position: "absolute",
                    left: Math.round(pt.x * scale) - 18,
                    top: Math.round(pt.y * scale) - 18,
                  }}
                  className="z-40 pointer-events-none"
                >
                  <div
                    className={`w-9 h-9 rounded-full flex items-center justify-center animate-ping ${
                      isRage
                        ? "bg-red-500/60 border-2 border-red-400"
                        : isDead
                          ? "bg-amber-500/50 border border-amber-300"
                          : "bg-blue-400/50 border border-blue-300"
                    }`}
                  />
                  {isRage && (
                    <div className="absolute -top-6 -left-4 flex items-center gap-1 bg-red-600 text-white text-[10px] font-bold px-1.5 py-0.5 rounded shadow-lg whitespace-nowrap">
                      <Zap className="w-3 h-3 fill-yellow-300 text-yellow-300" />
                      {pt.count ?? 4}x RAGE TAP
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Controls, Scrubber & Breadcrumb Inspector */}
      <div className="flex-1 w-full bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-xs text-slate-400">Wireframe Session Stream</div>
            <div className="font-mono text-sm text-slate-100">{sessionId}</div>
          </div>
          <div className="flex items-center gap-1.5">
            {([1, 2, 4] as const).map((sp) => (
              <button
                key={sp}
                onClick={() => setSpeed(sp)}
                className={`px-2.5 py-1 rounded text-xs font-mono transition ${
                  speed === sp
                    ? "bg-blue-600 text-white"
                    : "bg-slate-800/80 text-slate-400 hover:text-slate-200"
                }`}
              >
                {sp}x
              </button>
            ))}
          </div>
        </div>

        {/* Scrubber with Red Lightning Rage Tap Markers */}
        <div className="space-y-2">
          <div className="relative h-7 flex items-center">
            {rageMarkers.map((rm, idx) => {
              const pct = Math.min(98, Math.max(2, (rm.ts / durationMs) * 100));
              return (
                <button
                  key={idx}
                  onClick={() => {
                    setCursorMs(rm.ts);
                    setPlaying(false);
                  }}
                  title={`Rage Tap (${rm.count}x) at ${(rm.ts / 1000).toFixed(1)}s`}
                  style={{ left: `${pct}%` }}
                  className="absolute -top-2 -translate-x-1/2 z-20 text-red-400 hover:scale-125 transition"
                >
                  <Zap className="w-4 h-4 fill-red-500 text-red-300" />
                </button>
              );
            })}
            <input
              type="range"
              min={0}
              max={durationMs}
              value={cursorMs}
              onChange={(e) => {
                setCursorMs(Number(e.target.value));
                setPlaying(false);
              }}
              className="w-full accent-blue-500 cursor-pointer"
            />
          </div>

          <div className="flex items-center justify-between text-xs">
            <div className="flex items-center gap-2">
              <button
                onClick={() => {
                  if (cursorMs >= durationMs) setCursorMs(0);
                  setPlaying((p) => !p);
                }}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium"
              >
                {playing ? <Pause className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
                {playing ? "Pause" : "Play"}
              </button>
              <button
                onClick={() => {
                  setCursorMs(0);
                  setPlaying(true);
                }}
                className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300"
                title="Restart"
              >
                <RotateCcw className="w-3.5 h-3.5" />
              </button>
            </div>
            <div className="font-mono text-slate-400">
              {(cursorMs / 1000).toFixed(2)}s / {(durationMs / 1000).toFixed(2)}s
            </div>
          </div>
        </div>

        {/* Network 5xx & Frustration Breadcrumbs */}
        <div className="border-t border-slate-800 pt-3">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
            Wireframe Op &amp; Network 5xx Breadcrumbs
          </div>
          <div className="space-y-1.5 max-h-60 overflow-y-auto pr-1">
            {networkBreadcrumbs.map((nb, idx) => (
              <button
                key={`net-${idx}`}
                onClick={() => {
                  setCursorMs(nb.ts);
                  setPlaying(false);
                }}
                className="w-full text-left flex items-center justify-between px-2.5 py-1.5 rounded-lg bg-red-950/30 border border-red-900/50 hover:bg-red-950/50 text-xs"
              >
                <div className="flex items-center gap-2 truncate">
                  <WifiOff className="w-3.5 h-3.5 text-red-400 shrink-0" />
                  <span className="font-mono text-red-300 font-semibold">
                    {nb.method} {nb.status}
                  </span>
                  <span className="font-mono text-slate-300 truncate">{nb.path}</span>
                </div>
                <span className="font-mono text-slate-400 shrink-0 ml-2">
                  {nb.dur}ms @ {(nb.ts / 1000).toFixed(1)}s
                </span>
              </button>
            ))}

            {rageMarkers.map((rm, idx) => (
              <button
                key={`rage-${idx}`}
                onClick={() => {
                  setCursorMs(rm.ts);
                  setPlaying(false);
                }}
                className="w-full text-left flex items-center justify-between px-2.5 py-1.5 rounded-lg bg-amber-950/30 border border-amber-800/40 hover:bg-amber-950/50 text-xs"
              >
                <div className="flex items-center gap-2">
                  <Zap className="w-3.5 h-3.5 text-amber-400 fill-amber-400" />
                  <span className="text-amber-200 font-medium">
                    Rage Tap Detected ({rm.count} rapid taps within 800ms)
                  </span>
                </div>
                <span className="font-mono text-slate-400">@ {(rm.ts / 1000).toFixed(1)}s</span>
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};
