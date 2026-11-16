import React, { useCallback, useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { api, default14DayRange, FlowsResult } from "../api/client";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

export const FlowsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  refreshToken,
}) => {
  const [anchorEvent, setAnchorEvent] = useState<string>("KYC_PAN_Submitted");
  const [direction, setDirection] = useState<"forward" | "reverse">("forward");
  const [steps, setSteps] = useState<number>(3);
  const [result, setResult] = useState<FlowsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const runFlows = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.queryFlows(projectId, {
        time_range: default14DayRange(),
        anchor_event: anchorEvent,
        direction,
        steps,
      });
      setResult(res);
    } finally {
      setLoading(false);
    }
  }, [projectId, anchorEvent, direction, steps]);

  useEffect(() => {
    void runFlows();
  }, [runFlows, refreshToken]);

  // Compute SVG Sankey node coordinates and Bezier ribbon links
  const sankey = useMemo(() => {
    if (!result || result.nodes.length === 0) return null;
    const colsMap = new Map<number, typeof result.nodes>();
    for (const n of result.nodes) {
      const list = colsMap.get(n.step_offset) ?? [];
      list.push(n);
      colsMap.set(n.step_offset, list);
    }
    const sortedCols = Array.from(colsMap.keys()).sort((a, b) => a - b);
    const svgW = 920;
    const svgH = 420;
    const colGap = sortedCols.length > 1 ? (svgW - 180) / (sortedCols.length - 1) : 0;

    const nodeRects = new Map<
      string,
      { x: number; y: number; h: number; label: string; count: number; isDrop: boolean }
    >();

    sortedCols.forEach((colOffset, cIdx) => {
      const colNodes = (colsMap.get(colOffset) ?? []).slice(0, 7);
      const colTotal = Math.max(
        1,
        colNodes.reduce((acc, n) => acc + n.user_count, 0),
      );
      const usableH = svgH - 70 - Math.max(0, colNodes.length - 1) * 14;
      let curY = 36;
      const x = 60 + cIdx * colGap;

      for (const n of colNodes) {
        const h = Math.max(22, (n.user_count / colTotal) * usableH);
        nodeRects.set(n.id, {
          x,
          y: curY,
          h,
          label: n.event_name,
          count: n.user_count,
          isDrop: n.event_name === "$Drop_Off",
        });
        curY += h + 14;
      }
    });

    const srcOutOffset = new Map<string, number>();
    const dstInOffset = new Map<string, number>();

    const ribbons = result.links
      .filter((l) => nodeRects.has(l.source) && nodeRects.has(l.target))
      .map((l) => {
        const s = nodeRects.get(l.source)!;
        const t = nodeRects.get(l.target)!;
        const sThick = Math.max(3, (l.user_count / Math.max(1, s.count)) * s.h);
        const tThick = Math.max(3, (l.user_count / Math.max(1, t.count)) * t.h);
        const sOff = srcOutOffset.get(l.source) ?? 0;
        const tOff = dstInOffset.get(l.target) ?? 0;
        srcOutOffset.set(l.source, sOff + sThick);
        dstInOffset.set(l.target, tOff + tThick);

        const x0 = s.x + 22;
        const y0 = s.y + Math.min(s.h - 2, sOff + sThick / 2);
        const x1 = t.x;
        const y1 = t.y + Math.min(t.h - 2, tOff + tThick / 2);
        const cx = (x0 + x1) / 2;

        return {
          key: `${l.source}->${l.target}`,
          d: `M ${x0},${y0} C ${cx},${y0} ${cx},${y1} ${x1},${y1}`,
          strokeW: Math.max(2.5, (sThick + tThick) / 2),
          isDrop: t.isDrop,
          count: l.user_count,
          srcLabel: s.label,
          dstLabel: t.label,
        };
      });

    return { svgW, svgH, nodeRects: Array.from(nodeRects.entries()), ribbons };
  }, [result]);

  return (
    <div className="space-y-5">
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3 text-xs">
          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <span className="text-slate-400 mr-2">Anchor Event:</span>
            <select
              value={anchorEvent}
              onChange={(e) => setAnchorEvent(e.target.value)}
              className="bg-transparent text-slate-100 font-medium focus:outline-none"
            >
              {[
                "App_Open",
                "OTP_Verified",
                "KYC_PAN_Submitted",
                "Video_KYC_Completed",
                "UPI_Autopay_Mandate_Approved",
                "First_Disbursal_Success",
                ...availableEvents,
              ]
                .filter((v, i, a) => a.indexOf(v) === i)
                .map((ev) => (
                  <option key={ev} value={ev} className="bg-slate-900">
                    {ev}
                  </option>
                ))}
            </select>
          </div>

          <div className="flex items-center bg-[#090d16] border border-slate-800 rounded-lg p-1">
            <button
              onClick={() => setDirection("forward")}
              className={`px-2.5 py-1 rounded ${
                direction === "forward" ? "bg-blue-600 text-white" : "text-slate-400"
              }`}
            >
              Forward (+N)
            </button>
            <button
              onClick={() => setDirection("reverse")}
              className={`px-2.5 py-1 rounded ${
                direction === "reverse" ? "bg-blue-600 text-white" : "text-slate-400"
              }`}
            >
              Reverse (-N)
            </button>
          </div>

          <div className="flex items-center gap-2 bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <span className="text-slate-400">Steps: {steps}</span>
            <input
              type="range"
              min={2}
              max={5}
              value={steps}
              onChange={(e) => setSteps(Number(e.target.value))}
              className="w-24 accent-blue-500"
            />
          </div>
        </div>

        <button
          onClick={() => void runFlows()}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
          Rebuild Sankey
        </button>
      </div>

      {/* Interactive SVG Sankey Path Diagram */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-5">
        <div className="flex items-center justify-between mb-2">
          <div className="text-xs text-slate-400">
            Consecutive repeated events collapsed automatically (<code className="text-blue-300">A → A ⇒ A</code>)
          </div>
          {result && (
            <span className="font-mono text-xs text-emerald-400">
              Exec: {(result.exec_time_us / 1000).toFixed(2)} ms
            </span>
          )}
        </div>

        {sankey ? (
          <svg viewBox={`0 0 ${sankey.svgW} ${sankey.svgH}`} className="w-full h-[420px]">
            {sankey.ribbons.map((r) => (
              <path
                key={r.key}
                d={r.d}
                fill="none"
                stroke={r.isDrop ? "#ef4444" : "#3b82f6"}
                strokeOpacity={r.isDrop ? 0.28 : 0.36}
                strokeWidth={r.strokeW}
                className="hover:stroke-opacity-75 transition cursor-pointer"
              >
                <title>
                  {r.srcLabel} → {r.dstLabel}: {r.count.toLocaleString()} unique users
                </title>
              </path>
            ))}

            {sankey.nodeRects.map(([id, n]) => (
              <g key={id}>
                <rect
                  x={n.x}
                  y={n.y}
                  width={22}
                  height={n.h}
                  rx={4}
                  fill={n.isDrop ? "#dc2626" : "#2563eb"}
                  stroke={n.isDrop ? "#f87171" : "#60a5fa"}
                  strokeWidth={1}
                />
                <text
                  x={n.x + 28}
                  y={n.y + Math.min(n.h / 2, 14)}
                  className="fill-slate-100 text-[11px] font-medium"
                >
                  {n.label}
                </text>
                <text
                  x={n.x + 28}
                  y={n.y + Math.min(n.h / 2, 14) + 13}
                  className="fill-slate-400 text-[10px] font-mono"
                >
                  {n.count.toLocaleString()} users
                </text>
              </g>
            ))}
          </svg>
        ) : (
          <div className="h-64 flex items-center justify-center text-xs text-slate-500">
            No flow transitions found for anchor event.
          </div>
        )}
      </div>
    </div>
  );
};
