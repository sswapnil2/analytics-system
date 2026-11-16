import React, { useCallback, useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { api, default14DayRange, RetentionResult } from "../api/client";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

function heatBg(pct: number): string {
  if (pct >= 65) return "rgba(37, 99, 235, 0.85)";
  if (pct >= 45) return "rgba(37, 99, 235, 0.65)";
  if (pct >= 30) return "rgba(37, 99, 235, 0.48)";
  if (pct >= 20) return "rgba(37, 99, 235, 0.32)";
  if (pct >= 10) return "rgba(37, 99, 235, 0.18)";
  if (pct > 0) return "rgba(37, 99, 235, 0.09)";
  return "transparent";
}

export const RetentionWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  refreshToken,
}) => {
  const [startEvent, setStartEvent] = useState<string>("App_Open");
  const [returnEvent, setReturnEvent] = useState<string>("Quick_Grocery_Browse");
  const [mode, setMode] = useState<"n_day" | "unbounded" | "frequency_stickiness">("n_day");
  const [numPeriods, setNumPeriods] = useState<number>(10);
  const [result, setResult] = useState<RetentionResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const runRetention = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.queryRetention(projectId, {
        time_range: default14DayRange(),
        start_event: startEvent,
        return_event: returnEvent,
        mode,
        unit: "day",
        num_periods: numPeriods,
      });
      setResult(res);
    } finally {
      setLoading(false);
    }
  }, [projectId, startEvent, returnEvent, mode, numPeriods]);

  useEffect(() => {
    void runRetention();
  }, [runRetention, refreshToken]);

  const eventOptions = [
    "App_Open",
    "Quick_Grocery_Browse",
    "Quick_Commerce_Order_Paid",
    "OTP_Verified",
    "UPI_Autopay_Mandate_Approved",
    ...availableEvents,
  ].filter((v, i, a) => a.indexOf(v) === i);

  return (
    <div className="space-y-5">
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 flex flex-wrap items-center justify-between gap-3 text-xs">
        <div className="flex flex-wrap items-center gap-3">
          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <span className="text-slate-400 mr-2">Cohort Entry:</span>
            <select
              value={startEvent}
              onChange={(e) => setStartEvent(e.target.value)}
              className="bg-transparent text-slate-100 font-medium focus:outline-none"
            >
              {eventOptions.map((ev) => (
                <option key={ev} value={ev} className="bg-slate-900">
                  {ev}
                </option>
              ))}
            </select>
          </div>

          <div className="bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <span className="text-slate-400 mr-2">Return Event:</span>
            <select
              value={returnEvent}
              onChange={(e) => setReturnEvent(e.target.value)}
              className="bg-transparent text-slate-100 font-medium focus:outline-none"
            >
              {eventOptions.map((ev) => (
                <option key={ev} value={ev} className="bg-slate-900">
                  {ev}
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center bg-[#090d16] border border-slate-800 rounded-lg p-1">
            {(
              [
                ["n_day", "N-Day Exact"],
                ["unbounded", "Unbounded"],
                ["frequency_stickiness", "Stickiness"],
              ] as const
            ).map(([k, lbl]) => (
              <button
                key={k}
                onClick={() => setMode(k)}
                className={`px-2.5 py-1 rounded ${
                  mode === k ? "bg-blue-600 text-white" : "text-slate-400"
                }`}
              >
                {lbl}
              </button>
            ))}
          </div>

          <select
            value={numPeriods}
            onChange={(e) => setNumPeriods(Number(e.target.value))}
            className="bg-[#090d16] border border-slate-800 rounded-lg px-2.5 py-1.5 text-slate-200"
          >
            <option value={7}>7 Days</option>
            <option value={10}>10 Days</option>
            <option value={13}>13 Days</option>
          </select>
        </div>

        <button
          onClick={() => void runRetention()}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
          Compute Cohorts
        </button>
      </div>

      {/* Cohort Retention Heatmap Triangle */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 overflow-x-auto">
        <div className="flex items-center justify-between mb-3">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Exact Unsampled RoaringBitmap Cohort Triangle ({mode.replace("_", " ")})
          </div>
          {result && (
            <span className="font-mono text-xs text-emerald-400">
              Latency: {(result.exec_time_us / 1000).toFixed(2)} ms
            </span>
          )}
        </div>

        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b border-slate-800 text-slate-400">
              <th className="py-2 pr-3 text-left">Cohort Date</th>
              <th className="py-2 px-3 text-right">Users</th>
              {Array.from({ length: numPeriods + 1 }, (_, i) => (
                <th key={i} className="py-2 px-2 text-center font-mono">
                  {mode === "frequency_stickiness" ? `≥${i + 1}d` : `D${i}`}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/40">
            {result?.cohorts.map((row, rIdx) => {
              const maxValidPeriod =
                mode === "frequency_stickiness"
                  ? numPeriods
                  : Math.max(0, result.cohorts.length - 1 - rIdx);
              return (
                <tr key={row.cohort_start_ts_ms}>
                  <td className="py-1.5 pr-3 font-mono text-slate-200 whitespace-nowrap">
                    {row.cohort_label}
                  </td>
                  <td className="py-1.5 px-3 text-right font-mono text-slate-300">
                    {row.cohort_size.toLocaleString()}
                  </td>
                  {row.retained_pcts.slice(0, numPeriods + 1).map((pct, cIdx) => {
                    if (cIdx > maxValidPeriod && row.retained_counts[cIdx] === 0) {
                      return (
                        <td key={cIdx} className="py-1.5 px-1 text-center text-slate-700 font-mono">
                          —
                        </td>
                      );
                    }
                    return (
                      <td key={cIdx} className="p-1">
                        <div
                          style={{ backgroundColor: heatBg(pct) }}
                          title={`${row.retained_counts[cIdx]} / ${row.cohort_size} users`}
                          className="rounded py-1.5 px-1.5 text-center font-mono text-[11px] text-white border border-slate-800/40"
                        >
                          {pct.toFixed(1)}%
                        </div>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};
