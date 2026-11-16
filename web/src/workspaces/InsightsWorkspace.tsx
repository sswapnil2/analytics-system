import React, { useCallback, useEffect, useState } from "react";
import { Calculator, Download, Plus, RefreshCw, Trash2 } from "lucide-react";
import { api, default14DayRange, InsightsResult } from "../api/client";

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  refreshToken: number;
}

interface SeriesDraft {
  label: string;
  eventName: string;
  metricKind: string;
  propName: string;
}

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#a855f7", "#ef4444", "#06b6d4"];

function buildMetricSpec(kind: string, prop: string) {
  switch (kind) {
    case "totals":
      return { type: "totals" };
    case "sessions":
      return { type: "sessions" };
    case "dau":
      return { type: "dau" };
    case "wau":
      return { type: "wau" };
    case "mau":
      return { type: "mau" };
    case "sum":
      return { type: "prop_sum", prop };
    case "avg":
      return { type: "prop_avg", prop };
    case "p50":
      return { type: "prop_percentile", prop, quantile: 0.5 };
    case "p90":
      return { type: "prop_percentile", prop, quantile: 0.9 };
    case "p99":
      return { type: "prop_percentile", prop, quantile: 0.99 };
    default:
      return { type: "uniques" };
  }
}

export const InsightsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  availableProps,
  refreshToken,
}) => {
  const [series, setSeries] = useState<SeriesDraft[]>([
    { label: "A", eventName: "App_Open", metricKind: "uniques", propName: "amount" },
    {
      label: "B",
      eventName: "First_Disbursal_Success",
      metricKind: "uniques",
      propName: "amount",
    },
  ]);
  const [formula, setFormula] = useState<string>("(B / A) * 100");
  const [breakdown, setBreakdown] = useState<string>("city");
  const [comparePrev, setComparePrev] = useState<boolean>(false);
  const [chartType, setChartType] = useState<"area" | "bar">("area");
  const [result, setResult] = useState<InsightsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const runQuery = useCallback(async () => {
    setLoading(true);
    try {
      const payload = {
        time_range: default14DayRange(),
        interval: "day",
        series: series.map((s) => ({
          label: s.label,
          event_name: s.eventName,
          metric: buildMetricSpec(s.metricKind, s.propName),
          filters: [],
        })),
        breakdown_by: breakdown ? [breakdown] : [],
        compare_previous_period: comparePrev,
        formula: formula.trim() ? formula.trim() : null,
      };
      const res = await api.queryInsights(projectId, payload);
      setResult(res);
    } finally {
      setLoading(false);
    }
  }, [projectId, series, formula, breakdown, comparePrev]);

  useEffect(() => {
    void runQuery();
  }, [runQuery, refreshToken]);

  const exportCsv = () => {
    if (!result) return;
    const rows = [
      ["Series", "Breakdown", "Overall", ...result.buckets_ts_ms.map((t) => new Date(t).toISOString().slice(0, 10))],
    ];
    for (const s of [...result.series, ...result.formula_series]) {
      rows.push([
        s.label,
        s.breakdown_key,
        s.overall_value.toFixed(2),
        ...s.points.map((p) => p.value.toFixed(2)),
      ]);
    }
    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pulse_insights_${projectId}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const displayedSeries = result
    ? (formula.trim() && result.formula_series.length > 0 ? result.formula_series : result.series).slice(0, 6)
    : [];

  const maxY = Math.max(
    1,
    ...displayedSeries.flatMap((s) => s.points.map((p) => p.value)),
  );

  return (
    <div className="space-y-5">
      {/* Builder Card */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4 space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="text-sm font-semibold text-slate-100">
            Insights Query Builder (Exact RoaringBitmap Uniques &amp; DDSketch Percentiles)
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => {
                const nextChar = String.fromCharCode(65 + series.length);
                setSeries([
                  ...series,
                  {
                    label: nextChar,
                    eventName: availableEvents[0] || "App_Open",
                    metricKind: "uniques",
                    propName: "amount",
                  },
                ]);
              }}
              className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs text-slate-200"
            >
              <Plus className="w-3.5 h-3.5" /> Add Metric
            </button>
            <button
              onClick={() => void runQuery()}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
              Run Insights
            </button>
          </div>
        </div>

        <div className="space-y-2">
          {series.map((s, idx) => (
            <div
              key={s.label}
              className="flex flex-wrap items-center gap-2 bg-[#090d16] border border-slate-800/70 rounded-lg px-3 py-2"
            >
              <span
                className="w-6 h-6 rounded flex items-center justify-center text-xs font-bold text-white"
                style={{ backgroundColor: COLORS[idx % COLORS.length] }}
              >
                {s.label}
              </span>
              <select
                value={s.eventName}
                onChange={(e) => {
                  const next = [...series];
                  next[idx] = { ...s, eventName: e.target.value };
                  setSeries(next);
                }}
                className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-100"
              >
                {(availableEvents.length > 0
                  ? availableEvents
                  : [
                      "App_Open",
                      "OTP_Verified",
                      "KYC_PAN_Submitted",
                      "Video_KYC_Completed",
                      "UPI_Autopay_Mandate_Approved",
                      "First_Disbursal_Success",
                      "Quick_Commerce_Order_Paid",
                      "Checkout_Any",
                    ]
                ).map((ev) => (
                  <option key={ev} value={ev}>
                    {ev}
                  </option>
                ))}
              </select>

              <select
                value={s.metricKind}
                onChange={(e) => {
                  const next = [...series];
                  next[idx] = { ...s, metricKind: e.target.value };
                  setSeries(next);
                }}
                className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-100"
              >
                <option value="uniques">Unique Users (RoaringBitmap)</option>
                <option value="totals">Total Events</option>
                <option value="sessions">Unique Sessions</option>
                <option value="dau">DAU</option>
                <option value="wau">WAU (7d Rolling)</option>
                <option value="mau">MAU (30d Rolling)</option>
                <option value="sum">Property Sum</option>
                <option value="avg">Property Avg</option>
                <option value="p50">Property p50 (DDSketch)</option>
                <option value="p90">Property p90 (DDSketch)</option>
                <option value="p99">Property p99 (DDSketch)</option>
              </select>

              {["sum", "avg", "p50", "p90", "p99"].includes(s.metricKind) && (
                <select
                  value={s.propName}
                  onChange={(e) => {
                    const next = [...series];
                    next[idx] = { ...s, propName: e.target.value };
                    setSeries(next);
                  }}
                  className="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-xs text-amber-300 font-mono"
                >
                  {["amount", ...availableProps.filter((p) => p !== "amount")].map((p) => (
                    <option key={p} value={p}>
                      {p}
                    </option>
                  ))}
                </select>
              )}

              {series.length > 1 && (
                <button
                  onClick={() => setSeries(series.filter((_, i) => i !== idx))}
                  className="ml-auto text-slate-500 hover:text-red-400"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
          ))}
        </div>

        {/* Formula + Breakdown Row */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 pt-1">
          <div className="flex items-center gap-2 bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5">
            <Calculator className="w-4 h-4 text-purple-400 shrink-0" />
            <input
              value={formula}
              onChange={(e) => setFormula(e.target.value)}
              placeholder="Formula e.g. (B / A) * 100"
              className="bg-transparent w-full text-xs font-mono text-purple-200 focus:outline-none"
            />
          </div>

          <div className="flex items-center gap-2 bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-xs">
            <span className="text-slate-400 shrink-0">Breakdown:</span>
            <select
              value={breakdown}
              onChange={(e) => setBreakdown(e.target.value)}
              className="bg-transparent w-full text-slate-100 focus:outline-none"
            >
              <option value="">None (Overall)</option>
              {[
                "city",
                "platform",
                "payment_method",
                "network",
                "ticket_band",
                "category",
                ...availableProps,
              ]
                .filter((v, i, a) => a.indexOf(v) === i)
                .map((p) => (
                  <option key={p} value={p} className="bg-slate-900">
                    {p}
                  </option>
                ))}
            </select>
          </div>

          <div className="flex items-center justify-between bg-[#090d16] border border-slate-800 rounded-lg px-3 py-1.5 text-xs">
            <label className="flex items-center gap-2 cursor-pointer text-slate-300">
              <input
                type="checkbox"
                checked={comparePrev}
                onChange={(e) => setComparePrev(e.target.checked)}
                className="accent-blue-500"
              />
              Compare Prev Period
            </label>
            <div className="flex items-center gap-1">
              <button
                onClick={() => setChartType("area")}
                className={`px-2 py-0.5 rounded ${
                  chartType === "area" ? "bg-blue-600 text-white" : "text-slate-400"
                }`}
              >
                Area
              </button>
              <button
                onClick={() => setChartType("bar")}
                className={`px-2 py-0.5 rounded ${
                  chartType === "bar" ? "bg-blue-600 text-white" : "text-slate-400"
                }`}
              >
                Bar
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Time-Series SVG Chart */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex flex-wrap items-center gap-3 text-xs">
            {displayedSeries.map((s, idx) => (
              <div key={`${s.label}-${s.breakdown_key}`} className="flex items-center gap-1.5">
                <span
                  className="w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                />
                <span className="text-slate-300 font-medium">
                  {s.label}: {s.breakdown_key}
                </span>
                <span className="font-mono text-slate-400">
                  ({s.overall_value.toLocaleString(undefined, { maximumFractionDigits: 2 })})
                </span>
              </div>
            ))}
          </div>
          {result && (
            <span className="font-mono text-xs text-emerald-400">
              Engine: {(result.exec_time_us / 1000).toFixed(2)} ms
            </span>
          )}
        </div>

        <div className="h-64 w-full">
          <svg viewBox="0 0 900 230" className="w-full h-full overflow-visible">
            {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
              const y = 200 - frac * 180;
              return (
                <g key={frac}>
                  <line x1={48} y1={y} x2={885} y2={y} stroke="#1e293b" strokeDasharray="3 3" />
                  <text x={42} y={y + 4} textAnchor="end" className="fill-slate-500 text-[10px] font-mono">
                    {(maxY * frac).toFixed(maxY >= 10 ? 0 : 1)}
                  </text>
                </g>
              );
            })}

            {displayedSeries.map((s, sIdx) => {
              const pts = s.points;
              if (pts.length === 0) return null;
              const color = COLORS[sIdx % COLORS.length];
              const xStep = pts.length > 1 ? 820 / (pts.length - 1) : 0;

              if (chartType === "bar") {
                const groupW = Math.max(8, 720 / Math.max(1, pts.length));
                const barW = Math.max(3, (groupW - 6) / Math.max(1, displayedSeries.length));
                return (
                  <g key={`${s.label}-${s.breakdown_key}`}>
                    {pts.map((p, pIdx) => {
                      const h = (p.value / maxY) * 180;
                      const x = 56 + pIdx * xStep - groupW / 2 + sIdx * barW;
                      return (
                        <rect
                          key={p.bucket_ts_ms}
                          x={x}
                          y={200 - h}
                          width={barW - 1}
                          height={Math.max(1, h)}
                          fill={color}
                          opacity={0.85}
                          rx={2}
                        />
                      );
                    })}
                  </g>
                );
              }

              const coords = pts.map((p, i) => {
                const x = 56 + i * xStep;
                const y = 200 - (p.value / maxY) * 180;
                return `${x},${y}`;
              });
              const areaPath = `M 56,200 L ${coords.join(" L ")} L ${56 + (pts.length - 1) * xStep},200 Z`;
              return (
                <g key={`${s.label}-${s.breakdown_key}`}>
                  <path d={areaPath} fill={color} opacity={0.12} />
                  <polyline
                    fill="none"
                    stroke={color}
                    strokeWidth={2.2}
                    points={coords.join(" ")}
                  />
                </g>
              );
            })}

            {result?.buckets_ts_ms.map((ts, idx) => {
              if (idx % 2 !== 0 && idx !== result.buckets_ts_ms.length - 1) return null;
              const n = result.buckets_ts_ms.length;
              const x = 56 + (n > 1 ? (idx * 820) / (n - 1) : 0);
              return (
                <text
                  key={ts}
                  x={x}
                  y={220}
                  textAnchor="middle"
                  className="fill-slate-500 text-[10px] font-mono"
                >
                  {new Date(ts).toISOString().slice(5, 10)}
                </text>
              );
            })}
          </svg>
        </div>
      </div>

      {/* Tabular Breakdown + CSV Export */}
      <div className="bg-[#0e1422] border border-slate-800/80 rounded-xl p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Tabular Series &amp; Formula Breakdown
          </div>
          <button
            onClick={exportCsv}
            className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-xs text-slate-200"
          >
            <Download className="w-3.5 h-3.5" /> Export CSV
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-xs text-left">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400">
                <th className="py-2 pr-4">Metric</th>
                <th className="py-2 pr-4">Breakdown Segment</th>
                <th className="py-2 pr-4 text-right">Overall</th>
                {result?.buckets_ts_ms.slice(-7).map((ts) => (
                  <th key={ts} className="py-2 px-2 text-right font-mono">
                    {new Date(ts).toISOString().slice(5, 10)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60">
              {result &&
                [...result.formula_series, ...result.series].slice(0, 18).map((row, idx) => (
                  <tr key={idx} className="hover:bg-slate-900/50">
                    <td className="py-2 pr-4 font-mono text-blue-300">{row.label}</td>
                    <td className="py-2 pr-4 text-slate-200 font-medium">{row.breakdown_key}</td>
                    <td className="py-2 pr-4 text-right font-mono font-semibold text-emerald-300">
                      {row.overall_value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                    </td>
                    {row.points.slice(-7).map((pt) => (
                      <td key={pt.bucket_ts_ms} className="py-2 px-2 text-right font-mono text-slate-300">
                        {pt.value.toLocaleString(undefined, { maximumFractionDigits: 1 })}
                      </td>
                    ))}
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
