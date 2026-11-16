import React, { useCallback, useEffect, useRef, useState } from "react";
import { Download, LineChart, Plus, Trash2 } from "lucide-react";
import { api, InsightsResult, SeriesOutput } from "../api/client";
import { errorMessage, humanizeEvent } from "../lib/format";
import { SaveToDashboard } from "../dashboards/SaveToDashboard";
import { RangePresets, rangeForDays } from "../dashboards/ReportCard";
import {
  btn,
  EmptyState,
  ErrorNotice,
  field,
  fieldLabel,
  PageHeader,
  Panel,
  RanIn,
  Spinner,
} from "../components/ui";

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

// Theme tokens, so the chart follows light and dark mode.
const COLORS = ["brand", "info", "warn", "danger", "ink-2", "muted"].map((v) => `rgb(var(--${v}))`);
const color = (i: number) => COLORS[i % COLORS.length];

const METRIC_OPTIONS: [string, string][] = [
  ["uniques", "Unique users"],
  ["totals", "Total events"],
  ["sessions", "Sessions"],
  ["dau", "Daily active users"],
  ["wau", "Weekly active users (7-day)"],
  ["mau", "Monthly active users (30-day)"],
  ["sum", "Sum of property"],
  ["avg", "Average of property"],
  ["p50", "Median (p50)"],
  ["p90", "p90"],
  ["p99", "p99"],
];
const PROP_METRICS = ["sum", "avg", "p50", "p90", "p99"];

const FALLBACK_EVENTS = [
  "App_Open",
  "OTP_Verified",
  "KYC_PAN_Submitted",
  "Video_KYC_Completed",
  "UPI_Autopay_Mandate_Approved",
  "First_Disbursal_Success",
  "Quick_Commerce_Order_Paid",
  "Checkout_Any",
];

const BREAKDOWN_DEFAULTS = ["city", "platform", "payment_method", "network", "ticket_band", "category"];

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

function formatValue(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "–";
  return v.toLocaleString("en-IN", { maximumFractionDigits: digits });
}

function formatDay(ts: number): string {
  return new Date(ts).toLocaleDateString("en-IN", { day: "numeric", month: "short" });
}

/** 1, 2, 5 x 10^n ceiling so axis ticks land on round numbers. */
function niceCeil(v: number): number {
  if (v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  const m = v / p;
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
}

/** Engine labels formula series "Formula((B / A) * 100)"; show it the way a PM would say it. */
function formulaLabel(raw: string): string {
  const expr = raw.replace(/^Formula\((.*)\)$/, "$1").trim();
  const ratio =
    expr.match(/^\(\s*([A-Z])\s*\/\s*([A-Z])\s*\)\s*\*\s*100$/) ?? expr.match(/^([A-Z])\s*\/\s*([A-Z])$/);
  if (ratio) return `Conversion (${ratio[1]} ÷ ${ratio[2]})`;
  return `Formula (${expr.replace(/\//g, "÷")})`;
}

// Plot area inside the 900x240 viewBox.
const PLOT = { left: 56, right: 885, top: 16, bottom: 200 };
const PLOT_W = PLOT.right - PLOT.left;
const PLOT_H = PLOT.bottom - PLOT.top;

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
  const [chartType, setChartType] = useState<"line" | "bar">("line");
  const [result, setResult] = useState<InsightsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const buildQuery = useCallback(
    () => ({
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
    }),
    [series, formula, breakdown, comparePrev],
  );

  const [days, setDays] = useState(14);
  const reqId = useRef(0);

  const runQuery = useCallback(async () => {
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryInsights(projectId, { time_range: rangeForDays(days), ...buildQuery() });
      if (id === reqId.current) setResult(res);
    } catch (err) {
      if (id === reqId.current) setError(errorMessage(err));
    } finally {
      if (id === reqId.current) setLoading(false);
    }
  }, [projectId, buildQuery, days]);

  useEffect(() => {
    void runQuery();
  }, [runQuery, refreshToken]);

  const updateSeries = (idx: number, patch: Partial<SeriesDraft>) =>
    setSeries(series.map((s, i) => (i === idx ? { ...s, ...patch } : s)));

  const seriesName = (s: SeriesOutput): string => {
    if (s.label.startsWith("Formula(")) return formulaLabel(s.label);
    const draft = series.find((d) => d.label === s.label);
    return draft ? `${humanizeEvent(draft.eventName)} (${s.label})` : s.label;
  };

  const exportCsv = () => {
    if (!result) return;
    const rows = [
      ["Series", "Segment", "Overall", ...result.buckets_ts_ms.map((t) => new Date(t).toISOString().slice(0, 10))],
    ];
    for (const s of [...result.series, ...result.formula_series]) {
      rows.push([
        seriesName(s),
        s.breakdown_key,
        s.overall_value === null ? "" : s.overall_value.toFixed(2),
        ...s.points.map((p) => (p.value === null ? "" : p.value.toFixed(2))),
      ]);
    }
    const csv = rows.map((r) => r.map((c) => `"${c.replace(/"/g, '""')}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `trends_${projectId}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const displayedSeries = result
    ? (formula.trim() && result.formula_series.length > 0 ? result.formula_series : result.series).slice(0, 6)
    : [];
  const buckets = result?.buckets_ts_ms ?? [];
  const n = buckets.length;
  const maxY = niceCeil(
    Math.max(0, ...displayedSeries.flatMap((s) => s.points.map((p) => p.value ?? 0))),
  );
  const bandW = PLOT_W / Math.max(1, n);
  const xAt = (i: number) => PLOT.left + (i + 0.5) * bandW;
  const yAt = (v: number) => PLOT.bottom - (v / maxY) * PLOT_H;
  const labelEvery = Math.max(1, Math.ceil(n / 8));
  const hasData = displayedSeries.some((s) => s.points.some((p) => p.value !== null && p.value !== 0));

  const tableRows = result ? [...result.formula_series, ...result.series].slice(0, 18) : [];
  const tableBuckets = buckets.slice(-7);

  const breakdownOptions = [...BREAKDOWN_DEFAULTS, ...availableProps].filter(
    (v, i, a) => a.indexOf(v) === i,
  );
  const eventOptions = availableEvents.length > 0 ? availableEvents : FALLBACK_EVENTS;

  return (
    <div className="space-y-5">
      <PageHeader
        title="Trends"
        description="How an event or metric changes over time, by segment."
        actions={
          <>
            <RangePresets days={days} onChange={setDays} />
            <SaveToDashboard
              projectId={projectId}
              kind="insights"
              defaultName={series.map((s) => humanizeEvent(s.eventName)).join(" and ")}
              query={buildQuery}
            />
            <button type="button" onClick={() => void runQuery()} disabled={loading} className={btn.primary}>
              {loading ? "Running…" : "Update chart"}
            </button>
          </>
        }
      />

      <Panel
        title="Metrics"
        actions={
          <button
            type="button"
            onClick={() =>
              setSeries([
                ...series,
                {
                  label: String.fromCharCode(65 + series.length),
                  eventName: eventOptions[0] || "App_Open",
                  metricKind: "uniques",
                  propName: "amount",
                },
              ])
            }
            className={btn.secondary}
          >
            <Plus className="h-4 w-4" /> Add metric
          </button>
        }
      >
        <div className="space-y-2">
          {series.map((s, idx) => (
            <div
              key={s.label}
              className="flex flex-wrap items-end gap-2 rounded-lg border border-line bg-sunken px-3 py-2"
            >
              <span
                className="mb-1 flex h-6 w-6 items-center justify-center rounded text-xs font-semibold text-on-brand"
                style={{ backgroundColor: color(idx) }}
                aria-hidden="true"
              >
                {s.label}
              </span>
              <label className="flex min-w-0 flex-col gap-1">
                <span className={fieldLabel}>Event {s.label}</span>
                <select
                  value={s.eventName}
                  onChange={(e) => updateSeries(idx, { eventName: e.target.value })}
                  className={field}
                >
                  {eventOptions.map((ev) => (
                    <option key={ev} value={ev}>
                      {humanizeEvent(ev)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Measure</span>
                <select
                  value={s.metricKind}
                  onChange={(e) => updateSeries(idx, { metricKind: e.target.value })}
                  className={field}
                >
                  {METRIC_OPTIONS.map(([value, text]) => (
                    <option key={value} value={value}>
                      {text}
                    </option>
                  ))}
                </select>
              </label>
              {PROP_METRICS.includes(s.metricKind) && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Property</span>
                  <select
                    value={s.propName}
                    onChange={(e) => updateSeries(idx, { propName: e.target.value })}
                    className={field}
                  >
                    {["amount", ...availableProps.filter((p) => p !== "amount")].map((p) => (
                      <option key={p} value={p}>
                        {p}
                      </option>
                    ))}
                  </select>
                </label>
              )}
              {series.length > 1 && (
                <button
                  type="button"
                  onClick={() => setSeries(series.filter((_, i) => i !== idx))}
                  aria-label={`Remove metric ${s.label}`}
                  className={`${btn.ghost} ml-auto mb-0.5 hover:text-danger`}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
            </div>
          ))}
        </div>

        <div className="grid grid-cols-1 gap-3 pt-4 sm:grid-cols-2 lg:grid-cols-3">
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Formula (optional)</span>
            <input
              value={formula}
              onChange={(e) => setFormula(e.target.value)}
              placeholder="(B / A) * 100"
              className={`${field} font-mono`}
            />
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Break down by</span>
            <select value={breakdown} onChange={(e) => setBreakdown(e.target.value)} className={field}>
              <option value="">No breakdown</option>
              {breakdownOptions.map((p) => (
                <option key={p} value={p}>
                  {p.replace(/_/g, " ")}
                </option>
              ))}
            </select>
          </label>
          <div className="flex flex-wrap items-end justify-between gap-3">
            <label className="flex items-center gap-2 py-1.5 text-sm text-ink-2">
              <input
                type="checkbox"
                checked={comparePrev}
                onChange={(e) => setComparePrev(e.target.checked)}
                className="accent-brand"
              />
              Compare with previous period
            </label>
            <div className="flex rounded-lg border border-line-strong p-0.5" role="group" aria-label="Chart type">
              {(
                [
                  ["line", "Line"],
                  ["bar", "Bar"],
                ] as const
              ).map(([k, text]) => (
                <button
                  key={k}
                  type="button"
                  aria-pressed={chartType === k}
                  onClick={() => setChartType(k)}
                  className={`rounded px-2.5 py-1 text-sm ${
                    chartType === k ? "bg-brand text-on-brand" : "text-muted hover:text-ink"
                  }`}
                >
                  {text}
                </button>
              ))}
            </div>
          </div>
        </div>
      </Panel>

      {error && <ErrorNotice message={error} onRetry={() => void runQuery()} />}

      <Panel
        title="Over time"
        meta={loading && result ? "Updating…" : <RanIn us={result?.exec_time_us} />}
      >
        {!result ? (
          loading ? (
            <div className="flex h-64 items-center justify-center">
              <Spinner />
            </div>
          ) : (
            <EmptyState title="No results yet" body="Pick an event and run the query." icon={LineChart} />
          )
        ) : !hasData ? (
          <EmptyState
            title="No activity in this period"
            body="None of these events happened in the last 14 days. Try a different event or remove the breakdown."
            icon={LineChart}
          />
        ) : (
          <>
            <ul className="mb-3 flex flex-wrap gap-x-4 gap-y-1.5 text-sm">
              {displayedSeries.map((s, idx) => (
                <li key={`${s.label}-${s.breakdown_key}`} className="flex items-center gap-1.5">
                  <span className="h-0.5 w-3 rounded" style={{ backgroundColor: color(idx) }} aria-hidden="true" />
                  <span className="font-medium text-ink-2">{seriesName(s)}</span>
                  {s.breakdown_key && <span className="text-muted">{s.breakdown_key}</span>}
                  <span className="tabular text-muted">{formatValue(s.overall_value)}</span>
                </li>
              ))}
            </ul>
            <div className="h-64 w-full">
              <svg
                viewBox="0 0 900 230"
                className="h-full w-full overflow-visible"
                role="img"
                aria-label="Trend chart; values are listed in the table below"
              >
                {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
                  const y = yAt(maxY * frac);
                  return (
                    <g key={frac}>
                      <line x1={PLOT.left} y1={y} x2={PLOT.right} y2={y} stroke="rgb(var(--line))" />
                      <text
                        x={PLOT.left - 8}
                        y={y + 4}
                        textAnchor="end"
                        fill="rgb(var(--muted))"
                        fontSize={11}
                        className="tabular"
                      >
                        {formatValue(maxY * frac, maxY >= 10 ? 0 : 1)}
                      </text>
                    </g>
                  );
                })}

                {displayedSeries.map((s, sIdx) => {
                  const c = color(sIdx);
                  const key = `${s.label}-${s.breakdown_key}`;
                  if (chartType === "bar") {
                    const barW = Math.max(2, (bandW * 0.8) / displayedSeries.length);
                    return (
                      <g key={key}>
                        {s.points.map((p, i) =>
                          p.value === null ? null : (
                            <rect
                              key={p.bucket_ts_ms}
                              x={PLOT.left + i * bandW + bandW * 0.1 + sIdx * barW}
                              y={yAt(p.value)}
                              width={Math.max(1, barW - 1)}
                              height={Math.max(0, PLOT.bottom - yAt(p.value))}
                              fill={c}
                              rx={1.5}
                            />
                          ),
                        )}
                      </g>
                    );
                  }
                  // Null points (e.g. formula with a zero denominator) break the line instead of dropping to 0.
                  let d = "";
                  let pen = false;
                  s.points.forEach((p, i) => {
                    if (p.value === null) {
                      pen = false;
                      return;
                    }
                    d += `${pen ? "L" : "M"}${xAt(i)},${yAt(p.value)} `;
                    pen = true;
                  });
                  return (
                    <g key={key}>
                      <path d={d} fill="none" stroke={c} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
                      {s.points.map((p, i) =>
                        p.value === null ? null : (
                          <circle key={p.bucket_ts_ms} cx={xAt(i)} cy={yAt(p.value)} r={2.5} fill={c}>
                            <title>
                              {`${seriesName(s)}${s.breakdown_key ? `, ${s.breakdown_key}` : ""}: ${formatValue(
                                p.value,
                              )} on ${formatDay(p.bucket_ts_ms)}`}
                            </title>
                          </circle>
                        ),
                      )}
                    </g>
                  );
                })}

                {buckets.map((ts, idx) =>
                  idx % labelEvery !== 0 && idx !== n - 1 ? null : (
                    <text
                      key={ts}
                      x={xAt(idx)}
                      y={222}
                      textAnchor="middle"
                      fill="rgb(var(--muted))"
                      fontSize={11}
                    >
                      {formatDay(ts)}
                    </text>
                  ),
                )}
              </svg>
            </div>
          </>
        )}
      </Panel>

      {result && tableRows.length > 0 && (
        <Panel
          title="By segment"
          meta="Last 7 days"
          actions={
            <button type="button" onClick={exportCsv} className={btn.secondary}>
              <Download className="h-4 w-4" /> Export CSV
            </button>
          }
        >
          <div className="-mx-4 overflow-x-auto px-4">
            <table className="w-full min-w-[720px] text-left text-sm">
              <thead>
                <tr className="border-b border-line text-xs text-muted">
                  <th className="sticky left-0 z-10 bg-panel py-2 pr-4 font-medium">Metric</th>
                  <th className="py-2 pr-4 font-medium">Segment</th>
                  <th className="py-2 pr-4 text-right font-medium">Overall</th>
                  {tableBuckets.map((ts) => (
                    <th key={ts} className="whitespace-nowrap px-2 py-2 text-right font-medium">
                      {formatDay(ts)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {tableRows.map((row, idx) => (
                  <tr key={`${row.label}-${row.breakdown_key}-${idx}`} className="hover:bg-hover">
                    <td className="sticky left-0 z-10 whitespace-nowrap bg-panel py-2 pr-4 text-ink">
                      {seriesName(row)}
                    </td>
                    <td className="whitespace-nowrap py-2 pr-4 text-ink-2">{row.breakdown_key || "All users"}</td>
                    <td className="tabular py-2 pr-4 text-right font-semibold text-ink">
                      {formatValue(row.overall_value)}
                    </td>
                    {row.points.slice(-7).map((pt) => (
                      <td key={pt.bucket_ts_ms} className="tabular px-2 py-2 text-right text-ink-2">
                        {formatValue(pt.value, 1)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
    </div>
  );
};
