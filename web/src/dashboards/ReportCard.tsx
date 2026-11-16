import React, { useCallback, useEffect, useState } from "react";
import {
  api,
  FunnelResult,
  InsightsResult,
  RetentionResult,
  SavedReport,
  TimeRange,
} from "../api/client";
import { Spinner } from "../components/ui";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";

export const RANGE_OPTIONS = [
  { days: 7, label: "Last 7 days" },
  { days: 14, label: "Last 14 days" },
  { days: 30, label: "Last 30 days" },
  { days: 90, label: "Last 90 days" },
] as const;

export function rangeForDays(days: number): TimeRange {
  const now = Date.now();
  return { start_ts_ms: now - days * 86_400_000, end_ts_ms: now + 3_600_000 };
}

export const KIND_LABEL: Record<SavedReport["report_kind"], string> = {
  funnel: "Funnel",
  insights: "Trend",
  retention: "Retention",
  flows: "Journey",
};

/** "Formula((B / A) * 100)" → "(B / A) * 100" */
const seriesLabel = (l: string) => l.replace(/^Formula\((.*)\)$/, "$1");

const SERIES_COLORS = ["rgb(var(--brand))", "rgb(var(--info))", "rgb(var(--warn))"];

type CardResult =
  | { kind: "funnel"; data: FunnelResult }
  | { kind: "insights"; data: InsightsResult }
  | { kind: "retention"; data: RetentionResult };

/** Runs a saved report with the dashboard's date range and draws a compact version of it. */
export const ReportCardBody: React.FC<{ projectId: string; report: SavedReport; days: number; refreshKey?: number }> = ({
  projectId,
  report,
  days,
  refreshKey = 0,
}) => {
  const [result, setResult] = useState<CardResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const run = useCallback(async () => {
    setLoading(true);
    setError(null);
    const body = { ...report.query_json, time_range: rangeForDays(days) };
    try {
      if (report.report_kind === "funnel") {
        setResult({ kind: "funnel", data: await api.queryFunnel(projectId, body) });
      } else if (report.report_kind === "insights") {
        setResult({
          kind: "insights",
          data: await api.queryInsights(projectId, { ...body, interval: days > 30 ? "week" : "day" }),
        });
      } else if (report.report_kind === "retention") {
        setResult({ kind: "retention", data: await api.queryRetention(projectId, body) });
      } else {
        setError("Journeys can't be shown on a dashboard yet. Open it in Journeys.");
      }
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId, report, days]);

  useEffect(() => {
    void run();
  }, [run, refreshKey]);

  if (error) {
    return (
      <div className="space-y-2 text-sm">
        <p className="text-danger">Couldn't load this report: {error}</p>
        <button type="button" onClick={() => void run()} className="font-medium text-brand hover:underline">
          Try again
        </button>
      </div>
    );
  }
  if (!result) {
    return (
      <div className="flex h-32 items-center justify-center">
        <Spinner />
      </div>
    );
  }
  return (
    <div className={loading ? "opacity-60 transition-opacity" : undefined} aria-busy={loading}>
      {result.kind === "funnel" && <FunnelMini data={result.data} />}
      {result.kind === "insights" && <TrendMini data={result.data} />}
      {result.kind === "retention" && <RetentionMini data={result.data} />}
    </div>
  );
};

const Headline: React.FC<{ value: string; children?: React.ReactNode }> = ({ value, children }) => (
  <p className="flex flex-wrap items-baseline gap-x-2">
    <span className="tabular text-[1.75rem] font-semibold tracking-[-0.02em] text-ink">{value}</span>
    {children}
  </p>
);

const FunnelMini: React.FC<{ data: FunnelResult }> = ({ data }) => {
  const steps = data.overall_steps;
  if (steps.length === 0 || steps[0].entered_users === 0) {
    return <p className="py-6 text-sm text-muted">No users entered this funnel in the selected dates.</p>;
  }
  const first = steps[0].entered_users;
  let worst = -1;
  for (let i = 1; i < steps.length; i++) {
    if (worst < 0 || steps[i].dropped_users > steps[worst].dropped_users) worst = i;
  }
  const last = steps[steps.length - 1];
  return (
    <div className="space-y-3">
      <Headline value={`${last.conversion_from_start_pct.toFixed(1)}%`}>
        <span className="text-sm text-muted">converted</span>
      </Headline>
      <div className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)_3.5rem] items-center gap-x-2.5 gap-y-1.5 text-[0.8125rem]">
        {steps.map((s, i) => {
          const kept = (s.entered_users / first) * 100;
          const prev = i === 0 ? 100 : (steps[i - 1].entered_users / first) * 100;
          return (
            <React.Fragment key={i}>
              <span className={`truncate ${i === worst ? "font-semibold" : ""}`} title={humanizeEvent(s.event_name)}>
                {humanizeEvent(s.event_name)}
              </span>
              <span className="flex h-3.5 overflow-hidden rounded-[3px]" aria-hidden="true">
                <span className="bg-brand" style={{ width: `${kept}%` }} />
                <span className={i === worst ? "hatch-drop-strong" : "hatch-drop"} style={{ width: `${Math.max(0, prev - kept)}%` }} />
              </span>
              <span className="tabular text-right">{formatCount(s.entered_users)}</span>
            </React.Fragment>
          );
        })}
      </div>
      {worst > 0 && (
        <p className="text-[0.8125rem] text-danger">
          Biggest drop: {formatCount(steps[worst].dropped_users)} users before {humanizeEvent(steps[worst].event_name)}.
        </p>
      )}
    </div>
  );
};

const TrendMini: React.FC<{ data: InsightsResult }> = ({ data }) => {
  const series = (data.formula_series.length ? data.formula_series : data.series).slice(0, 3);
  const main = series[0];
  if (!main || main.points.length === 0) {
    return <p className="py-6 text-sm text-muted">No events in the selected dates.</p>;
  }
  const prev = data.previous_series.find((p) => p.label === main.label && p.breakdown_key === main.breakdown_key);
  const total = main.overall_value ?? main.points.reduce((a, p) => a + (p.value ?? 0), 0);
  const prevTotal = prev ? prev.overall_value ?? prev.points.reduce((a, p) => a + (p.value ?? 0), 0) : null;
  const change = prevTotal ? ((total - prevTotal) / prevTotal) * 100 : null;

  const W = 420;
  const H = 110;
  const max = Math.max(1, ...series.flatMap((s) => s.points.map((p) => p.value ?? 0)));
  const n = main.points.length;
  const path = (pts: { value: number | null }[]) =>
    pts
      .map((p, i) => {
        const x = n === 1 ? W / 2 : (i / (n - 1)) * W;
        const y = H - 6 - ((p.value ?? 0) / max) * (H - 16);
        return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");

  return (
    <div className="space-y-2">
      <Headline value={formatCount(Math.round(total))}>
        {change !== null && Number.isFinite(change) && (
          <span className={`text-sm font-semibold ${change >= 0 ? "text-ok" : "text-danger"}`}>
            {change >= 0 ? "+" : "−"}
            {Math.abs(change).toFixed(0)}%
          </span>
        )}
        <span className="text-sm text-muted">{change !== null ? "vs previous period" : seriesLabel(main.label)}</span>
      </Headline>
      {n > 1 ? (
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label={`${main.label} over time`}>
          <line x1="0" y1={H - 6} x2={W} y2={H - 6} stroke="rgb(var(--line-strong))" />
          {prev && prev.points.length === n && (
            <path d={path(prev.points)} fill="none" stroke="rgb(var(--faint))" strokeWidth={1.5} strokeDasharray="4 4" />
          )}
          {series.map((s, i) => (
            <path
              key={`${s.label}-${s.breakdown_key}`}
              d={path(s.points)}
              fill="none"
              stroke={SERIES_COLORS[i]}
              strokeWidth={2.5}
              strokeLinejoin="round"
            />
          ))}
        </svg>
      ) : null}
      {series.length > 1 && (
        <div className="flex flex-wrap gap-3 text-xs text-ink-2">
          {series.map((s, i) => (
            <span key={`${s.label}-${s.breakdown_key}`} className="inline-flex items-center gap-1.5">
              <span className="h-[3px] w-3.5" style={{ background: SERIES_COLORS[i] }} />
              {s.breakdown_key ? `${seriesLabel(s.label)}: ${s.breakdown_key}` : seriesLabel(s.label)}
            </span>
          ))}
        </div>
      )}
    </div>
  );
};

const RetentionMini: React.FC<{ data: RetentionResult }> = ({ data }) => {
  const rows = data.cohorts.filter((c) => c.cohort_size > 0);
  if (rows.length === 0) {
    return <p className="py-6 text-sm text-muted">Nobody started in the selected dates.</p>;
  }
  const periods = Math.min(8, Math.max(...rows.map((r) => r.retained_pcts.length)));
  // Average each day over the start dates old enough to have reached it.
  const avg = Array.from({ length: periods }, (_, d) => {
    const reached = rows.filter((_, idx) => idx <= rows.length - 1 - d || d === 0);
    const vals = reached.map((r) => r.retained_pcts[d]).filter((v) => typeof v === "number");
    return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  });
  const lastIdx = avg.reduce<number>((acc, v, i) => (v !== null && i > 0 ? i : acc), 0);
  return (
    <div className="space-y-2.5">
      <Headline value={`${(avg[lastIdx] ?? 0).toFixed(0)}%`}>
        <span className="text-sm text-muted">back on day {lastIdx}</span>
      </Headline>
      <div className="grid gap-[3px] text-center text-[11px]" style={{ gridTemplateColumns: `repeat(${periods}, minmax(0, 1fr))` }}>
        {avg.map((v, i) => {
          const a = v === null ? 0 : Math.min(1, v / 100);
          return (
            <span
              key={i}
              title={v === null ? "Not reached yet" : `Day ${i}: ${v.toFixed(1)}%`}
              className={`tabular rounded py-1.5 ${i === 0 ? "bg-sunken text-ink-2" : a > 0.55 ? "text-on-brand" : "text-ink"}`}
              style={i === 0 || v === null ? undefined : { backgroundColor: `rgb(var(--brand) / ${0.08 + a * 0.85})` }}
            >
              {v === null ? "" : v.toFixed(0)}
            </span>
          );
        })}
      </div>
      <p className="text-xs text-muted">Average across start dates, % of users, day 0 to {periods - 1}</p>
    </div>
  );
};

/** Mixpanel-style date preset bar for report pages. */
export const RangePresets: React.FC<{ days: number; onChange: (days: number) => void }> = ({ days, onChange }) => (
  <div role="group" aria-label="Date range" className="inline-flex rounded-lg border border-line bg-panel p-0.5 text-[0.8125rem]">
    {RANGE_OPTIONS.map((o) => (
      <button
        key={o.days}
        type="button"
        aria-pressed={days === o.days}
        onClick={() => onChange(o.days)}
        className={`rounded-md px-2.5 py-1 font-medium ${days === o.days ? "bg-brand text-on-brand" : "text-ink-2 hover:bg-sunken"}`}
      >
        {o.days}D
      </button>
    ))}
  </div>
);
