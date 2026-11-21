import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  api,
  FlowsResult,
  FunnelResult,
  InsightsResult,
  isAbortError,
  RetentionResult,
  SavedReport,
  TimeRange,
} from "../api/client";
import { btn, ChartSkeleton, field, fieldLabel } from "../components/ui";
import { errorMessage, formatCount, formatDate, formatDateTime, humanizeEvent, segmentName } from "../lib/format";
import { ChartSeries, TrendChart } from "../components/TrendChart";
import { CHART_KINDS, cardInterval, chartKindBlocker, isAdditive } from "../lib/chart";

/**
 * A date range choice: "today", "yesterday", a number of days ("7", "14"…),
 * or "custom:<startMs>-<endMs>" (end exclusive). The older day-only form
 * "custom:YYYY-MM-DD:YYYY-MM-DD" (inclusive local dates) still parses, since saved links use it.
 */
export type RangeSel = string;

const PRESETS: [RangeSel, string][] = [
  ["today", "Today"],
  ["yesterday", "Yesterday"],
  ["7", "7D"],
  ["14", "14D"],
  ["30", "30D"],
  ["90", "90D"],
];

const DAY_MS = 86_400_000;
const startOfToday = () => new Date(new Date().setHours(0, 0, 0, 0)).getTime();
// Calendar-day step, so a daylight-saving day (23 or 25 hours) still ends at local midnight.
const addDays = (ms: number, n: number) => {
  const d = new Date(ms);
  return d.setDate(d.getDate() + n);
};
const parseDay = (d: string) => new Date(`${d}T00:00:00`).getTime();
const fmtDay = (ms: number) => new Date(ms).toLocaleDateString(undefined, { day: "numeric", month: "short" });
const fmtDayTime = (ms: number) =>
  new Date(ms).toLocaleString(undefined, { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
const isMidnight = (ms: number) => new Date(ms).setHours(0, 0, 0, 0) === ms;

/** The span of a "custom:" choice, in either encoding; null if malformed. */
function customRange(sel: RangeSel): TimeRange | null {
  const ms = /^custom:(\d+)-(\d+)$/.exec(sel);
  if (ms) return { start_ts_ms: Number(ms[1]), end_ts_ms: Number(ms[2]) };
  const day = /^custom:(\d{4}-\d\d-\d\d):(\d{4}-\d\d-\d\d)$/.exec(sel);
  if (day) return { start_ts_ms: parseDay(day[1]), end_ts_ms: addDays(parseDay(day[2]), 1) };
  return null;
}

/** ["3 Nov", "15 Nov"] for whole days (end shown inclusive), else with times. */
function customEnds(r: TimeRange): [string, string] {
  if (isMidnight(r.start_ts_ms) && isMidnight(r.end_ts_ms))
    return [fmtDay(r.start_ts_ms), fmtDay(addDays(r.end_ts_ms, -1))];
  return [fmtDayTime(r.start_ts_ms), fmtDayTime(r.end_ts_ms)];
}

export function rangeFor(sel: RangeSel): TimeRange {
  const now = Date.now();
  if (sel === "today") return { start_ts_ms: startOfToday(), end_ts_ms: now + 3_600_000 };
  if (sel === "yesterday") return { start_ts_ms: addDays(startOfToday(), -1), end_ts_ms: startOfToday() };
  const c = customRange(sel);
  if (c && Number.isFinite(c.start_ts_ms) && Number.isFinite(c.end_ts_ms) && c.start_ts_ms < c.end_ts_ms) return c;
  const days = Number(sel);
  const safeDays = Number.isFinite(days) && days > 0 ? Math.min(days, 3650) : 14;
  return { start_ts_ms: now - safeDays * DAY_MS, end_ts_ms: now + 3_600_000 };
}

/** Length of the range in days, for picking day or week buckets. */
function rangeDays(sel: RangeSel): number {
  const r = rangeFor(sel);
  return Math.max(1, Math.round((r.end_ts_ms - r.start_ts_ms) / DAY_MS));
}

/** "Last 14 days", "Today", "3 Nov – 15 Nov". */
export function rangeLabel(sel: RangeSel): string {
  if (sel === "today") return "Today";
  if (sel === "yesterday") return "Yesterday";
  const c = customRange(sel);
  if (c) return customEnds(c).join(" – ");
  return `Last ${sel} days`;
}

/** For sentences: "in the last 14 days", "today", "between 3 Nov and 15 Nov". */
export function rangePhrase(sel: RangeSel): string {
  if (sel === "today" || sel === "yesterday") return sel;
  const c = customRange(sel);
  if (c) {
    const [a, b] = customEnds(c);
    return `between ${a} and ${b}`;
  }
  return `in the last ${sel} days`;
}

/** Local "YYYY-MM-DDTHH:mm", the value format of <input type="datetime-local">. */
const localInput = (ms: number) => {
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
};
// Parsed as local time because there is no zone suffix.
const parseLocal = (v: string) => new Date(v).getTime();

/** Date preset bar with a Custom from/to popover. */
export const RangePresets: React.FC<{ value: RangeSel; onChange: (v: RangeSel) => void }> = ({ value, onChange }) => {
  const [open, setOpen] = useState(false);
  const r = rangeFor(value);
  const [from, setFrom] = useState(localInput(r.start_ts_ms));
  const [to, setTo] = useState(localInput(Math.min(Date.now(), r.end_ts_ms)));
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    const onDown = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && setOpen(false);
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onDown);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onDown);
    };
  }, [open]);
  const custom = value.startsWith("custom:");
  const seg = (on: boolean) =>
    `whitespace-nowrap rounded-md px-2.5 py-1.5 font-medium ${on ? "bg-brand text-on-brand" : "text-ink-2 hover:bg-hover"}`;
  const valid = from && to && parseLocal(from) < parseLocal(to);
  return (
    <div className="relative max-w-full" ref={ref}>
      <div role="group" aria-label="Date range" className="no-scrollbar flex max-w-full overflow-x-auto rounded-lg border border-line bg-panel p-0.5 text-compact">
        {PRESETS.map(([k, label]) => (
          <button key={k} type="button" aria-pressed={value === k} onClick={() => onChange(k)} className={seg(value === k)}>
            {label}
          </button>
        ))}
        <button type="button" aria-pressed={custom} aria-expanded={open} onClick={() => {
            // Start the picker from the range on screen, not the one when the bar mounted.
            if (!open) {
              setFrom(localInput(r.start_ts_ms));
              setTo(localInput(Math.min(Date.now(), r.end_ts_ms)));
            }
            setOpen((v) => !v);
          }} className={seg(custom)}>
          {custom ? rangeLabel(value) : "Custom"}
        </button>
      </div>
      {open && (
        <form
          className="absolute left-0 top-full z-40 mt-2 flex w-[min(20rem,calc(100vw-2rem))] flex-col gap-3 rounded-xl border border-line-strong bg-panel p-4 shadow-pop motion-safe:animate-pop"
          onSubmit={(e) => {
            e.preventDefault();
            if (!valid) return;
            onChange(`custom:${parseLocal(from)}-${parseLocal(to)}`);
            setOpen(false);
          }}
        >
          <div className="grid gap-3">
            <label className="flex flex-col gap-1.5">
              <span className={fieldLabel}>From</span>
              <input type="datetime-local" value={from} max={to || undefined} onChange={(e) => setFrom(e.target.value)} className={field} />
            </label>
            <label className="flex flex-col gap-1.5">
              <span className={fieldLabel}>To</span>
              <input type="datetime-local" value={to} min={from || undefined} onChange={(e) => setTo(e.target.value)} className={field} />
            </label>
          </div>
          {!valid && <p className="text-compact text-danger">Pick a start before the end.</p>}
          <div className="flex justify-end gap-2">
            <button type="button" className={btn.ghost} onClick={() => setOpen(false)}>
              Cancel
            </button>
            <button type="submit" className={btn.primary} disabled={!valid}>
              Apply
            </button>
          </div>
        </form>
      )}
    </div>
  );
};

export const KIND_LABEL: Record<SavedReport["report_kind"], string> = {
  funnel: "Funnel",
  insights: "Trend",
  retention: "Retention",
  flows: "Journey",
};

/** "Formula((B / A) * 100)" → "(B / A) * 100" */
const seriesLabel = (l: string) => l.replace(/^Formula\((.*)\)$/, "$1");

const SERIES_COLORS = Array.from({ length: 8 }, (_, i) => `rgb(var(--chart-${i + 1}))`);

type CardResult =
  | { kind: "funnel"; data: FunnelResult }
  | { kind: "insights"; data: InsightsResult }
  | { kind: "retention"; data: RetentionResult }
  | { kind: "flows"; data: FlowsResult };

/** Runs a saved report with the dashboard's date range and draws a compact version of it. */
export const ReportCardBody: React.FC<{
  projectId: string;
  report: SavedReport;
  range: RangeSel;
  refreshKey?: number;
  onLoaded?: () => void;
}> = ({ projectId, report, range, refreshKey = 0, onLoaded }) => {
  const [result, setResult] = useState<CardResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const latest = useRef(0);
  const abortRef = useRef<AbortController | null>(null);

  const run = useCallback(async () => {
    // Only the newest request may write, so a slow 90D reply can't land under a 7D label.
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    const id = ++latest.current;
    const set = <T,>(fn: (v: T) => void) => (v: T) => id === latest.current && !ctrl.signal.aborted && fn(v);
    setLoading(true);
    setError(null);
    const body = { ...report.query_json, time_range: rangeFor(range) };
    try {
      if (report.report_kind === "funnel") {
        set(setResult)({ kind: "funnel", data: await api.queryFunnel(projectId, body, ctrl.signal) });
      } else if (report.report_kind === "insights") {
        set(setResult)({
          kind: "insights",
          data: await api.queryInsights(projectId, { ...body, interval: cardInterval(report.query_json.interval, rangeDays(range)) }, ctrl.signal),
        });
      } else if (report.report_kind === "retention") {
        set(setResult)({ kind: "retention", data: await api.queryRetention(projectId, body, ctrl.signal) });
      } else {
        set(setResult)({ kind: "flows", data: await api.queryFlows(projectId, body, ctrl.signal) });
      }
    } catch (err) {
      if (!isAbortError(err)) set(setError)(errorMessage(err));
    } finally {
      if (id === latest.current && !ctrl.signal.aborted) {
        setLoading(false);
        onLoaded?.();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId, report, range]);

  useEffect(() => {
    void run();
    return () => {
      abortRef.current?.abort();
    };
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
      <ChartSkeleton />
    );
  }
  return (
    <div className={loading ? "opacity-60 transition-opacity" : undefined} aria-busy={loading}>
      {result.kind === "funnel" && (
        <FunnelMini data={result.data} splitBy={(report.query_json as { breakdown_by?: string | null }).breakdown_by ?? null} />
      )}
      {result.kind === "flows" && <FlowsMini data={result.data} />}
      {result.kind === "insights" && <TrendMini data={result.data} query={report.query_json} />}
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

const FunnelMini: React.FC<{ data: FunnelResult; splitBy: string | null }> = ({ data, splitBy }) => {
  const steps = data.overall_steps;
  if (steps.length === 0 || steps[0].entered_users === 0) {
    return <p className="py-6 text-sm text-muted">No users entered this funnel in the selected dates.</p>;
  }
  const first = steps[0].entered_users;
  // Loss into step i (users who reached i-1 but not i), so the bold row, strong hatch and sentence agree.
  const lostBefore = (i: number) => steps[i - 1].entered_users - steps[i].entered_users;
  let worst = -1;
  for (let i = 1; i < steps.length; i++) {
    if (lostBefore(i) > 0 && (worst < 0 || lostBefore(i) > lostBefore(worst))) worst = i;
  }
  const last = steps[steps.length - 1];
  return (
    <div className="space-y-3">
      <Headline value={`${last.conversion_from_start_pct.toFixed(1)}%`}>
        <span className="text-sm text-muted">converted</span>
      </Headline>
      <div className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)_3.5rem] items-center gap-x-2.5 gap-y-1.5 text-compact">
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
        <p className="text-compact text-danger">
          Biggest drop: {formatCount(lostBefore(worst))} users between {humanizeEvent(steps[worst - 1].event_name)} and {humanizeEvent(steps[worst].event_name)}.
        </p>
      )}
      {splitBy && data.breakdowns.length > 0 && <SplitMini splitBy={splitBy} groups={data.breakdowns} />}
    </div>
  );
};

/** A split funnel's segments on a card: conversion per segment, biggest segments first. */
const SplitMini: React.FC<{ splitBy: string; groups: FunnelResult["breakdowns"] }> = ({ splitBy, groups }) => {
  const rows = groups
    .map((g) => {
      const started = g.steps[0]?.entered_users ?? 0;
      const done = g.steps[g.steps.length - 1]?.entered_users ?? 0;
      return { name: g.breakdown_value || "(none)", started, rate: started ? (done / started) * 100 : 0 };
    })
    .filter((r) => r.started > 0)
    .sort((a, b) => b.started - a.started)
    .slice(0, 5);
  const best = Math.max(...rows.map((r) => r.rate), 1);
  return (
    <div className="border-t border-line pt-2.5">
      <p className="mb-1.5 text-xs text-muted">By {humanizeEvent(splitBy)}</p>
      <div className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)_3.5rem] items-center gap-x-2.5 gap-y-1 text-compact">
        {rows.map((r) => (
          <React.Fragment key={r.name}>
            <span className="truncate" title={`${r.name}: ${formatCount(r.started)} started`}>
              {r.name}
            </span>
            <span className="h-2 overflow-hidden rounded-[3px] bg-sunken" aria-hidden="true">
              <span className="block h-2 bg-brand" style={{ width: `${(r.rate / best) * 100}%` }} />
            </span>
            <span className="tabular text-right">{r.rate.toFixed(1)}%</span>
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

/** A journey on a card: where users went right after the anchor event. */
const FlowsMini: React.FC<{ data: FlowsResult }> = ({ data }) => {
  const anchor = data.nodes.find((n) => n.step_offset === 0);
  const next = data.nodes.filter((n) => Math.abs(n.step_offset) === 1).sort((a, b) => b.user_count - a.user_count).slice(0, 5);
  if (!anchor || next.length === 0) return <p className="py-6 text-sm text-muted">No journeys in the selected dates.</p>;
  const total = next.reduce((s, n) => s + n.user_count, 0) || 1;
  const name = (e: string) => (e === "$Drop_Off" ? "No further events" : e === "$Other" ? "Other events" : humanizeEvent(e));
  const before = next[0].step_offset < 0;
  return (
    <div className="space-y-2.5">
      <Headline value={formatCount(anchor.user_count)}>
        <span className="text-sm text-muted">users did {humanizeEvent(anchor.event_name)}</span>
      </Headline>
      <p className="text-xs text-muted">{before ? "What they did just before" : "What they did next"}</p>
      <div className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)_3rem] items-center gap-x-2.5 gap-y-1 text-compact">
        {next.map((n) => (
          <React.Fragment key={n.id}>
            <span className="truncate">{name(n.event_name)}</span>
            <span className="h-2 overflow-hidden rounded-[3px] bg-sunken" aria-hidden="true">
              <span className={`block h-2 ${n.event_name === "$Drop_Off" ? "bg-danger" : "bg-brand"}`} style={{ width: `${(n.user_count / total) * 100}%` }} />
            </span>
            <span className="tabular text-right">{Math.round((n.user_count / total) * 100)}%</span>
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

/** Inverse of the trend page's metric spec: "prop_sum" → "sum", for deciding what can stack. */
const metricKindOf = (m: unknown) => {
  const t = (m as { type?: string } | undefined)?.type ?? "uniques";
  return t === "prop_sum" ? "sum" : t;
};

const TrendMini: React.FC<{ data: InsightsResult; query: Record<string, unknown> }> = ({ data, query }) => {
  const formula = data.formula_series.length > 0;
  const series = (formula ? data.formula_series : data.series).slice(0, SERIES_COLORS.length);
  const [hidden, setHidden] = useState<Set<string>>(() => new Set());
  const main = series[0];
  if (!main || main.points.length === 0) {
    return <p className="py-6 text-sm text-muted">No events in the selected dates.</p>;
  }
  const prev = data.previous_series.find((p) => p.label === main.label && p.breakdown_key === main.breakdown_key);
  const total = main.overall_value ?? main.points.reduce((a, p) => a + (p.value ?? 0), 0);
  const prevTotal = prev ? prev.overall_value ?? prev.points.reduce((a, p) => a + (p.value ?? 0), 0) : null;
  const change = prevTotal ? ((total - prevTotal) / prevTotal) * 100 : null;

  // The card draws the chart kind the report was saved with, if this result can show it.
  const q = query as { chart_type?: unknown; breakdown_by?: string[]; series?: { metric?: unknown }[] };
  const saved = q.chart_type === "bar" ? "column" : q.chart_type;
  const wanted = CHART_KINDS.find((c) => c.kind === saved)?.kind ?? "line";
  const additive = isAdditive((q.series ?? []).map((s) => metricKindOf(s.metric)), Boolean(q.breakdown_by?.length), formula);
  const kind = chartKindBlocker(wanted, { seriesCount: series.length, additive }) ? "line" : wanted;
  const split = Boolean(q.breakdown_by?.length);
  const buckets = data.buckets_ts_ms;
  const n = buckets.length;
  // Bucket size from the spacing: hours, days, weeks or months.
  const gap = n < 2 ? 86_400_000 : buckets[1] - buckets[0];
  const step = gap < 86_400_000 ? "hour" : gap < 2 * 86_400_000 ? "day" : gap < 20 * 86_400_000 ? "week" : "month";
  // "A" → its event name, from the saved query.
  const nameOf = (label: string) => {
    const ev = (query as { series?: { label?: string; event_name?: string }[] }).series?.find((x) => x.label === label)?.event_name;
    return ev ? humanizeEvent(ev) : seriesLabel(label);
  };
  const chartSeries: ChartSeries[] = series.map((s, i) => ({
    key: `${s.label}-${s.breakdown_key}`,
    name: split ? segmentName(s.breakdown_key) : nameOf(s.label),
    color: SERIES_COLORS[i],
    points: s.points.map((p) => p.value),
    overall: s.overall_value,
  }));
  const fmt = (v: number | null | undefined) =>
    v === null || v === undefined || !Number.isFinite(v) ? "–" : v.toLocaleString(undefined, { maximumFractionDigits: 1 });

  return (
    <div className="space-y-2">
      <Headline value={formatCount(Math.round(total))}>
        {change !== null && Number.isFinite(change) && (
          <span className={`text-sm font-semibold ${change >= 0 ? "text-ok" : "text-danger"}`}>
            {change >= 0 ? "+" : "−"}
            {Math.abs(change).toFixed(0)}%
          </span>
        )}
        <span className="text-sm text-muted">{change !== null ? "vs previous period" : split ? segmentName(main.breakdown_key) : nameOf(main.label)}</span>
      </Headline>
      {kind !== "number" && (n > 1 || kind === "pie") && (
        <TrendChart
          compact
          kind={kind}
          series={chartSeries}
          buckets={buckets}
          axisLabel={(ts) =>
            step === "hour"
              ? formatDateTime(ts, { hour: "numeric" })
              : step === "month"
                ? formatDate(ts, { month: "short", year: "2-digit" })
                : formatDate(ts, { day: "numeric", month: "short" })
          }
          bucketTitle={(i) =>
            step === "hour"
              ? formatDateTime(buckets[i], { weekday: "short", day: "numeric", month: "short", hour: "numeric" })
              : step === "day"
                ? formatDate(buckets[i], { weekday: "short", day: "numeric", month: "short" })
                : step === "week"
                  ? `Week of ${formatDate(buckets[i], { day: "numeric", month: "short" })}`
                  : formatDate(buckets[i], { month: "long", year: "numeric" })
          }
          fmt={fmt}
          partialLast
          ghost={series.length === 1 && prev ? prev.points.map((p) => p.value) : undefined}
          hidden={series.length > 1 ? hidden : undefined}
          onToggle={
            series.length > 1
              ? (key) =>
                  setHidden((h) => {
                    const next = new Set(h);
                    if (next.has(key)) next.delete(key);
                    else if (chartSeries.some((c) => c.key !== key && !next.has(c.key))) next.add(key);
                    return next;
                  })
              : undefined
          }
          ariaLabel={`${seriesLabel(main.label)} over time`}
        />
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
    const reached = rows.filter((r) => d === 0 || addDays(r.cohort_start_ts_ms, d) <= Date.now());
    const vals = reached.map((r) => r.retained_pcts[d]).filter((v) => typeof v === "number");
    return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  });
  const lastIdx = avg.reduce<number>((acc, v, i) => (v !== null && i > 0 ? i : acc), 0);
  return (
    <div className="space-y-2.5">
      <Headline value={`${(avg[lastIdx] ?? 0).toFixed(0)}%`}>
        <span className="text-sm text-muted">back on day {lastIdx}</span>
      </Headline>
      <div className="grid gap-[3px] text-center text-2xs" style={{ gridTemplateColumns: `repeat(${periods}, minmax(0, 1fr))` }}>
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

/**
 * What a report is compared with: "" (nothing), "previous" (the same length right before the
 * range), or a custom range in `RangeSel` form, e.g. the weeks before a release.
 */
export type CompareSel = string;

export function compareRangeFor(main: TimeRange, sel: CompareSel): TimeRange | null {
  if (!sel) return null;
  if (sel === "previous") {
    const len = main.end_ts_ms - main.start_ts_ms;
    return { start_ts_ms: main.start_ts_ms - len, end_ts_ms: main.start_ts_ms };
  }
  return customRange(sel);
}

/** "3 Nov – 9 Nov" for the compared range. */
export function compareLabel(main: TimeRange, sel: CompareSel): string {
  const r = compareRangeFor(main, sel);
  return r ? customEnds(r).join(" – ") : "";
}

const dayStr = (ms: number) => {
  const d = new Date(ms);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
};

/** "Compare to" picker: nothing, the previous period, or two dates. */
export const CompareControl: React.FC<{ main: TimeRange; value: CompareSel; onChange: (v: CompareSel) => void }> = ({
  main,
  value,
  onChange,
}) => {
  const custom = value.startsWith("custom:");
  const cr = compareRangeFor(main, value);
  const from = cr ? dayStr(cr.start_ts_ms) : dayStr(main.start_ts_ms - (main.end_ts_ms - main.start_ts_ms));
  const to = cr ? dayStr(addDays(cr.end_ts_ms, -1)) : dayStr(addDays(main.start_ts_ms, -1));
  return (
    <div className="flex flex-col gap-1">
      <label className="flex flex-col gap-1">
        <span className={fieldLabel}>Compare to</span>
        <select
          value={custom ? "custom" : value}
          onChange={(e) =>
            onChange(e.target.value === "custom" ? `custom:${from}:${to}` : e.target.value)
          }
          className={field}
        >
          <option value="">No comparison</option>
          <option value="previous">Previous period</option>
          <option value="custom">Specific dates (e.g. before a release)</option>
        </select>
      </label>
      {custom && (
        <div className="flex items-center gap-1.5">
          <input
            type="date"
            aria-label="Compare from"
            value={from}
            onChange={(e) => e.target.value && onChange(`custom:${e.target.value}:${to}`)}
            className={`${field} w-auto`}
          />
          <span className="text-xs text-muted">to</span>
          <input
            type="date"
            aria-label="Compare to"
            value={to}
            onChange={(e) => e.target.value && onChange(`custom:${from}:${e.target.value}`)}
            className={`${field} w-auto`}
          />
        </div>
      )}
    </div>
  );
};
