import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  api,
  FunnelResult,
  InsightsResult,
  RetentionResult,
  SavedReport,
  TimeRange,
} from "../api/client";
import { btn, ChartSkeleton, field, fieldLabel } from "../components/ui";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";

/**
 * A date range choice: "today", "yesterday", a number of days ("7", "14"…),
 * or "custom:YYYY-MM-DD:YYYY-MM-DD" (inclusive local dates).
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

export function rangeFor(sel: RangeSel): TimeRange {
  const now = Date.now();
  if (sel === "today") return { start_ts_ms: startOfToday(), end_ts_ms: now + 3_600_000 };
  if (sel === "yesterday") return { start_ts_ms: addDays(startOfToday(), -1), end_ts_ms: startOfToday() };
  if (sel.startsWith("custom:")) {
    const [, from, to] = sel.split(":");
    return { start_ts_ms: parseDay(from), end_ts_ms: addDays(parseDay(to), 1) };
  }
  return { start_ts_ms: now - Number(sel) * DAY_MS, end_ts_ms: now + 3_600_000 };
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
  if (sel.startsWith("custom:")) {
    const [, from, to] = sel.split(":");
    return `${fmtDay(parseDay(from))} – ${fmtDay(parseDay(to))}`;
  }
  return `Last ${sel} days`;
}

/** For sentences: "in the last 14 days", "today", "between 3 Nov and 15 Nov". */
export function rangePhrase(sel: RangeSel): string {
  if (sel === "today" || sel === "yesterday") return sel;
  if (sel.startsWith("custom:")) {
    const [, from, to] = sel.split(":");
    return `between ${fmtDay(parseDay(from))} and ${fmtDay(parseDay(to))}`;
  }
  return `in the last ${sel} days`;
}

const iso = (ms: number) => {
  const d = new Date(ms);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
};

/** Date preset bar with a Custom from/to popover. */
export const RangePresets: React.FC<{ value: RangeSel; onChange: (v: RangeSel) => void }> = ({ value, onChange }) => {
  const [open, setOpen] = useState(false);
  const r = rangeFor(value);
  const [from, setFrom] = useState(iso(r.start_ts_ms));
  const [to, setTo] = useState(iso(Math.min(Date.now(), r.end_ts_ms - 1)));
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
  const valid = from && to && from <= to;
  return (
    <div className="relative max-w-full" ref={ref}>
      <div role="group" aria-label="Date range" className="no-scrollbar flex max-w-full overflow-x-auto rounded-lg border border-line bg-panel p-0.5 text-[0.8125rem]">
        {PRESETS.map(([k, label]) => (
          <button key={k} type="button" aria-pressed={value === k} onClick={() => onChange(k)} className={seg(value === k)}>
            {label}
          </button>
        ))}
        <button type="button" aria-pressed={custom} aria-expanded={open} onClick={() => {
            // Start the picker from the range on screen, not the one when the bar mounted.
            if (!open) {
              setFrom(iso(r.start_ts_ms));
              setTo(iso(Math.min(Date.now(), r.end_ts_ms - 1)));
            }
            setOpen((v) => !v);
          }} className={seg(custom)}>
          {custom ? rangeLabel(value) : "Custom"}
        </button>
      </div>
      {open && (
        <form
          className="absolute left-0 top-full z-40 mt-2 flex w-[min(20rem,calc(100vw-2rem))] flex-col gap-3 rounded-xl border border-line-strong bg-panel p-4 shadow-[0_12px_32px_rgb(0_0_0/0.16)]"
          onSubmit={(e) => {
            e.preventDefault();
            if (!valid) return;
            onChange(`custom:${from}:${to}`);
            setOpen(false);
          }}
        >
          <div className="grid grid-cols-2 gap-3">
            <label className="flex flex-col gap-1.5">
              <span className={fieldLabel}>From</span>
              <input type="date" value={from} max={to || undefined} onChange={(e) => setFrom(e.target.value)} className={field} />
            </label>
            <label className="flex flex-col gap-1.5">
              <span className={fieldLabel}>To</span>
              <input type="date" value={to} min={from || undefined} max={iso(Date.now())} onChange={(e) => setTo(e.target.value)} className={field} />
            </label>
          </div>
          {!valid && <p className="text-[0.8125rem] text-danger">Pick a start date on or before the end date.</p>}
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

const SERIES_COLORS = ["rgb(var(--brand))", "rgb(var(--info))", "rgb(var(--warn))"];

type CardResult =
  | { kind: "funnel"; data: FunnelResult }
  | { kind: "insights"; data: InsightsResult }
  | { kind: "retention"; data: RetentionResult };

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

  const run = useCallback(async () => {
    // Only the newest request may write, so a slow 90D reply can't land under a 7D label.
    const id = ++latest.current;
    const set = <T,>(fn: (v: T) => void) => (v: T) => id === latest.current && fn(v);
    setLoading(true);
    setError(null);
    const body = { ...report.query_json, time_range: rangeFor(range) };
    try {
      if (report.report_kind === "funnel") {
        set(setResult)({ kind: "funnel", data: await api.queryFunnel(projectId, body) });
      } else if (report.report_kind === "insights") {
        set(setResult)({
          kind: "insights",
          data: await api.queryInsights(projectId, { ...body, interval: rangeDays(range) > 30 ? "week" : "day" }),
        });
      } else if (report.report_kind === "retention") {
        set(setResult)({ kind: "retention", data: await api.queryRetention(projectId, body) });
      } else {
        set(setError)("Journeys can't be shown on a dashboard yet. Open it in Journeys.");
      }
    } catch (err) {
      set(setError)(errorMessage(err));
    } finally {
      if (id === latest.current) {
        setLoading(false);
        onLoaded?.();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId, report, range]);

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
      <ChartSkeleton />
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
          Biggest drop: {formatCount(lostBefore(worst))} users between {humanizeEvent(steps[worst - 1].event_name)} and {humanizeEvent(steps[worst].event_name)}.
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
