import React, { useCallback, useEffect, useRef, useState } from "react";
import { Download, LineChart, Plus, RefreshCw, Trash2 } from "lucide-react";
import { api, InsightsResult, isAbortError, SeriesOutput } from "../api/client";
import { buildCsv } from "../lib/csv";
import { downloadTextFile, errorMessage, formatDate, formatDateTime, humanizeEvent } from "../lib/format";
import { SaveToDashboard, useSavedReport } from "../dashboards/SaveToDashboard";
import { AnnotationsList, useAnnotations } from "../components/Annotations";
import { RangePresets, RangeSel, rangeFor, rangePhrase } from "../dashboards/ReportCard";
import {
  btn,
  EmptyState,
  ErrorNotice,
  field,
  fieldLabel,
  PageHeader,
  Panel,
  RanIn,
  ReportLayout,
  ChartSkeleton,
  SortHeader,
  useSort,
} from "../components/ui";
import { FilterPanel, EMPTY_FILTER, FilterDraft, describeFilter, fromGlobalFilter, toGlobalFilter, useCohortNames } from "../components/FilterPanel";

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  numericProps: string[];
  refreshToken: number;
}

interface SeriesDraft {
  label: string;
  eventName: string;
  metricKind: string;
  propName: string;
}

// Theme tokens, so the chart follows light and dark mode.
const COLORS = ["brand", "info", "warn", "ink-2", "muted"] /* no orange: it means drop-offs */.map((v) => `rgb(var(--${v}))`);
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
const CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "SGD", "BRL"];

function sanitizeCurrency(raw: unknown): string {
  return typeof raw === "string" && CURRENCIES.includes(raw) ? raw : "";
}

function nextSeriesLabel(existing: SeriesDraft[]): string {
  const used = new Set(existing.map((s) => s.label));
  for (let i = 0; i < 26; i++) {
    const ch = String.fromCharCode(65 + i);
    if (!used.has(ch)) return ch;
  }
  return `S${existing.length + 1}`;
}

/** Inverse of buildMetricSpec, for opening a saved report. */
function metricKindOf(m: { type?: string; quantile?: number } | undefined): string {
  if (!m?.type) return "uniques";
  if (m.type === "prop_sum") return "sum";
  if (m.type === "prop_avg") return "avg";
  if (m.type === "prop_percentile") return m.quantile === 0.5 ? "p50" : m.quantile === 0.99 ? "p99" : "p90";
  return m.type;
}

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
  return v.toLocaleString(undefined, { maximumFractionDigits: digits });
}

type Interval = "hour" | "day" | "week" | "month";
const INTERVAL_NOUN: Record<Interval, string> = { hour: "hour", day: "day", week: "week", month: "month" };

/** Label for one chart bucket at the report's granularity (weeks are labelled by their Monday). */
function formatBucket(ts: number, interval: Interval = "day"): string {
  if (interval === "hour") return formatDateTime(ts, { day: "numeric", month: "short", hour: "numeric" });
  if (interval === "month") return formatDate(ts, { month: "short", year: "numeric" });
  return formatDate(ts, { day: "numeric", month: "short" });
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

function defaultSeriesFromCatalog(events: string[], props: string[]): SeriesDraft[] {
  const clean = events.filter((e) => !e.startsWith("$"));
  if (clean.length === 0) return [];
  const defaultProp = props[0] ?? "";
  return [{ label: "A", eventName: clean[0], metricKind: "uniques", propName: defaultProp }];
}

export const InsightsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  availableProps,
  numericProps,
  refreshToken,
}) => {
  const [series, setSeries] = useState<SeriesDraft[]>(() =>
    defaultSeriesFromCatalog(availableEvents, numericProps),
  );
  const [formula, setFormula] = useState<string>("");
  const [debouncedFormula, setDebouncedFormula] = useState<string>("");
  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedFormula(formula), 350);
    return () => window.clearTimeout(t);
  }, [formula]);
  const [currency, setCurrency] = useState<string>("");
  const safeCurrency = sanitizeCurrency(currency);
  // Currency only dresses property sums/averages; counts of users stay plain numbers.
  const money = Boolean(safeCurrency) && series.every((s) => PROP_METRICS.includes(s.metricKind));
  const fmt = (v: number | null | undefined, digits = 2) =>
    money && v !== null && v !== undefined && Number.isFinite(v)
      ? v.toLocaleString(undefined, { style: "currency", currency: safeCurrency, maximumFractionDigits: digits > 0 ? 2 : 0 })
      : formatValue(v, digits);
  const [breakdown, setBreakdown] = useState<string>("");
  const [interval, setInterval_] = useState<Interval>("day");
  const [comparePrev, setComparePrev] = useState<boolean>(false);
  const [chartType, setChartType] = useState<"line" | "bar" | "number">("line");
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const sort = useSort<"metric" | "segment" | "overall">("overall", true, ["metric", "segment"]);
  const [filter, setFilter] = useState<FilterDraft>(EMPTY_FILTER);
  const [result, setResult] = useState<InsightsResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSeries((prev) =>
      prev.length > 0 ? prev : defaultSeriesFromCatalog(availableEvents, numericProps),
    );
  }, [availableEvents, numericProps]);

  const buildQuery = useCallback(
    (formulaOverride?: string) => {
      const f = (formulaOverride ?? formula).trim();
      return {
        interval,
        series: series.map((s) => ({
          label: s.label,
          event_name: s.eventName,
          metric: buildMetricSpec(s.metricKind, s.propName),
          filters: [],
        })),
        breakdown_by: breakdown ? [breakdown] : [],
        compare_previous_period: comparePrev,
        formula: f ? f : null,
        global_filter: toGlobalFilter(filter),
        // Display only; the engine ignores it.
        display_currency: safeCurrency || undefined,
      };
    },
    [interval, series, formula, breakdown, comparePrev, filter, safeCurrency],
  );

  const [range, setRange] = useState<RangeSel>("14");
  const reqId = useRef(0);
  const queryAbortRef = useRef<AbortController | null>(null);
  const openError = useSavedReport(
    projectId,
    (q) => {
      const s = q as { series?: { label: string; event_name: string; metric?: { type?: string; prop?: string; quantile?: number } }[];
        breakdown_by?: string[]; compare_previous_period?: boolean; formula?: string | null };
      if (s.series?.length)
        setSeries(s.series.map((x) => ({ label: x.label, eventName: x.event_name, metricKind: metricKindOf(x.metric), propName: x.metric?.prop ?? numericProps[0] ?? "" })));
      setCurrency(sanitizeCurrency((q as { display_currency?: unknown }).display_currency));
      setBreakdown(s.breakdown_by?.[0] ?? "");
      const iv = (q as { interval?: string }).interval;
      setInterval_(iv === "hour" || iv === "week" || iv === "month" ? iv : "day");
      setComparePrev(!!s.compare_previous_period);
      const nextFormula = s.formula ?? "";
      setFormula(nextFormula);
      setDebouncedFormula(nextFormula);
      setFilter(fromGlobalFilter((q as { global_filter?: unknown }).global_filter));
    },
    setRange,
    { query: buildQuery, range },
  );

  const buildQueryRef = useRef(buildQuery);
  buildQueryRef.current = buildQuery;

  const runQuery = useCallback(async () => {
    if (series.length === 0 || series.some((s) => !s.eventName)) {
      setResult(null);
      return;
    }
    queryAbortRef.current?.abort();
    const ctrl = new AbortController();
    queryAbortRef.current = ctrl;
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryInsights(
        projectId,
        { time_range: rangeFor(range), ...buildQueryRef.current(debouncedFormula) },
        ctrl.signal,
      );
      // A cohort split comes back as in_cohort / not_in_cohort; the summary names the cohort.
      const cohortKey = (k: string) => (k === "in_cohort" ? "In cohort" : k === "not_in_cohort" ? "Not in cohort" : k);
      if (id === reqId.current && !ctrl.signal.aborted)
        setResult({
          ...res,
          series: res.series.map((x) => ({ ...x, breakdown_key: cohortKey(x.breakdown_key) })),
          previous_series: res.previous_series.map((x) => ({ ...x, breakdown_key: cohortKey(x.breakdown_key) })),
        });
    } catch (err) {
      if (!isAbortError(err) && id === reqId.current && !ctrl.signal.aborted) setError(errorMessage(err));
    } finally {
      if (id === reqId.current && !ctrl.signal.aborted) setLoading(false);
    }
  }, [projectId, interval, series, debouncedFormula, breakdown, comparePrev, filter, safeCurrency, range]);

  useEffect(() => {
    void runQuery();
    return () => {
      queryAbortRef.current?.abort();
    };
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
    downloadTextFile(`trends_${projectId}.csv`, buildCsv(rows), "text/csv;charset=utf-8");
  };

  const displayedSeries = result
    ? (formula.trim() && result.formula_series.length > 0 ? result.formula_series : result.series).slice(0, 6)
    : [];
  const shownRange = rangeFor(range);
  const notes = useAnnotations(projectId, shownRange.start_ts_ms, shownRange.end_ts_ms);
  const buckets = result?.buckets_ts_ms ?? [];
  // Bucket index each annotation falls in (the last bucket starting at or before it).
  const noteMarks = notes.list
    .map((a) => {
      let i = -1;
      for (let j = 0; j < buckets.length && buckets[j] <= a.ts_ms; j++) i = j;
      return { a, i };
    })
    .filter((m) => m.i >= 0);
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

  const cohortNames = useCohortNames(projectId);
  const breakdownOptions = Array.from(
    new Set([...availableProps, breakdown].filter((p) => p && !p.startsWith("$cohort:"))),
  );
  const cohortName = (dim: string) =>
    cohortNames.find((c) => `$cohort:${c.id}` === dim)?.name ?? dim.slice("$cohort:".length);
  const breakdownLabel = (dim: string) =>
    dim.startsWith("$cohort:") ? `in cohort ${cohortName(dim)}` : dim.replace(/_/g, " ");
  const eventOptions = Array.from(
    new Set([...availableEvents.filter((e) => !e.startsWith("$")), ...series.map((s) => s.eventName)].filter(Boolean)),
  );

  const reportTitle =
    series.length === 0
      ? "Trends"
      : formula.trim()
        ? formula
            .trim()
            .replace(/\b([A-Z])\b/g, (m) => {
              const d = series.find((x) => x.label === m);
              return d ? humanizeEvent(d.eventName) : m;
            })
            .replace(/\*/g, "×")
        : series.map((s) => humanizeEvent(s.eventName)).join(" and ");

  return (
    <div className="space-y-5">
      <PageHeader
        title={reportTitle}
        description="Trend: how an event or metric changes over time, by segment."
        actions={
          <>
            <RangePresets value={range} onChange={setRange} />
            <SaveToDashboard
              projectId={projectId}
              kind="insights"
              defaultName={reportTitle}
              query={buildQuery}
            />
            <button type="button" onClick={() => void runQuery()} disabled={loading} className={btn.ghost} aria-label="Refresh results">
              <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
            </button>
          </>
        }
      />

      <ReportLayout
        summary={[
          `${series.length} metric${series.length === 1 ? "" : "s"}`,
          formula.trim() ? `formula ${formula.trim()}` : null,
          breakdown ? `by ${breakdownLabel(breakdown)}` : "no breakdown",
          describeFilter(filter),
        ]
          .filter(Boolean)
          .join(" · ")}
        query={<>
      <FilterPanel projectId={projectId} availableProps={availableProps} anchorEvent={series[0]?.eventName ?? ""} value={filter} onChange={setFilter} />
      <Panel
        title="Metrics"
        actions={
          <button
            type="button"
            disabled={eventOptions.length === 0}
            onClick={() =>
              setSeries([
                ...series,
                {
                  label: nextSeriesLabel(series),
                  eventName: eventOptions[0] ?? "",
                  metricKind: "uniques",
                  propName: numericProps[0] ?? "",
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
              className="flex flex-col gap-2 rounded-lg border border-line bg-sunken px-3 py-2.5"
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
                  onChange={(e) =>
                    updateSeries(idx, {
                      metricKind: e.target.value,
                      propName: numericProps.includes(s.propName) ? s.propName : numericProps[0] ?? s.propName,
                    })
                  }
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
                    {Array.from(new Set([s.propName, ...numericProps].filter(Boolean))).map((p) => (
                      <option key={p} value={p}>
                        {humanizeEvent(p)}
                      </option>
                    ))}
                  </select>
                  {numericProps.length === 0 && (
                    <span className="text-xs text-muted">No number properties have arrived yet. Send one, like revenue: 499.</span>
                  )}
                </label>
              )}
              {PROP_METRICS.includes(s.metricKind) && idx === 0 && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Show values as</span>
                  <select value={currency} onChange={(e) => setCurrency(e.target.value)} className={field}>
                    <option value="">Plain number</option>
                    {CURRENCIES.map((c) => (
                      <option key={c} value={c}>
                        {c}
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
                  className={`${btn.ghost} ml-auto mb-0.5 hover:bg-destructive-soft hover:text-destructive`}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
            </div>
          ))}
        </div>

        <div className="grid grid-cols-1 gap-3 pt-4 sm:grid-cols-2 xl:grid-cols-1">
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
            <span className={fieldLabel}>Group by</span>
            <select value={interval} onChange={(e) => setInterval_(e.target.value as Interval)} className={field}>
              <option value="hour">Hour</option>
              <option value="day">Day</option>
              <option value="week">Week</option>
              <option value="month">Month</option>
            </select>
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
              {cohortNames.length > 0 && (
                <optgroup label="In a cohort or not">
                  {cohortNames.map((c) => (
                    <option key={c.id} value={`$cohort:${c.id}`}>
                      {c.name}
                    </option>
                  ))}
                </optgroup>
              )}
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
          </div>
        </div>
      </Panel>
        </>}
      >

      {openError && <ErrorNotice message={openError} />}
      {error && <ErrorNotice message={error} onRetry={() => void runQuery()} />}

      <Panel
        title={chartType === "number" ? "For the whole period" : "Over time"}
        meta={loading && result ? "Updating…" : <RanIn us={result?.exec_time_us} />}
        actions={
            <div className="flex rounded-lg border border-line-strong p-0.5" role="group" aria-label="Chart type">
              {(
                [
                  ["line", "Line"],
                  ["bar", "Bar"],
                  ["number", "Number"],
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
        }
      >
        {!result ? (
          loading ? (
            <ChartSkeleton />
          ) : (
            <EmptyState title="No results yet" body="Pick an event and run the query." icon={LineChart} />
          )
        ) : !hasData ? (
          <EmptyState
            title="No activity in this period"
            body={`None of these events happened ${rangePhrase(range)}. Try a different event or remove the breakdown.`}
            icon={LineChart}
          />
        ) : chartType === "number" ? (
          <ul className="grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-3">
            {displayedSeries.map((s, idx) => {
              const prev = result.previous_series.find((p) => p.label === s.label && p.breakdown_key === s.breakdown_key);
              const change =
                prev?.overall_value && s.overall_value !== null
                  ? ((s.overall_value - prev.overall_value) / prev.overall_value) * 100
                  : null;
              return (
                <li key={`${s.label}-${s.breakdown_key}`} className="border-l-[3px] pl-4" style={{ borderColor: color(idx) }}>
                  <p className="text-sm text-ink-2">
                    {seriesName(s)}
                    {s.breakdown_key && <span className="text-muted">, {s.breakdown_key}</span>}
                  </p>
                  <p className="tabular text-[2.5rem] font-semibold leading-tight tracking-[-0.02em] text-ink">
                    {fmt(s.overall_value)}
                  </p>
                  {change !== null && Number.isFinite(change) && (
                    <p className={`text-sm font-semibold ${change >= 0 ? "text-ok" : "text-danger"}`}>
                      {change >= 0 ? "+" : "−"}
                      {Math.abs(change).toFixed(0)}% <span className="font-normal text-muted">vs the previous period</span>
                    </p>
                  )}
                </li>
              );
            })}
          </ul>
        ) : (
          <>
            <ul className="mb-3 flex flex-wrap gap-x-4 gap-y-1.5 text-sm">
              {displayedSeries.map((s, idx) => (
                <li key={`${s.label}-${s.breakdown_key}`} className="flex items-center gap-1.5">
                  <span className="h-0.5 w-3 rounded" style={{ backgroundColor: color(idx) }} aria-hidden="true" />
                  <span className="font-medium text-ink-2">{seriesName(s)}</span>
                  {s.breakdown_key && <span className="text-muted">{s.breakdown_key}</span>}
                  <span className="tabular text-muted">{fmt(s.overall_value)}</span>
                </li>
              ))}
            </ul>
            <div className="relative h-64 w-full">
              <svg
                viewBox="0 0 900 230"
                className="h-full w-full overflow-visible"
                role="img"
                aria-label="Trend chart; values are listed in the table below"
                onMouseMove={(e) => {
                  const r = e.currentTarget.getBoundingClientRect();
                  const x = ((e.clientX - r.left) / r.width) * 900;
                  const i = Math.floor((x - PLOT.left) / bandW);
                  setHoverIdx(i >= 0 && i < n ? i : null);
                }}
                onMouseLeave={() => setHoverIdx(null)}
              >
                {hoverIdx !== null && (
                  <line
                    x1={xAt(hoverIdx)}
                    x2={xAt(hoverIdx)}
                    y1={PLOT.bottom - PLOT_H}
                    y2={PLOT.bottom}
                    stroke="rgb(var(--line-strong))"
                    strokeWidth={1.5}
                  />
                )}
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
                        {fmt(maxY * frac, maxY >= 10 ? 0 : 1)}
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
                  // The last bucket is today and still filling up, so draw it dotted.
                  const lastI = s.points.length - 1;
                  const tail =
                    lastI > 0 && s.points[lastI].value !== null && s.points[lastI - 1].value !== null
                      ? `M${xAt(lastI - 1)},${yAt(s.points[lastI - 1].value!)} L${xAt(lastI)},${yAt(s.points[lastI].value!)}`
                      : "";
                  s.points.forEach((p, i) => {
                    if (i === lastI && tail) return;
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
                      {tail && <path d={tail} fill="none" stroke={c} strokeWidth={2} strokeDasharray="3 4" strokeLinecap="round" />}
                      {s.points.map((p, i) =>
                        p.value === null ? null : (
                          <circle key={p.bucket_ts_ms} cx={xAt(i)} cy={yAt(p.value)} r={2.5} fill={c}>
                            <title>
                              {`${seriesName(s)}${s.breakdown_key ? `, ${s.breakdown_key}` : ""}: ${fmt(
                                p.value,
                              )} on ${formatBucket(p.bucket_ts_ms, interval)}`}
                            </title>
                          </circle>
                        ),
                      )}
                    </g>
                  );
                })}

                {noteMarks.map(({ a, i }) => (
                  <g key={a.id}>
                    <line
                      x1={xAt(i)}
                      x2={xAt(i)}
                      y1={PLOT.bottom - PLOT_H}
                      y2={PLOT.bottom}
                      stroke="rgb(var(--warn))"
                      strokeWidth={1.5}
                      strokeDasharray="4 3"
                    />
                    <circle cx={xAt(i)} cy={PLOT.bottom - PLOT_H} r={4} fill="rgb(var(--warn))">
                      <title>{`${formatDate(a.ts_ms)}: ${a.text}`}</title>
                    </circle>
                  </g>
                ))}

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
                      {formatBucket(ts, interval)}
                    </text>
                  ),
                )}
              </svg>
              {hoverIdx !== null && (
                <div
                  className="pointer-events-none absolute top-2 z-10 min-w-[11rem] rounded-lg border border-line-strong bg-panel px-3 py-2 text-[0.8125rem] shadow-[0_8px_24px_rgb(0_0_0/0.12)]"
                  style={
                    xAt(hoverIdx) / 900 > 0.6
                      ? { right: `${100 - (xAt(hoverIdx) / 900) * 100 + 1.5}%` }
                      : { left: `${(xAt(hoverIdx) / 900) * 100 + 1.5}%` }
                  }
                >
                  <p className="mb-1.5 font-semibold text-ink">
                    {interval === "day"
                      ? formatDate(buckets[hoverIdx], { weekday: "short", day: "numeric", month: "short" })
                      : formatBucket(buckets[hoverIdx], interval)}
                    {hoverIdx === n - 1 && <span className="font-normal text-muted"> (so far)</span>}
                  </p>
                  <ul className="space-y-1">
                    {displayedSeries.map((s, idx) => (
                      <li key={`${s.label}-${s.breakdown_key}`} className="flex items-center gap-2">
                        <span className="h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: color(idx) }} aria-hidden="true" />
                        <span className="min-w-0 flex-1 truncate text-ink-2">
                          {s.breakdown_key || seriesName(s)}
                        </span>
                        <span className="tabular font-semibold text-ink">{fmt(s.points[hoverIdx]?.value ?? null)}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </>
        )}
        <AnnotationsList projectId={projectId} annotations={notes.list} error={notes.error} onChanged={() => void notes.reload()} />
      </Panel>

      {result && tableRows.length > 0 && (
        <Panel
          title="By segment"
          meta={`Overall is ${rangePhrase(range)}; columns show the last ${Math.min(7, tableBuckets.length)} ${INTERVAL_NOUN[interval]}${Math.min(7, tableBuckets.length) === 1 ? "" : "s"}`}
          actions={
            <button type="button" onClick={exportCsv} className={btn.secondary}>
              <Download className="h-4 w-4" /> Export CSV
            </button>
          }
        >
          <div className="-mx-4 max-h-[28rem] overflow-auto px-4">
            <table className="w-full min-w-[720px] text-left text-sm">
              <thead>
                <tr className="border-b border-line text-xs text-muted">
                  <SortHeader label="Metric" active={sort.key === "metric"} desc={sort.desc} onClick={() => sort.toggle("metric")} />
                  <SortHeader label="Segment" active={sort.key === "segment"} desc={sort.desc} onClick={() => sort.toggle("segment")} />
                  <SortHeader label="Overall" align="right" active={sort.key === "overall"} desc={sort.desc} onClick={() => sort.toggle("overall")} />
                  {tableBuckets.map((ts) => (
                    <th key={ts} className="sticky top-0 z-10 whitespace-nowrap bg-panel px-2 py-2 text-right font-medium">
                      {formatBucket(ts, interval)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {sort.sortBy(tableRows, (r, k) =>
                  k === "metric" ? seriesName(r) : k === "segment" ? r.breakdown_key || "All users" : r.overall_value ?? -Infinity,
                ).map((row, idx) => (
                  <tr key={`${row.label}-${row.breakdown_key}-${idx}`} className="hover:bg-hover">
                    <td className="whitespace-nowrap py-2 pr-4 text-ink">
                      {seriesName(row)}
                    </td>
                    <td className="whitespace-nowrap py-2 pr-4 text-ink-2">{row.breakdown_key || "All users"}</td>
                    <td className="tabular py-2 pl-3 text-right font-semibold text-ink">
                      {fmt(row.overall_value)}
                    </td>
                    {row.points.slice(-7).map((pt) => (
                      <td key={pt.bucket_ts_ms} className="tabular px-2 py-2 text-right text-ink-2">
                        {fmt(pt.value, 1)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
      </ReportLayout>
    </div>
  );
};
