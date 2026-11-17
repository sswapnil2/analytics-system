import React, { useCallback, useEffect, useRef, useState } from "react";
import { CalendarCheck, RefreshCw } from "lucide-react";
import { api, RetentionResult } from "../api/client";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";
import { typicalOrder } from "../lib/eventOrder";
import { SaveToDashboard, useSavedReport } from "../dashboards/SaveToDashboard";
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
} from "../components/ui";
import { FilterPanel, EMPTY_FILTER, FilterDraft, describeFilter, fromGlobalFilter, toGlobalFilter, topValues } from "../components/FilterPanel";

type Unit = "day" | "week" | "month";
const ANY = "$any_event";
const evLabel = (e: string) => (e === ANY ? "Any event" : humanizeEvent(e));
const UNIT_WORD: Record<Unit, string> = { day: "Day", week: "Week", month: "Month" };
const PERIOD_CHOICES: Record<Unit, number[]> = { day: [7, 14, 30], week: [4, 8, 12], month: [3, 6, 12] };

/** When period `p` after a start date ends (the engine buckets calendar days, weeks and months). */
function periodEnd(startMs: number, p: number, unit: Unit): number {
  if (unit === "month") {
    const d = new Date(startMs);
    d.setUTCMonth(d.getUTCMonth() + p + 1);
    return d.getTime();
  }
  return startMs + (p + 1) * (unit === "week" ? 7 : 1) * 86_400_000;
}
/** "done": the period is over; "partial": it has started but not finished; "future": not started. */
function periodState(startMs: number, p: number, unit: Unit, now = Date.now()): "done" | "partial" | "future" {
  if (periodEnd(startMs, p, unit) <= now) return "done";
  return periodEnd(startMs, p - 1, unit) <= now ? "partial" : "future";
}
/** Average % back in each period, weighted by users, over start dates whose period is over. */
function averageCurve(cohorts: RetentionResult["cohorts"], numPeriods: number, unit: Unit): (number | null)[] {
  return Array.from({ length: numPeriods + 1 }, (_, d) => {
    let users = 0;
    let back = 0;
    for (const c of cohorts) {
      if (d > 0 && periodState(c.cohort_start_ts_ms, d, unit) !== "done") continue;
      users += c.cohort_size;
      back += c.retained_counts[d] ?? 0;
    }
    return users ? (back / users) * 100 : null;
  });
}

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  refreshToken: number;
}

type Mode = "n_day" | "unbounded" | "frequency_stickiness";

const modes = (unit: Unit): [Mode, string][] => [
  ["n_day", `Returned in ${UNIT_WORD[unit].toLowerCase()} N`],
  ["unbounded", `Returned in or after ${UNIT_WORD[unit].toLowerCase()} N`],
  ["frequency_stickiness", "Days active per week"],
];

export const RetentionWorkspace: React.FC<Props> = ({ projectId, availableEvents, availableProps, refreshToken }) => {
  const cleanEvents = availableEvents.filter((e) => !e.startsWith("$"));
  const [startEvent, setStartEvent] = useState<string>(() => cleanEvents[0] ?? "");
  const [returnEvent, setReturnEvent] = useState<string>(() => cleanEvents[1] ?? cleanEvents[0] ?? "");
  const [mode, setMode] = useState<Mode>("n_day");
  const [numPeriods, setNumPeriods] = useState<number>(14);
  const [unit, setUnit] = useState<Unit>("day");
  const [splitBy, setSplitBy] = useState<string>("");
  const [segments, setSegments] = useState<{ value: string; result: RetentionResult }[] | null>(null);
  const [filter, setFilter] = useState<FilterDraft>(EMPTY_FILTER);
  const [result, setResult] = useState<RetentionResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const clean = availableEvents.filter((e) => !e.startsWith("$"));
    if (clean.length === 0) return;
    setStartEvent((prev) => prev || clean[0]);
    setReturnEvent((prev) => prev || clean[1] || clean[0]);
    // Default to "did the usual first action, then came back and did it again", unless someone picked events meanwhile.
    let alive = true;
    void typicalOrder(projectId, clean.slice(0, 4)).then(([first]) => {
      if (!alive || !first) return;
      setStartEvent((prev) => (prev === clean[0] ? first : prev));
      setReturnEvent((prev) => (prev === (clean[1] || clean[0]) ? first : prev));
    });
    return () => {
      alive = false;
    };
  }, [projectId, availableEvents]);

  const buildQuery = useCallback(
    () => ({
      start_event: startEvent,
      return_event: returnEvent,
      mode,
      unit,
      num_periods: numPeriods,
      global_filter: toGlobalFilter(filter),
      // Studio-only: the split runs as one query per value; the engine ignores this field.
      split_by: splitBy || undefined,
    }),
    [startEvent, returnEvent, mode, unit, numPeriods, filter, splitBy],
  );

  const [range, setRange] = useState<RangeSel>("14");
  const reqId = useRef(0);
  const openError = useSavedReport(
    projectId,
    (q) => {
      const s = q as { start_event?: string; return_event?: string; mode?: Mode; num_periods?: number; unit?: Unit; split_by?: string };
      if (s.unit === "week" || s.unit === "month" || s.unit === "day") setUnit(s.unit);
      setSplitBy(s.split_by ?? "");
      if (s.start_event) setStartEvent(s.start_event);
      if (s.return_event) setReturnEvent(s.return_event);
      if (s.mode) setMode(s.mode);
      if (s.num_periods) setNumPeriods(s.num_periods);
      setFilter(fromGlobalFilter((q as { global_filter?: unknown }).global_filter));
    },
    setRange,
  );

  const runRetention = useCallback(async () => {
    if (!startEvent || !returnEvent) {
      setResult(null);
      return;
    }
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const body = { time_range: rangeFor(range), ...buildQuery() };
      const res = await api.queryRetention(projectId, body);
      if (id === reqId.current) setResult(res);
      if (!splitBy) {
        setSegments(null);
      } else {
        // One retention query per common value, each with the split added to the filter.
        const values = await topValues(projectId, startEvent === ANY ? returnEvent : startEvent, splitBy, 6);
        const rows = await Promise.all(
          values.map(async (value) => ({
            value,
            result: await api.queryRetention(projectId, {
              ...body,
              global_filter: {
                ...body.global_filter,
                predicates: [...body.global_filter.predicates, { prop_name: splitBy, filter: { op: "eq", val: value } }],
              },
            }),
          })),
        );
        if (id === reqId.current) setSegments(rows);
      }
    } catch (err) {
      if (id === reqId.current) setError(errorMessage(err));
    } finally {
      if (id === reqId.current) setLoading(false);
    }
  }, [projectId, buildQuery, range, startEvent, returnEvent, splitBy]);

  useEffect(() => {
    void runRetention();
  }, [runRetention, refreshToken]);

  const eventOptions = Array.from(
    new Set([ANY, ...availableEvents.filter((e) => !e.startsWith("$")), startEvent, returnEvent].filter(Boolean)),
  );
  const MODES = modes(unit);
  const word = UNIT_WORD[unit];

  const stickiness = mode === "frequency_stickiness";
  const cohorts = result?.cohorts ?? [];
  const reportTitle =
    startEvent && returnEvent
      ? `${evLabel(startEvent)}, then ${evLabel(returnEvent)}`
      : "Retention";

  return (
    <div className="space-y-5">
      <PageHeader
        title={reportTitle}
        description="Retention: how many users come back after their first action."
        actions={
          <>
            <RangePresets value={range} onChange={setRange} />
            <SaveToDashboard
              projectId={projectId}
              kind="retention"
              defaultName={reportTitle}
              query={buildQuery}
            />
            <button type="button" onClick={() => void runRetention()} disabled={loading} className={btn.ghost} aria-label="Refresh results">
              <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
            </button>
          </>
        }
      />

      <ReportLayout
        summary={`${evLabel(startEvent)}, then ${evLabel(returnEvent)} · ${
          MODES.find(([k]) => k === mode)?.[1].toLowerCase() ?? ""
        }${splitBy ? ` · split by ${humanizeEvent(splitBy)}` : ""}${describeFilter(filter) ? ` · ${describeFilter(filter)}` : ""}`}
        query={<>
      <FilterPanel projectId={projectId} availableProps={availableProps} anchorEvent={startEvent === ANY ? returnEvent : startEvent} value={filter} onChange={setFilter} />
      <Panel title="Query">
        <div className="flex flex-col gap-4">
          <label className="flex min-w-0 flex-col gap-1">
            <span className={fieldLabel}>First action</span>
            <select value={startEvent} onChange={(e) => setStartEvent(e.target.value)} className={field}>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {evLabel(ev)}
                </option>
              ))}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1">
            <span className={fieldLabel}>Came back and did</span>
            <select value={returnEvent} onChange={(e) => setReturnEvent(e.target.value)} className={field}>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {evLabel(ev)}
                </option>
              ))}
            </select>
          </label>
          <fieldset className="flex flex-col gap-1.5">
            <legend className={`${fieldLabel} mb-1.5`}>Count users who</legend>
            {MODES.map(([k, text]) => (
              <label key={k} className="flex cursor-pointer items-center gap-2.5 py-0.5 text-sm text-ink">
                <input
                  type="radio"
                  name="retention-mode"
                  value={k}
                  checked={mode === k}
                  onChange={() => setMode(k)}
                  className="h-4 w-4 accent-brand"
                />
                {text}
              </label>
            ))}
          </fieldset>
          <div className="grid grid-cols-2 gap-2">
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>Measure in</span>
              <select
                value={unit}
                onChange={(e) => {
                  const u = e.target.value as Unit;
                  setUnit(u);
                  setNumPeriods(PERIOD_CHOICES[u][1]);
                }}
                className={field}
              >
                <option value="day">Days</option>
                <option value="week">Weeks</option>
                <option value="month">Months</option>
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>Show</span>
              <select value={numPeriods} onChange={(e) => setNumPeriods(Number(e.target.value))} className={field}>
                {/* A saved report may carry a length the menu doesn't offer; show it rather than a wrong one. */}
                {[...new Set([...PERIOD_CHOICES[unit], numPeriods])].sort((x, y) => x - y).map((n) => (
                  <option key={n} value={n}>{n} {word.toLowerCase()}s</option>
                ))}
              </select>
            </label>
          </div>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Split by</span>
            <select value={splitBy} onChange={(e) => setSplitBy(e.target.value)} className={field}>
              <option value="">No split</option>
              {Array.from(new Set([...availableProps, splitBy].filter(Boolean))).map((p) => (
                <option key={p} value={p}>
                  {humanizeEvent(p)}
                </option>
              ))}
            </select>
          </label>
        </div>
      </Panel>
        </>}
      >

      {openError && <ErrorNotice message={openError} />}
      {error && <ErrorNotice message={error} onRetry={() => void runRetention()} />}

      {!stickiness && cohorts.length > 0 && (
        <Panel title={`Average return by ${word.toLowerCase()}`} meta={`Weighted by users, only start dates whose ${word.toLowerCase()} is over`}>
          <RetentionCurve cohorts={cohorts} numPeriods={numPeriods} unit={unit} />
        </Panel>
      )}

      {!stickiness && splitBy && segments && segments.length > 0 && (
        <Panel title={`By ${humanizeEvent(splitBy)}`} meta={`Average % back, the ${segments.length} most common values`}>
          <SegmentTable segments={segments} numPeriods={numPeriods} unit={unit} />
        </Panel>
      )}

      <Panel
        title="Users who came back, by start date"
        meta={loading && result ? "Updating…" : <RanIn us={result?.exec_time_us} />}
      >
        {cohorts.length === 0 ? (
          loading ? (
            <ChartSkeleton rows={6} />
          ) : (
            <EmptyState
              icon={CalendarCheck}
              title="No users to follow yet"
              body={`Nobody did ${humanizeEvent(startEvent)} ${rangePhrase(range)}. Pick a different first action.`}
            />
          )
        ) : (
          <div className="-mx-4 overflow-x-auto px-4">
            <table className="w-full border-separate border-spacing-1 text-sm">
              <thead>
                <tr className="text-xs text-muted">
                  <th className="sticky left-0 z-10 bg-panel py-1.5 pr-3 text-left font-medium">Start date</th>
                  <th className="px-2 py-1.5 text-right font-medium">Users</th>
                  {Array.from({ length: numPeriods + 1 }, (_, i) => (
                    <th key={i} className="whitespace-nowrap px-1 py-1.5 text-center font-medium">
                      {stickiness ? `${i + 1}+ days` : `${word} ${i}`}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cohorts.map((row) => {
                  return (
                    <tr key={row.cohort_start_ts_ms}>
                      <td className="sticky left-0 z-10 whitespace-nowrap bg-panel py-1 pr-3 text-ink">
                        {new Date(row.cohort_start_ts_ms).toLocaleDateString(undefined, {
                          day: "numeric",
                          month: "short",
                          timeZone: "UTC",
                        })}
                      </td>
                      <td className="tabular px-2 py-1 text-right text-ink-2">{formatCount(row.cohort_size)}</td>
                      {row.retained_pcts.slice(0, numPeriods + 1).map((pct, cIdx) => {
                        // Later start dates haven't reached later periods; the current period is still filling.
                        const state = stickiness || cIdx === 0 ? "done" : periodState(row.cohort_start_ts_ms, cIdx, unit);
                        if (state === "future") {
                          return <td key={cIdx} aria-label="Not reached yet" />;
                        }
                        if (state === "partial") {
                          return (
                            <td
                              key={cIdx}
                              title={`Still in progress: ${formatCount(row.retained_counts[cIdx])} back so far`}
                              className="tabular rounded border border-dashed border-line-strong px-1.5 py-1.5 text-center text-xs text-faint"
                            >
                              {pct.toFixed(1)}%
                            </td>
                          );
                        }
                        const tip = `${formatCount(row.retained_counts[cIdx])} of ${formatCount(row.cohort_size)} users`;
                        // The first column is the starting group itself, so keep it neutral.
                        if (cIdx === 0) {
                          return (
                            <td
                              key={cIdx}
                              title={tip}
                              className="tabular rounded bg-sunken px-1.5 py-1.5 text-center text-xs text-ink-2"
                            >
                              {pct.toFixed(1)}%
                            </td>
                          );
                        }
                        const v = Math.min(1, Math.max(0, pct / 100));
                        return (
                          <td
                            key={cIdx}
                            title={tip}
                            style={{ backgroundColor: `rgb(var(--brand) / ${0.08 + v * 0.85})` }}
                            className={`tabular rounded px-1.5 py-1.5 text-center text-xs ${
                              v > 0.55 ? "text-on-brand" : "text-ink"
                            }`}
                          >
                            {pct.toFixed(1)}%
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {cohorts.length > 0 && !stickiness && (
          <p className="mt-2 text-xs text-muted">Dashed cells are still in progress and will rise until the {word.toLowerCase()} ends.</p>
        )}
      </Panel>
      </ReportLayout>
    </div>
  );
};

/** Retention split by a property: one row per value, average % back per period. */
const SegmentTable: React.FC<{ segments: { value: string; result: RetentionResult }[]; numPeriods: number; unit: Unit }> = ({
  segments,
  numPeriods,
  unit,
}) => {
  const shown = Math.min(numPeriods, 8);
  return (
    <div className="-mx-4 overflow-x-auto px-4">
      <table className="w-full border-separate border-spacing-1 text-sm">
        <thead>
          <tr className="text-xs text-muted">
            <th className="py-1.5 pr-3 text-left font-medium">Segment</th>
            <th className="px-2 py-1.5 text-right font-medium">Users</th>
            {Array.from({ length: shown }, (_, i) => (
              <th key={i} className="whitespace-nowrap px-1 py-1.5 text-center font-medium">
                {UNIT_WORD[unit]} {i + 1}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {segments.map(({ value, result }) => {
            const curve = averageCurve(result.cohorts, numPeriods, unit);
            const users = result.cohorts.reduce((s, c) => s + c.cohort_size, 0);
            return (
              <tr key={value}>
                <td className="whitespace-nowrap py-1 pr-3 text-ink">{value}</td>
                <td className="tabular px-2 py-1 text-right text-ink-2">{formatCount(users)}</td>
                {curve.slice(1, shown + 1).map((v, i) => {
                  if (v === null) return <td key={i} aria-label="Not reached yet" />;
                  const a = Math.min(1, v / 100);
                  return (
                    <td
                      key={i}
                      style={{ backgroundColor: `rgb(var(--brand) / ${0.08 + a * 0.85})` }}
                      className={`tabular rounded px-1.5 py-1.5 text-center text-xs ${a > 0.55 ? "text-on-brand" : "text-ink"}`}
                    >
                      {v.toFixed(1)}%
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

/** Retention curve: average % back in each period, weighted by users. */
const RetentionCurve: React.FC<{ cohorts: RetentionResult["cohorts"]; numPeriods: number; unit: Unit }> = ({ cohorts, numPeriods, unit }) => {
  const [hover, setHover] = useState<number | null>(null);
  const pts = averageCurve(cohorts, numPeriods, unit);
  const W = 640;
  const H = 180;
  const pad = { l: 36, r: 8, t: 10, b: 24 };
  const max = Math.max(10, ...pts.slice(1).map((v) => v ?? 0)) * 1.1;
  const x = (d: number) => pad.l + (d / numPeriods) * (W - pad.l - pad.r);
  const y = (v: number) => H - pad.b - (v / max) * (H - pad.t - pad.b);
  const known = pts.map((v, d) => [d, v] as const).filter(([d, v]) => d > 0 && v !== null) as [number, number][];
  if (known.length === 0) return <p className="text-sm text-muted">Not enough days have passed to draw a curve.</p>;
  const line = known.map(([d, v], i) => `${i ? "L" : "M"}${x(d).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const area = `${line} L${x(known[known.length - 1][0])},${y(0)} L${x(known[0][0])},${y(0)} Z`;
  const ticks = [0, max / 2, max].map((t) => Math.round(t));
  const hv = hover !== null ? known.find(([d]) => d === hover) : undefined;
  return (
    <div className="relative">
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="w-full"
      role="img"
      aria-label={`Average retention by ${UNIT_WORD[unit].toLowerCase()}; exact values are in the table below`}
      onMouseMove={(e) => {
        const r = e.currentTarget.getBoundingClientRect();
        const vx = ((e.clientX - r.left) / r.width) * W;
        const d = Math.round(((vx - pad.l) / (W - pad.l - pad.r)) * numPeriods);
        setHover(d >= 1 && d <= numPeriods ? d : null);
      }}
      onMouseLeave={() => setHover(null)}
    >
      {ticks.map((t) => (
        <g key={t}>
          <line x1={pad.l} x2={W - pad.r} y1={y(t)} y2={y(t)} stroke="rgb(var(--line))" />
          <text x={pad.l - 6} y={y(t) + 4} textAnchor="end" className="fill-faint text-[11px]">
            {t}%
          </text>
        </g>
      ))}
      <path d={area} fill="rgb(var(--brand) / 0.1)" />
      <path d={line} fill="none" stroke="rgb(var(--brand))" strokeWidth={2.5} strokeLinejoin="round" />
      {hv && <line x1={x(hv[0])} x2={x(hv[0])} y1={pad.t} y2={y(0)} stroke="rgb(var(--line-strong))" strokeWidth={1.5} />}
      {known.map(([d, v]) => (
        <circle key={d} cx={x(d)} cy={y(v)} r={hover === d ? 5 : 3} fill="rgb(var(--brand))" />
      ))}
      {pts.map((_, d) => (
        <text key={d} x={x(d)} y={H - 6} textAnchor="middle" className="fill-faint text-[11px]">
          {d === 0 ? `${UNIT_WORD[unit]} 0` : d}
        </text>
      ))}
    </svg>
    {hv && (
      <div
        className="pointer-events-none absolute top-0 rounded-lg border border-line-strong bg-panel px-3 py-2 text-[0.8125rem] shadow-[0_8px_24px_rgb(0_0_0/0.12)]"
        style={x(hv[0]) / W > 0.6 ? { right: `${100 - (x(hv[0]) / W) * 100 + 2}%` } : { left: `${(x(hv[0]) / W) * 100 + 2}%` }}
      >
        <p className="font-semibold text-ink">{UNIT_WORD[unit]} {hv[0]}</p>
        <p className="tabular text-ink-2">{hv[1].toFixed(1)}% came back</p>
      </div>
    )}
    </div>
  );
};
