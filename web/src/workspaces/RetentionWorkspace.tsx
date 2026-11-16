import React, { useCallback, useEffect, useRef, useState } from "react";
import { CalendarCheck, RefreshCw } from "lucide-react";
import { api, RetentionResult } from "../api/client";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";
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
  ReportLayout,
  Spinner,
} from "../components/ui";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

type Mode = "n_day" | "unbounded" | "frequency_stickiness";

const MODES: [Mode, string][] = [
  ["n_day", "Returned on day N"],
  ["unbounded", "Returned on or after day N"],
  ["frequency_stickiness", "Days active per week"],
];

export const RetentionWorkspace: React.FC<Props> = ({ projectId, availableEvents, refreshToken }) => {
  const [startEvent, setStartEvent] = useState<string>("App_Open");
  const [returnEvent, setReturnEvent] = useState<string>("Quick_Grocery_Browse");
  const [mode, setMode] = useState<Mode>("n_day");
  const [numPeriods, setNumPeriods] = useState<number>(10);
  const [result, setResult] = useState<RetentionResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const buildQuery = useCallback(
    () => ({ start_event: startEvent, return_event: returnEvent, mode, unit: "day", num_periods: numPeriods }),
    [startEvent, returnEvent, mode, numPeriods],
  );

  const [days, setDays] = useState(14);
  const reqId = useRef(0);

  const runRetention = useCallback(async () => {
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryRetention(projectId, { time_range: rangeForDays(days), ...buildQuery() });
      if (id === reqId.current) setResult(res);
    } catch (err) {
      if (id === reqId.current) setError(errorMessage(err));
    } finally {
      if (id === reqId.current) setLoading(false);
    }
  }, [projectId, buildQuery, days]);

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

  const stickiness = mode === "frequency_stickiness";
  const cohorts = result?.cohorts ?? [];

  return (
    <div className="space-y-5">
      <PageHeader
        title={`${humanizeEvent(startEvent)}, then ${humanizeEvent(returnEvent)}`}
        description="Retention: how many users come back after their first action."
        actions={
          <>
            <RangePresets days={days} onChange={setDays} />
            <SaveToDashboard
              projectId={projectId}
              kind="retention"
              defaultName={`${humanizeEvent(startEvent)}, then ${humanizeEvent(returnEvent)}`}
              query={buildQuery}
            />
            <button type="button" onClick={() => void runRetention()} disabled={loading} className={btn.ghost} aria-label="Refresh results">
              <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
            </button>
          </>
        }
      />

      <ReportLayout
        query={
      <Panel title="Query">
        <div className="flex flex-col gap-4">
          <label className="flex min-w-0 flex-col gap-1">
            <span className={fieldLabel}>First action</span>
            <select value={startEvent} onChange={(e) => setStartEvent(e.target.value)} className={field}>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {humanizeEvent(ev)}
                </option>
              ))}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1">
            <span className={fieldLabel}>Came back and did</span>
            <select value={returnEvent} onChange={(e) => setReturnEvent(e.target.value)} className={field}>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {humanizeEvent(ev)}
                </option>
              ))}
            </select>
          </label>
          <div className="flex flex-col gap-1">
            <span className={fieldLabel} id="retention-mode">
              Count users who
            </span>
            <div
              className="flex flex-wrap rounded-lg border border-line-strong p-0.5"
              role="group"
              aria-labelledby="retention-mode"
            >
              {MODES.map(([k, text]) => (
                <button
                  key={k}
                  type="button"
                  aria-pressed={mode === k}
                  onClick={() => setMode(k)}
                  className={`rounded px-2.5 py-1 text-sm ${
                    mode === k ? "bg-brand text-on-brand" : "text-muted hover:text-ink"
                  }`}
                >
                  {text}
                </button>
              ))}
            </div>
          </div>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Days to show</span>
            <select value={numPeriods} onChange={(e) => setNumPeriods(Number(e.target.value))} className={field}>
              <option value={7}>7 days</option>
              <option value={10}>10 days</option>
              <option value={13}>13 days</option>
            </select>
          </label>
        </div>
      </Panel>
        }
      >

      {error && <ErrorNotice message={error} onRetry={() => void runRetention()} />}

      {!stickiness && cohorts.length > 0 && (
        <Panel title="Average return by day" meta="Weighted by users, only start dates old enough to reach each day">
          <RetentionCurve cohorts={cohorts} numPeriods={numPeriods} />
        </Panel>
      )}

      <Panel
        title="Users who came back, by start date"
        meta={loading && result ? "Updating…" : <RanIn us={result?.exec_time_us} />}
      >
        {cohorts.length === 0 ? (
          loading ? (
            <div className="flex h-48 items-center justify-center">
              <Spinner />
            </div>
          ) : (
            <EmptyState
              icon={CalendarCheck}
              title="No users to follow yet"
              body={`Nobody did ${humanizeEvent(startEvent)} in the last ${days} days. Pick a different first action.`}
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
                      {stickiness ? `${i + 1}+ days` : `Day ${i}`}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cohorts.map((row, rIdx) => {
                  // Later cohorts have not had time to reach later days yet.
                  const maxValidPeriod = stickiness ? numPeriods : Math.max(0, cohorts.length - 1 - rIdx);
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
                        if (cIdx > maxValidPeriod && row.retained_counts[cIdx] === 0) {
                          return <td key={cIdx} aria-label="Not reached yet" />;
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
      </Panel>
      </ReportLayout>
    </div>
  );
};

/** Mixpanel-style retention curve: average % back on each day, weighted by users. */
const RetentionCurve: React.FC<{ cohorts: RetentionResult["cohorts"]; numPeriods: number }> = ({ cohorts, numPeriods }) => {
  const pts: (number | null)[] = Array.from({ length: numPeriods + 1 }, (_, d) => {
    let users = 0;
    let back = 0;
    cohorts.forEach((c, i) => {
      if (d > 0 && d > cohorts.length - 1 - i) return; // not reached yet
      users += c.cohort_size;
      back += c.retained_counts[d] ?? 0;
    });
    return users ? (back / users) * 100 : null;
  });
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
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label="Average retention by day; exact values are in the table below">
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
      {known.map(([d, v]) => (
        <circle key={d} cx={x(d)} cy={y(v)} r={3} fill="rgb(var(--brand))">
          <title>{`Day ${d}: ${v.toFixed(1)}%`}</title>
        </circle>
      ))}
      {pts.map((_, d) => (
        <text key={d} x={x(d)} y={H - 6} textAnchor="middle" className="fill-faint text-[11px]">
          {d === 0 ? "Day 0" : d}
        </text>
      ))}
    </svg>
  );
};
