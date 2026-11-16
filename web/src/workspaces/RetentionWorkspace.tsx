import React, { useCallback, useEffect, useState } from "react";
import { CalendarCheck } from "lucide-react";
import { api, default14DayRange, RetentionResult } from "../api/client";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";
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

  const runRetention = useCallback(async () => {
    setLoading(true);
    setError(null);
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
    } catch (err) {
      setError(errorMessage(err));
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

  const stickiness = mode === "frequency_stickiness";
  const cohorts = result?.cohorts ?? [];

  return (
    <div className="space-y-5">
      <PageHeader
        title="Retention"
        description="How many users come back after their first action."
        actions={
          <button type="button" onClick={() => void runRetention()} disabled={loading} className={btn.primary}>
            {loading ? "Running…" : "Run"}
          </button>
        }
      />

      <Panel>
        <div className="flex flex-wrap items-end gap-4">
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
              className="flex flex-wrap rounded-md border border-line-strong p-0.5"
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

      {error && <ErrorNotice message={error} onRetry={() => void runRetention()} />}

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
              body={`Nobody did ${humanizeEvent(startEvent)} in the last 14 days. Pick a different first action.`}
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
    </div>
  );
};
