import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowDown, Plus, RefreshCw, Trash2, X } from "lucide-react";
import { api, DroppedSessionSample, FunnelResult } from "../api/client";
import { SessionReplay } from "../components/WireframePlayer";
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
import {
  displayUserId,
  errorMessage,
  formatCount,
  formatDuration,
  humanizeEvent,
} from "../lib/format";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

type OrderMode = "loose_order" | "strict_order" | "any_order";

const DEFAULT_STEPS = [
  "App_Open",
  "KYC_PAN_Submitted",
  "Video_KYC_Completed",
  "UPI_Autopay_Mandate_Approved",
  "First_Disbursal_Success",
];

const SEGMENT_PROPS = ["platform", "city", "payment_method", "network"];

const WINDOWS: { hours: number; label: string }[] = [
  { hours: 1, label: "1 hour" },
  { hours: 24, label: "1 day" },
  { hours: 168, label: "7 days" },
  { hours: 336, label: "14 days" },
];

const pct = (v: number) => `${v.toFixed(1)}%`;

export const FunnelsWorkspace: React.FC<Props> = ({ projectId, availableEvents, refreshToken }) => {
  const [steps, setSteps] = useState<string[]>(DEFAULT_STEPS);
  const [orderMode, setOrderMode] = useState<OrderMode>("loose_order");
  const [windowHours, setWindowHours] = useState<number>(168);
  const [excludeEvent, setExcludeEvent] = useState<string>("");
  const [holdConstant, setHoldConstant] = useState<string>("");
  const [breakdownBy, setBreakdownBy] = useState<string>("platform");
  const [result, setResult] = useState<FunnelResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [drawerStep, setDrawerStep] = useState<number | null>(null);

  const eventOptions = useMemo(
    () => Array.from(new Set([...DEFAULT_STEPS, ...availableEvents])).filter((e) => !e.startsWith("$")),
    [availableEvents],
  );

  const buildQuery = useCallback(
    () => ({
      steps: steps.map((event_name) => ({ event_name, filters: [], first_time_ever: false })),
      order_mode: orderMode,
      conversion_window_ms: windowHours * 3_600_000,
      exclusions: excludeEvent
        ? [{ event_name: excludeEvent, between_steps: [1, Math.min(2, steps.length - 1)] }]
        : [],
      hold_constant_props: holdConstant ? [holdConstant] : [],
      breakdown_by: breakdownBy || null,
    }),
    [steps, orderMode, windowHours, excludeEvent, holdConstant, breakdownBy],
  );

  const [days, setDays] = useState(14);
  const reqId = useRef(0);

  const runFunnel = useCallback(async () => {
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const res = await api.queryFunnel(projectId, { time_range: rangeForDays(days), ...buildQuery() });
      if (id === reqId.current) setResult(res);
    } catch (err) {
      if (id === reqId.current) setError(errorMessage(err));
    } finally {
      if (id === reqId.current) setLoading(false);
    }
  }, [projectId, buildQuery, days]);

  useEffect(() => {
    void runFunnel();
  }, [runFunnel, refreshToken]);

  const overall = result?.overall_steps ?? [];
  const first = overall[0];
  const last = overall[overall.length - 1];

  // The step that loses the most users is the one worth investigating first.
  const biggestDrop = useMemo(() => {
    let best = -1;
    let bestLost = 0;
    for (let i = 1; i < overall.length; i++) {
      const lost = overall[i - 1].entered_users - overall[i].entered_users;
      if (lost > bestLost) {
        bestLost = lost;
        best = i;
      }
    }
    return best;
  }, [overall]);

  const reportTitle = `${humanizeEvent(steps[0] ?? "")} → ${humanizeEvent(steps[steps.length - 1] ?? "")}`;

  return (
    <div className="space-y-5">
      <PageHeader
        title={reportTitle}
        description="Funnel: where users drop out, with recordings of the ones who left."
        actions={
          <>
            <RangePresets days={days} onChange={setDays} />
            <SaveToDashboard
              projectId={projectId}
              kind="funnel"
              defaultName={reportTitle}
              query={buildQuery}
            />
            <button type="button" onClick={() => void runFunnel()} disabled={loading} className={btn.ghost} aria-label="Refresh results">
              <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
            </button>
          </>
        }
      />

      <ReportLayout
        query={
      <Panel title="Steps">
        <ol className="flex flex-wrap items-center gap-1.5 xl:flex-col xl:items-stretch xl:gap-1">
          {steps.map((st, idx) => (
            <li key={idx} className="flex items-center gap-1.5 xl:flex-col xl:items-stretch xl:gap-1">
              {idx > 0 && <span className="text-xs text-faint xl:pl-2">then</span>}
              <span className="flex items-center gap-1.5 rounded-lg border border-line-strong bg-field py-1 pl-1.5 pr-1">
              <span
                className="flex h-5 w-5 items-center justify-center rounded bg-brand-soft text-xs font-semibold text-brand tabular"
                aria-hidden="true"
              >
                {idx + 1}
              </span>
              <select
                aria-label={`Step ${idx + 1} event`}
                value={st}
                onChange={(e) => {
                  const next = [...steps];
                  next[idx] = e.target.value;
                  setSteps(next);
                }}
                className="min-w-0 flex-1 truncate bg-transparent text-sm text-ink focus:outline-none"
              >
                {eventOptions.map((ev) => (
                  <option key={ev} value={ev}>
                    {humanizeEvent(ev)}
                  </option>
                ))}
              </select>
              {steps.length > 2 && (
                <button
                  type="button"
                  onClick={() => setSteps(steps.filter((_, i) => i !== idx))}
                  className="rounded p-1 text-faint hover:bg-hover hover:text-danger"
                  aria-label={`Remove step ${idx + 1}`}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
              </span>
            </li>
          ))}
          <li>
            <button
              type="button"
              onClick={() => setSteps([...steps, eventOptions.find((e) => !steps.includes(e)) ?? eventOptions[0]])}
              className={btn.ghost}
            >
              <Plus className="h-4 w-4" /> Add step
            </button>
          </li>
        </ol>

        <div className="mt-4 grid grid-cols-1 gap-3 border-t border-line pt-4 sm:grid-cols-2 xl:grid-cols-1">
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Step order</span>
            <select value={orderMode} onChange={(e) => setOrderMode(e.target.value as OrderMode)} className={field}>
              <option value="loose_order">In order, other events allowed between</option>
              <option value="strict_order">In order, nothing in between</option>
              <option value="any_order">Any order</option>
            </select>
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Time to complete</span>
            <select value={windowHours} onChange={(e) => setWindowHours(Number(e.target.value))} className={field}>
              {WINDOWS.map((w) => (
                <option key={w.hours} value={w.hours}>
                  Within {w.label}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Exclude users who did (between steps 2 and 3)</span>
            <select value={excludeEvent} onChange={(e) => setExcludeEvent(e.target.value)} className={field}>
              <option value="">Nothing excluded</option>
              {Array.from(new Set(["NSDL_Gateway_504_Timeout", ...availableEvents]))
                .filter((e) => !e.startsWith("$"))
                .map((ev) => (
                  <option key={ev} value={ev}>
                    {humanizeEvent(ev)}
                  </option>
                ))}
            </select>
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Same value on every step</span>
            <select value={holdConstant} onChange={(e) => setHoldConstant(e.target.value)} className={field}>
              <option value="">Not required</option>
              {SEGMENT_PROPS.map((p) => (
                <option key={p} value={p}>
                  {humanizeEvent(p)}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Split by</span>
            <select value={breakdownBy} onChange={(e) => setBreakdownBy(e.target.value)} className={field}>
              <option value="">No split</option>
              {SEGMENT_PROPS.map((p) => (
                <option key={p} value={p}>
                  {humanizeEvent(p)}
                </option>
              ))}
            </select>
          </label>
        </div>
      </Panel>
        }
      >

      {error && <ErrorNotice message={`The funnel didn't run: ${error}`} onRetry={() => void runFunnel()} />}

      {!result && loading && (
        <Panel>
          <div className="flex h-48 items-center justify-center">
            <Spinner label="Running funnel" />
          </div>
        </Panel>
      )}

      {result && first && last && (
        <Panel title="Conversion" meta={<RanIn us={result.exec_time_us} />}>
          {first.entered_users === 0 ? (
            <EmptyState
              title={`No one did the first step in the last ${days} days`}
              body={`Check that "${humanizeEvent(first.event_name)}" is being sent, or pick a different first step.`}
            />
          ) : (
            <>
              <div className="mb-6 max-w-3xl space-y-1.5">
                <p className="text-xl font-semibold leading-snug tracking-[-0.01em] text-ink">
                  <span className="tabular">{pct(last.conversion_from_start_pct)}</span> of users who did{" "}
                  {humanizeEvent(first.event_name)} went on to {humanizeEvent(last.event_name)} within{" "}
                  {WINDOWS.find((w) => w.hours === windowHours)?.label ?? `${windowHours} hours`}.
                </p>
                {biggestDrop > 0 && (
                  <p className="text-[0.9375rem] text-ink-2">
                    The biggest drop is between {humanizeEvent(overall[biggestDrop - 1].event_name)} and{" "}
                    {humanizeEvent(overall[biggestDrop].event_name)}:{" "}
                    <span className="tabular">
                      {formatCount(overall[biggestDrop - 1].entered_users - overall[biggestDrop].entered_users)}
                    </span>{" "}
                    users, {pct(100 - overall[biggestDrop].conversion_from_prev_pct)} of that step.
                  </p>
                )}
              </div>

              <ol className="space-y-0">
                {overall.map((st, idx) => {
                  const lost = idx > 0 ? overall[idx - 1].entered_users - st.entered_users : 0;
                  const isBiggest = idx === biggestDrop;
                  const keptPct = (st.entered_users / first.entered_users) * 100;
                  const lostPct = idx > 0 ? (lost / first.entered_users) * 100 : 0;
                  return (
                    <li key={st.step_index}>
                      {idx > 0 && lost > 0 && (
                        isBiggest ? (
                          <div className="my-1.5 flex flex-wrap items-center gap-3 rounded-lg bg-danger-soft px-3.5 py-2.5 text-sm text-danger sm:ml-[13.5rem]">
                            <ArrowDown className="h-4 w-4" aria-hidden="true" />
                            <span className="font-semibold tabular">
                              {formatCount(lost)} dropped ({pct(100 - st.conversion_from_prev_pct)}). Biggest drop
                            </span>
                            <button
                              type="button"
                              onClick={() => setDrawerStep(idx)}
                              className="ml-auto inline-flex h-8 items-center rounded-lg bg-danger px-3 font-semibold text-on-brand hover:bg-danger/90"
                            >
                              See who dropped and why
                            </button>
                          </div>
                        ) : (
                          <div className="flex items-center gap-2.5 py-0.5 text-[0.8125rem] text-muted sm:ml-[13.5rem]">
                            <ArrowDown className="h-3.5 w-3.5" aria-hidden="true" />
                            <span className="tabular">
                              {formatCount(lost)} dropped ({pct(100 - st.conversion_from_prev_pct)})
                            </span>
                            <button
                              type="button"
                              onClick={() => setDrawerStep(idx)}
                              className="font-medium text-brand hover:underline"
                            >
                              See who
                            </button>
                          </div>
                        )
                      )}
                      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-1.5 py-2.5 sm:grid-cols-[12.5rem_minmax(0,1fr)_7.5rem]">
                        <span className="truncate text-sm font-semibold text-ink" title={humanizeEvent(st.event_name)}>
                          {idx + 1}. {humanizeEvent(st.event_name)}
                        </span>
                        <span
                          className="col-span-2 row-start-2 flex h-8 overflow-hidden rounded-md bg-sunken sm:col-span-1 sm:row-start-auto"
                          role="img"
                          aria-label={`${formatCount(st.entered_users)} users reached this step${
                            lost > 0 ? `, ${formatCount(lost)} dropped before it` : ""
                          }`}
                        >
                          <span className="bg-brand" style={{ width: `${Math.max(0.5, keptPct)}%` }} />
                          {lostPct > 0 && (
                            <span className={isBiggest ? "hatch-drop-strong" : "hatch-drop"} style={{ width: `${lostPct}%` }} />
                          )}
                        </span>
                        <span className="flex flex-col text-right">
                          <span className="font-semibold tabular text-ink">{formatCount(st.entered_users)}</span>
                          <span className="text-xs text-muted tabular">
                            {idx === 0 ? "100%" : `${pct(st.conversion_from_prev_pct)} of previous`}
                          </span>
                        </span>
                      </div>
                    </li>
                  );
                })}
              </ol>
              <div className="mt-4 flex flex-wrap gap-4 text-[0.8125rem] text-ink-2">
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-2.5 w-3.5 rounded-sm bg-brand" aria-hidden="true" />
                  Reached this step
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="hatch-drop h-2.5 w-3.5 rounded-sm" aria-hidden="true" />
                  Dropped before this step
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="hatch-drop-strong h-2.5 w-3.5 rounded-sm" aria-hidden="true" />
                  Biggest drop
                </span>
              </div>
            </>
          )}
        </Panel>
      )}

      {result && first && first.entered_users > 0 && (
        <div className="grid grid-cols-1 gap-5 2xl:grid-cols-2">
          <Panel title={breakdownBy ? `By ${humanizeEvent(breakdownBy)}` : "By segment"}>
            {result.breakdowns.length === 0 ? (
              <p className="text-sm text-muted">Choose a property in "Split by" to compare segments.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-line text-left text-xs text-muted">
                      <th className="py-2 pr-3 font-medium">Segment</th>
                      <th className="py-2 pr-3 text-right font-medium">Started</th>
                      <th className="py-2 pr-3 text-right font-medium">Completed</th>
                      <th className="py-2 text-right font-medium">Conversion</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-line">
                    {[...result.breakdowns]
                      .sort(
                        (a, b) =>
                          (b.steps[b.steps.length - 1]?.conversion_from_start_pct ?? 0) -
                          (a.steps[a.steps.length - 1]?.conversion_from_start_pct ?? 0),
                      )
                      .map((bg) => {
                        const started = bg.steps[0]?.entered_users ?? 0;
                        const done = bg.steps[bg.steps.length - 1]?.entered_users ?? 0;
                        const rate = bg.steps[bg.steps.length - 1]?.conversion_from_start_pct ?? 0;
                        return (
                          <tr key={bg.breakdown_value}>
                            <td className="py-2 pr-3 text-ink">{bg.breakdown_value || "(not set)"}</td>
                            <td className="py-2 pr-3 text-right text-ink-2">{formatCount(started)}</td>
                            <td className="py-2 pr-3 text-right text-ink-2">{formatCount(done)}</td>
                            <td className="py-2 text-right">
                              <span className="inline-flex items-center gap-2">
                                <span className="hidden h-1.5 w-16 overflow-hidden rounded-full bg-sunken sm:block">
                                  <span className="block h-full bg-brand" style={{ width: `${Math.min(100, rate)}%` }} />
                                </span>
                                <span className="font-medium text-ink">{pct(rate)}</span>
                              </span>
                            </td>
                          </tr>
                        );
                      })}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>

          <Panel title="Time to complete" meta={`${formatCount(result.time_to_convert.converted_count)} completed`}>
            {result.time_to_convert.converted_count === 0 ? (
              <p className="text-sm text-muted">No one completed every step in this window.</p>
            ) : (
              <>
                <dl className="mb-4 grid grid-cols-3 gap-3">
                  {[
                    ["Median", result.time_to_convert.p50_ms],
                    ["90% within", result.time_to_convert.p90_ms],
                    ["99% within", result.time_to_convert.p99_ms],
                  ].map(([label, v]) => (
                    <div key={label as string}>
                      <dt className="text-xs text-muted">{label}</dt>
                      <dd className="text-lg font-semibold text-ink tabular">{formatDuration(v as number)}</dd>
                    </div>
                  ))}
                </dl>
                <div className="flex h-32 items-end gap-1.5" role="img" aria-label="Distribution of time to complete">
                  {result.time_to_convert.histogram_bins.map(([lo, hi, cnt], i, bins) => {
                    const maxBin = Math.max(1, ...bins.map((b) => b[2]));
                    return (
                      <div key={i} className="flex h-full flex-1 flex-col justify-end" title={`${formatDuration(lo)}–${formatDuration(hi)}: ${formatCount(cnt)} users`}>
                        <div className="w-full rounded-t bg-info/70" style={{ height: `${Math.max(2, (cnt / maxBin) * 100)}%` }} />
                      </div>
                    );
                  })}
                </div>
                <div className="mt-1 flex justify-between text-xs text-faint tabular">
                  <span>{formatDuration(result.time_to_convert.histogram_bins[0]?.[0] ?? 0)}</span>
                  <span>
                    {formatDuration(
                      result.time_to_convert.histogram_bins[result.time_to_convert.histogram_bins.length - 1]?.[1] ?? 0,
                    )}
                  </span>
                </div>
              </>
            )}
          </Panel>
        </div>
      )}
      </ReportLayout>

      {drawerStep !== null && result && (
        <DropOffDrawer
          projectId={projectId}
          fromStep={humanizeEvent(overall[drawerStep - 1]?.event_name ?? "")}
          toStep={humanizeEvent(overall[drawerStep]?.event_name ?? "")}
          droppedCount={(overall[drawerStep - 1]?.entered_users ?? 0) - (overall[drawerStep]?.entered_users ?? 0)}
          samples={result.dropped_sample_sessions.filter((d) => d.dropped_before_step === drawerStep)}
          onClose={() => setDrawerStep(null)}
        />
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// Drop-off drawer: who left at this step, and their recording.
// ---------------------------------------------------------------------------

const DropOffDrawer: React.FC<{
  projectId: string;
  fromStep: string;
  toStep: string;
  droppedCount: number;
  samples: DroppedSessionSample[];
  onClose: () => void;
}> = ({ projectId, fromStep, toStep, droppedCount, samples, onClose }) => {
  const [selected, setSelected] = useState<DroppedSessionSample | null>(
    () => samples.find((d) => d.has_replay) ?? samples[0] ?? null,
  );
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [onClose]);

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <button type="button" aria-label="Close" onClick={onClose} className="absolute inset-0 bg-ink/40" tabIndex={-1} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="dropoff-title"
        className="relative flex h-full w-full max-w-5xl flex-col border-l border-line bg-canvas shadow-2xl"
      >
        <div className="flex items-start justify-between gap-4 border-b border-line bg-panel px-5 py-4">
          <div className="min-w-0">
            <h2 id="dropoff-title" className="text-lg font-semibold text-ink">
              {formatCount(droppedCount)} users didn't reach {toStep}
            </h2>
            <p className="mt-0.5 text-sm text-muted">
              They did {fromStep}, then stopped. Pick someone to see what happened on their screen.
            </p>
          </div>
          <button ref={closeRef} type="button" onClick={onClose} className={btn.ghost} aria-label="Close drop-off details">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="flex min-h-0 flex-1 flex-col md:flex-row">
          <div className="max-h-56 shrink-0 overflow-y-auto border-b border-line md:max-h-none md:w-64 md:border-b-0 md:border-r">
            {samples.length === 0 ? (
              <p className="p-4 text-sm text-muted">No example sessions were sampled for this step.</p>
            ) : (
              <ul aria-label="Users who dropped off">
                {samples.map((d) => {
                  const active = selected?.session_id === d.session_id;
                  return (
                    <li key={d.session_id}>
                      <button
                        type="button"
                        onClick={() => setSelected(d)}
                        aria-current={active ? "true" : undefined}
                        className={`w-full border-l-2 px-4 py-2.5 text-left ${
                          active ? "border-brand bg-brand-soft" : "border-transparent hover:bg-hover"
                        }`}
                      >
                        <span className="flex items-center justify-between gap-2">
                          <span className="truncate text-sm font-medium text-ink tabular">{displayUserId(d.distinct_id)}</span>
                          <span
                            className={`shrink-0 rounded px-1.5 py-0.5 text-[11px] font-medium ${
                              d.has_replay
                                ? "bg-ok-soft text-ok"
                                : "bg-sunken text-faint"
                            }`}
                          >
                            {d.has_replay ? "Recording" : "No video"}
                          </span>
                        </span>
                        <span className="block text-xs text-muted">
                          Left {new Date(d.drop_ts_ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })}
                        </span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
            {droppedCount > samples.length && samples.length > 0 && (
              <p className="px-4 py-3 text-xs text-faint">
                Showing {samples.length} examples of {formatCount(droppedCount)}.
              </p>
            )}
          </div>

          <div className="min-w-0 flex-1 overflow-y-auto p-5">
            {selected ? (
              <SessionReplay
                key={selected.session_id}
                projectId={projectId}
                sessionId={selected.session_id}
                offsetKb={selected.replay_offset_kb}
              />
            ) : (
              <EmptyState title="Pick a user" body="Their screen recording plays here." />
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
