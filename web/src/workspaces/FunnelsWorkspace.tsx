import React, { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { ArrowDown, Download, Plus, RefreshCw, Trash2, X } from "lucide-react";
import { api, DroppedSessionSample, FunnelResult, isAbortError } from "../api/client";
import { SessionReplay } from "../components/WireframePlayer";
import { useAccess } from "../auth/access";
import { typicalOrder } from "../lib/eventOrder";
import { FunnelQueryBody, UserTimeline, WhyTheyLeft } from "./DropOffInsights";
import { SaveToDashboard, useSavedReport } from "../dashboards/SaveToDashboard";
import {
  CompareControl,
  CompareSel,
  compareLabel,
  compareRangeFor,
  RangePresets,
  RangeSel,
  rangeFor,
  rangePhrase,
} from "../dashboards/ReportCard";
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
import { FilterPanel, useTopValues, EMPTY_FILTER, FilterDraft, describeFilter, fromGlobalFilter, toGlobalFilter } from "../components/FilterPanel";
import { displayUserId, downloadTextFile, errorMessage, formatCount, formatDateTime, formatDuration, humanizeEvent } from "../lib/format";
import { PropertyPicker, propertyOptions } from "../components/PropertyPicker";

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  refreshToken: number;
}

type OrderMode = "loose_order" | "strict_order" | "any_order";

/** One "where" condition on a single funnel step (e.g. Product Viewed where category = shoes). */
type StepWhere = { prop: string; op: "eq" | "neq" | "contains"; value: string };

const stepWhereToFilters = (w: StepWhere | null | undefined) =>
  w && w.prop && w.value.trim() ? [{ prop_name: w.prop, filter: { op: w.op, val: w.value.trim() } }] : [];

function stepWhereFromFilters(filters: unknown): StepWhere | null {
  const f = Array.isArray(filters) ? (filters[0] as { prop_name?: string; filter?: { op?: string; val?: unknown } }) : null;
  if (!f?.prop_name || !f.filter) return null;
  const op = f.filter.op === "neq" || f.filter.op === "contains" ? f.filter.op : "eq";
  return { prop: f.prop_name, op, value: String(f.filter.val ?? "") };
}

const WINDOWS: { hours: number; label: string }[] = [
  { hours: 1, label: "1 hour" },
  { hours: 24, label: "1 day" },
  { hours: 168, label: "7 days" },
  { hours: 336, label: "14 days" },
];

const pct = (v: number) => `${v.toFixed(1)}%`;
const delta = (pts: number) => `${pts >= 0 ? "+" : "−"}${Math.abs(pts).toFixed(1)} pts now`;

function defaultStepsFromCatalog(events: string[]): string[] {
  const clean = events.filter((e) => !e.startsWith("$"));
  if (clean.length >= 2) return clean.slice(0, Math.min(4, clean.length));
  if (clean.length === 1) return [clean[0], clean[0]];
  return [];
}

export const FunnelsWorkspace: React.FC<Props> = ({
  projectId,
  availableEvents,
  availableProps,
  refreshToken,
}) => {
  const [steps, setSteps] = useState<string[]>(() => defaultStepsFromCatalog(availableEvents));
  // Indexed like `steps`; a missing entry means no condition on that step.
  const [stepWhere, setStepWhere] = useState<(StepWhere | null)[]>([]);
  const [orderMode, setOrderMode] = useState<OrderMode>("loose_order");
  const [windowHours, setWindowHours] = useState<number>(168);
  const [excludeEvent, setExcludeEvent] = useState<string>("");
  const [holdConstant, setHoldConstant] = useState<string>("");
  const [breakdownBy, setBreakdownBy] = useState<string>("");
  const [filter, setFilter] = useState<FilterDraft>(EMPTY_FILTER);
  const [compare, setCompare] = useState<CompareSel>("");
  const [result, setResult] = useState<FunnelResult | null>(null);
  const [prevResult, setPrevResult] = useState<FunnelResult | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [drawerStep, setDrawerStep] = useState<number | null>(null);

  useEffect(() => {
    setSteps((prev) => (prev.length > 0 ? prev : defaultStepsFromCatalog(availableEvents)));
    // Reorder the automatic default into the order users actually go, unless someone changed it meanwhile.
    const auto = defaultStepsFromCatalog(availableEvents);
    if (new Set(auto).size < 2) return;
    let alive = true;
    void typicalOrder(projectId, auto).then((ordered) => {
      if (alive) setSteps((prev) => (prev.join("\u0000") === auto.join("\u0000") ? ordered : prev));
    });
    return () => {
      alive = false;
    };
  }, [projectId, availableEvents]);

  const eventOptions = useMemo(
    () => Array.from(new Set([...availableEvents, ...steps])).filter((e) => e && !e.startsWith("$")),
    [availableEvents, steps],
  );

  const propOptions = useMemo(
    () => Array.from(new Set([...availableProps, holdConstant, breakdownBy].filter(Boolean))),
    [availableProps, holdConstant, breakdownBy],
  );

  const buildQuery = useCallback(
    () => ({
      steps: steps.map((event_name, i) => ({ event_name, filters: stepWhereToFilters(stepWhere[i]), first_time_ever: false })),
      order_mode: orderMode,
      conversion_window_ms: windowHours * 3_600_000,
      exclusions: excludeEvent
        ? [{ event_name: excludeEvent, between_steps: [1, Math.min(2, steps.length - 1)] }]
        : [],
      hold_constant_props: holdConstant ? [holdConstant] : [],
      breakdown_by: breakdownBy || null,
      global_filter: toGlobalFilter(filter),
      // Studio-only: the comparison runs as a second query; the engine ignores this field.
      compare_to: compare || undefined,
    }),
    [steps, stepWhere, orderMode, windowHours, excludeEvent, holdConstant, breakdownBy, filter, compare],
  );

  const [range, setRange] = useState<RangeSel>("14");
  const reqId = useRef(0);
  const funnelAbortRef = useRef<AbortController | null>(null);
  const { error: openError, saved: savedReport } = useSavedReport(
    projectId,
    (q) => {
      const s = q as { steps?: { event_name: string; filters?: unknown }[]; order_mode?: OrderMode; conversion_window_ms?: number;
        exclusions?: { event_name: string }[]; hold_constant_props?: string[]; breakdown_by?: string | null; compare_to?: string };
      if (s.steps?.length) {
        setSteps(s.steps.map((x) => x.event_name));
        setStepWhere(s.steps.map((x) => stepWhereFromFilters(x.filters)));
      }
      if (s.order_mode) setOrderMode(s.order_mode);
      if (s.conversion_window_ms) setWindowHours(s.conversion_window_ms / 3_600_000);
      setExcludeEvent(s.exclusions?.[0]?.event_name ?? "");
      setHoldConstant(s.hold_constant_props?.[0] ?? "");
      setBreakdownBy(s.breakdown_by ?? "");
      setCompare(s.compare_to ?? "");
      setFilter(fromGlobalFilter((q as { global_filter?: unknown }).global_filter));
    },
    setRange,
    { query: buildQuery, range },
  );

  const runFunnel = useCallback(async () => {
    if (steps.length < 2) {
      setResult(null);
      return;
    }
    funnelAbortRef.current?.abort();
    const ctrl = new AbortController();
    funnelAbortRef.current = ctrl;
    const id = ++reqId.current;
    setLoading(true);
    setError(null);
    try {
      const main = rangeFor(range);
      const cmp = compareRangeFor(main, compare);
      const [res, prev] = await Promise.all([
        api.queryFunnel(projectId, { time_range: main, ...buildQuery() }, ctrl.signal),
        cmp ? api.queryFunnel(projectId, { time_range: cmp, ...buildQuery() }, ctrl.signal) : Promise.resolve(null),
      ]);
      if (id === reqId.current && !ctrl.signal.aborted) {
        setResult(res);
        setPrevResult(prev);
      }
    } catch (err) {
      if (!isAbortError(err) && id === reqId.current && !ctrl.signal.aborted) setError(errorMessage(err));
    } finally {
      if (id === reqId.current && !ctrl.signal.aborted) setLoading(false);
    }
  }, [projectId, buildQuery, range, steps.length, compare]);

  useEffect(() => {
    void runFunnel();
    return () => {
      funnelAbortRef.current?.abort();
    };
  }, [runFunnel, refreshToken]);

  const overall = result?.overall_steps ?? [];
  // The exact query behind the results, for the drop-off drawer's follow-up queries.
  const drawerQuery = useMemo(() => ({ time_range: rangeFor(range), ...buildQuery() }) as FunnelQueryBody, [range, buildQuery]);
  const first = overall[0];
  const last = overall[overall.length - 1];
  // Only a comparison of the same steps is meaningful.
  const prevAll = prevResult && prevResult.overall_steps.length === overall.length ? prevResult.overall_steps : [];
  // An empty comparison period has no rates to compare; say so once instead of "was 0%" per step.
  const prevEmpty = prevAll.length > 0 && prevAll[0].entered_users === 0;
  const prevSteps = prevEmpty ? [] : prevAll;
  const prevFirst = prevSteps[0];
  const prevLast = prevSteps[prevSteps.length - 1];

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

  const reportTitle =
    steps.length >= 2
      ? `${humanizeEvent(steps[0] ?? "")} → ${humanizeEvent(steps[steps.length - 1] ?? "")}`
      : "Funnels";

  return (
    <div className="space-y-5">
      <PageHeader
        title={reportTitle}
        description="Funnel: where users drop out, with recordings of the ones who left."
        actions={
          <>
            <RangePresets value={range} onChange={setRange} />
            <SaveToDashboard saved={savedReport}
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
        summary={[
          `${steps.length} steps`,
          `within ${WINDOWS.find((w) => w.hours === windowHours)?.label ?? `${windowHours} hours`}`,
          breakdownBy ? `split by ${humanizeEvent(breakdownBy)}` : "no split",
          describeFilter(filter),
        ].filter(Boolean).join(" · ")}
        query={<>
      <FilterPanel projectId={projectId} availableProps={availableProps} anchorEvent={steps[0] ?? ""} value={filter} onChange={setFilter} />
      <Panel title="Steps">
        <ol className="flex flex-wrap items-center gap-1.5 xl:flex-col xl:items-stretch xl:gap-1">
          {steps.map((st, idx) => (
            <li key={idx} className="flex min-w-0 max-w-full flex-wrap items-center gap-1.5 xl:flex-col xl:items-stretch xl:gap-1">
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
                  onClick={() => {
                    setSteps(steps.filter((_, i) => i !== idx));
                    setStepWhere(stepWhere.filter((_, i) => i !== idx));
                  }}
                  className="rounded p-1 text-faint hover:bg-destructive-soft hover:text-destructive"
                  aria-label={`Remove step ${idx + 1}`}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
              </span>
              <StepWhereEditor
                idx={idx}
                projectId={projectId}
                event={st}
                value={stepWhere[idx] ?? null}
                props={availableProps}
                onChange={(w) => {
                  const next = [...stepWhere];
                  next[idx] = w;
                  setStepWhere(next);
                }}
              />
            </li>
          ))}
          <li>
            <button
              type="button"
              disabled={eventOptions.length === 0}
              onClick={() =>
                setSteps([
                  ...steps,
                  eventOptions.find((e) => !steps.includes(e)) ?? eventOptions[0] ?? "",
                ])
              }
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
              {eventOptions.map((ev) => (
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
              {propOptions.map((p) => (
                <option key={p} value={p}>
                  {humanizeEvent(p)}
                </option>
              ))}
            </select>
          </label>
          <CompareControl main={rangeFor(range)} value={compare} onChange={setCompare} />
          <PropertyPicker
            label="Split by"
            value={breakdownBy}
            onChange={setBreakdownBy}
            noneLabel="No split"
            recentKey={`pulse.recent-props.${projectId}`}
            options={propertyOptions(propOptions)}
          />
        </div>
      </Panel>
        </>}
      >

      {openError && <ErrorNotice message={openError} />}
      {error && <ErrorNotice message={`The funnel didn't run: ${error}`} onRetry={() => void runFunnel()} />}

      {steps.length < 2 && !loading && (
        <Panel>
          {eventOptions.length === 0 ? (
            <EmptyState
              title="No events in this project yet"
              body="Send events to your project via the SDK or /v1/ingest to build a conversion funnel."
            />
          ) : (
            <EmptyState
              title="Pick the steps of a journey you care about"
              body="Add at least two steps under Steps, in the order users take them, for example sign-up then first purchase."
            />
          )}
        </Panel>
      )}

      {!result && loading && (
        <Panel>
          <ChartSkeleton rows={5} />
        </Panel>
      )}

      {result && first && last && (
        <Panel title="Conversion" meta={<RanIn us={result.exec_time_us} />}>
          {first.entered_users === 0 ? (
            <EmptyState
              title={`No one did the first step ${rangePhrase(range)}`}
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
                {prevEmpty && (
                  <p className="text-[0.9375rem] text-ink-2">
                    {compareLabel(rangeFor(range), compare)}: no users entered this funnel, so there's nothing to compare.
                  </p>
                )}
                {prevLast && prevFirst && (
                  <p className="text-[0.9375rem] text-ink-2">
                    {compareLabel(rangeFor(range), compare)}:{" "}
                    <span className="tabular">{pct(prevLast.conversion_from_start_pct)}</span> of{" "}
                    {formatCount(prevFirst.entered_users)} users ({delta(last.conversion_from_start_pct - prevLast.conversion_from_start_pct)}).
                  </p>
                )}
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
                          {prevSteps[idx] && (
                            <span className="text-xs text-info tabular" title={compareLabel(rangeFor(range), compare)}>
                              was {idx === 0 ? formatCount(prevSteps[idx].entered_users) : pct(prevSteps[idx].conversion_from_prev_pct)}
                            </span>
                          )}
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
              <SegmentCompare groups={result.breakdowns} />
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
                <TimeHistogram bins={result.time_to_convert.histogram_bins} />
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
          fromEvent={overall[drawerStep - 1]?.event_name ?? ""}
          toEvent={overall[drawerStep]?.event_name ?? ""}
          step={drawerStep}
          query={drawerQuery}
          availableProps={availableProps}
          overallRate={(overall[drawerStep]?.entered_users ?? 0) / Math.max(1, overall[drawerStep - 1]?.entered_users ?? 0)}
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
  fromEvent: string;
  toEvent: string;
  step: number;
  query: FunnelQueryBody;
  availableProps: string[];
  overallRate: number;
  droppedCount: number;
  samples: DroppedSessionSample[];
  onClose: () => void;
}> = ({ projectId, fromEvent, toEvent, step, query, availableProps, overallRate, droppedCount, samples, onClose }) => {
  const fromStep = humanizeEvent(fromEvent);
  const toStep = humanizeEvent(toEvent);
  const [selected, setSelected] = useState<DroppedSessionSample | null>(
    () => samples.find((d) => d.has_replay) ?? samples[0] ?? null,
  );
  const { canAdmin } = useAccess();
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const downloadAll = async () => {
    setDownloading(true);
    setDownloadError(null);
    try {
      const csv = await api.exportFunnelDropped(projectId, query, step);
      downloadTextFile(`dropped-before-${toEvent}.csv`, csv, "text/csv;charset=utf-8");
    } catch (err) {
      setDownloadError(`Couldn't download the list: ${errorMessage(err)}`);
    } finally {
      setDownloading(false);
    }
  };
  const closeRef = useRef<HTMLButtonElement>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  // Runs once per open: focus moves in, Tab stays inside, and focus goes back to the opener on close.
  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") return onCloseRef.current();
      if (e.key !== "Tab" || !dialogRef.current) return;
      const els = dialogRef.current.querySelectorAll<HTMLElement>(
        'button:not([disabled]), a[href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
      );
      if (els.length === 0) return;
      const first = els[0];
      const last = els[els.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
      opener?.focus?.();
    };
  }, []);

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <button type="button" aria-label="Close" onClick={onClose} className="absolute inset-0 bg-ink/40 motion-safe:animate-fade-in" tabIndex={-1} />
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="dropoff-title"
        className="relative flex h-full w-full max-w-5xl flex-col border-l border-line bg-canvas shadow-2xl motion-safe:animate-slide-in-right"
      >
        <div className="flex items-start justify-between gap-4 border-b border-line bg-panel px-5 py-4">
          <div className="min-w-0">
            <h2 id="dropoff-title" className="text-lg font-semibold text-ink">
              {formatCount(droppedCount)} users didn't reach {toStep}
            </h2>
            <p className="mt-0.5 text-sm text-muted">
              They did {fromStep}, then stopped. Pick someone to see their recording or what they did.
            </p>
            {downloadError && <p className="mt-1 text-sm text-danger">{downloadError}</p>}
          </div>
          {canAdmin && (
            <button
              type="button"
              className={`${btn.secondary} ml-auto`}
              disabled={downloading || droppedCount === 0}
              onClick={() => void downloadAll()}
            >
              <Download className="h-4 w-4" /> {downloading ? "Preparing…" : `Download all ${formatCount(droppedCount)}`}
            </button>
          )}
          <button ref={closeRef} type="button" onClick={onClose} className={btn.ghost} aria-label="Close drop-off details">
            <X className="h-5 w-5" />
          </button>
        </div>

        <WhyTheyLeft
          projectId={projectId}
          query={query}
          step={step}
          fromEvent={fromEvent}
          toEvent={toEvent}
          overallRate={overallRate}
          availableProps={availableProps}
        />

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
                          Left {formatDateTime(d.drop_ts_ms, { dateStyle: "medium", timeStyle: "short" })}
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
            {selected && !selected.has_replay ? (
              <UserTimeline
                key={selected.session_id}
                projectId={projectId}
                distinctId={selected.distinct_id}
                canonicalUid={selected.canonical_uid}
                dropTs={selected.drop_ts_ms}
                fromEvent={fromEvent}
              />
            ) : selected ? (
              <SessionReplay
                key={selected.session_id}
                projectId={projectId}
                sessionId={selected.session_id}
                offsetKb={selected.replay_offset_kb}
              />
            ) : (
              <EmptyState title="Pick a user" body="Their recording, or the events around when they left, shows here." />
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

// Shared series palette; never orange, which means drop-offs here.
/** Time-to-complete bars; hovering one shows its range and count. */
const TimeHistogram: React.FC<{ bins: [number, number, number][] }> = ({ bins }) => {
  const [hover, setHover] = useState<number | null>(null);
  const maxBin = Math.max(1, ...bins.map((b) => b[2]));
  const text = ([lo, hi, cnt]: [number, number, number]) => `${formatDuration(lo)}–${formatDuration(hi)}: ${formatCount(cnt)} users`;
  return (
    <div className="relative">
      <div
        className="flex h-32 items-end gap-1.5"
        role="img"
        aria-label={`Distribution of time to complete. ${bins.map(text).join("; ")}`}
        onMouseLeave={() => setHover(null)}
      >
        {bins.map((b, i) => (
          <div key={i} className="flex h-full flex-1 flex-col justify-end" onMouseEnter={() => setHover(i)}>
            <div
              className={`w-full rounded-t transition-opacity duration-150 ${hover !== null && hover !== i ? "opacity-50" : ""}`}
              style={{ height: `${Math.max(2, (b[2] / maxBin) * 100)}%`, background: "rgb(var(--chart-2) / 0.7)" }}
            />
          </div>
        ))}
      </div>
      {hover !== null && bins[hover] && (
        <div
          className="pointer-events-none absolute -top-2 z-10 -translate-y-full whitespace-nowrap rounded-lg border border-line-strong bg-panel px-3 py-2 text-[0.8125rem] shadow-pop motion-safe:animate-pop"
          style={(hover + 0.5) / bins.length > 0.6 ? { right: `${100 - ((hover + 1) / bins.length) * 100}%` } : { left: `${(hover / bins.length) * 100}%` }}
        >
          <p className="font-semibold text-ink">
            {formatDuration(bins[hover][0])}–{formatDuration(bins[hover][1])}
          </p>
          <p className="tabular text-ink-2">{formatCount(bins[hover][2])} users</p>
        </div>
      )}
    </div>
  );
};

const SEG_COLORS = [1, 2, 3, 4].map((i) => `rgb(var(--chart-${i}))`);

type SegKey = "segment" | "started" | "completed" | "rate";

/** Funnel split by a property: per-step bars for the biggest segments, then a sortable table. */
const SegmentCompare: React.FC<{ groups: FunnelResult["breakdowns"] }> = ({ groups }) => {
  const sort = useSort<SegKey>("rate", true, ["segment"]);
  const started = (g: FunnelResult["breakdowns"][number]) => g.steps[0]?.entered_users ?? 0;
  const done = (g: FunnelResult["breakdowns"][number]) => g.steps[g.steps.length - 1]?.entered_users ?? 0;
  const rate = (g: FunnelResult["breakdowns"][number]) => g.steps[g.steps.length - 1]?.conversion_from_start_pct ?? 0;
  const top = [...groups].sort((a, b) => started(b) - started(a)).slice(0, SEG_COLORS.length);
  const stepNames = top[0]?.steps.map((st) => humanizeEvent(st.event_name)) ?? [];
  const rows = sort.sortBy(groups, (g, k) =>
    k === "segment" ? g.breakdown_value || "(not set)" : k === "started" ? started(g) : k === "completed" ? done(g) : rate(g),
  );
  return (
    <div className="space-y-5">
      <div>
        <ul className="mb-3 flex flex-wrap gap-x-4 gap-y-1 text-[0.8125rem] text-ink-2">
          {top.map((g, i) => (
            <li key={g.breakdown_value} className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-sm" style={{ background: SEG_COLORS[i] }} aria-hidden="true" />
              {g.breakdown_value || "(not set)"}
            </li>
          ))}
        </ul>
        <ol className="space-y-3">
          {stepNames.slice(1).map((name, si) => (
            <li key={name + si}>
              <p className="mb-1 text-[0.8125rem] font-medium text-ink">
                {si + 2}. {name}
              </p>
              <div className="space-y-1">
                {top.map((g, i) => {
                  const p = g.steps[si + 1]?.conversion_from_start_pct ?? 0;
                  return (
                    <div key={g.breakdown_value} className="flex items-center gap-2">
                      <span className="h-2.5 flex-1 overflow-hidden rounded-sm bg-sunken">
                        <span className="block h-full" style={{ width: `${Math.min(100, p)}%`, background: SEG_COLORS[i] }} />
                      </span>
                      <span className="tabular w-12 text-right text-xs text-ink-2">{pct(p)}</span>
                    </div>
                  );
                })}
              </div>
            </li>
          ))}
        </ol>
        <p className="mt-2 text-xs text-muted">Share of each segment's starters who reached the step.</p>
      </div>
      <div className="max-h-[24rem] overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-line text-xs text-muted">
              <SortHeader label="Segment" active={sort.key === "segment"} desc={sort.desc} onClick={() => sort.toggle("segment")} />
              <SortHeader label="Started" align="right" active={sort.key === "started"} desc={sort.desc} onClick={() => sort.toggle("started")} />
              <SortHeader label="Completed" align="right" active={sort.key === "completed"} desc={sort.desc} onClick={() => sort.toggle("completed")} />
              <SortHeader label="Conversion" align="right" active={sort.key === "rate"} desc={sort.desc} onClick={() => sort.toggle("rate")} />
            </tr>
          </thead>
          <tbody className="divide-y divide-line">
            {rows.map((g) => (
              <tr key={g.breakdown_value}>
                <td className="py-2 pr-3 text-ink">{g.breakdown_value || "(not set)"}</td>
                <td className="tabular py-2 pl-3 text-right text-ink-2">{formatCount(started(g))}</td>
                <td className="tabular py-2 pl-3 text-right text-ink-2">{formatCount(done(g))}</td>
                <td className="tabular py-2 pl-3 text-right font-medium text-ink">{pct(rate(g))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

/** "+ where" on one funnel step: a single property condition, shown inline under the step. */
const StepWhereEditor: React.FC<{
  idx: number;
  projectId: string;
  /** The step's event; value suggestions come from it. */
  event: string;
  value: StepWhere | null;
  props: string[];
  onChange: (w: StepWhere | null) => void;
}> = ({ idx, projectId, event, value, props, onChange }) => {
  const listId = useId();
  const suggestions = useTopValues(projectId, value ? event : "", value?.prop ?? "");
  if (!value) {
    return (
      <button
        type="button"
        disabled={props.length === 0}
        onClick={() => onChange({ prop: "", op: "eq", value: "" })}
        className="self-start rounded px-1.5 py-0.5 text-xs text-muted hover:bg-hover hover:text-ink disabled:opacity-50"
        aria-label={`Add a condition to step ${idx + 1}`}
      >
        + where
      </button>
    );
  }
  return (
    <span className="flex min-w-0 max-w-full flex-wrap items-center gap-1 pl-1.5 text-xs">
      <span className="text-faint">where</span>
      <PropertyPicker
        dense
        label={`Step ${idx + 1} condition property`}
        value={value.prop}
        onChange={(p) => onChange({ ...value, prop: p, value: "" })}
        recentKey={`pulse.recent-props.${projectId}`}
        options={propertyOptions(Array.from(new Set([value.prop, ...props])).filter(Boolean))}
      />
      <select
        aria-label={`Step ${idx + 1} condition operator`}
        value={value.op}
        onChange={(e) => onChange({ ...value, op: e.target.value as StepWhere["op"] })}
        className={`${field} w-auto py-1 text-xs`}
      >
        <option value="eq">is</option>
        <option value="neq">is not</option>
        <option value="contains">contains</option>
      </select>
      <input
        aria-label={`Step ${idx + 1} condition value`}
        value={value.value}
        onChange={(e) => onChange({ ...value, value: e.target.value })}
        placeholder="value"
        list={suggestions.length ? listId : undefined}
        className={`${field} w-28 py-1 text-xs`}
      />
      {suggestions.length > 0 && (
        <datalist id={listId}>
          {suggestions.map((v) => (
            <option key={v} value={v} />
          ))}
        </datalist>
      )}
      <button
        type="button"
        onClick={() => onChange(null)}
        className="rounded p-1 text-faint hover:bg-hover hover:text-ink"
        aria-label={`Remove the condition on step ${idx + 1}`}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
};
