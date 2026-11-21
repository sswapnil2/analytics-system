import React, { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, FlaskConical, Plus, RefreshCw, Rocket, Square, ToggleLeft } from "lucide-react";
import { api, Experiment, ExperimentResults, FeatureFlag, FlagPatch } from "../api/client";
import { useCohortNames } from "../components/FilterPanel";
import { EmptyState, ErrorNotice, PageHeader, Panel, RanIn, btn, field, fieldLabel, ListSkeleton, ChartSkeleton } from "../components/ui";
import { useAccess } from "../auth/access";
import { PropertyPicker } from "../components/PropertyPicker";
import { errorMessage, formatCount, formatPValue, formatPercent, formatProbability, humanizeEvent } from "../lib/format";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

const DAY_MS = 86_400_000;

const FLAG_KEY_PATTERN = /^[a-z0-9_]+$/;

/** Lift arrives in percentage points (12.3 = +12.3%). */
function formatLift(pct: number): string {
  const text = formatPercent(pct / 100);
  return pct > 0 ? `+${text}` : text;
}

/**
 * Users needed per variant to detect a relative lift of `mde` on a base rate
 * `p` (two-sided α = 0.05, 80% power): n ≈ 16·p(1−p) / (p·mde)².
 */
export function usersPerVariant(p: number, mde: number): number {
  if (p <= 0 || p >= 1 || mde <= 0) return Infinity;
  return Math.ceil((16 * p * (1 - p)) / (p * mde) ** 2);
}

/** Base conversion rate and daily active users for the estimate, from the last 14 days. */
function useBaseline(projectId: string, event: string) {
  const [b, setB] = useState<{ rate: number; dailyUsers: number } | null>(null);
  useEffect(() => {
    if (!event) return setB(null);
    let alive = true;
    const end = Date.now();
    api
      .queryInsights(projectId, {
        time_range: { start_ts_ms: end - 14 * DAY_MS, end_ts_ms: end },
        interval: "day",
        series: [event, "$any_event"].map((event_name, i) => ({ label: String(i), event_name, metric: { type: "uniques" }, filters: [] })),
        breakdown_by: [],
      })
      .then((res) => {
        const [conv, all] = [0, 1].map((i) => res.series.find((s) => s.label === String(i))?.overall_value ?? 0);
        if (alive) setB(all > 0 ? { rate: conv / all, dailyUsers: all / 14 } : null);
      })
      .catch(() => alive && setB(null));
    return () => {
      alive = false;
    };
  }, [projectId, event]);
  return b;
}

function validateFlagKey(key: string): string | null {
  if (!key.trim()) return "Enter a flag key.";
  if (!FLAG_KEY_PATTERN.test(key.trim())) return "Use lowercase letters, numbers and underscores only.";
  return null;
}

const SampleEstimate: React.FC<{
  baseline: { rate: number; dailyUsers: number } | null;
  mde: number;
  flag: FeatureFlag | undefined;
  days: number;
  event: string;
}> = ({ baseline, mde, flag, days, event }) => {
  if (!baseline) return null;
  const name = humanizeEvent(event);
  const variants = Math.max(2, flag?.variants.length ?? 2);
  const n = usersPerVariant(baseline.rate, mde);
  const perDay = baseline.dailyUsers * ((flag?.rollout_pct ?? 100) / 100);
  const need = Math.ceil((n * variants) / Math.max(perDay, 1e-9));
  const short = need > days;
  return (
    <p className={`rounded-lg border px-3.5 py-2.5 text-sm ${short ? "border-warn/40 bg-warn-soft text-ink" : "border-line bg-sunken text-ink-2"}`}>
      {Number.isFinite(n) ? (
        <>
          About <span className="font-semibold text-ink">{formatCount(n)} users per variant</span> to spot a {Math.round(mde * 100)}% change
          on today's {formatPercent(baseline.rate, 1)} conversion. At ~{formatCount(Math.round(perDay))} users a day
          {flag ? ` (${Math.round(flag.rollout_pct)}% rollout)` : ""}, that's about{" "}
          <span className="font-semibold text-ink">{need} days</span>
          {short ? `, longer than the ${days} days you picked.` : "."}
        </>
      ) : (
        baseline.rate >= 1
          ? `Every active user already does ${name}, so there's no lift left to measure. Pick an event fewer people reach.`
          : baseline.rate <= 0
            ? `No one did ${name} in the last 14 days, so there's no baseline to estimate from.`
            : "Not enough data on this event yet to estimate how long the test needs."
      )}
    </p>
  );
};

const SNIPPETS: Record<string, (k: string) => string> = {
  Web: (k) => `const variant = await pulse.getVariant("${k}", { trackExposure: true });`,
  Android: (k) => `val variant = pulse.getVariant("${k}", trackExposure = true)`,
  iOS: (k) => `let variant = try await pulse.getVariant(flagKey: "${k}", trackExposure: true)`,
};

const ExposureSnippets: React.FC<{ flagKey: string }> = ({ flagKey }) => {
  const [tab, setTab] = useState("Web");
  return (
    <div className="mt-2">
      <div role="tablist" className="flex gap-1">
        {Object.keys(SNIPPETS).map((t) => (
          <button
            key={t}
            type="button"
            role="tab"
            aria-selected={tab === t}
            onClick={() => setTab(t)}
            className={`rounded px-2 py-0.5 text-xs font-medium ${tab === t ? "bg-panel text-ink shadow-sm" : "text-muted hover:text-ink"}`}
          >
            {t}
          </button>
        ))}
      </div>
      <pre className="mt-1.5 overflow-x-auto font-mono text-xs text-ink">{SNIPPETS[tab](flagKey)}</pre>
    </div>
  );
};

export const ExperimentsFlagsWorkspace: React.FC<Props> = ({ projectId, availableEvents, refreshToken }) => {
  const { canEdit } = useAccess();
  const cohorts = useCohortNames(projectId);
  const [mde, setMde] = useState(10);
  const [actionError, setActionError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);
  const [flags, setFlags] = useState<FeatureFlag[]>([]);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [selectedExpId, setSelectedExpId] = useState<string>("");
  const [expResults, setExpResults] = useState<ExperimentResults | null>(null);
  const [newFlagKey, setNewFlagKey] = useState<string>("");
  const [newFlagDesc, setNewFlagDesc] = useState<string>("");
  const [keyError, setKeyError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [loaded, setLoaded] = useState<boolean>(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [flagError, setFlagError] = useState<string | null>(null);
  // The flag whose change last saved; shows a short "Saved" next to it.
  const [savedFlag, setSavedFlag] = useState<string | null>(null);
  const savedTimerRef = useRef<number>();
  useEffect(() => () => window.clearTimeout(savedTimerRef.current), []);
  const [creating, setCreating] = useState<boolean>(false);

  const [expFormOpen, setExpFormOpen] = useState(false);
  const [expName, setExpName] = useState("");
  const [expFlagId, setExpFlagId] = useState("");
  const [expHypothesis, setExpHypothesis] = useState("");
  // No default: the busiest event is usually one everyone does, which a test can't move.
  const [expMetric, setExpMetric] = useState("");
  const eventOptions = availableEvents.map((ev) => ({ value: ev, label: humanizeEvent(ev), group: "Events" }));
  const [expGuardrail, setExpGuardrail] = useState("");
  const [expWindowDays, setExpWindowDays] = useState(7);
  const [expDays, setExpDays] = useState(14);
  const [expError, setExpError] = useState<string | null>(null);
  const baseline = useBaseline(projectId, expFormOpen ? expMetric : "");
  const [expCreating, setExpCreating] = useState(false);

  const selectedExpIdRef = useRef(selectedExpId);
  selectedExpIdRef.current = selectedExpId;
  const resultsSeqRef = useRef(0);
  const rolloutTimersRef = useRef<Map<string, number>>(new Map());
  const [rolloutDrafts, setRolloutDrafts] = useState<Record<string, number>>({});

  useEffect(
    () => () => {
      for (const id of rolloutTimersRef.current.values()) window.clearTimeout(id);
      rolloutTimersRef.current.clear();
    },
    [],
  );

  const loadAll = useCallback(async () => {
    const seq = ++resultsSeqRef.current;
    setLoading(true);
    setLoadError(null);
    try {
      const [fList, eList] = await Promise.all([
        api.getFlags(projectId),
        api.getExperiments(projectId),
      ]);
      setFlags(fList);
      setExperiments(eList);
      const cur = selectedExpIdRef.current;
      const targetExp = eList.some((e) => e.id === cur) ? cur : eList[0]?.id || "";
      if (targetExp !== cur) {
        selectedExpIdRef.current = targetExp;
        setSelectedExpId(targetExp);
      }
      if (targetExp) {
        const res = await api.getExperimentResults(projectId, targetExp);
        if (resultsSeqRef.current === seq && selectedExpIdRef.current === targetExp) setExpResults(res);
      } else if (resultsSeqRef.current === seq) {
        setExpResults(null);
      }
    } catch (err) {
      if (resultsSeqRef.current === seq) setLoadError(errorMessage(err));
    } finally {
      if (resultsSeqRef.current === seq) {
        setLoading(false);
        setLoaded(true);
      }
    }
  }, [projectId]);

  useEffect(() => {
    void loadAll();
  }, [loadAll, refreshToken]);

  const handleSelectExperiment = async (expId: string) => {
    selectedExpIdRef.current = expId;
    setSelectedExpId(expId);
    const seq = ++resultsSeqRef.current;
    if (!expId) {
      setExpResults(null);
      return;
    }
    setLoading(true);
    setLoadError(null);
    try {
      const res = await api.getExperimentResults(projectId, expId);
      if (resultsSeqRef.current === seq && selectedExpIdRef.current === expId) setExpResults(res);
    } catch (err) {
      if (resultsSeqRef.current === seq && selectedExpIdRef.current === expId) setLoadError(errorMessage(err));
    } finally {
      if (resultsSeqRef.current === seq) setLoading(false);
    }
  };

  const clearDraftIfIdle = useCallback((flagId: string) => {
    if (rolloutTimersRef.current.has(flagId)) return;
    setRolloutDrafts((prev) => {
      if (!(flagId in prev)) return prev;
      const next = { ...prev };
      delete next[flagId];
      return next;
    });
  }, []);

  const patchFlag = useCallback(async (flag: FeatureFlag, body: FlagPatch) => {
    setFlagError(null);
    try {
      const updated = await api.patchFlag(projectId, flag.id, body);
      setFlags((prev) => prev.map((f) => (f.id === flag.id ? updated : f)));
      if (body.rollout_pct !== undefined) clearDraftIfIdle(flag.id);
      setSavedFlag(flag.id);
      window.clearTimeout(savedTimerRef.current);
      savedTimerRef.current = window.setTimeout(() => setSavedFlag(null), 2500);
    } catch (err) {
      if (body.rollout_pct !== undefined) clearDraftIfIdle(flag.id);
      setFlagError(`Couldn't update ${flag.key}: ${errorMessage(err)}`);
    }
  }, [projectId, clearDraftIfIdle]);

  const scheduleRolloutPatch = (flag: FeatureFlag, pctValue: number) => {
    setRolloutDrafts((prev) => ({ ...prev, [flag.id]: pctValue }));
    const prevTimer = rolloutTimersRef.current.get(flag.id);
    if (prevTimer !== undefined) window.clearTimeout(prevTimer);
    const id = window.setTimeout(() => {
      rolloutTimersRef.current.delete(flag.id);
      void patchFlag(flag, { rollout_pct: pctValue });
    }, 250);
    rolloutTimersRef.current.set(flag.id, id);
  };

  const createExperiment = async () => {
    const flag = flags.find((f) => f.id === expFlagId);
    if (!expName.trim() || !flag || !expMetric) {
      setExpError("Add a name, pick a flag and pick the event that counts as a conversion.");
      return;
    }
    setExpCreating(true);
    setExpError(null);
    const now = Date.now();
    const metric = (event_name: string) => ({
      name: humanizeEvent(event_name),
      event_name,
      conversion_window_ms: expWindowDays * DAY_MS,
    });
    try {
      const created = await api.createExperiment(projectId, {
        name: expName.trim(),
        flag_key: flag.key,
        flag_id: flag.id,
        hypothesis: expHypothesis.trim(),
        start_ts_ms: now,
        end_ts_ms: now + expDays * DAY_MS,
        primary_metric: metric(expMetric),
        guardrail_metrics: expGuardrail ? [metric(expGuardrail)] : [],
      });
      setExpFormOpen(false);
      setExpName("");
      setExpHypothesis("");
      setExpGuardrail("");
      setSelectedExpId(created.id);
      selectedExpIdRef.current = created.id;
      await loadAll();
    } catch (e) {
      setExpError(`Couldn't create experiment: ${errorMessage(e)}`);
    } finally {
      setExpCreating(false);
    }
  };

  /** Stop the experiment; with `winner`, also give everyone that variant. */
  const conclude = async (exp: Experiment, flag: FeatureFlag | undefined, winner?: string) => {
    const what = winner ? `Ship ${winner} to all users and stop "${exp.name}"?` : `Stop "${exp.name}"? Results stay available.`;
    if (!window.confirm(what)) return;
    setActing(true);
    setActionError(null);
    try {
      if (winner && flag) {
        const updated = await api.patchFlag(projectId, flag.id, {
          enabled: true,
          rollout_pct: 100,
          variants: flag.variants.map((v) => ({ key: v.key, weight_bps: v.key === winner ? 10_000 : 0 })),
        });
        setFlags((prev) => prev.map((f) => (f.id === flag.id ? updated : f)));
      }
      const done = await api.patchExperiment(projectId, exp.id, { status: "concluded", end_ts_ms: Date.now() });
      setExperiments((prev) => prev.map((e) => (e.id === exp.id ? done : e)));
    } catch (err) {
      setActionError(errorMessage(err));
    } finally {
      setActing(false);
    }
  };

  const createFlag = async () => {
    const err = validateFlagKey(newFlagKey);
    setKeyError(err);
    if (err) return;
    setCreating(true);
    setFlagError(null);
    try {
      await api.createFlag(projectId, {
        key: newFlagKey.trim(),
        description: newFlagDesc.trim(),
        rollout_pct: 50,
      });
      setNewFlagKey("");
      setNewFlagDesc("");
      await loadAll();
    } catch (e) {
      setFlagError(`Couldn't create flag: ${errorMessage(e)}`);
    } finally {
      setCreating(false);
    }
  };

  const selectedExp = experiments.find((e) => e.id === selectedExpId);
  const selectedFlag = selectedExp
    ? flags.find((f) => f.id === selectedExp.flag_id || f.key === selectedExp.flag_key)
    : undefined;
  // With one variant, a full rollout gives everyone the same experience; with two or more it stays randomized.
  const rolloutBreaksTest =
    selectedExp?.status.toLowerCase() === "running" &&
    selectedFlag !== undefined &&
    selectedFlag.rollout_pct >= 100 &&
    selectedFlag.variants.length < 2;
  const noExposures = expResults !== null && expResults.variants.every((v) => v.sample_size === 0);

  return (
    <div className="space-y-5">
      <PageHeader
        title="Experiments"
        description="Feature flags and the A/B tests that measure them."
        actions={
          <div className="flex items-center gap-2">
            <button type="button" onClick={() => void loadAll()} disabled={loading} className={btn.secondary}>
              <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
              Update results
            </button>
            {canEdit && (
              <button type="button" onClick={() => setExpFormOpen((v) => !v)} className={btn.primary}>
                <Plus className="h-3.5 w-3.5" />
                New experiment
              </button>
            )}
          </div>
        }
      />

      {expFormOpen && (
        <Panel title="New experiment" meta="Compares the variants of one feature flag">
          <form
            noValidate
            onSubmit={(e) => {
              e.preventDefault();
              void createExperiment();
            }}
            className="space-y-4"
          >
            {expError && <ErrorNotice message={expError} />}
            {flags.length === 0 && (
              <p className="text-sm text-ink-2">Create a feature flag below first. The experiment splits users by its variants.</p>
            )}
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Name</span>
                <input value={expName} onChange={(e) => setExpName(e.target.value)} placeholder="e.g. Shorter sign-up form" className={field} />
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Feature flag</span>
                <select value={expFlagId} onChange={(e) => setExpFlagId(e.target.value)} className={field} disabled={flags.length === 0}>
                  <option value="">Pick a flag</option>
                  {flags.map((f) => (
                    <option key={f.id} value={f.id}>
                      {f.key} ({f.variants.map((v) => v.key).join(" / ") || "on"})
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 sm:col-span-2">
                <span className={fieldLabel}>Hypothesis</span>
                <input
                  value={expHypothesis}
                  onChange={(e) => setExpHypothesis(e.target.value)}
                  placeholder="e.g. Fewer fields means more people finish sign-up"
                  className={field}
                />
              </label>
              {availableEvents.length === 0 ? (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Counts as a conversion</span>
                  <select disabled className={field}>
                    <option>No events yet</option>
                  </select>
                </label>
              ) : (
                <PropertyPicker
                  label="Counts as a conversion"
                  value={expMetric}
                  onChange={setExpMetric}
                  placeholder="Search events"
                  recentKey={`pulse.recent-events.${projectId}`}
                  options={eventOptions}
                />
              )}
              <PropertyPicker
                label="Guardrail (optional)"
                value={expGuardrail}
                onChange={setExpGuardrail}
                noneLabel="None"
                placeholder="Search events"
                recentKey={`pulse.recent-events.${projectId}`}
                options={eventOptions}
              />
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Conversion window</span>
                <select value={expWindowDays} onChange={(e) => setExpWindowDays(Number(e.target.value))} className={field}>
                  {[1, 7, 14, 30].map((d) => (
                    <option key={d} value={d}>
                      Within {d} {d === 1 ? "day" : "days"} of first seeing the flag
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Smallest lift worth detecting</span>
                <select value={mde} onChange={(e) => setMde(Number(e.target.value))} className={field}>
                  {[5, 10, 20, 30].map((d) => (
                    <option key={d} value={d}>
                      {d}% relative change
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Run for</span>
                <select value={expDays} onChange={(e) => setExpDays(Number(e.target.value))} className={field}>
                  {[7, 14, 28].map((d) => (
                    <option key={d} value={d}>
                      {d} days from today
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <SampleEstimate baseline={baseline} mde={mde / 100} flag={flags.find((f) => f.id === expFlagId)} days={expDays} event={expMetric} />
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setExpFormOpen(false)} className={btn.ghost}>
                Cancel
              </button>
              <button type="submit" disabled={expCreating || flags.length === 0} className={btn.primary}>
                {expCreating ? "Creating…" : "Create experiment"}
              </button>
            </div>
          </form>
        </Panel>
      )}

      {loadError && <ErrorNotice message={loadError} onRetry={() => void loadAll()} />}

      <Panel
        title="Results"
        meta={expResults ? <RanIn us={expResults.exec_time_us} /> : undefined}
        actions={
          experiments.length > 0 ? (
            <>
              <label htmlFor="experiment-select" className="sr-only">
                Experiment
              </label>
              <select
                id="experiment-select"
                value={selectedExpId}
                onChange={(e) => void handleSelectExperiment(e.target.value)}
                className={field}
              >
                {experiments.map((ex) => (
                  <option key={ex.id} value={ex.id}>
                    {ex.status === "running" ? ex.name : `${ex.name} · ${ex.status === "concluded" ? "stopped" : ex.status}`}
                  </option>
                ))}
              </select>
            </>
          ) : undefined
        }
      >
        {!loaded ? (
          <ListSkeleton rows={3} />
        ) : experiments.length === 0 ? (
          <EmptyState
            icon={FlaskConical}
            title="No experiments yet"
            body="An experiment compares the variants of a feature flag on one conversion event."
            action={
              canEdit && (
                <button type="button" onClick={() => setExpFormOpen(true)} className={btn.primary}>
                  <Plus className="h-3.5 w-3.5" />
                  New experiment
                </button>
              )
            }
          />
        ) : !expResults ? (
          loading ? <ChartSkeleton rows={2} /> : <EmptyState title="No results for this experiment yet" />
        ) : (
          <div className="space-y-4">
            {rolloutBreaksTest && selectedFlag && (
              <div
                role="alert"
                className="flex items-start gap-3 rounded-lg border border-warn/40 bg-warn-soft px-3.5 py-3 text-sm text-ink"
              >
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warn" />
                <p>
                  This experiment is running, but <code className="font-mono">{selectedFlag.key}</code> is rolled out
                  to 100% of users. Everyone gets the same variant, so the test is no longer randomized. Lower the
                  rollout or stop the experiment.
                </p>
              </div>
            )}

            {actionError && <ErrorNotice message={`Couldn't update the experiment: ${actionError}`} />}
            {selectedExp && (
              <div className="flex flex-wrap items-center gap-2">
                <span
                  className={`rounded px-2 py-0.5 text-xs font-medium ${
                    selectedExp.status === "running" ? "bg-ok/15 text-ok" : "bg-hover text-muted"
                  }`}
                >
                  {selectedExp.status === "running"
                    ? `Running · ends ${new Date(selectedExp.end_ts_ms).toLocaleDateString()}`
                    : selectedExp.status === "concluded"
                      ? `Stopped ${new Date(selectedExp.end_ts_ms).toLocaleDateString()}`
                      : "Paused"}
                </span>
                {canEdit && selectedExp.status !== "concluded" && (
                  <>
                    {expResults.variants
                      .filter((v) => !v.is_control && v.statistically_significant && v.relative_lift_pct > 0)
                      .map((v) => (
                        <button
                          key={v.variant_key}
                          type="button"
                          disabled={acting}
                          onClick={() => void conclude(selectedExp, selectedFlag, v.variant_key)}
                          className={btn.primary}
                        >
                          <Rocket className="h-3.5 w-3.5" />
                          Ship {v.variant_key}
                        </button>
                      ))}
                    <button type="button" disabled={acting} onClick={() => void conclude(selectedExp, selectedFlag)} className={btn.secondary}>
                      <Square className="h-3.5 w-3.5" />
                      Stop experiment
                    </button>
                  </>
                )}
              </div>
            )}

            {expResults.hypothesis && (
              <p className="text-sm text-ink-2">
                <span className="font-medium text-ink">Hypothesis:</span> {expResults.hypothesis}
              </p>
            )}

            {noExposures && (
              <div className="rounded-lg border border-line bg-sunken px-3.5 py-3 text-sm text-ink-2">
                <p className="font-medium text-ink">No one has seen this experiment yet</p>
                <p className="mt-1">
                  Users count once your app asks for their variant with exposure tracking on. That sends a{" "}
                  <code className="font-mono">$experiment_started</code> event with the flag key and variant.
                </p>
                <ExposureSnippets flagKey={expResults.flag_key} />
              </div>
            )}

            {!noExposures && (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[820px] text-sm">
                <thead>
                  <tr className="border-b border-line text-xs text-muted">
                    <th className="py-2 text-left font-medium">Variant</th>
                    <th className="py-2 text-right font-medium">Users</th>
                    <th className="py-2 text-right font-medium">Conversions</th>
                    <th className="py-2 text-right font-medium">Conversion rate</th>
                    <th className="py-2 text-right font-medium">Lift</th>
                    <th className="px-4 py-2 text-center font-medium">95% range of lift</th>
                    <th className="py-2 text-right font-medium">p-value</th>
                    <th className="py-2 text-right font-medium">Chance to beat control</th>
                    <th className="py-2 pl-4 text-left font-medium">Result</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {expResults.variants.map((v) => {
                    const ciMin = Math.max(-30, Math.min(30, v.ci95_low_pct));
                    const ciMax = Math.max(-30, Math.min(30, v.ci95_high_pct));
                    const leftPct = ((ciMin + 30) / 60) * 100;
                    const widthPct = Math.max(3, ((ciMax - ciMin) / 60) * 100);

                    return (
                      <tr key={v.variant_key}>
                        <td className="py-3 font-medium text-ink">
                          <span className="flex items-center gap-1.5">
                            {v.variant_key}
                            {v.is_control && (
                              <span className="rounded bg-hover px-1.5 py-0.5 text-xs text-muted">Control</span>
                            )}
                          </span>
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                        <td className="tabular py-3 text-right text-ink-2">{formatCount(v.conversions)}</td>
                        <td className="tabular py-3 text-right font-medium text-ink">
                          {formatPercent(v.conversion_rate, 2)}
                        </td>
                        <td
                          className={`tabular py-3 text-right font-semibold ${
                            v.is_control ? "text-faint" : v.relative_lift_pct >= 0 ? "text-ok" : "text-danger"
                          }`}
                        >
                          {v.is_control ? "–" : formatLift(v.relative_lift_pct)}
                        </td>
                        <td className="px-4 py-3">
                          {v.is_control ? (
                            <div className="text-center text-faint">Baseline</div>
                          ) : (
                            <div className="space-y-1">
                              <div className="relative h-2.5 overflow-hidden rounded-full border border-line bg-sunken">
                                <div className="absolute bottom-0 left-1/2 top-0 w-px bg-line-strong" />
                                <div
                                  style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
                                  className={`absolute bottom-0.5 top-0.5 rounded-full ${
                                    v.ci95_low_pct > 0 ? "bg-ok" : "bg-warn"
                                  }`}
                                />
                              </div>
                              <div className="tabular text-center text-xs text-muted">
                                {formatLift(v.ci95_low_pct)} to {formatLift(v.ci95_high_pct)}
                              </div>
                            </div>
                          )}
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">
                          {v.is_control ? "–" : formatPValue(v.p_value)}
                        </td>
                        <td className="tabular py-3 text-right font-semibold text-ink">
                          {v.is_control ? "–" : formatProbability(v.bayesian_prob_to_beat_control)}
                        </td>
                        <td className="py-3 pl-4">
                          {v.is_control ? (
                            <span className="text-faint">–</span>
                          ) : v.statistically_significant ? (
                            <span className="inline-flex items-center gap-1.5 text-ok">
                              <span className="h-2 w-2 rounded-full bg-ok" aria-hidden />
                              Significant
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 text-muted">
                              <span className="h-2 w-2 rounded-full bg-line-strong" aria-hidden />
                              Not yet significant
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            )}

            {!noExposures && expResults.guardrails.length > 0 && (
              <div className="border-t border-line pt-4">
                <h3 className="mb-2 text-sm font-medium text-ink">Guardrails</h3>
                <div className="space-y-3">
                  {expResults.guardrails.map((g) => (
                    <div key={g.metric_name}>
                      <p className="mb-1.5 text-xs text-muted">{g.metric_name}</p>
                      <div className="flex flex-wrap gap-2">
                        {g.variants.map((gv) => (
                          <div
                            key={gv.variant_key}
                            className="rounded-lg border border-line bg-sunken px-3 py-2 text-sm"
                          >
                            <span className="text-ink-2">{gv.variant_key}: </span>
                            <span className="tabular text-ink">
                              {formatPercent(gv.conversion_rate, 2)} ({formatCount(gv.conversions)} of{" "}
                              {formatCount(gv.sample_size)})
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <p className="text-xs text-faint">Bayesian and frequentist statistics, 20,000 simulations</p>
          </div>
        )}
      </Panel>

      <Panel title="Feature flags">
        <div className="space-y-4">
          {flagError && <ErrorNotice message={flagError} />}

          {!loaded ? (
            <ListSkeleton />
          ) : flags.length === 0 ? (
            <EmptyState
              icon={ToggleLeft}
              title="No feature flags yet"
              body="Create a flag below to roll a feature out to a share of users."
            />
          ) : (
            <ul className="space-y-3">
              {flags.map((f) => {
                const pct = Math.round(rolloutDrafts[f.id] ?? f.rollout_pct);
                const sliderId = `rollout-${f.id}`;
                return (
                  <li
                    key={f.id}
                    className="flex flex-wrap items-center justify-between gap-4 rounded-lg border border-line bg-sunken p-4"
                  >
                    <div className="min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-sm font-semibold text-ink">{f.key}</span>
                        <button
                          type="button"
                          role="switch"
                          aria-checked={f.enabled}
                          aria-label={`${f.key} enabled`}
                          disabled={!canEdit}
                          onClick={() => void patchFlag(f, { enabled: !f.enabled })}
                          className={`rounded px-2 py-0.5 text-xs font-medium ${
                            f.enabled ? "border border-ok/40 bg-ok/15 text-ok" : "border border-line bg-hover text-muted"
                          }`}
                        >
                          {f.enabled ? "On" : "Off"}
                        </button>
                        <span role="status" className="text-xs text-ok">
                          {savedFlag === f.id && (
                            <span className="inline-flex items-center gap-1 motion-safe:animate-fade-in">
                              <Check className="h-3.5 w-3.5" aria-hidden="true" /> Saved
                            </span>
                          )}
                        </span>
                      </div>
                      {f.description && <div className="text-sm text-muted">{f.description}</div>}
                    </div>

                    <label className="flex min-w-0 max-w-full items-center gap-2">
                      <span className={fieldLabel}>Who</span>
                      <select
                        value={f.targeting_cohort_id ?? ""}
                        disabled={!canEdit}
                        onChange={(e) => void patchFlag(f, { targeting_cohort_id: e.target.value })}
                        className={`${field} min-w-0 max-w-full py-1`}
                      >
                        <option value="">Everyone</option>
                        {cohorts.map((c) => (
                          <option key={c.id} value={c.id}>
                            Users in {c.name}
                          </option>
                        ))}
                      </select>
                    </label>

                    <div className="flex w-full items-center gap-3 sm:w-auto">
                      <label htmlFor={sliderId} className={fieldLabel}>
                        Rollout
                      </label>
                      <input
                        id={sliderId}
                        type="range"
                        min={0}
                        max={100}
                        value={pct}
                        aria-valuetext={`${pct}% of users`}
                        disabled={!canEdit}
                        onChange={(e) => scheduleRolloutPatch(f, Number(e.target.value))}
                        className="w-full accent-brand sm:w-40"
                      />
                      <span className="tabular w-20 text-right text-sm font-medium text-ink">{pct}% of users</span>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}

          {canEdit && <form
            noValidate
            onSubmit={(e) => {
              e.preventDefault();
              void createFlag();
            }}
            className="grid grid-cols-1 items-start gap-3 border-t border-line pt-4 sm:grid-cols-[minmax(0,14rem)_1fr_auto]"
          >
            <div className="space-y-1">
              <label htmlFor="new-flag-key" className={fieldLabel}>
                Flag key
              </label>
              <input
                id="new-flag-key"
                value={newFlagKey}
                onChange={(e) => {
                  setNewFlagKey(e.target.value);
                  if (keyError) setKeyError(validateFlagKey(e.target.value));
                }}
                placeholder="new_checkout_flow"
                aria-invalid={keyError ? true : undefined}
                aria-describedby={keyError ? "new-flag-key-error" : undefined}
                className={`${field} w-full font-mono ${keyError ? "border-danger" : ""}`}
              />
              {keyError && (
                <p id="new-flag-key-error" className="text-xs text-danger">
                  {keyError}
                </p>
              )}
            </div>
            <div className="space-y-1">
              <label htmlFor="new-flag-desc" className={fieldLabel}>
                Description
              </label>
              <input
                id="new-flag-desc"
                value={newFlagDesc}
                onChange={(e) => setNewFlagDesc(e.target.value)}
                placeholder="What this flag controls"
                className={`${field} w-full`}
              />
            </div>
            <button type="submit" disabled={creating} className={`${btn.secondary} sm:mt-5`}>
              <Plus className="h-3.5 w-3.5" />
              {creating ? "Creating…" : "Create flag"}
            </button>
          </form>}
        </div>
      </Panel>
    </div>
  );
};
