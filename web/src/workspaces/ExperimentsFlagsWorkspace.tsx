import React, { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, FlaskConical, Pause, Play, Plus, RefreshCw, Rocket, Square, ToggleLeft } from "lucide-react";
import { api, Experiment, ExperimentKind, ExperimentResults, FeatureFlag, FlagPatch, MetricDirection, MetricType, TrafficGroup, VariantLiftRow } from "../api/client";
import { HoldoutsAndLayers } from "../components/HoldoutsAndLayers";
import { BanditResultsView } from "../components/BanditResults";
import { LiftChart } from "../components/LiftChart";
import { useCohortNames } from "../components/FilterPanel";
import { EmptyState, ErrorNotice, PageHeader, Panel, RanIn, btn, field, fieldLabel, ListSkeleton, ChartSkeleton } from "../components/ui";
import { useAccess } from "../auth/access";
import { PropertyPicker } from "../components/PropertyPicker";
import { errorMessage, formatCount, formatPValue, formatPercent, formatProbability, humanizeEvent, propertyOptions } from "../lib/format";
import { usersPerVariant } from "../lib/power";

interface Props {
  projectId: string;
  availableEvents: string[];
  availableProps: string[];
  refreshToken: number;
}

const METRIC_TYPES: { value: MetricType; label: string; eventLabel: string }[] = [
  { value: "conversion", label: "Conversion: did the user do it", eventLabel: "Counts as a conversion" },
  { value: "count", label: "Count: times per user", eventLabel: "Event to count" },
  { value: "sum", label: "Sum: a property's total per user", eventLabel: "Event with the value" },
  { value: "ratio", label: "Ratio: total of one over another", eventLabel: "Numerator event" },
];

/** A variant's metric value: a rate for conversions, a plain number otherwise. */
function formatMetricValue(type: MetricType | undefined, v: VariantLiftRow): string {
  if (!type || type === "conversion") return formatPercent(v.conversion_rate, 2);
  return v.mean_value.toLocaleString(undefined, { maximumFractionDigits: Math.abs(v.mean_value) < 10 ? 3 : 2 });
}

const DAY_MS = 86_400_000;

const METRIC_TYPE_LABEL: Record<MetricType, string> = {
  conversion: "conversion",
  count: "count per user",
  sum: "sum per user",
  ratio: "ratio",
};

function describeMetric(m: Experiment["primary_metric"]): string {
  const raw = m.metric_type ?? "conversion";
  const type: MetricType = raw === "continuous" ? (m.numeric_prop ? "sum" : "count") : raw;
  const of = m.numeric_prop && type !== "count" ? ` of ${m.numeric_prop}` : "";
  const per = type === "ratio" && m.denominator ? ` ÷ ${humanizeEvent(m.denominator.event_name)}` : "";
  const event = humanizeEvent(m.event_name);
  const head = event === m.name && !of && !per ? "" : `${event}${of}${per} · `;
  return `${head}${METRIC_TYPE_LABEL[type]} · within ${Math.round(m.conversion_window_ms / DAY_MS)}d of exposure`;
}

/** Absolute difference from control: percentage points for rates, metric units otherwise. */
function formatAbsDiff(type: MetricType | undefined, d: number): string {
  const conv = !type || type === "conversion";
  const n = conv ? d * 100 : d;
  const text = n.toLocaleString(undefined, { maximumFractionDigits: Math.abs(n) < 10 ? 2 : 1 });
  return `${n > 0 ? "+" : ""}${text}${conv ? " pts" : ""}`;
}

/** No metric events from anyone exposed: usually broken tracking, not "no effect". */
function hasNoMetricData(type: MetricType | undefined, rows: VariantLiftRow[]): boolean {
  return rows.length > 0 && rows.every((v) => (!type || type === "conversion" ? v.conversions === 0 : v.mean_value === 0));
}

const ExperimentSetup: React.FC<{
  exp: Experiment;
  flag: FeatureFlag | undefined;
  cohortName: (id: string) => string;
  usersPerVariantNow: number;
}> = ({ exp, flag, cohortName, usersPerVariantNow }) => {
  const planned = exp.analysis?.planned_users_per_variant ?? null;
  const elapsed = Math.max(0, Math.min(Date.now(), exp.end_ts_ms) - exp.start_ts_ms);
  const total = Math.max(1, exp.end_ts_ms - exp.start_ts_ms);
  const bandit = exp.kind === "bandit";
  const rows: [string, React.ReactNode][] = [
    ["Primary metric", <><span className="font-medium text-ink">{exp.primary_metric.name}</span> · {describeMetric(exp.primary_metric)}</>],
  ];
  if (!bandit) {
    rows.push([
      "Guardrails",
      exp.guardrail_metrics.length
        ? exp.guardrail_metrics.map((g) => (
            <div key={g.event_name}>
              <span className="font-medium text-ink">{g.name}</span> · bad when it{" "}
              {g.direction === "increase_is_bad" ? "goes up" : "goes down"}
              {g.auto_stop ? " · auto-stops" : ""}
            </div>
          ))
        : "None",
    ]);
    rows.push([
      "Analysis",
      `${exp.analysis?.sequential ? "Sequential" : "Fixed horizon"} · ${Math.round((1 - (exp.analysis?.alpha ?? 0.05)) * 100)}% confidence · CUPED ${exp.analysis?.cuped ? "on" : "off"}`,
    ]);
  }
  rows.push([
    "Flag",
    flag ? (
      <>
        <code className="font-mono">{flag.key}</code> · {flag.enabled ? "on" : "off"} · {Math.round(flag.rollout_pct)}% of{" "}
        {flag.targeting_cohort_id ? `users in ${cohortName(flag.targeting_cohort_id)}` : "everyone"} · split{" "}
        {flag.variants.map((v) => `${v.key} ${formatPercent(v.weight_bps / 10_000, 0)}`).join(" / ")}
        {flag.layer && ` · ${formatPercent((flag.layer.end_bps - flag.layer.start_bps) / 10_000, 0)} of a layer`}
        {flag.holdout && ` · ${formatPercent(flag.holdout.bps / 10_000, 0)} held out`}
      </>
    ) : (
      <code className="font-mono">{exp.flag_key}</code>
    ),
  ]);
  rows.push([
    "Dates",
    `${new Date(exp.start_ts_ms).toLocaleDateString()} to ${new Date(exp.end_ts_ms).toLocaleDateString()} · day ${Math.min(
      Math.ceil(elapsed / DAY_MS) || 1,
      Math.ceil(total / DAY_MS),
    )} of ${Math.ceil(total / DAY_MS)}`,
  ]);
  return (
    <details className="rounded-lg border border-line bg-sunken px-3.5 py-3 text-sm" open>
      <summary className="cursor-pointer font-medium text-ink">Setup</summary>
      <dl className="mt-2 grid gap-x-4 gap-y-1.5 sm:grid-cols-[9rem_1fr]">
        {rows.map(([k, v]) => (
          <React.Fragment key={k}>
            <dt className="text-muted">{k}</dt>
            <dd className="min-w-0 text-ink-2">{v}</dd>
          </React.Fragment>
        ))}
        {planned && !bandit && (
          <>
            <dt className="text-muted">Sample</dt>
            <dd className="text-ink-2">
              <div className="flex items-center gap-2">
                <div className="h-2 w-40 overflow-hidden rounded-full border border-line bg-panel" aria-hidden>
                  <div className="h-full bg-brand" style={{ width: `${Math.min(100, (usersPerVariantNow / planned) * 100)}%` }} />
                </div>
                <span className="tabular">
                  {formatCount(usersPerVariantNow)} of {formatCount(planned)} planned users per variant
                </span>
              </div>
            </dd>
          </>
        )}
      </dl>
    </details>
  );
};

const FLAG_KEY_PATTERN = /^[a-z0-9_]+$/;

/** Lift arrives in percentage points (12.3 = +12.3%). */
function formatLift(pct: number): string {
  const text = formatPercent(pct / 100);
  return pct > 0 ? `+${text}` : text;
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
  sequential: boolean;
}> = ({ baseline, mde, flag, days, event, sequential }) => {
  if (!baseline) return null;
  const name = humanizeEvent(event);
  const variants = Math.max(2, flag?.variants.length ?? 2);
  const n = usersPerVariant(baseline.rate, mde, { variants, sequential });
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
          {sequential && " Sequential testing needs more users, but you can stop as soon as a result is significant."}
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

export const ExperimentsFlagsWorkspace: React.FC<Props> = ({ projectId, availableEvents, availableProps, refreshToken }) => {
  const { canEdit } = useAccess();
  const cohorts = useCohortNames(projectId);
  const [mde, setMde] = useState(10);
  const [actionError, setActionError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);
  const [flags, setFlags] = useState<FeatureFlag[]>([]);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [holdouts, setHoldouts] = useState<TrafficGroup[]>([]);
  const [layers, setLayers] = useState<TrafficGroup[]>([]);
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
  const [expMetricType, setExpMetricType] = useState<MetricType>("conversion");
  const [expProp, setExpProp] = useState("");
  const [expDenominator, setExpDenominator] = useState("");
  const [expCuped, setExpCuped] = useState(true);
  const propOptions = propertyOptions(availableProps.filter((p) => !p.startsWith("user.")));
  const [segment, setSegment] = useState("");
  const segmentRef = useRef(segment);
  segmentRef.current = segment;
  const [expGuardrail, setExpGuardrail] = useState("");
  const [expGuardrailDirection, setExpGuardrailDirection] = useState<MetricDirection>("increase_is_bad");
  const [expSequential, setExpSequential] = useState(true);
  const [expKind, setExpKind] = useState<ExperimentKind>("ab");
  /** Bandits need to know which way is better to pick a winner. */
  const [expLowerIsBetter, setExpLowerIsBetter] = useState(false);
  const [expAutoStop, setExpAutoStop] = useState(true);
  const [expWindowDays, setExpWindowDays] = useState(7);
  const [expDays, setExpDays] = useState(14);
  const [expLayerId, setExpLayerId] = useState("");
  const [expLayerPct, setExpLayerPct] = useState(50);
  const [expError, setExpError] = useState<string | null>(null);
  const baseline = useBaseline(projectId, expFormOpen ? expMetric : "");
  // A validation message is about the fields as they were; drop it once any of them changes.
  useEffect(() => setExpError(null), [expName, expFlagId, expMetric, expMetricType, expProp, expDenominator, expKind]);
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
      const [fList, eList, hList, lList] = await Promise.all([
        api.getFlags(projectId),
        api.getExperiments(projectId),
        api.listTrafficGroups(projectId, "holdouts"),
        api.listTrafficGroups(projectId, "layers"),
      ]);
      setFlags(fList);
      setExperiments(eList);
      setHoldouts(hList);
      setLayers(lList);
      const cur = selectedExpIdRef.current;
      const targetExp = eList.some((e) => e.id === cur) ? cur : eList[0]?.id || "";
      if (targetExp !== cur) {
        selectedExpIdRef.current = targetExp;
        setSelectedExpId(targetExp);
      }
      if (targetExp) {
        const res = await api.getExperimentResults(projectId, targetExp, segmentRef.current || undefined);
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

  const handleSelectExperiment = async (expId: string, seg = segmentRef.current) => {
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
      const res = await api.getExperimentResults(projectId, expId, seg || undefined);
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

  /** Flags already confirmed this visit, so dragging a slider asks once, not on every step. */
  const confirmedFlagsRef = useRef<Set<string>>(new Set());
  /** Changing a flag under a live experiment changes who sees what mid-test; make that a deliberate step. */
  const confirmFlagChange = (flag: FeatureFlag, effect: string) => {
    if (confirmedFlagsRef.current.has(flag.id)) return true;
    const live = experiments.find(
      (e) => (e.flag_id === flag.id || e.flag_key === flag.key) && e.status !== "concluded",
    );
    if (!live) return true;
    const ok = window.confirm(
      `"${live.name}" is ${live.status} on ${flag.key}. ${effect} changes who sees each variant and can skew its results. Continue?`,
    );
    if (ok) confirmedFlagsRef.current.add(flag.id);
    return ok;
  };

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
    if (!confirmFlagChange(flag, "Changing the rollout")) return;
    setRolloutDrafts((prev) => ({ ...prev, [flag.id]: pctValue }));
    const prevTimer = rolloutTimersRef.current.get(flag.id);
    if (prevTimer !== undefined) window.clearTimeout(prevTimer);
    const id = window.setTimeout(() => {
      rolloutTimersRef.current.delete(flag.id);
      void patchFlag(flag, { rollout_pct: pctValue });
    }, 250);
    rolloutTimersRef.current.set(flag.id, id);
  };

  const closeExpForm = () => {
    setExpFormOpen(false);
    setExpName("");
    setExpFlagId("");
    setExpHypothesis("");
    setExpMetric("");
    setExpMetricType("conversion");
    setExpProp("");
    setExpDenominator("");
    setExpGuardrail("");
    setExpGuardrailDirection("increase_is_bad");
    setExpKind("ab");
    setExpLowerIsBetter(false);
    setExpLayerId("");
    setExpError(null);
  };

  const createExperiment = async () => {
    const flag = flags.find((f) => f.id === expFlagId);
    if (!expName.trim() || !flag || !expMetric) {
      setExpError("Add a name, pick a flag and pick the metric's event.");
      return;
    }
    if (expMetricType === "sum" && !expProp) {
      setExpError("Pick the property to add up.");
      return;
    }
    if (expMetricType === "ratio" && !expDenominator) {
      setExpError("Pick the denominator event.");
      return;
    }
    setExpCreating(true);
    setExpError(null);
    const now = Date.now();
    const metric = (event_name: string, direction: MetricDirection) => ({
      name: humanizeEvent(event_name),
      event_name,
      conversion_window_ms: expWindowDays * DAY_MS,
      direction,
    });
    const bandit = expKind === "bandit";
    if (bandit && flag.variants.length < 2) {
      setExpError("A bandit needs a flag with at least two variants.");
      setExpCreating(false);
      return;
    }
    const primary = {
      ...metric(expMetric, bandit && expLowerIsBetter ? "increase_is_bad" : "decrease_is_bad"),
      metric_type: expMetricType,
      numeric_prop: expMetricType === "sum" || expMetricType === "ratio" ? expProp || null : null,
      denominator: expMetricType === "ratio" ? { event_name: expDenominator } : null,
    };
    // The estimate assumes a conversion rate; other metric types use the server's default horizon.
    const planned = baseline && expMetricType === "conversion"
      ? usersPerVariant(baseline.rate, mde / 100, { variants: Math.max(2, flag.variants.length), sequential: expSequential })
      : Infinity;
    try {
      const created = await api.createExperiment(projectId, {
        name: expName.trim(),
        flag_key: flag.key,
        flag_id: flag.id,
        hypothesis: expHypothesis.trim(),
        start_ts_ms: now,
        end_ts_ms: now + expDays * DAY_MS,
        primary_metric: primary,
        kind: expKind,
        layer: expLayerId ? { layer_id: expLayerId, traffic_pct: expLayerPct } : null,
        guardrail_metrics: expGuardrail && !bandit
          ? [{ ...metric(expGuardrail, expGuardrailDirection), auto_stop: expAutoStop && expSequential }]
          : [],
        analysis: {
          sequential: expSequential,
          alpha: 0.05,
          planned_users_per_variant: Number.isFinite(planned) ? planned : null,
          cuped: expCuped,
        },
      });
      if (!flag.enabled) await api.patchFlag(projectId, flag.id, { enabled: true });
      closeExpForm();
      setSelectedExpId(created.id);
      selectedExpIdRef.current = created.id;
      await loadAll();
    } catch (e) {
      setExpError(`Couldn't create experiment: ${errorMessage(e)}`);
    } finally {
      setExpCreating(false);
    }
  };

  /** Pause keeps the flag as it is and only stops guardrail checks; resume picks them up again. */
  const setStatus = async (exp: Experiment, status: "running" | "paused") => {
    setActing(true);
    setActionError(null);
    try {
      const done = await api.patchExperiment(projectId, exp.id, { status });
      setExperiments((prev) => prev.map((e) => (e.id === exp.id ? done : e)));
    } catch (err) {
      setActionError(errorMessage(err));
    } finally {
      setActing(false);
    }
  };

  /** Stop the experiment; with `winner`, also give everyone that variant. */
  const conclude = async (exp: Experiment, flag: FeatureFlag | undefined, winner?: string) => {
    const hurt = winner
      ? (expResults?.guardrails ?? []).filter((g) => g.regressed_variants.includes(winner)).map((g) => g.metric_name)
      : [];
    const warning = hurt.length ? `\n\nWarning: ${winner} made ${hurt.join(", ")} significantly worse.` : "";
    const what = winner
      ? `Ship ${winner} and stop "${exp.name}"?\n\nThe flag ${exp.flag_key} turns on for 100% of users, all on ${winner}. Results stay available.${warning}`
      : `Stop "${exp.name}"?\n\nThe flag ${exp.flag_key} keeps its current split until you change it; results stop updating but stay available.`;
    if (!window.confirm(what)) return;
    setActing(true);
    setActionError(null);
    try {
      if (winner && flag) {
        const updated = await api.patchFlag(projectId, flag.id, {
          enabled: true,
          rollout_pct: 100,
          // Leave the layer so the winner reaches everyone, not just this experiment's slice.
          ...(flag.layer ? { layer_id: "" as const } : {}),
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
        // Off until an experiment starts on it, so creating a flag never changes what users see.
        enabled: false,
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
  const banditResults = expResults?.bandit ?? null;
  const noExposures =
    expResults !== null &&
    (banditResults
      ? banditResults.arms.every((a) => a.users === 0)
      : expResults.variants.every((v) => v.sample_size === 0));
  const isConversion = !expResults?.metric_type || expResults.metric_type === "conversion";
  const valueLabel = isConversion ? "Conversion rate" : expResults?.metric_type === "ratio" ? "Ratio" : "Per user";

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
              <button type="button" onClick={() => (expFormOpen ? closeExpForm() : setExpFormOpen(true))} className={btn.primary}>
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
              <label className="flex flex-col gap-1 sm:col-span-2">
                <span className={fieldLabel}>Type</span>
                <select value={expKind} onChange={(e) => setExpKind(e.target.value as ExperimentKind)} className={field}>
                  <option value="ab">A/B test: fixed split, tells you if the difference is real</option>
                  <option value="bandit">Bandit: moves traffic to the best variant every day</option>
                </select>
                {expKind === "bandit" && (
                  <span className="text-xs text-muted">
                    Fewer users get the worse variants while it runs. You get each variant's chance to be best instead of a
                    p-value, so use an A/B test when you need to know how big the difference is.
                  </span>
                )}
              </label>
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
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Metric</span>
                <select
                  value={expMetricType}
                  onChange={(e) => setExpMetricType(e.target.value as MetricType)}
                  className={field}
                >
                  {METRIC_TYPES.map((t) => (
                    <option key={t.value} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
              </label>
              {availableEvents.length === 0 ? (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>{METRIC_TYPES.find((t) => t.value === expMetricType)?.eventLabel}</span>
                  <select disabled className={field}>
                    <option>No events yet</option>
                  </select>
                </label>
              ) : (
                <PropertyPicker
                  label={METRIC_TYPES.find((t) => t.value === expMetricType)?.eventLabel ?? "Event"}
                  value={expMetric}
                  onChange={setExpMetric}
                  placeholder="Search events"
                  recentKey={`pulse.recent-events.${projectId}`}
                  options={eventOptions}
                />
              )}
              {(expMetricType === "sum" || expMetricType === "ratio") && (
                <PropertyPicker
                  label={expMetricType === "sum" ? "Property to add up" : "Numerator property (optional)"}
                  value={expProp}
                  onChange={setExpProp}
                  noneLabel={expMetricType === "ratio" ? "Count the events" : undefined}
                  recentKey={`pulse.recent-props.${projectId}`}
                  options={propOptions}
                />
              )}
              {expMetricType === "ratio" && (
                <PropertyPicker
                  label="Denominator event"
                  value={expDenominator}
                  onChange={setExpDenominator}
                  placeholder="Search events"
                  recentKey={`pulse.recent-events.${projectId}`}
                  options={eventOptions}
                />
              )}
              {expKind === "bandit" && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Better when it</span>
                  <select
                    value={expLowerIsBetter ? "down" : "up"}
                    onChange={(e) => setExpLowerIsBetter(e.target.value === "down")}
                    className={field}
                  >
                    <option value="up">Goes up (purchases, sign-ups)</option>
                    <option value="down">Goes down (errors, cancellations)</option>
                  </select>
                </label>
              )}
              {expKind === "ab" && (
              <PropertyPicker
                label="Guardrail (optional)"
                value={expGuardrail}
                onChange={setExpGuardrail}
                noneLabel="None"
                placeholder="Search events"
                recentKey={`pulse.recent-events.${projectId}`}
                options={eventOptions}
              />
              )}
              {expGuardrail && expKind === "ab" && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Guardrail is bad when it</span>
                  <select
                    value={expGuardrailDirection}
                    onChange={(e) => setExpGuardrailDirection(e.target.value as MetricDirection)}
                    className={field}
                  >
                    <option value="increase_is_bad">Goes up (errors, cancellations)</option>
                    <option value="decrease_is_bad">Goes down (purchases, retention)</option>
                  </select>
                </label>
              )}
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
              {expMetricType === "conversion" && expKind === "ab" && (
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
              )}
              {layers.length > 0 && (
                <div className="flex flex-col gap-1 sm:col-span-2">
                  <span className={fieldLabel}>Layer</span>
                  <div className="flex flex-wrap items-center gap-2">
                    <select aria-label="Layer" value={expLayerId} onChange={(e) => setExpLayerId(e.target.value)} className={field}>
                      <option value="">None: can overlap other experiments</option>
                      {layers.map((l) => (
                        <option key={l.id} value={l.id}>
                          {l.name} ({formatPercent((l.free_bps ?? 0) / 10_000, 0)} free)
                        </option>
                      ))}
                    </select>
                    {expLayerId && (
                      <label className="flex items-center gap-2 text-sm text-ink-2">
                        Use
                        <input
                          type="number"
                          min={1}
                          max={100}
                          value={expLayerPct}
                          onChange={(e) => setExpLayerPct(Number(e.target.value))}
                          className={`${field} w-20`}
                        />
                        % of the layer's users
                      </label>
                    )}
                  </div>
                  {expLayerId && (
                    <span className="text-xs text-muted">
                      No one in this experiment is in another experiment of the same layer. It gets a free slice, or an
                      error if there isn't one that big.
                    </span>
                  )}
                </div>
              )}
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
            {expKind === "ab" && (
            <>
            <label className="flex items-start gap-2 text-sm text-ink">
              <input
                type="checkbox"
                checked={expSequential}
                onChange={(e) => setExpSequential(e.target.checked)}
                className="mt-0.5"
              />
              <span>
                Sequential testing
                <span className="block text-xs text-muted">
                  Results stay valid however often you check them, so you can stop early. Turn off only if you'll read the
                  results once, at the end.
                </span>
              </span>
            </label>
            {expGuardrail && (
              <label className="flex items-start gap-2 text-sm text-ink">
                <input
                  type="checkbox"
                  checked={expAutoStop && expSequential}
                  disabled={!expSequential}
                  onChange={(e) => setExpAutoStop(e.target.checked)}
                  className="mt-0.5"
                />
                <span>
                  Stop automatically if the guardrail gets worse
                  <span className="block text-xs text-muted">
                    {expSequential
                      ? "Checked every 5 minutes. If a variant makes it significantly worse, everyone goes back to control and the experiment stops."
                      : "Needs sequential testing, since results are checked every few minutes."}
                  </span>
                </span>
              </label>
            )}
            <label className="flex items-start gap-2 text-sm text-ink">
              <input type="checkbox" checked={expCuped} onChange={(e) => setExpCuped(e.target.checked)} className="mt-0.5" />
              <span>
                Reduce noise with pre-experiment data (CUPED)
                <span className="block text-xs text-muted">
                  Adjusts for what each user did in the 14 days before they saw the flag, so results get significant
                  sooner. Has no effect on brand-new users or ratio metrics.
                </span>
              </span>
            </label>
            {expMetricType === "conversion" && (
            <SampleEstimate
              baseline={baseline}
              mde={mde / 100}
              flag={flags.find((f) => f.id === expFlagId)}
              days={expDays}
              event={expMetric}
              sequential={expSequential}
            />
            )}
            </>
            )}
            <div className="flex justify-end gap-2">
              <button type="button" onClick={closeExpForm} className={btn.ghost}>
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

            {!noExposures && expResults.sample_ratio_mismatch && (
              <div
                role="alert"
                className="flex items-start gap-3 rounded-lg border border-danger/40 bg-danger-soft px-3.5 py-3 text-sm text-ink"
              >
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-danger" />
                <p>
                  <span className="font-medium">The traffic split is off, so these results can't be trusted.</span>{" "}
                  Variants got a different share of users than the flag's weights (p {formatPValue(expResults.srm_p_value).startsWith("<") ? "" : "= "}{formatPValue(expResults.srm_p_value)}).
                  This usually means exposures are tracked differently per variant, or the weights changed mid-test. No
                  result is marked significant until it's fixed.
                </p>
              </div>
            )}

            {selectedExp?.stop_reason && (
              <div
                role="alert"
                className="flex items-start gap-3 rounded-lg border border-danger/40 bg-danger-soft px-3.5 py-3 text-sm text-ink"
              >
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-danger" />
                <p>
                  <span className="font-medium">Stopped by a guardrail.</span> {selectedExp.stop_reason}.
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
                <span
                  className="rounded bg-hover px-2 py-0.5 text-xs font-medium text-muted"
                  title={expResults.statistical_methodology_note}
                >
                  {banditResults
                    ? "Bandit · traffic moves daily"
                    : expResults.sequential
                      ? "Sequential · safe to check any time"
                      : "Fixed horizon · read at the end"}
                </span>
                {!noExposures && !banditResults && (
                  <span
                    className={`rounded px-2 py-0.5 text-xs font-medium ${
                      expResults.sample_ratio_mismatch ? "bg-danger-soft text-danger" : "bg-ok/15 text-ok"
                    }`}
                    title={`Sample ratio check: p ${formatPValue(expResults.srm_p_value)}`}
                  >
                    {expResults.sample_ratio_mismatch ? "Traffic split off" : "Traffic split OK"}
                  </span>
                )}
                {expResults.cuped_applied && (
                  <span
                    className="rounded bg-hover px-2 py-0.5 text-xs font-medium text-muted"
                    title="CUPED: adjusted for each user's pre-exposure value of the metric"
                  >
                    CUPED · {formatPercent(expResults.variance_reduction_pct / 100, 0)} less noise
                  </span>
                )}
                {canEdit && selectedExp.status !== "concluded" && (
                  <>
                    {banditResults?.ready_to_ship && banditResults.best_variant && (
                      <button
                        type="button"
                        disabled={acting}
                        onClick={() => void conclude(selectedExp, selectedFlag, banditResults.best_variant ?? undefined)}
                        className={btn.primary}
                      >
                        <Rocket className="h-3.5 w-3.5" />
                        Ship {banditResults.best_variant}
                      </button>
                    )}
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
                    <button
                      type="button"
                      disabled={acting}
                      onClick={() => void setStatus(selectedExp, selectedExp.status === "paused" ? "running" : "paused")}
                      className={btn.secondary}
                    >
                      {selectedExp.status === "paused" ? <Play className="h-3.5 w-3.5" /> : <Pause className="h-3.5 w-3.5" />}
                      {selectedExp.status === "paused" ? "Resume" : "Pause"}
                    </button>
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

            {selectedExp && (
              <ExperimentSetup
                exp={selectedExp}
                flag={selectedFlag}
                cohortName={(id) => cohorts.find((c) => c.id === id)?.name ?? id}
                usersPerVariantNow={
                  expResults.variants.length ? Math.min(...expResults.variants.map((v) => v.sample_size)) : 0
                }
              />
            )}

            {!noExposures && !banditResults && hasNoMetricData(expResults.metric_type, expResults.variants) && (
              <div role="alert" className="flex items-start gap-3 rounded-lg border border-warn/40 bg-warn-soft px-3.5 py-3 text-sm text-ink">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warn" />
                <p>
                  <span className="font-medium">No {expResults.metric_name} events from exposed users yet.</span> Check
                  that the event is tracked for the same user ids that send <code className="font-mono">$experiment_started</code>.
                  Until it arrives every variant reads zero, which is not the same as no effect.
                </p>
              </div>
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

            {!noExposures && banditResults && (
              <BanditResultsView
                results={banditResults}
                metricType={expResults.metric_type}
                confidence={expResults.confidence_level ?? 0.95}
                burnInUsers={selectedExp?.bandit?.burn_in_users ?? 100}
              />
            )}

            {!noExposures && !banditResults && (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[900px] text-sm">
                <thead>
                  <tr className="border-b border-line text-xs text-muted">
                    <th className="py-2 text-left font-medium">Variant</th>
                    <th className="py-2 text-right font-medium">Users</th>
                    {isConversion && <th className="py-2 text-right font-medium">Conversions</th>}
                    <th className="py-2 text-right font-medium">{valueLabel}</th>
                    <th className="py-2 text-right font-medium">Lift</th>
                    <th className="py-2 text-right font-medium">Difference</th>
                    <th className="px-4 py-2 text-center font-medium">
                      {Math.round((expResults.confidence_level ?? 0.95) * 100)}% range of lift
                    </th>
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
                        {isConversion && <td className="tabular py-3 text-right text-ink-2">{formatCount(v.conversions)}</td>}
                        <td className="tabular py-3 text-right font-medium text-ink">
                          {formatMetricValue(expResults.metric_type, v)}
                        </td>
                        <td
                          className={`tabular py-3 text-right font-semibold ${
                            v.is_control ? "text-faint" : v.relative_lift_pct >= 0 ? "text-ok" : "text-danger"
                          }`}
                        >
                          {v.is_control ? "–" : formatLift(v.relative_lift_pct)}
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">
                          {v.is_control || v.abs_diff === undefined ? "–" : formatAbsDiff(expResults.metric_type, v.abs_diff)}
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

            {!noExposures && expResults.multi_variant_users > 0 && (
              <p className="text-xs text-muted">
                {formatCount(expResults.multi_variant_users)} users saw more than one variant and are left out.
              </p>
            )}

            {!noExposures && (
              <LiftChart series={expResults.timeseries} confidence={expResults.confidence_level ?? 0.95} />
            )}

            {!noExposures && !banditResults && (
              <div className="space-y-2 border-t border-line pt-4">
                <div className="flex flex-wrap items-end justify-between gap-2">
                  <h3 className="text-sm font-medium text-ink">By segment</h3>
                  <div className="w-56">
                    <PropertyPicker
                      label="Segment by"
                      dense
                      value={segment}
                      onChange={(v) => {
                        setSegment(v);
                        void handleSelectExperiment(selectedExpId, v);
                      }}
                      noneLabel="No segments"
                      recentKey={`pulse.recent-props.${projectId}`}
                      options={propertyOptions(availableProps)}
                    />
                  </div>
                </div>
                {segment && expResults.segments.every((sg) => sg.segment === "(none)") && (
                  <p className="text-xs text-muted">
                    Exposures don't carry this property. Segments read it from the{" "}
                    <code className="font-mono">$experiment_started</code> event, or pick a user profile property.
                  </p>
                )}
                {expResults.segments.length > 0 && (
                  <>
                    <div className="overflow-x-auto">
                      <table className="w-full min-w-[560px] text-sm">
                        <thead>
                          <tr className="border-b border-line text-xs text-muted">
                            <th className="py-2 text-left font-medium">{humanizeEvent(segment.replace(/^user\./, ""))}</th>
                            <th className="py-2 text-left font-medium">Variant</th>
                            <th className="py-2 text-right font-medium">Users</th>
                            <th className="py-2 text-right font-medium">{valueLabel}</th>
                            <th className="py-2 text-right font-medium">Lift</th>
                            <th className="py-2 text-right font-medium">Range of lift</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-line">
                          {expResults.segments.flatMap((sg) =>
                            sg.variants.map((v, i) => (
                              <tr key={`${sg.segment}:${v.variant_key}`}>
                                <td className="py-2 text-ink">{i === 0 ? (sg.segment === "$other" ? "Other" : sg.segment || "(none)") : ""}</td>
                                <td className="py-2 text-ink-2">{v.variant_key}</td>
                                <td className="tabular py-2 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                                <td className="tabular py-2 text-right text-ink">{formatMetricValue(expResults.metric_type, v)}</td>
                                <td className="tabular py-2 text-right text-ink-2">{v.is_control ? "–" : formatLift(v.relative_lift_pct)}</td>
                                <td className="tabular py-2 text-right text-muted">
                                  {v.is_control ? "–" : `${formatLift(v.ci95_low_pct)} to ${formatLift(v.ci95_high_pct)}`}
                                </td>
                              </tr>
                            )),
                          )}
                        </tbody>
                      </table>
                    </div>
                    <p className="text-xs text-faint">
                      Exploratory: with many segments some will look different by chance, so confirm a segment finding with
                      its own experiment.
                    </p>
                  </>
                )}
              </div>
            )}

            {!noExposures && expResults.guardrails.length > 0 && (
              <div className="border-t border-line pt-4">
                <h3 className="mb-2 text-sm font-medium text-ink">Guardrails</h3>
                <div className="space-y-3">
                  {expResults.guardrails.map((g) => {
                    const noData = hasNoMetricData(g.metric_type, g.variants);
                    const better = g.variants.some(
                      (v) => !v.is_control && v.statistically_significant && !g.regressed_variants.includes(v.variant_key),
                    );
                    const [status, tone] = noData
                      ? ["No data, check tracking", "bg-warn-soft text-warn"]
                      : g.regressed_variants.length
                        ? ["Harmed", "bg-danger-soft text-danger"]
                        : better
                          ? ["Improved", "bg-ok/15 text-ok"]
                          : ["Not harmed so far", "bg-hover text-muted"];
                    return (
                    <div key={g.metric_name}>
                      <p className="mb-1.5 flex flex-wrap items-center gap-2 text-xs text-muted">
                        <span>
                          {g.metric_name} · bad when it {g.direction === "increase_is_bad" ? "goes up" : "goes down"}
                        </span>
                        <span className={`rounded px-1.5 py-0.5 font-medium ${tone}`}>{status}</span>
                      </p>
                      <div className="flex flex-wrap gap-2">
                        {g.variants.map((gv) => {
                          const regressed = g.regressed_variants.includes(gv.variant_key);
                          return (
                            <div
                              key={gv.variant_key}
                              className={`rounded-lg border px-3 py-2 text-sm ${
                                regressed ? "border-danger/40 bg-danger-soft" : "border-line bg-sunken"
                              }`}
                            >
                              <span className="text-ink-2">{gv.variant_key}: </span>
                              <span className="tabular text-ink">
                                {!g.metric_type || g.metric_type === "conversion"
                                  ? `${formatPercent(gv.conversion_rate, 2)} (${formatCount(gv.conversions)} of ${formatCount(gv.sample_size)})`
                                  : `${formatMetricValue(g.metric_type, gv)} per user (${formatCount(gv.sample_size)} users)`}
                              </span>
                              {!gv.is_control && (
                                <span className={`tabular ml-2 text-xs ${regressed ? "font-medium text-danger" : "text-muted"}`}>
                                  {formatLift(gv.relative_lift_pct)}, p {formatPValue(gv.p_value)}
                                  {regressed ? " · worse" : gv.statistically_significant ? " · significant" : ""}
                                </span>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                    );
                  })}
                </div>
              </div>
            )}

            <p className="text-xs text-faint">{expResults.statistical_methodology_note}</p>
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
                          onClick={() => {
                            if (confirmFlagChange(f, f.enabled ? "Turning the flag off" : "Turning the flag on"))
                              void patchFlag(f, { enabled: !f.enabled });
                          }}
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
                        onChange={(e) => {
                          if (confirmFlagChange(f, "Changing who gets the flag"))
                            void patchFlag(f, { targeting_cohort_id: e.target.value });
                        }}
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

                    {holdouts.length > 0 && (
                      <label className="flex min-w-0 max-w-full items-center gap-2">
                        <span className={fieldLabel}>Holdout</span>
                        <select
                          value={f.holdout?.id ?? ""}
                          disabled={!canEdit}
                          onChange={(e) => {
                            if (confirmFlagChange(f, "Changing the holdout"))
                              void patchFlag(f, { holdout_id: e.target.value });
                          }}
                          className={`${field} min-w-0 max-w-full py-1`}
                        >
                          <option value="">None</option>
                          {holdouts.map((h) => (
                            <option key={h.id} value={h.id}>
                              {h.name} ({formatPercent(h.bps / 10_000, 0)} held back)
                            </option>
                          ))}
                        </select>
                      </label>
                    )}
                    {f.layer && (
                      <span className="rounded bg-hover px-2 py-0.5 text-xs text-muted">
                        {layers.find((l) => l.id === f.layer?.id)?.name ?? "Layer"} ·{" "}
                        {formatPercent((f.layer.end_bps - f.layer.start_bps) / 10_000, 0)} of its users
                      </span>
                    )}

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

      <HoldoutsAndLayers
        projectId={projectId}
        holdouts={holdouts}
        layers={layers}
        events={availableEvents}
        canEdit={canEdit}
        onChanged={loadAll}
      />
    </div>
  );
};
