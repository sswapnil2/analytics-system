import React, { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, FlaskConical, Pause, Play, Plus, RefreshCw, Rocket, Square, ToggleLeft } from "lucide-react";
import { api, BanditResults, DecisionInput, Experiment, ExperimentKind, ExperimentMetric, ExperimentResults, FeatureFlag, FlagPatch, MetricDirection, MetricResult, MetricType, TrafficGroup, VariantLiftRow } from "../api/client";
import { HoldoutsAndLayers } from "../components/HoldoutsAndLayers";
import { ConcludeDialog, StartReview } from "../components/ExperimentDialogs";
import { dayOfTotal, ExperimentIndex } from "../components/ExperimentIndex";
import { defaultVariants, toAllocations, VariantDraft, VariantsEditor, variantsProblem } from "../components/VariantsEditor";
import { isTwoStep, MetricRow, metricRowProblem, MetricRows } from "../components/MetricRows";
import { StepWhere, StepWhereEditor, stepWhereToFilters } from "../components/StepWhere";
import { AudiencePicker, EVERYONE, KEEP, NEW } from "../components/AudiencePicker";
import { Cond, cohortDefinition } from "../lib/cohort";
import { BanditResultsView } from "../components/BanditResults";
import { LiftChart } from "../components/LiftChart";
import { clearFilterCaches, useCohortNames } from "../components/FilterPanel";
import { EmptyState, ErrorNotice, PageHeader, Panel, RanIn, btn, field, fieldLabel, ListSkeleton, ChartSkeleton } from "../components/ui";
import { useAccess } from "../auth/access";
import { PropertyPicker } from "../components/PropertyPicker";
import { errorMessage, formatCount, formatDuration, formatPValue, formatPercent, formatProbability, humanizeEvent, propertyOptions, withUnit } from "../lib/format";
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
  { value: "step_through", label: "Step-through: share who go from A to B", eventLabel: "Then did (B)" },
  { value: "time_between", label: "Time from A to B", eventLabel: "Then did (B)" },
];

/** Metrics shown as a rate: conversions and step-throughs. */
const isRate = (type: MetricType | undefined) => !type || type === "conversion" || type === "step_through";

/** A variant's metric value: a rate for conversions, a plain number otherwise. */
/** The unit of the metric named `name` in this experiment (primary, guardrail or secondary). */
function unitOf(exp: Experiment | null | undefined, name: string): string | null {
  if (!exp) return null;
  const all = [exp.primary_metric, ...exp.guardrail_metrics, ...(exp.secondary_metrics ?? [])];
  return all.find((m) => m.name === name)?.unit ?? null;
}

function formatMetricValue(type: MetricType | undefined, v: VariantLiftRow, unit?: string | null): string {
  if (!type || type === "conversion") return formatPercent(v.conversion_rate, 2);
  if (type === "step_through") return formatPercent(v.mean_value, 1);
  if (type === "time_between") return formatDuration(v.mean_value * 1000);
  return withUnit(v.mean_value.toLocaleString(undefined, { maximumFractionDigits: Math.abs(v.mean_value) < 10 ? 3 : 2 }), unit);
}

const DAY_MS = 86_400_000;

/** Segments smaller than this (users per variant) are too noisy to show a lift for. */
const MIN_SEGMENT_USERS = 100;

/** Properties that make no sense as segments: Pulse's own, experiment plumbing and ids. */
function isSegmentable(prop: string, pii: Set<string>): boolean {
  const name = prop.replace(/^user\./, "");
  if (name.startsWith("$") || pii.has(name)) return false;
  if (["variant", "flag_key", "holdout_id", "group"].includes(name)) return false;
  // Ids are one value per user or merchant: every segment would be a single user.
  return !/(^|_)id$/i.test(name);
}

const STATUS_LABEL: Record<string, string> = { draft: "draft", running: "running", paused: "paused", concluded: "stopped" };

/**
 * Share of active users the experiment can reach: rollout × layer slice × users outside the
 * holdout. `layerPct` overrides the flag's slice (a layer picked in the form).
 */
function trafficShare(flag: FeatureFlag | undefined, layerPct: number | null): number {
  if (!flag) return 1;
  const layer = layerPct !== null ? layerPct / 100 : flag.layer ? (flag.layer.end_bps - flag.layer.start_bps) / 10_000 : 1;
  return (flag.rollout_pct / 100) * layer * (1 - (flag.holdout?.bps ?? 0) / 10_000);
}

function trafficSentence(flag: FeatureFlag | undefined, layerPct: number | null): string {
  const parts = [`${Math.round(flag?.rollout_pct ?? 100)}% rollout`];
  if (layerPct !== null || flag?.layer) parts.push("its layer slice");
  if (flag?.holdout) parts.push(`${formatPercent(flag.holdout.bps / 10_000, 0)} held out`);
  const cohort = flag?.targeting_cohort_id ? ", and only the targeted cohort within that" : "";
  return `About ${formatPercent(trafficShare(flag, layerPct), 0)} of active users can be in it (${parts.join(", ")}${cohort}).`;
}

const METRIC_TYPE_LABEL: Record<MetricType, string> = {
  conversion: "conversion",
  count: "count per user",
  sum: "sum per user",
  ratio: "ratio",
  step_through: "step-through rate",
  time_between: "time between",
};

/** True when the primary metric is better low (time, errors): its direction says going up is bad. */
const lowerIsBetter = (exp: Experiment | null | undefined) => exp?.primary_metric.direction === "increase_is_bad";
/** A lift turned so that positive means better than control. */
const gain = (exp: Experiment | null | undefined, liftPct: number) => (lowerIsBetter(exp) ? -liftPct : liftPct);

function describeMetric(m: Experiment["primary_metric"]): string {
  const raw = m.metric_type ?? "conversion";
  const type: MetricType = raw === "continuous" ? (m.numeric_prop ? "sum" : "count") : raw;
  const of = m.numeric_prop && type !== "count" ? ` of ${m.numeric_prop}` : "";
  const per = type === "ratio" && m.denominator ? ` ÷ ${humanizeEvent(m.denominator.event_name)}` : "";
  if (isTwoStep(type) && m.denominator) {
    const pair = `${humanizeEvent(m.denominator.event_name)} → ${humanizeEvent(m.event_name)}`;
    const lead = m.name.startsWith(pair) ? "" : `${pair} · `;
    return `${lead}${METRIC_TYPE_LABEL[type]} · within ${Math.round(m.conversion_window_ms / DAY_MS)}d of exposure`;
  }
  const where = (m.filters ?? [])
    .map((f) => ` where ${f.prop_name} ${f.filter.op === "neq" ? "is not" : f.filter.op === "contains" ? "contains" : "="} ${String(f.filter.val ?? "")}`)
    .join(",");
  const event = humanizeEvent(m.event_name) + where;
  const head = event === m.name && !of && !per ? "" : `${event}${of}${per} · `;
  return `${head}${METRIC_TYPE_LABEL[type]} · within ${Math.round(m.conversion_window_ms / DAY_MS)}d of exposure`;
}

/** Absolute difference from control: percentage points for rates, metric units otherwise. */
function formatAbsDiff(type: MetricType | undefined, d: number, unit?: string | null): string {
  if (type === "time_between") return `${d > 0 ? "+" : d < 0 ? "−" : ""}${formatDuration(Math.abs(d) * 1000)}`;
  const conv = isRate(type);
  const n = conv ? d * 100 : d;
  const text = n.toLocaleString(undefined, { maximumFractionDigits: Math.abs(n) < 10 ? 2 : 1 });
  return conv ? `${n > 0 ? "+" : ""}${text} pts` : withUnit(`${n > 0 ? "+" : ""}${text}`, unit);
}

/** No metric events from anyone exposed: usually broken tracking, not "no effect". */
function hasNoMetricData(type: MetricType | undefined, rows: VariantLiftRow[]): boolean {
  return rows.length > 0 && rows.every((v) => (!type || type === "conversion" ? v.conversions === 0 : v.mean_value === 0));
}

/**
 * One plain sentence on what the primary result means for the decision, checked against the
 * lift the experiment was planned to detect, so a "significant" +0.3% doesn't read as a win.
 */
type Tone = "ok" | "warn" | "danger" | "muted";

function verdict(
  exp: Experiment,
  results: ExperimentResults,
  blindGuardrails: string[],
): { tone: Tone; lines: string[] } | null {
  const treatments = results.variants.filter((v) => !v.is_control);
  if (!treatments.length) return null;
  const target = exp.analysis?.target_lift_pct ?? null;
  const planned = exp.analysis?.planned_users_per_variant ?? null;
  const usersNow = Math.min(...results.variants.map((v) => v.sample_size));
  const pct = (x: number) => `${x > 0 ? "+" : ""}${x.toFixed(1)}%`;
  const range = (v: VariantLiftRow) => `${pct(v.ci95_low_pct)} to ${pct(v.ci95_high_pct)}`;

  let tone = "muted" as Tone;
  let lines: string[];
  if (results.sample_ratio_mismatch) {
    tone = "danger";
    lines = ["Don't decide on this yet: the traffic split is off, so the comparison isn't fair."];
  } else {
    lines = treatments.map((v) => {
      const better = lowerIsBetter(exp) ? "lower" : "higher";
      if (!results.sequential && planned && usersNow < planned) {
        return `Too early for ${v.variant_key}: this is a fixed-horizon test at ${formatCount(usersNow)} of ${formatCount(planned)} planned users per variant, so today's p-value isn't valid yet (likely ${range(v)}).`;
      }
      if (v.statistically_significant && gain(exp, v.relative_lift_pct) > 0) {
        if (target === null || Math.min(gain(exp, v.ci95_low_pct), gain(exp, v.ci95_high_pct)) >= target) {
          tone = tone === "danger" ? tone : "ok";
          return `${v.variant_key} beats control${target !== null ? ` by at least the ${target}% you planned for` : ""}: ${pct(v.relative_lift_pct)} (likely ${range(v)}, ${better} is better).`;
        }
        if (tone === "muted") tone = "warn";
        return `${v.variant_key} beats control, but not clearly by the ${target}% you planned for: ${pct(v.relative_lift_pct)} (likely ${range(v)}, ${better} is better). Decide whether a gain this size is worth shipping.`;
      }
      if (v.statistically_significant) {
        tone = "danger";
        return `${v.variant_key} is worse than control: ${pct(v.relative_lift_pct)} (likely ${range(v)}).`;
      }
      if (planned && usersNow < planned) {
        return `Too early for ${v.variant_key}: ${formatCount(usersNow)} of ${formatCount(planned)} planned users per variant. The likely range is still ${range(v)}.`;
      }
      if (tone === "muted") tone = "warn";
      return `No clear difference for ${v.variant_key} at the planned sample (likely ${range(v)}). If a change this size would matter, it didn't show up: treat as inconclusive.`;
    });
  }
  if (target === null && treatments.some((v) => v.statistically_significant && gain(exp, v.relative_lift_pct) > 0)) {
    lines.push("No target lift was set, so Pulse can't say whether this gain is big enough to be worth shipping.");
  }
  if (blindGuardrails.length) {
    if (tone === "ok") tone = "warn";
    lines.push(`${blindGuardrails.join(", ")} ${blindGuardrails.length > 1 ? "have" : "has"} no data, so harm can't be ruled out.`);
  }
  return { tone, lines };
}

const Verdict: React.FC<{ exp: Experiment; results: ExperimentResults; blindGuardrails: string[] }> = ({
  exp,
  results,
  blindGuardrails,
}) => {
  const v = verdict(exp, results, blindGuardrails);
  if (!v) return null;
  const { tone, lines } = v;
  const box = {
    ok: "border-ok/40 bg-ok/10",
    warn: "border-warn/40 bg-warn-soft",
    danger: "border-danger/40 bg-danger-soft",
    muted: "border-line bg-sunken",
  }[tone];
  return (
    <div role="status" className={`space-y-1 rounded-lg border px-3.5 py-3 text-sm text-ink ${box}`}>
      {lines.map((l) => (
        <p key={l}>{l}</p>
      ))}
    </div>
  );
};

/** A plain-text write-up to paste into a doc or chat: setup, result, verdict, guardrails, decision. */
/** One line on what to do now, from the same reading as the verdict. */
function recommendation(exp: Experiment, results: ExperimentResults, blindGuardrails: string[]): string {
  if (results.sample_ratio_mismatch) return "don't decide yet: fix the traffic split first, then restart.";
  const harmed = results.guardrails.filter((g) => g.regressed_variants.length).map((g) => g.metric_name);
  if (harmed.length) return `don't ship: ${harmed.join(", ")} got worse.`;
  const planned = exp.analysis?.planned_users_per_variant;
  const least = results.variants.length ? Math.min(...results.variants.map((v) => v.sample_size)) : 0;
  // A fixed-horizon test can't be read before its planned sample without inflating false wins.
  if (!results.sequential && planned && least < planned) return "wait: a fixed-horizon test is only valid at its planned sample.";
  const target = exp.analysis?.target_lift_pct ?? null;
  const winners = results.variants.filter((v) => !v.is_control && v.statistically_significant && gain(exp, v.relative_lift_pct) > 0);
  const best = winners.sort((a, b) => gain(exp, b.relative_lift_pct) - gain(exp, a.relative_lift_pct))[0];
  if (best) {
    const meetsTarget = target === null || Math.min(gain(exp, best.ci95_low_pct), gain(exp, best.ci95_high_pct)) >= target;
    const caveat = blindGuardrails.length ? ` after checking ${blindGuardrails.join(", ")} another way` : "";
    return meetsTarget
      ? `ship ${best.variant_key}${caveat}.`
      : `${best.variant_key} wins but may be below the ${target}% target: decide if the gain is worth it${caveat}.`;
  }
  if (results.variants.some((v) => !v.is_control && v.statistically_significant)) return "keep control: the change made things worse.";
  if (planned && least < planned) return "wait: too early to call.";
  return "no clear difference at the planned sample: treat as inconclusive.";
}

function experimentSummary(
  exp: Experiment,
  flag: FeatureFlag | undefined,
  results: ExperimentResults,
  blindGuardrails: string[],
  link: string,
): string {
  const day = (ms: number) => new Date(ms).toLocaleDateString();
  const out = [
    `${exp.name} (${STATUS_LABEL[exp.status] ?? exp.status}, ${day(exp.start_ts_ms)} to ${day(exp.end_ts_ms)})`,
    "",
  ];
  // Status first, so a pasted summary can't be read as a final result while it isn't one.
  if (exp.status !== "concluded" && exp.status !== "draft") {
    const { day: d, total } = dayOfTotal(exp);
    const planned = exp.analysis?.planned_users_per_variant;
    const least = results.variants.length ? Math.min(...results.variants.map((v) => v.sample_size)) : 0;
    out.push(
      `Status: day ${d} of ${total}${planned ? ` · ${formatCount(least)} of ${formatCount(planned)} planned users per variant` : ""} · traffic split ${
        results.sample_ratio_mismatch ? "OFF" : "OK"
      }${blindGuardrails.length ? ` · no data on ${blindGuardrails.join(", ")}` : ""}.`,
      `Recommendation: ${recommendation(exp, results, blindGuardrails)}`,
      "",
    );
  }
  if (exp.problem) out.push(`Problem: ${exp.problem}`);
  if (exp.hypothesis) out.push(`Hypothesis: ${exp.hypothesis}`);
  out.push(`Primary metric: ${exp.primary_metric.name} (${describeMetric(exp.primary_metric)})`);
  if (flag) {
    out.push(`Variants: ${flag.variants.map((v) => `${v.key} ${formatPercent(v.weight_bps / 10_000, 0)}`).join(" / ")}. ${trafficSentence(flag, null)}`);
  }
  out.push("", "Result:");
  for (const v of results.variants) {
    const pct = (x: number) => `${x > 0 ? "+" : ""}${x.toFixed(1)}%`;
    const lift = v.is_control
      ? ""
      : `, ${pct(v.relative_lift_pct)} vs control (likely ${pct(v.ci95_low_pct)} to ${pct(v.ci95_high_pct)})${v.statistically_significant ? ", significant" : ", not significant"}`;
    out.push(`- ${v.variant_key}: ${formatMetricValue(results.metric_type, v, exp.primary_metric.unit)} of ${formatCount(v.sample_size)} users${lift}`);
  }
  const vd = verdict(exp, results, blindGuardrails);
  if (vd) out.push("", "What it means:", ...vd.lines.map((l) => `- ${l}`));
  if (results.guardrails.length) {
    out.push("", "Guardrails:");
    for (const g of results.guardrails) {
      const state = blindGuardrails.includes(g.metric_name)
        ? "no data"
        : g.regressed_variants.length
          ? `harmed by ${g.regressed_variants.join(", ")}`
          : "not harmed";
      out.push(`- ${g.metric_name}: ${state}`);
    }
  }
  if (results.secondary?.length) {
    out.push("", "Secondary metrics:");
    for (const m of results.secondary) {
      out.push(`- ${m.metric_name}: ${m.variants.map((v) => `${v.variant_key} ${formatMetricValue(m.metric_type, v, unitOf(exp, m.metric_name))}`).join(", ")}`);
    }
  }
  if (results.sample_ratio_mismatch) out.push("", "Warning: the traffic split is off, so these results can't be trusted.");
  if (exp.decision) {
    const d = exp.decision;
    const what = d.outcome === "ship" ? `Shipped ${d.variant}` : d.outcome === "keep_control" ? "Kept control" : "Inconclusive";
    out.push("", `Decision: ${what}, by ${d.decided_by} on ${day(d.decided_at_ms)}.${d.reason ? ` ${d.reason}` : ""}`);
    for (const a of d.acknowledged ?? []) out.push(`- Accepted: ${a}`);
  }
  out.push("", link);
  return out.join("\n");
}

/** Copies text, then says so on the button for a moment. */
const CopyButton: React.FC<{ label: string; text: () => string }> = ({ label, text }) => {
  const [done, setDone] = useState<"ok" | "fail" | null>(null);
  return (
    <button
      type="button"
      className={btn.ghost}
      onClick={() => {
        navigator.clipboard
          .writeText(text())
          .then(() => setDone("ok"))
          .catch(() => setDone("fail"))
          .finally(() => setTimeout(() => setDone(null), 2000));
      }}
    >
      {done === "ok" ? <Check className="h-3.5 w-3.5" /> : null}
      <span aria-live="polite">{done === "ok" ? "Copied" : done === "fail" ? "Couldn't copy" : label}</span>
    </button>
  );
};

/** Secondary metrics: what moved besides the primary metric. Reported only, never decides. */
const SecondaryResults: React.FC<{ metrics: MetricResult[]; exp: Experiment | null | undefined }> = ({ metrics, exp }) => (
  <div className="border-t border-line pt-4">
    <h3 className="mb-1 text-sm font-medium text-ink">Secondary metrics</h3>
    <p className="mb-2 text-xs text-muted">For context: these don't decide the outcome, and with several of them one may move by chance.</p>
    <div className="overflow-x-auto">
      <table className="w-full min-w-[560px] text-sm">
        <thead>
          <tr className="border-b border-line text-xs text-muted">
            <th className="py-2 text-left font-medium">Metric</th>
            <th className="py-2 text-left font-medium">Variant</th>
            <th className="py-2 text-right font-medium">Users</th>
            <th className="py-2 text-right font-medium">Value</th>
            <th className="py-2 text-right font-medium">Difference</th>
            <th className="py-2 text-right font-medium">Read</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-line">
          {metrics.flatMap((m) =>
            m.variants.map((v, i) => (
              <tr key={`${m.metric_name}:${v.variant_key}`}>
                <td className="py-2 text-ink">{i === 0 ? m.metric_name : ""}</td>
                <td className="py-2 text-ink-2">{v.variant_key}</td>
                <td className="tabular py-2 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                <td className="tabular py-2 text-right text-ink">
                  {formatMetricValue(m.metric_type, v, unitOf(exp, m.metric_name))}
                </td>
                <td className="tabular py-2 text-right text-ink-2">
                  {v.is_control || v.abs_diff === undefined ? "–" : formatAbsDiff(m.metric_type, v.abs_diff, unitOf(exp, m.metric_name))}
                </td>
                <td className="py-2 text-right text-muted">
                  {v.is_control
                    ? "–"
                    : v.statistically_significant
                      ? v.relative_lift_pct > 0
                        ? "Higher"
                        : "Lower"
                      : "No clear difference"}
                </td>
              </tr>
            )),
          )}
        </tbody>
      </table>
    </div>
  </div>
);

const ExperimentSetup: React.FC<{
  exp: Experiment;
  flag: FeatureFlag | undefined;
  cohortName: (id: string) => string;
  usersPerVariantNow: number;
  /** Whether the project has a webhook; null when unknown (no permission to look). */
  hasWebhook: boolean | null;
  /** Turns this experiment's webhook notifications on or off; absent for viewers. */
  onNotify?: (on: boolean) => void;
}> = ({ exp, flag, cohortName, usersPerVariantNow, hasWebhook, onNotify }) => {
  const planned = exp.analysis?.planned_users_per_variant ?? null;
  const bandit = exp.kind === "bandit";
  const rows: [string, React.ReactNode][] = [];
  if (exp.problem) rows.push(["Problem", <span className="whitespace-pre-wrap">{exp.problem}</span>]);
  rows.push([
    "Primary metric",
    <>
      <span className="font-medium text-ink">{exp.primary_metric.name}</span> · {describeMetric(exp.primary_metric)} ·{" "}
      {lowerIsBetter(exp) ? "lower is better" : "higher is better"}
    </>,
  ]);
  if (!bandit) {
    rows.push([
      "Guardrails",
      exp.guardrail_metrics.length
        ? exp.guardrail_metrics.map((g) => (
            <div key={`${g.name}:${g.event_name}`}>
              <span className="font-medium text-ink">{g.name}</span> · {describeMetric(g)} · bad when it{" "}
              {g.direction === "increase_is_bad" ? "goes up" : "goes down"}
              {g.auto_stop ? " · auto-stops" : ""}
            </div>
          ))
        : "None",
    ]);
    if (exp.secondary_metrics?.length) {
      rows.push([
        "Secondary",
        exp.secondary_metrics.map((m) => (
          <div key={`${m.name}:${m.event_name}`}>
            <span className="font-medium text-ink">{m.name}</span> · {describeMetric(m)}
          </div>
        )),
      ]);
    }
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
  if (exp.owner || exp.created_by)
    rows.push(["Owner", exp.owner && exp.created_by && exp.owner !== exp.created_by ? `${exp.owner} (created by ${exp.created_by})` : exp.owner || exp.created_by]);
  if (!bandit)
    rows.push([
      "Notifications",
      <>
        <label className="flex items-start gap-2">
          <input
            type="checkbox"
            checked={exp.notify !== false}
            disabled={!onNotify}
            onChange={(e) => onNotify?.(e.target.checked)}
            className="mt-1"
          />
          <span>
            Send a webhook when the result turns significant, the planned sample is reached, a guardrail gets worse or has
            no data, or auto-stop fires.
          </span>
        </label>
        <span className={`block text-xs ${hasWebhook === false ? "text-warn" : "text-muted"}`}>
          {hasWebhook === false
            ? "This project has no webhook yet, so nothing is sent. An admin can add one under Security and integrations → Provider credentials (Alert webhook)."
            : hasWebhook
              ? "Sent to the project's webhook. Every milestone is also in the audit log."
              : "Sent to the project's webhook, if an admin has set one up. Every milestone is also in the audit log."}
        </span>
      </>,
    ]);
  if (exp.decision) {
    const d = exp.decision;
    const what = d.outcome === "ship" ? `Shipped ${d.variant}` : d.outcome === "keep_control" ? "Kept control" : "Inconclusive";
    const flagDid = d.flag_action === "rollout" ? `everyone gets ${d.variant}` : d.flag_action === "rollback" ? "flag turned off" : "flag left as it was";
    rows.push([
      "Decision",
      <>
        <span className="font-medium text-ink">{what}</span> · {flagDid} · {d.decided_by},{" "}
        {new Date(d.decided_at_ms).toLocaleDateString()}
        {d.reason && <div className="whitespace-pre-wrap">{d.reason}</div>}
        {!!d.acknowledged?.length && (
          <ul className="mt-1 list-disc pl-4 text-xs text-warn">
            {d.acknowledged.map((a) => (
              <li key={a}>Accepted: {a}</li>
            ))}
          </ul>
        )}
      </>,
    ]);
  }
  rows.push([
    "Dates",
    `${new Date(exp.start_ts_ms).toLocaleDateString()} to ${new Date(exp.end_ts_ms).toLocaleDateString()} · day ${dayOfTotal(exp).day} of ${dayOfTotal(exp).total}`,
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

/**
 * Why a metric has no data from users in the test: the event never fired since it started, or it fired
 * but not from those users within the window (wrong user id, fired before exposure, other platform).
 */
const WhyNoData: React.FC<{ projectId: string; event: string; exp: Experiment }> = ({ projectId, event, exp }) => {
  const [users, setUsers] = useState<number | null>(null);
  useEffect(() => {
    let alive = true;
    setUsers(null);
    api
      .queryInsights(projectId, {
        time_range: { start_ts_ms: exp.start_ts_ms, end_ts_ms: Math.min(Date.now(), exp.end_ts_ms) },
        interval: "day",
        series: [{ label: "0", event_name: event, metric: { type: "uniques" }, filters: [] }],
        breakdown_by: [],
      })
      .then((res) => alive && setUsers(res.series[0]?.overall_value ?? 0))
      .catch(() => alive && setUsers(null));
    return () => {
      alive = false;
    };
  }, [projectId, event, exp.start_ts_ms, exp.end_ts_ms]);
  if (users === null) return null;
  const name = humanizeEvent(event);
  return (
    <p className="mb-1.5 text-xs text-ink-2">
      {users === 0
        ? `No one has sent ${name} since this test started. Check the event name, or that your app tracks it.`
        : `${formatCount(users)} users sent ${name} since this test started, but none of them within the window after seeing this test. They may be users outside it, it may fire before the flag is checked, or it may be tracked under a different user id than the exposure. Ask your engineer which.`}
    </p>
  );
};

function validateFlagKey(key: string): string | null {
  if (!key.trim()) return "Enter a flag key.";
  if (!FLAG_KEY_PATTERN.test(key.trim())) return "Use lowercase letters, numbers and underscores only.";
  return null;
}

/** Why the users seen so far can't back shipping, or null when they can. */
function sampleCaveat(exp: Experiment, results: ExperimentResults | null, bandit: BanditResults | null): string | null {
  const sizes = bandit ? bandit.arms.map((a) => a.users) : (results?.variants ?? []).map((v) => v.sample_size);
  const total = sizes.reduce((a, n) => a + n, 0);
  if (exp.status === "draft" || total === 0) return "No one has been in this experiment, so there is no evidence either way.";
  const planned = exp.analysis?.planned_users_per_variant;
  const least = Math.min(...sizes);
  if (planned && least < planned)
    return `Only ${formatCount(least)} of the planned ${formatCount(planned)} users per variant (${Math.round((least / planned) * 100)}%) have been in the test, so this result may not hold.`;
  return null;
}

/** Users per variant for the target lift, daily users who can be in it, and the days that takes. */
function plan(
  baseline: { rate: number; dailyUsers: number },
  mde: number,
  flag: FeatureFlag | undefined,
  sequential: boolean,
  layerPct: number | null,
  eligible: number | null,
) {
  const variants = Math.max(2, flag?.variants.length ?? 2);
  const n = usersPerVariant(baseline.rate, mde, { variants, sequential });
  // ponytail: the audience's share of the last 14 days' active users scales daily traffic; its own
  // conversion rate may differ from everyone's, so the estimate stays approximate.
  const audienceShare = eligible === null ? 1 : Math.min(1, eligible / Math.max(1, baseline.dailyUsers * 14));
  const perDay = baseline.dailyUsers * trafficShare(flag, layerPct) * audienceShare;
  return { n, perDay, need: Math.ceil((n * variants) / Math.max(perDay, 1e-9)), audienceShare };
}

/** Longest an experiment can be planned to run from the form. */
const MAX_DAYS = 120;

const SampleEstimate: React.FC<{
  baseline: { rate: number; dailyUsers: number } | null;
  mde: number;
  flag: FeatureFlag | undefined;
  days: number;
  event: string;
  sequential: boolean;
  layerPct: number | null;
  onUseDays: (days: number) => void;
  /** Users in the chosen audience now; null for everyone. */
  eligible: number | null;
}> = ({ baseline, mde, flag, days, event, sequential, layerPct, onUseDays, eligible }) => {
  if (!baseline) return null;
  const name = humanizeEvent(event);
  const { n, perDay, need, audienceShare } = plan(baseline, mde, flag, sequential, layerPct, eligible);
  const short = need > days;
  return (
    <p className={`rounded-lg border px-3.5 py-2.5 text-sm ${short ? "border-warn/40 bg-warn-soft text-ink" : "border-line bg-sunken text-ink-2"}`}>
      {Number.isFinite(n) ? (
        <>
          About <span className="font-semibold text-ink">{formatCount(n)} users per variant</span> to spot a {Math.round(mde * 100)}% change
          on today's {formatPercent(baseline.rate, 1)} conversion. {trafficSentence(flag, layerPct)}
          {eligible !== null && ` Your audience is about ${formatPercent(audienceShare, 0)} of active users.`} At ~
          {formatCount(Math.round(perDay))} users a day, that's about{" "}
          <span className="font-semibold text-ink">{need} days</span>
          {short ? `, longer than the ${days} days you picked.` : "."}
          {short && need <= MAX_DAYS && (
            <>
              {" "}
              <button type="button" onClick={() => onUseDays(need)} className="font-medium text-ink underline">
                Run for {need} days
              </button>
            </>
          )}
          {short && need > MAX_DAYS && ` That's more than ${MAX_DAYS} days: aim for a bigger change or more traffic.`}
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
  const [cohortsVersion, setCohortsVersion] = useState(0);
  const cohorts = useCohortNames(projectId, cohortsVersion);
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
  const [newVariants, setNewVariants] = useState<VariantDraft[]>(defaultVariants);
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
  const [expProblem, setExpProblem] = useState("");
  const [expAudience, setExpAudience] = useState(KEEP);
  const [expAudienceConds, setExpAudienceConds] = useState<Cond[]>([]);
  /** Users matching the chosen audience now; null for everyone. */
  const [expEligible, setExpEligible] = useState<number | null>(null);
  const [expOwner, setExpOwner] = useState("");
  /** Whether the project has a webhook for notifications; null until known or when we can't look. */
  const [hasWebhook, setHasWebhook] = useState<boolean | null>(null);
  useEffect(() => {
    let alive = true;
    setHasWebhook(null);
    api
      .listByokCredentials(projectId)
      .then((creds) => alive && setHasWebhook(creds.some((c) => c.category === "webhook")))
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, [projectId]);
  // No default: the busiest event is usually one everyone does, which a test can't move.
  const [expMetric, setExpMetric] = useState("");
  const [expWhere, setExpWhere] = useState<StepWhere | null>(null);
  const eventOptions = availableEvents.map((ev) => ({ value: ev, label: humanizeEvent(ev), group: "Events" }));
  const [expMetricType, setExpMetricType] = useState<MetricType>("conversion");
  const [expProp, setExpProp] = useState("");
  const [expUnit, setExpUnit] = useState("");
  const [expDenominator, setExpDenominator] = useState("");
  const [expCuped, setExpCuped] = useState(true);
  const propOptions = propertyOptions(availableProps.filter((p) => !p.startsWith("user.") && !p.startsWith("$")));
  const [segment, setSegment] = useState("");
  /** Properties marked as personal data in the lexicon; never offered as segments. */
  const [piiProps, setPiiProps] = useState<Set<string>>(new Set());
  useEffect(() => {
    const ctl = new AbortController();
    api
      .getLexicon(projectId, ctl.signal)
      .then((lx) => setPiiProps(new Set(lx.properties.filter((p) => p.pii_masked).map((p) => p.name))))
      .catch(() => {});
    return () => ctl.abort();
  }, [projectId]);
  const segmentRef = useRef(segment);
  segmentRef.current = segment;
  const [expGuardrails, setExpGuardrails] = useState<MetricRow[]>([]);
  const [expSecondaries, setExpSecondaries] = useState<MetricRow[]>([]);
  const [expSequential, setExpSequential] = useState(true);
  const [expKind, setExpKind] = useState<ExperimentKind>("ab");
  /** Bandits need to know which way is better to pick a winner. */
  const [expLowerIsBetter, setExpLowerIsBetter] = useState(false);
  const [expWindowDays, setExpWindowDays] = useState(7);
  const [expDays, setExpDays] = useState(14);
  const [expLayerId, setExpLayerId] = useState("");
  const [expLayerPct, setExpLayerPct] = useState(50);
  const [expError, setExpError] = useState<string | null>(null);
  const baseline = useBaseline(projectId, expFormOpen ? expMetric : "");
  // A validation message is about the fields as they were; drop it once any of them changes.
  useEffect(() => setExpError(null), [expName, expFlagId, expMetric, expMetricType, expProp, expDenominator, expKind]);
  const [expCreating, setExpCreating] = useState(false);
  /** The review shown before going live: the form's new experiment, or a saved draft. */
  const [review, setReview] = useState<"form" | "draft" | null>(null);
  /** Open conclude dialog, optionally preselecting the variant to ship. */
  const [concludeFor, setConcludeFor] = useState<{ variant?: string } | null>(null);

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

  // Deep links: `exp=<id>` opens that experiment (shareable); `new=1&from=A&to=B` (from a
  // funnel drop-off) opens the form with an A-to-B step-through metric filled in.
  const selectRef = useRef<(id: string) => void>(() => {});
  useEffect(() => {
    const apply = (initial: boolean) => {
      const [path, search = ""] = window.location.hash.replace(/^#/, "").split("?");
      if (!path.replace(/^\//, "").startsWith("experiments")) return;
      const params = new URLSearchParams(search);
      const exp = params.get("exp");
      if (exp && exp !== selectedExpIdRef.current) {
        if (initial) {
          selectedExpIdRef.current = exp; // loadAll picks it up
          setSelectedExpId(exp);
        } else selectRef.current(exp);
      }
      if (params.get("new") === "1") {
        const from = params.get("from");
        const to = params.get("to");
        setExpFormOpen(true);
        if (from && to) {
          setExpMetricType("step_through");
          setExpDenominator(from);
          setExpMetric(to);
          setExpHypothesis(`If we fix what stops people after ${humanizeEvent(from)}, more of them will reach ${humanizeEvent(to)}.`);
          setExpProblem(`Users drop off between ${humanizeEvent(from)} and ${humanizeEvent(to)}.`);
        }
        for (const k of ["new", "from", "to"]) params.delete(k);
        window.history.replaceState(null, "", `#${path}?${params.toString()}`);
      }
    };
    apply(true);
    const onHash = () => apply(false);
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    void loadAll();
  }, [loadAll, refreshToken]);

  const handleSelectExperiment = async (expId: string, seg = segmentRef.current) => {
    selectedExpIdRef.current = expId;
    setSelectedExpId(expId);
    // Keep the URL on this experiment, so it can be shared.
    {
      const [path, search = ""] = window.location.hash.replace(/^#/, "").split("?");
      const params = new URLSearchParams(search);
      if (expId) params.set("exp", expId);
      else params.delete("exp");
      window.history.replaceState(null, "", `#${path}?${params.toString()}`);
    }
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
  selectRef.current = (id) => void handleSelectExperiment(id);

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
      (e) => (e.flag_id === flag.id || e.flag_key === flag.key) && (e.status === "running" || e.status === "paused"),
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
    setExpProblem("");
    setExpOwner("");
    setExpAudience(KEEP);
    setExpAudienceConds([]);
    setExpMetric("");
    setExpWhere(null);
    setExpMetricType("conversion");
    setExpProp("");
    setExpUnit("");
    setExpDenominator("");
    setExpGuardrails([]);
    setExpSecondaries([]);
    setExpKind("ab");
    setExpLowerIsBetter(false);
    setExpLayerId("");
    setExpError(null);
  };

  /** The form's first problem, or null when it can be saved. */
  const expFormProblem = (): string | null => {
    const flag = flags.find((f) => f.id === expFlagId);
    if (!expName.trim() || !flag || !expMetric) return "Add a name, pick a flag and pick the metric's event.";
    if (expMetricType === "sum" && !expProp) return "Pick the property to add up.";
    if (expMetricType === "ratio" && !expDenominator) return "Pick the denominator event.";
    if (isTwoStep(expMetricType) && !expDenominator) return "Pick the first step (A).";
    if (expKind === "ab") {
      for (const r of [...expGuardrails, ...expSecondaries]) {
        const p = metricRowProblem(r);
        if (p) return p;
      }
    }
    if (expKind === "bandit" && flag.variants.length < 2) return "A bandit needs a flag with at least two variants.";
    const layer = layers.find((l) => l.id === expLayerId);
    if (layer && expLayerPct * 100 > (layer.free_bps ?? 10_000))
      return `${layer.name} has ${formatPercent((layer.free_bps ?? 10_000) / 10_000, 0)} free; ask for that much or less.`;
    if (expAudience === NEW && !cohortDefinition(expAudienceConds, "and"))
      return "Finish the new audience: every line needs an event, or a property and a value.";
    const busy = experiments.find((e) => e.flag_key === flag.key && (e.status === "running" || e.status === "paused"));
    if (busy && expAudience !== KEEP)
      return `"${busy.name}" is live on ${flag.key}, so its audience can't change now. Keep the flag's audience or pick another flag.`;
    return null;
  };

  const createExperiment = async (status: "draft" | "running") => {
    setReview(null);
    const flag = flags.find((f) => f.id === expFlagId);
    const problem = expFormProblem();
    if (problem || !flag) {
      setExpError(problem);
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
    const primary = {
      ...metric(expMetric, expLowerIsBetter ? "increase_is_bad" : "decrease_is_bad"),
      ...(isTwoStep(expMetricType) ? { name: `${humanizeEvent(expDenominator)} → ${humanizeEvent(expMetric)}` } : {}),
      metric_type: expMetricType,
      numeric_prop: expMetricType === "sum" || expMetricType === "ratio" ? expProp || null : null,
      unit: expMetricType === "sum" || expMetricType === "ratio" ? expUnit.trim() || null : null,
      denominator: expMetricType === "ratio" || isTwoStep(expMetricType) ? { event_name: expDenominator } : null,
      filters: stepWhereToFilters(expWhere),
    };
    const fromRow = (r: MetricRow, guardrail: boolean): ExperimentMetric => ({
      name: isTwoStep(r.type)
        ? `${humanizeEvent(r.from)} → ${humanizeEvent(r.event)}${r.type === "time_between" ? " (time)" : ""}`
        : humanizeEvent(r.event),
      event_name: r.event,
      filters: stepWhereToFilters(r.where),
      metric_type: r.type,
      denominator: isTwoStep(r.type) ? { event_name: r.from } : null,
      conversion_window_ms: expWindowDays * DAY_MS,
      direction: r.direction,
      auto_stop: guardrail && r.autoStop && expSequential,
    });
    // The estimate assumes a conversion rate; other metric types use the server's default horizon.
    const planned = baseline && expMetricType === "conversion"
      ? usersPerVariant(baseline.rate, mde / 100, { variants: Math.max(2, flag.variants.length), sequential: expSequential })
      : Infinity;
    try {
      // The audience lives on the flag, so set it first; the experiment then reports on that audience.
      let cohortId: string | null = null;
      if (expAudience === EVERYONE) cohortId = "";
      else if (expAudience === NEW) {
        const def = cohortDefinition(expAudienceConds, "and");
        const cohort = (await api.createCohort(projectId, {
          name: `${expName.trim()} audience`,
          description: `Made for the experiment "${expName.trim()}".`,
          definition: def,
        })) as { id: string };
        cohortId = cohort.id;
        clearFilterCaches(projectId);
        setCohortsVersion((v) => v + 1);
        // If anything below fails, a retry reuses this cohort instead of making another.
        setExpAudience(cohort.id);
      } else if (expAudience) cohortId = expAudience;
      if (cohortId !== null && cohortId !== (flag.targeting_cohort_id ?? "")) {
        const updated = await api.patchFlag(projectId, flag.id, { targeting_cohort_id: cohortId });
        setFlags((prev) => prev.map((f) => (f.id === updated.id ? updated : f)));
      }
      const created = await api.createExperiment(projectId, {
        name: expName.trim(),
        flag_key: flag.key,
        flag_id: flag.id,
        hypothesis: expHypothesis.trim(),
        problem: expProblem.trim(),
        owner: expOwner.trim(),
        start_ts_ms: now,
        end_ts_ms: now + expDays * DAY_MS,
        primary_metric: primary,
        kind: expKind,
        status,
        layer: expLayerId ? { layer_id: expLayerId, traffic_pct: expLayerPct } : null,
        guardrail_metrics: bandit ? [] : expGuardrails.map((r) => fromRow(r, true)),
        secondary_metrics: bandit ? [] : expSecondaries.map((r) => fromRow(r, false)),
        analysis: {
          sequential: expSequential,
          alpha: 0.05,
          planned_users_per_variant: Number.isFinite(planned) ? planned : null,
          cuped: expCuped,
          target_lift_pct: mde,
        },
      });
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

  /** Ends the experiment with a recorded decision; the server applies its flag action. */
  const conclude = async (exp: Experiment, decision: DecisionInput) => {
    setActing(true);
    setActionError(null);
    try {
      await api.patchExperiment(projectId, exp.id, { status: "concluded", decision });
      setConcludeFor(null);
      await loadAll();
    } catch (err) {
      setActionError(errorMessage(err));
      setConcludeFor(null);
    } finally {
      setActing(false);
    }
  };

  /** Starts a saved draft: its clock starts now and its flag turns on. */
  const startDraft = async (exp: Experiment) => {
    setReview(null);
    setActing(true);
    setActionError(null);
    try {
      await api.patchExperiment(projectId, exp.id, { status: "running" });
      await loadAll();
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
    const vErr = variantsProblem(newVariants);
    if (vErr) {
      setFlagError(vErr);
      return;
    }
    setCreating(true);
    setFlagError(null);
    try {
      await api.createFlag(projectId, {
        key: newFlagKey.trim(),
        description: newFlagDesc.trim(),
        rollout_pct: 50,
        // Off until an experiment starts on it, so creating a flag never changes what users see.
        enabled: false,
        variants: toAllocations(newVariants),
      });
      setNewFlagKey("");
      setNewFlagDesc("");
      setNewVariants(defaultVariants());
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
  /** Guardrails with no data among exposed users: harm can't be ruled out, and auto-stop can't fire. */
  const blindGuardrails =
    expResults && !noExposures ? expResults.guardrails.filter((g) => hasNoMetricData(g.metric_type, g.variants)).map((g) => g.metric_name) : [];
  const harmedBy: Record<string, string[]> = {};
  for (const g of expResults?.guardrails ?? []) for (const v of g.regressed_variants) (harmedBy[v] ??= []).push(g.metric_name);
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
              const problem = expFormProblem();
              setExpError(problem);
              if (!problem) setReview("form");
            }}
            className="space-y-4"
          >
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
                <span className={fieldLabel}>Problem</span>
                <textarea
                  value={expProblem}
                  onChange={(e) => setExpProblem(e.target.value)}
                  rows={2}
                  maxLength={2000}
                  placeholder="e.g. 17% of users quit after the PAN step"
                  className={field}
                />
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Owner</span>
                <input
                  value={expOwner}
                  onChange={(e) => setExpOwner(e.target.value)}
                  maxLength={200}
                  placeholder="You, unless someone else answers for it"
                  className={field}
                />
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
                  onChange={(e) => {
                    const t = e.target.value as MetricType;
                    setExpMetricType(t);
                    // Shorter is better for time; the PM can still flip it.
                    setExpLowerIsBetter(t === "time_between");
                  }}
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
              {expMetric && (
                <div className="sm:col-span-2">
                  <StepWhereEditor
                    idx={0}
                    name="the metric's event"
                    projectId={projectId}
                    event={expMetric}
                    value={expWhere}
                    props={availableProps.filter((p) => !p.startsWith("user.") && !p.startsWith("$"))}
                    onChange={setExpWhere}
                  />
                </div>
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
              {(expMetricType === "sum" || expMetricType === "ratio") && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>Unit (optional)</span>
                  <input
                    value={expUnit}
                    onChange={(e) => setExpUnit(e.target.value)}
                    maxLength={12}
                    placeholder="₹, $, items…"
                    className={field}
                  />
                </label>
              )}
              {(expMetricType === "ratio" || isTwoStep(expMetricType)) && (
                <PropertyPicker
                  label={expMetricType === "ratio" ? "Denominator event" : "First step (A)"}
                  value={expDenominator}
                  onChange={setExpDenominator}
                  placeholder="Search events"
                  recentKey={`pulse.recent-events.${projectId}`}
                  options={eventOptions}
                />
              )}
              {(
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
              {expKind === "ab" && (
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Smallest lift worth shipping (%)</span>
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={mde}
                  onChange={(e) => setMde(Math.min(100, Math.max(1, Number(e.target.value) || 1)))}
                  className={field}
                />
                <span className="text-xs text-muted">
                  Relative change vs control. The verdict checks results against it, and the sample size is planned for it.
                </span>
              </label>
              )}
              <AudiencePicker
                projectId={projectId}
                flag={flags.find((f) => f.id === expFlagId)}
                value={expAudience}
                conds={expAudienceConds}
                onChange={(v, c) => {
                  setExpAudience(v);
                  setExpAudienceConds(c);
                }}
                onEligible={setExpEligible}
                events={eventOptions}
                props={availableProps}
              />
              {layers.length > 0 && (
                <div className="flex flex-col gap-1 sm:col-span-2">
                  <span className={fieldLabel}>Layer</span>
                  <div className="flex flex-wrap items-center gap-2">
                    <select aria-label="Layer" value={expLayerId} onChange={(e) => setExpLayerId(e.target.value)} className={field}>
                      <option value="">None: can overlap other experiments</option>
                      {layers.map((l) => {
                        const free = l.free_bps ?? 10_000;
                        // Slices are at least 1% (100 bps); a layer with less free can't take another experiment.
                        return (
                          <option key={l.id} value={l.id} disabled={free < 100}>
                            {l.name} ({free < 100 ? "full: stop an experiment in it first" : `${formatPercent(free / 10_000, 0)} free`})
                          </option>
                        );
                      })}
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
                <span className={fieldLabel}>Run for (days)</span>
                <input
                  type="number"
                  min={1}
                  max={MAX_DAYS}
                  value={expDays}
                  onChange={(e) => setExpDays(Math.min(MAX_DAYS, Math.max(1, Math.round(Number(e.target.value)) || 1)))}
                  className={field}
                />
                <span className="text-xs text-muted">Ends {new Date(Date.now() + expDays * DAY_MS).toLocaleDateString()}</span>
              </label>
              {expKind === "ab" && (
                <>
                  <MetricRows
                    title="Guardrails"
                    hint="Things the change must not make worse, such as errors or KYC failures. Pulse flags a variant that makes one significantly worse, and can stop the experiment."
                    rows={expGuardrails}
                    onChange={setExpGuardrails}
                    guardrail
                    sequential={expSequential}
                    events={eventOptions}
                    recentKey={`pulse.recent-events.${projectId}`}
                    projectId={projectId}
                    props={availableProps.filter((p) => !p.startsWith("user.") && !p.startsWith("$"))}
                  />
                  <MetricRows
                    title="Secondary metrics"
                    hint="Other things you want to see move, like time to finish a step. Shown with the results; they don't decide the outcome."
                    rows={expSecondaries}
                    onChange={setExpSecondaries}
                    guardrail={false}
                    sequential={expSequential}
                    events={eventOptions}
                    recentKey={`pulse.recent-events.${projectId}`}
                    projectId={projectId}
                    props={availableProps.filter((p) => !p.startsWith("user.") && !p.startsWith("$"))}
                  />
                </>
              )}
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
              layerPct={expLayerId ? expLayerPct : null}
              onUseDays={setExpDays}
              eligible={expEligible}
            />
            )}
            </>
            )}
            {expError && <ErrorNotice message={expError} />}
            <div className="flex flex-wrap justify-end gap-2">
              <button type="button" onClick={closeExpForm} className={btn.ghost}>
                Cancel
              </button>
              <button
                type="button"
                disabled={expCreating || flags.length === 0}
                onClick={() => void createExperiment("draft")}
                className={btn.secondary}
              >
                Save as draft
              </button>
              <button type="submit" disabled={expCreating || flags.length === 0} className={btn.primary}>
                {expCreating ? "Starting…" : "Review and start"}
              </button>
            </div>
          </form>
        </Panel>
      )}

      {loadError && <ErrorNotice message={loadError} onRetry={() => void loadAll()} />}

      {experiments.length > 1 && (
        <Panel title="All experiments" meta={`${experiments.length} in this project`}>
          <ExperimentIndex
            experiments={experiments}
            selectedId={selectedExpId}
            onSelect={(id) => void handleSelectExperiment(id)}
          />
        </Panel>
      )}

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
                    {`${ex.name} · ${STATUS_LABEL[ex.status] ?? ex.status}`}
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

            {selectedExp?.status === "paused" && (
              <p role="status" className="rounded-lg border border-line bg-sunken px-3.5 py-2.5 text-sm text-ink-2">
                <span className="font-medium text-ink">Paused.</span> Users keep getting their variants, results keep
                counting, and guardrails{selectedExp.guardrail_metrics.some((g) => g.auto_stop) ? " and auto-stop" : ""} keep
                watching.{selectedExp.kind === "bandit" ? " Daily traffic moves are on hold." : ""} The end date doesn't
                move. To take the variant away from users, stop the experiment and keep control.
              </p>
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
                  {selectedExp.status === "draft"
                    ? "Draft · not started"
                    : selectedExp.status === "running"
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
                {canEdit && selectedExp.status === "draft" && (
                  <>
                    <button type="button" disabled={acting} onClick={() => setReview("draft")} className={btn.primary}>
                      <Play className="h-3.5 w-3.5" />
                      Start…
                    </button>
                    <button type="button" disabled={acting} onClick={() => setConcludeFor({})} className={btn.secondary}>
                      Discard draft
                    </button>
                  </>
                )}
                {canEdit && (selectedExp.status === "running" || selectedExp.status === "paused") && (
                  <>
                    {banditResults?.ready_to_ship && banditResults.best_variant && (
                      <button
                        type="button"
                        disabled={acting}
                        onClick={() => setConcludeFor({ variant: banditResults.best_variant ?? undefined })}
                        className={btn.primary}
                      >
                        <Rocket className="h-3.5 w-3.5" />
                        Ship {banditResults.best_variant}
                      </button>
                    )}
                    {expResults.variants
                      .filter((v) => !v.is_control && v.statistically_significant && gain(selectedExp, v.relative_lift_pct) > 0)
                      .map((v) => (
                        <button
                          key={v.variant_key}
                          type="button"
                          disabled={acting}
                          onClick={() => setConcludeFor({ variant: v.variant_key })}
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
                      title={
                        selectedExp.status === "paused"
                          ? "Turn guardrail checks back on"
                          : selectedExp.kind === "bandit"
                            ? "Stops daily traffic moves; users keep their variants and guardrails keep watching"
                            : "Marks the test as on hold; users keep their variants and guardrails keep watching"
                      }
                      className={btn.secondary}
                    >
                      {selectedExp.status === "paused" ? <Play className="h-3.5 w-3.5" /> : <Pause className="h-3.5 w-3.5" />}
                      {selectedExp.status === "paused" ? "Resume" : "Pause"}
                    </button>
                    <button type="button" disabled={acting} onClick={() => setConcludeFor({})} className={btn.secondary}>
                      <Square className="h-3.5 w-3.5" />
                      Stop experiment…
                    </button>
                  </>
                )}
              </div>
            )}

            {selectedExp && (
              <div className="flex flex-wrap gap-1">
                <CopyButton label="Copy link" text={() => window.location.href} />
                {!banditResults && (
                  <CopyButton
                    label="Copy summary"
                    text={() => experimentSummary(selectedExp, selectedFlag, expResults, blindGuardrails, window.location.href)}
                  />
                )}
              </div>
            )}

            {expResults.hypothesis && (
              <p className="text-sm text-ink-2">
                <span className="font-medium text-ink">Hypothesis:</span> {expResults.hypothesis}
              </p>
            )}

            {selectedExp && !noExposures && !banditResults && (
              <Verdict exp={selectedExp} results={expResults} blindGuardrails={blindGuardrails} />
            )}

            {selectedExp && (
              <ExperimentSetup
                exp={selectedExp}
                flag={selectedFlag}
                cohortName={(id) => cohorts.find((c) => c.id === id)?.name ?? id}
                hasWebhook={hasWebhook}
                onNotify={
                  canEdit
                    ? (on) =>
                        void api
                          .patchExperiment(projectId, selectedExp.id, { notify: on })
                          .then((u) => setExperiments((prev) => prev.map((e) => (e.id === u.id ? u : e))))
                          .catch((err) => setActionError(`Couldn't change notifications: ${errorMessage(err)}`))
                    : undefined
                }
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
                          {formatMetricValue(expResults.metric_type, v, selectedExp?.primary_metric.unit)}
                        </td>
                        <td
                          className={`tabular py-3 text-right font-semibold ${
                            v.is_control ? "text-faint" : gain(selectedExp, v.relative_lift_pct) >= 0 ? "text-ok" : "text-danger"
                          }`}
                        >
                          {v.is_control ? "–" : formatLift(v.relative_lift_pct)}
                        </td>
                        <td className="tabular py-3 text-right text-ink-2">
                          {v.is_control || v.abs_diff === undefined ? "–" : formatAbsDiff(expResults.metric_type, v.abs_diff, selectedExp?.primary_metric.unit)}
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
                                    Math.min(gain(selectedExp, v.ci95_low_pct), gain(selectedExp, v.ci95_high_pct)) > 0 ? "bg-ok" : "bg-warn"
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
                          {v.is_control
                            ? "–"
                            : formatProbability(lowerIsBetter(selectedExp) ? 1 - v.bayesian_prob_to_beat_control : v.bayesian_prob_to_beat_control)}
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
                      // Segments read event properties off the exposure event, so say so: a property the
                      // app only sends on other events is empty here.
                      options={propertyOptions(availableProps.filter((p) => isSegmentable(p, piiProps))).map((o) =>
                        o.group === "Event properties" ? { ...o, group: "On the exposure event" } : o,
                      )}
                    />
                  </div>
                </div>
                {(() => {
                  // How many exposed users the chosen property leaves blank, and a better source if there is one.
                  if (!segment || expResults.segments.every((sg) => sg.segment === "(none)")) return null;
                  const users = (sg: (typeof expResults.segments)[number]) => sg.variants.reduce((a, v) => a + v.sample_size, 0);
                  const total = expResults.segments.reduce((a, sg) => a + users(sg), 0);
                  const none = expResults.segments.filter((sg) => sg.segment === "(none)").reduce((a, sg) => a + users(sg), 0);
                  if (!total || none / total < 0.5) return null;
                  const profile = !segment.startsWith("user.") && availableProps.includes(`user.${segment}`) ? `user.${segment}` : null;
                  return (
                    <p className="text-xs text-warn">
                      {formatPercent(none / total, 0)} of users have no {humanizeEvent(segment.replace(/^user\./, ""))}{" "}
                      {segment.startsWith("user.") ? "on their profile" : "on the exposure event"}, so most land in "No{" "}
                      {humanizeEvent(segment.replace(/^user\./, "")).toLowerCase()} set".
                      {profile && (
                        <>
                          {" "}
                          <button
                            type="button"
                            className="font-medium underline"
                            onClick={() => {
                              setSegment(profile);
                              void handleSelectExperiment(selectedExpId, profile);
                            }}
                          >
                            Use the profile's {humanizeEvent(segment)} instead
                          </button>
                        </>
                      )}
                    </p>
                  );
                })()}
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
                          {expResults.segments.flatMap((sg) => {
                            const tooFew = sg.variants.some((v) => v.sample_size < MIN_SEGMENT_USERS);
                            const label =
                              sg.segment === "$other"
                                ? "Other"
                                : !sg.segment || sg.segment === "(none)"
                                  ? `No ${humanizeEvent(segment.replace(/^user\./, "")).toLowerCase()} set`
                                  : sg.segment;
                            return sg.variants.map((v, i) => (
                              <tr key={`${sg.segment}:${v.variant_key}`}>
                                <td className="py-2 text-ink">{i === 0 ? label : ""}</td>
                                <td className="py-2 text-ink-2">{v.variant_key}</td>
                                <td className="tabular py-2 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                                <td className="tabular py-2 text-right text-ink">{formatMetricValue(expResults.metric_type, v, selectedExp?.primary_metric.unit)}</td>
                                <td className="tabular py-2 text-right text-ink-2">
                                  {v.is_control ? "–" : tooFew ? "Too few users" : formatLift(v.relative_lift_pct)}
                                </td>
                                <td className="tabular py-2 text-right text-muted">
                                  {v.is_control || tooFew
                                    ? "–"
                                    : `${formatLift(Math.max(-100, v.ci95_low_pct))} to ${formatLift(v.ci95_high_pct)}`}
                                </td>
                              </tr>
                            ));
                          })}
                        </tbody>
                      </table>
                    </div>
                    <p className="text-xs text-faint">
                      Exploratory: with many segments some will look different by chance, so confirm a segment finding with
                      its own experiment. Segments with fewer than {MIN_SEGMENT_USERS} users in a variant show no lift.
                    </p>
                  </>
                )}
              </div>
            )}

            {!noExposures && !banditResults && (expResults.secondary?.length ?? 0) > 0 && (
              <SecondaryResults metrics={expResults.secondary ?? []} exp={selectedExp} />
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
                      {noData && selectedExp && (
                        <WhyNoData projectId={projectId} event={g.event_name} exp={selectedExp} />
                      )}
                      {noData && selectedExp?.guardrail_metrics.find((m) => m.name === g.metric_name)?.auto_stop && (
                        <p className="mb-1.5 text-xs text-warn">
                          Auto-stop can't protect you while this has no data: it only acts on a measured harm.
                        </p>
                      )}
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
                                  : `${formatMetricValue(g.metric_type, gv, unitOf(selectedExp, g.metric_name))} per user (${formatCount(gv.sample_size)} users)`}
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
                              void patchFlag(f, { holdout_id: e.target.value }).then(loadAll);
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
            <div className="sm:order-last sm:col-span-3">
              <VariantsEditor variants={newVariants} onChange={setNewVariants} />
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
        experiments={experiments}
        onChanged={loadAll}
      />

      {review === "form" && (() => {
        const flag = flags.find((f) => f.id === expFlagId);
        return (
          <StartReview
            name={expName.trim()}
            flagKey={flag?.key ?? ""}
            flagWasOff={!flag?.enabled}
            trafficText={trafficSentence(flag, expLayerId ? expLayerPct : null)}
            endsText={`on ${new Date(Date.now() + expDays * DAY_MS).toLocaleDateString()}`}
            recap={[
              `Primary: ${isTwoStep(expMetricType) ? `${humanizeEvent(expDenominator)} → ` : ""}${humanizeEvent(expMetric)}${
                expWhere?.prop && expWhere.value.trim() ? ` where ${expWhere.prop} ${expWhere.op === "eq" ? "=" : expWhere.op === "neq" ? "is not" : "contains"} ${expWhere.value.trim()}` : ""
              } (${METRIC_TYPE_LABEL[expMetricType]}, within ${expWindowDays}d), ${expLowerIsBetter ? "lower" : "higher"} is better${
                expKind === "ab" ? `, worth shipping at ${mde}% or more` : ""
              }.`,
              ...(expKind === "ab"
                ? [
                    expGuardrails.length
                      ? `Guardrails: ${expGuardrails
                          .map(
                            (g) =>
                              `${humanizeEvent(g.event)}${g.where?.prop && g.where.value.trim() ? ` where ${g.where.prop} ${g.where.op === "eq" ? "=" : g.where.op === "neq" ? "is not" : "contains"} ${g.where.value.trim()}` : ""}${g.autoStop && expSequential ? " (auto-stops)" : ""}`,
                          )
                          .join(", ")}.`
                      : "No guardrails: nothing will flag harm.",
                    ...(expSecondaries.length ? [`Secondary: ${expSecondaries.map((g) => humanizeEvent(g.event)).join(", ")}.`] : []),
                  ]
                : []),
              `Variants: ${flag?.variants.map((v) => `${v.key} ${formatPercent(v.weight_bps / 10_000, 0)}`).join(" / ") || "on"}.`,
              `Audience: ${
                expAudience === KEEP
                  ? flag?.targeting_cohort_id ? `the flag's cohort (${cohorts.find((c) => c.id === flag.targeting_cohort_id)?.name ?? "cohort"})` : "everyone"
                  : expAudience === EVERYONE
                    ? "everyone"
                    : expAudience === NEW
                      ? "the new audience you built"
                      : cohorts.find((c) => c.id === expAudience)?.name ?? "a cohort"
              }${expEligible !== null ? `, about ${formatCount(expEligible)} users today` : ""}.`,
            ]}
            warning={(() => {
              if (!baseline || expMetricType !== "conversion" || expKind !== "ab") return null;
              const { need } = plan(baseline, mde / 100, flag, expSequential, expLayerId ? expLayerPct : null, expEligible);
              return Number.isFinite(need) && need > expDays
                ? `Underpowered: the plan needs about ${need} days and you picked ${expDays}. You may not be able to tell a ${mde}% change from noise.`
                : null;
            })()}
            busy={expCreating}
            onStart={() => void createExperiment("running")}
            onClose={() => setReview(null)}
          />
        );
      })()}
      {review === "draft" && selectedExp && (
        <StartReview
          name={selectedExp.name}
          flagKey={selectedExp.flag_key}
          flagWasOff={!selectedFlag?.enabled}
          trafficText={trafficSentence(selectedFlag, null)}
          endsText={`on ${new Date(Date.now() + Math.max(DAY_MS, selectedExp.end_ts_ms - selectedExp.start_ts_ms)).toLocaleDateString()}`}
          recap={[
            `Primary: ${selectedExp.primary_metric.name} (${describeMetric(selectedExp.primary_metric)}), ${lowerIsBetter(selectedExp) ? "lower" : "higher"} is better.`,
            selectedExp.guardrail_metrics.length
              ? `Guardrails: ${selectedExp.guardrail_metrics.map((g) => `${g.name}${g.auto_stop ? " (auto-stops)" : ""}`).join(", ")}.`
              : "No guardrails: nothing will flag harm.",
            `Variants: ${selectedFlag?.variants.map((v) => `${v.key} ${formatPercent(v.weight_bps / 10_000, 0)}`).join(" / ") || "on"}.`,
          ]}
          warning={null}
          busy={acting}
          onStart={() => void startDraft(selectedExp)}
          onClose={() => setReview(null)}
        />
      )}
      {concludeFor && selectedExp && (
        <ConcludeDialog
          experimentName={selectedExp.name}
          flagKey={selectedExp.flag_key}
          treatments={
            selectedExp.status === "draft"
              ? []
              : banditResults
                ? banditResults.arms.map((a) => a.variant_key)
                : (expResults?.variants ?? []).filter((v) => !v.is_control).map((v) => v.variant_key)
          }
          initialVariant={concludeFor.variant}
          blindGuardrails={blindGuardrails}
          harmedBy={harmedBy}
          sampleCaveat={sampleCaveat(selectedExp, expResults, banditResults)}
          busy={acting}
          onConclude={(d) => void conclude(selectedExp, d)}
          onClose={() => setConcludeFor(null)}
        />
      )}
    </div>
  );
};
