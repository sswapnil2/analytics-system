import React, { useState } from "react";
import { Bookmark, Plus, X } from "lucide-react";
import { api, ExperimentMetric, MetricDirection, MetricType, SavedMetric } from "../api/client";
import { errorMessage, humanizeEvent } from "../lib/format";
import { PickerOption, PropertyPicker } from "./PropertyPicker";
import { StepWhere, StepWhereEditor, stepWhereFromFilters } from "./StepWhere";
import { btn, field, fieldLabel } from "./ui";

/** One guardrail or secondary metric in the experiment form. */
export interface MetricRow {
  event: string;
  type: Extract<MetricType, "conversion" | "count" | "step_through" | "time_between">;
  /** First step, for step-through and time-between metrics. */
  from: string;
  direction: MetricDirection;
  autoStop: boolean;
  /** Only events matching this condition count (e.g. status = failed). */
  where: StepWhere | null;
  /** Set when the row came from, or was saved as, a saved metric. */
  name?: string;
}

/** What a row measures, in words: "OTP Verified → KYC PAN Submitted (time)". */
export function metricRowName(r: MetricRow): string {
  if (r.name) return r.name;
  if (isTwoStep(r.type)) return `${humanizeEvent(r.from)} → ${humanizeEvent(r.event)}${r.type === "time_between" ? " (time)" : ""}`;
  const w = r.where?.prop && r.where.value.trim() ? r.where : null;
  return `${humanizeEvent(r.event)}${w ? ` where ${w.prop} ${w.op === "neq" ? "is not" : w.op === "contains" ? "contains" : "="} ${w.value.trim()}` : ""}`;
}

const ROW_TYPES = ["conversion", "count", "step_through", "time_between"];

function rowFromSaved(m: SavedMetric, guardrail: boolean): MetricRow {
  const s = m.spec;
  const type = s.metric_type as MetricRow["type"];
  return {
    event: s.event_name,
    type,
    from: isTwoStep(type) ? s.denominator?.event_name ?? "" : "",
    direction: s.direction ?? (guardrail ? "increase_is_bad" : "decrease_is_bad"),
    autoStop: guardrail,
    where: stepWhereFromFilters(s.filters),
    name: m.name,
  };
}

export const newMetricRow = (guardrail: boolean): MetricRow => ({
  event: "",
  type: "conversion",
  from: "",
  direction: guardrail ? "increase_is_bad" : "decrease_is_bad",
  autoStop: guardrail,
  where: null,
});

export const isTwoStep = (t: MetricType | "continuous" | undefined) => t === "step_through" || t === "time_between";

const TYPES: { value: MetricRow["type"]; label: string }[] = [
  { value: "conversion", label: "Did it (rate)" },
  { value: "count", label: "Times per user" },
  { value: "step_through", label: "Went from A to B (rate)" },
  { value: "time_between", label: "Time from A to B" },
];

/** A row's first problem, or null. */
export function metricRowProblem(r: MetricRow): string | null {
  if (!r.event) return "Pick an event for every guardrail and secondary metric, or remove the empty row.";
  if (isTwoStep(r.type) && !r.from) return "Pick the first step (A) for every A-to-B metric.";
  return null;
}

/** Editable list of guardrail or secondary metrics. */
export const MetricRows: React.FC<{
  title: string;
  hint: string;
  rows: MetricRow[];
  onChange: (rows: MetricRow[]) => void;
  guardrail: boolean;
  sequential: boolean;
  events: PickerOption[];
  recentKey: string;
  projectId: string;
  /** Event properties offered for a "where" condition. */
  props: string[];
  /** The project's saved metrics, offered for reuse. */
  saved: SavedMetric[];
  /** The spec a row would be saved as. */
  toSpec: (r: MetricRow) => ExperimentMetric;
  onSaved: () => void;
}> = ({ title, hint, rows, onChange, guardrail, sequential, events, recentKey, projectId, props, saved, toSpec, onSaved }) => {
  // Editing a row's definition detaches it from the saved metric it came from.
  const set = (i: number, patch: Partial<MetricRow>) =>
    onChange(rows.map((r, j) => (j === i ? { ...r, ...patch, name: "event" in patch || "type" in patch || "from" in patch || "where" in patch ? undefined : r.name } : r)));
  const [naming, setNaming] = useState<{ i: number; name: string; error: string | null; busy: boolean } | null>(null);
  const usable = saved.filter((m) => ROW_TYPES.includes(m.spec.metric_type ?? "conversion"));
  const saveRow = async () => {
    if (!naming) return;
    const name = naming.name.trim();
    if (!name) return setNaming({ ...naming, error: "Give it a name." });
    setNaming({ ...naming, busy: true, error: null });
    try {
      await api.createSavedMetric(projectId, { ...toSpec(rows[naming.i]), name, auto_stop: false });
      onChange(rows.map((r, j) => (j === naming.i ? { ...r, name } : r)));
      setNaming(null);
      onSaved();
    } catch (e) {
      setNaming({ ...naming, busy: false, error: errorMessage(e) });
    }
  };
  return (
    <fieldset className="space-y-2 sm:col-span-2">
      <legend className={fieldLabel}>{title}</legend>
      <p className="text-xs text-muted">{hint}</p>
      {rows.map((r, i) => (
        <div key={i} className="grid grid-cols-1 gap-2 rounded-lg border border-line p-2.5 sm:grid-cols-2">
          {r.name && (
            <p className="flex items-center gap-1.5 text-xs text-muted sm:col-span-2">
              <Bookmark className="h-3.5 w-3.5" aria-hidden="true" /> Saved metric: <span className="font-medium text-ink">{r.name}</span>
            </p>
          )}
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Measure</span>
            <select
              value={r.type}
              onChange={(e) => {
                const type = e.target.value as MetricRow["type"];
                // Time is better short, so a guardrail on it is bad when it goes up.
                set(i, type === "time_between" ? { type, direction: "increase_is_bad" } : { type });
              }}
              className={field}
            >
              {TYPES.map((t) => (
                <option key={t.value} value={t.value}>
                  {t.label}
                </option>
              ))}
            </select>
          </label>
          {isTwoStep(r.type) && (
            <PropertyPicker
              label="First step (A)"
              value={r.from}
              onChange={(v) => set(i, { from: v })}
              placeholder="Search events"
              recentKey={recentKey}
              options={events}
            />
          )}
          <PropertyPicker
            label={isTwoStep(r.type) ? "Then (B)" : "Event"}
            value={r.event}
            onChange={(v) => set(i, { event: v })}
            placeholder="Search events"
            recentKey={recentKey}
            options={events}
          />
          {r.event && (
            <div className="sm:col-span-2">
              <StepWhereEditor
                idx={i}
                name={`${title.toLowerCase()} ${i + 1}`}
                projectId={projectId}
                event={r.event}
                value={r.where}
                props={props}
                onChange={(w) => set(i, { where: w })}
              />
            </div>
          )}
          {guardrail && (
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>Bad when it</span>
              <select
                value={r.direction}
                onChange={(e) => set(i, { direction: e.target.value as MetricDirection })}
                className={field}
              >
                <option value="increase_is_bad">Goes up (errors, failures, time)</option>
                <option value="decrease_is_bad">Goes down (purchases, retention)</option>
              </select>
            </label>
          )}
          <div className="flex flex-wrap items-center justify-between gap-2 sm:col-span-2">
            {guardrail ? (
              <label className="flex items-center gap-2 text-sm text-ink">
                <input
                  type="checkbox"
                  checked={r.autoStop && sequential}
                  disabled={!sequential}
                  onChange={(e) => set(i, { autoStop: e.target.checked })}
                />
                {sequential ? "Stop automatically if this gets worse" : "Auto-stop needs sequential testing"}
              </label>
            ) : (
              <span />
            )}
            <span className="flex flex-wrap items-center gap-2">
              {r.event && !r.name && naming?.i !== i && (
                <button type="button" onClick={() => setNaming({ i, name: metricRowName(r), error: null, busy: false })} className={btn.ghost}>
                  <Bookmark className="h-3.5 w-3.5" />
                  Save as a metric
                </button>
              )}
            <button
              type="button"
              onClick={() => onChange(rows.filter((_, j) => j !== i))}
              className={btn.ghost}
              aria-label={`Remove ${title.toLowerCase()} ${i + 1}`}
            >
              <X className="h-3.5 w-3.5" />
              Remove
            </button>
            </span>
          </div>
          {naming?.i === i && (
            <div className="flex flex-wrap items-end gap-2 sm:col-span-2">
              <label className="flex min-w-0 flex-1 flex-col gap-1">
                <span className={fieldLabel}>Name it so your team can reuse it</span>
                <input
                  value={naming.name}
                  maxLength={80}
                  onChange={(e) => setNaming({ ...naming, name: e.target.value, error: null })}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      void saveRow();
                    }
                  }}
                  placeholder="e.g. KYC failure"
                  className={field}
                />
              </label>
              <button type="button" disabled={naming.busy} onClick={() => void saveRow()} className={btn.secondary}>
                {naming.busy ? "Saving…" : "Save"}
              </button>
              <button type="button" onClick={() => setNaming(null)} className={btn.ghost}>
                Cancel
              </button>
              {naming.error && <p role="alert" className="w-full text-xs text-danger">{naming.error}</p>}
            </div>
          )}
        </div>
      ))}
      <div className="flex flex-wrap items-center gap-2">
        <button type="button" onClick={() => onChange([...rows, newMetricRow(guardrail)])} className={btn.secondary}>
          <Plus className="h-3.5 w-3.5" />
          Add {guardrail ? "guardrail" : "secondary metric"}
        </button>
        {usable.length > 0 && (
          <select
            aria-label={`Add a saved metric as a ${guardrail ? "guardrail" : "secondary metric"}`}
            value=""
            onChange={(e) => {
              const m = usable.find((x) => x.id === e.target.value);
              if (m) onChange([...rows, rowFromSaved(m, guardrail)]);
            }}
            className={`${field} w-auto py-1`}
          >
            <option value="">Add a saved metric…</option>
            {usable.map((m) => (
              <option key={m.id} value={m.id}>
                {m.name}
              </option>
            ))}
          </select>
        )}
      </div>
    </fieldset>
  );
};
