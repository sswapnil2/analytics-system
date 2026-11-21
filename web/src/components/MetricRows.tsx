import React from "react";
import { Plus, X } from "lucide-react";
import { MetricDirection, MetricType } from "../api/client";
import { PickerOption, PropertyPicker } from "./PropertyPicker";
import { btn, field, fieldLabel } from "./ui";

/** One guardrail or secondary metric in the experiment form. */
export interface MetricRow {
  event: string;
  type: Extract<MetricType, "conversion" | "count" | "step_through" | "time_between">;
  /** First step, for step-through and time-between metrics. */
  from: string;
  direction: MetricDirection;
  autoStop: boolean;
}

export const newMetricRow = (guardrail: boolean): MetricRow => ({
  event: "",
  type: "conversion",
  from: "",
  direction: guardrail ? "increase_is_bad" : "decrease_is_bad",
  autoStop: guardrail,
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
}> = ({ title, hint, rows, onChange, guardrail, sequential, events, recentKey }) => {
  const set = (i: number, patch: Partial<MetricRow>) => onChange(rows.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  return (
    <fieldset className="space-y-2 sm:col-span-2">
      <legend className={fieldLabel}>{title}</legend>
      <p className="text-xs text-muted">{hint}</p>
      {rows.map((r, i) => (
        <div key={i} className="grid grid-cols-1 gap-2 rounded-lg border border-line p-2.5 sm:grid-cols-2">
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
            <button
              type="button"
              onClick={() => onChange(rows.filter((_, j) => j !== i))}
              className={btn.ghost}
              aria-label={`Remove ${title.toLowerCase()} ${i + 1}`}
            >
              <X className="h-3.5 w-3.5" />
              Remove
            </button>
          </div>
        </div>
      ))}
      <button type="button" onClick={() => onChange([...rows, newMetricRow(guardrail)])} className={btn.secondary}>
        <Plus className="h-3.5 w-3.5" />
        Add {guardrail ? "guardrail" : "secondary metric"}
      </button>
    </fieldset>
  );
};
