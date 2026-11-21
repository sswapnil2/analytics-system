import React, { useId } from "react";
import { X } from "lucide-react";
import { useTopValues } from "./FilterPanel";
import { PropertyPicker, propertyOptions } from "./PropertyPicker";
import { field } from "./ui";

/** One "where" condition on a single funnel step (e.g. Product Viewed where category = shoes). */
export type StepWhere = { prop: string; op: "eq" | "neq" | "contains"; value: string };

export const stepWhereToFilters = (w: StepWhere | null | undefined) =>
  w && w.prop && w.value.trim() ? [{ prop_name: w.prop, filter: { op: w.op, val: w.value.trim() } }] : [];

export function stepWhereFromFilters(filters: unknown): StepWhere | null {
  const f = Array.isArray(filters) ? (filters[0] as { prop_name?: string; filter?: { op?: string; val?: unknown } }) : null;
  if (!f?.prop_name || !f.filter) return null;
  const op = f.filter.op === "neq" || f.filter.op === "contains" ? f.filter.op : "eq";
  return { prop: f.prop_name, op, value: String(f.filter.val ?? "") };
}

/** "+ where" on one funnel step: a single property condition, shown inline under the step. */
export const StepWhereEditor: React.FC<{
  idx: number;
  projectId: string;
  /** The step's event; value suggestions come from it. */
  event: string;
  value: StepWhere | null;
  props: string[];
  onChange: (w: StepWhere | null) => void;
  /** What the condition is on, for screen readers; defaults to "step N". */
  name?: string;
}> = ({ idx, projectId, event, value, props, onChange, name = `step ${idx + 1}` }) => {
  const listId = useId();
  const suggestions = useTopValues(projectId, value ? event : "", value?.prop ?? "");
  if (!value) {
    return (
      <button
        type="button"
        disabled={props.length === 0}
        onClick={() => onChange({ prop: "", op: "eq", value: "" })}
        className="self-start rounded px-1.5 py-0.5 text-xs text-muted hover:bg-hover hover:text-ink disabled:opacity-50"
        aria-label={`Add a condition to ${name}`}
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
        label={`${name[0].toUpperCase()}${name.slice(1)} condition property`}
        value={value.prop}
        onChange={(p) => onChange({ ...value, prop: p, value: "" })}
        recentKey={`pulse.recent-props.${projectId}`}
        options={propertyOptions(Array.from(new Set([value.prop, ...props])).filter(Boolean))}
      />
      <select
        aria-label={`${name[0].toUpperCase()}${name.slice(1)} condition operator`}
        value={value.op}
        onChange={(e) => onChange({ ...value, op: e.target.value as StepWhere["op"] })}
        className={`${field} w-auto py-1 text-xs`}
      >
        <option value="eq">is</option>
        <option value="neq">is not</option>
        <option value="contains">contains</option>
      </select>
      <input
        aria-label={`${name[0].toUpperCase()}${name.slice(1)} condition value`}
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
        aria-label={`Remove the condition on ${name}`}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
};
