import React, { useEffect, useId, useState } from "react";
import { Plus, X } from "lucide-react";
import { api } from "../api/client";
import { humanizeEvent } from "../lib/format";
import { Panel, field, fieldLabel } from "./ui";

/** Who a report counts: property conditions (all must match) and, optionally, a saved cohort. */
type Op = "eq" | "neq" | "contains" | "gt" | "lt" | "is_set";
export interface FilterRow {
  prop: string;
  op: Op;
  value: string;
}
export interface FilterDraft {
  rows: FilterRow[];
  cohortId: string;
}
export const EMPTY_FILTER: FilterDraft = { rows: [], cohortId: "" };

const OPS: { op: Op; label: string; word: string }[] = [
  { op: "eq", label: "is", word: "=" },
  { op: "neq", label: "is not", word: "≠" },
  { op: "contains", label: "contains", word: "contains" },
  { op: "gt", label: "greater than", word: ">" },
  { op: "lt", label: "less than", word: "<" },
  { op: "is_set", label: "is set", word: "is set" },
];

const complete = (r: FilterRow) => r.prop && (r.op === "is_set" || r.value.trim() !== "");

/** The `global_filter` every query endpoint accepts. Incomplete rows are left out. */
export function toGlobalFilter(d: FilterDraft) {
  const predicates = d.rows.filter(complete).map((r) => {
    const v = r.value.trim();
    const num = Number(v);
    const filter =
      r.op === "is_set"
        ? { op: "is_set" }
        : r.op === "gt" || r.op === "lt"
          ? { op: r.op, val: Number.isFinite(num) ? num : 0 }
          : { op: r.op, val: v };
    return { prop_name: r.prop, filter };
  });
  return { predicates, cohort: d.cohortId ? { kind: "cohort_ref", cohort_id: d.cohortId } : null };
}

/** Reads a saved report's `global_filter` back into the editor. */
export function fromGlobalFilter(gf: unknown): FilterDraft {
  const g = gf as { predicates?: { prop_name: string; filter: { op: Op; val?: unknown } }[]; cohort?: { cohort_id?: string } | null };
  return {
    rows: (g?.predicates ?? []).map((p) => ({ prop: p.prop_name, op: p.filter.op, value: p.filter.val == null ? "" : String(p.filter.val) })),
    cohortId: g?.cohort?.cohort_id ?? "",
  };
}

/** "platform = android, in Power buyers" for the collapsed query summary; empty when nothing filters. */
export function describeFilter(d: FilterDraft, cohortName?: string): string {
  const parts = d.rows
    .filter(complete)
    .map((r) => `${humanizeEvent(r.prop)} ${OPS.find((o) => o.op === r.op)?.word}${r.op === "is_set" ? "" : ` ${r.value.trim()}`}`);
  if (d.cohortId) parts.push(`in ${cohortName ?? "cohort"}`);
  return parts.join(", ");
}

/** Saved cohorts for the picker, loaded once per project. */
export function useCohortNames(projectId: string) {
  const [cohorts, setCohorts] = useState<{ id: string; name: string }[]>([]);
  useEffect(() => {
    let alive = true;
    api
      .getCohorts(projectId)
      .then((list) => alive && setCohorts(list.map((c) => ({ id: c.id, name: c.name }))))
      .catch(() => alive && setCohorts([]));
    return () => {
      alive = false;
    };
  }, [projectId]);
  return cohorts;
}

/** Most common values of `prop` on `anchorEvent` over the last 30 days, for suggestions. */
function useTopValues(projectId: string, anchorEvent: string, prop: string) {
  const [values, setValues] = useState<string[]>([]);
  useEffect(() => {
    if (!prop || !anchorEvent) return setValues([]);
    let alive = true;
    const end = Date.now();
    api
      .queryInsights(projectId, {
        time_range: { start_ts_ms: end - 30 * 86_400_000, end_ts_ms: end },
        interval: "day",
        series: [{ label: "A", event_name: anchorEvent, metric: { type: "totals" }, filters: [] }],
        breakdown_by: [prop],
      })
      .then((res) => {
        if (!alive) return;
        const rows = res.series
          .filter((s) => s.breakdown_key && s.breakdown_key !== "(none)")
          .sort((a, b) => (b.overall_value ?? 0) - (a.overall_value ?? 0));
        setValues(rows.slice(0, 50).map((s) => s.breakdown_key));
      })
      .catch(() => alive && setValues([]));
    return () => {
      alive = false;
    };
  }, [projectId, anchorEvent, prop]);
  return values;
}

const Row: React.FC<{
  projectId: string;
  anchorEvent: string;
  row: FilterRow;
  props: string[];
  onChange: (r: FilterRow) => void;
  onRemove: () => void;
}> = ({ projectId, anchorEvent, row, props, onChange, onRemove }) => {
  const listId = useId();
  const values = useTopValues(projectId, anchorEvent, row.prop);
  return (
    <li className="space-y-1.5 rounded-lg border border-line bg-sunken p-2">
      <div className="flex items-center gap-1.5">
        <select
          aria-label="Property"
          value={row.prop}
          onChange={(e) => onChange({ ...row, prop: e.target.value, value: "" })}
          className={`${field} min-w-0 flex-1 py-1`}
        >
          {props.map((p) => (
            <option key={p} value={p}>
              {humanizeEvent(p)}
            </option>
          ))}
        </select>
        <button
          type="button"
          onClick={onRemove}
          aria-label={`Remove ${humanizeEvent(row.prop)} filter`}
          className="rounded p-1 text-muted hover:bg-hover hover:text-ink"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      <div className="flex items-center gap-1.5">
        <select
          aria-label="Condition"
          value={row.op}
          onChange={(e) => onChange({ ...row, op: e.target.value as Op })}
          className={`${field} w-[7.5rem] shrink-0 py-1`}
        >
          {OPS.map((o) => (
            <option key={o.op} value={o.op}>
              {o.label}
            </option>
          ))}
        </select>
        {row.op !== "is_set" && (
          <>
            <input
              aria-label="Value"
              value={row.value}
              onChange={(e) => onChange({ ...row, value: e.target.value })}
              list={listId}
              inputMode={row.op === "gt" || row.op === "lt" ? "decimal" : undefined}
              placeholder={values[0] ?? "value"}
              className={`${field} min-w-0 flex-1 py-1`}
            />
            <datalist id={listId}>
              {values.map((v) => (
                <option key={v} value={v} />
              ))}
            </datalist>
          </>
        )}
      </div>
    </li>
  );
};

/**
 * "Filter" panel for a report's query rail. `anchorEvent` is the event used to
 * suggest property values (a report's first event).
 */
export const FilterPanel: React.FC<{
  projectId: string;
  availableProps: string[];
  anchorEvent: string;
  value: FilterDraft;
  onChange: (d: FilterDraft) => void;
}> = ({ projectId, availableProps, anchorEvent, value, onChange }) => {
  const cohorts = useCohortNames(projectId);
  const props = Array.from(new Set([...availableProps, ...value.rows.map((r) => r.prop)])).filter(Boolean);
  const setRow = (i: number, r: FilterRow) => onChange({ ...value, rows: value.rows.map((x, j) => (j === i ? r : x)) });
  const active = value.rows.filter(complete).length + (value.cohortId ? 1 : 0);
  return (
    <Panel title="Filter" meta={active > 0 ? `${active} active` : "everyone"}>
      <div className="space-y-3">
        {value.rows.length > 0 && (
          <ul className="space-y-2">
            {value.rows.map((r, i) => (
              <Row
                key={i}
                projectId={projectId}
                anchorEvent={anchorEvent}
                row={r}
                props={props}
                onChange={(nr) => setRow(i, nr)}
                onRemove={() => onChange({ ...value, rows: value.rows.filter((_, j) => j !== i) })}
              />
            ))}
          </ul>
        )}
        {props.length > 0 && (
          <button
            type="button"
            onClick={() => onChange({ ...value, rows: [...value.rows, { prop: props[0], op: "eq", value: "" }] })}
            className="inline-flex items-center gap-1 text-sm font-medium text-brand hover:underline"
          >
            <Plus className="h-3.5 w-3.5" />
            {value.rows.length === 0 ? "Only count events where…" : "And where…"}
          </button>
        )}
        <label className="block space-y-1">
          <span className={fieldLabel}>Users in cohort</span>
          <select
            value={value.cohortId}
            onChange={(e) => onChange({ ...value, cohortId: e.target.value })}
            className={`${field} w-full py-1`}
          >
            <option value="">All users</option>
            {cohorts.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
        {cohorts.length === 0 && (
          <p className="text-xs text-muted">
            Save a cohort on the <a href="#/cohorts" className="text-brand hover:underline">Cohorts</a> page to narrow a report to it.
          </p>
        )}
      </div>
    </Panel>
  );
};
