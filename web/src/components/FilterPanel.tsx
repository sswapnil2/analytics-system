import React, { useEffect, useId, useState } from "react";
import { Plus, X } from "lucide-react";
import { api } from "../api/client";
import { humanizeEvent } from "../lib/format";
import { Panel, field, fieldLabel } from "./ui";

/** Who a report counts: property conditions (all must match) and, optionally, a saved cohort. */
type Op = "eq" | "neq" | "contains" | "gt" | "lt" | "is_set";
export interface FilterRow {
  id?: string;
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
  const g = gf as {
    predicates?: Array<{ prop_name?: unknown; filter?: { op?: unknown; val?: unknown } | null }>;
    cohort?: { cohort_id?: unknown } | null;
  };
  const validOps = new Set<Op>(["eq", "neq", "contains", "gt", "lt", "is_set"]);
  const rows: FilterRow[] = [];
  for (const p of g?.predicates ?? []) {
    if (!p || typeof p.prop_name !== "string" || !p.filter || typeof p.filter.op !== "string") continue;
    if (!validOps.has(p.filter.op as Op)) continue;
    rows.push({
      id: nextRowId(),
      prop: p.prop_name,
      op: p.filter.op as Op,
      value: p.filter.val == null ? "" : String(p.filter.val),
    });
  }
  return {
    rows,
    cohortId: typeof g?.cohort?.cohort_id === "string" ? g.cohort.cohort_id : "",
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

let filterRowSeq = 0;
const nextRowId = () => `fr_${++filterRowSeq}`;

const CACHE_TTL_MS = 60_000;
const cohortCache = new Map<string, { ts: number; data: { id: string; name: string }[] }>();
const topValuesCache = new Map<string, { ts: number; promise: Promise<string[]> }>();

/** Clears cached filter metadata for one project or all projects (call on sign-out, account switch, or cohort creation). */
export function clearFilterCaches(projectId?: string): void {
  if (!projectId) {
    cohortCache.clear();
    topValuesCache.clear();
    return;
  }
  cohortCache.delete(projectId);
  const prefix = `${projectId}\u0000`;
  for (const key of topValuesCache.keys()) {
    if (key.startsWith(prefix)) topValuesCache.delete(key);
  }
}

/** Saved cohorts for the picker, cached briefly per project. */
export function useCohortNames(projectId: string) {
  const [cohorts, setCohorts] = useState<{ id: string; name: string }[]>(() => {
    const hit = cohortCache.get(projectId);
    return hit && Date.now() - hit.ts < CACHE_TTL_MS ? hit.data : [];
  });
  useEffect(() => {
    const hit = cohortCache.get(projectId);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) {
      setCohorts(hit.data);
      return;
    }
    let alive = true;
    api
      .getCohorts(projectId)
      .then((list) => {
        const mapped = list.map((c) => ({ id: c.id, name: c.name }));
        cohortCache.set(projectId, { ts: Date.now(), data: mapped });
        if (alive) setCohorts(mapped);
      })
      .catch(() => alive && setCohorts([]));
    return () => {
      alive = false;
    };
  }, [projectId]);
  return cohorts;
}

/** Most common values of `prop` on `anchorEvent` over the last 30 days, busiest first (cached 60s). */
export async function topValues(projectId: string, anchorEvent: string, prop: string, limit = 50): Promise<string[]> {
  const key = `${projectId}\u0000${anchorEvent}\u0000${prop}\u0000${limit}`;
  const hit = topValuesCache.get(key);
  if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.promise;
  const end = Date.now();
  const promise = api
    .queryInsights(projectId, {
      time_range: { start_ts_ms: end - 30 * 86_400_000, end_ts_ms: end },
      interval: "day",
      series: [{ label: "A", event_name: anchorEvent, metric: { type: "totals" }, filters: [] }],
      breakdown_by: [prop],
    })
    .then((res) =>
      res.series
        .filter((s) => s.breakdown_key && s.breakdown_key !== "(none)")
        .sort((a, b) => (b.overall_value ?? 0) - (a.overall_value ?? 0))
        .slice(0, limit)
        .map((s) => s.breakdown_key),
    );
  topValuesCache.set(key, { ts: Date.now(), promise });
  promise.catch(() => topValuesCache.delete(key));
  return promise;
}

function useTopValues(projectId: string, anchorEvent: string, prop: string) {
  const [values, setValues] = useState<string[]>([]);
  useEffect(() => {
    if (!prop || !anchorEvent) return setValues([]);
    let alive = true;
    topValues(projectId, anchorEvent, prop)
      .then((v) => alive && setValues(v))
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
                key={r.id ?? `${r.prop}-${i}`}
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
            onClick={() => onChange({ ...value, rows: [...value.rows, { id: nextRowId(), prop: props[0], op: "eq", value: "" }] })}
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
