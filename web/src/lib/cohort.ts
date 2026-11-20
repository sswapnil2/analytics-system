/** Cohort builder lines <-> the saved cohort definition (BehavioralCohortDef in pulse-exec). */

/** One line of the cohort builder. `days` 0 means any time. */
export interface Cond {
  type: "did" | "didnt" | "prop";
  event: string;
  minCount: number;
  days: number;
  prop: string;
  value: string;
}
function condDef(c: Cond) {
  const now = Date.now();
  const time_range = c.days ? { start_ts_ms: now - c.days * 86_400_000, end_ts_ms: now } : null;
  if (c.type === "did") return { kind: "performed_event", event_name: c.event, min_count: c.minCount, time_range, where_clauses: [] };
  if (c.type === "didnt") return { kind: "not_performed_event", event_name: c.event, time_range };
  return { kind: "property_match", predicate: { prop_name: c.prop, filter: { op: "eq", val: c.value.trim() } }, time_range };
}

/** The saved definition: one condition as is, several joined with and/or. */
export function cohortDefinition(conds: Cond[], match: "and" | "or") {
  const ok = conds.filter((c) => (c.type === "prop" ? c.prop && c.value.trim() : c.event));
  if (ok.length === 0) return null;
  return ok.length === 1 ? condDef(ok[0]) : { kind: match, clauses: ok.map(condDef) };
}

type Def = { kind: string; [k: string]: unknown };

/** One saved condition back into a builder line, or null if the builder can't show it. */
function condFromDef(d: Def): Cond | null {
  const tr = d.time_range as { start_ts_ms: number; end_ts_ms: number } | null | undefined;
  // Saved windows are fixed dates; editing re-anchors them to today with the nearest choice.
  const span = tr ? (tr.end_ts_ms - tr.start_ts_ms) / 86_400_000 : 0;
  const days = tr ? [7, 30, 90].reduce((a, b) => (Math.abs(b - span) < Math.abs(a - span) ? b : a)) : 0;
  const base = { minCount: 1, days, event: "", prop: "", value: "" };
  if (d.kind === "performed_event" && !(d.where_clauses as unknown[] | undefined)?.length)
    return { ...base, type: "did", event: String(d.event_name), minCount: Number(d.min_count) || 1 };
  if (d.kind === "not_performed_event") return { ...base, type: "didnt", event: String(d.event_name) };
  const pred = d.predicate as { prop_name: string; filter: { op: string; val: unknown } } | undefined;
  if (d.kind === "property_match" && pred?.filter.op === "eq") return { ...base, type: "prop", prop: pred.prop_name, value: String(pred.filter.val) };
  return null;
}

/** A saved definition back into builder lines; null for ones it can't show (fixed user lists, cohorts of cohorts). */
export function condsFromDefinition(def: unknown): { conds: Cond[]; match: "and" | "or" } | null {
  const d = def as Def | null;
  if (!d || typeof d !== "object") return null;
  const parts = d.kind === "and" || d.kind === "or" ? (d.clauses as Def[]) : [d];
  const conds = parts.map(condFromDef);
  if (conds.length === 0 || conds.some((c) => !c)) return null;
  return { conds: conds as Cond[], match: d.kind === "or" ? "or" : "and" };
}

