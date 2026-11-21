import React, { useEffect, useState } from "react";
import { Plus, X } from "lucide-react";
import { api, FeatureFlag, isAbortError } from "../api/client";
import { Cond, cohortDefinition } from "../lib/cohort";
import { formatCount } from "../lib/format";
import { PickerOption, PropertyPicker, propertyOptions } from "./PropertyPicker";
import { btn, field, fieldLabel } from "./ui";

/** Who the experiment's flag serves: "" keeps the flag's audience, "__everyone", "__new" (built here) or a cohort id. */
export const KEEP = "";
export const EVERYONE = "__everyone";
export const NEW = "__new";

const newCond = (): Cond => ({ type: "didnt", event: "", minCount: 1, days: 0, prop: "", value: "" });

/** The cohort definition a choice stands for; null means everyone. */
export function audienceDefinition(
  value: string,
  conds: Cond[],
  flag: FeatureFlag | undefined,
  cohorts: { id: string; definition: unknown }[],
): unknown | null {
  if (value === NEW) return cohortDefinition(conds, "and");
  if (value === EVERYONE) return null;
  const id = value || flag?.targeting_cohort_id;
  return id ? cohorts.find((c) => c.id === id)?.definition ?? null : null;
}

/** Pick or build who an experiment includes, with a live count of matching users. */
export const AudiencePicker: React.FC<{
  projectId: string;
  flag: FeatureFlag | undefined;
  value: string;
  conds: Cond[];
  onChange: (value: string, conds: Cond[]) => void;
  /** Matching users right now; null for everyone or while unknown. */
  onEligible: (users: number | null) => void;
  events: PickerOption[];
  props: string[];
}> = ({ projectId, flag, value, conds, onChange, onEligible, events, props }) => {
  const [cohorts, setCohorts] = useState<{ id: string; name: string; definition: unknown }[]>([]);
  const [count, setCount] = useState<number | null>(null);
  useEffect(() => {
    api.getCohorts(projectId).then(setCohorts).catch(() => setCohorts([]));
  }, [projectId]);

  const def = audienceDefinition(value, conds, flag, cohorts);
  const defKey = JSON.stringify(def);
  useEffect(() => {
    setCount(null);
    onEligible(null);
    if (!def) return;
    const ctl = new AbortController();
    const t = window.setTimeout(() => {
      api
        .previewCohort(projectId, def, ctl.signal)
        .then((r) => {
          setCount(r.user_count);
          onEligible(r.user_count);
        })
        .catch((e) => {
          if (!isAbortError(e)) setCount(null);
        });
    }, 400);
    return () => {
      ctl.abort();
      window.clearTimeout(t);
    };
    // The definition is compared by value (defKey), so a new object with the same content doesn't refetch.
  }, [projectId, defKey]);

  const flagCohort = flag?.targeting_cohort_id ? cohorts.find((c) => c.id === flag.targeting_cohort_id)?.name ?? "a cohort" : null;
  const set = (i: number, patch: Partial<Cond>) => onChange(value, conds.map((c, j) => (j === i ? { ...c, ...patch } : c)));

  return (
    <fieldset className="space-y-2 sm:col-span-2">
      <legend className={fieldLabel}>Who's in it</legend>
      <select
        aria-label="Audience"
        value={value}
        onChange={(e) => onChange(e.target.value, e.target.value === NEW && conds.length === 0 ? [newCond()] : conds)}
        className={field}
      >
        <option value={KEEP}>{flag ? `As the flag targets now: ${flagCohort ?? "everyone"}` : "As the flag targets"}</option>
        <option value={EVERYONE}>Everyone</option>
        {cohorts.map((c) => (
          <option key={c.id} value={c.id}>
            Cohort: {c.name}
          </option>
        ))}
        <option value={NEW}>New audience…</option>
      </select>
      {value === NEW && (
        <div className="space-y-2 rounded-lg border border-line p-2.5">
          <p className="text-xs text-muted">Users who match every line. It's saved as a cohort and set on the flag.</p>
          {conds.map((c, i) => (
            <div key={i} className="grid grid-cols-1 gap-2 sm:grid-cols-[10rem_minmax(0,1fr)_minmax(0,9rem)_auto] sm:items-end">
              <select
                aria-label={`Audience line ${i + 1} kind`}
                value={c.type}
                onChange={(e) => set(i, { type: e.target.value as Cond["type"] })}
                className={field}
              >
                <option value="didnt">Hasn't done</option>
                <option value="did">Has done</option>
                <option value="prop">Property is</option>
              </select>
              {c.type === "prop" ? (
                <div className="grid grid-cols-2 gap-2">
                  <PropertyPicker
                    label={`Audience line ${i + 1} property`}
                    value={c.prop}
                    onChange={(p) => set(i, { prop: p })}
                    recentKey={`pulse.recent-props.${projectId}`}
                    options={propertyOptions(props)}
                  />
                  <input
                    aria-label={`Audience line ${i + 1} value`}
                    value={c.value}
                    onChange={(e) => set(i, { value: e.target.value })}
                    placeholder="value"
                    className={`${field} self-end`}
                  />
                </div>
              ) : (
                <PropertyPicker
                  label={`Audience line ${i + 1} event`}
                  value={c.event}
                  onChange={(v) => set(i, { event: v })}
                  placeholder="Search events"
                  recentKey={`pulse.recent-events.${projectId}`}
                  options={events}
                />
              )}
              {c.type !== "prop" ? (
                <select
                  aria-label={`Audience line ${i + 1} time`}
                  value={c.days}
                  onChange={(e) => set(i, { days: Number(e.target.value) })}
                  className={field}
                >
                  <option value={0}>ever</option>
                  <option value={7}>in the last 7 days</option>
                  <option value={30}>in the last 30 days</option>
                  <option value={90}>in the last 90 days</option>
                </select>
              ) : (
                <span />
              )}
              <button
                type="button"
                onClick={() => onChange(value, conds.filter((_, j) => j !== i))}
                className={btn.ghost}
                aria-label={`Remove audience line ${i + 1}`}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          ))}
          <button type="button" onClick={() => onChange(value, [...conds, newCond()])} className={btn.ghost}>
            <Plus className="h-3.5 w-3.5" />
            Add a line
          </button>
        </div>
      )}
      {def !== null && (
        <p className="text-xs text-muted" aria-live="polite">
          {count === null ? "Counting matching users…" : `About ${formatCount(count)} users match today.`} Users join or
          leave as they start or stop matching; results count each from their first exposure.
        </p>
      )}
    </fieldset>
  );
};
