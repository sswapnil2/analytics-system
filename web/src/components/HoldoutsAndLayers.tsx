import React, { useState } from "react";
import { Plus, Trash2 } from "lucide-react";
import { api, ExperimentResults, TrafficGroup } from "../api/client";
import { ErrorNotice, Panel, btn, field, fieldLabel } from "./ui";
import { errorMessage, formatCount, formatPercent, formatPValue, humanizeEvent } from "../lib/format";

type Kind = "holdouts" | "layers";

const pct = (bps: number) => formatPercent(bps / 10_000, bps % 100 === 0 ? 0 : 1);
const lift = (p: number) => `${p > 0 ? "+" : ""}${formatPercent(p / 100)}`;

/** Exposed users against held-back users, on a conversion metric the viewer picks. */
const HoldoutImpact: React.FC<{ projectId: string; holdout: TrafficGroup; events: string[] }> = ({
  projectId,
  holdout,
  events,
}) => {
  const [event, setEvent] = useState("");
  const [res, setRes] = useState<ExperimentResults | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const measure = async (ev: string) => {
    setEvent(ev);
    setRes(null);
    setError(null);
    if (!ev) return;
    setBusy(true);
    try {
      const metric = { name: humanizeEvent(ev), event_name: ev, conversion_window_ms: 7 * 86_400_000 };
      setRes(await api.getHoldoutResults(projectId, holdout.id, metric, holdout.created_at_ms));
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  const users = res?.variants.reduce((a, v) => a + v.sample_size, 0) ?? 0;
  return (
    <div className="space-y-2">
      <label className="flex flex-wrap items-center gap-2 text-sm text-ink-2">
        Impact on
        <select value={event} onChange={(e) => void measure(e.target.value)} className={`${field} py-1`}>
          <option value="">Pick a conversion event</option>
          {events.map((e) => (
            <option key={e} value={e}>
              {humanizeEvent(e)}
            </option>
          ))}
        </select>
      </label>
      {busy && <p className="text-xs text-muted">Measuring…</p>}
      {error && <ErrorNotice message={error} />}
      {res && users === 0 && (
        <p className="text-xs text-muted">
          No one has reached a flag in this holdout yet. Users count once your app asks for a variant with exposure
          tracking on; SDKs then send a <code className="font-mono">$holdout_assigned</code> event.
        </p>
      )}
      {res && users > 0 && (
        <>
          {res.sample_ratio_mismatch && (
            <p role="alert" className="text-xs text-danger">
              The holdout's share is off (p {formatPValue(res.srm_p_value)}), so this can't be trusted. Check that every
              app sending these events uses an SDK that supports holdouts.
            </p>
          )}
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-line text-xs text-muted">
                <th className="py-1.5 text-left font-medium">Group</th>
                <th className="py-1.5 text-right font-medium">Users</th>
                <th className="py-1.5 text-right font-medium">Converted</th>
                <th className="py-1.5 text-right font-medium">Lift</th>
                <th className="py-1.5 text-right font-medium">Range</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-line">
              {res.variants.map((v) => (
                <tr key={v.variant_key}>
                  <td className="py-1.5 text-ink">{v.is_control ? "Held back" : "Got the flags"}</td>
                  <td className="tabular py-1.5 text-right text-ink-2">{formatCount(v.sample_size)}</td>
                  <td className="tabular py-1.5 text-right text-ink">{formatPercent(v.conversion_rate, 2)}</td>
                  <td
                    className={`tabular py-1.5 text-right font-medium ${
                      v.is_control ? "text-faint" : v.statistically_significant ? (v.relative_lift_pct > 0 ? "text-ok" : "text-danger") : "text-ink-2"
                    }`}
                  >
                    {v.is_control ? "–" : lift(v.relative_lift_pct)}
                  </td>
                  <td className="tabular py-1.5 text-right text-muted">
                    {v.is_control ? "–" : `${lift(v.ci95_low_pct)} to ${lift(v.ci95_high_pct)}`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="text-xs text-faint">
            The combined effect of every flag in this holdout since it was created. Sequential, so safe to check any
            time.
          </p>
        </>
      )}
    </div>
  );
};

/** Holdouts (a share of users kept off a set of flags) and layers (experiments that never share a user). */
export const HoldoutsAndLayers: React.FC<{
  projectId: string;
  holdouts: TrafficGroup[];
  layers: TrafficGroup[];
  events: string[];
  canEdit: boolean;
  onChanged: () => Promise<void>;
}> = ({ projectId, holdouts, layers, events, canEdit, onChanged }) => {
  const [name, setName] = useState<Record<Kind, string>>({ holdouts: "", layers: "" });
  const [holdoutPct, setHoldoutPct] = useState(5);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
      await onChanged();
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  const create = (kind: Kind) =>
    run(async () => {
      if (!name[kind].trim()) throw new Error("Add a name.");
      await api.createTrafficGroup(projectId, kind, {
        name: name[kind].trim(),
        ...(kind === "holdouts" ? { holdout_pct: holdoutPct } : {}),
      });
      setName((n) => ({ ...n, [kind]: "" }));
    });
  const remove = (kind: Kind, g: TrafficGroup) => {
    if (!window.confirm(`Delete "${g.name}"? This can't be undone.`)) return;
    void run(() => api.deleteTrafficGroup(projectId, kind, g.id));
  };

  const createForm = (kind: Kind, placeholder: string) =>
    canEdit && (
      <form
        noValidate
        onSubmit={(e) => {
          e.preventDefault();
          void create(kind);
        }}
        className="flex flex-wrap items-end gap-2"
      >
        <label className="flex min-w-0 flex-1 flex-col gap-1">
          <span className={fieldLabel}>Name</span>
          <input
            value={name[kind]}
            onChange={(e) => setName((n) => ({ ...n, [kind]: e.target.value }))}
            placeholder={placeholder}
            className={field}
          />
        </label>
        {kind === "holdouts" && (
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Held back</span>
            <select value={holdoutPct} onChange={(e) => setHoldoutPct(Number(e.target.value))} className={field}>
              {[1, 2, 5, 10, 20].map((p) => (
                <option key={p} value={p}>
                  {p}% of users
                </option>
              ))}
            </select>
          </label>
        )}
        <button type="submit" disabled={busy} className={btn.secondary}>
          <Plus className="h-3.5 w-3.5" />
          {kind === "holdouts" ? "Create holdout" : "Create layer"}
        </button>
      </form>
    );

  const deleteButton = (kind: Kind, g: TrafficGroup) =>
    canEdit && (
      <button
        type="button"
        disabled={busy || g.flags.length > 0}
        title={g.flags.length > 0 ? "Detach its flags first" : `Delete ${g.name}`}
        aria-label={`Delete ${g.name}`}
        onClick={() => remove(kind, g)}
        className={`${btn.ghost} disabled:opacity-40`}
      >
        <Trash2 className="h-3.5 w-3.5" />
      </button>
    );

  return (
    <Panel title="Holdouts and layers">
      <div className="space-y-4">
        {error && <ErrorNotice message={error} />}
        <div className="grid gap-6 lg:grid-cols-2">
          <section className="space-y-3">
            <div>
              <h3 className="text-sm font-medium text-ink">Holdouts</h3>
              <p className="text-sm text-muted">
                Keep a share of users off every flag you attach, then see what all those launches added up to. Attach a
                flag from its card above; held-back users get no variant, even after you ship.
              </p>
            </div>
            {holdouts.map((h) => (
              <div key={h.id} className="space-y-2 rounded-lg border border-line bg-sunken p-3">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <p className="font-medium text-ink">{h.name}</p>
                    <p className="text-xs text-muted">
                      {pct(h.bps)} held back ·{" "}
                      {h.flags.length ? h.flags.map((f) => f.flag_key).join(", ") : "no flags attached yet"}
                    </p>
                  </div>
                  {deleteButton("holdouts", h)}
                </div>
                {h.flags.length > 0 && <HoldoutImpact projectId={projectId} holdout={h} events={events} />}
              </div>
            ))}
            {createForm("holdouts", "e.g. 2026 H2 launches")}
          </section>

          <section className="space-y-3">
            <div>
              <h3 className="text-sm font-medium text-ink">Layers</h3>
              <p className="text-sm text-muted">
                Experiments in the same layer never share a user, so tests on the same page can't affect each other.
                Pick a layer when you start an experiment.
              </p>
            </div>
            {layers.map((l) => (
              <div key={l.id} className="space-y-2 rounded-lg border border-line bg-sunken p-3">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <p className="font-medium text-ink">{l.name}</p>
                    <p className="text-xs text-muted">{pct(l.free_bps ?? 10_000)} free</p>
                  </div>
                  {deleteButton("layers", l)}
                </div>
                <div className="relative h-6 overflow-hidden rounded border border-line bg-panel" aria-hidden>
                  {l.flags.map((f) => (
                    <div
                      key={f.flag_key}
                      title={`${f.flag_key}: ${pct((f.end_bps ?? 0) - (f.start_bps ?? 0))}${f.enabled ? "" : " (off, slice reusable)"}`}
                      className={`absolute inset-y-0 truncate border-r border-panel px-1 text-[0.6875rem] leading-6 ${
                        f.enabled ? "bg-brand text-on-brand" : "bg-hover text-muted"
                      }`}
                      style={{ left: `${(f.start_bps ?? 0) / 100}%`, width: `${((f.end_bps ?? 0) - (f.start_bps ?? 0)) / 100}%` }}
                    >
                      {f.flag_key}
                    </div>
                  ))}
                </div>
                <ul className="text-xs text-muted">
                  {l.flags.map((f) => (
                    <li key={f.flag_key}>
                      <span className="font-mono">{f.flag_key}</span> · {pct((f.end_bps ?? 0) - (f.start_bps ?? 0))}
                      {f.enabled ? "" : " · off, its slice can be reused"}
                    </li>
                  ))}
                  {l.flags.length === 0 && <li>No experiments yet.</li>}
                </ul>
              </div>
            ))}
            {createForm("layers", "e.g. Checkout page")}
          </section>
        </div>
      </div>
    </Panel>
  );
};
