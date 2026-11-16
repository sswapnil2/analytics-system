import React, { useEffect, useRef, useState } from "react";
import { LayoutDashboard } from "lucide-react";
import { api, Dashboard, ReportKind } from "../api/client";
import { btn, field, fieldLabel } from "../components/ui";
import { errorMessage } from "../lib/format";
import type { RangeSel } from "./ReportCard";

const NEW = "__new__";

/**
 * "Save to dashboard" for a report page: saves the current query as a report
 * and adds it as a card. The query is stored without its date range; the
 * dashboard supplies one.
 */
export const SaveToDashboard: React.FC<{
  projectId: string;
  kind: ReportKind;
  defaultName: string;
  query: () => Record<string, unknown>;
}> = ({ projectId, kind, defaultName, query }) => {
  const [open, setOpen] = useState(false);
  const [dashboards, setDashboards] = useState<Dashboard[] | null>(null);
  const [name, setName] = useState(defaultName);
  const [target, setTarget] = useState<string>(NEW);
  const [newName, setNewName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedTo, setSavedTo] = useState<Dashboard | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    setName(defaultName);
    setSavedTo(null);
    setError(null);
    api
      .listDashboards(projectId)
      .then((list) => {
        setDashboards(list);
        // `#/funnels?dashboard=<id>` comes from a dashboard's Add menu: preselect it.
        const wanted = new URLSearchParams(window.location.hash.split("?")[1] ?? "").get("dashboard");
        setTarget(list.find((d) => d.id === wanted)?.id ?? list[0]?.id ?? NEW);
      })
      .catch((err) => setError(errorMessage(err)));
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    const onClick = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onClick);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onClick);
    };
  }, [open, projectId, defaultName]);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return setError("Give the report a name.");
    if (target === NEW && !newName.trim()) return setError("Name the new dashboard.");
    setBusy(true);
    setError(null);
    try {
      const { time_range: _omit, ...q } = query();
      void _omit;
      const report = await api.createReport(projectId, { name: name.trim(), report_kind: kind, query_json: q });
      const card = { kind: "report" as const, report_id: report.id, width: 1 as const };
      let dash: Dashboard;
      if (target === NEW) {
        dash = await api.createDashboard(projectId, { name: newName.trim(), cards: [card] });
      } else {
        const current = await api.getDashboard(projectId, target);
        dash = await api.updateDashboard(projectId, target, {
          name: current.name,
          description: current.description,
          cards: [...current.cards, card],
        });
      }
      setSavedTo(dash);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="relative" ref={panelRef}>
      <button type="button" onClick={() => setOpen((v) => !v)} className={btn.secondary} aria-expanded={open}>
        <LayoutDashboard className="h-4 w-4" />
        Save to dashboard
      </button>
      {open && (
        <div
          role="dialog"
          aria-label="Save to dashboard"
          className="absolute right-0 top-full z-40 mt-2 w-[min(22rem,calc(100vw-2rem))] rounded-xl border border-line-strong bg-panel p-4 shadow-[0_12px_32px_rgb(0_0_0/0.16)]"
        >
          {savedTo ? (
            <div className="space-y-3 text-sm" role="status">
              <p className="text-ink">
                Saved to <span className="font-semibold">{savedTo.name}</span>.
              </p>
              <div className="flex gap-2">
                <a href={`#/dashboards/${savedTo.id}`} className={btn.primary} onClick={() => setOpen(false)}>
                  Open dashboard
                </a>
                <button type="button" className={btn.ghost} onClick={() => setOpen(false)}>
                  Done
                </button>
              </div>
            </div>
          ) : (
            <form onSubmit={(e) => void save(e)} className="space-y-3" noValidate>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Report name</span>
                <input value={name} onChange={(e) => setName(e.target.value)} className={field} autoFocus />
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Dashboard</span>
                <select value={target} onChange={(e) => setTarget(e.target.value)} className={field} disabled={!dashboards}>
                  {(dashboards ?? []).map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.name}
                    </option>
                  ))}
                  <option value={NEW}>New dashboard…</option>
                </select>
              </label>
              {target === NEW && (
                <label className="flex flex-col gap-1">
                  <span className={fieldLabel}>New dashboard name</span>
                  <input
                    value={newName}
                    onChange={(e) => setNewName(e.target.value)}
                    placeholder="Onboarding health"
                    className={field}
                  />
                </label>
              )}
              {error && (
                <p role="alert" className="text-sm text-danger">
                  {error}
                </p>
              )}
              <div className="flex justify-end gap-2">
                <button type="button" className={btn.ghost} onClick={() => setOpen(false)}>
                  Cancel
                </button>
                <button type="submit" disabled={busy} className={btn.primary}>
                  {busy ? "Saving…" : "Save"}
                </button>
              </div>
            </form>
          )}
        </div>
      )}
    </div>
  );
};

/**
 * Opens a saved report on its report page: `#/funnels?report=<id>&range=<sel>` (from a
 * dashboard card) loads that report's query into the page once, through `apply`.
 */
export function useSavedReport(
  projectId: string,
  apply: (query: Record<string, unknown>) => void,
  setRange: (r: RangeSel) => void,
): string | null {
  const [error, setError] = useState<string | null>(null);
  const applyRef = useRef(apply);
  applyRef.current = apply;
  useEffect(() => {
    const params = new URLSearchParams(window.location.hash.split("?")[1] ?? "");
    const id = params.get("report");
    const range = params.get("range");
    if (range && /^(today|yesterday|\d+|custom:\d{4}-\d\d-\d\d:\d{4}-\d\d-\d\d)$/.test(range)) setRange(range as RangeSel);
    if (!id) return;
    let live = true;
    api
      .listReports(projectId)
      .then((list) => {
        if (!live) return;
        const r = list.find((x) => x.id === id);
        if (r) applyRef.current(r.query_json);
        else setError("That saved report was deleted. Showing a new report instead.");
      })
      .catch((err) => live && setError(`Couldn't open the saved report: ${errorMessage(err)}`));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);
  return error;
}
