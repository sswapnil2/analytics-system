import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDown,
  ArrowUp,
  LayoutDashboard,
  Link2,
  Maximize2,
  Minimize2,
  Pencil,
  Plus,
  Trash2,
  X,
} from "lucide-react";
import { api, Dashboard, DashboardCard, SavedReport } from "../api/client";
import { btn, EmptyState, ErrorNotice, field, fieldLabel, PageHeader, Panel, Spinner } from "../components/ui";
import { errorMessage, formatCount } from "../lib/format";
import { KIND_LABEL, RANGE_OPTIONS, ReportCardBody } from "./ReportCard";

/** `#/dashboards` lists them; `#/dashboards/<id>` opens one. */
export function dashboardIdFromHash(): string | null {
  const m = window.location.hash.match(/^#\/?dashboards\/([^/?#]+)/);
  return m ? decodeURIComponent(m[1]) : null;
}

const REPORT_PAGE: Record<SavedReport["report_kind"], string> = {
  funnel: "funnels",
  insights: "trends",
  retention: "retention",
  flows: "journeys",
};

function formatWhen(ms: number | null | undefined): string {
  if (!ms) return "";
  return new Date(ms).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });
}

export const DashboardsWorkspace: React.FC<{ projectId: string; refreshToken: number }> = ({
  projectId,
  refreshToken,
}) => {
  const [openId, setOpenId] = useState<string | null>(dashboardIdFromHash);

  useEffect(() => {
    const onHash = () => setOpenId(dashboardIdFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  return openId ? (
    <DashboardView key={openId} projectId={projectId} dashboardId={openId} refreshToken={refreshToken} />
  ) : (
    <DashboardList projectId={projectId} refreshToken={refreshToken} />
  );
};

// ---------------------------------------------------------------------------

const DashboardList: React.FC<{ projectId: string; refreshToken: number }> = ({ projectId, refreshToken }) => {
  const [list, setList] = useState<Dashboard[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setError(null);
    try {
      setList(await api.listDashboards(projectId));
    } catch (err) {
      setError(errorMessage(err));
    }
  }, [projectId]);

  useEffect(() => {
    void load();
  }, [load, refreshToken]);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    setBusy(true);
    try {
      const d = await api.createDashboard(projectId, { name: name.trim(), cards: [] });
      window.location.hash = `/dashboards/${d.id}`;
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-5">
      <PageHeader
        title="Dashboards"
        description="Saved funnels, trends and retention charts, side by side, ready to share with your team."
        actions={
          <button type="button" onClick={() => setCreating(true)} className={btn.primary}>
            <Plus className="h-4 w-4" />
            New dashboard
          </button>
        }
      />

      {creating && (
        <Panel>
          <form onSubmit={(e) => void create(e)} className="flex flex-wrap items-end gap-3">
            <label className="flex min-w-[16rem] flex-1 flex-col gap-1">
              <span className={fieldLabel}>Dashboard name</span>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Onboarding health"
                className={field}
                autoFocus
              />
            </label>
            <button type="submit" disabled={busy || !name.trim()} className={btn.primary}>
              {busy ? "Creating…" : "Create dashboard"}
            </button>
            <button type="button" onClick={() => setCreating(false)} className={btn.ghost}>
              Cancel
            </button>
          </form>
        </Panel>
      )}

      {error && <ErrorNotice message={error} onRetry={() => void load()} />}

      {!list ? (
        !error && <Spinner />
      ) : list.length === 0 ? (
        <Panel>
          <EmptyState
            icon={LayoutDashboard}
            title="No dashboards yet"
            body="Create one, or open a funnel, trend or retention report and choose Save to dashboard."
            action={
              <button type="button" onClick={() => setCreating(true)} className={btn.primary}>
                New dashboard
              </button>
            }
          />
        </Panel>
      ) : (
        <ul className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {list.map((d) => {
            const reports = d.cards.filter((c) => c.kind === "report").length;
            return (
              <li key={d.id}>
                <a
                  href={`#/dashboards/${d.id}`}
                  className="flex h-full flex-col gap-2 rounded-xl border border-line bg-panel p-5 hover:border-line-strong hover:bg-hover"
                >
                  <span className="text-[0.9375rem] font-semibold text-ink">{d.name}</span>
                  {d.description && <span className="line-clamp-2 text-sm text-muted">{d.description}</span>}
                  <span className="mt-auto pt-2 text-xs text-muted">
                    {formatCount(reports)} {reports === 1 ? "report" : "reports"}
                    {d.updated_at_ms || d.created_at_ms ? `, updated ${formatWhen(d.updated_at_ms ?? d.created_at_ms)}` : ""}
                    {d.created_by ? `, by ${d.created_by}` : ""}
                  </span>
                </a>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------

const iconBtn = "inline-flex h-8 w-8 items-center justify-center rounded-lg text-muted hover:bg-hover hover:text-ink disabled:opacity-30";

const DashboardView: React.FC<{ projectId: string; dashboardId: string; refreshToken: number }> = ({
  projectId,
  dashboardId,
  refreshToken,
}) => {
  const [dash, setDash] = useState<Dashboard | null>(null);
  const [reports, setReports] = useState<Map<string, SavedReport>>(new Map());
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [days, setDays] = useState<number>(14);
  const [editingTextId, setEditingTextId] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [sharing, setSharing] = useState(false);
  const [editingTitle, setEditingTitle] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const load = useCallback(async () => {
    setLoadError(null);
    try {
      const [d, reps] = await Promise.all([api.getDashboard(projectId, dashboardId), api.listReports(projectId)]);
      setDash(d);
      setReports(new Map(reps.map((r) => [r.id, r])));
    } catch (err) {
      setLoadError(errorMessage(err));
    }
  }, [projectId, dashboardId]);

  useEffect(() => {
    void load();
  }, [load, refreshToken]);

  /** Optimistic save: show the change now, roll back if the server refuses it. */
  const persist = async (next: Dashboard) => {
    const before = dash;
    setDash(next);
    setSaveError(null);
    try {
      const saved = await api.updateDashboard(projectId, next.id, {
        name: next.name,
        description: next.description,
        cards: next.cards,
      });
      setDash(saved);
      return saved;
    } catch (err) {
      setDash(before);
      setSaveError(`Couldn't save your change: ${errorMessage(err)}`);
      return null;
    }
  };

  if (loadError) {
    return (
      <div className="space-y-4">
        <a href="#/dashboards" className="text-sm text-muted hover:text-ink">
          All dashboards
        </a>
        <ErrorNotice message={`Couldn't open this dashboard: ${loadError}`} onRetry={() => void load()} />
      </div>
    );
  }
  if (!dash) return <Spinner />;

  const cards = dash.cards;
  const updateCards = (fn: (c: DashboardCard[]) => DashboardCard[]) => void persist({ ...dash, cards: fn(cards) });
  const move = (i: number, delta: number) =>
    updateCards((c) => {
      const next = [...c];
      const [it] = next.splice(i, 1);
      next.splice(i + delta, 0, it);
      return next;
    });

  const addText = async () => {
    const saved = await persist({ ...dash, cards: [{ kind: "text", text: "Add a note for your team.", width: 2 }, ...cards] });
    if (saved?.cards[0]?.id) setEditingTextId(saved.cards[0].id);
  };

  const onBoard = new Set(cards.filter((c) => c.report_id).map((c) => c.report_id as string));

  return (
    <div className="space-y-5">
      <a href="#/dashboards" className="text-sm text-muted hover:text-ink">
        All dashboards
      </a>

      <header className="flex flex-wrap items-end justify-between gap-4">
        <div className="min-w-0 max-w-2xl flex-1">
          {editingTitle ? (
            <TitleEditor
              dash={dash}
              onCancel={() => setEditingTitle(false)}
              onSave={async (name, description) => {
                if (await persist({ ...dash, name, description })) setEditingTitle(false);
              }}
            />
          ) : (
            <div className="group">
              <h1 className="flex items-center gap-2 text-[1.875rem] font-semibold leading-tight tracking-[-0.02em] text-ink">
                {dash.name}
                <button type="button" onClick={() => setEditingTitle(true)} className={iconBtn} aria-label="Rename dashboard">
                  <Pencil className="h-4 w-4" />
                </button>
              </h1>
              {dash.description && <p className="mt-1 text-[0.9375rem] text-muted">{dash.description}</p>}
            </div>
          )}
        </div>
        <div className="relative flex flex-wrap items-center gap-2">
          <label className="inline-flex h-9 items-center gap-2 rounded-lg border border-line-strong bg-panel pl-3 pr-1 text-sm font-medium text-ink-2">
            Dates
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="h-7 bg-transparent text-ink focus:outline-none"
            >
              {RANGE_OPTIONS.map((o) => (
                <option key={o.days} value={o.days}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <button type="button" onClick={() => void addText()} className={btn.secondary}>
            Add text
          </button>
          <button type="button" onClick={() => setAdding(true)} className={btn.secondary}>
            Add report
          </button>
          <button type="button" onClick={() => setSharing((v) => !v)} className={btn.primary} aria-expanded={sharing}>
            <Link2 className="h-4 w-4" />
            Share
          </button>
          {sharing && <SharePopover dashboardId={dash.id} onClose={() => setSharing(false)} />}
        </div>
      </header>

      {saveError && <ErrorNotice message={saveError} />}

      {adding && (
        <AddReportPanel
          reports={Array.from(reports.values())}
          onBoard={onBoard}
          onAdd={(r) => updateCards((c) => [...c, { kind: "report", report_id: r.id, width: 1 }])}
          onClose={() => setAdding(false)}
        />
      )}

      {cards.length === 0 ? (
        <Panel>
          <EmptyState
            icon={LayoutDashboard}
            title="This dashboard is empty"
            body="Add a saved report, or open Funnels, Trends or Retention and choose Save to dashboard."
            action={
              <button type="button" onClick={() => setAdding(true)} className={btn.primary}>
                Add report
              </button>
            }
          />
        </Panel>
      ) : (
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
          {cards.map((card, i) => {
            const report = card.report_id ? reports.get(card.report_id) : undefined;
            const controls = (
              <div className="flex shrink-0 items-center opacity-100 sm:opacity-0 sm:transition-opacity sm:group-focus-within:opacity-100 sm:group-hover:opacity-100">
                <button type="button" className={iconBtn} disabled={i === 0} onClick={() => move(i, -1)} aria-label="Move up">
                  <ArrowUp className="h-4 w-4" />
                </button>
                <button
                  type="button"
                  className={iconBtn}
                  disabled={i === cards.length - 1}
                  onClick={() => move(i, 1)}
                  aria-label="Move down"
                >
                  <ArrowDown className="h-4 w-4" />
                </button>
                <button
                  type="button"
                  className={`${iconBtn} hidden lg:inline-flex`}
                  onClick={() => updateCards((c) => c.map((x, j) => (j === i ? { ...x, width: x.width === 2 ? 1 : 2 } : x)))}
                  aria-label={card.width === 2 ? "Make half width" : "Make full width"}
                >
                  {card.width === 2 ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
                </button>
                <button
                  type="button"
                  className={iconBtn}
                  onClick={() => updateCards((c) => c.filter((_, j) => j !== i))}
                  aria-label="Remove from dashboard"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            );

            if (card.kind === "text") {
              const editing = editingTextId !== null && editingTextId === card.id;
              return (
                <section
                  key={card.id ?? i}
                  className={`group flex items-start gap-3 rounded-xl border border-line bg-panel px-5 py-4 ${card.width === 2 ? "lg:col-span-2" : ""}`}
                >
                  {editing ? (
                    <TextCardEditor
                      initial={card.text ?? ""}
                      onCancel={() => setEditingTextId(null)}
                      onSave={(text) => {
                        updateCards((c) => c.map((x, j) => (j === i ? { ...x, text } : x)));
                        setEditingTextId(null);
                      }}
                    />
                  ) : (
                    <>
                      <p className="min-w-0 flex-1 whitespace-pre-line text-[0.9375rem] leading-relaxed text-ink-2">{card.text}</p>
                      <button
                        type="button"
                        className={iconBtn}
                        onClick={() => setEditingTextId(card.id ?? null)}
                        aria-label="Edit note"
                        disabled={!card.id}
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      {controls}
                    </>
                  )}
                </section>
              );
            }

            return (
              <section
                key={card.id ?? i}
                className={`group flex flex-col rounded-xl border border-line bg-panel ${card.width === 2 ? "lg:col-span-2" : ""}`}
              >
                <div className="flex items-start gap-2 px-5 pb-0 pt-4">
                  <div className="min-w-0 flex-1">
                    {report ? (
                      <a
                        href={`#/${REPORT_PAGE[report.report_kind]}`}
                        className="block truncate text-[0.9375rem] font-semibold text-ink hover:underline"
                      >
                        {report.name}
                      </a>
                    ) : (
                      <span className="text-[0.9375rem] font-semibold text-muted">Report removed</span>
                    )}
                    <span className="text-xs text-muted">{report ? KIND_LABEL[report.report_kind] : "It was deleted"}</span>
                  </div>
                  {controls}
                </div>
                <div className="px-5 pb-5 pt-3">
                  {report ? (
                    <ReportCardBody projectId={projectId} report={report} days={days} />
                  ) : (
                    <p className="text-sm text-muted">Remove this card or add the report again.</p>
                  )}
                </div>
              </section>
            );
          })}
        </div>
      )}

      <div className="flex items-center justify-between gap-3 border-t border-line pt-4 text-xs text-muted">
        <span>
          {dash.created_by ? `Created by ${dash.created_by}. ` : ""}
          {dash.updated_at_ms ? `Last changed ${formatWhen(dash.updated_at_ms)}.` : ""}
        </span>
        {confirmDelete ? (
          <span className="flex items-center gap-2">
            <span className="text-ink">Delete this dashboard? Its reports stay saved.</span>
            <button
              type="button"
              className="font-semibold text-danger hover:underline"
              onClick={async () => {
                try {
                  await api.deleteDashboard(projectId, dash.id);
                  window.location.hash = "/dashboards";
                } catch (err) {
                  setSaveError(`Couldn't delete the dashboard: ${errorMessage(err)}`);
                  setConfirmDelete(false);
                }
              }}
            >
              Delete
            </button>
            <button type="button" className="hover:text-ink" onClick={() => setConfirmDelete(false)}>
              Keep it
            </button>
          </span>
        ) : (
          <button type="button" className="inline-flex items-center gap-1 hover:text-danger" onClick={() => setConfirmDelete(true)}>
            <Trash2 className="h-3.5 w-3.5" />
            Delete dashboard
          </button>
        )}
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------

const TitleEditor: React.FC<{
  dash: Dashboard;
  onSave: (name: string, description: string | null) => void;
  onCancel: () => void;
}> = ({ dash, onSave, onCancel }) => {
  const [name, setName] = useState(dash.name);
  const [description, setDescription] = useState(dash.description ?? "");
  return (
    <form
      className="space-y-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (name.trim()) onSave(name.trim(), description.trim() || null);
      }}
    >
      <input
        value={name}
        onChange={(e) => setName(e.target.value)}
        aria-label="Dashboard name"
        className={`${field} w-full text-lg font-semibold`}
        autoFocus
      />
      <input
        value={description}
        onChange={(e) => setDescription(e.target.value)}
        aria-label="Description"
        placeholder="What is this dashboard for?"
        className={`${field} w-full`}
      />
      <div className="flex gap-2">
        <button type="submit" className={btn.primary} disabled={!name.trim()}>
          Save
        </button>
        <button type="button" className={btn.ghost} onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  );
};

const TextCardEditor: React.FC<{ initial: string; onSave: (t: string) => void; onCancel: () => void }> = ({
  initial,
  onSave,
  onCancel,
}) => {
  const [text, setText] = useState(initial);
  return (
    <div className="w-full space-y-2">
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={3}
        maxLength={5000}
        aria-label="Note"
        className={`${field} w-full`}
        autoFocus
      />
      <div className="flex gap-2">
        <button type="button" className={btn.primary} onClick={() => onSave(text.trim() || "Add a note for your team.")}>
          Save note
        </button>
        <button type="button" className={btn.ghost} onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  );
};

const AddReportPanel: React.FC<{
  reports: SavedReport[];
  onBoard: Set<string>;
  onAdd: (r: SavedReport) => void;
  onClose: () => void;
}> = ({ reports, onBoard, onAdd, onClose }) => {
  const available = useMemo(
    () => reports.filter((r) => !onBoard.has(r.id)).sort((a, b) => b.created_at_ms - a.created_at_ms),
    [reports, onBoard],
  );
  return (
    <Panel
      title="Add a saved report"
      actions={
        <button type="button" onClick={onClose} className={iconBtn} aria-label="Close">
          <X className="h-4 w-4" />
        </button>
      }
    >
      {available.length === 0 ? (
        <p className="text-sm text-muted">
          Every saved report is already here. Open{" "}
          <a href="#/funnels" className="text-brand hover:underline">
            Funnels
          </a>
          ,{" "}
          <a href="#/trends" className="text-brand hover:underline">
            Trends
          </a>{" "}
          or{" "}
          <a href="#/retention" className="text-brand hover:underline">
            Retention
          </a>{" "}
          and choose Save to dashboard to make a new one.
        </p>
      ) : (
        <ul className="-my-2 divide-y divide-line">
          {available.map((r) => (
            <li key={r.id} className="flex items-center justify-between gap-3 py-2.5">
              <span className="min-w-0">
                <span className="block truncate text-sm font-medium text-ink">{r.name}</span>
                <span className="text-xs text-muted">{KIND_LABEL[r.report_kind]}</span>
              </span>
              <button type="button" className={btn.secondary} onClick={() => onAdd(r)}>
                Add
              </button>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
};

const SharePopover: React.FC<{ dashboardId: string; onClose: () => void }> = ({ dashboardId, onClose }) => {
  const ref = useRef<HTMLDivElement>(null);
  const [copied, setCopied] = useState(false);
  const link = `${window.location.origin}${window.location.pathname}#/dashboards/${encodeURIComponent(dashboardId)}`;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node) && !(e.target as HTMLElement).closest("[aria-expanded]"))
        onClose();
    };
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onClick);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onClick);
    };
  }, [onClose]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(link);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div
      ref={ref}
      role="dialog"
      aria-label="Share dashboard"
      className="absolute right-0 top-full z-40 mt-2 w-[min(22.5rem,calc(100vw-2rem))] space-y-3 rounded-xl border border-line-strong bg-panel p-4 shadow-[0_12px_32px_rgb(0_0_0/0.16)]"
    >
      <h2 className="text-[0.9375rem] font-semibold">Share this dashboard</h2>
      <p className="text-[0.8125rem] leading-relaxed text-ink-2">
        Anyone signed in to this Pulse workspace can open the link and see the same charts. User ids stay masked.
      </p>
      <div className="flex gap-2">
        <input
          readOnly
          value={link}
          aria-label="Dashboard link"
          onFocus={(e) => e.currentTarget.select()}
          className={`${field} min-w-0 flex-1 bg-canvas text-[0.8125rem]`}
        />
        <button type="button" className={btn.primary} onClick={() => void copy()}>
          {copied ? "Copied" : "Copy link"}
        </button>
      </div>
    </div>
  );
};
