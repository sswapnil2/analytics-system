import React, { useCallback, useEffect, useMemo, useState } from "react";
import { CheckCircle2, ChevronLeft, ChevronRight, Download, PlayCircle, Search, Trash2, Users } from "lucide-react";
import { api, DeletionAuditRecord, UserDetailResponse, UserEventItem, UserProfileItem } from "../api/client";
import { useAccess } from "../auth/access";
import { SessionReplay } from "../components/WireframePlayer";
import { EmptyState, ErrorNotice, ListSkeleton, PageHeader, Panel, Spinner, btn, field } from "../components/ui";
import { errorMessage, formatCount, formatTimestamp, humanizeEvent } from "../lib/format";

const PAGE = 50;
const TIMELINE_STEP = 50;

function formatPropValue(v: unknown): string {
  if (v === null || v === undefined || v === "") return "—";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

function formatRevenue(amount: number, props?: Record<string, unknown>): string {
  const formatted = amount.toLocaleString("en-US", { maximumFractionDigits: 2 });
  const currency =
    typeof props?.currency === "string" && props.currency.trim() ? props.currency.trim().toUpperCase() : "USD";
  return currency === "USD" ? `$${formatted}` : `${currency} ${formatted}`;
}

/** The three properties most users on this page have, so columns fit the project's own data. */
function commonKeys(users: UserProfileItem[]): string[] {
  const counts = new Map<string, number>();
  for (const u of users) for (const k of Object.keys(u.props ?? {})) if (k !== "total_revenue") counts.set(k, (counts.get(k) ?? 0) + 1);
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 3).map(([k]) => k);
}

const dayLabel = (ts: number) =>
  new Date(ts).toLocaleDateString(undefined, { weekday: "short", day: "numeric", month: "short", year: "numeric" });
const timeLabel = (ts: number) => new Date(ts).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });

/** Event props worth showing in a timeline row: skip internal `$` keys and long blobs. */
const readableProps = (ev: UserEventItem) =>
  Object.entries(ev.props ?? {})
    .filter(([k, v]) => !k.startsWith("$") && formatPropValue(v).length <= 60)
    .slice(0, 4);

export const UsersWorkspace: React.FC<{ projectId: string }> = ({ projectId }) => {
  const { canAdmin } = useAccess();
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [offset, setOffset] = useState(0);
  const [users, setUsers] = useState<UserProfileItem[]>([]);
  const [deletions, setDeletions] = useState<DeletionAuditRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<UserDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [shownEvents, setShownEvents] = useState(TIMELINE_STEP);
  const [replay, setReplay] = useState<string | null>(null);

  const [confirmErase, setConfirmErase] = useState(false);
  const [erasing, setErasing] = useState(false);
  const [eraseNotice, setEraseNotice] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const [list, audit] = await Promise.all([
        api.listUsers(projectId, query.trim(), PAGE, offset),
        api.listDeletionAudit(projectId),
      ]);
      setUsers(list);
      setDeletions(audit);
    } catch (err) {
      setLoadError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId, query, offset]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    setSelectedId(null);
    setDetail(null);
    setOffset(0);
  }, [projectId]);

  const open = async (distinctId: string) => {
    setSelectedId(distinctId);
    setDetailLoading(true);
    setDetailError(null);
    setConfirmErase(false);
    setEraseNotice(null);
    setShownEvents(TIMELINE_STEP);
    setReplay(null);
    try {
      setDetail(await api.getUserDetail(projectId, distinctId));
    } catch (err) {
      setDetail(null);
      setDetailError(errorMessage(err));
    } finally {
      setDetailLoading(false);
    }
  };

  const erase = async () => {
    if (!detail) return;
    setErasing(true);
    setDetailError(null);
    try {
      const audit = await api.deleteUserDpdp(projectId, detail.distinct_id);
      setEraseNotice(
        `Erased ${audit.distinct_id_masked}: ${formatCount(audit.events_purged)} events and ${formatCount(audit.replays_purged)} replays removed (audit ${audit.id}). If they sign up again, their new activity is counted.`,
      );
      setConfirmErase(false);
      setDetail(null);
      setSelectedId(null);
      await load();
    } catch (err) {
      setDetailError(errorMessage(err));
    } finally {
      setErasing(false);
    }
  };

  // Every profile matching the search, page by page, as CSV.
  const exportCsv = async () => {
    setExporting(true);
    try {
      const all: UserProfileItem[] = [];
      // The API returns at most 500 per page.
      for (let off = 0; ; off += 500) {
        const page = await api.listUsers(projectId, query.trim(), 500, off);
        all.push(...page);
        if (page.length < 500) break;
      }
      const keys = Array.from(new Set(all.flatMap((u) => Object.keys(u.props ?? {})))).sort();
      const rows = [["distinct_id", "total_revenue", "updated_at", ...keys]];
      for (const u of all)
        rows.push([u.distinct_id, String(u.total_revenue), new Date(u.updated_at_ms).toISOString(), ...keys.map((k) => (u.props?.[k] === undefined ? "" : formatPropValue(u.props[k])))]);
      const csv = rows.map((r) => r.map((c) => `"${c.replace(/"/g, '""')}"`).join(",")).join("\n");
      const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
      const a = document.createElement("a");
      a.href = url;
      a.download = `users-${projectId}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setLoadError(`Couldn't export users: ${errorMessage(err)}`);
    } finally {
      setExporting(false);
    }
  };

  const columns = useMemo(() => commonKeys(users), [users]);
  const timeline = useMemo(() => {
    const days: { day: string; events: UserEventItem[] }[] = [];
    for (const ev of (detail?.events ?? []).slice(0, shownEvents)) {
      const day = dayLabel(ev.event_ts_ms);
      if (days[days.length - 1]?.day !== day) days.push({ day, events: [] });
      days[days.length - 1].events.push(ev);
    }
    return days;
  }, [detail, shownEvents]);
  const replays = new Set(detail?.replay_sessions ?? []);

  return (
    <div className="space-y-5">
      <PageHeader
        title="Users"
        description="Find a user, see their properties and everything they did, and play their recorded sessions."
        actions={
          <button type="button" className={btn.secondary} onClick={() => void exportCsv()} disabled={exporting || users.length === 0}>
            {exporting ? <Spinner label="Exporting" /> : <Download className="h-4 w-4" />} Export CSV
          </button>
        }
      />

      <Panel
        title="People"
        meta={users.length ? `${formatCount(offset + 1)}–${formatCount(offset + users.length)}` : undefined}
        actions={
          <form
            onSubmit={(e) => {
              e.preventDefault();
              setOffset(0);
              setQuery(search);
            }}
            className="flex items-center gap-2"
          >
            <input
              type="search"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search id or any property value"
              className={`${field} w-64`}
              aria-label="Search users"
            />
            <button type="submit" className={btn.secondary}>
              <Search className="h-3.5 w-3.5" /> Search
            </button>
          </form>
        }
      >
        {loadError && <ErrorNotice message={loadError} onRetry={() => void load()} />}
        {eraseNotice && (
          <div className="mb-4 flex items-center gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-sm text-ink" role="status">
            <CheckCircle2 className="h-4 w-4 shrink-0 text-ok" />
            <span>{eraseNotice}</span>
          </div>
        )}
        {loading ? (
          <ListSkeleton rows={6} />
        ) : users.length === 0 ? (
          <EmptyState
            icon={Users}
            title={query ? `No users match "${query}"` : "No user profiles yet"}
            body={
              query
                ? "Search matches the start or any part of an id or property value. Try a shorter term."
                : "Profiles are created when you send identify or $set calls (POST /v1/identify, /v1/engage, or the SDK's identify)."
            }
          />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-line text-xs font-medium text-muted">
                    <th className="py-2 pr-3">User</th>
                    {columns.map((k) => (
                      <th key={k} className="py-2 pr-3">{humanizeEvent(k)}</th>
                    ))}
                    <th className="py-2 pr-3 text-right">Revenue</th>
                    <th className="py-2 text-right">Last updated</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {users.map((u) => (
                    <tr
                      key={u.distinct_id}
                      onClick={() => void open(u.distinct_id)}
                      className={`cursor-pointer ${selectedId === u.distinct_id ? "bg-brand-soft/40" : "hover:bg-hover/60"}`}
                    >
                      <td className="py-2.5 pr-3">
                        <button type="button" className="font-mono text-xs text-ink underline-offset-2 hover:underline" onClick={(e) => { e.stopPropagation(); void open(u.distinct_id); }}>
                          {u.distinct_id_masked}
                        </button>
                      </td>
                      {columns.map((k) => (
                        <td key={k} className="max-w-[12rem] truncate py-2.5 pr-3 text-xs text-ink-2">{formatPropValue(u.props?.[k])}</td>
                      ))}
                      <td className="py-2.5 pr-3 text-right font-mono text-xs tabular-nums text-ink">{formatRevenue(u.total_revenue, u.props)}</td>
                      <td className="py-2.5 text-right text-xs text-muted">{formatTimestamp(u.updated_at_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-3 flex items-center justify-end gap-2">
              <button type="button" className={btn.ghost} disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>
                <ChevronLeft className="h-4 w-4" /> Previous
              </button>
              <button type="button" className={btn.ghost} disabled={users.length < PAGE} onClick={() => setOffset(offset + PAGE)}>
                Next <ChevronRight className="h-4 w-4" />
              </button>
            </div>
          </>
        )}
      </Panel>

      {(detailLoading || detailError || detail) && (
        <Panel title={detail ? detail.distinct_id_masked : "User"}>
          {detailLoading ? (
            <Spinner label="Loading the user's profile and events" />
          ) : detailError ? (
            <ErrorNotice message={detailError} />
          ) : (
            detail && (
              <div className="space-y-5">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <p className="text-sm text-ink-2">
                    Lifetime revenue{" "}
                    <span className="font-medium text-ink">{formatRevenue(detail.profile?.total_revenue ?? 0, detail.profile?.props)}</span> ·{" "}
                    {formatCount(detail.events.length)} recent events · {formatCount(detail.replay_sessions.length)} recorded sessions
                  </p>
                  {canAdmin &&
                    (!confirmErase ? (
                      <button
                        type="button"
                        onClick={() => setConfirmErase(true)}
                        className="inline-flex items-center gap-1.5 rounded-lg border border-destructive/30 bg-destructive-soft px-3 py-1.5 text-xs font-semibold text-destructive hover:opacity-90"
                      >
                        <Trash2 className="h-3.5 w-3.5" /> Erase this user
                      </button>
                    ) : (
                      <div className="flex flex-wrap items-center gap-2 text-xs text-ink-2">
                        <span>Removes their profile, events and recordings. This can't be undone.</span>
                        <button
                          type="button"
                          disabled={erasing}
                          onClick={() => void erase()}
                          className="inline-flex items-center gap-1.5 rounded-lg bg-destructive px-3 py-1.5 font-semibold text-white hover:opacity-90 disabled:opacity-50"
                        >
                          {erasing ? <Spinner label="Erasing" /> : "Erase permanently"}
                        </button>
                        <button type="button" disabled={erasing} onClick={() => setConfirmErase(false)} className={btn.secondary}>
                          Cancel
                        </button>
                      </div>
                    ))}
                </div>

                <div>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">Properties</h3>
                  {Object.keys(detail.profile?.props ?? {}).length === 0 ? (
                    <p className="text-sm text-muted">No profile properties set for this user.</p>
                  ) : (
                    <dl className="grid grid-cols-1 gap-1.5 sm:grid-cols-2 lg:grid-cols-4">
                      {Object.entries(detail.profile?.props ?? {}).map(([k, v]) => (
                        <div key={k} className="rounded-lg border border-line bg-panel px-2.5 py-1.5">
                          <dt className="text-[11px] text-muted">{humanizeEvent(k)}</dt>
                          <dd className="truncate text-sm font-medium text-ink" title={formatPropValue(v)}>{formatPropValue(v)}</dd>
                        </div>
                      ))}
                    </dl>
                  )}
                </div>

                {replay && (
                  <div>
                    <div className="mb-2 flex items-center justify-between">
                      <h3 className="text-xs font-semibold uppercase tracking-wide text-muted">Recording</h3>
                      <button type="button" className={btn.ghost} onClick={() => setReplay(null)}>Close</button>
                    </div>
                    <SessionReplay key={replay} projectId={projectId} sessionId={replay} />
                  </div>
                )}

                <div>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">Activity, newest first</h3>
                  {timeline.length === 0 ? (
                    <p className="text-sm text-muted">No events stored for this user.</p>
                  ) : (
                    <ol className="space-y-4">
                      {timeline.map(({ day, events }) => (
                        <li key={day}>
                          <p className="mb-1.5 text-xs font-medium text-ink-2">{day}</p>
                          <ol className="space-y-1 border-l border-line pl-3">
                            {events.map((ev, i) => {
                              const prev = events[i - 1];
                              const sessionStart = ev.session_id && prev?.session_id !== ev.session_id;
                              return (
                                <li key={`${ev.event_ts_ms}-${i}`} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 text-sm">
                                  <span className="w-12 shrink-0 tabular text-xs text-muted">{timeLabel(ev.event_ts_ms)}</span>
                                  <span className="font-medium text-ink">{humanizeEvent(ev.event_name)}</span>
                                  {readableProps(ev).map(([k, v]) => (
                                    <span key={k} className="text-xs text-ink-2">
                                      <span className="text-muted">{humanizeEvent(k)}</span> {formatPropValue(v)}
                                    </span>
                                  ))}
                                  {sessionStart && replays.has(ev.session_id) && (
                                    <button type="button" className="inline-flex items-center gap-1 text-xs text-brand hover:underline" onClick={() => setReplay(ev.session_id)}>
                                      <PlayCircle className="h-3.5 w-3.5" /> Play session
                                    </button>
                                  )}
                                </li>
                              );
                            })}
                          </ol>
                        </li>
                      ))}
                    </ol>
                  )}
                  {detail.events.length > shownEvents && (
                    <button type="button" className={`${btn.ghost} mt-3`} onClick={() => setShownEvents(shownEvents + TIMELINE_STEP)}>
                      Show {formatCount(Math.min(TIMELINE_STEP, detail.events.length - shownEvents))} more
                    </button>
                  )}
                </div>
              </div>
            )
          )}
        </Panel>
      )}

      <Panel title="Erasure log" meta={`${formatCount(deletions.length)} erasures`}>
        {deletions.length === 0 ? (
          <p className="text-sm text-muted">No users have been erased.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="border-b border-line text-muted">
                  <th className="py-2 pr-3">Audit id</th>
                  <th className="py-2 pr-3">User</th>
                  <th className="py-2 pr-3 text-right">Events removed</th>
                  <th className="py-2 pr-3 text-right">Recordings removed</th>
                  <th className="py-2 pr-3">Requested by</th>
                  <th className="py-2 text-right">Erased at</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {deletions.map((d) => (
                  <tr key={d.id}>
                    <td className="py-2 pr-3 font-mono text-ink-2">{d.id}</td>
                    <td className="py-2 pr-3 font-mono text-ink">{d.distinct_id_masked}</td>
                    <td className="py-2 pr-3 text-right tabular-nums text-ink">{formatCount(d.events_purged)}</td>
                    <td className="py-2 pr-3 text-right tabular-nums text-ink">{formatCount(d.replays_purged)}</td>
                    <td className="py-2 pr-3 text-ink-2">{d.requested_by}</td>
                    <td className="py-2 text-right text-muted">{formatTimestamp(d.deleted_at_ms)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </div>
  );
};
