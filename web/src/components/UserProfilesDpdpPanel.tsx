import React, { useCallback, useEffect, useState } from "react";
import { CheckCircle2, Search, Trash2, Users } from "lucide-react";
import {
  api,
  DeletionAuditRecord,
  UserDetailResponse,
  UserProfileItem,
} from "../api/client";
import {
  EmptyState,
  ErrorNotice,
  ListSkeleton,
  Panel,
  Spinner,
  btn,
  field,
} from "../components/ui";
import {
  errorMessage,
  formatCount,
  formatTimestamp,
  humanizeEvent,
} from "../lib/format";

function formatPropValue(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v);
  }
  return JSON.stringify(v);
}

function formatRevenue(amount: number, props?: Record<string, unknown>): string {
  const formatted = amount.toLocaleString("en-IN", { maximumFractionDigits: 2 });
  const currency = typeof props?.currency === "string" ? props.currency : null;
  if (currency === "INR") return `₹${formatted}`;
  if (currency) return `${currency} ${formatted}`;
  return amount > 0 ? `₹${formatted}` : "0";
}

export const UserProfilesDpdpPanel: React.FC<{ projectId: string }> = ({
  projectId,
}) => {
  const [search, setSearch] = useState("");
  const [users, setUsers] = useState<UserProfileItem[]>([]);
  const [deletions, setDeletions] = useState<DeletionAuditRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [selectedDistinctId, setSelectedDistinctId] = useState<string | null>(null);
  const [detail, setDetail] = useState<UserDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const [confirmErase, setConfirmErase] = useState(false);
  const [erasing, setErasing] = useState(false);
  const [eraseNotice, setEraseNotice] = useState<string | null>(null);

  const loadProfilesAndAudit = useCallback(
    async (queryText: string) => {
      setLoading(true);
      setLoadError(null);
      try {
        const [userList, auditList] = await Promise.all([
          api.listUsers(projectId, queryText.trim(), 50),
          api.listDeletionAudit(projectId),
        ]);
        setUsers(userList);
        setDeletions(auditList);
      } catch (err) {
        setLoadError(errorMessage(err));
      } finally {
        setLoading(false);
      }
    },
    [projectId],
  );

  useEffect(() => {
    setSelectedDistinctId(null);
    setDetail(null);
    setConfirmErase(false);
    setEraseNotice(null);
    void loadProfilesAndAudit("");
  }, [projectId, loadProfilesAndAudit]);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    await loadProfilesAndAudit(search);
  };

  const handleInspect = async (distinctId: string) => {
    setSelectedDistinctId(distinctId);
    setDetailLoading(true);
    setDetailError(null);
    setConfirmErase(false);
    setEraseNotice(null);
    try {
      const res = await api.getUserDetail(projectId, distinctId);
      setDetail(res);
    } catch (err) {
      setDetail(null);
      setDetailError(errorMessage(err));
    } finally {
      setDetailLoading(false);
    }
  };

  const handleEraseUser = async () => {
    if (!detail) return;
    setErasing(true);
    setDetailError(null);
    try {
      const audit = await api.deleteUserDpdp(projectId, detail.distinct_id);
      setEraseNotice(
        `Erased ${audit.distinct_id_masked}: ${formatCount(audit.events_purged)} events and ${formatCount(audit.replays_purged)} replays purged (audit ${audit.id}).`,
      );
      setConfirmErase(false);
      setDetail(null);
      setSelectedDistinctId(null);
      await loadProfilesAndAudit(search);
    } catch (err) {
      setDetailError(errorMessage(err));
    } finally {
      setErasing(false);
    }
  };

  return (
    <Panel
      title="User Profiles & DPDP Erasure"
      meta={`${formatCount(users.length)} profiles · ${formatCount(deletions.length)} erasures`}
      actions={
        <form onSubmit={handleSearch} className="flex items-center gap-2">
          <input
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search distinct ID, plan, UTM..."
            className={`${field} w-56`}
            aria-label="Search user profiles"
          />
          <button type="submit" className={btn.secondary}>
            <Search className="h-3.5 w-3.5" />
            Search
          </button>
        </form>
      }
    >
      <p className="mb-4 text-xs text-muted">
        Inspect user profiles, attribution properties, session replays, and execute Digital Personal Data Protection (DPDP) right-to-erasure.
      </p>

      {loadError && (
        <ErrorNotice
          message={loadError}
          onRetry={() => loadProfilesAndAudit(search)}
        />
      )}

      {eraseNotice && (
        <div
          className="mb-4 flex items-center gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-xs text-ink"
          role="status"
        >
          <CheckCircle2 className="h-4 w-4 shrink-0 text-ok" />
          <span>{eraseNotice}</span>
        </div>
      )}

      <div className="space-y-6">
        {loading ? (
          <ListSkeleton rows={5} />
        ) : users.length === 0 ? (
          <EmptyState
            icon={Users}
            title="No matching user profiles"
            body="Try clearing the search filter or ingesting user profile updates via /v1/engage."
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-b border-line text-xs font-medium text-muted">
                  <th className="py-2 pr-3">User</th>
                  <th className="py-2 pr-3">Plan</th>
                  <th className="py-2 pr-3">UTM Source</th>
                  <th className="py-2 pr-3 text-right">Revenue</th>
                  <th className="py-2 pr-3">Updated</th>
                  <th className="py-2 text-right">Inspect</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {users.map((u) => {
                  const plan = formatPropValue(u.props?.plan);
                  const utmSource = formatPropValue(
                    u.props?.$initial_utm_source ?? u.props?.utm_source,
                  );
                  const selected = selectedDistinctId === u.distinct_id;
                  return (
                    <tr
                      key={u.distinct_id}
                      className={selected ? "bg-brand-soft/40" : "hover:bg-hover/60"}
                    >
                      <td className="py-2.5 pr-3 font-mono text-xs text-ink">
                        {u.distinct_id_masked}
                      </td>
                      <td className="py-2.5 pr-3 text-xs text-ink-2">{plan}</td>
                      <td className="py-2.5 pr-3 text-xs text-ink-2">
                        {utmSource}
                      </td>
                      <td className="py-2.5 pr-3 text-right font-mono text-xs tabular-nums text-ink">
                        {formatRevenue(u.total_revenue, u.props)}
                      </td>
                      <td className="py-2.5 pr-3 text-xs text-muted">
                        {formatTimestamp(u.updated_at_ms)}
                      </td>
                      <td className="py-2.5 text-right">
                        <button
                          type="button"
                          onClick={() => handleInspect(u.distinct_id)}
                          className={btn.secondary}
                        >
                          {selected && detailLoading ? (
                            <Spinner label="Loading" />
                          ) : (
                            "Inspect"
                          )}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {(detailLoading || detailError || detail) && (
          <div className="rounded-xl border border-line bg-sunken p-4 space-y-4">
            {detailLoading ? (
              <Spinner label="Loading user profile, events, and replays..." />
            ) : detailError ? (
              <ErrorNotice message={detailError} />
            ) : (
              detail && (
                <>
                  <div className="flex flex-wrap items-start justify-between gap-3 border-b border-line pb-3">
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-sm font-semibold text-ink">
                          {detail.distinct_id_masked}
                        </span>
                        <span className="rounded bg-panel px-2 py-0.5 font-mono text-[11px] text-muted border border-line">
                          uid {detail.canonical_uid}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-muted">
                        Lifetime revenue:{" "}
                        <span className="font-medium text-ink">
                          {formatRevenue(
                            detail.profile?.total_revenue ?? 0,
                            detail.profile?.props,
                          )}
                        </span>{" "}
                        · {formatCount(detail.events.length)} events ·{" "}
                        {formatCount(detail.replay_sessions.length)} replay
                        sessions
                      </p>
                    </div>

                    <div className="flex items-center gap-2">
                      {!confirmErase ? (
                        <button
                          type="button"
                          onClick={() => setConfirmErase(true)}
                          className="inline-flex whitespace-nowrap items-center justify-center gap-1.5 rounded-lg border border-destructive/30 bg-destructive-soft px-3 py-1.5 text-xs font-semibold text-destructive hover:opacity-90"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                          Erase User (DPDP)
                        </button>
                      ) : (
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            disabled={erasing}
                            onClick={handleEraseUser}
                            className="inline-flex whitespace-nowrap items-center justify-center gap-1.5 rounded-lg bg-destructive px-3 py-1.5 text-xs font-semibold text-white hover:opacity-90 disabled:opacity-50"
                          >
                            {erasing ? (
                              <Spinner label="Erasing" />
                            ) : (
                              "Confirm Permanent Erasure"
                            )}
                          </button>
                          <button
                            type="button"
                            disabled={erasing}
                            onClick={() => setConfirmErase(false)}
                            className={btn.secondary}
                          >
                            Cancel
                          </button>
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                    <div>
                      <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
                        Profile Properties
                      </h4>
                      {Object.keys(detail.profile?.props ?? {}).length === 0 ? (
                        <p className="text-xs text-muted">
                          No user profile properties set.
                        </p>
                      ) : (
                        <dl className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
                          {Object.entries(detail.profile?.props ?? {}).map(
                            ([k, v]) => (
                              <div
                                key={k}
                                className="rounded-lg border border-line bg-panel px-2.5 py-1.5"
                              >
                                <dt className="font-mono text-[11px] text-muted">
                                  {k}
                                </dt>
                                <dd className="truncate text-xs font-medium text-ink">
                                  {formatPropValue(v)}
                                </dd>
                              </div>
                            ),
                          )}
                        </dl>
                      )}
                    </div>

                    <div>
                      <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
                        Replay Sessions
                      </h4>
                      {detail.replay_sessions.length === 0 ? (
                        <p className="text-xs text-muted">
                          No session recordings linked to this user.
                        </p>
                      ) : (
                        <div className="flex flex-wrap gap-1.5">
                          {detail.replay_sessions.map((sid) => (
                            <span
                              key={sid}
                              className="inline-flex items-center rounded-md border border-line bg-panel px-2.5 py-1 font-mono text-xs text-ink"
                            >
                              {sid}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>

                  <div>
                    <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
                      Recent Events ({formatCount(detail.events.length)})
                    </h4>
                    {detail.events.length === 0 ? (
                      <p className="text-xs text-muted">
                        No events stored for this user.
                      </p>
                    ) : (
                      <div className="max-h-60 overflow-y-auto rounded-lg border border-line bg-panel">
                        <table className="w-full text-left text-xs">
                          <thead className="sticky top-0 border-b border-line bg-panel text-muted">
                            <tr>
                              <th className="px-3 py-2">Event</th>
                              <th className="px-3 py-2">Session</th>
                              <th className="px-3 py-2">Properties</th>
                              <th className="px-3 py-2 text-right">Time</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-line">
                            {detail.events.slice(0, 20).map((ev, idx) => (
                              <tr key={`${ev.event_ts_ms}-${idx}`}>
                                <td className="px-3 py-2 font-medium text-ink">
                                  {humanizeEvent(ev.event_name)}
                                </td>
                                <td className="px-3 py-2 font-mono text-muted">
                                  {ev.session_id || "—"}
                                </td>
                                <td className="max-w-xs truncate px-3 py-2 font-mono text-[11px] text-ink-2">
                                  {JSON.stringify(ev.props)}
                                </td>
                                <td className="px-3 py-2 text-right text-muted">
                                  {formatTimestamp(ev.event_ts_ms)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </>
              )
            )}
          </div>
        )}

        <div>
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
            DPDP Erasure Audit Log
          </h3>
          {deletions.length === 0 ? (
            <p className="text-xs text-muted">
              No DPDP erasure requests recorded yet.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-line text-muted">
                    <th className="py-2 pr-3">Audit ID</th>
                    <th className="py-2 pr-3">Masked User</th>
                    <th className="py-2 pr-3 text-right">Events Purged</th>
                    <th className="py-2 pr-3 text-right">Replays Purged</th>
                    <th className="py-2 pr-3">Requested By</th>
                    <th className="py-2 text-right">Erased At</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {deletions.map((d) => (
                    <tr key={d.id}>
                      <td className="py-2 pr-3 font-mono text-ink-2">{d.id}</td>
                      <td className="py-2 pr-3 font-mono text-ink">
                        {d.distinct_id_masked}
                      </td>
                      <td className="py-2 pr-3 text-right tabular-nums text-ink">
                        {formatCount(d.events_purged)}
                      </td>
                      <td className="py-2 pr-3 text-right tabular-nums text-ink">
                        {formatCount(d.replays_purged)}
                      </td>
                      <td className="py-2 pr-3 text-ink-2">{d.requested_by}</td>
                      <td className="py-2 text-right text-muted">
                        {formatTimestamp(d.deleted_at_ms)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </Panel>
  );
};
