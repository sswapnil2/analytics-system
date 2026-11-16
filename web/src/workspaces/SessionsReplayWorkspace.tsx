import React, { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, RefreshCw, Search, Zap } from "lucide-react";
import { api, SessionSearchResult, SessionSummaryItem } from "../api/client";
import { SessionReplay } from "../components/WireframePlayer";
import { btn, EmptyState, ErrorNotice, field, fieldLabel, PageHeader, Panel, Spinner } from "../components/ui";
import { displayUserId, errorMessage, formatCount, formatDuration, humanizeEvent } from "../lib/format";

interface Props {
  projectId: string;
  availableEvents: string[];
  refreshToken: number;
}

const Toggle: React.FC<{
  pressed: boolean;
  onChange: () => void;
  icon: React.ComponentType<{ className?: string }>;
  tone: "danger" | "warn";
  children: React.ReactNode;
}> = ({ pressed, onChange, icon: Icon, tone, children }) => (
  <button
    type="button"
    aria-pressed={pressed}
    onClick={onChange}
    className={`inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-sm ${
      pressed
        ? tone === "danger"
          ? "border-danger bg-danger-soft text-danger"
          : "border-warn bg-warn-soft text-warn"
        : "border-line-strong bg-panel text-ink-2 hover:bg-hover"
    }`}
  >
    <Icon className="h-4 w-4" />
    {children}
  </button>
);

export const SessionsReplayWorkspace: React.FC<Props> = ({ projectId, availableEvents, refreshToken }) => {
  const [eventFilter, setEventFilter] = useState<string>("NSDL_Gateway_504_Timeout");
  const [onlyRage, setOnlyRage] = useState<boolean>(true);
  const [onlyErrors, setOnlyErrors] = useState<boolean>(false);
  const [searchRes, setSearchRes] = useState<SessionSearchResult | null>(null);
  const [selected, setSelected] = useState<SessionSummaryItem | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const eventOptions = useMemo(
    () =>
      Array.from(new Set(["NSDL_Gateway_504_Timeout", "KYC_PAN_Submitted", ...availableEvents])).filter(
        (e) => !e.startsWith("$"),
      ),
    [availableEvents],
  );

  const runSearch = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.searchSessions(projectId, {
        event_name: eventFilter || null,
        only_rage_tap: onlyRage,
        only_errors: onlyErrors,
        limit: 30,
      });
      setSearchRes(res);
      setSelected(res.sessions[0] ?? null);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [projectId, eventFilter, onlyRage, onlyErrors]);

  useEffect(() => {
    void runSearch();
  }, [runSearch, refreshToken]);

  const sessions = searchRes?.sessions ?? [];

  return (
    <div className="space-y-5">
      <PageHeader
        title="Session replays"
        description="Watch what users saw and tapped, starting with the sessions where something went wrong."
      />

      <Panel>
        <div className="flex flex-wrap items-end gap-3">
          <label className="flex min-w-[14rem] flex-col gap-1">
            <span className={fieldLabel}>Sessions that include</span>
            <select value={eventFilter} onChange={(e) => setEventFilter(e.target.value)} className={field}>
              <option value="">Any event</option>
              {eventOptions.map((ev) => (
                <option key={ev} value={ev}>
                  {humanizeEvent(ev)}
                </option>
              ))}
            </select>
          </label>
          <Toggle pressed={onlyRage} onChange={() => setOnlyRage((v) => !v)} icon={Zap} tone="danger">
            Rage taps
          </Toggle>
          <Toggle pressed={onlyErrors} onChange={() => setOnlyErrors((v) => !v)} icon={AlertTriangle} tone="warn">
            Errors and timeouts
          </Toggle>
          <button type="button" onClick={() => void runSearch()} disabled={loading} className={btn.secondary}>
            <RefreshCw className={`h-4 w-4 ${loading ? "motion-safe:animate-spin" : ""}`} />
            Refresh
          </button>
        </div>
      </Panel>

      {error && <ErrorNotice message={`Couldn't search sessions: ${error}`} onRetry={() => void runSearch()} />}

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-12">
        <Panel
          title="Sessions"
          meta={
            searchRes ? `${formatCount(sessions.length)} found` : undefined
          }
          className="xl:col-span-4"
        >
          {loading && !searchRes ? (
            <Spinner label="Searching" />
          ) : sessions.length === 0 ? (
            <EmptyState
              icon={Search}
              title="No sessions match"
              body="Turn off a filter or choose a different event."
            />
          ) : (
            <ul className="-m-4 max-h-[640px] divide-y divide-line overflow-y-auto" aria-label="Matching sessions">
              {sessions.map((s) => {
                const active = selected?.session_id === s.session_id;
                return (
                  <li key={s.session_id}>
                    <button
                      type="button"
                      onClick={() => setSelected(s)}
                      aria-current={active ? "true" : undefined}
                      className={`w-full border-l-2 px-4 py-3 text-left ${
                        active ? "border-brand bg-brand-soft" : "border-transparent hover:bg-hover"
                      }`}
                    >
                      <span className="flex items-center justify-between gap-2">
                        <span className="truncate text-sm font-medium text-ink tabular">
                          {displayUserId(s.distinct_id)}
                        </span>
                        <span className="flex shrink-0 gap-1">
                          {s.has_rage_tap && (
                            <span className="rounded bg-danger-soft px-1.5 py-0.5 text-xs font-medium text-danger">
                              Rage taps
                            </span>
                          )}
                          {s.has_error && (
                            <span className="rounded bg-warn-soft px-1.5 py-0.5 text-xs font-medium text-warn">
                              Error
                            </span>
                          )}
                        </span>
                      </span>
                      <span className="mt-1 block text-xs text-muted tabular">
                        {new Date(s.start_ts_ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })}
                        {", "}
                        {formatDuration(s.duration_ms)}, {formatCount(s.event_count)} events
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </Panel>

        <div className="space-y-5 xl:col-span-8">
          <Panel title="Recording">
            {selected ? (
              <SessionReplay key={selected.session_id} projectId={projectId} sessionId={selected.session_id} />
            ) : (
              <EmptyState title="Pick a session" body="Its screen recording plays here." />
            )}
          </Panel>

          {selected && (
            <Panel title="Events in this session" meta={formatCount(selected.events.length)}>
              <ol className="space-y-1.5">
                {selected.events.map(([ts, evName], idx) => {
                  const bad = /504|timeout|error|fail/i.test(evName);
                  return (
                    <li key={idx} className="flex items-baseline gap-3 text-sm">
                      <span className="w-20 shrink-0 text-muted tabular">
                        {new Date(ts).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                      </span>
                      <span className={bad ? "font-medium text-danger" : "text-ink"}>{humanizeEvent(evName)}</span>
                    </li>
                  );
                })}
              </ol>
            </Panel>
          )}
        </div>
      </div>
    </div>
  );
};
