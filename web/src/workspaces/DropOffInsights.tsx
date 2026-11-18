import React, { useEffect, useState } from "react";
import { UsersRound } from "lucide-react";
import { api, UserEventItem } from "../api/client";
import { useAccess } from "../auth/access";
import { btn, Spinner } from "../components/ui";
import { errorMessage, formatCount, humanizeEvent } from "../lib/format";

/** The funnel query behind the report, with its time range and global filter. */
export interface FunnelQueryBody {
  time_range: { start_ts_ms: number; end_ts_ms: number };
  global_filter?: { predicates: unknown[]; cohort: { cohort_id: string } | null };
  [k: string]: unknown;
}

const pct = (n: number) => `${Math.round(n * 100)}%`;
const nodeName = (e: string) => (e === "$Drop_Off" ? "No further events" : e === "$Other" ? "Other events" : humanizeEvent(e));

/** What people did right after `fromEvent` when it wasn't `toEvent`. */
function useNextInstead(projectId: string, q: FunnelQueryBody, fromEvent: string, toEvent: string) {
  const [rows, setRows] = useState<{ name: string; share: number; users: number }[] | null>(null);
  useEffect(() => {
    let alive = true;
    api
      .queryFlows(projectId, { time_range: q.time_range, anchor_event: fromEvent, direction: "forward", steps: 1, global_filter: q.global_filter })
      .then((res) => {
        const next = res.nodes.filter((n) => n.step_offset === 1 && n.event_name !== toEvent && n.event_name !== fromEvent);
        const total = next.reduce((s, n) => s + n.user_count, 0) || 1;
        const sorted = next.sort((a, b) => b.user_count - a.user_count).slice(0, 5);
        if (alive) setRows(sorted.map((n) => ({ name: nodeName(n.event_name), users: n.user_count, share: n.user_count / total })));
      })
      .catch(() => alive && setRows([]));
    return () => {
      alive = false;
    };
  }, [projectId, q, fromEvent, toEvent]);
  return rows;
}

/**
 * Segments that continue past this step noticeably less often than everyone,
 * found by re-running the funnel split by each property.
 * ponytail: one funnel query per property (first 8); add a server-side "compare" query if this gets slow.
 */
function useWorstSegments(projectId: string, q: FunnelQueryBody, step: number, overallRate: number, props: string[]) {
  const [rows, setRows] = useState<{ prop: string; value: string; rate: number; users: number }[] | null>(null);
  useEffect(() => {
    let alive = true;
    const candidates = props.filter((p) => !p.includes(".")).slice(0, 8);
    Promise.all(
      candidates.map((prop) =>
        api
          .queryFunnel(projectId, { ...q, breakdown_by: prop })
          .then((res) => {
            if (res.breakdowns.length > 25) return []; // ids, prices: too many values to mean anything
            return res.breakdowns.flatMap((g) => {
              const before = g.steps[step - 1]?.entered_users ?? 0;
              const after = g.steps[step]?.entered_users ?? 0;
              return before >= 10 && g.breakdown_value && g.breakdown_value !== "(none)"
                ? [{ prop, value: g.breakdown_value, rate: after / before, users: before }]
                : [];
            });
          })
          .catch(() => []),
      ),
    ).then((all) => {
      const worse = all
        .flat()
        .filter((r) => overallRate - r.rate >= 0.05)
        .sort((a, b) => (overallRate - b.rate) * b.users - (overallRate - a.rate) * a.users)
        .slice(0, 4);
      if (alive) setRows(worse);
    });
    return () => {
      alive = false;
    };
  }, [projectId, q, step, overallRate, props]);
  return rows;
}

/** "Why they left": where they went instead, who drops most, and saving them as a cohort. */
export const WhyTheyLeft: React.FC<{
  projectId: string;
  query: FunnelQueryBody;
  step: number;
  fromEvent: string;
  toEvent: string;
  overallRate: number;
  availableProps: string[];
}> = ({ projectId, query, step, fromEvent, toEvent, overallRate, availableProps }) => {
  const { canEdit } = useAccess();
  const instead = useNextInstead(projectId, query, fromEvent, toEvent);
  const worst = useWorstSegments(projectId, query, step, overallRate, availableProps);
  const [saved, setSaved] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const saveCohort = async () => {
    const name = `Did ${humanizeEvent(fromEvent)}, not ${humanizeEvent(toEvent)}`;
    setSaving(true);
    try {
      const range = query.time_range;
      // The server saves the exact drop-off list (step filters, window and order included).
      await api.cohortFromFunnelDropped(
        projectId,
        query,
        step,
        name,
        `Dropped off between ${humanizeEvent(fromEvent)} and ${humanizeEvent(toEvent)}, ${new Date(range.start_ts_ms).toLocaleDateString()} – ${new Date(range.end_ts_ms).toLocaleDateString()}. A fixed list saved ${new Date().toLocaleDateString()}.`,
      );
      setSaved(`Saved "${name}". Use it from the Filter panel on any report.`);
    } catch (err) {
      setSaved(`Couldn't save the cohort: ${errorMessage(err)}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <section aria-label="Why they left" className="grid grid-cols-1 gap-4 border-b border-line bg-panel px-5 py-4 lg:grid-cols-2">
      <div>
        <h3 className="text-sm font-semibold text-ink">What they did next instead</h3>
        {instead === null ? (
          <Spinner label="Loading" />
        ) : instead.length === 0 ? (
          <p className="mt-1 text-sm text-muted">Nothing else recorded after {humanizeEvent(fromEvent)}.</p>
        ) : (
          <ul className="mt-2 space-y-1.5">
            {instead.map((r) => (
              <li key={r.name} className="text-sm">
                <div className="flex justify-between gap-2">
                  <span className="truncate text-ink-2">{r.name}</span>
                  <span className="tabular text-muted">{pct(r.share)}</span>
                </div>
                <div className="mt-0.5 h-1.5 rounded bg-sunken">
                  <div className="h-1.5 rounded bg-warn" style={{ width: pct(r.share) }} />
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
      <div>
        <h3 className="text-sm font-semibold text-ink">Who drops most here</h3>
        <p className="text-xs text-muted">{pct(overallRate)} of everyone continue to {humanizeEvent(toEvent)}.</p>
        {worst === null ? (
          <Spinner label="Comparing segments" />
        ) : worst.length === 0 ? (
          <p className="mt-1 text-sm text-muted">No segment drops noticeably more than average.</p>
        ) : (
          <ul className="mt-2 space-y-1.5">
            {worst.map((r) => (
              <li key={`${r.prop}=${r.value}`} className="flex items-baseline justify-between gap-2 text-sm">
                <span className="min-w-0 truncate text-ink-2">
                  {humanizeEvent(r.prop)} is <span className="font-semibold text-ink">{r.value}</span>
                </span>
                <span className="shrink-0 tabular text-danger">
                  {pct(r.rate)} <span className="text-xs text-muted">of {formatCount(r.users)}</span>
                </span>
              </li>
            ))}
          </ul>
        )}
        {canEdit && (
          <div className="mt-3 space-y-1">
            <button type="button" onClick={() => void saveCohort()} disabled={saving} className={btn.secondary}>
              <UsersRound className="h-3.5 w-3.5" />
              {saving ? "Saving…" : "Save these users as a cohort"}
            </button>
            {saved && (
              <p role="status" className="text-xs text-muted">
                {saved}
              </p>
            )}
          </div>
        )}
      </div>
    </section>
  );
};

/** A dropped user's events around the moment they left, for when there's no recording. */
export const UserTimeline: React.FC<{
  projectId: string;
  distinctId: string;
  canonicalUid: number;
  dropTs: number;
  fromEvent: string;
}> = ({
  projectId,
  distinctId,
  canonicalUid,
  dropTs,
  fromEvent,
}) => {
  const [events, setEvents] = useState<UserEventItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    api
      .getUserDetail(projectId, distinctId, canonicalUid)
      .then((d) => alive && setEvents(d.events))
      .catch((err) => alive && setError(errorMessage(err)));
    return () => {
      alive = false;
    };
  }, [projectId, distinctId, canonicalUid]);

  if (error) return <p className="text-sm text-muted">Couldn't load this user's events: {error}</p>;
  if (!events) return <Spinner label="Loading events" />;
  // The hour before they left and the hour after.
  const near = events
    .filter((e) => Math.abs(e.event_ts_ms - dropTs) <= 3_600_000)
    .sort((a, b) => a.event_ts_ms - b.event_ts_ms);
  const shown = near.length > 0 ? near : [...events].sort((a, b) => b.event_ts_ms - a.event_ts_ms).slice(0, 15).reverse();
  if (shown.length === 0) return <p className="text-sm text-muted">No recording and no stored events for this user.</p>;
  return (
    <div className="space-y-2">
      <p className="text-sm text-muted">No recording for this session, so here's what they did around the time they left.</p>
      <ol className="divide-y divide-line rounded-lg border border-line bg-panel">
        {shown.map((e, i) => {
          const left = e.event_name === fromEvent && Math.abs(e.event_ts_ms - dropTs) < 1000;
          return (
            <li key={i} className={`flex items-baseline gap-3 px-3 py-2 text-sm ${left ? "bg-danger-soft" : ""}`}>
              <span className="w-16 shrink-0 tabular text-xs text-muted">
                {new Date(e.event_ts_ms).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}
              </span>
              <span className="min-w-0 flex-1 truncate text-ink">{humanizeEvent(e.event_name)}</span>
              <span className="hidden min-w-0 max-w-[50%] truncate text-xs text-faint sm:block">
                {Object.entries(e.props)
                  .filter(([, v]) => v !== null && v !== "")
                  .slice(0, 3)
                  .map(([k, v]) => `${humanizeEvent(k)}: ${String(v)}`)
                  .join(" · ")}
              </span>
              {left && <span className="shrink-0 text-xs font-semibold text-danger">Left here</span>}
            </li>
          );
        })}
      </ol>
    </div>
  );
};
