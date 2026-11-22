import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, Bot, Check, ChevronDown, ChevronRight, ChevronUp, CircleX, Copy, Link2, PlayCircle, Plus, Search, Trash2, X } from "lucide-react";
import {
  AgentAnalytics,
  AgentBreakdownRow,
  AgentDataset,
  AgentEvaluation,
  AgentEvaluator,
  AgentExperiment,
  AgentSessionRow,
  AgentSpan,
  AgentTraceDetail,
  AgentTraceQuery,
  AgentTraceRow,
  api,
  isAbortError,
} from "../api/client";
import { useAccess } from "../auth/access";
import { EmptyState, ErrorNotice, Kbd, ListSkeleton, PageHeader, Panel, Segmented, Tabs, btn, field, fieldLabel } from "../components/ui";
import { niceTicks } from "../lib/chart";
import { formatScore, formatUsd, orderSpanTree, toChatMessages } from "../lib/agentTrace";
import { displayUserId, errorMessage, formatCount, formatDate, formatDateTime, formatDuration, formatPercent } from "../lib/format";

type Tab = "overview" | "traces" | "sessions" | "evaluations" | "datasets" | "experiments";
const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "traces", label: "Traces" },
  { id: "sessions", label: "Sessions" },
  { id: "evaluations", label: "Evaluations" },
  { id: "datasets", label: "Datasets" },
  { id: "experiments", label: "Experiments" },
];
const RANGES = [
  { days: 1, label: "24h" },
  { days: 7, label: "7D" },
  { days: 30, label: "30D" },
  { days: 90, label: "90D" },
];

// ---------------------------------------------------------------------------
// URL state: `#/agents?tab=traces&trace=<id>&range=7&p=<project>` so views can be shared.
// replaceState doesn't fire `hashchange`, so the app router isn't re-run on every change.

const hashParams = () => new URLSearchParams(window.location.hash.split("?")[1] ?? "");
function setHashParams(update: Record<string, string | null>) {
  const [path] = window.location.hash.split("?");
  const p = hashParams();
  for (const [k, v] of Object.entries(update)) (v ? p.set(k, v) : p.delete(k));
  window.history.replaceState(null, "", `${path}?${p.toString()}`);
}

/** Run-list filters. They live in the URL so filtered views can be shared and the Overview can link into them. */
interface Filters {
  q: string;
  agent: string;
  version: string;
  env: string;
  model: string;
  failed: boolean;
  looped: boolean;
  sort: string;
}
const NO_FILTERS: Filters = { q: "", agent: "", version: "", env: "", model: "", failed: false, looped: false, sort: "newest" };
const filterParams = (f: Filters): Record<string, string | null> => ({
  q: f.q || null,
  agent: f.agent || null,
  version: f.version || null,
  env: f.env || null,
  model: f.model || null,
  failed: f.failed ? "1" : null,
  looped: f.looped ? "1" : null,
  sort: f.sort === "newest" ? null : f.sort,
});

function readHash(): { tab: Tab; trace: string | null; days: number; filters: Filters } {
  const p = hashParams();
  const trace = p.get("trace");
  const t = p.get("tab") as Tab | null;
  const d = Number(p.get("range"));
  return {
    tab: t && TABS.some((x) => x.id === t) ? t : trace ? "traces" : "overview",
    trace,
    days: RANGES.some((r) => r.days === d) ? d : 7,
    filters: {
      q: p.get("q") ?? "",
      agent: p.get("agent") ?? "",
      version: p.get("version") ?? "",
      env: p.get("env") ?? "",
      model: p.get("model") ?? "",
      failed: p.get("failed") === "1",
      looped: p.get("looped") === "1",
      sort: p.get("sort") ?? "newest",
    },
  };
}

/** Loads on dependency change, aborting the previous request. */
function useLoad<T>(load: (signal: AbortSignal) => Promise<T>, deps: unknown[]) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    const ctrl = new AbortController();
    setError(null);
    load(ctrl.signal)
      .then((d) => !ctrl.signal.aborted && setData(d))
      .catch((e) => !isAbortError(e) && !ctrl.signal.aborted && setError(errorMessage(e)));
    return () => ctrl.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);
  return { data, error, reload: () => setNonce((n) => n + 1) };
}

const replayHref = (projectId: string, sessionId: string) =>
  `#/replays?session=${encodeURIComponent(sessionId)}&p=${encodeURIComponent(projectId)}`;

const StatusBadge: React.FC<{ status: string }> = ({ status }) => {
  const ok = status === "ok";
  const running = status === "running";
  return (
    <span
      className={`whitespace-nowrap rounded px-1.5 py-0.5 text-xs font-medium ${
        ok
          ? "bg-ok-soft text-ok"
          : running
            ? "bg-sunken text-muted"
            : status === "timeout" || status === "cancelled"
              ? "bg-warn-soft text-warn"
              : "bg-danger-soft text-danger"
      }`}
    >
      {ok ? "Succeeded" : running ? "Running" : status === "timeout" ? "Timed out" : status === "cancelled" ? "Cancelled" : "Failed"}
    </span>
  );
};

const ResultBadge: React.FC<{ ev: AgentEvaluation }> = ({ ev }) =>
  ev.error ? (
    <span className="whitespace-nowrap rounded bg-sunken px-1.5 py-0.5 text-xs font-medium text-muted">Couldn't run</span>
  ) : ev.passed === false ? (
    <span className="whitespace-nowrap rounded bg-danger-soft px-1.5 py-0.5 text-xs font-medium text-danger">Failed</span>
  ) : (
    <span className="whitespace-nowrap rounded bg-ok-soft px-1.5 py-0.5 text-xs font-medium text-ok">Passed</span>
  );

const th = "py-2 pr-4 text-left font-medium";
const thR = "py-2 pl-4 text-right font-medium";
const td = "py-2.5 pr-4 text-ink-2";
const tdR = "tabular py-2.5 pl-4 text-right text-ink-2";

const Table: React.FC<{ head: React.ReactNode; children: React.ReactNode; minWidth?: number }> = ({
  head,
  children,
  minWidth = 640,
}) => (
  <div className="-mx-5 overflow-x-auto px-5">
    <table className="w-full text-sm" style={{ minWidth }}>
      <thead>
        <tr className="border-b border-line text-xs text-muted">{head}</tr>
      </thead>
      <tbody className="divide-y divide-line">{children}</tbody>
    </table>
  </div>
);

/** Small inline status line for write actions. */
const useAction = () => {
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const run = async (fn: () => Promise<unknown>, okText: string) => {
    setBusy(true);
    setMsg(null);
    try {
      await fn();
      setMsg({ ok: true, text: okText });
      return true;
    } catch (e) {
      setMsg({ ok: false, text: errorMessage(e) });
      return false;
    } finally {
      setBusy(false);
    }
  };
  const note = msg && (
    <span role="status" className={`text-sm ${msg.ok ? "text-ok" : "text-danger"}`}>
      {msg.text}
    </span>
  );
  return { busy, run, note };
};

// ---------------------------------------------------------------------------

export const AgentObservabilityWorkspace: React.FC<{ projectId: string; refreshToken: number }> = ({
  projectId,
  refreshToken,
}) => {
  const [tab, setTabState] = useState<Tab>(() => readHash().tab);
  const [openTrace, setOpenTraceState] = useState<string | null>(() => readHash().trace);
  const [days, setDaysState] = useState<number>(() => readHash().days);
  const [filters, setFiltersState] = useState<Filters>(() => readHash().filters);
  // Pasted links and back/forward change the hash without remounting; follow them.
  useEffect(() => {
    const sync = () => {
      const h = readHash();
      setTabState(h.tab);
      setOpenTraceState(h.trace);
      setDaysState(h.days);
      setFiltersState(h.filters);
    };
    window.addEventListener("hashchange", sync);
    return () => window.removeEventListener("hashchange", sync);
  }, []);
  // Rounded to the minute so re-renders don't refetch with a new timestamp.
  const startTs = useMemo(() => Math.floor((Date.now() - days * 86_400_000) / 60_000) * 60_000, [days, refreshToken]);

  const setTab = (t: Tab) => {
    setTabState(t);
    // The open run only belongs in links to the Traces tab.
    setHashParams({ tab: t === "overview" ? null : t, trace: t === "traces" ? openTrace : null });
  };
  const setOpenTrace = useCallback((id: string | null) => {
    setOpenTraceState(id);
    setHashParams({ trace: id, step: null });
  }, []);
  const setDays = (d: number) => {
    setDaysState(d);
    setHashParams({ range: d === 7 ? null : String(d) });
  };
  const setFilters = useCallback((patch: Partial<Filters>) => {
    setFiltersState((f) => {
      const next = { ...f, ...patch };
      setHashParams(filterParams(next));
      return next;
    });
  }, []);
  /** From the Overview: open the run list with exactly these filters. */
  const drill = (patch: Partial<Filters>) => {
    const next = { ...NO_FILTERS, ...patch };
    setFiltersState(next);
    setTabState("traces");
    setOpenTraceState(null);
    setHashParams({ tab: "traces", trace: null, ...filterParams(next) });
    window.scrollTo({ top: 0 });
  };
  const showTrace = (id: string) => {
    setTabState("traces");
    setOpenTraceState(id);
    setHashParams({ tab: "traces", trace: id });
  };

  const analytics = useLoad((s) => api.getAgentAnalytics(projectId, startTs, s), [projectId, startTs]);
  // "Is anything set up at all" is asked without a time range, so an old project with a
  // quiet week sees an empty period, not the first-run setup guide.
  const ever = useLoad((s) => api.listAgentTraces(projectId, {}, s), [projectId, refreshToken]);

  if (analytics.error) return <ErrorNotice message={analytics.error} onRetry={analytics.reload} />;
  if (!analytics.data || !ever.data) return <ListSkeleton rows={6} />;

  return (
    <div className="space-y-5">
      <PageHeader
        title="AI agents"
        description="How your LLM agents behave in production: every run, the steps inside it, what it cost, and how it scored."
        actions={
          ever.data.total > 0 && (
            <Segmented label="Time range" options={RANGES.map((r) => ({ value: r.days, label: r.label }))} value={days} onChange={setDays} />
          )
        }
      />
      {ever.data.total === 0 ? (
        <SetupGuide />
      ) : (
        <>
          <Tabs label="AI agent views" tabs={TABS} value={tab} onChange={setTab} panelId="agents-panel" />
          <div role="tabpanel" id="agents-panel" aria-labelledby={`agents-panel-tab-${tab}`}>
          {tab === "overview" && <Overview a={analytics.data} onDrill={drill} />}
          {tab === "traces" && (
            <Traces
              projectId={projectId}
              startTs={startTs}
              analytics={analytics.data}
              openTrace={openTrace}
              setOpenTrace={setOpenTrace}
              filters={filters}
              setFilters={setFilters}
            />
          )}
          {tab === "sessions" && <Sessions projectId={projectId} refreshToken={refreshToken} />}
          {tab === "evaluations" && (
            <Evaluations projectId={projectId} refreshToken={refreshToken} agents={analytics.data.by_agent} onTrace={showTrace} />
          )}
          {tab === "datasets" && <Datasets projectId={projectId} refreshToken={refreshToken} />}
          {tab === "experiments" && <Experiments projectId={projectId} refreshToken={refreshToken} />}
          </div>
        </>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------

const SetupGuide: React.FC = () => {
  const origin = window.location.origin;
  const now = Date.now();
  const curl = `curl -X POST ${origin}/v1/agent/traces \\
  -H "content-type: application/json" -H "x-pulse-key: <INGEST_KEY>" \\
  -d '{"traces":[{"trace_id":"tr_1","agent_name":"support-agent","start_ts_ms":${now},"duration_ms":900,"status":"ok"}],
       "spans":[{"span_id":"sp_1","trace_id":"tr_1","name":"answer","span_type":"llm",
                 "model":"gpt-4o-mini","start_ts_ms":${now},"duration_ms":800,
                 "input_tokens":900,"output_tokens":120}]}'`;
  return (
    <Panel>
      <EmptyState
        icon={Bot}
        title="No agent runs in this project yet"
        body="Send a trace for each agent run and its LLM calls, tool calls and retrieval steps show up here, with cost, latency and quality scores."
      />
      <div className="mx-auto max-w-3xl space-y-4 text-sm text-ink-2">
        <ol className="list-decimal space-y-1.5 pl-5">
          <li>
            Use the agent SDK (<code className="text-ink">sdks/ts-agent</code> or <code className="text-ink">sdks/py-agent</code>), or
            any OpenTelemetry exporter pointed at <code className="text-ink">{origin}/v1/otel/v1/traces</code>. GenAI and
            OpenInference attributes are mapped automatically.
          </li>
          <li>Authenticate with this project's ingest key, the same one your app uses for events.</li>
          <li>Or try it by hand:</li>
        </ol>
        <pre className="overflow-x-auto rounded-lg bg-sunken p-3 text-xs text-ink">{curl}</pre>
      </div>
    </Panel>
  );
};

// ---------------------------------------------------------------------------

const Kpi: React.FC<{ label: string; value: string; sub?: string; onClick: () => void; hint: string }> = ({
  label,
  value,
  sub,
  onClick,
  hint,
}) => (
  <button
    type="button"
    onClick={onClick}
    title={hint}
    className="rounded-xl border border-line bg-panel px-4 py-3 text-left hover:border-line-strong hover:bg-hover"
  >
    <p className="text-xs text-muted">{label}</p>
    <p className="tabular mt-1 text-xl font-semibold text-ink">{value}</p>
    <p className="mt-0.5 text-xs text-faint tabular">{sub ?? hint}</p>
  </button>
);

const Overview: React.FC<{ a: AgentAnalytics; onDrill: (f: Partial<Filters>) => void }> = ({ a, onDrill }) => {
  const [hover, setHover] = useState<number | null>(null);
  if (a.total_executions === 0) {
    return (
      <Panel>
        <EmptyState title="No runs in this period" body="Choose a longer time range above to see earlier runs." />
      </Panel>
    );
  }
  // At least 3 so a quiet week gets whole-number ticks.
  const ticks = niceTicks(Math.max(3, ...a.timeseries.map((b) => b.executions)), 3);
  const top = ticks[ticks.length - 1];
  const labelEvery = Math.ceil(a.timeseries.length / 10);
  const sources = Object.entries(a.failure_sources ?? {}).sort((x, y) => y[1] - x[1]);
  const hb = hover === null ? null : a.timeseries[hover];
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Kpi label="Runs" value={formatCount(a.total_executions)} sub={`${formatCount(a.failed_executions)} failed`} hint="View all runs" onClick={() => onDrill({})} />
        <Kpi label="Success rate" value={formatPercent(a.success_rate)} hint="View failed runs" onClick={() => onDrill({ failed: true })} />
        <Kpi label="p95 latency" value={formatDuration(a.latency.p95_ms)} sub={`median ${formatDuration(a.latency.p50_ms)}`} hint="View slowest runs" onClick={() => onDrill({ sort: "slowest" })} />
        <Kpi label="Cost" value={formatUsd(a.total_cost_usd)} sub={`${formatUsd(a.cost_per_execution_usd)} per run`} hint="View most expensive runs" onClick={() => onDrill({ sort: "costliest" })} />
        {a.loop_warning_count ? (
          <Kpi
            label="Eval pass rate"
            value={a.eval_pass_rate == null ? "–" : formatPercent(a.eval_pass_rate)}
            sub={`${formatCount(a.loop_warning_count)} runs looped`}
            hint="View runs that looped"
            onClick={() => onDrill({ looped: true })}
          />
        ) : (
          <Kpi
            label="Eval pass rate"
            value={a.eval_pass_rate == null ? "–" : formatPercent(a.eval_pass_rate)}
            hint="View lowest-scoring runs"
            onClick={() => onDrill({ sort: "worst" })}
          />
        )}
      </div>

      <div className="grid gap-5 lg:grid-cols-3">
        <Panel
          title="Runs per day"
          className="lg:col-span-2"
          meta={
            hb
              ? `${formatDate(hb.bucket_ts_ms)}: ${formatCount(hb.executions)} runs, ${formatCount(hb.errors)} failed, p95 ${formatDuration(hb.p95_latency_ms)}`
              : "Point at a day for details"
          }
        >
          {/* Value axis on the left, one focusable column per day, failed runs hatched on top. */}
          <div className="flex gap-2">
            <div className="tabular flex h-40 w-8 shrink-0 flex-col-reverse justify-between text-right text-2xs text-faint" aria-hidden="true">
              {ticks.map((v) => (
                <span key={v} className="leading-none">{formatCount(v)}</span>
              ))}
            </div>
            <div className="min-w-0 flex-1">
              <div className="relative h-40" onMouseLeave={() => setHover(null)}>
                <div className="absolute inset-0 flex flex-col-reverse justify-between" aria-hidden="true">
                  {ticks.map((v) => (
                    <span key={v} className="h-px bg-line" />
                  ))}
                </div>
                <ul className="relative flex h-full items-end gap-1" aria-label="Agent runs per day">
                  {a.timeseries.map((b, i) => (
                    <li
                      key={b.bucket_ts_ms}
                      tabIndex={0}
                      className="flex h-full flex-1 flex-col justify-end rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-brand"
                      onMouseEnter={() => setHover(i)}
                      onFocus={() => setHover(i)}
                      onBlur={() => setHover(null)}
                      aria-label={`${formatDate(b.bucket_ts_ms)}: ${b.executions} runs, ${b.errors} failed`}
                    >
                      <div
                        className={`flex w-full flex-col overflow-hidden rounded-t transition-[height,opacity] duration-300 ease-out ${hover !== null && hover !== i ? "opacity-50" : ""}`}
                        style={{ height: `${b.executions ? Math.max(2, (b.executions / top) * 100) : 0}%` }}
                      >
                        {b.errors > 0 && <div className="hatch-drop-strong shrink-0" style={{ height: `${(b.errors / b.executions) * 100}%`, minHeight: 3 }} />}
                        <div className="flex-1 bg-brand" />
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
              <div className="tabular mt-1.5 flex gap-1 text-2xs text-faint" aria-hidden="true">
                {a.timeseries.map((b, i) => (
                  <span key={b.bucket_ts_ms} className="flex-1 truncate text-center">
                    {i % labelEvery === 0 ? formatDate(b.bucket_ts_ms, { day: "numeric", month: "short", year: undefined }) : ""}
                  </span>
                ))}
              </div>
            </div>
          </div>
          <div className="mt-3 flex items-center gap-4 text-xs text-muted">
            <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-sm bg-brand" aria-hidden="true" />Succeeded</span>
            <span className="inline-flex items-center gap-1.5"><span className="hatch-drop-strong h-2.5 w-2.5 rounded-sm" aria-hidden="true" />Failed</span>
          </div>
        </Panel>
        <Panel
          title="Where runs fail"
          actions={
            sources.length > 0 && (
              <button type="button" className="text-sm font-medium text-brand hover:underline" onClick={() => onDrill({ failed: true })}>
                View failed runs
              </button>
            )
          }
        >
          {sources.length === 0 ? (
            <p className="text-sm text-muted">No failed runs in this period.</p>
          ) : (
            <ul className="space-y-2 text-sm">
              {sources.map(([k, n]) => (
                <li key={k} className="flex justify-between gap-2">
                  <span className="capitalize text-ink-2">{k}</span>
                  <span className="tabular text-ink">{formatCount(n)}</span>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>

      <Breakdown
        title="By version"
        rows={a.by_version}
        keyLabel="Version"
        onRow={(r) => {
          const [agent, version] = r.key.split("@");
          onDrill({ agent, version: version ?? "" });
        }}
      />
      <Breakdown
        title="By model"
        meta="Runs that called each model. Tokens and cost are that model's calls."
        rows={a.by_model}
        keyLabel="Model"
        onRow={(r) => onDrill({ model: r.key })}
      />
      <Panel title="Tools">
        {a.by_tool.length === 0 ? (
          <p className="text-sm text-muted">No tool calls recorded.</p>
        ) : (
          <Table
            head={
              <>
                <th className={th}>Tool</th>
                <th className={thR}>Calls</th>
                <th className={thR}>Failure rate</th>
                <th className={thR}>Retries</th>
                <th className={thR}>p95 latency</th>
              </>
            }
          >
            {a.by_tool.map((t) => (
              <tr key={t.tool_name}>
                <td className={`${td} font-medium text-ink`}>{t.tool_name}</td>
                <td className={tdR}>{formatCount(t.invocations)}</td>
                <td className={tdR}>{formatPercent(t.failure_rate)}</td>
                <td className={tdR}>{formatCount(t.total_retries)}</td>
                <td className={tdR}>{formatDuration(t.p95_latency_ms)}</td>
              </tr>
            ))}
          </Table>
        )}
      </Panel>
    </div>
  );
};

const Breakdown: React.FC<{
  title: string;
  meta?: string;
  rows: AgentBreakdownRow[];
  keyLabel: string;
  onRow: (r: AgentBreakdownRow) => void;
}> = ({ title, meta, rows, keyLabel, onRow }) => (
  <Panel title={title} meta={meta}>
    <Table
      head={
        <>
          <th className={th}>{keyLabel}</th>
          <th className={thR}>Runs</th>
          <th className={thR}>Failure rate</th>
          <th className={thR}>p95 latency</th>
          <th className={thR}>Tokens</th>
          <th className={thR}>Cost</th>
          <th className={thR}>Eval score</th>
        </>
      }
    >
      {rows.map((r) => (
        <tr key={`${r.key}|${r.secondary ?? ""}`} onClick={() => onRow(r)} className="cursor-pointer hover:bg-hover">
          <td className={`${td} font-medium text-ink`}>
            <button type="button" className="text-left hover:underline" onClick={(e) => { e.stopPropagation(); onRow(r); }}>
              {r.key}
            </button>
            {r.secondary && <span className="ml-1.5 text-xs font-normal text-faint">{r.secondary}</span>}
          </td>
          <td className={tdR}>{formatCount(r.executions)}</td>
          <td className={tdR}>{formatPercent(r.error_rate)}</td>
          <td className={tdR}>{formatDuration(r.p95_latency_ms)}</td>
          <td className={tdR}>{formatCount(r.total_tokens)}</td>
          <td className={tdR}>{formatUsd(r.total_cost_usd)}</td>
          <td className={tdR}>{formatScore(r.avg_eval_score)}</td>
        </tr>
      ))}
    </Table>
  </Panel>
);

// ---------------------------------------------------------------------------

const SORTS: { id: string; label: string; by: string; dir: "asc" | "desc" }[] = [
  { id: "newest", label: "Newest first", by: "start_ts_ms", dir: "desc" },
  { id: "slowest", label: "Slowest first", by: "duration_ms", dir: "desc" },
  { id: "costliest", label: "Most expensive first", by: "total_cost_usd", dir: "desc" },
  { id: "tokens", label: "Most tokens first", by: "total_tokens", dir: "desc" },
  { id: "worst", label: "Lowest eval score first", by: "avg_eval_score", dir: "asc" },
];

const Traces: React.FC<{
  projectId: string;
  startTs: number;
  analytics: AgentAnalytics;
  openTrace: string | null;
  setOpenTrace: (id: string | null) => void;
  filters: Filters;
  setFilters: (patch: Partial<Filters>) => void;
}> = ({ projectId, startTs, analytics, openTrace, setOpenTrace, filters, setFilters }) => {
  const { q: search, agent, version, env, model, failed: onlyFailed, looped, sort } = filters;
  const [searchDraft, setSearchDraft] = useState(search);
  useEffect(() => setSearchDraft(search), [search]);
  const [rows, setRows] = useState<AgentTraceRow[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Version keys come back as "agent@version"; only offer versions of the chosen agent.
  const versions = analytics.by_version
    .map((r) => r.key.split("@"))
    .filter(([a, v]) => v && (!agent || a === agent))
    .map(([, v]) => v);
  const filtered = Boolean(search || agent || version || env || model || onlyFailed || looped);

  const load = useCallback(
    async (from: string | null, signal?: AbortSignal) => {
      setLoading(true);
      setError(null);
      const s = SORTS.find((x) => x.id === sort) ?? SORTS[0];
      const q: AgentTraceQuery = {
        status: onlyFailed ? "error" : undefined,
        agent_name: agent || undefined,
        agent_version: version || undefined,
        environment: env || undefined,
        model: model || undefined,
        has_loop_warning: looped || undefined,
        search: search || undefined,
        sort_by: s.by,
        sort_dir: s.dir,
        start_ts_ms: startTs,
        cursor: from,
      };
      try {
        const res = await api.listAgentTraces(projectId, q, signal);
        setRows((prev) => (from ? [...prev, ...res.traces] : res.traces));
        setCursor(res.next_cursor);
        setTotal(res.total);
      } catch (e) {
        if (!isAbortError(e)) setError(errorMessage(e));
      } finally {
        if (!signal?.aborted) setLoading(false);
      }
    },
    [projectId, startTs, onlyFailed, agent, version, env, model, looped, search, sort],
  );

  useEffect(() => {
    const ctrl = new AbortController();
    void load(null, ctrl.signal);
    return () => ctrl.abort();
  }, [load]);

  const clear = () => setFilters({ ...NO_FILTERS, sort });
  const openIdx = rows.findIndex((r) => r.trace_id === openTrace);

  return (
    <div className="space-y-5">
      {openTrace && (
        <TraceView
          projectId={projectId}
          traceId={openTrace}
          onClose={() => setOpenTrace(null)}
          onPrev={openIdx > 0 ? () => setOpenTrace(rows[openIdx - 1].trace_id) : undefined}
          onNext={openIdx >= 0 && openIdx < rows.length - 1 ? () => setOpenTrace(rows[openIdx + 1].trace_id) : undefined}
        />
      )}
      <Panel title="Runs" meta={`${formatCount(total)} ${filtered ? "matching" : "in this period"}`}>
        <div className="mb-4 flex flex-wrap items-end gap-2">
          <form
            className="relative min-w-[220px] flex-1"
            onSubmit={(e) => {
              e.preventDefault();
              setFilters({ q: searchDraft.trim() });
            }}
          >
            <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-faint" />
            <input
              type="search"
              aria-label="Search runs"
              placeholder="Search error type, model, agent, run id"
              className={`${field} w-full pl-8`}
              value={searchDraft}
              onChange={(e) => {
                setSearchDraft(e.target.value);
                if (!e.target.value) setFilters({ q: "" });
              }}
            />
          </form>
          <select aria-label="Agent" className={field} value={agent} onChange={(e) => setFilters({ agent: e.target.value, version: "" })}>
            <option value="">All agents</option>
            {analytics.by_agent.map((r) => (
              <option key={r.key} value={r.key}>{r.key}</option>
            ))}
          </select>
          <select aria-label="Version" className={field} value={version} onChange={(e) => setFilters({ version: e.target.value })}>
            <option value="">All versions</option>
            {[...new Set(versions)].map((v) => (
              <option key={v} value={v}>{v}</option>
            ))}
          </select>
          {analytics.by_environment.length > 1 && (
            <select aria-label="Environment" className={field} value={env} onChange={(e) => setFilters({ env: e.target.value })}>
              <option value="">All environments</option>
              {analytics.by_environment.map((r) => (
                <option key={r.key} value={r.key}>{r.key}</option>
              ))}
            </select>
          )}
          {analytics.by_model.length > 1 && (
            <select aria-label="Model" className={field} value={model} onChange={(e) => setFilters({ model: e.target.value })}>
              <option value="">All models</option>
              {analytics.by_model.map((r) => (
                <option key={r.key} value={r.key}>{r.key}</option>
              ))}
            </select>
          )}
          <select aria-label="Sort runs" className={field} value={sort} onChange={(e) => setFilters({ sort: e.target.value })}>
            {SORTS.map((s) => (
              <option key={s.id} value={s.id}>{s.label}</option>
            ))}
          </select>
          <label className="flex items-center gap-2 px-1 py-2 text-sm text-ink-2">
            <input type="checkbox" checked={onlyFailed} onChange={(e) => setFilters({ failed: e.target.checked })} />
            Failed only
          </label>
          {(looped || analytics.loop_warning_count > 0) && (
            <label className="flex items-center gap-2 px-1 py-2 text-sm text-ink-2">
              <input type="checkbox" checked={looped} onChange={(e) => setFilters({ looped: e.target.checked })} />
              Looped only
            </label>
          )}
          {filtered && (
            <button type="button" className={btn.ghost} onClick={clear}>
              Clear filters
            </button>
          )}
        </div>
        {error ? (
          <ErrorNotice message={error} onRetry={() => void load(null)} />
        ) : !loading && rows.length === 0 ? (
          <EmptyState
            title={filtered ? "No runs match these filters" : "No runs in this period"}
            body={filtered ? "Loosen a filter or clear them." : "Choose a longer time range at the top of the page."}
            action={filtered ? <button type="button" className={btn.secondary} onClick={clear}>Clear filters</button> : undefined}
          />
        ) : (
          <>
            <Table
              minWidth={880}
              head={
                <>
                  <th className={th}>Started</th>
                  <th className={th}>Agent</th>
                  <th className={th}>Result</th>
                  <th className={th}>Model</th>
                  <th className={thR}>Duration</th>
                  <th className={thR}>Tokens</th>
                  <th className={thR}>Cost</th>
                  <th className={thR}>Eval</th>
                </>
              }
            >
              {rows.map((t) => (
                <tr
                  key={t.trace_id}
                  onClick={() => setOpenTrace(t.trace_id)}
                  className={`cursor-pointer hover:bg-hover ${openTrace === t.trace_id ? "bg-brand-soft" : ""}`}
                >
                  <td className={`${td} tabular whitespace-nowrap`}>
                    <button
                      type="button"
                      className="text-left hover:underline"
                      onClick={(e) => {
                        e.stopPropagation();
                        setOpenTrace(t.trace_id);
                      }}
                    >
                      {formatDateTime(t.start_ts_ms, { dateStyle: "medium", timeStyle: "short" })}
                    </button>
                  </td>
                  <td className={td}>
                    <span className="font-medium text-ink">{t.agent_name}</span>
                    <span className="ml-1.5 text-xs text-faint">{t.agent_version}</span>
                  </td>
                  <td className={td}>
                    <StatusBadge status={t.status} />
                    {t.has_loop_warning && <span className="ml-1.5 text-xs text-warn">Looped</span>}
                  </td>
                  <td className={td}>{t.primary_model ?? "–"}</td>
                  <td className={tdR}>{formatDuration(t.duration_ms)}</td>
                  <td className={tdR}>{formatCount(t.total_tokens)}</td>
                  <td className={tdR}>{formatUsd(t.total_cost_usd)}</td>
                  <td className={tdR}>{formatScore(t.avg_eval_score)}</td>
                </tr>
              ))}
            </Table>
            {cursor && (
              <div className="mt-4 flex justify-center">
                <button type="button" className={btn.secondary} disabled={loading} onClick={() => void load(cursor)}>
                  {loading ? "Loading…" : "Load more"}
                </button>
              </div>
            )}
          </>
        )}
      </Panel>
    </div>
  );
};

// ---------------------------------------------------------------------------

const CopyButton: React.FC<{ text: () => string; label: string }> = ({ text, label }) => {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      aria-label={label}
      className="inline-flex items-center gap-1 text-xs font-normal text-muted hover:text-ink"
      onClick={() => {
        void navigator.clipboard?.writeText(text()).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1500);
        });
      }}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
      {copied ? "Copied" : "Copy"}
    </button>
  );
};

const Json: React.FC<{ value: unknown }> = ({ value }) =>
  value === null || value === undefined ? (
    <p className="text-xs text-faint">Not captured</p>
  ) : (
    <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-sunken p-3 font-mono text-xs text-ink">
      {typeof value === "string" ? value : JSON.stringify(value, null, 2)}
    </pre>
  );

/** Chat messages as a conversation when the payload is chat-shaped, raw JSON otherwise. */
const Payload: React.FC<{ label: string; value: unknown }> = ({ label, value }) => {
  const [raw, setRaw] = useState(false);
  const msgs = useMemo(() => toChatMessages(value), [value]);
  const has = value !== null && value !== undefined;
  return (
    <div>
      <div className="mb-1 flex items-center justify-between gap-2">
        <p className="text-xs font-semibold text-ink-2">{label}</p>
        <span className="flex items-center gap-3">
          {msgs && (
            <button type="button" className="text-xs text-muted hover:text-ink" aria-pressed={raw} onClick={() => setRaw(!raw)}>
              {raw ? "Conversation" : "Raw JSON"}
            </button>
          )}
          {has && (
            <CopyButton
              label={`Copy ${label.toLowerCase()}`}
              text={() => (typeof value === "string" ? value : JSON.stringify(value, null, 2))}
            />
          )}
        </span>
      </div>
      {!msgs || raw ? (
        <Json value={value} />
      ) : (
        <ol className="max-h-80 space-y-2 overflow-auto">
          {msgs.map((m, i) => (
            <li key={i} className={`rounded-lg px-3 py-2 text-sm ${m.role === "assistant" ? "bg-brand-soft" : "bg-sunken"}`}>
              <p className="mb-0.5 text-2xs font-semibold uppercase tracking-wide text-muted">{m.role}</p>
              <p className="whitespace-pre-wrap break-words text-ink">{m.text || <span className="text-faint">(empty)</span>}</p>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
};

const typing = (e: KeyboardEvent) =>
  e.metaKey || e.ctrlKey || e.altKey || /^(INPUT|TEXTAREA|SELECT)$/.test((e.target as HTMLElement)?.tagName ?? "") ||
  (e.target as HTMLElement)?.isContentEditable || document.querySelector('[role="dialog"]') !== null;

// Step name and waterfall share these columns in the axis row and every step row.
const STEP_GRID = "grid grid-cols-[minmax(0,1fr)_3.75rem] items-center gap-x-3 md:grid-cols-[minmax(0,5fr)_minmax(0,4fr)_3.75rem]";

/**
 * One run: the step tree with a waterfall on the left, the selected step on the right.
 * The deepest failed step is selected on open, so the cause is on screen without a click.
 * Keys: ↑/↓ or J/K move, ←/→ collapse and expand, [ and ] change run, Esc closes.
 * Design notes: docs/design/trace-view-decision.md.
 */
const TraceView: React.FC<{
  projectId: string;
  traceId: string;
  onClose: () => void;
  onPrev?: () => void;
  onNext?: () => void;
}> = ({ projectId, traceId, onClose, onPrev, onNext }) => {
  const { canEdit } = useAccess();
  const { data, error, reload } = useLoad<AgentTraceDetail>((s) => api.getAgentTrace(projectId, traceId, s), [projectId, traceId]);
  const [sel, setSelState] = useState<string | null>(() => hashParams().get("step"));
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [panel, setPanel] = useState<"dataset" | "review" | null>(null);
  const action = useAction();
  const [copied, setCopied] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const treeRef = useRef<HTMLDivElement>(null);

  const setSel = useCallback((id: string | null) => {
    setSelState(id);
    setHashParams({ step: id });
  }, []);

  const loaded = data && data.trace.trace_id === traceId ? data : null;
  const rows = useMemo(() => (loaded ? orderSpanTree(loaded.spans) : []), [loaded]);
  // Rows under a collapsed step are hidden; `kids` counts everything beneath each step.
  const { visible, kids } = useMemo(() => {
    const kids = new Map<string, number>();
    const stack: { id: string; depth: number }[] = [];
    const visible: typeof rows = [];
    let hideBelow = Infinity;
    for (const r of rows) {
      while (stack.length && stack[stack.length - 1].depth >= r.depth) stack.pop();
      for (const a of stack) kids.set(a.id, (kids.get(a.id) ?? 0) + 1);
      stack.push({ id: r.span.span_id, depth: r.depth });
      if (r.depth <= hideBelow) {
        hideBelow = collapsed.has(r.span.span_id) ? r.depth : Infinity;
        visible.push(r);
      }
    }
    return { visible, kids };
  }, [rows, collapsed]);
  // The cause is usually the deepest failed step: its parents only failed because it did.
  const culprit = useMemo(() => {
    const failed = rows.filter((r) => r.span.status === "error");
    return failed.length ? failed.reduce((a, b) => (b.depth > a.depth ? b : a)).span.span_id : null;
  }, [rows]);

  // A newly opened run starts on its failed step (or its first step) and scrolls into view.
  useEffect(() => {
    if (!loaded) return;
    setCollapsed(new Set());
    setPanel(null);
    setSelState((cur) => {
      const next = cur && rows.some((r) => r.span.span_id === cur) ? cur : (culprit ?? rows[0]?.span.span_id ?? null);
      if (next !== cur) setHashParams({ step: next });
      return next;
    });
    rootRef.current?.scrollIntoView({ block: "nearest" });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loaded?.trace.trace_id]);
  // Closing the run drops its step from the link.
  useEffect(() => () => setHashParams({ step: null }), []);

  const move = useCallback(
    (delta: number, focus: boolean) => {
      if (!visible.length) return;
      const i = visible.findIndex((r) => r.span.span_id === sel);
      const next = visible[Math.max(0, Math.min(visible.length - 1, (i < 0 ? 0 : i) + delta))].span.span_id;
      setSel(next);
      const el = treeRef.current?.querySelector<HTMLElement>(`[data-span="${CSS.escape(next)}"]`);
      el?.scrollIntoView({ block: "nearest" });
      if (focus) el?.focus();
    },
    [visible, sel, setSel],
  );
  const toggle = (id: string, open?: boolean) =>
    setCollapsed((c) => {
      const n = new Set(c);
      if (open ?? c.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (typing(e)) return;
      if (e.key === "j") move(1, false);
      else if (e.key === "k") move(-1, false);
      else if (e.key === "[" && onPrev) onPrev();
      else if (e.key === "]" && onNext) onNext();
      else if (e.key === "Escape") onClose();
      else return;
      e.preventDefault();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [move, onPrev, onNext, onClose]);

  const onTreeKey = (e: React.KeyboardEvent) => {
    const cur = visible.find((r) => r.span.span_id === sel);
    if (e.key === "ArrowDown") move(1, true);
    else if (e.key === "ArrowUp") move(-1, true);
    else if (e.key === "Home") move(-visible.length, true);
    else if (e.key === "End") move(visible.length, true);
    else if (e.key === "ArrowLeft" && cur && kids.get(cur.span.span_id) && !collapsed.has(cur.span.span_id)) toggle(cur.span.span_id, false);
    else if (e.key === "ArrowRight" && cur && collapsed.has(cur.span.span_id)) toggle(cur.span.span_id, true);
    else return;
    e.preventDefault();
  };

  const nav = (
    <>
      <button type="button" className={btn.ghost} onClick={onPrev} disabled={!onPrev} aria-label="Previous run" title="Previous run  [" aria-keyshortcuts="[">
        <ChevronUp className={`h-4 w-4 ${onPrev ? "" : "opacity-40"}`} />
      </button>
      <button type="button" className={btn.ghost} onClick={onNext} disabled={!onNext} aria-label="Next run" title="Next run  ]" aria-keyshortcuts="]">
        <ChevronDown className={`h-4 w-4 ${onNext ? "" : "opacity-40"}`} />
      </button>
      <button type="button" className={btn.ghost} onClick={onClose} aria-label="Close run" title="Close  Esc" aria-keyshortcuts="Escape">
        <X className="h-4 w-4" />
      </button>
    </>
  );
  if (error) return <div ref={rootRef}><Panel title="Run" actions={nav}><ErrorNotice message={error} onRetry={reload} /></Panel></div>;
  if (!loaded) return <div ref={rootRef}><Panel title="Run" actions={nav}><ListSkeleton rows={4} /></Panel></div>;

  const t = loaded.trace;
  const t0 = Math.min(t.start_ts_ms, ...loaded.spans.map((s) => s.start_ts_ms));
  const total = Math.max(1, ...loaded.spans.map((s) => s.start_ts_ms + s.duration_ms - t0), t.duration_ms);
  const selected = loaded.spans.find((s) => s.span_id === sel) ?? null;
  const culpritName = loaded.spans.find((s) => s.span_id === culprit)?.name;
  // Re-running evaluators appends results; show the newest per evaluator and step.
  const seenEval = new Set<string>();
  const evals = loaded.evaluations.filter((ev) => {
    const k = `${ev.evaluator_id}|${ev.span_id ?? ""}`;
    return seenEval.has(k) ? false : (seenEval.add(k), true);
  });
  const runEvalsList = evals.filter((ev) => !ev.span_id);
  const failedEvalSteps = new Set(evals.filter((ev) => ev.span_id && ev.passed === false).map((ev) => ev.span_id));

  const runEvals = () =>
    action
      .run(() => api.runAgentTraceEvals(projectId, traceId), "Evaluators queued. Results appear in a few seconds.")
      .then((ok) => ok && window.setTimeout(reload, 2500));

  const Stat: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
    <span className="flex flex-col">
      <span className="text-xs text-muted">{label}</span>
      <span className="tabular text-sm text-ink">{children}</span>
    </span>
  );

  return (
    <div ref={rootRef} className="scroll-mt-4">
    <Panel
      title={`${t.agent_name} ${t.agent_version}`}
      meta={formatDateTime(t.start_ts_ms, { dateStyle: "medium", timeStyle: "medium" })}
      actions={
        <>
          <button
            type="button"
            className={btn.ghost}
            onClick={() => {
              void navigator.clipboard?.writeText(window.location.href).then(() => {
                setCopied(true);
                window.setTimeout(() => setCopied(false), 1500);
              });
            }}
          >
            {copied ? <Check className="h-4 w-4" /> : <Link2 className="h-4 w-4" />}
            <span className="sr-only sm:not-sr-only">{copied ? "Copied" : "Copy link"}</span>
          </button>
          {t.session_id && (
            <a className={btn.ghost} href={replayHref(projectId, t.session_id)}>
              <PlayCircle className="h-4 w-4" />
              <span className="sr-only sm:not-sr-only">Session replay</span>
            </a>
          )}
          {nav}
        </>
      }
    >
      <div className="mb-4 flex flex-wrap items-center gap-x-6 gap-y-3">
        <StatusBadge status={t.status} />
        <Stat label="Duration">{formatDuration(t.duration_ms)}</Stat>
        <Stat label="Tokens">{formatCount(t.total_tokens)}</Stat>
        <Stat label="Cost">{formatUsd(t.total_cost_usd)}</Stat>
        <Stat label="Steps">{formatCount(loaded.spans.length)}</Stat>
        {t.user_id && <Stat label="User">{displayUserId(t.user_id)}</Stat>}
        <span className="flex flex-col">
          <span className="text-xs text-muted">Run id</span>
          <span className="font-mono text-xs leading-5 text-ink-2">{t.trace_id}</span>
        </span>
      </div>

      {t.error_message && (
        <button
          type="button"
          disabled={!culprit}
          onClick={() => culprit && setSel(culprit)}
          className="mb-4 flex w-full items-start gap-2 rounded-lg bg-danger-soft px-3 py-2 text-left text-sm text-ink enabled:hover:ring-1 enabled:hover:ring-danger/40"
        >
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-danger" />
          <span className="min-w-0 flex-1 break-words">
            {t.error_type && <span className="font-semibold">{t.error_type}: </span>}
            {t.error_message}
          </span>
          {culpritName && sel !== culprit && <span className="shrink-0 font-medium text-danger">Show {culpritName}</span>}
        </button>
      )}

      <div className="grid rounded-lg border border-line lg:grid-cols-[minmax(0,1fr)_minmax(0,26rem)]">
        <div className="min-w-0 border-b border-line lg:border-b-0 lg:border-r">
          <div className={`${STEP_GRID} border-b border-line px-3 py-1.5 text-2xs text-muted`}>
            <span>Step</span>
            <span className="tabular hidden justify-between md:flex" aria-hidden="true">
              {[0, 0.25, 0.5, 0.75, 1].map((f) => (
                <span key={f}>{f === 0 ? "0" : formatDuration(total * f)}</span>
              ))}
            </span>
            <span className="text-right">Duration</span>
          </div>
          <div ref={treeRef} role="tree" aria-label="Steps in this run" onKeyDown={onTreeKey} className="divide-y divide-line">
            {visible.map(({ span: s, depth }) => (
              <SpanRow
                key={s.span_id}
                s={s}
                depth={depth}
                left={((s.start_ts_ms - t0) / total) * 100}
                width={(s.duration_ms / total) * 100}
                selected={sel === s.span_id}
                children_={kids.get(s.span_id) ?? 0}
                collapsed={collapsed.has(s.span_id)}
                evalFailed={failedEvalSteps.has(s.span_id)}
                onSelect={() => setSel(s.span_id)}
                onToggle={() => toggle(s.span_id)}
              />
            ))}
          </div>
          <p className="hidden items-center gap-x-4 gap-y-1 border-t border-line px-3 py-2 text-xs text-muted lg:flex lg:flex-wrap">
            <span><Kbd>J</Kbd> <Kbd>K</Kbd> step</span>
            <span><Kbd>←</Kbd> <Kbd>→</Kbd> collapse, expand</span>
            <span><Kbd>[</Kbd> <Kbd>]</Kbd> run</span>
            <span><Kbd>Esc</Kbd> close</span>
          </p>
        </div>
        <div className="min-w-0 lg:sticky lg:top-4 lg:max-h-[calc(100vh-2rem)] lg:self-start lg:overflow-y-auto" aria-live="polite">
          {selected ? (
            <SpanInspector
              s={selected}
              startOffset={selected.start_ts_ms - t0}
              share={selected.duration_ms / total}
              evals={evals.filter((ev) => ev.span_id === selected.span_id)}
            />
          ) : (
            <p className="p-4 text-sm text-muted">This run recorded no steps.</p>
          )}
        </div>
      </div>

      <div className="mt-5">
        <h3 className="mb-2 text-sm font-semibold text-ink">Run evaluations</h3>
        {evals.length === 0 ? (
          <p className="text-sm text-muted">Not evaluated yet.{canEdit ? " Run evaluators or add a review below." : ""}</p>
        ) : runEvalsList.length === 0 ? (
          <p className="text-sm text-muted">Only individual steps were scored. Select a step to see its result.</p>
        ) : (
          <EvalList evals={runEvalsList} />
        )}
      </div>

      {canEdit && (
        <div className="mt-4 space-y-3 border-t border-line pt-4">
          <div className="flex flex-wrap items-center gap-2">
            <button type="button" className={btn.secondary} disabled={action.busy} onClick={() => void runEvals()}>
              Run evaluators
            </button>
            <button
              type="button"
              className={btn.secondary}
              aria-expanded={panel === "dataset"}
              onClick={() => setPanel(panel === "dataset" ? null : "dataset")}
            >
              Add to dataset
            </button>
            <button
              type="button"
              className={btn.secondary}
              aria-expanded={panel === "review"}
              onClick={() => setPanel(panel === "review" ? null : "review")}
            >
              Review
            </button>
            {action.note}
          </div>
          {panel === "dataset" && (
            <AddToDataset projectId={projectId} traceId={traceId} onDone={() => setPanel(null)} />
          )}
          {panel === "review" && (
            <ReviewForm
              onSubmit={(passed, note) =>
                action
                  .run(() => api.reviewAgentTrace(projectId, t, passed, note), "Review saved.")
                  .then((ok) => {
                    if (ok) {
                      setPanel(null);
                      reload();
                    }
                  })
              }
              busy={action.busy}
            />
          )}
        </div>
      )}
    </Panel>
    </div>
  );
};

const EvalList: React.FC<{ evals: AgentEvaluation[] }> = ({ evals }) => (
  <ul className="space-y-2">
    {evals.map((ev) => (
      <li key={ev.id} className="flex flex-wrap items-baseline gap-2 text-sm">
        <ResultBadge ev={ev} />
        <span className="font-medium text-ink">{ev.evaluator_name}</span>
        {(ev.error ?? ev.reasoning) && <span className="min-w-0 break-words text-ink-2">{ev.error ?? ev.reasoning}</span>}
      </li>
    ))}
  </ul>
);

const SPAN_KIND: Record<string, string> = { llm: "Model call", tool: "Tool call", retrieval: "Retrieval", agent: "Agent" };

/** Everything about the selected step: what failed, what it cost, what went in and came out. */
const SpanInspector: React.FC<{ s: AgentSpan; startOffset: number; share: number; evals: AgentEvaluation[] }> = ({
  s,
  startOffset,
  share,
  evals,
}) => {
  const Fact: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
    <div className="min-w-0">
      <dt className="text-xs text-muted">{label}</dt>
      <dd className="tabular truncate text-sm text-ink">{children}</dd>
    </div>
  );
  return (
    <div className="space-y-4 p-4 text-sm">
      <div>
        <h3 className="break-words text-md font-semibold text-ink">{s.name}</h3>
        <p className="text-xs text-muted">
          {SPAN_KIND[s.span_type] ?? s.span_type} · starts at {startOffset <= 0 ? "0 ms" : formatDuration(startOffset)} ·{" "}
          {formatPercent(share, 0)} of the run
        </p>
      </div>
      {s.status === "error" && (
        <p role="alert" className="flex items-start gap-2 rounded-lg bg-danger-soft px-3 py-2 text-ink">
          <CircleX className="mt-0.5 h-4 w-4 shrink-0 text-danger" />
          <span className="min-w-0 break-words">
            {s.error_type && <span className="font-semibold">{s.error_type}: </span>}
            {s.error_message ?? "This step failed without an error message."}
          </span>
        </p>
      )}
      <dl className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-3">
        <Fact label="Duration">{formatDuration(s.duration_ms)}</Fact>
        {s.model && <Fact label="Model">{s.model}</Fact>}
        {s.model && (
          <Fact label="Tokens in / out">
            {formatCount(s.input_tokens)} / {formatCount(s.output_tokens)}
          </Fact>
        )}
        {s.cost_usd > 0 && <Fact label="Cost">{formatUsd(s.cost_usd)}</Fact>}
        {s.tool_name && <Fact label="Tool">{s.tool_name}</Fact>}
        {s.tool_retry_count > 0 && <Fact label="Retries">{formatCount(s.tool_retry_count)}</Fact>}
      </dl>
      <Payload label="Input" value={s.input} />
      <Payload label="Output" value={s.output} />
      {evals.length > 0 && (
        <div>
          <p className="mb-1.5 text-xs font-semibold text-ink-2">Evaluations on this step</p>
          <EvalList evals={evals} />
        </div>
      )}
    </div>
  );
};

const AddToDataset: React.FC<{ projectId: string; traceId: string; onDone: () => void }> = ({ projectId, traceId, onDone }) => {
  const { data, error, reload } = useLoad((s) => api.listAgentDatasets(projectId, s), [projectId]);
  const [choice, setChoice] = useState("");
  const [newName, setNewName] = useState("");
  const action = useAction();
  if (error) return <ErrorNotice message={error} onRetry={reload} />;
  if (!data) return <p className="text-sm text-muted">Loading datasets…</p>;
  const creating = choice === "__new" || data.datasets.length === 0;
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const ok = await action.run(async () => {
      let id = choice;
      if (creating) id = (await api.createAgentDataset(projectId, newName.trim(), "")).id;
      await api.addTraceToDataset(projectId, traceId, id);
    }, "Added to the dataset.");
    if (ok) window.setTimeout(onDone, 900);
  };
  return (
    <form className="flex flex-wrap items-end gap-2" onSubmit={(e) => void submit(e)}>
      {data.datasets.length > 0 && (
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Dataset</span>
          <select className={field} value={choice} onChange={(e) => setChoice(e.target.value)} required>
            <option value="" disabled>Choose…</option>
            {data.datasets.map((d: AgentDataset) => (
              <option key={d.id} value={d.id}>{d.name} ({formatCount(d.item_count)} {d.item_count === 1 ? "case" : "cases"})</option>
            ))}
            <option value="__new">New dataset…</option>
          </select>
        </label>
      )}
      {creating && (
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>New dataset name</span>
          <input className={field} value={newName} onChange={(e) => setNewName(e.target.value)} required maxLength={120} placeholder="e.g. Support regressions" />
        </label>
      )}
      <button type="submit" className={btn.primary} disabled={action.busy}>Add run</button>
      {action.note}
    </form>
  );
};

const ReviewForm: React.FC<{ onSubmit: (passed: boolean, note: string) => void; busy: boolean }> = ({ onSubmit, busy }) => {
  const [note, setNote] = useState("");
  return (
    <div className="flex flex-wrap items-end gap-2">
      <label className="flex min-w-[260px] flex-1 flex-col gap-1">
        <span className={fieldLabel}>Note (optional)</span>
        <input className={field} value={note} onChange={(e) => setNote(e.target.value)} maxLength={2000} placeholder="What was right or wrong about this run?" />
      </label>
      <button type="button" className={btn.secondary} disabled={busy} onClick={() => onSubmit(true, note)}>
        <Check className="h-4 w-4 text-ok" /> Good run
      </button>
      <button type="button" className={btn.secondary} disabled={busy} onClick={() => onSubmit(false, note)}>
        <X className="h-4 w-4 text-danger" /> Bad run
      </button>
    </div>
  );
};

const SpanRow: React.FC<{
  s: AgentSpan;
  depth: number;
  left: number;
  width: number;
  selected: boolean;
  /** Steps nested under this one, at any depth. */
  children_: number;
  collapsed: boolean;
  evalFailed: boolean;
  onSelect: () => void;
  onToggle: () => void;
}> = ({ s, depth, left, width, selected, children_, collapsed, evalFailed, onSelect, onToggle }) => {
  const failed = s.status === "error";
  const tokens = s.input_tokens + s.output_tokens;
  return (
    <div
      role="treeitem"
      data-span={s.span_id}
      aria-level={depth + 1}
      aria-selected={selected}
      aria-expanded={children_ ? !collapsed : undefined}
      tabIndex={selected ? 0 : -1}
      onClick={onSelect}
      className={`${STEP_GRID} cursor-pointer gap-y-1 px-3 py-2 outline-none transition-colors duration-100 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand ${
        selected ? "bg-brand-soft shadow-[inset_2px_0_0_rgb(var(--brand))]" : "hover:bg-hover"
      }`}
    >
      <span className="flex min-w-0 items-center gap-1.5 text-sm" style={{ paddingLeft: Math.min(depth, 12) * 14 }}>
        {children_ ? (
          <button
            type="button"
            tabIndex={-1}
            aria-label={collapsed ? `Expand ${s.name}` : `Collapse ${s.name}`}
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            className="-m-1 rounded p-1 text-faint hover:bg-hover hover:text-ink"
          >
            <ChevronRight className={`h-3.5 w-3.5 transition-transform duration-150 ${collapsed ? "" : "rotate-90"}`} />
          </button>
        ) : (
          <span className="w-3.5 shrink-0" />
        )}
        <span className={`shrink-0 rounded px-1.5 py-0.5 text-2xs uppercase tracking-wide text-muted ${selected ? "bg-panel" : "bg-sunken"}`}>{s.span_type}</span>
        <span className={`truncate font-medium ${failed ? "text-danger" : "text-ink"}`}>{s.name}</span>
        {failed && <CircleX className="h-3.5 w-3.5 shrink-0 text-danger" role="img" aria-label="Failed" />}
        {!failed && evalFailed && <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-warn" role="img" aria-label="Failed an evaluation" />}
        {collapsed && children_ > 0 && <span className="shrink-0 text-xs text-muted">+{children_}</span>}
        {tokens > 0 && <span className="tabular ml-auto hidden shrink-0 pl-2 text-xs text-muted sm:inline">{formatCount(tokens)} tok</span>}
      </span>
      <span
        className="order-last col-span-2 h-2 rounded md:order-none md:col-span-1"
        aria-hidden="true"
        // Quarter gridlines that line up with the axis labels above.
        style={{
          backgroundColor: selected ? "rgb(var(--panel))" : "rgb(var(--sunken))",
          backgroundImage: "linear-gradient(to right, rgb(var(--line-strong)) 1px, transparent 1px)",
          backgroundSize: "25% 100%",
        }}
      >
        <span
          className={`block h-2 rounded ${failed ? "bg-danger" : "bg-brand"}`}
          style={{ marginLeft: `${Math.min(99, left)}%`, width: `${Math.max(1, Math.min(100 - left, width))}%` }}
        />
      </span>
      <span className="tabular text-right text-xs text-muted">{formatDuration(s.duration_ms)}</span>
    </div>
  );
};

// ---------------------------------------------------------------------------

const Sessions: React.FC<{ projectId: string; refreshToken: number }> = ({ projectId, refreshToken }) => {
  const { data, error, reload } = useLoad((s) => api.listAgentSessions(projectId, 100, s), [projectId, refreshToken]);
  if (error) return <ErrorNotice message={error} onRetry={reload} />;
  if (!data) return <ListSkeleton rows={6} />;
  return (
    <Panel title="Sessions" meta={`${formatCount(data.total)} total`}>
      <p className="mb-3 text-sm text-muted">
        Runs that share a session id. When your app records sessions with the Pulse SDK using the same id, open the screen
        recording to see what the user did around the conversation.
      </p>
      <Table
        minWidth={820}
        head={
          <>
            <th className={th}>Last activity</th>
            <th className={th}>User</th>
            <th className={thR}>Runs</th>
            <th className={thR}>Failed</th>
            <th className={thR}>Cost</th>
            <th className={thR}>Eval</th>
            <th className={thR}>Replays</th>
          </>
        }
      >
        {data.sessions.map((s: AgentSessionRow) => (
          <tr key={s.session_id}>
            <td className={`${td} tabular`}>{formatDateTime(s.last_ts_ms, { dateStyle: "medium", timeStyle: "short" })}</td>
            <td className={td}>{s.user_id ? displayUserId(s.user_id) : "–"}</td>
            <td className={tdR}>{formatCount(s.trace_count)}</td>
            <td className={tdR}>{s.error_count ? <span className="text-warn">{formatCount(s.error_count)}</span> : "0"}</td>
            <td className={tdR}>{formatUsd(s.total_cost_usd)}</td>
            <td className={tdR}>{formatScore(s.avg_eval_score)}</td>
            <td className="py-2.5 pl-4 text-right">
              <a className="text-sm font-medium text-brand hover:underline" href={replayHref(projectId, s.session_id)}>
                Open replay
              </a>
            </td>
          </tr>
        ))}
      </Table>
    </Panel>
  );
};

// ---------------------------------------------------------------------------

/** Built-in checks and the one setting each needs (config key on the server). */
const CHECKS: { kind: string; label: string; help: string; param?: { key: string; label: string; type: "number" | "text" | "list" } }[] = [
  { kind: "no_errors", label: "No errors", help: "Fails when the run or any step errored." },
  { kind: "no_loop", label: "No tool loops", help: "Fails when the agent repeats the same tool call in a loop." },
  { kind: "output_not_empty", label: "Answer not empty", help: "Fails when the output is missing or blank." },
  { kind: "latency_budget", label: "Latency budget", help: "Fails when the run takes longer than the budget.", param: { key: "max_ms", label: "Max milliseconds", type: "number" } },
  { kind: "cost_budget", label: "Cost budget", help: "Fails when the run costs more than the budget.", param: { key: "max_usd", label: "Max USD", type: "number" } },
  { kind: "token_budget", label: "Token budget", help: "Fails when the run uses more tokens.", param: { key: "max_tokens", label: "Max tokens", type: "number" } },
  { kind: "contains_keywords", label: "Contains keywords", help: "Passes when the output mentions every keyword.", param: { key: "keywords", label: "Keywords, comma separated", type: "list" } },
  { kind: "regex_match", label: "Matches a pattern", help: "Passes when the output matches a regular expression.", param: { key: "pattern", label: "Regular expression", type: "text" } },
  { kind: "tool_sequence", label: "Tool order", help: "Passes when tools were called in this order.", param: { key: "expected_tools", label: "Tools in order, comma separated", type: "list" } },
  { kind: "pii_leakage", label: "No personal data in output", help: "Fails when the output contains emails, card numbers or keys." },
  { kind: "llm_judge", label: "LLM judge", help: "An LLM scores each run against your rubric. Needs an LLM key under Security and integrations.", param: { key: "rubric", label: "Rubric", type: "text" } },
];
const checkLabel = (kind: string) => CHECKS.find((c) => c.kind === kind)?.label ?? kind.replace(/_/g, " ");

const EvaluatorForm: React.FC<{ projectId: string; agents: AgentBreakdownRow[]; onSaved: () => void; onCancel: () => void }> = ({
  projectId,
  agents,
  onSaved,
  onCancel,
}) => {
  const [name, setName] = useState("");
  const [kind, setKind] = useState("no_errors");
  const [param, setParam] = useState("");
  const [target, setTarget] = useState("trace");
  const [agent, setAgent] = useState("");
  const [sample, setSample] = useState(100);
  const action = useAction();
  const check = CHECKS.find((c) => c.kind === kind)!;
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    let config: Record<string, unknown> = {};
    if (check.param) {
      const v =
        check.param.type === "number"
          ? Number(param)
          : check.param.type === "list"
            ? param.split(",").map((x) => x.trim()).filter(Boolean)
            : param;
      config = { [check.param.key]: v };
    }
    const ok = await action.run(
      () =>
        api.saveAgentEvaluator(projectId, {
          name: name.trim() || check.label,
          kind,
          target_type: target,
          agent_name: agent || null,
          sample_rate: sample / 100,
          enabled: true,
          config,
        }),
      "Evaluator created. It scores new runs as they arrive.",
    );
    if (ok) onSaved();
  };
  return (
    <form className="space-y-4" onSubmit={(e) => void submit(e)}>
      <div className="grid gap-3 md:grid-cols-2">
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Check</span>
          <select className={field} value={kind} onChange={(e) => { setKind(e.target.value); setParam(""); }}>
            {CHECKS.map((c) => (
              <option key={c.kind} value={c.kind}>{c.label}</option>
            ))}
          </select>
          <span className="text-xs text-muted">{check.help}</span>
        </label>
        {check.param && (
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>{check.param.label}</span>
            {check.param.key === "rubric" ? (
              <textarea className={field} rows={3} required value={param} onChange={(e) => setParam(e.target.value)} placeholder="e.g. The answer is correct, cites the help article, and is under 80 words." />
            ) : (
              <input
                className={field}
                required
                type={check.param.type === "number" ? "number" : "text"}
                min={check.param.type === "number" ? 0 : undefined}
                step="any"
                value={param}
                onChange={(e) => setParam(e.target.value)}
              />
            )}
          </label>
        )}
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Name</span>
          <input className={field} value={name} onChange={(e) => setName(e.target.value)} placeholder={check.label} maxLength={120} />
        </label>
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Score each</span>
          <select className={field} value={target} onChange={(e) => setTarget(e.target.value)}>
            <option value="trace">Run</option>
            <option value="span">Step</option>
            <option value="session">Session</option>
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Agent</span>
          <select className={field} value={agent} onChange={(e) => setAgent(e.target.value)}>
            <option value="">All agents</option>
            {agents.map((a) => (
              <option key={a.key} value={a.key}>{a.key}</option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Share of runs to score: {sample}%</span>
          <input type="range" min={1} max={100} value={sample} onChange={(e) => setSample(Number(e.target.value))} />
          {kind === "llm_judge" && <span className="text-xs text-muted">Each scored run is one LLM call on your key; lower this to control cost.</span>}
        </label>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <button type="submit" className={btn.primary} disabled={action.busy}>Create evaluator</button>
        <button type="button" className={btn.ghost} onClick={onCancel}>Cancel</button>
        {action.note}
      </div>
    </form>
  );
};

const Evaluations: React.FC<{
  projectId: string;
  refreshToken: number;
  agents: AgentBreakdownRow[];
  onTrace: (id: string) => void;
}> = ({ projectId, refreshToken, agents, onTrace }) => {
  const { canEdit } = useAccess();
  const evaluators = useLoad((s) => api.listAgentEvaluators(projectId, s), [projectId, refreshToken]);
  const results = useLoad((s) => api.listAgentEvaluations(projectId, 100, s), [projectId, refreshToken]);
  const [creating, setCreating] = useState(false);
  const action = useAction();

  const toggle = (e: AgentEvaluator) =>
    action
      .run(
        () => api.saveAgentEvaluator(projectId, { ...e, config: e.config ?? {}, enabled: !e.enabled }),
        `${e.name} turned ${e.enabled ? "off" : "on"}.`,
      )
      .then(() => evaluators.reload());
  const remove = (e: AgentEvaluator) => {
    if (!window.confirm(`Delete the evaluator "${e.name}"? Its past results stay.`)) return;
    void action.run(() => api.deleteAgentEvaluator(projectId, e.id), `${e.name} deleted.`).then(() => evaluators.reload());
  };

  return (
    <div className="space-y-5">
      <Panel
        title="Evaluators"
        meta={evaluators.data ? formatCount(evaluators.data.evaluators.length) : undefined}
        actions={
          <>
            {action.note}
            {canEdit && !creating && (
              <button type="button" className={btn.primary} onClick={() => setCreating(true)}>
                <Plus className="h-4 w-4" /> New evaluator
              </button>
            )}
          </>
        }
      >
        {creating && (
          <div className="mb-5 rounded-lg border border-line p-4">
            <EvaluatorForm
              projectId={projectId}
              agents={agents}
              onCancel={() => setCreating(false)}
              onSaved={() => {
                setCreating(false);
                evaluators.reload();
              }}
            />
          </div>
        )}
        {evaluators.error ? (
          <ErrorNotice message={evaluators.error} onRetry={evaluators.reload} />
        ) : !evaluators.data ? (
          <ListSkeleton rows={3} />
        ) : evaluators.data.evaluators.length === 0 ? (
          !creating && (
            <EmptyState
              title="No evaluators yet"
              body="Evaluators score runs automatically as they arrive: errors, latency or cost budgets, loops, or an LLM judge with your rubric."
              action={canEdit ? <button type="button" className={btn.primary} onClick={() => setCreating(true)}>New evaluator</button> : undefined}
            />
          )
        ) : (
          <Table
            head={
              <>
                <th className={th}>Name</th>
                <th className={th}>Check</th>
                <th className={th}>Scores each</th>
                <th className={th}>Agent</th>
                <th className={thR}>Sampled</th>
                <th className={thR}>Status</th>
                {canEdit && <th className={thR}><span className="sr-only">Actions</span></th>}
              </>
            }
          >
            {evaluators.data.evaluators.map((e: AgentEvaluator) => (
              <tr key={e.id}>
                <td className={`${td} font-medium text-ink`}>{e.name}</td>
                <td className={td}>{checkLabel(e.kind)}</td>
                <td className={td}>{e.target_type === "trace" ? "run" : e.target_type === "span" ? "step" : e.target_type}</td>
                <td className={td}>{e.agent_name ?? "All agents"}</td>
                <td className={tdR}>{formatPercent(e.sample_rate, 0)}</td>
                <td className="py-2.5 pl-4 text-right">
                  {canEdit ? (
                    <button
                      type="button"
                      role="switch"
                      aria-checked={e.enabled}
                      aria-label={`${e.name} ${e.enabled ? "on" : "off"}`}
                      disabled={action.busy}
                      onClick={() => void toggle(e)}
                      className={`rounded px-2 py-0.5 text-xs font-medium ${e.enabled ? "bg-ok-soft text-ok" : "bg-sunken text-muted"}`}
                    >
                      {e.enabled ? "On" : "Off"}
                    </button>
                  ) : (
                    <span className="text-sm text-ink-2">{e.enabled ? "On" : "Off"}</span>
                  )}
                </td>
                {canEdit && (
                  <td className="py-2.5 pl-2 text-right">
                    <button type="button" className={btn.ghost} aria-label={`Delete ${e.name}`} onClick={() => remove(e)}>
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </Table>
        )}
      </Panel>
      <Panel title="Recent evaluations">
        {results.error ? (
          <ErrorNotice message={results.error} onRetry={results.reload} />
        ) : !results.data ? (
          <ListSkeleton rows={5} />
        ) : results.data.evaluations.length === 0 ? (
          <p className="text-sm text-muted">No evaluations yet.</p>
        ) : (
          <Table
            minWidth={820}
            head={
              <>
                <th className={th}>When</th>
                <th className={th}>Evaluator</th>
                <th className={th}>Result</th>
                <th className={th}>Why</th>
                <th className={thR}>Run</th>
              </>
            }
          >
            {results.data.evaluations.map((ev: AgentEvaluation) => (
              <tr key={ev.id}>
                <td className={`${td} tabular whitespace-nowrap`}>{formatDateTime(ev.created_at_ms, { dateStyle: "medium", timeStyle: "short" })}</td>
                <td className={td}>
                  <span className="font-medium text-ink">{ev.evaluator_name}</span>
                  <span className="ml-1.5 text-xs text-faint">{ev.target_type === "trace" ? "run" : ev.target_type === "span" ? "step" : ev.target_type}</span>
                </td>
                <td className={td}>
                  <ResultBadge ev={ev} />
                </td>
                <td className={`${td} max-w-md`}>{ev.error ?? ev.reasoning ?? "–"}</td>
                <td className="py-2.5 pl-4 text-right">
                  {ev.trace_id ? (
                    <button type="button" className="whitespace-nowrap text-sm font-medium text-brand hover:underline" onClick={() => onTrace(ev.trace_id!)}>
                      View run
                    </button>
                  ) : (
                    "–"
                  )}
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Panel>
    </div>
  );
};

// ---------------------------------------------------------------------------

const Datasets: React.FC<{ projectId: string; refreshToken: number }> = ({ projectId, refreshToken }) => {
  const { canEdit } = useAccess();
  const { data, error, reload } = useLoad((s) => api.listAgentDatasets(projectId, s), [projectId, refreshToken]);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [desc, setDesc] = useState("");
  const action = useAction();
  if (error) return <ErrorNotice message={error} onRetry={reload} />;
  if (!data) return <ListSkeleton rows={3} />;
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (await action.run(() => api.createAgentDataset(projectId, name.trim(), desc.trim()), "Dataset created.")) {
      setCreating(false);
      setName("");
      setDesc("");
      reload();
    }
  };
  return (
    <Panel
      title="Datasets"
      actions={
        canEdit &&
        !creating && (
          <button type="button" className={btn.primary} onClick={() => setCreating(true)}>
            <Plus className="h-4 w-4" /> New dataset
          </button>
        )
      }
    >
      {creating && (
        <form className="mb-5 flex flex-wrap items-end gap-2 rounded-lg border border-line p-4" onSubmit={(e) => void submit(e)}>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Name</span>
            <input className={field} required maxLength={120} value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Support regressions" />
          </label>
          <label className="flex min-w-[240px] flex-1 flex-col gap-1">
            <span className={fieldLabel}>Description</span>
            <input className={field} maxLength={500} value={desc} onChange={(e) => setDesc(e.target.value)} placeholder="What these test cases cover" />
          </label>
          <button type="submit" className={btn.primary} disabled={action.busy}>Create</button>
          <button type="button" className={btn.ghost} onClick={() => setCreating(false)}>Cancel</button>
          {action.note}
        </form>
      )}
      {data.datasets.length === 0 ? (
        <p className="text-sm text-muted">
          No datasets yet. A dataset is a set of test cases, often production runs that went wrong. Open a run under Traces
          and use Add to dataset.
        </p>
      ) : (
        <Table
          head={
            <>
              <th className={th}>Name</th>
              <th className={th}>Description</th>
              <th className={thR}>Version</th>
              <th className={thR}>Cases</th>
              <th className={thR}>Updated</th>
            </>
          }
        >
          {data.datasets.map((d: AgentDataset) => (
            <tr key={d.id}>
              <td className={`${td} font-medium text-ink`}>{d.name}</td>
              <td className={td}>{d.description || "–"}</td>
              <td className={tdR}>v{d.version}</td>
              <td className={tdR}>{formatCount(d.item_count)}</td>
              <td className={tdR}>{formatDate(d.updated_at_ms)}</td>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
};

const Experiments: React.FC<{ projectId: string; refreshToken: number }> = ({ projectId, refreshToken }) => {
  const { data, error, reload } = useLoad((s) => api.listAgentExperiments(projectId, s), [projectId, refreshToken]);
  if (error) return <ErrorNotice message={error} onRetry={reload} />;
  if (!data) return <ListSkeleton rows={3} />;
  return (
    <Panel title="Experiments">
      {data.experiments.length === 0 ? (
        <p className="text-sm text-muted">
          No experiments yet. An experiment runs an agent version against a dataset so you can compare versions before shipping.
        </p>
      ) : (
        <Table
          minWidth={760}
          head={
            <>
              <th className={th}>Name</th>
              <th className={th}>Agent</th>
              <th className={th}>Status</th>
              <th className={thR}>Progress</th>
              <th className={thR}>Passed</th>
              <th className={thR}>Avg score</th>
            </>
          }
        >
          {data.experiments.map((x: AgentExperiment) => (
            <tr key={x.id}>
              <td className={`${td} font-medium text-ink`}>{x.name}</td>
              <td className={td}>
                {x.agent_name} <span className="text-xs text-faint">{x.agent_version}</span>
              </td>
              <td className={`${td} capitalize`}>{x.status}</td>
              <td className={tdR}>
                {formatCount(x.completed_items)} / {formatCount(x.total_items)}
              </td>
              <td className={tdR}>{x.completed_items ? formatCount(x.passed_items) : "–"}</td>
              <td className={tdR}>{formatScore(x.avg_score)}</td>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
};
