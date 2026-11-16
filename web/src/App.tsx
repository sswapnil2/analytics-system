import React, { useCallback, useEffect, useState } from "react";
import {
  Activity,
  Bot,
  Cpu,
  Database,
  Filter,
  FlaskConical,
  GitBranch,
  Grid3X3,
  LineChart,
  Smartphone,
  Sparkles,
  Zap,
} from "lucide-react";
import { api, NodeStatus } from "./api/client";
import { CohortsLookupsWorkspace } from "./workspaces/CohortsLookupsWorkspace";
import { ExperimentsFlagsWorkspace } from "./workspaces/ExperimentsFlagsWorkspace";
import { FlowsWorkspace } from "./workspaces/FlowsWorkspace";
import { FunnelsWorkspace } from "./workspaces/FunnelsWorkspace";
import { InsightsWorkspace } from "./workspaces/InsightsWorkspace";
import { LexiconMcpWorkspace } from "./workspaces/LexiconMcpWorkspace";
import { RetentionWorkspace } from "./workspaces/RetentionWorkspace";
import { SessionsReplayWorkspace } from "./workspaces/SessionsReplayWorkspace";

type WorkspaceId =
  | "insights"
  | "funnels"
  | "flows"
  | "retention"
  | "sessions"
  | "cohorts"
  | "experiments"
  | "lexicon";

interface NavItem {
  id: WorkspaceId;
  label: string;
  subtitle: string;
  icon: React.ComponentType<{ className?: string }>;
}

const NAV_ITEMS: NavItem[] = [
  {
    id: "insights",
    label: "Insights",
    subtitle: "Series · DDSketch · Formulas",
    icon: LineChart,
  },
  {
    id: "funnels",
    label: "Funnels",
    subtitle: "Exact u32 · Drop-Off Replay",
    icon: Filter,
  },
  {
    id: "flows",
    label: "Flows",
    subtitle: "Forward & Reverse Sankey",
    icon: GitBranch,
  },
  {
    id: "retention",
    label: "Retention",
    subtitle: "N-Day · Unbounded · Stickiness",
    icon: Grid3X3,
  },
  {
    id: "sessions",
    label: "Sessions & Replay",
    subtitle: "Bloom Pruned · Phone Player",
    icon: Smartphone,
  },
  {
    id: "cohorts",
    label: "Cohorts & Lookups",
    subtitle: "RoaringBitmap · SQL Props",
    icon: Database,
  },
  {
    id: "experiments",
    label: "Experiments & Flags",
    subtitle: "Z-Test · Bayesian · Edge ETag",
    icon: FlaskConical,
  },
  {
    id: "lexicon",
    label: "Lexicon & MCP",
    subtitle: "Governance · JSON-RPC Tools",
    icon: Bot,
  },
];

function formatLatency(us: number): string {
  if (us <= 0) return "—";
  if (us < 1000) return `${us} µs`;
  return `${(us / 1000).toFixed(2)} ms`;
}

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<WorkspaceId>("funnels");
  const [projectId, setProjectId] = useState<string>("proj_fintech_in");
  const [status, setStatus] = useState<NodeStatus | null>(null);
  const [availableEvents, setAvailableEvents] = useState<string[]>([]);
  const [availableProps, setAvailableProps] = useState<string[]>([]);
  const [refreshToken, setRefreshToken] = useState<number>(0);
  const [bursting, setBursting] = useState<boolean>(false);
  const [burstToast, setBurstToast] = useState<string | null>(null);

  const loadStatus = useCallback(async () => {
    try {
      const s = await api.getStatus(projectId);
      setStatus(s);
    } catch {
      // Keep last status on transient poll hiccup
    }
  }, [projectId]);

  const loadCatalog = useCallback(async () => {
    try {
      const [lex, ext] = await Promise.all([
        api.getLexicon(projectId),
        api.getSchemaExtensions(projectId),
      ]);
      const evNames = new Set<string>();
      for (const ev of lex.events) {
        if (ev.status !== "dropped") {
          evNames.add(ev.name);
        }
      }
      for (const ce of ext.custom_events) {
        evNames.add(ce.name);
      }
      const propNames = new Set<string>();
      for (const p of lex.properties) {
        propNames.add(p.name);
      }
      for (const cp of ext.custom_properties) {
        propNames.add(cp.name);
      }
      for (const lt of ext.lookup_tables) {
        for (const col of lt.sample_columns) {
          propNames.add(`${lt.name}.${col}`);
        }
      }
      setAvailableEvents(Array.from(evNames).sort());
      setAvailableProps(Array.from(propNames).sort());
    } catch {
      // Fallback defaults inside workspaces will handle empty arrays
    }
  }, [projectId]);

  useEffect(() => {
    void loadStatus();
    void loadCatalog();
    const timer = window.setInterval(() => {
      void loadStatus();
    }, 2500);
    return () => window.clearInterval(timer);
  }, [loadStatus, loadCatalog]);

  const handleSimulateBurst = async () => {
    setBursting(true);
    try {
      const res = await api.simulateBurst(projectId, 1800);
      await loadStatus();
      await loadCatalog();
      setRefreshToken((n) => n + 1);
      setBurstToast(
        `+${res.accepted.toLocaleString()} events ingested · ${res.deduped} flaky retries deduped`,
      );
      window.setTimeout(() => setBurstToast(null), 4500);
    } finally {
      setBursting(false);
    }
  };

  const totalStoredRows = (status?.l0_rows ?? 0) + (status?.l1_rows ?? 0);

  return (
    <div className="min-h-screen bg-[#090d16] text-slate-100 flex flex-col font-sans antialiased">
      {/* Global Topbar */}
      <header className="h-14 border-b border-slate-800/90 bg-[#0c1220]/95 backdrop-blur px-4 flex items-center justify-between gap-3 sticky top-0 z-30">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center shadow-lg shadow-blue-500/20">
            <Sparkles className="w-4 h-4 text-slate-950" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold tracking-tight text-sm text-white">
                PULSE ANALYTICS
              </span>
              <span className="text-[10px] font-mono uppercase px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-300 border border-blue-500/30">
                Vortex + Roaring u32
              </span>
            </div>
            <p className="text-[11px] text-slate-400 leading-none mt-0.5">
              Zero-Middleware Disaggregated S3 + Ephemeral NVMe Engine
            </p>
          </div>

          {/* Project Switcher */}
          <div className="ml-3 pl-3 border-l border-slate-800">
            <select
              aria-label="Active Project"
              value={projectId}
              onChange={(e) => {
                setProjectId(e.target.value);
                setRefreshToken((n) => n + 1);
              }}
              className="bg-[#111827] border border-slate-700/80 text-xs text-slate-200 rounded-md px-2.5 py-1.5 focus:outline-none focus:border-blue-500"
            >
              {(status?.projects && status.projects.length > 0
                ? status.projects
                : [
                    {
                      id: "proj_fintech_in",
                      name: "Pulse SuperApp (IN)",
                      timezone: "Asia/Kolkata",
                    },
                  ]
              ).map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name} ({p.id})
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Live Telemetry Pills + Traffic Burst Action */}
        <div className="flex items-center gap-2.5 flex-wrap justify-end">
          {burstToast && (
            <div className="text-xs px-2.5 py-1 rounded-md bg-emerald-500/15 border border-emerald-500/40 text-emerald-300 font-mono">
              {burstToast}
            </div>
          )}

          {/* Live Ingestion EPS & Stored Rows */}
          <div className="flex items-center gap-2 px-2.5 py-1 rounded-md bg-[#111827] border border-slate-800 text-xs">
            <Activity className="w-3.5 h-3.5 text-emerald-400" />
            <span className="text-slate-400">Ingest:</span>
            <span className="font-mono text-emerald-300 font-medium">
              {(status?.live_eps ?? 0).toLocaleString()} eps
            </span>
            <span className="text-slate-600">·</span>
            <span className="font-mono text-slate-200">
              {totalStoredRows.toLocaleString()} rows
            </span>
            {(status?.deduped_events_total ?? 0) > 0 && (
              <span className="text-[11px] font-mono text-amber-300/90">
                ({status?.deduped_events_total.toLocaleString()} deduped)
              </span>
            )}
          </div>

          {/* Engine Query Latency Pill */}
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#111827] border border-slate-800 text-xs">
            <Cpu className="w-3.5 h-3.5 text-cyan-400" />
            <span className="text-slate-400">Engine:</span>
            <span className="font-mono text-cyan-300 font-semibold">
              {formatLatency(status?.last_query_latency_us ?? 0)}
            </span>
          </div>

          {/* Snapshot Epoch & u32 Graph Badge */}
          <div className="hidden xl:flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#111827] border border-slate-800 text-xs font-mono">
            <span className="text-purple-300">Epoch #{status?.snapshot_epoch ?? 0}</span>
            <span className="text-slate-600">·</span>
            <span className="text-slate-300">{status?.segment_count ?? 0} Vortex segs</span>
            <span className="text-slate-600">·</span>
            <span className="text-blue-300">
              {(status?.interned_users ?? 0).toLocaleString()} u32 IDs
            </span>
          </div>

          {/* Simulate Traffic Burst Button */}
          <button
            type="button"
            onClick={() => void handleSimulateBurst()}
            disabled={bursting}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white shadow-sm shadow-blue-600/30 transition"
          >
            <Zap className="w-3.5 h-3.5" />
            {bursting ? "Streaming Burst…" : "Simulate Traffic Burst"}
          </button>
        </div>
      </header>

      {/* Main Studio Body */}
      <div className="flex-1 flex min-h-0">
        {/* Left Navigation Rail */}
        <aside className="w-60 shrink-0 border-r border-slate-800/90 bg-[#0b101c] p-3 flex flex-col justify-between">
          <nav className="space-y-1">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setActiveTab(item.id)}
                  className={`w-full text-left px-3 py-2.5 rounded-lg transition flex items-start gap-2.5 border ${
                    isActive
                      ? "bg-blue-600/15 border-blue-500/40 text-white"
                      : "border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/40"
                  }`}
                >
                  <Icon
                    className={`w-4 h-4 mt-0.5 shrink-0 ${
                      isActive ? "text-blue-400" : "text-slate-500"
                    }`}
                  />
                  <div className="min-w-0">
                    <div className="text-xs font-medium leading-tight truncate">{item.label}</div>
                    <div className="text-[10px] text-slate-500 truncate mt-0.5">
                      {item.subtitle}
                    </div>
                  </div>
                </button>
              );
            })}
          </nav>

          {/* Architecture Invariants Footer */}
          <div className="mt-6 p-3 rounded-lg bg-[#0f172a]/80 border border-slate-800/80 space-y-1.5 text-[11px] text-slate-400">
            <div className="font-semibold text-slate-200 flex items-center justify-between">
              <span>Physical Storage</span>
              <span className="text-[10px] font-mono text-emerald-400">RPO = 0</span>
            </div>
            <div className="font-mono text-[10px] text-slate-400 leading-relaxed">
              <div>L0 Hot RAM: {(status?.l0_rows ?? 0).toLocaleString()}</div>
              <div>L1 Vortex: {(status?.l1_rows ?? 0).toLocaleString()}</div>
              <div>Key: {status?.default_api_key ?? "pk_pulse_live_india_01"}</div>
            </div>
          </div>
        </aside>

        {/* Active Workspace Canvas */}
        <main className="flex-1 min-w-0 p-5 overflow-y-auto">
          {activeTab === "insights" && (
            <InsightsWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              availableProps={availableProps}
              refreshToken={refreshToken}
            />
          )}
          {activeTab === "funnels" && (
            <FunnelsWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              refreshToken={refreshToken}
            />
          )}
          {activeTab === "flows" && (
            <FlowsWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              refreshToken={refreshToken}
            />
          )}
          {activeTab === "retention" && (
            <RetentionWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              refreshToken={refreshToken}
            />
          )}
          {activeTab === "sessions" && (
            <SessionsReplayWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              refreshToken={refreshToken}
            />
          )}
          {activeTab === "cohorts" && (
            <CohortsLookupsWorkspace
              projectId={projectId}
              availableEvents={availableEvents}
              onCatalogChanged={() => {
                void loadCatalog();
                setRefreshToken((n) => n + 1);
              }}
            />
          )}
          {activeTab === "experiments" && (
            <ExperimentsFlagsWorkspace projectId={projectId} refreshToken={refreshToken} />
          )}
          {activeTab === "lexicon" && (
            <LexiconMcpWorkspace
              projectId={projectId}
              onLexiconChanged={() => {
                void loadCatalog();
                setRefreshToken((n) => n + 1);
              }}
            />
          )}
        </main>
      </div>
    </div>
  );
};

export default App;
