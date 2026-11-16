import React, { useCallback, useEffect, useState } from "react";
import {
  Bot,
  FlaskConical,
  GitBranch,
  Grid3X3,
  LineChart,
  ListFilter,
  LogOut,
  Monitor,
  Moon,
  Smartphone,
  Sun,
  Users,
} from "lucide-react";
import {
  api,
  ApiAuthError,
  getStoredAdminToken,
  NodeStatus,
  setStoredAdminToken,
} from "./api/client";
import { btn, field } from "./components/ui";
import { errorMessage, formatCount } from "./lib/format";
import { CohortsLookupsWorkspace } from "./workspaces/CohortsLookupsWorkspace";
import { ExperimentsFlagsWorkspace } from "./workspaces/ExperimentsFlagsWorkspace";
import { FlowsWorkspace } from "./workspaces/FlowsWorkspace";
import { FunnelsWorkspace } from "./workspaces/FunnelsWorkspace";
import { InsightsWorkspace } from "./workspaces/InsightsWorkspace";
import { LexiconMcpWorkspace } from "./workspaces/LexiconMcpWorkspace";
import { RetentionWorkspace } from "./workspaces/RetentionWorkspace";
import { SessionsReplayWorkspace } from "./workspaces/SessionsReplayWorkspace";

type WorkspaceId =
  | "trends"
  | "funnels"
  | "journeys"
  | "retention"
  | "replays"
  | "cohorts"
  | "experiments"
  | "events";

interface NavItem {
  id: WorkspaceId;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}

const NAV_GROUPS: { label: string; items: NavItem[] }[] = [
  {
    label: "Analyze",
    items: [
      { id: "funnels", label: "Funnels", icon: ListFilter },
      { id: "trends", label: "Trends", icon: LineChart },
      { id: "journeys", label: "Journeys", icon: GitBranch },
      { id: "retention", label: "Retention", icon: Grid3X3 },
    ],
  },
  {
    label: "Investigate",
    items: [{ id: "replays", label: "Session replays", icon: Smartphone }],
  },
  {
    label: "Audience",
    items: [{ id: "cohorts", label: "Cohorts", icon: Users }],
  },
  {
    label: "Ship",
    items: [{ id: "experiments", label: "Experiments", icon: FlaskConical }],
  },
  {
    label: "Data",
    items: [{ id: "events", label: "Events & privacy", icon: Bot }],
  },
];

const ALL_IDS = NAV_GROUPS.flatMap((g) => g.items.map((i) => i.id));
const DEFAULT_TAB: WorkspaceId = "funnels";

function tabFromHash(): WorkspaceId {
  const h = window.location.hash.replace(/^#\/?/, "") as WorkspaceId;
  return ALL_IDS.includes(h) ? h : DEFAULT_TAB;
}

type ThemePref = "system" | "light" | "dark";
const THEME_KEY = "pulse_theme";

function readTheme(): ThemePref {
  try {
    const t = window.localStorage.getItem(THEME_KEY);
    return t === "light" || t === "dark" ? t : "system";
  } catch {
    return "system";
  }
}

function applyTheme(t: ThemePref) {
  const el = document.documentElement;
  if (t === "system") el.removeAttribute("data-theme");
  else el.setAttribute("data-theme", t);
  try {
    window.localStorage.setItem(THEME_KEY, t);
  } catch {
    // storage unavailable: theme still applies for this visit
  }
}

const Logo: React.FC = () => (
  <svg viewBox="0 0 24 24" className="h-6 w-6" aria-hidden="true">
    <rect width="24" height="24" rx="6" className="fill-brand" />
    <path
      d="M4 13h4l2-5 3 9 2-6 1.5 2H20"
      fill="none"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="stroke-on-brand"
    />
  </svg>
);

// ---------------------------------------------------------------------------
// Sign-in
// ---------------------------------------------------------------------------

const SignIn: React.FC<{ onSignedIn: () => void; reason?: string }> = ({ onSignedIn, reason }) => {
  const [token, setToken] = useState("");
  const [error, setError] = useState<string | null>(reason ?? null);
  const [checking, setChecking] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const value = token.trim();
    if (!value) {
      setError("Paste your admin token to continue.");
      return;
    }
    setChecking(true);
    setError(null);
    setStoredAdminToken(value);
    try {
      await api.getStatus();
      onSignedIn();
    } catch (err) {
      setStoredAdminToken("");
      setError(
        err instanceof ApiAuthError
          ? "That token wasn't accepted. Check that you copied the whole sk_ token."
          : `Couldn't reach the Pulse server: ${errorMessage(err)}`,
      );
    } finally {
      setChecking(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-canvas p-4">
      <form
        onSubmit={(e) => void submit(e)}
        className="w-full max-w-sm space-y-5 rounded-xl border border-line bg-panel p-6"
        noValidate
      >
        <div className="flex items-center gap-2.5">
          <Logo />
          <span className="text-base font-semibold text-ink">Pulse</span>
        </div>
        <div className="space-y-1.5">
          <h1 className="text-lg font-semibold text-ink">Sign in to your workspace</h1>
          <p className="text-sm text-muted">
            Use the admin token printed in the server log the first time Pulse started. It starts
            with <code className="font-mono text-ink-2">sk_</code>.
          </p>
        </div>
        <div className="space-y-1.5">
          <label htmlFor="token" className="text-sm font-medium text-ink">
            Admin token
          </label>
          <input
            id="token"
            type="password"
            autoComplete="current-password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="sk_…"
            className={`${field} w-full font-mono`}
            aria-invalid={!!error}
            aria-describedby={error ? "token-error" : undefined}
            autoFocus
          />
          {error && (
            <p id="token-error" role="alert" className="text-sm text-danger">
              {error}
            </p>
          )}
        </div>
        <button type="submit" disabled={checking} className={`${btn.primary} w-full py-2`}>
          {checking ? "Signing in…" : "Sign in"}
        </button>
        <p className="text-xs text-faint">
          The token is kept for this browser tab only and cleared when you close it.
        </p>
      </form>
    </div>
  );
};

// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

export const App: React.FC = () => {
  const [signedIn, setSignedIn] = useState<boolean>(() => !!getStoredAdminToken());
  const [signInReason, setSignInReason] = useState<string | undefined>();
  const [activeTab, setActiveTab] = useState<WorkspaceId>(tabFromHash);
  const [projectId, setProjectId] = useState<string>("");
  const [status, setStatus] = useState<NodeStatus | null>(null);
  const [availableEvents, setAvailableEvents] = useState<string[]>([]);
  const [availableProps, setAvailableProps] = useState<string[]>([]);
  const [refreshToken, setRefreshToken] = useState<number>(0);
  const [theme, setTheme] = useState<ThemePref>(readTheme);
  const [flushing, setFlushing] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => applyTheme(theme), [theme]);

  useEffect(() => {
    const onHash = () => setActiveTab(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const signOut = useCallback((reason?: string) => {
    setStoredAdminToken("");
    setStatus(null);
    setProjectId("");
    setSignInReason(reason);
    setSignedIn(false);
  }, []);

  const loadStatus = useCallback(async () => {
    try {
      const s = await api.getStatus(projectId || undefined);
      setStatus(s);
      if (!projectId && s.active_project_id) setProjectId(s.active_project_id);
    } catch (err) {
      if (err instanceof ApiAuthError) signOut("Your session ended. Sign in again to continue.");
    }
  }, [projectId, signOut]);

  const loadCatalog = useCallback(async () => {
    if (!projectId) return;
    try {
      const [lex, ext] = await Promise.all([
        api.getLexicon(projectId),
        api.getSchemaExtensions(projectId),
      ]);
      const evNames = new Set<string>();
      for (const ev of lex.events) if (ev.status !== "dropped") evNames.add(ev.name);
      for (const ce of ext.custom_events) evNames.add(ce.name);
      const propNames = new Set<string>();
      for (const p of lex.properties) propNames.add(p.name);
      for (const cp of ext.custom_properties) propNames.add(cp.name);
      for (const lt of ext.lookup_tables)
        for (const col of lt.sample_columns) propNames.add(`${lt.name}.${col}`);
      setAvailableEvents(Array.from(evNames).sort());
      setAvailableProps(Array.from(propNames).sort());
    } catch (err) {
      if (err instanceof ApiAuthError) signOut("Your session ended. Sign in again to continue.");
    }
  }, [projectId, signOut]);

  // Poll status only while signed in and the tab is visible.
  useEffect(() => {
    if (!signedIn) return;
    void loadStatus();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void loadStatus();
    }, 5000);
    return () => window.clearInterval(timer);
  }, [signedIn, loadStatus]);

  useEffect(() => {
    if (signedIn) void loadCatalog();
  }, [signedIn, loadCatalog]);

  const go = (id: WorkspaceId) => {
    window.location.hash = `/${id}`;
    setActiveTab(id);
  };

  const handleFlush = async () => {
    setFlushing(true);
    try {
      const res = await api.flushStorage();
      await loadStatus();
      setRefreshToken((n) => n + 1);
      setNotice(
        `Saved buffered events to storage (${res.segments_flushed} file${res.segments_flushed === 1 ? "" : "s"}).`,
      );
    } catch (err) {
      setNotice(`Couldn't save buffered events: ${errorMessage(err)}`);
    } finally {
      setFlushing(false);
      window.setTimeout(() => setNotice(null), 5000);
    }
  };

  if (!signedIn) {
    return (
      <SignIn
        reason={signInReason}
        onSignedIn={() => {
          setSignInReason(undefined);
          setSignedIn(true);
          setRefreshToken((n) => n + 1);
        }}
      />
    );
  }

  const activePid = projectId || status?.active_project_id || "";
  const totalEvents = (status?.l0_rows ?? 0) + (status?.l1_rows ?? 0);
  const projects = status?.projects ?? [];
  const nextTheme: Record<ThemePref, ThemePref> = { system: "light", light: "dark", dark: "system" };
  const ThemeIcon = theme === "light" ? Sun : theme === "dark" ? Moon : Monitor;

  return (
    <div className="flex min-h-screen flex-col bg-canvas text-ink md:flex-row">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-3 focus:top-3 focus:z-50 focus:rounded-md focus:bg-panel focus:px-3 focus:py-2"
      >
        Skip to content
      </a>

      {/* Sidebar: full height on desktop, a scrolling tab strip on small screens. */}
      <aside className="shrink-0 border-b border-line bg-panel md:sticky md:top-0 md:flex md:h-screen md:w-56 md:flex-col md:border-b-0 md:border-r">
        <div className="flex items-center gap-2.5 px-4 py-3 md:py-4">
          <Logo />
          <span className="text-base font-semibold tracking-[-0.01em]">Pulse</span>
        </div>

        <nav
          aria-label="Workspaces"
          className="flex gap-1 overflow-x-auto px-2 pb-2 md:block md:flex-1 md:space-y-4 md:overflow-y-auto md:pb-0"
        >
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="flex gap-1 md:block md:space-y-0.5">
              <div className="hidden px-2 pb-1 text-xs font-medium text-faint md:block">
                {group.label}
              </div>
              {group.items.map((item) => {
                const Icon = item.icon;
                const active = activeTab === item.id;
                return (
                  <a
                    key={item.id}
                    href={`#/${item.id}`}
                    onClick={(e) => {
                      e.preventDefault();
                      go(item.id);
                    }}
                    aria-current={active ? "page" : undefined}
                    className={`flex shrink-0 items-center gap-2.5 whitespace-nowrap rounded-md px-2 py-1.5 text-sm ${
                      active
                        ? "bg-brand-soft font-medium text-brand"
                        : "text-ink-2 hover:bg-hover hover:text-ink"
                    }`}
                  >
                    <Icon className="h-4 w-4 shrink-0" />
                    {item.label}
                  </a>
                );
              })}
            </div>
          ))}
        </nav>

        <div className="hidden space-y-2 border-t border-line px-4 py-3 text-xs text-muted md:block">
          <div className="tabular">
            <span className="text-ink">{formatCount(totalEvents)}</span> events stored
          </div>
          <div className="tabular">
            {(status?.live_eps ?? 0) > 0 ? (
              <span className="inline-flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-ok" aria-hidden="true" />
                Receiving {formatCount(status?.live_eps)} events/s
              </span>
            ) : (
              "No events in the last few seconds"
            )}
          </div>
          {(status?.l0_rows ?? 0) > 0 && (
            <button
              type="button"
              onClick={() => void handleFlush()}
              disabled={flushing}
              className="text-left text-brand hover:underline disabled:opacity-50"
              title="Writes events still held in memory to durable storage"
            >
              {flushing ? "Saving…" : `Save ${formatCount(status?.l0_rows)} buffered events now`}
            </button>
          )}
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-30 flex h-14 items-center justify-between gap-3 border-b border-line bg-canvas/90 px-4 backdrop-blur md:px-6">
          <label className="flex min-w-0 items-center gap-2 text-sm">
            <span className="hidden text-muted sm:inline">Project</span>
            <select
              value={activePid}
              onChange={(e) => {
                setProjectId(e.target.value);
                setRefreshToken((n) => n + 1);
              }}
              className={`${field} max-w-[16rem] truncate font-medium`}
              disabled={projects.length === 0}
            >
              {projects.length === 0 && (
                <option value={activePid}>{activePid || "Loading…"}</option>
              )}
              {projects.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
            </select>
          </label>

          <div className="flex items-center gap-1">
            {notice && (
              <span role="status" className="mr-2 hidden text-sm text-muted lg:inline">
                {notice}
              </span>
            )}
            <button
              type="button"
              onClick={() => setTheme(nextTheme[theme])}
              className={btn.ghost}
              aria-label={`Theme: ${theme}. Switch to ${nextTheme[theme]}`}
              title={`Theme: ${theme}`}
            >
              <ThemeIcon className="h-4 w-4" />
            </button>
            <button type="button" onClick={() => signOut()} className={btn.ghost}>
              <LogOut className="h-4 w-4" />
              <span className="hidden sm:inline">Sign out</span>
            </button>
          </div>
        </header>

        <main id="main" className="min-w-0 flex-1 px-4 py-6 md:px-6 lg:px-8">
          <div className="mx-auto max-w-[1400px]">
            {!activePid ? (
              <p className="text-sm text-muted" role="status">
                Loading your workspace…
              </p>
            ) : (
              <>
                {activeTab === "trends" && (
                  <InsightsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "funnels" && (
                  <FunnelsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "journeys" && (
                  <FlowsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "retention" && (
                  <RetentionWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "replays" && (
                  <SessionsReplayWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "cohorts" && (
                  <CohortsLookupsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    onCatalogChanged={() => {
                      void loadCatalog();
                      setRefreshToken((n) => n + 1);
                    }}
                  />
                )}
                {activeTab === "experiments" && (
                  <ExperimentsFlagsWorkspace projectId={activePid} refreshToken={refreshToken} />
                )}
                {activeTab === "events" && (
                  <LexiconMcpWorkspace
                    projectId={activePid}
                    onLexiconChanged={() => {
                      void loadCatalog();
                      setRefreshToken((n) => n + 1);
                    }}
                  />
                )}
              </>
            )}
          </div>
        </main>
      </div>
    </div>
  );
};

export default App;
