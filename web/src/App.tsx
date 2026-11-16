import React, { useCallback, useEffect, useState } from "react";
import {
  FlaskConical,
  GitBranch,
  CalendarCheck,
  LineChart,
  Filter,
  LayoutDashboard,
  LogOut,
  Monitor,
  Moon,
  PlayCircle,
  ShieldCheck,
  Sun,
  Users,
} from "lucide-react";
import {
  api,
  ApiAuthError,
  AuthUser,
  getStoredAdminToken,
  NodeStatus,
  setStoredAdminToken,
} from "./api/client";
import { AuthCallback, Logo, parseAuthHash, SignInScreen, SignUpScreen } from "./auth/AuthScreens";
import { btn } from "./components/ui";
import { errorMessage, formatCount, isNotFound } from "./lib/format";
import { CohortsLookupsWorkspace } from "./workspaces/CohortsLookupsWorkspace";
import { ExperimentsFlagsWorkspace } from "./workspaces/ExperimentsFlagsWorkspace";
import { FlowsWorkspace } from "./workspaces/FlowsWorkspace";
import { FunnelsWorkspace } from "./workspaces/FunnelsWorkspace";
import { InsightsWorkspace } from "./workspaces/InsightsWorkspace";
import { LexiconMcpWorkspace } from "./workspaces/LexiconMcpWorkspace";
import { RetentionWorkspace } from "./workspaces/RetentionWorkspace";
import { SessionsReplayWorkspace } from "./workspaces/SessionsReplayWorkspace";
import { DashboardsWorkspace } from "./dashboards/DashboardsWorkspace";

type WorkspaceId =
  | "dashboards"
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
    label: "",
    items: [{ id: "dashboards", label: "Dashboards", icon: LayoutDashboard }],
  },
  {
    label: "Analyze",
    items: [
      { id: "funnels", label: "Funnels", icon: Filter },
      { id: "trends", label: "Trends", icon: LineChart },
      { id: "journeys", label: "Journeys", icon: GitBranch },
      { id: "retention", label: "Retention", icon: CalendarCheck },
    ],
  },
  {
    label: "Investigate",
    items: [{ id: "replays", label: "Session replays", icon: PlayCircle }],
  },
  {
    label: "Audience and shipping",
    items: [
      { id: "cohorts", label: "Cohorts", icon: Users },
      { id: "experiments", label: "Experiments", icon: FlaskConical },
      { id: "events", label: "Events and privacy", icon: ShieldCheck },
    ],
  },
];

const ALL_IDS = NAV_GROUPS.flatMap((g) => g.items.map((i) => i.id));
const DEFAULT_TAB: WorkspaceId = "funnels";

function tabFromHash(): WorkspaceId {
  const h = window.location.hash.replace(/^#\/?/, "").split(/[/?]/)[0] as WorkspaceId;
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


// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

export const App: React.FC = () => {
  const [signedIn, setSignedIn] = useState<boolean>(() => !!getStoredAdminToken());
  const [authRoute, setAuthRoute] = useState(parseAuthHash);
  const [signInReason, setSignInReason] = useState<string | undefined>();
  const [activeTab, setActiveTab] = useState<WorkspaceId>(tabFromHash);
  const [projectId, setProjectId] = useState<string>("");
  const [status, setStatus] = useState<NodeStatus | null>(null);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [availableEvents, setAvailableEvents] = useState<string[]>([]);
  const [availableProps, setAvailableProps] = useState<string[]>([]);
  const [refreshToken, setRefreshToken] = useState<number>(0);
  const [theme, setTheme] = useState<ThemePref>(readTheme);
  const [flushing, setFlushing] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => applyTheme(theme), [theme]);

  useEffect(() => {
    const onHash = () => {
      setActiveTab(tabFromHash());
      setAuthRoute(parseAuthHash());
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const signOut = useCallback((reason?: string) => {
    setStoredAdminToken("");
    setStatus(null);
    setUser(null);
    setProjectId("");
    setSignInReason(reason);
    setSignedIn(false);
    window.history.replaceState(null, "", "#/signin");
    setAuthRoute(parseAuthHash());
  }, []);

  const handleSignedIn = useCallback(() => {
    setSignInReason(undefined);
    setSignedIn(true);
    setRefreshToken((n) => n + 1);
    const seg = window.location.hash.replace(/^#\/?/, "").split(/[/?]/)[0] as WorkspaceId;
    if (!ALL_IDS.includes(seg)) {
      window.history.replaceState(null, "", `#/${DEFAULT_TAB}`);
      setActiveTab(DEFAULT_TAB);
    }
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

  // Who is signed in. Admin tokens aren't a person, so /me answers 404 for them.
  useEffect(() => {
    if (!signedIn) return;
    api
      .getMe()
      .then(setUser)
      .catch((err) => {
        if (!isNotFound(err)) setUser(null);
      });
  }, [signedIn]);

  const go = (id: WorkspaceId) => {
    window.location.hash = `/${id}`;
    setActiveTab(id);
  };

  const handleSignOut = async () => {
    try {
      await api.logOut();
    } catch {
      // Session may already be gone; clearing locally is what matters.
    }
    signOut();
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

  if (authRoute.view === "callback") {
    return <AuthCallback code={authRoute.params.get("code")} onSignedIn={handleSignedIn} />;
  }

  if (!signedIn) {
    return authRoute.view === "signup" ? (
      <SignUpScreen onSignedIn={handleSignedIn} />
    ) : (
      <SignInScreen
        onSignedIn={handleSignedIn}
        reason={signInReason}
        googleError={authRoute.params.get("error")}
      />
    );
  }

  const activePid = projectId || status?.active_project_id || "";
  const totalEvents = (status?.l0_rows ?? 0) + (status?.l1_rows ?? 0);
  const projects = status?.projects ?? [];
  const nextTheme: Record<ThemePref, ThemePref> = { system: "light", light: "dark", dark: "system" };
  const ThemeIcon = theme === "light" ? Sun : theme === "dark" ? Moon : Monitor;
  const displayName = user?.name || user?.email || "Admin token";
  const initials =
    (user?.name || user?.email || "A")
      .split(/[\s@.]+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((w) => w[0]?.toUpperCase())
      .join("") || "A";

  const accountControls = (
    <div className="flex items-center gap-0.5">
      <button
        type="button"
        onClick={() => setTheme(nextTheme[theme])}
        className={`${btn.ghost} h-8 w-8 px-0`}
        aria-label={`Theme: ${theme}. Switch to ${nextTheme[theme]}`}
        title={`Theme: ${theme}`}
      >
        <ThemeIcon className="h-4 w-4" />
      </button>
      <button
        type="button"
        onClick={() => void handleSignOut()}
        className={`${btn.ghost} h-8 w-8 px-0`}
        aria-label="Sign out"
        title="Sign out"
      >
        <LogOut className="h-4 w-4" />
      </button>
    </div>
  );

  return (
    <div className="flex min-h-screen flex-col bg-canvas text-ink md:flex-row">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-3 focus:top-3 focus:z-50 focus:rounded-lg focus:bg-panel focus:px-3 focus:py-2"
      >
        Skip to content
      </a>

      {/* Sidebar: full height on desktop, a scrolling tab strip on small screens. */}
      <aside className="shrink-0 border-b border-line bg-panel md:sticky md:top-0 md:flex md:h-screen md:w-60 md:flex-col md:gap-5 md:border-b-0 md:border-r md:px-3.5 md:py-5">
        <div className="flex items-center justify-between gap-2.5 px-4 py-3 md:px-1.5 md:py-0">
          <span className="flex items-center gap-2.5">
            <Logo size="sm" />
            <span className="text-[1.0625rem] font-bold tracking-[-0.01em]">Pulse</span>
          </span>
          <span className="md:hidden">{accountControls}</span>
        </div>

        <label className="mx-4 mb-3 flex flex-col gap-0.5 rounded-lg border border-line bg-canvas px-2.5 py-1.5 md:mx-0 md:mb-0">
          <span className="text-xs text-muted">Project</span>
          <select
            value={activePid}
            onChange={(e) => {
              setProjectId(e.target.value);
              setRefreshToken((n) => n + 1);
            }}
            className="w-full truncate bg-transparent text-sm font-semibold text-ink focus:outline-none"
            disabled={projects.length === 0}
          >
            {projects.length === 0 && <option value={activePid}>{activePid || "Loading…"}</option>}
            {projects.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </label>

        <nav
          aria-label="Workspaces"
          className="no-scrollbar flex gap-1 overflow-x-auto px-2 pb-2 md:block md:flex-1 md:space-y-[18px] md:overflow-y-auto md:px-0 md:pb-0"
        >
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="flex gap-1 md:block md:space-y-0.5">
              {group.label && <div className="hidden px-2.5 pb-1 text-xs text-muted md:block">{group.label}</div>}
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
                    className={`flex h-9 shrink-0 items-center gap-2.5 whitespace-nowrap rounded-lg px-2.5 text-sm ${
                      active
                        ? "bg-brand-soft font-semibold text-brand"
                        : "text-ink-2 hover:bg-hover hover:text-ink"
                    }`}
                  >
                    <Icon className="h-[17px] w-[17px] shrink-0" />
                    {item.label}
                  </a>
                );
              })}
            </div>
          ))}
        </nav>

        <div className="hidden space-y-1.5 px-1.5 text-xs text-muted md:block">
          <div className="tabular">
            <span className="text-ink-2">{formatCount(totalEvents)}</span> events stored
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
          {notice && (
            <p role="status" className="text-ink-2">
              {notice}
            </p>
          )}
        </div>

        <div className="hidden items-center gap-2.5 border-t border-line px-1.5 pt-3 md:flex">
          <span
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-sunken text-[0.8125rem] font-semibold text-ink-2"
            aria-hidden="true"
          >
            {initials}
          </span>
          <span className="flex min-w-0 flex-1 flex-col">
            <span className="truncate text-sm font-semibold">{displayName}</span>
            {user?.name && <span className="truncate text-xs text-muted">{user.email}</span>}
          </span>
          {accountControls}
        </div>
      </aside>

      <main id="main" tabIndex={-1} className="min-w-0 flex-1 px-4 py-7 outline-none md:px-8">
        <div className="mx-auto max-w-[1200px]">
            {!activePid ? (
              <p className="text-sm text-muted" role="status">
                Loading your workspace…
              </p>
            ) : (
              <>
                {activeTab === "dashboards" && (
                  <DashboardsWorkspace projectId={activePid} refreshToken={refreshToken} />
                )}
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
  );
};

export default App;
