import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  Check,
  Copy,
  FlaskConical,
  GitBranch,
  CalendarCheck,
  LineChart,
  Filter,
  LayoutDashboard,
  Lock,
  LogOut,
  Monitor,
  Moon,
  PlayCircle,
  Settings,
  ShieldCheck,
  Sun,
  Terminal,
  UserRound,
  Users,
  X,
} from "lucide-react";
import {
  api,
  ApiAuthError,
  AuthUser,
  getStoredAdminToken,
  NodeStatus,
  roleAtLeast,
  setStoredAdminToken,
} from "./api/client";
import {
  AcceptInviteScreen,
  AuthCallback,
  ForgotPasswordScreen,
  Logo,
  parseAuthHash,
  ResetPasswordScreen,
  SignInScreen,
  VerifyEmailScreen,
  SignUpScreen,
} from "./auth/AuthScreens";
import { ProfilePage } from "./account/ProfilePage";
import { SettingsPage } from "./account/SettingsPage";
import { btn, EmptyState, ErrorNotice, Panel } from "./components/ui";
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
import { ByokCommercialPanel } from "./components/ByokCommercialPanel";
import { AccessContext } from "./auth/access";

type WorkspaceId =
  | "dashboards"
  | "trends"
  | "funnels"
  | "journeys"
  | "retention"
  | "replays"
  | "cohorts"
  | "experiments"
  | "events"
  | "security"
  | "profile"
  | "settings";

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
      { id: "security", label: "Enterprise and security", icon: Lock },
    ],
  },
];

// Profile and Settings open from the account area, not the workspace nav.
const ACCOUNT_IDS: WorkspaceId[] = ["profile", "settings"];
const ALL_IDS = [...NAV_GROUPS.flatMap((g) => g.items.map((i) => i.id)), ...ACCOUNT_IDS];
// Report builders that default to the project's own events.
const NEEDS_EVENTS: WorkspaceId[] = ["trends", "funnels", "journeys", "retention", "cohorts"];
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
  const [numericProps, setNumericProps] = useState<string[]>([]);
  const [catalogLoaded, setCatalogLoaded] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [refreshToken, setRefreshToken] = useState<number>(0);
  const [theme, setTheme] = useState<ThemePref>(readTheme);
  const [flushing, setFlushing] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [quickstartOpen, setQuickstartOpen] = useState(false);
  const [quickstartDismissed, setQuickstartDismissed] = useState(false);
  const [copiedQuickstart, setCopiedQuickstart] = useState(false);
  const [quickstartTab, setQuickstartTab] = useState("Web");
  const [mintedIngestKey, setMintedIngestKey] = useState<string>("");
  const [mintingKey, setMintingKey] = useState(false);

  useEffect(() => applyTheme(theme), [theme]);

  useEffect(() => {
    const onHash = () => {
      setActiveTab(tabFromHash());
      setAuthRoute(parseAuthHash());
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  // Only say a session "ended" if this tab had one; a leftover token from another install just signs out.
  const hadSession = useRef(false);
  const signOut = useCallback((reason?: string) => {
    hadSession.current = false;
    setStoredAdminToken("");
    setStatus(null);
    setUser(null);
    setProjectId("");
    setSignInReason(reason);
    setSignedIn(false);
    window.history.replaceState(null, "", "#/signin");
    setAuthRoute(parseAuthHash());
  }, []);
  const sessionEnded = useCallback(
    () => signOut(hadSession.current ? "Your session ended. Sign in again to continue." : undefined),
    [signOut],
  );

  const handleSignedIn = useCallback(() => {
    // An invite can sign a different account in over an existing one: drop the old account's state.
    setUser(null);
    setStatus(null);
    setProjectId("");
    setSignInReason(undefined);
    setSignedIn(true);
    setRefreshToken((n) => n + 1);
    // Signing in from an invite link ("Sign in as …") goes back to that invite.
    const next = new URLSearchParams(window.location.hash.split("?")[1] ?? "").get("next");
    const toInvite = !!next && /^invite\/[0-9a-f]+$/.test(next);
    if (toInvite) window.history.replaceState(null, "", `#/${next}`);
    const seg = window.location.hash.replace(/^#\/?/, "").split(/[/?]/)[0] as WorkspaceId;
    if (!toInvite && !ALL_IDS.includes(seg)) {
      window.history.replaceState(null, "", `#/${DEFAULT_TAB}`);
    }
    // replaceState fires no hashchange, so re-read the route (leaves invite/reset screens).
    setActiveTab(tabFromHash());
    setAuthRoute(parseAuthHash());
  }, []);

  const loadStatus = useCallback(async () => {
    try {
      const s = await api.getStatus(projectId || undefined);
      hadSession.current = true;
      setStatus(s);
      if (!projectId && s.active_project_id) setProjectId(s.active_project_id);
    } catch (err) {
      if (err instanceof ApiAuthError && err.status === 401) sessionEnded();
    }
  }, [projectId, sessionEnded]);

  const loadCatalog = useCallback(async () => {
    if (!projectId) return;
    setCatalogError(null);
    try {
      const [lex, ext] = await Promise.all([
        api.getLexicon(projectId),
        api.getSchemaExtensions(projectId),
      ]);
      // Busiest events first: reports default to them and pickers list them on top.
      const sortedLex = [...lex.events]
        .filter((ev) => ev.status !== "dropped" && ev.status !== "hidden" && !ev.name.startsWith("$"))
        .sort((a, b) => b.volume_24h - a.volume_24h || a.name.localeCompare(b.name));
      const evNames = new Set<string>();
      for (const ev of sortedLex) evNames.add(ev.name);
      for (const ce of ext.custom_events) evNames.add(ce.name);
      const propNames = new Set<string>();
      for (const p of lex.properties) propNames.add(p.name);
      for (const cp of ext.custom_properties) propNames.add(cp.name);
      for (const lt of ext.lookup_tables)
        for (const col of lt.sample_columns) propNames.add(`${lt.name}.${col}`);
      setAvailableEvents(Array.from(evNames));
      setAvailableProps(Array.from(propNames).sort());
      // Numbers as seen at ingest, plus calculated properties (usually arithmetic).
      setNumericProps(
        [...lex.properties.filter((p) => /int|float/.test(p.inferred_types)).map((p) => p.name), ...ext.custom_properties.map((c) => c.name)].sort(),
      );
      setCatalogLoaded(true);
    } catch (err) {
      if (err instanceof ApiAuthError && err.status === 401) sessionEnded();
      else setCatalogError(errorMessage(err));
    }
  }, [projectId, sessionEnded]);

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
  }, [signedIn, refreshToken]);

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

  // Invite and reset links work whether or not someone is signed in on this browser.
  if (authRoute.view === "invite") {
    return <AcceptInviteScreen token={authRoute.token} onSignedIn={handleSignedIn} />;
  }
  if (authRoute.view === "reset") {
    return <ResetPasswordScreen token={authRoute.token} />;
  }
  if (authRoute.view === "verify") {
    return <VerifyEmailScreen token={authRoute.token} onSignedIn={handleSignedIn} />;
  }

  if (authRoute.view === "callback") {
    return <AuthCallback code={authRoute.params.get("code")} onSignedIn={handleSignedIn} />;
  }

  if (!signedIn) {
    return authRoute.view === "signup" ? (
      <SignUpScreen onSignedIn={handleSignedIn} />
    ) : authRoute.view === "forgot" ? (
      <ForgotPasswordScreen />
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
  // The event list loads once per project; while it's empty, reload it as soon as
  // the status poll sees events, so the first data shows up without a page reload.
  const waitingForFirstEvents = catalogLoaded && availableEvents.length === 0 && totalEvents > 0;
  useEffect(() => {
    if (waitingForFirstEvents) void loadCatalog();
  }, [waitingForFirstEvents, totalEvents, loadCatalog]);
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

  // Admin tokens have no user and act as admins; signed-in users by their role.
  const canManageKeys = !user || roleAtLeast(user.role, "admin");
  const access = { canEdit: !user || roleAtLeast(user.role, "member"), canAdmin: canManageKeys };
  const origin = typeof window !== "undefined" ? window.location.origin : "";
  const showQuickstart = quickstartOpen || (status !== null && totalEvents === 0 && !quickstartDismissed);
  const sampleEvent = availableEvents.find((e) => !e.startsWith("$")) ?? "page_viewed";
  const sampleProp = availableProps[0] ?? "plan";
  const keyText = mintedIngestKey || "<PASTE_OR_GENERATE_INGEST_KEY>";
  const quickstartSnippets: Record<string, string> = {
    Web: `npm install @pulse-analytics/ts-web

import { PulseWebClient } from "@pulse-analytics/ts-web";

const pulse = new PulseWebClient({ endpointUrl: "${origin}", apiKey: "${keyText}" });
pulse.identify("user_1");
pulse.track("${sampleEvent}", { ${sampleProp}: "pro" });`,
    Android: `// Kotlin, with the generated bindings (see docs/mobile-core.md)
val pulse = PulseClient(SdkConfig(
    endpointUrl = "${origin}", apiKey = "${keyText}",
    storageDir = context.filesDir.path, flushBatchSize = 50u, flushIntervalMs = 10_000u))
pulse.identify("user_1")
pulse.track("${sampleEvent}", """{"${sampleProp}": "pro"}""")`,
    iOS: `// Swift, with the generated bindings (see docs/mobile-core.md)
let pulse = try PulseClient(config: SdkConfig(
    endpointUrl: "${origin}", apiKey: "${keyText}",
    storageDir: NSTemporaryDirectory(), flushBatchSize: 50, flushIntervalMs: 10_000))
try pulse.identify(userId: "user_1")
try pulse.track(eventName: "${sampleEvent}", propsJson: #"{"${sampleProp}": "pro"}"#)`,
    HTTP: `curl -X POST "${origin}/v1/ingest" \\
  -H "Content-Type: application/json" \\
  -H "x-pulse-key: ${keyText}" \\
  -d '{
    "event": "${sampleEvent}",
    "userId": "user_1",
    "properties": { "${sampleProp}": "pro" }
  }'`,
  };
  const curlSnippet = quickstartSnippets[quickstartTab];

  const mintQuickstartKey = async () => {
    if (!activePid) return;
    setMintingKey(true);
    try {
      const created = await api.createApiKey(activePid, { label: "Quickstart Ingest Key" });
      setMintedIngestKey(created.plaintext_key);
    } catch (err) {
      setNotice(`Couldn't create API key: ${errorMessage(err)}`);
    } finally {
      setMintingKey(false);
    }
  };

  const copyQuickstart = async () => {
    try {
      await navigator.clipboard.writeText(curlSnippet);
      setCopiedQuickstart(true);
      window.setTimeout(() => setCopiedQuickstart(false), 2000);
    } catch {
      // Clipboard may be restricted in headless contexts.
    }
  };

  const accountControls = (
    <div className="flex items-center gap-0.5">
      <button
        type="button"
        onClick={() => {
          setQuickstartDismissed(false);
          setQuickstartOpen((v) => !v);
        }}
        className={`${btn.ghost} h-8 w-8 px-0`}
        aria-label="Send events quickstart"
        title="Send events quickstart"
      >
        <Terminal className="h-4 w-4" />
      </button>
      <a
        href="#/profile"
        onClick={(e) => {
          e.preventDefault();
          go("profile");
        }}
        className={`${btn.ghost} h-8 w-8 px-0 md:hidden`}
        aria-label="Profile"
        title="Profile"
      >
        <UserRound className="h-4 w-4" />
      </a>
      <a
        href="#/settings"
        onClick={(e) => {
          e.preventDefault();
          go("settings");
        }}
        aria-current={activeTab === "settings" ? "page" : undefined}
        className={`${btn.ghost} h-8 w-8 px-0 ${activeTab === "settings" ? "text-brand" : ""}`}
        aria-label="Settings"
        title="Settings: members, invites and API keys"
      >
        <Settings className="h-4 w-4" />
      </a>
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
              // Reports pick their defaults from the catalog, so wait for the new project's.
              setCatalogLoaded(false);
              setAvailableEvents([]);
              setAvailableProps([]);
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
              {group.items.filter((item) => item.id !== "security" || canManageKeys).map((item) => {
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
          {canManageKeys && (status?.l0_rows ?? 0) > 0 && (
            <button
              type="button"
              onClick={() => void handleFlush()}
              disabled={flushing}
              className="text-left text-brand hover:underline disabled:opacity-50"
              title="Writes events still held in memory to durable storage"
            >
              {flushing ? "Saving…" : `${formatCount(status?.l0_rows)} recent events in memory · Save to disk`}
            </button>
          )}
          {notice && (
            <p role="status" className="text-ink-2">
              {notice}
            </p>
          )}
        </div>

        <div className="hidden flex-col gap-2 border-t border-line px-1.5 pt-3 md:flex">
          <div className="flex items-center gap-2.5">
          <span
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-sunken text-[0.8125rem] font-semibold text-ink-2"
            aria-hidden="true"
          >
            {initials}
          </span>
          <a
            href="#/profile"
            onClick={(e) => {
              e.preventDefault();
              go("profile");
            }}
            aria-current={activeTab === "profile" ? "page" : undefined}
            title="Your profile"
            className="-mx-1 flex min-w-0 flex-1 flex-col rounded-md px-1 hover:bg-hover"
          >
            <span className={`truncate text-sm font-semibold ${activeTab === "profile" ? "text-brand" : ""}`}>
              {displayName}
            </span>
            {user?.name && <span className="truncate text-xs text-muted">{user.email}</span>}
          </a>
          </div>
          {accountControls}
        </div>
      </aside>

      <main id="main" tabIndex={-1} className="min-w-0 flex-1 px-4 py-7 outline-none md:px-8">
        <div className="mx-auto max-w-[1200px] space-y-5">
          {showQuickstart && (
            <Panel
              title={totalEvents === 0 ? "Send your first event" : "Send events to this project"}
              meta={activePid || undefined}
              actions={
                <div className="flex items-center gap-2">
                  {canManageKeys && (<>
                  <button
                    type="button"
                    disabled={mintingKey || !activePid}
                    onClick={() => void mintQuickstartKey()}
                    className={btn.secondary}
                  >
                    {mintingKey ? "Generating…" : mintedIngestKey ? "Generate another key" : "Generate ingest key"}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      go("settings");
                      setQuickstartOpen(false);
                    }}
                    className={btn.ghost}
                  >
                    Manage keys
                  </button>
                  </>)}
                  <button
                    type="button"
                    onClick={() => {
                      setQuickstartOpen(false);
                      setQuickstartDismissed(true);
                    }}
                    className={`${btn.ghost} h-8 w-8 px-0`}
                    aria-label="Close quickstart"
                    title="Close"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
              }
            >
              <div role="tablist" aria-label="Platform" className="mb-3 flex gap-1">
                {Object.keys(quickstartSnippets).map((t) => (
                  <button
                    key={t}
                    type="button"
                    role="tab"
                    aria-selected={quickstartTab === t}
                    onClick={() => setQuickstartTab(t)}
                    className={`rounded-md px-2.5 py-1 text-sm font-medium ${quickstartTab === t ? "bg-sunken text-ink" : "text-muted hover:text-ink"}`}
                  >
                    {t}
                  </button>
                ))}
              </div>
              {quickstartTab === "HTTP" && (
                <p className="mb-3 text-sm text-ink-2">
                  Accepts Segment, top-level arrays, or Pulse native JSON batches at <code className="font-mono">/v1/ingest</code>.
                </p>
              )}
              <div className="relative rounded-lg border border-line bg-sunken p-3.5">
                <button
                  type="button"
                  onClick={() => void copyQuickstart()}
                  className={`${btn.ghost} absolute right-2 top-2 h-7 px-2 text-xs`}
                >
                  {copiedQuickstart ? <Check className="h-3.5 w-3.5 text-ok" /> : <Copy className="h-3.5 w-3.5" />}
                  {copiedQuickstart ? "Copied" : "Copy"}
                </button>
                <pre className="overflow-x-auto pr-20 font-mono text-xs leading-relaxed text-ink">
                  {curlSnippet}
                </pre>
              </div>
            </Panel>
          )}
            {activeTab === "profile" ? (
              <ProfilePage user={user} onUserChanged={setUser} />
            ) : activeTab === "settings" ? (
              <SettingsPage
                user={user}
                projectId={activePid}
                projectName={projects.find((p) => p.id === activePid)?.name ?? activePid}
              />
            ) : catalogError && NEEDS_EVENTS.includes(activeTab) ? (
              <ErrorNotice message={catalogError} onRetry={() => void loadCatalog()} />
            ) : !activePid || (!catalogLoaded && NEEDS_EVENTS.includes(activeTab)) ? (
              <p className="text-sm text-muted" role="status">
                Loading your workspace…
              </p>
            ) : availableEvents.length === 0 && NEEDS_EVENTS.includes(activeTab) ? (
              <EmptyState
                title="No events in this project yet"
                body="Reports fill in once your app sends its first event."
                action={
                  <button
                    type="button"
                    onClick={() => {
                      setQuickstartDismissed(false);
                      setQuickstartOpen(true);
                    }}
                    className={btn.primary}
                  >
                    Show how to send events
                  </button>
                }
              />
            ) : (
              <AccessContext.Provider key={activePid} value={access}>
                {activeTab === "dashboards" && (
                  <DashboardsWorkspace projectId={activePid} refreshToken={refreshToken} />
                )}
                {activeTab === "trends" && (
                  <InsightsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
                    numericProps={numericProps}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "funnels" && (
                  <FunnelsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "journeys" && (
                  <FlowsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "retention" && (
                  <RetentionWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
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
                    availableProps={availableProps}
                    onCatalogChanged={() => {
                      void loadCatalog();
                      setRefreshToken((n) => n + 1);
                    }}
                  />
                )}
                {activeTab === "experiments" && (
                  <ExperimentsFlagsWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    refreshToken={refreshToken}
                  />
                )}
                {activeTab === "events" && (
                  <LexiconMcpWorkspace
                    projectId={activePid}
                    availableEvents={availableEvents}
                    availableProps={availableProps}
                    onLexiconChanged={() => {
                      void loadCatalog();
                      setRefreshToken((n) => n + 1);
                    }}
                  />
                )}
                {activeTab === "security" && canManageKeys && (
                  <ByokCommercialPanel projectId={activePid} />
                )}
              </AccessContext.Provider>
            )}
        </div>
      </main>
    </div>
  );
};

export default App;
