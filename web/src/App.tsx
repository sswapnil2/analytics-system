import React, { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Bot,
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
  Search,
  Settings,
  ShieldCheck,
  Sun,
  Terminal,
  UserRound,
  Users,
  X,
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react";
import {
  ADMIN_TOKEN_STORAGE_KEY,
  api,
  ApiAuthError,
  AuthUser,
  getStoredAdminToken,
  hasStoredSession,
  isAbortError,
  NodeStatus,
  roleAtLeast,
  SESSION_HINT_STORAGE_KEY,
  setStoredAdminToken,
  setUnauthorizedHandler,
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
import { ErrorBoundary } from "./components/ErrorBoundary";
import { clearFilterCaches } from "./components/FilterPanel";
import { CommandPalette } from "./components/CommandPalette";
import { btn, EmptyState, ErrorNotice, Kbd, Panel, Spinner, Tabs } from "./components/ui";
import type { Command } from "./lib/palette";
import { errorMessage, formatCount, isNotFound, setProjectTimeZone } from "./lib/format";
import { projectFromHash as projectInHash, withProject } from "./lib/projectUrl";
import { AccessContext } from "./auth/access";

// Route-level code splitting: each workspace loads on demand.
const ProfilePage = lazy(() => import("./account/ProfilePage").then((m) => ({ default: m.ProfilePage })));
const SettingsPage = lazy(() => import("./account/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const DashboardsWorkspace = lazy(() => import("./dashboards/DashboardsWorkspace").then((m) => ({ default: m.DashboardsWorkspace })));
const InsightsWorkspace = lazy(() => import("./workspaces/InsightsWorkspace").then((m) => ({ default: m.InsightsWorkspace })));
const FunnelsWorkspace = lazy(() => import("./workspaces/FunnelsWorkspace").then((m) => ({ default: m.FunnelsWorkspace })));
const FlowsWorkspace = lazy(() => import("./workspaces/FlowsWorkspace").then((m) => ({ default: m.FlowsWorkspace })));
const RetentionWorkspace = lazy(() => import("./workspaces/RetentionWorkspace").then((m) => ({ default: m.RetentionWorkspace })));
const SessionsReplayWorkspace = lazy(() => import("./workspaces/SessionsReplayWorkspace").then((m) => ({ default: m.SessionsReplayWorkspace })));
const AgentObservabilityWorkspace = lazy(() =>
  import("./workspaces/AgentObservabilityWorkspace").then((m) => ({ default: m.AgentObservabilityWorkspace })),
);
const UsersWorkspace = lazy(() => import("./workspaces/UsersWorkspace").then((m) => ({ default: m.UsersWorkspace })));
const CohortsLookupsWorkspace = lazy(() => import("./workspaces/CohortsLookupsWorkspace").then((m) => ({ default: m.CohortsLookupsWorkspace })));
const ExperimentsFlagsWorkspace = lazy(() => import("./workspaces/ExperimentsFlagsWorkspace").then((m) => ({ default: m.ExperimentsFlagsWorkspace })));
const LexiconMcpWorkspace = lazy(() => import("./workspaces/LexiconMcpWorkspace").then((m) => ({ default: m.LexiconMcpWorkspace })));
const SecurityPanel = lazy(() => import("./components/SecurityPanel").then((m) => ({ default: m.SecurityPanel })));

/** Escapes a string for safe embedding inside single-quoted POSIX shell literals. */
function shellSingleQuote(s: string): string {
  return `'${s.replace(/'/g, "'\\''")}'`;
}

type WorkspaceId =
  | "dashboards"
  | "trends"
  | "funnels"
  | "journeys"
  | "retention"
  | "replays"
  | "agents"
  | "users"
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
    items: [
      { id: "replays", label: "Session replays", icon: PlayCircle },
      { id: "agents", label: "AI agents", icon: Bot },
    ],
  },
  {
    label: "Audience",
    items: [
      { id: "users", label: "Users", icon: UserRound },
      { id: "cohorts", label: "Cohorts", icon: Users },
      { id: "experiments", label: "Experiments", icon: FlaskConical },
    ],
  },
  {
    label: "Data",
    items: [
      { id: "events", label: "Events and privacy", icon: ShieldCheck },
      { id: "security", label: "Security and integrations", icon: Lock },
    ],
  },
];

// Profile and Settings open from the account area, not the workspace nav.
const ACCOUNT_IDS: WorkspaceId[] = ["profile", "settings"];
const PAGE_TITLES: Record<string, string> = {
  ...Object.fromEntries(NAV_GROUPS.flatMap((g) => g.items.map((i) => [i.id, i.label]))),
  profile: "Profile",
  settings: "Settings",
};
const AGENT_VIEWS = ["Traces", "Sessions", "Evaluations", "Datasets", "Experiments"];
const ALL_IDS = [...NAV_GROUPS.flatMap((g) => g.items.map((i) => i.id)), ...ACCOUNT_IDS];
// Report builders that default to the project's own events.
const NEEDS_EVENTS: WorkspaceId[] = ["trends", "funnels", "journeys", "retention", "cohorts"];
const DEFAULT_TAB: WorkspaceId = "dashboards";

function tabFromHash(): WorkspaceId {
  const h = window.location.hash.replace(/^#\/?/, "").split(/[/?]/)[0] as WorkspaceId;
  return ALL_IDS.includes(h) ? h : DEFAULT_TAB;
}

type ThemePref = "system" | "light" | "dark";
const THEME_KEY = "pulse_theme";
const PROJECT_KEY = "pulse_project";
const NAV_KEY = "pulse_nav_collapsed";

const projectFromHash = () => projectInHash(window.location.hash);

/** The link's project, else the one this browser last used. */
function initialProject(): string {
  if (projectFromHash()) return projectFromHash();
  try {
    return window.localStorage.getItem(PROJECT_KEY) ?? "";
  } catch {
    return "";
  }
}

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
  const [signedIn, setSignedIn] = useState<boolean>(() => hasStoredSession());
  const [authRoute, setAuthRoute] = useState(parseAuthHash);
  const [signInReason, setSignInReason] = useState<string | undefined>();
  const [activeTab, setActiveTab] = useState<WorkspaceId>(tabFromHash);
  const [projectId, setProjectId] = useState<string>(initialProject);
  const [status, setStatus] = useState<NodeStatus | null>(null);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [availableEvents, setAvailableEvents] = useState<string[]>([]);
  const [availableProps, setAvailableProps] = useState<string[]>([]);
  const [numericProps, setNumericProps] = useState<string[]>([]);
  const [catalogLoaded, setCatalogLoaded] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [refreshToken, setRefreshToken] = useState<number>(0);
  const [theme, setTheme] = useState<ThemePref>(readTheme);
  // Desktop sidebar can shrink to an icon rail; remembered per browser.
  const [navCollapsed, setNavCollapsed] = useState<boolean>(() => {
    try {
      return window.localStorage.getItem(NAV_KEY) === "1";
    } catch {
      return false;
    }
  });
  const toggleNav = () =>
    setNavCollapsed((v) => {
      try {
        window.localStorage.setItem(NAV_KEY, v ? "0" : "1");
      } catch {
        // storage unavailable: the choice lasts this visit
      }
      return !v;
    });
  const nc = navCollapsed ? "md:hidden" : ""; // hidden in the icon rail
  const [flushing, setFlushing] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [quickstartOpen, setQuickstartOpen] = useState(false);
  const [quickstartDismissed, setQuickstartDismissed] = useState(false);
  const [copiedQuickstart, setCopiedQuickstart] = useState(false);
  const [quickstartTab, setQuickstartTab] = useState("Web");
  const [mintedIngestKey, setMintedIngestKey] = useState<string>("");
  const [mintingKey, setMintingKey] = useState(false);

  useEffect(() => applyTheme(theme), [theme]);

  const [paletteOpen, setPaletteOpen] = useState(false);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((v) => !v);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // A new page starts at its top, names the browser tab, and shows itself in the phone nav strip.
  const navRef = useRef<HTMLElement>(null);
  useEffect(() => {
    if (!signedIn) {
      document.title = "Pulse";
      return;
    }
    document.title = `${PAGE_TITLES[activeTab] ?? "Pulse"} · Pulse`;
    window.scrollTo({ top: 0 });
    navRef.current?.querySelector('[aria-current="page"]')?.scrollIntoView({ block: "nearest", inline: "center" });
  }, [activeTab, signedIn]);

  const projectIdRef = useRef(projectId);
  projectIdRef.current = projectId;
  const lastStatusJsonRef = useRef<string>("");
  const catalogAbortRef = useRef<AbortController | null>(null);
  const noticeTimerRef = useRef<number | null>(null);
  const copyTimerRef = useRef<number | null>(null);

  const showNotice = useCallback((msg: string, durationMs = 6000) => {
    setNotice(msg);
    if (noticeTimerRef.current !== null) window.clearTimeout(noticeTimerRef.current);
    noticeTimerRef.current = window.setTimeout(() => {
      setNotice(null);
      noticeTimerRef.current = null;
    }, durationMs);
  }, []);

  useEffect(
    () => () => {
      catalogAbortRef.current?.abort();
      if (noticeTimerRef.current !== null) window.clearTimeout(noticeTimerRef.current);
      if (copyTimerRef.current !== null) window.clearTimeout(copyTimerRef.current);
    },
    [],
  );

  // Reports pick their defaults from the catalog, so wait for the new project's.
  // `fresh`: picked by hand, so drop the report built for the old project from the URL.
  const switchProject = useCallback((id: string, fresh = false) => {
    if (fresh) {
      const [path, search = ""] = window.location.hash.replace(/^#/, "").split("?");
      const params = new URLSearchParams(search);
      params.delete("q");
      params.delete("report");
      params.set("p", id);
      window.history.replaceState(null, "", `#${path}?${params.toString()}`);
    }
    setProjectId(id);
    setCatalogLoaded(false);
    setAvailableEvents([]);
    setAvailableProps([]);
    setMintedIngestKey("");
    setRefreshToken((n) => n + 1);
  }, []);

  useEffect(() => {
    const onHash = () => {
      setActiveTab(tabFromHash());
      setAuthRoute(parseAuthHash());
      const p = projectFromHash();
      if (p && p !== projectIdRef.current) switchProject(p);
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, [switchProject]);

  // Only say a session "ended" if this tab had one; a leftover token from another install just signs out.
  const hadSession = useRef(false);
  const signOut = useCallback((reason?: string, navigate = true) => {
    hadSession.current = false;
    lastStatusJsonRef.current = "";
    catalogAbortRef.current?.abort();
    clearFilterCaches();
    setStoredAdminToken("");
    setStatus(null);
    setUser(null);
    setProjectId("");
    setMintedIngestKey("");
    setSignInReason((prev) => reason ?? prev);
    setSignedIn(false);
    if (navigate) {
      window.history.replaceState(null, "", "#/signin");
      setAuthRoute(parseAuthHash());
    }
  }, []);
  const sessionEnded = useCallback(() => {
    if (!hadSession.current && !hasStoredSession()) return;
    signOut(hadSession.current ? "Your session ended. Sign in again to continue." : undefined);
  }, [signOut]);

  // Global 401 handler (active only when signed in, so SignInScreen probes never trigger signOut)
  // + cross-tab sign-out & account-switch sync.
  useEffect(() => {
    if (signedIn) setUnauthorizedHandler(sessionEnded);
    else setUnauthorizedHandler(null);
    const onStorage = (e: StorageEvent) => {
      if (e.key !== null && e.key !== ADMIN_TOKEN_STORAGE_KEY && e.key !== SESSION_HINT_STORAGE_KEY) return;
      if (!e.newValue) {
        if (hadSession.current) signOut("You were signed out in another tab.");
      } else if (signedIn && e.newValue !== e.oldValue) {
        // Another tab signed in as a different user/session: reset local state to avoid cross-account UI mismatch.
        clearFilterCaches();
        lastStatusJsonRef.current = "";
        setUser(null);
        setStatus(null);
        setMintedIngestKey("");
        setRefreshToken((n) => n + 1);
      }
    };
    window.addEventListener("storage", onStorage);
    return () => {
      setUnauthorizedHandler(null);
      window.removeEventListener("storage", onStorage);
    };
  }, [signedIn, sessionEnded, signOut]);

  const handleSignedIn = useCallback(() => {
    // An invite can sign a different account in over an existing one: drop the old account's state.
    clearFilterCaches();
    lastStatusJsonRef.current = "";
    setUser(null);
    setStatus(null);
    setProjectId(initialProject());
    setMintedIngestKey("");
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
      const serialized = JSON.stringify(s);
      if (serialized !== lastStatusJsonRef.current) {
        lastStatusJsonRef.current = serialized;
        setStatus(s);
      }
      if (!projectId && s.active_project_id) setProjectId(s.active_project_id);
    } catch (err) {
      if (err instanceof ApiAuthError && err.status === 401) return;
      // A link or remembered project this account can't open (deleted, other org).
      if (projectId && ((err instanceof ApiAuthError && err.status === 403) || isNotFound(err))) {
        switchProject("");
        showNotice("That project isn't available to you, so Pulse opened your default project.", 8000);
      }
    }
  }, [projectId, switchProject, showNotice]);

  const loadCatalog = useCallback(async () => {
    if (!projectId) return;
    catalogAbortRef.current?.abort();
    const ctrl = new AbortController();
    catalogAbortRef.current = ctrl;
    setCatalogError(null);
    try {
      const [lex, ext] = await Promise.all([
        api.getLexicon(projectId, ctrl.signal),
        api.getSchemaExtensions(projectId, ctrl.signal),
      ]);
      if (ctrl.signal.aborted) return;
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
      // Profile properties ($set / identify traits) filter and split every report as user.<key>.
      for (const k of ext.user_properties ?? []) propNames.add(`user.${k}`);
      setAvailableEvents(Array.from(evNames));
      setAvailableProps(Array.from(propNames).sort());
      // Numbers as seen at ingest, plus calculated properties (usually arithmetic).
      setNumericProps(
        [...lex.properties.filter((p) => /int|float/.test(p.inferred_types)).map((p) => p.name), ...ext.custom_properties.map((c) => c.name)].sort(),
      );
      setCatalogLoaded(true);
    } catch (err) {
      if (isAbortError(err) || ctrl.signal.aborted) return;
      if (err instanceof ApiAuthError && err.status === 401) return;
      setCatalogError(errorMessage(err));
    }
  }, [projectId]);

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

  const totalEvents = (status?.l0_rows ?? 0) + (status?.l1_rows ?? 0);
  // Hooks stay above the early returns below (rules of hooks).
  // The event list loads once per project; while it's empty, reload it as soon as
  // the status poll sees events, so the first data shows up without a page reload.
  const waitingForFirstEvents = catalogLoaded && availableEvents.length === 0 && totalEvents > 0;
  useEffect(() => {
    if (waitingForFirstEvents) void loadCatalog();
  }, [waitingForFirstEvents, totalEvents, loadCatalog]);
  // Say so when the first event lands, instead of the setup panel silently vanishing.
  const lastTotal = useRef<{ pid: string; total: number } | null>(null);
  useEffect(() => {
    if (!status) return;
    const pid = status.active_project_id;
    const prev = lastTotal.current;
    lastTotal.current = { pid, total: totalEvents };
    if (prev?.pid === pid && prev.total === 0 && totalEvents > 0) {
      showNotice(`First event received: ${formatCount(totalEvents)} so far. Reports update as more arrive.`, 8000);
    }
  }, [status, totalEvents, showNotice]);

  // Explicit admin-token sessions (sk_ / non-ps_ tokens) act as admins; user sessions (ps_ / cookie)
  // fail closed to viewer until /v1/auth/me resolves so viewers never flash admin UI.
  const storedToken = getStoredAdminToken();
  const isAdminTokenSession = Boolean(storedToken) && !storedToken.startsWith("ps_");
  const canManageKeys = isAdminTokenSession || (user !== null && roleAtLeast(user.role, "admin"));
  const canEdit = isAdminTokenSession || (user !== null && roleAtLeast(user.role, "member"));
  const access = useMemo(() => ({ canEdit, canAdmin: canManageKeys }), [canEdit, canManageKeys]);

  // Every Studio URL carries its project (`p=`), so reloads and shared links open the
  // same project; the last one used is remembered for bare URLs.
  const urlPid = projectId || status?.active_project_id || "";
  useEffect(() => {
    if (!signedIn || !urlPid) return;
    try {
      window.localStorage.setItem(PROJECT_KEY, urlPid);
    } catch {
      // Private mode: links still carry the project.
    }
    const sync = () => {
      const next = withProject(window.location.hash, urlPid, ALL_IDS);
      if (next) window.history.replaceState(null, "", next);
    };
    sync();
    // In-app navigation drops `p`; put it back. A link with another `p` is a switch,
    // which the route listener above handles.
    const fill = () => {
      if (!projectFromHash()) sync();
    };
    window.addEventListener("hashchange", fill);
    return () => window.removeEventListener("hashchange", fill);
  }, [signedIn, urlPid]);

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
      showNotice(
        `Saved buffered events to storage (${res.segments_flushed} file${res.segments_flushed === 1 ? "" : "s"}).`,
        5000,
      );
    } catch (err) {
      showNotice(`Couldn't save buffered events: ${errorMessage(err)}`, 5000);
    } finally {
      setFlushing(false);
    }
  };

  // Invite and reset links work whether or not someone is signed in on this browser.
  if (authRoute.view === "invite") {
    return <AcceptInviteScreen token={authRoute.token} onSignedIn={handleSignedIn} />;
  }
  if (authRoute.view === "reset") {
    return (
      <ResetPasswordScreen
        token={authRoute.token}
        onSignedOut={() => signOut(undefined, false)}
      />
    );
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
  const projects = status?.projects ?? [];
  // Set during render so children format with the right zone on this same render.
  setProjectTimeZone(projects.find((p) => p.id === activePid)?.timezone);
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

  const origin = typeof window !== "undefined" ? window.location.origin : "";
  const showQuickstart = quickstartOpen || (status !== null && totalEvents === 0 && !quickstartDismissed);
  const sampleEvent = availableEvents.find((e) => !e.startsWith("$")) ?? "page_viewed";
  const sampleProp = availableProps[0] ?? "plan";
  const keyText = mintedIngestKey || "<PASTE_OR_GENERATE_INGEST_KEY>";
  const originJson = JSON.stringify(origin);
  const keyJson = JSON.stringify(keyText);
  const eventJson = JSON.stringify(sampleEvent);
  const propsJson = JSON.stringify({ [sampleProp]: "pro" });
  const httpBody = JSON.stringify(
    { event: sampleEvent, userId: "user_1", properties: { [sampleProp]: "pro" } },
    null,
    2,
  );
  const quickstartSnippets: Record<string, string> = {
    Web: [
      "npm install @pulse-analytics/ts-web",
      "",
      'import { PulseWebClient } from "@pulse-analytics/ts-web";',
      "",
      `const pulse = new PulseWebClient({ endpointUrl: ${originJson}, apiKey: ${keyJson} });`,
      'pulse.identify("user_1");',
      `pulse.track(${eventJson}, ${propsJson});`,
    ].join("\n"),
    Android: [
      "// Kotlin, with the generated bindings (see docs/mobile-core.md)",
      "val pulse = PulseClient(SdkConfig(",
      `    endpointUrl = ${originJson.replace(/\$/g, "\\$")}, apiKey = ${keyJson.replace(/\$/g, "\\$")},`,
      "    storageDir = context.filesDir.path, flushBatchSize = 50u, flushIntervalMs = 10_000u))",
      'pulse.identify("user_1")',
      `pulse.track(${eventJson.replace(/\$/g, "\\$")}, ${JSON.stringify(propsJson).replace(/\$/g, "\\$")})`,
    ].join("\n"),
    iOS: [
      "// Swift, with the generated bindings (see docs/mobile-core.md)",
      "let pulse = try PulseClient(config: SdkConfig(",
      `    endpointUrl = ${originJson}, apiKey = ${keyJson},`,
      "    storageDir: NSTemporaryDirectory(), flushBatchSize: 50, flushIntervalMs: 10_000))",
      'try pulse.identify(userId: "user_1")',
      `try pulse.track(eventName: ${eventJson}, propsJson: ${JSON.stringify(propsJson)})`,
    ].join("\n"),
    HTTP: [
      `curl -X POST ${shellSingleQuote(`${origin}/v1/ingest`)} \\`,
      "  -H 'Content-Type: application/json' \\",
      `  -H ${shellSingleQuote(`x-pulse-key: ${keyText}`)} \\`,
      `  -d ${shellSingleQuote(httpBody)}`,
    ].join("\n"),
  };
  const curlSnippet = quickstartSnippets[quickstartTab];

  const mintQuickstartKey = async () => {
    if (!activePid) return;
    setMintingKey(true);
    try {
      const created = await api.createApiKey(activePid, { label: "Quickstart Ingest Key" });
      setMintedIngestKey(created.plaintext_key);
    } catch (err) {
      showNotice(`Couldn't create API key: ${errorMessage(err)}`, 6000);
    } finally {
      setMintingKey(false);
    }
  };

  const copyQuickstart = async () => {
    try {
      await navigator.clipboard.writeText(curlSnippet);
      setCopiedQuickstart(true);
      if (copyTimerRef.current !== null) window.clearTimeout(copyTimerRef.current);
      copyTimerRef.current = window.setTimeout(() => {
        setCopiedQuickstart(false);
        copyTimerRef.current = null;
      }, 2000);
    } catch {
      // Clipboard may be restricted in headless contexts.
    }
  };

  const commands: Command[] = [
    ...NAV_GROUPS.flatMap((g) => g.items)
      .filter((i) => i.id !== "security" || canManageKeys)
      .map((i) => ({ id: `go-${i.id}`, label: i.label, group: "Go to", icon: i.icon, run: () => go(i.id) })),
    ...AGENT_VIEWS.map((v) => ({
      id: `agents-${v}`,
      label: `AI agents: ${v}`,
      group: "Go to",
      icon: Bot,
      keywords: "runs spans llm evals",
      run: () => {
        window.location.hash = `/agents?tab=${v.toLowerCase()}`;
      },
    })),
    { id: "go-settings", label: "Settings", group: "Go to", icon: Settings, keywords: "members invites api keys projects", run: () => go("settings") },
    { id: "go-profile", label: "Profile", group: "Go to", icon: UserRound, keywords: "account password", run: () => go("profile") },
    ...projects
      .filter((p) => p.id !== activePid)
      .map((p) => ({ id: `proj-${p.id}`, label: `Switch to ${p.name}`, group: "Projects", keywords: p.id, run: () => switchProject(p.id, true) })),
    ...(["light", "dark", "system"] as ThemePref[])
      .filter((t) => t !== theme)
      .map((t) => ({ id: `theme-${t}`, label: `Use ${t} theme`, group: "Actions", icon: t === "light" ? Sun : t === "dark" ? Moon : Monitor, run: () => setTheme(t) })),
    {
      id: "quickstart",
      label: "Show how to send events",
      group: "Actions",
      icon: Terminal,
      keywords: "quickstart sdk install ingest key curl",
      run: () => {
        setQuickstartDismissed(false);
        setQuickstartOpen(true);
        window.scrollTo({ top: 0 });
      },
    },
    { id: "nav-toggle", label: navCollapsed ? "Expand sidebar" : "Collapse sidebar", group: "Actions", icon: navCollapsed ? PanelLeftOpen : PanelLeftClose, run: toggleNav },
    { id: "signout", label: "Sign out", group: "Actions", icon: LogOut, run: () => void handleSignOut() },
  ];

  const accountControls = (
    <div className={`flex items-center gap-0.5 ${navCollapsed ? "md:flex-col" : ""}`}>
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
      <button
        type="button"
        onClick={toggleNav}
        className={`${btn.ghost} hidden h-8 w-8 px-0 md:inline-flex`}
        aria-label={navCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        aria-expanded={!navCollapsed}
        title={navCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        {navCollapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
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
      <aside className={`shrink-0 border-b border-line bg-panel md:sticky md:top-0 md:flex md:h-screen md:flex-col md:overflow-hidden md:transition-[width] md:duration-200 md:ease-out ${navCollapsed ? "md:w-[4.25rem]" : "md:w-60"} md:gap-4 md:border-b-0 md:border-r md:px-3.5 md:py-5`}>
        <div className="flex items-center justify-between gap-2.5 px-4 py-3 md:px-1.5 md:py-0">
          <span className="flex items-center gap-2.5">
            <Logo size="sm" />
            <span className={`text-[1.0625rem] font-bold tracking-[-0.01em] ${nc}`}>Pulse</span>
          </span>
          <span className="md:hidden">{accountControls}</span>
        </div>

        <label className={`mx-4 mb-3 flex flex-col gap-0.5 rounded-lg border border-line bg-canvas px-2.5 py-1.5 md:mx-0 md:mb-0 ${nc}`}>
          <span className="text-xs text-muted">Project</span>
          <select
            value={activePid}
            onChange={(e) => switchProject(e.target.value, true)}
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

        <button
          type="button"
          onClick={() => setPaletteOpen(true)}
          aria-label="Search pages and actions"
          aria-keyshortcuts="Meta+K Control+K"
          title="Search pages and actions"
          className="mx-4 mb-3 hidden h-9 items-center gap-2.5 rounded-lg border border-line px-2.5 text-sm text-muted hover:bg-hover hover:text-ink md:mx-0 md:mb-0 md:flex"
        >
          <Search className="h-4 w-4 shrink-0" />
          <span className={`flex-1 text-left ${nc}`}>Search</span>
          <span className={nc}><Kbd>{/Mac|iPhone|iPad/.test(navigator.platform) ? "⌘K" : "Ctrl K"}</Kbd></span>
        </button>

        <nav
          ref={navRef}
          aria-label="Workspaces"
          className="no-scrollbar flex gap-1 overflow-x-auto px-2 pb-2 md:block md:flex-1 md:space-y-3 md:overflow-y-auto md:px-0 md:pb-0"
        >
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="flex gap-1 md:block md:space-y-0.5">
              {group.label &&
                (navCollapsed ? (
                  <div className="mx-2.5 mb-2 hidden h-px bg-line md:block" aria-hidden="true" />
                ) : (
                  <div className="hidden px-2.5 pb-1 text-xs text-muted md:block">{group.label}</div>
                ))}
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
                    title={navCollapsed ? item.label : undefined}
                    className={`flex h-9 shrink-0 items-center gap-2.5 whitespace-nowrap rounded-lg px-2.5 text-sm transition-colors duration-150 ${
                      active
                        ? "bg-brand-soft font-semibold text-brand"
                        : "text-ink-2 hover:bg-hover hover:text-ink"
                    }`}
                  >
                    <Icon className="h-[17px] w-[17px] shrink-0" />
                    <span className={navCollapsed ? "md:sr-only" : ""}>{item.label}</span>
                  </a>
                );
              })}
            </div>
          ))}
        </nav>

        <div className={`hidden space-y-1.5 px-1.5 text-xs text-muted md:block ${nc}`}>
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
          <div className={`flex items-center gap-2.5 ${nc}`}>
          <span
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-sunken text-compact font-semibold text-ink-2"
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

      {paletteOpen && <CommandPalette commands={commands} onClose={() => setPaletteOpen(false)} />}

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
              <div className="mb-3">
                <Tabs
                  label="Platform"
                  variant="pill"
                  tabs={Object.keys(quickstartSnippets).map((t) => ({ id: t, label: t }))}
                  value={quickstartTab}
                  onChange={setQuickstartTab}
                />
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
            <ErrorBoundary resetKey={`${activeTab}:${activePid}`}>
              <Suspense
                fallback={
                  <div className="py-12">
                    <Spinner label="Loading workspace…" />
                  </div>
                }
              >
                {activeTab === "profile" ? (
                  <ProfilePage user={user} onUserChanged={setUser} />
                ) : activeTab === "settings" ? (
                  <SettingsPage
                    user={user}
                    projectId={activePid}
                    projectName={projects.find((p) => p.id === activePid)?.name ?? activePid}
                    projects={projects}
                    onProjectCreated={(id) => switchProject(id, true)}
                    onProjectsChanged={() => void loadStatus()}
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
                    {activeTab === "agents" && (
                      <AgentObservabilityWorkspace projectId={activePid} refreshToken={refreshToken} />
                    )}
                    {activeTab === "users" && <UsersWorkspace projectId={activePid} />}
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
                        availableProps={availableProps}
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
                    {activeTab === "security" &&
                      (canManageKeys ? (
                        <SecurityPanel projectId={activePid} />
                      ) : (
                        <Panel>
                          <EmptyState
                            icon={Lock}
                            title="Admin access required"
                            body="Encryption, BYOK keys, and audit logs are restricted to workspace admins and owners."
                            action={
                              <button type="button" onClick={() => go("dashboards")} className={btn.secondary}>
                                Back to Dashboards
                              </button>
                            }
                          />
                        </Panel>
                      ))}
                  </AccessContext.Provider>
                )}
              </Suspense>
            </ErrorBoundary>
        </div>
      </main>
    </div>
  );
};

export default App;
